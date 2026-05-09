import json
import os
import time
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

import nextcord
from nextcord.ext import commands


def _data_path() -> Path:
    return Path(os.getenv("PARK_DATA_PATH", "data/parked.json"))


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _now_ts() -> int:
    return int(time.time())


class ParkCog(commands.Cog):
    """Simple parked-turn tracker for pbp servers."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.data = self._load()

    # ---------- storage ----------
    def _load(self) -> dict:
        path = _data_path()
        if path.exists():
            try:
                return json.loads(path.read_text(encoding="utf-8"))
            except Exception:
                pass
        return {"guilds": {}}

    def _save(self) -> None:
        _atomic_write_json(_data_path(), self.data)

    def _g(self, guild_id: int) -> dict:
        g = self.data["guilds"].setdefault(str(guild_id), {})
        g.setdefault("parks", {})  # user_id -> [entries]
        g.setdefault("settings", {
            "remind_in_channel": True,
            "dm_user": False,
            "max_reason_len": 500,
        })
        return g

    # ---------- permissions ----------
    def _can_manage_parks(self, member: nextcord.Member) -> bool:
        perms = getattr(member, "guild_permissions", None)
        if perms is None:
            return False
        return bool(
            perms.administrator
            or perms.manage_guild
            or perms.manage_messages
            or perms.manage_threads
        )

    async def _fetch_member_safe(self, guild: nextcord.Guild, user_id: int) -> Optional[nextcord.Member]:
        member = guild.get_member(user_id)
        if member is not None:
            return member
        try:
            return await guild.fetch_member(user_id)
        except Exception:
            return None

    def _active_entries(self, g: dict, user_id: int) -> List[Dict[str, Any]]:
        rows = g.get("parks", {}).get(str(user_id), []) or []
        return [r for r in rows if isinstance(r, dict) and not r.get("cleared")]

    def _active_entry_refs(self, rows: List[Dict[str, Any]]) -> List[Tuple[int, Dict[str, Any]]]:
        active: List[Tuple[int, Dict[str, Any]]] = []
        for row in rows or []:
            if isinstance(row, dict) and not row.get("cleared"):
                active.append((len(active) + 1, row))
        return active

    def _display_reason(self, entry: dict) -> str:
        reason = str(entry.get("reason") or "").strip()
        if not reason:
            origin = str(entry.get("origin_jump_url") or "").strip()
            if origin:
                return f"Context: {origin}"
            return "No reason provided."
        return reason

    async def _send_park_reminder(self, message: nextcord.Message, entries: List[Dict[str, Any]]) -> None:
        if not entries:
            return

        g = self._g(message.guild.id)
        settings = g.get("settings", {}) or {}

        lines = []
        for idx, entry in enumerate(entries, start=1):
            ch_id = int(entry.get("origin_channel_id") or 0)
            parked_by = int(entry.get("parked_by") or 0)
            ts = int(entry.get("created_ts") or _now_ts())
            waiting = self._display_reason(entry)

            prefix = f"**{idx}.** "
            if ch_id:
                prefix += f"<#{ch_id}>"
            else:
                prefix += "(unknown channel)"
            if parked_by:
                prefix += f" • parked by <@{parked_by}>"
            prefix += f" • <t:{ts}:R>"
            lines.append(prefix)
            lines.append(waiting)

        embed = nextcord.Embed(
            title="⏸️ You have parked turns waiting",
            description="\n".join(lines[:20]),
            color=nextcord.Color.orange(),
        )
        embed.set_footer(text="After you act, have a DM run !unpark @you or !unpark @you <#>")

        if bool(settings.get("remind_in_channel", True)):
            try:
                await message.channel.send(
                    content=f"{message.author.mention} you still have parked turns waiting.",
                    embed=embed,
                    allowed_mentions=nextcord.AllowedMentions(users=True, roles=False, everyone=False),
                )
            except Exception:
                pass

        if bool(settings.get("dm_user", False)):
            try:
                await message.author.send(embed=embed)
            except Exception:
                pass

    # ---------- listeners ----------
    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        if not message.guild or message.author.bot:
            return

        g = self._g(message.guild.id)
        entries = self._active_entries(g, message.author.id)
        if not entries:
            return

        await self._send_park_reminder(message, entries)

    # ---------- commands ----------
    @commands.group(name="park", invoke_without_command=True)
    async def park(self, ctx: commands.Context, target: nextcord.Member = None, *, reason: str = ""):
        if target is None:
            await ctx.send("Usage: `!park @user <reason/link>` or `!park list`")
            return
        if not ctx.guild:
            return
        if not self._can_manage_parks(ctx.author):
            await ctx.send("❌ You need Manage Messages, Manage Threads, Manage Server, or Administrator to park someone.")
            return

        g = self._g(ctx.guild.id)
        max_reason_len = int((g.get("settings") or {}).get("max_reason_len", 500))
        reason = (reason or "").strip()
        if len(reason) > max_reason_len:
            reason = reason[:max_reason_len - 1] + "…"

        entry = {
            "created_ts": _now_ts(),
            "parked_by": int(ctx.author.id),
            "reason": reason,
            "origin_channel_id": int(ctx.channel.id),
            "origin_jump_url": getattr(ctx.message, "jump_url", ""),
            "cleared": False,
            "cleared_ts": 0,
            "cleared_by": 0,
        }

        g.setdefault("parks", {}).setdefault(str(target.id), []).append(entry)
        self._save()

        active_count = len(self._active_entries(g, target.id))
        embed = nextcord.Embed(
            title="⏸️ Turn parked",
            description=(
                f"Target: {target.mention}\n"
                f"Channel: {ctx.channel.mention}\n"
                f"Active parked turns: **{active_count}**"
            ),
            color=nextcord.Color.orange(),
        )
        embed.add_field(name="Waiting on", value=self._display_reason(entry), inline=False)
        await ctx.send(embed=embed)

    @commands.command(name="unpark")
    async def unpark(self, ctx: commands.Context, target: nextcord.Member, which: Optional[int] = None):
        if not ctx.guild:
            return
        if not self._can_manage_parks(ctx.author):
            await ctx.send("❌ You need Manage Messages, Manage Threads, Manage Server, or Administrator to unpark someone.")
            return

        g = self._g(ctx.guild.id)
        rows = g.setdefault("parks", {}).get(str(target.id), []) or []
        active_refs = self._active_entry_refs(rows)
        if not active_refs:
            await ctx.send(f"ℹ️ {target.mention} has no parked turns right now.")
            return

        now = _now_ts()

        if which is None:
            cleared = 0
            for _, row in active_refs:
                row["cleared"] = True
                row["cleared_ts"] = now
                row["cleared_by"] = int(ctx.author.id)
                cleared += 1

            self._save()
            await ctx.send(f"✅ Cleared **{cleared}** parked turn{'s' if cleared != 1 else ''} for {target.mention}.")
            return

        if which < 1:
            await ctx.send("❌ Park number must be 1 or higher. Example: `!unpark @user 2`")
            return
        if which > len(active_refs):
            await ctx.send(
                f"❌ {target.mention} only has **{len(active_refs)}** active parked turn"
                f"{'s' if len(active_refs) != 1 else ''}. Use `!park list` to see the numbering."
            )
            return

        _, row = active_refs[which - 1]
        row["cleared"] = True
        row["cleared_ts"] = now
        row["cleared_by"] = int(ctx.author.id)
        self._save()

        reason = self._display_reason(row)
        if len(reason) > 180:
            reason = reason[:179] + "…"

        await ctx.send(
            f"✅ Cleared parked turn **#{which}** for {target.mention}.\n"
            f"Waiting on was: {reason}"
        )

    @park.command(name="list")
    async def park_list(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await self._send_park_list(ctx)

    @commands.command(name="parklist")
    async def parklist_alias(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await self._send_park_list(ctx)

    @commands.command(name="park_list")
    async def park_list_alias(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await self._send_park_list(ctx)

    async def _send_park_list(self, ctx: commands.Context):
        if not self._can_manage_parks(ctx.author):
            await ctx.send("❌ You need Manage Messages, Manage Threads, Manage Server, or Administrator to view the park list.")
            return

        g = self._g(ctx.guild.id)
        parks = g.get("parks", {}) or {}
        lines = []

        for uid_str, rows in parks.items():
            try:
                uid = int(uid_str)
            except Exception:
                continue
            active = [r for r in (rows or []) if isinstance(r, dict) and not r.get("cleared")]
            if not active:
                continue

            member = ctx.guild.get_member(uid)
            mention = member.mention if member else f"<@{uid}>"
            lines.append(f"{mention} — **{len(active)}** parked")

            for idx, entry in enumerate(active[:5], start=1):
                ch_id = int(entry.get("origin_channel_id") or 0)
                ch_txt = f"<#{ch_id}>" if ch_id else "(unknown channel)"
                lines.append(f"  {idx}. {ch_txt} • {self._display_reason(entry)}")

        if not lines:
            await ctx.send("✅ Nobody is parked right now.")
            return

        per_page = 20
        total_pages = (len(lines) + per_page - 1) // per_page
        for page in range(total_pages):
            page_lines = lines[page * per_page:(page + 1) * per_page]
            embed = nextcord.Embed(
                title="⏸️ Parked turns",
                description="\n".join(page_lines),
                color=nextcord.Color.orange(),
            )
            embed.set_footer(text=f"Page {page + 1}/{total_pages} • Use !unpark @user <#> to clear one")
            await ctx.send(embed=embed)



def setup(bot: commands.Bot):
    bot.add_cog(ParkCog(bot))
