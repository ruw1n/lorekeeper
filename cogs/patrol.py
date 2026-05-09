import json
import os
import time
from pathlib import Path
from typing import Optional, List, Dict, Any

import nextcord
from nextcord.ext import commands


RANKS = [
    ("Recruit", 1, 3),
    ("Private", 4, 6),
    ("Corporal", 7, 9),
]

PATROL_ROLE_ID = 1239250114409922590


def _data_path() -> Path:
    return Path(os.getenv("PATROL_DATA_PATH", "data/patrols.json"))


def _atomic_write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


def _now_ts() -> int:
    return int(time.time())


def _display_name(char_name: str) -> str:
    return str(char_name or "").replace("_", " ").strip()


def _char_xp_path(char_name: str) -> Path:
    return Path("data") / "xp" / f"{str(char_name).replace(' ', '_')}.json"


def _infer_level_from_xp(char_name: str) -> Optional[int]:
    path = _char_xp_path(char_name)
    if not path.exists():
        return None
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(rows, list) or not rows:
            return None
        for row in reversed(rows):
            try:
                lvl = int(row.get("after_lvl"))
                if 1 <= lvl <= 20:
                    return lvl
            except Exception:
                continue
    except Exception:
        return None
    return None


def _rank_for_level(level: int) -> Optional[tuple[str, str]]:
    for label, low, high in RANKS:
        if low <= level <= high:
            return label, f"{low}-{high}"
    return None


def _age_text(since_ts: int) -> str:
    delta = max(0, _now_ts() - int(since_ts or 0))
    days = delta // 86400
    if days >= 1:
        return f"{days} DAY{'S' if days != 1 else ''} READY"
    hours = delta // 3600
    if hours >= 1:
        return f"{hours} HR{'S' if hours != 1 else ''} READY"
    mins = delta // 60
    if mins >= 5:
        return f"{mins} MIN READY"
    return "JUST ADDED"


class PatrolCog(commands.Cog):
    """Simple patrol board for mini."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.data = self._load()

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
        g = self.data.setdefault("guilds", {}).setdefault(str(guild_id), {})
        g.setdefault("entries", {})
        return g

    def _is_patrol_dm(self, member: nextcord.Member) -> bool:
        perms = getattr(member, "guild_permissions", None)
        if not perms:
            return False
        return any([
            perms.administrator,
            perms.manage_guild,
            perms.manage_messages,
            perms.manage_threads,
        ])

    def _mentions_patrol_role(self, message: nextcord.Message) -> bool:
        if not message.guild:
            return False
        return any(role.id == PATROL_ROLE_ID for role in message.role_mentions)

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        if message.author.bot:
            return
        if message.guild is None:
            return
        if message.content.startswith("!"):
            return
        if not self._mentions_patrol_role(message):
            return

        count = len((self._g(message.guild.id).get("entries") or {}))

        await message.channel.send(
            content="📜 **This is who has signed up for patrols already:**",
            embed=self._board_embed(message.guild),
            allowed_mentions=nextcord.AllowedMentions.none(),
        )

        
    def _board_embed(self, guild: nextcord.Guild) -> nextcord.Embed:
        g = self._g(guild.id)
        entries = list((g.get("entries") or {}).values())

        by_rank: Dict[str, List[Dict[str, Any]]] = {label: [] for label, _, _ in RANKS}
        for e in entries:
            rank = str(e.get("rank") or "")
            if rank in by_rank:
                by_rank[rank].append(e)

        for rank_entries in by_rank.values():
            rank_entries.sort(key=lambda e: (int(e.get("since", 0) or 0), int(e.get("level", 0) or 0), str(e.get("char_name") or "").lower()))

        total = len(entries)
        embed = nextcord.Embed(
            title="🪖 Patrol Board",
            description=(
                f"**{total}** adventurer{'s' if total != 1 else ''} currently listed for patrol.\n"
                "Players can join with `!patrol <level> [character]`."
            ),
        )

        for label, low, high in RANKS:
            rank_entries = by_rank[label]
            if rank_entries:
                lines = []
                for e in rank_entries:
                    uid = int(e.get("user_id") or 0)
                    char_name = _display_name(str(e.get("char_name") or "Unknown"))
                    lvl = int(e.get("level") or 0)
                    age = _age_text(int(e.get("since") or 0))
                    lines.append(f"• <@{uid}> ({char_name} {lvl}) · {age}")
                value = "\n".join(lines)
            else:
                value = f"No levels {low}–{high} in queue."
            embed.add_field(name=f"{label} ({low}–{high})", value=value[:1024], inline=False)

        embed.set_footer(text="Use !patrol <level> [character], !patrol remove")
        return embed

    async def _send_board(self, ctx: commands.Context, *, content: Optional[str] = None) -> None:
        await ctx.send(content=content, embed=self._board_embed(ctx.guild))

    def _upsert_entry(self, guild_id: int, user_id: int, *, char_name: str, level: int) -> dict:
        rank = _rank_for_level(level)
        if rank is None:
            raise ValueError("Level must be from 1 to 9.")
        rank_name, band = rank
        g = self._g(guild_id)
        entries = g.setdefault("entries", {})
        prev = entries.get(str(user_id)) or {}
        since = int(prev.get("since") or _now_ts())
        entry = {
            "user_id": int(user_id),
            "char_name": str(char_name),
            "level": int(level),
            "rank": rank_name,
            "band": band,
            "since": since,
            "updated_ts": _now_ts(),
        }
        entries[str(user_id)] = entry
        self._save()
        return entry

    def _remove_entry(self, guild_id: int, user_id: int) -> Optional[dict]:
        g = self._g(guild_id)
        entries = g.setdefault("entries", {})
        removed = entries.pop(str(user_id), None)
        self._save()
        return removed

    def _clear_entries(self, guild_id: int) -> int:
        g = self._g(guild_id)
        entries = g.setdefault("entries", {})
        count = len(entries)
        entries.clear()
        self._save()
        return count
        
    def _resolve_char_name(self, ctx: commands.Context, explicit_name: str = "") -> str:
        explicit_name = str(explicit_name or "").strip()
        if explicit_name:
            return explicit_name.replace(" ", "_")
        return ctx.author.display_name.replace(" ", "_")

    @commands.command(name="patrol", aliases=["patrols"])
    async def patrol(self, ctx: commands.Context, *args: str):
        """Patrol board / signup.

        Examples:
        !patrol
        !patrol 5
        !patrol 5 Lok
        !patrol remove
        !patrol remove @user
        """
        if ctx.guild is None:
            await ctx.send("❌ Patrols only work in a server.")
            return

        tokens = [str(a).strip() for a in args if str(a).strip()]
        head = tokens[0].lower() if tokens else ""

        if not tokens or head in {"list", "board", "show", "embed"}:
            await self._send_board(ctx)
            return

        if head == "clear":
            if not self._is_patrol_dm(ctx.author):
                await ctx.send("❌ Only patrol DMs/mods can clear the patrol board.")
                return

            count = self._clear_entries(ctx.guild.id)
            await ctx.send(
                f"🧹 Cleared the patrol board. Removed **{count}** entr{'y' if count == 1 else 'ies'}."
            )
            return

        if head in {"remove", "unlist", "off", "leave"}:
            target = ctx.author
            if ctx.message.mentions:
                if not self._is_patrol_dm(ctx.author):
                    await ctx.send("❌ Only patrol DMs/mods can remove someone else from the patrol board.")
                    return
                target = ctx.message.mentions[0]

            removed = self._remove_entry(ctx.guild.id, target.id)
            if removed is None:
                if target.id == ctx.author.id:
                    await ctx.send("• You are not currently listed for patrol.")
                else:
                    await ctx.send(f"• {target.mention} is not currently listed for patrol.")
                return

            char_name = _display_name(str(removed.get("char_name") or "Unknown"))
            if target.id == ctx.author.id:
                await ctx.send(f"✅ Removed **{char_name}** from the patrol board.")
            else:
                await ctx.send(f"✅ Removed {target.mention} (**{char_name}**) from the patrol board.")
            return



        try:
            level = int(head)
        except ValueError:
            await ctx.send(
                "❌ Usage:\n"
                "• `!patrol` → show board\n"
                "• `!patrol <level>` → join using your display name\n"
                "• `!patrol <level> <character>` → join with a specific character\n"
                "• `!patrol remove` → remove yourself"
            )
            return

        if not (1 <= level <= 9):
            await ctx.send("❌ Patrol level must be between 1 and 9.")
            return
            
        explicit_char = " ".join(tokens[1:]).strip()
        char_name = self._resolve_char_name(ctx, explicit_char)
        if not char_name:
            await ctx.send("❌ I couldn't determine your character. Use `!patrol <level> <character>`.")
            return

        entry = self._upsert_entry(
            ctx.guild.id,
            ctx.author.id,
            char_name=char_name,
            level=level,
        )
        await ctx.send(
            f"✅ Added {ctx.author.mention} to patrol as **{_display_name(char_name)}** — "
            f"Level **{level}** ({entry['rank']})."
        )


def setup(bot: commands.Bot):
    bot.add_cog(PatrolCog(bot))
