import asyncio
import json
import os
import re
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

import nextcord
from nextcord.ext import commands


def _db_path() -> Path:
    return Path(os.getenv("LORE_DB_PATH", "data/lore.sqlite3"))


def _now_ts() -> int:
    return int(time.time())


def _clean_text(s: str) -> str:
    # Normalize common Discord typography (curly quotes, zero-width spaces) and whitespace.
    s = (s or "").replace("\u200b", "")
    s = s.replace("’", "'").replace("‘", "'").replace("‛", "'").replace("´", "'").replace("`", "'")
    s = s.replace("“", "\"").replace("”", "\"").replace("„", "\"")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _emoji_safe(s: str) -> str:
    # Keep SQLite happy if someone posts weird nulls.
    return (s or "").replace("\x00", "")

STOPWORDS = {
    "a","an","and","are","as","at","be","but","by","for","from","how","i","in","is","it",
    "of","on","or","that","the","this","to","was","what","when","where","who","why","with"
}
def _fts_query_from_question(q: str) -> str:
    raw = _clean_text(q)

    # If user supplied a quoted phrase, treat it as an FTS phrase query.
    m = re.search(r'"([^"]{2,200})"', raw)
    if m:
        phrase = _clean_text(m.group(1))
        return f'"{phrase}"'

    # Also support exact:foo bar (no quotes needed)
    low = raw.lower()
    if low.startswith("exact:") or low.startswith("phrase:"):
        phrase = _clean_text(raw.split(":", 1)[1])
        if phrase:
            return f'"{phrase}"'

    ql = raw.lower()
    terms = re.findall(r"[a-z0-9]+", ql)
    terms = [t for t in terms if len(t) >= 3 and t not in STOPWORDS]
    if not terms:
        return ql  # fallback
    if len(terms) >= 2:
        return " AND ".join(terms)
    return terms[0]
    
@dataclass
class BackfillProgress:
    running: bool = False
    started_ts: int = 0
    channels_total: int = 0
    channels_done: int = 0
    msgs_seen: int = 0          # NEW: scanned/visited
    msgs_saved: int = 0
    last_where: str = ""
    last_msg_ts: int = 0        # NEW: timestamp of last scanned msg
    last_update_ts: int = 0
    error: str = ""
    skips: Dict[str, int] = field(default_factory=dict)

class LoreCog(commands.Cog):
    """Lore archiver + searchable index.

    Designed to "piggyback" on RPXP channel flagging: by default, it archives
    whatever channels/threads you already flagged for RPXP.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

        self._conn: Optional[sqlite3.Connection] = None
        self._db_lock = asyncio.Lock()

        # One backfill per guild at a time.
        self._backfill_tasks: Dict[int, asyncio.Task] = {}
        self._progress: Dict[int, BackfillProgress] = {}

    # ------------------------- DB -------------------------
    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn

        path = _db_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(path), check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_db(conn)
        self._conn = conn
        return conn

    def _init_db(self, conn: sqlite3.Connection) -> None:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_messages (
                guild_id       INTEGER NOT NULL,
                channel_id     INTEGER NOT NULL,
                thread_id      INTEGER NOT NULL DEFAULT 0,
                message_id     INTEGER NOT NULL,
                created_ts     INTEGER NOT NULL,
                edited_ts      INTEGER NOT NULL DEFAULT 0,
                author_user_id INTEGER NOT NULL DEFAULT 0,
                webhook_id     INTEGER NOT NULL DEFAULT 0,
                speaker_name   TEXT NOT NULL DEFAULT '',
                speaker_type   TEXT NOT NULL DEFAULT '',
                content        TEXT NOT NULL DEFAULT '',
                attachments_json TEXT NOT NULL DEFAULT '[]',
                jump_url       TEXT NOT NULL DEFAULT '',
                PRIMARY KEY (guild_id, message_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_settings (
                guild_id INTEGER PRIMARY KEY,
                use_rpxp_channels INTEGER NOT NULL DEFAULT 1
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_backfill_state (
                guild_id      INTEGER NOT NULL,
                target_type   TEXT NOT NULL,           -- 'channel' or 'thread'
                target_id     INTEGER NOT NULL,         -- channel.id or thread.id
                fully_scanned INTEGER NOT NULL DEFAULT 0,
                latest_ts     INTEGER NOT NULL DEFAULT 0,
                updated_ts    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, target_type, target_id)
            );
            """
        )        
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_channels (
                guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL,
                include_threads INTEGER NOT NULL DEFAULT 1,
                PRIMARY KEY (guild_id, channel_id)
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lore_chan_ts ON lore_messages(guild_id, channel_id, created_ts);"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lore_speaker_ts ON lore_messages(guild_id, speaker_name, created_ts);"
        )

        # Best-effort full-text search (works on most Python builds).
        try:
            conn.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS lore_fts USING fts5(
                    content,
                    speaker_name,
                    channel_id UNINDEXED,
                    created_ts UNINDEXED,
                    message_id UNINDEXED
                );
                """
            )
        except Exception:
            # If FTS5 isn't available, !ask will still work in "excerpts only" mode.
            pass

        conn.commit()

    def _extract_embed_text(self, message: nextcord.Message) -> str:
        parts: List[str] = []
        for e in (getattr(message, "embeds", None) or []):
            try:
                if getattr(e, "title", None):
                    parts.append(str(e.title))
                if getattr(e, "description", None):
                    parts.append(str(e.description))

                for f in (getattr(e, "fields", None) or []):
                    # fields have .name and .value
                    if getattr(f, "name", None):
                        parts.append(str(f.name))
                    if getattr(f, "value", None):
                        parts.append(str(f.value))

                footer = getattr(e, "footer", None)
                if footer and getattr(footer, "text", None):
                    parts.append(str(footer.text))

                author = getattr(e, "author", None)
                if author and getattr(author, "name", None):
                    parts.append(str(author.name))
            except Exception:
                continue

        return _clean_text("\n".join(parts))
        
    # ------------------------- scope / settings -------------------------
    async def _use_rpxp_channels(self, guild_id: int) -> bool:
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT use_rpxp_channels FROM lore_settings WHERE guild_id=?",
                (int(guild_id),),
            ).fetchone()
            if row is None:
                conn.execute(
                    "INSERT OR IGNORE INTO lore_settings(guild_id, use_rpxp_channels) VALUES (?, 1)",
                    (int(guild_id),),
                )
                conn.commit()
                return True
            return bool(int(row["use_rpxp_channels"]) or 0)

    async def _set_use_rpxp_channels(self, guild_id: int, value: bool) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO lore_settings(guild_id, use_rpxp_channels) VALUES (?, ?) "
                "ON CONFLICT(guild_id) DO UPDATE SET use_rpxp_channels=excluded.use_rpxp_channels",
                (int(guild_id), 1 if value else 0),
            )
            conn.commit()



    async def _bf_get_state(self, guild_id: int, target_type: str, target_id: int) -> Tuple[int, int]:
        """Return (fully_scanned, latest_ts) from lore_backfill_state."""
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT fully_scanned, latest_ts FROM lore_backfill_state WHERE guild_id=? AND target_type=? AND target_id=?",
                (int(guild_id), str(target_type), int(target_id)),
            ).fetchone()
            if not row:
                return 0, 0
            return int(row["fully_scanned"] or 0), int(row["latest_ts"] or 0)


    async def _bf_set_state(self, guild_id: int, target_type: str, target_id: int, *, fully_scanned: int, latest_ts: int) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_backfill_state(guild_id, target_type, target_id, fully_scanned, latest_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, target_type, target_id) DO UPDATE SET
                    fully_scanned=excluded.fully_scanned,
                    latest_ts=excluded.latest_ts,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), str(target_type), int(target_id), int(fully_scanned), int(latest_ts), _now_ts()),
            )
            conn.commit()


    async def _bf_latest_ts_in_messages(self, guild_id: int, target_type: str, target_id: int) -> int:
        """Compute max(created_ts) currently stored for this channel/thread."""
        async with self._db_lock:
            conn = self._get_conn()
            if target_type == "thread":
                row = conn.execute(
                    "SELECT MAX(created_ts) AS m FROM lore_messages WHERE guild_id=? AND thread_id=?",
                    (int(guild_id), int(target_id)),
                ).fetchone()
            else:
                row = conn.execute(
                    "SELECT MAX(created_ts) AS m FROM lore_messages WHERE guild_id=? AND channel_id=? AND thread_id=0",
                    (int(guild_id), int(target_id)),
                ).fetchone()
            return int(row["m"] or 0) if row else 0

    async def _explicit_channels(self, guild_id: int) -> List[int]:
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                "SELECT channel_id FROM lore_channels WHERE guild_id=?",
                (int(guild_id),),
            ).fetchall()
        return [int(r["channel_id"]) for r in rows]

    async def _add_explicit_channel(self, guild_id: int, channel_id: int, include_threads: bool = True) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT OR REPLACE INTO lore_channels(guild_id, channel_id, include_threads) VALUES (?, ?, ?)",
                (int(guild_id), int(channel_id), 1 if include_threads else 0),
            )
            conn.commit()

    async def _remove_explicit_channel(self, guild_id: int, channel_id: int) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                "DELETE FROM lore_channels WHERE guild_id=? AND channel_id=?",
                (int(guild_id), int(channel_id)),
            )
            conn.commit()

    async def _rpxp_flagged_channel_ids(self, guild_id: int) -> List[int]:
        # Lore can run standalone, but if RPXPCog is loaded, we piggyback.
        rpxp = self.bot.get_cog("RPXPCog")
        if rpxp is None:
            return []
        try:
            g = rpxp._g(int(guild_id))  # uses RPXP's stored channel list
            return [int(x) for x in (g.get("channels") or [])]
        except Exception:
            return []

    async def _scope_channel_ids(self, guild_id: int) -> List[int]:
        ids = set(await self._explicit_channels(guild_id))
        if await self._use_rpxp_channels(guild_id):
            ids.update(await self._rpxp_flagged_channel_ids(guild_id))
        return sorted(ids)

    # ------------------------- message capture -------------------------
    def _resolve_speaker(self, message: nextcord.Message) -> Tuple[int, int, str, str]:
        """Returns (author_user_id, webhook_id, speaker_name, speaker_type)."""
        if getattr(message, "webhook_id", None):
            webhook_id = int(message.webhook_id or 0)
            name = getattr(message.author, "name", "") or getattr(message.author, "display_name", "") or ""
            return (0, webhook_id, _emoji_safe(name), "webhook")

        # Normal user message.
        uid = int(getattr(message.author, "id", 0) or 0)
        name = getattr(message.author, "display_name", "") or getattr(message.author, "name", "") or ""
        return (uid, 0, _emoji_safe(name), "user")

    def _skip(self, progress: Optional[BackfillProgress], key: str, n: int = 1) -> None:
        if not progress:
            return
        progress.skips[key] = int(progress.skips.get(key, 0)) + int(n)
            
    async def _should_archive_with_reason(self, message: nextcord.Message) -> Tuple[bool, str]:
        if not message.guild:
            return False, "no_guild"

        # Keep Tupperbox/webhook proxies (webhook_id). Ignore other bots unless allowlisted.
        if not getattr(message, "webhook_id", None) and getattr(message.author, "bot", False):
            if int(getattr(message.author, "id", 0) or 0) not in self._allowed_bot_ids():
                return False, "bot"

        # Ignore command-y noise by default.
        s = (message.content or "").lstrip()
        if s.startswith("!") or s.startswith("/"):
            return False, "command"

        scope = set(await self._scope_channel_ids(message.guild.id))

        # Threads: accept if the thread itself is in scope OR its parent is.
        if isinstance(message.channel, nextcord.Thread):
            if int(message.channel.id) in scope:
                return True, "ok"
            if getattr(message.channel, "parent_id", None) and int(message.channel.parent_id) in scope:
                return True, "ok"
            return False, "out_of_scope"

        return (int(message.channel.id) in scope), ("ok" if int(message.channel.id) in scope else "out_of_scope")


    async def _should_archive(self, message: nextcord.Message) -> bool:
        ok, _ = await self._should_archive_with_reason(message)
        return ok

    def _allowed_bot_ids(self) -> set[int]:
        raw = os.getenv("LORE_ALLOW_BOT_IDS", "")
        return {int(x) for x in re.findall(r"\d+", raw)}

    async def _insert_message_row(
        self, *, guild_id: int, channel_id: int, thread_id: int, message_id: int,
        created_ts: int, edited_ts: int, author_user_id: int, webhook_id: int,
        speaker_name: str, speaker_type: str, content: str, attachments_json: str,
        jump_url: str
    ) -> bool:
        """Insert message; returns True if new. Otherwise updates and returns False."""
        async with self._db_lock:
            conn = self._get_conn()

            exists = conn.execute(
                "SELECT 1 FROM lore_messages WHERE guild_id=? AND message_id=? LIMIT 1",
                (int(guild_id), int(message_id)),
            ).fetchone() is not None

            if not exists:
                conn.execute(
                    """
                    INSERT INTO lore_messages (
                        guild_id, channel_id, thread_id, message_id, created_ts, edited_ts,
                        author_user_id, webhook_id, speaker_name, speaker_type,
                        content, attachments_json, jump_url
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        int(guild_id), int(channel_id), int(thread_id), int(message_id),
                        int(created_ts), int(edited_ts), int(author_user_id), int(webhook_id),
                        speaker_name, speaker_type, content, attachments_json, jump_url,
                    ),
                )
                inserted = True
            else:
                conn.execute(
                    """
                    UPDATE lore_messages
                    SET channel_id=?, thread_id=?, edited_ts=?,
                        author_user_id=?, webhook_id=?, speaker_name=?, speaker_type=?,
                        content=?, attachments_json=?, jump_url=?
                    WHERE guild_id=? AND message_id=?
                    """,
                    (
                        int(channel_id), int(thread_id), int(edited_ts),
                        int(author_user_id), int(webhook_id),
                        speaker_name, speaker_type, content, attachments_json, jump_url,
                        int(guild_id), int(message_id),
                    ),
                )
                inserted = False

            # Keep FTS in sync (best-effort)
            try:
                conn.execute("DELETE FROM lore_fts WHERE rowid=?", (int(message_id),))
                conn.execute(
                    "INSERT OR IGNORE INTO lore_fts(rowid, content, speaker_name, channel_id, created_ts, message_id) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        int(message_id),
                        content,
                        speaker_name,
                        int(channel_id),
                        int(created_ts),
                        int(message_id),
                    ),
                )
            except Exception:
                pass

            conn.commit()
            return inserted

    async def _archive_message(self, message: nextcord.Message, progress: Optional[BackfillProgress] = None) -> bool:
        ok, reason = await self._should_archive_with_reason(message)
        if not ok:
            if reason == "bot":
                bid = int(getattr(message.author, "id", 0) or 0)
                # keep the aggregate counter too
                self._skip(progress, "bot")
                # and add a per-bot-id counter so !lore status can reveal the culprit(s)
                self._skip(progress, f"bot:{bid}")
            else:
                self._skip(progress, reason)
            return False

        raw = _clean_text(message.content or "")
        emb = self._extract_embed_text(message)
        content = _clean_text((raw + "\n" + emb).strip())

        if not content and not getattr(message, "attachments", None):
            self._skip(progress, "empty")
            return False

        author_user_id, webhook_id, speaker_name, speaker_type = self._resolve_speaker(message)
        created_ts = int(message.created_at.timestamp())
        edited_ts = int(message.edited_at.timestamp()) if getattr(message, "edited_at", None) else 0

        if isinstance(message.channel, nextcord.Thread):
            channel_id = int(getattr(message.channel, "parent_id", 0) or message.channel.id)
            thread_id = int(message.channel.id)
        else:
            channel_id = int(message.channel.id)
            thread_id = 0

        jump_url = getattr(message, "jump_url", "") or ""

        atts = []
        for a in (getattr(message, "attachments", None) or []):
            try:
                atts.append({"url": a.url, "filename": a.filename, "size": a.size})
            except Exception:
                continue
        attachments_json = json.dumps(atts, ensure_ascii=False)

        inserted = await self._insert_message_row(
            guild_id=message.guild.id,
            channel_id=channel_id,
            thread_id=thread_id,
            message_id=message.id,
            created_ts=created_ts,
            edited_ts=edited_ts,
            author_user_id=author_user_id,
            webhook_id=webhook_id,
            speaker_name=speaker_name,
            speaker_type=speaker_type,
            content=_emoji_safe(content),
            attachments_json=_emoji_safe(attachments_json),
            jump_url=_emoji_safe(jump_url),
        )

        if inserted:
            self._skip(progress, "inserted")
        else:
            self._skip(progress, "exists_or_updated")

        return inserted

    @commands.Cog.listener()
    async def on_message(self, message: nextcord.Message):
        # Archive is intentionally "quiet"; no user-visible output.
        try:
            await self._archive_message(message)
        except Exception:
            # Never break the bot if the archiver hits an edge-case.
            return

    @commands.Cog.listener()
    async def on_message_edit(self, before: nextcord.Message, after: nextcord.Message):
        # Update stored content on edit (best-effort).
        try:
            await self._archive_message(after)
        except Exception:
            return

    # ------------------------- backfill -------------------------
    async def _iter_threads_best_effort(self, parent: nextcord.abc.GuildChannel) -> List[nextcord.Thread]:
        threads: Dict[int, nextcord.Thread] = {}

        # Active threads already cached.
        for t in (getattr(parent, "threads", None) or []):
            try:
                threads[int(t.id)] = t
            except Exception:
                continue

        # Archived threads: Nextcord has had a couple method names over time.
        for meth in ("archived_threads", "public_archived_threads", "private_archived_threads"):
            fn = getattr(parent, meth, None)
            if fn is None:
                continue
            try:
                res = fn(limit=None)
                # Some versions return an async iterator.
                if hasattr(res, "__aiter__"):
                    async for t in res:
                        try:
                            threads[int(t.id)] = t
                        except Exception:
                            continue
                # Some versions return (threads, has_more).
                elif isinstance(res, tuple) and res and isinstance(res[0], list):
                    for t in res[0]:
                        try:
                            threads[int(t.id)] = t
                        except Exception:
                            continue
            except TypeError:
                # Some variants require a before= parameter.
                try:
                    res = fn(before=None, limit=None)
                    if hasattr(res, "__aiter__"):
                        async for t in res:
                            try:
                                threads[int(t.id)] = t
                            except Exception:
                                continue
                except Exception:
                    pass
            except Exception:
                continue

        return list(threads.values())

    async def _backfill_channel(self, guild: nextcord.Guild, chan: nextcord.abc.GuildChannel, progress: BackfillProgress, *, force_full: bool = False):
        # Backfill parent channel messages.
        gid = int(guild.id)
        cid = int(chan.id)

        fully_scanned, _state_latest = await self._bf_get_state(gid, "channel", cid)

        after_dt = None

        # did_full=True means: "this run should be sufficient to mark the channel fully scanned at the end"
        # (either because it's a full scan, or because we're resuming from a saved cursor and then finishing).
        did_full = True
        used_resume_cursor = False

        if not force_full:
            if fully_scanned:
                # Already completed in the past: do incremental fetch only.
                latest_ts = await self._bf_latest_ts_in_messages(gid, "channel", cid)
                if latest_ts > 0:
                    after_dt = datetime.fromtimestamp(max(0, latest_ts - 3), tz=timezone.utc)
                    did_full = False
            elif _state_latest > 0:
                # Mid-channel resume (from periodic checkpoint)
                after_dt = datetime.fromtimestamp(max(0, _state_latest - 3), tz=timezone.utc)
                used_resume_cursor = True
                did_full = True  # finishing from cursor completes the full scan overall

        checkpoint_every = int(os.getenv("LORE_BF_CHECKPOINT_EVERY_SEEN", "5000"))
        seen_local = 0
        try:
            cursor = after_dt          # datetime | message | None
            last_cursor_id = 0
            channel_errored = False

            while True:
                batch = []
                async for msg in chan.history(limit=200, oldest_first=True, after=cursor):
                    batch.append(msg)

                if not batch:
                    break

                # Guard against pagination loops (same last message repeating)
                if int(batch[-1].id) == int(last_cursor_id):
                    progress.error = f"History pagination stuck in channel {chan.id} at msg {last_cursor_id}"
                    channel_errored = True
                    break

                last_cursor_id = int(batch[-1].id)
                cursor = batch[-1]  # advance cursor forward

                for msg in batch:
                    if not msg.guild:
                        continue

                    progress.msgs_seen += 1
                    try:
                        progress.last_msg_ts = int(msg.created_at.timestamp())
                    except Exception:
                        pass

                    try:
                        saved = await self._archive_message(msg, progress=progress)
                        if saved:
                            progress.msgs_saved += 1
                    except Exception:
                        pass

                    # per-channel checkpointing (your existing seen_local logic)
                    seen_local += 1
                    if (not force_full) and (not fully_scanned) and checkpoint_every > 0:
                        if progress.last_msg_ts and (seen_local % checkpoint_every) == 0:
                            await self._bf_set_state(
                                gid, "channel", cid,
                                fully_scanned=0,
                                latest_ts=int(progress.last_msg_ts),
                            )

                    if (progress.msgs_seen % 500) == 0:
                        progress.last_update_ts = _now_ts()
                        await asyncio.sleep(0)
                   
        except Exception:
            # Missing perms or unsupported channel type.
            return

        # Mark/refresh channel state
        latest_ts_now = await self._bf_latest_ts_in_messages(gid, "channel", cid)
        await self._bf_set_state(
            gid, "channel", cid,
            fully_scanned=(1 if ((not channel_errored) and (fully_scanned or did_full or used_resume_cursor)) else 0),
            latest_ts=latest_ts_now,
        )


        # Backfill threads under this channel, best-effort.
        try:
            threads = await self._iter_threads_best_effort(chan)
        except Exception:
            threads = []

        for t in threads:
            tid = int(t.id)
            progress.last_where = f"thread:{tid}"

            t_fully, t_state_latest = await self._bf_get_state(gid, "thread", tid)

            t_after = None
            t_did_full = True
            t_used_resume_cursor = False

            if not force_full:
                if t_fully:
                    t_latest = await self._bf_latest_ts_in_messages(gid, "thread", tid)
                    if t_latest > 0:
                        t_after = datetime.fromtimestamp(max(0, t_latest - 3), tz=timezone.utc)
                        t_did_full = False
                elif t_state_latest > 0:
                    t_after = datetime.fromtimestamp(max(0, t_state_latest - 3), tz=timezone.utc)
                    t_used_resume_cursor = True
                    t_did_full = True

            t_seen_local = 0
            errored = False
            try:
                cursor = t_after  # datetime | message | None
                last_cursor_id = 0

                while True:
                    batch = []
                    async for msg in t.history(limit=200, oldest_first=True, after=cursor):
                        batch.append(msg)

                    if not batch:
                        break

                    # Guard against pagination loops (same last message repeating)
                    if int(batch[-1].id) == int(last_cursor_id):
                        progress.error = f"History pagination stuck in thread {tid} at msg {last_cursor_id}"
                        errored = True
                        break

                    last_cursor_id = int(batch[-1].id)
                    cursor = batch[-1]  # advance cursor forward

                    for msg in batch:
                        if not msg.guild:
                            continue

                        progress.msgs_seen += 1
                        t_seen_local += 1
                        try:
                            progress.last_msg_ts = int(msg.created_at.timestamp())
                        except Exception:
                            pass

                        try:
                            saved = await self._archive_message(msg, progress=progress)
                            if saved:
                                progress.msgs_saved += 1
                        except Exception:
                            pass

                        if (not force_full) and (not t_fully) and checkpoint_every > 0:
                            if progress.last_msg_ts and (t_seen_local % checkpoint_every) == 0:
                                await self._bf_set_state(
                                    gid, "thread", tid,
                                    fully_scanned=0,
                                    latest_ts=int(progress.last_msg_ts),
                                )

                        if (progress.msgs_seen % 500) == 0:
                            progress.last_update_ts = _now_ts()
                            await asyncio.sleep(0)
            except Exception:
                errored = True
            finally:
                t_latest_now = await self._bf_latest_ts_in_messages(gid, "thread", tid)
                await self._bf_set_state(
                    gid, "thread", tid,
                    fully_scanned=(1 if (t_fully or ((not errored) and (t_did_full or t_used_resume_cursor))) else 0),
                    latest_ts=t_latest_now,
                )

    async def _run_backfill(self, guild: nextcord.Guild, force_full: bool = False):
        gid = int(guild.id)
        p = self._progress.setdefault(gid, BackfillProgress())
        p.running = True
        p.started_ts = _now_ts()
        p.channels_done = 0
        p.msgs_seen = 0
        p.msgs_saved = 0
        p.skips = {}
        p.error = ""
        p.last_where = ""
        p.last_update_ts = _now_ts()

        try:
            ids = await self._scope_channel_ids(gid)
            p.channels_total = len(ids)

            for cid in ids:
                p.last_where = f"channel:{cid}"
                ch = guild.get_channel(cid)
                if ch is None:
                    try:
                        ch = await guild.fetch_channel(cid)
                    except Exception:
                        ch = None
                if ch is None:
                    p.channels_done += 1
                    continue

                # If the scope item is already a Thread, backfill it directly.
                if isinstance(ch, nextcord.Thread):
                    tid = int(ch.id)
                    t_fully, t_state_latest = await self._bf_get_state(gid, "thread", tid)

                    t_after = None
                    t_did_full = True
                    if not force_full:
                        if t_fully:
                            t_latest = await self._bf_latest_ts_in_messages(gid, "thread", tid)
                            if t_latest > 0:
                                t_after = datetime.fromtimestamp(max(0, t_latest - 3), tz=timezone.utc)
                                t_did_full = False
                        elif t_state_latest > 0:
                            t_after = datetime.fromtimestamp(max(0, t_state_latest - 3), tz=timezone.utc)
                            t_did_full = True  # finish-from-cursor completes the scan

                    checkpoint_every = int(os.getenv("LORE_BF_CHECKPOINT_EVERY_SEEN", "5000"))
                    t_seen_local = 0

                    t_errored = False
                    try:
                        cursor = t_after  # datetime | message | None
                        last_cursor_id = 0

                        while True:
                            batch = []
                            async for msg in ch.history(limit=200, oldest_first=True, after=cursor):
                                batch.append(msg)

                            if not batch:
                                break

                            if int(batch[-1].id) == int(last_cursor_id):
                                p.error = f"History pagination stuck in thread {tid} at msg {last_cursor_id}"
                                t_errored = True
                                break

                            last_cursor_id = int(batch[-1].id)
                            cursor = batch[-1]

                            for msg in batch:
                                if not msg.guild:
                                    continue

                                p.msgs_seen += 1
                                try:
                                    p.last_msg_ts = int(msg.created_at.timestamp())
                                except Exception:
                                    pass

                                try:
                                    saved = await self._archive_message(msg, progress=p)
                                    if saved:
                                        p.msgs_saved += 1
                                except Exception:
                                    pass

                                t_seen_local += 1
                                if (not force_full) and (not t_fully) and checkpoint_every > 0:
                                    if p.last_msg_ts and (t_seen_local % checkpoint_every) == 0:
                                        await self._bf_set_state(
                                            gid, "thread", tid,
                                            fully_scanned=0,
                                            latest_ts=int(p.last_msg_ts),
                                        )

                                if (p.msgs_seen % 500) == 0:
                                    p.last_update_ts = _now_ts()
                                    await asyncio.sleep(0)
                    except Exception:
                        t_errored = True
                    finally:
                        t_latest_now = await self._bf_latest_ts_in_messages(gid, "thread", tid)
                        await self._bf_set_state(
                            gid, "thread", tid,
                            fully_scanned=(1 if (t_fully or ((not t_errored) and t_did_full)) else 0),
                            latest_ts=t_latest_now,
                        )

                else:
                    await self._backfill_channel(guild, ch, p, force_full=force_full)

                p.channels_done += 1
                p.last_update_ts = _now_ts()

        except Exception as e:
            p.error = str(e)
        finally:
            p.running = False
            p.last_update_ts = _now_ts()

    # ------------------------- querying -------------------------
    async def _fts_available(self) -> bool:
        async with self._db_lock:
            conn = self._get_conn()
            try:
                conn.execute("SELECT 1 FROM lore_fts LIMIT 1").fetchone()
                return True
            except Exception:
                return False

    async def _fts_search(self, guild_id: int, query: str, limit: int = 8) -> List[int]:
        """FTS search that behaves more like a human expects.

        - If user quotes a phrase, try the exact phrase first.
        - If that yields few hits, broaden with NEAR(...) and prefix tokens (shadow*).
        - Avoid OR-broadening quoted phrases (prevents 'thread soup').
        """
        q_primary = _fts_query_from_question(query)
        q_primary = re.sub(r"[`]+", " ", (q_primary or "")).strip()
        if not q_primary:
            return []

        cands: List[str] = [q_primary]

        def _tokenize(s: str) -> List[str]:
            toks = re.findall(r"[a-z0-9]+", (s or "").lower())
            # cheap plural → singular heuristic (shadows -> shadow) to improve recall without real stemming
            toks2 = []
            for t in toks:
                if t.endswith("s") and (not t.endswith("ss")) and len(t) >= 5:
                    t = t[:-1]
                toks2.append(t)
            toks = [t for t in toks2 if len(t) >= 3 and t not in STOPWORDS]
            return toks

        def _prefixify(t: str) -> str:
            if not t or t.endswith("*"):
                return t
            if re.fullmatch(r"[a-z0-9]+", t) and len(t) >= 4:
                return t + "*"
            return t

        if q_primary.startswith('"') and q_primary.endswith('"') and len(q_primary) > 2:
            phrase = q_primary.strip('"')
            toks = [_prefixify(t) for t in _tokenize(phrase)]
            if toks:
                if len(toks) >= 2:
                    near_dist = int(os.getenv("LORE_FTS_NEAR_DIST", str(max(6, 2 * len(toks) + 4))))
                    cands.append(f"NEAR({ ' '.join(toks) }, {near_dist})")
                    cands.append(" AND ".join(toks))
                else:
                    cands.append(toks[0])

        if (" AND " in q_primary) and (not q_primary.startswith('"')):
            parts = [p.strip() for p in q_primary.split(" AND ") if p.strip()]
            parts2 = [_prefixify(p.lower()) for p in parts]
            if parts2 and parts2 != parts:
                cands.append(" AND ".join(parts2))

        min_hits = int(os.getenv("LORE_FTS_FALLBACK_MIN_HITS", "6"))
        min_hits = max(1, min(min_hits, limit))

        async with self._db_lock:
            conn = self._get_conn()

            def run(q: str) -> List[int]:
                rows = conn.execute(
                    "SELECT message_id FROM lore_fts WHERE lore_fts MATCH ? ORDER BY bm25(lore_fts) LIMIT ?",
                    (q, int(limit)),
                ).fetchall()
                return [int(r[0]) for r in rows]

            out: List[int] = []
            seen = set()

            for i, cand in enumerate(cands):
                try:
                    ids = run(cand)
                except Exception:
                    continue

                for mid in ids:
                    if mid in seen:
                        continue
                    seen.add(mid)
                    out.append(mid)
                    if len(out) >= limit:
                        return out

                if i == 0 and len(out) >= min_hits:
                    return out

            if not out and " AND " in q_primary and not q_primary.startswith('"'):
                try:
                    ids = run(q_primary.replace(" AND ", " OR "))
                    for mid in ids:
                        if mid in seen:
                            continue
                        seen.add(mid)
                        out.append(mid)
                        if len(out) >= limit:
                            break
                except Exception:
                    pass

            return out

    async def _fetch_messages(self, guild_id: int, message_ids: List[int]) -> List[sqlite3.Row]:
        if not message_ids:
            return []
        ph = ",".join(["?"] * len(message_ids))
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                f"SELECT * FROM lore_messages WHERE guild_id=? AND message_id IN ({ph}) ORDER BY created_ts ASC",
                (int(guild_id), *[int(x) for x in message_ids]),
            ).fetchall()
        return rows


    def _pick_seed_ids(self, ids: List[int], k: int) -> List[int]:
        if not ids:
            return []
        if len(ids) <= k:
            return ids
        # evenly spaced across the list so we seed both early + late evidence
        idxs = [round(i * (len(ids) - 1) / (k - 1)) for i in range(k)]
        seen = set()
        out = []
        for ix in idxs:
            mid = int(ids[ix])
            if mid in seen:
                continue
            seen.add(mid)
            out.append(mid)
        return out

    def _guess_names_from_question(self, q: str) -> List[str]:
        q = _clean_text(q)
        if not q:
            return []

        out: List[str] = []

        # who is X
        if q.lower().startswith("who is "):
            out.append(q[7:].strip())

        # X's relationship to Y
        m = re.search(r"(.+?)'s relationship to (.+)", q, flags=re.I)
        if m:
            out.append(m.group(1).strip())
            out.append(m.group(2).strip())

        # X and Y (simple co-query)
        if re.search(r"\band\b", q, flags=re.I):
            parts = re.split(r"\band\b", q, maxsplit=1, flags=re.I)
            if len(parts) == 2:
                a, b = parts[0].strip(), parts[1].strip()
                if 0 < len(a) <= 60 and 0 < len(b) <= 60:
                    out.append(a)
                    out.append(b)

        # cleanup + dedupe (case-insensitive)
        cleaned: List[str] = []
        seen = set()
        for n in out:
            n = _clean_text(re.sub(r"[?!.:,;]+$", "", n))
            if not n:
                continue
            k = n.lower()
            if k in seen:
                continue
            seen.add(k)
            cleaned.append(n)

        return cleaned


    def _is_bio_question(self, q: str) -> bool:
        """Heuristic: bio-like queries are short character-name queries (e.g., 'Voza', 'Reynard Blackstrand', 'who is X')."""
        ql = _clean_text(q or "").lower().strip()
        if not ql:
            return False
        if ql.startswith("who is "):
            return True
        toks = re.findall(r"[a-z0-9']+", ql)
        if not toks:
            return False
        if len(toks) <= 2:
            return True
        if len(toks) == 3 and ("of" not in toks) and ("the" not in toks) and ("in" not in toks) and ("at" not in toks):
            return True
        return False

    async def _window_message_ids(
        self,
        guild_id: int,
        channel_id: int,
        thread_id: int,
        center_ts: int,
        *,
        before_n: int = 20,
        after_n: int = 20,
    ) -> List[int]:
        """Return message_ids in the same channel/thread around a center timestamp."""
        async with self._db_lock:
            conn = self._get_conn()

            before = conn.execute(
                """
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND created_ts<=?
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (int(guild_id), int(channel_id), int(thread_id), int(center_ts), int(before_n)),
            ).fetchall()

            after = conn.execute(
                """
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND created_ts>?
                ORDER BY created_ts ASC
                LIMIT ?
                """,
                (int(guild_id), int(channel_id), int(thread_id), int(center_ts), int(after_n)),
            ).fetchall()

        # before is DESC, flip to ASC; then append after
        ids = [int(r[0]) for r in reversed(before)] + [int(r[0]) for r in after]

        # dedupe in-order
        seen = set()
        out = []
        for mid in ids:
            if mid in seen:
                continue
            seen.add(mid)
            out.append(mid)
        return out
    async def _count_messages(self, guild_id: int) -> int:
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM lore_messages WHERE guild_id=?",
                (int(guild_id),),
            ).fetchone()
            return int(row["c"]) if row else 0

    async def _openai_answer(self, *, question: str, excerpts: List[sqlite3.Row]) -> Optional[str]:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None

        try:
            from openai import OpenAI  # type: ignore
        except Exception:
            return None

        model = os.getenv("LORE_MODEL", "gpt-4o-mini")
        client = OpenAI(api_key=api_key)

        # Keep prompt grounded.
        context_lines = []
        for i, r in enumerate(excerpts, start=1):
            sid = f"S{i}"
            who = (r["speaker_name"] or "?")
            url = (r["jump_url"] or "")
            txt = _clean_text(r["content"] or "")
            if len(txt) > 700:
                txt = txt[:700] + "…"
            context_lines.append(f"[{sid}] {who}\n{txt}\nURL: {url}")
        is_bio = self._is_bio_question(question)

        if is_bio:
            fmt = (
                "One-sentence identity / headline\n"
                "Public reputation / quirks (4–8 bullets)\n"
                "Roles & affiliations (4–8 bullets)\n"
                "Major events / quests (6–12 bullets)\n"
                "Key relationships (4–10 bullets)\n"
                "Arc evolution over time (5–10 bullets, earliest → latest)\n"
                "Every bullet must include at least one [S#] citation.\n"
            )
        else:
            fmt = (
                "One-sentence headline (what is it?)\n"
                "What it is (3–6 bullets)\n"
                "Origin / first appearance (3–8 bullets)\n"
                "Key scenes / threads (4–10 bullets)\n"
                "Notable figures & factions (3–10 bullets)\n"
                "Timeline / developments (5–12 bullets, earliest → latest)\n"
                "Open questions / uncertainties (0–6 bullets)\n"
                "Every bullet must include at least one [S#] citation.\n"
            )

        prompt = (
            "You are the lore historian for a roleplay Discord server.\n"
            "Use ONLY the sources below. If something isn't supported, say you can't confirm it.\n"
            "Include notable quirks, self-promotion, rumors, and reputation — but only when supported by sources.\n"
            "For topics/places/events: prioritize concrete details (who/what/where/when) and cite them.\n"
            "For people: cover their arc across time (early → recent).\n"
            "Cite sources using [S#] markers (example: [S12]). Do NOT write the word 'source'.\n\n"
            "Write the final answer using this FORMAT exactly:\n"
            + fmt +
            "\nQUESTION: " + question + "\n\n"
            "SOURCES:\n" + "\n\n".join(context_lines)
        )


        try:
            max_out = int(os.getenv("LORE_ASK_MAX_OUTPUT_TOKENS", "2400"))
            resp = client.responses.create(model=model, input=prompt, max_output_tokens=max_out)
            return getattr(resp, "output_text", None) or None
        except Exception:
            return None

    async def _send_chunks(self, ctx: commands.Context, text: str, limit: int = 1900):
        text = text or ""
        while text:
            chunk = text[:limit]
            # try not to cut mid-line
            cut = chunk.rfind("\n")
            if cut > 400:
                chunk = chunk[:cut]
            await ctx.send(chunk)
            text = text[len(chunk):].lstrip("\n")
                    
    def _inline_source_links(self, text: str, rows: List[sqlite3.Row]) -> str:
        if not text:
            return text

        # Map S# -> jump_url for ALL provided excerpts
        ref = {}
        for i, r in enumerate(rows, start=1):
            url = (r["jump_url"] or "").strip()
            if url:
                ref[f"S{i}"] = url

        # 1) Expand bracket lists: [S86, S100] -> [S86] [S100]
        def expand_list(m: re.Match) -> str:
            inside = m.group(1)
            toks = re.findall(r"S\d+", inside)
            return " ".join(f"[{t}]" for t in toks)

        out = re.sub(r"\[((?:\s*S\d+\s*,)+\s*S\d+\s*)\]", expand_list, text)

        # 2) Wrap bare citations: "... Corps S18." -> "... Corps [S18]."
        # Avoid touching ones already bracketed.
        out = re.sub(r"(?<!\[)\bS(\d+)\b", r"[S\1]", out)

        # 3) Linkify [S#] -> [S#](jump_url)
        def repl(m: re.Match) -> str:
            sid = m.group(1)  # like "S12"
            url = ref.get(sid)
            return f"[{sid}]({url})" if url else f"[{sid}]"

        out = re.sub(r"\[(S\d+)\]", repl, out)

        # 4) spacing between adjacent citations: ...)(...) -> ...) (...)
        out = out.replace(")[", ") [")
        return out
            
    # ------------------------- commands -------------------------
    @commands.group(name="lore", invoke_without_command=True)
    async def lore(self, ctx: commands.Context):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        use_rpxp = await self._use_rpxp_channels(gid)
        explicit = await self._explicit_channels(gid)
        total = await self._count_messages(gid)

        p = self._progress.get(gid, BackfillProgress())
        status = "✅ idle"
        if p.running:
            status = f"🗄️ backfilling ({p.channels_done}/{max(1,p.channels_total)} channels, {p.msgs_saved} msgs saved)"
        if p.error:
            status += f"\n⚠️ last error: {p.error}"

        embed = nextcord.Embed(
            title="Lore Archive",
            description=(
                f"Status: {status}\n"
                f"Stored messages (this server): **{total}**\n\n"
                f"Scope: {'RPXP-flagged channels' if use_rpxp else 'explicit lore channels only'}\n"
                f"Explicit lore channels: {len(explicit)}\n\n"
                "Commands:\n"
                "• `!lore use_rpxp on|off`\n"
                "• `!lore add` / `!lore remove` (current channel)\n"
                "• `!lore channels`\n"
                "• `!lore backfill` (admin)\n"
                "• `!lore ask <question>`\n"
            ),
            color=nextcord.Color.dark_teal(),
        )
        await ctx.send(embed=embed)

    @lore.command(name="use_rpxp")
    @commands.has_permissions(manage_guild=True)
    async def lore_use_rpxp(self, ctx: commands.Context, value: str):
        if not ctx.guild:
            return
        v = str(value or "").strip().lower()
        on = v in ("1", "true", "yes", "y", "on", "enable", "enabled")
        off = v in ("0", "false", "no", "n", "off", "disable", "disabled")
        if not (on or off):
            await ctx.send("❌ Use `!lore use_rpxp on` or `!lore use_rpxp off`.")
            return
        await self._set_use_rpxp_channels(ctx.guild.id, on)
        await ctx.send(f"✅ Lore scope now uses RPXP-flagged channels: **{on}**")

    @lore.command(name="add")
    @commands.has_permissions(manage_guild=True)
    async def lore_add(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await self._add_explicit_channel(ctx.guild.id, ctx.channel.id, include_threads=True)
        await ctx.send(f"✅ Added to lore scope: <#{ctx.channel.id}>")

    @lore.command(name="remove")
    @commands.has_permissions(manage_guild=True)
    async def lore_remove(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await self._remove_explicit_channel(ctx.guild.id, ctx.channel.id)
        await ctx.send(f"🧹 Removed from lore scope: <#{ctx.channel.id}>")

    @lore.command(name="channels")
    async def lore_channels(self, ctx: commands.Context):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        use_rpxp = await self._use_rpxp_channels(gid)
        explicit = await self._explicit_channels(gid)
        rpxp = await self._rpxp_flagged_channel_ids(gid) if use_rpxp else []

        lines = []
        if use_rpxp:
            lines.append("**RPXP-flagged (auto):**")
            lines.extend([f"• <#{cid}>" for cid in rpxp] if rpxp else ["• (none)"])
        lines.append("\n**Explicit lore channels:**")
        lines.extend([f"• <#{cid}>" for cid in explicit] if explicit else ["• (none)"])

        # Discord hard limit: 2000 chars per message.
        text = "\n".join(lines)
        chunk_max = 1900  # leave headroom
        buf = ""
        for ln in text.splitlines():
            if len(buf) + len(ln) + 1 > chunk_max:
                await ctx.send(buf)
                buf = ""
            buf += ln + "\n"
        if buf.strip():
            await ctx.send(buf)
                
    
    @lore.command(name="debugmsg")
    @commands.has_permissions(manage_guild=True)
    async def lore_debugmsg(self, ctx: commands.Context, *, ref: str):
        """Debug: check whether a specific message is stored, and if not, why it was skipped."""
        if not ctx.guild:
            return
        raw = _clean_text(ref)
        if not raw:
            await ctx.send("❌ Usage: `!lore debugmsg <message link | message id>`")
            return

        gid = int(ctx.guild.id)

        # Parse Discord message link: /channels/<guild>/<channel>/<message>
        ch_id = None
        msg_id = None

        m = re.search(r"/channels/\d+/(\d+)/(\d+)", raw)
        if m:
            ch_id = int(m.group(1))
            msg_id = int(m.group(2))
        else:
            nums = [int(x) for x in re.findall(r"\d{16,22}", raw)]
            if nums:
                msg_id = int(nums[-1])
            if len(nums) >= 2:
                ch_id = int(nums[-2])

        if not msg_id:
            await ctx.send("❌ Couldn't parse a message id. Paste a Message Link (recommended).")
            return

        # 1) Check DB
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT speaker_name, channel_id, thread_id, created_ts, jump_url, substr(content,1,240) AS snippet "
                "FROM lore_messages WHERE guild_id=? AND message_id=?",
                (gid, int(msg_id)),
            ).fetchone()

        if row:
            ch_id2 = int(row["channel_id"])
            th_id2 = int(row["thread_id"] or 0)
            where = f"<#{ch_id2}>" + (f" (thread {th_id2})" if th_id2 else "")
            url = (row["jump_url"] or "").strip()
            who = (row["speaker_name"] or "?").strip() or "?"
            snip = (row["snippet"] or "").replace("\n", " ").strip()
            await ctx.send(
                f"✅ **Stored** {where} <t:{int(row['created_ts'])}:R> **{who}**\n"
                f"{snip}\n"
                + (f"<{url}>" if url else "")
            )
            return

        # 2) Not stored: try to fetch from Discord and explain skip reason.
        if not ch_id:
            await ctx.send("❌ Not in DB, and I couldn't infer the channel id. Paste a Message Link.")
            return

        ch = ctx.guild.get_channel(int(ch_id))
        if ch is None:
            try:
                ch = await ctx.guild.fetch_channel(int(ch_id))
            except Exception:
                ch = None

        if ch is None:
            await ctx.send(f"❌ Not in DB, and I can't access channel `{ch_id}` (missing perms or not found).")
            return

        try:
            msg = await ch.fetch_message(int(msg_id))
        except Exception:
            await ctx.send("❌ Not in DB, and I couldn't fetch that message (missing perms or it was deleted).")
            return

        ok, reason = await self._should_archive_with_reason(msg)

        scope = set(await self._scope_channel_ids(gid))
        in_scope = False
        scope_note = ""
        try:
            if isinstance(msg.channel, nextcord.Thread):
                tid = int(msg.channel.id)
                pid = int(getattr(msg.channel, "parent_id", 0) or 0)
                in_scope = (tid in scope) or (pid in scope)
                scope_note = f"thread={tid} parent={pid}"
            else:
                cid = int(getattr(msg.channel, "id", 0) or 0)
                in_scope = (cid in scope)
                scope_note = f"channel={cid}"
        except Exception:
            pass

        author_id = int(getattr(msg.author, "id", 0) or 0)
        is_bot = bool(getattr(msg.author, "bot", False))
        wh_id = int(getattr(msg, "webhook_id", 0) or 0)

        extra = ""
        if (not ok) and reason == "bot":
            extra = f"\n➡️ This was posted by a bot account (**{author_id}**). Add it to `LORE_ALLOW_BOT_IDS` and rerun `!lore backfill full`."
        if (not ok) and reason == "out_of_scope":
            extra = f"\n➡️ This channel isn't in lore scope. Use `!lore add` in that channel (or enable RPXP scope), then rerun `!lore backfill full`."

        await ctx.send(
            f"❌ **Not stored**. Fetch OK.\n"
            f"Would archive? **{ok}** (reason: `{reason}`)\n"
            f"Scope: **{in_scope}** ({scope_note})\n"
            f"Author: {author_id} (bot={is_bot}) | webhook_id={wh_id}\n"
            + extra
        )

    @lore.command(name="status")
    async def lore_status(self, ctx: commands.Context):
        if not ctx.guild:
            return

        p = self._progress.get(int(ctx.guild.id), BackfillProgress())
        if not p.running and not p.error:
            await ctx.send("✅ Lore backfill is idle.")
            return

        now = _now_ts()
        elapsed = max(1, now - int(p.started_ts or now))
        seen_rate = (p.msgs_seen / elapsed) if p.msgs_seen else 0.0
        saved_rate = (p.msgs_saved / elapsed) if p.msgs_saved else 0.0

        where = p.last_where or "(n/a)"
        m = re.match(r"^(channel|thread):(\d+)$", where)
        where_pretty = where
        if m and ctx.guild:
            cid = int(m.group(2))
            ch = ctx.guild.get_channel(cid)
            if ch is not None:
                where_pretty = f"{m.group(1)}: <#{cid}>"

        msg = (
            f"🗄️ Backfill status: {'running' if p.running else 'idle'}\n"
            f"Channels: {p.channels_done}/{max(1, p.channels_total)}\n"
            f"Seen: {p.msgs_seen:,}  |  Saved: {p.msgs_saved:,}\n"
            f"Rate: {seen_rate:.1f}/s seen  |  {saved_rate:.2f}/s saved\n"
            f"Last: {where_pretty}"
        )
        if p.last_msg_ts:
            msg += f"\nLast msg time: <t:{int(p.last_msg_ts)}:R>"
        if p.error:
            msg += f"\n⚠️ Last error: {p.error}"
        top = sorted(p.skips.items(), key=lambda kv: kv[1], reverse=True)[:5]
        if top:
            msg += "\nSkips: " + ", ".join(f"{k}={v:,}" for k, v in top)

        await ctx.send(msg)

    @lore.command(name="backfill")
    @commands.has_permissions(manage_guild=True)
    async def lore_backfill(self, ctx: commands.Context, mode: str = ""):
        if not ctx.guild:
            return
        force_full = (mode or "").strip().lower() in ("full", "all", "rescan")    
        gid = int(ctx.guild.id)
        t = self._backfill_tasks.get(gid)
        if t and not t.done():
            await ctx.send("🗄️ Backfill is already running. Use `!lore status`.")
            return

        self._progress[gid] = BackfillProgress(running=True, started_ts=_now_ts())
        task = asyncio.create_task(self._run_backfill(ctx.guild, force_full=force_full))
        self._backfill_tasks[gid] = task
        await ctx.send(
            "🗄️ Started lore backfill for the current scope (RPXP-flagged channels + explicit lore channels). "
            "Use `!lore status` any time."
        )

    async def _speaker_search_ids(
        self,
        guild_id: int,
        name: str,
        limit: int = 50,
        *,
        order: str = "DESC",
    ) -> List[int]:
        name = _clean_text(name)
        # Normalize apostrophes so K'lliara == K’lliara == Klliara
        name_norm = re.sub(r"[’‘']+", "", name).lower()
        if not name_norm:
            return []
        order_sql = "ASC" if str(order).upper().startswith("A") else "DESC"
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                f"""
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=?
                  AND LOWER(REPLACE(REPLACE(REPLACE(speaker_name, '’', ''), '‘', ''), '''', '')) = ?
                ORDER BY created_ts {order_sql}
                LIMIT ?
                """,
                (int(guild_id), name_norm, int(limit)),
            ).fetchall()
        return [int(r[0]) for r in rows]


    async def _speaker_search_ids_like(
        self,
        guild_id: int,
        name: str,
        limit: int = 80,
        *,
        order: str = "DESC",
    ) -> List[int]:
        name = _clean_text(name)
        name_norm = re.sub(r"[’‘']+", "", name).lower()
        if not name_norm:
            return []
        pat = f"%{name_norm}%"
        order_sql = "ASC" if str(order).upper().startswith("A") else "DESC"

        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                f"""
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=?
                  AND LOWER(REPLACE(REPLACE(REPLACE(speaker_name, '’', ''), '‘', ''), '''', '')) LIKE ?
                ORDER BY created_ts {order_sql}
                LIMIT ?
                """,
                (int(guild_id), pat, int(limit)),
            ).fetchall()
        return [int(r[0]) for r in rows]


    @lore.command(name="peek")
    @commands.has_permissions(manage_guild=True)
    async def lore_peek(self, ctx: commands.Context, *, query: str):
        """Debug: show what evidence the lore retriever is using (with jump links)."""
        if not ctx.guild:
            return
        query = _clean_text(query)
        if not query:
            await ctx.send("❌ Usage: `!lore peek <query>`")
            return

        if not await self._fts_available():
            await ctx.send("❌ Lore search isn't available (SQLite FTS5 missing).")
            return

        fts_limit = int(os.getenv("LORE_ASK_FTS_LIMIT", "80"))
        speaker_limit = int(os.getenv("LORE_ASK_SPEAKER_LIMIT", "60"))
        seed_count = int(os.getenv("LORE_ASK_WINDOW_SEEDS", "8"))
        win_before = int(os.getenv("LORE_ASK_WIN_BEFORE", "20"))
        win_after = int(os.getenv("LORE_ASK_WIN_AFTER", "20"))
        max_excerpts = int(os.getenv("LORE_ASK_MAX_EXCERPTS", "180"))

        peek_n = int(os.getenv("LORE_PEEK_N", "12"))
        anchor_n = int(os.getenv("LORE_PEEK_ANCHORS_N", "8"))

        question = query.strip()
        fts_q = _fts_query_from_question(question)

        rows, stats, anchor_ids = await self._retrieve_rows_for_question(
            ctx.guild.id,
            question,
            fts_limit=fts_limit,
            speaker_limit=speaker_limit,
            seed_count=seed_count,
            win_before=win_before,
            win_after=win_after,
            max_excerpts=max_excerpts,
        )

        if not rows:
            await ctx.send("I couldn't find anything relevant in the archive yet.")
            return

        base_rows_raw = await self._fetch_messages(ctx.guild.id, anchor_ids)
        base_map = {int(r["message_id"]): r for r in base_rows_raw}
        base_rows = [base_map[i] for i in anchor_ids if i in base_map]

        is_bio = self._is_bio_question(question)

        mode = self._intent_mode(question)
        rows_for_model = rows

        if mode == "timeline":
            rows_for_model = sorted(rows_for_model, key=lambda r: int(r["created_ts"]))
        elif mode == "recent":
            rows_for_model = sorted(rows_for_model, key=lambda r: int(r["created_ts"]))[-max_excerpts:]
        else:
            if is_bio:
                bins = int(os.getenv("LORE_ASK_STRATA_BINS", "8"))
                per_bin = int(os.getenv("LORE_ASK_STRATA_PER_BIN", "16"))
                recent_bonus = int(os.getenv("LORE_ASK_STRATA_RECENT_BONUS", "40"))
                rows_for_model = self._time_stratified_rows(rows_for_model, bins=bins, per_bin=per_bin, recent_bonus=recent_bonus)
            else:
                bins = int(os.getenv("LORE_TOPIC_STRATA_BINS", "6"))
                per_bin = int(os.getenv("LORE_TOPIC_STRATA_PER_BIN", "10"))
                recent_bonus = int(os.getenv("LORE_TOPIC_STRATA_RECENT_BONUS", "18"))
                rows_for_model = self._time_stratified_rows(rows_for_model, bins=bins, per_bin=per_bin, recent_bonus=recent_bonus)

        thread_rows = [r for r in rows_for_model if int(r["thread_id"] or 0) != 0]
        distinct_threads = sorted({int(r["thread_id"] or 0) for r in thread_rows})
        distinct_channels = sorted({int(r["channel_id"]) for r in rows_for_model})
        ts_min = int(rows_for_model[0]["created_ts"])
        ts_max = int(rows_for_model[-1]["created_ts"])

        header = (
            f"🔎 **Lore Peek** — `{question}`\n"
            f"Mode: **{mode}**\n"
            f"FTS query: `{fts_q}`\n"
            f"FTS hits: **{stats.get('fts_hits', 0)}** | After speaker/names: **{stats.get('after_speaker_names', 0)}** "
            f"| Window add: **{stats.get('window_add', 0)}** | Edge kept: **{stats.get('edge_kept', 0)}** | Final rows: **{len(rows_for_model)}**\n"
            f"Channels in pool: **{len(distinct_channels)}** | Thread msgs: **{len(thread_rows)}** (threads: **{len(distinct_threads)}**)\n"
            f"Time span: <t:{ts_min}:D> → <t:{ts_max}:D>\n"
        )

        lines = [header]
        lines.append("**FTS anchor hits (actual matches):**")
        if not base_rows:
            lines.append("• (none)")
        else:
            for i, r in enumerate(base_rows[:anchor_n], start=1):
                who = (r["speaker_name"] or "?").strip() or "?"
                url = (r["jump_url"] or "").strip()
                txt = (r["content"] or "").strip().replace("\n", " ")
                if len(txt) > 240:
                    txt = txt[:240] + "…"
                ch_id = int(r["channel_id"])
                th_id = int(r["thread_id"] or 0)
                where = f"<#{ch_id}>" + (f" (thread {th_id})" if th_id else "")
                jump = f"A{i} <{url}>" if url else f"A{i}"
                lines.append(f"• {jump} <t:{int(r['created_ts'])}:R> **{who}** in {where}\n  {txt}")

        lines.append("\n**Top excerpts from FINAL pool (what ask is feeding):**")
        for i, r in enumerate(rows_for_model[:peek_n], start=1):
            who = (r["speaker_name"] or "?").strip() or "?"
            url = (r["jump_url"] or "").strip()
            txt = (r["content"] or "").strip().replace("\n", " ")
            if len(txt) > 240:
                txt = txt[:240] + "…"
            ch_id = int(r["channel_id"])
            th_id = int(r["thread_id"] or 0)
            where = f"<#{ch_id}>" + (f" (thread {th_id})" if th_id else "")
            jump = f"S{i} <{url}>" if url else f"S{i}"
            lines.append(f"• {jump} <t:{int(r['created_ts'])}:R> **{who}** in {where}\n  {txt}")

        if len(rows_for_model) > peek_n:
            lines.append("\n**Newest excerpts from FINAL pool (often shows thread starts/ends):**")
            for i, r in enumerate(rows_for_model[-peek_n:], start=1):
                who = (r["speaker_name"] or "?").strip() or "?"
                url = (r["jump_url"] or "").strip()
                txt = (r["content"] or "").strip().replace("\n", " ")
                if len(txt) > 240:
                    txt = txt[:240] + "…"
                ch_id = int(r["channel_id"])
                th_id = int(r["thread_id"] or 0)
                where = f"<#{ch_id}>" + (f" (thread {th_id})" if th_id else "")
                jump = f"N{i} <{url}>" if url else f"N{i}"
                lines.append(f"• {jump} <t:{int(r['created_ts'])}:R> **{who}** in {where}\n  {txt}")


        await self._send_chunks(ctx, "\n".join(lines))

    @lore.command(name="ask")
    async def lore_ask(self, ctx: commands.Context, *, question: str):
        if not ctx.guild:
            return
        question = _clean_text(question)
        if not question:
            await ctx.send("❌ Ask a real question.")
            return

        if not await self._fts_available():
            await ctx.send("❌ Lore search isn't available (SQLite FTS5 missing). The archive is still recording messages.")
            return

        fts_limit = int(os.getenv("LORE_ASK_FTS_LIMIT", "80"))
        speaker_limit = int(os.getenv("LORE_ASK_SPEAKER_LIMIT", "60"))
        seed_count = int(os.getenv("LORE_ASK_WINDOW_SEEDS", "8"))
        win_before = int(os.getenv("LORE_ASK_WIN_BEFORE", "20"))
        win_after = int(os.getenv("LORE_ASK_WIN_AFTER", "20"))
        max_excerpts = int(os.getenv("LORE_ASK_MAX_EXCERPTS", "180"))

        rows, _stats, _anchors = await self._retrieve_rows_for_question(
            ctx.guild.id,
            question,
            fts_limit=fts_limit,
            speaker_limit=speaker_limit,
            seed_count=seed_count,
            win_before=win_before,
            win_after=win_after,
            max_excerpts=max_excerpts,
        )

        if not rows:
            await ctx.send("I couldn't find anything relevant in the archive yet.")
            return

        is_bio = self._is_bio_question(question)

        mode = self._intent_mode(question)
        if mode == "timeline":
            rows = sorted(rows, key=lambda r: int(r["created_ts"]))
        elif mode == "recent":
            rows = sorted(rows, key=lambda r: int(r["created_ts"]))[-max_excerpts:]
        else:
            if is_bio:
                bins = int(os.getenv("LORE_ASK_STRATA_BINS", "8"))
                per_bin = int(os.getenv("LORE_ASK_STRATA_PER_BIN", "16"))
                recent_bonus = int(os.getenv("LORE_ASK_STRATA_RECENT_BONUS", "40"))
                rows = self._time_stratified_rows(rows, bins=bins, per_bin=per_bin, recent_bonus=recent_bonus)
            else:
                bins = int(os.getenv("LORE_TOPIC_STRATA_BINS", "6"))
                per_bin = int(os.getenv("LORE_TOPIC_STRATA_PER_BIN", "10"))
                recent_bonus = int(os.getenv("LORE_TOPIC_STRATA_RECENT_BONUS", "18"))
                rows = self._time_stratified_rows(rows, bins=bins, per_bin=per_bin, recent_bonus=recent_bonus)

        answer = await self._openai_answer(question=question, excerpts=rows)
        if answer:
            answer = self._inline_source_links(answer, rows)
            await self._send_chunks(ctx, answer)
            return

        lines = ["**Top lore excerpts (set `OPENAI_API_KEY` + install `openai` for narrated answers):**"]
        for r in rows[:8]:
            who = (r["speaker_name"] or "?")
            url = (r["jump_url"] or "")
            txt = (r["content"] or "")
            if len(txt) > 220:
                txt = txt[:220] + "…"
            lines.append(f"• **{who}:** {txt}\n  {url}")
        await ctx.send("\n".join(lines))

    def _intent_mode(self, q: str) -> str:
        ql = (q or "").lower()
        if any(w in ql for w in ["latest", "recent", "today", "currently", "now", "as of", "this week", "last week"]):
            return "recent"
        if any(w in ql for w in ["timeline", "chronology", "history of", "over time"]):
            return "timeline"
        return "balanced"

    def _time_stratified_rows(
        self,
        rows: List[sqlite3.Row],
        *,
        bins: int = 5,
        per_bin: int = 12,
        recent_bonus: int = 20,
    ) -> List[sqlite3.Row]:
        """Pick evidence across the full timeline: early/mid/recent, plus extra recent."""
        if not rows:
            return []

        rows_sorted = sorted(rows, key=lambda r: int(r["created_ts"]))
        if len(rows_sorted) <= bins * per_bin + recent_bonus:
            return rows_sorted

        ts0 = int(rows_sorted[0]["created_ts"])
        ts1 = int(rows_sorted[-1]["created_ts"])
        span = max(1, ts1 - ts0)
        width = max(1, span // bins)

        buckets: List[List[sqlite3.Row]] = [[] for _ in range(bins)]
        for r in rows_sorted:
            idx = min(bins - 1, (int(r["created_ts"]) - ts0) // width)
            buckets[idx].append(r)

        picked: List[sqlite3.Row] = []
        for b in buckets:
            if not b:
                continue
            if len(b) <= per_bin:
                picked.extend(b)
            else:
                # evenly spaced picks within the bucket
                step = max(1, len(b) // per_bin)
                picked.extend(b[::step][:per_bin])

        # extra “current arc” evidence
        picked.extend(rows_sorted[-recent_bonus:])

        # de-dupe by message_id, keep chronological order
        seen = set()
        out = []
        for r in sorted(picked, key=lambda r: int(r["created_ts"])):
            mid = int(r["message_id"])
            if mid in seen:
                continue
            seen.add(mid)
            out.append(r)
        return out

    @lore.command(name="reindex")
    @commands.has_permissions(manage_guild=True)
    async def lore_reindex(self, ctx: commands.Context):
        if not ctx.guild:
            return
        if not await self._fts_available():
            await ctx.send("❌ Lore search isn't available (SQLite FTS5 missing).")
            return

        async with self._db_lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM lore_fts;")
            conn.execute(
                "INSERT INTO lore_fts(rowid, content, speaker_name, channel_id, created_ts, message_id) "
                "SELECT message_id, content, speaker_name, channel_id, created_ts, message_id "
                "FROM lore_messages"
            )
            conn.commit()

        await ctx.send("✅ Rebuilt full-text index from stored lore messages.")

    def _rows_near_anchors(
        self,
        rows: List[sqlite3.Row],
        anchor_ids: set[int],
        *,
        radius: int = 3,
        max_rows: int = 180,
    ) -> List[sqlite3.Row]:
        """Keep rows near anchors within each (channel_id, thread_id).

        Prevents cross-thread bleed when rows from multiple threads are interleaved by time.
        """
        if not rows or not anchor_ids:
            return rows

        groups: Dict[Tuple[int, int], List[sqlite3.Row]] = {}
        for r in rows:
            key = (int(r["channel_id"]), int(r["thread_id"] or 0))
            groups.setdefault(key, []).append(r)

        kept: List[Tuple[int, int, int, sqlite3.Row]] = []  # (dist, created_ts, message_id, row)

        for _key, grp in groups.items():
            grp_sorted = sorted(grp, key=lambda r: int(r["created_ts"]))
            idxs = [i for i, r in enumerate(grp_sorted) if int(r["message_id"]) in anchor_ids]
            if not idxs:
                continue

            n = len(grp_sorted)
            keep_idx = set()
            for i in idxs:
                a = max(0, i - radius)
                b = min(n, i + radius + 1)
                keep_idx.update(range(a, b))

            for j in keep_idx:
                dist = min(abs(j - a) for a in idxs)
                r = grp_sorted[j]
                kept.append((dist, int(r["created_ts"]), int(r["message_id"]), r))

        if not kept:
            return rows

        best_by_mid: Dict[int, Tuple[int, int, int, sqlite3.Row]] = {}
        for dist, ts, mid, r in kept:
            prev = best_by_mid.get(mid)
            if prev is None or dist < prev[0]:
                best_by_mid[mid] = (dist, ts, mid, r)

        kept2 = list(best_by_mid.values())
        kept2.sort(key=lambda x: (x[0], x[1]))
        if max_rows and len(kept2) > max_rows:
            kept2 = kept2[:max_rows]

        kept2.sort(key=lambda x: x[1])
        return [x[3] for x in kept2]

    async def _thread_edge_message_ids(
        self,
        guild_id: int,
        thread_id: int,
        *,
        head_n: int = 12,
        tail_n: int = 8,
    ) -> List[int]:
        """Return message_ids from the start and end of a thread (helps 'origin' questions)."""
        async with self._db_lock:
            conn = self._get_conn()
            head = conn.execute(
                """
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=? AND thread_id=?
                ORDER BY created_ts ASC
                LIMIT ?
                """,
                (int(guild_id), int(thread_id), int(head_n)),
            ).fetchall()
            tail = conn.execute(
                """
                SELECT message_id
                FROM lore_messages
                WHERE guild_id=? AND thread_id=?
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (int(guild_id), int(thread_id), int(tail_n)),
            ).fetchall()

        ids = [int(r[0]) for r in head] + [int(r[0]) for r in reversed(tail)]
        seen = set()
        out: List[int] = []
        for mid in ids:
            if mid in seen:
                continue
            seen.add(mid)
            out.append(mid)
        return out

    async def _retrieve_rows_for_question(
        self,
        guild_id: int,
        question: str,
        *,
        fts_limit: int,
        speaker_limit: int,
        seed_count: int,
        win_before: int,
        win_after: int,
        max_excerpts: int,
    ) -> Tuple[List[sqlite3.Row], Dict[str, int], List[int]]:
        q = (question or "").strip()
        names = self._guess_names_from_question(q)

        # 1) FTS anchors (primary evidence)
        fts_anchor_ids = await self._fts_search(guild_id, q, limit=fts_limit)
        ids: List[int] = list(fts_anchor_ids)

        # Only do speaker lookup if it actually looks like a bio/name query.
        is_bio = self._is_bio_question(q)
        # Speaker search helps for character names; it doesn't hurt topics (usually returns 0).
        do_speaker = is_bio or (len(names) == 1)

        if do_speaker:
            name_hint = q[7:].strip() if q.lower().startswith("who is ") else (names[0] if names else q)
            ids += await self._speaker_search_ids_like(guild_id, name_hint, limit=speaker_limit, order="DESC")
            ids += await self._speaker_search_ids_like(guild_id, name_hint, limit=min(40, speaker_limit), order="ASC")

        for nm in names:
            ids += await self._fts_search(guild_id, nm, limit=25)
            ids += await self._speaker_search_ids(guild_id, nm, limit=speaker_limit, order="DESC")
            ids += await self._speaker_search_ids(guild_id, nm, limit=min(40, speaker_limit), order="ASC")

        # de-dupe ids while keeping order
        seen = set()
        ids = [x for x in ids if not (x in seen or seen.add(x))]
        ids_before_windows = len(ids)

        # 2) Window expansion — seed ONLY from real anchors if we have them
        seed_source = fts_anchor_ids or ids
        seed_ids = self._pick_seed_ids(seed_source, seed_count)
        seed_rows = await self._fetch_messages(guild_id, seed_ids)

        window_ids: List[int] = []

        # Thread edge sampling: include the start/end of anchor threads (helps "origin" questions)
        head_n = int(os.getenv("LORE_TOPIC_THREAD_HEAD_N", "12"))
        tail_n = int(os.getenv("LORE_TOPIC_THREAD_TAIL_N", "8"))

        edge_ids: List[int] = []
        try:
            anchor_rows = await self._fetch_messages(guild_id, list(fts_anchor_ids))
            anchor_thread_ids = sorted({int(r["thread_id"] or 0) for r in anchor_rows if int(r["thread_id"] or 0) != 0})
            for tid in anchor_thread_ids:
                edge_ids += await self._thread_edge_message_ids(guild_id, tid, head_n=head_n, tail_n=tail_n)
        except Exception:
            edge_ids = []

        # edge_ids are fetched separately to avoid polluting the main pool

        for r in seed_rows:
            try:
                window_ids += await self._window_message_ids(
                    guild_id,
                    int(r["channel_id"]),
                    int(r["thread_id"] or 0),
                    int(r["created_ts"]),
                    before_n=win_before,
                    after_n=win_after,
                )
            except Exception:
                continue

        ids += window_ids
        seen = set()
        ids = [x for x in ids if not (x in seen or seen.add(x))]
        ids = ids[:800]

        rows_all = await self._fetch_messages(guild_id, ids)
        rows_all = sorted(rows_all, key=lambda r: int(r["created_ts"]))

        # Fetch edge rows separately (thread start/end for anchor threads), then filter to only those
        # that mention the main topic terms (prevents unrelated thread-head chatter from entering the pool).
        focus_terms = []
        try:
            toks = re.findall(r"[a-z0-9']+", q.lower())
            toks2 = []
            for t in toks:
                if t.endswith("s") and (not t.endswith("ss")) and len(t) >= 5:
                    t = t[:-1]
                toks2.append(t)
            focus_terms = [t for t in toks2 if len(t) >= 4 and t not in STOPWORDS][:5]
        except Exception:
            focus_terms = []

        edge_rows_filtered: List[sqlite3.Row] = []
        if edge_ids and focus_terms:
            edge_rows = await self._fetch_messages(guild_id, edge_ids)
            for r in edge_rows:
                c = (r["content"] or "").lower()
                primary = focus_terms[0]
                if primary in c:
                    edge_rows_filtered.append(r)

        rows = rows_all

        # 3) Topic focus (thread-local), unless bio query
        if (not is_bio) and rows and fts_anchor_ids:
            radius = int(os.getenv("LORE_TOPIC_CONTEXT_RADIUS", "3"))
            rows = self._rows_near_anchors(
                rows,
                set(int(x) for x in fts_anchor_ids),  # focus on true anchors only
                radius=radius,
                max_rows=max_excerpts,
            )

        # Ensure topic-relevant thread-start/end messages are present (useful for "where did it come from?")
        if edge_rows_filtered and not is_bio:
            by_id = {int(r["message_id"]): r for r in rows}
            for r in edge_rows_filtered:
                by_id[int(r["message_id"])] = r
            rows = sorted(by_id.values(), key=lambda r: int(r["created_ts"]))

            # Enforce cap while preserving edge rows
            if max_excerpts and len(rows) > max_excerpts:
                edge_keep = {int(r["message_id"]) for r in edge_rows_filtered}
                kept_edges = [r for r in rows if int(r["message_id"]) in edge_keep]
                if len(kept_edges) >= max_excerpts:
                    rows = kept_edges[:max_excerpts]
                else:
                    non_edges = [r for r in rows if int(r["message_id"]) not in edge_keep]
                    need = max_excerpts - len(kept_edges)
                    non_edges = non_edges[-need:] if need > 0 else []
                    rows = sorted(non_edges + kept_edges, key=lambda r: int(r["created_ts"]))

        stats = {
            "fts_hits": len(fts_anchor_ids),
            "after_speaker_names": ids_before_windows,
            "window_add": len(window_ids),
            "edge_kept": len(edge_rows_filtered),
            "final_rows": len(rows),
        }
        return rows, stats, fts_anchor_ids




def setup(bot: commands.Bot):
    bot.add_cog(LoreCog(bot))
