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
import itertools
import inspect
import nextcord
from nextcord.ext import commands
from collections import Counter, defaultdict

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
    
# add near STOPWORDS / other module-level constants

SKILLS_5E = [
    "Athletics",
    "Acrobatics",
    "Sleight of Hand",
    "Stealth",
    "Arcana",
    "History",
    "Investigation",
    "Nature",
    "Religion",
    "Animal Handling",
    "Insight",
    "Medicine",
    "Perception",
    "Survival",
    "Deception",
    "Intimidation",
    "Performance",
    "Persuasion",
]

SKILLS_5E_MAP = {s.lower(): s for s in SKILLS_5E}

CHALLENGE_DC_BY_DIFFICULTY = {
    "very easy": 5,
    "easy": 10,
    "medium": 15,
    "hard": 20,
    "very hard": 25,
    "nearly impossible": 30,
}

CHALLENGE_DIFFICULTY_LABELS = {
    "very easy": "Very easy",
    "easy": "Easy",
    "medium": "Medium",
    "hard": "Hard",
    "very hard": "Very hard",
    "nearly impossible": "Nearly impossible",
}

STOPWORDS = {
    "a","an","and","are","as","at","be","but","by","for","from","how","i","in","is","it",
    "of","on","or","that","the","this","to","was","what","when","where","who","why","with"
}
def _fts_query_from_question(q: str) -> str:
    raw = _clean_text(q)

    # Allow "exact:" / "phrase:" prefixes but don't let them pollute token parsing.
    raw2 = raw
    m_prefix = re.match(r"(?i)^(exact|phrase)\s*:\s*(.+)$", raw2)
    prefix_mode = None
    if m_prefix:
        prefix_mode = m_prefix.group(1).lower()
        raw2 = _clean_text(m_prefix.group(2))

    # If user supplied a quoted phrase, treat it as an FTS phrase query,
    # BUT keep any remaining tokens outside the quotes as AND-filters.
    m = re.search(r'"([^"]{2,200})"', raw2)
    if m:
        phrase = _clean_text(m.group(1))
        rest = _clean_text((raw2[:m.start()] + " " + raw2[m.end():]).strip())
        rest_terms = re.findall(r"[a-z0-9]+", rest.lower())
        rest_terms = [t for t in rest_terms if len(t) >= 3 and t not in STOPWORDS]
        if rest_terms:
            return f"\"{phrase}\" AND " + " AND ".join(rest_terms)
        return f"\"{phrase}\""

    # Also support exact:foo bar (no quotes needed)
    low = raw.lower()
    if prefix_mode in ("exact", "phrase"):
        phrase = _clean_text(raw2)
        if phrase:
            return f'"{phrase}"'

    ql = raw2.lower()
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

        # prevents overlapping "sticky chat" replies per (guild, channel, thread, persona, user)
        self._scene_chat_locks: Dict[tuple, asyncio.Lock] = {}
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

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_scene_meta (
                guild_id      INTEGER NOT NULL,
                channel_id    INTEGER NOT NULL,
                thread_id     INTEGER NOT NULL DEFAULT 0,
                persona_canon TEXT NOT NULL DEFAULT '',
                k             TEXT NOT NULL DEFAULT '',
                v             TEXT NOT NULL DEFAULT '',
                updated_ts    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, channel_id, thread_id, persona_canon, k)
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lore_scene_meta_lookup "
            "ON lore_scene_meta(guild_id, channel_id, thread_id, persona_canon, updated_ts);"
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

        # --- lore map (entity graph) ---
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_entities (
                entity_id   INTEGER PRIMARY KEY,
                guild_id    INTEGER NOT NULL,
                canon       TEXT NOT NULL,      -- normalized key
                display     TEXT NOT NULL,      -- pretty name
                kind        TEXT NOT NULL,      -- 'person' | 'org' | 'event' | 'thing'
                score       REAL NOT NULL DEFAULT 0,
                mentions    INTEGER NOT NULL DEFAULT 0,
                first_ts    INTEGER NOT NULL DEFAULT 0,
                last_ts     INTEGER NOT NULL DEFAULT 0,
                UNIQUE (guild_id, canon, kind)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_entity_mentions (
                guild_id    INTEGER NOT NULL,
                entity_id   INTEGER NOT NULL,
                message_id  INTEGER NOT NULL,
                created_ts  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, entity_id, message_id)
            );
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_entity_edges (
                guild_id    INTEGER NOT NULL,
                a_entity_id INTEGER NOT NULL,
                b_entity_id INTEGER NOT NULL,
                weight      REAL NOT NULL DEFAULT 0,
                first_ts    INTEGER NOT NULL DEFAULT 0,
                last_ts     INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, a_entity_id, b_entity_id)
            );
            """
        )

        conn.execute("CREATE INDEX IF NOT EXISTS idx_lore_entities_kind_score ON lore_entities(guild_id, kind, score);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lore_mentions_entity_ts ON lore_entity_mentions(guild_id, entity_id, created_ts);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lore_edges_a ON lore_entity_edges(guild_id, a_entity_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lore_edges_b ON lore_entity_edges(guild_id, b_entity_id);")


        # --- personas (cached character sheets for RP) ---
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_personas (
                guild_id    INTEGER NOT NULL,
                canon       TEXT NOT NULL,     -- normalized key (title-stripped)
                display     TEXT NOT NULL,     -- pretty name
                profile     TEXT NOT NULL DEFAULT '',
                stats_json  TEXT NOT NULL DEFAULT '{}',
                built_ts    INTEGER NOT NULL DEFAULT 0,
                updated_ts  INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, canon)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_lore_personas_guild_ts ON lore_personas(guild_id, updated_ts);")

        # --- scene memory (rolling in-channel RP memory) ---
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_scene_state (
                guild_id      INTEGER NOT NULL,
                channel_id    INTEGER NOT NULL,
                thread_id     INTEGER NOT NULL DEFAULT 0,
                persona_canon TEXT NOT NULL DEFAULT '',
                enabled       INTEGER NOT NULL DEFAULT 0,
                summary       TEXT NOT NULL DEFAULT '',
                updated_ts    INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY (guild_id, channel_id, thread_id, persona_canon)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS lore_scene_turns (
                turn_id       INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id      INTEGER NOT NULL,
                channel_id    INTEGER NOT NULL,
                thread_id     INTEGER NOT NULL DEFAULT 0,
                persona_canon TEXT NOT NULL DEFAULT '',
                role          TEXT NOT NULL DEFAULT '',   -- 'user' | 'assistant'
                speaker       TEXT NOT NULL DEFAULT '',
                content       TEXT NOT NULL DEFAULT '',
                created_ts    INTEGER NOT NULL DEFAULT 0
            );
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_lore_scene_turns_lookup "
            "ON lore_scene_turns(guild_id, channel_id, thread_id, persona_canon, created_ts);"
        )
        
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
        
            

    def _time_stratified_rows(
        self,
        rows: List[sqlite3.Row],
        *,
        bins: int = 5,
        per_bin: int = 12,
        recent_bonus: int = 20,
    ) -> List[sqlite3.Row]:
        """Pick evidence across the full timeline: early/mid/recent, plus extra recent.

        Unlike a purely-even sampler, this also prefers "high-signal" rows in each bin
        (longer RP beats, named things, distinctive tokens) so rare-but-important facts
        like named abilities/events don't get missed.
        """
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

        def _signal_score(r: sqlite3.Row) -> float:
            txt = _clean_text(r["content"] or "")
            if not txt:
                return 0.0

            score = 0.0
            score += min(6.0, len(txt) / 220.0)

            # quoted speech / stronger declarative lines
            if '"' in txt or "“" in txt or "”" in txt:
                score += 1.2

            # proper-noun-ish / event-ish / org-ish phrasing
            low = txt.lower()
            if re.search(r"\b(battle|siege|ritual|trial|expedition|church|order|knights|house|corps)\b", low):
                score += 2.0

            # multiple capitalized words often signals named entities
            caps = re.findall(r"\b[A-Z][a-zA-Z'’\-]+\b", txt)
            if len(caps) >= 2:
                score += min(2.0, len(caps) * 0.25)

            return score

        picked: List[sqlite3.Row] = []

        for bucket in buckets:
            if not bucket:
                continue
            ranked = sorted(bucket, key=_signal_score, reverse=True)
            picked.extend(ranked[:per_bin])

        # extra recent rows
        recent_rows = rows_sorted[-recent_bonus:] if recent_bonus > 0 else []

        seen = set()
        out: List[sqlite3.Row] = []
        for r in sorted(picked + recent_rows, key=lambda r: int(r["created_ts"])):
            mid = int(r["message_id"])
            if mid in seen:
                continue
            seen.add(mid)
            out.append(r)

        return out
    
    def _bio_alias_map(self) -> Dict[str, List[str]]:
        raw = (os.getenv("LORE_BIO_ALIASES", "") or "").strip()
        out: Dict[str, List[str]] = {}
        if not raw:
            return out

        # format: key|a1|a2 ; key2|b1|b2
        for entry in raw.split(";"):
            entry = _clean_text(entry)
            if not entry:
                continue
            parts = [p.strip().lower() for p in entry.split("|") if p.strip()]
            if len(parts) < 2:
                continue
            key = parts[0]
            aliases = [a for a in parts[1:] if a and a != key]
            if aliases:
                out[key] = aliases
        return out

    def _bio_alias_terms(self, subj_l: str) -> List[str]:
        subj_l = (subj_l or "").strip().lower()
        if not subj_l:
            return []
        m = self._bio_alias_map()

        # best-match: exact key, else keys that contain the subject, else keys contained by subject
        if subj_l in m:
            return m[subj_l]

        best_key = None
        best_len = 0
        for k in m.keys():
            if subj_l in k or k in subj_l:
                if len(k) > best_len:
                    best_key = k
                    best_len = len(k)

        return m.get(best_key, []) if best_key else []        
        
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

        # Keep webhook proxies. For other bots: denylist wins; otherwise keep "lore-like" bot posts.
        if getattr(message.author, "bot", False) and not getattr(message, "webhook_id", None):
            bid = int(getattr(message.author, "id", 0) or 0)
            if bid in self._denied_bot_ids():
                return False, "bot"
            if bid not in self._allowed_bot_ids():
                # Heuristic: keep bot posts that look like RP/lore (embeds/long text/attachments)
                raw_txt = _clean_text(message.content or "")
                emb_txt = self._extract_embed_text(message)
                looks_lore = len(_clean_text((raw_txt + "\n" + emb_txt).strip())) >= 20
                if (not looks_lore) and (not getattr(message, "attachments", None)):
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

            pid = getattr(message.channel, "parent_id", None)
            if not pid:
                parent = getattr(message.channel, "parent", None)
                pid = getattr(parent, "id", None) if parent else None

            if pid and int(pid) in scope:
                return True, "ok"

            return False, "out_of_scope"

        return (int(message.channel.id) in scope), ("ok" if int(message.channel.id) in scope else "out_of_scope")


    async def _should_archive(self, message: nextcord.Message) -> bool:
        ok, _ = await self._should_archive_with_reason(message)
        return ok

    def _allowed_bot_ids(self) -> set[int]:
        raw = os.getenv("LORE_ALLOW_BOT_IDS", "")
        return {int(x) for x in re.findall(r"\d+", raw)}

    def _denied_bot_ids(self) -> set[int]:
        raw = os.getenv("LORE_DENY_BOT_IDS", "")
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

        # Thread starter messages live in the parent channel but still belong to a thread.
        # Nextcord exposes that via message.thread on the starter message.
        th = getattr(message, "thread", None)
        if th and isinstance(th, nextcord.Thread):
            channel_id = int(message.channel.id)
            thread_id = int(th.id)
        elif isinstance(message.channel, nextcord.Thread):
            pid = getattr(message.channel, "parent_id", None)
            if not pid:
                parent = getattr(message.channel, "parent", None)
                pid = getattr(parent, "id", None) if parent else None

            channel_id = int(pid or message.channel.id)
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
            pass

        # Sticky scene chat (optional)
        try:
            await self._scene_chat_maybe_reply(message)
        except Exception as e:
            if int(os.getenv("LORE_OPENAI_DEBUG", "0") or "0") != 0:
                import traceback
                traceback.print_exc()
            return

    @commands.Cog.listener()
    async def on_message_edit(self, before: nextcord.Message, after: nextcord.Message):
        # Update stored content on edit (best-effort).
        try:
            await self._archive_message(after)
        except Exception:
            return

    # ------------------------- backfill -------------------------

    def _is_transient_discord_error(self, e: Exception) -> bool:
        s = f"{type(e).__name__}: {e}".lower()

        if isinstance(e, nextcord.DiscordServerError):
            return True

        transient_bits = (
            "503",
            "502",
            "504",
            "service unavailable",
            "upstream connect error",
            "disconnect/reset before headers",
            "reset reason: overflow",
            "server error",
            "temporarily unavailable",
        )
        return any(bit in s for bit in transient_bits)


    async def _retry_discord_op(
        self,
        op,
        *,
        progress: Optional[BackfillProgress] = None,
        label: str = "discord_retry",
        tries: int = 5,
        base_delay: float = 1.0,
    ):
        last_exc = None

        for attempt in range(tries):
            try:
                result = op()
                if inspect.isawaitable(result):
                    result = await result

                # a later success should clear an old transient headline
                if progress and progress.error and "transient" in progress.error.lower():
                    progress.error = ""

                return result

            except Exception as e:
                last_exc = e

                if not self._is_transient_discord_error(e) or attempt >= (tries - 1):
                    raise

                if progress:
                    self._skip(progress, f"{label}_retry")
                    # only record as headline if nothing worse is already set
                    if not progress.error:
                        progress.error = f"transient {label}: {type(e).__name__}: {e}"

                await asyncio.sleep(base_delay * (2 ** attempt))

        raise last_exc
        
    def _note_progress_error(self, progress: Optional[BackfillProgress], label: str, e: Exception) -> None:
        """Record a single 'headline' error so !lore status surfaces *something* useful."""
        if not progress:
            return
        self._skip(progress, label)
        if not progress.error:
            progress.error = f"{label}: {type(e).__name__}: {e}"

    async def _archive_try(self, message: nextcord.Message, progress: Optional[BackfillProgress]) -> bool:
        """Archive one message with safe error accounting. Returns True only for NEW inserts."""
        try:
            saved = await self._archive_message(message, progress=progress)
            if saved and progress:
                progress.msgs_saved += 1
            return bool(saved)
        except Exception as e:
            self._note_progress_error(progress, "archive_error", e)
            return False

    async def _get_channel_or_thread(self, guild: nextcord.Guild, cid: int):
        """Best-effort: resolve either a channel or a thread id."""
        ch = None
        try:
            ch = guild.get_channel(int(cid))
        except Exception:
            ch = None

        if ch is None:
            try:
                # nextcord has get_thread on some versions
                get_th = getattr(guild, "get_thread", None)
                if callable(get_th):
                    ch = get_th(int(cid))
            except Exception:
                ch = None

        if ch is None:
            try:
                ch = await guild.fetch_channel(int(cid))
            except Exception:
                ch = None
        return ch

    async def _iter_threads_best_effort(self, parent: nextcord.abc.GuildChannel) -> List[nextcord.Thread]:
        """Return active + archived threads we can see for a parent channel (best-effort).

        Nextcord's archived thread APIs vary by version:
          - async iterator
          - list (single page)
          - tuple: (page, has_more)
        This routine tries to handle all of them and paginate when possible.
        """
        threads: Dict[int, nextcord.Thread] = {}

        # Active threads already cached.
        for t in (getattr(parent, "threads", None) or []):
            try:
                threads[int(t.id)] = t
            except Exception:
                continue

        cap = int(os.getenv("LORE_BF_THREAD_LIST_LIMIT", "0") or "0")  # 0 = unlimited best-effort
        per_page = int(os.getenv("LORE_BF_THREAD_LIST_PAGE", "100") or "100")

        DISCORD_EPOCH_MS = 1420070400000

        def _snowflake_to_dt(snowflake: int) -> Optional[datetime]:
            try:
                ms = (int(snowflake) >> 22) + DISCORD_EPOCH_MS
                return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
            except Exception:
                return None

        async def _maybe_await(x):
            return await x if inspect.isawaitable(x) else x

        def _add_many(page_threads):
            for t in (page_threads or []):
                try:
                    threads[int(t.id)] = t
                except Exception:
                    continue

        async def _call(fn, *, limit: int, before):
            """Try common call signatures across Nextcord variants."""
            # Prefer datetime-based before (Discord API expects datetime)
            try:
                return await _maybe_await(fn(limit=limit, before=before))
            except TypeError:
                pass
            except Exception:
                return None

            # Some variants accept the thread object or id
            try:
                return await _maybe_await(fn(limit=limit, before=getattr(before, "id", before)))
            except TypeError:
                pass
            except Exception:
                return None

            # Some variants accept a snowflake/Object
            try:
                obj = nextcord.Object(id=int(getattr(before, "id", before) or 0))
                return await _maybe_await(fn(limit=limit, before=obj))
            except Exception:
                return None

        async def _consume(fn):
            fetched = 0
            before = datetime.now(timezone.utc)

            # First page
            res = None
            try:
                res = await _maybe_await(fn(limit=per_page, before=before))
            except TypeError:
                try:
                    res = await _maybe_await(fn(limit=per_page))
                except Exception:
                    return
            except Exception:
                return

            # Async iterator case
            if hasattr(res, "__aiter__"):
                try:
                    async for t in res:
                        _add_many([t])
                        fetched += 1
                        if cap > 0 and fetched >= cap:
                            break
                except Exception:
                    pass
                return

            # List case (some Nextcord versions return one page as a list; paginate manually)
            if isinstance(res, list) and (not res or isinstance(res[0], nextcord.Thread)):
                page = list(res)
                while page and (cap <= 0 or fetched < cap):
                    _add_many(page)
                    fetched += len(page)
                    if cap > 0 and fetched >= cap:
                        break

                    # If fewer than requested came back, assume we're done.
                    if len(page) < per_page:
                        break

                    last = page[-1]
                    before_dt = getattr(last, "archive_timestamp", None) or getattr(last, "created_at", None)
                    if not isinstance(before_dt, datetime):
                        before_dt = _snowflake_to_dt(int(getattr(last, "id", 0) or 0))

                    next_limit = per_page if cap <= 0 else min(per_page, cap - fetched)
                    res2 = await _call(fn, limit=next_limit, before=before_dt or last)

                    if isinstance(res2, list) and (not res2 or isinstance(res2[0], nextcord.Thread)):
                        page = list(res2)
                        continue

                    # If it suddenly returns tuple pagination, let the tuple handler below take over.
                    if isinstance(res2, tuple) and res2 and isinstance(res2[0], (list, tuple)):
                        res = res2
                        break

                    break

                # If we ended due to a tuple handoff, fall through; otherwise we're done.
                if not (isinstance(res, tuple) and res and isinstance(res[0], (list, tuple))):
                    return

            # Tuple pagination case: (page, has_more)
            try:
                page = list(res[0])
            except Exception:
                return
            has_more = bool(res[1]) if (isinstance(res, tuple) and len(res) > 1) else False

            _add_many(page)
            fetched += len(page)

            while has_more and page and (cap <= 0 or fetched < cap):
                last = page[-1]

                before_dt = getattr(last, "archive_timestamp", None) or getattr(last, "created_at", None)
                if not isinstance(before_dt, datetime):
                    before_dt = _snowflake_to_dt(int(getattr(last, "id", 0) or 0))

                next_limit = per_page if cap <= 0 else min(per_page, cap - fetched)

                res2 = await _call(fn, limit=next_limit, before=before_dt or last)
                if not (isinstance(res2, tuple) and res2 and isinstance(res2[0], (list, tuple))):
                    break

                page = list(res2[0])
                has_more = bool(res2[1]) if len(res2) > 1 else False
                _add_many(page)
                fetched += len(page)

        for meth in ("public_archived_threads", "private_archived_threads", "archived_threads"):
            fn = getattr(parent, meth, None)
            if fn is None:
                continue
            await _consume(fn)

        return list(threads.values())

    async def _backfill_thread(
        self,
        guild: nextcord.Guild,
        thread: nextcord.Thread,
        progress: BackfillProgress,
        *,
        force_full: bool = False,
        parent_chan: Optional[nextcord.abc.GuildChannel] = None,
        checkpoint_every: int = 5000,
    ) -> None:
        gid = int(guild.id)
        tid = int(thread.id)

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

        # When force_full, ensure we start from the beginning of history.
        # Some Nextcord variants interpret after=None as “start from most recent”.
        if force_full and t_after is None:
            t_after = datetime.fromtimestamp(1420070401, tz=timezone.utc)  # 2015-01-01 + 1s


        errored = False
        seen_local = 0

        try:
            # Best-effort: archive thread starter message (often in parent channel, not in Thread.history)
            try:
                parent = parent_chan or getattr(thread, "parent", None)
                if parent is None:
                    pid = getattr(thread, "parent_id", None)
                    if pid:
                        parent = guild.get_channel(int(pid))
                starter_ids: List[int] = []
                for attr in ("message_id", "starter_message_id"):
                    mid = getattr(thread, attr, None)
                    if mid:
                        try:
                            starter_ids.append(int(mid))
                        except Exception:
                            pass
                starter_ids.append(int(thread.id))  # fallback

                starter = None
                if parent is not None and hasattr(parent, "fetch_message"):
                    for sid in starter_ids:
                        try:
                            starter = await self._retry_discord_op(
                                lambda sid=sid: parent.fetch_message(int(sid)),
                                progress=progress,
                                label="starter_fetch",
                                tries=4,
                                base_delay=0.75,
                            )
                            break
                        except Exception:
                            continue

                if starter is None and hasattr(thread, "fetch_message"):
                    for sid in starter_ids:
                        try:
                            starter = await self._retry_discord_op(
                                lambda sid=sid: thread.fetch_message(int(sid)),
                                progress=progress,
                                label="starter_fetch",
                                tries=4,
                                base_delay=0.75,
                            )
                            break
                        except Exception:
                            continue

                if starter is not None:
                    await self._archive_try(starter, progress)
            except Exception as e:
                self._note_progress_error(progress, "starter_error", e)

            # Always sweep the first few messages of the thread (captures intros that incremental scans skip)
            head_n = int(os.getenv("LORE_BF_THREAD_HEAD_N", "5"))
            if head_n > 0:
                try:
                    async for m in thread.history(limit=head_n, oldest_first=True, after=t_after):
                        if not m.guild:
                            continue
                        progress.msgs_seen += 1
                        try:
                            progress.last_msg_ts = int(m.created_at.timestamp())
                        except Exception:
                            pass
                        await self._archive_try(m, progress)
                except Exception as e:
                    self._note_progress_error(progress, "thread_head_error", e)

            cursor = t_after  # datetime | message | None
            last_cursor_id = 0

            while True:
                batch: List[nextcord.Message] = []

                async def _grab_batch():
                    out: List[nextcord.Message] = []
                    async for msg in thread.history(limit=200, oldest_first=True, after=cursor):
                        out.append(msg)
                    return out

                batch = await self._retry_discord_op(
                    _grab_batch,
                    progress=progress,
                    label="thread_history",
                    tries=5,
                    base_delay=1.0,
                )

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
                    seen_local += 1
                    try:
                        progress.last_msg_ts = int(msg.created_at.timestamp())
                    except Exception:
                        pass

                    await self._archive_try(msg, progress)

                    if (not force_full) and (not t_fully) and checkpoint_every > 0:
                        if progress.last_msg_ts and (seen_local % checkpoint_every) == 0:
                            await self._bf_set_state(
                                gid, "thread", tid,
                                fully_scanned=0,
                                latest_ts=int(progress.last_msg_ts),
                            )

                    if (progress.msgs_seen % 500) == 0:
                        progress.last_update_ts = _now_ts()
                        await asyncio.sleep(0)

        except Exception as e:
            errored = True
            self._note_progress_error(progress, "thread_error", e)
        finally:
            t_latest_now = await self._bf_latest_ts_in_messages(gid, "thread", tid)
            await self._bf_set_state(
                gid, "thread", tid,
                fully_scanned=(1 if (t_fully or ((not errored) and (t_did_full or t_used_resume_cursor))) else 0),
                latest_ts=t_latest_now,
            )

    async def _backfill_channel(
        self,
        guild: nextcord.Guild,
        chan: nextcord.abc.GuildChannel,
        progress: BackfillProgress,
        *,
        force_full: bool = False,
    ):
        # Backfill parent channel messages.
        gid = int(guild.id)
        cid = int(chan.id)

        fully_scanned, state_latest = await self._bf_get_state(gid, "channel", cid)

        after_dt = None
        did_full = True
        used_resume_cursor = False

        if not force_full:
            if fully_scanned:
                latest_ts = await self._bf_latest_ts_in_messages(gid, "channel", cid)
                if latest_ts > 0:
                    after_dt = datetime.fromtimestamp(max(0, latest_ts - 3), tz=timezone.utc)
                    did_full = False
            elif state_latest > 0:
                after_dt = datetime.fromtimestamp(max(0, state_latest - 3), tz=timezone.utc)
                used_resume_cursor = True
                did_full = True  # finishing from cursor completes the full scan overall

        # When force_full, ensure we start from the beginning of history.
        if force_full and after_dt is None:
            after_dt = datetime.fromtimestamp(1420070401, tz=timezone.utc)  # 2015-01-01 + 1s


        checkpoint_every = int(os.getenv("LORE_BF_CHECKPOINT_EVERY_SEEN", "5000"))
        seen_local = 0
        found_threads: Dict[int, nextcord.Thread] = {}

        channel_errored = False
        try:
            cursor = after_dt          # datetime | message | None
            last_cursor_id = 0

            while True:
                batch: List[nextcord.Message] = []

                async def _grab_batch():
                    out: List[nextcord.Message] = []
                    async for msg in chan.history(limit=200, oldest_first=True, after=cursor):
                        out.append(msg)
                    return out

                batch = await self._retry_discord_op(
                    _grab_batch,
                    progress=progress,
                    label="channel_history",
                    tries=5,
                    base_delay=1.0,
                )

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
                    seen_local += 1
                    try:
                        progress.last_msg_ts = int(msg.created_at.timestamp())
                    except Exception:
                        pass

                    # Discover threads from starter messages in this channel history.
                    try:
                        th = getattr(msg, "thread", None)
                        if th and isinstance(th, nextcord.Thread):
                            found_threads[int(th.id)] = th
                    except Exception:
                        pass

                    await self._archive_try(msg, progress)

                    # per-channel checkpointing
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

        except Exception as e:
            # Missing perms or unsupported channel type.
            channel_errored = True
            self._note_progress_error(progress, "channel_error", e)
            return

        # Mark/refresh channel state
        latest_ts_now = await self._bf_latest_ts_in_messages(gid, "channel", cid)
        await self._bf_set_state(
            gid, "channel", cid,
            fully_scanned=(1 if ((not channel_errored) and (fully_scanned or did_full or used_resume_cursor)) else 0),
            latest_ts=latest_ts_now,
        )

        # Backfill threads under this channel (found from starter messages + best-effort listing).
        threads: List[nextcord.Thread] = list(found_threads.values())
        try:
            threads.extend(await self._iter_threads_best_effort(chan))
        except Exception as e:
            self._note_progress_error(progress, "thread_list_error", e)

        uniq: Dict[int, nextcord.Thread] = {}
        for t in threads:
            try:
                uniq[int(t.id)] = t
            except Exception:
                continue

        threads = list(uniq.values())
        for t in threads:
            await self._backfill_thread(
                guild,
                t,
                progress,
                force_full=force_full,
                parent_chan=chan,
                checkpoint_every=checkpoint_every,
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

                ch = await self._get_channel_or_thread(guild, int(cid))
                if ch is None:
                    p.channels_done += 1
                    continue

                if isinstance(ch, nextcord.Thread):
                    await self._backfill_thread(
                        guild,
                        ch,
                        p,
                        force_full=force_full,
                        parent_chan=getattr(ch, "parent", None),
                        checkpoint_every=int(os.getenv("LORE_BF_CHECKPOINT_EVERY_SEEN", "5000")),
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

    async def _fts_available(self) -> bool:
        async with self._db_lock:
            conn = self._get_conn()
            try:
                conn.execute("SELECT 1 FROM lore_fts LIMIT 1").fetchone()
                return True
            except Exception:
                return False

    async def _fts_search(
        self,
        guild_id: int,
        query: str,
        limit: int = 8,
        *,
        field: Optional[str] = None,
    ) -> List[int]:
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
                    """
                    SELECT lore_fts.message_id
                    FROM lore_fts
                    JOIN lore_messages m
                      ON m.message_id = lore_fts.rowid
                    WHERE m.guild_id = ?
                      AND lore_fts MATCH ?
                    ORDER BY bm25(lore_fts)
                    LIMIT ?
                    """,
                    (int(guild_id), q, int(limit)),
                ).fetchall()
                return [int(r[0]) for r in rows]

            out: List[int] = []
            seen = set()

            def _apply_field(q: str) -> str:
                f = (field or "").strip()
                if f in ("content", "speaker_name"):
                    return f"{f}:({q})"
                return q
                
            for i, cand in enumerate(cands):
                try:
                    ids = run(_apply_field(cand))
                except Exception as e:
                    if os.getenv("LORE_FTS_DEBUG", "0") != "0":
                        print(f"[lore] FTS error ({field=}): {type(e).__name__}: {e} | q={_apply_field(cand)!r}")
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


                # As a last resort, broaden to OR
                #try:
                #    ids3 = run(q_primary.replace(" AND ", " OR "))
                #    for mid in ids3:
                #        if mid in seen:
                #            continue
                #        seen.add(mid)
                #        out.append(mid)
                #        if len(out) >= limit:
                #            break
                #except Exception:
                #    pass

                #return out
                
            # If strict AND got too few hits, broaden gradually:
            #   1) try 2-term combos (less "thread soup")
            #   2) last resort OR
            if (" AND " in q_primary) and (not q_primary.startswith('"')) and (len(out) < min_hits):
                parts = [p.strip() for p in q_primary.split(" AND ") if p.strip()]

                if len(parts) >= 3:
                    # Prefer combos with longer tokens (often proper nouns like "pakhtun")
                    parts_sorted = sorted(parts, key=len, reverse=True)
                    for a, b in itertools.combinations(parts_sorted, 2):
                        try:
                            ids2 = run(_apply_field(f"{a} AND {b}"))
                        except Exception:
                            continue
                        for mid in ids2:
                            if mid in seen:
                                continue
                            seen.add(mid)
                            out.append(mid)
                            if len(out) >= limit:
                                return out
                        if len(out) >= min_hits:
                            break

                if len(out) < min_hits:
                    try:
                        ids3 = run(_apply_field(q_primary.replace(" AND ", " OR ")))
                        for mid in ids3:
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
        ids = list(ids or [])
        k = int(k or 0)

        if not ids or k <= 0:
            return []
        if k == 1:
            # pick a representative point (middle) to avoid biasing old/new
            return [int(ids[(len(ids) - 1) // 2])]
        if len(ids) <= k:
            return [int(x) for x in ids]


        # evenly spaced across the list so we seed both early + late evidence
        idxs = [round(i * (len(ids) - 1) / (k - 1)) for i in range(k)]
        seen = set()
        out: List[int] = []
        for ix in idxs:
            mid = int(ids[int(ix)])
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

        # recent arcs of X / major arcs of X / death of X / fate of X / what happened to X
        for pat in (
            r"(?i)^recent arcs of (.+)$",
            r"(?i)^major arcs of (.+)$",
            r"(?i)^death of (.+)$",
            r"(?i)^fate of (.+)$",
            r"(?i)^what happened to (.+)$",
            r"(?i)^how did (.+?) die$",
        ):
            m = re.match(pat, q)
            if m:
                out.append(m.group(1).strip())

        # X's relationship to Y
        m = re.search(r"(.+?)'s relationship to (.+)", q, flags=re.I)
        if m:
            out.append(m.group(1).strip())
            out.append(m.group(2).strip())

        # relationship between X and Y
        m = re.match(
            r"(?i)^(?:what(?:'s| is)\s+)?(?:the\s+)?relationship between\s+(.+?)\s+and\s+(.+)$",
            q,
        )
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
    def _question_wants_status(self, q: str) -> bool:
        ql = _clean_text(q or "").lower().strip()
        if not ql:
            return False

        if re.match(r"^(recent|latest|newest|timeline|chrono|chronological)\s*:\s*", ql):
            return True

        status_prefixes = (
            "status of ", "current status of ", "state of ", "current state of ",
            "what happened to ", "fate of ", "death of ", "where is ", "where are ",
            "who leads ", "who rules ", "who controls ", "who governs ",
            "is ", "are ", "was ", "were ", "does ", "do ", "did ", "can ",
            "how destroyed ", "how burned ", "how damaged ", "how bad ", "what level of ",
        )
        if ql.startswith(status_prefixes):
            return True

        status_terms = {
            "status", "current", "currently", "now", "recent", "recently", "latest", "newest",
            "still", "alive", "dead", "died", "slain", "killed", "missing", "gone", "lost",
            "destroyed", "burned", "damaged", "ruined", "ashes", "rebuilding", "rebuilt",
            "functional", "inhabited", "occupied", "independent", "control", "controlled",
            "leader", "leadership", "rules", "governs", "governed", "supplies", "stationed",
            "defended", "holds", "held", "condition", "state", "what", "how", "where", "when", "why",
        }
        toks = re.findall(r"[a-z0-9']+", ql)
        return any(t in status_terms for t in toks)

    def _looks_like_name_lookup(self, q: str) -> bool:
        ql = _clean_text(q or "").lower().strip()
        if not ql or self._question_wants_status(ql):
            return False

        explicit_non_name = (
            "recent arcs of ", "major arcs of ", "what happened to ", "fate of ",
            "death of ", "how did ", "where did ", "when did ", "why did ",
        )
        if ql.startswith(explicit_non_name):
            return False

        toks = [t for t in re.findall(r"[a-z0-9']+", ql) if t and t not in STOPWORDS]
        if not toks:
            return False

        entityish = {
            "order", "church", "guild", "house", "clan", "company", "corps", "legion",
            "city", "town", "village", "fort", "fortress", "keep", "kingdom", "empire",
            "temple", "council", "academy", "college", "battle", "siege", "ritual", "expedition",
        }
        if any(t in entityish for t in toks):
            return False

        return len(toks) <= 3

    def _infer_subject_kind_from_rows(self, question: str, rows: Optional[List[sqlite3.Row]] = None) -> str:
        """Best-effort subject typing from retrieved rows: 'person', 'org', 'place', or 'unknown'.

        This is intentionally lightweight. It exists to stop bare proper-noun lookups like
        'Jorlyn' from being forced into status mode while still letting place/org lookups like
        'Bastion' or 'Order Crimson Rain' resolve as status overviews.
        """
        q = _clean_text(question or "")
        if not q or not rows:
            return "unknown"

        subj = q[7:].strip() if q.lower().startswith("who is ") else q
        subj = _clean_text(subj)
        subj_l = subj.lower().strip()
        if not subj_l:
            return "unknown"

        subj_rx = re.escape(subj)
        speaker_exact = 0
        speaker_partial = 0
        person_score = 0.0
        org_score = 0.0
        place_score = 0.0

        org_terms = {
            "order", "church", "guild", "house", "clan", "company", "corps", "legion",
            "faction", "mercenary", "mercenaries", "nobility", "government", "militia",
        }
        place_terms = {
            "city", "town", "village", "fort", "fortress", "keep", "kingdom", "empire",
            "district", "slums", "slum", "wall", "walls", "gate", "harbor", "street",
            "streets", "region", "province", "capital",
        }
        person_title_pat = (
            r"(?:lord|lady|sir|dame|auctor|captain|commander|queen|king|prince|princess|"
            r"marshal|field marshal|battle-capsarii|general|sergeant|major)"
        )

        for r in rows[:120]:
            raw = _clean_text(r["content"] or "")
            txt = raw.lower()
            who = _clean_text(r["speaker_name"] or "")
            who_l = who.lower()

            if subj_l and who_l == subj_l:
                speaker_exact += 1
                person_score += 7.0
            elif subj_l and subj_l in who_l:
                speaker_partial += 1
                person_score += 3.5

            if subj_l and subj_l not in txt and subj_l not in who_l:
                continue

            # Strong person-ish signals.
            if re.search(rf"\b{person_title_pat}\s+{subj_rx}\b", raw, flags=re.I):
                person_score += 4.0
            if re.search(rf"\b{subj_rx}\s+[A-Z][a-zA-Z'’\-]+\b", raw):
                person_score += 3.0
            if re.search(rf"\b{subj_rx}\b.*\b(?:he|she|his|her|husband|wife|son|daughter)\b", txt, flags=re.I):
                person_score += 1.5

            # Strong org-ish signals.
            if re.search(rf"\b(?:order|church|guild|house|corps|company|legion|clan)\s+(?:of\s+)?{subj_rx}\b", raw, flags=re.I):
                org_score += 5.0
            if re.search(rf"\b{subj_rx}\s+(?:order|church|guild|house|corps|company|legion|clan)\b", raw, flags=re.I):
                org_score += 4.0
            if any(t in txt for t in org_terms):
                org_score += 1.2

            # Strong place-ish signals.
            if re.search(rf"\b(?:city|town|village|fort|fortress|keep|district|slums?|walls?)\s+of\s+{subj_rx}\b", raw, flags=re.I):
                place_score += 5.0
            if re.search(rf"\b{subj_rx}\s+(?:city|town|village|fort|fortress|keep|district|slums?)\b", raw, flags=re.I):
                place_score += 4.0
            if any(t in txt for t in place_terms):
                place_score += 1.2

        if speaker_exact >= 1 or person_score >= max(org_score, place_score) + 2.0 and person_score >= 5.0:
            return "person"
        if org_score >= place_score + 2.0 and org_score >= 5.0:
            return "org"
        if place_score >= org_score + 2.0 and place_score >= 5.0:
            return "place"
        if person_score > max(org_score, place_score) and person_score >= 6.0:
            return "person"
        return "unknown"

    def _infer_answer_profile(self, question: str, rows: Optional[List[sqlite3.Row]] = None) -> str:
        """Return 'bio', 'status', or 'topic'."""
        q = _clean_text(question or "")
        ql = q.lower().strip()
        if not ql:
            return "topic"

        if self._question_wants_status(ql):
            return "status"

        if self._is_bio_question(ql):
            return "bio"

        if not rows:
            return "topic"

        subj = q[7:].strip() if ql.startswith("who is ") else q
        subj = _clean_text(subj)
        subj_l = subj.lower()
        subj_l_nopunct = re.sub(r"[’‘']+", "", subj_l)
        query_toks = [t for t in re.findall(r"[a-z0-9']+", ql) if len(t) >= 3 and t not in STOPWORDS][:4]

        speaker_hits = 0
        exact_speaker_hits = 0
        content_hits = 0
        for r in rows:
            who = _clean_text(r["speaker_name"] or "").lower()
            who_nopunct = re.sub(r"[’‘']+", "", who)
            txt = _clean_text(r["content"] or "").lower()
            if subj_l_nopunct and subj_l_nopunct in who_nopunct:
                speaker_hits += 1
            if subj_l_nopunct and who_nopunct == subj_l_nopunct:
                exact_speaker_hits += 1
            if subj_l and subj_l in txt:
                content_hits += 1
            elif query_toks and sum(1 for t in query_toks if t in txt) >= max(1, min(2, len(query_toks))):
                content_hits += 1

        subject_kind = self._infer_subject_kind_from_rows(q, rows)
        if subject_kind == "person":
            return "bio"
        if subject_kind == "org":
            return "org"
        if subject_kind == "place":
            return "status"

        if exact_speaker_hits >= 2:
            return "bio"
        if speaker_hits >= 4 and speaker_hits >= max(3, content_hits):
            return "bio"
        if self._looks_like_name_lookup(q) and speaker_hits >= 2 and speaker_hits >= content_hits:
            return "bio"

        if len(query_toks) <= 3:
            return "status"
        return "topic"

    def _is_bio_question(self, q: str) -> bool:
        """Conservative heuristic: only explicit person/bio-style asks force bio mode."""
        ql = _clean_text(q or "").lower().strip()
        if not ql:
            return False

        non_bio_prefixes = (
            "recent arcs of ",
            "major arcs of ",
            "death of ",
            "fate of ",
            "what happened to ",
            "how did ",
            "where did ",
            "when did ",
            "why did ",
            "status of ",
            "current status of ",
            "state of ",
            "current state of ",
        )
        if ql.startswith(non_bio_prefixes):
            return False

        if ql.startswith(("who is ", "who's ", "tell me about ", "describe ")):
            return True

        if (
            "relationship between " in ql
            or "'s relationship to " in ql
        ):
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

    # ------------------------- lore map (entity graph) -------------------------

    TITLE_PREFIXES = {
        "lord", "lady", "sir", "dame", "captain", "commander",
        "duke", "baron", "count", "king", "queen", "prince", "princess",
        "bishop", "father", "mother", "saint",
    }

    def _strip_titles(self, canon: str) -> str:
        toks = (canon or "").split()
        while toks and toks[0] in self.TITLE_PREFIXES:
            toks = toks[1:]
        return " ".join(toks).strip()


    def _map_display(self, s: str) -> str:
        s = _clean_text(s or "")
        s = re.sub(r"(?:'s|’s)\b", "", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s
        
                
    def _map_canon(self, s: str) -> str:
        s = _clean_text(s or "").lower()
        # keep letters/numbers/spaces/'/- , drop the rest
        s = re.sub(r"[^a-z0-9\s'’\-]", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        # drop leading articles
        s = re.sub(r"^(the|a|an)\s+", "", s).strip()
        # after your existing clean/lower/strip steps
        s = re.sub(r"(?:'s|’s)\b", "", s)   # Pakhtun's -> Pakhtun
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _map_looks_noisy(self, text: str) -> bool:
        t = _clean_text(text or "")
        if not t:
            return True
        # common combat/roll noise markers (tune later if needed)
        low = t.lower()
        if any(x in low for x in ("damage:", "attack roll", "target hp", "ac ", "hp ", "initiative", "critical hit")):
            return True
        digits = sum(ch.isdigit() for ch in t)
        if len(t) >= 40 and (digits / max(1, len(t))) > 0.22:
            return True
        return False

    def _map_extract_phrases(self, text: str) -> List[Tuple[str, str]]:
        """
        Return [(kind, phrase_display)] found in message content.
        kind in: org/event/thing
        """
        t = _clean_text(text or "")
        if not t:
            return []
        if self._map_looks_noisy(t):
            return []

        out: List[Tuple[str, str]] = []

        # Events like "Battle of Arinock"
        event_heads = (
            "Battle", "Siege", "Trial", "Ritual", "Expedition", "Raid", "Massacre",
            "Council", "Summit", "Treaty", "Accord", "Coronation", "Wedding", "Funeral",
            "Assassination", "Rebellion", "Uprising", "Fall", "Rise"
        )
        ev_re = r"\b(" + "|".join(event_heads) + r")\s+of\s+([A-Z][\w'’\-]+(?:\s+[A-Z][\w'’\-]+){0,4})\b"
        for m in re.finditer(ev_re, t):
            out.append(("event", f"{m.group(1)} of {m.group(2)}"))

        # Orgs like "Order of Crimson Rain", "House Blackmire", "Guild of ...", etc.
        org_heads = (
            "Order", "Guild", "House", "Clan", "Company", "Cult", "Legion", "Syndicate",
            "Brotherhood", "Circle", "Cabal", "Council", "Temple", "Church", "Inquisition",
            "Academy", "College", "Coven"
        )
        org_re = r"\b(" + "|".join(org_heads) + r")\s+(of\s+)?([A-Z][\w'’\-]+(?:\s+[A-Z][\w'’\-]+){0,4})\b"
        for m in re.finditer(org_re, t):
            mid = "of " if m.group(2) else ""
            out.append(("org", f"{m.group(1)} {mid}{m.group(3)}".strip()))

        # Generic title-cased multiword phrases (2..5 words)
        # e.g. "Pyramid of Shadows", "Explorer Corps", "Six Dragons"
        generic = re.finditer(r"\b[A-Z][a-z][\w'’\-]+(?:\s+(?:of|the|and|in|at|to))?\s+[A-Z][\w'’\-]+(?:\s+[A-Z][\w'’\-]+){0,3}\b", t)
        for m in generic:
            phrase = m.group(0).strip()
            # quick sanity filters
            if len(phrase) < 6:
                continue
            if phrase.split()[0] in ("The", "A", "An"):
                continue
            out.append(("thing", phrase))

        # de-dupe while keeping order
        seen = set()
        out2: List[Tuple[str, str]] = []
        for k, p in out:
            key = (k, self._map_canon(p))
            if key in seen:
                continue
            seen.add(key)
            out2.append((k, p))
        return out2

    async def _map_rebuild_guild(
        self,
        guild_id: int,
        *,
        min_mentions: int = 3,
        max_people: int = 250,
        max_other: int = 750,
    ) -> Dict[str, int]:
        """
        Build entities + edges from lore_messages for one guild_id.
        Returns stats dict.
        """
        gid = int(guild_id)
        # ensure schema exists
        async with self._db_lock:
            _ = self._get_conn()

        path = _db_path()
        scan = sqlite3.connect(str(path), check_same_thread=False)
        scan.row_factory = sqlite3.Row

        speaker_ct = Counter()
        speaker_first = {}
        speaker_last = {}

        cand_ct = Counter()
        cand_first = {}
        cand_last = {}
        cand_kind = {}
        cand_display = {}

        # Pass 1: count candidates
        cur = scan.execute(
            """
            SELECT message_id, created_ts, speaker_name, content
            FROM lore_messages
            WHERE guild_id=?
            ORDER BY created_ts ASC
            """,
            (gid,),
        )
        while True:
            rows = cur.fetchmany(2000)
            if not rows:
                break
            for r in rows:
                ts = int(r["created_ts"] or 0)
                sp = (r["speaker_name"] or "").strip()
                if sp:
                    csp = self._map_canon(sp)
                    if csp:
                        speaker_ct[csp] += 1
                        speaker_first[csp] = min(int(speaker_first.get(csp, ts) or ts), ts)
                        speaker_last[csp] = max(int(speaker_last.get(csp, ts) or ts), ts)

                txt = r["content"] or ""
                for kind, phrase in self._map_extract_phrases(txt):
                    c = self._map_canon(phrase)
                    if not c:
                        continue
                    cand_ct[(kind, c)] += 1
                    cand_kind[(kind, c)] = kind
                    cand_first[(kind, c)] = min(int(cand_first.get((kind, c), ts) or ts), ts)
                    cand_last[(kind, c)] = max(int(cand_last.get((kind, c), ts) or ts), ts)
                    # keep a stable display (prefer longer / more "titled")
                    prev = cand_display.get((kind, c), "")
                    if (not prev) or (len(phrase) > len(prev)):
                        cand_display[(kind, c)] = self._map_display(phrase)

        total_msgs = int(sum(1 for _ in speaker_first.keys()))  # not true total; just a placeholder if needed
        # Pick entities
        people = [p for p, _n in speaker_ct.most_common(max_people)]
        other_pool = []
        for (kind, c), n in cand_ct.items():
            if int(n) < int(min_mentions):
                continue
            base = float(n)
            if kind == "org":
                base += 2.0
            if kind == "event":
                base += 2.5
            other_pool.append((base, kind, c))
        other_pool.sort(reverse=True, key=lambda x: x[0])
        other = other_pool[:max_other]

        # Write phase: wipe + insert entities
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM lore_entity_edges WHERE guild_id=?", (gid,))
            conn.execute("DELETE FROM lore_entity_mentions WHERE guild_id=?", (gid,))
            conn.execute("DELETE FROM lore_entities WHERE guild_id=?", (gid,))
            conn.commit()

            ent_rows = []
            for csp in people:
                # display as title-ish (keep original canon, but nicer)
                disp = " ".join(w.capitalize() for w in csp.split())
                score = float(speaker_ct[csp])
                ent_rows.append((gid, csp, disp, "person", score, int(speaker_ct[csp]), int(speaker_first.get(csp, 0) or 0), int(speaker_last.get(csp, 0) or 0)))

            for score, kind, c in other:
                disp = cand_display.get((kind, c), c)
                ent_rows.append((gid, c, disp, kind, float(score), int(cand_ct[(kind, c)]), int(cand_first.get((kind, c), 0) or 0), int(cand_last.get((kind, c), 0) or 0)))

            conn.executemany(
                """
                INSERT INTO lore_entities(guild_id, canon, display, kind, score, mentions, first_ts, last_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                ent_rows,
            )
            conn.commit()

            ent = conn.execute(
                "SELECT entity_id, canon, kind FROM lore_entities WHERE guild_id=?",
                (gid,),
            ).fetchall()
            id_by = {(str(r["kind"]), str(r["canon"])): int(r["entity_id"]) for r in ent}
            person_id = {canon: eid for (kind, canon), eid in id_by.items() if kind == "person"}
        
        # Pass 2: build mentions + edges (in memory), then flush
        mentions_batch = []
        edges = {}  # (a,b) -> [w, first_ts, last_ts]

        cur2 = scan.execute(
            """
            SELECT message_id, created_ts, speaker_name, content
            FROM lore_messages
            WHERE guild_id=?
            ORDER BY created_ts ASC
            """,
            (gid,),
        )
        while True:
            rows = cur2.fetchmany(2000)
            if not rows:
                break
            for r in rows:
                mid = int(r["message_id"] or 0)
                ts = int(r["created_ts"] or 0)
                ids = set()

                sp = (r["speaker_name"] or "").strip()
                if sp:
                    csp = self._map_canon(sp)
                    eid = id_by.get(("person", csp))
                    if eid:
                        ids.add(eid)

                txt = r["content"] or ""
                for kind, phrase in self._map_extract_phrases(txt):
                    c = self._map_canon(phrase)
                    if kind == "thing":
                        c2 = self._strip_titles(c)
                        if c2 in person_id:
                            ids.add(person_id[c2])
                            continue
                    eid = id_by.get((kind, c))
                    if eid:
                        ids.add(eid)

                if not ids:
                    continue

                for eid in ids:
                    mentions_batch.append((gid, int(eid), mid, ts))

                if len(ids) >= 2:
                    ids_sorted = sorted(ids)
                    for i in range(len(ids_sorted)):
                        for j in range(i + 1, len(ids_sorted)):
                            a = int(ids_sorted[i])
                            b = int(ids_sorted[j])
                            key = (a, b)
                            if key not in edges:
                                edges[key] = [1.0, ts, ts]
                            else:
                                edges[key][0] += 1.0
                                edges[key][1] = min(edges[key][1], ts)
                                edges[key][2] = max(edges[key][2], ts)

        scan.close()

        async with self._db_lock:
            conn = self._get_conn()

            if mentions_batch:
                conn.executemany(
                    """
                    INSERT OR IGNORE INTO lore_entity_mentions(guild_id, entity_id, message_id, created_ts)
                    VALUES (?, ?, ?, ?)
                    """,
                    mentions_batch,
                )

            edge_rows = [(gid, a, b, float(v[0]), int(v[1]), int(v[2])) for (a, b), v in edges.items()]
            if edge_rows:
                conn.executemany(
                    """
                    INSERT INTO lore_entity_edges(guild_id, a_entity_id, b_entity_id, weight, first_ts, last_ts)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    edge_rows,
                )
            # after INSERT lore_entity_mentions / edges, before commit (or right after)
            conn.execute(
                """
                UPDATE lore_entities
                SET mentions = (
                    SELECT COUNT(*) FROM lore_entity_mentions em
                    WHERE em.guild_id = lore_entities.guild_id
                      AND em.entity_id = lore_entities.entity_id
                )
                WHERE guild_id=?
                """,
                (gid,),
            )
            conn.execute(
                """
                UPDATE lore_entities
                SET score = mentions
                          + CASE kind WHEN 'org' THEN 2.0
                                      WHEN 'event' THEN 2.5
                                      ELSE 0.0 END
                WHERE guild_id=?
                """,
                (gid,),
            )
            conn.commit()

            # quick counts by kind
            kinds = conn.execute(
                "SELECT kind, COUNT(*) AS c FROM lore_entities WHERE guild_id=? GROUP BY kind",
                (gid,),
            ).fetchall()
            by_kind = {str(r["kind"]): int(r["c"] or 0) for r in kinds}

        return {
            "entities": int(len(ent_rows)),
            "edges": int(len(edges)),
            "mentions": int(len(mentions_batch)),
            "people": int(by_kind.get("person", 0)),
            "orgs": int(by_kind.get("org", 0)),
            "events": int(by_kind.get("event", 0)),
            "things": int(by_kind.get("thing", 0)),
        }
            
    def _persona_canon(self, name: str) -> str:
        # normalize + strip leading titles so "Lord Starshield" and "Starshield" collide
        c = self._map_canon(name or "")
        c = self._strip_titles(c)
        return c.strip()

    async def _persona_get(self, guild_id: int, canon: str) -> Optional[sqlite3.Row]:
        async with self._db_lock:
            conn = self._get_conn()
            return conn.execute(
                "SELECT * FROM lore_personas WHERE guild_id=? AND canon=?",
                (int(guild_id), str(canon)),
            ).fetchone()

    async def _persona_upsert(self, guild_id: int, canon: str, display: str, profile: str, stats: dict) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            now = _now_ts()
            conn.execute(
                """
                INSERT INTO lore_personas(guild_id, canon, display, profile, stats_json, built_ts, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, canon) DO UPDATE SET
                    display=excluded.display,
                    profile=excluded.profile,
                    stats_json=excluded.stats_json,
                    built_ts=excluded.built_ts,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), str(canon), str(display), str(profile), json.dumps(stats, ensure_ascii=False), int(now), int(now)),
            )
            conn.commit()

    async def _persona_delete(self, guild_id: int, canon: str) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute("DELETE FROM lore_personas WHERE guild_id=? AND canon=?", (int(guild_id), str(canon)))
            conn.commit()

    async def _persona_list(self, guild_id: int, limit: int = 25) -> List[sqlite3.Row]:
        async with self._db_lock:
            conn = self._get_conn()
            return conn.execute(
                "SELECT canon, display, updated_ts FROM lore_personas WHERE guild_id=? ORDER BY updated_ts DESC LIMIT ?",
                (int(guild_id), int(limit)),
            ).fetchall()

    # ------------------------- scene memory (RP) -------------------------

    async def _scene_boost_terms(self, guild_id: int, scene_text: str, *, limit: int = 6) -> List[str]:
        scene_text = _clean_text(scene_text or "")
        if not scene_text:
            return []

        cands: List[str] = []

        # Speakers (from "Name: ..." lines)
        for ln in scene_text.splitlines():
            m = re.match(r"^([^:]{2,60}):\s+", ln.strip())
            if m:
                who = m.group(1).strip()
                if who and who.lower() != "user":
                    cands.append(who)

        # Phrase-like entities (org/event/thing) using your lore-map extractor
        try:
            for _k, phrase in self._map_extract_phrases(scene_text):
                if phrase:
                    cands.append(phrase)
        except Exception:
            pass

        # Canonize + dedupe
        canon_to_display: Dict[str, str] = {}
        for s in cands:
            disp = _clean_text(s)
            if not disp:
                continue
            canon = self._map_canon(disp)
            if not canon:
                continue
            canon_to_display[canon] = disp
            # also try title-stripped form (helps match person entries)
            canon2 = self._strip_titles(canon)
            if canon2:
                canon_to_display.setdefault(canon2, disp)

        canons = list(canon_to_display.keys())[:40]
        if not canons:
            return []

        # Prefer terms that actually exist in the lore map (if built)
        async with self._db_lock:
            conn = self._get_conn()
            ph = ",".join(["?"] * len(canons))
            rows = conn.execute(
                f"""
                SELECT canon, display, score
                FROM lore_entities
                WHERE guild_id=? AND canon IN ({ph})
                ORDER BY score DESC
                LIMIT 20
                """,
                (int(guild_id), *canons),
            ).fetchall()

        out: List[str] = []
        if rows:
            for r in rows:
                disp = str(r["display"] or "").strip()
                if disp:
                    out.append(disp)
                    if len(out) >= limit:
                        break
        else:
            # fallback: just return the raw candidates
            for c in canons:
                out.append(canon_to_display.get(c, c))
                if len(out) >= limit:
                    break

        # final dedupe case-insensitive
        seen = set()
        final = []
        for t in out:
            k = t.lower().strip()
            if not k or k in seen:
                continue
            seen.add(k)
            final.append(t)
        return final[:limit]
            
    async def _scene_meta_get(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str, k: str) -> str:
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT v FROM lore_scene_meta
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=? AND k=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), str(k)),
            ).fetchone()
        return str(row["v"] or "").strip() if row else ""


    async def _scene_meta_set(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str, k: str, v: str) -> None:
        v = (v or "").replace("\x00", "").strip()
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_scene_meta(guild_id, channel_id, thread_id, persona_canon, k, v, updated_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon, k) DO UPDATE SET
                    v=excluded.v,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), str(k), str(v), _now_ts()),
            )
            conn.commit()


    async def _scene_meta_all(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> Dict[str, str]:
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                """
                SELECT k, v
                FROM lore_scene_meta
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchall()
        out: Dict[str, str] = {}
        for r in (rows or []):
            kk = str(r["k"] or "").strip()
            vv = str(r["v"] or "").strip()
            if kk:
                out[kk] = vv
        return out


    async def _scene_directives_text(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> str:
        meta = await self._scene_meta_all(guild_id, channel_id, thread_id, persona_canon)
        # "director" keys
        goal = meta.get("goal", "").strip()
        intent = meta.get("intent", "").strip()
        mood = meta.get("mood", "").strip()
        mode = meta.get("mode", "").strip()
        style = meta.get("style", "").strip()
        bounds = meta.get("bounds", "").strip()

        lines = []
        if mode:   lines.append(f"MODE: {mode}")
        if style:  lines.append(f"STYLE: {style}")
        if mood:   lines.append(f"MOOD: {mood}")
        if goal:   lines.append(f"GOAL: {goal}")
        if intent: lines.append(f"INTENT: {intent}")
        if bounds: lines.append(f"BOUNDS: {bounds}")
        return "\n".join(lines).strip()



    def _scene_loc_from_message(self, message: nextcord.Message) -> Tuple[int, int]:
        ch = getattr(message, "channel", None)
        if isinstance(ch, nextcord.Thread):
            pid = getattr(ch, "parent_id", None)
            if not pid:
                parent = getattr(ch, "parent", None)
                pid = getattr(parent, "id", None) if parent else None
            return int(pid or ch.id), int(ch.id)
        return int(getattr(ch, "id", 0) or 0), 0

    def _scene_chat_lock(self, key: tuple) -> asyncio.Lock:
        lk = self._scene_chat_locks.get(key)
        if lk is None:
            lk = asyncio.Lock()
            self._scene_chat_locks[key] = lk
        return lk

    async def _scene_chat_find_active_canon(self, guild_id: int, channel_id: int, thread_id: int, user_id: int) -> str:
        """Return persona_canon if this user has sticky chat ON in this location."""
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT m_user.persona_canon AS canon
                FROM lore_scene_meta m_user
                JOIN lore_scene_meta m_on
                  ON m_on.guild_id=m_user.guild_id
                 AND m_on.channel_id=m_user.channel_id
                 AND m_on.thread_id=m_user.thread_id
                 AND m_on.persona_canon=m_user.persona_canon
                WHERE m_user.guild_id=? AND m_user.channel_id=? AND m_user.thread_id=?
                  AND m_user.k='chat_user_id' AND m_user.v=?
                  AND m_on.k='chat_on' AND m_on.v='1'
                ORDER BY m_user.updated_ts DESC
                LIMIT 1
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(int(user_id))),
            ).fetchone()
        return str(row["canon"] or "").strip() if row else ""
    def _scene_loc_from_ctx(self, ctx: commands.Context) -> Tuple[int, int]:
        """Return (channel_id, thread_id) using the same convention as lore_messages:
        - in a thread: channel_id = parent channel id, thread_id = thread id
        - not in a thread: channel_id = channel id, thread_id = 0
        """
        ch = getattr(ctx, "channel", None)
        if isinstance(ch, nextcord.Thread):
            pid = getattr(ch, "parent_id", None)
            if not pid:
                parent = getattr(ch, "parent", None)
                pid = getattr(parent, "id", None) if parent else None
            return int(pid or ch.id), int(ch.id)
        return int(getattr(ch, "id", 0) or 0), 0

    def _scene_trim_text(self, s: str, max_chars: int) -> str:
        s = (s or "").replace("\x00", "").strip()
        if max_chars > 0 and len(s) > max_chars:
            return s[:max_chars].rstrip() + "…"
        return s


    async def _scene_chat_maybe_reply(self, message: nextcord.Message) -> None:
        # only guild text, only real user messages (skip bots + webhooks)
        if not message.guild:
            return
        if getattr(message.author, "bot", False):
            return
        if getattr(message, "webhook_id", None):
            return

        prompt_raw = (message.content or "").strip()
        if not prompt_raw:
            return
        if prompt_raw.lstrip().startswith(("!", "/")):
            return

        gid = int(message.guild.id)
        ch_id, th_id = self._scene_loc_from_message(message)

        canon = await self._scene_chat_find_active_canon(gid, ch_id, th_id, int(message.author.id))
        if not canon:
            return

        # make sure scene is enabled (chat assumes continuity)
        if not await self._scene_is_enabled(gid, ch_id, th_id, canon):
            return

        # tiny cooldown so quick multi-messages don’t explode
        cooldown = float(os.getenv("LORE_SCENE_CHAT_COOLDOWN_SEC", "1.25") or "1.25")
        last_ts_s = (await self._scene_meta_get(gid, ch_id, th_id, canon, "chat_last_ts")) or "0"
        try:
            last_ts = float(last_ts_s)
        except Exception:
            last_ts = 0.0
        now = time.time()
        if cooldown > 0 and (now - last_ts) < cooldown:
            return

        lock_key = (gid, ch_id, th_id, canon, int(message.author.id))
        async with self._scene_chat_lock(lock_key):
            # re-check cooldown once inside lock
            last_ts_s = (await self._scene_meta_get(gid, ch_id, th_id, canon, "chat_last_ts")) or "0"
            try:
                last_ts = float(last_ts_s)
            except Exception:
                last_ts = 0.0
            now = time.time()
            if cooldown > 0 and (now - last_ts) < cooldown:
                return

            row = await self._persona_get(gid, canon)
            if not row:
                # don’t spam the channel; user can build persona first via !lore rp once
                return

            persona = str(row["profile"] or "")
            disp = str(row["display"] or canon)
            
            try:
                persona_stats = json.loads(str(row["stats_json"] or "{}") or "{}")
                if not isinstance(persona_stats, dict):
                    persona_stats = {}
            except Exception:
                persona_stats = {}
                
            prompt = _clean_text(prompt_raw)
            user_name = getattr(message.author, "display_name", None) or getattr(message.author, "name", None) or "User"

            scene_text = await self._scene_memory_text(gid, ch_id, th_id, canon, character_name=disp)
            directives = await self._scene_directives_text(gid, ch_id, th_id, canon)
            # Sticky chat should default to chat-mode shaping unless you explicitly set a different mode.
            d0 = (directives or "").strip()
            if "mode:" not in d0.lower():
                directives = (d0 + "\n" if d0 else "") + "mode: chat"

            # retrieval (mirror your !lore rp defaults)
            fts_limit = int(os.getenv("LORE_RP_FTS_LIMIT", os.getenv("LORE_ASK_FTS_LIMIT", "200")))
            speaker_limit = int(os.getenv("LORE_RP_SPEAKER_LIMIT", os.getenv("LORE_ASK_SPEAKER_LIMIT", "200")))
            win_before = int(os.getenv("LORE_RP_WIN_BEFORE", os.getenv("LORE_ASK_WIN_BEFORE", "6")))
            win_after = int(os.getenv("LORE_RP_WIN_AFTER", os.getenv("LORE_ASK_WIN_AFTER", "10")))
            max_excerpts = int(os.getenv("LORE_RP_MAX_EXCERPTS", "80"))

            rows, _stats2, _anchors = await self._retrieve_rows_for_question(
                gid,
                prompt,
                fts_limit=fts_limit,
                speaker_limit=speaker_limit,
                seed_count=int(os.getenv("LORE_ASK_WINDOW_SEEDS", "12")),
                win_before=win_before,
                win_after=win_after,
                max_excerpts=max_excerpts,
            )

            # optional: same scene-boost behavior as your rp command
            if scene_text and int(os.getenv("LORE_SCENE_RETRIEVAL_BOOST", "1") or "1") != 0:
                boost_n = int(os.getenv("LORE_SCENE_BOOST_N", "6") or "6")
                boost_limit = int(os.getenv("LORE_SCENE_BOOST_FTS_LIMIT", "25") or "25")
                boost_max_rows = int(os.getenv("LORE_SCENE_BOOST_MAX_ROWS", "80") or "80")

                terms = await self._scene_boost_terms(gid, scene_text, limit=boost_n)
                boost_ids: List[int] = []
                seen_ids = set()
                for t in terms:
                    ids = await self._fts_search(gid, f"\"{t}\"", limit=boost_limit) or await self._fts_search(gid, t, limit=boost_limit)
                    for mid in (ids or []):
                        if mid in seen_ids:
                            continue
                        seen_ids.add(mid)
                        boost_ids.append(mid)
                        if len(boost_ids) >= boost_max_rows:
                            break
                    if len(boost_ids) >= boost_max_rows:
                        break

                boost_rows = await self._fetch_messages(gid, boost_ids)
                if boost_rows:
                    rows = list(rows or []) + list(boost_rows or [])

            # Ensure some “voice” excerpts
            extra_voice, _ = await self._persona_collect_sources(gid, disp)
            by_id = {int(r["message_id"]): r for r in (rows or [])}
            for r in extra_voice[:20]:
                by_id[int(r["message_id"])] = r
            rows = sorted(by_id.values(), key=lambda r: int(r["created_ts"] or 0))

            # Force chat mode defaults for sticky chat so cleanup + chat rules actually engage.
            if "mode:" not in (directives or "").lower():
                directives = ("mode: chat\n" + (directives or "")).strip()

            async with message.channel.typing():
                out = await self._openai_rp(
                    name=disp,
                    persona=persona,
                    prompt=prompt,
                    excerpts=rows,
                    scene=scene_text,
                    directives=directives,
                    stats=persona_stats,  # NEW
                )

            if not out:
                if int(os.getenv("LORE_OPENAI_DEBUG", "0") or "0") != 0:
                    try:
                        await message.reply("⚠️ RP call returned no text (check bot console for OpenAI error).", mention_author=False)
                    except Exception:
                        pass
                return

            # persist for reroll/continue + continuity
            await self._scene_meta_set(gid, ch_id, th_id, canon, "last_user", prompt)
            await self._scene_meta_set(gid, ch_id, th_id, canon, "last_out", out)
            await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_last_ts", str(time.time()))

            await self._scene_append_turn(gid, ch_id, th_id, canon, role="user", speaker=user_name, content=prompt)
            await self._scene_append_turn(gid, ch_id, th_id, canon, role="assistant", speaker=disp, content=out)

            # Send as plain text (no embeds) and chunk to respect Discord limits.
            await self._reply_chunks(message, f"🎭 **{disp}**\n" + (out or ""), limit=1900, mention_author=False)


    async def _scene_is_enabled(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> bool:
        default_on = int(os.getenv("LORE_SCENE_DEFAULT_ON", "0") or "0") != 0
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT enabled
                FROM lore_scene_state
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()

            if row is None:
                if default_on:
                    conn.execute(
                        """
                        INSERT OR IGNORE INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                        VALUES (?, ?, ?, ?, 1, '', ?)
                        """,
                        (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), _now_ts()),
                    )
                    conn.commit()
                    return True
                return False

            return bool(int(row["enabled"] or 0))

    async def _scene_set_enabled(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str, enabled: bool) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                VALUES (?, ?, ?, ?, ?, '', ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon) DO UPDATE SET
                    enabled=excluded.enabled,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), 1 if enabled else 0, _now_ts()),
            )
            conn.commit()

    async def _scene_clear(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                "DELETE FROM lore_scene_turns WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?",
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            )
            conn.execute(
                """
                INSERT INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                VALUES (?, ?, ?, ?, 1, '', ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon) DO UPDATE SET
                    summary='',
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), _now_ts()),
            )
            conn.commit()

    async def _scene_append_turn(
        self,
        guild_id: int,
        channel_id: int,
        thread_id: int,
        persona_canon: str,
        *,
        role: str,
        speaker: str,
        content: str,
    ) -> None:
        max_turn_chars = int(os.getenv("LORE_SCENE_TURN_CHARS", "900") or "900")
        keep_turns = int(os.getenv("LORE_SCENE_MAX_TURNS", "24") or "24")
        hard_cap = int(os.getenv("LORE_SCENE_HARD_CAP_TURNS", "80") or "80")  # safety

        txt = self._scene_trim_text(content, max_turn_chars)
        spk = self._scene_trim_text(speaker, 80)

        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_scene_turns(guild_id, channel_id, thread_id, persona_canon, role, speaker, content, created_ts)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), str(role), str(spk), str(txt), _now_ts()),
            )

            # Hard cap only (don’t delete-to-keep_turns; rollup handles that)
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM lore_scene_turns
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()
            c = int(row["c"] or 0) if row else 0

            if c > hard_cap:
                # delete oldest overflow
                overflow = c - hard_cap
                old_ids = conn.execute(
                    """
                    SELECT turn_id
                    FROM lore_scene_turns
                    WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                    ORDER BY created_ts ASC, turn_id ASC
                    LIMIT ?
                    """,
                    (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), int(overflow)),
                ).fetchall()
                if old_ids:
                    ids = [int(r[0]) for r in old_ids]
                    ph = ",".join(["?"] * len(ids))
                    conn.execute(
                        f"DELETE FROM lore_scene_turns WHERE guild_id=? AND turn_id IN ({ph})",
                        (int(guild_id), *ids),
                    )

            conn.execute(
                """
                INSERT INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                VALUES (?, ?, ?, ?, 1, '', ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon) DO UPDATE SET
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), _now_ts()),
            )
            conn.commit()

        # Only roll up after the assistant reply (avoids summarizing mid-exchange)
        if role == "assistant":
            await self._scene_rollup_if_needed(
                int(guild_id), int(channel_id), int(thread_id), str(persona_canon),
                character_name=str(spk or "Character"),
            )

    async def _scene_get_turns(
        self,
        guild_id: int,
        channel_id: int,
        thread_id: int,
        persona_canon: str,
        *,
        limit: int,
    ) -> List[sqlite3.Row]:
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                """
                SELECT role, speaker, content, created_ts, turn_id
                FROM lore_scene_turns
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                ORDER BY created_ts DESC, turn_id DESC
                LIMIT ?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), int(limit)),
            ).fetchall()

        # We fetched newest-first; return chronological for readability.
        rows = list(rows or [])
        rows.reverse()
        return rows

    async def _scene_get_summary(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> str:
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT summary
                FROM lore_scene_state
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()
        return str(row["summary"] or "").strip() if row else ""


    async def _scene_set_summary(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str, summary: str) -> None:
        summary = (summary or "").replace("\x00", "").strip()
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon) DO UPDATE SET
                    summary=excluded.summary,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), summary, _now_ts()),
            )
            conn.commit()


    async def _openai_scene_summary_update(self, *, prior_summary: str, transcript: str) -> Optional[str]:
        # Uses your existing OpenAI plumbing.
        # If OPENAI_API_KEY is missing, just skip summarization (falls back to “recent turns only”).
        if not os.getenv("OPENAI_API_KEY"):
            return None

        prior_summary = (prior_summary or "").strip()
        transcript = (transcript or "").strip()
        if not transcript:
            return None

        max_in = int(os.getenv("LORE_SCENE_SUMMARY_INPUT_CHARS", "14000") or "14000")
        if len(transcript) > max_in:
            transcript = transcript[-max_in:]

        system = (
            "You are maintaining a rolling summary of a Discord RP scene.\n"
            "Rules:\n"
            "- Use ONLY the provided transcript + prior summary.\n"
            "- Do NOT invent facts.\n"
            "- Keep names/spellings exactly as written.\n"
            "- Capture: current location/situation, goals, relationships, promises/debts, conflicts, key items, and open questions.\n"
            "- Write in 6–14 short bullets. No prose paragraphs.\n"
        )
        user = (
            "PRIOR SUMMARY (may be empty):\n"
            f"{prior_summary}\n\n"
            "NEW TRANSCRIPT CHUNK:\n"
            f"{transcript}\n\n"
            "Return the UPDATED SUMMARY only."
        )

        max_out = int(os.getenv("LORE_SCENE_SUMMARY_MAX_OUTPUT_TOKENS", "220") or "220")
        return await self._openai_chat_text(system=system, user=user, max_out=max_out, temperature=0.2)


    async def _scene_rollup_if_needed(
        self,
        guild_id: int,
        channel_id: int,
        thread_id: int,
        persona_canon: str,
        *,
        character_name: str,
    ) -> None:
        if int(os.getenv("LORE_SCENE_SUMMARIZE_ON", "1") or "1") == 0:
            return

        keep_turns = int(os.getenv("LORE_SCENE_MAX_TURNS", "24") or "24")
        keep_recent = int(os.getenv("LORE_SCENE_SUMMARIZE_KEEP_RECENT", str(max(8, keep_turns // 2))) or str(max(8, keep_turns // 2)))

        # 1) Decide what to summarize + capture the chunk outside the lock
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT COUNT(*) AS c
                FROM lore_scene_turns
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()
            count = int(row["c"] or 0) if row else 0

            if count <= keep_turns:
                return

            n_summarize = max(0, count - keep_recent)
            if n_summarize < 4:
                return

            prior = conn.execute(
                """
                SELECT summary
                FROM lore_scene_state
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()
            prior_summary = str(prior["summary"] or "").strip() if prior else ""

            old_rows = conn.execute(
                """
                SELECT turn_id, role, speaker, content
                FROM lore_scene_turns
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=?
                ORDER BY created_ts ASC, turn_id ASC
                LIMIT ?
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), int(n_summarize)),
            ).fetchall()

        if not old_rows:
            return

        # Build transcript chunk
        lines: List[str] = []
        turn_ids: List[int] = []
        for r in old_rows:
            turn_ids.append(int(r["turn_id"]))
            role = str(r["role"] or "")
            speaker = str(r["speaker"] or "").strip() or ("User" if role == "user" else character_name)
            content = str(r["content"] or "").strip()
            if content:
                lines.append(f"{speaker}: {content}")

        transcript = "\n".join(lines).strip()
        if not transcript:
            return

        # 2) Ask the model to update the rolling summary
        new_summary = await self._openai_scene_summary_update(prior_summary=prior_summary, transcript=transcript)
        if not new_summary:
            return

        # 3) Commit: write summary + delete summarized turns
        async with self._db_lock:
            conn = self._get_conn()
            conn.execute(
                """
                INSERT INTO lore_scene_state(guild_id, channel_id, thread_id, persona_canon, enabled, summary, updated_ts)
                VALUES (?, ?, ?, ?, 1, ?, ?)
                ON CONFLICT(guild_id, channel_id, thread_id, persona_canon) DO UPDATE SET
                    summary=excluded.summary,
                    updated_ts=excluded.updated_ts
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon), str(new_summary).strip(), _now_ts()),
            )

            ph = ",".join(["?"] * len(turn_ids))
            conn.execute(
                f"DELETE FROM lore_scene_turns WHERE guild_id=? AND turn_id IN ({ph})",
                (int(guild_id), *[int(x) for x in turn_ids]),
            )
            conn.commit()
                
    async def _scene_memory_text(
        self,
        guild_id: int,
        channel_id: int,
        thread_id: int,
        persona_canon: str,
        *,
        character_name: str,
    ) -> str:
        limit = int(os.getenv("LORE_SCENE_MAX_TURNS", "24") or "24")
        max_chars = int(os.getenv("LORE_SCENE_MAX_CHARS", "3500") or "3500")

        summary = await self._scene_get_summary(guild_id, channel_id, thread_id, persona_canon)
        turns = await self._scene_get_turns(guild_id, channel_id, thread_id, persona_canon, limit=limit)

        if (not summary) and (not turns):
            return ""

        lines: List[str] = []
        if summary:
            lines.append("Summary so far:")
            lines.append(summary.strip())
            lines.append("")

        for r in (turns or []):
            role = str(r["role"] or "")
            speaker = str(r["speaker"] or "").strip() or ("User" if role == "user" else character_name)
            content = str(r["content"] or "").strip()
            if content:
                lines.append(f"{speaker}: {content}")

        text = "\n".join(lines).strip()
        if max_chars > 0 and len(text) > max_chars:
            text = "…\n" + text[-max_chars:]
        return text
        
    async def _persona_collect_sources(self, guild_id: int, name: str) -> Tuple[List[sqlite3.Row], dict]:
        """
        Build a compact evidence set:
        - self-voice (speaker_name match)
        - plus mentions via FTS (so NPCs still work)
        """
        gid = int(guild_id)
        name_clean = _clean_text(name or "")
        canon = self._persona_canon(name_clean)

        self_limit = int(os.getenv("LORE_PERSONA_SELF_LIMIT", "500"))
        mention_limit = int(os.getenv("LORE_PERSONA_MENTION_LIMIT", "250"))
        max_excerpts = int(os.getenv("LORE_PERSONA_MAX_EXCERPTS", "160"))

        # 1) self-voice ids (speaker_name LIKE canon)
        like = f"%{canon}%"
        async with self._db_lock:
            conn = self._get_conn()
            ids_desc = conn.execute(
                """
                SELECT message_id FROM lore_messages
                WHERE guild_id=? AND lower(trim(speaker_name)) LIKE ?
                ORDER BY created_ts DESC
                LIMIT ?
                """,
                (gid, like, self_limit),
            ).fetchall()
            ids_asc = conn.execute(
                """
                SELECT message_id FROM lore_messages
                WHERE guild_id=? AND lower(trim(speaker_name)) LIKE ?
                ORDER BY created_ts ASC
                LIMIT ?
                """,
                (gid, like, max(80, self_limit // 4)),
            ).fetchall()

        self_ids = [int(r[0]) for r in ids_desc] + [int(r[0]) for r in ids_asc]

        # 2) mention ids (FTS)
        mention_ids = []
        try:
            # prefer quoted phrase first (better precision)
            mention_ids = await self._fts_search(gid, f"\"{name_clean}\"", limit=mention_limit)
            if not mention_ids:
                mention_ids = await self._fts_search(gid, name_clean, limit=mention_limit)
        except Exception:
            mention_ids = []

        # fetch rows
        ids_all = []
        seen = set()
        for mid in (self_ids + mention_ids):
            if mid in seen:
                continue
            seen.add(mid)
            ids_all.append(mid)

        rows_all = await self._fetch_messages(gid, ids_all)

        # --- NEW: estimate "typical post length" for this persona (from self-voice rows) ---
        self_id_set = set(int(x) for x in self_ids)
        self_lens: List[int] = []
        for r in (rows_all or []):
            try:
                mid = int(r["message_id"] or 0)
            except Exception:
                continue
            if mid not in self_id_set:
                continue

            txt = str(r["content"] or "").strip()
            if len(txt) < 20:
                continue

            # Optional: drop obvious combat/roll spam so it doesn't skew lengths
            try:
                if hasattr(self, "_map_looks_noisy") and self._map_looks_noisy(txt):
                    continue
            except Exception:
                pass

            self_lens.append(len(txt))

        def _pct(vals: List[int], p: float) -> int:
            if not vals:
                return 0
            vals = sorted(vals)
            if len(vals) == 1:
                return int(vals[0])
            i = (len(vals) - 1) * float(p)
            lo = int(i)
            hi = min(len(vals) - 1, lo + 1)
            frac = i - lo
            return int(round(vals[lo] + (vals[hi] - vals[lo]) * frac))

        len_stats = {}
        if self_lens:
            len_stats = {
                "self_len_n": int(len(self_lens)),
                "self_len_mean": int(round(sum(self_lens) / max(1, len(self_lens)))),
                "self_len_p25": _pct(self_lens, 0.25),
                "self_len_p50": _pct(self_lens, 0.50),
                "self_len_p75": _pct(self_lens, 0.75),
                "self_len_p90": _pct(self_lens, 0.90),
            }
            
        # time-stratify so persona isn’t “recent-only”
        rows_all = sorted(rows_all, key=lambda r: int(r["created_ts"] or 0))
        bins = int(os.getenv("LORE_PERSONA_STRATA_BINS", "10"))
        per_bin = int(os.getenv("LORE_PERSONA_STRATA_PER_BIN", "10"))
        recent_bonus = int(os.getenv("LORE_PERSONA_STRATA_RECENT_BONUS", "10"))
        sampled = self._time_stratified_rows(rows_all, bins=bins, per_bin=per_bin, recent_bonus=recent_bonus)

        if max_excerpts and len(sampled) > max_excerpts:
            sampled = sampled[-max_excerpts:]

        stats = {
            "canon": canon,
            "self_rows_total": len(set(self_ids)),
            "mention_rows_total": len(set(mention_ids)),
            "sampled_rows": len(sampled),
            **len_stats,  # NEW
        }
        return sampled, stats

    # replace your existing _openai_chat_text with this version
    # only two real changes:
    # 1) adds model: Optional[str] = None to the signature
    # 2) uses "model or ..." instead of always reading LORE_MODEL

    async def _openai_chat_text(
        self,
        *,
        system: str,
        user: str,
        max_out: int,
        temperature: float,
        model: Optional[str] = None,
    ) -> Optional[str]:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None

        debug = int(os.getenv("LORE_OPENAI_DEBUG", "0") or "0") != 0

        try:
            from openai import OpenAI  # type: ignore
        except Exception as e:
            if debug:
                print(f"[lore] OpenAI import failure: {type(e).__name__}: {e}")
            return None

        model = model or os.getenv("LORE_MODEL", "gpt-4o-mini")
        client = OpenAI(api_key=api_key)

        def _call() -> Optional[str]:
            try:
                prompt = f"{system}\n\n{user}"
                resp = client.responses.create(
                    model=model,
                    input=prompt,
                    temperature=float(temperature),
                    max_output_tokens=int(max_out),
                )
                text = getattr(resp, "output_text", None)
                if text:
                    return text.strip()
            except Exception as e:
                if debug:
                    print(f"[lore] OpenAI responses.create failed: {type(e).__name__}: {e}")

            try:
                resp = client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user},
                    ],
                    temperature=float(temperature),
                    max_tokens=int(max_out),
                )
                text = (resp.choices[0].message.content or "").strip()
                return text or None
            except Exception as e:
                if debug:
                    print(f"[lore] OpenAI chat.completions failed: {type(e).__name__}: {e}")

            return None

        return await asyncio.to_thread(_call)


    def _extract_json_array_text(self, raw: str) -> str:
        s = (raw or "").strip()
        if not s:
            return ""

        # fenced json block
        m = re.search(r"```(?:json)?\s*(\[[\s\S]*?\])\s*```", s, flags=re.I)
        if m:
            return m.group(1).strip()

        # plain array somewhere in the reply
        start = s.find("[")
        end = s.rfind("]")
        if start != -1 and end != -1 and end > start:
            return s[start:end + 1].strip()

        return s


    def _parse_challenge_rows(self, raw: str) -> List[dict]:
        blob = self._extract_json_array_text(raw)
        if not blob:
            return []

        try:
            data = json.loads(blob)
        except Exception:
            return []

        if not isinstance(data, list):
            return []

        out: List[dict] = []
        seen = set()

        for item in data:
            if not isinstance(item, dict):
                continue

            skill_raw = _clean_text(str(item.get("skill", "")))
            difficulty_raw = _clean_text(str(item.get("difficulty", ""))).lower()
            why = _clean_text(str(item.get("why", "")))

            skill = SKILLS_5E_MAP.get(skill_raw.lower())
            dc = CHALLENGE_DC_BY_DIFFICULTY.get(difficulty_raw)
            difficulty = CHALLENGE_DIFFICULTY_LABELS.get(difficulty_raw)

            if not skill or not difficulty or dc is None or not why or skill in seen:
                continue

            out.append({
                "skill": skill,
                "difficulty": difficulty,
                "dc": dc,
                "why": why[:350],
            })
            seen.add(skill)

            if len(out) >= 3:
                break

        return out if len(out) == 3 else []


    async def _openai_challenge(self, *, topic: str) -> Optional[List[dict]]:
        topic = _clean_text(topic)
        if not topic:
            return None

        system = (
            "You help a Dungeon Master create D&D 5E skill challenge prompts.\n"
            "Choose exactly 3 relevant skills for the scenario.\n"
            "Only use skills from the allowed list.\n"
            "Prefer the most direct, first-order skills for solving the problem.\n"
            "Do NOT choose gimmick, entertainment, or indirect distraction skills unless the scenario explicitly calls for them.\n"
            "For social conflict, prioritize de-escalation, sincerity, reading intent, empathy, negotiation, or careful observation.\n"
            "Avoid force, coercion, intimidation, or physical-control skills unless the topic clearly involves danger, violence, pursuit, restraint, or emergency rescue.\n"
            "Silently rank candidate skills by direct usefulness, then return the best 3.\n"
            "Return ONLY JSON.\n"
        )

        allowed = ", ".join(SKILLS_5E)

        user = (
            f"Scenario topic: {topic}\n\n"
            f"Allowed skills: {allowed}\n\n"
            "Allowed difficulty labels:\n"
            "- Very easy\n"
            "- Easy\n"
            "- Medium\n"
            "- Hard\n"
            "- Very hard\n"
            "- Nearly impossible\n\n"
            "Return exactly a JSON array with 3 objects in this format:\n"
            "[\n"
            '  {"skill": "Persuasion", "difficulty": "Hard", "why": "One or two sentences."},\n'
            '  {"skill": "Insight", "difficulty": "Easy", "why": "One or two sentences."},\n'
            '  {"skill": "Perception", "difficulty": "Very easy", "why": "One or two sentences."}\n'
            "]\n\n"
            "Rules:\n"
            "- Pick exactly 3 different skills.\n"
            "- Only use skills from the allowed list.\n"
            "- Base difficulty on opposition, urgency, risk, complexity, and consequences of failure.\n"
            "- Only use the allowed difficulty labels exactly as written.\n"
            "- At least 2 of the 3 should be obvious first-line choices for the situation.\n"
            "- Avoid novelty picks unless they are clearly central to the topic.\n"
            "- Prefer what a DM would most naturally call for at the table.\n"
            "- Use the full difficulty scale naturally when appropriate.\n"
            "- Do not default to one Easy, one Medium, and one Hard.\n"
            "- Very easy and Nearly impossible are rare, but use them when the scenario truly fits.\n"
            "- Choose the difficulty for each task independently based on how hard that specific action would be.\n"
            "- No markdown.\n"
            "- No code fences.\n"
            "- No text before or after the JSON.\n"
        )

        model = os.getenv("CHALLENGE_MODEL", os.getenv("LORE_MODEL", "gpt-4o-mini"))
        temp = float(os.getenv("CHALLENGE_TEMPERATURE", "0.45"))
        max_out = int(os.getenv("CHALLENGE_MAX_OUTPUT_TOKENS", "260"))

        raw = await self._openai_chat_text(
            system=system,
            user=user,
            max_out=max_out,
            temperature=temp,
            model=model,
        )

        rows = self._parse_challenge_rows(raw or "")
        if rows:
            penalties = sum(self._challenge_skill_penalty(topic, r["skill"]) for r in rows)
            penalties += self._challenge_difficulty_penalty(rows)
            if penalties == 0:
                return rows

        retry_user = user + (
            "\nIMPORTANT: Your previous reply was too conservative, too indirect, or tonally wrong."
            " Choose more natural, table-appropriate skills."
            " Also vary the difficulty labels based on the specific action, not the overall scene."
            " Do not cluster everything around Easy and Medium if a harder or easier rating fits better."
            " Return valid JSON only."
        )

        raw = await self._openai_chat_text(
            system=system,
            user=retry_user,
            max_out=max_out,
            temperature=0.35,
            model=model,
        )

        rows = self._parse_challenge_rows(raw or "")
        if rows:
            penalties = sum(self._challenge_skill_penalty(topic, r["skill"]) for r in rows)
            penalties += self._challenge_difficulty_penalty(rows)
            if penalties == 0:
                return rows

        return rows or None


    def _challenge_difficulty_penalty(self, rows: List[dict]) -> int:
        if not rows:
            return 1

        diffs = [str(r.get("difficulty", "")).lower() for r in rows]
        uniq = set(diffs)

        penalty = 0

        # Penalize all-middle outputs.
        if all(d in ("easy", "medium") for d in diffs):
            penalty += 2

        # Penalize no spread at all.
        if len(uniq) <= 1:
            penalty += 2

        # Mild penalty if all three are from the same narrow band.
        if uniq.issubset({"easy", "medium", "hard"}) and len(uniq) <= 2:
            penalty += 1

        return penalty
        
    def _challenge_skill_penalty(self, topic: str, skill: str) -> int:
        t = _clean_text(topic).lower()
        s = skill.lower()

        def has_any(*words: str) -> bool:
            return any(w in t for w in words)

        family_care = has_any(
            "adopt", "adoption", "child", "kid", "baby", "infant", "orphan",
            "parent", "guardian", "foster", "family", "caretaker", "care for",
            "raise", "comfort", "reassure", "earn trust", "bond with"
        )

        force_context = has_any(
            "fight", "brawl", "drag", "haul", "restrain", "wrestle", "chase",
            "pursue", "flee", "escape", "riot", "rowdy", "violent", "attack",
            "kidnap", "rescue", "burning", "fire", "collapse", "danger", "emergency"
        )

        performance_context = has_any(
            "perform", "show", "sing", "dance", "talent", "play", "entertain", "toast", "speech"
        )
        arcana_context = has_any(
            "magic", "spell", "arcane", "ritual", "glyph", "curse", "enchant"
        )
        religion_context = has_any(
            "temple", "priest", "holy", "divine", "faith", "ritual", "undead"
        )
        history_context = has_any(
            "legend", "historical", "ancient", "heirloom", "lineage", "record"
        )
        nature_context = has_any(
            "forest", "wild", "beast", "plant", "natural", "terrain"
        )
        medicine_context = has_any(
            "wound", "injury", "poison", "disease", "stabilize", "ill", "sick", "heal"
        )

        # Soft/family topics should strongly avoid force-flavored skills unless the topic itself is dangerous.
        if family_care and not force_context:
            if s == "athletics":
                return 3
            if s == "intimidation":
                return 3
            if s == "acrobatics":
                return 2
            if s == "sleight of hand":
                return 2

        # Existing niche-skill penalties, but a little more explicit.
        if s == "performance" and not performance_context:
            return 1
        if s == "arcana" and not arcana_context:
            return 1
        if s == "religion" and not religion_context:
            return 1
        if s == "history" and not history_context:
            return 1
        if s == "nature" and not nature_context:
            return 1
        if s == "medicine" and not medicine_context:
            return 1

        return 0
        
        
    async def _openai_persona_sheet(self, *, name: str, excerpts: List[sqlite3.Row]) -> Optional[str]:
        max_excerpt_chars = int(os.getenv("LORE_OPENAI_EXCERPT_CHARS", "420"))
        max_sources_chars = int(os.getenv("LORE_PERSONA_SOURCES_CHARS", "45000"))

        blocks = []
        total = 0
        for i, r in enumerate(excerpts, start=1):
            who = (r["speaker_name"] or "?")
            txt = _clean_text(r["content"] or "")
            if len(txt) > max_excerpt_chars:
                txt = txt[:max_excerpt_chars] + "…"
            b = f"[S{i}] {who}: {txt}"
            if blocks and (total + len(b) > max_sources_chars):
                break
            blocks.append(b)
            total += len(b)

        system = (
            "You are a character voice coach + lore continuity editor.\n"
            "Build a compact persona sheet for roleplay from ONLY the sources.\n"
            "If something isn’t supported, mark it as unknown.\n"
            "Do NOT write citations; this is a cached persona.\n"
        )
        user = (
            f"Character: {name}\n\n"
            "Output a persona sheet in this exact format:\n"
            "VOICE (2–4 bullets)\n"
            "MANNERISMS (2–6 bullets)\n"
            "VALUES / GOALS (2–6 bullets)\n"
            "RELATIONSHIPS (2–8 bullets; only if supported)\n"
            "TABOOS / DON'TS (1–6 bullets)\n"
            "CATCHPHRASES / QUOTES (3–8 short quotes; only verbatim lines)\n\n"
            "SOURCES:\n" + "\n".join(blocks)
        )
        max_out = int(os.getenv("LORE_PERSONA_MAX_OUTPUT_TOKENS", "900"))
        return await self._openai_chat_text(system=system, user=user, max_out=max_out, temperature=0.25)

    async def _openai_rp(self, *, name, persona, prompt, excerpts, scene, directives, stats: Optional[dict] = None):
        max_excerpt_chars = int(os.getenv("LORE_OPENAI_EXCERPT_CHARS", "420"))
        max_sources_chars = int(os.getenv("LORE_RP_SOURCES_CHARS", "45000"))

        blocks: List[str] = []

        total = 0
        for i, r in enumerate(excerpts, start=1):
            who = (r["speaker_name"] or "?")
            txt = _clean_text(r["content"] or "")
            if len(txt) > max_excerpt_chars:
                txt = txt[:max_excerpt_chars] + "…"
            b = f"[S{i}] {who}: {txt}"
            if blocks and (total + len(b) > max_sources_chars):
                break
            blocks.append(b)
            total += len(b)
        # Voice samples: grab a few short lines spoken by THIS character (helps cadence a lot)
        voice_n = int(os.getenv("LORE_RP_VOICE_SAMPLES", "6"))
        voice_samples: List[str] = []
        if voice_n > 0:
            name_l = (name or "").lower().strip()
            for r in (excerpts or []):
                who_l = (r["speaker_name"] or "").lower()
                if name_l and (name_l in who_l or who_l in name_l):
                    t = _clean_text(r["content"] or "")
                    if not t:
                        continue
                    if len(t) > 180:
                        t = t[:180] + "…"
                    voice_samples.append(t)
                    if len(voice_samples) >= voice_n:
                        break

        system = (
            f"You are roleplaying as {name} in a Discord RP server.\n"
            "Stay strictly in-character.\n"
            "Do not invent new lore facts; if unsure, respond in-character with uncertainty (no follow-up questions).\n"
            "Do not mention 'sources' or citations unless the user explicitly asks.\n"
            "Keep replies Discord-friendly (no huge walls of text unless MODE asks for it).\n\n"
            "PERSONA SHEET:\n"
            f"{persona}\n\n"
            "STYLE (always follow):\n"
            "- Write like a real Discord RP log: sharp, specific, conversational.\n"
            "- Prefer dialogue over narration.\n"
            "- Avoid RP-app mannerisms / stage directions (e.g. 'smirks', 'grins', 'chuckles', 'paces', 'tilts head').\n"
            "- Do NOT narrate your own facial expressions/body language unless it materially changes the scene.\n"
            "- If an action beat is truly needed, use at most ONE short italic clause (<= 6 words).\n"
            "- Do not prepend headers like '🎭 Name' or 'Name:' in the reply.\n\n"
        )

        # Default RP style: sound like a real Discord log, not a roleplay app.
        system += (
            "STYLE (always follow):\n"
            "- Write like a human in a Discord RP log: sharp, specific, conversational.\n"
            "- Prefer dialogue over narration.\n"
            "- Avoid RP-app mannerisms / stage directions (e.g. 'smirks', 'grins', 'chuckles', 'paces', 'tilts head').\n"
            "- Do NOT narrate your own facial expressions/body language unless it materially changes the scene.\n"
            "- If an action beat is truly needed, use at most ONE short italic clause (<= 6 words).\n"
            "- Do not prepend headers like '🎭 Name' or 'Name:' in the reply.\n\n"
        )
        dlow = (directives or "").lower()
        nsfw_allowed = int(os.getenv("LORE_RP_ALLOW_NSFW", "0")) != 0
        nsfw_allowed = nsfw_allowed or (("nsfw:" in dlow) and any(x in dlow for x in ("allowed", "allow", "on", "true", "yes")))

        if nsfw_allowed:
            system += (
                "NSFW is allowed ONLY between consenting adults. "
                "Never depict minors or non-consensual/coerced sexual content. "
                "If the user tries to steer there, refuse and fade-to-black.\n\n"
            )
        else:
            system += "No explicit sexual content. If it comes up, fade-to-black.\n\n"
        user = ""
        if voice_samples:
            user += (
                "VOICE SAMPLES (verbatim; imitate tone/cadence, not facts):\n- "
                + "\n- ".join(voice_samples)
                + "\n\n"
            )

        if directives.strip():
            system += "SCENE DIRECTIVES (designer controls; follow them unless they contradict the persona):\n" + directives.strip() + "\n\n"

        if scene.strip():
            user += (
                "SCENE MEMORY (recent conversation in *this* channel/thread; current context, not canon-lore):\n"
                f"{scene.strip()}\n\n"
            )

        user += (
            f"User prompt: {prompt}\n\n"
            "SOURCES (lore excerpts for grounding; do not mention 'sources' in your reply):\n"
            + "\n".join(blocks)
        )

        # Optional mode shaping (cheap but effective)
        mode = (directives or "").lower()
        if "mode:" in mode and "adventure" in mode:
            user += "\n\nOutput: 2–5 paragraphs max + end with 2–4 short choices the user can pick from."
        elif "mode:" in mode and "script" in mode:
            user += "\n\nOutput: script-style with dialogue and short bracketed actions."
        elif "mode:" in mode and "chat" in mode:
            dyn_len = int(os.getenv("LORE_RP_DYNAMIC_LEN", "1") or "1") != 0
            min_chars = int(os.getenv("LORE_RP_CHAT_MIN_CHARS", "120") or "120")
            max_chars = int(os.getenv("LORE_RP_CHAT_MAX_CHARS", "1400") or "1400")

            # --- per-persona dynamic length (wide scale, still persona-bounded) ---
            eff_min, eff_max = min_chars, max_chars
            p25 = p50 = p75 = p90 = 0
            if isinstance(stats, dict):
                try:
                    p25 = int(stats.get("self_len_p25") or 0)
                    p50 = int(stats.get("self_len_p50") or 0)
                    p75 = int(stats.get("self_len_p75") or 0)
                    p90 = int(stats.get("self_len_p90") or 0)
                except Exception:
                    p25 = p50 = p75 = p90 = 0

            # Don’t let a global floor (e.g. 120) force terse characters to be wordy.
            if p25 > 0:
                eff_min = max(60, min(eff_min, int(p25 * 0.85)))
            if p90 > 0:
                eff_max = min(eff_max, int(p90 * 1.35 + 80))

            # Infer desired “depth” from the prompt (no new env knobs needed).
            v = 0.65  # default: slightly longer than median
            if dyn_len:
                q = _clean_text(prompt).lower()
                qlen = len(q)
                qs = q.count("?")
                deep = any(k in q for k in (
                    "tell me about", "explain", "break down", "walk me through",
                    "what do you know", "in detail", "details", "how does", "why does", "what happened"
                ))
                brief = any(k in q for k in ("tldr", "brief", "short", "quick"))
                v = 0.15 + min(1.0, qlen / 220.0) * 0.50 + min(0.20, qs * 0.06)
                if deep:
                    v += 0.20
                if brief:
                    v -= 0.25
                v = max(0.0, min(1.0, v))

            target_chars = 0
            if dyn_len and (p25 or p50):
                lo = p25 or p50
                hi = p90 or p75 or p50
                if hi < lo:
                    lo, hi = hi, lo
                target_chars = int(round(lo + (hi - lo) * v))
            elif dyn_len:
                # fallback to your old behavior if stats are missing
                target_chars = int(p75 or p50 or 0)

            if target_chars > 0:
                target_chars = max(eff_min, min(eff_max, target_chars))

                # crude but effective tiering for "Discord vibe"
                if target_chars <= 240:
                    smin, smax = 1, 3
                elif target_chars <= 480:
                    smin, smax = 2, 6
                elif target_chars <= 800:
                    smin, smax = 3, 8
                else:
                    smin, smax = 4, 10

                user += (
                    f"\n\nOutput: ONE Discord post. Aim ~{target_chars} characters (about {smin}–{smax} sentences).\n"
                    "- Sound like the character’s real voice from the excerpts (cadence, profanity level, cruelty/humor).\n"
                    "- If the user asked you a question: answer it directly. DO NOT ask a follow-up question.\n"
                    "- If the user did NOT ask a question: you may ask ONE short question occasionally, but most replies should end as a statement.\n"
                    "- Avoid generic follow-ups like “Why do you ask?” / “What brought this up?”\n"
                    "- If the user asks about lore (place/person/event/thing), answer with 2–5 concrete facts drawn from SOURCES (paraphrase; no citations).\n"

                    
                    "- No stage-direction filler (no 'smirks', 'paces', 'contemplating', etc). No theatrical asides.\n"
                    "- Prefer lines of dialogue in quotes; keep narration minimal.\n"
                    "- If you include an action beat, ONE short italic clause (<= 6 words) and only if it changes the moment.\n"
                    "- No recaps, no essay tone, no meta talk. If you need a missing detail, make a reasonable in-character assumption and continue.\n"

                )
                if target_chars >= 650:
                    user += "- Paragraph breaks allowed (1–2), still ONE post.\n"
            else:
                # fallback (your current behavior)
                user += (
                    "\n\nOutput: ONE Discord post. 2–6 sentences max.\n"
                    "- Sound like the character’s real voice from the excerpts (cadence, profanity level, cruelty/humor).\n"
                    "- No stage-direction filler (no 'smirks', 'paces', 'contemplating', etc). No theatrical asides.\n"
                    "- Prefer lines of dialogue in quotes; keep narration minimal.\n"
                    "- If you include an action beat, ONE short italic clause (<= 6 words) and only if it changes the moment.\n"
                    "- If the user asks about lore (place/person/event/thing), answer with 2–5 concrete facts drawn from SOURCES (paraphrase; no citations).\n"
                    "- Do NOT ask follow-up questions.\n"
                    "- No recaps, no essay tone, no meta talk. If you need a missing detail, make a reasonable in-character assumption and continue.\n"

                )

        base_max_out = int(os.getenv("LORE_RP_MAX_OUTPUT_TOKENS", "700"))
        max_out = base_max_out

        # If we computed a target length for chat mode, set a per-persona token budget (still capped by base_max_out)
        if ("mode:" in mode and "chat" in mode) and target_chars > 0:
            # ~3.8 chars/token + some headroom
            est = int(target_chars / 3.8) + 80
            max_out = max(120, min(base_max_out, est))
        temp = float(os.getenv("LORE_RP_TEMPERATURE", "0.85"))
        out = await self._openai_chat_text(system=system, user=user, max_out=max_out, temperature=temp)

        # Safety-net cleanup for chat mode (prevents RP-app "smirks" openings and name headers).
        if out and ("mode:" in mode and "chat" in mode) and int(os.getenv("LORE_RP_CHAT_CLEANUP", "1")) != 0:
            cleaned = (out or "").strip()
            cleaned = re.sub(r"(?s)^\s*🎭\s*[^\n]{1,80}\n+", "", cleaned).strip()
            cleaned = re.sub(rf"(?im)^\s*{re.escape(name)}\s*[:\-]\s*", "", cleaned).strip()
            cleaned = re.sub(r"(?s)^\s*\*[^*\n]{1,80}\*\s*", "", cleaned).strip()
            # Kill inline RP-app stage directions like "...? chuckles What..."
            cleaned = re.sub(r"(?i)([?.!]\s*)(smirks|grins|chuckles|laughs|paces|nods|shrugs)\s+(?=[A-Z])", r"\1", cleaned).strip()
            # No-question mode in chat: convert '?' to '.' (prevents botty follow-ups)
            cleaned = cleaned.replace("?", ".")
            if cleaned:
                out = cleaned

        return out
        
    def _subject_from_question(self, question: str) -> str:
        q = _clean_text(question or "")
        if not q:
            return ""
        for pfx in ("who is ", "who's ", "tell me about ", "describe "):
            if q.lower().startswith(pfx):
                return _clean_text(q[len(pfx):])
        return q

    def _canon_claim_text(self, s: str) -> str:
        s = _clean_text(s or "").lower()
        s = s.replace("’", "'").replace("‘", "'")
        s = re.sub(r"[^a-z0-9'\s]+", " ", s)
        s = re.sub(r"\s+", " ", s).strip()
        return s

    def _claim_entity_variants(self, ent: str) -> List[str]:
        raw = _clean_text(ent or "")
        if not raw:
            return []

        cands: List[str] = []
        seen = set()

        def add(x: str) -> None:
            x = self._canon_claim_text(x)
            if not x or x in seen:
                return
            seen.add(x)
            cands.append(x)

        add(raw)
        no_article = re.sub(r"^(?:the|a|an)\s+", "", raw, flags=re.I)
        add(no_article)
        no_title = re.sub(
            r"^(?:lord|lady|sir|dame|captain|commander|queen|king|prince|princess|field marshal|marshal|battle-capsarii|auctor)\s+",
            "",
            no_article,
            flags=re.I,
        )
        add(no_title)

        parts = no_title.split()
        if len(parts) == 1:
            add(parts[0])

        return cands

    def _claim_patterns_for_line(self, line: str) -> List[str]:
        ll = self._canon_claim_text(line)
        pats: List[str] = []

        if re.search(r"\b(lead(?:er|ership|s)?|led by|auctor|head|commander|marshal|captain|ruler)\b", ll):
            pats.extend([
                r"\blead(?:er|ership|s)?\b",
                r"\bled by\b",
                r"\bauctor\b",
                r"\bhead(?:ed)? by\b",
                r"\bcommander\b",
                r"\bmarshal\b",
                r"\bcaptain\b",
                r"\bruler\b",
                r"\bpassed leadership to\b",
            ])

        if re.search(r"\b(govern(?:s|ed|ing)?|rule(?:s|d)?|control(?:s|led)?|de facto government|ruling entity|domain)\b", ll):
            pats.extend([
                r"\bgovern(?:s|ed|ing)?\b",
                r"\brule(?:s|d)?\b",
                r"\bcontrol(?:s|led)?\b",
                r"\bde facto government\b",
                r"\bruling entity\b",
                r"\bdomain\b",
                r"\bunder .* domain\b",
            ])

        if re.search(r"\b(member of|members of|part of|affiliated with|associated with|allied with|served with|served in|belongs to|employed by)\b", ll):
            pats.extend([
                r"\bmember of\b",
                r"\bmembers of\b",
                r"\bpart of\b",
                r"\baffiliated with\b",
                r"\bassociated with\b",
                r"\ballied with\b",
                r"\bserved with\b",
                r"\bserved in\b",
                r"\bbelongs to\b",
                r"\bemployed by\b",
            ])

        out: List[str] = []
        seen = set()
        for p in pats:
            if p not in seen:
                seen.add(p)
                out.append(p)
        return out

    def _claim_entities_from_line(self, line: str, *, question: str = "") -> List[str]:
        text = line.strip().lstrip("-•* ").strip()
        if not text or text.endswith(":"):
            return []

        ents: List[str] = []
        seen = set()

        def add(ent: str) -> None:
            ent = _clean_text(ent or "")
            if not ent:
                return
            low = ent.lower()
            bad_starts = {
                "one-sentence", "what", "current", "recent", "key", "damage", "important",
                "open", "major", "signature", "voice", "roles", "public", "recurring",
                "arc", "roleplay", "leadership", "territory", "origin", "timeline",
            }
            first = re.split(r"\s+", low)[0]
            if first in bad_starts:
                return
            if low not in seen:
                seen.add(low)
                ents.append(ent)

        patterns = [
            r"\b(?:Order|Church|Guild|Company|Legion|Clan|Temple|Council|Academy|College)\s+of\s+[A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){0,4}\b",
            r"\b(?:Explorer Corps|Knights of [A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){0,3})\b",
            r"\b(?:House|Lord|Lady|Sir|Dame|Queen|King|Prince|Princess|Auctor|Captain|Commander|Marshal|Field Marshal|Battle-Capsarii)\s+[A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){0,3}\b",
            r"\b[A-Z][A-Za-z'’\-]+(?:\s+[A-Z][A-Za-z'’\-]+){1,4}\b",
            r"\b[A-Z][A-Za-z'’\-]{4,}\b(?:'s)?",
        ]
        for pat in patterns:
            for m in re.finditer(pat, text):
                add(m.group(0).rstrip("'s"))

        subj = self._subject_from_question(question)
        if subj:
            add(subj)

        return ents[:5]

    def _source_supports_claim_line(self, src_text: str, entities: List[str], patterns: List[str]) -> bool:
        norm = self._canon_claim_text(src_text)
        if not norm or len(entities) < 2 or not patterns:
            return False

        matched = 0
        for ent in entities:
            ok = False
            for v in self._claim_entity_variants(ent):
                if not v:
                    continue
                if re.search(rf"\b{re.escape(v)}\b", norm):
                    ok = True
                    break
            if ok:
                matched += 1
            if matched >= 2:
                break

        if matched < 2:
            return False

        return any(re.search(p, norm) for p in patterns)

    def _prune_unsupported_claim_lines(self, text: str, *, question: str, excerpts: List[sqlite3.Row]) -> str:
        if not text:
            return text

        debug = int(os.getenv("LORE_OPENAI_DEBUG", "0") or "0") != 0
        out_lines: List[str] = []
        for raw_line in text.splitlines():
            line = raw_line.strip()
            if not line:
                out_lines.append(raw_line)
                continue
            if line.endswith(":"):
                out_lines.append(raw_line)
                continue

            patterns = self._claim_patterns_for_line(line)
            if not patterns:
                out_lines.append(raw_line)
                continue

            entities = self._claim_entities_from_line(line, question=question)
            if len(entities) < 2:
                out_lines.append(raw_line)
                continue

            supported = False
            for r in excerpts[:140]:
                if self._source_supports_claim_line(_clean_text(r["content"] or ""), entities, patterns):
                    supported = True
                    break

            if supported:
                out_lines.append(raw_line)
            else:
                if debug:
                    print(f"[lore] pruned unsupported claim line: {line}")
                continue

        cleaned = "\n".join(out_lines)
        cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()
        return cleaned

    async def _openai_answer(self, *, question: str, excerpts: List[sqlite3.Row], answer_profile: Optional[str] = None) -> Optional[str]:
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            return None

        try:
            from openai import OpenAI  # type: ignore
        except Exception as e:
            print(f"[lore] OpenAI failure: {type(e).__name__}: {e}")
            return None

        model = os.getenv("LORE_MODEL", "gpt-4o-mini")
        client = OpenAI(api_key=api_key)

        max_excerpt_chars = int(os.getenv("LORE_OPENAI_EXCERPT_CHARS", "420"))
        max_sources_chars = int(os.getenv("LORE_OPENAI_SOURCES_CHARS", "60000"))

        answer_profile = answer_profile or self._infer_answer_profile(question, excerpts)
        is_bio = answer_profile == "bio"
        is_org = answer_profile == "org"
        is_status = answer_profile == "status"

        # For long messages, don't just take the first N chars (that often misses the "important line" later).
        # Instead: for BIO queries, prefer windows near death/marriage/chosen/etc. keywords, otherwise head+tail.
        focus_terms: List[str] = []
        if is_bio:
            ql = (question or "").strip()
            # crude subject extraction (matches your lore_ask helper well enough)
            subj = ql
            for pfx in ("who is ", "who's ", "tell me about ", "describe "):
                if subj.lower().startswith(pfx):
                    subj = subj[len(pfx):].strip()
                    break
            subj_l = _clean_text(subj).lower().strip()
            if subj_l:
                focus_terms.extend([subj_l])
                focus_terms.extend(self._bio_alias_terms(subj_l))

            # Relationship/status terms (death/marriage/etc)
            focus_terms.extend([t.strip().lower() for t in (os.getenv(
                "LORE_BIO_REL_TERMS",
                "husband|wife|married|widow|widower|fiance|betrothed|son|daughter|child|dead|died|slain|killed|funeral"
            ).split("|")) if t.strip()])

            # Extra "important fact" terms you care about (chosen/battles/founded orgs/etc)
            focus_terms.extend([t.strip().lower() for t in (os.getenv("LORE_BIO_IMPORT_TERMS", "").split("|")) if t.strip()])

            # de-dupe, keep order
            seen_ft = set()
            focus_terms = [t for t in focus_terms if not (t in seen_ft or seen_ft.add(t))]

        def _trim_for_model(txt: str) -> str:
            if not txt:
                return ""
            if len(txt) <= max_excerpt_chars:
                return txt

            tl = txt.lower()
            hits: List[int] = []
            for t in focus_terms:
                if not t:
                    continue
                pos = tl.rfind(t)  # last match tends to capture "later arc" / death lines
                if pos != -1:
                    hits.append(pos)

            if hits:
                pos = max(hits)
                half = max(80, int(max_excerpt_chars * 0.5))
                start = max(0, pos - half)
                end = min(len(txt), start + max_excerpt_chars)
                start = max(0, end - max_excerpt_chars)
                return ("…" if start > 0 else "") + txt[start:end] + ("…" if end < len(txt) else "")

            # fallback: keep both head + tail (better than head-only for long RP blocks)
            head_n = int(max_excerpt_chars * 0.6)
            tail_n = max_excerpt_chars - head_n - 3
            return txt[:head_n].rstrip() + "\n…\n" + txt[-tail_n:].lstrip()

        context_lines: List[str] = []
        total_chars = 0
        if is_bio:
            subj = question[7:].strip() if question.lower().startswith("who is ") else question
            excerpts = self._bio_priority_rows(excerpts, subj)
        elif is_org:
            excerpts = self._org_priority_rows(excerpts, question)
        elif is_status:
            excerpts = self._status_priority_rows(excerpts, question)
        for i, r in enumerate(excerpts, start=1):
            sid = f"S{i}"
            who = (r["speaker_name"] or "?")
            url = (r["jump_url"] or "")
            txt = _trim_for_model(_clean_text(r["content"] or ""))
            block = f"[{sid}] {who}\n{txt}\nURL: {url}"
            if context_lines and (total_chars + len(block) > max_sources_chars):
                break
            context_lines.append(block)
            total_chars += len(block)

        allow_unlabeled_arcs = int(os.getenv("LORE_BIO_ALLOW_UNLABELED_ARCS", "0")) != 0
        powers_named_only = int(os.getenv("LORE_BIO_POWERS_NAMED_ONLY", "0")) != 0
        if is_bio:
            arc_line = (
                "Major arcs / events (0–10 bullets; include named events when the name appears verbatim; "
                "otherwise you MAY include 'Unlabeled arc:' summaries grounded in sources; if sources explicitly show a major turning point (death/perished/slain/killed, marriage, promotion, exile), include it as a bullet)\n"
                if allow_unlabeled_arcs else
                "Major arcs / events (0–10 bullets; include named events when the name appears verbatim; "
                "for unnamed recurring storylines, describe them as normal bullets—no 'Unlabeled arc:' label and no invented titles; if sources explicitly show a major turning point (death/perished/slain/killed, marriage, promotion, exile), include it as a bullet)\n"
            )

            power_line = (
                "Signature abilities / powers (0–10 bullets; ONLY include if the ability/power name appears verbatim in sources; "
                "otherwise write '(none explicitly named in sources)')\n"
                if powers_named_only else
                "Signature abilities / powers / techniques (0–10 bullets; include named powers when named; otherwise include "
                "'Described capability:' bullets when the sources clearly describe an ability/technique/item-effect in action)\n"
            )

            fmt = (
                "One-sentence identity / headline\n"
                + arc_line
                + power_line
                + "Voice / mannerisms (0–8 bullets; prefer 1–3 short direct quotes when available)\n"
                + "Roles & affiliations (0–8 bullets; if none, write '(none explicitly stated in sources)')\n"
                + "Key relationships (0–10 bullets; prioritize family, spouse/ex-spouse, mentor/student, employer/agent/spy, loyal allies, major rivals; if unclear, write '(not clearly stated in sources)')\n"
                + "For 'Key relationships', prefer durable relationship facts (family, marriage, ex-spouse, mentor, spy-handler, sworn loyalty, rivalry) over one-off conversations.\n"
                + "Public reputation / quirks (0–6 bullets; do not over-focus on one-off crude jokes)\n"
                + "Recurring scenes & themes (0–12 bullets)\n"
                + "Arc evolution over time (3–6 bullets, earliest → latest)\n"
                + "Roleplay hooks (2–6 bullets; what they want / what they avoid)\n"
                + "Open questions / uncertainties (0–6 bullets)\n"
            )
        elif is_org:
            fmt = (
                "One-sentence organization status / headline\n"
                "What it is / role (2–6 bullets; what kind of organization it is and what it does)\n"
                "Current posture / latest confirmed (3–8 bullets; directly answer the question first; lead with the newest clearly supported facts)\n"
                "Recent developments / changes (3–8 bullets; newest → older)\n"
                "Leadership / members / factions (2–10 bullets)\n"
                "Territory / operations / influence (2–8 bullets; if unclear, write '(not clearly stated in sources)')\n"
                "Important historical context (2–6 bullets; only what helps explain the current posture)\n"
                "Open questions / uncertainties (0–6 bullets)\n"
            )
        elif is_status:
            fmt = (
                "One-sentence current status / headline\n"
                "Current state / latest confirmed (3–8 bullets; directly answer the question first; lead with the newest clearly supported facts)\n"
                "Recent developments / changes (3–8 bullets; newest → older)\n"
                "Key people / factions / control (2–8 bullets)\n"
                "Damage / condition / operational state (0–6 bullets; if unclear, write '(not clearly stated in sources)')\n"
                "Important context (2–6 bullets; only what helps explain the current state)\n"
                "Open questions / uncertainties (0–6 bullets)\n"
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
            )

        # right before prompt construction
        bio_arc_rule = ""
        bio_power_rule = ""

        if is_bio:
            bio_arc_rule = (
                "If the sources clearly describe a recurring storyline but it has no explicit name, you may include it as "
                "'Unlabeled arc: ...' (do not fabricate a proper-noun title).\n"
                if allow_unlabeled_arcs else
                "If the sources clearly describe a recurring storyline but it has no explicit name, describe it plainly as a normal bullet "
                "(do NOT write 'Unlabeled arc:' and do NOT invent a title).\n"
            )

            bio_power_rule = (
                "For 'Signature abilities / powers', only list abilities/powers whose names appear verbatim in sources. "
                "If none, write '(none explicitly named in sources)'.\n"
                if powers_named_only else
                "For 'Signature abilities / powers / techniques': include named powers when named; otherwise you MAY include "
                "'Described capability:' bullets when the sources clearly describe an ability/technique/item-effect in action. "
            )

        prompt = (
            "You are the lore historian for a roleplay Discord server.\n"
            "Use ONLY the sources below. If something isn't supported, say you can't confirm it.\n"
            "Include notable quirks, self-promotion, rumors, and reputation — but only when supported by sources.\n"
            "DO NOT write filler bullets. If you can't name the thing or describe a concrete act, omit it.\n"
            "If sources contain explicit sexual content/harassment, summarize it neutrally and briefly (max 1 bullet) and avoid explicit wording/quotes.\n"
            "If a section is weakly supported, prefer '(none explicitly stated in sources)' over guessing.\n"
            "Never output empty citation placeholders such as (), (,), (,,), or blank brackets.\n"
            "If citing sources inline, use only S# references like S1 or [S1]. If you do not have a citation marker, omit it entirely.\n"
            "For sections with modest support, prefer fewer concrete bullets over placeholder text.\n"
            "When a character has many relevant excerpts, be generously specific: include supported scenes, conflicts, travel, affiliations, recurring interactions, and distinctive behavior.\n"
            "If citing sources, use only S# references like S1 or [S1]; never output empty parentheses ().\n"
            "For topics/places/events: prioritize concrete details (who/what/where/when) and cite them.\n"
            "For people: cover their arc across time (early → recent). Do not let first-introduction snippets dominate if later excerpts show clearer, more consequential actions.\n"
            "For people with changing status over time, prefer later status-changing facts over earlier superseded descriptions (e.g. healing, faction shifts, major meetings, named battles).\n"
            "For people with enough evidence, include at least 1–3 later or more recent concrete actions/scenes somewhere in the bio when supported.\n"
            "When older and newer sources conflict, prefer the newest clearly supported state; treat older conflicting claims as historical or superseded unless later sources reaffirm them.\n"
            "For current-state or status questions, answer the present/latest confirmed situation before background.\n"
            "For organizations, describe current posture, leadership, membership, operations, and influence — not just immediate events. Do not frame an organization like a damaged location unless the sources are specifically about its condition.\n"
            "For bare topic lookups, if the sources indicate an ongoing situation, answer as a status overview rather than an encyclopedia entry.\n"
            + (bio_arc_rule if bio_arc_rule else "")
            + (bio_power_rule if bio_power_rule else "")
            + "If the question is about a specific person X, do NOT assign titles/roles/affiliations to X unless the excerpt explicitly says so about X (name in text OR speaker_name is X).\n"
            "If an excerpt looks like it is directed at someone else (reply/mention of another name), treat that fact as about that other person, not X.\n"
            "If unsure, put it under 'Open questions / uncertainties' instead of asserting it.\n"
            "For a person with many clearly relevant excerpts, give a partial bio from the supported facts you DO have; do not refuse the entire answer merely because some sections are weak.\n"
            "For 'Major arcs / named events': scan sources for explicit event-style names (e.g., 'Battle of X', 'Siege of Y', 'Expedition to Z') and include them ONLY if they appear verbatim.\n"
            "For named abilities/spells/attacks/items: explicitly list notable users mentioned in the sources.\n"
            "For organizations: explicitly list members mentioned in the sources with one-sentence identities.\n"
            "For leadership, control, government, membership, or affiliation claims, require an explicit link between the entities in the sources. Do not combine nearby facts from different entities into a new relationship.\n"
            "Never infer that a person leads organization A merely because that person leads some other group operating in the same place, or because organization A controls a place where that person is active.\n\n"
            "Write the final answer using this FORMAT exactly:\n"
            + fmt +
            "\nQUESTION: " + question + "\n\n"
            "SOURCES:\n" + "\n\n".join(context_lines)
        )

        try:
            max_out = int(os.getenv("LORE_ASK_MAX_OUTPUT_TOKENS", "2400"))
            resp = client.responses.create(model=model, input=prompt, max_output_tokens=max_out)
            text = getattr(resp, "output_text", None) or None
            if text:
                text = self._prune_unsupported_claim_lines(text, question=question, excerpts=excerpts)
            if text and is_bio and not allow_unlabeled_arcs:
                text = re.sub(r"(?mi)^\s*Unlabeled arc:\s*", "", text)
            return text
        except Exception as e:
            print(f"[lore] OpenAI failure: {type(e).__name__}: {e}")
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
                    
                    
    async def _reply_chunks(
        self,
        message: nextcord.Message,
        text: str,
        *,
        limit: int = 1900,
        mention_author: bool = False,
    ) -> None:
        """Reply/send long text safely in <=2000-char chunks."""
        text = text or ""
        first = True
        allowed = nextcord.AllowedMentions.none()

        while text:
            chunk = text[:limit]
            # try not to cut mid-line
            cut = chunk.rfind("\n")
            if cut > 400:
                chunk = chunk[:cut]

            if first:
                # Prefer replying to anchor the first chunk; fall back to send if reply fails.
                try:
                    await message.reply(chunk, mention_author=mention_author, allowed_mentions=allowed)
                except Exception:
                    await message.channel.send(chunk, allowed_mentions=allowed)
                first = False
            else:
                await message.channel.send(chunk, allowed_mentions=allowed)

            text = text[len(chunk):].lstrip("\n")
                    
    def _tidy_empty_citation_placeholders(self, text: str) -> str:
        """Remove citation shells left behind by source-marker cleanup.

        OpenAI sometimes writes things like ``(S1, S2)``. If we strip the
        source markers for a clean Discord answer, the punctuation shell can
        become ``()``, ``(,)``, or ``(,,)``. This keeps the final post tidy.
        """
        out = text or ""

        # Empty/garbage citation shells. Repeat because deleting one shell can
        # expose another in awkward model output.
        for _ in range(2):
            out = re.sub(r"\(\s*(?:,\s*)*\)", "", out)
            out = re.sub(r"\(\s*(?:\[\s*\]\s*)+\)", "", out)
            out = re.sub(r"\(\s*(?:,\s*)*(?:\[\s*\]\s*)*(?:,\s*)*\)", "", out)

        # Tidy spaces/punctuation without flattening intentional newlines.
        out = re.sub(r"[ \t]{2,}", " ", out)
        out = re.sub(r"[ \t]+([.,;:!?])", r"\1", out)
        out = re.sub(r"([.,;:!?])\s+([.,;:!?])", r"\1", out)
        out = re.sub(r",\s*,+", ", ", out)
        out = re.sub(r"\.\s*\.+", ".", out)
        out = re.sub(r"\n{3,}", "\n\n", out)
        return out.strip()

    def _strip_source_markers_for_public_answer(self, text: str) -> str:
        """Remove S# citation markers from the public !lore ask answer.

        This removes the whole parenthesized citation group first, so
        ``(S1, S2)`` does not become ``(,)`` after marker stripping.
        """
        out = text or ""

        src = r"(?:\[S\d+\]|(?<!\w)S\d+(?!\w))"

        # Parenthesized source-only groups: (S1), (S1, S2), ([S1], [S2]).
        out = re.sub(rf"\s*\(\s*{src}(?:\s*,?\s*{src})*\s*,?\s*\)", "", out)

        # Non-parenthesized source markers/lists.
        out = re.sub(rf"\s*{src}(?:\s*,?\s*{src})*", "", out)

        return self._tidy_empty_citation_placeholders(out)

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
        out = re.sub(r"(?<!\[)\bS(\d+)\b", r"[S\1]", out)

        # 3) Linkify [S#] -> [S#](jump_url)
        def repl(m: re.Match) -> str:
            sid = m.group(1)
            url = ref.get(sid)
            return f"[{sid}]({url})" if url else f"[{sid}]"

        out = re.sub(r"\[(S\d+)\]", repl, out)

        # 4) spacing between adjacent citations
        out = out.replace(")[", ") [")

        # 5) kill empty citation placeholders like (), (,), (,,), ([ ]), ([]), etc.
        out = self._tidy_empty_citation_placeholders(out)

        return out


    @commands.command(name="challenge")
    async def challenge(self, ctx: commands.Context, *, topic: str = ""):
        if not ctx.guild:
            return

        topic = _clean_text(topic)
        if not topic:
            await ctx.send("❌ Usage: `!challenge <topic>`")
            return

        rows = await self._openai_challenge(topic=topic)
        if not rows:
            await ctx.send("❌ I couldn't build a valid 5E skill challenge just now. Try again in a moment.")
            return

        embed = nextcord.Embed(
            title=f"🎲 Skill Challenge: {topic}",
            description="Three relevant 5E skills for this situation:",
            color=nextcord.Color.blurple(),
        )

        for i, row in enumerate(rows, start=1):
            embed.add_field(
                name=f"{i}. {row['skill']} — {row['difficulty']} (DC {row['dc']})",
                value=row["why"],
                inline=False,
            )

        embed.set_footer(text="Use these as prompt ideas for your scene or challenge.")
        await ctx.send(embed=embed)
                        
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
            if "transient" in p.error.lower():
                msg += f"\n⚠️ Last transient error seen: {p.error}"
            else:
                msg += f"\n⚠️ Last error: {p.error}"
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
        show_full = str(raw or "").lower().startswith("full:")
        if show_full:
            raw = raw.split(":", 1)[1].strip()
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
        raw_txt = _clean_text(msg.content or "")
        emb_txt = self._extract_embed_text(msg)
        combined = _clean_text((raw_txt + "\n" + emb_txt).strip())
        att_n = len(getattr(msg, "attachments", None) or [])

        empty_would_skip = (not combined) and (att_n == 0)

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
            f"Content chars: {len(combined)} (raw={len(raw_txt)} embeds={len(emb_txt)} atts={att_n})\n"
            + (f"➡️ NOTE: _archive_message() would skip this as `empty`.\n" if empty_would_skip else "")
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
        # inside lore_backfill (replace the task=asyncio.create_task(...) part)

        self._progress[gid] = BackfillProgress(running=True, started_ts=_now_ts())

        async def _run_and_notify():
            try:
                await self._run_backfill(ctx.guild, force_full=force_full)
            finally:
                p = self._progress.get(gid, BackfillProgress())
                msg = (
                    f"🔔 {ctx.author.mention} **Lore backfill complete!**\n"
                    f"Seen: {p.msgs_seen:,} | Saved: {p.msgs_saved:,}"
                )
                if p.error:
                    msg += f"\n⚠️ Last error: {p.error}"
                await ctx.send(
                    msg,
                    allowed_mentions=nextcord.AllowedMentions(users=True, roles=False, everyone=False),
                )

        task = asyncio.create_task(_run_and_notify())
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

        answer_profile = self._infer_answer_profile(question, rows)
        is_bio = answer_profile == "bio"
        is_org = answer_profile == "org"
        mode = self._intent_mode(question, answer_profile=answer_profile)
        bio_subject_hint = ""
        if is_bio:
            subj0 = question[7:].strip() if question.lower().startswith("who is ") else question
            subj0 = _clean_text(subj0)

            bio_role_terms = [t.strip().lower() for t in (os.getenv(
                "LORE_BIO_ROLE_TERMS",
                "auctor|leader|head|marshal|commander|ruler"
            ).split("|")) if t.strip()]
            role_set = set(bio_role_terms)

            toks = [t for t in re.findall(r"[a-z0-9']+", subj0.lower()) if t]
            toks_no_role = [t for t in toks if t not in role_set]

            subj_hint = " ".join(toks_no_role) if (toks_no_role and len(toks_no_role) < len(toks)) else subj0
            bio_subject_hint = subj_hint or subj0
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
            elif answer_profile == "org":
                rows_for_model = self._org_priority_rows(rows_for_model, question)
            elif answer_profile == "status":
                rows_for_model = self._status_priority_rows(rows_for_model, question)
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

        # Default: keep peek compact (no anchor-hit spam). Turn on via env.
        show_anchors = int(os.getenv("LORE_PEEK_SHOW_ANCHORS", "0")) != 0
        lines = [header]
        if show_anchors:

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
        wide_bio = bool(re.match(r"(?i)^\s*wide\s*:\s*", question))
        question = re.sub(r"(?i)^\s*wide\s*:\s*", "", question).strip()
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

        rows_raw, _stats, _anchors = await self._retrieve_rows_for_question(
            ctx.guild.id,
            question,
            fts_limit=fts_limit,
            speaker_limit=speaker_limit,
            seed_count=seed_count,
            win_before=win_before,
            win_after=win_after,
            max_excerpts=max_excerpts,
        )

        if not rows_raw:
            await ctx.send("I couldn't find anything relevant in the archive yet.")
            return

        # Strip query-control prefixes before sending to model
        question_for_model = re.sub(r"(?i)^(exact|phrase)\s*:\s*", "", question).strip()

        answer_profile = self._infer_answer_profile(question_for_model, rows_raw)
        is_bio = answer_profile == "bio"
        is_org = answer_profile == "org"
        mode = self._intent_mode(question, answer_profile=answer_profile)
        bio_subject_hint = ""
        if is_bio:
            subj0 = question[7:].strip() if question.lower().startswith("who is ") else question
            subj0 = _clean_text(subj0)

            bio_role_terms = [t.strip().lower() for t in (os.getenv(
                "LORE_BIO_ROLE_TERMS",
                "auctor|leader|head|marshal|commander|ruler"
            ).split("|")) if t.strip()]
            role_set = set(bio_role_terms)

            toks = [t for t in re.findall(r"[a-z0-9']+", subj0.lower()) if t]
            toks_no_role = [t for t in toks if t not in role_set]

            subj_hint = " ".join(toks_no_role) if (toks_no_role and len(toks_no_role) < len(toks)) else subj0
            bio_subject_hint = subj_hint or subj0
        # ---------- helpers ----------
        def _merge_rows(a, b):
            by_id = {int(r["message_id"]): r for r in (a or [])}
            for r in (b or []):
                by_id[int(r["message_id"])] = r
            return sorted(by_id.values(), key=lambda r: int(r["created_ts"]))

        def _bio_subject(q: str) -> str:
            ql = (q or "").strip()
            for pfx in ("who is ", "who's ", "tell me about ", "describe "):
                if ql.lower().startswith(pfx):
                    return ql[len(pfx):].strip()
            return ql.strip()

        # If a bare lookup resolves to a person, do a second retrieval pass with BIO-tuned anchoring.
        # This restores the stronger old persona/person retrieval without forcing places/orgs like
        # Bastion or Order Crimson Rain back into bio mode.
        if answer_profile == "bio" and not self._is_bio_question(question_for_model):
            bio_rows_raw, _bio_stats, bio_anchors = await self._retrieve_rows_for_question(
                ctx.guild.id,
                question,
                fts_limit=fts_limit,
                speaker_limit=speaker_limit,
                seed_count=seed_count,
                win_before=win_before,
                win_after=win_after,
                max_excerpts=max_excerpts,
                force_bio=True,
            )
            if bio_rows_raw:
                rows_raw = _merge_rows(rows_raw, bio_rows_raw)
                if bio_anchors:
                    _anchors = list(dict.fromkeys([*(_anchors or []), *bio_anchors]))

        # ---------- ALWAYS keep these ids if we can ----------
        must_keep_ids: set[int] = set()
        if _anchors:
            must_keep_ids.update(int(x) for x in _anchors)

        # Merge anchor rows into the raw pool EARLY (before any filtering/sampling)
        if _anchors:
            anchor_rows = await self._fetch_messages(ctx.guild.id, [int(x) for x in _anchors])
            rows_raw = _merge_rows(rows_raw, anchor_rows)

        subj = ""
        subj_l = ""
        focused_threads: set[int] = set()
        focused_channels: set[int] = set()

        # ---------- BIO GUARD (RUNS ON RAW ROWS BEFORE STRATIFICATION) ----------
        if is_bio:
            subj = _bio_subject(question_for_model)
            subj_l = (subj or "").lower().strip()

            if subj_l:
                bio_terms = {subj_l}

                # If first-name-only, try to infer surname-ish token from speaker_name
                if " " not in subj_l:
                    for r in rows_raw:
                        who_full = (r["speaker_name"] or "").lower()
                        if subj_l in who_full:
                            toks = [t for t in re.findall(r"[a-z0-9']+", who_full) if len(t) >= 4 and t not in STOPWORDS]
                            if toks:
                                last = toks[-1]
                                if last != subj_l:
                                    bio_terms.add(last)
                            break

                # Add configured alias terms
                for a in (self._bio_alias_terms(subj_l) or []):
                    if a:
                        bio_terms.add(str(a).lower().strip())

                focused: list = []
                for r in rows_raw:
                    who = (r["speaker_name"] or "").lower()
                    txt = (r["content"] or "").lower()
                    if any(t in who or t in txt for t in bio_terms):
                        focused.append(r)
                        tid = int(r["thread_id"] or 0)
                        if tid:
                            focused_threads.add(tid)
                        else:
                            focused_channels.add(int(r["channel_id"]))

                # Always keep the focused evidence if we found it
                must_keep_ids.update(int(r["message_id"]) for r in focused)

                keep_ctx = int(os.getenv("LORE_BIO_KEEP_THREAD_CONTEXT", "1")) != 0
                rel_terms = [t.strip().lower() for t in (os.getenv(
                    "LORE_BIO_REL_TERMS",
                    "husband|wife|married|widow|widower|fiance|betrothed|son|daughter|child|dead|died|slain|killed|funeral"
                ).split("|")) if t.strip()]

                if keep_ctx and rel_terms and (focused_threads or focused_channels):
                    # In-memory rescue within rows_raw so local relationship/fate lines survive sampling.
                    for r in rows_raw:
                        tid = int(r["thread_id"] or 0)
                        ch = int(r["channel_id"])
                        if (tid and tid in focused_threads) or ((not tid) and ch in focused_channels):
                            txt = (r["content"] or "").lower()
                            if any(rt in txt for rt in rel_terms):
                                must_keep_ids.add(int(r["message_id"]))

                    # Optional: extra FTS pull of relationship terms, filtered back to the same local threads/channels.
                    rel_thread_limit = int(os.getenv("LORE_BIO_REL_THREAD_FTS_LIMIT", "60"))
                    rel_max_terms = int(os.getenv("LORE_BIO_REL_THREAD_MAX_TERMS", "18"))
                    rel_per_term_cap = int(os.getenv("LORE_BIO_REL_THREAD_PULL", "2"))

                    if rel_thread_limit > 0:
                        terms_use = rel_terms[:max(1, rel_max_terms)]
                        per_term = max(1, rel_thread_limit // max(1, len(terms_use)))
                        per_term = min(per_term, max(1, rel_per_term_cap))

                        extra_ids: list[int] = []
                        for rt in terms_use:
                            extra_ids += await self._fts_search(ctx.guild.id, rt, limit=per_term, field="content")

                        seen = set()
                        extra_ids = [x for x in extra_ids if not (x in seen or seen.add(x))]

                        if extra_ids:
                            extra = await self._fetch_messages(ctx.guild.id, extra_ids)
                            for r in extra:
                                tid = int(r["thread_id"] or 0)
                                ch = int(r["channel_id"])
                                if (tid and tid in focused_threads) or ((not tid) and ch in focused_channels):
                                    must_keep_ids.add(int(r["message_id"]))

                # If we have enough evidence, narrow to just “this subject + local context”
                min_keep = int(os.getenv("LORE_BIO_MIN_FOCUSED_ROWS", "18"))
                if len(focused) >= min_keep and keep_ctx and (focused_threads or focused_channels):
                    head_n = int(os.getenv("LORE_BIO_THREAD_HEAD_N", "6"))
                    tail_n = int(os.getenv("LORE_BIO_THREAD_TAIL_N", "6"))
                    sample_n = int(os.getenv("LORE_BIO_THREAD_SAMPLE_N", "20"))
                    per_thread_cap = int(os.getenv("LORE_BIO_PER_THREAD_CAP", "28"))

                    thread_ids_to_expand = list(focused_threads)[: int(os.getenv("LORE_BIO_THREAD_MAX", "10"))]
                    extra_thread_ids: list[int] = []

                    for tid in thread_ids_to_expand:
                        ids = []
                        ids += await self._thread_edge_message_ids(ctx.guild.id, tid, head_n=head_n, tail_n=tail_n)
                        ids += await self._thread_sample_message_ids(ctx.guild.id, tid, k=sample_n)

                        seen = set()
                        ids = [x for x in ids if not (x in seen or seen.add(x))]
                        if per_thread_cap > 0 and len(ids) > per_thread_cap:
                            ids = ids[:per_thread_cap]

                        extra_thread_ids += ids

                    if extra_thread_ids:
                        extra_thread_rows = await self._fetch_messages(ctx.guild.id, extra_thread_ids)
                        rows_raw = _merge_rows(rows_raw, extra_thread_rows)

                    local_pool = []
                    for r in rows_raw:
                        tid = int(r["thread_id"] or 0)
                        ch = int(r["channel_id"])
                        if (tid and tid in focused_threads) or ((not tid) and ch in focused_channels):
                            local_pool.append(r)

                    bio_ctx_radius = int(os.getenv("LORE_BIO_CTX_RADIUS", "6"))
                    bio_ctx_max = int(os.getenv("LORE_BIO_CTX_MAX", "260"))

                    expanded = self._rows_near_anchors(
                        local_pool,
                        must_keep_ids,
                        radius=bio_ctx_radius,
                        max_rows=bio_ctx_max,
                    )

                    by_id = {int(r["message_id"]): r for r in expanded}
                    for r in local_pool:
                        mid = int(r["message_id"])
                        if mid in must_keep_ids:
                            by_id[mid] = r
                    rows_raw = sorted(by_id.values(), key=lambda r: int(r["created_ts"]))

        # ---------- Ensure enough “self voice” for BIO ----------
        min_self = int(os.getenv("LORE_BIO_MIN_SELF_ROWS", "12"))
        if is_bio and subj_l and (min_self > 0):
            self_rows = [r for r in rows_raw if subj_l in (r["speaker_name"] or "").lower()]
            if len(self_rows) < min_self:
                extra_ids: List[int] = []
                extra_ids += await self._speaker_search_ids_like(ctx.guild.id, subj_l, limit=min_self * 3, order="DESC")
                extra_ids += await self._speaker_search_ids_like(ctx.guild.id, subj_l, limit=min_self * 2, order="ASC")
                extra = await self._fetch_messages(ctx.guild.id, extra_ids)
                rows_raw = _merge_rows(rows_raw, extra)
                must_keep_ids.update(int(r["message_id"]) for r in extra if subj_l in (r["speaker_name"] or "").lower())

        # ---------- Sampling / ordering (AFTER bio guard) ----------
        rows = rows_raw

        rows_for_model = rows

        if mode == "timeline":
            rows_for_model = sorted(rows_for_model, key=lambda r: int(r["created_ts"]))
        elif mode == "recent":
            rows_for_model = sorted(rows_for_model, key=lambda r: int(r["created_ts"]))[-max_excerpts:]
        else:
            if is_bio:
                rows_for_model = self._bio_priority_rows(rows_for_model, bio_subject_hint)
            elif is_org:
                rows_for_model = self._org_priority_rows(rows_for_model, question_for_model)
            elif answer_profile == "status":
                rows_for_model = self._status_priority_rows(rows_for_model, question_for_model)
            else:
                bins = int(os.getenv("LORE_TOPIC_STRATA_BINS", "10"))
                per_bin = int(os.getenv("LORE_TOPIC_STRATA_PER_BIN", "12"))
                recent_bonus = int(os.getenv("LORE_TOPIC_STRATA_RECENT_BONUS", "22"))
                rows_for_model = self._time_stratified_rows(
                    rows_for_model,
                    bins=bins,
                    per_bin=per_bin,
                    recent_bonus=recent_bonus,
                )

        rows = rows_for_model

        # Re-insert must-keep ids after sampling
        if must_keep_ids:
            by_id = {int(r["message_id"]): r for r in rows}
            missing = [mid for mid in must_keep_ids if mid not in by_id]
            if missing:
                extra = await self._fetch_messages(ctx.guild.id, list(missing))
                for r in extra:
                    by_id[int(r["message_id"])] = r
            rows = sorted(by_id.values(), key=lambda r: int(r["created_ts"]))

        # ---------- Prefer RP/webhook excerpts, but NEVER drop must-keep ----------
        max_user_rows = int(os.getenv("LORE_ASK_MAX_USER_ROWS", "20"))
        wh = []
        usr = []
        for r in rows:
            if str(r["speaker_type"]) == "webhook" or int(r["webhook_id"] or 0) != 0:
                wh.append(r)
            else:
                usr.append(r)

        if wh and max_user_rows != 0:
            usr_keep = self._time_stratified_rows(usr, bins=6, per_bin=8, recent_bonus=12)
            if max_user_rows > 0 and len(usr_keep) > max_user_rows:
                step = max(1, len(usr_keep) // max_user_rows)
                usr_keep = usr_keep[::step][:max_user_rows]
            rows = _merge_rows(wh, usr_keep)
        elif wh and max_user_rows == 0:
            rows = wh

        # Re-insert must-keep ids again (this is the stage that was killing your “perished” lines)
        if must_keep_ids:
            by_id = {int(r["message_id"]): r for r in rows}
            missing = [mid for mid in must_keep_ids if mid not in by_id]
            if missing:
                extra = await self._fetch_messages(ctx.guild.id, list(missing))
                for r in extra:
                    by_id[int(r["message_id"])] = r
            rows = sorted(by_id.values(), key=lambda r: int(r["created_ts"]))


        answer = await self._openai_answer(question=question_for_model, excerpts=rows, answer_profile=answer_profile)
        if answer:
            # Strip raw source markers like [S12] / S12 from the final visible reply.
            # Do the whole source-only parenthetical first so (S1, S2) does not
            # become an ugly empty shell like (,), (,,), or ().
            answer = self._strip_source_markers_for_public_answer(answer)

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
        await self._send_chunks(ctx, "\n".join(lines))
        
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

    async def _thread_sample_message_ids(self, guild_id: int, thread_id: int, k: int = 24) -> List[int]:
        """Evenly sample k messages across a thread (by OFFSET), to capture the arc."""
        k = int(k or 0)
        if k <= 0:
            return []

        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                "SELECT COUNT(*) AS c FROM lore_messages WHERE guild_id=? AND thread_id=?",
                (int(guild_id), int(thread_id)),
            ).fetchone()
            n = int(row["c"] or 0) if row else 0
            if n <= 0:
                return []

            # If thread is small, just take all message_ids in order.
            if n <= k:
                rows = conn.execute(
                    "SELECT message_id FROM lore_messages WHERE guild_id=? AND thread_id=? ORDER BY created_ts ASC",
                    (int(guild_id), int(thread_id)),
                ).fetchall()
                return [int(r[0]) for r in rows]

            # Pick evenly spaced offsets.
            if k <= 1:
                idxs = [(n - 1) // 2]
            else:
                idxs = [round(i * (n - 1) / (k - 1)) for i in range(k)]
                
            # Pick evenly spaced offsets.
            idxs = [round(i * (n - 1) / (k - 1)) for i in range(k)]
            out: List[int] = []
            seen = set()
            for off in idxs:
                r = conn.execute(
                    "SELECT message_id FROM lore_messages WHERE guild_id=? AND thread_id=? "
                    "ORDER BY created_ts ASC LIMIT 1 OFFSET ?",
                    (int(guild_id), int(thread_id), int(off)),
                ).fetchone()
                if not r:
                    continue
                mid = int(r[0])
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
        force_bio: Optional[bool] = None,
    ) -> Tuple[List[sqlite3.Row], Dict[str, int], List[int]]:
        q = (question or "").strip()
        m_rel = re.match(
            r"(?i)^(?:what(?:'s| is)\s+)?(?:the\s+)?relationship between\s+(.+?)\s+and\s+(.+)$",
            q,
        )
        if m_rel:
            a = _clean_text(m_rel.group(1))
            b = _clean_text(m_rel.group(2))
            q = f"{a}'s relationship to {b}"
        names = self._guess_names_from_question(q)

        # 1) FTS anchors (primary evidence)
        is_bio = self._is_bio_question(q) if force_bio is None else bool(force_bio)

        bio_subject_hint = ""
        if is_bio:
            # Bio queries: prioritize "voice" evidence (speaker_name) so the bio isn't dominated
            # by passing mentions. Keep some mention evidence too.
            speaker_first = int(os.getenv("LORE_BIO_FTS_SPEAKER_LIMIT", "0") or "0")
            content_limit = int(os.getenv("LORE_BIO_FTS_CONTENT_LIMIT", "0") or "0")
            # --- BIO subject cleanup (so queries like "auctor jorlyn" still target "jorlyn") ---
            subj0 = q[7:].strip() if q.lower().startswith("who is ") else q
            subj0 = _clean_text(subj0)
            bio_role_terms = [t.strip().lower() for t in (os.getenv(
                "LORE_BIO_ROLE_TERMS",
                "auctor|leader|head|marshal|commander|ruler"
            ).split("|")) if t.strip()]
            role_set = set(bio_role_terms)
            subj_hint = subj0
            toks = [t for t in re.findall(r"[a-z0-9']+", subj0.lower()) if t]
            toks_no_role = [t for t in toks if t not in role_set]
            if toks_no_role and len(toks_no_role) < len(toks):
                subj_hint = " ".join(toks_no_role)
            bio_subject_hint = subj_hint or subj0
            if speaker_first <= 0 and content_limit <= 0:
                # Default split: ~60% speaker, ~40% content.
                speaker_first = max(16, int(fts_limit * 0.6))
                content_limit = max(0, int(fts_limit) - int(speaker_first))

            speaker_first = max(0, min(int(fts_limit), int(speaker_first)))
            content_limit = max(0, min(int(fts_limit), int(content_limit)))
            if speaker_first == 0 and content_limit == 0:
                content_limit = int(fts_limit)

            fts_anchor_ids: List[int] = []
            if speaker_first:
                # Speaker-name search should use the cleaned subject (not "auctor jorlyn")
                fts_anchor_ids += await self._fts_search(guild_id, bio_subject_hint, limit=speaker_first, field="speaker_name")

            if content_limit:
                fts_anchor_ids += await self._fts_search(guild_id, q, limit=content_limit, field="content")
            bio_role_limit = int(os.getenv("LORE_BIO_ROLE_FTS_LIMIT", "24"))

            seen = set()
            if bio_role_terms and bio_role_limit > 0 and bio_subject_hint:

                per_term = max(3, bio_role_limit // max(1, len(bio_role_terms)))
                phrase = f"\"{bio_subject_hint}\"" if " " in bio_subject_hint else bio_subject_hint

                for rt in bio_role_terms:
                    # forces co-occurrence: (jorlyn AND auctor)
                    fts_anchor_ids += await self._fts_search(
                        guild_id,
                        f"{phrase} {rt}",
                        limit=per_term,
                        field="content",
                    )
            # --- BIO relationship/status augmentation (married/dead/son/etc.) ---
            rel_terms = [t.strip().lower() for t in (os.getenv(
                "LORE_BIO_REL_TERMS",
                "husband|wife|married|widow|widower|fiance|betrothed|son|daughter|child|dead|died|slain|killed|funeral"
            ).split("|")) if t.strip()]
            rel_limit = int(os.getenv("LORE_BIO_REL_FTS_LIMIT", "36"))

            if rel_terms and rel_limit > 0 and bio_subject_hint:
                per_term = max(2, rel_limit // max(1, len(rel_terms)))
                phrase = f"\"{bio_subject_hint}\"" if " " in bio_subject_hint else bio_subject_hint

                # existing strict co-occurrence pass
                for rt in rel_terms:
                    fts_anchor_ids += await self._fts_search(
                        guild_id,
                        f"{phrase} {rt}",
                        limit=per_term,
                        field="content",
                    )

                # extra fate/death rescue pass:
                # if the subject is found in a thread, also pull nearby rows that describe the death
                # without repeating the full name every time ("corpse", "remains", "body", etc.)
                ql = _clean_text(q).lower()
                death_words = {
                    "dead", "died", "death", "slain", "killed", "murdered",
                    "corpse", "body", "remains", "grave", "buried", "passing", "fate"
                }
                wants_fate = any(w in ql for w in death_words) or any(t in death_words for t in rel_terms)

                if wants_fate:
                    fate_terms = [t for t in rel_terms if t in death_words]
                    # make sure the common narrative words are always included
                    for extra in ("corpse", "body", "remains", "death", "dead", "died", "slain", "killed"):
                        if extra not in fate_terms:
                            fate_terms.append(extra)

                    subj_ids = await self._fts_search(
                        guild_id,
                        phrase,
                        limit=max(20, rel_limit),
                        field="content",
                    )

                    subj_rows = await self._fetch_messages(guild_id, subj_ids)
                    seen_scope = set()

                    for r in subj_rows:
                        ch_id = int(r["channel_id"] or 0)
                        th_id = int(r["thread_id"] or 0)
                        key = (ch_id, th_id)
                        if key in seen_scope:
                            continue
                        seen_scope.add(key)

                        # scan this same thread/channel for fate terms even if the exact name isn't repeated
                        async with self._db_lock:
                            conn = self._get_conn()
                            for ft in fate_terms:
                                rows2 = conn.execute(
                                    """
                                    SELECT message_id
                                    FROM lore_messages
                                    WHERE guild_id=?
                                      AND channel_id=?
                                      AND thread_id=?
                                      AND LOWER(content) LIKE ?
                                    ORDER BY created_ts DESC
                                    LIMIT ?
                                    """,
                                    (
                                        int(guild_id),
                                        ch_id,
                                        th_id,
                                        f"%{ft.lower()}%",
                                        max(6, per_term),
                                    ),
                                ).fetchall()
                                fts_anchor_ids += [int(x[0]) for x in rows2]

                # de-dupe after bio relationship augmentation
                seen = set()
                fts_anchor_ids = [x for x in fts_anchor_ids if not (x in seen or seen.add(x))]
                    
            # de-dupe after bio role/title augmentation (preserve order)
            seen = set()
            fts_anchor_ids = [x for x in fts_anchor_ids if not (x in seen or seen.add(x))]
            
            # --- BIO faction / event / important-fact augmentation ---
            import_terms = [
                t.strip().lower()
                for t in (os.getenv(
                    "LORE_BIO_IMPORT_TERMS",
                    "battle of|siege of|trial of|ritual of|expedition to|"
                    "knights of|church of|order of|house of|house|"
                    "member of|part of|served with|served in|fought in|"
                    "chosen|chosen of|founded|formed|started"
                ).split("|"))
                if t.strip()
            ]
            import_limit = int(os.getenv("LORE_BIO_IMPORT_FTS_LIMIT", "48"))

            if import_terms and import_limit > 0 and bio_subject_hint:
                phrase = f"\"{bio_subject_hint}\"" if " " in bio_subject_hint else bio_subject_hint
                per_term = max(2, import_limit // max(1, len(import_terms)))

                # 1) strict co-occurrence: subject + important term
                for term in import_terms:
                    fts_anchor_ids += await self._fts_search(
                        guild_id,
                        f"{phrase} {term}",
                        limit=per_term,
                        field="content",
                    )

                # 2) map-neighbor rescue for bios:
                # if the lore map knows nearby org/event nodes for this subject,
                # search those neighbor display names directly even when query isn't "short".
                try:
                    bio_map_entities = await self._map_pick_entities_for_question(
                        guild_id,
                        bio_subject_hint,
                        names=[bio_subject_hint],
                        limit=max(1, int(os.getenv("LORE_ASK_MAP_MAX_ENTITIES", "3") or "3")),
                    )

                    neighbor_terms: List[str] = []
                    neighbor_k = int(os.getenv("LORE_BIO_MAP_NEIGHBORS", os.getenv("LORE_ASK_MAP_NEIGHBORS", "4")) or "4")

                    for er in (bio_map_entities or []):
                        try:
                            eid = int(er["entity_id"])
                        except Exception:
                            continue

                        disp = _clean_text(str(er["display"] or ""))
                        if disp and disp.lower() != bio_subject_hint.lower():
                            neighbor_terms.append(disp)

                        for nd in await self._map_neighbor_displays(guild_id, eid, limit=neighbor_k):
                            nd = _clean_text(nd)
                            if nd:
                                neighbor_terms.append(nd)

                    seen_terms = set()
                    neighbor_terms = [t for t in neighbor_terms if not (t.lower() in seen_terms or seen_terms.add(t.lower()))]

                    per_neighbor = max(2, min(8, import_limit // max(1, len(neighbor_terms) or 1)))
                    for nd in neighbor_terms:
                        # Co-occur with the subject whenever possible
                        fts_anchor_ids += await self._fts_search(
                            guild_id,
                            f'{phrase} "{nd}"',
                            limit=per_neighbor,
                            field="content",
                        )
                        # Also allow direct phrase hits for the org/event itself
                        fts_anchor_ids += await self._fts_search(
                            guild_id,
                            f'"{nd}"',
                            limit=max(2, per_neighbor // 2),
                            field="content",
                        )
                except Exception:
                    pass

                # 3) local-thread/channel rescue:
                # once we know subject rows, pull nearby important-term lines from those same scopes
                subj_ids = await self._fts_search(
                    guild_id,
                    phrase,
                    limit=max(20, import_limit),
                    field="content",
                )
                subj_rows = await self._fetch_messages(guild_id, subj_ids)
                seen_scope = set()

                async with self._db_lock:
                    conn = self._get_conn()
                    for r in subj_rows:
                        ch_id = int(r["channel_id"] or 0)
                        th_id = int(r["thread_id"] or 0)
                        key = (ch_id, th_id)
                        if key in seen_scope:
                            continue
                        seen_scope.add(key)

                        for term in import_terms:
                            rows2 = conn.execute(
                                """
                                SELECT message_id
                                FROM lore_messages
                                WHERE guild_id=?
                                  AND channel_id=?
                                  AND thread_id=?
                                  AND LOWER(content) LIKE ?
                                ORDER BY created_ts DESC
                                LIMIT ?
                                """,
                                (
                                    int(guild_id),
                                    ch_id,
                                    th_id,
                                    f"%{term.lower()}%",
                                    max(4, per_term),
                                ),
                            ).fetchall()
                            fts_anchor_ids += [int(x[0]) for x in rows2]

                seen = set()
                fts_anchor_ids = [x for x in fts_anchor_ids if not (x in seen or seen.add(x))]
            
        else:
            fts_anchor_ids = await self._fts_search(guild_id, q, limit=fts_limit)

            # --- Topic role/title augmentation (helps pull leadership facts like "Auctor") ---
            role_terms = [t.strip() for t in (os.getenv(
                "LORE_TOPIC_ROLE_TERMS",
                "auctor|leader|head|marshal|commander|ruler"
            ).split("|")) if t.strip()]
            role_limit = int(os.getenv("LORE_TOPIC_ROLE_FTS_LIMIT", "18"))

            if role_terms and role_limit > 0:
                toks = [t for t in re.findall(r"[a-z0-9']+", q.lower()) if len(t) >= 4 and t not in STOPWORDS]
                generic = {"order", "guild", "house", "clan", "society"}
                toks = [t for t in toks if t not in generic]

                # e.g. "order crimson rain" -> "crimson rain"
                best_phrase = " ".join(toks[:3]) if len(toks) >= 2 else ""
                if best_phrase:
                    per_term = max(3, role_limit // max(1, len(role_terms)))
                    for rt in role_terms:
                        fts_anchor_ids += await self._fts_search(
                            guild_id,
                            f"\"{best_phrase}\" {rt}",
                            limit=per_term,
                        )

                # de-dupe
                seen = set()
                fts_anchor_ids = [x for x in fts_anchor_ids if not (x in seen or seen.add(x))]
                
        # Alias expansion for bios (titles/nicknames) — use cleaned bio subject when available
        subj = bio_subject_hint or (q[7:].strip() if q.lower().startswith("who is ") else q)

        subj_l = _clean_text(subj).lower()
        for a in self._bio_alias_terms(subj_l):
            fts_anchor_ids += await self._fts_search(guild_id, a, limit=20, field="content")

        seen = set()
        fts_anchor_ids = [x for x in fts_anchor_ids if not (x in seen or seen.add(x))]

        ids: List[int] = list(fts_anchor_ids)

        # Only do speaker lookup when it plausibly helps identify a person lookup.
        is_bio = self._is_bio_question(q)
        # Still allow some speaker recall for short bare lookups like "Jorlyn",
        # but do not let that alone force the final answer into bio format.
        do_speaker = is_bio or (len(names) == 1) or self._looks_like_name_lookup(q)

        if do_speaker:
            name_hint = bio_subject_hint if (is_bio and bio_subject_hint) else (q[7:].strip() if q.lower().startswith("who is ") else (names[0] if names else q))

            ids += await self._speaker_search_ids_like(guild_id, name_hint, limit=speaker_limit, order="DESC")
            ids += await self._speaker_search_ids_like(guild_id, name_hint, limit=min(40, speaker_limit), order="ASC")

        for nm in names:
            ids += await self._fts_search(guild_id, nm, limit=25)
            ids += await self._speaker_search_ids(guild_id, nm, limit=speaker_limit, order="DESC")
            ids += await self._speaker_search_ids(guild_id, nm, limit=min(40, speaker_limit), order="ASC")


        # --- Lore-map assisted query expansion (disambiguation + better recall) ---
        # Uses the lore map as an *index* (not as evidence): it helps pick better anchors/terms,
        # then we still ground answers in actual message excerpts.
        if int(os.getenv("LORE_ASK_USE_MAP", "1") or "1") != 0:
            try:
                map_max_entities = int(os.getenv("LORE_ASK_MAP_MAX_ENTITIES", "3") or "3")
                map_seed_k = int(os.getenv("LORE_ASK_MAP_MENTION_SEEDS", "8") or "8")
                map_neighbor_k = int(os.getenv("LORE_ASK_MAP_NEIGHBORS", "4") or "4")
                map_fts_per_term = int(os.getenv("LORE_ASK_MAP_FTS_PER_TERM", "10") or "10")
                map_expand_tokens_le = int(os.getenv("LORE_ASK_MAP_EXPAND_TOKENS_LE", "3") or "3")
                map_expand_on_low_fts = int(os.getenv("LORE_ASK_MAP_EXPAND_ON_LOW_FTS", "8") or "8")

                toks_q = [t for t in self._map_canon(q).split() if t and t not in STOPWORDS]
                short_q = (len(toks_q) <= max(1, map_expand_tokens_le))
                low_fts = (len(fts_anchor_ids) < max(0, map_expand_on_low_fts))

                map_entities = await self._map_pick_entities_for_question(
                    guild_id,
                    q,
                    names=names,
                    limit=map_max_entities,
                )

                if map_entities:
                    # 1) Add a few direct mention anchors per entity (captures real in-world usage quickly)
                    for er in map_entities:
                        try:
                            eid = int(er["entity_id"])
                            ids += await self._map_sample_mention_message_ids(guild_id, eid, k=map_seed_k)
                        except Exception:
                            continue

                    # 2) Add FTS anchors for the entity display names (and optionally top neighbors)
                    map_fts_ids: List[int] = []
                    for er in map_entities:
                        disp = _clean_text(str(er["display"] or ""))
                        if disp:
                            map_fts_ids += await self._fts_search(guild_id, f'"{disp}"', limit=map_fts_per_term, field="content")

                    if (short_q or low_fts) and map_neighbor_k > 0:
                        for er in map_entities:
                            try:
                                eid = int(er["entity_id"])
                            except Exception:
                                continue
                            for nd in await self._map_neighbor_displays(guild_id, eid, limit=map_neighbor_k):
                                map_fts_ids += await self._fts_search(
                                    guild_id,
                                    f'"{nd}"',
                                    limit=max(3, map_fts_per_term // 2),
                                    field="content",
                                )

                    if map_fts_ids:
                        # Treat these as "anchors" too (they’re still real message hits).
                        fts_anchor_ids += map_fts_ids
                        ids += map_fts_ids
            except Exception:
                pass

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
        anchor_thread_ids: List[int] = []
        try:
            anchor_rows = await self._fetch_messages(guild_id, list(fts_anchor_ids))
            # NOTE: keep this list around; BIO expansion will prefer these threads.
            anchor_thread_ids = []
            seen_tid = set()
            for r in anchor_rows:
                tid = int(r["thread_id"] or 0)
                if tid and tid not in seen_tid:
                    seen_tid.add(tid)
                    anchor_thread_ids.append(tid)

            # (topic mode) edge sampling for anchor threads
            for tid in sorted(anchor_thread_ids):
                edge_ids += await self._thread_edge_message_ids(guild_id, tid, head_n=head_n, tail_n=tail_n)
        except Exception:
            edge_ids = []
            anchor_thread_ids = []

        # edge_ids are fetched separately to avoid polluting the main pool (topics),
        # but for bios we DO include them so the model can see intros/conclusions.
        if is_bio and edge_ids:
            ids += edge_ids

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

        # --- BIO thread expansion (A): anchor-thread-first + bm25-based top threads ---
        if is_bio:
            name_hint = q[7:].strip() if q.lower().startswith("who is ") else q
            name_hint = _clean_text(name_hint).lower()

            bio_head = int(os.getenv("LORE_BIO_THREAD_HEAD_N", "8"))
            bio_tail = int(os.getenv("LORE_BIO_THREAD_TAIL_N", "8"))
            bio_sample = int(os.getenv("LORE_BIO_THREAD_SAMPLE_N", "28"))
            bio_max_threads = int(os.getenv("LORE_BIO_THREAD_MAX", "10"))

            if bio_max_threads > 0:
                candidate_threads: List[int] = []

                # 1) Prefer threads that contain actual anchors (best evidence).
                for tid in anchor_thread_ids:
                    if tid:
                        candidate_threads.append(int(tid))

                # 2) Fill remaining slots with "best matching" threads by bm25 (not just mention volume).
                #    This avoids picking massive chatter threads where the name appears often.
                top_threads: List[int] = []
                try:
                    term_q0 = _fts_query_from_question(name_hint)
                    term_q = f"content:({term_q0}) OR speaker_name:({term_q0})"

                    async with self._db_lock:
                        conn = self._get_conn()
                        rows_t = conn.execute(
                            """
                            SELECT m.thread_id,
                                   MIN(bm25(lore_fts)) AS best_score,
                                   COUNT(*) AS c
                            FROM lore_messages m
                            JOIN lore_fts ON lore_fts.rowid = m.message_id
                            WHERE m.guild_id=?
                              AND m.thread_id != 0
                              AND lore_fts MATCH ?
                            GROUP BY m.thread_id
                            ORDER BY best_score ASC, c DESC
                            LIMIT ?
                            """,
                            (int(guild_id), term_q, int(bio_max_threads * 10)),
                        ).fetchall()

                    top_threads = [int(r[0]) for r in rows_t if int(r[0] or 0) != 0]
                except Exception:
                    top_threads = []

                # Merge + de-dupe threads (preserve order: anchor threads first, then bm25 list)
                seen_tid = set()
                merged_threads: List[int] = []
                for tid in candidate_threads + top_threads:
                    tid = int(tid or 0)
                    if not tid or tid in seen_tid:
                        continue
                    seen_tid.add(tid)
                    merged_threads.append(tid)

                merged_threads = merged_threads[:bio_max_threads]

                # Add head/tail + evenly sampled interior from those threads
                extra_ids: List[int] = []
                for tid in merged_threads:
                    extra_ids += await self._thread_edge_message_ids(guild_id, tid, head_n=bio_head, tail_n=bio_tail)
                    extra_ids += await self._thread_sample_message_ids(guild_id, tid, k=bio_sample)

                if extra_ids:
                    ids += extra_ids
                    seen2 = set()
                    ids = [x for x in ids if not (x in seen2 or seen2.add(x))]

        cap = int(os.getenv("LORE_ASK_IDS_CAP", "800"))
        ids = ids[:cap]

        rows_all = await self._fetch_messages(guild_id, ids)
        rows_all = sorted(rows_all, key=lambda r: int(r["created_ts"]))

        # --- BIO per-thread cap (B): prevent one giant thread from dominating ---
        if is_bio and rows_all:
            per_thread_cap = int(os.getenv("LORE_BIO_PER_THREAD_CAP", "28") or "28")
            per_thread_cap = max(0, per_thread_cap)

            if per_thread_cap > 0:
                # Always keep true anchors + bio edge rows, even if a thread is huge.
                keep_ids = {int(x) for x in fts_anchor_ids}
                if edge_ids:
                    keep_ids.update(int(x) for x in edge_ids)

                by_tid: Dict[int, List[sqlite3.Row]] = {}
                for r in rows_all:
                    tid = int(r["thread_id"] or 0)
                    by_tid.setdefault(tid, []).append(r)

                kept_rows: List[sqlite3.Row] = []

                for tid, lst in by_tid.items():
                    if tid == 0 or len(lst) <= per_thread_cap:
                        kept_rows.extend(lst)
                        continue

                    # Split into (must-keep anchors/edges) and other rows
                    anchors = [r for r in lst if int(r["message_id"]) in keep_ids]
                    others = [r for r in lst if int(r["message_id"]) not in keep_ids]

                    # Deduplicate anchors in-order
                    seen_mid = set()
                    anchors = [r for r in anchors if not (int(r["message_id"]) in seen_mid or seen_mid.add(int(r["message_id"])))]

                    # If anchors alone exceed the cap, keep them anyway (rare, but correct).
                    remaining = max(0, per_thread_cap - len(anchors))
                    if remaining <= 0:
                        kept_rows.extend(anchors)
                        continue

                    # Evenly sample the "others" across time so we keep early+mid+late arc.
                    other_ids = [int(r["message_id"]) for r in others]
                    sampled_other_ids = set(self._pick_seed_ids(other_ids, remaining)) if other_ids else set()

                    kept_rows.extend([r for r in others if int(r["message_id"]) in sampled_other_ids])
                    kept_rows.extend(anchors)

                # Dedup globally + sort
                by_id = {}
                for r in kept_rows:
                    by_id[int(r["message_id"])] = r
                rows_all = sorted(by_id.values(), key=lambda r: int(r["created_ts"]))

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
            focus_terms = [t for t in toks2 if len(t) >= 4 and t not in STOPWORDS]
            # Prefer specific terms so "black" doesn't dominate
            focus_terms = sorted(set(focus_terms), key=len, reverse=True)[:5]
        except Exception:
            focus_terms = []

        edge_rows_filtered: List[sqlite3.Row] = []
        if edge_ids and focus_terms:
            edge_rows = await self._fetch_messages(guild_id, edge_ids)
            for r in edge_rows:
                c = (r["content"] or "").lower()
                need = 2 if len(focus_terms) >= 2 else 1
                hits = sum(1 for t in focus_terms if t in c)
                if hits >= need:
                    edge_rows_filtered.append(r)

        rows = rows_all

        if (not is_bio) and rows and fts_anchor_ids:
            radius = int(os.getenv("LORE_TOPIC_CONTEXT_RADIUS", "3"))
            if radius > 0:
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

    @lore.group(name="map", invoke_without_command=True)
    async def lore_map(self, ctx: commands.Context):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)

        async with self._db_lock:
            conn = self._get_conn()
            row_e = conn.execute(
                "SELECT kind, COUNT(*) AS c FROM lore_entities WHERE guild_id=? GROUP BY kind",
                (gid,),
            ).fetchall()
            row_edges = conn.execute(
                "SELECT COUNT(*) AS c FROM lore_entity_edges WHERE guild_id=?",
                (gid,),
            ).fetchone()
            total_edges = int(row_edges["c"] or 0) if row_edges else 0

        by_kind = {str(r["kind"]): int(r["c"] or 0) for r in (row_e or [])}

        embed = nextcord.Embed(
            title="Lore Map",
            description=(
                "Builds a *memory network* of people, orgs, events, and named things from your archived lore.\n\n"
                "Commands:\n"
                "• `!lore map build` (admin)\n"
                "• `!lore map top [people|orgs|events|things|all] [n]`\n"
                "• `!lore map show <name>`\n"
            ),
            color=nextcord.Color.blurple(),
        )
        embed.add_field(
            name="Current graph",
            value=(
                f"People: **{by_kind.get('person', 0)}**\n"
                f"Orgs: **{by_kind.get('org', 0)}**\n"
                f"Events: **{by_kind.get('event', 0)}**\n"
                f"Things: **{by_kind.get('thing', 0)}**\n"
                f"Edges: **{total_edges}**"
            ),
            inline=False,
        )
        await ctx.send(embed=embed)

    @lore_map.command(name="build")
    @commands.has_permissions(manage_guild=True)
    async def lore_map_build(self, ctx: commands.Context):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)

        total = await self._count_messages(gid)
        if total <= 0:
            await ctx.send("❌ No stored lore messages yet for this server.")
            return

        msg = await ctx.send("🧠 Building lore map from archived messages…")
        stats = await self._map_rebuild_guild(gid)

        embed = nextcord.Embed(
            title="✅ Lore Map Built",
            description=f"Processed stored messages in this server: **{total}**",
            color=nextcord.Color.green(),
        )
        embed.add_field(
            name="Nodes",
            value=(
                f"People: **{stats['people']}**\n"
                f"Orgs: **{stats['orgs']}**\n"
                f"Events: **{stats['events']}**\n"
                f"Things: **{stats['things']}**\n"
                f"Total: **{stats['entities']}**"
            ),
            inline=True,
        )
        embed.add_field(
            name="Links",
            value=(
                f"Edges: **{stats['edges']}**\n"
                f"Mentions stored: **{stats['mentions']}**"
            ),
            inline=True,
        )
        await msg.edit(content=None, embed=embed)

    @lore_map.command(name="top")
    async def lore_map_top(self, ctx: commands.Context, kind: str = "all", n: int = 10):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        kind = (kind or "all").strip().lower()
        n = max(3, min(25, int(n or 10)))

        kind_map = {
            "people": "person",
            "persons": "person",
            "person": "person",
            "org": "org",
            "orgs": "org",
            "events": "event",
            "event": "event",
            "things": "thing",
            "thing": "thing",
            "all": "all",
        }
        k = kind_map.get(kind, "all")

        async with self._db_lock:
            conn = self._get_conn()

            def fetch(k2: str):
                if k2 == "all":
                    return conn.execute(
                        "SELECT display, kind, mentions, last_ts FROM lore_entities WHERE guild_id=? ORDER BY score DESC LIMIT ?",
                        (gid, n),
                    ).fetchall()
                return conn.execute(
                    "SELECT display, kind, mentions, last_ts FROM lore_entities WHERE guild_id=? AND kind=? ORDER BY score DESC LIMIT ?",
                    (gid, k2, n),
                ).fetchall()

            if k == "all":
                rows_person = fetch("person")
                rows_org = fetch("org")
                rows_event = fetch("event")
                rows_thing = fetch("thing")
            else:
                rows_person = fetch(k)

        embed = nextcord.Embed(
            title="📌 Lore Map — Top Nodes",
            description="(by mentions + small boosts for orgs/events)",
            color=nextcord.Color.dark_teal(),
        )

        def fmt(rows):
            lines = []
            for r in (rows or []):
                disp = str(r["display"] or "?")
                m = int(r["mentions"] or 0)
                lines.append(f"• **{disp}** — {m}")
            return "\n".join(lines) if lines else "• (none yet)"

        if k == "all":
            embed.add_field(name="People", value=fmt(rows_person), inline=False)
            embed.add_field(name="Organizations", value=fmt(rows_org), inline=False)
            embed.add_field(name="Events", value=fmt(rows_event), inline=False)
            embed.add_field(name="Things", value=fmt(rows_thing), inline=False)
        else:
            label = {"person": "People", "org": "Organizations", "event": "Events", "thing": "Things"}.get(k, "Top")
            embed.add_field(name=label, value=fmt(rows_person), inline=False)

        await ctx.send(embed=embed)

    @lore_map.command(name="show")
    async def lore_map_show(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        q = self._map_canon(name or "")
        if not q:
            await ctx.send("❌ Usage: `!lore map show <name>`")
            return

        async with self._db_lock:
            conn = self._get_conn()

            # exact canon match preferred, otherwise partial
            row = conn.execute(
                "SELECT * FROM lore_entities WHERE guild_id=? AND canon=? ORDER BY score DESC LIMIT 1",
                (gid, q),
            ).fetchone()
            if row is None:
                row = conn.execute(
                    "SELECT * FROM lore_entities WHERE guild_id=? AND canon LIKE ? ORDER BY score DESC LIMIT 1",
                    (gid, f"%{q}%",),
                ).fetchone()

            if row is None:
                await ctx.send("❌ I couldn't find that in the lore map yet. Try `!lore map top` or run `!lore map build`.")
                return

            eid = int(row["entity_id"])
            disp = str(row["display"] or "?")
            kind = str(row["kind"] or "thing")
            mentions = int(row["mentions"] or 0)

            neigh = conn.execute(
                """
                SELECT
                    CASE WHEN e.a_entity_id=? THEN e.b_entity_id ELSE e.a_entity_id END AS other_id,
                    e.weight AS w,
                    e.last_ts AS last_ts
                FROM lore_entity_edges e
                WHERE e.guild_id=? AND (e.a_entity_id=? OR e.b_entity_id=?)
                ORDER BY e.weight DESC
                LIMIT 12
                """,
                (eid, gid, eid, eid),
            ).fetchall()

            other_ids = [int(r["other_id"]) for r in (neigh or [])]
            other_map = {}
            if other_ids:
                ph = ",".join(["?"] * len(other_ids))
                rows2 = conn.execute(
                    f"SELECT entity_id, display, kind, mentions FROM lore_entities WHERE guild_id=? AND entity_id IN ({ph})",
                    (gid, *other_ids),
                ).fetchall()
                other_map = {int(r["entity_id"]): r for r in rows2}

            # recent mentions
            recent = conn.execute(
                """
                SELECT m.jump_url, m.created_ts, m.channel_id, m.thread_id
                FROM lore_entity_mentions em
                JOIN lore_messages m ON m.guild_id=em.guild_id AND m.message_id=em.message_id
                WHERE em.guild_id=? AND em.entity_id=?
                ORDER BY em.created_ts DESC
                LIMIT 3
                """,
                (gid, eid),
            ).fetchall()

        embed = nextcord.Embed(
            title=f"🕸️ Lore Map — {disp}",
            description=f"Type: **{kind}** • Mentions: **{mentions}**",
            color=nextcord.Color.blurple(),
        )

        lines = []
        for r in (neigh or []):
            oid = int(r["other_id"])
            w = float(r["w"] or 0.0)
            rr = other_map.get(oid)
            if not rr:
                continue
            od = str(rr["display"] or "?")
            ok = str(rr["kind"] or "thing")
            lines.append(f"• **{od}** ({ok}) — {int(w)}")
        embed.add_field(name="Top connections", value="\n".join(lines) if lines else "• (none)", inline=False)

        if recent:
            rlines = []
            for r in recent:
                url = (r["jump_url"] or "").strip()
                cid = int(r["channel_id"] or 0)
                rlines.append(f"• <#{cid}> {url}" if url else f"• <#{cid}>")
            embed.add_field(name="Recent mentions", value="\n".join(rlines), inline=False)

        await ctx.send(embed=embed)



    @lore.group(name="persona", invoke_without_command=True)
    async def lore_persona(self, ctx: commands.Context):
        if not ctx.guild:
            return
        embed = nextcord.Embed(
            title="🎭 Lore Personas",
            description=(
                "Auto-build cached character voice sheets from your archived messages.\n\n"
                "Commands:\n"
                "• `!lore persona build <name>`\n"
                "• `!lore persona show <name>`\n"
                "• `!lore persona list`\n"
                "• `!lore persona clear <name>`\n\n"
                "Tip: For multi-word names, just type them normally.\n"
            ),
            color=nextcord.Color.blurple(),
        )
        await ctx.send(embed=embed)

    @lore_persona.command(name="list")
    async def lore_persona_list(self, ctx: commands.Context):
        if not ctx.guild:
            return
        rows = await self._persona_list(int(ctx.guild.id), limit=25)
        if not rows:
            await ctx.send("• (no personas built yet)")
            return
        lines = []
        for r in rows:
            disp = str(r["display"] or r["canon"] or "?")
            ts = int(r["updated_ts"] or 0)
            when = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d") if ts else "?"
            lines.append(f"• **{disp}** (updated {when})")
        await ctx.send("\n".join(lines))

    @lore_persona.command(name="build")
    async def lore_persona_build(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        name = _clean_text(name or "")
        if not name:
            await ctx.send("❌ Usage: `!lore persona build <name>`")
            return

        msg = await ctx.send(f"🧠 Building persona for **{name}**…")
        excerpts, stats = await self._persona_collect_sources(gid, name)
        if not excerpts:
            await msg.edit(content=f"❌ I couldn't find any stored messages/mentions for **{name}**.")
            return

        sheet = await self._openai_persona_sheet(name=name, excerpts=excerpts)
        if not sheet:
            await msg.edit(content="❌ OpenAI is not configured (missing OPENAI_API_KEY) or the request failed.")
            return

        canon = stats.get("canon") or self._persona_canon(name)
        await self._persona_upsert(gid, canon, name, sheet, stats)

        embed = nextcord.Embed(
            title="✅ Persona Built",
            description=f"**{name}**\n\n{sheet[:3500]}",
            color=nextcord.Color.green(),
        )
        embed.set_footer(text=f"self_rows={stats.get('self_rows_total')} mention_rows={stats.get('mention_rows_total')} sampled={stats.get('sampled_rows')}")
        await msg.edit(content=None, embed=embed)

    @lore_persona.command(name="show")
    async def lore_persona_show(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        row = await self._persona_get(gid, canon)
        if not row:
            await ctx.send("❌ Not built yet. Try: `!lore persona build <name>`")
            return
        disp = str(row["display"] or name)
        prof = str(row["profile"] or "")
        embed = nextcord.Embed(title=f"🎭 {disp}", description=prof[:3900], color=nextcord.Color.dark_teal())
        await ctx.send(embed=embed)

    @lore_persona.command(name="clear")
    @commands.has_permissions(manage_guild=True)
    async def lore_persona_clear(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        await self._persona_delete(gid, canon)
        await ctx.send(f"🧹 Cleared persona cache for **{_clean_text(name)}**")


    @lore.command(name="rp")
    async def lore_rp(self, ctx: commands.Context, *, raw: str):
        """
        Usage:
          !lore rp Name | your message here
        (the | lets names contain spaces without quoting)
        """
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        raw = _clean_text(raw or "")
        if "|" not in raw:
            await ctx.send("❌ Usage: `!lore rp Name | <what you say>`")
            return

        name, prompt = [s.strip() for s in raw.split("|", 1)]
        if not name or not prompt:
            await ctx.send("❌ Usage: `!lore rp Name | <what you say>`")
            return

        canon = self._persona_canon(name)
        row = await self._persona_get(gid, canon)
        if not row:
            # auto-build on first use (best UX)
            tmp = await ctx.send(f"🧠 No cached persona for **{name}** — building one now…")
            excerpts_p, _stats = await self._persona_collect_sources(gid, name)
            if not excerpts_p:
                await tmp.edit(content=f"❌ I couldn't find any stored messages/mentions for **{name}**.")
                return
            sheet = await self._openai_persona_sheet(name=name, excerpts=excerpts_p)
            if not sheet:
                await tmp.edit(content="❌ OpenAI is not configured (missing OPENAI_API_KEY) or the request failed.")
                return
            await self._persona_upsert(gid, canon, name, sheet, _stats)
            row = await self._persona_get(gid, canon)
            await tmp.edit(content=None)

        persona = str(row["profile"] or "")
        disp = str(row["display"] or name)


                
        # Scene memory (optional, per channel/thread + character)
        scene_ch_id, scene_th_id = self._scene_loc_from_ctx(ctx)
        scene_enabled = await self._scene_is_enabled(gid, scene_ch_id, scene_th_id, canon)
        scene_text = ""
        if scene_enabled:
            scene_text = await self._scene_memory_text(
                gid, scene_ch_id, scene_th_id, canon, character_name=disp
            )
            
        # Pull grounding excerpts using the same retrieval engine as !lore ask
        fts_limit = int(os.getenv("LORE_RP_FTS_LIMIT", os.getenv("LORE_ASK_FTS_LIMIT", "200")))
        speaker_limit = int(os.getenv("LORE_RP_SPEAKER_LIMIT", os.getenv("LORE_ASK_SPEAKER_LIMIT", "200")))
        win_before = int(os.getenv("LORE_RP_WIN_BEFORE", os.getenv("LORE_ASK_WIN_BEFORE", "6")))
        win_after = int(os.getenv("LORE_RP_WIN_AFTER", os.getenv("LORE_ASK_WIN_AFTER", "10")))
        max_excerpts = int(os.getenv("LORE_RP_MAX_EXCERPTS", "80"))

        rows, _stats2, _anchors = await self._retrieve_rows_for_question(
            gid,
            prompt,
            fts_limit=fts_limit,
            speaker_limit=speaker_limit,
            seed_count=int(os.getenv("LORE_ASK_WINDOW_SEEDS", "12")),
            win_before=win_before,
            win_after=win_after,
            max_excerpts=max_excerpts,
        )

        # Optional: boost retrieval using entities mentioned in the current scene
        if scene_enabled and scene_text and int(os.getenv("LORE_SCENE_RETRIEVAL_BOOST", "1") or "1") != 0:
            boost_n = int(os.getenv("LORE_SCENE_BOOST_N", "6") or "6")
            boost_limit = int(os.getenv("LORE_SCENE_BOOST_FTS_LIMIT", "25") or "25")
            boost_max_rows = int(os.getenv("LORE_SCENE_BOOST_MAX_ROWS", "80") or "80")

            terms = await self._scene_boost_terms(gid, scene_text, limit=boost_n)
            boost_ids: List[int] = []
            seen_ids = set()
            for t in terms:
                ids = await self._fts_search(gid, f"\"{t}\"", limit=boost_limit)
                if not ids:
                    ids = await self._fts_search(gid, t, limit=boost_limit)
                for mid in ids:
                    if mid in seen_ids:
                        continue
                    seen_ids.add(mid)
                    boost_ids.append(mid)
                    if len(boost_ids) >= boost_max_rows:
                        break
                if len(boost_ids) >= boost_max_rows:
                    break

            boost_rows = await self._fetch_messages(gid, boost_ids)
            # Merge immediately so downstream `by_id = { ... }` includes them
            if boost_rows:
                rows = list(rows or []) + list(boost_rows or [])
        # Ensure we have *some* in-character voice in the excerpt pool too
        extra_voice, _ = await self._persona_collect_sources(gid, disp)
        by_id = {int(r["message_id"]): r for r in (rows or [])}
        for r in extra_voice[:20]:
            by_id[int(r["message_id"])] = r
        rows = sorted(by_id.values(), key=lambda r: int(r["created_ts"] or 0))

        # Scene directives (optional, per channel/thread + character)
        directives = ""
        if scene_enabled:
            directives = await self._scene_directives_text(gid, scene_ch_id, scene_th_id, canon)

        async with ctx.typing():
            out = await self._openai_rp(
                name=disp,
                persona=persona,
                prompt=prompt,
                excerpts=rows,
                scene=scene_text,
                directives=directives,
                stats=persona_stats,  # NEW
            )

        if not out:
            await ctx.send("❌ OpenAI is not configured (missing OPENAI_API_KEY) or the request failed.")
            return

        if scene_enabled:
            await self._scene_meta_set(gid, scene_ch_id, scene_th_id, canon, "last_user", prompt)
            await self._scene_meta_set(gid, scene_ch_id, scene_th_id, canon, "last_out", out)
            user_name = getattr(ctx.author, "display_name", None) or getattr(ctx.author, "name", None) or "User"
            await self._scene_append_turn(gid, scene_ch_id, scene_th_id, canon, role="user", speaker=user_name, content=prompt)
            await self._scene_append_turn(gid, scene_ch_id, scene_th_id, canon, role="assistant", speaker=disp, content=out)
        # Send as normal text (Discord 2000-char limit → chunk)
        await self._send_chunks(ctx, f"🎭 **{disp}**\n" + (out or ""))
                

    @lore.group(name="scene", invoke_without_command=True)
    async def lore_scene(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await ctx.send(
            "🎬 **Scene mode** (rolling RP memory per channel/thread + character)\n"
            "Commands:\n"
            "• `!lore scene on <Name>`\n"
            "• `!lore scene off <Name>`\n"
            "• `!lore scene clear <Name>`\n"
            "• `!lore scene status [Name]`\n"
            "• `!lore scene show <Name> [n]`\n"
        )

    @lore_scene.command(name="on")
    async def lore_scene_on(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_set_enabled(gid, ch_id, th_id, canon, True)
        await ctx.send(f"✅ Scene mode **ON** here for **{_clean_text(name)}**")

    @lore_scene.command(name="off")
    async def lore_scene_off(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_set_enabled(gid, ch_id, th_id, canon, False)
        await ctx.send(f"🛑 Scene mode **OFF** here for **{_clean_text(name)}**")

    @lore_scene.command(name="clear")
    async def lore_scene_clear(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_clear(gid, ch_id, th_id, canon)
        await ctx.send(f"🧹 Cleared scene memory here for **{_clean_text(name)}**")

    @lore_scene.command(name="status")
    async def lore_scene_status(self, ctx: commands.Context, *, name: str = ""):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        name = _clean_text(name or "")
        if name:
            canon = self._persona_canon(name)
            on = await self._scene_is_enabled(gid, ch_id, th_id, canon)
            await ctx.send(f"🎬 Scene mode for **{name}** here: **{on}**")
            return

        # list enabled personas in this channel/thread
        async with self._db_lock:
            conn = self._get_conn()
            rows = conn.execute(
                """
                SELECT persona_canon
                FROM lore_scene_state
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND enabled=1
                ORDER BY updated_ts DESC
                LIMIT 25
                """,
                (gid, int(ch_id), int(th_id)),
            ).fetchall()

        if not rows:
            await ctx.send("🎬 Scene mode: (none enabled here)")
            return

        canons = [str(r["persona_canon"] or "") for r in rows]
        # Try to pretty-print from persona cache if available
        async with self._db_lock:
            conn = self._get_conn()
            ph = ",".join(["?"] * len(canons))
            pr = conn.execute(
                f"SELECT canon, display FROM lore_personas WHERE guild_id=? AND canon IN ({ph})",
                (gid, *canons),
            ).fetchall()
        disp_map = {str(r["canon"]): str(r["display"] or r["canon"]) for r in (pr or [])}

        lines = [f"• **{disp_map.get(c, c)}**" for c in canons if c]
        await ctx.send("🎬 Scene mode enabled here:\n" + "\n".join(lines))

    @lore_scene.command(name="show")
    async def lore_scene_show(self, ctx: commands.Context, name: str, n: int = 12):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        n = max(4, min(40, int(n or 12)))

        turns = await self._scene_get_turns(gid, ch_id, th_id, canon, limit=n)
        if not turns:
            await ctx.send("• (no scene turns yet)")
            return

        lines = []
        for r in turns:
            sp = str(r["speaker"] or "").strip() or "?"
            txt = str(r["content"] or "").strip()
            if txt:
                lines.append(f"**{sp}:** {txt}")

        text = "\n".join(lines)
        await ctx.send(text[:1900] if len(text) > 1900 else text)


    @lore_scene.command(name="mode")
    async def lore_scene_mode(self, ctx: commands.Context, name: str, mode: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        m = (mode or "").strip().lower()
        allowed = {"chat", "adventure", "script", "novel"}
        if m not in allowed:
            await ctx.send("❌ Mode must be one of: `chat`, `adventure`, `script`, `novel`")
            return

        await self._scene_meta_set(gid, ch_id, th_id, canon, "mode", m)
        await ctx.send(f"🎬 Mode for **{_clean_text(name)}** here set to **{m}**")


    @lore_scene.command(name="goal")
    async def lore_scene_goal(self, ctx: commands.Context, *, raw: str):
        # Usage: !lore scene goal Name | text...
        if not ctx.guild:
            return
        if "|" not in raw:
            await ctx.send("❌ Usage: `!lore scene goal Name | <goal text>`")
            return
        name, goal = [s.strip() for s in raw.split("|", 1)]
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_meta_set(gid, ch_id, th_id, canon, "goal", goal)
        await ctx.send(f"🧭 Goal set for **{_clean_text(name)}** here.")


    @lore_scene.command(name="style")
    async def lore_scene_style(self, ctx: commands.Context, *, raw: str):
        if not ctx.guild:
            return
        if "|" not in raw:
            await ctx.send("❌ Usage: `!lore scene style Name | <style notes>`")
            return
        name, style = [s.strip() for s in raw.split("|", 1)]
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_meta_set(gid, ch_id, th_id, canon, "style", style)
        await ctx.send(f"🎨 Style set for **{_clean_text(name)}** here.")


    @lore_scene.command(name="bounds")
    async def lore_scene_bounds(self, ctx: commands.Context, *, raw: str):
        if not ctx.guild:
            return
        if "|" not in raw:
            await ctx.send("❌ Usage: `!lore scene bounds Name | <boundaries>`")
            return
        name, bounds = [s.strip() for s in raw.split("|", 1)]
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        await self._scene_meta_set(gid, ch_id, th_id, canon, "bounds", bounds)
        await ctx.send(f"🛡️ Bounds set for **{_clean_text(name)}** here.")


    @lore_scene.command(name="profile")
    async def lore_scene_profile(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)
        meta = await self._scene_meta_all(gid, ch_id, th_id, canon)
        show_keys = ["mode", "goal", "style", "mood", "intent", "bounds"]
        lines = []
        for k in show_keys:
            v = (meta.get(k, "") or "").strip()
            if v:
                lines.append(f"• **{k}**: {v}")
        await ctx.send("🎛️ Scene profile:\n" + ("\n".join(lines) if lines else "• (no settings yet)"))

    async def _scene_delete_last_assistant(self, guild_id: int, channel_id: int, thread_id: int, persona_canon: str) -> None:
        async with self._db_lock:
            conn = self._get_conn()
            row = conn.execute(
                """
                SELECT turn_id
                FROM lore_scene_turns
                WHERE guild_id=? AND channel_id=? AND thread_id=? AND persona_canon=? AND role='assistant'
                ORDER BY created_ts DESC, turn_id DESC
                LIMIT 1
                """,
                (int(guild_id), int(channel_id), int(thread_id), str(persona_canon)),
            ).fetchone()
            if row:
                conn.execute("DELETE FROM lore_scene_turns WHERE guild_id=? AND turn_id=?", (int(guild_id), int(row["turn_id"])))
                conn.commit()


    @lore_scene.command(name="reroll")
    async def lore_scene_reroll(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        last_user = await self._scene_meta_get(gid, ch_id, th_id, canon, "last_user")
        if not last_user:
            await ctx.send("❌ No last prompt stored for this scene yet.")
            return

        # delete last assistant turn so reroll replaces it
        await self._scene_delete_last_assistant(gid, ch_id, th_id, canon)

        # build persona + scene + directives (same as lore_rp)
        row = await self._persona_get(gid, canon)
        if not row:
            await ctx.send("❌ Persona not cached yet for this name. Use `!lore rp Name | ...` once first.")
            return
        persona = str(row["profile"] or "")
        disp = str(row["display"] or name)
        scene_text = await self._scene_memory_text(gid, ch_id, th_id, canon, character_name=disp)
        directives = await self._scene_directives_text(gid, ch_id, th_id, canon)

        rows, _, _ = await self._retrieve_rows_for_question(gid, last_user, fts_limit=200, speaker_limit=200, seed_count=12, win_before=6, win_after=10, max_excerpts=80)

        async with ctx.typing():
            out = await self._openai_rp(name=disp, persona=persona, prompt=last_user, excerpts=rows, scene=scene_text, directives=directives)

        if not out:
            await ctx.send("❌ OpenAI not configured or request failed.")
            return

        await self._scene_meta_set(gid, ch_id, th_id, canon, "last_out", out)
        await self._scene_append_turn(gid, ch_id, th_id, canon, role="assistant", speaker=disp, content=out)
        embed = nextcord.Embed(title=f"🎭 {disp} (reroll)", description=out, color=nextcord.Color.purple())
        await ctx.send(embed=embed)


    @lore_scene.command(name="continue")
    async def lore_scene_continue(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        last_out = await self._scene_meta_get(gid, ch_id, th_id, canon, "last_out")
        if not last_out:
            await ctx.send("❌ No last reply stored for this scene yet.")
            return

        row = await self._persona_get(gid, canon)
        if not row:
            await ctx.send("❌ Persona not cached yet. Use `!lore rp Name | ...` once first.")
            return
        persona = str(row["profile"] or "")
        disp = str(row["display"] or name)
        scene_text = await self._scene_memory_text(gid, ch_id, th_id, canon, character_name=disp)
        directives = await self._scene_directives_text(gid, ch_id, th_id, canon)

        prompt = "Continue your last message without repeating. Pick up naturally from where you left off."

        rows, _, _ = await self._retrieve_rows_for_question(gid, disp, fts_limit=120, speaker_limit=120, seed_count=8, win_before=4, win_after=8, max_excerpts=60)

        async with ctx.typing():
            out = await self._openai_rp(name=disp, persona=persona, prompt=prompt, excerpts=rows, scene=scene_text, directives=directives)

        if not out:
            await ctx.send("❌ OpenAI not configured or request failed.")
            return

        await self._scene_meta_set(gid, ch_id, th_id, canon, "last_out", out)
        await self._scene_append_turn(gid, ch_id, th_id, canon, role="assistant", speaker=disp, content=out)
        embed = nextcord.Embed(title=f"🎭 {disp} (continue)", description=out, color=nextcord.Color.purple())
        await ctx.send(embed=embed)
        
            
    # --- Sticky in-channel RP chat (respond to normal messages) ---

    @lore_scene.group(name="chat", invoke_without_command=True)
    async def lore_scene_chat(self, ctx: commands.Context):
        if not ctx.guild:
            return
        await ctx.send(
            "💬 **Scene Chat** (sticky RP replies to your normal messages here)\n"
            "• `!lore scene chat on <Name>`\n"
            "• `!lore scene chat off [Name]`\n"
            "• `!lore scene chat status [Name]`\n"
        )

    @lore_scene_chat.command(name="on")
    async def lore_scene_chat_on(self, ctx: commands.Context, *, name: str):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        canon = self._persona_canon(name)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        # ensure scene is enabled (chat assumes scene continuity)
        await self._scene_set_enabled(gid, ch_id, th_id, canon, True)

        await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_on", "1")
        await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_user_id", str(int(ctx.author.id)))
        await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_last_ts", "0")

        await ctx.send(
            f"✅ Sticky chat **ON** here for **{_clean_text(name)}**.\n"
            f"Now just talk normally in this channel/thread and I’ll reply in-character.\n"
            f"Stop with: `!lore scene chat off {_clean_text(name)}`"
        )

    @lore_scene_chat.command(name="off")
    async def lore_scene_chat_off(self, ctx: commands.Context, *, name: str = ""):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        name = _clean_text(name or "")
        if name:
            canon = self._persona_canon(name)
            await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_on", "0")
            await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_user_id", "")
            await ctx.send(f"🛑 Sticky chat **OFF** here for **{name}**.")
            return

        # no name: turn off whichever chat you have active here (if any)
        canon = await self._scene_chat_find_active_canon(gid, ch_id, th_id, int(ctx.author.id))
        if not canon:
            await ctx.send("• (no active sticky chat for you in this channel/thread)")
            return
        await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_on", "0")
        await self._scene_meta_set(gid, ch_id, th_id, canon, "chat_user_id", "")
        await ctx.send("🛑 Sticky chat **OFF** here.")

    @lore_scene_chat.command(name="status")
    async def lore_scene_chat_status(self, ctx: commands.Context, *, name: str = ""):
        if not ctx.guild:
            return
        gid = int(ctx.guild.id)
        ch_id, th_id = self._scene_loc_from_ctx(ctx)

        name = _clean_text(name or "")
        if name:
            canon = self._persona_canon(name)
            on = (await self._scene_meta_get(gid, ch_id, th_id, canon, "chat_on")).strip() == "1"
            uid = (await self._scene_meta_get(gid, ch_id, th_id, canon, "chat_user_id")).strip()
            await ctx.send(f"💬 Sticky chat for **{name}** here: **{on}**" + (f" (user_id={uid})" if uid else ""))
            return

        canon = await self._scene_chat_find_active_canon(gid, ch_id, th_id, int(ctx.author.id))
        await ctx.send("💬 Sticky chat here: **ON**" if canon else "💬 Sticky chat here: **OFF**")       
    def _intent_mode(self, question: str, answer_profile: str = "topic") -> str:
        """
        Returns: 'balanced' | 'timeline' | 'recent'
        Used by lore_ask / lore_peek to decide sorting/sampling behavior.
        """
        q = _clean_text(question or "").lower().strip()

        if re.match(r"^(timeline|chrono|chronological)\s*:\s*", q):
            return "timeline"
        if re.match(r"^(recent|latest|newest)\s*:\s*", q):
            return "recent"

        if any(w in q for w in (" timeline", " chronological", " chronologically", " in order")):
            return "timeline"
        if any(w in q for w in (" most recent", " latest", " newest", " recently")):
            return "recent"

        if answer_profile in ("status", "org") or self._question_wants_status(q):
            return "recent"

        return "balanced"

    def _org_priority_rows(self, rows: List[sqlite3.Row], question: str = "") -> List[sqlite3.Row]:
        if not rows:
            return []

        rows_sorted = sorted(rows, key=lambda r: int(r["created_ts"] or 0))
        ql = _clean_text(question).lower()
        q_terms = [t for t in re.findall(r"[a-z0-9']+", ql) if len(t) >= 4 and t not in STOPWORDS]
        org_terms = {
            "order", "church", "corps", "house", "company", "guild", "knights", "mercenary",
            "leadership", "leader", "auctor", "commander", "members", "member", "faction",
            "allies", "alliance", "governs", "governed", "rules", "controls", "influence",
            "territory", "operations", "trade", "supplies", "stationed", "defense", "expansion",
            "founded", "formed", "retaken", "reclaimed", "moved", "relocated", "domain",
        }

        def score(r: sqlite3.Row) -> float:
            txt = _clean_text(r["content"] or "").lower()
            ts = int(r["created_ts"] or 0)
            s = 0.0
            s += ts / 1_000_000_000_000.0
            if any(t in txt for t in org_terms):
                s += 8.0
            if q_terms:
                s += min(6.0, sum(1 for t in q_terms if t in txt) * 2.0)
            if re.search(r"\b(order|church|corps|house|company|guild|knights)\b", txt):
                s += 2.5
            if re.search(r"\b(led by|leader|auctor|member of|part of|serves|served|rules|governs|retook|retaken|reclaimed|moved to|stationed|supplies)\b", txt):
                s += 2.5
            s += min(3.0, len(txt) / 280.0)
            return s

        ranked = sorted(rows_sorted, key=score, reverse=True)
        head = ranked[:48]
        remaining_ids = {int(r["message_id"]) for r in head}
        rest = [r for r in rows_sorted if int(r["message_id"]) not in remaining_ids]
        tail = self._time_stratified_rows(
            rest,
            bins=max(4, int(os.getenv("LORE_TOPIC_STRATA_BINS", "10")) // 2),
            per_bin=max(2, int(os.getenv("LORE_TOPIC_STRATA_PER_BIN", "12")) // 2),
            recent_bonus=max(10, int(os.getenv("LORE_TOPIC_STRATA_RECENT_BONUS", "22"))),
        )
        return sorted(head + tail, key=lambda r: int(r["created_ts"] or 0))

    def _status_priority_rows(self, rows: List[sqlite3.Row], question: str = "") -> List[sqlite3.Row]:
        if not rows:
            return []

        rows_sorted = sorted(rows, key=lambda r: int(r["created_ts"] or 0))
        ql = _clean_text(question).lower()
        q_terms = [t for t in re.findall(r"[a-z0-9']+", ql) if len(t) >= 4 and t not in STOPWORDS]
        status_terms = {
            "alive", "dead", "died", "slain", "killed", "missing", "gone", "lost",
            "destroyed", "burned", "damaged", "ruined", "ashes", "rebuilding", "rebuilt",
            "functional", "inhabited", "occupied", "independent", "control", "controlled",
            "leader", "leadership", "rules", "governs", "governed", "supplies", "stationed",
            "defended", "holds", "held", "condition", "state", "status", "current", "latest",
        }

        def score(r: sqlite3.Row) -> float:
            txt = _clean_text(r["content"] or "").lower()
            ts = int(r["created_ts"] or 0)
            s = 0.0
            s += ts / 1_000_000_000_000.0
            if any(t in txt for t in status_terms):
                s += 8.0
            if q_terms:
                s += min(6.0, sum(1 for t in q_terms if t in txt) * 2.0)
            if re.search(r"\b(still|now|currently|at this point|as of)\b", txt):
                s += 2.0
            s += min(3.0, len(txt) / 280.0)
            return s

        ranked = sorted(rows_sorted, key=score, reverse=True)
        head = ranked[:40]
        remaining_ids = {int(r["message_id"]) for r in head}
        rest = [r for r in rows_sorted if int(r["message_id"]) not in remaining_ids]
        tail = self._time_stratified_rows(
            rest,
            bins=max(4, int(os.getenv("LORE_TOPIC_STRATA_BINS", "10")) // 2),
            per_bin=max(2, int(os.getenv("LORE_TOPIC_STRATA_PER_BIN", "12")) // 2),
            recent_bonus=max(8, int(os.getenv("LORE_TOPIC_STRATA_RECENT_BONUS", "22"))),
        )
        return sorted(head + tail, key=lambda r: int(r["created_ts"] or 0))

    def _bio_priority_rows(self, rows: List[sqlite3.Row], subject_hint: str = "") -> List[sqlite3.Row]:
        if not rows:
            return []

        rows_sorted = sorted(rows, key=lambda r: int(r["created_ts"] or 0))

        import_terms = [
            t.strip().lower()
            for t in (os.getenv(
                "LORE_BIO_IMPORT_TERMS",
                "battle of|siege of|trial of|ritual of|expedition to|"
                "knights of|church of|order of|house of|house|"
                "member of|part of|served with|served in|fought in|"
                "chosen|chosen of|founded|formed|started|healed|restored"
            ).split("|"))
            if t.strip()
        ]

        turning_terms = {
            "met", "joined", "became", "healed", "restored", "revealed",
            "learned", "discovered", "swore", "served", "fought", "battle",
            "church", "knights", "order", "tyrin", "arinock"
        }

        subject_l = _clean_text(subject_hint).lower()

        def score(r: sqlite3.Row) -> float:
            txt = _clean_text(r["content"] or "").lower()
            spk = _clean_text(r["speaker_name"] or "").lower()
            ts = int(r["created_ts"] or 0)

            s = 0.0
            s += ts / 1_000_000_000_000.0

            if subject_l:
                if subject_l in txt:
                    s += 12.0
                if spk == subject_l:
                    s += 10.0

            if any(t in txt for t in import_terms):
                s += 8.0
            if any(t in txt for t in turning_terms):
                s += 4.0
            if '"' in txt:
                s += 1.0

            s += min(3.0, len(txt) / 300.0)
            return s

        ranked = sorted(rows_sorted, key=score, reverse=True)
        head = ranked[:36]

        remaining_ids = {int(r["message_id"]) for r in head}
        rest = [r for r in rows_sorted if int(r["message_id"]) not in remaining_ids]
        tail = self._time_stratified_rows(
            rest,
            bins=int(os.getenv("LORE_ASK_STRATA_BINS", "12")),
            per_bin=max(3, int(os.getenv("LORE_ASK_STRATA_PER_BIN", "10")) // 2),
            recent_bonus=max(6, int(os.getenv("LORE_ASK_STRATA_RECENT_BONUS", "12"))),
        )

        out = []
        seen = set()
        for r in head + tail:
            mid = int(r["message_id"])
            if mid in seen:
                continue
            seen.add(mid)
            out.append(r)
        return out

def setup(bot: commands.Bot):
    bot.add_cog(LoreCog(bot))
