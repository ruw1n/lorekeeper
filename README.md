# Lorekeeper Discord Bot

A self-hosted Discord bot for roleplay servers that archives in-character channels and turns them into a searchable, AI-assisted living wiki.

It was originally built for a long-running D&D / roleplay server where players kept asking things like:

- “What happened to this NPC?”
- “Where did this faction first appear?”
- “What did my character say about that prophecy six months ago?”
- “Can the bot help me write in this character’s voice?”

Lorekeeper stores selected Discord messages in a local SQLite database, builds searchable lore context, and can answer questions with excerpts from the archived server history.

> **Status:** hobby project / experimental community release.  
> It is provided as-is for people comfortable running their own Discord bot.

---

## What it does

- Archives messages from selected RP/lore channels.
- Supports backfilling old messages into a local SQLite database.
- Searches archived lore with SQLite FTS.
- Answers lore questions with AI-assisted retrieval.
- Shows source-style Discord jump links when possible.
- Builds rough “lore maps” of people, organizations, events, and named things.
- Can build character/persona summaries from archived posts.
- Includes optional RP/scene helper tools for generating in-character-ish replies or scene continuations.
- Includes a small D&D 5E `!challenge` helper that suggests skill checks and DCs for a situation.
- Can piggyback on RPXP-flagged channels if you use the included RPXP cog.

This bot is best thought of as a **living wiki assistant**, not an authority. It can retrieve, summarize, and connect server lore, but a human DM/admin should still decide what is canon.

---

## Important warnings

### Do not commit secrets

Never upload your `.env`, bot token, OpenAI API key, or live SQLite database to GitHub.

At minimum, add this to `.gitignore`:

```gitignore
.env
*.sqlite3
*.sqlite3-*
data/
__pycache__/
*.pyc
```

### This stores Discord message content

Lorekeeper saves roleplay/lore messages locally. Depending on your server, those messages may include private character writing, player conversations, sensitive story material, or NSFW content.

Before using this bot on a public or semi-public server:

- Tell your players what is being archived.
- Archive only the channels you actually need.
- Do not archive private channels without consent.
- Be careful sharing your database with anyone.
- Consider disabling RP helper features if your server does not want AI-generated character text.

### AI answers can be wrong

The bot retrieves context and asks an AI model to summarize or answer. It may misunderstand, overstate, merge similar names, or miss older context. Treat answers as “research assistant output,” not final canon.

---

## Requirements

- Python 3.10+ recommended
- A Discord bot token
- Message Content Intent enabled for your bot
- An OpenAI API key for AI-assisted features
- SQLite, included with Python

Python packages used by the included cogs:

```txt
nextcord
python-dotenv
openai
```

Optional: create a `requirements.txt` containing the above packages.

---

## Repository layout

Recommended layout:

```txt
lorekeeper-bot/
├─ bot.py
├─ requirements.txt
├─ .env.example
├─ .gitignore
├─ data/
│  └─ .gitkeep
└─ cogs/
   ├─ lore.py
   ├─ rpxp.py
   ├─ park.py
   └─ patrol.py
```

The included `bot.py` expects the cog files to be importable as `cogs.lore`, `cogs.rpxp`, `cogs.park`, and `cogs.patrol`, so make sure they are inside a `cogs/` folder.

If you only want the lore/wiki functionality, you can disable the extra cogs by commenting out their `bot.load_extension(...)` lines in `bot.py`.

---

## Install

Clone the repo:

```bash
git clone https://github.com/YOURNAME/lorekeeper-bot.git
cd lorekeeper-bot
```

Create a virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Create your environment file:

```bash
cp .env.example .env
nano .env
```

Run the bot:

```bash
python bot.py
```

---

## Minimal `.env.example`

```env
# Discord
DISCORD_TOKEN=put_your_discord_bot_token_here
RPXP_PREFIX=!

# Local database
LORE_DB_PATH=data/lore.sqlite3

# OpenAI
OPENAI_API_KEY=put_your_openai_api_key_here
LORE_MODEL=gpt-4o-mini
CHALLENGE_MODEL=gpt-4o-mini

# Optional: allow or deny specific bot/webhook IDs during archive ingestion.
# Useful if your RP server uses proxy bots, dice bots, or logging bots.
LORE_ALLOW_BOT_IDS=
LORE_DENY_BOT_IDS=

# Safer public default. Turn on only if your server explicitly wants this.
LORE_RP_ALLOW_NSFW=0

# Debugging
LORE_OPENAI_DEBUG=0
```

Your private server may have many more tuning knobs. For a public release, start minimal and only expose advanced settings after people understand the basics.

---

## Useful optional settings

These are not required, but may be useful for larger servers.

```env
# Lore question answering
LORE_ASK_MAX_OUTPUT_TOKENS=2400
LORE_OPENAI_EXCERPT_CHARS=1400
LORE_OPENAI_SOURCES_CHARS=60000
LORE_ASK_FTS_LIMIT=200
LORE_ASK_SPEAKER_LIMIT=200
LORE_ASK_MAX_EXCERPTS=110

# Retrieval sampling
LORE_ASK_STRATA_BINS=12
LORE_ASK_STRATA_PER_BIN=10
LORE_ASK_STRATA_RECENT_BONUS=12
LORE_ASK_IDS_CAP=2500

# Bio/persona/thread context
LORE_BIO_KEEP_THREAD_CONTEXT=1
LORE_BIO_THREAD_MAX=10
LORE_BIO_THREAD_HEAD_N=6
LORE_BIO_THREAD_TAIL_N=6
LORE_BIO_THREAD_SAMPLE_N=20
LORE_BIO_PER_THREAD_CAP=28

# Topic retrieval
LORE_TOPIC_STRATA_BINS=10
LORE_TOPIC_STRATA_PER_BIN=12
LORE_TOPIC_STRATA_RECENT_BONUS=22
LORE_TOPIC_CONTEXT_RADIUS=5

# Scene/RP helpers
LORE_SCENE_DEFAULT_ON=0
LORE_SCENE_SUMMARIZE_ON=1
LORE_SCENE_MAX_TURNS=24
LORE_SCENE_MAX_CHARS=3500
LORE_RP_TEMPERATURE=0.75
LORE_RP_MAX_OUTPUT_TOKENS=700
LORE_RP_DYNAMIC_LEN=1
LORE_RP_CHAT_MIN_CHARS=120
LORE_RP_CHAT_MAX_CHARS=1400
LORE_RP_VOICE_SAMPLES=6
```

Notes:

- Larger limits may improve answers but can increase API cost.
- More excerpts are not always better; too much context can make answers muddier.
- Keep only one `LORE_DB_PATH` line in your `.env`.
- For a public template, avoid shipping server-specific aliases, character names, faction names, or private lore terms.

---

## Discord bot setup

In the Discord Developer Portal:

1. Create an application.
2. Add a bot user.
3. Copy the bot token into `.env` as `DISCORD_TOKEN`.
4. Enable **Message Content Intent**.
5. Invite the bot to your server with permissions to:
   - Read messages/view channels
   - Send messages
   - Read message history
   - Embed links
   - Use external emojis, if desired
   - Manage messages only if you add features that require it

The bot can only archive channels it can see.

---

## OpenAI API setup

Lorekeeper's archive/backfill features use SQLite and Discord only, but the living-wiki answers, challenge helper, persona summaries, and RP/scene helpers need an OpenAI API key.

### 1. Create an OpenAI API key

1. Go to the OpenAI Platform dashboard.
2. Create or select a project.
3. Create an API key for that project.
4. Copy it immediately and keep it somewhere safe. You may not be able to view the full key again later.
5. Put the key in your local `.env` file:

```env
OPENAI_API_KEY=sk-your-key-here
```

Do **not** paste your API key into Discord, commit it to GitHub, or share it with server members. Anyone with that key may be able to spend your API credits.

### 2. Add billing / credits if needed

OpenAI API usage is separate from a normal ChatGPT subscription. If API calls fail even though your key is correct, check your Platform billing, project limits, and model access.

### 3. Pick models

The public template uses a smaller model by default so hobby servers do not accidentally burn money while testing:

```env
LORE_MODEL=gpt-4o-mini
CHALLENGE_MODEL=gpt-4o-mini
```

You can use a stronger model for lore answers if your API account supports it:

```env
LORE_MODEL=gpt-4o
CHALLENGE_MODEL=gpt-4o-mini
```

General advice:

- Use a smaller model while setting the bot up.
- Use `!lore peek <query>` before repeatedly asking broad lore questions.
- Lower context/output limits if your server is large.
- Only raise model/context settings after you understand your usage.

### 4. Install the Python SDK

If you installed `requirements.txt`, this should already be done. Otherwise:

```bash
pip install openai
```

### 5. Test the key outside Discord

A quick smoke test:

```bash
source .venv/bin/activate
python - <<'EOF'
from openai import OpenAI
client = OpenAI()
resp = client.responses.create(
    model="gpt-4o-mini",
    input="Reply with exactly: Lorekeeper API test OK"
)
print(resp.output_text)
EOF
```

If that works, restart the Discord bot and try:

```txt
!challenge sneaking past castle guards
```

Then try a lore command after you have added/backfilled at least one channel:

```txt
!lore ask What has happened recently?
```

### 6. Common API problems

- **Missing key:** make sure `.env` has `OPENAI_API_KEY=...` and restart the bot.
- **Wrong environment:** make sure you are running the bot from the folder that contains `.env`.
- **No credits / billing issue:** check OpenAI Platform billing and project limits.
- **Model not available:** switch `LORE_MODEL` and `CHALLENGE_MODEL` to a model your account can access.
- **Costs too high:** lower `LORE_OPENAI_SOURCES_CHARS`, `LORE_ASK_MAX_EXCERPTS`, and `LORE_ASK_MAX_OUTPUT_TOKENS`.

---

## First-time use

Start the bot:

```bash
python bot.py
```

In Discord, choose which channels should be part of the lore archive.

To add the current channel explicitly:

```txt
!lore add
```

To remove the current channel:

```txt
!lore remove
```

To see the current scope:

```txt
!lore channels
```

To backfill old messages from the configured scope:

```txt
!lore backfill
```

To check progress:

```txt
!lore status
```

To force a fuller rescan:

```txt
!lore backfill full
```

Backfills can take a while on large servers and may be limited by Discord API rate limits. Start with one or two channels before pointing it at years of server history.

---

## Core commands

### Lore archive

```txt
!lore
!lore add
!lore remove
!lore channels
!lore use_rpxp on
!lore use_rpxp off
!lore backfill
!lore backfill full
!lore status
!lore reindex
```

### Ask the living wiki

```txt
!lore ask <question>
```

Examples:

```txt
!lore ask Who is Lady K'lliara?
!lore ask What happened at the Battle of Arinock?
!lore ask timeline: what happened to the ghostflame?
!lore ask recent: what has Faluzure been doing lately?
```

The bot works best when names, places, or events are spelled clearly. If your server has many characters with similar names, include extra context.

### Peek at retrieved context

```txt
!lore peek <query>
```

Useful for debugging what the bot is finding before it sends context to the AI model.

### Lore map

```txt
!lore map
!lore map build
!lore map top
!lore map top people 15
!lore map top orgs 10
!lore map show <name>
```

The lore map is a rough relationship graph built from archived mentions. It is useful for discovery, not perfect canon tracking.

### Personas

```txt
!lore persona
!lore persona list
!lore persona build <name>
!lore persona show <name>
!lore persona clear <name>
```

Personas summarize how a character or speaker tends to appear in archived posts. Use with care; it is not a replacement for player consent or actual character sheets.

### RP/scene helpers

```txt
!lore rp <prompt>
!lore scene
!lore scene on
!lore scene off
!lore scene clear
!lore scene status
!lore scene show
!lore scene mode <mode>
!lore scene goal <goal>
!lore scene style <style>
!lore scene bounds <boundaries>
!lore scene profile <profile>
!lore scene continue
!lore scene reroll
!lore scene chat on
!lore scene chat off
!lore scene chat status
```

For public/community use, consider disabling or hiding these until server admins understand what they do.

### D&D helper

```txt
!challenge <situation>
```

Example:

```txt
!challenge sneaking into a noble's masquerade ball
```

The bot suggests three relevant 5E skills, difficulties, DCs, and short reasoning.

---

## RPXP integration

The lore cog can piggyback on channels already flagged by the included RPXP cog.

If enabled:

```txt
!lore use_rpxp on
```

Lorekeeper will include RPXP-flagged channels in its archive scope.

If disabled:

```txt
!lore use_rpxp off
```

Lorekeeper will only use channels explicitly added with:

```txt
!lore add
```

This is useful if you want XP tracking and lore archiving to cover different channels.

---

## Tips for better lore answers

- Archive clean RP/lore channels, not every off-topic chat channel.
- Backfill before asking broad history questions.
- Use specific names and event terms.
- Ask timeline questions with `timeline:` when chronology matters.
- Ask recent-status questions with `recent:` when you care about newer posts.
- Use `!lore peek` when answers seem wrong.
- Rebuild or reindex after major imports.
- Keep aliases and server-specific terms in your private `.env`, not the public repo.

---

## Cost control

AI-assisted commands use your OpenAI API key. Costs depend on the model, how many excerpts are retrieved, and how large your context limits are.

To reduce cost:

- Use a smaller default model.
- Lower `LORE_OPENAI_SOURCES_CHARS`.
- Lower `LORE_ASK_MAX_EXCERPTS`.
- Lower `LORE_ASK_MAX_OUTPUT_TOKENS`.
- Use `!lore peek` before running broad questions repeatedly.
- Disable scene/RP helpers if your server does not need them.

---

## Troubleshooting

### The bot starts but does not read messages

Check that:

- Message Content Intent is enabled in the Discord Developer Portal.
- The bot has permission to view the channel.
- The bot has permission to read message history.
- The channel has been added with `!lore add`, or RPXP scope is enabled and the channel is RPXP-flagged.

### `!lore ask` says OpenAI is not configured

Check that:

- `.env` contains `OPENAI_API_KEY`.
- You restarted the bot after editing `.env`.
- The `openai` Python package is installed.
- Your selected model is available to your API account.

### Backfill finds fewer messages than expected

Check that:

- The bot can see the channel/thread.
- The channel is inside the lore scope.
- You are not denying the bot/proxy ID that posts your RP messages.
- Threads are included and accessible.
- The messages are not empty/attachment-only messages that your config skips.

### Answers feel too recent-only

Increase retrieval spread settings such as:

```env
LORE_ASK_STRATA_BINS=12
LORE_ASK_STRATA_PER_BIN=10
LORE_ASK_STRATA_RECENT_BONUS=12
LORE_ASK_IDS_CAP=2500
```

Then test with `!lore peek <query>`.

### Answers are too long or expensive

Lower:

```env
LORE_ASK_MAX_OUTPUT_TOKENS
LORE_OPENAI_SOURCES_CHARS
LORE_ASK_MAX_EXCERPTS
```

---

## Development notes

This was built as a practical server tool, not a polished SaaS product. Expect rough edges.

Good future improvements:

- Slash command support.
- Cleaner setup wizard.
- Per-server admin dashboard.
- Better export/import tooling.
- Better permission controls for AI/RP features.
- Dockerfile and systemd examples.
- Tests for retrieval behavior.
- Safer defaults for public servers.

Pull requests are welcome, but please keep the project self-hosted and privacy-conscious.

---

## License

Recommended layout:

```txt
LICENSE.txt
LICENSES/
└─ LICENSE-MIT
```

Suggested `LICENSE.txt`:

```txt
Lorekeeper Discord Bot license summary

• Source code (Python): MIT — see LICENSES/LICENSE-MIT
• Example configuration files: MIT — see LICENSES/LICENSE-MIT
• Documentation: MIT — see LICENSES/LICENSE-MIT

This repository does not include your server lore, Discord message archive, SQLite database, private prompts, API keys, bot tokens, or campaign setting material. Do not commit those.
```

Use the standard MIT license text in `LICENSES/LICENSE-MIT`, replacing the year/name with your own.

If you later add art, setting lore, character writing, campaign material, or other non-code assets, consider licensing those separately. Code and private campaign content are not the same thing.

---

## Support

This is a free community release. I cannot promise setup help, hosting help, custom features, or emergency debugging for your server.

You are welcome to fork it, modify it, and adapt it to your own campaign.

If it helps your server, great. That is exactly why I shared it.
