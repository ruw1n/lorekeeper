import os
import nextcord
from nextcord.ext import commands
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN missing. Put it in .env or your environment.")

PREFIX = os.getenv("RPXP_PREFIX", "!")
DATA_PATH = os.getenv("RPXP_DATA_PATH", "data/rpxp.json")

intents = nextcord.Intents.default()
intents.message_content = True
intents.reactions = True
intents.guilds = True

bot = commands.Bot(command_prefix=PREFIX, intents=intents)

@bot.event
async def on_ready():
    print(f"✅ RPXP online as {bot.user} ({bot.user.id}) | prefix={PREFIX} | data={DATA_PATH}")

bot.load_extension("cogs.rpxp")
bot.load_extension("cogs.lore")
bot.load_extension("cogs.park")

bot.run(TOKEN)
