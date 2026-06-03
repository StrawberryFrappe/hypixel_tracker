import asyncio
import logging
import os
import re
from datetime import datetime, timezone

import discord
import httpx
import psycopg2
import psycopg2.extras
from discord.ext import commands, tasks
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("bazaar.discord_bot")

DISCORD_BOT_TOKEN = os.environ.get("DISCORD_BOT_TOKEN", "")
ALLOWED_USER_IDS = {
    int(value.strip())
    for value in os.environ.get("DISCORD_ALLOWED_USER_IDS", "").split(",")
    if value.strip().isdigit()
}
ALLOWED_GUILD_IDS = {
    int(value.strip())
    for value in os.environ.get("DISCORD_ALLOWED_GUILD_IDS", "").split(",")
    if value.strip().isdigit()
}
ALERT_CHANNEL_ID = int(os.environ.get("DISCORD_ALERT_CHANNEL_ID", "0") or "0")

API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000").rstrip("/")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-latest")
DISK_ALERT_PERCENT = float(os.environ.get("DISK_ALERT_PERCENT", "80"))

FORBIDDEN_SQL = re.compile(
    r"\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|copy|vacuum|analyze|call|do)\b",
    re.IGNORECASE,
)

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


def is_allowed(ctx: commands.Context) -> bool:
    if ALLOWED_USER_IDS and ctx.author.id not in ALLOWED_USER_IDS:
        return False
    if ALLOWED_GUILD_IDS and ctx.guild and ctx.guild.id not in ALLOWED_GUILD_IDS:
        return False
    return True


async def api_get(path: str) -> dict:
    async with httpx.AsyncClient(timeout=30.0) as client:
        response = await client.get(f"{API_BASE_URL}{path}", headers={"X-API-Key": API_KEY})
        response.raise_for_status()
        return response.json()


async def api_post(path: str, params: dict | None = None) -> dict:
    async with httpx.AsyncClient(timeout=120.0) as client:
        response = await client.post(f"{API_BASE_URL}{path}", params=params or {}, headers={"X-API-Key": API_KEY})
        response.raise_for_status()
        return response.json()


def format_bytes(value: int | float | None) -> str:
    if value is None:
        return "unknown"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    value = float(value)
    for unit in units:
        if abs(value) < 1024 or unit == units[-1]:
            return f"{value:.2f} {unit}"
        value /= 1024
    return f"{value:.2f} TiB"


def format_hypixel_ms(value: int) -> str:
    return datetime.fromtimestamp(value / 1000, tz=timezone.utc).strftime("%d/%m/%Y")


def validate_select_sql(sql: str) -> str:
    statement = sql.strip().rstrip(";")
    lowered = statement.lower()
    if not (lowered.startswith("select ") or lowered.startswith("with ")):
        raise ValueError("Only SELECT/CTE queries are allowed.")
    if FORBIDDEN_SQL.search(statement):
        raise ValueError("Mutation, DDL, COPY, and maintenance statements are blocked.")
    if ";" in statement:
        raise ValueError("Only one SQL statement is allowed.")
    return statement


def run_readonly_sql(sql: str) -> list[dict]:
    statement = validate_select_sql(sql)
    limited = f"SELECT * FROM ({statement}) AS sandbox_query LIMIT 25"
    with psycopg2.connect(POSTGRES_URI) as conn:
        conn.set_session(readonly=True, autocommit=True)
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SET statement_timeout = 5000")
            cur.execute(limited)
            return [dict(row) for row in cur.fetchall()]


async def ask_anthropic(question: str, context: dict) -> str:
    if not ANTHROPIC_API_KEY:
        return "Anthropic is not configured. Set ANTHROPIC_API_KEY to enable natural-language analysis."
    prompt = (
        "You are a concise Hypixel SkyBlock Bazaar database and data-science expert. "
        "Use the provided archive context. Do not claim live facts that are not in the context. "
        "If deeper analysis needs SQL, suggest the exact read-only SQL query.\n\n"
        f"Archive context:\n{context}\n\nQuestion: {question}"
    )
    async with httpx.AsyncClient(timeout=60.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 800,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        response.raise_for_status()
        data = response.json()
    return "".join(block.get("text", "") for block in data.get("content", []) if block.get("type") == "text")


@bot.check
async def globally_allowed(ctx: commands.Context) -> bool:
    if is_allowed(ctx):
        return True
    logger.warning("Rejected Discord command from user=%s guild=%s", ctx.author.id, ctx.guild.id if ctx.guild else None)
    return False


@bot.event
async def on_ready() -> None:
    logger.info("Discord bot connected as %s", bot.user)
    disk_pressure_watch.start()


@bot.command(name="status")
async def status(ctx: commands.Context) -> None:
    data = await api_get("/storage/status")
    disk = data["disk"]
    await ctx.reply(
        "\n".join(
            [
                f"Snapshots: {data['snapshot_count']}",
                f"DB size: {format_bytes(data['database_size_bytes'])}",
                f"Disk used: {disk['used_percent']}% ({format_bytes(disk['free_bytes'])} free)",
                f"Estimated remaining: {data['estimated_days_remaining_at_current_average']} days",
            ]
        )
    )


@bot.command(name="backup")
async def backup(ctx: commands.Context, days: int = 7) -> None:
    data = await api_post("/exports", {"days": days, "expires_in_days": 7})
    export = data["export"]
    await ctx.reply(
        "Backup from "
        f"{format_hypixel_ms(export['start_source_last_updated'])} "
        f"to {format_hypixel_ms(export['end_source_last_updated'])} is ready: "
        f"{export['download_url']}. Expected deletion: "
        f"{datetime.fromisoformat(export['expires_at']).strftime('%d/%m/%Y')}."
    )


@bot.command(name="sql")
async def sql(ctx: commands.Context, *, query: str) -> None:
    try:
        rows = await asyncio.to_thread(run_readonly_sql, query)
    except Exception as exc:
        await ctx.reply(f"SQL rejected or failed: {exc}")
        return
    text = str(rows[:5])
    if len(text) > 1800:
        text = text[:1800] + "..."
    await ctx.reply(f"```json\n{text}\n```")


@bot.command(name="ask")
async def ask(ctx: commands.Context, *, question: str) -> None:
    storage = await api_get("/storage/status")
    latest = await api_get("/snapshots?limit=1")
    context = {
        "storage": storage,
        "latest_snapshot_metadata": latest.get("snapshots", [{}])[0] if latest.get("snapshots") else None,
    }
    answer = await ask_anthropic(question, context)
    await ctx.reply(answer[:1900])


@tasks.loop(minutes=30)
async def disk_pressure_watch() -> None:
    if not ALERT_CHANNEL_ID:
        return
    try:
        data = await api_get("/storage/status")
        disk = data["disk"]
        if disk["used_percent"] < DISK_ALERT_PERCENT:
            return
        channel = bot.get_channel(ALERT_CHANNEL_ID)
        if channel:
            await channel.send(
                f"Disk pressure alert: {disk['used_percent']}% used, "
                f"{format_bytes(disk['free_bytes'])} free, "
                f"estimated {data['estimated_days_remaining_at_current_average']} archive days remaining."
            )
    except Exception:
        logger.exception("Disk pressure watch failed")


def main() -> None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is required for the Discord bot service")
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
