import asyncio
import json
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


# --------------------------------------------------------------------------- #
# Access control
# --------------------------------------------------------------------------- #
def is_allowed(author_id: int, guild_id: int | None) -> bool:
    if ALLOWED_USER_IDS and author_id not in ALLOWED_USER_IDS:
        return False
    if ALLOWED_GUILD_IDS and guild_id is not None and guild_id not in ALLOWED_GUILD_IDS:
        return False
    return True


# --------------------------------------------------------------------------- #
# Backend helpers (API + read-only SQL + product lookup)
# --------------------------------------------------------------------------- #
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


async def get_product_quote(product_id: str) -> dict:
    pid = product_id.strip().upper()
    data = await api_get("/snapshots/latest")
    products = data.get("payload", {}).get("products", {})
    info = products.get(pid)
    if not info:
        sample = list(products)[:10]
        return {"error": f"Product '{pid}' not found in latest snapshot.", "example_product_ids": sample}
    return {
        "product_id": pid,
        "quick_status": info.get("quick_status", {}),
        "snapshot_fetched_at": data.get("metadata", {}).get("fetched_at"),
    }


# --------------------------------------------------------------------------- #
# Natural-language layer (Anthropic tool use)
# --------------------------------------------------------------------------- #
SYSTEM_PROMPT = (
    "You are the private operations and market-analysis assistant for a Hypixel SkyBlock "
    "Bazaar raw-archive service. You help exactly one operator manage and query their own "
    "archive. Be concise and practical.\n\n"
    "Scope: you only know about this Bazaar archive. Do NOT answer about real-world stock "
    "markets, news, or general topics, and never invent live facts. If asked something "
    "outside the Bazaar archive, say it is out of scope.\n\n"
    "Use the tools to get real data before answering. The archive stores raw gzipped "
    "Hypixel Bazaar API responses; per-product prices live inside snapshot payloads (use "
    "get_product_quote), while SQL only sees snapshot/export metadata tables. When you "
    "create a backup, always give the operator the download link and its expiry."
)

TOOLS = [
    {
        "name": "get_archive_status",
        "description": "Get current archive and disk status: snapshot count, database size, disk usage percent, free space, and estimated days of storage remaining.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "create_backup",
        "description": "Create a compressed range backup (.jsonl.gz) of recent snapshots and return an expiring signed download link.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "How many days back to include (1-90).", "default": 7},
                "expires_in_days": {"type": "integer", "description": "Days until the download link expires (1-30).", "default": 7},
            },
            "required": [],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Run ONE read-only SELECT/WITH query against archive metadata tables. "
            "Tables: raw_bazaar_snapshots(id, endpoint, fetched_at, source_last_updated, response_hash, "
            "status_code, payload_size_bytes, compressed_size_bytes) and export_bundles. "
            "Per-product prices are NOT here (they are inside compressed payloads). Returns up to 25 rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "A single read-only SELECT or WITH statement."}},
            "required": ["query"],
        },
    },
    {
        "name": "get_product_quote",
        "description": "Get the latest Bazaar quick-status (buy/sell price, volumes, orders) for one product id (e.g. ENCHANTED_DIAMOND, INK_SACK:3) from the most recent archived snapshot.",
        "input_schema": {
            "type": "object",
            "properties": {"product_id": {"type": "string", "description": "Hypixel Bazaar product id."}},
            "required": ["product_id"],
        },
    },
]


async def run_tool(name: str, tool_input: dict) -> str:
    try:
        if name == "get_archive_status":
            result = await api_get("/storage/status")
        elif name == "create_backup":
            days = int(tool_input.get("days", 7))
            expires = int(tool_input.get("expires_in_days", 7))
            result = await api_post("/exports", {"days": days, "expires_in_days": expires})
        elif name == "run_sql":
            rows = await asyncio.to_thread(run_readonly_sql, tool_input.get("query", ""))
            result = {"rows": rows, "row_count": len(rows)}
        elif name == "get_product_quote":
            result = await get_product_quote(tool_input.get("product_id", ""))
        else:
            result = {"error": f"Unknown tool {name}"}
    except Exception as exc:  # noqa: BLE001 - surface tool errors back to the model
        result = {"error": str(exc)}
    return json.dumps(result, default=str)


async def anthropic_request(messages: list[dict]) -> dict:
    async with httpx.AsyncClient(timeout=90.0) as client:
        response = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": ANTHROPIC_MODEL,
                "max_tokens": 1024,
                "system": SYSTEM_PROMPT,
                "tools": TOOLS,
                "messages": messages,
            },
        )
        response.raise_for_status()
        return response.json()


async def handle_request(user_text: str) -> str:
    if not ANTHROPIC_API_KEY:
        return "Natural-language mode is unavailable: ANTHROPIC_API_KEY is not set."

    messages: list[dict] = [{"role": "user", "content": user_text}]
    for _ in range(6):
        data = await anthropic_request(messages)
        blocks = data.get("content", [])
        if data.get("stop_reason") == "tool_use":
            messages.append({"role": "assistant", "content": blocks})
            tool_results = []
            for block in blocks:
                if block.get("type") == "tool_use":
                    logger.info("Tool call: %s %s", block.get("name"), block.get("input"))
                    output = await run_tool(block["name"], block.get("input", {}))
                    tool_results.append(
                        {"type": "tool_result", "tool_use_id": block["id"], "content": output}
                    )
            messages.append({"role": "user", "content": tool_results})
            continue
        text = "".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        return text or "(no response)"
    return "Stopped after too many reasoning steps. Try a more specific request."


async def send_chunked(channel, text: str) -> None:
    for i in range(0, len(text), 1900):
        await channel.send(text[i : i + 1900])


# --------------------------------------------------------------------------- #
# Discord events
# --------------------------------------------------------------------------- #
@bot.event
async def on_ready() -> None:
    logger.info("Discord bot connected as %s", bot.user)
    if not disk_pressure_watch.is_running():
        disk_pressure_watch.start()


@bot.event
async def on_message(message: discord.Message) -> None:
    if message.author.bot or (bot.user and message.author.id == bot.user.id):
        return
    guild_id = message.guild.id if message.guild else None
    if not is_allowed(message.author.id, guild_id):
        return
    content = message.content.strip()
    if not content:
        return
    async with message.channel.typing():
        try:
            reply = await handle_request(content)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Request handling failed")
            reply = f"Sorry, I hit an error handling that: {exc}"
    await send_chunked(message.channel, reply)


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
