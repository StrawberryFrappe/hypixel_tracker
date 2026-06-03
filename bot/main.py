import asyncio
import gzip
import json
import logging
import os
from collections import deque

import discord
import httpx
import psycopg2
import psycopg2.extras
from discord.ext import tasks
from discord.ext import commands
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

API_BASE_URL = os.environ.get("API_BASE_URL", "http://api:8000").rstrip("/")
API_KEY = os.environ.get("API_KEY", "dev-api-key")
# Data work runs as the least-privilege `agent` role (read canonical, read/write sandbox).
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/bazaar_data")
AGENT_POSTGRES_URI = os.environ.get("AGENT_POSTGRES_URI", POSTGRES_URI)
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
ANTHROPIC_MODEL = os.environ.get("ANTHROPIC_MODEL", "claude-3-5-haiku-latest")
DISK_ALERT_PERCENT = float(os.environ.get("DISK_ALERT_PERCENT", "80"))
MEMORY_TURNS = int(os.environ.get("BOT_MEMORY_TURNS", "10"))
SQL_TIMEOUT_MS = int(os.environ.get("BOT_SQL_TIMEOUT_MS", "10000"))
SQL_ROW_LIMIT = int(os.environ.get("BOT_SQL_ROW_LIMIT", "50"))

intents = discord.Intents.default()
intents.message_content = True
bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)

# Per-channel short rolling conversation memory (cleared on restart).
CHANNEL_MEMORY: dict[int, deque] = {}


def history_for(channel_id: int) -> deque:
    return CHANNEL_MEMORY.setdefault(channel_id, deque(maxlen=MEMORY_TURNS * 2))


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
# Backend helpers (API + sandbox SQL + snapshot loading)
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


PRODUCT_PRICES_DDL = """
CREATE TABLE IF NOT EXISTS sandbox.product_prices (
  product_id text NOT NULL,
  source_last_updated bigint NOT NULL,
  fetched_at timestamptz,
  sell_price double precision,
  buy_price double precision,
  sell_volume bigint,
  buy_volume bigint,
  sell_moving_week bigint,
  buy_moving_week bigint,
  sell_orders bigint,
  buy_orders bigint,
  PRIMARY KEY (product_id, source_last_updated)
)
"""


def run_sql(query: str) -> dict:
    statement = (query or "").strip()
    if not statement:
        raise ValueError("Empty query.")
    conn = psycopg2.connect(AGENT_POSTGRES_URI)
    try:
        conn.autocommit = True
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(f"SET statement_timeout = {SQL_TIMEOUT_MS}")
            cur.execute(statement)
            if cur.description is not None:
                rows = cur.fetchmany(SQL_ROW_LIMIT)
                total = cur.rowcount if cur.rowcount is not None else len(rows)
                return {
                    "columns": [d[0] for d in cur.description],
                    "rows": [dict(r) for r in rows],
                    "returned_rows": len(rows),
                    "total_rows": total,
                    "truncated": total > len(rows),
                }
            return {"status": "ok", "rowcount": cur.rowcount}
    finally:
        conn.close()


def load_market_snapshot(count: int = 1, snapshot_id: int | None = None) -> dict:
    conn = psycopg2.connect(AGENT_POSTGRES_URI)
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(PRODUCT_PRICES_DDL)
            if snapshot_id is not None:
                cur.execute(
                    "SELECT id, source_last_updated, fetched_at, payload_gzip "
                    "FROM public.raw_bazaar_snapshots WHERE id = %s",
                    (snapshot_id,),
                )
            else:
                cur.execute(
                    "SELECT id, source_last_updated, fetched_at, payload_gzip "
                    "FROM public.raw_bazaar_snapshots ORDER BY id DESC LIMIT %s",
                    (max(1, min(int(count), 200)),),
                )
            snaps = cur.fetchall()
            if not snaps:
                return {"loaded_snapshots": 0, "message": "No snapshots available to load."}

            attempted = 0
            for sid, slu, fetched_at, payload_gzip in snaps:
                data = json.loads(gzip.decompress(bytes(payload_gzip)).decode("utf-8"))
                products = data.get("products", {})
                values = []
                for pid, info in products.items():
                    qs = info.get("quick_status", {})
                    values.append(
                        (
                            pid, slu, fetched_at,
                            qs.get("sellPrice"), qs.get("buyPrice"),
                            qs.get("sellVolume"), qs.get("buyVolume"),
                            qs.get("sellMovingWeek"), qs.get("buyMovingWeek"),
                            qs.get("sellOrders"), qs.get("buyOrders"),
                        )
                    )
                if values:
                    psycopg2.extras.execute_values(
                        cur,
                        "INSERT INTO sandbox.product_prices (product_id, source_last_updated, "
                        "fetched_at, sell_price, buy_price, sell_volume, buy_volume, "
                        "sell_moving_week, buy_moving_week, sell_orders, buy_orders) VALUES %s "
                        "ON CONFLICT (product_id, source_last_updated) DO NOTHING",
                        values,
                    )
                    attempted += len(values)
            cur.execute("SELECT count(*) FROM sandbox.product_prices")
            total_in_table = cur.fetchone()[0]
            return {
                "loaded_snapshots": len(snaps),
                "snapshot_ids": [s[0] for s in snaps],
                "product_rows_attempted": attempted,
                "sandbox_product_prices_total_rows": total_in_table,
                "note": "Per-product rows are in sandbox.product_prices; analyze it with run_sql.",
            }
    finally:
        conn.close()


async def get_product_quote(product_id: str) -> dict:
    pid = product_id.strip().upper()
    data = await api_get("/snapshots/latest")
    products = data.get("payload", {}).get("products", {})
    info = products.get(pid)
    if not info:
        return {"error": f"Product '{pid}' not found in latest snapshot.", "example_product_ids": list(products)[:10]}
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
    "Bazaar raw-archive service, working for a single operator. Act like a diligent "
    "accountant: be proactive, do the work yourself, and keep your own books.\n\n"
    "Scope: ONLY the Hypixel Bazaar archive. Never answer about real-world stock markets, "
    "news, or general topics, and never invent live facts. If a request is out of scope, "
    "say so in one sentence.\n\n"
    "How you work:\n"
    "- Prefer doing over asking. To answer market questions, call load_market_snapshot to "
    "materialize data, then run_sql to compute the answer. Do NOT ask the operator to pick "
    "products for you.\n"
    "- You own a writable Postgres schema `sandbox`. Build and maintain your own tables there "
    "(e.g. sandbox.product_prices, derived rollups, watchlists) so your analysis gets richer "
    "over time. You may read the immutable archive (public.raw_bazaar_snapshots, "
    "public.export_bundles) but cannot modify it.\n"
    "- load_market_snapshot(count, snapshot_id) parses raw snapshots into "
    "sandbox.product_prices(product_id, source_last_updated, fetched_at, sell_price, "
    "buy_price, sell_volume, buy_volume, sell_moving_week, buy_moving_week, sell_orders, "
    "buy_orders). Load several snapshots when you need trends/velocity over time.\n"
    "- Bazaar price semantics: buy_price = instant-buy (higher), sell_price = instant-sell "
    "(lower). Flip margin per unit ≈ buy_price - sell_price; margin% ≈ (buy_price - "
    "sell_price)/sell_price. Velocity ≈ moving_week volume. 'Good' opportunities pair a "
    "healthy margin% with high moving-week velocity.\n"
    "- Keep replies concise. Show concrete product ids and numbers. If you are genuinely "
    "blocked, ask exactly ONE specific question."
)

TOOLS = [
    {
        "name": "get_archive_status",
        "description": "Archive + disk status: snapshot count, DB size, disk usage %, free space, estimated days remaining.",
        "input_schema": {"type": "object", "properties": {}, "required": []},
    },
    {
        "name": "load_market_snapshot",
        "description": "Parse raw Bazaar snapshots into sandbox.product_prices for analysis. Loads the latest snapshot by default; pass count for the N most recent (trends/velocity) or snapshot_id for a specific one.",
        "input_schema": {
            "type": "object",
            "properties": {
                "count": {"type": "integer", "description": "Number of most-recent snapshots to load (1-200).", "default": 1},
                "snapshot_id": {"type": "integer", "description": "Load one specific snapshot id instead."},
            },
            "required": [],
        },
    },
    {
        "name": "run_sql",
        "description": (
            "Run SQL as the sandbox agent. You can SELECT the canonical archive "
            "(public.raw_bazaar_snapshots, public.export_bundles) and freely CREATE/INSERT/"
            "UPDATE/SELECT in your own `sandbox` schema (e.g. sandbox.product_prices). You "
            "cannot modify canonical tables. SELECTs return up to "
            f"{SQL_ROW_LIMIT} rows."
        ),
        "input_schema": {
            "type": "object",
            "properties": {"query": {"type": "string", "description": "One or more SQL statements."}},
            "required": ["query"],
        },
    },
    {
        "name": "get_product_quote",
        "description": "Latest quick-status (buy/sell price, volumes, orders) for one product id from the most recent snapshot.",
        "input_schema": {
            "type": "object",
            "properties": {"product_id": {"type": "string"}},
            "required": ["product_id"],
        },
    },
    {
        "name": "create_backup",
        "description": "Create a compressed range backup (.jsonl.gz) of recent snapshots and return an expiring signed download link.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days": {"type": "integer", "description": "Days back to include (1-90).", "default": 7},
                "expires_in_days": {"type": "integer", "description": "Days until the link expires (1-30).", "default": 7},
            },
            "required": [],
        },
    },
]


async def run_tool(name: str, tool_input: dict) -> str:
    try:
        if name == "get_archive_status":
            result = await api_get("/storage/status")
        elif name == "load_market_snapshot":
            result = await asyncio.to_thread(
                load_market_snapshot,
                int(tool_input.get("count", 1)),
                tool_input.get("snapshot_id"),
            )
        elif name == "run_sql":
            result = await asyncio.to_thread(run_sql, tool_input.get("query", ""))
        elif name == "get_product_quote":
            result = await get_product_quote(tool_input.get("product_id", ""))
        elif name == "create_backup":
            result = await api_post(
                "/exports",
                {"days": int(tool_input.get("days", 7)), "expires_in_days": int(tool_input.get("expires_in_days", 7))},
            )
        else:
            result = {"error": f"Unknown tool {name}"}
    except Exception as exc:  # noqa: BLE001 - surface tool errors to the model
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


async def handle_request(base_messages: list[dict]) -> str:
    if not ANTHROPIC_API_KEY:
        return "Natural-language mode is unavailable: ANTHROPIC_API_KEY is not set."

    messages = list(base_messages)
    for _ in range(8):
        data = await anthropic_request(messages)
        blocks = data.get("content", [])
        if data.get("stop_reason") == "tool_use":
            messages.append({"role": "assistant", "content": blocks})
            tool_results = []
            for block in blocks:
                if block.get("type") == "tool_use":
                    logger.info("Tool call: %s %s", block.get("name"), block.get("input"))
                    output = await run_tool(block["name"], block.get("input", {}))
                    tool_results.append({"type": "tool_result", "tool_use_id": block["id"], "content": output})
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

    history = history_for(message.channel.id)
    base_messages = list(history) + [{"role": "user", "content": content}]
    async with message.channel.typing():
        try:
            reply = await handle_request(base_messages)
        except Exception as exc:  # noqa: BLE001
            logger.exception("Request handling failed")
            reply = f"Sorry, I hit an error handling that: {exc}"

    history.append({"role": "user", "content": content})
    history.append({"role": "assistant", "content": reply[:4000]})
    await send_chunked(message.channel, reply)


@tasks.loop(minutes=30)
async def disk_pressure_watch() -> None:
    try:
        data = await api_get("/storage/status")
        disk = data["disk"]
        if disk["used_percent"] < DISK_ALERT_PERCENT:
            return
        msg = (
            f"⚠️ Disk pressure: {disk['used_percent']}% used, "
            f"{format_bytes(disk['free_bytes'])} free, "
            f"~{data['estimated_days_remaining_at_current_average']} archive days left."
        )
        for uid in ALLOWED_USER_IDS:
            try:
                user = await bot.fetch_user(uid)
                await user.send(msg)
            except Exception:
                logger.exception("Failed to DM disk alert to %s", uid)
    except Exception:
        logger.exception("Disk pressure watch failed")


def main() -> None:
    if not DISCORD_BOT_TOKEN:
        raise RuntimeError("DISCORD_BOT_TOKEN is required for the Discord bot service")
    bot.run(DISCORD_BOT_TOKEN)


if __name__ == "__main__":
    main()
