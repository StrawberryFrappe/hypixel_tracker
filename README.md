# Hypixel Bazaar Raw Archive

Lean Docker Compose system for archiving exact Hypixel SkyBlock Bazaar API responses.

The goal is to preserve raw market history first, then build future schemas, analytics, and trading-bot experiments from that immutable archive.

## Architecture

- `postgres`: durable archive database.
- `collector`: polls `https://api.hypixel.net/v2/skyblock/bazaar` every 15 seconds.
- `api`: authenticated FastAPI service for snapshot metadata, raw payload retrieval, storage status, and range exports.
- `discord_bot`: private operational and market-analysis bot.

There is no Adminer service in this lean build.

## Storage Strategy

Each unique Bazaar response is stored as:

- queryable metadata: fetch time, Hypixel `lastUpdated`, hash, size, status code;
- compressed exact response body.

Duplicate prevention uses both Hypixel `lastUpdated` and a SHA-256 hash of the exact response body.

Measured sample:

- raw response: about `3.07 MiB`;
- gzip response: about `432 KiB`;
- estimated unique cadence: about one snapshot every 20 seconds.

Estimated compressed growth:

- about `1.78 GiB/day`;
- about `12.5 GiB/week`;
- about `80-100 days` on a 250 GB VPS if roughly 180 GiB is reserved for the live archive.

## Configuration

Create `.env` from these values:

```env
POSTGRES_USER=user
POSTGRES_PASSWORD=change-me
POSTGRES_DB=bazaar_data

API_KEY=change-this-api-key
API_PORT=8001
PUBLIC_BASE_URL=http://your-server:8001

POLL_INTERVAL_SECONDS=15
SNAPSHOT_INTERVAL_SECONDS=20
BACKUP_CACHE_BYTES=26843545600
DISK_WARN_PERCENT=80

DISCORD_BOT_TOKEN=
DISCORD_ALLOWED_USER_IDS=
DISCORD_ALLOWED_GUILD_IDS=
DISCORD_ALERT_CHANNEL_ID=
DISK_ALERT_PERCENT=80

ANTHROPIC_API_KEY=
ANTHROPIC_MODEL=claude-3-5-haiku-latest
```

Use a strong `API_KEY` before exposing the API outside localhost.

## Running

```bash
docker compose up -d --build
```

API:

```bash
curl http://localhost:8001/health
curl -H "X-API-Key: $API_KEY" http://localhost:8001/storage/status
curl -H "X-API-Key: $API_KEY" http://localhost:8001/snapshots?limit=5
```

Create a weekly export:

```bash
curl -X POST -H "X-API-Key: $API_KEY" "http://localhost:8001/exports?days=7&expires_in_days=7"
```

The API returns a signed download link. Export cache cleanup deletes expired exports and oldest exports when the backup cache budget is exceeded.

## API

- `GET /health`
- `GET /snapshots`
- `GET /snapshots/latest`
- `GET /snapshots/{id}`
- `GET /storage/status`
- `GET /exports`
- `POST /exports`
- `GET /exports/{id}?token=...`

All endpoints except `/health` require `X-API-Key`.

## Discord Bot

Commands:

- `!status`: archive and disk status.
- `!backup 7`: create a range export for the last 7 days and return an expiring link.
- `!ask <question>`: ask the Anthropic-backed market expert using archive context.
- `!sql <select query>`: run read-only SQL with timeout and row limits.

The bot only responds to configured users/guilds. It sends disk pressure alerts to the configured alert channel.

## Course Criteria Mapping

- Reproducible deployment: Docker Compose.
- Configuration by environment: `.env` variables.
- Secrets: no real keys in source.
- External integration: Hypixel API and Discord.
- Scheduled execution: collector polling loop.
- Error handling: request timeouts, duplicate skips, failed-fetch logs.
- Health and observability: `/health`, `/storage/status`, structured logs.
- Continuity: compressed range exports with expiring links.
- Security: API key auth, private Discord allowlist, read-only SQL guard.
- Rollback: git branch plus Compose rebuild/redeploy.
