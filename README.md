# Bazaar Guru

Bazaar Guru is a LAN-only, unofficial, advisory-only Hypixel SkyBlock Bazaar
assistant. This branch contains the Release 1 foundation scaffold. It does not
collect market data, rank trades, authenticate users, or perform game actions.

## Current State

Implemented:

- Python 3.13 FastAPI package with `/api/v1` liveness and dual-database readiness.
- Independent Raw and Application SQLAlchemy metadata and Alembic tracks.
- Vue 3 and TypeScript terminal shell with explicit unavailable/degraded states.
- Two private PostgreSQL 17 services, non-superuser migration/runtime roles,
  bounded resources, migration jobs, and local-CA Caddy edge.
- Locked Python/npm dependencies, tests, CI, secret scanning, and MkDocs.

Not implemented:

- Hypixel collection, raw payload capture, curation, Parquet, or scoring.
- Authentication, settings, exports, backup, restore, or deployment automation.
- Stable LAN hostname selection and local CA distribution.

No market value shown by the shell is real or synthetic. Data features remain
visibly pending until their later implementation and review.

## Repository Layout

```text
src/bazaar_guru/    FastAPI, configuration, database metadata, CLI
migrations/raw/     Raw PostgreSQL Alembic environment and revisions
migrations/app/     Application PostgreSQL Alembic environment and revisions
tests/              Network-free Python tests
frontend/           Vue 3, TypeScript, Vite, Vitest shell
infra/              Caddy, PostgreSQL bootstrap, service images
docs/               Sanitized public MkDocs sources
agents/             Accepted project harness and private-local ignore boundary
```

## Local Development

Requirements: `uv`, Python 3.13 (managed by `uv` if absent), Node.js 22.12+, npm,
and Docker Compose for configuration or integration work.

```bash
uv sync --all-groups
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run mkdocs build --strict

cd frontend
npm ci
npm test
npm run typecheck
npm run build
```

Unit tests do not require databases, containers, or network access.

## Environment

Create a local environment file and replace every `CHANGEME` placeholder:

```bash
cp .env.example .env
docker compose --env-file .env config --quiet
```

`.env` is ignored. Compose requires every database password and DSN; it has no
fallback credentials. Keep passwords URL-safe or percent-encode them in DSNs.
Do not reuse bootstrap, migrator, and runtime credentials.

Default edge binding is `127.0.0.1:443`. This prevents accidental LAN exposure
during scaffold work. Do not change it to a LAN address until authentication,
stable hostname, local CA distribution, and deployment gates are complete.

## Database Migrations

Migration commands are deliberately separate and each reads only its designated
URL:

```bash
BAZAAR_GURU_RAW_MIGRATION_DATABASE_URL='postgresql+psycopg://...' \
  BAZAAR_GURU_RAW_RUNTIME_ROLE='raw_runtime' \
  uv run alembic -c alembic_raw.ini upgrade head

BAZAAR_GURU_APP_MIGRATION_DATABASE_URL='postgresql+psycopg://...' \
  BAZAAR_GURU_APP_RUNTIME_ROLE='app_runtime' \
  uv run alembic -c alembic_app.ini upgrade head
```

Both initial migrations are reversible, grant bounded runtime access, and record
the revisions checked by service readiness. No application path calls
`MetaData.create_all()`.

For a disposable Compose migration smoke, review `.env` and then run:

```bash
docker compose --env-file .env up --build raw-migrate app-migrate
```

This starts databases and writes named volumes. It is intentionally not part of
the network-free unit suite and should never target operator data.

## Compose Scaffold

```bash
docker compose --env-file .env up --build
```

Raw and Application PostgreSQL have no host ports and use isolated networks.
Database health requires authenticated runtime access and a durable completed
bootstrap marker. Migration, API, collector, and edge processes start
independently; readiness remains degraded until expected revisions are present.
A health-only collector scaffold probes Raw; API receives its sanitized status
and connects only to Application PostgreSQL. Caddy serves the SPA even during a
data-service failure and proxies `/api/*`; its internal CA state persists in a
named volume.

The collector performs no collection. Opt-in worker topology only prints a
pending-phase message and exits:

```bash
docker compose --env-file .env --profile pipeline-scaffold run --rm worker-scaffold
```

Neither collector nor worker contacts Hypixel or performs data-pipeline writes.
Migration jobs create only version and scaffold-state objects.

## Health Contract

- `GET /api/v1/health/live`: process liveness; never probes a database.
- `GET /api/v1/health/ready`: independently reports Raw and Application
  connectivity; returns `503` with `degraded` if either is unavailable.

Responses expose only availability labels, never DSNs or database errors.

## Product Boundary

Bazaar Guru is not affiliated with Hypixel. It is an analysis aid, not a public
Hypixel proxy, bot, automated trader, or source of guaranteed profit. Collection
and all data-dependent behavior remain subject to current Hypixel policy review.

See `docs/` for public architecture, development, and security notes. Accepted
requirements and gates remain under `agents/`.
