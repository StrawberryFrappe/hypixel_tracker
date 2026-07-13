# Technology Stack

- Status: Accepted in principle on 2026-07-13; implementation evidence pending
- Updated: 2026-07-13

## Current Stack

| Area | Current Choice | Assessment |
|---|---|---|
| Runtime | Python 3.9 | Unsupported upstream and should be replaced |
| API | FastAPI in one module | Useful framework, weak current structure and contracts |
| ORM | SQLAlchemy models duplicated in API and processor | Drift and query-lifecycle risk |
| Database | PostgreSQL 15 | Viable foundation, but schema lacks migrations and retention design |
| Collection | `requests` polling loop | Functional concept, weak validation/retry/checkpoint behavior |
| Processing | Latest-row polling ORM loop | Silently skips backlog and scales poorly |
| Frontend | None | Required for the target product |
| Deployment | Docker Compose | Appropriate for one host, currently insecure and brittle |
| Tests | Stateful manual script | Not an automated test suite |

## Target Selection Criteria

- Supported runtime with reproducible, pinned dependencies.
- PostgreSQL-compatible migrations from the first target schema.
- Background jobs with durable checkpoints, idempotency, and bounded resources.
- Efficient time-range queries and measured retention/downsampling.
- A chart-capable web frontend with responsive second-monitor behavior.
- Local HTTPS, one-admin sessions, and scoped API keys.
- Docker Compose deployment within two CPU cores and 5.2 GiB RAM.
- Unit, contract, integration, migration, browser, benchmark, and recovery tests.

## Accepted Direction

- Python 3.13, FastAPI, SQLAlchemy 2, Alembic, and locked dependencies.
- Two PostgreSQL 17 containers: Raw PostgreSQL and Application PostgreSQL.
- Raw PostgreSQL stores fetch attempts, source revisions, and compressed exact
  response bodies.
- Application PostgreSQL stores serving projections, aggregates, auth, jobs,
  settings, and operations.
- Immutable Parquet generations provide long-term ML-ready curated artifacts.
- Vue 3, TypeScript, Vite, and ECharts provide Bazaar Guru webpage.
- Caddy provides LAN-only HTTPS and static asset delivery.
- Docker Compose runs one API, one collector, and one heavy worker at a time.
- Redis and TimescaleDB remain excluded until benchmark evidence justifies them.

## Remaining Gate

Implementation must prove migrations for both databases, least-privilege roles,
resource caps, rebuild behavior, and target-host capacity before release.
