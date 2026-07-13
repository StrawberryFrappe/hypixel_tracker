# Architecture

## Scaffold Topology

Browser traffic enters one Caddy edge. Caddy terminates local HTTPS, serves the
compiled Vue application, and forwards `/api/*` to FastAPI. PostgreSQL services
have no host ports.

Two PostgreSQL 17 databases establish a hard data boundary:

| Store | Current schema | Planned responsibility |
|---|---|---|
| Raw | `raw.scaffold_state` | Bounded fetch ledger and exact compressed source bodies |
| Application | `app.scaffold_state` | Curated serving projections and application control state |

Each database has its own bootstrap, migrator, and runtime role. Bootstrap roles
exist only to initialize non-superuser roles. Separate Alembic environments,
revision histories, URLs, and version tables prevent accidental cross-migration.

## Startup Order

1. Caddy may serve the static degraded shell without any data service.
2. Raw and Application PostgreSQL become healthy only after authenticated role
   access and completed bootstrap markers pass independently.
3. `raw-migrate` and `app-migrate` upgrade only their designated databases.
4. API and collector processes expose liveness independently while readiness
   remains degraded until expected database revisions are available.

API liveness has no database dependency. A health-only collector scaffold probes
Raw and returns one sanitized availability bit on an internal network. API probes
that status and its own Application connection, returning HTTP `503` when either
is unavailable. API never receives Raw credentials or joins the Raw network.
Error details and connection strings are not returned.

## Planned Boundaries

Collector currently exposes internal health only; worker is an opt-in, exit-only
scaffold command. Future collector work will write Raw only. Future curation work will coordinate Raw,
immutable Parquet, and Application publication without claiming a distributed
transaction. None of that pipeline exists in this phase.

Authentication, recovery, forecasting, retention, and deployment controls are
also intentionally absent. They require separate implementation and evidence.
