# ADR 0007: Target Stack and Migrations

- Status: Accepted in principle; implementation evidence blocking release
- Date: 2026-07-13

## Context

The current stack uses unsupported Python 3.9, unpinned dependencies, duplicated
ORM models, and `create_all()` without migrations. The user is open to changing
technologies, while the deployment host has two CPU cores, 5.2 GiB RAM, Docker,
and a large dedicated data disk.

## Decision

Use Python 3.13, FastAPI, SQLAlchemy 2, Alembic, PostgreSQL 17, Vue 3,
TypeScript, Vite, ECharts, Caddy, and Docker Compose. Run separate Raw and
Application PostgreSQL containers with independent migrations and roles. Use a
PostgreSQL-backed job protocol; do not add Redis or TimescaleDB without evidence.
Use locked dependencies, bounded jobs, local HTTPS, automated tests, and hard
resource caps for the single deployment laptop.

Use separate migrator roles and version tables. Curator declares compatible Raw
and Application schema ranges and refuses startup outside them. Cross-database
changes follow expand-contract order: expand both schemas, deploy compatible
code, backfill/reconcile, switch readers/writers, then contract only after
rollback window. Tests cover fresh install, each supported upgrade order,
interrupted migration, incompatible partial state, and rollback. Recovery runs
before destructive contract migrations.

Normal topology is Caddy/Vue, API, Collector, one Curator/Scheduler worker, Raw
PostgreSQL, and Application PostgreSQL. Recovery is an on-demand Compose profile;
host controller stops Curator/Scheduler first, mounts recovery-only credentials,
and never exposes Docker socket to application containers.

## Consequences

Implementation may begin only under an approved plan. Release remains blocked
until migrations, role separation, container memory limits, backup/restore, and
target-host benchmarks prove this topology viable.
