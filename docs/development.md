# Development

## Toolchains

- Python `>=3.13,<3.14`, managed and locked with `uv`.
- Vue 3, TypeScript, Vite, Vitest, and ECharts, locked with npm.
- PostgreSQL 17 and Caddy through Docker Compose.

Install locked Python dependencies (network access may be required):

```bash
uv sync --all-groups
```

After dependencies are present, run checks without application network access:

```bash
uv lock --check
uv run ruff check .
uv run ruff format --check .
uv run pytest
uv run mkdocs build --strict
```

Run frontend checks:

```bash
cd frontend
npm ci
npm test
npm run typecheck
npm run build
```

Validate Compose without starting services:

```bash
docker compose --env-file .env.example config --quiet
```

## Migration Discipline

Raw migration commands use `alembic_raw.ini` and only
`BAZAAR_GURU_RAW_MIGRATION_DATABASE_URL`. Application commands use
`alembic_app.ini` and only `BAZAAR_GURU_APP_MIGRATION_DATABASE_URL`.

```bash
BAZAAR_GURU_RAW_RUNTIME_ROLE=raw_runtime \
  uv run alembic -c alembic_raw.ini upgrade head
BAZAAR_GURU_APP_RUNTIME_ROLE=app_runtime \
  uv run alembic -c alembic_app.ini upgrade head
```

Schema changes require reversible migrations. ORM `create_all()` is not a
migration mechanism. Unit tests inspect independent metadata, revision heads,
version tables, and environment bindings without opening network connections.

## Integration Smoke

Database smoke work is opt-in because it starts containers and creates named
volumes. Use development-only credentials, verify the target, and run only the
migration jobs:

```bash
docker compose --env-file .env up --build raw-migrate app-migrate
```

Never point integration tests at persistent operator databases.
