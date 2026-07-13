# Run State

## Current Phase

Harness and staged Release 1-3 plan accepted on 2026-07-13. Release 1 scaffold,
offline migrations, CI, Vue shell, and public-doc foundation are implemented.
Independent backend and frontend reviews converged to clean P0/P1.

## Verified State

- Target repository: clean `main` at `c8ae84fc0a0b1376a16f310934a032e7c7b77669`
  before harness files were added.
- Harness kernel: `main` at `d7a31eacd3f595b24df9c8245f94796cbd177325`.
- Current application code and `origin/lean` were statically audited.
- Official API docs, API policy, one live Bazaar payload, and one live item
  metadata payload were inspected on 2026-07-13.
- The deployment laptop was inspected read-only; access details remain under
  ignored local records.
- No application code, database, container, build, or deployment was changed
  during mounting.
- Release 1 scaffold tests pass: 20 Python tests, one Vue test, lint, format,
  typecheck, frontend build, strict MkDocs build, npm audit, dependency locks,
  Compose rendering, and offline Raw/Application upgrade/downgrade SQL.
- Database startup rejects placeholders and unsafe partial initialization;
  runtime readiness checks expected revisions and required tables.
- GitHub Actions use commit SHAs and Pages deployment authority is isolated to
  the main-branch deploy job.
- GitHub CI run `29273575490` passed Python, frontend, Compose, and secret-scan
  jobs without annotations.

## Product Direction

Hybrid rescue: exact raw archive, deterministic curated history, versioned API,
data-dense second-monitor dashboard, reproducible datasets, operations, and
tested backup/restore. Forecasting is staged behind the data foundation.

## Active Blockers

- Raw quota and curated retention require target-host benchmarks.
- Local certificate distribution, API-key scopes, and backup encryption/scope
  require implementation-plan decisions.
- Forecast model metrics and acceptance thresholds remain Release 2 decisions.
- Online PostgreSQL role/migration, Compose restart/outage, image build, Caddy,
  browser, `/srv`, and resource-cap evidence remain before release. Local Docker
  daemon socket is unavailable for scaffold integration smoke.
- Cavecrew config now targets `openai/gpt-5.6-sol`; current OpenCode session must
  restart before dispatch verification.

## Next Action

Refresh collector policy/fixture evidence, then implement bounded adaptive
collector and exact Raw PostgreSQL ledger/body capture under R1-WI-004.
