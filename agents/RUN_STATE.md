# Run State

## Current Phase

Harness and staged Release 1-3 plan accepted on 2026-07-13. Corrected architecture
review is clean; Git safety baseline is next.

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

## Product Direction

Hybrid rescue: exact raw archive, deterministic curated history, versioned API,
data-dense second-monitor dashboard, reproducible datasets, operations, and
tested backup/restore. Forecasting is staged behind the data foundation.

## Active Blockers

- Raw quota and curated retention require target-host benchmarks.
- Local certificate distribution, API-key scopes, and backup encryption/scope
  require implementation-plan decisions.
- Forecast model metrics and acceptance thresholds remain Release 2 decisions.
- Two-database migrations, role boundaries, and memory caps require implementation
  evidence before release.

## Next Action

Create backup branch/tag and stacked Release 1 branch exactly as
`planning/RELEASE_PLAN.md` specifies.
