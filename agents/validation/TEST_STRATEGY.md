# Test Strategy

## Current Baseline

The repository has no real automated test suite or CI. The existing
`scraper/test_sql_ingestion.py` is a stateful manual script: it writes persistent
dummy data, sleeps, races the real processor, uses unsafe hardcoded connection
details, contains a reserved-table SQL error, and does not reliably fail its
process on verification errors. It must not be treated as test evidence.

## Release 1 Layers

- Unit tests: parsing, validation, fee versions, depth features, scoring,
  retention eligibility, time boundaries, and authorization decisions.
- Contract tests: recorded Hypixel fixtures, schema evolution, `/api/v1`
  requests/responses, and error semantics.
- Integration tests: disposable PostgreSQL, exact raw hashes, duplicate handling,
  ordered backlog catch-up, idempotent transforms, and metadata fallback.
- Crash-consistency tests: interrupt after temporary write, staged metadata,
  final rename, and ready publication; reconciliation must produce one valid
  ready snapshot or a safely retryable staged state without orphan leaks.
- Migration tests: fresh upgrade, previous-version upgrade, downgrade or explicit
  rollback, and schema/API compatibility.
- Invariant/property tests: no prune before lineage, unique source identity,
  nonnegative source values, rank preservation, and source/curated reconciliation.
- Security tests: fail-closed sessions and API keys, scope matrix, restore
  authorization, secret handling, and LAN HTTPS.
- Browser tests: login, overview, product search/history, stale/degraded states,
  dataset download, settings, and protected recovery workflows.
- Operations tests: restart recovery, gap reporting, bounded jobs, storage
  pressure, backup integrity, clean dashboard restore, and clean CLI restore.
- Performance tests: collection cadence, transform lag, history queries, exports,
  restore throughput, memory limits, and disk growth on the target class.

## ML Layers

- Label maturity and leakage tests.
- Temporal split and feature reproducibility tests.
- Baseline comparisons and error analysis by product/liquidity regime.
- Train/serve feature parity and model rollback tests before Release 3.

## Environment Rule

Integration and recovery tests use disposable data. Never point them at a
persistent operator database without explicit task-specific approval.
