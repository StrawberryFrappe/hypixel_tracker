# Validation Gates

| Gate | Applies | Pass Criteria | Required Evidence |
|---|---|---|---|
| GATE-HM Harness | Always | Pass: adapted harness reviewed and explicitly accepted 2026-07-13 | Dated mount review and recorded acceptance |
| GATE-DR Doctor | Always | Normal and strict doctor report no blockers; no active placeholders | `DOCTOR.md` and command output |
| GATE-RESCUE Branch Safety | Release 1 | Work starts from `main`; no wholesale `origin/lean` merge | Git review and plan review |
| GATE-POLICY Hypixel | Collection and release | Current policy reviewed; respectful cached cadence; unofficial/advisory boundaries pass | Dated policy review |
| GATE-ARCH Architecture | Before application edits and release | Stack and two-database topology accepted; migrations, roles, job model, TLS, resource caps, and storage boundaries proven before release | ADR 0007, C4, migration/role/resource evidence |
| GATE-BENCH Capacity | Before retention/deployment | Host measures cadence, compression, transform/query throughput, memory, storage growth, and backup/restore speed | Benchmark report and retention/quota ADR |
| GATE-DATA Integrity | Release 1 | Exact bounded-body capture, ordered captured revisions, saga visibility, idempotency, Parquet rebuild, operations projection, gap evidence, and no prune-before-proof pass | Fixture, crash-boundary, replay, rebuild, and integration tests |
| GATE-API-SEC API Security | Release 1 | Version contract, one-admin-only flow, no shared credential/viewer account, scoped automation keys, negative auth tests, secrets, and HTTPS pass | Contract tests, threat review, HTTPS smoke |
| GATE-PQ Portfolio Quality | Dashboard releases | Real-data terminal passes 1280-2560 pixel desktop review, accepted accessibility checks, and all loading/empty/stale/degraded/error states | Browser tests and screenshots |
| GATE-EXPORT Reproducibility | Dataset work | Independent run reproduces schema, checksums, feature versions, and lineage without becoming a public proxy | Export validation and policy review |
| GATE-BR Backup/Restore | Recovery work | Published-watermark two-database/Parquet set verifies; all artifacts cross authenticated data edge; dashboard and management CLI restore candidate stores, preserve rollback auth, audit all outcomes, and recover clean environment | Signed manifest, representative restore, injected failure/restart matrix for every backup/restore phase, AppDB-down break-glass, startup reconciliation, paused-job resumption, switch/rollback, bootstrap expiry/retrieval/rotation, and clean restore drill |
| GATE-TEST Automated Tests | Code releases | Linked unit, contract, integration, migration, browser, security, and applicable performance tests pass | Test output or CI |
| GATE-DEPLOY Target Host | Release 1 completion | After pre-deployment gates pass, provisional candidate is deployed; stable LAN name/fallback, local HTTPS, `/srv` layout, resource bounds, restart recovery, and unauthorized-access checks then pass | Host preflight, provisional deployment evidence, second-device smoke, and soak |
| GATE-ML Baseline | Release 2 | Mature labels, leakage review, temporal baselines, reproducibility, metrics, and model report accepted | Baseline review |
| GATE-PRED Serving | Release 3 | Accepted model only; freshness, uncertainty, monitoring, rollback, API, and UI behavior pass | Serving review and E2E evidence |

## Rule

Passing tests alone does not pass a release. Every applicable gate needs linked
evidence or an explicitly accepted blocker.
