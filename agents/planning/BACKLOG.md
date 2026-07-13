# Backlog

## Harness

| ID | Priority | Item | Requirements | Evidence |
|---|---|---|---|---|
| HM-WI-001 | P0 | Review and explicitly accept the mounted harness | ADR-0001 | Mount review and user acceptance |

## Release 1

| ID | Priority | Item | Requirements | Evidence |
|---|---|---|---|---|
| R1-WI-001 | P0 | Complete policy/cadence review and capture representative fixtures | R1-FR-001, NFR-COMP-001 | Policy review and fixture hashes |
| R1-WI-002 | P0 | Select stack, migrations, service boundaries, and dependency strategy | NFR-QUAL-001 | Accepted ADR 0007 replacement |
| R1-WI-003 | P0 | Benchmark host and approve quota/retention/downsampling | R1-FR-004, NFR-OPS-002 | Benchmark report and retention ADR |
| R1-WI-004 | P0 | Implement exact collector and raw archive | R1-FR-001, R1-FR-002 | Contract and integration tests |
| R1-WI-005 | P0 | Implement crash-consistent raw finalization plus ordered curation, features, metadata, and lineage | R1-FR-003, R1-FR-003A, R1-FR-004 through R1-FR-006 | Crash-transition, reconciliation, replay, idempotency, and gap tests |
| R1-WI-006 | P0 | Implement `/api/v1`, auth, settings, and HTTPS | R1-FR-007, R1-FR-010, R1-FR-011, R1-FR-016 | API contract and security tests |
| R1-WI-007 | P1 | Implement market terminal workflows | R1-FR-008 through R1-FR-011 | Browser tests and screenshots |
| R1-WI-008 | P1 | Implement dataset exports and operations | R1-FR-012, R1-FR-013 | Reproducibility and operations evidence |
| R1-WI-009 | P0 | Implement backup, restore, and recovery drill | R1-FR-014, R1-FR-015 | Clean restore evidence |
| R1-WI-010 | P0 | Deploy and verify on the target laptop | NFR-SEC-001, NFR-OPS-001 | LAN HTTPS and deployment smoke |
| R1-WI-011 | P0 | Run final Release 1 review | All Release 1 requirements | Release review and accepted residuals |

## Release 2

| ID | Priority | Item | Requirements | Evidence |
|---|---|---|---|---|
| R2-WI-001 | P1 | Build versioned mature labels and calendar features | R2-FR-001, R2-FR-002 | Label and leakage review |
| R2-WI-002 | P1 | Train and evaluate reproducible baselines | R2-FR-003, R2-FR-004 | Model report and environment manifest |

## Release 3

| ID | Priority | Item | Requirements | Evidence |
|---|---|---|---|---|
| R3-WI-001 | P1 | Serve accepted predictions with provenance | R3-FR-001, R3-FR-002 | Serving contract and E2E tests |
| R3-WI-002 | P1 | Add monitoring and rollback | R3-FR-003 | Drift, error, and rollback evidence |
