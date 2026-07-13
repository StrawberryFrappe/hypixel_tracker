# Software Requirements Specification

- Status: Harness and staged implementation plan accepted 2026-07-13
- Product: Bazaar Guru
- Scope authority: `agents/intake/SOURCE_MANIFEST.md`

## Product Boundary

The system is an unofficial, advisory-only market history and analysis tool for
one administrator. It collects public Bazaar data, derives
transparent opportunity metrics, and later may serve validated forecasts. It
does not execute game actions or expose a public Hypixel API proxy.

## Release 1 Functional Requirements

| ID | Requirement |
|---|---|
| R1-FR-001 | Capture every bounded received Bazaar entity in Raw PostgreSQL before semantic validation, with endpoint/version, UTC and monotonic start/end, source timestamp when parseable, HTTP/redirect/error metadata, selected cache/rate headers, content metadata, compressed/decoded sizes and hashes, observation sequence, and validation result. |
| R1-FR-002 | Deduplicate bodies without skipping any captured source revision; expose collector failures and source timestamp discontinuities as possible gaps with unknown missed count. |
| R1-FR-003 | Transform raw updates deterministically, idempotently, and in source order with a durable checkpoint and explicit failure state. |
| R1-FR-003A | Persist fetch attempt, body identity, compressed body, and unvalidated observation atomically within Raw PostgreSQL before parsing; write semantic validation and source revision afterward in a separate idempotent transaction; reconcile cross-database curation without claiming a distributed transaction. |
| R1-FR-003B | Publish curation through explicit saga states and expose only Application PostgreSQL generations marked visible in one transaction; test reconciliation at every crash boundary. |
| R1-FR-004 | Forbid raw pruning until finalized Parquet, visible Application projection, published Raw lineage, and projection rebuild proof are durable; apply deletion only under an accepted measured quota. |
| R1-FR-005 | Curate all eight quick-status fields, best prices, selected rank/depth/imbalance measures, spread and liquidity features, market and ingestion times, and schema/transform versions. |
| R1-FR-005A | Store complete manifest-backed Parquet generations sufficient to rebuild every retained Application serving projection after Raw pruning. |
| R1-FR-006 | Enrich products from official item metadata and derive deterministic display names for uncovered virtual IDs while preserving source provenance. |
| R1-FR-007 | Provide a versioned `/api/v1` contract for catalog, latest/history, opportunity features, datasets, operations, backups, restore controls, and settings. |
| R1-FR-008 | Provide a data-dense dashboard with market overview, product search/history, ranked opportunities, dataset exports, and operations status. |
| R1-FR-009 | Show loading, empty, stale, degraded, and error states and never present stale observations as live. |
| R1-FR-010 | Rank trades using configured fee rules, liquidity/depth, net and percentage return, and volatility. Ignore fee deductions visibly when no active fee rule exists. |
| R1-FR-011 | Store administrator preferences for budget, risk tolerance, and holding horizon and show scoring assumptions. |
| R1-FR-012 | Export reproducible curated datasets with time range, schema/feature versions, checksums, and source lineage. |
| R1-FR-013 | Project freshness, possible gaps, transform lag/failures, high-watermarks, storage/quota, backup status, and restore history into Application PostgreSQL for API operations views while immutable completion facts remain in Raw PostgreSQL. |
| R1-FR-014 | Generate independent weekly UTC recovery sets at a published curation watermark, containing consistent Raw range, Application nonsecret control/projection state, finalized Parquet, schema/image metadata, and release-pinned signed manifests; every terminal/crash state must audit and resume paused jobs. |
| R1-FR-015 | Support guarded dashboard restore and normal/break-glass maintenance CLI restore through a fsynced host journal independent of both databases; build logical candidate databases/Parquet path, preserve active auth on failure, deliver fresh successful-candidate bootstrap through protected host-local retrieval, and verify clean switch/rollback drills. |
| R1-FR-016 | Support one administrator account and separately scoped, revocable automation API keys with fail-closed authorization; no viewer accounts or shared administrator credentials. |

## Release 2 Functional Requirements

| ID | Requirement |
|---|---|
| R2-FR-001 | Build versioned, leakage-safe labels for 15-minute, 1-hour, and 24-hour horizons. |
| R2-FR-002 | Derive Hypixel calendar features from an authoritative conversion rather than a hardcoded remembered duration. |
| R2-FR-003 | Train and evaluate a reproducible baseline with temporal splits, accepted metrics, feature/label lineage, and a model report. |
| R2-FR-004 | Keep forecasts offline until the baseline review explicitly accepts serving. |

## Release 3 Functional Requirements

| ID | Requirement |
|---|---|
| R3-FR-001 | Serve only accepted model versions with horizon, market timestamp, freshness, uncertainty, limitations, and provenance. |
| R3-FR-002 | Add forecast signals to opportunity ranking without hiding the non-model score components. |
| R3-FR-003 | Monitor feature freshness, drift indicators, prediction error, model version, and rollback readiness. |

## Non-Functional Requirements

| ID | Requirement |
|---|---|
| NFR-DATA-001 | Preserve UTC source and ingestion timestamps, raw checksums, transform lineage, and replayability within retained raw history. |
| NFR-DATA-002 | Detect and report collection gaps, malformed payloads, partial transforms, and schema changes. |
| NFR-DATA-003 | Keep Raw PostgreSQL and Application PostgreSQL logically and operationally separate with independent migrations, roles, health, quotas, and backups. |
| NFR-DATA-004 | Define Raw-to-Parquet-to-Application saga states, serving visibility, rebuild, correction, backup watermark, and reconciliation invariants without a distributed transaction. |
| NFR-SEC-001 | Bind to the intended LAN interface, terminate local HTTPS, keep PostgreSQL private, prohibit default credentials, and protect secrets. |
| NFR-SEC-002 | Authentication and authorization must fail closed and have negative tests for every protected operation. |
| NFR-COMP-001 | Follow current Hypixel policy, caching guidance, branding rules, no-proxy restrictions, and game-integrity requirements. |
| NFR-OPS-001 | Recover unattended from routine process/database restarts while surfacing data gaps and degraded state. |
| NFR-OPS-002 | Derive storage quotas, retention, query limits, and job concurrency from target-host benchmarks. |
| NFR-OPS-003 | Keep restore orchestration and startup reconciliation available while Application PostgreSQL is unavailable, without exposing a recovery LAN listener or Docker socket. |
| NFR-QUAL-001 | Use schema migrations and automated unit, contract, integration, migration, browser, security, benchmark, and restore tests. |
| NFR-QUAL-002 | Use separate migration versions and roles for both databases, an expand-contract compatibility matrix, startup version checks, interrupted-upgrade recovery, and rollback evidence. |
| NFR-UX-001 | Pass portfolio-quality review across desktop widths from 1280 through 2560 pixels with clear freshness, advisory language, and accepted accessibility evidence. |
| NFR-ML-001 | Prevent future leakage, immature labels, train/serve skew, and cherry-picked evaluation. |

## Constraints

- One Ubuntu 26.04 LTS laptop with Ryzen 3 3200U, 5.2 GiB RAM, and Docker.
- Durable storage under ext4 `/srv`, approximately 870 GiB free at inspection.
- Two bounded PostgreSQL 17 containers run on the deployment laptop: one raw
  database and one application database.
- Forecast training runs on the separate development workstation. Only accepted,
  bounded inference may run on the deployment laptop.
- Best-effort personal operation; one Compose deployment.
- No valuable current database needs migration, but destructive actions still
  require explicit approval.
- Multi-year curated history is an aspiration subject to measured economics.

## Acceptance Evidence

Requirement completion must link to tests, API contracts, screenshots or browser
evidence, benchmark reports, policy review, backup manifests, restore drills,
deployment smoke results, and later model reports through `TRACEABILITY.md`.
