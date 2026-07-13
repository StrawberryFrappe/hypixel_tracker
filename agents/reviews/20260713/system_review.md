# System and Maturity Review

- Date: 2026-07-13
- Scope: Current `main`, evidence branch `origin/lean`, upstream data, and target host
- Verdict: Rescue required; neither branch is safe as the full product

## Current Architecture

`main` runs PostgreSQL, a raw fetcher, a latest-only SQL processor, FastAPI, and
Adminer. Stale Mongo files remain but are not wired. The API mirrors ORM models
and exposes latest/product-history routes without authentication.

## Findings

| Severity | Finding | Evidence and Required Direction |
|---|---|---|
| P0 | SQL processing silently loses historical snapshots after lag or downtime | `scraper/sql_processor.py:90-103` reads only the newest raw row; use ordered durable checkpoints |
| P0 | Adminer exposes database-owner access with documented static credentials | `docker-compose.yml:6-9,52-57`; remove from normal deployment and use private DB roles |
| P1 | Storage is unbounded at high frequency | Full raw plus status/offers have no retention, quota, partitioning, or capacity gates |
| P1 | Startup/readiness is unreliable and `/health` is false-positive liveness | Fixed sleeps and simple `depends_on`; API health does not check database or freshness |
| P1 | Direct scraper image is broken | Default `main.py` imports omitted `pymongo` and no Mongo service exists |
| P1 | The only test is destructive, flaky, and not an automated assertion suite | `scraper/test_sql_ingestion.py` writes persistent data and queries reserved `update` incorrectly |
| P1 | API query lifecycle, validation, timestamp semantics, and errors are unsafe | Per-request engines, negative limits, local-time ambiguity, leaked internal errors, and empty `/latest` becomes 500 |
| P1 | Schema integrity and evolution are absent | Duplicated models, nullable relationships, weak indexes, and no migrations |
| P2 | Product names, response contracts, precision, and operations are incomplete | IDs used as names, float prices, unstable pagination, no metrics or gap visibility |

## Evidence Branch Review

`origin/lean` introduces a compressed raw archive, API authentication, local
exports, and a Discord/Anthropic bot. Useful archive concepts may be
reimplemented, but the branch removes required history APIs and has blocking
faults: fail-open Discord allowlists, unbounded/concurrent export risks,
resource-unsafe LLM SQL, insecure deployment defaults, no database migration,
and no tests. It must not be merged wholesale.

## Live Upstream Evidence

The inspected Bazaar payload contained 1,933 products and 54,365 visible summary
levels. Summary rank matters; quick prices are top-2-percent-volume weighted
averages and are not always reproducible from the visible top 30. Empty books
use zero source values, which curated analytics should expose with availability
flags. Full per-level normalization at source cadence is not viable without
aggressive lifecycle design.

Official item metadata directly covered only about half of Bazaar IDs. Product
metadata must preserve canonical IDs and record official versus derived names.

## Target Host Evidence

The deployment laptop is an Acer Aspire A315-42 running Ubuntu 26.04 LTS in UTC. It has an
AMD Ryzen 3 3200U, two cores/four threads, 5.2 GiB RAM, 8 GiB swap, Docker
29.1.3, Compose 2.40.3, and approximately 870 GiB free on ext4 `/srv`. This can
host the Release 1 foundation if workloads are bounded and benchmarked. Heavy
model training may need separate resources or tight scheduling.

Its DHCP address differed from the stale local SSH configuration. Deployment
needs a stable hostname or DHCP reservation before local HTTPS. Access details
remain in ignored project-local records.

## Recommendation

Use the hybrid target in `architecture/C4.md`: exact rolling raw history,
deterministic compact curation, transparent market API/dashboard, bounded
operations, and staged ML. Select technology and retention only after accepted
architecture and target-host benchmarks.
