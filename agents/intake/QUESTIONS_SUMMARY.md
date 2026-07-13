# Questions Summary

## Answered Decisions

| Topic | Decision |
|---|---|
| Product direction | Hybrid rescue rather than repairing `main` or adopting `origin/lean` wholesale |
| Required capabilities | Raw archive, product-history API, dashboard, dataset exports, operations, backup and restore |
| Primary use | Second-monitor trading assistant for one administrator |
| Portfolio viewers | Screenshots or supervised demonstration; no viewer accounts |
| Existing data | No valuable database history must be migrated |
| Network | One laptop on the local network, not public Internet hosting |
| Access | One admin account plus simple scoped API keys |
| Transport | Local HTTPS |
| API evolution | New versioned API; no legacy response compatibility requirement |
| Database topology | Two PostgreSQL 17 containers: compressed exact raw database and separate application database |
| Frontend | Vue 3/TypeScript webpage; React explicitly rejected |
| Capture | Preserve every observed distinct source update in the raw rolling archive |
| Raw pressure | Prune oldest eligible raw bodies after verified transformation, under a measured quota |
| Retention | Derive policy from target-host benchmarks rather than assume fixed tiers |
| Dashboard | Data-dense market terminal with overview, product explorer, exports, and operations |
| Dashboard evidence | Responsive desktop layout from 1280 through 2560 pixels |
| Opportunity score | Fees, liquidity/depth, profit percent, volatility, and later forecast signal |
| User preferences | Store budget, risk, and holding horizon in dashboard settings |
| Backups | Weekly downloadable bundles; support dashboard and CLI restore |
| Reliability | Best-effort service with automatic recovery and visible gaps |
| Forecast delivery | Data foundation, then offline baseline, then served predictions |
| Forecast horizons | 15 minutes, 1 hour, and 24 hours, with Hypixel calendar features |
| Forecast training host | Current development workstation; deployment laptop serves data and accepted inference only |
| Exclusions | Discord/LLM, automated trading, native mobile, and multi-server scaling |
| Source authority | User answers for product; official policy and schema for upstream constraints |

## Open Decisions

| Topic | Why It Remains Open | Required Resolution |
|---|---|---|
| Raw quota | Sample estimates are not host benchmarks | Measure compression, throughput, and safe `/srv` headroom |
| Curated retention | Depends on row width, query latency, and forecast needs | Benchmark and approve retention/downsampling policy |
| Certificate distribution | Local HTTPS must stay usable for private clients | Select local CA, stable hostname, and renewal method |
| API-key scopes | Required endpoints and automation clients are not implemented | Threat review and explicit scope matrix |
| Backup content | Independent weekly Raw/Application/Parquet sets plus standalone nonsecret control snapshot; auth/secrets excluded | Benchmark size, retention count, streaming, and representative restore |
| Fee values | Bazaar fees change over time | Versioned admin settings; scoring ignores fees when no active rule exists |
| Model acceptance | No training history exists yet | Release 2 metric, baseline, and temporal validation decision |
| Accessibility evidence | Keyboard operation, focus behavior, contrast, and chart alternatives need concrete criteria | Confirm during the dashboard implementation plan and portfolio review |

## Workflow Decision

Harness and full staged plan were accepted on 2026-07-13. Release work uses
stacked branches based on the prior release. Each release still requires clean
tests/review and remains unmerged for user review unless separately approved.
