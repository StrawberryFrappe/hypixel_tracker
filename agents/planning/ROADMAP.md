# Roadmap

## Phase 0: Harness and Preconditions

- Accept the mounted harness and surfaced assumptions.
- Select the target stack and migrations.
- Review Hypixel policy and collection cadence.
- Benchmark target-host storage and transform/query throughput.
- Approve retention, quota, security, and recovery designs.

Stop condition: no application implementation before harness acceptance; no
deployment before all Phase 0 technical gates pass.

## Release 1: Reliable Trading Assistant Foundation

1. Reproducible project structure, migrations, fixtures, and CI.
2. Exact raw collector with integrity, validation, and gap evidence.
3. Ordered idempotent curation with lineage and metadata enrichment.
4. Versioned API, admin authentication, scoped keys, and local HTTPS.
5. Data-dense market terminal and transparent opportunity ranking.
6. Reproducible dataset exports and operations visibility.
7. Weekly backups, dashboard/CLI restore, and clean recovery drill.
8. Target-laptop deployment and second-device verification.

Stop condition: Release 1 passes all non-ML gates and has no unresolved P0/P1
review findings.

## Release 2: Forecasting Baseline

1. Mature 15-minute, 1-hour, and 24-hour labels and game-calendar features.
2. Establish leakage-safe temporal splits and simple baselines.
3. Publish reproducibility evidence, metrics, errors, and limitations.
4. Decide whether any model is useful enough to serve.

Stop condition: predictions remain offline until explicit baseline acceptance.

## Release 3: Served Predictions

1. Version and serve an accepted model behind the prediction boundary.
2. Integrate forecasts without hiding fees, liquidity, volatility, or freshness.
3. Monitor drift, errors, latency, and rollback readiness.

## Long-Term Aspiration

Retain enough curated data for several Hypixel years and potentially several
real years. This is not guaranteed until storage and query economics are proven.
