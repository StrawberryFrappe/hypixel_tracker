# ADR 0003: Raw Database, Application Database, and Curated Archive

- Status: Accepted topology; quotas remain benchmark-gated
- Date: 2026-07-13

## Context

Exact raw responses enable replay and future feature work, but normalizing every
visible order-book level at source cadence would create unsustainable row growth.

## Decision

Run two PostgreSQL containers. Raw PostgreSQL stores fetch attempts, source
revisions, integrity metadata, and compressed exact response bodies. Application
PostgreSQL stores products, curated serving history/aggregates, auth, jobs,
settings, and operations. Immutable manifest-backed Parquet generations preserve
full curated ML features outside operational databases.

Curator reads Raw PostgreSQL in source-sequence order, writes deterministic
Parquet generations and idempotent Application PostgreSQL projections, then
publishes through `captured`, `parquet_ready`, `projection_ready`,
`raw_published`, and `app_visible` saga states. API reads only Application
generations made visible in one transaction. No distributed transaction is
claimed. Replay and reconciliation repair every partial cross-store state.

Raw PostgreSQL retains immutable completion/prune facts. Curator projects
freshness, possible gaps, failures, lag, and high-watermarks idempotently into
Application PostgreSQL. Parquet manifests contain every field needed to rebuild
Application serving projections after raw pruning. Raw bodies become
prune-eligible only after visible projection and tested rebuild verification.

## Consequences

Two database processes consume more RAM but provide explicit failure, role,
migration, quota, and backup boundaries. Hard resource caps are mandatory.
Feature changes can replay exact bodies only while raw retention permits;
long-term model features remain in versioned Parquet generations. Retention and
downsampling remain blocked until measured on target host.
