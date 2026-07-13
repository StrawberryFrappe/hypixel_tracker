# ADR 0002: Hybrid Rescue and Release Policy

- Status: Proposed until user acceptance
- Date: 2026-07-13

## Context

`main` has useful product-history intent but loses backlogged snapshots.
`origin/lean` improves raw archival ideas while removing required API behavior
and adding unresolved security and operations risks.

## Decision

Keep `main` as the baseline and reimplement reviewed concepts selectively. Do
not merge `origin/lean` wholesale. Deliver three stages: data/dashboard
foundation, validated offline forecast baseline, then served predictions.

## Consequences

Legacy compatibility is not required. Release 1 cannot claim predictive value.
Each stage has an independent acceptance review and rollback boundary.
