# ADR 0005: Backup, Export, and Restore Boundaries

- Status: Accepted in principle; implementation and recovery drills pending
- Date: 2026-07-13

## Context

Analytical exports are not disaster-recovery backups. The laptop has no second
server-side storage target, so a backup becomes off-host only after download.

## Decision

Provide reproducible dataset exports separately from weekly downloadable system
backup bundles. Support both guarded dashboard restore and a tested maintenance
CLI restore. Include integrity manifests and restore history. Never embed active
credentials in downloadable bundles.

Orchestrate restore through a restricted host controller and fsynced journal
outside both databases. Normal requests cross Caddy and authenticated API;
break-glass CLI crosses a separate hardened administrator SSH management
boundary and has no recovery LAN listener. SSH carries commands only, never
backup/recovery artifacts. Restore
uses candidate logical databases inside existing PostgreSQL containers plus a
candidate Parquet path, verifies before switch, preserves current auth on
failure, records every outcome, resumes jobs, and exposes successful fresh
bootstrap only through protected host-local retrieval.

## Consequences

The operator must download bundles regularly. Exact content, encryption, local
staging quota, and recovery objectives require an implementation review and an
end-to-end restore drill.
