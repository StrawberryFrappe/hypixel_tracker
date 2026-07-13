# Approved Staged Release Plan

- Accepted: 2026-07-13
- Product: Bazaar Guru
- Delivery: stacked review branches; no release branch merges before user review

## Git Safety

1. Push backup branch `backup/pre-bazaar-guru-20260713` and annotated tag
   `pre-bazaar-guru-20260713` at baseline `c8ae84f`.
2. Create `release/1-foundation` from baseline and commit accepted harness first.
3. Create `release/2-forecasting` from reviewed Release 1 branch.
4. Create `release/3-predictions` from reviewed Release 2 branch.
5. Push each branch and open stacked PRs for user review. Do not merge `main`
   while user is asleep.

## Release 1: Foundation

1. Install/review deployment-laptop Claude Code and OpenCode tooling separately.
2. Scaffold locked Python/Vue monorepo, two-database migrations, CI, secret scan,
   Compose, Caddy, MkDocs, and GitHub Pages.
3. Implement bounded adaptive collector and complete Raw PostgreSQL ledger/body
   capture.
4. Implement curation saga, immutable Parquet generations, Application
   PostgreSQL serving projection, operations summaries, rebuild, correction, and
   raw-prune proof.
5. Benchmark two PostgreSQL containers plus collector/API/worker on target class;
   accept quotas and retention only from evidence.
6. Implement `/api/v1`, one-admin auth, scoped automation keys, settings,
   opportunity scoring, and operations.
7. Implement Bazaar Guru Vue webpage: overview, product explorer, datasets,
   operations, recovery, settings, all required states, and accessibility.
8. Implement independent signed weekly recovery sets, critical USB control
   bundle, candidate-store dashboard/CLI restore, switch, rollback, fresh auth,
   and audit.
9. Build sanitized public MkDocs Pages and complete tests/evidence.
10. Push Release 1 branch and PR. After every pre-deployment gate passes, deploy
    exact branch candidate provisionally to collect GATE-DEPLOY evidence. Keep it
    provisional until manual CA trust, second-device test, backup download,
    restore, and 24-72h soak complete.

## Release 2: Forecasting Baseline

1. Branch from Release 1.
2. Build 15-minute, 1-hour, and 24-hour labels plus authoritative Hypixel
   calendar features.
3. Train reproducible baseline on development workstation with temporal splits,
   leakage tests, metrics, model report, and no serving.
4. Review until clean; push stacked branch/PR.

## Release 3: Prediction Serving

1. Branch from Release 2 only if baseline gate accepts a useful model.
2. Serve accepted artifact with provenance, freshness, uncertainty, monitoring,
   drift/error evidence, and rollback.
3. Integrate forecast signal without hiding non-model ranking components.
4. Review until clean; push stacked branch/PR.

## Independent Review Checkpoints

Run tests, then independent adversarial review after tooling, scaffold/migrations,
collector/raw, curation/rebuild, benchmark/retention, API/security, Vue/scoring,
recovery, docs/deployment, and each release final diff. Fix all P0/P1 findings
and rerun until clean. Record capability downgrades honestly.

## Destructive Allowlist

After replacement candidate passes health, source cycles, transform, restart,
and rollback checks, delete only exact old tracker containers, volumes, network,
and reviewed deployment contents recorded in the approved plan. Never wildcard
prune or touch WIP Agent/secretaria resources.

USB content deletion requires stable by-id/serial/UUID match, complete recursive
read-only inventory, recognized Ubuntu media validation, and exact path
allowlist. Any unknown file blocks deletion. Preserve filesystem; do not claim
secure erasure.

## Stop Conditions

Stop release progression on policy conflict, identity mismatch, unknown USB
content, sanitation leak, failed CI/test, resource-envelope failure, failed
restore, unresolved P0/P1, artifact mismatch, migration incompatibility, or risk
to unrelated state.

## Context Continuity

At each release boundary, update RUN_STATE, traceability, evidence, reviews, and
ignored project-local logbook. Produce a concise handoff before context
compression or continuation.
