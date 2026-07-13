# Project Agent Instructions

This project uses the mounted harness under `agents/` as its operating contract.

## Required Read Order

1. `agents/README.md`
2. `agents/RUN_STATE.md`
3. `agents/intake/PROJECT_BRIEF.md`
4. `agents/intake/PROJECT_PROFILE.md`
5. `agents/intake/SOURCE_MANIFEST.md`
6. `agents/intake/QUESTIONS_SUMMARY.md`
7. `agents/intake/ASSUMPTIONS.md`
8. `agents/architecture/SRS.md`
9. `agents/architecture/C4.md`
10. `agents/validation/GATES.md`
11. `agents/execution/WORKFLOW.md`
12. Latest relevant review under `agents/reviews/`

## Core Rules

- Do not edit application code, build, compile, deploy, or run project tooling
  until the user explicitly accepts the mounted harness. This gate is
  unconditional and is not waived by confidence, context, or small scope.
- Only a new explicit user override after a concrete risk disclosure may bypass
  that gate. Silence or approval given before the mounted harness existed does
  not count.
- Surface every inferred assumption for user review before implementation.
- Start from `main`. Treat `origin/lean` as design evidence only; do not merge or
  fast-forward it wholesale.
- Never commit credentials, downloaded backups, raw API payloads, runtime data,
  or agent-local memory.
- Use migrations for target schema changes. Do not use ORM `create_all()` as
  migration management.
- A raw snapshot may be pruned only after its curated transform and lineage are
  durably verified and the active retention policy permits deletion.
- Follow the current Hypixel API policy and game-integrity rules. The product is
  unofficial, advisory-only, and must not automate in-game actions.
- Use subagents for independent review when available. Record capability
  downgrades rather than simulating reviewers.
- Keep raw notes and task logs under ignored `agents/local/`; promote durable
  decisions into committed harness documents.
- If work drifts from the accepted harness, stop and write a handoff before
  continuing.

## Completion Standard

Work is complete only when its linked requirements and validation gates pass,
or when remaining blockers are documented and explicitly accepted by the user.
