# Hypixel Bazaar Tracker Agent Harness

This directory is the project-specific operating contract for the rescue and
completion of the Hypixel Bazaar trading assistant.

## Read Order

1. `../AGENTS.md`
2. `RUN_STATE.md`
3. `intake/PROJECT_BRIEF.md`
4. `intake/PROJECT_PROFILE.md`
5. `intake/SOURCE_MANIFEST.md`
6. `intake/QUESTIONS_SUMMARY.md`
7. `intake/ASSUMPTIONS.md`
8. `planning/ROADMAP.md`
9. `planning/BACKLOG.md`
10. `planning/RELEASE_PLAN.md`
11. `architecture/SRS.md`
12. `architecture/C4.md`
13. `architecture/TECH_STACK.md`
14. `validation/GATES.md`
15. `execution/WORKFLOW.md`
16. Latest relevant review under `reviews/`

## Operating Contract

- The mounted harness must be explicitly accepted before implementation.
- `main` is the implementation baseline. `origin/lean` is evidence only.
- Product scope comes from the user's recorded answers, subject to official
  Hypixel policies and game rules.
- Architecture decisions remain proposed where evidence is incomplete.
- Every change requires a plan, review, relevant tests, and traceable evidence.
- Local-only notes belong under `local/`, which is ignored by Git.
- Secrets and private access details are never copied into this harness.

## Delivery Boundary

The work is staged. Release 1 establishes reliable data, API, dashboard,
operations, export, and recovery foundations. Release 2 validates an offline
forecasting baseline. Release 3 may serve predictions only after that baseline
passes its gates.

## Completion Standard

A work item is complete only when its linked requirements and gates pass. A
documented blocker can remain open only with explicit user acceptance.

## Harness Acceptance Checklist

Before accepting the harness, the user should review the project brief, open
questions, every entry in `intake/ASSUMPTIONS.md`, current and target C4 views,
proposed ADRs, staged roadmap, and residual mount-review findings.
