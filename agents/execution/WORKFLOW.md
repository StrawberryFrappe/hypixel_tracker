# Workflow

## Harness Mounting

1. Inspect repository rules, code, docs, tests, branches, and deployment.
2. Run capability scan and independent read-only audits.
3. Grill the user on product, data, deployment, security, quality, and workflow.
4. Mount adapted project documents and surface every assumption.
5. Run independent harness review and fix findings.
6. Run normal and strict harness doctor validation.
7. Stop for explicit user acceptance.

The acceptance stop is unconditional. No application code edit, build, compile,
deployment, or project-tooling run occurs before acceptance. Only an explicit
user override issued after a concrete risk disclosure can bypass it; prior
approval or silence cannot.

## Implementation Work

1. Read the harness and relevant latest reviews.
2. Investigate current files and callers without mutation.
3. Write a scoped plan with requirements, files, risks, tests, and evidence.
4. Obtain explicit user approval before the first mutation.
5. Review the plan with an independent subagent when available.
6. Implement the smallest accepted scope.
7. Update migrations, docs, traceability, and evidence as part of the change.
8. Run review; fix every unresolved P0/P1 and address or document lower findings.
9. Run linked tests and gates.
10. Report direct, delegated, skipped, simulated, deferred, and blocked work.

## Project-Specific Safety Rules

- Do not merge `origin/lean` wholesale.
- Do not reset or destroy a database without explicit task-specific approval,
  even though current data is considered disposable.
- Schema changes start with migrations and rollback evidence.
- Raw pruning requires verified transform lineage and an accepted quota.
- Deployment changes require target-host preflight and a rollback path.
- Recommendations must expose freshness, fees, liquidity, and uncertainty.
- Forecast work cannot cross the Release 2 or Release 3 acceptance boundaries.

## Drift Protocol

If work departs from accepted requirements, architecture, or gates:

1. Stop forward work.
2. Record the deviation under ignored `agents/local/`.
3. Create `agents/execution/HANDOFF.md` from the kernel template only for a real
   handoff.
4. Ask the user to approve a revised plan before continuing.
