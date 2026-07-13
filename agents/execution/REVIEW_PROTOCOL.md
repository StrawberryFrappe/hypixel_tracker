# Review Protocol

## Severity

- P0: safety, data-loss, credential, policy, or correctness blocker.
- P1: release blocker unless explicitly mitigated and accepted.
- P2: should fix in scope or document with an owner and trigger.
- P3: optional improvement.

## Required Review Types

- Harness mount and assumption review.
- Plan review before substantial implementation.
- Data integrity and lifecycle review.
- Security and trust-boundary review.
- API and migration review.
- UX and real-browser review for dashboard changes.
- Backup/restore and deployment review.
- Leakage and temporal-evaluation review for ML work.
- Final release review.

## Rules

- Use a real independent subagent when available and identify its actual type.
- Do not label simulated perspectives as independent review.
- Findings cite files, lines, commands, source evidence, screenshots, or test
  output.
- Review all changed files and related contracts, not only the latest edit.
- Fix every P0/P1 before proceeding unless the user explicitly accepts a
  documented mitigation.
- A blocking finding updates `RUN_STATE.md`, relevant work items, and gates.
- Rerun affected validation after fixes.

## Capability Downgrade

This platform provides generic `explore` subagents, which are suitable for
read-only audit and review. The specialized cavecrew investigator, builder, and
reviewer names referenced by the user's global workflow are unavailable. Use
generic subagents honestly and perform implementation directly when no matching
builder exists.
