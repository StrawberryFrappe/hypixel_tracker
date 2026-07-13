# ADR 0001: Harness Mounting Policy

- Status: Proposed until user acceptance
- Date: 2026-07-13

## Context

The existing system is incomplete and has conflicting branch directions. The
project needs a durable operating contract before implementation resumes.

## Decision

Use the committed `agents/` tree as the project harness. Keep local memory under
ignored `agents/local/`. Require plans, independent review when available,
traceability, tests, and evidence. Stop unconditionally after mounting until the
user accepts the harness and surfaced assumptions.

## Consequences

Implementation is slower to start but less likely to repeat previous restarts or
silently change product direction. Acceptance of this ADR occurs with acceptance
of the mounted harness.
