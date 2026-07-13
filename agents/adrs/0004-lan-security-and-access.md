# ADR 0004: LAN Security and Access

- Status: Proposed until security review
- Date: 2026-07-13

## Context

The service is private but credentials over plain LAN HTTP can be observed.
Current `main` exposes Adminer with documented owner credentials.

## Decision

Use local HTTPS and one administrator account. Do not create viewer accounts or
share the administrator credential; portfolio viewers use screenshots or a
supervised demonstration. Separately scoped revocable API keys are for approved
automation clients, not user identity. Bind only to the intended LAN interface,
remove Adminer from normal deployment, prohibit default credentials, keep the
database unexposed, and make authorization fail closed.

## Consequences

A stable hostname and trusted local certificate distribution are deployment
requirements. Authentication recovery must not depend on an exposed database UI.
