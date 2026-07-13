# Harness Mount Review

- Date: 2026-07-13
- Scope: Project-specific harness readiness
- Verdict: Accepted by user on 2026-07-13
- Implementation status: Separate implementation plan and approval required

## Work Performed

- Inspected both code branches, current documentation, deployment configuration,
  tests, official upstream sources, live payload shape, and target-host capacity.
- Grilled the user on product, data, retention, ML, security, dashboard,
  deployment, recovery, and workflow decisions.
- Mounted 36 project-specific harness files including current and target C4
  architecture, SRS, ADRs, planning, traceability, validation, and reviews.
- Kept credentials and host access details out of committed documentation.
- Used a real generic `explore` subagent for independent adversarial review.

## Review Results

The first independent pass found 17 issues: two completion blockers, five P1
architecture/contract issues, eight P2 consistency/evidence issues, and two P3
documentation issues. All were addressed.

The second pass found four remaining C4 defects involving authorization flow,
CLI restore intake, rollback/resume/audit direction, and forecast-to-ranking
flow. All were fixed. The third pass returned `CLEAN FOR MOUNT VERDICT`.

## Required Checks

| Check | Result |
|---|---|
| Existing project rules preserved | Pass; no prior target `AGENTS.md` or conflicting rule file existed |
| Kernel refreshed before mounting | Pass; `d7a31eacd3f595b24df9c8245f94796cbd177325` |
| Project profile and brief populated | Pass |
| Source manifest and authority populated | Pass |
| User answers summarized | Pass |
| Assumptions visible in required read order | Pass |
| Initial ADRs created | Pass; seven proposed decisions |
| Current and target C4 architecture | Pass; 10 Mermaid blocks render successfully |
| Requirements, backlog, and traceability | Pass |
| Validation gates reflect product and risks | Pass |
| Capability scan completed honestly | Pass |
| Local memory and project work log ignored | Pass |
| Credentials absent from committed harness | Pass |
| Implementation gate explicit and unconditional | Pass |
| Independent review converged | Pass |
| Normal and strict doctor | Pass; final rerun reported 0 blockers and 0 warnings |

## Surfaced Assumptions Requiring User Review

1. One sampled Bazaar payload and its observed cadence are representative enough
   for initial sizing until a sustained benchmark replaces them.
2. `/srv` can be allocated to project data with reserved host headroom.
3. LAN credentials still require HTTPS even on a private network.
4. Existing database contents remain disposable, but any destructive action
   still requires task-specific approval.
5. Daily item metadata refresh is an acceptable initial policy candidate.
6. Weekly manual backup download is acceptable operator work.
7. Forecast training may need constrained scheduling or a different machine.
8. A stable hostname or DHCP reservation will be available for local HTTPS.
9. Primary dashboard workflows should meet the proposed keyboard, focus,
   contrast, and non-color-only status baseline.

## Open Pre-Implementation Gates

- User acceptance of this mounted harness.
- Target stack and migration decision.
- Current Hypixel policy and cadence review before collector activation.
- Target-host benchmark and accepted raw quota/curated retention.
- Local certificate distribution and API-key scope design.
- Exact backup contents, encryption, staging quota, and recovery objectives.
- Release 2 model metrics and acceptance thresholds.

## Direct, Delegated, and Skipped Work

- Direct: repository inspection, user interview, host preflight, source review,
  harness writing, fixes, and validation.
- Delegated: current-code audit, `origin/lean` audit, live payload analysis, item
  metadata analysis, mount design critique, and three review passes.
- Skipped by rule: application tests, builds, database operations, containers,
  deployment, browser testing, and application implementation.
- Not simulated: specialized cavecrew roles were unavailable and are recorded as
  a capability downgrade.

## Decision

The mounted harness is coherent and ready for the user's explicit accept/reject
decision. Acceptance approves this operating contract and permits a separate
implementation plan; it does not pre-approve application changes or deployment.

## User Acceptance

Accepted on 2026-07-13. User corrected ASM-007: forecasting training runs on the
current development workstation. User accepted ASM-008 as permission to attempt
stable LAN naming without promising router support. All other surfaced
assumptions were accepted.
