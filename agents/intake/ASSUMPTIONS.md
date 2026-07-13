# Assumptions Register

These assumptions are surfaced for explicit user review. They are not accepted
architecture decisions.

The user accepted all entries on 2026-07-13, with ASM-007 corrected and ASM-008
accepted as a best-effort implementation goal rather than a promised network
capability. Expiration triggers still require evidence-based replacement.

| ID | Assumption | Risk | Expiration Trigger |
|---|---|---|---|
| ASM-001 | The inspected 3.22 MB Bazaar payload and approximately 20-second update cadence are representative enough for initial sizing | Storage may differ materially over time | Replace with a multi-day host benchmark |
| ASM-002 | The dedicated `/srv` disk can be used for project runtime data | Other future services may need its capacity | Confirm allocation and reserve headroom before deployment |
| ASM-003 | The LAN is private but not trusted enough for plaintext credentials | HTTPS setup may add client friction | Threat review and client inventory |
| ASM-004 | No current database contents need preservation | A reset could still surprise the user | Confirm again before any destructive database action |
| ASM-005 | Daily item metadata refresh is respectful and sufficient | Upstream behavior or policy may change | Policy review and observed metadata cadence |
| ASM-006 | Weekly manual download is an acceptable backup operator task | Missed downloads leave backups on the same host | First recovery-objective review and restore drill |
| ASM-007 | Forecast training runs on the current development workstation, not the two-core deployment laptop | Dataset/model transfer and environment parity need design | Release 2 training and transfer benchmark |
| ASM-008 | The project may establish stable access through mDNS, local DNS, or a DHCP reservation, but router support is not promised | TLS and clients fail when the address changes | Deployment network setup and second-device test |
| ASM-009 | Primary dashboard workflows should support keyboard operation, visible focus, readable contrast, and non-color-only status cues | Portfolio evidence could otherwise exclude users or hide state | Dashboard plan and accessibility review |
