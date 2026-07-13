# Traceability

| Source or Decision | Requirements | ADRs | Work Items | Gates and Evidence |
|---|---|---|---|---|
| Kernel acceptance rule | Harness governance | 0001 | HM-WI-001 | GATE-HM, GATE-DR, mount review |
| Hybrid rescue decision | Release boundaries | 0002 | R1-WI-002, R1-WI-011 | GATE-RESCUE, architecture review |
| Official Bazaar schema and live payload | R1-FR-001, R1-FR-002, R1-FR-003, R1-FR-003A, R1-FR-004 through R1-FR-006, NFR-DATA-001/002 | 0003 | R1-WI-001, R1-WI-003 through R1-WI-005 | GATE-POLICY, GATE-BENCH, GATE-DATA, GATE-TEST |
| User-selected two-database topology | R1-FR-003A/003B/004/005A, NFR-DATA-003/004, NFR-QUAL-002 | 0003, 0007 | R1-WI-002, R1-WI-005 | GATE-ARCH, GATE-DATA, GATE-TEST, EV-006 scaffold evidence |
| Official Hypixel policy | NFR-COMP-001 | 0006 | R1-WI-001 | GATE-POLICY and dated policy evidence |
| LAN deployment and one-admin decision | R1-FR-007, R1-FR-016, NFR-SEC-001/002 | 0004 | R1-WI-006, R1-WI-010 | GATE-API-SEC, GATE-DEPLOY |
| Dashboard and trade ranking | R1-FR-008 through R1-FR-011, NFR-UX-001 | 0004, 0006 | R1-WI-006, R1-WI-007 | GATE-API-SEC, GATE-PQ, browser evidence |
| Dataset decision | R1-FR-012 | 0003 | R1-WI-008 | GATE-EXPORT, reproducibility evidence |
| Operations visibility decision | R1-FR-013, NFR-OPS-001 | 0003, 0005 | R1-WI-008, R1-WI-009, R1-WI-010 | GATE-TEST, GATE-BR, GATE-DEPLOY |
| Weekly download and restore decision | R1-FR-014/015, NFR-OPS-001/003 | 0005 | R1-WI-009, R1-WI-010 | GATE-BR, GATE-TEST, GATE-DEPLOY, failure matrix, break-glass, and clean restore drill |
| Stack open and host constraints | NFR-OPS-002, NFR-QUAL-001 | 0007 | R1-WI-002/003 | GATE-ARCH, GATE-BENCH, GATE-TEST, GATE-DEPLOY |
| Forecast horizons and staged serving | R2-FR-001 through R3-FR-003, NFR-ML-001 | 0002, 0003 | R2-WI-001/002, R3-WI-001/002 | GATE-ML, GATE-PRED |

## Update Rule

Every accepted implementation change must link requirement IDs, work items,
tests, and evidence. A changed requirement must update the SRS, C4 when its
architecture changes, related ADRs, and this table in the same work item.
