# Capability Scan

- Date: 2026-07-13
- Scope: Harness mounting and pre-implementation investigation

## Available

| Capability | Status | Evidence |
|---|---|---|
| Repository file reads, glob, and content search | Available | Full target and kernel inventories completed |
| Git inspection | Available | Branches, commits, status, and diffs inspected |
| Parallel shell and file tools | Available | Used for repository and host preflight |
| Generic read-only subagents | Available | `explore` agents audited current code, `origin/lean`, live schemas, and mount structure |
| Web fetch | Available | Official API docs and policy inspected |
| Remote SSH inspection | Available | Target host hardware, storage, Docker, network, and clock inspected read-only |
| Code editing | Available after approval | Used only for approved harness files during mounting |

## Unavailable or Not Exercised

| Capability | Status | Consequence |
|---|---|---|
| Specialized cavecrew subagent names | Unavailable | Use generic subagents honestly; do not simulate specialist identities |
| Browser UI verification | Not exercised | No frontend exists; browser gate remains future work |
| Application tests/builds | Prohibited before harness acceptance | Current application behavior remains static-audit evidence only |
| Deployment mutation | Not exercised | Host preflight is evidence, not deployment validation |

## Review Independence

Generic `explore` subagents are real separate sessions and may perform read-only
independent review. Implementation remains direct unless an available agent type
matches the task.
