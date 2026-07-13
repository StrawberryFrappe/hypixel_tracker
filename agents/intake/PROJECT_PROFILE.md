# Project Profile

| Attribute | Assessment |
|---|---|
| Project type | Data collection, time-series analytics, private web application, operations, future ML |
| Lifecycle | Rescue and controlled rebuild of an incomplete prototype |
| Primary user | One administrator; portfolio viewers use evidence or supervised demos |
| Deployment | One Ubuntu laptop, Docker available, LAN-only HTTPS |
| Data sensitivity | Public market data plus private credentials, settings, API keys, and backups |
| Availability | Best-effort personal service with visible gaps and automatic recovery |
| Quality posture | Portfolio-quality UI and production-minded data integrity |
| Existing data | Disposable; no migration of valuable history required |

## Maturity Assessment

The current `main` branch is a prototype. It has no migration framework, CI,
real automated tests, frontend, retention policy, recovery path, or reliable
backlog processing. `origin/lean` contains useful archive concepts but is a
breaking pivot with unresolved security and operational faults.

## Highest Risks

1. Silent loss of normalized history when processing lags.
2. Unbounded raw and normalized storage growth.
3. Misleading trade rankings caused by stale data, fees, or thin liquidity.
4. Weak authentication, exposed management tools, or insecure defaults.
5. Backups that cannot restore the service.
6. Forecast leakage, unreproducible training data, or unsupported claims.
7. A cluttered dashboard that fails the second-monitor workflow.

## Active Quality Gates

Harness, policy, architecture, benchmark, data integrity, API security,
portfolio quality, export reproducibility, backup/restore, automated tests, and
deployment gates apply to Release 1. ML and prediction gates apply later.
