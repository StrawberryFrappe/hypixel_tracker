# Project Brief

## Product

A LAN-hosted, second-monitor trading assistant for the Hypixel SkyBlock Bazaar.
It collects market history, identifies evidence-backed trade opportunities, and
prepares reproducible data for later forecasting. It never trades automatically.

## Users

- One administrator and primary operator.
- Portfolio viewers evaluating screenshots or a supervised demonstration; they
  do not receive product accounts.

## Release 1 Outcomes

- Capture every observed distinct Bazaar source update as exact compressed raw
  data with integrity and collection metadata.
- Deterministically transform raw updates into queryable, versioned market
  history without silently skipping a backlog.
- Provide a coherent `/api/v1` contract and a polished data-dense market
  terminal for overview, product exploration, dataset export, and operations.
- Rank opportunities using configurable fees, liquidity/depth, profit percent,
  and volatility. Forecast signals enter only after model validation.
- Generate weekly downloadable backups and support tested dashboard and CLI
  restore paths.

## Later Outcomes

- Release 2: reproducible labels and an offline forecasting baseline for
  15-minute, 1-hour, and 24-hour horizons, including Hypixel calendar features.
  Training runs on the development workstation using versioned datasets.
- Release 3: serve accepted model predictions with freshness, uncertainty,
  provenance, monitoring, and rollback.

## Deployment

One Ubuntu laptop on the local network, accessed by its administrator through
local HTTPS. Durable data belongs on the dedicated `/srv` filesystem. The
service is best-effort and must recover automatically while making collection
gaps visible.

## Non-Goals

- Discord or LLM assistant integration.
- Automated in-game trading or guaranteed-profit claims.
- Native mobile application.
- Public Internet hosting in Release 1.
- Multi-server scaling.
- Preserving the current database; the user stated it has no valuable data.

## Success

The operator can open the dashboard on a second monitor, immediately judge data
freshness, inspect a product or ranked opportunity, export a reproducible
dataset, and verify recovery from a downloaded backup. The system must not hide
stale data or publish unreproducible forecasts.
