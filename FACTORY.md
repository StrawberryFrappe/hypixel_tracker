# FACTORY.md

## Current Objective

Build a lean, operable Hypixel Bazaar raw archive that can support future analytics and trading-bot schemas without recollecting historical data.

## Factory Roles

- Orchestrator: plans work, tracks decisions, verifies integration.
- Backend specialist: API, collector, Docker Compose, auth, exports.
- Data/database specialist: raw archive model, duplicate prevention, storage estimates, SQL sandbox policy.
- Integration specialist: Discord bot, Anthropic integration, alerting.
- QA specialist: API, collector, export, and bot verification.

## Backlog

- Add migration tooling if the archive schema changes after deployment.
- Add optional remote backup target when local storage pressure makes it necessary.
- Add derived analytics schemas from raw snapshots.
- Add richer Discord market commands for specific products and trend windows.
- Add production TLS/reverse-proxy documentation.

## Decisions

- Archive only the Hypixel SkyBlock Bazaar endpoint in v1.
- Store compressed exact response bodies plus queryable metadata.
- Use `lastUpdated` and SHA-256 response hash for duplicate prevention.
- Use Discord for alerts and expiring backup links, not as backup storage.
- Remove Adminer from the deployed system.
- Keep canonical archive tables immutable.
- Allow SQL analysis only through read-only, limited queries.

## Operational Criteria

- Secrets are environment variables.
- API requires `X-API-Key` except `/health`.
- Collector retries naturally through its polling loop and never inserts failed responses.
- Exports are compressed range bundles with expiration.
- Storage pressure is observable through `/storage/status` and Discord alerts.
