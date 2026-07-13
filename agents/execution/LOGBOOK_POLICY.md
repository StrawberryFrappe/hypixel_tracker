# Logbook Policy

## Local-Only Material

Keep raw Q&A, temporary prompts, copied payloads, credentials, access details,
scratch calculations, and task-by-task notes under `agents/local/`. This tree is
ignored except for its `.gitignore` sentinel.

## Durable Material

Promote facts that should survive sessions into:

- intake docs for product and source authority;
- ADRs for accepted choices and consequences;
- SRS and C4 for requirements and architecture;
- backlog and traceability for planned work;
- validation docs for gates and evidence;
- dated reviews for findings and decisions.

## Security

Never place passwords, tokens, private keys, session cookies, downloadable
backup contents, or raw production payloads in committed docs or logs. If a
secret is disclosed in conversation, avoid reproducing it and recommend
rotation.

## Entry Minimum

Local task entries should record date, scope, changed files, why, review result,
tests, deviations, and unresolved blockers. Summaries must distinguish observed
evidence from assumptions.
