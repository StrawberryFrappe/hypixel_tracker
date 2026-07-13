# Security

## Current Controls

- Compose refuses absent database passwords and DSNs.
- `.env` and runtime data are ignored; examples contain only explicit placeholders.
- Raw and Application PostgreSQL expose no host ports.
- Database networks are isolated and runtime roles are non-superuser roles.
- Migrators and runtimes use distinct credentials and URLs.
- API receives no Raw credentials and has no route to the Raw database network.
- Caddy is the sole inbound edge and binds loopback by default.
- Caddy provides local-CA TLS and baseline response security headers.
- Containers receive no Docker socket, privileged mode, or embedded production credentials.
- Database startup rejects development placeholders, reused credentials, invalid roles,
  and existing data directories lacking a completed bootstrap marker.
- Bootstrap database logins are disabled after least-privilege roles are created.
- Scaffold runtime roles can only read their version and sentinel tables; later
  migrations must grant each new capability explicitly.
- API health responses omit connection strings and exception details.

## Not Yet Security-Complete

Authentication and authorization are not part of this scaffold. Do not bind the
edge to a LAN interface yet. Stable hostname selection, CA trust distribution,
one-administrator authentication, scoped API keys, origin/CSRF controls, secret
provisioning, backups, and restore controls remain release gates.

Local HTTPS alone does not make an unauthenticated service safe. Loopback binding
is the safe scaffold default.

## Public Documentation Boundary

Public docs must not include credentials, private addresses, host access details,
raw API payloads, runtime data, backup manifests, or personal settings. Secret
scanning runs in CI, but review remains required.

## Compliance Boundary

Bazaar Guru is unofficial and advisory-only. Future collection must follow
current Hypixel policies, use respectful caching, and avoid becoming a public
proxy. The application must never execute or automate in-game actions.
