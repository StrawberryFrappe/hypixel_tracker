# Release 1 Scaffold Review

- Date: 2026-07-13
- Verdict: Clean P0/P1 for scaffold scope
- Scope: Python/Vue workspace, independent migrations and roles, Compose,
  Caddy, CI, public documentation, and scaffold tests

## Review Process

Two real generic subagents independently reviewed backend/data/security and
frontend/CI/documentation. The configured cavecrew model was corrected from the
invalid `haiku` shorthand to `openai/gpt-5.6-sol`, but the running OpenCode
session retained its old registry and requires restart before cavecrew dispatch
can be verified. No reviewer identity or result was simulated.

Review rounds found and fixed:

- Non-transactional PostgreSQL role setup and unauthenticated health checks.
- Missing durable bootstrap completion proof and unsafe partial-init restart.
- Overbroad runtime privileges and connection-only readiness probes.
- Placeholder credentials accepted by runtime and migration configuration.
- API, collector, and static edge startup coupled to migration success.
- Broken FastAPI documentation under the edge content security policy.
- GitHub Pages manual-ref deployment and workflow-wide deployment authority.
- Mutable GitHub Action tags; actions now use full commit SHAs.
- Low-contrast terminal labels, pending-navigation semantics, and reduced-motion
  behavior.
- Inaccurate migration, startup, Node version, and network-free documentation.

Final backend and frontend review results: `CLEAN P0/P1`.

## Validation

- `uv sync --all-groups --locked`: passed.
- `uv lock --check`: passed.
- `uv run ruff check .`: passed.
- `uv run ruff format --check .`: 18 files formatted.
- `uv run pytest`: 20 passed.
- `npm ci`: passed; 177 packages installed.
- `npm test`: 1 passed.
- `npm run typecheck`: passed.
- `npm run build`: passed.
- `npm audit --audit-level=moderate`: 0 vulnerabilities.
- `uv run mkdocs build --strict`: passed.
- `docker compose --env-file .env.example config --quiet`: passed.
- Compose rendering without required variables: failed closed as expected.
- Raw and Application offline upgrade/downgrade SQL generation: passed.
- PostgreSQL bootstrap scripts: shell syntax and environment-guard tests passed.
- `git diff --check`: passed.
- GitHub CI run `29273575490`: Python, frontend, Compose, and gitleaks jobs passed
  with SHA-pinned current-major actions and no annotations.

## Residual Evidence Gaps

- Docker image build was attempted but no local Docker daemon socket exists.
- No live PostgreSQL 17 test yet proves role grants, bootstrap `NOLOGIN`, marker
  persistence, authenticated health, or online upgrade/downgrade.
- No Compose outage/restart test, Caddy HTTPS smoke, browser viewport/accessibility
  evidence, target `/srv` bind-mount override, or resource benchmark exists yet.
- Local `gitleaks` is unavailable; the SHA-pinned GitHub secret scan passed.
- Container base tags and GitHub-hosted runner labels remain mutable; deployment
  image digests require a later release manifest.

## Decision

Scaffold scope may advance to collector/raw implementation. This review does not
pass Release 1 architecture, data, deployment, browser, or recovery gates; the
residual integration evidence remains required before release.
