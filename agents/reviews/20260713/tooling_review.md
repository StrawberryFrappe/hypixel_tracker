# Deployment Laptop Agent Tooling Review

- Date: 2026-07-13
- Verdict: Clean; authentication intentionally deferred

## Installed

- Claude Code native 2.1.207; binary reinstalled, prior state preserved.
- OpenCode 1.17.18; model configured as `openai/gpt-5.6-sol`.
- Nonsecret global project rules, constrained cavecrew agents, and safe caveman
  mode/commit/review commands.

## Security

- SSH key matched prior trusted laptop key after DHCP address change; strict host
  checking and persistent known-host pinning enabled.
- OpenCode defaults to Plan, requires approval for edits/shell/tasks/web, denies
  external directories, disables sharing, and only notifies about updates.
- Reviewer and investigator deny all mutation/network tools. Builder is a
  subagent with shell/network/external access denied and edits requiring approval.
- Provider credentials, plugins, MCP headers, auth state, sessions, histories,
  caches, and source OpenCode configs were not copied.
- Risky cross-provider compression/statistics skills were removed.
- Interactive, login, and direct SSH PATH resolve both binaries.

## Validation

- Claude doctor: native/search/update installation healthy; authentication absent.
- OpenCode version/config/agent inventory: valid.
- Independent generic reviewer: `CLEAN TOOLING` after one fix round.

## Deferred

User must authenticate Claude Code and OpenCode. Provider connectivity and model
inference smoke tests remain blocked until then.
