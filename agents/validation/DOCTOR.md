# Harness Doctor

## Mount Validation Commands

Run from the target repository root:

```bash
git diff --check -- AGENTS.md agents/
git ls-files --others --exclude-standard -- AGENTS.md agents/
while IFS= read -r file; do git diff --no-index --check /dev/null "$file"; code=$?; [ "$code" -eq 0 ] || [ "$code" -eq 1 ] || exit "$code"; done < <(git ls-files --others --exclude-standard -- AGENTS.md agents/)
git check-ignore -v --no-index agents/local/probe
python ../agent-harness-kernel/scripts/harness_doctor.py --root .
python ../agent-harness-kernel/scripts/harness_doctor.py --root . --strict
```

Also search active harness Markdown for the doctor placeholder markers, inspect
the complete mounted-file inventory, and review both tracked diffs and untracked
file contents.

## Initial Mount Result

- Date: 2026-07-13
- `git diff --check`: exit 0.
- Initial untracked-file `git diff --no-index --check` loop: exit 0 across the
  complete 35-file mount inventory.
- Local ignore probe: matched `agents/local/.gitignore:1:*`.
- Active placeholder-marker search: no matches.
- Normal doctor: exit 0, 0 hard blockers, 0 warnings before and after the final
  independent review.
- Strict doctor: exit 0, 0 hard blockers, 0 warnings before and after the final
  independent review.
- C4 render: all 11 Mermaid blocks rendered successfully through Mermaid CLI
  using the installed system Chrome; outputs were temporary under
  `/tmp/opencode`.

The dated mount review, complete mounted inventory, and ignored project-local
logbook were present for the final rerun.

## Post-Acceptance Architecture Validation

- Date: 2026-07-13
- Mounted inventory: 37 files after accepted release plan and architecture
  corrections.
- Strict doctor: exit 0, 0 hard blockers, 0 warnings.
- `git diff --check`: exit 0.
- C4 render: all 11 Mermaid blocks passed.

## Interpretation

- Missing required files or local-memory ignore rules are hard blockers.
- Active placeholders are hard blockers under strict mode.
- A zero doctor exit validates harness structure, not application correctness.
- Mount validation is historical evidence. Application work now follows the
  accepted stacked release plan and its independent review/test gates.
