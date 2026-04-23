# Create Issue

Draft a well-formed GitHub issue for `gha-cache-oxide` from a short description, then file it.

## Input

The user provides a short description of the work: $ARGUMENTS

If `$ARGUMENTS` is empty, ask the user what the issue is about before doing anything else.

## Phase 1: Understand the request

1. Read `CLAUDE.md` so the issue reflects project conventions (constitution, scopes, file-length limits, clippy rules).
2. Skim the areas of the codebase most relevant to the request (Grep/Glob). If the request is vague or cross-cutting, spawn the `Explore` agent for a medium-depth scan rather than reading files one by one.
3. Check for existing related issues and open PRs:
   ```sh
   gh issue list --state all --search "<keywords>"
   gh pr list --state all --search "<keywords>"
   ```
   If a clear duplicate exists, stop and surface it to the user with a link — do not file a second one.

## Phase 2: Classify

Pick one label/type based on what the work is:

- `feat` — new user-visible capability
- `fix` — bug fix (include repro steps + expected vs actual)
- `refactor` — internal restructuring, no behaviour change
- `perf` — performance work (must include a measurable target)
- `test` — adding tests to existing behaviour
- `docs` — documentation only
- `chore` / `ci` / `build` — tooling, CI, build plumbing

If the description spans more than one type, propose splitting it into multiple issues and confirm with the user before filing.

## Phase 3: Draft

Draft the issue body using this template. Fill every section; delete sections only if they genuinely don't apply, and say why.

```markdown
## Context
<Why this matters — what user, workflow, or constraint is driving it. Link upstream behaviour in github-actions-cache-server if relevant.>

## Proposal
<What to build or change, at the level of the observable behaviour. Not a full implementation plan.>

## Acceptance Criteria
- [ ] <Concrete, verifiable criterion — prefer "endpoint X returns 204 on Y" over "works correctly">
- [ ] <...>

## Out of Scope
- <Explicitly list adjacent work that is NOT part of this issue>

## Notes / Open Questions
<Anything ambiguous that the implementer will need to decide, or that the user should weigh in on.>
```

**Title:** conventional-commit style — `<type>(<scope>): <imperative short description>` (e.g. `feat(api): implement reserve cache endpoint`). Scope comes from the CLAUDE.md list (`api`, `storage`, `db`, `config`, `cli`, `server`) or omit if cross-cutting.

**Dependencies:** if this issue needs another issue to land first, add a `## Dependencies` section at the bottom of the body:
```markdown
## Dependencies
Depends on #<number>
Depends on #<other>
```
`/implement-issue` (via `.claude/scripts/load-issue-context.sh`) greps for `Depends on #N` lines to build the stacking chain — one dependency per line.

## Phase 4: Confirm, then file

1. Show the user the drafted title + body.
2. Use `AskUserQuestion` to confirm, with options:
   - "File as-is"
   - "Edit before filing" — let the user dictate changes, redraft, reconfirm
   - "Cancel"
3. On confirm, file it:
   ```sh
   gh issue create --title "<title>" --body "$(cat <<'EOF'
   <body>
   EOF
   )"
   ```
4. Report the issue URL back to the user.

## Conventions

- Write issues so a future contributor (or Claude Code running `/implement-issue`) can act on them without asking the author for clarification.
- Acceptance criteria are concrete and testable. "Feels fast" is not a criterion; "p95 download latency for a 100 MiB entry under 200 ms on local filesystem" is.
- Prefer several focused issues over one mega-issue. If the draft acceptance list exceeds ~6 items or spans multiple subsystems, split it.
- Never invent requirements the user didn't state. If the request is under-specified, surface the ambiguity in **Notes / Open Questions** rather than guessing.
