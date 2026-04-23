# Implement Issue

Plan → implement → review → remediate → PR for a `gha-cache-oxide` GitHub issue, with support for stacked PRs when issues form a dependency chain.

## Input

The user provides a GitHub issue number: $ARGUMENTS

If `$ARGUMENTS` is empty, ask for it before doing anything else.

## Phase 1: Load context

1. Run the context loader to fetch everything in one shot:
   ```
   .claude/scripts/load-issue-context.sh <number>
   ```
   Output includes: issue details, all comments, dependency issue states, open `feat/` PRs, current git state.
2. Read `CLAUDE.md` so the plan respects the constitution (drop-in compatibility, tested-or-not-shipped, simplicity, correctness, transparency) and the code standards (clippy denies, file-length limits, scopes).
3. From the script output, confirm every `Depends on #N` is either closed/merged or represented by a live `feat/` PR we can stack on.

### Stacking decision

Decide whether this issue **stacks on a previous branch** or **starts fresh from main**.

1. If any dependency has an unmerged `feat/` PR branch → **stack on that branch**.
2. If all dependencies are merged (or there are none) → **branch from main**.
3. If more than one unmerged predecessor exists → STOP and surface to the user; those predecessors must merge first.

```sh
# Stacking:
git fetch origin <predecessor-branch>
git checkout -b feat/<number>-<slug> origin/<predecessor-branch>

# Fresh from main:
git fetch origin main
git checkout -b feat/<number>-<slug> origin/main
```

Report the decision explicitly:
```
Stacking: feat/<this> → feat/<pred> → ... → main
PR will target: feat/<pred>
```
or:
```
Fresh branch: feat/<this> from main
PR will target: main
```

**Record this in the plan** — it must survive context compression.

## Phase 2: Plan

1. `EnterPlanMode`
2. Explore the relevant code areas (Grep / Glob / `Explore` subagent for medium-depth scans). For a port of an upstream file, skim the corresponding TypeScript in `../github-actions-cache-server/` so the Rust port is a faithful translation.
3. Design the implementation:
   - Files to create / modify
   - Types, traits, functions (with rough signatures)
   - Integration points with existing modules
   - Tests to write, TDD-first
   - Any conformance-suite scenarios to extend (for storage / db drivers)

### Plan contents — must include all five sections

#### 1. Branch & stacking
```markdown
## Branch Strategy
- **Branch name:** `feat/<number>-<slug>`
- **Base branch:** `main` | `feat/<pred>`
- **PR target:** `main` | `feat/<pred>`
- **Create command:** `git fetch origin <base> && git checkout -b feat/<number>-<slug> origin/<base>`
```

#### 2. Implementation plan
The actual code changes — files, types, signatures, integration points, rough line counts. For ports of upstream files, reference the upstream path (e.g. `lib/storage.ts#matchCacheEntry`).

#### 3. Implementation order
Numbered TDD steps:
1. Create the branch (exact command from §1)
2. Write failing tests
3. Implement until tests pass
4. Quality gate (§4 of the post-impl checklist)
5. Open PR

#### 4. Conformance / parity impact
Which parity surfaces this touches:
- [ ] Extends the storage conformance suite? (issues #11)
- [ ] Extends the DB conformance suite? (issue #13)
- [ ] Affects the E2E golden files? (issue #10)
- [ ] Changes the env contract? → update `CLAUDE.md` / README if so

#### 5. Post-implementation checklist
Paste verbatim:

```markdown
## Post-Implementation Checklist

### Quality gate (all must pass)
- [ ] `cargo fmt`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo test`
- [ ] File-length check: every touched file ≤ 500 lines soft / 700 hard
- [ ] No `.unwrap()` / `.expect()` / `panic!` / `todo!` outside `#[cfg(test)]`

### Multi-agent review (4 agents in parallel)
1. **Acceptance criteria** — each criterion PASS/FAIL with file:line evidence
2. **Code quality** — file length, cognitive complexity (≤15), function length (≤60 lines), function args (≤5), duplication, naming, error handling via `thiserror` / `Result`
3. **Architecture** — module boundaries, public surface minimal (prefer `pub(crate)`), parity with upstream behaviour where applicable
4. **Test coverage** — public API coverage, edge cases, storage/DB conformance extensions where applicable, meaningful assertions

Each agent receives:
- Issue description + acceptance criteria
- Diff command: `git diff <base-branch>...HEAD`

### Remediation
- Fix all MAJOR findings; re-run quality gate; re-review changed areas
- Present MINOR findings to the user for decision

### PR
- Push: `git push -u origin <branch>`
- `gh pr create --base <target> --title "..." --body "..."`
- Body: `Closes #<number>`, summary, test plan, stack section if stacked
- Wait for CI green (`gh pr checks <num> --watch`); fix failures if any
- Report PR URL and next issue in sequence (if any)
```

Write the plan via plan-mode and **stop**. Wait for user approval before proceeding.

## Phase 3: Implement

1. Create the branch per §1 of the approved plan
2. `TaskCreate` one task per implementation step; mark them as you go
3. TDD:
   - Write the tests first
   - Run `cargo test` to confirm they fail
   - Implement until they pass
   - Refactor with tests still green
4. Respect project standards (denied clippy lints, file limits, no planning docs in-repo)

## Phase 4: Quality gate

```sh
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

All three clean before proceeding. If a test needs external services, mark it `#[ignore]` and document how to run it.

## Phase 5: Multi-agent review

Spawn **four** review agents in parallel. Scope the diff to this issue only:

```sh
# Stacked PR:
git diff <predecessor-branch>...HEAD
# Fresh branch:
git diff origin/main...HEAD
```

Pass the issue body + acceptance criteria + correct diff command to each agent. Agents work from `git diff` output, not file reads.

### Agent 1 — Acceptance criteria
**subagent_type:** `general-purpose`
```
Review the changes (using the provided diff command) against the issue's acceptance criteria.
For each criterion: PASS or FAIL with file:line evidence.
Flag partially-met or ambiguous criteria.
```

### Agent 2 — Code quality & standards
**subagent_type:** `general-purpose`
```
Review for:
- File length (soft 500 / hard 700)
- No .unwrap()/.expect()/.panic!()/todo!() outside #[cfg(test)]
- Function length ≤60, cognitive complexity ≤15, function args ≤5
- Type complexity ≤200
- Duplication vs existing patterns
- Naming consistent with surrounding code
- Error handling via thiserror / Result<T, _> (no ad-hoc string errors in library code)
- Dependency additions justified
Rate each file: CLEAN / MINOR / MAJOR.
```

### Agent 3 — Architecture & parity
**subagent_type:** `general-purpose`
```
Review for:
- Module boundaries (new types live in the right module?)
- Public surface minimal (default to pub(crate))
- Integration correctness with AppState / middleware layers
- Upstream parity where applicable — flag any observable behaviour diverging from falcondev-oss/github-actions-cache-server without justification
- Downstream impact on open issues in the stack
Rate: CLEAN / MINOR / MAJOR.
```

### Agent 4 — Test coverage & correctness
**subagent_type:** `general-purpose`
```
Review for:
- All new public functions/methods covered by tests
- Edge cases: empty input, concurrent access, error paths, boundary conditions
- Assertions actually check meaningful behaviour (not just "doesn't panic")
- Storage/DB conformance suite extended where applicable (#11, #13)
- E2E golden files updated where response shapes changed (#10)
- Integration tests where they're required by the issue
Rate: CLEAN / MINOR / MAJOR.
```

### Synthesis

```markdown
## Review Summary
### Verdict: PASS / PASS WITH MINOR / NEEDS REMEDIATION
### Acceptance Criteria: X/Y

### Findings by severity
#### MAJOR (must fix before PR)
- [ ] <finding> — <file:line> (from: <agent>)

#### MINOR (fix or skip with user decision)
- [ ] <finding> — <file:line> (from: <agent>)

#### NOTES
- <observation> (from: <agent>)
```

## Phase 6: Remediate

- MAJOR → fix, re-run quality gate (§4), re-review only the changed areas
- MINOR → present to user for decision via `AskUserQuestion`:
  - "Create PR as-is"
  - "Fix minor items first"
  - "I'll review the changes myself first"

**Stop and wait for user confirmation** before opening the PR.

## Phase 7: PR

1. `git push -u origin feat/<number>-<slug>`
2. Create PR with the correct base:
   ```sh
   # Stacked:
   gh pr create --base feat/<pred> --title "..." --body "..."
   # Fresh:
   gh pr create --base main --title "..." --body "..."
   ```
3. PR body:
   - Title: conventional commit matching the issue (e.g. `feat(api): implement reserve cache endpoint`)
   - Body:
     - `Closes #<number>`
     - Summary
     - Test plan
     - Parity notes — any deviation from upstream behaviour, with reason
     - If stacked:
       ```
       ## Stack
       - #<PR-N> ← **this PR**
       - #<PR-N-1> (base)
       - main
       ```
4. Wait for CI green:
   ```sh
   gh pr checks <pr-number> --watch
   ```
   - Green → report URL
   - Red → investigate, fix, push, wait again
5. If a next issue can stack on this one, tell the user:
   ```
   Next in sequence: #<next> — <title>
   Run: /implement-issue <next>
   ```

## Conventions

- Branch: `feat/<number>-<slug>` (`feat/4-sqlx-sqlite-baseline`)
- Commits: conventional commits — types `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`, `perf`, `build`; scopes `api`, `storage`, `db`, `config`, `cli`, `server`, `auth`, `tasks`, `ops`
- Reference the issue number in commit messages
- One logical change per commit; squash noise commits before pushing
- When porting a specific upstream file, open the commit message body with `Port of <upstream path>@<upstream SHA>` so drift is traceable

## Stacking reference

### Linear chain (works)
```
main ← feat/3-config ← feat/4-db ← feat/7-matching
         PR #A (→main)  PR #B (→#3) PR #C (→#4)
```
Each PR shows only its own diff. Merge bottom-up.

### Fan-out (stacking stops)
```
feat/4-db ─┐
           ├─► feat/7-matching     ← PR C
           └─► feat/8-twirp        ← PR D (also needs #5, #6)
```
When two siblings share a dependency that isn't merged yet, their shared dependency must merge to `main` before either can proceed independently.

### After a stacked PR merges
When the base of the stack merges to main:
```sh
gh pr edit <next-pr> --base main
# or rebase locally:
git rebase --onto main feat/<merged> feat/<next>
```
