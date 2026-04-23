# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Constitution

1. **Drop-in compatible** — this is a Rust port of [github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server). The public HTTP contract with `actions/cache` is the specification; deviation is a bug.
2. **If it is not tested, it is not shipped** — every handler, every storage driver, every reservation path has tests. Integration tests talk real HTTP; storage drivers are exercised against real backends (filesystem, object store via Docker), not mocks.
3. **Simplicity is king** — no speculative abstractions. Three similar lines beats a premature trait. Add a `StorageDriver` variant when the second driver lands, not before.
4. **Correctness over speed** — match the TypeScript server's observable behaviour first; benchmark and optimise later, with numbers.
5. **Transparent in all** — structured logging, honest error codes, documented defaults. Never silently fall back or swallow errors.

## Project Overview

`gha-cache-oxide` is a self-hostable, drop-in replacement for GitHub's hosted Actions cache, implemented in Rust. It speaks the protocol used by `actions/cache` so existing workflows work unchanged.

- **Binary:** `gha-cache-oxide` (HTTP server)
- **Protocol:** GitHub Actions cache HTTP API (reserve → upload parts → commit → download)
- **Storage:** pluggable drivers (filesystem, S3-compatible, etc.)
- **Metadata:** pluggable DB (SQLite default)
- **Upstream reference:** [falcondev-oss/github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server) — match its behaviour unless there's a documented reason to diverge.

The codebase is at an early stage. When adding the first slice of a subsystem (HTTP routes, storage trait, DB layer), land it behind a minimal, tested vertical slice before generalising.

## Commands

```sh
cargo build                                             # Build
cargo test                                              # Run all tests
cargo test <name>                                       # Run a single test by name substring
cargo test -- --ignored                                 # Run tests that need external services
cargo clippy --all-targets -- -D warnings               # Lint (must pass clean)
cargo fmt                                               # Format
cargo fmt -- --check                                    # Check formatting without writing
cargo run                                               # Run the server
```

Before every commit, all three must pass clean:

```sh
cargo fmt
cargo clippy --all-targets -- -D warnings
cargo test
```

## Code Standards

### Enforced limits (`clippy.toml`)

| Limit                    | Value |
|--------------------------|-------|
| Function length          | 60 lines |
| Cognitive complexity     | 15 |
| Function arguments       | 5 |
| Type complexity          | 200 |

### Clippy rules (`Cargo.toml` → `[lints.clippy]`)

- **Denied:** `unwrap_used`, `expect_used`, `panic`, `todo`
- **Warned:** `pedantic`, `nursery` groups
- **Allowed:** `module_name_repetitions`, `must_use_candidate`

`#[allow(...)]` belongs in test code (`#[cfg(test)]` modules). In production code, propagate `Result` or pattern-match — do not reach for `.unwrap()` / `.expect()` / `panic!()`.

### File length

- **Soft limit: 500 lines** — split before you cross it.
- **Hard limit: 700 lines** — must be refactored before merging.

### Error handling

- Use `thiserror` for library/module-level errors; derive `From` for conversions.
- HTTP errors map to the status codes the GitHub client expects. Don't invent new codes — check upstream behaviour first.
- No silent fallbacks. If a storage backend is misconfigured, fail loud at startup, not on the first request.

### Testing

- Unit tests: `#[cfg(test)] mod tests` in the source file.
- Integration tests: `tests/` directory — drive the HTTP surface via a real server bound to a random port.
- Storage driver tests run against real backends. Mark tests requiring external services (Docker, S3, etc.) with `#[ignore]` and run with `cargo test -- --ignored`.
- Every bug fix starts with a failing test.

## Conventions

### Commits

[Conventional Commits](https://www.conventionalcommits.org/) strictly. One logical change per commit.

Types: `feat`, `fix`, `refactor`, `test`, `docs`, `chore`, `ci`, `perf`, `build`

Suggested scopes (grow organically as the code does): `api`, `storage`, `db`, `config`, `cli`, `server`.

Examples:
- `feat(api): implement reserve cache endpoint`
- `fix(storage): handle concurrent part uploads for same reservation`
- `test(db): add sqlite schema migration test`

### Dependencies

- Reach for reputable crates over hand-rolling. Check maintenance activity and transitive footprint before adding.
- Pin versions in `Cargo.toml`. Justify new crates in the PR description.

### Naming

- `snake_case` for Rust; modules named after the domain concept, not the implementation (`storage`, not `s3_helpers`).
- Keep public surface minimal — prefer `pub(crate)` over `pub`.

## Claude Code Commands

Custom slash commands live in `.claude/commands/`:

- `/create-issue` — draft and file a GitHub issue against this repo with project-specific context.
- `/implement-issue <number>` — plan → implement → review → PR cycle for a filed issue, with stacked-PR support when issues depend on each other. Driven by `.claude/scripts/load-issue-context.sh`, which resolves `Depends on #N` lines in issue bodies.

## Reference

- Upstream TypeScript server: https://github.com/falcondev-oss/github-actions-cache-server
- `actions/cache` client: https://github.com/actions/cache
