# gha-cache-oxide

A Rust port of [**github-actions-cache-server**](https://github.com/falcondev-oss/github-actions-cache-server) by [@falcondev-oss](https://github.com/falcondev-oss) — a self-hostable, drop-in replacement for GitHub's hosted Actions cache. It speaks the same HTTP protocol as `actions/cache`, so existing workflows work unchanged.

> **Status:** Early development. M1 (end-to-end smoke with filesystem + SQLite) in progress — see the [project milestones](https://github.com/mpecan/gha-cache-oxide/milestones) for what's landing when.

## What this is

- **Protocol** — GitHub Actions Cache v2 (Twirp RPCs + Azure-style block upload + plain download).
- **Storage** — pluggable drivers: filesystem and S3-compatible today, GCS deferred.
- **Metadata** — pluggable database: SQLite today, Postgres and MySQL coming.
- **Auth** — GitHub Actions OIDC JWT verification against the public JWKS.

For the public HTTP contract with `actions/cache`, the upstream project is the specification; deviations are bugs.

## Configuration

Configuration is driven entirely by environment variables — names match upstream's [`lib/schemas.ts`](https://github.com/falcondev-oss/github-actions-cache-server/blob/main/lib/schemas.ts) verbatim. The authoritative list lives in [`src/config/`](./src/config).

Minimum required to boot:

| Var | Example | Notes |
|---|---|---|
| `API_BASE_URL` | `http://localhost:3000` | Base URL the server advertises to clients in signed URLs. |
| `STORAGE_DRIVER` | `filesystem`, `s3`, `gcs` | Required. Each driver unlocks its own set of vars (e.g. `STORAGE_FILESYSTEM_PATH`, `STORAGE_S3_BUCKET`). |
| `DB_DRIVER` | `sqlite`, `postgres`, `mysql` | Required. Each driver unlocks its own set of vars (e.g. `DB_SQLITE_PATH`, `DB_POSTGRES_URL`). |

Optional knobs with defaults: `PORT` (3000), `LOG_FORMAT` (`text`/`json`, default `text`), `CACHE_CLEANUP_OLDER_THAN_DAYS` (90), `DISABLE_CLEANUP_JOBS`, `ENABLE_DIRECT_DOWNLOADS`, `SKIP_TOKEN_VALIDATION`, `MANAGEMENT_API_KEY`.

Secrets (`AWS_SECRET_ACCESS_KEY`, `DB_POSTGRES_PASSWORD`, `DB_MYSQL_PASSWORD`, `DB_POSTGRES_URL`, `MANAGEMENT_API_KEY`) are redacted in startup logs.

## Database

SQLite is the only driver wired up today (M1). Postgres lands in #14, MySQL is deferred.

Schema migrations live under [`migrations/sqlite/`](./migrations/sqlite) and run automatically on startup. Column names match upstream's `lib/migrations.ts` verbatim (camelCase) so anyone debugging against an upstream-created DB sees the same shape.

Queries use `sqlx` runtime-checked `query_as` rather than the `query!` / `query_as!` macros — deliberately avoiding an offline-mode cache (`.sqlx/`) that would need regeneration every time a query changes. Schema correctness is instead enforced by unit tests exercising each query against an in-memory SQLite. If you later want compile-time checking, install `sqlx-cli` (`cargo install sqlx-cli --no-default-features --features sqlite,rustls`) and enable offline mode per-query.

## Acknowledgements

This project would not exist without [github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server) by [falconDev IT GmbH](https://github.com/falcondev-oss). Their TypeScript implementation is the reference we port from — including the observable HTTP behaviour, the storage abstraction, and the lazy merge-on-first-download design. Any correctness this port has is owed to their work; any bugs are ours.

If you're looking for a production-ready, battle-tested cache server today, use theirs. This port exists to offer a Rust-native alternative for deployments that prefer the Rust runtime profile, not to replace the original.

## License

MIT. See [LICENSE](./LICENSE) for the full text and [NOTICE](./NOTICE) for attribution details. The upstream project's copyright is preserved in both files.
