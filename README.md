# gha-cache-oxide

A Rust port of [**github-actions-cache-server**](https://github.com/falcondev-oss/github-actions-cache-server) by [@falcondev-oss](https://github.com/falcondev-oss) — a self-hostable, drop-in replacement for GitHub's hosted Actions cache. It speaks the same HTTP protocol as `actions/cache`, so existing workflows work unchanged.

> **Status:** Early development. M1 (end-to-end smoke with filesystem + SQLite) in progress — see the [project milestones](https://github.com/mpecan/gha-cache-oxide/milestones) for what's landing when.

## What this is

- **Protocol** — GitHub Actions Cache v2 (Twirp RPCs + Azure-style block upload + plain download).
- **Storage** — pluggable drivers: filesystem and S3-compatible today, GCS deferred.
- **Metadata** — pluggable database: SQLite and Postgres today, MySQL deferred.
- **Auth** — GitHub Actions OIDC JWT verification against the public JWKS.

For the public HTTP contract with `actions/cache`, the upstream project is the specification; deviations are bugs.

## Quickstart with Docker

```sh
git clone https://github.com/mpecan/gha-cache-oxide.git
cd gha-cache-oxide
docker compose up --build
# in another terminal:
curl -fsS http://localhost:3000/health
# {"ok":true}
```

The compose stack runs the server with **filesystem storage + SQLite** on a
named Docker volume (`cache-data`). `API_BASE_URL=http://localhost:3000` is
pre-wired so cache clients see correct upload/download URLs out of the box.

Stop with `docker compose down` — data persists across restarts. To wipe
the cache state, `docker compose down -v` (removes the named volume).

The image is built from a `rust:alpine` (musl) builder onto
`gcr.io/distroless/static-debian12:nonroot`, so it ships as a fully-static
binary on a ~2 MiB base. No shell, no package manager, runs as the
non-root UID 65532.

## Configuration

Configuration is driven entirely by environment variables — names match upstream's [`lib/schemas.ts`](https://github.com/falcondev-oss/github-actions-cache-server/blob/main/lib/schemas.ts) verbatim. The authoritative list lives in [`src/config/`](./src/config).

Minimum required to boot:

| Var | Example | Notes |
|---|---|---|
| `API_BASE_URL` | `http://localhost:3000` | Base URL the server advertises to clients in signed URLs. |
| `STORAGE_DRIVER` | `filesystem`, `s3`, `gcs` | Required. Each driver unlocks its own set of vars (e.g. `STORAGE_FILESYSTEM_PATH`, `STORAGE_S3_BUCKET`). |
| `DB_DRIVER` | `sqlite`, `postgres`, `mysql` | Required. Each driver unlocks its own set of vars (e.g. `DB_SQLITE_PATH`, `DB_POSTGRES_URL`). |

Optional knobs with defaults: `PORT` (3000), `LOG_FORMAT` (`text`/`json`, default `text`), `CACHE_CLEANUP_OLDER_THAN_DAYS` (90), `DISABLE_CLEANUP_JOBS`, `ENABLE_DIRECT_DOWNLOADS`, `SKIP_TOKEN_VALIDATION`, `MANAGEMENT_API_KEY`, `DEFAULT_ACTIONS_RESULTS_URL` (default `https://results-receiver.actions.githubusercontent.com`), `PROXY_MAX_REQUEST_BODY_BYTES` (default 16 MiB = `16777216`).

`DEFAULT_ACTIONS_RESULTS_URL` is the receiver the catch-all fallback
proxy forwards unhandled paths to — `actions/cache` clients reach
beyond the cache surface for artifact summaries / session telemetry,
and matching upstream we forward those calls verbatim. Override it for
GitHub Enterprise endpoints or air-gapped deployments.

`PROXY_MAX_REQUEST_BODY_BYTES` caps the size of any single body the
fallback proxy will forward. Cache uploads use the explicit blob
routes and never reach the fallback, so this only limits the small
RPCs `actions/cache` makes against the receiver. Operators on
constrained networks can lower it; the default is generous (16 MiB).

Secrets (`AWS_SECRET_ACCESS_KEY`, `DB_POSTGRES_PASSWORD`, `DB_MYSQL_PASSWORD`, `DB_POSTGRES_URL`, `MANAGEMENT_API_KEY`) are redacted in startup logs.

## Deviations from upstream (cache HTTP API)

The `actions/cache` wire contract is the spec; the surface diverges from
upstream only in the following deliberate, tested ways:

- **`GetCacheEntryDownloadURL` probes storage before responding (#72).**
  Upstream serves the matched entry's URL unconditionally; if the blob
  has been wiped (operator deleted the bucket, S3 retention rule fired,
  parts dir was reaped without the merged copy), the `actions/cache`
  client gets the URL and 404s on the subsequent fetch. This port runs
  one `count_files_in_folder` probe on the matched location:
  - empty parts folder (not-yet-merged path) **or** empty location root
    (merged + `ENABLE_DIRECT_DOWNLOADS=on`) → delete the
    `storage_locations` row (FK CASCADE removes `cache_entries`),
    re-run `match_cache_entry`, return the next-best candidate
    (or `{ok:false}` if exhausted).
  - merged + `ENABLE_DIRECT_DOWNLOADS=off` is **not** probed; the
    server-mediated `/download/<id>` route has its own missing-blob
    recovery (lazy-merge fallback for #17).
  Capped at three probes per request to bound work on a misconfigured
  backend. Pinned by `tests/twirp_purge_retry.rs`. The narrow window
  "merged file deleted while parts/* still exist" is intentionally not
  caught by this coarse probe; that's the lazy-merge path's territory.

## Management API

A small REST/JSON surface is exposed under `/management` for operators
who want to inspect or prune the cache without going through the
GitHub Actions client. The whole sub-router is gated by
`MANAGEMENT_API_KEY` (sent as `Authorization: Bearer <KEY>`).

| Method | Path                             | Description                                                       |
|--------|----------------------------------|-------------------------------------------------------------------|
| `GET`  | `/management/cache-entries`      | Paginated list. Optional query params: `scope`, `repoId`, `page`, `itemsPerPage`. |
| `GET`  | `/management/cache-entries/{id}` | Single-entry fetch. Returns the row body or 404. |
| `GET`  | `/management/cache-entries/match` | Runs the same matching algorithm `GetCacheEntryDownloadURL` uses. Query: `primaryKey`, `version`, `repoId`, `scopes` (multi), `restoreKeys` (multi, optional). Returns `{match, type}` or 404. |
| `DELETE` | `/management/cache-entries`      | Bulk delete by query filter (`key`, `version`, `scope`, `repoId`). At least one filter required. Returns `{deleted: N}`. |
| `DELETE` | `/management/cache-entries/{id}` | Deletes the entry, its storage location row, and the folder on the storage adapter. Returns 204. |
| `GET`  | `/management/storage-locations`  | Paginated list. `page`, `itemsPerPage` query params (defaults 1 / 20, max 100). |
| `GET`  | `/management/storage-locations/{id}` | Single-location fetch. Returns the row body or 404. |
| `DELETE` | `/management/storage-locations/{id}` | Removes the row and its underlying folder. Returns 204; 404 if the row is already gone. |
| `POST` | `/management/cleanup/trigger`    | Runs one cleanup pass synchronously and returns the per-task counts as JSON. |

Auth behaviour:

- `MANAGEMENT_API_KEY` **unset** → every route returns `501 Not Implemented`
  with `{"statusCode":501,"message":"Management API not enabled - set MANAGEMENT_API_KEY"}`.
- Header missing / not a Bearer token → `401`.
- Wrong key → `401`.

### Deviation from upstream

Upstream exposes the same surface via [oRPC](https://orpc.unnoq.com/) under
`/management-api`, gated by the `x-api-key` header. This port deliberately
diverges:

1. Plain REST/JSON (no oRPC dependency, callable from `curl` / shell scripts).
2. `Authorization: Bearer <KEY>` instead of `x-api-key` — the more conventional
   token convention, requested in the original issue.
3. `DELETE /management/cache-entries/{id}` and `DELETE /management/storage-locations/{id}`
   return **404** when the id does not exist (upstream's Kysely `delete` is
   idempotent and silently returns void). REST-idiomatic — operators see "you
   got the id wrong" rather than a silent success.
4. `GET /management/cache-entries/match` returns **404** when no entry matches
   (upstream returns `200 OK` with body `null`). Status-code branching beats
   nullable-body branching for `curl` / shell-script clients.
5. `DELETE /management/cache-entries` (bulk filter) **rejects an empty filter
   set with 400** (upstream silently deletes every row), and **honours the
   `repoId` filter** (upstream's `deleteMany` accepts `repoId` in input but
   silently drops it from the WHERE — see `lib/api/cache-entries.ts:163-168`).
   Both divergences are pinned by tests; a future "fix to match upstream" will
   fail loud.
6. Bulk delete also returns `{"deleted": N}` (upstream returns nothing) so
   operators can verify the filter caught what they expected.
7. `findMany` filter accepts `scope` / `repoId` only, not `key` / `version` —
   open a follow-up issue if you need finer-grained list filtering.
8. No OpenAPI spec or oRPC `_rpc` surface yet — tracked separately as #77.

The wire shape of `cache_entries` / `storage_locations` rows themselves
matches upstream verbatim (camelCase keys), so scripts that decode either
server's responses see the same fields.

## Database

SQLite and Postgres are wired up. MySQL is deferred.

Schema migrations live under [`migrations/sqlite/`](./migrations/sqlite) and [`migrations/postgres/`](./migrations/postgres) and run automatically on startup. Column names match upstream's `lib/migrations.ts` verbatim (camelCase) — on Postgres the camelCase identifiers are double-quoted at write time so operators pointed at an upstream-populated database see the same shape.

Queries use `sqlx` runtime-checked `query_as` rather than the `query!` / `query_as!` macros — deliberately avoiding an offline-mode cache (`.sqlx/`) that would need regeneration every time a query changes. Schema correctness is instead enforced by unit tests exercising each query against an in-memory SQLite. If you later want compile-time checking, install `sqlx-cli` (`cargo install sqlx-cli --no-default-features --features sqlite,rustls`) and enable offline mode per-query.

## Benchmarks

`cargo bench` runs the criterion harness in [`benches/protocol.rs`](./benches/protocol.rs).
The default cell — filesystem storage + in-memory SQLite — needs no external
setup. Output is criterion's standard HTML report under `target/criterion/`.

### Groups

| Group                              | What it measures                                                  |
|------------------------------------|-------------------------------------------------------------------|
| `upload_finalize/<MiB>`            | Reserve → upload → finalize at 1 / 16 / 64 MiB. Reports MiB/s.    |
| `download_cold/16MiB`              | First download of an entry — exercises the lazy-merge path.       |
| `download_warm/16MiB`              | Subsequent downloads — exercises the merged-blob fast path.       |
| `match_cache_entry/<count>`        | `Db::match_cache_entry` over 10k / 100k seeded rows.              |

Cold vs. warm is the AC distinction issue #21 asks for: the cold bench
times the lazy-merge path (`merge::start_lazy_merge`); the warm bench
times the direct `object_store::get` on the merged blob.

### Comparing against upstream

Upstream's [`benchmark.ts`](https://github.com/falcondev-oss/github-actions-cache-server/blob/main/benchmark.ts)
reports total milliseconds for `TOTAL_REQUESTS` parallel 500 MiB
roundtrips at concurrency 20. Our harness reports per-operation
latency + throughput at smaller, varied sizes for criterion's
statistical model to converge. To compare, run upstream against the
same backend (filesystem + SQLite), divide their total by request
count, and contrast against our `upload_finalize/16` plus
`download_warm/16MiB` median latencies.

### Detecting regressions

```sh
# Snapshot a baseline.
cargo bench -- --save-baseline main

# After a change:
cargo bench -- --baseline main
```

Criterion flags throughput drops > ~5 % with statistical confidence — enough
to catch e.g. a missing index on `cache_entries(scope, repoId)` without
extra tooling. The matcher group's per-row scaling (10 k vs. 100 k)
makes index regressions especially loud.

### Other matrix cells (planned)

The default cell is fs + sqlite. Issue #21 calls for a matrix over
`{filesystem, s3} × {sqlite, postgres}` switched via env vars
(`BENCH_DB`, `BENCH_STORAGE`). Those cells are **not yet wired** —
the bench harness today is hard-coded to fs + sqlite. Adding them
is straightforward but out of scope for this PR; the conformance
suites for storage (#11) and DB (#13) already give cross-driver
*correctness* coverage. Open a follow-up issue if you need
cross-driver *perf* numbers.

## Acknowledgements

This project would not exist without [github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server) by [falconDev IT GmbH](https://github.com/falcondev-oss). Their TypeScript implementation is the reference we port from — including the observable HTTP behaviour, the storage abstraction, and the lazy merge-on-first-download design. Any correctness this port has is owed to their work; any bugs are ours.

If you're looking for a production-ready, battle-tested cache server today, use theirs. This port exists to offer a Rust-native alternative for deployments that prefer the Rust runtime profile, not to replace the original.

## License

MIT. See [LICENSE](./LICENSE) for the full text and [NOTICE](./NOTICE) for attribution details. The upstream project's copyright is preserved in both files.
