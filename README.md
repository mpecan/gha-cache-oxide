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
`gcr.io/distroless/static-debian13:nonroot` (both digest-pinned), so it ships as a fully-static
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

Optional knobs with defaults: `PORT` (3000), `LOG_FORMAT` (`text`/`json`, default `text`), `CACHE_CLEANUP_OLDER_THAN_DAYS` (90), `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS` (unset), `DISABLE_CLEANUP_JOBS`, `CLEANUP_UPLOADS_SCHEDULE` (`*/5 * * * *`), `CLEANUP_HOURLY_SCHEDULE` (`0 * * * *`), `CLEANUP_DAILY_SCHEDULE` (`0 0 * * *`), `ENABLE_DIRECT_DOWNLOADS`, `SKIP_TOKEN_VALIDATION`, `MANAGEMENT_API_KEY`, `DEFAULT_ACTIONS_RESULTS_URL` (default `https://results-receiver.actions.githubusercontent.com`), `PROXY_MAX_REQUEST_BODY_BYTES` (default 16 MiB = `16777216`).

The three `CLEANUP_*_SCHEDULE` knobs are cron expressions and default
to upstream's `nitro.config.ts` cron lines verbatim — `*/5 * * * *`
for `cleanup:uploads`, `0 * * * *` for `cleanup:parts` +
`cleanup:merges`, and `0 0 * * *` for `cleanup:cache-entries` +
`cleanup:storage-locations`. 5-field upstream syntax is normalised to
6-field (sec=0) before parsing; operators can also write 6-field
directly (e.g. `*/30 * * * * *` for every 30 seconds). Cadences are
wall-clock aligned, just like upstream cron. Set
`DISABLE_CLEANUP_JOBS=true` to disable cleanup entirely.

`CACHE_CLEANUP_OLDER_THAN_DAYS` expires entries by their **last
download**, exactly like upstream — so an entry that is saved but never
restored never expires. `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS` is an
opt-in (not in upstream) that also reaps never-downloaded entries once
they were last committed more than that many days ago (re-saving the
same key restarts the clock), in the same daily
`cleanup:cache-entries` pass. Unset keeps upstream behaviour; `0` is
rejected. `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS=7` with
`CACHE_CLEANUP_OLDER_THAN_DAYS=30` mirrors the Forgejo runner's own
cache GC.

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

This is a **deliberate divergence** from upstream's
`routes/[...path].ts`, which forwards bodies without a cap — local
DoS mitigation, not a feature parity item. Set
`PROXY_MAX_REQUEST_BODY_BYTES=0` to disable the cap entirely (full
upstream parity); the parser normalises `0` to `usize::MAX` so any
incoming body forwards through to the receiver.

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

- **Overlapping `FinalizeCacheEntryUpload` calls commit once.** The
  commit transaction claims the `uploads` row first; of two finalizes
  racing for the same upload (a client retry while the first is still
  running) exactly one commits and the other gets `not_found`. Upstream
  lets both through, and the second then deletes the folder the entry
  points at as "superseded", losing the cache. Pinned by
  `concurrent_finalize_commits_once_and_keeps_blobs` in
  `src/cache_tests.rs`.

## Forgejo runner cache (v1 `_apis/artifactcache`)

Forgejo runners send cache traffic to an external server only through
their built-in cache proxy, which speaks the GitHub cache API **v1**
(`/_apis/artifactcache/*`); the v2 twirp path points at Forgejo itself.
Upstream dropped v1 in v9.0.0. Oxide serves it as an opt-in second
dialect so one oxide deployment can back a fleet of Forgejo runners.
The v2 surface is unaffected.

Enable it by setting the secret shared with the runners:

| Var | Notes |
|---|---|
| `FORGEJO_CACHE_SECRET` | Must equal the runner's `cache.secret`. Unset → the v1 routes are not mounted. |

Runner side (`config.yml`):

```yaml
cache:
  enabled: true
  external_server: "http://gha-cache-oxide.ci-cache.svc:3000/"
  secret: "<same value as FORGEJO_CACHE_SECRET>"
```

The reference implementation, and the spec, is `forgejo-runner` v13.2.0
`act/artifactcache` (what `forgejo-runner cache-server` serves). Every
request is authenticated by the proxy's `Forgejo-Cache-MAC` header
(HMAC-SHA256 over repo, run number, timestamp and write-isolation key)
and rejected with 403 if it does not verify or the timestamp is in the
future. Entries are scoped per repository and per write-isolation key,
falling back from the run's key to the shared (empty-key) scope on read,
exactly as act does. Chunks are streamed to object storage as they
arrive (parallel, any order); commit validates that they tile the
declared size and rewrites them server-side into oxide's normal parts
layout. Deliberate deviations from act are listed at the top of
[`src/routes/forgejo/mod.rs`](./src/routes/forgejo/mod.rs).

Write-isolated PR caches and superseded keys are often saved and never
restored; set `CACHE_CLEANUP_UNUSED_OLDER_THAN_DAYS` (see
[Configuration](#configuration)) if they should not accumulate.

Every v1 commit starts a **background merge** of the entry's parts into
its single `merged` blob, so the first restore is one sequential read
instead of a merge performed inline at the client's pace (measured ~5×
slower on Garage). A download that arrives before the merge finishes
waits for it (then streams `merged`) — up to 60 s, after which it gets
`503` + `Retry-After` and `@actions/cache` retries; a failed merge
falls back to the usual lazy merge on first download. Graceful shutdown waits for
in-flight merges — give the pod a termination grace period long enough
for your largest entry (e.g. 60 s), or a killed merge's claim blocks
that entry's downloads until the startup sweep clears it (1 h). The
v2 surface keeps upstream's merge-on-first-download.

Prometheus counters for the dialect are served at `GET /metrics`
(unauthenticated; mounted only with the dialect):

| Metric | Labels |
|---|---|
| `gha_cache_oxide_forgejo_cache_lookups_total` | `result` (`hit`/`miss`), `repo`, `key_prefix` |
| `gha_cache_oxide_forgejo_upload_bytes_total` | `repo`, `key_prefix` |
| `gha_cache_oxide_forgejo_download_bytes_total` | `repo`, `key_prefix` |
| `gha_cache_oxide_forgejo_commits_total` | `repo`, `key_prefix` |
| `gha_cache_oxide_forgejo_{upload_errors,commit_errors,auth_failures}_total` | — |
| `gha_cache_oxide_merges_total` | `result` (`ok`/`error`) |
| `gha_cache_oxide_merge_duration_seconds` (histogram) | — |

`repo` is the MAC-validated `owner/name` from the runner's cache proxy.
`key_prefix` classifies the cache key by the tool that wrote it — the
first `-`-segment, plus the second if it is purely alphabetic, so
`v0-rust-…` → `v0-rust`, `node-cache-…` → `node-cache`, BuildKit's
`buildkit-blob-…` / `index-buildkit-…` → `buildkit-blob` /
`index-buildkit`. Lookups are attributed to the primary requested key;
bytes and commits to the entry's key. Segments that look like data
collapse (`_num` for digits, `_hash` for 7+ hex / letters-and-digits),
so `${{ github.sha }}-build` is `_hash-build`, not one label per run.
At most 32 prefixes per repo (then `key_prefix="_other"`) and 256
label sets overall (then `repo="_other", key_prefix="_other"`) are
tracked; oxide logs a warning the first time either cap is hit.

**Breaking for existing queries:** these four families used to be
unlabelled (or `result`-only) and always present at 0. They now carry
`repo` / `key_prefix` and a series appears on first traffic, so
aggregate them (`sum(rate(…_upload_bytes_total[5m]))`, hit rate
`sum by (result)(…_cache_lookups_total)`) and prefer `or vector(0)` over
`absent()` for idle servers.

buildx / BuildKit `type=gha` cache works over this dialect: without
`ACTIONS_CACHE_SERVICE_V2`, buildx passes `ACTIONS_CACHE_URL` (the
runner's cache proxy) and BuildKit speaks v1. The BuildKit container
must be able to reach the proxy's advertised address (the runner's
`cache.proxy_host` / outbound IP). `tests/forgejo_e2e/buildkit.sh`
proves export → fresh-daemon import through the real runner proxy.

Tests: `tests/forgejo.rs` ports act's `handler_test.go`; the manual
end-to-end harness in [`tests/forgejo_e2e/`](./tests/forgejo_e2e) drives
the real `@actions/cache` client (v4 and v6) through the real runner
cache proxy against oxide on Garage.

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
| `GET`  | `/management/_docs/spec.json`    | OpenAPI 3.1 spec describing the surface above. Auth-gated like every other route. |

A committed snapshot of the spec lives at `docs/openapi.json` so
operators can pre-fetch it without authenticating; a no-drift
integration test pins it against the live endpoint, regenerated via
`OPENAPI_REGENERATE=1 cargo test --test management spec_snapshot_matches_committed_file`.

### oRPC `_rpc` surface (#77 part 2)

For SDK compatibility, an oRPC-shaped wire surface is mounted at
`/management-api/_rpc/...`. Seven procedures are exposed at upstream's
URLs (`cacheEntries/{findMany,get,match,delete,deleteMany}`,
`storageLocations/{get,delete}`), authed by `X-Api-Key:
<MANAGEMENT_API_KEY>` (matching upstream's `lib/api/base.ts`
middleware). Each procedure accepts a `POST` body of the form
`{"json": <input>, "meta"?: ...}` and returns
`{"json": <output>}` on success or `{"json": <error>}` with a
mapped HTTP status on failure (the error body matches
`@orpc/client`'s `ORPCErrorJSON` shape — `{defined, code, status, message}`).

The upstream TypeScript SDK at
[`sdk/index.ts`](https://github.com/falcondev-oss/github-actions-cache-server/blob/main/sdk/index.ts)
should work against this server unchanged. A few caveats:

- The wire-format fixtures are pinned by `tests/management/rpc.rs`
  but not by an end-to-end SDK round-trip in CI — a future orpc
  release that changes the envelope shape (`{json, meta}`,
  `ORPCErrorJSON`) could break compatibility silently.
- We don't implement orpc's JS-special-type round-tripping (`Date`,
  `BigInt`, `Map`, `Set`, `undefined`, `NaN`, `±Infinity`, `RegExp`,
  `URL`, `Blob`). The management API only round-trips plain
  JSON-safe types so this hasn't surfaced; `meta` arrays in incoming
  requests are tolerated and ignored.
- The `CORSPlugin` and `onError` interceptors in upstream's
  `_rpc.ts` are not ported (CORS is a deployment concern; logging is
  already handled by `tracing`).

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
8. OpenAPI spec is served at `/management/_docs/spec.json` (#77 part 1) AND an oRPC `_rpc` wire-format surface is mounted at `/management-api/_rpc/...` so the upstream TypeScript SDK works unchanged (#77 part 2). See "Management API" §§"oRPC `_rpc` surface" for the wire-format contract and parity caveats.

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
