-- Initial schema for gha-cache-oxide (Postgres).
--
-- Equivalent to `migrations/sqlite/0001_initial_schema.sql` at the column
-- and index level; only dialect-specific knobs differ:
--   - SQLite `INTEGER` → Postgres `BIGINT` on count/part-count fields. Rust
--     represents every count as `i64` in `entities.rs`, so narrowing these
--     to `INTEGER` on Postgres (as upstream Kysely's `'integer'` mapping
--     does) would silently truncate values > 2^31 back through sqlx. The
--     widen is intentional; `pg_dump` taken by the upstream server won't
--     restore into this schema verbatim, but live-data reads/writes
--     round-trip cleanly in both directions.
--   - SQLite `BIGINT` timestamp columns → Postgres `BIGINT` (same Rust type).
--   - Column-name casing is preserved verbatim (camelCase). Postgres folds
--     unquoted identifiers to lowercase, so every camelCase column name is
--     double-quoted on write; queries in `src/db/queries.rs` match. This is
--     necessary to stay observably compatible with an upstream-populated
--     Postgres database — upstream's Kysely writes `"folderName"` etc. the
--     same way.
--   - Foreign-key cascade syntax is identical across both dialects.

CREATE TABLE IF NOT EXISTS storage_locations (
    id                 TEXT    PRIMARY KEY,
    "folderName"       TEXT    NOT NULL,
    "partCount"        BIGINT  NOT NULL,
    "mergeStartedAt"   BIGINT,
    "mergedAt"         BIGINT,
    "partsDeletedAt"   BIGINT,
    "lastDownloadedAt" BIGINT
);

CREATE TABLE IF NOT EXISTS cache_entries (
    id           TEXT    PRIMARY KEY,
    key          TEXT    NOT NULL,
    version      TEXT    NOT NULL,
    "updatedAt"  BIGINT  NOT NULL,
    "locationId" TEXT    NOT NULL REFERENCES storage_locations(id) ON DELETE CASCADE,
    scope        TEXT    NOT NULL,
    "repoId"     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS uploads (
    id                        BIGINT  PRIMARY KEY,
    key                       TEXT    NOT NULL,
    version                   TEXT    NOT NULL,
    "createdAt"               BIGINT  NOT NULL,
    "lastPartUploadedAt"      BIGINT,
    "folderName"              TEXT    NOT NULL,
    "finishedPartUploadCount" BIGINT  NOT NULL DEFAULT 0,
    "startedPartUploadCount"  BIGINT  NOT NULL DEFAULT 0,
    scope                     TEXT    NOT NULL,
    "repoId"                  TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_key_version ON cache_entries(key, version);
CREATE INDEX IF NOT EXISTS idx_cache_entries_scope        ON cache_entries(scope);
CREATE INDEX IF NOT EXISTS idx_cache_entries_repoId       ON cache_entries("repoId");
CREATE INDEX IF NOT EXISTS idx_uploads_key_version        ON uploads(key, version);
CREATE INDEX IF NOT EXISTS idx_uploads_scope              ON uploads(scope);
CREATE INDEX IF NOT EXISTS idx_uploads_repoId             ON uploads("repoId");
