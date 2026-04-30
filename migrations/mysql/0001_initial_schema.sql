-- Initial schema for gha-cache-oxide (MySQL 8+).
--
-- Equivalent to migrations/postgres/0001_initial_schema.sql at the
-- column / index level. Dialect-specific notes:
--   - MySQL is case-folded for identifiers based on
--     `lower_case_table_names`. Backtick-quote every camelCase column
--     to preserve case verbatim, matching upstream Kysely's
--     MysqlDialect output and our Postgres double-quoting policy. A
--     bucket populated by the upstream server remains observably
--     compatible with this adapter.
--   - All BIGINT columns hold ms-since-epoch timestamps (i64 in Rust).
--   - `partCount`, `*PartUploadCount` are widened from upstream's
--     `'integer'` Kysely mapping to BIGINT for the same reason the
--     postgres migration documents at lines 5-11: Rust represents
--     every count as i64 in `entities.rs`, so narrowing on write
--     would silently truncate values > 2^31 round-tripping through
--     sqlx. Live data round-trips cleanly between this and an
--     upstream-populated database.
--   - VARCHAR lengths for `key` (512), `version`/`scope`/`repoId`
--     (255), and `id` (36) match upstream `lib/migrations.ts`
--     verbatim. `folderName` is narrowed from upstream's `text` to
--     VARCHAR(64): folder names are numeric upload ids well under 19
--     digits, so the bound is generous; the cap keeps the row size
--     deterministic for index planning.
--   - `key` is a MySQL reserved word — backtick-quoted in every
--     reference (column definitions and indexes here, and again in
--     every SQL literal in src/db/mysql.rs).
--   - InnoDB + utf8mb4 are the MySQL 8 defaults; spelled out so
--     behaviour is deterministic across server config.
--   - The `(key, version)` index uses a 255-byte prefix on `key` to
--     stay under InnoDB's 3072-byte index-key limit on utf8mb4
--     (4 bytes/char). Equality lookups still hit the index; full-key
--     equality is verified against the row.

CREATE TABLE IF NOT EXISTS storage_locations (
    id                 VARCHAR(36)  PRIMARY KEY,
    `folderName`       VARCHAR(64)  NOT NULL,
    `partCount`        BIGINT       NOT NULL,
    `mergeStartedAt`   BIGINT       NULL,
    `mergedAt`         BIGINT       NULL,
    `partsDeletedAt`   BIGINT       NULL,
    `lastDownloadedAt` BIGINT       NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS cache_entries (
    id           VARCHAR(36)  PRIMARY KEY,
    `key`        VARCHAR(512) NOT NULL,
    version      VARCHAR(255) NOT NULL,
    `updatedAt`  BIGINT       NOT NULL,
    `locationId` VARCHAR(36)  NOT NULL,
    scope        VARCHAR(255) NOT NULL,
    `repoId`     VARCHAR(255) NOT NULL,
    CONSTRAINT fk_cache_entries_location
        FOREIGN KEY (`locationId`) REFERENCES storage_locations(id)
        ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS uploads (
    id                        BIGINT       PRIMARY KEY,
    `key`                     VARCHAR(512) NOT NULL,
    version                   VARCHAR(255) NOT NULL,
    `createdAt`               BIGINT       NOT NULL,
    `lastPartUploadedAt`      BIGINT       NULL,
    `folderName`              VARCHAR(64)  NOT NULL,
    `finishedPartUploadCount` BIGINT       NOT NULL DEFAULT 0,
    `startedPartUploadCount`  BIGINT       NOT NULL DEFAULT 0,
    scope                     VARCHAR(255) NOT NULL,
    `repoId`                  VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE INDEX idx_cache_entries_key_version ON cache_entries(`key`(255), version);
CREATE INDEX idx_cache_entries_scope        ON cache_entries(scope);
CREATE INDEX idx_cache_entries_repoId       ON cache_entries(`repoId`);
CREATE INDEX idx_uploads_key_version        ON uploads(`key`(255), version);
CREATE INDEX idx_uploads_scope              ON uploads(scope);
CREATE INDEX idx_uploads_repoId             ON uploads(`repoId`);
