-- Initial schema for gha-cache-oxide.
--
-- Mirrors the shape upstream reaches after all four sequential migrations
-- in lib/migrations.ts (github-actions-cache-server). We start fresh: no
-- migration history to preserve because there is no running deployment.
-- Column names are verbatim camelCase (folderName, partCount, mergedAt,
-- etc.) so tooling or operators pointed at an upstream-created database
-- observe identical column identifiers.

CREATE TABLE IF NOT EXISTS storage_locations (
    id                TEXT    PRIMARY KEY,
    folderName        TEXT    NOT NULL,
    partCount         INTEGER NOT NULL,
    mergeStartedAt    BIGINT,
    mergedAt          BIGINT,
    partsDeletedAt    BIGINT,
    lastDownloadedAt  BIGINT
);

CREATE TABLE IF NOT EXISTS cache_entries (
    id          TEXT   PRIMARY KEY,
    key         TEXT   NOT NULL,
    version     TEXT   NOT NULL,
    updatedAt   BIGINT NOT NULL,
    locationId  TEXT   NOT NULL REFERENCES storage_locations(id) ON DELETE CASCADE,
    scope       TEXT   NOT NULL,
    repoId      TEXT   NOT NULL
);

CREATE TABLE IF NOT EXISTS uploads (
    id                         BIGINT  PRIMARY KEY,
    key                        TEXT    NOT NULL,
    version                    TEXT    NOT NULL,
    createdAt                  BIGINT  NOT NULL,
    lastPartUploadedAt         BIGINT,
    folderName                 TEXT    NOT NULL,
    finishedPartUploadCount    INTEGER NOT NULL DEFAULT 0,
    startedPartUploadCount     INTEGER NOT NULL DEFAULT 0,
    scope                      TEXT    NOT NULL,
    repoId                     TEXT    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_cache_entries_key_version ON cache_entries(key, version);
CREATE INDEX IF NOT EXISTS idx_cache_entries_scope        ON cache_entries(scope);
CREATE INDEX IF NOT EXISTS idx_cache_entries_repoId       ON cache_entries(repoId);
CREATE INDEX IF NOT EXISTS idx_uploads_key_version        ON uploads(key, version);
CREATE INDEX IF NOT EXISTS idx_uploads_scope              ON uploads(scope);
CREATE INDEX IF NOT EXISTS idx_uploads_repoId             ON uploads(repoId);
