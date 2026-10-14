-- Size of a storage location's cache object in bytes, measured from
-- storage by the `cleanup:repo-size` pass. NULL until measured. Not in
-- upstream's schema; upstream ignores the extra column.
ALTER TABLE storage_locations ADD COLUMN sizeBytes BIGINT;

-- `cleanup:repo-size` joins entries to locations every hour.
CREATE INDEX idx_cache_entries_locationId ON cache_entries(locationId);
