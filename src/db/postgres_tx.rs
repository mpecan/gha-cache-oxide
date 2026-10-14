//! [`PostgresTx`]: the `DbTx` half of the postgres driver, split from `postgres.rs`
//! to keep each file under the length limit.

use async_trait::async_trait;
use sqlx::{Postgres, Transaction};

use crate::db::DbTx;
use crate::db::entities::{CacheEntryCoord, PreviousLocation};
use crate::db::id::new_uuid;

/// Postgres transaction handle. Wraps `sqlx::Transaction<'_, Postgres>`.
pub struct PostgresTx<'a> {
    pub(super) tx: Transaction<'a, Postgres>,
}

#[async_trait]
impl DbTx for PostgresTx<'_> {
    async fn commit(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.commit().await
    }

    async fn rollback(self: Box<Self>) -> Result<(), sqlx::Error> {
        self.tx.rollback().await
    }

    async fn insert_storage_location(
        &mut self,
        id: &str,
        folder_name: &str,
        part_count: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO storage_locations (id, \"folderName\", \"partCount\", \"mergeStartedAt\", \"mergedAt\", \"partsDeletedAt\", \"lastDownloadedAt\") \
             VALUES ($1, $2, $3, NULL, NULL, NULL, NULL)",
        )
        .bind(id)
        .bind(folder_name)
        .bind(part_count)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }

    async fn upsert_cache_entry(
        &mut self,
        coord: CacheEntryCoord<'_>,
        new_location_id: &str,
        now_ms: i64,
    ) -> Result<Option<PreviousLocation>, sqlx::Error> {
        let existing: Option<(String, String, String)> = sqlx::query_as(
            "SELECT ce.id, ce.\"locationId\", sl.\"folderName\" \
             FROM cache_entries ce \
             INNER JOIN storage_locations sl ON sl.id = ce.\"locationId\" \
             WHERE ce.key = $1 AND ce.version = $2 AND ce.scope = $3 AND ce.\"repoId\" = $4",
        )
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .fetch_optional(&mut *self.tx)
        .await?;

        if let Some((entry_id, old_location_id, old_folder_name)) = existing {
            sqlx::query(
                "UPDATE cache_entries SET \"updatedAt\" = $1, \"locationId\" = $2 WHERE id = $3",
            )
            .bind(now_ms)
            .bind(new_location_id)
            .bind(&entry_id)
            .execute(&mut *self.tx)
            .await?;
            Ok(Some(PreviousLocation {
                id: old_location_id,
                folder_name: old_folder_name,
            }))
        } else {
            let new_entry_id = new_uuid();
            sqlx::query(
                "INSERT INTO cache_entries (id, key, version, scope, \"repoId\", \"updatedAt\", \"locationId\") \
                 VALUES ($1, $2, $3, $4, $5, $6, $7)",
            )
            .bind(&new_entry_id)
            .bind(coord.key)
            .bind(coord.version)
            .bind(coord.scope)
            .bind(coord.repo_id)
            .bind(now_ms)
            .bind(new_location_id)
            .execute(&mut *self.tx)
            .await?;
            Ok(None)
        }
    }

    async fn delete_location_if_unused(&mut self, id: &str) -> Result<bool, sqlx::Error> {
        let done = sqlx::query(
            "DELETE FROM storage_locations \
             WHERE id = $1 AND \"lastDownloadedAt\" IS NULL \
             AND (\"mergeStartedAt\" IS NULL OR \"mergedAt\" IS NOT NULL)",
        )
        .bind(id)
        .execute(&mut *self.tx)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_storage_location(&mut self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM storage_locations WHERE id = $1")
            .bind(id)
            .execute(&mut *self.tx)
            .await?;
        Ok(())
    }

    async fn delete_upload(&mut self, id: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("DELETE FROM uploads WHERE id = $1")
            .bind(id)
            .execute(&mut *self.tx)
            .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_upload_if_stale(
        &mut self,
        id: i64,
        cutoff_ms: i64,
    ) -> Result<bool, sqlx::Error> {
        let rows = sqlx::query(
            "DELETE FROM uploads \
             WHERE id = $1 \
               AND \"createdAt\" < $2 \
               AND (\"lastPartUploadedAt\" IS NULL OR \"lastPartUploadedAt\" < $3)",
        )
        .bind(id)
        .bind(cutoff_ms)
        .bind(cutoff_ms)
        .execute(&mut *self.tx)
        .await?
        .rows_affected();
        Ok(rows == 1)
    }

    async fn mark_parts_deleted(
        &mut self,
        location_id: &str,
        now_ms: i64,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE storage_locations SET \"partsDeletedAt\" = $1 WHERE id = $2")
            .bind(now_ms)
            .bind(location_id)
            .execute(&mut *self.tx)
            .await?;
        Ok(())
    }

    async fn seed_cache_entry(
        &mut self,
        id: &str,
        coord: CacheEntryCoord<'_>,
        updated_at_ms: i64,
        location_id: &str,
    ) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO cache_entries (id, key, version, scope, \"repoId\", \"updatedAt\", \"locationId\") \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(id)
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .bind(updated_at_ms)
        .bind(location_id)
        .execute(&mut *self.tx)
        .await?;
        Ok(())
    }
}
