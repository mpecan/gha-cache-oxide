//! [`MysqlTx`]: the `DbTx` half of the mysql driver, split from `mysql.rs`
//! to keep each file under the length limit.

use async_trait::async_trait;
use sqlx::{MySql, Transaction};

use crate::db::DbTx;
use crate::db::entities::{CacheEntryCoord, PreviousLocation};
use crate::db::id::new_uuid;

/// `MySQL` transaction handle. Wraps `sqlx::Transaction<'_, MySql>`.
pub struct MysqlTx<'a> {
    pub(super) tx: Transaction<'a, MySql>,
}

#[async_trait]
impl DbTx for MysqlTx<'_> {
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
            "INSERT INTO storage_locations (id, `folderName`, `partCount`, `mergeStartedAt`, `mergedAt`, `partsDeletedAt`, `lastDownloadedAt`) \
             VALUES (?, ?, ?, NULL, NULL, NULL, NULL)",
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
            "SELECT ce.id, ce.`locationId`, sl.`folderName` \
             FROM cache_entries ce \
             INNER JOIN storage_locations sl ON sl.id = ce.`locationId` \
             WHERE ce.`key` = ? AND ce.version = ? AND ce.scope = ? AND ce.`repoId` = ?",
        )
        .bind(coord.key)
        .bind(coord.version)
        .bind(coord.scope)
        .bind(coord.repo_id)
        .fetch_optional(&mut *self.tx)
        .await?;

        if let Some((entry_id, old_location_id, old_folder_name)) = existing {
            sqlx::query("UPDATE cache_entries SET `updatedAt` = ?, `locationId` = ? WHERE id = ?")
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
                "INSERT INTO cache_entries (id, `key`, version, scope, `repoId`, `updatedAt`, `locationId`) \
                 VALUES (?, ?, ?, ?, ?, ?, ?)",
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
             WHERE id = ? AND `lastDownloadedAt` IS NULL \
             AND (`mergeStartedAt` IS NULL OR `mergedAt` IS NOT NULL)",
        )
        .bind(id)
        .execute(&mut *self.tx)
        .await?;
        Ok(done.rows_affected() == 1)
    }

    async fn delete_storage_location(&mut self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM storage_locations WHERE id = ?")
            .bind(id)
            .execute(&mut *self.tx)
            .await?;
        Ok(())
    }

    async fn delete_upload(&mut self, id: i64) -> Result<bool, sqlx::Error> {
        let done = sqlx::query("DELETE FROM uploads WHERE id = ?")
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
             WHERE id = ? \
               AND `createdAt` < ? \
               AND (`lastPartUploadedAt` IS NULL OR `lastPartUploadedAt` < ?)",
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
        sqlx::query("UPDATE storage_locations SET `partsDeletedAt` = ? WHERE id = ?")
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
            "INSERT INTO cache_entries (id, `key`, version, scope, `repoId`, `updatedAt`, `locationId`) \
             VALUES (?, ?, ?, ?, ?, ?, ?)",
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
