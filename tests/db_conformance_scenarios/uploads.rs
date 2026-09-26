//! Upload-row helper scenarios: the no-op contract on unknown ids and
//! the claim semantics of `delete_upload` / `touch_upload` the commit
//! paths rely on. A sibling submodule so `mod.rs` stays under the
//! 700-line hard limit.

use gha_cache_oxide::db::Db;
use gha_cache_oxide::db::entities::{CacheEntryCoord, NewUpload};
use gha_cache_oxide::db::id::new_upload_id;

/// Upstream contract: the update/delete helpers silently no-op on
/// missing rows rather than raising. A driver that flipped to
/// erroring would break `touch_location_downloaded`'s fire-and-forget
/// call site in the download handler.
pub async fn update_helpers_are_noops_on_unknown_ids(db: &dyn Db) {
    db.increment_upload_started(1).await.unwrap();
    db.increment_upload_finished(1, 0).await.unwrap();
    assert!(!db.delete_upload(1).await.unwrap(), "no row to delete");
    assert!(!db.touch_upload(1, 0).await.unwrap(), "no row to touch");
    db.touch_location_downloaded("does-not-exist", 0)
        .await
        .unwrap();
}

/// `delete_upload` reports whether it removed the row — both on the
/// pool and inside a transaction — so concurrent commits of one upload
/// can tell which of them claimed it. `touch_upload` moves only
/// `lastPartUploadedAt` and reports whether the row still exists.
pub async fn upload_claim_and_touch_report_row_presence(db: &dyn Db) {
    let coord = CacheEntryCoord {
        key: "k",
        version: "v",
        scope: "scn-upload-claim",
        repo_id: "r",
    };
    let mut ids = Vec::new();
    for _ in 0..2 {
        let id = new_upload_id();
        db.create_upload(NewUpload {
            id,
            coord,
            folder_name: "f",
            created_at_ms: 1,
        })
        .await
        .unwrap();
        ids.push(id);
    }

    assert!(db.touch_upload(ids[0], 42).await.unwrap());
    // Same value twice in one millisecond must still report the row as
    // present (MySQL counts *changed* rows unless CLIENT_FOUND_ROWS is
    // negotiated, which sqlx does) — `late_chunk` relies on it.
    assert!(db.touch_upload(ids[0], 42).await.unwrap());
    let touched = db.find_upload_by_id(ids[0]).await.unwrap().unwrap();
    assert_eq!(touched.last_part_uploaded_at, Some(42));
    assert_eq!(touched.started_part_upload_count, 0);
    assert_eq!(touched.finished_part_upload_count, 0);

    assert!(db.delete_upload(ids[0]).await.unwrap());
    assert!(!db.delete_upload(ids[0]).await.unwrap());
    assert!(!db.touch_upload(ids[0], 43).await.unwrap());

    let mut tx = db.begin().await.unwrap();
    assert!(tx.delete_upload(ids[1]).await.unwrap());
    assert!(!tx.delete_upload(ids[1]).await.unwrap());
    tx.commit().await.unwrap();
    assert!(db.find_upload_by_id(ids[1]).await.unwrap().is_none());
}
