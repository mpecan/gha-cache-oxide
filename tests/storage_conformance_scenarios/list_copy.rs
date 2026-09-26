//! `list_folder` / `copy` scenarios (added with the Forgejo v1 dialect,
//! whose commit path lists chunks and copies them into `parts/`).

use crate::{Harness, StorageError, bytes_stream, collect};

pub async fn list_folder_returns_sorted_relative_names_with_sizes(h: &Harness) {
    // Upload out of order; the listing must come back name-sorted
    // (the Forgejo v1 commit path relies on that to order chunks).
    for (name, len) in [
        ("chunks/0000000000000010", 3_usize),
        ("chunks/0000000000000000", 16),
    ] {
        h.adapter
            .upload_stream(name, bytes_stream(vec![7; len]))
            .await
            .unwrap();
    }
    h.adapter
        .upload_stream("chunks-sibling/x", bytes_stream(vec![0]))
        .await
        .unwrap();

    let listed = h.adapter.list_folder("chunks").await.unwrap();
    let got: Vec<(&str, u64)> = listed.iter().map(|o| (o.name.as_str(), o.size)).collect();
    assert_eq!(
        got,
        vec![("0000000000000000", 16), ("0000000000000010", 3)],
        "segment-aware, sorted, relative names with sizes"
    );
}

pub async fn list_missing_folder_is_empty(h: &Harness) {
    assert!(
        h.adapter
            .list_folder("no-such-dir")
            .await
            .unwrap()
            .is_empty()
    );
}

pub async fn copy_duplicates_object(h: &Harness) {
    h.adapter
        .upload_stream("src/a", bytes_stream(b"payload".to_vec()))
        .await
        .unwrap();
    h.adapter.copy("src/a", "dst/b").await.unwrap();

    let copied = collect(h.adapter.download_stream("dst/b").await.unwrap()).await;
    assert_eq!(copied, b"payload");
    let original = collect(h.adapter.download_stream("src/a").await.unwrap()).await;
    assert_eq!(original, b"payload", "copy must leave the source in place");
}

pub async fn copy_missing_source_is_object_not_found(h: &Harness) {
    match h.adapter.copy("nope/a", "dst/b").await {
        Err(StorageError::ObjectNotFound(_)) => {}
        other => panic!("expected ObjectNotFound, got {other:?}"),
    }
}

pub async fn copy_rejects_traversal(h: &Harness) {
    for (from, to) in [("../a", "b"), ("a", "../b")] {
        match h.adapter.copy(from, to).await {
            Err(StorageError::InvalidObjectName { .. }) => {}
            other => {
                panic!("copy({from:?}, {to:?}): expected InvalidObjectName, got {other:?}")
            }
        }
    }
}
