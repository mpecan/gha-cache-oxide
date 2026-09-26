//! `signed_url` scenarios.

use crate::{Harness, StorageError, bytes_stream};

/// `signed_url` must be `None` for backends that cannot sign (the
/// server proxies the download) and `Some(_)` for backends that can
/// (the client goes direct). `Harness::signs_urls` says which.
///
/// For signing backends, the returned URL must be directly fetchable —
/// issue #12's acceptance criterion: "`signed_url` returns a URL that
/// `reqwest::get` can fetch". We upload `"content"` and assert the
/// downloaded body matches byte-for-byte.
pub async fn signed_url_matches_capability(h: &Harness) {
    let payload = b"content";
    h.adapter
        .upload_stream("obj", bytes_stream(payload.to_vec()))
        .await
        .unwrap();
    let got = h.adapter.signed_url("obj").await.unwrap();
    if h.signs_urls {
        let url = got.expect("signs_urls=true backend must return Some(url)");
        let resp = reqwest::get(url.clone())
            .await
            .unwrap_or_else(|e| panic!("reqwest::get({url}) failed: {e}"));
        assert!(resp.status().is_success(), "GET {url} -> {}", resp.status());
        let body = resp.bytes().await.unwrap();
        assert_eq!(&body[..], payload, "signed URL returned wrong body");
    } else {
        assert!(
            got.is_none(),
            "signs_urls=false backend must return None, got {got:?}"
        );
    }
}

pub async fn signed_url_validates_object_name(h: &Harness) {
    match h.adapter.signed_url("../evil").await {
        Err(StorageError::InvalidObjectName { .. }) => {}
        Err(other) => panic!("expected InvalidObjectName, got {other:?}"),
        Ok(_) => panic!("expected Err, got Ok"),
    }
}
