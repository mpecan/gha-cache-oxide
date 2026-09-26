//! Conformance suite for the Forgejo runner v1 cache dialect.
//!
//! Ports `TestHandler` from forgejo-runner v13.2.0
//! `act/artifactcache/handler_test.go` case by case (test names follow
//! the Go subtests), over real HTTP against a server on a random port.
//! Cases where oxide deliberately deviates (see `src/routes/forgejo/mod.rs`)
//! assert oxide's behaviour and say so. Oxide-specific cases live in
//! `tests/forgejo_oxide.rs`.

#![allow(clippy::unwrap_used, clippy::panic, clippy::expect_used)]

mod forgejo_common;

use forgejo_common::{Runner, VERSION, random_bytes, spawn, tick};
use reqwest::{Method, StatusCode};

async fn setup() -> (forgejo_common::Server, Runner) {
    let srv = spawn(Some(forgejo_common::SECRET)).await;
    let runner = Runner::new(&srv);
    (srv, runner)
}

// ---- ported from act handler_test.go ----------------------------------

#[tokio::test]
async fn get_not_exist() {
    let (_srv, r) = setup().await;
    assert_eq!(
        r.find("get_not_exist", VERSION).await.0,
        StatusCode::NO_CONTENT
    );
}

#[tokio::test]
async fn reserve_and_upload() {
    let (_srv, r) = setup().await;
    r.upload_normally("reserve_and_upload", VERSION, &random_bytes(100))
        .await;
}

#[tokio::test]
async fn clean() {
    let (_srv, r) = setup().await;
    let resp = r.request(Method::POST, "/clean").send().await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn reserve_with_bad_request() {
    let (_srv, r) = setup().await;
    let resp = r
        .request(Method::POST, "/caches")
        .header("Content-Type", "application/json")
        .body("invalid json")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn duplicate_reserve() {
    let (_srv, r) = setup().await;
    let first = r.reserve("duplicate_reserve", VERSION, 100).await;
    let second = r.reserve("duplicate_reserve", VERSION, 100).await;
    assert_ne!(first, second);
}

#[tokio::test]
async fn upload_with_bad_id() {
    let (_srv, r) = setup().await;
    let status = r.patch("invalid_id", "bytes 0-99/*", Vec::new()).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn upload_without_reserve() {
    let (_srv, r) = setup().await;
    let status = r.patch(1000, "bytes 0-99/*", Vec::new()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

/// Deviation: act answers 400 "already complete"; oxide consumes the
/// reservation on commit, so the id is simply unknown afterwards (404).
#[tokio::test]
async fn upload_with_complete() {
    let (_srv, r) = setup().await;
    let content = random_bytes(100);
    let id = r.reserve("upload_with_complete", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content.clone()).await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, None).await, StatusCode::OK);
    assert_eq!(
        r.patch(id, "bytes 0-99/*", content).await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn upload_with_invalid_range() {
    let (_srv, r) = setup().await;
    let id = r.reserve("upload_with_invalid_range", VERSION, 100).await;
    let status = r.patch(id, "bytes xx-99/*", random_bytes(100)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn commit_with_bad_id() {
    let (_srv, r) = setup().await;
    assert_eq!(r.commit("invalid_id", None).await, StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn commit_with_not_exist_id() {
    let (_srv, r) = setup().await;
    assert_eq!(r.commit(100, None).await, StatusCode::NOT_FOUND);
}

/// Deviation: second commit is 404 (reservation consumed), not 400.
#[tokio::test]
async fn duplicate_commit() {
    let (_srv, r) = setup().await;
    let id = r.reserve("duplicate_commit", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-99/*", random_bytes(100)).await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::OK);
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::NOT_FOUND);
}

/// Deviation: act checks against reserve's `cacheSize` and answers 500;
/// oxide checks the size declared on commit (what `@actions/cache` sends)
/// and answers 400, discarding the reservation.
#[tokio::test]
async fn commit_early() {
    let (srv, r) = setup().await;
    let content = random_bytes(100);
    let id = r.reserve("commit_early", VERSION, 100).await;
    assert_eq!(
        r.patch(id, "bytes 0-59/*", content[..50].to_vec()).await,
        StatusCode::OK
    );
    assert_eq!(r.commit(id, Some(100)).await, StatusCode::BAD_REQUEST);
    assert!(srv.db.find_upload_by_id(id).await.unwrap().is_none());
    assert_eq!(
        r.find("commit_early", VERSION).await.0,
        StatusCode::NO_CONTENT
    );
}

/// Deviation: artifact ids are opaque UUIDs, so a malformed id is just
/// unknown (404) rather than act's 400.
#[tokio::test]
async fn get_with_bad_id() {
    let (_srv, r) = setup().await;
    let resp = r
        .request(Method::GET, "/artifacts/invalid_id")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_with_not_exist_id() {
    let (_srv, r) = setup().await;
    let resp = r
        .request(Method::GET, "/artifacts/100")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn get_with_bad_mac() {
    let (_srv, r) = setup().await;
    r.upload_normally("get_with_bad_mac", VERSION, &random_bytes(100))
        .await;
    assert_eq!(r.find("get_with_bad_mac", VERSION).await.0, StatusCode::OK);

    let bad = Runner {
        mac_override: Some(
            "33f0e850ba0bdfd2f3e66ff79c1f8004b8226114e3b2e65c229222bb59df0f9d".into(),
        ),
        ..r.clone()
    };
    assert_eq!(
        bad.find("get_with_bad_mac", VERSION).await.0,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn get_with_multiple_keys() {
    let (_srv, r) = setup().await;
    let k = "get_with_multiple_keys";
    let keys = [format!("{k}_a_b_c"), format!("{k}_a_b"), format!("{k}_a")];
    let contents = [random_bytes(100), random_bytes(200), random_bytes(300)];
    for (key, content) in keys.iter().zip(&contents) {
        r.upload_normally(key, VERSION, content).await;
        tick().await;
    }
    // `_a_b_x` matches nothing; `_a_b` matches `_a_b` exactly.
    let req = format!("{k}_a_b_x,{k}_a_b,{k}_a");
    let (key, bytes) = r.find_and_download(&req, VERSION).await;
    assert_eq!(key, keys[1]);
    assert_eq!(bytes.as_ref(), contents[1].as_slice());
}

#[tokio::test]
async fn find_cant_match_without_write_isolation_key_match() {
    let (_srv, r) = setup().await;
    let key = "find_cant_match_without_wik";
    r.with_key("TestWriteKey")
        .upload_normally(key, VERSION, &[0; 64])
        .await;

    let other = r.with_key("AnotherTestWriteKey").find(key, VERSION).await.0;
    assert_eq!(other, StatusCode::NO_CONTENT);
    assert_eq!(r.find(key, VERSION).await.0, StatusCode::NO_CONTENT);
    let same = r.with_key("TestWriteKey").find(key, VERSION).await.0;
    assert_eq!(same, StatusCode::OK);
}

#[tokio::test]
async fn find_prefers_write_isolation_key_match() {
    let (_srv, r) = setup().await;
    let key = "find_prefers_wik_match";
    r.with_key("TestWriteKey")
        .upload_normally(key, VERSION, &[0; 64])
        .await;
    tick().await;
    // The shared-scope entry is newer, but the isolated one must win.
    r.upload_normally(key, VERSION, &[0; 128]).await;

    let (_, bytes) = r
        .with_key("TestWriteKey")
        .find_and_download(key, VERSION)
        .await;
    assert_eq!(bytes.as_ref(), [0; 64].as_slice());
}

#[tokio::test]
async fn find_falls_back_if_matching_write_isolation_key_not_available() {
    let (_srv, r) = setup().await;
    let key = "find_falls_back";
    r.upload_normally(key, VERSION, &[0; 128]).await;
    let (_, bytes) = r
        .with_key("TestWriteKey")
        .find_and_download(key, VERSION)
        .await;
    assert_eq!(bytes.as_ref(), [0; 128].as_slice());
}

#[tokio::test]
async fn case_insensitive() {
    let (_srv, r) = setup().await;
    let content = random_bytes(100);
    r.upload_normally("case_insensitive_ABC", VERSION, &content)
        .await;
    let (status, body) = r.find("case_insensitive_aBc", VERSION).await;
    assert_eq!(status, StatusCode::OK);
    let body = body.unwrap();
    assert_eq!(body["result"], "hit");
    assert_eq!(body["cacheKey"], "case_insensitive_abc");
}

async fn exact_keys_are_preferred(first_request_key: Option<&str>) {
    let (_srv, r) = setup().await;
    let k = "exact_keys_are_preferred";
    let keys = [format!("{k}_a"), format!("{k}_a_b_c"), format!("{k}_a_b")];
    let contents = [random_bytes(100), random_bytes(200), random_bytes(300)];
    for (key, content) in keys.iter().zip(&contents) {
        r.upload_normally(key, VERSION, content).await;
        tick().await;
    }
    let mut req: Vec<String> = first_request_key.map(String::from).into_iter().collect();
    req.extend([format!("{k}_a"), format!("{k}_a_b")]);
    // `_a` prefix-matches all three and `_a_b` is newer, but `_a` is exact.
    let (key, bytes) = r.find_and_download(&req.join(","), VERSION).await;
    assert_eq!(key, keys[0]);
    assert_eq!(bytes.as_ref(), contents[0].as_slice());
}

#[tokio::test]
async fn exact_keys_are_preferred_key_0() {
    exact_keys_are_preferred(None).await;
}

#[tokio::test]
async fn exact_keys_are_preferred_key_1() {
    exact_keys_are_preferred(Some(
        "------------------------------------------------------",
    ))
    .await;
}

#[tokio::test]
async fn upload_across_write_isolation_key() {
    let (_srv, r) = setup().await;
    let id = r
        .with_key("CorrectKey")
        .reserve("upload_across_wik", VERSION, 256)
        .await;
    let status = r
        .with_key("WrongKey")
        .patch(id, "bytes 0-99/*", vec![0; 256])
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn commit_across_write_isolation_key() {
    let (_srv, r) = setup().await;
    let correct = r.with_key("CorrectKey");
    let id = correct.reserve("commit_across_wik", VERSION, 256).await;
    assert_eq!(
        correct.patch(id, "bytes 0-99/*", vec![0; 256]).await,
        StatusCode::OK
    );
    assert_eq!(
        r.with_key("WrongKey").commit(id, None).await,
        StatusCode::FORBIDDEN
    );
}

#[tokio::test]
async fn get_across_write_isolation_key() {
    let (_srv, r) = setup().await;
    let key = "get_across_wik";
    let isolated = "get_across_wik_isolated";
    r.upload_normally(key, VERSION, &[0; 128]).await;
    r.with_key("CorrectKey")
        .upload_normally(isolated, VERSION, &[0; 128])
        .await;

    // Shared-scope entries are readable with any isolation key.
    let wrong = r.with_key("WhoopsWrongKey");
    let (_, bytes) = wrong.find_and_download(key, VERSION).await;
    assert_eq!(bytes.len(), 128);

    // An isolated entry's location is 403 for a different key.
    let (_, body) = r.with_key("CorrectKey").find(isolated, VERSION).await;
    let location = body.unwrap()["archiveLocation"]
        .as_str()
        .unwrap()
        .to_string();
    assert_eq!(wrong.download(&location).await.0, StatusCode::FORBIDDEN);
}

// ---- MAC validation across every route --------------------------------

#[tokio::test]
async fn every_route_rejects_bad_mac_future_timestamp_and_missing_headers() {
    let (srv, r) = setup().await;
    let bad_mac = Runner {
        mac_override: Some("00".repeat(32)),
        ..r.clone()
    };
    let future = Runner {
        timestamp: (chrono::Utc::now().timestamp() + 3600).to_string(),
        ..r.clone()
    };
    let routes = [
        (Method::GET, "/cache?keys=k&version=v"),
        (Method::POST, "/caches"),
        (Method::PATCH, "/caches/1"),
        (Method::POST, "/caches/1"),
        (Method::GET, "/artifacts/1"),
        (Method::POST, "/clean"),
    ];
    for (method, path) in &routes {
        for (label, runner) in [("bad mac", &bad_mac), ("future ts", &future)] {
            let resp = runner.request(method.clone(), path).send().await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::FORBIDDEN,
                "{label}: {method} {path}"
            );
        }
        let url = format!("{}{path}", r.base);
        let resp = r.http.request(method.clone(), url).send().await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::FORBIDDEN,
            "no headers: {method} {path}"
        );
    }
    assert_eq!(srv.metrics.forgejo.auth_failures.get(), 18);
}
