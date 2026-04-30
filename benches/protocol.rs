//! Criterion benchmark harness — issue #21.
//!
//! Four groups, all running on the default fs+sqlite cell so a bare
//! `cargo bench` is a single-command run with no external setup:
//!
//! - `upload_finalize/<MiB>`             — reserve → upload → finalize at 1 / 16 / 64 MiB.
//! - `download_cold/16MiB`               — first download (lazy-merge path).
//! - `download_warm/16MiB`               — subsequent download (merged-blob fast path).
//! - `match_cache_entry/{miss,hit}/<count>` — `Db::match_cache_entry`
//!   on 10k / 100k seeded rows. `miss` covers the full SQL walk
//!   (catches scan-path index regressions); `hit` covers the early-
//!   exit on an exact-key match (catches hit-branch regressions).
//!
//! The router is driven via `tower::ServiceExt::oneshot` (no TCP), which
//! is faster + more reproducible than a real listener while still
//! exercising every layer the production path uses (axum routing, auth
//! middleware, twirp handlers, sqlx queries, `object_store` writes).
//!
//! Matrix cells beyond fs+sqlite (Postgres, S3) are NOT wired in this
//! harness — see README §"Benchmarks" for the planned-but-deferred
//! env-var hooks.

#![allow(
    clippy::unwrap_used,
    clippy::panic,
    clippy::expect_used,
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::too_many_lines,
    clippy::cast_possible_truncation,
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss
)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use gha_cache_oxide::auth::{AuthError, JwkEntry, JwksCache, JwksFetcher};
use gha_cache_oxide::config::{AppConfig, DbConfig, StorageConfig};
use gha_cache_oxide::db::entities::{CacheEntryCoord, MatchRequest};
use gha_cache_oxide::db::{Db, SqliteDb};
use gha_cache_oxide::state::AppState;
use gha_cache_oxide::storage::{FilesystemAdapter, StorageAdapter};
use jsonwebtoken::{Algorithm, EncodingKey, Header, encode};
use rsa::RsaPrivateKey;
use rsa::pkcs1::EncodeRsaPrivateKey;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::runtime::Runtime;
use tower::ServiceExt;

const MIB: u64 = 1024 * 1024;
const TWIRP_PREFIX: &str = "/twirp/github.actions.results.api.v1.CacheService";
const ISSUER: &str = "https://token.actions.githubusercontent.com";
const REPO_ID: &str = "42";
const SCOPE: &str = "refs/heads/main";
const VERSION: &str = "bench-v1";
const API_BASE_URL: &str = "http://bench-host";

// --- Token + harness ----------------------------------------------------

struct StubFetcher;

#[async_trait::async_trait]
impl JwksFetcher for StubFetcher {
    async fn fetch(&self) -> Result<Vec<JwkEntry>, AuthError> {
        Ok(vec![JwkEntry {
            kid: "bench".into(),
            n: String::new(),
            e: String::new(),
        }])
    }
}

/// One-shot RSA-key + JWT fixture so token-minting cost (~200 ms) lands
/// once across all bench groups.
fn write_token() -> String {
    use std::sync::OnceLock;
    static TOKEN: OnceLock<String> = OnceLock::new();
    TOKEN
        .get_or_init(|| {
            let mut rng = rsa::rand_core::OsRng;
            let private = RsaPrivateKey::new(&mut rng, 2048).unwrap();
            let pem = private
                .to_pkcs1_pem(rsa::pkcs1::LineEnding::LF)
                .unwrap()
                .to_string();
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let claims = json!({
                "iss": ISSUER,
                "iat": now,
                "exp": now + 3600,
                "ac": serde_json::to_string(&json!([
                    {"Scope": SCOPE, "Permission": 3}
                ]))
                .unwrap(),
                "repository_id": REPO_ID,
            });
            let mut header = Header::new(Algorithm::RS256);
            header.kid = Some("bench".into());
            let key = EncodingKey::from_rsa_pem(pem.as_bytes()).unwrap();
            encode(&header, &claims, &key).unwrap()
        })
        .clone()
}

struct Harness {
    router: Router,
    token: String,
    counter: AtomicU64,
    _tmp: TempDir,
}

impl Harness {
    async fn new() -> Self {
        let tmp = TempDir::new().unwrap();
        let db = SqliteDb::connect_in_memory().await.unwrap();
        db.migrate().await.unwrap();
        let db: Arc<dyn Db> = Arc::new(db);
        let storage: Arc<dyn StorageAdapter> =
            Arc::new(FilesystemAdapter::new(tmp.path()).unwrap());
        let jwks = Arc::new(JwksCache::new(Arc::new(StubFetcher)));
        let config = AppConfig {
            api_base_url: API_BASE_URL.parse().unwrap(),
            // Bench tokens are signed but we skip verification so the
            // matcher path doesn't need a real JWKS endpoint behind it.
            skip_token_validation: true,
            ..AppConfig::test_defaults(
                StorageConfig::Filesystem {
                    path: tmp.path().to_path_buf(),
                },
                DbConfig::Sqlite {
                    path: PathBuf::from(":memory:"),
                },
            )
        };
        let state = AppState::new(db, storage, jwks, config);
        let router = gha_cache_oxide::build_app(state);
        Self {
            router,
            token: write_token(),
            counter: AtomicU64::new(0),
            _tmp: tmp,
        }
    }

    fn next_key(&self) -> String {
        format!("bench-key-{}", self.counter.fetch_add(1, Ordering::Relaxed))
    }
}

/// Strips `API_BASE_URL` from a router-minted signed URL, leaving the
/// path + query the in-process router can consume directly.
fn strip_base(full_url: &str) -> &str {
    full_url
        .strip_prefix(API_BASE_URL)
        .unwrap_or_else(|| panic!("expected URL prefixed with {API_BASE_URL}, got {full_url}"))
}

// --- HTTP helpers (oneshot-driven) -------------------------------------

async fn body_json(resp: axum::response::Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    let value: Value = serde_json::from_slice(&bytes).unwrap();
    (status, value)
}

fn post(uri: &str, token: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .header(header::AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

fn put_bytes(uri: &str, body: Vec<u8>) -> Request<Body> {
    Request::builder()
        .method("PUT")
        .uri(uri)
        .body(Body::from(body))
        .unwrap()
}

fn get(uri: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())
        .unwrap()
}

// --- Protocol roundtrip helpers ----------------------------------------

/// Reserve → single-part upload → finalize, returning the cache-entry id.
async fn upload_finalize(h: &Harness, key: &str, payload: Vec<u8>) -> String {
    let create_uri = format!("{TWIRP_PREFIX}/CreateCacheEntry");
    let req = post(
        &create_uri,
        &h.token,
        &json!({"key": key, "version": VERSION}),
    );
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "CreateCacheEntry: {body}");
    let signed_upload = body["signed_upload_url"].as_str().unwrap().to_string();
    let upload_path = strip_base(&signed_upload).to_string();

    // Single-part upload — the bench writes the whole payload as block 0.
    let put_uri = format!("{upload_path}?comp=block&blockid={}", blockid_48(0));
    let resp = h
        .router
        .clone()
        .oneshot(put_bytes(&put_uri, payload))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "PUT part 0");

    let finalize_uri = format!("{TWIRP_PREFIX}/FinalizeCacheEntryUpload");
    let req = post(
        &finalize_uri,
        &h.token,
        &json!({"key": key, "version": VERSION}),
    );
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "FinalizeCacheEntryUpload: {body}");
    body["entry_id"].as_str().unwrap().to_string()
}

/// Resolves the signed download URL, then GETs it and drains the body.
/// Returns the byte count so the bench can sanity-check it.
async fn download(h: &Harness, key: &str) -> usize {
    let req = post(
        &format!("{TWIRP_PREFIX}/GetCacheEntryDownloadURL"),
        &h.token,
        &json!({"key": key, "version": VERSION}),
    );
    let resp = h.router.clone().oneshot(req).await.unwrap();
    let (status, body) = body_json(resp).await;
    assert_eq!(status, StatusCode::OK, "GetCacheEntryDownloadURL: {body}");
    assert!(body["ok"].as_bool().unwrap_or(false), "no match: {body}");
    let dl_path = strip_base(body["signed_download_url"].as_str().unwrap()).to_string();

    let resp = h.router.clone().oneshot(get(&dl_path)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    bytes.len()
}

fn blockid_48(index: u64) -> String {
    let uuid = "11111111-2222-3333-4444-555555555555";
    let buf = format!("{uuid}{index:012}");
    assert_eq!(buf.len(), 48);
    base64::engine::general_purpose::STANDARD.encode(buf.as_bytes())
}

fn deterministic_payload(size_mib: u64) -> Vec<u8> {
    // Pseudo-random but reproducible — keeps the dirty-cache-line cost
    // consistent across runs without paying for a /dev/urandom read each
    // iter.
    let len = (size_mib * MIB) as usize;
    let mut buf = vec![0u8; len];
    for (i, b) in buf.iter_mut().enumerate() {
        *b = (i & 0xff) as u8;
    }
    buf
}

// --- Bench groups ------------------------------------------------------

fn upload_finalize_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("upload_finalize");
    group.sample_size(10);

    // One harness shared across iterations; each iter mints a unique
    // key via the harness counter so uploads don't collide. Cheaper
    // than spawning a fresh DB+tempdir per iter, and the only state
    // that grows is `cache_entries`/`uploads` rows — small relative
    // to the payload throughput we're measuring.
    let harness = rt.block_on(Harness::new());
    let harness_ref = &harness;

    for &size_mib in &[1u64, 16, 64] {
        let payload = deterministic_payload(size_mib);
        group.throughput(Throughput::Bytes(size_mib * MIB));
        group.bench_with_input(
            BenchmarkId::from_parameter(size_mib),
            &payload,
            |b, payload| {
                b.to_async(&rt).iter(|| async move {
                    let key = harness_ref.next_key();
                    upload_finalize(harness_ref, &key, payload.clone()).await;
                });
            },
        );
    }
    group.finish();
}

fn download_cold_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("download_cold");
    group.sample_size(10);
    let size_mib = 16u64;
    group.throughput(Throughput::Bytes(size_mib * MIB));

    // Cold downloads are one-shot: the first GET on an entry takes the
    // lazy-merge path, every subsequent GET hits the merged blob. To
    // avoid pre-allocating a fixed pool (which criterion's warmup loop
    // can blow through silently) we use `iter_custom`: per inner-iter,
    // upload a fresh entry, then time **only** the download. Criterion
    // sums the timed sections; the upload cost stays out of the
    // measurement.
    let harness = rt.block_on(Harness::new());
    let payload = deterministic_payload(size_mib);
    let harness_ref = &harness;
    let payload_ref = &payload;

    group.bench_function(BenchmarkId::from_parameter(format!("{size_mib}MiB")), |b| {
        b.to_async(&rt).iter_custom(|iters| async move {
            let mut total = Duration::ZERO;
            for _ in 0..iters {
                let key = harness_ref.next_key();
                upload_finalize(harness_ref, &key, payload_ref.clone()).await;
                let start = Instant::now();
                download(harness_ref, &key).await;
                total += start.elapsed();
            }
            total
        });
    });
    group.finish();
}

fn download_warm_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("download_warm");
    group.sample_size(20);
    let size_mib = 16u64;
    group.throughput(Throughput::Bytes(size_mib * MIB));

    // One-shot setup: upload + finalize once, then issue ONE download to
    // populate the merged blob. Subsequent downloads in the iter loop
    // hit the merged-blob fast path.
    let (harness, key) = rt.block_on(async {
        let h = Harness::new().await;
        let key = h.next_key();
        upload_finalize(&h, &key, deterministic_payload(size_mib)).await;
        // Warm the merged blob.
        download(&h, &key).await;
        (h, key)
    });
    let harness_ref = &harness;
    let key_ref = &key;

    group.bench_function(BenchmarkId::from_parameter(format!("{size_mib}MiB")), |b| {
        b.to_async(&rt).iter(|| async move {
            download(harness_ref, key_ref).await;
        });
    });
    group.finish();
}

fn match_cache_entry_bench(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let mut group = c.benchmark_group("match_cache_entry");

    for &count in &[10_000_i64, 100_000_i64] {
        // Fresh DB seeded with `count` entries — restored on every count
        // bump. Seeding takes ~5-30 s for 100k rows on a laptop; we pay
        // it once outside the iter loop. The matcher iter call lands at
        // the bottom of the SQL walk on every iteration (the seeded key
        // doesn't match the primary, only a prefix-restore on the last
        // scope hits).
        let db: Arc<dyn Db> = rt.block_on(async {
            let db = SqliteDb::connect_in_memory().await.unwrap();
            db.migrate().await.unwrap();
            seed_cache_entries(&db, count).await;
            Arc::new(db)
        });

        // Miss path — full SQL walk, lands on the last `find_entry_*`
        // call returning `None`. Catches index regressions on the
        // covering scan paths.
        group.bench_with_input(BenchmarkId::new("miss", count), &count, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = db.clone();
                async move {
                    let scopes = vec![SCOPE];
                    let restore_keys = vec!["miss-prefix-"];
                    let req = MatchRequest {
                        primary_key: "primary-miss",
                        restore_keys: &restore_keys,
                        version: VERSION,
                        scopes: &scopes,
                        repo_id: REPO_ID,
                    };
                    db.match_cache_entry(req).await.unwrap();
                }
            });
        });

        // Hit path — exact-primary match on a real seeded key. Catches
        // regressions in the hit branch (e.g. an `ORDER BY updatedAt
        // DESC LIMIT 1` losing its index).
        let hit_key = format!("seeded-key-{}", count - 1);
        let hit_key_ref = &hit_key;
        group.bench_with_input(BenchmarkId::new("hit", count), &count, |b, _| {
            b.to_async(&rt).iter(|| {
                let db = db.clone();
                async move {
                    let scopes = vec![SCOPE];
                    let restore_keys: Vec<&str> = vec![];
                    let req = MatchRequest {
                        primary_key: hit_key_ref.as_str(),
                        restore_keys: &restore_keys,
                        version: VERSION,
                        scopes: &scopes,
                        repo_id: REPO_ID,
                    };
                    let m = db.match_cache_entry(req).await.unwrap();
                    assert!(m.is_some(), "expected hit on seeded key");
                }
            });
        });
    }
    group.finish();
}

/// Seeds `count` `cache_entries` rows, all under one scope and version.
/// Keys are unique strings so the matcher's exact-key path is a true
/// hash-index lookup rather than collapsing to one row.
async fn seed_cache_entries(db: &SqliteDb, count: i64) {
    let mut tx = db.begin().await.unwrap();
    // One storage_location, all entries point at it — keeps seeding fast
    // while still populating cache_entries to the target count.
    tx.insert_storage_location("loc-bench", "fldr-bench", 1)
        .await
        .unwrap();
    for i in 0..count {
        let id = format!("entry-bench-{i}");
        let key = format!("seeded-key-{i}");
        tx.seed_cache_entry(
            &id,
            CacheEntryCoord {
                key: &key,
                version: VERSION,
                scope: SCOPE,
                repo_id: REPO_ID,
            },
            i,
            "loc-bench",
        )
        .await
        .unwrap();
    }
    tx.commit().await.unwrap();
}

criterion_group!(
    benches,
    match_cache_entry_bench,
    upload_finalize_bench,
    download_cold_bench,
    download_warm_bench,
);
criterion_main!(benches);
