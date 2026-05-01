//! oRPC `_rpc` wire-format adapter for the management API (issue #77 part 2).
//!
//! Mirrors upstream's `routes/management-api/_rpc.ts` so the
//! TypeScript SDK at `sdk/index.ts` works against this server.
//!
//! # Wire format
//!
//! Reverse-engineered from `@orpc/server@1.x`:
//! - URL: `POST /management-api/_rpc/<group>/<procedure>`
//!   (e.g. `/cacheEntries/findMany`). Procedure names mirror
//!   upstream's router shape verbatim
//!   (`lib/api/{cache-entries,storage-locations}.ts`).
//! - Auth: `X-Api-Key: <MANAGEMENT_API_KEY>` header (not Bearer —
//!   upstream's `lib/api/base.ts` middleware reads the same header).
//! - Method: POST always.
//! - Request body: `{"json": <input>, "meta"?: <type-hints>}`. Our
//!   procedures take only JSON-safe inputs; we accept an incoming
//!   `meta` array and ignore it, never emit one.
//! - Success body: `{"json": <output>}` with HTTP 200.
//! - Error body: `{"json": <RpcErrorBody>}` with HTTP status mapped
//!   per `COMMON_ORPC_ERROR_DEFS` (`NOT_FOUND` → 404, etc.).
//!
//! # Parity caveats (also in README + PR description)
//!
//! - No upstream TS SDK in CI; tests use hand-crafted fixtures.
//!   Future orpc releases changing the envelope can break us silently.
//! - No special-type round-tripping (`Date`, `BigInt`, `Map`, `Set`,
//!   `undefined`, etc.); management API uses only plain JSON.
//! - No CORS / `onError` interceptor (deployment concerns / `tracing`).

use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderName, StatusCode};
use axum::middleware::{self as axum_mw, Next};
use axum::response::{IntoResponse, Json, Response};
use axum::routing::post;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::db::entities::{CacheEntryFilter, MatchRequest};
use crate::state::AppState;

use super::pagination::PageQuery;
use super::rpc_preprocess::{deserialize_one_or_many, deserialize_one_or_many_opt};

const X_API_KEY: HeaderName = HeaderName::from_static("x-api-key");

// ---- envelope types -----------------------------------------------------

/// Inbound request envelope. `meta` is parsed permissively (we accept
/// an optional array of any-typed entries) but ignored — we never use
/// JS-specific encodings on the request side.
#[derive(Debug, Deserialize)]
pub(super) struct RpcRequest<T> {
    pub json: T,
    #[serde(default)]
    #[allow(dead_code)]
    pub meta: Option<Value>,
}

/// Outbound success envelope. Always `{"json": <T>}`; we never emit
/// `meta` because the management API only round-trips JSON-safe types.
#[derive(Debug, Serialize)]
struct RpcSuccess<T>
where
    T: Serialize,
{
    json: T,
}

/// Outbound error body — the inner shape that goes into
/// `{"json": <RpcErrorBody>}`. Matches the upstream `ORPCErrorJSON`
/// shape exactly (`{defined, code, status, message, data?}`).
#[derive(Debug, Serialize)]
struct RpcErrorBody {
    /// `false` for unhandled / framework-level errors; `true` only
    /// when an error was declared by the procedure (we never declare
    /// any, so this is always `false`).
    defined: bool,
    /// Upstream-compat code from `COMMON_ORPC_ERROR_DEFS`
    /// (`NOT_FOUND`, `BAD_REQUEST`, etc.).
    code: &'static str,
    /// HTTP status that mirrors the body's status — repeated so
    /// clients can decode errors from the body alone (matches
    /// upstream's wire shape).
    status: u16,
    /// Human-readable message.
    message: String,
}

/// Maps an internal failure to a `(HTTP status, ORPC code, message)` triple.
#[derive(Debug, Clone)]
struct RpcErr {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl RpcErr {
    fn unauthorized(message: &str) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            code: "UNAUTHORIZED",
            message: message.to_string(),
        }
    }
    fn service_unavailable(message: &str) -> Self {
        Self {
            status: StatusCode::SERVICE_UNAVAILABLE,
            code: "SERVICE_UNAVAILABLE",
            message: message.to_string(),
        }
    }
    fn not_found(message: &str) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NOT_FOUND",
            message: message.to_string(),
        }
    }
    fn bad_request(message: &str) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "BAD_REQUEST",
            message: message.to_string(),
        }
    }
    fn internal(message: &str) -> Self {
        tracing::error!(message, "rpc internal error");
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "INTERNAL_SERVER_ERROR",
            message: "Internal Server Error".to_string(),
        }
    }
}

impl IntoResponse for RpcErr {
    fn into_response(self) -> Response {
        let body = RpcSuccess {
            json: RpcErrorBody {
                defined: false,
                code: self.code,
                status: self.status.as_u16(),
                message: self.message,
            },
        };
        (self.status, Json(body)).into_response()
    }
}

fn ok<T: Serialize>(value: T) -> Response {
    Json(RpcSuccess { json: value }).into_response()
}

// ---- auth middleware ----------------------------------------------------

/// `X-Api-Key` middleware mirroring upstream's `lib/api/base.ts` +
/// `_rpc.ts`.
///
/// Applied to every `/management-api/_rpc/...` route. Returns the
/// upstream-compatible 503 envelope when the key isn't configured —
/// clients see `{"json": {"code": "SERVICE_UNAVAILABLE", ...}}`
/// matching `routes/management-api/_rpc.ts:18`
/// (`createError({statusCode: 503, message: 'Management API is disabled'})`).
async fn require_x_api_key(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let Some(configured) = state.config.management_api_key.as_ref() else {
        return RpcErr::service_unavailable("Management API is disabled").into_response();
    };
    let Some(provided) = req.headers().get(&X_API_KEY).and_then(|v| v.to_str().ok()) else {
        return RpcErr::unauthorized("Missing X-Api-Key header").into_response();
    };
    if provided != configured.expose() {
        return RpcErr::unauthorized("Invalid X-Api-Key").into_response();
    }
    next.run(req).await
}

// ---- procedure inputs ---------------------------------------------------

/// Input for `cacheEntries.findMany`.
///
/// Upstream's Zod schema (`lib/api/cache-entries.ts:86-100`) accepts
/// `key`, `version`, `scope`, `repoId`, `itemsPerPage` (default 20,
/// max 100), and `page` (default 1). `key` and `version` are
/// accepted on the wire for SDK compatibility but **not currently
/// applied** — `Db::list_cache_entries` only filters on
/// `scope`/`repoId` (REST-side gap, README divergence #7). The
/// `dead_code` allow is honest about this.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FindManyInput {
    #[allow(dead_code)]
    #[serde(default)]
    key: Option<String>,
    #[allow(dead_code)]
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repo_id: Option<String>,
    #[serde(default)]
    page: Option<u32>,
    #[serde(default)]
    items_per_page: Option<u32>,
}

#[derive(Debug, Deserialize)]
struct IdInput {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct MatchInput {
    primary_key: String,
    /// Mirrors upstream's `z.preprocess((val) => Array.isArray(val) ? val : [val], ...)`
    /// — a scalar string is normalised to a one-element array.
    #[serde(default, deserialize_with = "deserialize_one_or_many_opt")]
    restore_keys: Option<Vec<String>>,
    /// Same scalar-or-array preprocess.
    #[serde(deserialize_with = "deserialize_one_or_many")]
    scopes: Vec<String>,
    repo_id: String,
    version: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteManyInput {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    version: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    repo_id: Option<String>,
}

// ---- procedure outputs --------------------------------------------------

/// Paginated list output. Upstream's Zod output schema is just
/// `{total, items}` (`lib/api/cache-entries.ts:103-107`); we match
/// that exactly so SDK-typed clients see the same shape. Pagination
/// params remain on the input.
#[derive(Debug, Serialize)]
struct ListEnvelope<T>
where
    T: Serialize,
{
    total: i64,
    items: Vec<T>,
}

#[derive(Debug, Serialize)]
struct MatchOutput {
    #[serde(rename = "match")]
    match_field: crate::db::entities::CacheEntry,
    #[serde(rename = "type")]
    type_field: crate::db::entities::MatchType,
}

#[derive(Debug, Serialize)]
struct DeleteManyOutput {
    deleted: u64,
}

// ---- procedure handlers -------------------------------------------------

/// `cacheEntries.findMany` — upstream procedure name is `findMany`,
/// not `list`. The TS SDK calls `client.cacheEntries.findMany(...)`,
/// which `toHttpPath` translates to `/cacheEntries/findMany`.
async fn cache_entries_find_many(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<FindManyInput>>,
) -> Response {
    let input = req.json;
    let pq = PageQuery {
        page: input.page,
        items_per_page: input.items_per_page,
    };
    let p = pq.resolve();
    // Note: upstream's `findMany` filters on `key`/`version`/`scope`
    // but our `Db::list_cache_entries` only takes `scope`/`repo_id`.
    // This is a known REST-side gap (README §"Management API",
    // divergence #7); the RPC surface inherits the same limitation.
    // The SDK will see `key`/`version` accepted but not applied; the
    // returned page may include rows that don't match those filters.
    let scope = input.scope.as_deref();
    let repo_id = input.repo_id.as_deref();
    let items = match state
        .db
        .list_cache_entries(scope, repo_id, p.limit, p.offset)
        .await
    {
        Ok(v) => v,
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    let total = match state.db.count_cache_entries(scope, repo_id).await {
        Ok(n) => n,
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    ok(ListEnvelope { total, items })
}

async fn cache_entries_get(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<IdInput>>,
) -> Response {
    match state.db.find_cache_entry_by_id(&req.json.id).await {
        Ok(Some(entry)) => ok(entry),
        Ok(None) => RpcErr::not_found("Cache entry not found").into_response(),
        Err(e) => RpcErr::internal(&e.to_string()).into_response(),
    }
}

/// `cacheEntries.match` — upstream returns 200 + body `null` on no
/// match (`lib/api/cache-entries.ts:55-66,75`), not a 404. We mirror
/// that so SDK-typed clients see the documented `MatchedEntry | null`
/// return type.
async fn cache_entries_match(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<MatchInput>>,
) -> Response {
    let input = req.json;
    if input.scopes.is_empty() {
        return RpcErr::bad_request("scopes must contain at least one entry").into_response();
    }
    let scope_slice: Vec<&str> = input.scopes.iter().map(String::as_str).collect();
    let restore_owned = input.restore_keys.unwrap_or_default();
    let restore_slice: Vec<&str> = restore_owned.iter().map(String::as_str).collect();
    let request = MatchRequest {
        primary_key: &input.primary_key,
        restore_keys: &restore_slice,
        version: &input.version,
        scopes: &scope_slice,
        repo_id: &input.repo_id,
    };
    match state.db.match_cache_entry(request).await {
        Ok(Some(matched)) => ok(MatchOutput {
            match_field: matched.entry,
            type_field: matched.match_type,
        }),
        Ok(None) => ok(Value::Null),
        Err(e) => RpcErr::internal(&e.to_string()).into_response(),
    }
}

async fn cache_entries_delete(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<IdInput>>,
) -> Response {
    let location = match state.db.find_location_for_entry(&req.json.id).await {
        Ok(Some(l)) => l,
        Ok(None) => return RpcErr::not_found("Cache entry not found").into_response(),
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    if let Err(e) = tx.delete_storage_location(&location.id).await {
        return RpcErr::internal(&e.to_string()).into_response();
    }
    if let Err(e) = tx.commit().await {
        return RpcErr::internal(&e.to_string()).into_response();
    }
    if let Err(e) = state.storage.delete_folder(&location.folder_name).await {
        tracing::warn!(
            error = %e,
            location_id = %location.id,
            folder = %location.folder_name,
            "rpc cache_entries.delete: folder removal failed; row already deleted",
        );
    }
    drop(crate::tasks::cleanup::spawn_locations_sweep(&state));
    // Upstream returns no body; we mirror that with a JSON `null`
    // success envelope.
    ok(Value::Null)
}

async fn cache_entries_delete_many(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<DeleteManyInput>>,
) -> Response {
    let input = req.json;
    let filter = CacheEntryFilter {
        key: input.key.as_deref(),
        version: input.version.as_deref(),
        scope: input.scope.as_deref(),
        repo_id: input.repo_id.as_deref(),
    };
    if filter.key.is_none()
        && filter.version.is_none()
        && filter.scope.is_none()
        && filter.repo_id.is_none()
    {
        return RpcErr::bad_request("at least one filter required: key, version, scope, or repoId")
            .into_response();
    }
    match state.db.delete_cache_entries_by_filter(filter).await {
        Ok(deleted) => {
            drop(crate::tasks::cleanup::spawn_locations_sweep(&state));
            ok(DeleteManyOutput { deleted })
        }
        Err(e) => RpcErr::internal(&e.to_string()).into_response(),
    }
}

async fn storage_locations_get(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<IdInput>>,
) -> Response {
    match state.db.find_storage_location_by_id(&req.json.id).await {
        Ok(Some(loc)) => ok(loc),
        Ok(None) => RpcErr::not_found("Storage location not found").into_response(),
        Err(e) => RpcErr::internal(&e.to_string()).into_response(),
    }
}

async fn storage_locations_delete(
    State(state): State<AppState>,
    Json(req): Json<RpcRequest<IdInput>>,
) -> Response {
    let location = match state.db.find_storage_location_by_id(&req.json.id).await {
        Ok(Some(l)) => l,
        Ok(None) => return RpcErr::not_found("Storage location not found").into_response(),
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    let mut tx = match state.db.begin().await {
        Ok(t) => t,
        Err(e) => return RpcErr::internal(&e.to_string()).into_response(),
    };
    if let Err(e) = tx.delete_storage_location(&location.id).await {
        return RpcErr::internal(&e.to_string()).into_response();
    }
    if let Err(e) = tx.commit().await {
        return RpcErr::internal(&e.to_string()).into_response();
    }
    if let Err(e) = state.storage.delete_folder(&location.folder_name).await {
        tracing::warn!(
            error = %e,
            location_id = %location.id,
            folder = %location.folder_name,
            "rpc storage_locations.delete: folder removal failed; row already deleted",
        );
    }
    ok(Value::Null)
}

// ---- 404 fallback for unknown procedures --------------------------------

/// Catches any `/management-api/_rpc/...` URL that doesn't match a
/// registered procedure and returns the orpc-shaped 404 envelope.
/// Without this the axum default 404 returns an empty body that the
/// SDK treats as a malformed-response error rather than a clean
/// `NOT_FOUND`.
async fn unknown_procedure(req: Request<Body>) -> Response {
    let path = req.uri().path().to_string();
    RpcErr::not_found(&format!("Unknown RPC procedure: {path}")).into_response()
}

// ---- router -------------------------------------------------------------

/// Builds the `/management-api/_rpc` sub-router. Caller `nest`s this
/// in `build_app`.
///
/// Procedure names mirror upstream's router shape verbatim
/// (`cacheEntriesRouter` exposes `findMany`, `get`, `match`, `delete`,
/// `deleteMany`; `storageLocationsRouter` exposes `get` + `delete`
/// — see `lib/api/{cache-entries,storage-locations}.ts`). The TS SDK
/// accesses procedures by these exact names, so a typo here breaks
/// SDK compatibility silently.
pub fn router(state: AppState) -> Router<AppState> {
    Router::new()
        .route("/cacheEntries/findMany", post(cache_entries_find_many))
        .route("/cacheEntries/get", post(cache_entries_get))
        .route("/cacheEntries/match", post(cache_entries_match))
        .route("/cacheEntries/delete", post(cache_entries_delete))
        .route("/cacheEntries/deleteMany", post(cache_entries_delete_many))
        .route("/storageLocations/get", post(storage_locations_get))
        .route("/storageLocations/delete", post(storage_locations_delete))
        .fallback(unknown_procedure)
        .layer(axum_mw::from_fn_with_state(state, require_x_api_key))
}
