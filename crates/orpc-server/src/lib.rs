//! Server-side adapter for the [oRPC](https://orpc.unnoq.com) wire
//! format, built on `axum`.
//!
//! Reverse-engineered from `@orpc/server@1.x` /
//! `@orpc/client/standard::StandardRPCSerializer`. There's no
//! upstream Rust server library; this crate is a hand-rolled
//! drop-in for axum services that want to be wire-compatible with
//! the JavaScript SDK at [`@orpc/client`].
//!
//! # Wire format
//!
//! - URL: `POST <base>/<group>/<procedure>` (procedure path joined
//!   with `/`).
//! - Request body: JSON `{"json": <input>, "meta"?: <type-hints>}`.
//!   The `meta` field carries JS-special-type encodings (`Date`,
//!   `BigInt`, `Map`, `Set`, `undefined`, etc.); this crate accepts
//!   and ignores it. Inputs must be plain JSON-safe types.
//! - Success body: JSON `{"json": <output>}` with HTTP 200.
//! - Error body: JSON `{"json": <ORPCErrorJSON>}` where
//!   `ORPCErrorJSON = {defined: bool, code, status, message,
//!   data?}`. HTTP status mirrors the body's `status` field.
//!
//! # Quick start
//!
//! ```no_run
//! use axum::extract::Json;
//! use axum::response::{IntoResponse, Response};
//! use orpc_server::{ok, RpcErr, RpcRequest};
//! use serde::Deserialize;
//!
//! #[derive(Deserialize)]
//! struct GetInput { id: String }
//!
//! async fn get(Json(req): Json<RpcRequest<GetInput>>) -> Response {
//!     // Pretend to look up `req.json.id`...
//!     if req.json.id.is_empty() {
//!         return RpcErr::bad_request("id must be non-empty").into_response();
//!     }
//!     ok(serde_json::json!({ "id": req.json.id }))
//! }
//! # let _ = get;
//! ```
//!
//! # Caveats
//!
//! - JS-special-type round-tripping (`Date`, `BigInt`, `Map`, `Set`,
//!   `undefined`, `NaN`, `±Infinity`, `RegExp`, `URL`, `Blob`) is
//!   **not** implemented. Inputs/outputs must be plain JSON-safe
//!   types. Incoming `meta` arrays are tolerated; outgoing responses
//!   never include one.
//! - Server-streaming (`@orpc/server`'s event-iterator support) is
//!   not implemented.
//! - `CORSPlugin` and `onError` interceptor (upstream concepts) are
//!   the caller's responsibility — apply axum's `tower-http::cors`
//!   layer and use `tracing` directly for logging.
//!
//! [`@orpc/client`]: https://www.npmjs.com/package/@orpc/client

pub mod error;
pub mod preprocess;

mod envelope;

pub use envelope::{RpcRequest, ok};
pub use error::RpcErr;
