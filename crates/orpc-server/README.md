# orpc-server

Server-side adapter for the [oRPC](https://orpc.unnoq.com) wire format,
built on `axum`. Reverse-engineered from `@orpc/server@1.x` because no
upstream Rust server library exists.

This crate ships from inside the
[`gha-cache-oxide`](https://github.com/mpecan/gha-cache-oxide) repo
(it backs that project's `/management-api/_rpc` surface) but is
free-standing: the only deps are `axum`, `serde`, `serde_json`, and
`tracing`.

## Status

Internal helper, not published to crates.io. Use it by `path`
dependency from the same workspace; the API is stable enough for that
but not yet promised to external callers.

## What's covered

- **Envelope types**: `RpcRequest<T>` (parses `{"json": <T>, "meta"?: <ignored>}`)
  and the `ok()` helper for `{"json": <output>}` success bodies.
- **Errors**: `RpcErr` with constructors for the common
  `COMMON_ORPC_ERROR_DEFS` codes (`bad_request`, `unauthorized`,
  `forbidden`, `not_found`, `conflict`, `internal`,
  `not_implemented`, `service_unavailable`). `IntoResponse` impl
  produces the upstream-compatible
  `{"json": {defined, code, status, message}}` envelope with HTTP
  status mirroring `body.status`.
- **Zod-style preprocess**: `preprocess::one_or_many` /
  `one_or_many_opt` for procedures whose inputs accept either a
  scalar string or an array of strings.

## What's NOT covered

- **JS-special-type round-tripping** (`Date`, `BigInt`, `Map`, `Set`,
  `undefined`, `NaN`, `±Infinity`, `RegExp`, `URL`, `Blob`). Inputs
  and outputs must be plain JSON-safe types. Incoming `meta` arrays
  are tolerated and ignored.
- **Server-streaming** (event-iterators).
- **CORS** / **`onError` interceptor**. Apply axum's
  `tower-http::cors` layer and your own `tracing` subscribers
  directly.
- **Procedure dispatch / type-safe routers**. Each procedure is a
  plain axum handler; mount them via `Router::route(...)` yourself.
  A future minor-version may add a `router!` macro or builder.

## Quick start

```rust
use axum::extract::Json;
use axum::response::{IntoResponse, Response};
use orpc_server::{ok, RpcErr, RpcRequest};
use serde::Deserialize;

#[derive(Deserialize)]
struct GetInput { id: String }

async fn get(Json(req): Json<RpcRequest<GetInput>>) -> Response {
    if req.json.id.is_empty() {
        return RpcErr::bad_request("id must be non-empty").into_response();
    }
    // ... look up `req.json.id`, then ...
    ok(serde_json::json!({ "id": req.json.id }))
}
```

Compatible with `@orpc/client@1.x`'s `RPCLink` for `{json}` envelope
and `ORPCErrorJSON` error shape. Verified by wire-fixture tests in
`gha-cache-oxide`'s `tests/management/rpc_*.rs` (no end-to-end TS SDK
in CI, so future orpc envelope changes can break compatibility
silently — pin your `@orpc/client` version).

## License

MIT.
