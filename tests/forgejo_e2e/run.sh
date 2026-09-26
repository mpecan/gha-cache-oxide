#!/bin/sh
# End-to-end check of the Forgejo v1 dialect against the real stack:
#
#   @actions/cache (v4 and v6) → forgejo-runner cacheproxy (v13.2.0)
#     → gha-cache-oxide → Garage (S3, in a container)
#
# Requires: cargo, go (GOTOOLCHAIN=auto fetches 1.26), node + npm, zstd,
# and podman or docker. Not part of `cargo test`; see README.md.
#
#   tests/forgejo_e2e/run.sh            # 150 MiB cache
#   BIG_MB=500 tests/forgejo_e2e/run.sh # ~ the largest Rust target caches
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
WORK=$(mktemp -d)
CTR=${E2E_CONTAINER:-$(command -v podman || command -v docker)}
NAME=oxide-e2e-garage-$$
OXIDE_PORT=${OXIDE_PORT:-3100}
PROXY_PORT=${PROXY_PORT:-3200}
SECRET=e2e-secret
export BIG_MB=${BIG_MB:-150}

cleanup() {
    [ -n "${OXIDE:-}" ] && kill "$OXIDE" 2>/dev/null || true
    [ -n "${PROXY:-}" ] && kill "$PROXY" 2>/dev/null || true
    "$CTR" rm -f "$NAME" >/dev/null 2>&1 || true
    rm -rf "$WORK"
}
trap cleanup EXIT

echo "--- garage"
cat > "$WORK/garage.toml" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(openssl rand -hex 32)"
[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
EOF
"$CTR" run -d --name "$NAME" -p 127.0.0.1::3900 \
    -v "$WORK/garage.toml:/etc/garage.toml:ro" docker.io/dxflrs/garage:v2.1.0 >/dev/null
S3_PORT=$("$CTR" port "$NAME" 3900/tcp | head -1 | sed 's/.*://')
g() { "$CTR" exec "$NAME" /garage "$@"; }
for _ in $(seq 1 50); do g status >/dev/null 2>&1 && break; sleep 0.2; done
NODE=$(g status 2>/dev/null | awk '/127.0.0.1:3901/{print $1; exit}')
g layout assign -z dc1 -c 5G "$NODE" >/dev/null
g layout apply --version 1 >/dev/null
g bucket create gha-cache >/dev/null
g key create oxide-key > "$WORK/key.txt"
g bucket allow --read --write --owner gha-cache --key oxide-key >/dev/null
KEY_ID=$(awk '/Key ID/{print $3}' "$WORK/key.txt")
KEY_SECRET=$(awk '/Secret key/{print $3}' "$WORK/key.txt")

echo "--- build"
(cd "$ROOT" && cargo build --quiet --bin gha-cache-oxide)
(cd "$HERE/proxy" && GOTOOLCHAIN=auto go build -o "$WORK/proxy" .)
[ -d "$HERE/node_modules" ] || (cd "$HERE" && npm install --silent --no-audit --no-fund)

echo "--- start oxide + runner cache proxy"
API_BASE_URL="http://127.0.0.1:$OXIDE_PORT" PORT="$OXIDE_PORT" \
    STORAGE_DRIVER=s3 STORAGE_S3_BUCKET=gha-cache \
    AWS_ENDPOINT_URL="http://127.0.0.1:$S3_PORT" AWS_REGION=garage \
    AWS_ACCESS_KEY_ID="$KEY_ID" AWS_SECRET_ACCESS_KEY="$KEY_SECRET" \
    DB_DRIVER=sqlite DB_SQLITE_PATH="$WORK/oxide.db" FORGEJO_CACHE_SECRET="$SECRET" \
    "$ROOT/target/debug/gha-cache-oxide" > "$WORK/oxide.log" 2>&1 &
OXIDE=$!
"$WORK/proxy" "http://127.0.0.1:$OXIDE_PORT" "$SECRET" "$PROXY_PORT" \
    e2e/rust-repo refs/pull/9/head > "$WORK/proxy.out" 2>&1 &
PROXY=$!
for _ in $(seq 1 100); do
    curl -sf "http://127.0.0.1:$OXIDE_PORT/health" >/dev/null 2>&1 &&
        [ "$(wc -l < "$WORK/proxy.out")" -ge 2 ] && break
    sleep 0.2
done
URL_SHARED=$(sed -n 's/^=//p' "$WORK/proxy.out")
URL_ISOLATED=$(sed -n 's#^refs/pull/9/head=##p' "$WORK/proxy.out")
export URL_SHARED URL_ISOLATED

status=0
for pkg in @actions/cache actions-cache-6; do
    echo "--- $pkg ($BIG_MB MiB)"
    out=$(cd "$HERE" && CACHE_PKG=$pkg node e2e.mjs 2>&1) || true
    printf '%s\n' "$out" | grep -E '^(ok|FAIL|E2E)' || true
    printf '%s\n' "$out" | grep -q '^E2E PASS$' || status=1
done

echo "--- metrics"
curl -s "http://127.0.0.1:$OXIDE_PORT/metrics" | grep -v '^#'
if [ "$status" -ne 0 ]; then
    echo "--- oxide log (tail)"
    tail -40 "$WORK/oxide.log"
fi
exit "$status"
