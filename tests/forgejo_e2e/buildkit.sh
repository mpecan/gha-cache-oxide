#!/bin/sh
# End-to-end check of buildx/BuildKit `type=gha` cache over the Forgejo
# v1 dialect:
#
#   BuildKit (container) --v1--> forgejo-runner cacheproxy --> oxide --> Garage
#
# Builds tests/forgejo_e2e/buildkit/Dockerfile twice on fresh BuildKit
# daemons: the first exports cache, the second must import it (its RUN
# steps report CACHED). Prints oxide's per-repo / key-prefix metrics.
#
# The runtime token is a JWT shaped like Forgejo's (`ac`, `exp`, `nbf`
# claims): go-actions-cache parses it unverified and refuses to start
# without them. Requires podman (or docker via E2E_CONTAINER), cargo,
# go, python3. Not part of `cargo test`.
set -eu

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
CTR=${E2E_CONTAINER:-$(command -v podman || command -v docker)}
BUILDKIT_IMAGE=${BUILDKIT_IMAGE:-docker.io/moby/buildkit:v0.25.1}
# How containers reach this host (podman: host.containers.internal;
# Docker Desktop: host.docker.internal).
HOST_ALIAS=${HOST_ALIAS:-host.containers.internal}
W=$(mktemp -d)
NAME=oxide-bk-garage-$$

cleanup() {
    [ -n "${OXIDE:-}" ] && kill "$OXIDE" 2>/dev/null || true
    [ -n "${PROXY:-}" ] && kill "$PROXY" 2>/dev/null || true
    "$CTR" rm -f "$NAME" >/dev/null 2>&1 || true
    rm -rf "$W"
}
trap cleanup EXIT

CONTAINER=$CTR GARAGE_NAME=$NAME GARAGE_PORT=3901 \
    "$ROOT/.github/scripts/start-garage.sh" > "$W/keys" 2>/dev/null
. "$W/keys"

(cd "$ROOT" && cargo build --quiet --bin gha-cache-oxide)
(cd "$HERE/proxy" && GOTOOLCHAIN=auto go build -o "$W/proxy" .)

API_BASE_URL=http://127.0.0.1:3100 PORT=3100 STORAGE_DRIVER=s3 \
    STORAGE_S3_BUCKET=gha-cache-test AWS_ENDPOINT_URL=http://127.0.0.1:3901 \
    AWS_REGION=us-east-1 AWS_ACCESS_KEY_ID="$S3_TEST_ACCESS_KEY" \
    AWS_SECRET_ACCESS_KEY="$S3_TEST_SECRET_KEY" DB_DRIVER=sqlite \
    DB_SQLITE_PATH="$W/db" FORGEJO_CACHE_SECRET=bk \
    "$ROOT/target/debug/gha-cache-oxide" > "$W/oxide.log" 2>&1 &
OXIDE=$!
PROXY_HOST_OVERRIDE="http://$HOST_ALIAS:3200" \
    "$W/proxy" http://127.0.0.1:3100 bk 3200 mpecan/images > "$W/proxy.out" 2>&1 &
PROXY=$!
for _ in $(seq 1 100); do
    curl -sf http://127.0.0.1:3100/health >/dev/null 2>&1 && [ -s "$W/proxy.out" ] && break
    sleep 0.2
done
URL=$(sed -n 's/^=//p' "$W/proxy.out")
TOKEN=$(python3 "$HERE/buildkit/jwt.py")
echo "cache url: $URL"

build() {
    "$CTR" run --rm --privileged -v "$HERE/buildkit:/ctx:ro" \
        --entrypoint buildctl-daemonless.sh "$BUILDKIT_IMAGE" \
        build --frontend dockerfile.v0 --local context=/ctx --local dockerfile=/ctx \
        --progress plain \
        --export-cache "type=gha,url=$URL,token=$TOKEN,scope=buildkit,mode=max" \
        --import-cache "type=gha,url=$URL,token=$TOKEN,scope=buildkit" 2>&1
}

echo "--- build 1 (cold: exports cache)"
build > "$W/b1.log"
grep -E "writing layer .* done|ERROR" "$W/b1.log" || true
echo "--- build 2 (fresh daemon: must import cache)"
build > "$W/b2.log"
grep -E "CACHED|ERROR" "$W/b2.log" || true

echo "--- oxide metrics"
curl -s http://127.0.0.1:3100/metrics | grep -v '^#' | grep -E "lookups|bytes_total|commits_total|errors"
status=0
[ "$(grep -c ' CACHED$' "$W/b2.log")" -ge 2 ] || { echo "FAIL: second build did not import the cache"; status=1; }
grep -q 'key_prefix="buildkit-blob"' <<EOM || { echo "FAIL: no buildkit-blob traffic"; status=1; }
$(curl -s http://127.0.0.1:3100/metrics)
EOM
[ "$status" -eq 0 ] && echo "BUILDKIT E2E PASS"
exit "$status"
