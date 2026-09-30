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
ready=0
for _ in $(seq 1 100); do
    curl -sf http://127.0.0.1:3100/health >/dev/null 2>&1 && [ -s "$W/proxy.out" ] && { ready=1; break; }
    sleep 0.2
done
if [ "$ready" -ne 1 ]; then
    echo "FAIL: oxide or the runner cache proxy did not come up"
    tail -20 "$W/oxide.log" "$W/proxy.out"
    exit 1
fi
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

run_build() {
    if ! build > "$W/$1.log"; then
        echo "FAIL: $1 exited non-zero"
        tail -30 "$W/$1.log"
        exit 1
    fi
}

echo "--- build 1 (cold: exports cache)"
run_build b1
grep -E "writing layer|ERROR" "$W/b1.log" || true
echo "--- build 2 (fresh daemon: must import cache)"
run_build b2
grep -E "CACHED|ERROR" "$W/b2.log" || true

curl -s http://127.0.0.1:3100/metrics > "$W/metrics"
echo "--- oxide metrics"
grep -v '^#' "$W/metrics" | grep -E "lookups|bytes_total|commits_total|errors" || true

status=0
if [ "$(grep -c ' CACHED$' "$W/b2.log")" -lt 2 ]; then
    echo "FAIL: second build did not import the cache"; status=1
fi
if ! grep -q 'key_prefix="buildkit-blob"} [1-9]' "$W/metrics"; then
    echo "FAIL: no buildkit-blob traffic reached oxide"; status=1
fi
if ! grep -q 'lookups_total{result="hit",repo="mpecan/images",key_prefix="index-buildkit"} [1-9]' "$W/metrics"; then
    echo "FAIL: the cache index was not found through oxide"; status=1
fi
[ "$status" -eq 0 ] && echo "BUILDKIT E2E PASS"
exit "$status"
