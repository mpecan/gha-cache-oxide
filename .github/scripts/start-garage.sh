#!/bin/sh
# Starts a single-node Garage (S3-compatible) container for the ignored
# S3 tests, creates the test bucket and an access key, and prints the
# credentials as KEY=VALUE lines on stdout:
#
#   S3_TEST_ACCESS_KEY=GK...
#   S3_TEST_SECRET_KEY=...
#
# CI appends them to $GITHUB_ENV. Locally:
#
#   eval "$(CONTAINER=podman .github/scripts/start-garage.sh | sed 's/^/export /')"
#   S3_TEST_ENDPOINT=http://localhost:3900 cargo test -- --ignored s3
#
# Env: CONTAINER (default docker), GARAGE_IMAGE, GARAGE_NAME (default
# garage), GARAGE_PORT (default 3900), S3_TEST_BUCKET (default
# gha-cache-test). The region is us-east-1 because that is what the
# tests sign with by default (Garage rejects a region mismatch).
set -eu

CTR=${CONTAINER:-docker}
IMAGE=${GARAGE_IMAGE:-docker.io/dxflrs/garage:v2.1.0}
NAME=${GARAGE_NAME:-garage}
PORT=${GARAGE_PORT:-3900}
BUCKET=${S3_TEST_BUCKET:-gha-cache-test}

conf=$(mktemp)
cat > "$conf" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(openssl rand -hex 32)"
[s3_api]
s3_region = "us-east-1"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
EOF
chmod 644 "$conf"

"$CTR" run -d --name "$NAME" -p "$PORT:3900" \
    -v "$conf:/etc/garage.toml:ro" "$IMAGE" >/dev/null

g() { "$CTR" exec "$NAME" /garage "$@"; }

# The image has no shell or curl, so readiness is "the CLI can reach
# the node over RPC".
node=""
for _ in $(seq 1 60); do
    node=$(g status 2>/dev/null | awk '/127.0.0.1:3901/{print $1; exit}') || true
    [ -n "$node" ] && break
    sleep 1
done
if [ -z "$node" ]; then
    echo "garage did not become ready in 60s" >&2
    "$CTR" logs "$NAME" >&2
    exit 1
fi

g layout assign -z dc1 -c 1G "$node" >/dev/null
g layout apply --version 1 >/dev/null
g bucket create "$BUCKET" >/dev/null
keys=$(g key create gha-cache-test)
g bucket allow --read --write --owner "$BUCKET" --key gha-cache-test >/dev/null

echo "$keys" | awk '/Key ID/{print "S3_TEST_ACCESS_KEY=" $3} /Secret key/{print "S3_TEST_SECRET_KEY=" $3}'
