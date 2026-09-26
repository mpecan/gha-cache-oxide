# syntax=docker/dockerfile:1.7
#
# Multi-stage build for gha-cache-oxide.
#
# Stage 1 — `rust:alpine` produces a fully-static binary against musl
# libc, so the runtime stage can be a distroless/static image. The
# `--mount=type=cache` directives keep the cargo registry and the
# `target/` dir out of the image while reusing them across builds —
# the binary is `cp`'d to /tmp before the layer ends so it survives
# the cache mount.
#
# Stage 2 — `gcr.io/distroless/static-debian13:nonroot` is ~2 MiB,
# has no shell, runs as the non-root UID 65532 by default, and matches
# the issue #20 brief verbatim. No HEALTHCHECK directive: distroless
# has no curl/wget/sh; the README's `curl /health` step is the
# acceptance-criteria probe instead.

# Both base images are pinned as literal `tag@digest` (multi-arch index
# digests, so amd64 and arm64 builds resolve the same release). The tag
# documents intent; the digest is what is pulled. Keep them literal —
# no ARG interpolation — so Dependabot can bump tag and digest together
# and a tag edit can never silently build against a stale digest.
#
# Builder tag matches `rust-toolchain.toml` (1.93.0).
FROM rust:1.94.1-alpine3.20@sha256:6b1a8a05a7d4863f87c383ceb645bf038c5dba41e5a43fb7c7cc4a252b313a35 AS builder

# musl-dev is the toolchain — the `rust:alpine` image already targets
# x86_64-unknown-linux-musl, but the C runtime headers aren't on the
# image by default (sqlx uses none, but pulling musl-dev keeps the
# build immune to future deps that bring in `cc`).
RUN apk add --no-cache musl-dev

WORKDIR /build

# Manifests + source. `clippy.toml` is harmless (lint-only) but copying
# it keeps the build context shape predictable. `.sqlx/` is not used by
# this project — sqlx queries are runtime-checked via `query_as`.
COPY Cargo.toml Cargo.lock clippy.toml ./
COPY src ./src
COPY migrations ./migrations
# `[[bench]]` in Cargo.toml (#43) declares `benches/protocol.rs`. Cargo
# parses every declared target during ANY build (even `--bin <name>`),
# so the file must exist on disk or the manifest fails to parse.
COPY benches ./benches
# Workspace member `orpc-server` (#77 Phase B refactor). The root
# Cargo.toml's `[workspace] members = ["crates/orpc-server"]` line
# triggers manifest resolution at parse time, so the sub-crate's
# `Cargo.toml` must be present even for a bin-only build.
COPY crates ./crates

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked --bin gha-cache-oxide && \
    cp target/release/gha-cache-oxide /tmp/gha-cache-oxide

# Pre-seed the volume mount point so it inherits non-root ownership
# when Docker creates the named volume on first run. Without this,
# Docker creates `/var/lib/gha-cache` as `root:root 0755` and the
# UID 65532 nonroot user gets `EACCES` on `create_dir_all` /
# `cache.db` open. Distroless has no shell, so we can't `mkdir` in
# the runtime stage — seed it here and `COPY --chown` it across.
RUN mkdir -p /seed/gha-cache

# ----------------------------------------------------------------------

FROM gcr.io/distroless/static-debian13:nonroot@sha256:e2e927ec666bae08560abb3c55d0659eceabb657f56b6782ab500a9fc7f555e3 AS runtime

COPY --from=builder /tmp/gha-cache-oxide /usr/local/bin/gha-cache-oxide
# `nonroot` resolves to UID 65532 via the distroless image's
# `/etc/passwd`. Numeric form would also work and is included as a
# comment for operators who customise the base image.
COPY --from=builder --chown=nonroot:nonroot /seed/gha-cache /var/lib/gha-cache

# Default port; the binary itself reads PORT (and every other knob)
# from the environment. Documented in `src/config/mod.rs`.
ENV PORT=3000
EXPOSE 3000

USER nonroot

ENTRYPOINT ["/usr/local/bin/gha-cache-oxide"]
