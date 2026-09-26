# Forgejo v1 dialect: end-to-end harness

Manual (not part of `cargo test`) check of the whole path a Forgejo
Actions job's cache traffic takes:

```
@actions/cache 4.1.0 / 6.2.0  →  forgejo-runner v13.2.0 cacheproxy  →  gha-cache-oxide  →  Garage (S3)
```

- `proxy/` is a ~40-line Go program that starts the runner's own
  `act/cacheproxy` handler (the code a runner runs when
  `cache.external_server` is set), registers a run without and a run with a
  write-isolation key, and prints each run's `ACTIONS_CACHE_URL`.
- `e2e.mjs` calls `saveCache` / `restoreCache` from the real
  `@actions/cache` package against those URLs. It covers a cold miss, a
  multi-chunk save (32 MiB chunks, uploaded 4 in parallel), a byte-identical
  restore, restore-key prefix matching, and write isolation in both
  directions. 6.2.0 is the version bundled in `Swatinem/rust-cache@v2`.
- `run.sh` starts a single-node Garage container, builds and starts oxide
  with `STORAGE_DRIVER=s3` against it, runs the scenario with both client
  versions, and prints `/metrics`.

```sh
tests/forgejo_e2e/run.sh              # 150 MiB cache
BIG_MB=500 tests/forgejo_e2e/run.sh   # ≈ the largest Rust target caches
```

Needs cargo, go (`GOTOOLCHAIN=auto` fetches 1.26), node + npm, zstd, and
podman or docker (`E2E_CONTAINER` overrides which one).
