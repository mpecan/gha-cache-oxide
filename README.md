# gha-cache-oxide

A Rust port of [**github-actions-cache-server**](https://github.com/falcondev-oss/github-actions-cache-server) by [@falcondev-oss](https://github.com/falcondev-oss) — a self-hostable, drop-in replacement for GitHub's hosted Actions cache. It speaks the same HTTP protocol as `actions/cache`, so existing workflows work unchanged.

> **Status:** Early development. M1 (end-to-end smoke with filesystem + SQLite) in progress — see the [project milestones](https://github.com/mpecan/gha-cache-oxide/milestones) for what's landing when.

## What this is

- **Protocol** — GitHub Actions Cache v2 (Twirp RPCs + Azure-style block upload + plain download).
- **Storage** — pluggable drivers: filesystem today, S3-compatible and GCS coming.
- **Metadata** — pluggable database: SQLite today, Postgres and MySQL coming.
- **Auth** — GitHub Actions OIDC JWT verification against the public JWKS.

For the public HTTP contract with `actions/cache`, the upstream project is the specification; deviations are bugs.

## Acknowledgements

This project would not exist without [github-actions-cache-server](https://github.com/falcondev-oss/github-actions-cache-server) by [falconDev IT GmbH](https://github.com/falcondev-oss). Their TypeScript implementation is the reference we port from — including the observable HTTP behaviour, the storage abstraction, and the lazy merge-on-first-download design. Any correctness this port has is owed to their work; any bugs are ours.

If you're looking for a production-ready, battle-tested cache server today, use theirs. This port exists to offer a Rust-native alternative for deployments that prefer the Rust runtime profile, not to replace the original.

## License

MIT. See [LICENSE](./LICENSE) for the full text and [NOTICE](./NOTICE) for attribution details. The upstream project's copyright is preserved in both files.
