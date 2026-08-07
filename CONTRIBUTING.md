# Contributing

## Before opening a PR

1. Read [`docs/VERIFICATION.md`](docs/VERIFICATION.md).
2. Run verification locally with **one rustc job** (`export CARGO_BUILD_JOBS=1`). Maintainers also mirror the PR subset on AWS when local broker containers are awkward.

Local quick gate (same as CI `fmt` / `clippy` / `deny`):

```bash
export CARGO_BUILD_JOBS=1
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo deny check
```

3. For broker-backed contracts, use the docker compose helpers under `infra/` when present, or rely on GitHub Actions (postgres/redis/nats services). Maintainers also run fleet contracts on AWS.

4. Confirm GitHub Actions `boson-matrix` is green on your branch (full postgres/redis/nats e2e runs there).

## Supply chain

Dependency and license policy lives in [`deny.toml`](deny.toml) and [`docs/supply-chain.md`](docs/supply-chain.md). The PR CI `deny` job must pass.

## Security

See [`SECURITY.md`](SECURITY.md) for private vulnerability reporting. Do not open public issues for exploitable bugs.
