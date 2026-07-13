# Documentation verification baseline

Re-run after test or CI changes. See the root [README.md](../README.md) verify section and
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Remote CI (native-aws)

Mirror the PR subset on a provisioned bench host (deny, clippy, crate tests, mem/sqlite e2e,
examples, docs):

```bash
./infra/native-aws/scripts/run-remote-ci.sh
```

Broker contracts against a provisioned fleet:

```bash
./infra/native-aws/scripts/run-redis-e2e.sh
./infra/native-aws/scripts/run-nats-e2e.sh
```

See [`infra/native-aws/README.md`](../infra/native-aws/README.md).

## What GitHub Actions runs (merge gate)

[`.github/workflows/boson-matrix.yml`](../.github/workflows/boson-matrix.yml) on every push/PR to `main`:

| Job | Purpose |
|-----|---------|
| `check` | `cargo check -p uf-boson --features mem` |
| `deny` | `cargo deny check` ([`deny.toml`](../deny.toml), [`docs/supply-chain.md`](supply-chain.md)) |
| `clippy` | workspace clippy `-D warnings` |
| crate jobs | testkit, mem, sqlite, sql-common, core, runtime/macros, telemetry, axum |
| `e2e` | postgres + redis + nats services; backend contracts + `boson-e2e --include-ignored` |
| `coverage` | non-blocking `cargo-llvm-cov` artifact |
| `bench-smoke` | BM-B0 / BM-B1 |
| `examples` / `docs` | facade examples and rustdoc |
| `sentrux` | optional structural quality gate |

Scylla contract steps run only when secret `BOSON_TEST_SCYLLA_CONTACT_POINTS` is set.

## Commands (mirror of PR subset)

Use these on AWS via `run-remote-ci.sh`, or on any host with a Rust toolchain:

```bash
export CARGO_BUILD_JOBS=1

cargo check -p uf-boson --features mem
cargo deny check
cargo clippy --workspace --all-targets -- -D warnings

cargo test -p boson-testkit
cargo test -p boson-backend-mem
cargo test -p boson-backend-sqlite
cargo test -p boson-backend-sql-common
cargo test -p boson-core
cargo test -p boson-runtime
cargo test -p boson-macros
cargo test -p boson-telemetry
cargo test -p boson-e2e -- --test-threads=1
cargo test -p boson-axum

RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps
cargo test --doc -p boson-core
cargo test --doc -p boson-runtime
cargo test --doc -p boson-backend-mem
cargo test --doc -p uf-boson --features mem
cargo test --doc -p boson-telemetry

cargo run -p uf-boson --example minimal_enqueue --features mem
cargo run -p uf-boson --example task_macro --features mem
cargo run -p uf-boson --example idempotency_and_rate_limit --features mem
cargo run -p uf-boson --example axum_admin --features mem,axum

cargo run -p boson-bench -- experiments
cargo run -p boson-bench -- run --experiment bm-b0 --backend mem --topology isolated-lab --telemetry off --ops 1000
```

## E2E matrix

| Suite | Scenarios | Backends on PR | Notes |
|-------|-----------|----------------|-------|
| Smoke | 4 | mem, sqlite (also covered inside full) | `scenarios_smoke.rs` |
| Full | 30 | mem, sqlite, postgres, redis, nats (+ scylla if secret) | `--include-ignored` in PR `e2e` job |
| Catalog | 30 rows | 15 Happy / 15 Sad | [`boson-testkit/src/scenario/catalog.rs`](../boson-testkit/src/scenario/catalog.rs) |

Smoke scenario ids: `enqueue_and_drain`, `enqueue_only`, `run_lifecycle`, `idempotency_smoke`.

### Full broker env (GHA services or local docker)

```bash
export BOSON_TEST_POSTGRES_URL=postgres://boson:bench@127.0.0.1:5433/boson_bench
export BOSON_TEST_REDIS_URL=redis://127.0.0.1:6379
export BOSON_TEST_NATS_URL=nats://127.0.0.1:4222
export BOSON_NATS_STREAM_REPLICAS=1
export BOSON_NATS_QUEUE_MODE=workqueue

cargo test -p boson-backend-postgres -- --include-ignored
cargo test -p boson-backend-redis -- --ignored --test-threads=1
cargo test -p boson-backend-nats -- --ignored --test-threads=1
cargo test -p boson-e2e -- --include-ignored --test-threads=1
```

Fleet routing (Redis/NATS dual-broker) and Scylla cloud campaigns: [`infra/native-aws/scripts/`](../infra/native-aws/scripts/).

## Line coverage (CI artifact)

PR CI runs a non-blocking `coverage` job with `cargo-llvm-cov`:

```bash
./scripts/coverage.sh
./scripts/coverage.sh --lcov
./scripts/coverage.sh --full   # includes boson-e2e when services are up
```

Baseline ~40% line coverage (2026-07-08), workspace excluding e2e/bench with `--features mem`.

## Benchmark artifacts

Keep harness code, experiment IDs, and methodology docs in Git. Publish heavy JSON reports,
flamegraphs, and machine-specific captures as CI artifacts or release assets. Retain only small
golden fixtures when needed for assertions. Report directory:
`profiling/boson-bench/reports/` (see [`boson-bench/EXPERIMENTS.md`](../boson-bench/EXPERIMENTS.md)).

## Backend contract suites

Each adapter expands `backend_contract_suite!` (11 checks). See [`boson-testkit`](../boson-testkit/README.md).
