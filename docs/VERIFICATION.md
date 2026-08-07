# Documentation verification baseline

Re-run after test or CI changes. See the root [README.md](../README.md) verify section and
[CONTRIBUTING.md](../CONTRIBUTING.md).

## Remote CI (AWS)

Maintainers also mirror the PR subset on AWS (deny, clippy, crate tests, mem/sqlite e2e,
examples, docs) and run broker contracts against provisioned redis/nats fleets. Those
scripts are not shipped in this repository.

## What GitHub Actions runs (merge gate)

[`.github/workflows/boson-matrix.yml`](../.github/workflows/boson-matrix.yml) on every push/PR to `main`:

| Job | Purpose |
|-----|---------|
| `check` | `cargo check -p uf-boson --features mem` |
| `deny` | `cargo deny check` ([`deny.toml`](../deny.toml), [`docs/supply-chain.md`](supply-chain.md)) |
| `fmt` | `cargo fmt --all -- --check` |
| `clippy` | workspace clippy `-D warnings` (restriction lints: no unwrap/expect/println in product code) |
| crate jobs | testkit, mem, sqlite, sql-common, core, runtime/macros, telemetry, axum |
| `e2e` | postgres + redis + nats services; backend contracts + `boson-e2e --include-ignored` |
| `coverage` | non-blocking `cargo-llvm-cov` artifact |
| `bench-smoke` | BM-B0 / BM-B1 |
| `examples` / `docs` | public crate examples and rustdoc |
| `quality-review` | optional structural quality gate |

Scylla contract steps run only when secret `BOSON_TEST_SCYLLA_CONTACT_POINTS` is set.

## Commands (mirror of PR subset)

Use these on AWS via `run-remote-ci.sh`, or on any host with a Rust toolchain:

```bash
export CARGO_BUILD_JOBS=1

cargo check -p uf-boson --features mem
cargo deny check
cargo fmt --all -- --check
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
| Full | 32 | mem, sqlite, postgres, redis, nats (+ scylla if secret) | `--include-ignored` in PR `e2e` job |
| Catalog | 32 rows | includes security-hardening rows below | [`boson-testkit/src/scenario/catalog.rs`](../boson-testkit/src/scenario/catalog.rs) |

Smoke scenario ids: `enqueue_and_drain`, `enqueue_only`, `run_lifecycle`, `idempotency_smoke`.

### Security hardening campaign (L0)

| Layer | ID / assert | Where |
|-------|-------------|--------|
| Contract | `contract_claim_ignores_status_in_params` | `backend_contract_suite!` (all adapters) |
| Contract | `contract_try_claim_atomic`, `contract_try_claim_parallel`, lease contention/extend/expire | suite |
| Catalog | `cancel_running_job` | mid-run cooperative cancel → `Canceled` |
| Catalog | `long_job_lease_heartbeat_drain` | TTL 2s < sleep 5s; single success |
| Axum | admin 401/200, non-System actor, list clamp, retry cap | `boson-testkit/tests/axum_http_api.rs` |
| Unit | URL redact / `map_backend_connect_err` (happy + passworded sad) | `boson-core` `redact`, sql-common / redis / scylla `error_map` |
| Docs | Operator table: AdminAuth, ActorJsonPolicy, sanitize, URL credentials | [`SECURITY.md`](../SECURITY.md) |
| AWS | Redis/NATS/Scylla e2e on provisioned fleets (contracts + `scenarios_full` filters) | maintainer out-of-tree |

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

Fleet routing (Redis/NATS dual-broker) and Scylla cloud campaigns are run on AWS by maintainers; those scripts are not shipped in this repository.

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
`profiling/boson-bench/reports/` (see [`boson-bench/PERFORMANCE.md`](../boson-bench/PERFORMANCE.md)).

## Backend contract suites

Each adapter expands `backend_contract_suite!` (13 checks, including
`claim_ignores_status_in_params` and `try_claim_parallel`). See [`boson-testkit`](../boson-testkit/README.md).
