# boson-backend-scylla

ScyllaDB [`QueueBackend`](https://docs.rs/boson-core) for Boson.

## Local builds

Prefer a **single** Cargo job on resource-constrained hosts (parallel builds can OOM or stall):

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-scylla
cargo build -p boson-backend-scylla
```

Do **not** run multi-worker Scylla E2E against a local multi-node Docker cluster. Use a CI service or cloud contact points instead.

## Connect

```rust,ignore
use boson_backend_scylla::{ScyllaQueueBackend, ScyllaQueueConfig};

let backend = ScyllaQueueBackend::connect(ScyllaQueueConfig {
    contact_points: vec!["127.0.0.1:9042".into()],
    keyspace: "boson".into(),
    ready_shard_count: 256,
    shard_concurrency: 32,
    parallel_writes: true,
    pool_per_shard: None,
    ..Default::default()
}).await?;
```

## Contract tests

Uses the shared [`boson-testkit::backend_contract_suite!`](https://docs.rs/boson-testkit) harness (same checks as sqlite/postgres).

```bash
export CARGO_BUILD_JOBS=1
export BOSON_TEST_SCYLLA_CONTACT_POINTS=10.0.0.1:9042
cargo test -p boson-backend-scylla -- --ignored
```

Scenario matrix (happy/sad): `cargo test -p boson-e2e -- --include-ignored scylla` with the same env var.
