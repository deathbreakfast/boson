# boson-e2e

Matrix correctness integration tests over shared [`boson-testkit`](../boson-testkit/README.md) scenarios.

## Role

Answers: *Does the same Boson contract hold across backend × topology × telemetry?*

Uses `ScenarioRunner` from testkit — same `ScenarioSpec` steps as [`boson-bench`](../boson-bench/README.md), with assertions instead of timers. The **happy/sad catalog** lives in testkit (`correctness_catalog`); this crate only expands it via `matrix_scenario_suite!` / `matrix_smoke_suite!`.

## CI tiers

| Tier | Trigger | Command | Backends |
|------|---------|---------|----------|
| **Full matrix** | push/PR to `main` | `cargo test -p boson-e2e -- --include-ignored` + services | mem, sqlite, postgres, redis, nats (+ scylla if secret) |
| **AWS remote** | maintainer | `~/aws/boson/run-remote-ci.sh` | mem, sqlite (broker fleets via separate scripts) |
| **HTTP** | push/PR | `cargo test -p boson-axum` | route integration tests |

### Smoke helpers

- Files: [`tests/scenarios_smoke.rs`](tests/scenarios_smoke.rs), [`tests/matrix_smoke.rs`](tests/matrix_smoke.rs)
- Still useful for a fast local/AWS subset; PR CI runs the full ignored matrix with services.

### Full matrix (service-gated)

All entries in [`tests/scenarios_full.rs`](tests/scenarios_full.rs) are `#[ignore]` until `--include-ignored`. Unset service env **skips** (no fail):

| Backend | Env gate |
|---------|----------|
| postgres | `BOSON_TEST_POSTGRES_URL` or `BOSON_BENCH_POSTGRES_URL` |
| redis | `BOSON_TEST_REDIS_URL` |
| nats | `BOSON_TEST_NATS_URL` |
| scylla | `BOSON_TEST_SCYLLA_CONTACT_POINTS` (one or more `host:port`) |

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-e2e
export BOSON_TEST_POSTGRES_URL=postgres://...
export BOSON_TEST_REDIS_URL=redis://...
export BOSON_TEST_NATS_URL=nats://...
cargo test -p boson-e2e -- --include-ignored --test-threads=1
# or filter one backend:
cargo test -p boson-e2e -- --include-ignored scylla
```

Stable test names: `{scenario}_{backend}` (e.g. `enqueue_and_drain_scylla`).

Scylla-only alias: `cargo test -p boson-e2e --test scylla_cloud -- --ignored`.

Do **not** run multi-worker Scylla against a local multi-node Docker cluster (can OOM or stall the host). Linux CI runners and cloud hosts are fine.

## Build

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-e2e
cargo test -p boson-e2e
```

## Coverage map

| Area | Happy | Sad |
|------|-------|-----|
| Enqueue / drain | enqueue, multi-job, enqueue-only | unknown task |
| Rate limits | (under default limits via drain) | in-flight, EPS, task-config override |
| Idempotency | smoke, reuse while queued | after terminal; `None` mode allows dups |
| Handler / retry | run lifecycle, retry then success | terminal fail, retry exhaustion |
| Cancel | cancel queued | cancel missing (`JobNotFound`) |
| Priority | pool priority drain (within a pool) | — |
| Leases | split drain, multi-job split, restart | lease contention |
| Admin | list/count | get missing job |
| Telemetry | console boot + drain | — |

**Pools** are opaque routing labels in Boson (not hardware profiles). Product hosts wire instance type → pool name; that wiring is out of scope here.

## Related crates

- [`boson-testkit`](../boson-testkit/README.md) — catalog, contract suite, bootstrap
- [`boson-bench`](../boson-bench/README.md) — same scenarios, performance timings
- [`boson-axum`](../boson-axum/) — HTTP admin API integration tests
