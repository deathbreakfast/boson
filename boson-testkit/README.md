# boson-testkit

Shared matrix dimensions, scenario catalog, contract suite, and bootstrap fixtures for [`boson-e2e`](../boson-e2e/README.md) and [`boson-bench`](../boson-bench/README.md).

## Role

- [`ScenarioRunner`](src/runner.rs) — execute [`ScenarioSpec`](src/scenario/) steps with timing
- [`BootstrapSession`](src/bootstrap.rs) — install a [`MatrixSpec`](src/matrix.rs) backend + telemetry
- [`correctness_catalog`](src/scenario/catalog.rs) — happy/sad scenarios for **all** storage backends
- [`backend_contract_suite!`](src/macros.rs) — expand every `QueueBackend` contract for an adapter crate
- [`matrix_scenario_suite!`](src/macros.rs) / [`matrix_smoke_suite!`](src/macros.rs) — emit `{scenario}_{backend}` tests

Both e2e and bench run the **same** scenario definitions — e2e asserts correctness, bench records timings.

## Adding a storage backend

In-tree (workspace member):

1. Add `BackendAdapter::Foo` and a [`BootstrapSession::install`](src/bootstrap.rs) arm (how to connect).
2. Append to [`e2e_storage_backends()`](src/matrix.rs) (and [`smoke_storage_backends()`](src/matrix.rs) only if it needs no external service).
3. In the adapter crate’s `tests/`:

```rust
async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    // Return None when service env is unset (CI-friendly skip).
    ...
}
boson_testkit::backend_contract_suite!(
    fresh,
    "foo",
    ignore = "requires BOSON_TEST_FOO — run with --include-ignored"
);
```

No per-scenario test code. The full happy/sad catalog applies automatically via `matrix_scenario_suite!` in `boson-e2e`.

Out-of-tree adapters can call [`BootstrapSession::install_backend`](src/bootstrap.rs) with a pre-built `Arc<dyn QueueBackend>`, then [`run_catalog_entry`](src/scenario/catalog.rs), or contribute a `BackendAdapter` arm upstream.

## Pools

`pool` on jobs / task config is an **opaque string**. Boson partitions the ready queue by that label and orders by priority within a pool. It does **not** model hardware profiles (e.g. t3.medium). Mapping instance type → pool name is product/host wiring and belongs in application integration tests.

## PR CI enablement (workflows later)

| Tier | Command | Backends |
|------|---------|----------|
| Smoke | `cargo test -p boson-e2e` | `smoke_storage_backends()` = mem, sqlite |
| Full matrix | `cargo test -p boson-e2e -- --include-ignored` + env | `e2e_storage_backends()` |
| Contract | `cargo test -p boson-backend-<x> -- --include-ignored` + env | that adapter |

Unset service env → **skip**, not fail. Stable filters: `scylla`, `postgres`.

## Build

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-boson-extract
cargo test -p boson-testkit
```

## Related crates

- [`boson-e2e`](../boson-e2e/README.md) — matrix correctness tests
- [`boson-bench`](../boson-bench/README.md) — performance CLI
- [`boson-backend-mem`](../boson-backend-mem/README.md) — default mem backend
