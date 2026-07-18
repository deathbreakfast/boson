[![crates.io](https://img.shields.io/crates/v/uf-boson.svg)](https://crates.io/crates/uf-boson)
[![docs.rs](https://docs.rs/uf-boson/badge.svg)](https://docs.rs/uf-boson)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE-MIT)

# boson (`uf-boson` on crates.io)

Main crate — re-exports core types, runtime, optional backends, and the `#[task]` macro.

The crates.io package is **`uf-boson`** (`boson` is already taken). With `[lib] name = "boson"`,
imports stay `use boson::…`.

**Source of truth:** `cargo doc -p uf-boson --features mem,axum --open` — guided get-started with
[Mode 1 (embedded)](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary) and
[Mode 2 (remote worker)](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).
Published docs: https://docs.rs/uf-boson

## Role

- [`task`](https://docs.rs/boson-macros) — `#[task]` macro and typed `send_with`
- [`Boson`](https://docs.rs/boson-runtime) / [`BosonBuilder`](https://docs.rs/boson-runtime) — worker boot
- Feature-gated backends: `mem`, `sqlite`, `postgres`, `axum`, `telemetry-console`
- Fleet backends: [`boson-backend-redis`](https://docs.rs/boson-backend-redis), [`boson-backend-nats`](https://docs.rs/boson-backend-nats)
- [`prelude`](https://docs.rs/uf-boson/latest/boson/prelude/index.html) — common re-exports

## Cargo features

| Feature | Enables |
|---------|---------|
| `mem` | `MemQueueBackend` and bootstrap helpers |
| `sqlite` | `SqliteQueueBackend` and bootstrap helpers |
| `postgres` | `PostgresQueueBackend` and bootstrap helpers |
| `telemetry-console` | `ConsoleOpsLog` (always available via re-export) |
| `axum` | HTTP admin router and state types |

This crate ships with **no default features** (`default = []`).

## Boot a worker (Mode 1)

```toml
[dependencies]
boson = { package = "uf-boson", version = "0.1.1", features = ["mem"] }
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

```rust
use std::sync::Arc;

use boson::{configure, task, Boson, ExecutionContext, JsonExecutionContextFactory, MemQueueBackend};

#[task(name = "my_task")]
async fn my_task(ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
    let _ = ctx;
    Ok(())
}

let boson = Boson::builder()
    .queue_backend(Arc::new(MemQueueBackend::new()))
    .execution_context_factory(JsonExecutionContextFactory)
    .auto_registry()
    .build()?;
configure(boson);
```

With HTTP admin: `features = ["mem", "axum"]`. Full walkthrough: crate rustdoc Getting started and
[`task_macro`](https://github.com/unified-field-dev/boson/blob/main/boson/examples/task_macro.rs).

Mode 2 (enqueue host + worker): `remote_enqueue` / `remote_worker` examples (`--features sqlite`).

## Define handlers and enqueue

After boot, add handlers with `#[task]` and enqueue with `<TaskName>::send_with(...)`. See
[`boson-macros`](https://docs.rs/boson-macros) for policy attributes.

## Configuration precedence

| Layer | Resolution order |
|-------|------------------|
| Worker settings | `BosonBuilder` field → env var → default |
| Task config at enqueue | Persisted backend config → macro/descriptor defaults |
| Idempotency mode | Per-task override → runtime builder default |
| Queue backend | Explicit `queue_backend()` → global router |
| Ops log | Builder `ops_log()` → `NoOpsLog`; or `ops_log_from_env()` |
| Fleet URLs (Redis/NATS) | `BOSON_*_POOL_ROUTING` → `BOSON_*_URLS` |

## Related crates

- [`boson-macros`](https://docs.rs/boson-macros) — `#[boson::task]` proc macro
- [`boson-runtime`](https://docs.rs/boson-runtime) — worker runtime and builder
- [`boson-core`](https://docs.rs/boson-core) — shared types and `QueueBackend` trait
- [`boson-axum`](https://docs.rs/boson-axum) — HTTP admin API
