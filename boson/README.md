[![crates.io](https://img.shields.io/crates/v/uf-boson.svg)](https://crates.io/crates/uf-boson)
[![docs.rs](https://docs.rs/uf-boson/badge.svg)](https://docs.rs/uf-boson)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](../LICENSE-MIT)

# boson (`uf-boson` on crates.io)

Main crate — re-exports core types, runtime, optional backends, and the `#[task]` macro.

The crates.io package is **`uf-boson`** (`boson` is already taken). With `[lib] name = "boson"`,
imports stay `use boson::…`.

**Source of truth:** `cargo doc -p uf-boson --features mem,axum --open` — guided get-started with
[Embedded](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary) and
[Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).
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

## How to run examples

Navigational index: [`examples/README.md`](examples/README.md) (when-to-use ladder, host-mount sketches, success checks).

Canonical teaching path (start here). Topology docs:
[Embedded](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary) /
[Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

### 1. Embedded — `task_macro` (standalone)

One process, in-memory backend. No external services.

```bash
cargo run -p uf-boson --example task_macro --features mem
```

Success: stdout prints `greet world (actor=…)`.

### 2. Remote worker — SQLite (multi-process — run as a set)

Enqueue host and workers share one database file. They are **not** useful alone.

| Rule | Detail |
|------|--------|
| Shared env | Same `BOSON_SQLITE_PATH` on every process |
| Start order | Worker(s) first, then enqueue |
| Workers | Each needs a unique `BOSON_WORKER_ID`; `lease_ttl_secs > 0` (default 30 in the example) |
| Stop | Ctrl-C on each worker (or set `BOSON_WORKER_RUN_SECS` for scripted smoke) |

**Local SQLite** — 1–2 workers + enqueue:

```bash
export BOSON_SQLITE_PATH=/tmp/boson-remote.db

# Terminal 1 — worker A
BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example remote_worker --features sqlite

# Terminal 2 — worker B (optional)
BOSON_WORKER_ID=worker-b cargo run -p uf-boson --example remote_worker --features sqlite

# Terminal 3 — enqueue
cargo run -p uf-boson --example remote_enqueue --features sqlite
```

### 3. Remote worker — Postgres (multi-process — run as a set)

Same pattern against a shared database URL (production-shaped durable backend).

| Rule | Detail |
|------|--------|
| Shared env | Same `DATABASE_URL` (or `BOSON_POSTGRES_URL`) on every process |
| Start order | Worker(s) first, then enqueue |
| Workers | Unique `BOSON_WORKER_ID`; positive lease TTL |

```bash
export DATABASE_URL=postgres://localhost/boson

# Terminal 1 — worker A
BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example postgres_worker --features postgres

# Terminal 2 — worker B (optional)
BOSON_WORKER_ID=worker-b cargo run -p uf-boson --example postgres_worker --features postgres

# Terminal 3 — enqueue
cargo run -p uf-boson --example postgres_enqueue --features postgres
```

Stop with Ctrl-C on each worker. Real apps put `#[task]` handlers in a shared crate and
`use my_tasks as _;` from the worker binary.

### Other examples

| Example | Topology | Features | Notes |
|---------|----------|----------|-------|
| `minimal_enqueue` | Embedded | `mem` | Manual registry + `Boson::enqueue` |
| `idempotency_and_rate_limit` | Embedded | `mem` | Idempotency key + `max_in_flight` |
| `axum_admin` | Embedded + HTTP admin | `mem,axum` | Nest `/api/boson`; `BOSON_EXAMPLE_SERVE=1` to listen |

**Production:** Boson does not authenticate `/api/boson/*` by itself. Install host
[`AdminAuth`](https://docs.rs/uf-boson/latest/boson/trait.AdminAuth.html) and prefer
`BOSON_REQUIRE_ADMIN_AUTH=1` — see repository [`SECURITY.md`](../SECURITY.md).

## Boot a worker (embedded)

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
