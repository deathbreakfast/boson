# boson-runtime

Worker runtime, builder, and enqueue orchestration — **Integrating the server**.

Wire the worker process here once; task authors add handlers via [`boson-macros`](https://docs.rs/boson-macros)
without repeating boot steps.

## Role

- [`Boson`](src/boson.rs) / [`BosonBuilder`](src/builder.rs) — inject `QueueBackend`, identity factory, and `OpsLog`
- Background worker loop with lease-based claim and dispatch
- [`configure`](src/global.rs) / [`default`](src/global.rs) — process-wide default required for macro `send_with` (once at boot)
- [`TaskRegistry`](src/registry/mod.rs) — auto-discovery via `auto_registry` or manual registration in tests

## Boot with `#[task]` (once per worker)

```rust
use std::sync::Arc;

use boson_core::JsonExecutionContextFactory;
use boson_backend_mem::MemQueueBackend;
use boson_runtime::{configure, Boson};

let boson = Boson::builder()
    .queue_backend(Arc::new(MemQueueBackend::new()))
    .execution_context_factory(JsonExecutionContextFactory)
    .auto_registry()
    .build()?;
configure(boson);
```

Link every crate that defines `#[boson::task]` handlers into the worker binary. See
[`boson`](https://docs.rs/boson) **Integrating the server** and [`task_macro`](https://github.com/unified-field-dev/boson/blob/main/boson/examples/task_macro.rs).

## Manual task registration (tests)

```rust
use boson_runtime::{TaskDescriptor, TaskRegistry};

let mut registry = TaskRegistry::new();
let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::new("my_task", invoke_fn)));
registry.register(desc);
```

## Related crates

- [`boson`](https://docs.rs/boson) — main crate and documentation map
- [`boson-macros`](https://docs.rs/boson-macros) — task attribute macro
- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait, identity traits
- [`boson-telemetry`](https://docs.rs/boson-telemetry) — `OpsLog` adapters
