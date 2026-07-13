# boson-backend-mem

In-memory [`QueueBackend`](https://github.com/unified-field-dev/boson) adapter for tests, CI, and local development.

## Role

Implements `QueueBackend` using portable DTOs from `boson-core` — no network I/O or host-specific persistence.

Used by:

- `boson-testkit` default `BootstrapSession`
- Inline integration tests and `boson-e2e` CI slice
- `boson-bench` `backend=mem` campaigns

## Compose

```rust
use std::sync::Arc;

use boson::{Boson, MemQueueBackend};

let backend = Arc::new(MemQueueBackend::new());
let boson = Boson::builder()
    .queue_backend(backend)
    .execution_context_factory(your_factory)
    .build()?;
```

## Bootstrap

```rust
use boson_backend_mem::install_default_mem_backend;
use boson_core::default_backend_from_global;

let _backend = install_default_mem_backend();
let resolved = default_backend_from_global()?;
```

## Enable via `boson`

Available via the `boson` crate feature `mem` (forwards this adapter).

## Related crates

- [`boson`](https://docs.rs/uf-boson) — enable with `features = ["mem"]`
- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
