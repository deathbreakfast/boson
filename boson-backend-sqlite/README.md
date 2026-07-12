# boson-backend-sqlite

Embedded [`SQLite`](https://www.sqlite.org/) [`QueueBackend`](https://github.com/unified-field-dev/boson) for Boson jobs and runs.

## Role

Thin wrapper around [`boson-backend-sql-common`](https://docs.rs/boson-backend-sql-common) `SqlQueueBackend`.

## Compose

```rust
use std::sync::Arc;

use boson::{Boson, SqliteQueueBackend};

let backend = Arc::new(SqliteQueueBackend::new("boson.db").await?);
let boson = Boson::builder()
    .queue_backend(backend)
    .execution_context_factory(your_factory)
    .build()?;
```

## Bootstrap

```rust
use boson_backend_sqlite::install_default_sqlite_backend;
use boson_core::default_backend_from_global;

let _backend = install_default_sqlite_backend("boson.db").await?;
let resolved = default_backend_from_global()?;
```

## Enable via `boson`

Available via the `boson` crate feature `sqlite`.

## Related crates

- [`boson`](https://docs.rs/boson) — enable with `features = ["sqlite"]`
- [`boson-backend-sql-common`](https://docs.rs/boson-backend-sql-common) — shared SQL implementation
- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
