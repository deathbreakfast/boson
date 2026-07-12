# boson-backend-sql-common

Shared SQL [`QueueBackend`](https://github.com/unified-field-dev/boson) implementation for `SQLite` and `PostgreSQL`.

## Role

- [`SqlQueueBackend`](src/backend.rs) — connect, schema bootstrap, and trait implementation
- [`SqlDialect`](src/backend.rs) / [`SqlPool`](src/backend.rs) — engine selection
- Row mapping helpers in `row.rs`
- Used by [`boson-backend-sqlite`](https://docs.rs/boson-backend-sqlite) and [`boson-backend-postgres`](https://docs.rs/boson-backend-postgres)

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

## Related crates

- [`boson-backend-sqlite`](https://docs.rs/boson-backend-sqlite) — thin SQLite wrapper
- [`boson-backend-postgres`](https://docs.rs/boson-backend-postgres) — thin PostgreSQL wrapper
- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
