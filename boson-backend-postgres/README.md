# boson-backend-postgres

[`PostgreSQL`](https://www.postgresql.org/) [`QueueBackend`](https://github.com/unified-field-dev/boson) for Boson jobs and runs.

## Role

Thin wrapper around [`boson-backend-sql-common`](https://docs.rs/boson-backend-sql-common) `SqlQueueBackend`.

## Compose

```rust
use std::sync::Arc;

use boson::{Boson, PostgresQueueBackend};

let backend = Arc::new(PostgresQueueBackend::connect("postgres://localhost/boson").await?);
let boson = Boson::builder()
    .queue_backend(backend)
    .execution_context_factory(your_factory)
    .build()?;
```

## Bootstrap

```rust
use boson_backend_postgres::install_default_postgres_backend;
use boson_core::default_backend_from_global;

let _backend = install_default_postgres_backend("postgres://localhost/boson").await?;
let resolved = default_backend_from_global()?;
```

Isolated schema for parallel tests:

```rust
use boson_backend_postgres::install_isolated_postgres_backend;

let _backend = install_isolated_postgres_backend(url, "test_schema_1").await?;
```

## Environment

Postgres URL resolution for tests: `BOSON_TEST_POSTGRES_URL`, then `BOSON_BENCH_POSTGRES_URL`.

## Enable via `boson`

Available via the `boson` crate feature `postgres`.

## Related crates

- [`boson`](https://docs.rs/uf-boson) — enable with `features = ["postgres"]`
- [`boson-backend-sql-common`](https://docs.rs/boson-backend-sql-common) — shared SQL implementation
- [`boson-core`](https://docs.rs/boson-core) — `QueueBackend` trait definition
