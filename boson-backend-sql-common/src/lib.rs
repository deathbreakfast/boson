//! Shared SQL [`QueueBackend`](boson_core::QueueBackend) for `PostgreSQL` and `SQLite`.
//!
//! Reference implementation for **Developing the backend**; integrators typically use
//! [`boson_backend_sqlite::SqliteQueueBackend`](https://docs.rs/boson-backend-sqlite) or
//! [`boson_backend_postgres::PostgresQueueBackend`](https://docs.rs/boson-backend-postgres).
//!
//! ## Entry points
//!
//! - [`SqlQueueBackend`] — connect, schema bootstrap, and trait implementation
//! - [`SqlDialect`] / [`SqlPool`] — engine selection and pool wrapper
//!
//! ## Example (integrator wiring)
//!
//! ```rust,no_run
//! use boson_backend_sql_common::SqlQueueBackend;
//!
//! # async fn example() -> boson_core::Result<()> {
//! let backend = SqlQueueBackend::connect_sqlite("sqlite://:memory:").await?;
//! # Ok(())
//! # }
//! ```

mod backend;
mod delegate;
mod enqueue_rate;
mod error_map;
mod jobs;
mod leases;
mod macros;
mod queue_impl;
mod row;
mod runs;
mod schema;
mod task_config;

pub use backend::{SqlDialect, SqlPool, SqlQueueBackend};
pub use enqueue_rate::EnqueueRateLimiter;

pub(crate) use backend::bind_sql;
