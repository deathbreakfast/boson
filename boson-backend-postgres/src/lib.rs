//! `PostgreSQL` [`QueueBackend`](boson_core::QueueBackend) for Boson.
//!
//! **When to use:** shared durable state for Mode 1 or Mode 2 (enqueue hosts + worker binaries
//! against the same database). Enable via the `boson` crate `postgres` feature.
//!
//! Mode 2 workers should set a unique `worker_id` and `lease_ttl_secs > 0`. See the
//! [`boson`](https://docs.rs/uf-boson) crate
//! [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).
//!
//! ## Entry points
//!
//! - [`PostgresQueueBackend::connect`] — open a pool and bootstrap schema
//! - [`install_default_postgres_backend`] — register on the global [`QueueRouter`](boson_core::QueueRouter)
//!
//! ## Mode 2 — Enqueue binary
//!
//! Shared `DATABASE_URL` with the worker. No claim loop in this process:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use boson_backend_postgres::PostgresQueueBackend;
//! use boson_core::JsonExecutionContextFactory;
//! use boson_runtime::{configure, Boson};
//!
//! # async fn boot_enqueue() -> boson_core::Result<()> {
//! let url = std::env::var("DATABASE_URL")
//!     .unwrap_or_else(|_| "postgres://localhost/boson".into());
//! let backend = PostgresQueueBackend::connect(&url).await?;
//! let boson = Boson::builder()
//!     .queue_backend(Arc::new(backend))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .auto_registry()
//!     .without_worker()
//!     .build()?;
//! configure(boson);
//! // MyTask::send_with(...).await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## Mode 2 — Worker binary
//!
//! Same database URL, unique `worker_id`, and `lease_ttl_secs > 0`:
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use boson_backend_postgres::PostgresQueueBackend;
//! use boson_core::JsonExecutionContextFactory;
//! use boson_runtime::Boson;
//!
//! # async fn boot_worker() -> boson_core::Result<()> {
//! let url = std::env::var("DATABASE_URL")
//!     .unwrap_or_else(|_| "postgres://localhost/boson".into());
//! let backend = PostgresQueueBackend::connect(&url).await?;
//! let _boson = Boson::builder()
//!     .queue_backend(Arc::new(backend))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .worker_id(std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "worker-1".into()))
//!     .lease_ttl_secs(30)
//!     .auto_registry()
//!     .build()?;
//! # Ok(())
//! # }
//! ```
//!
//! Other Mode 2 backends:
//! [`SQLite`](../boson_backend_sqlite/index.html#mode-2--enqueue-binary),
//! [Redis](../boson_backend_redis/index.html#mode-2--enqueue-binary),
//! [NATS](../boson_backend_nats/index.html#mode-2--enqueue-binary).

mod bootstrap;

use boson_backend_sql_common::SqlQueueBackend;
use boson_core::Result;
use sqlx::PgPool;

pub use bootstrap::{
    install_default_postgres_backend, install_isolated_postgres_backend, postgres_test_url,
};

/// PostgreSQL-backed queue backend.
///
/// Mode 2 examples: [enqueue](index.html#mode-2--enqueue-binary) /
/// [worker](index.html#mode-2--worker-binary).
pub struct PostgresQueueBackend {
    inner: SqlQueueBackend,
}

impl PostgresQueueBackend {
    /// Connect to `PostgreSQL` at `url`.
    ///
    /// # Errors
    ///
    /// Returns an error when the pool cannot connect or schema bootstrap fails.
    pub async fn new(url: &str) -> Result<Self> {
        Self::connect(url).await
    }

    /// Connect using a `PostgreSQL` connection URL and wire into [`Boson`](https://docs.rs/boson-runtime).
    ///
    /// See crate-level [Mode 2 — Enqueue binary](index.html#mode-2--enqueue-binary) and
    /// [Mode 2 — Worker binary](index.html#mode-2--worker-binary).
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use boson_backend_postgres::PostgresQueueBackend;
    /// use boson_core::JsonExecutionContextFactory;
    /// use boson_runtime::Boson;
    ///
    /// # async fn connect() -> boson_core::Result<()> {
    /// let url = std::env::var("DATABASE_URL")
    ///     .unwrap_or_else(|_| "postgres://localhost/boson".into());
    /// let backend = PostgresQueueBackend::connect(&url).await?;
    /// let _boson = Boson::builder()
    ///     .queue_backend(Arc::new(backend))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .worker_id("worker-1")
    ///     .lease_ttl_secs(30) // Mode 2 multi-process
    ///     .auto_registry()
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the pool cannot connect or schema bootstrap fails.
    pub async fn connect(url: &str) -> Result<Self> {
        let inner = SqlQueueBackend::connect_postgres(url).await?;
        Ok(Self { inner })
    }

    /// Connect with an isolated schema (for parallel tests).
    ///
    /// # Errors
    ///
    /// Returns an error when schema creation, pool connect, or bootstrap fails.
    pub async fn connect_isolated(url: &str, schema: &str) -> Result<Self> {
        let inner = SqlQueueBackend::connect_postgres_isolated(url, schema).await?;
        Ok(Self { inner })
    }

    /// Wrap an existing pool (schema bootstrap runs).
    ///
    /// # Errors
    ///
    /// Returns an error when schema bootstrap fails.
    pub async fn from_pool(pool: PgPool) -> Result<Self> {
        let inner = SqlQueueBackend::from_postgres_pool(pool).await?;
        Ok(Self { inner })
    }

    /// Underlying connection pool.
    ///
    /// # Panics
    ///
    /// Panics if the inner pool is not `PostgreSQL` (internal invariant violation).
    #[must_use]
    pub fn pool(&self) -> &PgPool {
        match self.inner.pool() {
            boson_backend_sql_common::SqlPool::Postgres(pool) => pool,
            boson_backend_sql_common::SqlPool::Sqlite(_) => {
                panic!("postgres backend has non-postgres pool")
            }
        }
    }
}

impl std::fmt::Debug for PostgresQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresQueueBackend").finish_non_exhaustive()
    }
}

boson_backend_sql_common::delegate_queue_backend!(PostgresQueueBackend, inner);
