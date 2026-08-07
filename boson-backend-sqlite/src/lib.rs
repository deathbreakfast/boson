//! `SQLite` [`QueueBackend`](boson_core::QueueBackend) for Boson.
//!
//! **When to use:** durable single-host embedded boots, or remote worker on one machine when
//! enqueue and worker processes share the same database file (`BOSON_SQLITE_PATH`). Enable via the
//! `boson` crate `sqlite` feature.
//!
//! Getting started:
//! [Embedded](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary) /
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).
//!
//! ## Entry points
//!
//! - [`SqliteQueueBackend::new`] / [`SqliteQueueBackend::connect`] — open a database
//! - [`install_default_sqlite_backend`] — register on the global [`QueueRouter`](boson_core::QueueRouter)
//!
//! ## Remote worker — Enqueue binary
//!
//! Shared file path with the worker. No claim loop in this process:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_backend_sqlite::SqliteQueueBackend;
//! use boson_core::JsonExecutionContextFactory;
//! use boson::{configure, Boson};
//!
//! # async fn boot_enqueue() -> boson_core::Result<()> {
//! let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
//! let backend = SqliteQueueBackend::new(&path).await?;
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
//! Runnable: `cargo run -p uf-boson --example remote_enqueue --features sqlite`
//!
//! ## Remote worker — Worker binary
//!
//! Same `BOSON_SQLITE_PATH`, unique `worker_id`, and `lease_ttl_secs > 0`:
//!
//! ```rust,ignore
//! use std::sync::Arc;
//!
//! use boson_backend_sqlite::SqliteQueueBackend;
//! use boson_core::JsonExecutionContextFactory;
//! use boson::Boson;
//!
//! # async fn boot_worker() -> boson_core::Result<()> {
//! let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
//! let backend = SqliteQueueBackend::new(&path).await?;
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
//! Runnable: `cargo run -p uf-boson --example remote_worker --features sqlite`
//!
//! Other remote-worker backends:
//! [Postgres](../boson_backend_postgres/index.html#remote-worker--enqueue-binary),
//! [Redis](../boson_backend_redis/index.html#remote-worker--enqueue-binary),
//! [NATS](../boson_backend_nats/index.html#remote-worker--enqueue-binary).

mod bootstrap;

use std::path::Path;

use boson_backend_sql_common::SqlQueueBackend;
use boson_core::{BosonError, Result};
use sqlx::SqlitePool;

pub use bootstrap::install_default_sqlite_backend;

/// `SQLite`-backed queue backend.
///
/// Suitable for embedded boots and for remote worker when both binaries open the **same path**.
/// For multi-host fleets prefer Postgres, Redis, or NATS.
///
/// Remote-worker examples: [enqueue](index.html#remote-worker--enqueue-binary) /
/// [worker](index.html#remote-worker--worker-binary).
pub struct SqliteQueueBackend {
    inner: SqlQueueBackend,
}

impl SqliteQueueBackend {
    /// Open a `SQLite` database at `path` (creates the file if missing).
    ///
    /// See crate-level [Remote worker — Enqueue binary](index.html#remote-worker--enqueue-binary) and
    /// [Remote worker — Worker binary](index.html#remote-worker--worker-binary).
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use std::sync::Arc;
    ///
    /// use boson_backend_sqlite::SqliteQueueBackend;
    /// use boson_core::JsonExecutionContextFactory;
    /// use boson::Boson;
    ///
    /// # async fn boot() -> boson_core::Result<()> {
    /// let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson.db".into());
    /// let backend = SqliteQueueBackend::new(&path).await?;
    /// let _boson = Boson::builder()
    ///     .queue_backend(Arc::new(backend))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .auto_registry()
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the database cannot be opened or schema bootstrap fails.
    pub async fn new(path: impl AsRef<Path>) -> Result<Self> {
        let url = format!("sqlite://{}?mode=rwc", path.as_ref().display());
        Self::connect(&url).await
    }

    /// Connect using a `SQLite` connection URL.
    ///
    /// # Errors
    ///
    /// Returns an error when the pool cannot connect or schema bootstrap fails.
    pub async fn connect(url: &str) -> Result<Self> {
        let inner = SqlQueueBackend::connect_sqlite(url).await?;
        Ok(Self { inner })
    }

    /// Wrap an existing pool (schema bootstrap runs).
    ///
    /// # Errors
    ///
    /// Returns an error when schema bootstrap fails.
    pub async fn from_pool(pool: SqlitePool) -> Result<Self> {
        let inner = SqlQueueBackend::from_sqlite_pool(pool).await?;
        Ok(Self { inner })
    }

    /// Underlying connection pool.
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::Internal`] if the inner pool is not `SQLite`
    /// (internal invariant violation).
    pub fn pool(&self) -> Result<&SqlitePool> {
        match self.inner.pool() {
            boson_backend_sql_common::SqlPool::Sqlite(pool) => Ok(pool),
            boson_backend_sql_common::SqlPool::Postgres(_) => {
                Err(BosonError::internal("sqlite backend has non-sqlite pool"))
            }
        }
    }
}

impl std::fmt::Debug for SqliteQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SqliteQueueBackend").finish_non_exhaustive()
    }
}

boson_backend_sql_common::delegate_queue_backend!(SqliteQueueBackend, inner);
