//! `SQLite` [`QueueBackend`](boson_core::QueueBackend) for Boson.
//!
//! Choose at worker boot (**Integrating the server**). Enable via the `boson` crate `sqlite` feature.
//!
//! ## Entry points
//!
//! - [`SqliteQueueBackend::new`] / [`SqliteQueueBackend::connect`] — open a database
//! - [`install_default_sqlite_backend`] — register on the global [`QueueRouter`](boson_core::QueueRouter)

mod bootstrap;

use std::path::Path;

use boson_backend_sql_common::SqlQueueBackend;
use boson_core::Result;
use sqlx::SqlitePool;

pub use bootstrap::install_default_sqlite_backend;

/// SQLite-backed queue backend.
pub struct SqliteQueueBackend {
    inner: SqlQueueBackend,
}

impl SqliteQueueBackend {
    /// Open a `SQLite` database at `path` (creates the file if missing).
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
    /// # Panics
    ///
    /// Panics if the inner pool is not `SQLite` (internal invariant violation).
    #[must_use]
    pub fn pool(&self) -> &SqlitePool {
        match self.inner.pool() {
            boson_backend_sql_common::SqlPool::Sqlite(pool) => pool,
            boson_backend_sql_common::SqlPool::Postgres(_) => {
                panic!("sqlite backend has non-sqlite pool")
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
