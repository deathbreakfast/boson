//! `PostgreSQL` [`QueueBackend`](boson_core::QueueBackend) for Boson.
//!
//! Choose at worker boot. Enable via the `boson` crate `postgres` feature.
//!
//! ## Entry points
//!
//! - [`PostgresQueueBackend::connect`] — open a pool and bootstrap schema
//! - [`install_default_postgres_backend`] — register on the global [`QueueRouter`](boson_core::QueueRouter)

mod bootstrap;

use boson_backend_sql_common::SqlQueueBackend;
use boson_core::Result;
use sqlx::PgPool;

pub use bootstrap::{
    install_default_postgres_backend, install_isolated_postgres_backend, postgres_test_url,
};

/// PostgreSQL-backed queue backend.
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

    /// Connect using a `PostgreSQL` connection URL.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use boson_backend_postgres::PostgresQueueBackend;
    ///
    /// # async fn connect() -> boson_core::Result<()> {
    /// let backend = PostgresQueueBackend::connect("postgres://localhost/boson").await?;
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
