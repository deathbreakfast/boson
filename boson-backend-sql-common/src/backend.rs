use std::fmt;

use boson_core::Result;
use sqlx::{Executor, Pool, Postgres, Sqlite};

use crate::enqueue_rate::EnqueueRateLimiter;
use crate::error_map::map_err;
use crate::schema;

/// `SQLite` uses `?` placeholders; `PostgreSQL` uses `$1`, `$2`, …
pub fn bind_sql(dialect: SqlDialect, sql: &str) -> String {
    match dialect {
        SqlDialect::Sqlite => sql.to_string(),
        SqlDialect::Postgres => {
            let mut out = String::with_capacity(sql.len());
            let mut n = 1u32;
            for ch in sql.chars() {
                if ch == '?' {
                    out.push('$');
                    out.push_str(&n.to_string());
                    n += 1;
                } else {
                    out.push(ch);
                }
            }
            out
        }
    }
}

/// SQL dialect for query variants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    /// `PostgreSQL`.
    Postgres,
    /// `SQLite`.
    Sqlite,
}

/// Connection pool for a SQL backend.
#[derive(Clone)]
pub enum SqlPool {
    /// `SQLite` pool.
    Sqlite(Pool<Sqlite>),
    /// `PostgreSQL` pool.
    Postgres(Pool<Postgres>),
}

impl fmt::Debug for SqlPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sqlite(_) => f.debug_tuple("SqlPool::Sqlite").finish(),
            Self::Postgres(_) => f.debug_tuple("SqlPool::Postgres").finish(),
        }
    }
}

/// SQL-backed queue backend (`PostgreSQL` or `SQLite`).
pub struct SqlQueueBackend {
    pub(crate) pool: SqlPool,
    pub(crate) dialect: SqlDialect,
    pub(crate) enqueue_rate: EnqueueRateLimiter,
}

impl SqlQueueBackend {
    /// Open a `SQLite` pool, bootstrap schema, and return a backend.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the pool connection or schema bootstrap fails.
    pub async fn connect_sqlite(url: &str) -> Result<Self> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| map_err(&e))?;
        Self::from_sqlite_pool(pool).await
    }

    /// Open a `PostgreSQL` pool, bootstrap schema, and return a backend.
    ///
    /// # Errors
    ///
    /// Returns a backend error if the pool connection or schema bootstrap fails.
    pub async fn connect_postgres(url: &str) -> Result<Self> {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .connect(url)
            .await
            .map_err(|e| map_err(&e))?;
        Self::from_postgres_pool(pool).await
    }

    /// Connect to `PostgreSQL` with an isolated schema for parallel tests.
    ///
    /// # Errors
    ///
    /// Returns a backend error if schema creation, pool connection, or schema bootstrap fails.
    pub async fn connect_postgres_isolated(url: &str, schema: &str) -> Result<Self> {
        let admin = sqlx::postgres::PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .map_err(|e| map_err(&e))?;
        let ddl = format!("CREATE SCHEMA IF NOT EXISTS \"{schema}\"");
        admin.execute(ddl.as_str()).await.map_err(|e| map_err(&e))?;
        drop(admin);

        let schema = schema.to_string();
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(5)
            .after_connect(move |conn, _meta| {
                let schema = schema.clone();
                Box::pin(async move {
                    let sql = format!("SET search_path TO \"{schema}\"");
                    sqlx::query(&sql).execute(conn).await?;
                    Ok(())
                })
            })
            .connect(url)
            .await
            .map_err(|e| map_err(&e))?;
        Self::from_postgres_pool(pool).await
    }

    /// Wrap an existing `SQLite` pool (schema bootstrap runs).
    ///
    /// # Errors
    ///
    /// Returns a backend error if schema bootstrap fails.
    pub async fn from_sqlite_pool(pool: Pool<Sqlite>) -> Result<Self> {
        let backend = Self {
            pool: SqlPool::Sqlite(pool),
            dialect: SqlDialect::Sqlite,
            enqueue_rate: EnqueueRateLimiter::new(),
        };
        schema::ensure_schema(&backend).await?;
        Ok(backend)
    }

    /// Wrap an existing `PostgreSQL` pool (schema bootstrap runs).
    ///
    /// # Errors
    ///
    /// Returns a backend error if schema bootstrap fails.
    pub async fn from_postgres_pool(pool: Pool<Postgres>) -> Result<Self> {
        let backend = Self {
            pool: SqlPool::Postgres(pool),
            dialect: SqlDialect::Postgres,
            enqueue_rate: EnqueueRateLimiter::new(),
        };
        schema::ensure_schema(&backend).await?;
        Ok(backend)
    }

    /// Underlying connection pool.
    #[must_use]
    pub const fn pool(&self) -> &SqlPool {
        &self.pool
    }

    /// Engine dialect.
    #[must_use]
    pub const fn dialect(&self) -> SqlDialect {
        self.dialect
    }

    pub(crate) async fn run_ddl(&self, ddl: &str) -> Result<()> {
        match &self.pool {
            SqlPool::Sqlite(pool) => {
                pool.execute(ddl).await.map_err(|e| map_err(&e))?;
            }
            SqlPool::Postgres(pool) => {
                pool.execute(ddl).await.map_err(|e| map_err(&e))?;
            }
        }
        Ok(())
    }
}

impl fmt::Debug for SqlQueueBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SqlQueueBackend")
            .field("dialect", &self.dialect)
            .finish_non_exhaustive()
    }
}
