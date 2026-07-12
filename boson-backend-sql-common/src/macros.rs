#![allow(missing_docs)]

#[macro_export]
macro_rules! sql_execute {
    ($backend:expr, $sql:expr, |$q:ident| $body:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.execute(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.execute(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
            }
        }
        Ok::<(), boson_core::BosonError>(())
    }};
}

#[macro_export]
macro_rules! sql_fetch_optional_map {
    ($backend:expr, $sql:expr, |$q:ident| $bind:expr, |$row:ident| $map:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                match $q.fetch_optional(pool).await.map_err(|e| $crate::error_map::map_err(&e))? {
                    Some($row) => Ok(Some($map?)),
                    None => Ok(None),
                }
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                match $q.fetch_optional(pool).await.map_err(|e| $crate::error_map::map_err(&e))? {
                    Some($row) => Ok(Some($map?)),
                    None => Ok(None),
                }
            }
        }
    }};
}

#[macro_export]
macro_rules! sql_fetch_one_map {
    ($backend:expr, $sql:expr, |$q:ident| $bind:expr, |$row:ident| $map:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                let $row = $q.fetch_one(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
                $map
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                let $row = $q.fetch_one(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
                $map
            }
        }
    }};
}

#[macro_export]
macro_rules! sql_fetch_all_map {
    ($backend:expr, $sql:expr, |$q:ident| $bind:expr, |$row:ident| $map:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                let rows = $q.fetch_all(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
                rows.iter().map(|$row| $map).collect::<boson_core::Result<Vec<_>>>()
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $bind;
                let rows = $q.fetch_all(pool).await.map_err(|e| $crate::error_map::map_err(&e))?;
                rows.iter().map(|$row| $map).collect::<boson_core::Result<Vec<_>>>()
            }
        }
    }};
}

#[macro_export]
macro_rules! sql_fetch_optional {
    ($backend:expr, $sql:expr, |$q:ident| $body:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_optional(pool)
                    .await
                    .map_err(|e| $crate::error_map::map_err(&e))
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_optional(pool)
                    .await
                    .map_err(|e| $crate::error_map::map_err(&e))
            }
        }
    }};
}

#[macro_export]
macro_rules! sql_fetch_one {
    ($backend:expr, $sql:expr, |$q:ident| $body:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_one(pool).await.map_err(|e| $crate::error_map::map_err(&e))
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_one(pool).await.map_err(|e| $crate::error_map::map_err(&e))
            }
        }
    }};
}

#[macro_export]
macro_rules! sql_fetch_all {
    ($backend:expr, $sql:expr, |$q:ident| $body:expr) => {{
        match &$backend.pool {
            $crate::SqlPool::Sqlite(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_all(pool).await.map_err(|e| $crate::error_map::map_err(&e))
            }
            $crate::SqlPool::Postgres(pool) => {
                let $q = sqlx::query($sql);
                let $q = $body;
                $q.fetch_all(pool).await.map_err(|e| $crate::error_map::map_err(&e))
            }
        }
    }};
}
