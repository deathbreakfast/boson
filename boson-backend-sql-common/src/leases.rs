//! Distributed lease persistence for the SQL queue backend.

use boson_core::Result;
use chrono::{Duration, Utc};
use sqlx::Row;
use uuid::Uuid;

use crate::{bind_sql, sql_execute, sql_fetch_all_map, SqlQueueBackend};

impl SqlQueueBackend {
    pub(crate) async fn try_claim_run_lease_impl(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        self.purge_expired_leases_for_job_impl(job_id).await?;

        if self.has_active_lease_impl(job_id).await? {
            return Ok(None);
        }

        let now = Utc::now();
        let lease_id = Uuid::new_v4().to_string();
        let expires_at = now + Duration::seconds(ttl_secs);
        let sql = bind_sql(
            self.dialect,
            "INSERT INTO boson_lease (lease_id, job_id, worker_id, expires_at)
             VALUES (?, ?, ?, ?)",
        );
        sql_execute!(self, &sql, |q| {
            q.bind(&lease_id)
                .bind(job_id)
                .bind(worker_id)
                .bind(expires_at)
        })?;
        Ok(Some(lease_id))
    }

    pub(crate) async fn extend_lease_impl(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        let expires_at = Utc::now() + Duration::seconds(ttl_secs);
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_lease SET expires_at = ? WHERE lease_id = ?",
        );
        sql_execute!(self, &sql, |q| q.bind(expires_at).bind(lease_id))
    }

    pub(crate) async fn release_lease_impl(&self, lease_id: &str) -> Result<()> {
        let sql = bind_sql(self.dialect, "DELETE FROM boson_lease WHERE lease_id = ?");
        sql_execute!(self, &sql, |q| q.bind(lease_id))
    }

    pub(crate) async fn expired_lease_job_pairs_impl(&self) -> Result<Vec<(String, String)>> {
        let now = Utc::now();
        let sql = bind_sql(
            self.dialect,
            "SELECT lease_id, job_id FROM boson_lease WHERE expires_at <= ?",
        );
        sql_fetch_all_map!(self, &sql, |q| q.bind(now), |r| {
            Ok((r.get::<String, _>("lease_id"), r.get::<String, _>("job_id")))
        })
    }

    async fn purge_expired_leases_for_job_impl(&self, job_id: &str) -> Result<()> {
        let now = Utc::now();
        let sql = bind_sql(
            self.dialect,
            "DELETE FROM boson_lease WHERE job_id = ? AND expires_at <= ?",
        );
        sql_execute!(self, &sql, |q| q.bind(job_id).bind(now))
    }

    async fn has_active_lease_impl(&self, job_id: &str) -> Result<bool> {
        let now = Utc::now();
        let sql = bind_sql(
            self.dialect,
            "SELECT 1 AS present FROM boson_lease WHERE job_id = ? AND expires_at > ? LIMIT 1",
        );
        match &self.pool {
            crate::SqlPool::Sqlite(pool) => {
                let row = sqlx::query(&sql)
                    .bind(job_id)
                    .bind(now)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| crate::error_map::map_err(&e))?;
                Ok(row.is_some())
            }
            crate::SqlPool::Postgres(pool) => {
                let row = sqlx::query(&sql)
                    .bind(job_id)
                    .bind(now)
                    .fetch_optional(pool)
                    .await
                    .map_err(|e| crate::error_map::map_err(&e))?;
                Ok(row.is_some())
            }
        }
    }
}
