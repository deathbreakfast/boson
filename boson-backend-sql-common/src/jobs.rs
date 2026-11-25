//! Job persistence for the SQL queue backend.

use boson_core::{
    BosonError, Job, JobEnqueueDisposition, JobFilter, JobKey, JobStatus, Result, TaskConfig,
};
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::enqueue_rate::EnqueueRateLimiter;
use crate::filter_sql::{job_where, jobs_after_where};
use crate::row::{job_status_to_str, job_to_binds, row_to_job};
use crate::{
    bind_all, bind_sql, sql_execute, sql_execute_count, sql_fetch_all_map, sql_fetch_one_map,
    sql_fetch_optional_map, SqlQueueBackend,
};

impl SqlQueueBackend {
    pub(crate) async fn upsert_job_impl(&self, job: &Job) -> Result<()> {
        let (actor_json, params_json) = job_to_binds(job)?;
        let sql = bind_sql(
            self.dialect,
            "INSERT INTO boson_job
             (job_id, task_name, actor_json, params_json, priority, pool, status,
              idempotency_key, created_at, signature_hash, attempt, retried_from, dropped_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (job_id) DO UPDATE SET
              task_name = excluded.task_name,
              actor_json = excluded.actor_json,
              params_json = excluded.params_json,
              priority = excluded.priority,
              pool = excluded.pool,
              status = excluded.status,
              idempotency_key = excluded.idempotency_key,
              created_at = excluded.created_at,
              signature_hash = excluded.signature_hash,
              attempt = excluded.attempt,
              retried_from = excluded.retried_from,
              dropped_at = excluded.dropped_at",
        );
        sql_execute!(self, &sql, |q| {
            q.bind(&job.job_id)
                .bind(&job.task_name)
                .bind(&actor_json)
                .bind(&params_json)
                .bind(job.priority)
                .bind(&job.pool)
                .bind(job_status_to_str(job.status))
                .bind(&job.idempotency_key)
                .bind(job.created_at)
                .bind(i64::try_from(job.signature_hash).unwrap_or(i64::MAX))
                .bind(job.attempt)
                .bind(&job.retried_from)
                .bind(job.dropped_at)
        })
    }

    pub(crate) async fn find_nonterminal_by_idempotency_key_impl(
        &self,
        key: &str,
    ) -> Result<Option<String>> {
        if key.is_empty() {
            return Ok(None);
        }
        let sql = bind_sql(
            self.dialect,
            "SELECT job_id FROM boson_job
             WHERE idempotency_key = ? AND status IN ('queued', 'running')
             LIMIT 1",
        );
        sql_fetch_optional_map!(self, &sql, |q| q.bind(key), |r| {
            Ok::<String, boson_core::BosonError>(r.get("job_id"))
        })
    }

    pub(crate) async fn count_active_jobs_for_task_impl(&self, task_name: &str) -> Result<u32> {
        let sql = bind_sql(
            self.dialect,
            "SELECT COUNT(*) AS cnt FROM boson_job
             WHERE task_name = ? AND status IN ('queued', 'running')",
        );
        sql_fetch_one_map!(self, &sql, |q| q.bind(task_name), |r| {
            Ok::<u32, boson_core::BosonError>(
                u32::try_from(r.get::<i64, _>("cnt")).unwrap_or(u32::MAX),
            )
        })
    }

    pub(crate) async fn enqueue_with_policies_impl(
        &self,
        rate_limiter: &EnqueueRateLimiter,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        let idempotency = task_config.resolved_idempotency_mode(boson_core::IdempotencyMode::Lwt);
        let mut job = job;
        if idempotency == boson_core::IdempotencyMode::Lwt {
            if let Some(ref key) = job.idempotency_key {
                if !key.is_empty() {
                    if let Some(existing) =
                        self.find_nonterminal_by_idempotency_key_impl(key).await?
                    {
                        return Ok((existing, JobEnqueueDisposition::ReusedIdempotent));
                    }
                }
            }
        } else {
            job.idempotency_key = None;
        }

        let policy = &task_config.rate_limit_policy;
        if policy.max_in_flight > 0 {
            let count = self.count_active_jobs_for_task_impl(&job.task_name).await?;
            if count >= policy.max_in_flight {
                return Err(BosonError::RateLimited(job.task_name.clone()));
            }
        }

        if policy.max_enqueue_per_second > 0
            && !rate_limiter.try_record(&job.task_name, policy.max_enqueue_per_second)
        {
            return Err(BosonError::RateLimited(job.task_name.clone()));
        }

        let job_id = job.job_id.clone();
        match self.upsert_job_impl(&job).await {
            Ok(()) => Ok((job_id, JobEnqueueDisposition::InsertedNew)),
            Err(e) if e.is_backend_unique_violation() => {
                if let Some(ref key) = job.idempotency_key {
                    if let Some(existing) =
                        self.find_nonterminal_by_idempotency_key_impl(key).await?
                    {
                        return Ok((existing, JobEnqueueDisposition::ReusedIdempotent));
                    }
                }
                Err(e)
            }
            Err(e) => Err(e),
        }
    }

    pub(crate) async fn get_job_impl(&self, job_id: &str) -> Result<Option<Job>> {
        let sql = bind_sql(self.dialect, "SELECT * FROM boson_job WHERE job_id = ?");
        sql_fetch_optional_map!(self, &sql, |q| q.bind(job_id), |r| row_to_job(&r))
    }

    pub(crate) async fn list_jobs_impl(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        if let Some(status) = status_filter {
            let sql = bind_sql(
                self.dialect,
                "SELECT * FROM boson_job WHERE status = ? ORDER BY created_at ASC LIMIT ? OFFSET ?",
            );
            sql_fetch_all_map!(
                self,
                &sql,
                |q| {
                    q.bind(job_status_to_str(status))
                        .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                        .bind(i64::try_from(offset).unwrap_or(i64::MAX))
                },
                |r| row_to_job(r)
            )
        } else {
            let sql = bind_sql(
                self.dialect,
                "SELECT * FROM boson_job ORDER BY created_at ASC LIMIT ? OFFSET ?",
            );
            sql_fetch_all_map!(
                self,
                &sql,
                |q| q
                    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                    .bind(i64::try_from(offset).unwrap_or(i64::MAX)),
                |r| { row_to_job(r) }
            )
        }
    }

    pub(crate) async fn list_jobs_after_impl(
        &self,
        filter: &JobFilter,
        after: Option<&JobKey>,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let w = jobs_after_where(self.dialect, filter, after);
        let sql = bind_sql(
            self.dialect,
            &format!(
                "SELECT * FROM boson_job{} ORDER BY created_at ASC, job_id ASC LIMIT ?",
                w.sql()
            ),
        );
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);
        sql_fetch_all_map!(self, &sql, |q| bind_all!(q, &w.binds).bind(limit), |r| {
            row_to_job(r)
        })
    }

    pub(crate) async fn count_jobs_matching_impl(&self, filter: &JobFilter) -> Result<u64> {
        let w = job_where(self.dialect, filter);
        let sql = bind_sql(
            self.dialect,
            &format!("SELECT COUNT(*) AS cnt FROM boson_job{}", w.sql()),
        );
        sql_fetch_one_map!(self, &sql, |q| bind_all!(q, &w.binds), |r| Ok(
            u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX)
        ))
    }

    pub(crate) async fn drop_failed_job_impl(&self, job_id: &str) -> Result<()> {
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_job SET dropped_at = ?
             WHERE job_id = ? AND status = 'failed' AND dropped_at IS NULL",
        );
        let changed = sql_execute_count!(self, &sql, |q| q.bind(Utc::now()).bind(job_id))?;
        if changed > 0 {
            return Ok(());
        }
        match self.get_job_impl(job_id).await? {
            None => Err(BosonError::JobNotFound(job_id.to_string())),
            Some(job) if job.status != JobStatus::Failed => {
                Err(BosonError::InvalidConfig(format!(
                    "job {job_id} is {}; only failed jobs can be dropped",
                    job.status
                )))
            }
            Some(_) => Ok(()),
        }
    }

    pub(crate) async fn sweep_failed_impl(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let sql = bind_sql(
            self.dialect,
            "DELETE FROM boson_job WHERE status = 'failed' AND created_at < ?",
        );
        sql_execute_count!(self, &sql, |q| q.bind(older_than))
    }

    pub(crate) async fn purge_queued_in_pool_impl(&self, pool: &str) -> Result<u64> {
        let sql = bind_sql(
            self.dialect,
            "DELETE FROM boson_job WHERE pool = ? AND status = 'queued'",
        );
        sql_execute_count!(self, &sql, |q| q.bind(pool))
    }

    pub(crate) async fn cancel_job_if_active_impl(&self, job_id: &str) -> Result<()> {
        let existing = self.get_job_impl(job_id).await?;
        let Some(_) = existing else {
            return Err(BosonError::JobNotFound(job_id.to_string()));
        };
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_job SET status = 'canceled'
             WHERE job_id = ? AND status IN ('queued', 'running')",
        );
        sql_execute!(self, &sql, |q| q.bind(job_id))
    }

    pub(crate) async fn try_claim_job_impl(&self, job_id: &str) -> Result<Option<Job>> {
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_job SET status = 'running'
             WHERE job_id = ? AND status = 'queued'
              AND NOT EXISTS (SELECT 1 FROM boson_pool_state ps
                              WHERE ps.pool = boson_job.pool AND ps.paused = TRUE)
             RETURNING *",
        );
        sql_fetch_optional_map!(self, &sql, |q| q.bind(job_id), |r| row_to_job(&r))
    }

    pub(crate) async fn revert_job_to_queued_impl(&self, job_id: &str) -> Result<()> {
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_job SET status = 'queued'
             WHERE job_id = ? AND status = 'running'",
        );
        sql_execute!(self, &sql, |q| q.bind(job_id))
    }

    pub(crate) async fn distinct_pools_queued_impl(&self) -> Result<Vec<String>> {
        let sql = bind_sql(
            self.dialect,
            "SELECT DISTINCT pool FROM boson_job WHERE status = 'queued' ORDER BY pool",
        );
        sql_fetch_all_map!(self, &sql, |q| q, |r| {
            Ok::<String, boson_core::BosonError>(r.get("pool"))
        })
    }

    pub(crate) async fn list_queued_for_pool_sorted_impl(
        &self,
        pool: &str,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let sql = bind_sql(
            self.dialect,
            "SELECT * FROM boson_job
             WHERE status = 'queued' AND pool = ?
             ORDER BY priority ASC, created_at ASC
             LIMIT ?",
        );
        sql_fetch_all_map!(
            self,
            &sql,
            |q| q.bind(pool).bind(i64::try_from(limit).unwrap_or(i64::MAX)),
            |r| row_to_job(r)
        )
    }

    pub(crate) async fn count_jobs_impl(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        if let Some(status) = status_filter {
            let sql = bind_sql(
                self.dialect,
                "SELECT COUNT(*) AS cnt FROM boson_job WHERE status = ?",
            );
            sql_fetch_one_map!(self, &sql, |q| q.bind(job_status_to_str(status)), |r| Ok(
                u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX)
            ))
        } else {
            let sql = bind_sql(self.dialect, "SELECT COUNT(*) AS cnt FROM boson_job");
            sql_fetch_one_map!(self, &sql, |q| q, |r| {
                Ok(u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX))
            })
        }
    }

    pub(crate) async fn count_jobs_for_task_impl(
        &self,
        task_name: &str,
        status: Option<JobStatus>,
    ) -> Result<u64> {
        if let Some(s) = status {
            let sql = bind_sql(
                self.dialect,
                "SELECT COUNT(*) AS cnt FROM boson_job WHERE task_name = ? AND status = ?",
            );
            sql_fetch_one_map!(
                self,
                &sql,
                |q| q.bind(task_name).bind(job_status_to_str(s)),
                |r| Ok(u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX))
            )
        } else {
            let sql = bind_sql(
                self.dialect,
                "SELECT COUNT(*) AS cnt FROM boson_job WHERE task_name = ?",
            );
            sql_fetch_one_map!(self, &sql, |q| q.bind(task_name), |r| Ok(u64::try_from(
                r.get::<i64, _>("cnt")
            )
            .unwrap_or(u64::MAX)))
        }
    }
}
