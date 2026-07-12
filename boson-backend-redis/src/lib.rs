//! Redis [`QueueBackend`] for fleet-scale deployments.
//!
//! See the [crate README](../README.md) for connect snippets and environment variables.
//! Fleet URL resolution: `BOSON_REDIS_POOL_ROUTING` takes precedence over `BOSON_REDIS_URLS`
//! when both are set (see `fleet_urls_from_env` in the `fleet` module).
//!
//! To implement a custom adapter, see **How to implement** on [`QueueBackend`].

mod config;
mod enqueue_rate;
mod fleet;
pub mod keys;

use std::sync::Arc;

use async_trait::async_trait;
use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run,
    RunStatus, TaskConfig, TaskRunStats,
};
use chrono::{DateTime, Utc};
use enqueue_rate::EnqueueRateLimiter;
use redis::aio::ConnectionManager;
use redis::{AsyncCommands, Script};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use config::RedisQueueConfig;
pub use fleet::connect_fleet_from_env;

/// Lease row persisted in Redis.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseRow {
    lease_id: String,
    job_id: String,
    worker_id: String,
    expires_at: DateTime<Utc>,
}

/// Redis-backed queue (ZSET ready queue + JSON job bodies).
pub struct RedisQueueBackend {
    conn: ConnectionManager,
    keys: keys::Keyspace,
    enqueue_rate: EnqueueRateLimiter,
}

impl std::fmt::Debug for RedisQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisQueueBackend").finish_non_exhaustive()
    }
}

impl RedisQueueBackend {
    /// Connect using [`RedisQueueConfig`].
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use boson_backend_redis::{RedisQueueBackend, RedisQueueConfig};
    ///
    /// # async fn connect() -> boson_core::Result<()> {
    /// let config = RedisQueueConfig::default();
    /// let backend = RedisQueueBackend::connect(config).await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when the connection cannot be established.
    pub async fn connect(config: RedisQueueConfig) -> Result<Self> {
        Self::connect_with_keyspace(
            &config.url,
            keys::Keyspace::new(config.key_prefix),
        )
        .await
    }

    /// Connect to Redis at `url` (e.g. `redis://127.0.0.1:6379`).
    ///
    /// # Errors
    ///
    /// Returns an error when the connection cannot be established.
    pub async fn connect_url(url: &str) -> Result<Self> {
        Self::connect(RedisQueueConfig {
            url: url.into(),
            ..Default::default()
        })
        .await
    }

    /// Connect with an explicit key namespace (test isolation).
    ///
    /// # Errors
    ///
    /// Returns an error when the connection cannot be established.
    pub async fn connect_with_keyspace(url: &str, keyspace: keys::Keyspace) -> Result<Self> {
        let client = redis::Client::open(url).map_err(map_err)?;
        let conn = ConnectionManager::new(client).await.map_err(map_err)?;
        Ok(Self {
            conn,
            keys: keyspace,
            enqueue_rate: EnqueueRateLimiter::new(),
        })
    }

    /// Redis URL for tests (`BOSON_TEST_REDIS_URL` or local default).
    #[must_use]
    pub fn test_url() -> String {
        std::env::var("BOSON_TEST_REDIS_URL")
            .unwrap_or_else(|_| "redis://127.0.0.1:6379".into())
    }

    /// Flush Boson keys (test isolation). Uses `SCAN` + `DEL`.
    ///
    /// # Errors
    ///
    /// Returns an error when Redis commands fail.
    pub async fn flush_boson_keys(&self) -> Result<()> {
        let pattern = self.keys.scan_pattern();
        let mut conn = self.conn.clone();
        let mut cursor = 0_u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(500)
                .query_async(&mut conn)
                .await
                .map_err(map_err)?;
            if !batch.is_empty() {
                let _: () = conn.del(batch).await.map_err(map_err)?;
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(())
    }

    async fn load_job(&self, job_id: &str) -> Result<Option<Job>> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn.get(self.keys.job(job_id)).await.map_err(map_err)?;
        raw.map_or(Ok(None), |s| serde_json::from_str(&s).map_err(map_err).map(Some))
    }

    async fn save_job(&self, job: &Job) -> Result<()> {
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(job).map_err(map_err)?;
        let _: () = conn.set(self.keys.job(&job.job_id), json).await.map_err(map_err)?;
        Ok(())
    }

    async fn add_ready(&self, job: &Job) -> Result<()> {
        if job.status != JobStatus::Queued {
            return Ok(());
        }
        let mut conn = self.conn.clone();
        let score = keys::ready_score(job.priority, job.created_at.timestamp_millis());
        let ready_key = self.keys.ready(&job.pool);
        let _: () = conn
            .zadd(&ready_key, job.job_id.as_str(), score)
            .await
            .map_err(map_err)?;
        let _: () = conn
            .sadd(self.keys.pools_set(), job.pool.as_str())
            .await
            .map_err(map_err)?;
        Ok(())
    }

    async fn remove_ready(&self, pool: &str, job_id: &str) -> Result<()> {
        let mut conn = self.conn.clone();
        let ready_key = self.keys.ready(pool);
        let _: () = conn.zrem(&ready_key, job_id).await.map_err(map_err)?;
        let len: i64 = conn.zcard(&ready_key).await.map_err(map_err)?;
        if len == 0 {
            let _: () = conn
                .srem(self.keys.pools_set(), pool)
                .await
                .map_err(map_err)?;
        }
        Ok(())
    }

    async fn load_run(&self, run_id: &str) -> Result<Option<Run>> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn.get(self.keys.run(run_id)).await.map_err(map_err)?;
        raw.map_or(Ok(None), |s| serde_json::from_str(&s).map_err(map_err).map(Some))
    }

    async fn save_run(&self, run: &Run) -> Result<()> {
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(run).map_err(map_err)?;
        let _: () = conn.set(self.keys.run(&run.run_id), json).await.map_err(map_err)?;
        Ok(())
    }
}

const CLAIM_SCRIPT: &str = r#"
local raw = redis.call('GET', KEYS[1])
if not raw then return nil end
if not string.find(raw, '"status":"queued"', 1, true) then return nil end
local updated = string.gsub(raw, '"status":"queued"', '"status":"running"', 1)
redis.call('SET', KEYS[1], updated)
redis.call('ZREM', KEYS[2], ARGV[1])
return updated
"#;

const POP_CLAIM_SCRIPT: &str = r#"
local ids = redis.call('ZRANGE', KEYS[1], 0, 0)
if #ids == 0 then return nil end
local job_id = ids[1]
local job_key = KEYS[2] .. job_id
local raw = redis.call('GET', job_key)
if not raw then
  redis.call('ZREM', KEYS[1], job_id)
  return nil
end
if not string.find(raw, '"status":"queued"', 1, true) then
  redis.call('ZREM', KEYS[1], job_id)
  return nil
end
local updated = string.gsub(raw, '"status":"queued"', '"status":"running"', 1)
redis.call('SET', job_key, updated)
redis.call('ZREM', KEYS[1], job_id)
return updated
"#;

fn map_err(e: impl std::fmt::Display) -> BosonError {
    BosonError::Backend(e.to_string())
}

#[async_trait]
impl QueueBackend for RedisQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        let existing = self.load_job(&job.job_id).await?;
        if let Some(ref old) = existing {
            if old.status == JobStatus::Queued && job.status != JobStatus::Queued {
                self.remove_ready(&old.pool, &job.job_id).await?;
            } else if job.status == JobStatus::Queued {
                self.remove_ready(&old.pool, &job.job_id).await?;
                self.add_ready(job).await?;
            }
        } else if job.status == JobStatus::Queued {
            self.add_ready(job).await?;
        }
        self.save_job(job).await
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        let idempotency = task_config.resolved_idempotency_mode(IdempotencyMode::Lwt);
        let mut job = job;
        if idempotency == IdempotencyMode::Lwt {
            if let Some(ref key) = job.idempotency_key {
                if !key.is_empty() {
                    let mut conn = self.conn.clone();
                    let idem_key = self.keys.idempotency(key);
                    let inserted: bool = conn
                        .set_nx(&idem_key, job.job_id.as_str())
                        .await
                        .map_err(map_err)?;
                    if !inserted {
                        let existing_id: Option<String> =
                            conn.get(&idem_key).await.map_err(map_err)?;
                        if let Some(ref prior_id) = existing_id {
                            if let Some(prior) = self.load_job(prior_id).await? {
                                if matches!(prior.status, JobStatus::Queued | JobStatus::Running)
                                {
                                    return Ok((
                                        prior_id.clone(),
                                        JobEnqueueDisposition::ReusedIdempotent,
                                    ));
                                }
                            }
                            let _: () = conn
                                .set(&idem_key, job.job_id.as_str())
                                .await
                                .map_err(map_err)?;
                        }
                    }
                }
            }
        } else {
            job.idempotency_key = None;
        }

        let policy = &task_config.rate_limit_policy;
        if policy.max_in_flight > 0 {
            let count = self.count_active_jobs_for_task(&job.task_name).await?;
            if count >= policy.max_in_flight {
                return Err(BosonError::RateLimited(job.task_name.clone()));
            }
        }
        if policy.max_enqueue_per_second > 0
            && !self
                .enqueue_rate
                .try_record(&job.task_name, policy.max_enqueue_per_second)
        {
            return Err(BosonError::RateLimited(job.task_name.clone()));
        }

        let job_id = job.job_id.clone();
        self.save_job(&job).await?;
        self.add_ready(&job).await?;
        Ok((job_id, JobEnqueueDisposition::InsertedNew))
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.load_job(job_id).await
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let pattern = self.keys.job_pattern();
        let mut conn = self.conn.clone();
        let mut jobs = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(map_err)?;
            for key in batch {
                let raw: Option<String> = conn.get(&key).await.map_err(map_err)?;
                if let Some(s) = raw {
                    if let Ok(job) = serde_json::from_str::<Job>(&s) {
                        if status_filter.is_none_or(|st| job.status == st) {
                            jobs.push(job);
                        }
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        jobs.sort_by_key(|j| j.created_at);
        Ok(jobs.into_iter().skip(offset).take(limit).collect())
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        let Some(mut job) = self.load_job(job_id).await? else {
            return Err(BosonError::JobNotFound(job_id.to_string()));
        };
        if !matches!(job.status, JobStatus::Queued | JobStatus::Running) {
            return Ok(());
        }
        if job.status == JobStatus::Queued {
            self.remove_ready(&job.pool, job_id).await?;
        }
        job.status = JobStatus::Canceled;
        self.save_job(&job).await
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        let Some(job) = self.load_job(job_id).await? else {
            return Ok(None);
        };
        if job.status != JobStatus::Queued {
            return Ok(None);
        }
        let script = Script::new(CLAIM_SCRIPT);
        let result: Option<String> = script
            .key(self.keys.job(job_id))
            .key(self.keys.ready(&job.pool))
            .arg(job_id)
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(map_err)?;
        result
            .and_then(|s| serde_json::from_str(&s).ok())
            .map_or(Ok(None), |j| Ok(Some(j)))
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        let Some(mut job) = self.load_job(job_id).await? else {
            return Ok(());
        };
        if job.status != JobStatus::Running {
            return Ok(());
        }
        job.status = JobStatus::Queued;
        self.save_job(&job).await?;
        self.add_ready(&job).await
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        let mut conn = self.conn.clone();
        let pools: Vec<String> = conn.smembers(self.keys.pools_set()).await.map_err(map_err)?;
        let mut out = pools;
        out.sort();
        Ok(out)
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        let limit = limit.max(1);
        let mut conn = self.conn.clone();
        let ids: Vec<String> = conn
            .zrange(self.keys.ready(pool), 0, isize::try_from(limit.saturating_sub(1)).unwrap_or(0))
            .await
            .map_err(map_err)?;
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let job_keys: Vec<String> = ids.iter().map(|id| self.keys.job(id)).collect();
        let mut pipe = redis::pipe();
        for key in &job_keys {
            pipe.get(key);
        }
        let raws: Vec<Option<String>> = pipe.query_async(&mut conn).await.map_err(map_err)?;
        let mut jobs = Vec::new();
        for (id, raw) in ids.iter().zip(raws) {
            if let Some(s) = raw {
                if let Ok(job) = serde_json::from_str::<Job>(&s) {
                    if job.status == JobStatus::Queued && job.pool == pool {
                        jobs.push(job);
                    }
                }
            } else {
                let _: () = conn.zrem(self.keys.ready(pool), id).await.map_err(map_err)?;
            }
        }
        Ok(jobs)
    }

    async fn pop_claim_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        let script = Script::new(POP_CLAIM_SCRIPT);
        let result: Option<String> = script
            .key(self.keys.ready(pool))
            .key(self.keys.job_key_prefix())
            .invoke_async(&mut self.conn.clone())
            .await
            .map_err(map_err)?;
        result
            .and_then(|s| serde_json::from_str(&s).ok())
            .map_or(Ok(None), |j| Ok(Some(j)))
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        let jobs = self.list_jobs(status_filter, 0, usize::MAX).await?;
        Ok(u64::try_from(jobs.len()).unwrap_or(u64::MAX))
    }

    async fn count_jobs_for_task(
        &self,
        task_name: &str,
        status: Option<JobStatus>,
    ) -> Result<u64> {
        let jobs = self.list_jobs(status, 0, usize::MAX).await?;
        let count = jobs.iter().filter(|j| j.task_name == task_name).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        let jobs = self.list_jobs(None, 0, usize::MAX).await?;
        let count = jobs
            .iter()
            .filter(|j| {
                j.task_name == task_name
                    && matches!(j.status, JobStatus::Queued | JobStatus::Running)
            })
            .count();
        Ok(u32::try_from(count).unwrap_or(u32::MAX))
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        if key.is_empty() {
            return Ok(None);
        }
        let mut conn = self.conn.clone();
        let job_id: Option<String> = conn.get(self.keys.idempotency(key)).await.map_err(map_err)?;
        let Some(job_id) = job_id else {
            return Ok(None);
        };
        if let Some(job) = self.load_job(&job_id).await? {
            if matches!(job.status, JobStatus::Queued | JobStatus::Running) {
                return Ok(Some(job_id));
            }
        }
        Ok(None)
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        self.save_run(run).await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.load_run(run_id).await
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let pattern = self.keys.run_pattern();
        let mut conn = self.conn.clone();
        let mut runs = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(map_err)?;
            for key in batch {
                let raw: Option<String> = conn.get(&key).await.map_err(map_err)?;
                if let Some(s) = raw {
                    if let Ok(run) = serde_json::from_str::<Run>(&s) {
                        if job_id_filter.is_none_or(|id| run.job_id == id) {
                            runs.push(run);
                        }
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        runs.sort_by_key(|r| r.started_at);
        Ok(runs.into_iter().skip(offset).take(limit).collect())
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        let Some(mut run) = self.load_run(run_id).await? else {
            return Ok(());
        };
        run.status = status;
        run.finished_at = Some(Utc::now());
        run.duration_ms = duration_ms;
        run.error_message = error_message;
        self.save_run(&run).await
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        let runs = self.list_runs(job_id_filter, 0, usize::MAX).await?;
        Ok(u64::try_from(runs.len()).unwrap_or(u64::MAX))
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        let runs = self.list_runs(None, 0, usize::MAX).await?;
        let count = runs.iter().filter(|r| r.started_at >= since).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        let runs = self.list_runs(None, 0, usize::MAX).await?;
        let filtered: Vec<_> = runs.iter().filter(|r| r.task_name == task_name).collect();
        let runs_total = u32::try_from(filtered.len()).unwrap_or(u32::MAX);
        let success_count = u32::try_from(
            filtered
                .iter()
                .filter(|r| r.status == RunStatus::Success)
                .count(),
        )
        .unwrap_or(u32::MAX);
        Ok(TaskRunStats {
            runs_total,
            success_count,
        })
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn.get(self.keys.task_config(task_name)).await.map_err(map_err)?;
        raw.map_or(Ok(None), |s| serde_json::from_str(&s).map_err(map_err).map(Some))
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(config).map_err(map_err)?;
        let _: () = conn
            .set(self.keys.task_config(&config.task_name), json)
            .await
            .map_err(map_err)?;
        Ok(())
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        let mut conn = self.conn.clone();
        let existing: Option<String> = conn.get(self.keys.lease_by_job(job_id)).await.map_err(map_err)?;
        if let Some(ref lid) = existing {
            let raw: Option<String> = conn.get(self.keys.lease(lid)).await.map_err(map_err)?;
            if let Some(s) = raw {
                if let Ok(row) = serde_json::from_str::<LeaseRow>(&s) {
                    if row.expires_at > Utc::now() {
                        return Ok(None);
                    }
                }
            }
        }
        let lease_id = Uuid::new_v4().to_string();
        let row = LeaseRow {
            lease_id: lease_id.clone(),
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(ttl_secs),
        };
        let json = serde_json::to_string(&row).map_err(map_err)?;
        let inserted: bool = conn
            .set_nx(self.keys.lease_by_job(job_id), lease_id.as_str())
            .await
            .map_err(map_err)?;
        if !inserted {
            return Ok(None);
        }
        let _: () = conn.set(self.keys.lease(&lease_id), json).await.map_err(map_err)?;
        Ok(Some(lease_id))
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        let Some(mut row) = self
            .load_lease_row(lease_id)
            .await?
        else {
            return Ok(());
        };
        row.expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs);
        let mut conn = self.conn.clone();
        let json = serde_json::to_string(&row).map_err(map_err)?;
        let _: () = conn.set(self.keys.lease(lease_id), json).await.map_err(map_err)?;
        Ok(())
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        let Some(row) = self.load_lease_row(lease_id).await? else {
            return Ok(());
        };
        let mut conn = self.conn.clone();
        let _: () = conn.del(self.keys.lease(lease_id)).await.map_err(map_err)?;
        let _: () = conn
            .del(self.keys.lease_by_job(&row.job_id))
            .await
            .map_err(map_err)?;
        Ok(())
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        let pattern = self.keys.lease_pattern();
        let mut conn = self.conn.clone();
        let now = Utc::now();
        let mut out = Vec::new();
        let mut cursor = 0_u64;
        loop {
            let (next, batch): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(200)
                .query_async(&mut conn)
                .await
                .map_err(map_err)?;
            for key in batch {
                if key.contains(":lease-by-job:") {
                    continue;
                }
                let raw: Option<String> = conn.get(&key).await.map_err(map_err)?;
                if let Some(s) = raw {
                    if let Ok(row) = serde_json::from_str::<LeaseRow>(&s) {
                        if row.expires_at <= now {
                            out.push((row.lease_id, row.job_id));
                        }
                    }
                }
            }
            cursor = next;
            if cursor == 0 {
                break;
            }
        }
        Ok(out)
    }
}

impl RedisQueueBackend {
    async fn load_lease_row(&self, lease_id: &str) -> Result<Option<LeaseRow>> {
        let mut conn = self.conn.clone();
        let raw: Option<String> = conn.get(self.keys.lease(lease_id)).await.map_err(map_err)?;
        raw.map_or(Ok(None), |s| serde_json::from_str(&s).map_err(map_err).map(Some))
    }
}

/// Install default Redis backend on global router (tests).
///
/// # Errors
///
/// Returns an error when Redis is unreachable.
pub async fn install_default_redis_backend(url: &str) -> Result<Arc<RedisQueueBackend>> {
    let backend = Arc::new(RedisQueueBackend::connect_url(url).await?);
    boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
        Arc::clone(&backend) as Arc<dyn QueueBackend>,
    ));
    Ok(backend)
}
