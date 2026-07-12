//! `NATS` `JetStream` [`QueueBackend`] for fleet-scale deployments.
//!
//! See the [crate README](../README.md) for connect snippets and environment variables.
//! Fleet URL resolution: `BOSON_NATS_POOL_ROUTING` takes precedence over `BOSON_NATS_URLS`
//! when both are set (see `fleet_urls_from_env` in the `fleet` module).
//!
//! To implement a custom adapter, see **How to implement** on [`QueueBackend`].

mod config;
mod connect;
mod enqueue_rate;
mod fleet;
pub mod keys;
mod publish;
mod workqueue;

pub use config::{EnqueueMode, NatsEnqueueConfig};
pub use fleet::connect_fleet_from_env;

pub use workqueue::{connect_auto, NatsWorkQueueBackend};

use std::sync::Arc;

use async_nats::jetstream::kv::Store;
use async_nats::jetstream;
use async_trait::async_trait;
use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run,
    RunStatus, TaskConfig, TaskRunStats,
};
use chrono::{DateTime, Utc};
use enqueue_rate::EnqueueRateLimiter;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Lease row persisted in KV.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseRow {
    lease_id: String,
    job_id: String,
    worker_id: String,
    expires_at: DateTime<Utc>,
}

/// `NATS` `JetStream` KV queue backend.
pub struct NatsQueueBackend {
    kv: Store,
    keys: keys::Keyspace,
    enqueue_rate: EnqueueRateLimiter,
}

impl std::fmt::Debug for NatsQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsQueueBackend").finish_non_exhaustive()
    }
}

impl NatsQueueBackend {
    /// Connect to `NATS` at `url` and open the KV bucket.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use boson_backend_nats::NatsQueueBackend;
    ///
    /// # async fn connect() -> boson_core::Result<()> {
    /// let backend = NatsQueueBackend::connect("nats://127.0.0.1:4222").await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns an error when `NATS` or KV setup fails.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_keyspace(url, keys::Keyspace::from_env()).await
    }

    /// Connect with explicit key namespace.
    ///
    /// # Errors
    ///
    /// Returns an error when `NATS` or KV setup fails.
    pub async fn connect_with_keyspace(url: &str, keyspace: keys::Keyspace) -> Result<Self> {
        let client = connect::connect_nats(url).await.map_err(map_err)?;
        let js = jetstream::new(client);
        let bucket = keyspace.bucket();
        let kv = match js.get_key_value(&bucket).await {
            Ok(store) => store,
            Err(_) => js
                .create_key_value(async_nats::jetstream::kv::Config {
                    bucket,
                    ..Default::default()
                })
                .await
                .map_err(map_err)?,
        };
        Ok(Self {
            kv,
            keys: keyspace,
            enqueue_rate: EnqueueRateLimiter::new(),
        })
    }

    /// `NATS` URL for tests.
    #[must_use]
    pub fn test_url() -> String {
        std::env::var("BOSON_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into())
    }

    /// Delete all keys in this namespace (test isolation).
    ///
    /// # Errors
    ///
    /// Returns an error when KV list/delete fails.
    pub async fn flush_namespace(&self) -> Result<()> {
        let prefix = self.keys.namespace_prefix();
        let keys = self.list_keys_prefixed(&prefix).await?;
        for key in keys {
            self.kv_delete(&key).await?;
        }
        Ok(())
    }

    async fn kv_get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self
            .kv
            .get(key)
            .await
            .map_err(map_err)?
            .map(|bytes| bytes.to_vec()))
    }

    async fn kv_put(&self, key: &str, value: &[u8]) -> Result<()> {
        self.kv.put(key, value.to_vec().into()).await.map_err(map_err)?;
        Ok(())
    }

    async fn kv_delete(&self, key: &str) -> Result<()> {
        self.kv.delete(key).await.map_err(map_err)?;
        Ok(())
    }

    async fn list_keys_prefixed(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut stream = self.kv.keys().await.map_err(map_err)?;
        while let Some(key) = stream.next().await {
            let key = key.map_err(map_err)?;
            if key.starts_with(prefix) {
                keys.push(key);
            }
        }
        Ok(keys)
    }

    async fn load_job(&self, job_id: &str) -> Result<Option<Job>> {
        let raw = self.kv_get(&self.keys.job(job_id)).await?;
        raw.map_or(Ok(None), |bytes| {
            serde_json::from_slice(&bytes).map_err(map_err).map(Some)
        })
    }

    async fn save_job(&self, job: &Job) -> Result<()> {
        let bytes = serde_json::to_vec(job).map_err(map_err)?;
        self.kv_put(&self.keys.job(&job.job_id), &bytes).await
    }

    async fn add_ready(&self, job: &Job) -> Result<()> {
        if job.status != JobStatus::Queued {
            return Ok(());
        }
        let key = self.keys.ready(
            &job.pool,
            job.priority,
            job.created_at.timestamp_millis(),
            &job.job_id,
        );
        self.kv_put(&key, job.job_id.as_bytes()).await?;
        self.kv_put(&self.keys.pool_marker(&job.pool), b"1").await?;
        Ok(())
    }

    async fn remove_ready_for_job(&self, job: &Job) -> Result<()> {
        let prefix = self.keys.ready_prefix(&job.pool);
        let keys = self.list_keys_prefixed(&prefix).await?;
        for key in keys {
            if key.ends_with(&job.job_id) {
                self.kv_delete(&key).await?;
            }
        }
        Ok(())
    }

    async fn load_run(&self, run_id: &str) -> Result<Option<Run>> {
        let raw = self.kv_get(&self.keys.run(run_id)).await?;
        raw.map_or(Ok(None), |bytes| {
            serde_json::from_slice(&bytes).map_err(map_err).map(Some)
        })
    }

    async fn save_run(&self, run: &Run) -> Result<()> {
        let bytes = serde_json::to_vec(run).map_err(map_err)?;
        self.kv_put(&self.keys.run(&run.run_id), &bytes).await
    }

    async fn load_lease_row(&self, lease_id: &str) -> Result<Option<LeaseRow>> {
        let raw = self.kv_get(&self.keys.lease(lease_id)).await?;
        raw.map_or(Ok(None), |bytes| {
            serde_json::from_slice(&bytes).map_err(map_err).map(Some)
        })
    }
}

fn map_err(e: impl std::fmt::Display) -> BosonError {
    BosonError::Backend(e.to_string())
}

#[async_trait]
impl QueueBackend for NatsQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        let existing = self.load_job(&job.job_id).await?;
        if let Some(ref old) = existing {
            if old.status == JobStatus::Queued && job.status != JobStatus::Queued {
                self.remove_ready_for_job(old).await?;
            } else if job.status == JobStatus::Queued {
                self.remove_ready_for_job(old).await?;
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
                    let idem_key = self.keys.idempotency(key);
                    let inserted = self.kv_get(&idem_key).await?.is_none();
                    if inserted {
                        self.kv_put(&idem_key, job.job_id.as_bytes()).await?;
                    } else if let Some(bytes) = self.kv_get(&idem_key).await? {
                        let prior_id = String::from_utf8_lossy(&bytes).into_owned();
                        if let Some(prior) = self.load_job(&prior_id).await? {
                            if matches!(prior.status, JobStatus::Queued | JobStatus::Running) {
                                return Ok((
                                    prior_id,
                                    JobEnqueueDisposition::ReusedIdempotent,
                                ));
                            }
                        }
                        self.kv_put(&idem_key, job.job_id.as_bytes()).await?;
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
        let prefix = self.keys.job_prefix();
        let keys = self.list_keys_prefixed(&prefix).await?;
        let mut jobs = Vec::new();
        for key in keys {
            if let Some(bytes) = self.kv_get(&key).await? {
                if let Ok(job) = serde_json::from_slice::<Job>(&bytes) {
                    if status_filter.is_none_or(|st| job.status == st) {
                        jobs.push(job);
                    }
                }
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
            self.remove_ready_for_job(&job).await?;
        }
        job.status = JobStatus::Canceled;
        self.save_job(&job).await
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        let Some(mut job) = self.load_job(job_id).await? else {
            return Ok(None);
        };
        if job.status != JobStatus::Queued {
            return Ok(None);
        }
        job.status = JobStatus::Running;
        self.save_job(&job).await?;
        self.remove_ready_for_job(&job).await?;
        Ok(Some(job))
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
        let prefix = self.keys.pool_prefix();
        let keys = self.list_keys_prefixed(&prefix).await?;
        let mut out: Vec<String> = keys
            .iter()
            .filter_map(|key| key.strip_prefix(&prefix).map(str::to_string))
            .collect();
        out.sort();
        Ok(out)
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        let limit = limit.max(1);
        let prefix = self.keys.ready_prefix(pool);
        let mut keys = self.list_keys_prefixed(&prefix).await?;
        keys.sort();
        let mut jobs = Vec::new();
        for key in keys.into_iter().take(limit) {
            let Some(job_id) = key.rsplit('.').next() else {
                continue;
            };
            if let Some(job) = self.load_job(job_id).await? {
                if job.status == JobStatus::Queued && job.pool == pool {
                    jobs.push(job);
                }
            }
        }
        Ok(jobs)
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
        let Some(bytes) = self.kv_get(&self.keys.idempotency(key)).await? else {
            return Ok(None);
        };
        let job_id = String::from_utf8_lossy(&bytes).into_owned();
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
        let prefix = self.keys.run_prefix();
        let keys = self.list_keys_prefixed(&prefix).await?;
        let mut runs = Vec::new();
        for key in keys {
            if let Some(bytes) = self.kv_get(&key).await? {
                if let Ok(run) = serde_json::from_slice::<Run>(&bytes) {
                    if job_id_filter.is_none_or(|id| run.job_id == id) {
                        runs.push(run);
                    }
                }
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
        let raw = self.kv_get(&self.keys.task_config(task_name)).await?;
        raw.map_or(Ok(None), |bytes| {
            serde_json::from_slice(&bytes).map_err(map_err).map(Some)
        })
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        let bytes = serde_json::to_vec(config).map_err(map_err)?;
        self.kv_put(&self.keys.task_config(&config.task_name), &bytes)
            .await
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        if let Some(bytes) = self.kv_get(&self.keys.lease_by_job(job_id)).await? {
            let lid = String::from_utf8_lossy(&bytes).into_owned();
            if let Some(row) = self.load_lease_row(&lid).await? {
                if row.expires_at > Utc::now() {
                    return Ok(None);
                }
            }
        }
        if self.kv_get(&self.keys.lease_by_job(job_id)).await?.is_some() {
            return Ok(None);
        }
        let lease_id = Uuid::new_v4().to_string();
        let row = LeaseRow {
            lease_id: lease_id.clone(),
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(ttl_secs),
        };
        let json = serde_json::to_vec(&row).map_err(map_err)?;
        self.kv_put(&self.keys.lease_by_job(job_id), lease_id.as_bytes())
            .await?;
        self.kv_put(&self.keys.lease(&lease_id), &json).await?;
        Ok(Some(lease_id))
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        let Some(mut row) = self.load_lease_row(lease_id).await? else {
            return Ok(());
        };
        row.expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs);
        let json = serde_json::to_vec(&row).map_err(map_err)?;
        self.kv_put(&self.keys.lease(lease_id), &json).await
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        let Some(row) = self.load_lease_row(lease_id).await? else {
            return Ok(());
        };
        self.kv_delete(&self.keys.lease(lease_id)).await?;
        self.kv_delete(&self.keys.lease_by_job(&row.job_id)).await
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        let prefix = self.keys.lease_prefix();
        let keys = self.list_keys_prefixed(&prefix).await?;
        let now = Utc::now();
        let mut out = Vec::new();
        for key in keys {
            if key.contains(".lease_by_job.") {
                continue;
            }
            if let Some(bytes) = self.kv_get(&key).await? {
                if let Ok(row) = serde_json::from_slice::<LeaseRow>(&bytes) {
                    if row.expires_at <= now {
                        out.push((row.lease_id, row.job_id));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Install default `NATS` backend on global router (tests).
///
/// # Errors
///
/// Returns an error when `NATS` is unreachable.
pub async fn install_default_nats_backend(url: &str) -> Result<Arc<NatsQueueBackend>> {
    let backend = Arc::new(NatsQueueBackend::connect(url).await?);
    boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
        Arc::clone(&backend) as Arc<dyn QueueBackend>,
    ));
    Ok(backend)
}
