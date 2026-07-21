//! `JetStream` `WorkQueue` stream backend for Boson Tier 3 drain path.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use async_nats::jetstream::consumer::{pull, AckPolicy, DeliverPolicy};
use async_nats::jetstream::kv::Store;
use async_nats::jetstream::stream::{Config as StreamConfig, RetentionPolicy};
use async_nats::jetstream::{self, consumer::Consumer};
use async_trait::async_trait;
use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run,
    RunStatus, TaskConfig, TaskRunStats,
};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::config::{
    fetch_batch_from_env, skip_claim_kv_from_env, stream_replicas_from_env, EnqueueMode,
    NatsEnqueueConfig,
};
use crate::enqueue_rate::EnqueueRateLimiter;
use crate::keys::Keyspace;
use crate::publish::PublishPipeline;

/// Lease row persisted in KV.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct LeaseRow {
    lease_id: String,
    job_id: String,
    worker_id: String,
    expires_at: DateTime<Utc>,
}

type PendingAck = async_nats::jetstream::Message;

/// `NATS` `JetStream` `WorkQueue` backend (stream ready queue + KV job bodies).
pub struct NatsWorkQueueBackend {
    js: jetstream::Context,
    kv: Store,
    keys: Keyspace,
    config: NatsEnqueueConfig,
    pipeline: PublishPipeline,
    enqueue_rate: EnqueueRateLimiter,
    pending_acks: Mutex<HashMap<String, PendingAck>>,
    active_pools: Mutex<HashSet<String>>,
    ensured_pools: Mutex<HashSet<String>>,
    claim_buffers: Mutex<HashMap<String, VecDeque<Job>>>,
    claim_refill: Mutex<()>,
}

impl std::fmt::Debug for NatsWorkQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NatsWorkQueueBackend")
            .field("enqueue_mode", &self.config.enqueue_mode)
            .finish_non_exhaustive()
    }
}

impl NatsWorkQueueBackend {
    /// Connect to `NATS` and ensure KV + `WorkQueue` streams are available.
    ///
    /// # Errors
    ///
    /// Returns an error when `NATS`, KV, or stream setup fails.
    pub async fn connect(url: &str) -> Result<Self> {
        Self::connect_with_keyspace(url, Keyspace::from_env()).await
    }

    /// Connect with explicit key namespace (enqueue settings from env).
    ///
    /// # Errors
    ///
    /// Returns an error when `NATS`, KV, or stream setup fails.
    pub async fn connect_with_keyspace(url: &str, keyspace: Keyspace) -> Result<Self> {
        Self::connect_with_config(url, keyspace, NatsEnqueueConfig::from_env()).await
    }

    /// Connect with explicit key namespace and enqueue configuration.
    ///
    /// # Errors
    ///
    /// Returns an error when `NATS`, KV, or stream setup fails.
    pub async fn connect_with_config(
        url: &str,
        keyspace: Keyspace,
        config: NatsEnqueueConfig,
    ) -> Result<Self> {
        let client = crate::connect::connect_nats(url).await.map_err(map_err)?;
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
        let pipeline = PublishPipeline::new(config);
        Ok(Self {
            js,
            kv,
            keys: keyspace,
            pipeline,
            config,
            enqueue_rate: EnqueueRateLimiter::new(),
            pending_acks: Mutex::new(HashMap::new()),
            active_pools: Mutex::new(HashSet::new()),
            ensured_pools: Mutex::new(HashSet::new()),
            claim_buffers: Mutex::new(HashMap::new()),
            claim_refill: Mutex::new(()),
        })
    }

    /// `NATS` URL for integration tests.
    #[must_use]
    pub fn test_url() -> String {
        std::env::var("BOSON_TEST_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into())
    }

    /// Delete KV keys in this namespace (test isolation).
    ///
    /// # Errors
    ///
    /// Returns an error when listing or deleting KV entries fails.
    pub async fn flush_namespace(&self) -> Result<()> {
        let prefix = self.keys.namespace_prefix();
        let mut stream = self.kv.keys().await.map_err(map_err)?;
        while let Some(key) = stream.next().await {
            let key = key.map_err(map_err)?;
            if key.starts_with(&prefix) {
                self.kv_delete(&key).await?;
            }
        }
        Ok(())
    }

    /// Resolved enqueue configuration.
    #[must_use]
    pub const fn enqueue_config(&self) -> &NatsEnqueueConfig {
        &self.config
    }

    fn stream_name(&self, pool: &str) -> String {
        format!(
            "{}_wq_{}",
            self.keys.prefix().replace(':', "_"),
            pool.replace('.', "_")
        )
    }

    fn subject(&self, pool: &str) -> String {
        format!("{}.wq.{}", self.keys.prefix(), pool.replace('.', "_"))
    }

    async fn ensure_stream(&self, pool: &str) -> Result<()> {
        if self.ensured_pools.lock().await.contains(pool) {
            return Ok(());
        }
        let name = self.stream_name(pool);
        let subject = self.subject(pool);
        if self.js.get_stream(&name).await.is_err() {
            self.js
                .create_stream(StreamConfig {
                    name: name.clone(),
                    subjects: vec![subject.clone()],
                    retention: RetentionPolicy::WorkQueue,
                    max_messages: 1_000_000,
                    num_replicas: stream_replicas_from_env(),
                    ..Default::default()
                })
                .await
                .map_err(map_err)?;
        }
        self.ensured_pools.lock().await.insert(pool.to_string());
        Ok(())
    }

    async fn consumer(&self, pool: &str) -> Result<Consumer<pull::Config>> {
        self.ensure_stream(pool).await?;
        let stream = self
            .js
            .get_stream(self.stream_name(pool))
            .await
            .map_err(map_err)?;
        let durable = format!("{}-workers-{}", self.keys.prefix(), pool.replace('.', "_"));
        match stream.get_consumer(&durable).await {
            Ok(c) => Ok(c),
            Err(_) => stream
                .create_consumer(pull::Config {
                    durable_name: Some(durable),
                    ack_policy: AckPolicy::Explicit,
                    deliver_policy: DeliverPolicy::All,
                    max_ack_pending: i64::try_from(fetch_batch_from_env().max(64))
                        .unwrap_or(i64::MAX),
                    ..Default::default()
                })
                .await
                .map_err(map_err),
        }
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
        self.kv
            .put(key, value.to_vec().into())
            .await
            .map_err(map_err)?;
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

    fn job_payload_bytes(&self, job: &Job) -> Result<Bytes> {
        match self.config.enqueue_mode {
            EnqueueMode::Stream => Ok(Bytes::from(serde_json::to_vec(job).map_err(map_err)?)),
            EnqueueMode::Dual => Ok(Bytes::from(job.job_id.clone())),
        }
    }

    async fn publish_job(&self, job: &Job) -> Result<()> {
        let body = self.job_payload_bytes(job)?;
        self.ensure_stream(&job.pool).await?;
        let subject = self.subject(&job.pool);
        self.pipeline.publish(&self.js, subject, body).await?;
        self.active_pools.lock().await.insert(job.pool.clone());
        Ok(())
    }

    fn mirror_job_async(&self, job: &Job) {
        let kv = self.kv.clone();
        let key = self.keys.job(&job.job_id);
        let Ok(bytes) = serde_json::to_vec(job) else {
            return;
        };
        tokio::spawn(async move {
            let _ = kv.put(key, bytes.into()).await;
        });
    }

    async fn resolve_job_from_payload(&self, payload: &[u8]) -> Result<Option<Job>> {
        let job = if payload.first() == Some(&b'{') {
            serde_json::from_slice(payload).map_err(map_err)?
        } else {
            let job_id = String::from_utf8_lossy(payload).into_owned();
            match self.load_job(&job_id).await? {
                Some(j) => j,
                None => return Ok(None),
            }
        };

        if let Some(kv) = self.load_job(&job.job_id).await? {
            if !matches!(kv.status, JobStatus::Queued) {
                return Ok(None);
            }
        }

        if job.status != JobStatus::Queued {
            return Ok(None);
        }
        Ok(Some(job))
    }

    async fn ack_job(&self, job_id: &str) -> Result<()> {
        let pending = self.pending_acks.lock().await.remove(job_id);
        if let Some(msg) = pending {
            msg.ack().await.map_err(map_err)?;
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

    async fn claim_one_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        let consumer = self.consumer(pool).await?;
        let batch = consumer
            .fetch()
            .max_messages(1)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .map_err(map_err)?;
        let mut messages = batch;
        let Some(msg) = messages.next().await else {
            return Ok(None);
        };
        let msg = msg.map_err(map_err)?;
        let Some(mut job) = self.resolve_job_from_payload(&msg.payload).await? else {
            msg.ack().await.map_err(map_err)?;
            return Ok(None);
        };
        job.status = JobStatus::Running;
        if !skip_claim_kv_from_env() {
            self.save_job(&job).await?;
        }
        let job_id = job.job_id.clone();
        self.pending_acks.lock().await.insert(job_id, msg);
        Ok(Some(job))
    }

    async fn refill_claim_buffer(&self, pool: &str) -> Result<()> {
        let batch_size = fetch_batch_from_env();
        let consumer = self.consumer(pool).await?;
        let batch = consumer
            .fetch()
            .max_messages(batch_size)
            .expires(Duration::from_secs(2))
            .messages()
            .await
            .map_err(map_err)?;
        let skip_kv = skip_claim_kv_from_env();
        let mut messages = batch;
        let mut claimed = VecDeque::new();
        while let Some(msg) = messages.next().await {
            let msg = msg.map_err(map_err)?;
            let Some(mut job) = self.resolve_job_from_payload(&msg.payload).await? else {
                msg.ack().await.map_err(map_err)?;
                continue;
            };
            job.status = JobStatus::Running;
            if !skip_kv {
                self.save_job(&job).await?;
            }
            let job_id = job.job_id.clone();
            self.pending_acks.lock().await.insert(job_id, msg);
            claimed.push_back(job);
        }
        if !claimed.is_empty() {
            self.claim_buffers
                .lock()
                .await
                .entry(pool.to_string())
                .or_default()
                .extend(claimed);
        }
        Ok(())
    }
}

fn map_err(e: impl std::fmt::Display) -> BosonError {
    BosonError::Backend(format!("nats workqueue: {e}"))
}

#[async_trait]
impl QueueBackend for NatsWorkQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        if matches!(
            job.status,
            JobStatus::Success | JobStatus::Failed | JobStatus::Canceled
        ) {
            self.ack_job(&job.job_id).await?;
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
                    if let Some(bytes) = self.kv_get(&self.keys.idempotency(key)).await? {
                        let prior_id = String::from_utf8_lossy(&bytes).into_owned();
                        if let Some(prior) = self.load_job(&prior_id).await? {
                            if matches!(prior.status, JobStatus::Queued | JobStatus::Running) {
                                return Ok((prior_id, JobEnqueueDisposition::ReusedIdempotent));
                            }
                        }
                    }
                    self.kv_put(&self.keys.idempotency(key), job.job_id.as_bytes())
                        .await?;
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
        match self.config.enqueue_mode {
            EnqueueMode::Stream => {
                self.publish_job(&job).await?;
                if self.config.sync_kv_mirror {
                    self.save_job(&job).await?;
                } else {
                    self.mirror_job_async(&job);
                }
            }
            EnqueueMode::Dual => {
                self.save_job(&job).await?;
                self.publish_job(&job).await?;
            }
        }
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
        let mut stream = self.kv.keys().await.map_err(map_err)?;
        let mut jobs = Vec::new();
        while let Some(key) = stream.next().await {
            let key = key.map_err(map_err)?;
            if !key.starts_with(&prefix) {
                continue;
            }
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
        self.publish_job(&job).await
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        let mut pools: Vec<String> = self.active_pools.lock().await.iter().cloned().collect();
        pools.sort();
        Ok(pools)
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        let limit = limit.max(1);
        let jobs = self
            .list_jobs(Some(JobStatus::Queued), 0, usize::MAX)
            .await?;
        let mut queued: Vec<Job> = jobs.into_iter().filter(|j| j.pool == pool).collect();
        queued.sort_by(|a, b| {
            a.priority
                .cmp(&b.priority)
                .then_with(|| a.created_at.cmp(&b.created_at))
        });
        queued.truncate(limit);
        Ok(queued)
    }

    async fn pop_claim_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        if fetch_batch_from_env() == 1 {
            return self.claim_one_from_pool(pool).await;
        }
        let buffered_job = self
            .claim_buffers
            .lock()
            .await
            .get_mut(pool)
            .and_then(VecDeque::pop_front);
        if let Some(job) = buffered_job {
            return Ok(Some(job));
        }
        let _refill = self.claim_refill.lock().await;
        let buffered_job = self
            .claim_buffers
            .lock()
            .await
            .get_mut(pool)
            .and_then(VecDeque::pop_front);
        if let Some(job) = buffered_job {
            return Ok(Some(job));
        }
        self.refill_claim_buffer(pool).await?;
        Ok(self
            .claim_buffers
            .lock()
            .await
            .get_mut(pool)
            .and_then(VecDeque::pop_front))
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        let jobs = self.list_jobs(status_filter, 0, usize::MAX).await?;
        Ok(u64::try_from(jobs.len()).unwrap_or(u64::MAX))
    }

    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> Result<u64> {
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
        let mut stream = self.kv.keys().await.map_err(map_err)?;
        let mut runs = Vec::new();
        while let Some(key) = stream.next().await {
            let key = key.map_err(map_err)?;
            if !key.starts_with(&prefix) {
                continue;
            }
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
        if self
            .kv_get(&self.keys.lease_by_job(job_id))
            .await?
            .is_some()
        {
            return Ok(None);
        }
        let lease_id = Uuid::new_v4().to_string();
        let row = LeaseRow {
            lease_id: lease_id.clone(),
            job_id: job_id.to_string(),
            worker_id: worker_id.to_string(),
            expires_at: Utc::now() + chrono::Duration::seconds(ttl_secs),
        };
        let bytes = serde_json::to_vec(&row).map_err(map_err)?;
        self.kv_put(&self.keys.lease_by_job(job_id), lease_id.as_bytes())
            .await?;
        self.kv_put(&self.keys.lease(&lease_id), &bytes).await?;
        Ok(Some(lease_id))
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        let Some(mut row) = self.load_lease_row(lease_id).await? else {
            return Ok(());
        };
        row.expires_at = Utc::now() + chrono::Duration::seconds(ttl_secs);
        let bytes = serde_json::to_vec(&row).map_err(map_err)?;
        self.kv_put(&self.keys.lease(lease_id), &bytes).await
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        if let Some(row) = self.load_lease_row(lease_id).await? {
            self.kv_delete(&self.keys.lease_by_job(&row.job_id)).await?;
        }
        self.kv_delete(&self.keys.lease(lease_id)).await
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

impl NatsWorkQueueBackend {
    async fn load_lease_row(&self, lease_id: &str) -> Result<Option<LeaseRow>> {
        let raw = self.kv_get(&self.keys.lease(lease_id)).await?;
        raw.map_or(Ok(None), |bytes| {
            serde_json::from_slice(&bytes).map_err(map_err).map(Some)
        })
    }
}

/// Connect KV or `WorkQueue` backend based on `BOSON_NATS_QUEUE_MODE`.
///
/// # Errors
///
/// Returns an error when the selected backend cannot connect or initialize.
pub async fn connect_auto(url: &str) -> Result<Arc<dyn QueueBackend>> {
    match std::env::var("BOSON_NATS_QUEUE_MODE")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "workqueue" | "wq" => Ok(Arc::new(NatsWorkQueueBackend::connect(url).await?)),
        _ => Ok(Arc::new(crate::NatsQueueBackend::connect(url).await?)),
    }
}
