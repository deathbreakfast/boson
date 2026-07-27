//! Minimal [`QueueBackend`] stub: wrap [`MemQueueBackend`], override a couple of methods, and
//! delegate the rest — **Extending storage** (custom adapter sketch).
//!
//! ```bash
//! cargo run -p uf-boson --example custom_queue_backend_stub --features mem
//! ```
//!
//! Real adapters (`boson-backend-sqlite`, `-postgres`, `-redis`, `-nats`) implement every method
//! directly against their substrate. This sketch shows the **decorator** shape instead: wrap
//! any `Arc<dyn QueueBackend>` and intercept only the calls you care about (validation, auditing)
//! without reimplementing persistence. See `boson-core` [`QueueBackend`] rustdoc (**How to
//! implement**) and `task_macro` for the default mem wiring.

#![allow(clippy::print_stderr)]

use std::fmt::Debug;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use boson::{
    configure, task, Boson, BosonError, Job, JobEnqueueDisposition, JobStatus,
    JsonExecutionContextFactory, MemQueueBackend, QueueBackend, Run, RunStatus, TaskConfig,
    TaskRunStats,
};
use boson_core::Result;
use chrono::{DateTime, Utc};

/// Decorator over any [`QueueBackend`] adding task-name validation and enqueue auditing.
///
/// Delegates every method except [`Self::enqueue_with_policies`] (rejects blank task names and
/// counts inserts) — the minimal shape for a custom adapter that only needs to intercept enqueue.
struct AuditingQueueBackend {
    inner: Arc<dyn QueueBackend>,
    enqueues: AtomicUsize,
}

impl AuditingQueueBackend {
    fn new(inner: Arc<dyn QueueBackend>) -> Self {
        Self {
            inner,
            enqueues: AtomicUsize::new(0),
        }
    }
}

impl Debug for AuditingQueueBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditingQueueBackend")
            .field("enqueues", &self.enqueues.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl QueueBackend for AuditingQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        self.inner.upsert_job(job).await
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        if job.task_name.trim().is_empty() {
            return Err(BosonError::internal("task_name must not be blank"));
        }
        eprintln!(
            "auditing_backend: enqueue_with_policies task_name={}",
            job.task_name
        );
        let result = self.inner.enqueue_with_policies(job, task_config).await?;
        if matches!(result.1, JobEnqueueDisposition::InsertedNew) {
            self.enqueues.fetch_add(1, Ordering::SeqCst);
        }
        Ok(result)
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.inner.get_job(job_id).await
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        self.inner.list_jobs(status_filter, offset, limit).await
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        self.inner.cancel_job_if_active(job_id).await
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.inner.try_claim_job(job_id).await
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        self.inner.revert_job_to_queued(job_id).await
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        self.inner.distinct_pools_queued().await
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        self.inner.list_queued_for_pool_sorted(pool, limit).await
    }

    async fn pop_claim_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        self.inner.pop_claim_from_pool(pool).await
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        self.inner.count_jobs(status_filter).await
    }

    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> Result<u64> {
        self.inner.count_jobs_for_task(task_name, status).await
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        self.inner.count_active_jobs_for_task(task_name).await
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        self.inner.find_nonterminal_by_idempotency_key(key).await
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        self.inner.upsert_run(run).await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.inner.get_run(run_id).await
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.inner.list_runs(job_id_filter, offset, limit).await
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        self.inner
            .finish_run(run_id, status, duration_ms, error_message)
            .await
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        self.inner.count_runs(job_id_filter).await
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        self.inner.count_runs_since(since).await
    }

    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        self.inner.task_run_stats(task_name).await
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        self.inner.get_task_config(task_name).await
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        self.inner.upsert_task_config(config).await
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        self.inner
            .try_claim_run_lease(job_id, worker_id, ttl_secs)
            .await
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        self.inner.extend_lease(lease_id, ttl_secs).await
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        self.inner.release_lease(lease_id).await
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        self.inner.expired_lease_job_pairs().await
    }
}

#[task(name = "audited_greet")]
#[allow(clippy::unused_async)] // `#[task]` requires async handlers.
async fn audited_greet(
    ctx: Box<dyn boson::ExecutionContext>,
    name: String,
) -> boson_core::Result<()> {
    eprintln!("audited_greet {name} (actor={})", ctx.label());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let inner: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    let backend = Arc::new(AuditingQueueBackend::new(Arc::clone(&inner)));

    // Fail-closed proof: a blank task_name is rejected before it reaches the wrapped backend.
    let blank = Job::new(
        "",
        serde_json::json!({"System": {"operation": "demo"}}),
        serde_json::json!({}),
        0,
        "global",
        0,
        None,
    );
    assert!(backend
        .enqueue_with_policies(blank, &TaskConfig::default_for("audited_greet"))
        .await
        .is_err());

    let (boson, manual) = Boson::builder()
        .queue_backend(backend.clone() as Arc<dyn QueueBackend>)
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .without_worker()
        .build_manual()?;

    configure(boson.clone());

    AuditedGreet::send_with(
        serde_json::json!({"System": {"operation": "demo"}}),
        AuditedGreetParams {
            name: "world".into(),
        },
    )
    .await?;

    assert!(manual.try_run_next().await);
    let enqueues = backend.enqueues.load(Ordering::SeqCst);
    assert!(enqueues >= 1, "expected at least one audited enqueue");

    eprintln!(
        "custom_queue_backend_stub: blank task_name rejected; drained job through AuditingQueueBackend (enqueues={enqueues})"
    );
    Ok(())
}
