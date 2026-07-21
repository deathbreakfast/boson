//! In-memory [`QueueBackend`](boson_core::QueueBackend) implementation.

use std::sync::RwLock;

use async_trait::async_trait;
use boson_core::{
    Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run, RunStatus, TaskConfig,
    TaskRunStats,
};
use chrono::{DateTime, Utc};

use crate::enqueue_rate::EnqueueRateLimiter;
use crate::store::{read, write, write_fallible, Inner};

/// Process-local queue backend (not durable).
///
/// **When to use:** Mode 1 embedded apps, CI, and unit tests. **Not** for Mode 2 remote workers —
/// another process cannot see this memory. Prefer
/// [`SqliteQueueBackend`](https://docs.rs/boson-backend-sqlite) or
/// [`PostgresQueueBackend`](https://docs.rs/boson-backend-postgres) when processes share a queue.
///
/// Getting started:
/// [Mode 1](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary).
///
/// Thread-safe via [`std::sync::RwLock`].
///
/// The lock is held only inside synchronous `store::{read,write,write_fallible}` helpers — never
/// across `.await` points — so Tokio's async `RwLock` is unnecessary for this in-memory adapter.
#[derive(Debug)]
pub struct MemQueueBackend {
    inner: RwLock<Inner>,
    enqueue_rate: EnqueueRateLimiter,
}

impl Default for MemQueueBackend {
    fn default() -> Self {
        Self::new()
    }
}

impl MemQueueBackend {
    /// New empty backend.
    ///
    /// # Examples
    ///
    /// Wire into [`Boson::builder`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.Boson.html#method.builder)
    /// (feature `mem` on the `boson` facade):
    ///
    /// ```rust
    /// use std::sync::Arc;
    ///
    /// use boson_backend_mem::MemQueueBackend;
    /// use boson_core::QueueBackend;
    ///
    /// let backend: Arc<dyn QueueBackend> = Arc::new(MemQueueBackend::new());
    /// let _ = backend;
    /// ```
    ///
    /// Full boot with [`Boson`](https://docs.rs/boson-runtime):
    /// `Boson::builder().queue_backend(Arc::new(MemQueueBackend::new()))…`.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(Inner::new()),
            enqueue_rate: EnqueueRateLimiter::new(),
        }
    }
}

#[async_trait]
impl QueueBackend for MemQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        write(&self.inner, |inner| crate::jobs::upsert_job(inner, job))
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        write_fallible(&self.inner, |inner| {
            crate::jobs::enqueue_with_policies(inner, &self.enqueue_rate, &job, task_config)
        })
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        read(&self.inner, |inner| crate::jobs::get_job(inner, job_id))
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        read(&self.inner, |inner| {
            crate::jobs::list_jobs(inner, status_filter, offset, limit)
        })
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        write_fallible(&self.inner, |inner| {
            crate::jobs::cancel_job_if_active(inner, job_id)
        })
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        write(&self.inner, |inner| {
            crate::jobs::try_claim_job(inner, job_id)
        })
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        write(&self.inner, |inner| {
            crate::jobs::revert_job_to_queued(inner, job_id);
        })
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        read(&self.inner, crate::jobs::distinct_pools_queued)
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        read(&self.inner, |inner| {
            crate::jobs::list_queued_for_pool_sorted(inner, pool, limit)
        })
    }

    async fn pop_claim_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        write(&self.inner, |inner| {
            crate::jobs::pop_claim_from_pool(inner, pool)
        })
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        read(&self.inner, |inner| {
            crate::jobs::count_jobs(inner, status_filter)
        })
    }

    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> Result<u64> {
        read(&self.inner, |inner| {
            crate::jobs::count_jobs_for_task(inner, task_name, status)
        })
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        read(&self.inner, |inner| {
            crate::jobs::count_active_jobs_for_task(inner, task_name)
        })
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        read(&self.inner, |inner| {
            crate::jobs::find_nonterminal_by_idempotency_key(inner, key)
        })
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        write(&self.inner, |inner| crate::runs::upsert_run(inner, run))
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        read(&self.inner, |inner| crate::runs::get_run(inner, run_id))
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        read(&self.inner, |inner| {
            crate::runs::list_runs(inner, job_id_filter, offset, limit)
        })
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        write(&self.inner, |inner| {
            crate::runs::finish_run(inner, run_id, status, duration_ms, error_message);
        })
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        read(&self.inner, |inner| {
            crate::runs::count_runs(inner, job_id_filter)
        })
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        read(&self.inner, |inner| {
            crate::runs::count_runs_since(inner, since)
        })
    }

    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        read(&self.inner, |inner| {
            crate::runs::task_run_stats(inner, task_name)
        })
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        read(&self.inner, |inner| {
            crate::task_config::get_task_config(inner, task_name)
        })
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        write(&self.inner, |inner| {
            crate::task_config::upsert_task_config(inner, config);
        })
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        write(&self.inner, |inner| {
            crate::leases::try_claim_run_lease(inner, job_id, worker_id, ttl_secs)
        })
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        write(&self.inner, |inner| {
            crate::leases::extend_lease(inner, lease_id, ttl_secs);
        })
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        write(&self.inner, |inner| {
            crate::leases::release_lease(inner, lease_id);
        })
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        read(&self.inner, crate::leases::expired_lease_job_pairs)
    }
}
