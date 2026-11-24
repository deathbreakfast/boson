//! [`QueueBackend`] — queue persistence trait for custom storage adapters.
//!
//! **App authors:** pick a shipped backend by topology (mem = embedded only; `SQLite`/Postgres/Redis/NATS
//! for shared queues) using the [`boson`](https://docs.rs/uf-boson) crate
//! [backend table](https://docs.rs/uf-boson/latest/boson/index.html#embedded-one-binary).
//!
//! **Adapter authors:** see **How to implement** on [`QueueBackend`] for a step-by-step guide and
//! reference adapters.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use serde_json::Value;

use super::filter::{JobFilter, RunFilter};
use super::keyset::{page_jobs_after, page_runs_after, JobKey, RunKey};
use super::stats::tally_pool_stats;
use crate::error::{BosonError, Result};
use crate::models::{
    Job, JobStatus, PoolState, PoolStats, Run, RunStatus, TaskConfig, TaskRunStats,
};

/// Default router key for single-backend hosts.
pub const DEFAULT_BACKEND_NAME: &str = "default";

/// Whether [`QueueBackend::enqueue_with_policies`] inserted or reused a job.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobEnqueueDisposition {
    /// A new job row was inserted.
    InsertedNew,
    /// An existing non-terminal job matched the idempotency key.
    ReusedIdempotent,
}

/// Stable async trait for queue persistence (jobs, runs, config, leases).
///
/// Implement this trait in a separate adapter crate (for example `myorg-boson-backend-redis`) using
/// only DTOs from `boson-core`. The runtime holds `Arc<dyn QueueBackend>` and calls these methods
/// from the worker loop, enqueue path, and HTTP admin handlers.
///
/// # How to implement
///
/// 1. **Create an adapter crate** — depend on `boson-core`, `async-trait`, and your storage client.
/// 2. **Map DTOs to storage** — persist [`Job`], [`Run`], [`TaskConfig`], and lease records using
///    the types in [`crate::models`]; do not invent parallel schemas.
/// 3. **Implement every method** — group by jobs, runs, task config, and leases (see below).
/// 4. **Honor the contract** — especially idempotent enqueue, atomic claim, and lease exclusivity.
/// 5. **Wire at boot** — pass `Arc::new(YourBackend::new(...))` to
///    [`boson_runtime::BosonBuilder::queue_backend`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.queue_backend).
/// 6. **Validate** — copy integration tests from [`MemQueueBackend`](https://docs.rs/boson-backend-mem/latest/boson_backend_mem/struct.MemQueueBackend.html)
///    or [`SqlQueueBackend`](https://docs.rs/boson-backend-sql-common/latest/boson_backend_sql_common/struct.SqlQueueBackend.html).
///
/// # Method groups
///
/// | Group | Key methods | Notes |
/// |-------|-------------|-------|
/// | **Jobs** | [`enqueue_with_policies`](Self::enqueue_with_policies), [`try_claim_job`](Self::try_claim_job), [`list_queued_for_pool_sorted`](Self::list_queued_for_pool_sorted) | Apply [`TaskConfig`] rate limits on enqueue; claim must be atomic |
/// | **Runs** | [`upsert_run`](Self::upsert_run), [`finish_run`](Self::finish_run), [`task_run_stats`](Self::task_run_stats) | One run row per execution attempt |
/// | **Task config** | [`get_task_config`](Self::get_task_config), [`upsert_task_config`](Self::upsert_task_config) | Optional per-task overrides stored by the runtime |
/// | **Admin** (optional) | [`retry_job`](Self::retry_job), [`drop_failed_job`](Self::drop_failed_job), [`sweep_failed`](Self::sweep_failed), [`pool_stats`](Self::pool_stats), [`set_pool_paused`](Self::set_pool_paused), [`purge_queued_in_pool`](Self::purge_queued_in_pool) | Default bodies work through the methods above; sweep, pause and purge need storage support and return an error until an engine overrides them |
/// | **Leases** | [`try_claim_run_lease`](Self::try_claim_run_lease), [`extend_lease`](Self::extend_lease), [`expired_lease_job_pairs`](Self::expired_lease_job_pairs) | Required for multi-worker deploys; may no-op for single-node dev |
///
/// # Reference implementations
///
/// - [`MemQueueBackend`](https://docs.rs/boson-backend-mem/latest/boson_backend_mem/struct.MemQueueBackend.html) — in-memory, ideal starting point (~200 lines per module)
/// - [`SqlQueueBackend`](https://docs.rs/boson-backend-sql-common/latest/boson_backend_sql_common/struct.SqlQueueBackend.html) — shared SQL for `SQLite`/`PostgreSQL`
///
/// # Example
///
/// Skeleton only — every trait method must be implemented (see reference adapters above):
///
/// ```ignore
/// use std::sync::Arc;
///
/// use async_trait::async_trait;
/// use boson_core::{Job, JobEnqueueDisposition, QueueBackend, Result, TaskConfig};
/// use boson_runtime::Boson;
///
/// #[derive(Debug)]
/// pub struct MyQueueBackend {
///     // pool, client, or in-memory store
/// }
///
/// #[async_trait]
/// impl QueueBackend for MyQueueBackend {
///     async fn enqueue_with_policies(
///         &self,
///         job: Job,
///         task_config: &TaskConfig,
///     ) -> Result<(String, JobEnqueueDisposition)> {
///         // insert job; honor idempotency_key and rate limits from task_config
///         todo!()
///     }
///
///     async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
///         // atomically queued -> running, or return None
///         todo!()
///     }
///
///     // ... implement all remaining QueueBackend methods
/// }
///
/// // Integrator wiring (see boson crate Getting started — Embedded / Remote worker):
/// let boson = Boson::builder()
///     .queue_backend(Arc::new(MyQueueBackend { /* ... */ }))
///     .execution_context_factory(boson_core::JsonExecutionContextFactory)
///     .auto_registry()
///     .build()?;
/// ```
///
/// # Contract
///
/// - Implementations must be `Send + Sync`; hosts hold `Arc<dyn QueueBackend>`.
/// - Idempotency: when `job.idempotency_key` is set, `enqueue_with_policies` returns
///   [`ReusedIdempotent`](JobEnqueueDisposition::ReusedIdempotent) with the existing job id
///   if a non-terminal job already exists for that key.
/// - Claim: `try_claim_job` must atomically transition `queued` → `running` or return `None`.
/// - Leases: `try_claim_run_lease` returns `None` when another worker holds an active lease.
#[async_trait]
pub trait QueueBackend: Send + Sync + Debug {
    // --- Jobs ---

    /// Persist or update a job row.
    async fn upsert_job(&self, job: &Job) -> Result<()>;

    /// Insert job with idempotency semantics (see trait-level contract).
    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)>;

    /// Load one job by id.
    async fn get_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// List jobs with optional status filter and pagination.
    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>>;

    /// List up to `limit` jobs matching `filter` that sort strictly after `after`, oldest first.
    ///
    /// Order is `created_at ASC, job_id ASC`. A job comes after the key when its `created_at`
    /// is later than `after.created_at`, or equal with a greater `job_id`, so two jobs created in
    /// the same instant still page in a fixed order. `after: None` starts at the oldest job.
    /// `filter` narrows the scan (see [`JobFilter`]); the default filter leaves out dropped jobs.
    ///
    /// To page, pass `JobKey::from(last)` for the last job of each non-empty page; an empty page
    /// means the scan is done. Each job appears once across the pages, with no `OFFSET` drift
    /// when jobs are enqueued mid-scan: [`Job::new`] stamps `created_at` with the current time,
    /// so a new job sorts after every key already returned and shows up at the end of the scan.
    /// A job whose status changes mid-scan keeps its position; with a status filter, it shows
    /// up only if it matches when its page is read.
    ///
    /// Engines compare `job_id` with their own text ordering (byte order for in-memory engines
    /// and `SQLite`, the column collation on Postgres). Paging stays consistent within one engine.
    ///
    /// The default body loads every job through [`Self::list_jobs`] and pages them with
    /// [`page_jobs_after`], which is O(n) per page. Engines should override it with an indexed
    /// query.
    ///
    /// # Errors
    ///
    /// Returns the same storage errors as [`Self::list_jobs`].
    async fn list_jobs_after(
        &self,
        filter: &JobFilter,
        after: Option<&JobKey>,
        limit: usize,
    ) -> Result<Vec<Job>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let all = self
            .list_jobs(single_job_status(filter), 0, usize::MAX)
            .await?;
        Ok(page_jobs_after(all, filter, after, limit))
    }

    /// Count the jobs matching `filter`: the total for a [`Self::list_jobs_after`] scan.
    ///
    /// The default body loads every job through [`Self::list_jobs`]; engines should override it.
    ///
    /// # Errors
    ///
    /// Returns the same storage errors as [`Self::list_jobs`].
    async fn count_jobs_matching(&self, filter: &JobFilter) -> Result<u64> {
        let all = self
            .list_jobs(single_job_status(filter), 0, usize::MAX)
            .await?;
        let count = all.iter().filter(|j| filter.matches(j)).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    /// Cancel a job if it is still active (`queued` or `running`).
    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()>;

    /// Atomically claim a queued job for execution.
    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>>;

    /// Revert a job to `queued` (retry path).
    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()>;

    /// Distinct pool names with queued jobs.
    async fn distinct_pools_queued(&self) -> Result<Vec<String>>;

    /// Queued jobs for one pool, sorted by priority then created time.
    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>>;

    /// Atomically claim the highest-priority queued job from `pool` when supported.
    ///
    /// Backends that support single-round-trip claim (Redis, NATS `WorkQueue`) override this to
    /// pop and claim in one operation. The default returns `None` so callers fall back to
    /// [`Self::list_queued_for_pool_sorted`] + [`Self::try_claim_job`].
    async fn pop_claim_from_pool(&self, _pool: &str) -> Result<Option<Job>> {
        Ok(None)
    }

    /// Count jobs, optionally filtered by status.
    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64>;

    /// Count jobs for one task, optionally filtered by status.
    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> Result<u64>;

    /// Count active (`queued` + `running`) jobs for rate-limit checks.
    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32>;

    /// Find non-terminal job by idempotency key.
    ///
    /// Used by [`Self::enqueue_with_policies`] to return
    /// [`JobEnqueueDisposition::ReusedIdempotent`](JobEnqueueDisposition::ReusedIdempotent)
    /// when a job with the same key is still `queued` or `running`. Returns `None` when no
    /// matching non-terminal job exists.
    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>>;

    // --- Runs ---

    /// Persist or update a run row.
    async fn upsert_run(&self, run: &Run) -> Result<()>;

    /// Load one run by id.
    async fn get_run(&self, run_id: &str) -> Result<Option<Run>>;

    /// List runs with optional job filter and pagination.
    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>>;

    /// List up to `limit` runs matching `filter` that sort strictly after `after`, newest first.
    ///
    /// Order is `started_at DESC, run_id DESC`. A run comes after the key when its `started_at`
    /// is earlier than `after.started_at`, or equal with a smaller `run_id`, so two runs started
    /// in the same instant still page in a fixed order. `after: None` starts at the newest run.
    /// `filter` narrows the scan (see [`RunFilter`]).
    ///
    /// To page, pass `RunKey::from(last)` for the last run of each non-empty page; an empty page
    /// means the scan is done. Each run appears once across the pages. A run that starts
    /// mid-scan has a later `started_at` than every key already returned, so it sorts before the
    /// cursor and the scan does not return it; start a new scan with `after: None` to see it.
    ///
    /// Engines compare `run_id` with their own text ordering (byte order for in-memory engines
    /// and `SQLite`, the column collation on Postgres). Paging stays consistent within one engine.
    ///
    /// This order is newest first on every engine, including those whose [`Self::list_runs`]
    /// returns oldest first.
    ///
    /// The default body loads the runs through [`Self::list_runs`] and pages them with
    /// [`page_runs_after`], which is O(n) per page. Engines should override it with an indexed
    /// query.
    ///
    /// # Errors
    ///
    /// Returns the same storage errors as [`Self::list_runs`].
    async fn list_runs_after(
        &self,
        filter: &RunFilter,
        after: Option<&RunKey>,
        limit: usize,
    ) -> Result<Vec<Run>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let all = self
            .list_runs(filter.job_id.as_deref(), 0, usize::MAX)
            .await?;
        Ok(page_runs_after(all, filter, after, limit))
    }

    /// Count the runs matching `filter`: the total for a [`Self::list_runs_after`] scan.
    ///
    /// The default body loads the runs through [`Self::list_runs`]; engines should override it.
    ///
    /// # Errors
    ///
    /// Returns the same storage errors as [`Self::list_runs`].
    async fn count_runs_matching(&self, filter: &RunFilter) -> Result<u64> {
        let all = self
            .list_runs(filter.job_id.as_deref(), 0, usize::MAX)
            .await?;
        let count = all.iter().filter(|r| filter.matches(r)).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    /// Mark a run terminal with outcome fields.
    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()>;

    /// Count runs, optionally filtered by job id.
    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64>;

    /// Count runs with `started_at >= since`.
    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64>;

    /// Aggregate run totals for one task.
    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats>;

    // --- Task config ---

    /// Load task config by name.
    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>>;

    /// Persist task config.
    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()>;

    // --- Admin: failed jobs and pools ---

    /// Enqueue a fresh copy of a failed job and return the new job id.
    ///
    /// The new job carries the failed job's `params_json`, `signature_hash` and
    /// `idempotency_key`, starts at attempt 1, runs as `actor_json` (the person who asked for the
    /// retry), and links back through [`Job::retried_from`]. Priority, pool and policies come
    /// from the task type's current stored [`TaskConfig`], falling back to the failed job's
    /// priority and pool when none is stored. It goes through [`Self::enqueue_with_policies`],
    /// so rate limits and idempotency apply. The failed job and its runs stay as they are.
    ///
    /// The default body works on every engine through the methods above.
    ///
    /// # Errors
    ///
    /// [`BosonError::JobNotFound`] for an unknown id, [`BosonError::InvalidConfig`] when the job
    /// is not `failed`, [`BosonError::RateLimited`] from the task's limits, or storage errors.
    async fn retry_job(&self, job_id: &str, actor_json: Value) -> Result<String> {
        let old = self
            .get_job(job_id)
            .await?
            .ok_or_else(|| BosonError::JobNotFound(job_id.to_string()))?;
        if old.status != JobStatus::Failed {
            return Err(BosonError::InvalidConfig(format!(
                "job {job_id} is {}; only failed jobs can be retried",
                old.status
            )));
        }
        let stored = self.get_task_config(&old.task_name).await?;
        let config = stored.unwrap_or_else(|| {
            let mut config = TaskConfig::default_for(&old.task_name);
            config.priority = old.priority;
            config.pool.clone_from(&old.pool);
            config
        });
        let mut job = Job::new(
            &old.task_name,
            actor_json,
            old.params_json,
            config.priority,
            &config.pool,
            old.signature_hash,
            old.idempotency_key,
        );
        job.retried_from = Some(old.job_id);
        let (new_id, _) = self.enqueue_with_policies(job, &config).await?;
        Ok(new_id)
    }

    /// Mark a failed job dropped by setting [`Job::dropped_at`]. Dropping twice is a no-op.
    ///
    /// The job stays stored, so a list with [`JobFilter::include_dropped`] can still show it;
    /// [`Self::sweep_failed`] is the only hard delete.
    ///
    /// The default body loads the job, sets the field and writes it back with
    /// [`Self::upsert_job`], then reads it again to check the engine kept the field.
    ///
    /// # Errors
    ///
    /// [`BosonError::JobNotFound`] for an unknown id, [`BosonError::InvalidConfig`] when the job
    /// is not `failed`, or a backend error when the engine does not store `dropped_at`.
    async fn drop_failed_job(&self, job_id: &str) -> Result<()> {
        let mut job = self
            .get_job(job_id)
            .await?
            .ok_or_else(|| BosonError::JobNotFound(job_id.to_string()))?;
        if job.status != JobStatus::Failed {
            return Err(BosonError::InvalidConfig(format!(
                "job {job_id} is {}; only failed jobs can be dropped",
                job.status
            )));
        }
        if job.dropped_at.is_some() {
            return Ok(());
        }
        job.dropped_at = Some(Utc::now());
        self.upsert_job(&job).await?;
        let stored = self.get_job(job_id).await?;
        if stored.is_some_and(|j| j.dropped_at.is_some()) {
            Ok(())
        } else {
            Err(unsupported("drop_failed_job"))
        }
    }

    /// Delete `failed` jobs enqueued before `older_than` and return how many were removed.
    ///
    /// Jobs don't record when they failed, so a job's age is its `created_at`. Runs are kept.
    /// The host picks the age and the schedule; [`DEFAULT_FAILED_RETENTION`](crate::DEFAULT_FAILED_RETENTION)
    /// is the suggested age.
    ///
    /// # Errors
    ///
    /// The default body returns a backend error: the trait has no delete method to build it on.
    async fn sweep_failed(&self, older_than: DateTime<Utc>) -> Result<u64> {
        let _ = older_than;
        Err(unsupported("sweep_failed"))
    }

    /// Per-pool numbers, sorted by pool name.
    ///
    /// Covers every pool that has jobs, is named in a stored task config, or has a stored
    /// [`PoolState`]. The window is `now - window ..= now`: `added_in_window` counts jobs created
    /// in it and `done_in_window` counts jobs in a finished status with a run that finished in
    /// it.
    ///
    /// The default body loads every job and run and reads [`Self::get_pool_state`] per pool; it
    /// only sees pools that have jobs.
    ///
    /// # Errors
    ///
    /// Returns storage errors from the methods it reads through.
    async fn pool_stats(&self, window: chrono::Duration) -> Result<Vec<PoolStats>> {
        let since = Utc::now() - window;
        let jobs = self.list_jobs(None, 0, usize::MAX).await?;
        let runs = self.list_runs(None, 0, usize::MAX).await?;
        let mut stats = tally_pool_stats(&jobs, &runs, since);
        for entry in stats.values_mut() {
            entry.paused = self
                .get_pool_state(&entry.pool)
                .await?
                .is_some_and(|s| s.paused);
        }
        Ok(stats.into_values().collect())
    }

    /// Pause or resume claims from `pool`, recording `actor_label` and the time.
    ///
    /// While a pool is paused, [`Self::try_claim_job`] and [`Self::pop_claim_from_pool`] skip
    /// its jobs. Running jobs finish, and [`Self::enqueue_with_policies`] still accepts new jobs
    /// into it. Returns the stored state.
    ///
    /// # Errors
    ///
    /// The default body returns a backend error: pausing needs storage the trait doesn't have.
    async fn set_pool_paused(
        &self,
        pool: &str,
        paused: bool,
        actor_label: &str,
    ) -> Result<PoolState> {
        let _ = (pool, paused, actor_label);
        Err(unsupported("set_pool_paused"))
    }

    /// Stored pause state for `pool`, or `None` when it was never set.
    ///
    /// # Errors
    ///
    /// Returns storage errors. The default body returns `Ok(None)`.
    async fn get_pool_state(&self, pool: &str) -> Result<Option<PoolState>> {
        let _ = pool;
        Ok(None)
    }

    /// Delete the `queued` jobs in `pool` and return how many were removed. Running and
    /// finished jobs stay.
    ///
    /// # Errors
    ///
    /// The default body returns a backend error: the trait has no delete method to build it on.
    async fn purge_queued_in_pool(&self, pool: &str) -> Result<u64> {
        let _ = pool;
        Err(unsupported("purge_queued_in_pool"))
    }

    // --- Leases (distributed workers) ---
    //
    // Required when multiple worker processes share one backend (`BOSON_LEASE_TTL_SECS` > 0).
    // Single-node dev backends may no-op these methods.

    /// Attempt to claim a run lease for `job_id` (stored as lease task id).
    ///
    /// Returns `Some(lease_id)` when this worker holds the lease, or `None` when another worker
    /// holds an active lease for the same job.
    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>>;

    /// Extend lease TTL for a held lease.
    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()>;

    /// Release a held lease.
    async fn release_lease(&self, lease_id: &str) -> Result<()>;

    /// Expired leases as `(lease_record_id, job_id)`.
    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>>;
}

/// The one status a filter names, so default bodies can narrow [`QueueBackend::list_jobs`].
fn single_job_status(filter: &JobFilter) -> Option<JobStatus> {
    match filter.statuses.as_slice() {
        [status] => Some(*status),
        _ => None,
    }
}

/// Error for an admin method an engine doesn't implement.
fn unsupported(method: &str) -> BosonError {
    BosonError::backend(format!("{method} is not supported by this queue backend"))
}

/// Resolve the default backend from the process-global [`QueueRouter`](crate::QueueRouter).
///
/// # Errors
///
/// Returns [`BosonError::Internal`](crate::BosonError::Internal) when the global router
/// was not installed via [`QueueRouter::set_global`](crate::QueueRouter::set_global).
/// Propagates [`QueueRouter::resolve`](crate::QueueRouter::resolve) errors for the default name.
pub fn default_backend_from_global() -> Result<Arc<dyn QueueBackend>> {
    let router = crate::QueueRouter::global()?;
    router.resolve(DEFAULT_BACKEND_NAME)
}
