//! [`QueueBackend`] — queue persistence trait for custom storage adapters.
//!
//! **App authors:** pick a shipped backend by topology (mem = Mode 1 only; `SQLite`/Postgres/Redis/NATS
//! for shared queues) using the [`boson`](https://docs.rs/uf-boson) crate
//! [backend table](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary).
//!
//! **Adapter authors:** see **How to implement** on [`QueueBackend`] for a step-by-step guide and
//! reference adapters.

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::error::Result;
use crate::models::{Job, JobStatus, Run, RunStatus, TaskConfig, TaskRunStats};

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
/// // Integrator wiring (see boson crate Getting started — Mode 1 / Mode 2):
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
