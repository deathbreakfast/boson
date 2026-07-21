//! Main Boson runtime type.

use std::sync::Arc;

use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run,
    TaskConfig, TaskRunStats,
};
use chrono::{DateTime, Utc};

use crate::registry::TaskRegistry;
use crate::worker::WorkerSettings;

/// Boson work engine — enqueue, admin reads, and worker orchestration.
#[derive(Clone)]
pub struct Boson {
    pub(crate) backend: Arc<dyn QueueBackend>,
    pub(crate) registry: Arc<TaskRegistry>,
    worker: WorkerSettings,
    /// Runtime default when [`TaskConfig::idempotency_mode`] is unset.
    idempotency_mode: IdempotencyMode,
}

impl Boson {
    /// Construct from injected parts (used by builder and tests).
    pub fn from_parts(
        backend: Arc<dyn QueueBackend>,
        registry: Arc<TaskRegistry>,
        worker: WorkerSettings,
    ) -> Self {
        Self::from_parts_with_idempotency(backend, registry, worker, IdempotencyMode::Lwt)
    }

    /// Construct with an explicit default idempotency mode.
    pub fn from_parts_with_idempotency(
        backend: Arc<dyn QueueBackend>,
        registry: Arc<TaskRegistry>,
        worker: WorkerSettings,
        idempotency_mode: IdempotencyMode,
    ) -> Self {
        Self {
            backend,
            registry,
            worker,
            idempotency_mode,
        }
    }

    /// Runtime default idempotency mode (builder / task override may change per enqueue).
    #[must_use]
    pub const fn idempotency_mode(&self) -> IdempotencyMode {
        self.idempotency_mode
    }

    /// Worker settings this instance was built with.
    #[must_use]
    pub const fn worker_settings(&self) -> &WorkerSettings {
        &self.worker
    }

    /// Telemetry/runtime label (topology slug or `embedded`).
    #[must_use]
    pub fn runtime_label(&self) -> &str {
        &self.worker.runtime_label
    }

    /// Queue backend handle.
    #[must_use]
    pub fn queue_backend(&self) -> Arc<dyn QueueBackend> {
        Arc::clone(&self.backend)
    }

    /// Task registry.
    #[must_use]
    pub fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    /// Resolve task config from backend or registry defaults.
    ///
    /// Precedence: persisted backend config, else descriptor policy defaults, then runtime
    /// idempotency fallback when the mode is unset.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is unknown or the backend fails.
    pub async fn resolve_task_config(&self, task_name: &str) -> Result<TaskConfig> {
        let config = if let Some(c) = self.backend.get_task_config(task_name).await? {
            c
        } else {
            self.registry.get_or_err(task_name)?.to_task_config()
        };
        Ok(config.with_runtime_idempotency_fallback(self.idempotency_mode()))
    }

    /// Resolve priority and pool for enqueue.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is unknown or the backend fails.
    pub async fn resolve_priority_pool(&self, task_name: &str) -> Result<(i32, String)> {
        let config = self.resolve_task_config(task_name).await?;
        Ok((config.priority, config.pool))
    }

    /// Enqueue helper used internally and by admin APIs.
    pub(crate) async fn enqueue_internal(
        &self,
        task_name: &str,
        actor_json: serde_json::Value,
        params_json: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<String> {
        let descriptor = self.registry.get_or_err(task_name)?;
        let task_config = self.resolve_task_config(task_name).await?;
        let priority = task_config.priority;
        let pool = task_config.pool.clone();
        let job = Job::new(
            task_name,
            actor_json,
            params_json,
            priority,
            &pool,
            descriptor.signature_hash,
            idempotency_key,
        );
        let (job_id, disposition) = self
            .backend
            .enqueue_with_policies(job, &task_config)
            .await
            .map_err(|e| {
                if matches!(e, BosonError::RateLimited(_)) {
                    crate::telemetry::record_task_failed(task_name, "", "", &e.to_string(), false);
                }
                e
            })?;
        if disposition == JobEnqueueDisposition::InsertedNew {
            crate::telemetry::record_task_enqueued(task_name, self.runtime_label());
        }
        Ok(job_id)
    }

    /// Enqueue a job.
    ///
    /// Priority and pool come from persisted [`TaskConfig`](boson_core::TaskConfig) merged with
    /// [`TaskDescriptor`](crate::registry::TaskDescriptor) defaults. Optional `idempotency_key`
    /// deduplicates non-terminal jobs. Rate limits may return
    /// [`BosonError::RateLimited`](boson_core::BosonError::RateLimited).
    ///
    /// # Errors
    ///
    /// Returns an error if the task is unknown, rate limits apply, or the backend fails.
    pub async fn enqueue(
        &self,
        task_name: &str,
        actor_json: serde_json::Value,
        params_json: serde_json::Value,
        idempotency_key: Option<String>,
    ) -> Result<String> {
        self.enqueue_internal(task_name, actor_json, params_json, idempotency_key)
            .await
    }

    /// Get a job by id.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.backend.get_job(job_id).await
    }

    /// List jobs with optional status filter.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        self.backend.list_jobs(status_filter, offset, limit).await
    }

    /// Cancel a job if still active.
    ///
    /// # Errors
    ///
    /// Returns an error if the job is not found or the backend fails.
    pub async fn cancel_job(&self, job_id: &str) -> Result<()> {
        self.backend.cancel_job_if_active(job_id).await
    }

    /// Get or default task config.
    ///
    /// # Errors
    ///
    /// Returns an error if the task is unknown or the backend fails.
    pub async fn get_task_config(&self, task_name: &str) -> Result<TaskConfig> {
        self.resolve_task_config(task_name).await
    }

    /// Upsert task config.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn upsert_task_config(&self, config: TaskConfig) -> Result<()> {
        self.backend.upsert_task_config(&config).await
    }

    /// List runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.backend.list_runs(job_id_filter, offset, limit).await
    }

    /// Get run by id.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.backend.get_run(run_id).await
    }

    /// Count jobs.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        self.backend.count_jobs(status_filter).await
    }

    /// Count runs.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        self.backend.count_runs(job_id_filter).await
    }

    /// Count runs since timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        self.backend.count_runs_since(since).await
    }

    /// Count jobs for one task.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn count_jobs_for_task(
        &self,
        task_name: &str,
        status: Option<JobStatus>,
    ) -> Result<u64> {
        self.backend.count_jobs_for_task(task_name, status).await
    }

    /// Aggregate run stats for one task.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend fails.
    pub async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        self.backend.task_run_stats(task_name).await
    }
}
