//! Manual single-step worker for tests (no background task).

use std::sync::Arc;

use boson_core::ExecutionContextFactory;
use boson_core::QueueBackend;
use tokio::sync::Mutex;

use super::claim::claim_next_job;
use super::config::WorkerSettings;
use super::loop_::WorkerEngine;
use crate::registry::TaskRegistry;

/// Manual single-step worker for tests (no background task).
///
/// Use [`BosonBuilder::build_manual`](crate::BosonBuilder::build_manual) to obtain one alongside
/// [`Boson`](crate::Boson). Call [`try_run_next`](Self::try_run_next) to claim and execute at most
/// one queued job — useful in unit tests and the [`task_macro` example](https://github.com/unified-field-dev/boson/blob/main/boson/examples/task_macro.rs).
///
/// # Example
///
/// ```rust,no_run
/// use std::sync::Arc;
///
/// use boson_backend_mem::MemQueueBackend;
/// use boson_core::{ExecutionContext, JsonExecutionContextFactory};
/// use boson_macros::task;
/// use boson_runtime::{configure, Boson, ManualWorker};
///
/// #[task(name = "ping")]
/// async fn ping(_ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
///     Ok(())
/// }
///
/// # async fn run() -> boson_core::Result<()> {
/// let (boson, manual) = Boson::builder()
///     .queue_backend(Arc::new(MemQueueBackend::new()))
///     .execution_context_factory(JsonExecutionContextFactory)
///     .auto_registry()
///     .build_manual()?;
/// configure(boson);
///
/// Ping::send_with(serde_json::json!({"System": {}}), PingParams {}).await?;
/// assert!(manual.try_run_next().await); // runs the handler once
/// # Ok(())
/// # }
/// ```
pub struct ManualWorker {
    inner: Arc<WorkerEngine>,
    lock: Mutex<()>,
}

impl ManualWorker {
    /// Create a worker that can be driven step-by-step in tests.
    pub fn new(
        backend: Arc<dyn QueueBackend>,
        registry: Arc<TaskRegistry>,
        identity: Arc<dyn ExecutionContextFactory>,
        worker: WorkerSettings,
    ) -> Self {
        Self {
            inner: Arc::new(WorkerEngine {
                backend,
                registry,
                identity,
                worker,
            }),
            lock: Mutex::new(()),
        }
    }

    /// Process at most one job across all pools.
    pub async fn try_run_next(&self) -> bool {
        let _guard = self.lock.lock().await;
        let discovered = self
            .inner
            .backend
            .distinct_pools_queued()
            .await
            .unwrap_or_default();
        let pools = self.inner.worker.pools_to_poll(discovered);
        for pool in pools {
            if let Ok(Some((job, lease_id))) = claim_next_job(
                &self.inner.backend,
                &pool,
                &self.inner.worker.worker_id,
                self.inner.worker.lease_ttl_secs,
            )
            .await
            {
                self.inner.drive_run(job, lease_id).await;
                return true;
            }
        }
        false
    }
}
