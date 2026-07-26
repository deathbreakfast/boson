//! Manual single-step worker for tests (no background task).

use std::sync::{Arc, Mutex};

use boson_core::{ExecutionContextFactory, QueueBackend};

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
    /// True while a claim/execute step is in flight (never held across `.await`).
    in_flight: Mutex<bool>,
}

struct ClearInFlight<'a>(&'a Mutex<bool>);

impl Drop for ClearInFlight<'_> {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.0.lock() {
            *guard = false;
        }
    }
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
            in_flight: Mutex::new(false),
        }
    }

    /// Process at most one job across all pools.
    pub async fn try_run_next(&self) -> bool {
        {
            let Ok(mut in_flight) = self.in_flight.lock() else {
                return false;
            };
            if *in_flight {
                return false;
            }
            *in_flight = true;
        }
        let _clear = ClearInFlight(&self.in_flight);

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
