//! Resolve builder dependencies and construct [`Boson`] / [`ManualWorker`].

use std::sync::Arc;

use boson_core::{
    default_backend_from_global, BosonError, QueueBackend, Result,
};
use boson_telemetry::{install_ops_log, NoOpsLog, OpsLog};

use crate::registry::TaskRegistry;
use crate::telemetry::log_runtime_ready;
use crate::worker::{spawn_worker, ManualWorker, WorkerSettings};
use crate::{Boson, BosonBuilder};

impl BosonBuilder {
    pub(crate) fn resolve_backend(&self) -> Result<Arc<dyn QueueBackend>> {
        if let Some(ref b) = self.queue_backend {
            return Ok(Arc::clone(b));
        }
        default_backend_from_global()
    }

    pub(crate) fn resolve_registry(&self) -> Arc<TaskRegistry> {
        if let Some(ref r) = self.registry {
            return Arc::clone(r);
        }
        if self.use_auto_registry {
            Arc::new(TaskRegistry::auto_discover())
        } else {
            Arc::new(TaskRegistry::new())
        }
    }

    pub(crate) fn resolve_worker_settings(&self) -> WorkerSettings {
        WorkerSettings::resolve(
            self.worker_id.clone(),
            self.lease_ttl_secs,
            self.runtime_label.clone(),
            self.worker_pools.clone(),
            self.worker_poll_interval_ms,
        )
    }

    pub(crate) fn install_ops_log(&self) {
        let ops = self
            .ops_log
            .clone()
            .unwrap_or_else(|| Arc::new(NoOpsLog) as Arc<dyn OpsLog>);
        install_ops_log(ops);
    }

    /// Build [`Boson`] and optionally spawn the background worker loop.
    ///
    /// When [`without_worker`](Self::without_worker) was **not** set (default), a Tokio task polls
    /// queued jobs, claims them, dispatches registered handlers (from
    /// [`auto_registry`](Self::auto_registry) or [`registry`](Self::registry)), and applies retry
    /// policy — Mode 1 embedded or Mode 2 worker.
    ///
    /// When [`without_worker`](Self::without_worker) **was** set, no claim loop starts; use this
    /// for Mode 2 enqueue hosts, then [`configure`](crate::configure) + `send_with`.
    ///
    /// Enqueue with [`Boson::enqueue`] or macro `send_with` after [`configure`](crate::configure).
    /// For step-driven tests, use [`build_manual`](Self::build_manual) instead.
    ///
    /// Getting started:
    /// [Mode 1](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary) /
    /// [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).
    ///
    /// # Example — Mode 1 embedded
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use boson_backend_mem::MemQueueBackend;
    /// use boson_core::JsonExecutionContextFactory;
    /// use boson_runtime::{configure, Boson};
    ///
    /// # fn main() -> boson_core::Result<()> {
    /// let boson = Boson::builder()
    ///     .queue_backend(Arc::new(MemQueueBackend::new()))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .auto_registry()
    ///     .build()?; // worker loop runs in the background
    /// configure(boson);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::InvalidConfig`] if no execution context factory was set, or an error
    /// if the queue backend cannot be resolved.
    pub fn build(self) -> Result<Boson> {
        self.install_ops_log();
        let identity = self.execution_context_factory.clone().ok_or_else(|| {
            BosonError::InvalidConfig("missing required execution_context_factory".into())
        })?;
        let backend = self.resolve_backend()?;
        let registry = self.resolve_registry();
        let worker = self.resolve_worker_settings();
        let spawn_worker_flag = self.spawn_worker;

        let boson = Boson::from_parts_with_idempotency(
            Arc::clone(&backend),
            Arc::clone(&registry),
            worker.clone(),
            self.idempotency_mode,
        );
        log_runtime_ready(&worker.runtime_label);

        if spawn_worker_flag {
            spawn_worker(backend, registry, identity, worker);
        }

        Ok(boson)
    }

    /// Build without a background worker; returns [`ManualWorker`] for step-driven execution.
    ///
    /// Prefer this in **tests** when you want to enqueue then call
    /// [`ManualWorker::try_run_next`](crate::ManualWorker::try_run_next). For Mode 2 enqueue-only
    /// production hosts that never drain locally, prefer [`without_worker`](Self::without_worker)
    /// + [`build`](Self::build) instead (no `ManualWorker` handle needed).
    ///
    /// # Example — enqueue then drain one job
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
    /// async fn ping(_ctx: Box<dyn ExecutionContext>, n: u32) -> boson_core::Result<()> {
    ///     let _ = n;
    ///     Ok(())
    /// }
    ///
    /// # async fn demo() -> boson_core::Result<()> {
    /// let (boson, manual): (_, ManualWorker) = Boson::builder()
    ///     .queue_backend(Arc::new(MemQueueBackend::new()))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .auto_registry()
    ///     .without_worker()
    ///     .build_manual()?;
    /// configure(boson);
    /// Ping::send_with(serde_json::json!({}), PingParams { n: 1 }).await?;
    /// assert!(manual.try_run_next().await);
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::InvalidConfig`] if no execution context factory was set, or an error
    /// if the queue backend cannot be resolved.
    pub fn build_manual(self) -> Result<(Boson, ManualWorker)> {
        self.install_ops_log();
        let identity = self.execution_context_factory.clone().ok_or_else(|| {
            BosonError::InvalidConfig("missing required execution_context_factory".into())
        })?;
        let backend = self.resolve_backend()?;
        let registry = self.resolve_registry();
        let worker = self.resolve_worker_settings();
        let boson = Boson::from_parts_with_idempotency(
            Arc::clone(&backend),
            Arc::clone(&registry),
            worker.clone(),
            self.idempotency_mode,
        );
        let manual = ManualWorker::new(backend, registry, identity, worker);
        Ok((boson, manual))
    }
}
