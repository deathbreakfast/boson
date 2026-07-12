//! [`BosonBuilder`] — construct a [`Boson`] runtime with injected ports.

use std::sync::Arc;

use boson_core::{ExecutionContextFactory, QueueBackend};
use boson_telemetry::{ConsoleOpsLog, OpsLog};

use crate::registry::TaskRegistry;
use crate::Boson;

/// Builder for [`Boson`].
///
/// **Required:** a queue backend ([`queue_backend`](Self::queue_backend) or
/// [`queue_backend_from_global`](Self::queue_backend_from_global)) and an
/// [`execution_context_factory`](Self::execution_context_factory). For `#[task]` handlers, call
/// [`auto_registry`](Self::auto_registry) so inventory entries from linked crates are collected.
///
/// **Optional:** [`worker_id`](Self::worker_id) and [`lease_ttl_secs`](Self::lease_ttl_secs) for
/// distributed workers, [`ops_log`](Self::ops_log) for telemetry, [`without_worker`](Self::without_worker)
/// when driving [`ManualWorker`](crate::ManualWorker) in tests.
///
/// # Example
///
/// After [`build`](Self::build), call [`configure`](crate::configure) if callers use macro
/// `send_with` (not required when holding `Boson` and calling [`Boson::enqueue`] directly).
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
///     .build()?;
/// configure(boson);
/// # Ok(())
/// # }
/// ```
#[derive(Default)]
pub struct BosonBuilder {
    pub(crate) queue_backend: Option<Arc<dyn QueueBackend>>,
    pub(crate) execution_context_factory: Option<Arc<dyn ExecutionContextFactory>>,
    pub(crate) ops_log: Option<Arc<dyn OpsLog>>,
    pub(crate) registry: Option<Arc<TaskRegistry>>,
    pub(crate) use_auto_registry: bool,
    pub(crate) spawn_worker: bool,
    pub(crate) worker_id: Option<String>,
    pub(crate) lease_ttl_secs: Option<i64>,
    pub(crate) runtime_label: Option<String>,
    pub(crate) worker_pools: Option<Vec<String>>,
    pub(crate) worker_poll_interval_ms: Option<u64>,
    pub(crate) idempotency_mode: boson_core::IdempotencyMode,
}

impl BosonBuilder {
    /// Worker identity for lease claims (default: `INSTANCE_ID` / `BOSON_WORKER_ID` / `boson-worker-1`).
    #[must_use]
    pub fn worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    /// Run lease TTL in seconds; when `> 0`, claim path acquires leases before job claim.
    #[must_use]
    pub const fn lease_ttl_secs(mut self, secs: i64) -> Self {
        self.lease_ttl_secs = Some(secs);
        self
    }

    /// Telemetry/runtime label (default `embedded`; bench uses topology slug).
    #[must_use]
    pub fn runtime_label(mut self, label: impl Into<String>) -> Self {
        self.runtime_label = Some(label.into());
        self
    }

    /// Restrict this worker to specific pools (comma-free list). Unset = poll all queued pools.
    ///
    /// Also available via `BOSON_WORKER_POOLS=pool-a,pool-b`. Pin workers to disjoint pool sets
    /// for shared-nothing scaling (each worker skips `distinct_pools_queued` fan-out).
    #[must_use]
    pub fn worker_pools(
        mut self,
        pools: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.worker_pools = Some(pools.into_iter().map(Into::into).collect());
        self
    }

    /// Milliseconds between worker poll ticks (default 50; use 0 for bench drain tests).
    #[must_use]
    pub const fn worker_poll_interval_ms(mut self, ms: u64) -> Self {
        self.worker_poll_interval_ms = Some(ms);
        self
    }

    /// Default enqueue idempotency mode when a task does not override it.
    ///
    /// [`boson_core::IdempotencyMode::Lwt`] (default) is exactly-once under concurrent enqueue.
    /// [`boson_core::IdempotencyMode::None`] is at-least-once and skips coordination (higher throughput).
    #[must_use]
    pub const fn idempotency_mode(mut self, mode: boson_core::IdempotencyMode) -> Self {
        self.idempotency_mode = mode;
        self
    }

    /// Inject queue persistence backend explicitly.
    #[must_use]
    pub fn queue_backend(mut self, backend: Arc<dyn QueueBackend>) -> Self {
        self.queue_backend = Some(backend);
        self
    }

    /// Use global [`QueueRouter`](boson_core::QueueRouter) default backend.
    #[must_use]
    pub fn queue_backend_from_global(mut self) -> Self {
        self.queue_backend = None;
        self
    }

    /// Identity factory for handler dispatch.
    ///
    /// Maps stored `actor_json` to `Box<dyn ExecutionContext>` when a worker runs a job. For examples
    /// and smoke tests, pass [`JsonExecutionContextFactory`](boson_core::JsonExecutionContextFactory);
    /// production apps typically implement [`ExecutionContextFactory`](boson_core::ExecutionContextFactory).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use boson_backend_mem::MemQueueBackend;
    /// use boson_core::JsonExecutionContextFactory;
    /// use boson_runtime::Boson;
    ///
    /// # fn main() -> boson_core::Result<()> {
    /// let _boson = Boson::builder()
    ///     .queue_backend(Arc::new(MemQueueBackend::new()))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .build()?;
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub fn execution_context_factory(
        mut self,
        factory: impl ExecutionContextFactory + 'static,
    ) -> Self {
        self.execution_context_factory = Some(Arc::new(factory));
        self
    }

    /// Identity factory from existing `Arc`.
    #[must_use]
    pub fn execution_context_factory_arc(
        mut self,
        factory: Arc<dyn ExecutionContextFactory>,
    ) -> Self {
        self.execution_context_factory = Some(factory);
        self
    }

    /// Install ops log adapter (default [`boson_telemetry::NoOpsLog`]).
    #[must_use]
    pub fn ops_log(mut self, log: impl OpsLog + 'static) -> Self {
        self.ops_log = Some(Arc::new(log));
        self
    }

    /// Use console stderr ops log.
    #[must_use]
    pub fn ops_log_console(mut self) -> Self {
        self.ops_log = Some(Arc::new(ConsoleOpsLog));
        self
    }

    /// Use an existing task registry (e.g. testkit manual registration).
    #[must_use]
    pub fn registry(mut self, registry: Arc<TaskRegistry>) -> Self {
        self.registry = Some(registry);
        self.use_auto_registry = false;
        self
    }

    /// Discover tasks registered via Quark inventory (for example `#[boson::task]`).
    ///
    /// The worker binary must link every crate that defines inventory submissions; otherwise
    /// tasks defined in library crates will not appear in the registry. Add the task-owning crate
    /// as a dependency of the binary (for example `use my_worker as _;`).
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    ///
    /// use boson_backend_mem::MemQueueBackend;
    /// use boson_core::{ExecutionContext, JsonExecutionContextFactory};
    /// use boson_macros::task;
    /// use boson_runtime::{configure, Boson};
    ///
    /// #[task(name = "ping")]
    /// async fn ping(_ctx: Box<dyn ExecutionContext>) -> boson_core::Result<()> {
    ///     Ok(())
    /// }
    ///
    /// // When handlers live in a library crate, link it from `main`:
    /// // use my_worker as _;
    ///
    /// # fn main() -> boson_core::Result<()> {
    /// let boson = Boson::builder()
    ///     .queue_backend(Arc::new(MemQueueBackend::new()))
    ///     .execution_context_factory(JsonExecutionContextFactory)
    ///     .auto_registry()
    ///     .build()?;
    /// configure(boson);
    /// # Ok(())
    /// # }
    /// ```
    #[must_use]
    pub const fn auto_registry(mut self) -> Self {
        self.use_auto_registry = true;
        self
    }

    /// Do not spawn background worker (caller drives [`ManualWorker`](crate::ManualWorker)).
    #[must_use]
    pub const fn without_worker(mut self) -> Self {
        self.spawn_worker = false;
        self
    }
}

impl Boson {
    /// Create a new builder.
    #[must_use]
    pub fn builder() -> BosonBuilder {
        BosonBuilder {
            spawn_worker: true,
            ..Default::default()
        }
    }
}
