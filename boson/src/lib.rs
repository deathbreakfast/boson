//! Background jobs, task handlers, and worker runtime.
//!
//! For a full walkthrough with diagrams, see the [repository README](https://github.com/unified-field-dev/boson/blob/main/README.md).
//!
//! **Source of truth:** `cargo doc -p boson --features mem,axum --open` — architecture, boot
//! workflows, and examples live on the public API items and module pages below.
//!
//! ## Cargo features
//!
//! This crate ships with **no default features** (`default = []`). Enable explicitly:
//!
//! - `mem` — in-memory `MemQueueBackend` for tests and local dev
//! - `sqlite` — `SqliteQueueBackend` persistence
//! - `postgres` — `PostgresQueueBackend` persistence
//! - `telemetry-console` — marker for console ops log ([`ConsoleOpsLog`] is always re-exported)
//! - `axum` — HTTP admin API (`boson_router`, `BosonState`)
//!
//! Fleet backends (`boson-backend-redis`, `boson-backend-nats`) are separate workspace crates.
//!
// Maintainer doc rules (not rendered in public docs):
// - Task docs: #[task], macro attrs, send_with — link to boot items instead of duplicating.
// - Boot docs: BosonBuilder, auto_registry, configure, backend, telemetry, axum — once per process.
// - Backend docs: QueueBackend trait, reference adapters — link to How to implement.
//!
//! # Getting started
//!
//! Depend on this crate with the `mem` feature, boot a worker, and hold a runtime handle:
//!
//! ```rust
//! # #[cfg(feature = "mem")]
//! # {
//! # use std::sync::Arc;
//! # use boson::{Boson, JsonExecutionContextFactory, MemQueueBackend};
//! let _boson = Boson::builder()
//!     .queue_backend(Arc::new(MemQueueBackend::new()))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .build_manual()
//!     .expect("build");
//! # }
//! ```
//!
//! ## Documentation map
//!
//! Full snippets live on the linked items (not repeated here).
//!
//! - **Define a handler** — `#[task(name = "...")]` with typed params and `send_with`. Example on [`task`].
//! - **Boot a worker** — wire backend, identity factory, and task discovery once per process.
//!   Examples on [`BosonBuilder`], [`BosonBuilder::auto_registry`], [`configure`].
//! - **Enqueue work** — `<TaskName>::send_with(actor_json, params)` or [`Boson::enqueue`].
//! - **Run jobs** — background worker via [`BosonBuilder::build`], or step-driven
//!   [`ManualWorker::try_run_next`] for tests. Examples on each method.
//! - **Configure task policies** — retries, rate limits, pools via macro attributes or persisted
//!   [`TaskConfig`]. Example on [`TaskConfig`].
//! - **Choose persistence** — `MemQueueBackend` (`mem`), `SqliteQueueBackend` (`sqlite`),
//!   `PostgresQueueBackend` (`postgres`), or fleet crates
//!   [`boson-backend-redis`](https://docs.rs/boson-backend-redis) /
//!   [`boson-backend-nats`](https://docs.rs/boson-backend-nats). Connect examples on each backend type.
//! - **Mount HTTP admin** — nest `boson_router` at `/api/boson` ([`NEST_PATH`](https://docs.rs/boson/latest/boson/constant.NEST_PATH.html) when `axum` is enabled). Runnable:
//!   `cargo run -p boson --example axum_admin --features mem,axum`.
//! - **Implement custom persistence** — honor the [`QueueBackend`] contract; start from
//!   `MemQueueBackend` or see **How to implement** on the trait.
//!
//! Runnable binaries: `task_macro`, `minimal_enqueue`, `idempotency_and_rate_limit`, `axum_admin`
//! (`cargo run -p boson --example <name> --features mem`).
//!
//! ## Configuration precedence
//!
//! | Layer | Resolution order |
//! |-------|------------------|
//! | Worker settings | [`BosonBuilder`] field → environment variable → hardcoded default |
//! | Task config at enqueue | Persisted backend config → macro/descriptor defaults |
//! | Idempotency mode | Per-task override → [`BosonBuilder::idempotency_mode`] (default lease-backed) |
//! | Queue backend | Explicit [`BosonBuilder::queue_backend`] → global router |
//! | Ops log | [`BosonBuilder::ops_log`] → [`NoOpsLog`]; or [`ops_log_from_env`] separately |
//! | Fleet URLs (Redis/NATS) | `BOSON_*_POOL_ROUTING` → `BOSON_*_URLS` |
//!
//! See [`WorkerSettings`] and [`TaskConfig`] for field-level defaults.
//!
//! ## Adding another task
//!
//! When the worker is already booted, adding a handler is only the macro and enqueue call:
//!
//! ```rust,no_run
//! use boson::{task, ExecutionContext};
//!
//! #[task(name = "notify")]
//! async fn notify(ctx: Box<dyn ExecutionContext>, message: String) -> boson_core::Result<()> {
//!     let _ = (ctx, message);
//!     Ok(())
//! }
//!
//! # async fn enqueue() -> boson_core::Result<()> {
//! Notify::send_with(
//!     serde_json::json!({"System": {"operation": "notify"}}),
//!     NotifyParams { message: "hello".into() },
//! )
//! .await?;
//! # Ok(())
//! # }
//! ```
//!
//! ## First boot and first task
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use boson::{
//!     configure, task, Boson, ExecutionContext, JsonExecutionContextFactory,
//! };
//! # #[cfg(feature = "mem")]
//! # use boson::MemQueueBackend;
//!
//! #[task(name = "greet")]
//! async fn greet(ctx: Box<dyn ExecutionContext>, name: String) -> boson_core::Result<()> {
//!     let _ = (ctx, name);
//!     Ok(())
//! }
//!
//! # async fn run() -> boson_core::Result<()> {
//! # #[cfg(feature = "mem")]
//! # {
//! let boson = Boson::builder()
//!     .queue_backend(Arc::new(MemQueueBackend::new()))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .auto_registry()
//!     .build()?;
//! configure(boson);
//! # }
//!
//! Greet::send_with(
//!     serde_json::json!({"System": {"operation": "demo"}}),
//!     GreetParams { name: "world".into() },
//! )
//! .await?;
//! # Ok(())
//! # }
//! ```

pub mod prelude;

pub use boson_core::{
    default_backend_from_global, BosonError, ExecutionContext, ExecutionContextFactory,
    IdentityError, Job, JobEnqueueDisposition, JobStatus, JsonExecutionContextFactory,
    QueueBackend, QueueRouter, RateLimitPolicy, RetryPolicy, Run, RunStatus, TaskConfig,
    TaskRunStats,
};
/// Background task handler — typed params, `send_with` enqueue, and link-time registration.
///
/// # Example
///
/// Assumes the worker is already booted; for one-time setup see
/// [First boot and first task](crate#first-boot-and-first-task).
///
/// ```rust,no_run
/// use boson::{task, ExecutionContext};
///
/// #[task(name = "notify")]
/// async fn notify(
///     ctx: Box<dyn ExecutionContext>,
///     message: String,
/// ) -> boson_core::Result<()> {
///     let _ = (ctx, message);
///     Ok(())
/// }
///
/// # async fn enqueue() -> boson_core::Result<()> {
/// Notify::send_with(
///     serde_json::json!({"System": {"operation": "notify"}}),
///     NotifyParams { message: "hello".into() },
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
///
/// # Contract
///
/// - Function must be `async`.
/// - First parameter must be `Box<dyn ExecutionContext>`.
/// - Return type must be `Result<()>` (typically `boson_core::Result<()>`).
/// - `name = "..."` is required and must be the first attribute.
///
/// # Policy attributes
///
/// Optional: `priority`, `pool`, `max_attempts`, `base_delay_ms`, `backoff_multiplier`,
/// `max_delay_ms`, `max_in_flight`, `max_enqueue_per_second`. Defaults and meanings are documented
/// on [`boson_macros`](https://docs.rs/boson-macros).
pub use boson_macros::task;
pub use boson_runtime::{
    configure, default, Boson, BosonBuilder, InvokeFn, ManualWorker, TaskDescriptor, TaskRegistry,
    WorkerSettings,
};
pub use boson_telemetry::{install_ops_log, ops_log, ops_log_from_env, ConsoleOpsLog, NoOpsLog, OpsLog};

#[cfg(feature = "mem")]
pub use boson_backend_mem::{install_default_mem_backend, MemQueueBackend};

#[cfg(feature = "sqlite")]
pub use boson_backend_sqlite::{install_default_sqlite_backend, SqliteQueueBackend};

#[cfg(feature = "postgres")]
pub use boson_backend_postgres::{
    install_default_postgres_backend, install_isolated_postgres_backend, postgres_test_url,
    PostgresQueueBackend,
};

#[cfg(feature = "axum")]
pub use boson_axum::{boson_router, BosonState, NEST_PATH};
