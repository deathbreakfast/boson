//! Boson is a Rust job-work runtime: durable background tasks, retries, rate limits, and
//! pluggable persistence behind [`QueueBackend`].
//!
//! Wire a backend once with [`Boson::builder()`], define handlers with [`task`], then enqueue
//! with typed `send_with` (after [`configure`]) or [`Boson::enqueue`]. Swap `mem`, `sqlite`,
//! `postgres`, or fleet crates (`boson-backend-redis` / `boson-backend-nats`) without changing
//! task code.
//!
//! ## Features
//!
//! - **Typed task handlers** — [`task`] macro with policy attributes and generated `send_with`
//! - **Composable persistence** — inject [`MemQueueBackend`], [`SqliteQueueBackend`],
//!   [`PostgresQueueBackend`](https://docs.rs/boson-backend-postgres), or fleet backends on
//!   [`BosonBuilder`]
//! - **Embedded or remote workers** — one process can enqueue and drain, or many hosts can
//!   enqueue while a separate worker binary claims jobs (see
//!   [Mode 2](#mode-2--remote-worker-two-binaries))
//! - **Leases and pools** — multi-process coordination via [`BosonBuilder::lease_ttl_secs`] and
//!   [`WorkerSettings`]
//! - **HTTP admin (optional)** — nest [`boson_router`] at [`NEST_PATH`] when the `axum` feature
//!   is enabled
//!
//! *Background jobs without locking you into one queue store.*
//!
//! This crate ships with **no default features** (`default = []`). Enable explicitly:
//!
//! - `mem` — [`MemQueueBackend`] for tests and local Mode 1
//! - `sqlite` — [`SqliteQueueBackend`] durable single-host (or shared-file Mode 2)
//! - `postgres` — [`PostgresQueueBackend`](https://docs.rs/boson-backend-postgres) shared durable state
//! - `telemetry-console` — marker for console ops log ([`ConsoleOpsLog`] is always re-exported)
//! - `axum` — HTTP admin API ([`boson_router`], [`BosonState`], [`NEST_PATH`])
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
//! You always define and enqueue tasks the same way (`#[task]`, `send_with`). What changes is
//! **which process runs the worker loop**.
//!
//! ## Choose your topology
//!
//! - **[Mode 1 — Embedded](#mode-1--embedded-one-binary)** — one binary enqueues and drains.
//!   Start here.
//! - **[Mode 2 — Remote worker](#mode-2--remote-worker-two-binaries)** — API/host processes
//!   enqueue; a separate **worker** binary (or fleet) claims and runs jobs.
//!
//! After you pick a mode, continue with [define tasks](#3-define-tasks) (shared by every mode).
//!
//! ## Mode 1 — Embedded (one binary)
//!
//! This process enqueues jobs **and** runs the worker loop (or drives [`ManualWorker`] in tests).
//! There is no second binary. Default lease TTL is `0` (no distributed lease coordination).
//!
//! ```text
//! Your app ──enqueue──► Boson ──worker loop──► mem / `SQLite` / Postgres / …
//! ```
//!
//! | Backend | Type | Feature / crate | Topology | When to use |
//! |---------|------|-----------------|----------|-------------|
//! | In-memory | [`MemQueueBackend`] | `mem` | embedded only | Local experiments and tests |
//! | `SQLite` | [`SqliteQueueBackend`] | `sqlite` | embedded (or Mode 2 on one host) | Durable single host |
//! | Postgres | [`PostgresQueueBackend`](https://docs.rs/boson-backend-postgres) | `postgres` | embedded or remote | Shared durable state |
//! | Redis | `RedisQueueBackend` | [`boson-backend-redis`](https://docs.rs/boson-backend-redis) | remote / fleet | Broker-backed multi-host |
//! | NATS | `NatsQueueBackend` | [`boson-backend-nats`](https://docs.rs/boson-backend-nats) | remote / fleet | Broker-backed multi-host |
//!
//! **In-memory first run** (feature `mem`):
//!
//! ```rust,no_run
//! # #[cfg(feature = "mem")]
//! # {
//! use std::sync::Arc;
//!
//! use boson::{
//!     configure, task, Boson, ExecutionContext, JsonExecutionContextFactory, MemQueueBackend,
//! };
//!
//! #[task(name = "greet")]
//! async fn greet(ctx: Box<dyn ExecutionContext>, name: String) -> boson_core::Result<()> {
//!     let _ = (ctx, name);
//!     Ok(())
//! }
//!
//! # async fn run() -> boson_core::Result<()> {
//! let boson = Boson::builder()
//!     .queue_backend(Arc::new(MemQueueBackend::new()))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .auto_registry()
//!     .build()?; // background worker loop
//! configure(boson);
//!
//! Greet::send_with(
//!     serde_json::json!({"System": {"operation": "demo"}}),
//!     GreetParams { name: "world".into() },
//! )
//! .await?;
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! For step-driven tests, use [`BosonBuilder::without_worker`] + [`BosonBuilder::build_manual`]
//! and [`ManualWorker::try_run_next`] instead of [`BosonBuilder::build`].
//!
//! Runnable: `task_macro`, `minimal_enqueue`, `idempotency_and_rate_limit`
//! (`cargo run -p uf-boson --example <name> --features mem`).
//! Then continue with [define tasks](#3-define-tasks).
//!
//! ## Mode 2 — Remote worker (two binaries)
//!
//! Use this when HTTP/API processes should **enqueue only**, and a dedicated worker process (or
//! many workers) should **claim and run** jobs against **shared** persistence.
//!
//! [`MemQueueBackend`] cannot cross process boundaries — Mode 2 needs `SQLite` (shared path),
//! Postgres, Redis, or NATS.
//!
//! ```text
//! Enqueue binary(ies) ──send_with──► shared QueueBackend ◄──claim── Worker binary(ies)
//! ```
//!
//! ### What you create
//!
//! | Piece | Purpose |
//! |-------|---------|
//! | Shared task crate (recommended) | Same `#[task]` handlers + inventory on the worker |
//! | Enqueue binary | Boots Boson **without** a worker loop; [`auto_registry`](BosonBuilder::auto_registry) so
//!   descriptors exist for `send_with`; calls [`configure`] + `send_with` |
//! | Worker binary | Same backend URL/path; [`auto_registry`](BosonBuilder::auto_registry); unique
//!   [`worker_id`](BosonBuilder::worker_id); **`lease_ttl_secs > 0`**; [`build`](BosonBuilder::build) |
//! | Shared backend | `SQLite` path, Postgres URL, or Redis/NATS fleet |
//!
//! ### Enqueue binary
//!
//! This process must **not** spawn the drain loop. Use [`BosonBuilder::without_worker`] then
//! [`BosonBuilder::build`], still call [`BosonBuilder::auto_registry`] (enqueue looks up task
//! descriptors for priority/pool/policies), install with [`configure`], and enqueue.
//!
//! **Pick a shared backend** (each link has a Mode 2 enqueue-binary example):
//!
//! | Backend | Feature / crate | Mode 2 enqueue example |
//! |---------|-----------------|------------------------|
//! | `SQLite` | `sqlite` | [`SqliteQueueBackend` — enqueue](../boson_backend_sqlite/index.html#mode-2--enqueue-binary) |
//! | Postgres | `postgres` | [`PostgresQueueBackend` — enqueue](../boson_backend_postgres/index.html#mode-2--enqueue-binary) |
//! | Redis | [`boson-backend-redis`](https://docs.rs/boson-backend-redis) | [Redis — enqueue](../boson_backend_redis/index.html#mode-2--enqueue-binary) |
//! | NATS | [`boson-backend-nats`](https://docs.rs/boson-backend-nats) | [NATS — enqueue](../boson_backend_nats/index.html#mode-2--enqueue-binary) |
//!
//! `SQLite` sketch (same pattern on every backend page above):
//!
//! ```rust,no_run
//! # #[cfg(feature = "sqlite")]
//! # {
//! use std::sync::Arc;
//!
//! use boson::{
//!     configure, Boson, JsonExecutionContextFactory, SqliteQueueBackend,
//! };
//!
//! # async fn boot_enqueue() -> boson_core::Result<()> {
//! let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
//! let backend = SqliteQueueBackend::new(&path).await?;
//! let boson = Boson::builder()
//!     .queue_backend(Arc::new(backend))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .auto_registry() // descriptors for send_with — no claim loop
//!     .without_worker()
//!     .build()?;
//! configure(boson);
//! // Greet::send_with(...).await?;  // same API as Mode 1
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! API detail: [`BosonBuilder::without_worker`], [`BosonBuilder::auto_registry`], [`configure`].
//!
//! ### Worker binary
//!
//! A **different** binary owns the drain loop. Link every crate that defines `#[task]` handlers
//! (`use my_tasks as _;`) so inventory discovery works.
//!
//! **Pick the same shared backend** (each link has a Mode 2 worker-binary example):
//!
//! | Backend | Feature / crate | Mode 2 worker example |
//! |---------|-----------------|------------------------|
//! | `SQLite` | `sqlite` | [`SqliteQueueBackend` — worker](../boson_backend_sqlite/index.html#mode-2--worker-binary) |
//! | Postgres | `postgres` | [`PostgresQueueBackend` — worker](../boson_backend_postgres/index.html#mode-2--worker-binary) |
//! | Redis | [`boson-backend-redis`](https://docs.rs/boson-backend-redis) | [Redis — worker](../boson_backend_redis/index.html#mode-2--worker-binary) |
//! | NATS | [`boson-backend-nats`](https://docs.rs/boson-backend-nats) | [NATS — worker](../boson_backend_nats/index.html#mode-2--worker-binary) |
//!
//! `SQLite` sketch (same pattern on every backend page above):
//!
//! ```rust,no_run
//! # #[cfg(feature = "sqlite")]
//! # {
//! use std::sync::Arc;
//!
//! use boson::{Boson, JsonExecutionContextFactory, SqliteQueueBackend};
//!
//! # async fn boot_worker() -> boson_core::Result<()> {
//! let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
//! let backend = SqliteQueueBackend::new(&path).await?;
//! let _boson = Boson::builder()
//!     .queue_backend(Arc::new(backend))
//!     .execution_context_factory(JsonExecutionContextFactory)
//!     .worker_id(std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "worker-1".into()))
//!     .lease_ttl_secs(30) // required when multiple processes share the backend
//!     .auto_registry()
//!     .build()?; // background claim + dispatch loop
//! // keep the process alive (await shutdown, serve health, …)
//! # Ok(())
//! # }
//! # }
//! ```
//!
//! API detail: [`BosonBuilder::worker_id`], [`BosonBuilder::lease_ttl_secs`], [`WorkerSettings`],
//! [`BosonBuilder::auto_registry`].
//!
//! ### Run both
//!
//! 1. Start the **worker** (so claims are ready).
//! 2. Start one or more **enqueue** hosts.
//! 3. Each worker process needs a unique `BOSON_WORKER_ID` (or builder
//!    [`worker_id`](BosonBuilder::worker_id)) and a positive lease TTL (`BOSON_LEASE_TTL_SECS` or
//!    [`lease_ttl_secs`](BosonBuilder::lease_ttl_secs)).
//!
//! Runnable: `remote_worker`, `remote_enqueue`
//!
//! ```bash
//! export BOSON_SQLITE_PATH=/tmp/boson-remote.db
//! cargo run -p uf-boson --example remote_worker --features sqlite &
//! cargo run -p uf-boson --example remote_enqueue --features sqlite
//! ```
//!
//! ## 3. Define tasks
//!
//! When the worker is already booted (Mode 1) or the worker binary discovers inventory (Mode 2),
//! adding a handler is the macro plus an enqueue call:
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
//! Policy attributes (`priority`, `pool`, `max_attempts`, …) are documented on [`task`].
//! Persisted overrides use [`TaskConfig`].
//!
//! ## 4. Choose persistence
//!
//! Pick from the [Mode 1 backend table](#mode-1--embedded-one-binary). Connect examples live on
//! each backend type. Fleet Redis/NATS: see those crate docs for `connect_fleet_from_env` and
//! URL precedence (`BOSON_*_POOL_ROUTING` → `BOSON_*_URLS`).
//!
//! Custom adapters implement [`QueueBackend`] (start from [`MemQueueBackend`] or the trait’s
//! **How to implement** section).
//!
//! ## 5. Mount HTTP admin (optional)
//!
//! With feature `axum`, nest [`boson_router`] at [`NEST_PATH`] (`/api/boson`) using [`BosonState`]:
//!
//! Runnable: `cargo run -p uf-boson --example axum_admin --features mem,axum`
//! (`BOSON_EXAMPLE_SERVE=1` to listen).
//!
//! ## Prerequisites and gotchas
//!
//! - Enable the backend feature (or fleet crate) that matches your topology — `mem` is Mode 1 only.
//! - Mode 2 workers need **`lease_ttl_secs > 0`** and unique [`worker_id`](BosonBuilder::worker_id) values.
//! - Worker binaries must **link** every crate that submits `#[task]` inventory.
//! - [`configure`] is required in any process that calls macro `send_with` (including enqueue-only hosts).
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
//! ## Runnable examples
//!
//! | Example | Topology | Features |
//! |---------|----------|----------|
//! | `task_macro` | Mode 1 (manual drain) | `mem` |
//! | `minimal_enqueue` | Mode 1 | `mem` |
//! | `idempotency_and_rate_limit` | Mode 1 | `mem` |
//! | `axum_admin` | Mode 1 + HTTP | `mem,axum` |
//! | `remote_worker` | Mode 2 worker | `sqlite` |
//! | `remote_enqueue` | Mode 2 enqueue | `sqlite` |
//!
//! ```bash
//! cargo run -p uf-boson --example task_macro --features mem
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
/// Assumes the worker (or enqueue host) is already booted and [`configure`]d. For topology
/// choice see [Mode 1](crate#mode-1--embedded-one-binary) and
/// [Mode 2](crate#mode-2--remote-worker-two-binaries).
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
pub use boson_telemetry::{
    install_ops_log, ops_log, ops_log_from_env, ConsoleOpsLog, NoOpsLog, OpsLog,
};

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
