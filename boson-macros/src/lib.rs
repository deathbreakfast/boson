//! Proc macros for Boson background work.
//!
//! This crate exposes [`task`], an attribute macro that turns an async function into a Boson
//! task with:
//! - a typed params struct (`<FnName>Params`),
//! - a typed enqueue handle (`<TaskName>::send_with`),
//! - and link-time registration (see
//!   [Mode 1](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary) /
//!   [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries)
//!   on the [`boson`](https://docs.rs/uf-boson) crate for worker vs enqueue-host boot).
//!
//! # Define a task
//!
//! ```ignore
//! use boson_core::ExecutionContext;
//!
//! #[boson::task(name = "notify_user")]
//! pub async fn notify_user(
//!     ctx: Box<dyn ExecutionContext>,
//!     user_id: String,
//!     message: String,
//! ) -> boson_core::Result<()> {
//!     tracing::info!(actor = ctx.label(), %user_id, %message);
//!     Ok(())
//! }
//! ```
//!
//! The first parameter must be `Box<dyn ExecutionContext>`. How that context is built from
//! `actor_json` is configured when the worker boots — see [`ExecutionContext`](https://docs.rs/boson-core/latest/boson_core/trait.ExecutionContext.html)
//! and [`BosonBuilder`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html).
//!
//! # Enqueue work
//!
//! After **any process** that will call `send_with` has called
//! [`configure`](https://docs.rs/boson-runtime/latest/boson_runtime/fn.configure.html) (Mode 1
//! embedded **or** Mode 2 enqueue-only host):
//!
//! ```ignore
//! let job_id = NotifyUser::send_with(
//!     serde_json::json!({"System": {"operation": "notify"}}),
//!     NotifyUserParams {
//!         user_id: "user:123".into(),
//!         message: "Welcome!".into(),
//!     },
//! ).await?;
//! ```
//!
//! Pass the same `actor_json` shape you would use with [`Boson::enqueue`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.Boson.html#method.enqueue).
//! The worker that **runs** the job must have discovered the task via
//! [`auto_registry`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.auto_registry)
//! (and linked the crate that defines it).
//!
//! # Project setup (once per crate)
//!
//! - **Depend** — add this crate plus `quark`, `boson-runtime`, `boson-core`, `serde`, and
//!   `serde_json` to the crate that owns the handler (see [README](../README.md)).
//! - **Link** — when the handler lives in a library crate, make that crate a dependency of the
//!   **worker** binary so inventory entries are linked (for example `use my_worker as _;` in `main`).
//!
//! # Boot once (not per task)
//!
//! Worker / enqueue-host boot (`BosonBuilder`, `auto_registry`, `configure`, identity factory) is
//! **not** repeated for each new task. See the [`boson`](https://docs.rs/uf-boson) crate
//! [Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started) and the
//! [`task_macro` example](https://github.com/unified-field-dev/boson/blob/main/boson/examples/task_macro.rs).
//!
//! # Generated items
//!
//! For a task function `notify_user` with task name `"notify_user"`, the macro generates:
//!
//! | Item | Purpose |
//! |------|---------|
//! | `NotifyUserParams` | Serde payload struct for task arguments |
//! | `NotifyUser` | Handle type with `send_with(actor_json, params)` |
//! | `__notify_user_impl` | Original function body (internal) |
//!
//! Registration for worker dispatch is emitted at compile time; boot-time collection is described
//! on [`TaskRegistry`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.TaskRegistry.html) and in the [`boson`](https://docs.rs/uf-boson) crate
//! [Mode 1](https://docs.rs/uf-boson/latest/boson/index.html#mode-1--embedded-one-binary) /
//! [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries) sections.
//!
//! Task handle names are derived from the **task name** (dots become underscores, `PascalCase`).
//! Params struct names are derived from the **function name**.
//!
//! # Macro attributes
//!
//! `name` is required and must be the first attribute. Additional policy fields seed the initial
//! [`TaskConfig`](https://docs.rs/boson-core/latest/boson_core/struct.TaskConfig.html) on first enqueue (runtime admin may override later):
//!
//! | Attribute | Default | Meaning |
//! |-----------|---------|---------|
//! | `name` | *(required)* | Registry key and enqueue target |
//! | `priority` | `1` | Default priority (lower = higher priority) |
//! | `pool` | `"global"` | Worker pool name |
//! | `idempotency_mode` | *(inherit)* | `"lwt"` or `"none"` (override runtime default) |
//! | `max_attempts` | `3` | Default retry max attempts |
//! | `base_delay_ms` | `1000` | Retry base delay in milliseconds |
//! | `backoff_multiplier` | `2.0` | Retry backoff multiplier |
//! | `max_delay_ms` | `30000` | Retry max delay cap in milliseconds |
//! | `max_in_flight` | `100` | Default max in-flight jobs (`0` = unlimited) |
//! | `max_enqueue_per_second` | `50` | Default enqueue rate limit (`0` = unlimited) |
//!
//! ```ignore
//! #[boson::task(
//!     name = "process_order",
//!     priority = 10,
//!     pool = "checkout",
//!     max_in_flight = 200,
//! )]
//! async fn process_order(ctx: Box<dyn ExecutionContext>, order_id: String) -> boson_core::Result<()> {
//!     Ok(())
//! }
//! ```
//!
//! # Contract
//!
//! The annotated function must:
//! - be `async`
//! - accept `Box<dyn ExecutionContext>` as the first argument
//! - return `Result<()>` (for example `boson_core::Result<()>`)
//! - include `name = "..."` as the first macro attribute
//!
//! Free functions only — methods with `&self` are rejected at compile time.

use proc_macro::TokenStream;

mod task;
mod task_attrs;
mod task_expand;
mod task_validate;

/// Marks an async function as a Boson task.
///
/// Generates a params struct, a typed enqueue handle, and a registration entry for worker dispatch.
/// See the [crate-level documentation](self) for define/enqueue examples, policy attributes, and
/// worker boot (cross-link to the [`boson`](https://docs.rs/uf-boson) crate).
///
/// # Contract
///
/// - Function must be `async`.
/// - First parameter must be `Box<dyn ExecutionContext>`.
/// - Return type must be `Result<()>` (typically `boson_core::Result<()>`).
/// - `name` attribute is required (must be first).
///
/// # Policy attributes
///
/// Optional: `priority`, `pool`, `idempotency_mode` (`"lwt"` / `"none"`), `max_attempts`, `base_delay_ms`, `backoff_multiplier`,
/// `max_delay_ms`, `max_in_flight`, `max_enqueue_per_second`. See crate docs for defaults.
#[proc_macro_attribute]
pub fn task(attr: TokenStream, item: TokenStream) -> TokenStream {
    task::task_impl(attr, item)
}
