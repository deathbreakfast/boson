//! Worker runtime, builder, and enqueue orchestration.
//!
//! Wire the worker process here: inject a [`QueueBackend`](boson_core::QueueBackend), identity
//! factory, and optional telemetry. [`BosonBuilder::build`] enqueues **and** starts a background
//! loop that claims and runs jobs; use [`ManualWorker`] for step-driven tests.
//!
//! ## Entry points
//!
//! - [`Boson::builder`] — inject `QueueBackend`, `ExecutionContextFactory`, `OpsLog`
//! - [`BosonBuilder::build`] — spawn background worker loop (default)
//! - [`BosonBuilder::build_manual`] / [`ManualWorker`] — drive job execution step-by-step
//! - [`Boson::enqueue`] — enqueue work for background execution
//! - [`configure`] / [`default`] — process-wide default for macro `send_with` (once at boot)

mod bootstrap;
mod boson;
mod builder;
mod global;
mod registry;
mod telemetry;
mod worker;

pub use boson::Boson;
pub use builder::BosonBuilder;
pub use global::{configure, default};
pub use registry::{InvokeFn, TaskDefaults, TaskDescriptor, TaskRegistry};
pub use worker::{spawn_worker, ManualWorker, WorkerSettings};
