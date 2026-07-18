//! Shared types, [`QueueBackend`] trait, router, and identity hooks.
//!
//! Task authors usually start with [`boson`](https://docs.rs/uf-boson). This crate holds
//! shared DTOs, the [`QueueBackend`] trait, and identity types used by runtime and backend adapters.
//!
//! ## Entry points
//!
//! - [`Job`], [`Run`], [`TaskConfig`], [`ExecutionContext`] — portable data and handler context
//! - [`QueueBackend`] — persistence trait for jobs, runs, config, leases (**Developing the backend**)
//! - [`QueueRouter`] — register named backends at host boot (see
//!   [Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started) on `boson`)
//!
//! Hosts inject a `QueueBackend` implementation at build time; this crate has no I/O.

pub mod backend;
pub mod error;
pub mod identity;
pub mod models;
pub mod router;

pub use backend::{default_backend_from_global, JobEnqueueDisposition, QueueBackend};
pub use error::{BosonError, IdentityError, Result};
pub use identity::{ExecutionContext, ExecutionContextFactory, JsonExecutionContextFactory};
pub use models::{
    IdempotencyMode, Job, JobStatus, RateLimitPolicy, RetryPolicy, Run, RunStatus, TaskConfig,
    TaskRunStats,
};
pub use router::QueueRouter;
