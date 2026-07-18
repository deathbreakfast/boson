//! Convenient re-exports for application code.
//!
//! The prelude pulls together types from several crates; it is not a single workflow. See the
//! [`boson`](crate) crate [Getting started](crate#getting-started) for
//! [Mode 1](crate#mode-1--embedded-one-binary) /
//! [Mode 2](crate#mode-2--remote-worker-two-binaries),
//! [define tasks](crate#3-define-tasks), and [custom backends](crate#4-choose-persistence).

pub use crate::{
    configure, task, Boson, BosonBuilder, BosonError, ExecutionContext, ExecutionContextFactory,
    JsonExecutionContextFactory, Job, JobStatus, QueueBackend, Run, TaskConfig, TaskDescriptor,
    TaskRegistry, WorkerSettings,
};

/// Result alias matching core errors.
pub type Result<T> = boson_core::Result<T>;
