//! Convenient re-exports for application code.
//!
//! The prelude re-exports types from several crates for application imports. See the
//! [`boson`](crate) crate [Getting started](crate#getting-started) for
//! [Embedded](crate#embedded-one-binary) /
//! [Remote worker](crate#remote-worker-two-binaries),
//! [define tasks](crate#3-define-tasks), and [custom backends](crate#4-choose-persistence).

pub use crate::{
    configure, task, Boson, BosonBuilder, BosonError, ExecutionContext, ExecutionContextFactory,
    Job, JobStatus, JsonExecutionContextFactory, QueueBackend, Run, TaskConfig, TaskDescriptor,
    TaskRegistry, WorkerSettings,
};

/// Result alias matching core errors.
pub type Result<T> = boson_core::Result<T>;
