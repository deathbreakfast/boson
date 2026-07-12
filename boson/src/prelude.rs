//! Convenient re-exports for application code.
//!
//! The prelude pulls together types from several crates; it is not a single workflow. See the
//! [documentation map](crate#documentation-map) on the `boson` crate for **Creating tasks**,
//! **Integrating the server**, and **Developing the backend**.

pub use crate::{
    configure, task, Boson, BosonBuilder, BosonError, ExecutionContext, ExecutionContextFactory,
    JsonExecutionContextFactory, Job, JobStatus, QueueBackend, Run, TaskConfig, TaskDescriptor,
    TaskRegistry, WorkerSettings,
};

/// Result alias matching core errors.
pub type Result<T> = boson_core::Result<T>;
