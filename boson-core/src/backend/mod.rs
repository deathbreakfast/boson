//! Queue persistence trait and related types.

mod queue_backend;

pub use queue_backend::{
    default_backend_from_global, JobEnqueueDisposition, QueueBackend, DEFAULT_BACKEND_NAME,
};
