//! Process-wide default Boson instance for macro-generated `send_with` helpers.
//!
//! Generated task handles call [`default`] internally. Install the runtime once at application
//! boot with [`configure`] after [`crate::BosonBuilder::build`] or [`crate::BosonBuilder::build_manual`].

use std::sync::RwLock;

use super::Boson;

static DEFAULT_BOSON: RwLock<Option<Boson>> = std::sync::RwLock::new(None);

/// Install the process-wide default [`Boson`] instance.
///
/// Required before calling macro-generated `<TaskName>::send_with`. Typically called once after
/// building the runtime:
///
/// ```rust,no_run
/// # use std::sync::Arc;
/// # use boson_backend_mem::MemQueueBackend;
/// # use boson_core::JsonExecutionContextFactory;
/// # use boson_runtime::{configure, Boson};
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let boson = Boson::builder()
///     .queue_backend(Arc::new(MemQueueBackend::new()))
///     .execution_context_factory(JsonExecutionContextFactory)
///     .auto_registry()
///     .build()?;
/// configure(boson);
/// # Ok(())
/// # }
/// ```
///
/// # Panics
///
/// Panics if the internal lock is poisoned.
pub fn configure(boson: Boson) {
    let mut guard = DEFAULT_BOSON.write().unwrap();
    *guard = Some(boson);
}

/// Return the configured default [`Boson`] instance, if any.
///
/// Used by macro-generated `send_with` helpers. Returns `None` when [`configure`] has not been
/// called.
///
/// # Panics
///
/// Panics if the internal lock is poisoned.
pub fn default() -> Option<Boson> {
    let guard = DEFAULT_BOSON.read().unwrap();
    guard.clone()
}

