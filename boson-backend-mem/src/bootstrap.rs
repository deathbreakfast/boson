//! Bootstrap helper — register in-memory backend on the global [`QueueRouter`](boson_core::QueueRouter).

use std::sync::Arc;

use boson_core::{QueueBackend, QueueRouter};

use crate::MemQueueBackend;

/// Install a new [`MemQueueBackend`] as the process-global default backend.
///
/// Registers under `default` so [`default_backend_from_global`](boson_core::default_backend_from_global) resolves it.
///
/// Call once at host boot in tests or testkit bootstrap. Production hosts should inject
/// explicit backends — no silent in-memory fallback.
#[must_use]
pub fn install_default_mem_backend() -> Arc<MemQueueBackend> {
    let backend = Arc::new(MemQueueBackend::new());
    let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
    QueueRouter::set_global(QueueRouter::with_default(dyn_backend));
    backend
}
