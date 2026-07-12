//! Shared [`QueueBackend`] contract tests for in-memory backend.

use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::QueueBackend;

#[allow(clippy::unused_async)] // Contract suite macro awaits a uniform async factory.
async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    Some(Arc::new(MemQueueBackend::new()))
}

boson_testkit::backend_contract_suite!(fresh, "mem");
