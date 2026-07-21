//! Shared [`QueueBackend`] contract tests for in-memory backend.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_mem::MemQueueBackend;
use boson_core::QueueBackend;

#[allow(clippy::unused_async)] // Contract suite macro awaits a uniform async factory.
async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    Some(Arc::new(MemQueueBackend::new()))
}

boson_testkit::backend_contract_suite!(fresh, "mem");
