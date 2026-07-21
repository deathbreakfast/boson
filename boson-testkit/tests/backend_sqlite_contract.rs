//! Shared [`QueueBackend`] contract tests for `SQLite`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_sqlite::SqliteQueueBackend;
use boson_core::QueueBackend;

async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    let temp = tempfile::tempdir().expect("tempdir");
    // Leak tempdir for the duration of the process (contract tests are short-lived).
    let path = temp.path().join("contract.db");
    let backend =
        Arc::new(SqliteQueueBackend::new(&path).await.expect("connect")) as Arc<dyn QueueBackend>;
    std::mem::forget(temp);
    Some(backend)
}

boson_testkit::backend_contract_suite!(fresh, "sqlite");
