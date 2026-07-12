//! Shared [`QueueBackend`] contract tests for `SQLite`.

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
