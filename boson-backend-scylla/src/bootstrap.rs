//! Global router install helpers for Scylla.

use std::sync::Arc;

use boson_core::{QueueBackend, QueueRouter, Result};

use crate::{ScyllaQueueBackend, ScyllaQueueConfig};

/// Connect and register as the process-global default backend.
///
/// # Errors
///
/// Returns an error when connect or schema bootstrap fails.
pub async fn install_default_scylla_backend(
    config: ScyllaQueueConfig,
) -> Result<Arc<ScyllaQueueBackend>> {
    let backend = Arc::new(Box::pin(ScyllaQueueBackend::connect(config)).await?);
    let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
    QueueRouter::set_global(QueueRouter::with_default(dyn_backend));
    Ok(backend)
}

/// Contact points from `BOSON_TEST_SCYLLA_CONTACT_POINTS` (comma-separated).
#[must_use]
pub fn scylla_test_contact_points() -> Option<Vec<String>> {
    std::env::var("BOSON_TEST_SCYLLA_CONTACT_POINTS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(str::to_string)
                .collect()
        })
        .filter(|v: &Vec<String>| !v.is_empty())
}

/// Isolated keyspace name for one test run.
#[must_use]
pub fn isolated_keyspace(prefix: &str) -> String {
    let id = uuid::Uuid::new_v4().simple();
    format!("{prefix}_{id}")
}
