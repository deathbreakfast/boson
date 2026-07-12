//! Bootstrap helper — register `SQLite` backend on the global [`QueueRouter`](boson_core::QueueRouter).

use std::path::Path;
use std::sync::Arc;

use boson_core::{QueueBackend, QueueRouter};

use crate::SqliteQueueBackend;

/// Install a new [`SqliteQueueBackend`] as the process-global default backend.
///
/// # Errors
///
/// Propagates errors from [`SqliteQueueBackend::new`].
pub async fn install_default_sqlite_backend(
    path: impl AsRef<Path>,
) -> boson_core::Result<Arc<SqliteQueueBackend>> {
    let backend = Arc::new(SqliteQueueBackend::new(path).await?);
    let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
    QueueRouter::set_global(QueueRouter::with_default(dyn_backend));
    Ok(backend)
}
