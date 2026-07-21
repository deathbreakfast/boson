//! Bootstrap helper — register `PostgreSQL` backend on the global [`QueueRouter`](boson_core::QueueRouter).

use std::sync::Arc;

use boson_core::{QueueBackend, QueueRouter};
use uuid::Uuid;

use crate::PostgresQueueBackend;

const DEFAULT_POSTGRES_URL: &str = "postgres://boson:bench@127.0.0.1:5433/boson_bench";

/// Resolve postgres URL from env (test preferred, then bench, then default).
#[must_use]
pub fn postgres_test_url() -> String {
    std::env::var("BOSON_TEST_POSTGRES_URL")
        .or_else(|_| std::env::var("BOSON_BENCH_POSTGRES_URL"))
        .unwrap_or_else(|_| DEFAULT_POSTGRES_URL.to_string())
}

/// Install a new [`PostgresQueueBackend`] as the process-global default backend.
///
/// # Errors
///
/// Propagates errors from [`PostgresQueueBackend::connect`].
pub async fn install_default_postgres_backend(
    url: &str,
) -> boson_core::Result<Arc<PostgresQueueBackend>> {
    let backend = Arc::new(PostgresQueueBackend::connect(url).await?);
    let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
    QueueRouter::set_global(QueueRouter::with_default(dyn_backend));
    Ok(backend)
}

/// Install postgres backend with an isolated schema for test sessions.
///
/// # Errors
///
/// Propagates errors from [`PostgresQueueBackend::connect_isolated`].
pub async fn install_isolated_postgres_backend(
    url: &str,
) -> boson_core::Result<(Arc<PostgresQueueBackend>, String)> {
    let schema = format!("boson_{}", Uuid::new_v4().simple());
    let backend = Arc::new(PostgresQueueBackend::connect_isolated(url, &schema).await?);
    Ok((backend, schema))
}
