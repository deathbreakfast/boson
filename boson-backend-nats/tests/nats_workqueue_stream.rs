//! `WorkQueue` contract tests — stream-first enqueue mode.

use std::sync::Arc;

use boson_backend_nats::{keys, EnqueueMode, NatsEnqueueConfig, NatsWorkQueueBackend};
use boson_core::QueueBackend;
use uuid::Uuid;

async fn fresh_stream() -> Option<Arc<dyn QueueBackend>> {
    let url = NatsWorkQueueBackend::test_url();
    let keyspace = keys::Keyspace::new(format!("boson:{}", Uuid::new_v4()));
    let config = NatsEnqueueConfig::new(EnqueueMode::Stream, true, 64, true);
    let backend = match NatsWorkQueueBackend::connect_with_config(&url, keyspace, config).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("nats workqueue stream setup: {e}");
            return None;
        }
    };
    if backend.flush_namespace().await.is_err() {
        return None;
    }
    Some(Arc::new(backend) as Arc<dyn QueueBackend>)
}

boson_testkit::backend_contract_suite!(
    fresh_stream,
    "nats-wq-stream",
    ignore = "requires NATS at BOSON_TEST_NATS_URL — run with --include-ignored"
);
