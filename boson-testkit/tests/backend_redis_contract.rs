//! Shared [`QueueBackend`] contract tests for Redis.

use std::sync::Arc;

use boson_backend_redis::{keys, RedisQueueBackend};
use boson_core::QueueBackend;
use uuid::Uuid;

async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    let url = RedisQueueBackend::test_url();
    let keyspace = keys::Keyspace::new(format!("boson:{}", Uuid::new_v4()));
    let backend = match RedisQueueBackend::connect_with_keyspace(&url, keyspace).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("redis contract setup: {e}");
            return None;
        }
    };
    if backend.flush_boson_keys().await.is_err() {
        return None;
    }
    Some(Arc::new(backend) as Arc<dyn QueueBackend>)
}

boson_testkit::backend_contract_suite!(
    fresh,
    "redis",
    ignore = "requires Redis at BOSON_TEST_REDIS_URL — run with --include-ignored"
);
