//! Shared [`QueueBackend`] contract tests for NATS `JetStream` KV.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_nats::{keys, NatsQueueBackend};
use boson_core::QueueBackend;
use uuid::Uuid;

async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    let url = NatsQueueBackend::test_url();
    let keyspace = keys::Keyspace::new(format!("boson:{}", Uuid::new_v4()));
    let backend = match NatsQueueBackend::connect_with_keyspace(&url, keyspace).await {
        Ok(b) => b,
        Err(e) => {
            eprintln!("nats contract setup: {e}");
            return None;
        }
    };
    if backend.flush_namespace().await.is_err() {
        return None;
    }
    Some(Arc::new(backend) as Arc<dyn QueueBackend>)
}

boson_testkit::backend_contract_suite!(
    fresh,
    "nats",
    ignore = "requires NATS at BOSON_TEST_NATS_URL — run with --include-ignored"
);
