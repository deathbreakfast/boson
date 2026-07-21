//! Shared [`QueueBackend`] contract tests for `PostgreSQL`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_postgres::{postgres_test_url, PostgresQueueBackend};
use boson_core::QueueBackend;
use uuid::Uuid;

async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    if std::env::var("BOSON_TEST_POSTGRES_URL").is_err()
        && std::env::var("BOSON_BENCH_POSTGRES_URL").is_err()
    {
        return None;
    }
    let url = postgres_test_url();
    let schema = format!("boson_test_{}", Uuid::new_v4().simple());
    Some(Arc::new(
        PostgresQueueBackend::connect_isolated(&url, &schema)
            .await
            .expect("connect"),
    ) as Arc<dyn QueueBackend>)
}

boson_testkit::backend_contract_suite!(
    fresh,
    "postgres",
    ignore = "requires BOSON_TEST_POSTGRES_URL — run with --include-ignored"
);
