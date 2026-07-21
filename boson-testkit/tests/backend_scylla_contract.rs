//! Shared [`QueueBackend`] contract tests for Scylla.
//!
//! Requires `BOSON_TEST_SCYLLA_CONTACT_POINTS` (cloud or CI service — not local multi-node Docker).
//!
//! ```bash
//! export CARGO_BUILD_JOBS=1
//! export BOSON_TEST_SCYLLA_CONTACT_POINTS=10.0.0.1:9042
//! cargo test -p boson-backend-scylla -- --ignored
//! ```

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_scylla::{
    isolated_keyspace, scylla_test_contact_points, ScyllaQueueBackend, ScyllaQueueConfig,
};
use boson_core::QueueBackend;

async fn fresh() -> Option<Arc<dyn QueueBackend>> {
    let points = scylla_test_contact_points()?;
    let keyspace = isolated_keyspace("boson_contract");
    let backend = ScyllaQueueBackend::connect(ScyllaQueueConfig {
        contact_points: points,
        keyspace,
        ..Default::default()
    })
    .await
    .expect("scylla connect");
    Some(Arc::new(backend) as Arc<dyn QueueBackend>)
}

boson_testkit::backend_contract_suite!(
    fresh,
    "scylla",
    ignore = "requires BOSON_TEST_SCYLLA_CONTACT_POINTS — run with --include-ignored"
);
