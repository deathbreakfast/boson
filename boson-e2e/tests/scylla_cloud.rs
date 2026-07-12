//! Scylla-only entrypoint for the shared correctness catalog.
//!
//! Prefer `cargo test -p boson-e2e --test scenarios_full -- --ignored scylla` for
//! per-scenario filters. This binary runs the full catalog against Scylla in one go.
//!
//! Requires `BOSON_TEST_SCYLLA_CONTACT_POINTS` (CI service or cloud — not local multi-node Docker).

use boson_testkit::{correctness_catalog, run_named_catalog_entry, BackendAdapter};

#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires BOSON_TEST_SCYLLA_CONTACT_POINTS — run with --include-ignored"]
async fn scylla_correctness_catalog() {
    for entry in correctness_catalog() {
        run_named_catalog_entry(BackendAdapter::Scylla, entry.id).await;
    }
}
