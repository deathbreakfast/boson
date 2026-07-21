//! Matrix smoke — bootstrap + scenario wiring.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use boson_testkit::{
    matrix::{matrix_isolated_lab, smoke_storage_backends, BackendAdapter},
    run_named_catalog_entry, BootstrapSession, ScenarioSpec,
};

const SMOKE_CATALOG_IDS: &[&str] = &[
    "enqueue_and_drain",
    "enqueue_only",
    "run_lifecycle",
    "idempotency_smoke",
];

#[tokio::test(flavor = "multi_thread")]
async fn matrix_ci_backends_bootstrap_install() {
    for backend in smoke_storage_backends() {
        let mut session = BootstrapSession::new(matrix_isolated_lab(*backend));
        session.install().await.expect("bootstrap");
        assert!(session.is_ready());
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn matrix_smoke_catalog_sequential_mem() {
    for id in SMOKE_CATALOG_IDS {
        run_named_catalog_entry(BackendAdapter::Mem, id).await;
    }
}

#[test]
fn scenario_enqueue_and_drain_spec_roundtrips_json() {
    let spec = ScenarioSpec::enqueue_and_drain("noop");
    let json = serde_json::to_string(&spec).expect("serialize");
    let back: ScenarioSpec = serde_json::from_str(&json).expect("deserialize");
    assert_eq!(spec, back);
}
