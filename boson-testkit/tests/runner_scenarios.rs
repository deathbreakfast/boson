//! Integration tests for [`ScenarioRunner`](boson_testkit::runner::ScenarioRunner).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use boson_testkit::bootstrap::BootstrapSession;
use boson_testkit::fixtures::{
    register_fail_n_then_ok_task, register_noop_task, register_rate_limited_in_flight_task,
    reset_noop_hits,
};
use boson_testkit::matrix::MatrixSpec;
use boson_testkit::runner::{RunMode, ScenarioRunner};
use boson_testkit::scenario::ScenarioSpec;

async fn install_session(
    register: impl FnOnce(&mut boson_runtime::TaskRegistry),
) -> BootstrapSession {
    let mut session = BootstrapSession::new(MatrixSpec::ci_mem_isolated_lab());
    register(session.registry_mut().expect("unique registry"));
    session.install().await.expect("install");
    session
}

#[tokio::test]
async fn runner_enqueue_and_drain_correctness() {
    reset_noop_hits();
    let session = install_session(|r| register_noop_task(r, "noop")).await;
    let result = ScenarioRunner::new(&session)
        .run(
            &ScenarioSpec::enqueue_and_drain("noop"),
            RunMode::Correctness,
        )
        .await
        .expect("run");
    assert!(result.error.is_none(), "{:?}", result.error);
}

#[tokio::test]
async fn runner_enqueue_only_benchmark_timings() {
    let session = install_session(|r| register_noop_task(r, "noop")).await;
    let result = ScenarioRunner::new(&session)
        .run(&ScenarioSpec::enqueue_only("noop", 3), RunMode::Benchmark)
        .await
        .expect("run");
    assert!(result.error.is_none());
    assert_eq!(result.jobs_enqueued, 3);
    assert!(result.step_timings.iter().any(|t| t.op == "enqueue"));
}

#[tokio::test]
async fn runner_assert_enqueue_error_rate_limited() {
    let session = install_session(|r| register_rate_limited_in_flight_task(r, "limited")).await;
    let result = ScenarioRunner::new(&session)
        .run(
            &ScenarioSpec::rate_limit_in_flight("limited"),
            RunMode::Correctness,
        )
        .await
        .expect("run");
    assert!(result.error.is_none(), "{:?}", result.error);
}

#[tokio::test]
async fn runner_retry_backoff_succeeds() {
    let session = install_session(|r| register_fail_n_then_ok_task(r, "retryable", 2)).await;
    let result = ScenarioRunner::new(&session)
        .run(
            &ScenarioSpec::retry_then_success("retryable", 2),
            RunMode::Correctness,
        )
        .await
        .expect("run");
    assert!(result.error.is_none(), "{:?}", result.error);
}
