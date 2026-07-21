//! Redis broker fleet routing contract tests (requires 2 Redis URLs).

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use boson_backend_redis::{connect_fleet_from_env, keys, RedisQueueBackend};
use boson_core::{Job, QueueBackend};
use boson_testkit::fixtures::{empty_params, system_actor};

async fn fleet_backend() -> Option<Arc<dyn QueueBackend>> {
    let urls = std::env::var("BOSON_REDIS_URLS").ok()?;
    if urls.split(',').filter(|s| !s.trim().is_empty()).count() < 2 {
        return None;
    }
    connect_fleet_from_env().await.ok()
}

#[tokio::test]
#[ignore = "requires BOSON_REDIS_URLS with 2+ brokers"]
async fn fleet_routes_pools_to_distinct_brokers() {
    let Some(backend) = fleet_backend().await else {
        eprintln!("skip: set BOSON_REDIS_URLS=redis://127.0.0.1:6379,redis://127.0.0.1:6380");
        return;
    };

    let url0 = std::env::var("BOSON_REDIS_URLS")
        .unwrap()
        .split(',')
        .next()
        .unwrap()
        .trim()
        .to_string();
    let url1 = std::env::var("BOSON_REDIS_URLS")
        .unwrap()
        .split(',')
        .nth(1)
        .unwrap()
        .trim()
        .to_string();

    let ks = keys::Keyspace::from_env();
    let b0 = RedisQueueBackend::connect_with_keyspace(&url0, ks.clone())
        .await
        .expect("broker0");
    let b1 = RedisQueueBackend::connect_with_keyspace(&url1, ks)
        .await
        .expect("broker1");

    let tc = boson_core::TaskConfig::default_for("noop");

    let mut j0 = Job::new("noop", system_actor(), empty_params(), 0, "pool_0", 0, None);
    j0.job_id = "fleet-test-pool0".into();
    backend.enqueue_with_policies(j0, &tc).await.unwrap();

    let mut j1 = Job::new("noop", system_actor(), empty_params(), 0, "pool_1", 0, None);
    j1.job_id = "fleet-test-pool1".into();
    backend.enqueue_with_policies(j1, &tc).await.unwrap();

    assert!(b0.get_job("fleet-test-pool0").await.unwrap().is_some());
    assert!(b0.get_job("fleet-test-pool1").await.unwrap().is_none());
    assert!(b1.get_job("fleet-test-pool1").await.unwrap().is_some());
    assert!(b1.get_job("fleet-test-pool0").await.unwrap().is_none());
}
