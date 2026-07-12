//! Shared [`QueueBackend`] contract assertions.

use std::sync::Arc;

use boson_core::{
    JobEnqueueDisposition, JobStatus, QueueBackend, RateLimitPolicy, RunStatus, TaskConfig,
};
use serde_json::json;

/// Fixture environment for backend contract tests.
pub struct BackendEnv {
    /// Optional label for debugging.
    pub label: &'static str,
}

impl BackendEnv {
    /// New fixture env.
    #[must_use]
    pub const fn new(label: &'static str) -> Self {
        Self { label }
    }
}

fn task_config(task_name: &str) -> TaskConfig {
    TaskConfig::default_for(task_name)
}

fn task_config_with_rate_limit(task_name: &str, max_in_flight: u32, max_eps: u32) -> TaskConfig {
    let mut config = TaskConfig::default_for(task_name);
    config.rate_limit_policy = RateLimitPolicy {
        max_in_flight,
        max_enqueue_per_second: max_eps,
    };
    config
}

fn sample_job(task_name: &str, pool: &str, priority: i32, idempotency_key: Option<&str>) -> boson_core::Job {
    boson_core::Job::new(
        task_name,
        json!({"label": "test"}),
        json!({}),
        priority,
        pool,
        0,
        idempotency_key.map(str::to_string),
    )
}

async fn enqueue(
    backend: &dyn QueueBackend,
    job: boson_core::Job,
    config: &TaskConfig,
) -> (String, JobEnqueueDisposition) {
    backend
        .enqueue_with_policies(job, config)
        .await
        .expect("enqueue")
}

/// Asserts enqueue inserts and lists the job.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn enqueue_inserts_and_lists(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, disp) = enqueue(&*b, job, &config).await;
    assert_eq!(disp, JobEnqueueDisposition::InsertedNew);
    let listed = b.list_jobs(Some(JobStatus::Queued), 0, 10).await.unwrap();
    assert!(listed.iter().any(|j| j.job_id == job_id));
}

/// Asserts idempotency reuses non-terminal jobs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn idempotency_reuses_nonterminal(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job1 = sample_job("echo", "global", 1, Some("idem-1"));
    let (id1, _) = enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, Some("idem-1"));
    let (id2, disp) = enqueue(&*b, job2, &config).await;
    assert_eq!(disp, JobEnqueueDisposition::ReusedIdempotent);
    assert_eq!(id1, id2);
}

/// Asserts atomic job claim semantics.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn try_claim_atomic(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, _) = enqueue(&*b, job, &config).await;
    assert!(b.try_claim_job(&job_id).await.unwrap().is_some());
    assert!(b.try_claim_job(&job_id).await.unwrap().is_none());
}

/// Asserts pool priority ordering for queued jobs.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn pool_priority_order(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let low = sample_job("echo", "workers", 1, None);
    let high = sample_job("echo", "workers", 5, None);
    enqueue(&*b, high, &config).await;
    enqueue(&*b, low, &config).await;
    let queued = b.list_queued_for_pool_sorted("workers", 10).await.unwrap();
    assert_eq!(queued.len(), 2);
    assert!(queued[0].priority <= queued[1].priority);
}

/// Asserts max in-flight rate limit enforcement.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn max_in_flight_rate_limit(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config_with_rate_limit("echo", 1, 0);
    let job1 = sample_job("echo", "global", 1, None);
    enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, None);
    let err = b
        .enqueue_with_policies(job2, &config)
        .await
        .unwrap_err();
    assert!(matches!(err, boson_core::BosonError::RateLimited(_)));
}

/// Asserts max enqueue per second rate limit enforcement.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn max_enqueue_per_second(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config_with_rate_limit("echo", 0, 1);
    let job1 = sample_job("echo", "global", 1, None);
    enqueue(&*b, job1, &config).await;
    let job2 = sample_job("echo", "global", 1, None);
    let err = b
        .enqueue_with_policies(job2, &config)
        .await
        .unwrap_err();
    assert!(matches!(err, boson_core::BosonError::RateLimited(_)));
}

/// Asserts run lifecycle persistence.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn run_lifecycle(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    let (job_id, _) = enqueue(&*b, job, &config).await;
    let run = boson_core::Run::new(&job_id, "echo", 1);
    let run_id = run.run_id.clone();
    b.upsert_run(&run).await.unwrap();
    b.finish_run(&run_id, RunStatus::Success, Some(10), None)
        .await
        .unwrap();
    let loaded = b.get_run(&run_id).await.unwrap().expect("run");
    assert_eq!(loaded.status, RunStatus::Success);
}

/// Asserts the backend can be installed as the process-global default and resolved.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn global_router_resolves(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(Arc::clone(&b)));
    let config = task_config("echo");
    let job = sample_job("echo", "global", 1, None);
    enqueue(&*b, job, &config).await;
    let resolved = boson_core::default_backend_from_global().unwrap();
    assert!(Arc::ptr_eq(&b, &resolved));
}

/// Asserts lease contention blocks a second worker claim.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn lease_contention(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let job_id = "job-1";
    let l1 = b
        .try_claim_run_lease(job_id, "worker-a", 120)
        .await
        .unwrap()
        .expect("first claim");
    assert!(b
        .try_claim_run_lease(job_id, "worker-b", 120)
        .await
        .unwrap()
        .is_none());
    b.release_lease(&l1).await.unwrap();
    assert!(b
        .try_claim_run_lease(job_id, "worker-b", 120)
        .await
        .unwrap()
        .is_some());
}

/// Asserts lease extension keeps the lease active.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn extend_lease_refreshes_ttl(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let lease_id = b
        .try_claim_run_lease("job-2", "worker-a", 60)
        .await
        .unwrap()
        .expect("claim");
    b.extend_lease(&lease_id, 300).await.unwrap();
    assert!(b.expired_lease_job_pairs().await.unwrap().is_empty());
}

/// Asserts expired leases appear in expired pairs listing.
///
/// # Panics
///
/// Panics if backend operations fail or contract assertions are violated.
pub async fn expired_lease_pairs(b: Arc<dyn QueueBackend>, _env: &BackendEnv) {
    let lease_id = b
        .try_claim_run_lease("job-3", "worker-a", -1)
        .await
        .unwrap()
        .expect("claim with negative ttl => already expired");
    let pairs = b.expired_lease_job_pairs().await.unwrap();
    assert!(pairs.iter().any(|(lid, jid)| lid == &lease_id && jid == "job-3"));
}
