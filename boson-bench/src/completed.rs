//! Production-shaped completed durable tasks/s (BM-BC1).
//!
//! Steady enqueue of mixed sleep / retry-once jobs while background workers run with
//! hardened leases (`lease_ttl_secs = 30`) and run-row persistence. Primary metric is
//! terminal [`JobStatus::Success`](boson_core::JobStatus::Success) per second.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use boson_core::{ExecutionContextFactory, JobStatus, QueueBackend};
use boson_runtime::{spawn_worker, Boson, TaskRegistry, WorkerSettings};
use boson_testkit::fixtures::{empty_params, reset_sleep_hits, sleep_hit_count, system_actor};
use boson_testkit::BootstrapSession;
use serde_json::json;
use tokio::task::JoinSet;

use crate::config::BenchRunConfig;
use crate::report::ReportMetrics;

const SLEEP_TASK: &str = "sleep";
const RETRY_TASK: &str = "retryable";
const SLEEP_MS_MIN: u64 = 100;
const SLEEP_MS_SPAN: u64 = 401;

/// `BOSON_SKIP_RUN_ROWS` truthy values (`1` / `true` / `yes`).
#[must_use]
pub fn skip_run_rows_from_env() -> bool {
    std::env::var("BOSON_SKIP_RUN_ROWS")
        .is_ok_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

/// Fail-closed reason when leases or run persistence are disabled.
#[must_use]
pub fn closed_reason(cfg: &BenchRunConfig) -> Option<&'static str> {
    if cfg.drain.lease_ttl_secs <= 0 {
        return Some("disabled leases");
    }
    if cfg.drain.skip_run_persistence || skip_run_rows_from_env() {
        return Some("skipped run persistence");
    }
    None
}

fn duration_secs(cfg: &BenchRunConfig) -> u64 {
    std::env::var("BOSON_BENCH_BC1_DURATION_SECS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(cfg.publisher.duration_secs)
}

fn job_count_cap(cfg: &BenchRunConfig) -> Option<u64> {
    std::env::var("BOSON_BENCH_BC1_JOB_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(cfg.publisher.job_count)
}

fn is_retry_slot(n: u64) -> bool {
    n.is_multiple_of(5)
}

fn sleep_ms_for(n: u64) -> u64 {
    SLEEP_MS_MIN + (n % SLEEP_MS_SPAN)
}

fn guard_metrics(cfg: &BenchRunConfig) -> ReportMetrics {
    ReportMetrics {
        achieved_ops_per_sec: Some(0.0),
        completed_ops_per_sec: Some(0.0),
        worker_count: Some(cfg.drain.worker_count),
        lease_ttl_secs: Some(cfg.drain.lease_ttl_secs),
        run_rows_enabled: Some(!cfg.drain.skip_run_persistence && !skip_run_rows_from_env()),
        duplicate_executions: Some(0),
        residual_backlog: Some(0),
        metric_kind: Some("completed".into()),
        ..Default::default()
    }
}

/// BM-BC1: W background workers, 60s (default) mixed enqueue, bounded drain tail.
pub async fn run_completed(
    session: &BootstrapSession,
    backend: Arc<dyn QueueBackend>,
    registry: Arc<TaskRegistry>,
    identity: Arc<dyn ExecutionContextFactory>,
    runtime_label: &str,
    cfg: &BenchRunConfig,
) -> Result<ReportMetrics> {
    if closed_reason(cfg).is_some() {
        return Ok(guard_metrics(cfg));
    }

    reset_sleep_hits();
    let boson = Arc::new(session.build_boson_manual()?.0);
    let workers = cfg.drain.worker_count.max(1);
    spawn_completed_workers(&backend, &registry, &identity, runtime_label, cfg, workers);

    let start = Instant::now();
    let enqueued = enqueue_mix(Arc::clone(&boson), cfg, start).await?;
    let drain_deadline = Duration::from_secs(cfg.drain.timeout_secs);
    let backlog = wait_for_idle(&backend, drain_deadline).await?;
    let elapsed = start.elapsed().as_secs_f64().max(f64::EPSILON);
    finish_metrics(&backend, cfg, workers, enqueued, backlog, elapsed).await
}

fn spawn_completed_workers(
    backend: &Arc<dyn QueueBackend>,
    registry: &Arc<TaskRegistry>,
    identity: &Arc<dyn ExecutionContextFactory>,
    runtime_label: &str,
    cfg: &BenchRunConfig,
    workers: u32,
) {
    for w in 0..workers {
        let worker = WorkerSettings {
            worker_id: format!("bench-bc1-{w}"),
            lease_ttl_secs: cfg.drain.lease_ttl_secs,
            runtime_label: runtime_label.to_string(),
            worker_pools: cfg.worker_fleet.worker_pools.clone(),
            worker_poll_interval_ms: cfg.drain.poll_interval_ms,
            skip_run_persistence: false,
        };
        spawn_worker(
            Arc::clone(backend),
            Arc::clone(registry),
            Arc::clone(identity),
            worker,
        );
    }
}

struct EnqueueCounts {
    total: u64,
    sleep: u64,
}

async fn enqueue_mix(
    boson: Arc<Boson>,
    cfg: &BenchRunConfig,
    start: Instant,
) -> Result<EnqueueCounts> {
    let duration = Duration::from_secs(duration_secs(cfg));
    let cap = job_count_cap(cfg);
    let seq = Arc::new(AtomicU64::new(0));
    let sleep_n = Arc::new(AtomicU64::new(0));
    let clients = cfg.publisher.client_count.max(1);
    let mut join = JoinSet::new();

    for _ in 0..clients {
        let boson = Arc::clone(&boson);
        let seq = Arc::clone(&seq);
        let sleep_n = Arc::clone(&sleep_n);
        join.spawn(async move {
            loop {
                if start.elapsed() >= duration {
                    break;
                }
                let n = seq.fetch_add(1, Ordering::Relaxed);
                if cap.is_some_and(|max| n >= max) {
                    break;
                }
                let (task, params) = if is_retry_slot(n) {
                    (RETRY_TASK, empty_params())
                } else {
                    sleep_n.fetch_add(1, Ordering::Relaxed);
                    (SLEEP_TASK, json!({ "ms": sleep_ms_for(n) }))
                };
                let _ = boson.enqueue(task, system_actor(), params, None).await;
            }
        });
    }

    while join.join_next().await.transpose()?.is_some() {}
    let total = match cap {
        Some(max) => seq.load(Ordering::Relaxed).min(max),
        None => seq.load(Ordering::Relaxed),
    };
    Ok(EnqueueCounts {
        total,
        sleep: sleep_n.load(Ordering::Relaxed),
    })
}

async fn wait_for_idle(backend: &Arc<dyn QueueBackend>, timeout: Duration) -> Result<u64> {
    let deadline = Instant::now() + timeout;
    loop {
        let backlog = queued_and_running(backend).await?;
        if backlog == 0 {
            return Ok(0);
        }
        if Instant::now() >= deadline {
            return Ok(backlog);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn queued_and_running(backend: &Arc<dyn QueueBackend>) -> Result<u64> {
    let queued = backend.count_jobs(Some(JobStatus::Queued)).await?;
    let running = backend.count_jobs(Some(JobStatus::Running)).await?;
    Ok(queued.saturating_add(running))
}

async fn finish_metrics(
    backend: &Arc<dyn QueueBackend>,
    cfg: &BenchRunConfig,
    workers: u32,
    enqueued: EnqueueCounts,
    backlog: u64,
    elapsed: f64,
) -> Result<ReportMetrics> {
    let success = backend.count_jobs(Some(JobStatus::Success)).await?;
    let failed = backend.count_jobs(Some(JobStatus::Failed)).await?;
    let canceled = backend.count_jobs(Some(JobStatus::Canceled)).await?;
    let terminal_fail = failed.saturating_add(canceled);
    let sleep_hits = u64::try_from(sleep_hit_count()).unwrap_or(u64::MAX);
    let duplicates = sleep_hits.saturating_sub(enqueued.sleep);
    let error_rate = if enqueued.total == 0 {
        0.0
    } else {
        terminal_fail as f64 / enqueued.total as f64
    };
    let rate = success as f64 / elapsed;
    Ok(ReportMetrics {
        achieved_ops_per_sec: Some(rate),
        completed_ops_per_sec: Some(rate),
        error_rate: Some(error_rate),
        worker_count: Some(workers),
        pool_count: Some(cfg.publisher.pool_count),
        duplicate_executions: Some(duplicates),
        residual_backlog: Some(backlog),
        lease_ttl_secs: Some(cfg.drain.lease_ttl_secs),
        run_rows_enabled: Some(true),
        metric_kind: Some("completed".into()),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BC1_LEASE_TTL_SECS;
    use boson_core::ExecutionContextFactory;
    use boson_testkit::fixtures::{
        register_fail_n_then_ok_task, register_sleep_task, reset_sleep_hits,
    };
    use boson_testkit::matrix::MatrixSpec;
    use boson_testkit::StubExecutionContextFactory;
    use tokio::sync::Mutex;

    static MEM_LOCK: Mutex<()> = Mutex::const_new(());

    fn bc1_test_cfg() -> BenchRunConfig {
        let mut cfg = BenchRunConfig::for_experiment("bm-bc1");
        cfg.publisher.duration_secs = 2;
        cfg.publisher.job_count = Some(8);
        cfg.drain.worker_count = 2;
        cfg.drain.poll_interval_ms = 0;
        cfg.drain.timeout_secs = 15;
        cfg
    }

    async fn install_bc1() -> (
        boson_testkit::BootstrapSession,
        Arc<dyn QueueBackend>,
        Arc<TaskRegistry>,
        Arc<dyn ExecutionContextFactory>,
    ) {
        let mut session = boson_testkit::BootstrapSession::new(MatrixSpec::ci_mem_isolated_lab());
        {
            let registry = session.registry_mut().expect("unique registry");
            register_sleep_task(registry, SLEEP_TASK);
            register_fail_n_then_ok_task(registry, RETRY_TASK, 1);
        }
        session.install().await.expect("install");
        let backend = session.backend().expect("backend");
        let registry = session.registry();
        let identity: Arc<dyn ExecutionContextFactory> = Arc::new(StubExecutionContextFactory);
        (session, backend, registry, identity)
    }

    #[test]
    fn closed_reason_disabled_leases() {
        let mut cfg = BenchRunConfig::for_experiment("bm-bc1");
        cfg.drain.lease_ttl_secs = 0;
        assert_eq!(closed_reason(&cfg), Some("disabled leases"));
    }

    #[test]
    fn closed_reason_skipped_run_rows() {
        let mut cfg = BenchRunConfig::for_experiment("bm-bc1");
        cfg.drain.skip_run_persistence = true;
        assert_eq!(closed_reason(&cfg), Some("skipped run persistence"));
    }

    #[test]
    fn closed_reason_ok_when_hardened() {
        let cfg = BenchRunConfig::for_experiment("bm-bc1");
        assert_eq!(cfg.drain.lease_ttl_secs, BC1_LEASE_TTL_SECS);
        assert!(closed_reason(&cfg).is_none());
    }

    #[test]
    fn mix_is_eighty_sleep_twenty_retry() {
        let retry = (0..100).filter(|n| is_retry_slot(*n)).count();
        assert_eq!(retry, 20);
        assert!((100..=500).contains(&sleep_ms_for(0)));
        assert!((100..=500).contains(&sleep_ms_for(400)));
    }

    #[tokio::test]
    async fn mem_sleep_retry_reaches_success() {
        let _guard = MEM_LOCK.lock().await;
        reset_sleep_hits();
        let cfg = bc1_test_cfg();
        let (session, backend, registry, identity) = install_bc1().await;
        let metrics = run_completed(&session, backend, registry, identity, "isolated-lab", &cfg)
            .await
            .expect("run");
        assert_eq!(metrics.lease_ttl_secs, Some(BC1_LEASE_TTL_SECS));
        assert_eq!(metrics.run_rows_enabled, Some(true));
        assert_eq!(metrics.duplicate_executions, Some(0));
        assert_eq!(metrics.residual_backlog, Some(0));
        assert!(
            metrics.completed_ops_per_sec.unwrap_or(0.0) > 0.0,
            "expected terminal Success throughput, got {metrics:?}"
        );
        let (pass, notes) = crate::pass_eval::evaluate("bm-bc1", &metrics, None);
        assert!(pass, "{notes}");
    }

    #[tokio::test]
    async fn fail_closed_on_skip_run_rows() {
        let _guard = MEM_LOCK.lock().await;
        let mut cfg = bc1_test_cfg();
        cfg.drain.skip_run_persistence = true;
        let (session, backend, registry, identity) = install_bc1().await;
        let metrics = run_completed(&session, backend, registry, identity, "isolated-lab", &cfg)
            .await
            .expect("run");
        assert_eq!(metrics.run_rows_enabled, Some(false));
        let (pass, notes) = crate::pass_eval::evaluate("bm-bc1", &metrics, None);
        assert!(!pass, "skip_run_rows must fail closed");
        assert!(
            notes.contains("run persistence") || notes.contains("FAIL"),
            "{notes}"
        );
    }

    #[tokio::test]
    async fn fail_closed_on_lease_ttl_zero() {
        let _guard = MEM_LOCK.lock().await;
        let mut cfg = bc1_test_cfg();
        cfg.drain.lease_ttl_secs = 0;
        let (session, backend, registry, identity) = install_bc1().await;
        let metrics = run_completed(&session, backend, registry, identity, "isolated-lab", &cfg)
            .await
            .expect("run");
        assert_eq!(metrics.lease_ttl_secs, Some(0));
        let (pass, notes) = crate::pass_eval::evaluate("bm-bc1", &metrics, None);
        assert!(!pass, "lease_ttl=0 must fail closed");
        assert!(notes.contains("lease") || notes.contains("FAIL"), "{notes}");
    }

    #[tokio::test]
    async fn timeout_writes_fail_report_without_panic() {
        let _guard = MEM_LOCK.lock().await;
        reset_sleep_hits();
        let mut cfg = bc1_test_cfg();
        cfg.publisher.job_count = Some(16);
        cfg.drain.worker_count = 1;
        cfg.drain.timeout_secs = 0;
        let (session, backend, registry, identity) = install_bc1().await;
        let metrics = run_completed(&session, backend, registry, identity, "isolated-lab", &cfg)
            .await
            .expect("timeout must return metrics, not panic");
        let (pass, notes) = crate::pass_eval::evaluate("bm-bc1", &metrics, None);
        assert!(!pass, "timeout with residual work must fail: {notes}");
        assert!(
            metrics.residual_backlog.unwrap_or(0) > 0 || notes.contains("FAIL"),
            "{notes:?} {metrics:?}"
        );
    }
}
