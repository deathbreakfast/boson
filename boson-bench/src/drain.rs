//! Dequeue capacity experiments BM-BD* (prefill then parallel drain).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use boson_core::{ExecutionContextFactory, JobStatus, QueueBackend};
use boson_runtime::{spawn_worker, Boson, ManualWorker, TaskRegistry, WorkerSettings};
use boson_testkit::{fixtures::{empty_params, noop_hit_count, system_actor}, BootstrapSession};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::config::{BenchRunConfig, PoolLayout};
use crate::report::ReportMetrics;
use crate::resource_profile::ResourceSampler;
use crate::stats::MetricStats;

fn bench_client_index() -> Option<u32> {
    std::env::var("BOSON_BENCH_CLIENT_INDEX")
        .ok()
        .and_then(|v| v.parse().ok())
}

fn bench_client_count() -> Option<u32> {
    std::env::var("BOSON_BENCH_CLIENT_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
}

fn drain_only_mode() -> bool {
    std::env::var("BOSON_BENCH_DRAIN_ONLY")
        .ok()
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn central_prefill_mode() -> bool {
    std::env::var("BOSON_BENCH_CENTRAL_PREFILL")
        .ok()
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

fn pin_worker_pools_per_slot() -> bool {
    std::env::var("BOSON_BD2_PIN_WORKER_POOLS")
        .ok()
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}

/// Inclusive start, exclusive end pool slot indices for multibench client `client`.
fn multibench_pool_range(cfg: &BenchRunConfig, client: u32, clients: u32) -> Option<(u32, u32)> {
    if clients <= 1 {
        return None;
    }
    let k = cfg.publisher.pool_count.max(1);
    if k <= 1 || cfg.publisher.pool_layout != PoolLayout::DistinctPerSlot {
        return None;
    }
    let start = (client * k) / clients;
    let end = ((client + 1) * k) / clients;
    if start >= end {
        return None;
    }
    Some((start, end))
}

fn pool_names_for_range(start: u32, end: u32) -> Vec<String> {
    (start..end)
        .map(BenchRunConfig::pool_name_for_slot)
        .collect()
}

fn resolved_multibench_range(cfg: &BenchRunConfig) -> Option<(u32, u32)> {
    let bc = bench_client_count()?;
    let bi = bench_client_index()?;
    multibench_pool_range(cfg, bi, bc)
}

fn worker_pools_for(cfg: &BenchRunConfig, worker_index: u32) -> Option<Vec<String>> {
    // When multibench partitions pools to this client, pin within that range so
    // workers do not steal jobs from other embeds' pools (E1 cross-steal).
    if let Some((start, end)) = resolved_multibench_range(cfg) {
        if pin_worker_pools_per_slot() {
            let span = end.saturating_sub(start).max(1);
            let slot = start + (worker_index % span);
            return Some(vec![BenchRunConfig::pool_name_for_slot(slot)]);
        }
        return Some(pool_names_for_range(start, end));
    }
    if pin_worker_pools_per_slot() {
        let k = cfg.publisher.pool_count.max(1);
        if k > 1 && cfg.publisher.pool_layout == PoolLayout::DistinctPerSlot {
            return Some(vec![BenchRunConfig::pool_name_for_slot(worker_index % k)]);
        }
    }
    cfg.worker_fleet.worker_pools.clone()
}

async fn enqueue_prefill_slot(boson: &Boson, slot: u32, pool_count: u32) -> Result<()> {
    let task = if pool_count == 1 {
        "noop".to_string()
    } else {
        format!("noop_{slot}")
    };
    boson
        .enqueue(&task, system_actor(), empty_params(), None)
        .await?;
    Ok(())
}

/// Enqueue `count` noop jobs with no worker running.
///
/// Uses round-robin on distinct pools when `pool_count > 1` and
/// [`PoolLayout::DistinctPerSlot`]; otherwise enqueues to shared `noop` on `global`.
/// Multibench clients (`BOSON_BENCH_CLIENT_*`) partition pools to avoid cross-embed steal.
pub async fn prefill_queue(boson: &Boson, count: u64, cfg: &BenchRunConfig) -> Result<()> {
    if count == 0 {
        return Ok(());
    }
    let pool_count = cfg.publisher.pool_count.max(1);
    let distinct = pool_count > 1 && cfg.publisher.pool_layout == PoolLayout::DistinctPerSlot;

    if drain_only_mode() {
        if let Some(bi) = bench_client_index() {
            if bi > 0 {
                return Ok(());
            }
        }
        if !central_prefill_mode() {
            return Ok(());
        }
        let bc = bench_client_count().unwrap_or(1);
        if distinct {
            for client in 0..bc {
                let Some((start, end)) = multibench_pool_range(cfg, client, bc) else {
                    continue;
                };
                let span = end - start;
                for i in 0..count {
                    let slot = start + (i % u64::from(span)) as u32;
                    enqueue_prefill_slot(boson, slot, pool_count).await?;
                }
            }
            return Ok(());
        }
    }

    if distinct {
        if let Some((start, end)) = resolved_multibench_range(cfg) {
            let span = end - start;
            for i in 0..count {
                let slot = start + (i % u64::from(span)) as u32;
                enqueue_prefill_slot(boson, slot, pool_count).await?;
            }
            return Ok(());
        }
    }

    for i in 0..count {
        let task = if distinct {
            cfg.task_name_for_client(i as u32)
        } else {
            "noop".to_string()
        };
        boson
            .enqueue(&task, system_actor(), empty_params(), None)
            .await?;
    }
    Ok(())
}

async fn drain_timeout_message(
    backend: &Arc<dyn QueueBackend>,
    target: u64,
    hits_start: usize,
) -> String {
    let completed = noop_hit_count().saturating_sub(hits_start);
    let queued = backend.count_jobs(Some(JobStatus::Queued)).await.unwrap_or(u64::MAX);
    let running = backend.count_jobs(Some(JobStatus::Running)).await.unwrap_or(u64::MAX);
    format!(
        "drain timeout: completed {completed} of {target} jobs (queued={queued} running={running})"
    )
}

async fn wait_for_drain(
    backend: &Arc<dyn QueueBackend>,
    target: u64,
    hits_start: usize,
    timeout: Duration,
) -> Result<()> {
    let deadline = Instant::now() + timeout;
    loop {
        if u64::try_from(noop_hit_count().saturating_sub(hits_start)).unwrap_or(u64::MAX) >= target {
            return Ok(());
        }
        if Instant::now() >= deadline {
            anyhow::bail!(drain_timeout_message(backend, target, hits_start).await);
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

fn effective_prefill_count(base: u64, _cfg: &BenchRunConfig) -> u64 {
    if drain_only_mode() && central_prefill_mode() {
        if bench_client_index().unwrap_or(0) > 0 {
            return 0;
        }
        return base;
    }
    base
}

const fn effective_drain_target(base: u64) -> u64 {
    base
}

/// BM-BD1: W parallel [`ManualWorker`] tasks draining a prefilled queue.
pub async fn run_manual_drain(
    boson: Boson,
    backend: Arc<dyn QueueBackend>,
    registry: Arc<TaskRegistry>,
    identity: Arc<dyn ExecutionContextFactory>,
    worker_settings: WorkerSettings,
    bench_cfg: &BenchRunConfig,
) -> Result<ReportMetrics> {
    let drain = &bench_cfg.drain;
    let n = effective_prefill_count(drain.prefill_count, bench_cfg);
    let target = effective_drain_target(drain.prefill_count);
    let workers = drain.worker_count.max(1);
    prefill_queue(&boson, n, bench_cfg).await?;
    let hits_start = noop_hit_count();
    let samples: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::with_capacity(target as usize)));
    let completed: Arc<AtomicU64> = Arc::new(AtomicU64::new(0));
    let drain_start = Instant::now();
    let mut join = JoinSet::new();

    for w in 0..workers {
        let mut settings = worker_settings.clone();
        settings.worker_id = format!("bench-manual-{w}");
        settings.worker_pools = worker_pools_for(bench_cfg, w);
        let manual = ManualWorker::new(
            Arc::clone(&backend),
            Arc::clone(&registry),
            Arc::clone(&identity),
            settings,
        );
        let samples = Arc::clone(&samples);
        let completed = Arc::clone(&completed);
        join.spawn(async move {
            loop {
                if completed.load(Ordering::Relaxed) >= target {
                    break;
                }
                let op_start = Instant::now();
                if manual.try_run_next().await {
                    completed.fetch_add(1, Ordering::Relaxed);
                    samples.lock().await.push(op_start.elapsed().as_secs_f64() * 1000.0);
                } else if noop_hit_count().saturating_sub(hits_start) as u64 >= target {
                    break;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });
    }

    wait_for_drain(
        &backend,
        target,
        hits_start,
        Duration::from_secs(drain.timeout_secs),
    )
    .await?;
    while join.join_next().await.transpose()?.is_some() {}

    let elapsed = drain_start.elapsed().as_secs_f64();
    let drain_stats = MetricStats::summarize(samples.lock().await.clone());
    Ok(drain_metrics(target, workers, elapsed, bench_cfg, Some(drain_stats)))
}

fn drain_metrics(
    n: u64,
    workers: u32,
    elapsed: f64,
    bench_cfg: &BenchRunConfig,
    drain_ms: Option<MetricStats>,
) -> ReportMetrics {
    let rate = n as f64 / elapsed;
    ReportMetrics {
        achieved_ops_per_sec: Some(rate),
        drain_ops_per_sec: Some(rate),
        prefill_count: Some(n),
        worker_count: Some(workers),
        pool_count: Some(bench_cfg.publisher.pool_count),
        drain_ms,
        p99_ms: drain_ms.map(|s| s.p99),
        metric_kind: Some("drain".into()),
        ..Default::default()
    }
}

/// BM-BD2: W background workers with configurable poll interval.
pub async fn run_background_drain(
    session: &BootstrapSession,
    backend: Arc<dyn QueueBackend>,
    registry: Arc<TaskRegistry>,
    identity: Arc<dyn ExecutionContextFactory>,
    runtime_label: &str,
    cfg: &BenchRunConfig,
) -> Result<ReportMetrics> {
    let drain = &cfg.drain;
    let n = effective_prefill_count(drain.prefill_count, cfg);
    let target = effective_drain_target(drain.prefill_count);
    let workers = drain.worker_count.max(1);
    let boson = session.build_boson_manual()?.0;
    prefill_queue(&boson, n, cfg).await?;
    let hits_start = noop_hit_count();
    let drain_start = Instant::now();
    let mut sampler = ResourceSampler::start();

    for w in 0..workers {
        let worker = WorkerSettings {
            worker_id: format!("bench-bg-{w}"),
            lease_ttl_secs: 0,
            runtime_label: runtime_label.to_string(),
            worker_pools: worker_pools_for(cfg, w),
            worker_poll_interval_ms: drain.poll_interval_ms,
            skip_run_persistence: std::env::var("BOSON_SKIP_RUN_ROWS")
                .ok()
                .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes")),
        };
        spawn_worker(
            Arc::clone(&backend),
            Arc::clone(&registry),
            Arc::clone(&identity),
            worker,
        );
    }

    wait_for_drain(
        &backend,
        target,
        hits_start,
        Duration::from_secs(drain.timeout_secs),
    )
    .await?;
    sampler.sample();
    let _profile = sampler.finish();

    let elapsed = drain_start.elapsed().as_secs_f64();
    Ok(drain_metrics(target, workers, elapsed, cfg, None))
}
