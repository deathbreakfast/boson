//! Multi-client and multi-pool scale experiments BM-BP* / BM-BM*.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use anyhow::Result;
use boson_runtime::Boson;
use boson_testkit::fixtures::{empty_params, system_actor};
use tokio::sync::Mutex;
use tokio::task::JoinSet;

use crate::config::{BenchRunConfig, PublisherConfig};
use crate::report::ReportMetrics;
use crate::resource_profile::ResourceSampler;
use crate::stats::MetricStats;

/// Multi-client enqueue for a fixed duration; returns aggregate metrics.
pub async fn run_enqueue_clients(
    boson: Arc<Boson>,
    publisher: &PublisherConfig,
    duration: Duration,
    start: Instant,
) -> Result<ReportMetrics> {
    let client_count = publisher.client_count.max(1);
    let pool_count = publisher.pool_count.max(1);
    let bench_cfg = BenchRunConfig {
        publisher: publisher.clone(),
        ..BenchRunConfig::default()
    };
    let counters = Arc::new(ScaleCounters::new());
    spawn_enqueue_clients(
        boson,
        &bench_cfg,
        client_count,
        duration,
        start,
        Arc::clone(&counters),
    )
    .await?;
    let elapsed = start.elapsed().as_secs_f64();
    let mut metrics = finalize_scale_metrics(&counters, elapsed, client_count, pool_count);
    let all_samples = counters.samples.lock().await.clone();
    let stats = MetricStats::summarize(all_samples);
    metrics.enqueue_ms = Some(stats);
    metrics.p99_ms = Some(stats.p99);
    Ok(metrics)
}

struct ScaleCounters {
    ops_ok: AtomicU64,
    ops_err: AtomicU64,
    samples: Mutex<Vec<f64>>,
}

impl ScaleCounters {
    fn new() -> Self {
        Self {
            ops_ok: AtomicU64::new(0),
            ops_err: AtomicU64::new(0),
            samples: Mutex::new(Vec::new()),
        }
    }
}

async fn spawn_enqueue_clients(
    boson: Arc<Boson>,
    bench_cfg: &BenchRunConfig,
    client_count: u32,
    duration: Duration,
    start: Instant,
    counters: Arc<ScaleCounters>,
) -> Result<()> {
    let mut join = JoinSet::new();
    for client in 0..client_count {
        let boson = Arc::clone(&boson);
        let counters = Arc::clone(&counters);
        let task = bench_cfg.task_name_for_client(client);
        join.spawn(async move {
            let mut local_ok = 0u64;
            let mut local_err = 0u64;
            let mut local_samples = Vec::new();
            let mut n = 0u64;
            while start.elapsed() < duration {
                let op_start = Instant::now();
                match boson
                    .enqueue(&task, system_actor(), empty_params(), None)
                    .await
                {
                    Ok(_) => {
                        local_ok += 1;
                        local_samples.push(op_start.elapsed().as_secs_f64() * 1000.0);
                    }
                    Err(_) => local_err += 1,
                }
                n += 1;
                if n.is_multiple_of(50) {
                    tokio::task::yield_now().await;
                }
            }
            counters.ops_ok.fetch_add(local_ok, Ordering::SeqCst);
            counters.ops_err.fetch_add(local_err, Ordering::SeqCst);
            counters.samples.lock().await.extend(local_samples);
        });
    }

    while start.elapsed() < duration {
        tokio::time::sleep(Duration::from_secs(1)).await;
    }
    while join.join_next().await.transpose()?.is_some() {}
    Ok(())
}

fn finalize_scale_metrics(
    counters: &ScaleCounters,
    elapsed: f64,
    client_count: u32,
    pool_count: u32,
) -> ReportMetrics {
    let ok = counters.ops_ok.load(Ordering::SeqCst);
    let err = counters.ops_err.load(Ordering::SeqCst);
    let total = ok + err;
    let error_rate = if total == 0 { 0.0 } else { err as f64 / total as f64 };
    let stats = MetricStats::summarize(Vec::new());

    ReportMetrics {
        achieved_ops_per_sec: Some(ok as f64 / elapsed),
        error_rate: Some(error_rate),
        enqueue_ms: Some(stats),
        p99_ms: Some(stats.p99),
        client_count: Some(client_count),
        pool_count: Some(pool_count),
        ..Default::default()
    }
}

/// Run a scale experiment (multi-client enqueue).
pub async fn run_scale(boson: Arc<Boson>, cfg: &BenchRunConfig) -> Result<ReportMetrics> {
    let publisher = &cfg.publisher;
    let duration = Duration::from_secs(publisher.duration_secs);
    let start = Instant::now();
    let mut sampler = ResourceSampler::start();
    let metrics = run_enqueue_clients(boson, publisher, duration, start).await?;
    sampler.sample();
    Ok(metrics)
}

/// Track F BM-BF2: multi-client enqueue spread across registered tasks.
pub async fn run_task_fanout(boson: Arc<Boson>, cfg: &BenchRunConfig) -> Result<ReportMetrics> {
    let task_count = cfg.task_fanout_count.max(1);
    let publisher = PublisherConfig {
        client_count: cfg.publisher.client_count.max(1),
        pool_count: task_count,
        pool_layout: cfg.publisher.pool_layout,
        duration_secs: cfg.publisher.duration_secs,
    };
    run_scale(
        boson,
        &BenchRunConfig {
            publisher,
            ..cfg.clone()
        },
    )
    .await
}
