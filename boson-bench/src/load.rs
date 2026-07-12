//! Paced sustained load experiments BM-BL0 through BM-BL4.

use std::time::{Duration, Instant};

use anyhow::Result;
use boson_runtime::Boson;
use boson_testkit::fixtures::{empty_params, system_actor};
use tokio::time::sleep;

use crate::report::ReportMetrics;
use crate::resource_profile::ResourceSampler;
use crate::stats::MetricStats;

const LOAD_DURATION_SECS: u64 = 30;

/// Target ops/s for a load experiment id.
pub fn target_ops_for(id: &str) -> Option<u64> {
    match id {
        "bm-bl0" => Some(100),
        "bm-bl1" => Some(1_000),
        "bm-bl2" => Some(10_000),
        "bm-bl3" => Some(100_000),
        "bm-bl4" => Some(1_000_000),
        _ => None,
    }
}

/// Run a paced load experiment against an installed Boson instance.
pub async fn run_load(boson: &Boson, experiment_id: &str) -> Result<ReportMetrics> {
    let target_ops = target_ops_for(experiment_id)
        .ok_or_else(|| anyhow::anyhow!("unknown load experiment {experiment_id}"))?;

    let duration = Duration::from_secs(LOAD_DURATION_SECS);
    let start = Instant::now();
    let mut samples = Vec::new();
    let mut ops_ok = 0u64;
    let mut ops_err = 0u64;
    let mut next_tick = Instant::now();
    let mut resource_sampler = ResourceSampler::start();
    let interval = Duration::from_nanos(1_000_000_000 / target_ops.max(1));

    while start.elapsed() < duration {
        if Instant::now() < next_tick {
            sleep(next_tick - Instant::now()).await;
        }
        next_tick += interval;

        let op_start = Instant::now();
        match boson
            .enqueue("noop", system_actor(), empty_params(), None)
            .await
        {
            Ok(_) => {
                ops_ok += 1;
                samples.push(op_start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(_) => ops_err += 1,
        }

        if (ops_ok + ops_err).is_multiple_of(100) {
            resource_sampler.sample();
        }
    }

    resource_sampler.sample();
    let _profile = resource_sampler.finish();
    let elapsed = start.elapsed().as_secs_f64();
    let total = ops_ok + ops_err;
    let error_rate = if total == 0 {
        0.0
    } else {
        ops_err as f64 / total as f64
    };
    let stats = MetricStats::summarize(samples.clone());

    Ok(ReportMetrics {
        target_ops_per_sec: Some(target_ops),
        achieved_ops_per_sec: Some(ops_ok as f64 / elapsed),
        error_rate: Some(error_rate),
        enqueue_ms: Some(stats),
        p99_ms: Some(stats.p99),
        ..Default::default()
    })
}

/// Track I BM-BI1: keyed enqueue for `duration_secs` (unique keys, measures LWT insert path).
pub async fn run_keyed_enqueue(boson: &Boson, duration_secs: u64) -> Result<ReportMetrics> {
    let duration = Duration::from_secs(duration_secs);
    let start = Instant::now();
    let mut samples = Vec::new();
    let mut ops_ok = 0u64;
    let mut ops_err = 0u64;
    let mut n = 0u64;
    let mut resource_sampler = ResourceSampler::start();

    while start.elapsed() < duration {
        let key = format!("bi1-{n}");
        let op_start = Instant::now();
        match boson
            .enqueue("noop", system_actor(), empty_params(), Some(key))
            .await
        {
            Ok(_) => {
                ops_ok += 1;
                samples.push(op_start.elapsed().as_secs_f64() * 1000.0);
            }
            Err(_) => ops_err += 1,
        }
        n += 1;
        if n.is_multiple_of(100) {
            resource_sampler.sample();
            tokio::task::yield_now().await;
        }
    }

    resource_sampler.sample();
    let _profile = resource_sampler.finish();
    let elapsed = start.elapsed().as_secs_f64();
    let total = ops_ok + ops_err;
    let error_rate = if total == 0 {
        0.0
    } else {
        ops_err as f64 / total as f64
    };
    let stats = MetricStats::summarize(samples);

    Ok(ReportMetrics {
        achieved_ops_per_sec: Some(ops_ok as f64 / elapsed),
        error_rate: Some(error_rate),
        enqueue_ms: Some(stats),
        p99_ms: Some(stats.p99),
        ..Default::default()
    })
}
