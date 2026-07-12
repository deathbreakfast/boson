//! Shared session setup and scenario execution for bench runs.

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use boson_core::{ExecutionContextFactory, IdempotencyMode};
use boson_runtime::WorkerSettings;
use boson_testkit::{
    BootstrapSession, MatrixSpec, RunMode, ScenarioRunner, ScenarioSpec, StubExecutionContextFactory,
};

use crate::config::BenchRunConfig;
use crate::drain;
use crate::enqueue;
use crate::experiments::ExperimentPlan;
use crate::hardware::{self, captures_resource_profile};
use crate::http_bench;
use crate::load;
use crate::pass_eval;
use crate::report::{self, BenchReport, ReportDimensions, ReportMetrics};
use crate::resource_profile::ResourceSampler;
use crate::scale;
use crate::stats::MetricStats;
use crate::tasks;

fn dimensions_from_matrix(matrix: &MatrixSpec, hardware: &str, cfg: &BenchRunConfig) -> ReportDimensions {
    let storage_topology = cfg
        .storage_topology
        .clone()
        .or_else(|| {
            match matrix.backend_name() {
                "scylla" => Some("scylla-1".to_string()),
                "redis" => Some("redis-1".to_string()),
                "nats" => Some("nats-1".to_string()),
                _ => None,
            }
        });
    let bench_client_index = std::env::var("BOSON_BENCH_CLIENT_INDEX")
        .ok()
        .and_then(|v| v.parse().ok());
    let bench_client_count = std::env::var("BOSON_BENCH_CLIENT_COUNT")
        .ok()
        .and_then(|v| v.parse().ok());
    let fleet_size = std::env::var("BOSON_FLEET_SIZE")
        .ok()
        .and_then(|v| v.parse().ok());
    ReportDimensions {
        backend: matrix.backend_name().into(),
        topology: matrix.topology_name().into(),
        telemetry: matrix.telemetry_name().into(),
        hardware: hardware.to_string(),
        storage_topology,
        bench_client_index,
        bench_client_count,
        fleet_size,
        aggregate: None,
    }
}

fn session_for(matrix: MatrixSpec, cfg: &BenchRunConfig) -> BootstrapSession {
    let mut session = boson_testkit::BootstrapSession::new(matrix);
    if let Some(mode) = cfg.idempotency_mode {
        session = session.with_idempotency_mode(mode);
    }
    session
}

fn capacity_session(matrix: MatrixSpec, cfg: &BenchRunConfig) -> BootstrapSession {
    let mut session = session_for(matrix, cfg);
    if cfg.idempotency_mode.is_none() {
        session = session.with_idempotency_mode(IdempotencyMode::None);
    }
    session
}

fn worker_settings_for(matrix: &MatrixSpec, cfg: &BenchRunConfig) -> WorkerSettings {
    WorkerSettings {
        worker_id: matrix.worker_id(),
        lease_ttl_secs: matrix.lease_ttl_secs(),
        runtime_label: matrix.runtime_label().to_string(),
        worker_pools: cfg.worker_fleet.worker_pools.clone(),
        worker_poll_interval_ms: cfg.drain.poll_interval_ms,
        skip_run_persistence: std::env::var("BOSON_SKIP_RUN_ROWS")
            .ok()
            .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes")),
    }
}

/// Execute one experiment and build a report.
#[allow(clippy::cognitive_complexity, clippy::too_many_lines)] // Experiment dispatch keeps setup and reporting in one lifecycle.
pub async fn run_experiment(
    matrix: MatrixSpec,
    plan: ExperimentPlan,
    hardware: &str,
    warmup: u32,
    bench_cfg: BenchRunConfig,
) -> Result<BenchReport> {
    let hardware_detail = hardware::capture();
    let capture_resources = captures_resource_profile(hardware);
    let mut sampler = capture_resources.then(ResourceSampler::start);
    let dimensions = dimensions_from_matrix(&matrix, hardware, &bench_cfg);

    let (metrics, scenario_id, run_error) = if plan.id == "bm-b7" {
        let ops = plan.ops.unwrap_or(100);
        match http_bench::run_http_enqueue(ops).await {
            Ok(m) => (m, Some("http_enqueue".into()), None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id == "bm-bi1" {
        let mut session = session_for(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let boson = session.build_boson()?;
        match load::run_keyed_enqueue(&boson, 60).await {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id == "bm-bf2" {
        let mut session = session_for(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let boson = Arc::new(session.build_boson()?);
        match scale::run_task_fanout(boson, &bench_cfg).await {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id.starts_with("bm-be") {
        let mut session = capacity_session(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let (boson, _) = session.build_boson_manual()?;
        match enqueue::run_enqueue_capacity(Arc::new(boson), &bench_cfg).await {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id == "bm-bd1" {
        let mut session = capacity_session(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let backend = session.backend().ok_or_else(|| anyhow::anyhow!("no backend"))?;
        let registry = session.registry();
        let identity: Arc<dyn ExecutionContextFactory> = Arc::new(StubExecutionContextFactory);
        let (boson, _) = session.build_boson_manual()?;
        match drain::run_manual_drain(
            boson,
            backend,
            registry,
            identity,
            worker_settings_for(&matrix, &bench_cfg),
            &bench_cfg,
        )
        .await
        {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id == "bm-bd2" {
        let mut session = capacity_session(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let backend = session.backend().ok_or_else(|| anyhow::anyhow!("no backend"))?;
        let registry = session.registry();
        let identity: Arc<dyn ExecutionContextFactory> = Arc::new(StubExecutionContextFactory);
        match drain::run_background_drain(
            &session,
            backend,
            registry,
            identity,
            matrix.runtime_label(),
            &bench_cfg,
        )
        .await
        {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id.starts_with("bm-bl") {
        let mut session = session_for(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let boson = session.build_boson()?;
        match load::run_load(&boson, &plan.id).await {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else if plan.id.starts_with("bm-bm") || plan.id.starts_with("bm-bp") {
        let mut session = session_for(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;
        let boson = Arc::new(session.build_boson()?);
        match scale::run_scale(boson, &bench_cfg).await {
            Ok(m) => (m, None, None),
            Err(e) => (ReportMetrics::default(), None, Some(e.to_string())),
        }
    } else {
        let mut session = session_for(matrix.clone(), &bench_cfg);
        tasks::register_for_plan(session.registry_mut(), &plan, &bench_cfg);
        session.install().await?;

        if warmup > 0 && matches!(
            plan.id.as_str(),
            "bm-b0" | "bm-b1" | "bm-b2" | "bm-b3" | "bm-b5" | "bm-b6" | "bm-b7" | "bm-b8"
                | "bm-b11" | "bm-b12" | "bm-b13" | "bm-b14" | "bm-b17"
        ) {
            let warm = ScenarioSpec::enqueue_only("noop", warmup as usize);
            ScenarioRunner::new(&session)
                .run(&warm, RunMode::Benchmark)
                .await?;
        }

        let result = ScenarioRunner::new(&session)
            .run(&plan.scenario, RunMode::Benchmark)
            .await?;

        let mut enqueue_samples = Vec::new();
        let mut drain_samples = Vec::new();
        let mut admin_samples = Vec::new();
        for timing in &result.step_timings {
            match timing.op.as_str() {
                "enqueue" => enqueue_samples.extend(timing.samples_ms.iter().copied()),
                "drain" => drain_samples.extend(timing.samples_ms.iter().copied()),
                "admin_read" => admin_samples.extend(timing.samples_ms.iter().copied()),
                _ => {}
            }
        }

        let metrics = ReportMetrics {
            enqueue_ms: (!enqueue_samples.is_empty())
                .then(|| MetricStats::summarize(enqueue_samples)),
            drain_ms: (!drain_samples.is_empty())
                .then(|| MetricStats::summarize(drain_samples)),
            admin_read_ms: (!admin_samples.is_empty())
                .then(|| MetricStats::summarize(admin_samples)),
            ..Default::default()
        };
        (
            metrics,
            Some(result.scenario_id),
            result.error,
        )
    };

    if let Some(s) = sampler.as_mut() {
        s.sample();
    }
    let resource_profile = sampler.map(ResourceSampler::finish);

    let err_ref = run_error.as_deref();
    let (pass, notes) = pass_eval::evaluate(&plan.id, &metrics, err_ref);
    let status = if pass { "ok" } else { "fail" };

    let nats_pipeline = if dimensions.backend == "nats" {
        Some(report::NatsPipelineDimensions {
            enqueue_mode: std::env::var("BOSON_NATS_ENQUEUE_MODE").unwrap_or_else(|_| "dual".into()),
            sync_ack: std::env::var("BOSON_NATS_SYNC_ACK").unwrap_or_else(|_| "1".into()),
            max_inflight: std::env::var("BOSON_NATS_MAX_INFLIGHT").unwrap_or_else(|_| "256".into()),
        })
    } else {
        None
    };
    let diagnostics = std::env::var("BOSON_BENCH_NATS_BENCH_PEAK")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .map(|nats_bench_peak_ops| report::ReportDiagnostics {
            nats_bench_peak_ops: Some(nats_bench_peak_ops),
        });

    Ok(BenchReport {
        experiment_id: plan.id.clone(),
        dimensions,
        scenario_id,
        hardware_detail,
        metrics,
        bench_config: bench_cfg,
        resource_profile,
        pass_criteria: pass_eval::pass_criteria_for(&plan.id).into(),
        pass,
        status,
        notes,
        nats_pipeline,
        diagnostics,
        error: run_error,
    })
}

/// Run experiment and optionally write report file.
#[allow(clippy::large_futures)] // Experiment runner futures include backend clients.
pub async fn run_and_report(
    matrix: MatrixSpec,
    plan: ExperimentPlan,
    hardware: &str,
    warmup: u32,
    bench_cfg: BenchRunConfig,
    report_path: Option<&Path>,
) -> Result<BenchReport> {
    let report = run_experiment(matrix, plan, hardware, warmup, bench_cfg).await?;
    if let Some(path) = report_path {
        report::write_report(path, &report)?;
        println!("wrote {}", path.display());
    } else {
        let path = report::default_reports_dir().join(report.filename());
        report::write_report(&path, &report)?;
        println!("wrote {}", path.display());
    }
    Ok(report)
}
