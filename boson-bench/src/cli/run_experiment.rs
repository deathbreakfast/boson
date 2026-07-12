//! Single experiment run from CLI flags.

use std::path::PathBuf;

use anyhow::Result;
use boson_core::IdempotencyMode;
use clap::ValueEnum;

use crate::config::PoolLayout;
use crate::experiments;
use crate::matrix;
use crate::report;
use crate::runner;

use super::bench_config::{resolve_bench_config, BenchConfigOverrides};

/// Resolve hardware profile from CLI flag or `BOSON_BENCH_HARDWARE`.
pub fn resolve_hardware(cli: Option<String>) -> String {
    cli.or_else(|| std::env::var("BOSON_BENCH_HARDWARE").ok())
        .unwrap_or_else(|| "aws-c6i-large".into())
}

fn default_report_path(experiment: &str, matrix_slug: &str, hardware: &str) -> PathBuf {
    report::default_reports_dir().join(report::report_filename(experiment, matrix_slug, hardware))
}

/// CLI pool layout selector.
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum PoolLayoutArg {
    Shared,
    Distinct,
}

impl From<PoolLayoutArg> for PoolLayout {
    fn from(v: PoolLayoutArg) -> Self {
        match v {
            PoolLayoutArg::Shared => Self::Shared,
            PoolLayoutArg::Distinct => Self::DistinctPerSlot,
        }
    }
}

/// CLI inputs for a single experiment run.
pub struct RunExperimentArgs {
    /// Experiment id (e.g. `bm-b0`).
    pub experiment: String,
    /// Matrix backend slug.
    pub backend: String,
    /// Matrix topology slug.
    pub topology: String,
    /// Telemetry adapter slug.
    pub telemetry: String,
    /// Optional hardware profile override.
    pub hardware: Option<String>,
    /// Optional op count override.
    pub ops: Option<u32>,
    /// Warmup enqueue count before timed run.
    pub warmup: u32,
    /// Optional report output path.
    pub report: Option<PathBuf>,
    /// Bench knob overrides.
    pub bench: BenchConfigOverrides,
}

/// Run one experiment and write an optional JSON report.
#[allow(clippy::large_futures)] // Experiment runner futures include backend clients.
pub async fn run_single_experiment(args: RunExperimentArgs) -> Result<()> {
    let hardware = resolve_hardware(args.hardware);
    std::env::set_var("BOSON_BENCH_HARDWARE", &hardware);
    let matrix = matrix::matrix_from_cli(&args.backend, &args.topology, &args.telemetry)?;
    let plan = experiments::resolve_experiment(&args.experiment, args.ops)?;
    let bench_cfg = resolve_bench_config(&plan.id, args.bench);
    let report_path = args.report.or_else(|| {
        Some(default_report_path(
            &plan.id,
            &matrix.report_slug(),
            &hardware,
        ))
    });
    runner::run_and_report(
        matrix,
        plan,
        &hardware,
        args.warmup,
        bench_cfg,
        report_path.as_deref(),
    )
    .await?;
    Ok(())
}

/// Handle `Run`, `RunLoad`, and `RunScale` CLI variants.
#[allow(clippy::large_futures)] // Experiment runner futures include backend clients.
pub async fn dispatch_run(command: crate::cli::Command) -> Result<()> {
    use crate::cli::Command;
    match command {
        Command::Run {
            experiment,
            backend,
            topology,
            telemetry,
            hardware,
            ops,
            warmup,
            report,
            idempotency_mode,
            client_count,
            pool_count,
            pool_layout,
            prefill_count,
            worker_count,
            worker_poll_ms,
            task_fanout_count,
            storage_topology,
        } => {
            run_single_experiment(RunExperimentArgs {
                experiment,
                backend,
                topology,
                telemetry,
                hardware,
                ops,
                warmup,
                report,
                bench: BenchConfigOverrides {
                    idempotency_mode: idempotency_mode.and_then(|s| IdempotencyMode::parse(&s)),
                    client_count,
                    pool_count,
                    pool_layout: pool_layout.map(Into::into),
                    prefill_count,
                    worker_count,
                    worker_poll_ms,
                    task_fanout_count,
                    storage_topology,
                },
            })
            .await
        }
        Command::RunLoad {
            experiment,
            backend,
            topology,
            telemetry,
            hardware,
            report,
            ..
        }
        | Command::RunScale {
            experiment,
            backend,
            topology,
            telemetry,
            hardware,
            report,
            ..
        } => {
            run_single_experiment(RunExperimentArgs {
                experiment,
                backend,
                topology,
                telemetry,
                hardware,
                ops: None,
                warmup: 0,
                report,
                bench: BenchConfigOverrides::default(),
            })
            .await
        }
        _ => unreachable!("dispatch_run called with non-run command"),
    }
}
