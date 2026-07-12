//! Matrix subset batch runs.

use std::path::PathBuf;

use anyhow::Result;

use crate::experiments;
use crate::matrix;
use crate::report;
use crate::runner;

use super::bench_config::bench_config_for_experiment;
use super::run_experiment::resolve_hardware;

/// Run every experiment in a named matrix subset.
#[allow(clippy::large_futures)] // Experiment runner futures include backend clients.
pub async fn run_matrix_subset(
    subset: String,
    backend: String,
    topology: String,
    telemetry: String,
    hardware: Option<String>,
    warmup: u32,
    reports_dir: PathBuf,
) -> Result<()> {
    let hardware = resolve_hardware(hardware);
    std::env::set_var("BOSON_BENCH_HARDWARE", &hardware);
    let matrix = matrix::matrix_from_cli(&backend, &topology, &telemetry)?;
    let ids = experiments::subset_experiments(&subset)?;
    std::fs::create_dir_all(&reports_dir)?;
    for id in ids {
        let plan = experiments::resolve_experiment(id, None)?;
        let path = reports_dir.join(report::report_filename(
            &plan.id,
            &matrix.report_slug(),
            &hardware,
        ));
        println!("running {} …", plan.id);
        let bench_cfg = bench_config_for_experiment(&plan.id);
        runner::run_and_report(
            matrix.clone(),
            plan,
            &hardware,
            warmup,
            bench_cfg,
            Some(&path),
        )
        .await?;
    }
    Ok(())
}

/// Handle the `Matrix` CLI variant.
#[allow(clippy::large_futures)] // Experiment runner futures include backend clients.
pub async fn dispatch_matrix(command: crate::cli::Command) -> Result<()> {
    let crate::cli::Command::Matrix {
        subset,
        backend,
        topology,
        telemetry,
        hardware,
        warmup,
        reports_dir,
    } = command
    else {
        unreachable!("dispatch_matrix called with non-matrix command");
    };
    run_matrix_subset(
        subset,
        backend,
        topology,
        telemetry,
        hardware,
        warmup,
        reports_dir,
    )
    .await
}
