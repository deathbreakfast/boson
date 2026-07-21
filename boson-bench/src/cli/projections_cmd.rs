//! Fleet projection CLI commands.

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::projection;

/// Print fleet projection toward 10M jobs/s.
pub fn run_project_fleet(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_fleet(hardware, backend, reports_dir, out)
}

/// Print storage-node scaling curve from BM-BM4 reports.
pub fn run_project_scaling_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_scaling_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BE4 publisher scaling curve from sweep reports.
pub fn run_be4_publisher_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_be4_publisher_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BE4 shard scaling curve from sweep reports.
pub fn run_be4_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_be4_shard_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BE4 fleet scaling curve from sweep reports.
pub fn run_be4_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_be4_fleet_curve(hardware, backend, reports_dir, out)
}

/// Aggregate multibench BM-BE4 per-client reports.
pub fn run_be4_aggregate(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    cell_prefix: Option<&str>,
) -> Result<()> {
    projection::aggregate_be4_reports(hardware, backend, reports_dir, cell_prefix).map(|paths| {
        for p in paths {
            println!("wrote {}", p.display());
        }
    })
}

/// Print BM-BE4 multi-bench scaling curve.
pub fn run_be4_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_be4_multibench_curve(hardware, backend, reports_dir, out)
}

pub fn run_bd2_worker_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_bd2_worker_curve(hardware, backend, reports_dir, out)
}

pub fn run_bd2_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_bd2_shard_curve(hardware, backend, reports_dir, out)
}

pub fn run_bd2_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_bd2_fleet_curve(hardware, backend, reports_dir, out)
}

pub fn run_bd2_aggregate(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    cell_prefix: Option<&str>,
) -> Result<()> {
    projection::aggregate_bd2_reports(hardware, backend, reports_dir, cell_prefix).map(|paths| {
        for p in paths {
            println!("wrote {}", p.display());
        }
    })
}

pub fn run_bd2_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    projection::project_bd2_multibench_curve(hardware, backend, reports_dir, out)
}

/// Handle projection CLI variants.
pub fn dispatch_projections(command: crate::cli::Command) -> Result<()> {
    use crate::cli::Command;
    match command {
        Command::ProjectFleet {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_project_fleet(&hardware, &backend, &reports_dir, out),
        Command::ProjectScalingCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_project_scaling_curve(&hardware, &backend, &reports_dir, out),
        Command::Be4PublisherCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_be4_publisher_curve(&hardware, &backend, &reports_dir, out),
        Command::Be4ShardCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_be4_shard_curve(&hardware, &backend, &reports_dir, out),
        Command::Be4FleetCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_be4_fleet_curve(&hardware, &backend, &reports_dir, out),
        Command::Be4Aggregate {
            hardware,
            backend,
            reports_dir,
            cell_prefix,
        } => run_be4_aggregate(&hardware, &backend, &reports_dir, cell_prefix.as_deref()),
        Command::Be4MultibenchCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_be4_multibench_curve(&hardware, &backend, &reports_dir, out),
        Command::Bd2WorkerCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_bd2_worker_curve(&hardware, &backend, &reports_dir, out),
        Command::Bd2ShardCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_bd2_shard_curve(&hardware, &backend, &reports_dir, out),
        Command::Bd2FleetCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_bd2_fleet_curve(&hardware, &backend, &reports_dir, out),
        Command::Bd2Aggregate {
            hardware,
            backend,
            reports_dir,
            cell_prefix,
        } => run_bd2_aggregate(&hardware, &backend, &reports_dir, cell_prefix.as_deref()),
        Command::Bd2MultibenchCurve {
            hardware,
            backend,
            reports_dir,
            out,
        } => run_bd2_multibench_curve(&hardware, &backend, &reports_dir, out),
        _ => unreachable!("dispatch_projections called with non-projection command"),
    }
}
