//! Fleet projection toward 10M jobs/s from collected report JSONs.

mod bd2_aggregate;
mod bd2_common;
mod bd2_fleet_scaling;
mod bd2_multibench_scaling;
mod bd2_shard_scaling;
mod bd2_worker_scaling;
mod be4_aggregate;
mod be4_fleet_scaling;
mod be4_multibench_scaling;
mod be4_scaling;
mod be4_shard_scaling;
mod inputs;
mod model;
mod render;
mod scaling;

use std::path::{Path, PathBuf};

use anyhow::Result;

/// Target aggregate job schedules/s for fleet projection.
pub const TARGET_OPS_PER_SEC: f64 = 10_000_000.0;

/// Build and print a fleet projection from report JSONs in `reports_dir`.
pub fn project_fleet(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let mut inputs = inputs::load_from_dir(reports_dir, hardware, backend)?;
    inputs.hourly_usd = hardware_hourly_usd(hardware);
    let projection = model::compute(&inputs);
    let out_path =
        out.unwrap_or_else(|| reports_dir.join(format!("projection-{hardware}-{backend}.json")));
    inputs::write_projection(&out_path, &projection)?;
    println!("wrote {}", out_path.display());
    println!("{}", render::render_markdown(&projection));
    Ok(())
}

/// Print storage-node scaling curve from peak BM-BM4 reports.
pub fn project_scaling_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = scaling::load_scaling_curve(reports_dir, hardware, backend)?;
    let out_path =
        out.unwrap_or_else(|| reports_dir.join(format!("scaling-curve-{hardware}-{backend}.json")));
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", scaling::render_scaling_markdown(&curve));
    Ok(())
}

/// Print BM-BE4 publisher-count scaling curve (`JetStream` saturation).
pub fn project_be4_publisher_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    be4_scaling::be4_publisher_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BE4 pool-count (shard) scaling curve at fixed C.
pub fn project_be4_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    be4_shard_scaling::be4_shard_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BE4 broker fleet scaling curve (standalone NATS per pool).
pub fn project_be4_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    be4_fleet_scaling::be4_fleet_curve(hardware, backend, reports_dir, out)
}

/// Aggregate per-client multibench BM-BE4 reports.
pub fn aggregate_be4_reports(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    cell_prefix: Option<&str>,
) -> Result<Vec<PathBuf>> {
    be4_aggregate::aggregate_be4(
        reports_dir,
        None,
        Some(hardware),
        Some(backend),
        cell_prefix,
    )
}

/// Print BM-BE4 multi-bench scaling curve (embed fleet aggregate).
pub fn project_be4_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    be4_multibench_scaling::be4_multibench_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BD2 worker scaling curve from sweep reports.
pub fn project_bd2_worker_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    bd2_worker_scaling::bd2_worker_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BD2 shard scaling curve from sweep reports.
pub fn project_bd2_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    bd2_shard_scaling::bd2_shard_curve(hardware, backend, reports_dir, out)
}

/// Print BM-BD2 broker fleet scaling curve.
pub fn project_bd2_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    bd2_fleet_scaling::bd2_fleet_curve(hardware, backend, reports_dir, out)
}

/// Aggregate per-client multibench BM-BD2 reports.
pub fn aggregate_bd2_reports(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    cell_prefix: Option<&str>,
) -> Result<Vec<PathBuf>> {
    bd2_aggregate::aggregate_bd2(
        reports_dir,
        None,
        Some(hardware),
        Some(backend),
        cell_prefix,
    )
}

/// Print BM-BD2 multi-bench scaling curve.
pub fn project_bd2_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    bd2_multibench_scaling::bd2_multibench_curve(hardware, backend, reports_dir, out)
}

fn hardware_hourly_usd(hardware: &str) -> f64 {
    match hardware {
        "aws-t3-small" => 0.0208,
        "aws-t3-medium" => 0.0416,
        "aws-t4g-medium" => 0.0336,
        "aws-c7i-4xlarge" => 0.816,
        "aws-i4i-xlarge" => 0.312,
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::model::{compute, FleetProjectionInputs};

    #[test]
    fn projection_partitions_for_10m_target() {
        let inputs = FleetProjectionInputs {
            hardware: "aws-c6i-large".into(),
            backend: "mem".into(),
            per_partition_ceiling: Some(100_000.0),
            hourly_usd: 0.0416,
            ..FleetProjectionInputs::default()
        };
        let p = compute(&inputs);
        assert_eq!(p.partitions_for_10e6, Some(100));
    }
}
