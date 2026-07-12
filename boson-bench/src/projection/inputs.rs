//! Load projection inputs from report JSON files.

use std::path::Path;

use anyhow::Result;
use serde::Deserialize;

use super::model::{FleetProjection, FleetProjectionInputs};

#[derive(Debug, Deserialize)]
struct ReportFile {
    experiment_id: String,
    dimensions: ReportDims,
    metrics: ReportMetricsFile,
}

#[derive(Debug, Deserialize)]
struct ReportDims {
    hardware: String,
    backend: String,
}

#[derive(Debug, Deserialize, Default)]
struct ReportMetricsFile {
    achieved_ops_per_sec: Option<f64>,
}

/// Load projection inputs from matching reports in a directory.
pub fn load_from_dir(reports_dir: &Path, hardware: &str, backend: &str) -> Result<FleetProjectionInputs> {
    let mut inputs = FleetProjectionInputs {
        hardware: hardware.into(),
        backend: backend.into(),
        ..FleetProjectionInputs::default()
    };

    if !reports_dir.exists() {
        return Ok(inputs);
    }

    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let text = std::fs::read_to_string(&path)?;
        let report: ReportFile = match serde_json::from_str(&text) {
            Ok(r) => r,
            Err(_) => continue,
        };
        if report.dimensions.hardware != hardware || report.dimensions.backend != backend {
            continue;
        }
        let achieved = report.metrics.achieved_ops_per_sec;
        match report.experiment_id.as_str() {
            "bm-bl3" | "bm-bl4" if achieved.is_some() => {
                inputs.per_partition_ceiling = achieved;
            }
            "bm-bm2" => inputs.aggregate_ops_per_sec = achieved,
            "bm-bm4" => inputs.cluster_peak_ops_per_sec = achieved,
            _ => {}
        }
    }

    Ok(inputs)
}

/// Write projection JSON to disk.
pub fn write_projection(path: &Path, projection: &FleetProjection) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(projection)?)?;
    Ok(())
}
