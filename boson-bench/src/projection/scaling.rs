//! Storage-node scaling curve from BM-BM4 reports (e.g. Scylla multi-node).

use std::fmt::Write;
use std::path::Path;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/// One point on the scaling curve.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScalingPoint {
    /// Storage topology label.
    pub storage_topology: String,
    /// Number of storage nodes.
    pub node_count: u8,
    /// Peak BM-BM4 ops/s.
    pub peak_ops_per_sec: f64,
}

/// Scaling curve across storage topologies.
#[derive(Debug, Serialize, Deserialize)]
pub struct ScalingCurve {
    pub hardware: String,
    pub backend: String,
    pub points: Vec<ScalingPoint>,
}

#[derive(Debug, Deserialize)]
struct ReportFile {
    experiment_id: String,
    dimensions: ReportDims,
    metrics: Metrics,
}

#[derive(Debug, Deserialize)]
struct ReportDims {
    hardware: String,
    backend: String,
    storage_topology: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Metrics {
    achieved_ops_per_sec: Option<f64>,
}

/// Load scaling curve from BM-BM4 reports.
pub fn load_scaling_curve(reports_dir: &Path, hardware: &str, backend: &str) -> Result<ScalingCurve> {
    let mut points = Vec::new();
    if reports_dir.exists() {
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
            if report.experiment_id != "bm-bm4" {
                continue;
            }
            if report.dimensions.hardware != hardware || report.dimensions.backend != backend {
                continue;
            }
            if let (Some(topo), Some(ops)) = (
                report.dimensions.storage_topology,
                report.metrics.achieved_ops_per_sec,
            ) {
                let node_count = parse_node_count(&topo);
                points.push(ScalingPoint {
                    storage_topology: topo,
                    node_count,
                    peak_ops_per_sec: ops,
                });
            }
        }
    }
    points.sort_by_key(|p| p.node_count);
    Ok(ScalingCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        points,
    })
}

fn parse_node_count(topo: &str) -> u8 {
    topo.split('-')
        .next_back()
        .and_then(|s| s.strip_suffix('n'))
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| {
            if topo.contains("ha-3") {
                3
            } else if topo.contains("scale-4") {
                4
            } else if topo.contains("scale-5") {
                5
            } else {
                1
            }
        })
}

/// Render scaling curve as markdown.
pub fn render_scaling_markdown(curve: &ScalingCurve) -> String {
    let mut out = format!(
        "## Scaling curve ({}/{})\n\n| Topology | Nodes | Peak ops/s |\n|----------|-------|------------|\n",
        curve.hardware, curve.backend
    );
    for p in &curve.points {
        let _ = writeln!(
            out,
            "| {} | {} | {:.0} |",
            p.storage_topology, p.node_count, p.peak_ops_per_sec
        );
    }
    if curve.points.is_empty() {
        out.push_str("| *(pending Track T campaigns)* | | |\n");
    }
    out
}
