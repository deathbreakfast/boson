//! BM-BD2 broker fleet scaling curve — aggregate drain throughput vs fleet size N.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bd2_common::{self, classify_sublinear, BD2_EXPERIMENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bd2FleetPoint {
    pub fleet_size: u32,
    pub peak_drain_ops_per_sec: f64,
    pub pool_count: u32,
    pub worker_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fleet_efficiency: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Bd2FleetCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub per_broker_peak: Option<f64>,
    pub points: Vec<Bd2FleetPoint>,
    pub peak_drain_ops_per_sec: f64,
    pub peak_fleet_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub disclaimer: String,
}

pub fn bd2_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_bd2_fleet_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!("scaling-curve-bd2-fleet-{hardware}-{backend}.json"))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_fleet_markdown(&curve));
    Ok(())
}

#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_bd2_fleet_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Bd2FleetCurve> {
    let mut best: HashMap<u32, (f64, u32, u32, String)> = HashMap::new();
    let mut n1_peak: Option<f64> = None;

    if reports_dir.exists() {
        for entry in std::fs::read_dir(reports_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let text = std::fs::read_to_string(&path)?;
            let v: Value = match serde_json::from_str(&text) {
                Ok(x) => x,
                Err(_) => continue,
            };
            if v.get("experiment_id").and_then(|e| e.as_str()) != Some(BD2_EXPERIMENT) {
                continue;
            }
            let fname = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if !fname.contains("fleet-n") || fname.contains("-bc") || fname.contains("-aggregate-")
            {
                continue;
            }
            if v.pointer("/dimensions/hardware")
                .and_then(Value::as_str)
                != Some(hardware)
            {
                continue;
            }
            if v.pointer("/dimensions/backend").and_then(Value::as_str) != Some(backend) {
                continue;
            }
            let rate = bd2_common::drain_rate(&v);
            if rate <= 0.0 {
                continue;
            }
            let fleet_size = bd2_common::parse_fleet_size(&fname).or_else(|| {
                v.pointer("/dimensions/fleet_size")
                    .and_then(Value::as_u64)
                    .map(|n| n as u32)
            });
            let Some(fleet_size) = fleet_size else {
                continue;
            };
            let pool_count = bd2_common::pool_count(&v, &fname).unwrap_or(fleet_size);
            let worker_count = bd2_common::worker_count(&v, &fname).unwrap_or(1);
            if fleet_size == 1 {
                n1_peak = Some(n1_peak.map_or(rate, |p| p.max(rate)));
            }
            best
                .entry(fleet_size)
                .and_modify(|(best_rate, pc, wc, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *pc = pool_count;
                        *wc = worker_count;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, pool_count, worker_count, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BD2 fleet reports (bm-bd2-fleet-n*) for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let per_broker = n1_peak.or_else(|| best.get(&1).map(|(r, _, _, _)| *r));
    let mut points: Vec<Bd2FleetPoint> = best
        .into_iter()
        .map(|(fleet_size, (peak, pool_count, worker_count, file))| {
            let fleet_efficiency = per_broker.filter(|p| *p > 0.0).map(|p| {
                let ideal = p * f64::from(fleet_size);
                if ideal > 0.0 {
                    peak / ideal
                } else {
                    0.0
                }
            });
            Bd2FleetPoint {
                fleet_size,
                peak_drain_ops_per_sec: peak,
                pool_count,
                worker_count,
                fleet_efficiency,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.fleet_size);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_drain_ops_per_sec
                .partial_cmp(&b.peak_drain_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_drain_ops_per_sec = peak_point.peak_drain_ops_per_sec;
    let peak_fleet_size = peak_point.fleet_size;
    let scaling_verdict = per_broker.and_then(|n1| {
        points.last().map(|last| {
            let ideal = n1 * f64::from(last.fleet_size);
            let eff = if ideal > 0.0 {
                last.peak_drain_ops_per_sec / ideal
            } else {
                0.0
            };
            format!("fleet_{}", classify_sublinear(eff))
        })
    });

    Ok(Bd2FleetCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-bd2-fleet".into(),
        per_broker_peak: per_broker,
        points,
        peak_drain_ops_per_sec,
        peak_fleet_size,
        scaling_verdict,
        disclaimer: "fleet_efficiency = agg_drain / (N × n1_peak); pool-routed standalone NATS.".into(),
    })
}

pub fn render_fleet_markdown(curve: &Bd2FleetCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BD2 broker fleet scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak drain: **{:.0} ops/s** at N={}",
            curve.peak_drain_ops_per_sec, curve.peak_fleet_size
        ),
    ];
    if let Some(p) = curve.per_broker_peak {
        lines.push(format!("- per-broker peak (N=1): {p:.0} ops/s"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| N | drain ops/s | fleet_efficiency | K | W | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .fleet_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.fleet_size,
            p.peak_drain_ops_per_sec,
            eff,
            p.pool_count,
            p.worker_count,
            p.report_file
        ));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}
