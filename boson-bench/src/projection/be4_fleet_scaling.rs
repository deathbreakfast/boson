//! BM-BE4 broker fleet scaling curve — aggregate throughput vs fleet size N.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TARGETS: &[u64] = &[330_000, 1_000_000, 10_000_000];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FleetPoint {
    pub fleet_size: u32,
    pub peak_ops_per_sec: f64,
    pub pool_count: u32,
    pub client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fleet_efficiency: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Be4FleetCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub per_broker_peak: Option<f64>,
    pub points: Vec<FleetPoint>,
    pub peak_aggregate_ops_per_sec: f64,
    pub peak_fleet_size: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub brokers_for_target: HashMap<String, u64>,
    pub disclaimer: String,
}

pub fn be4_fleet_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_be4_fleet_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!("scaling-curve-be4-fleet-{hardware}-{backend}.json"))
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
pub fn load_be4_fleet_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Be4FleetCurve> {
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
            if v.get("experiment_id").and_then(|e| e.as_str()) != Some("bm-be4") {
                continue;
            }
            let fname = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if !fname.contains("fleet-n") {
                continue;
            }
            if fname.contains("-aggregate-") || fname.contains("-bc") {
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
            let rate = v
                .pointer("/metrics/achieved_ops_per_sec")
                .and_then(Value::as_f64)
                .unwrap_or(0.0);
            if rate <= 0.0 {
                continue;
            }
            let fleet_size = parse_fleet_size(&fname).or_else(|| {
                v.pointer("/metrics/pool_count")
                    .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
                    .and_then(Value::as_u64)
                    .map(|n| n as u32)
            });
            let Some(fleet_size) = fleet_size else {
                continue;
            };
            let client_count = v
                .pointer("/metrics/client_count")
                .or_else(|| v.pointer("/bench_config/publisher/client_count"))
                .and_then(Value::as_u64)
                .unwrap_or(256) as u32;
            let pool_count = v
                .pointer("/metrics/pool_count")
                .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
                .and_then(Value::as_u64)
                .unwrap_or_else(|| u64::from(fleet_size)) as u32;

            if fleet_size == 1 {
                n1_peak = Some(n1_peak.map_or(rate, |p| p.max(rate)));
            }

            best
                .entry(fleet_size)
                .and_modify(|(best_rate, pc, cc, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *pc = pool_count;
                        *cc = client_count;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, pool_count, client_count, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BE4 fleet reports (bm-be4-fleet-n*) for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let per_broker = n1_peak.or_else(|| best.get(&1).map(|(r, _, _, _)| *r));
    let mut points: Vec<FleetPoint> = best
        .into_iter()
        .map(|(fleet_size, (peak, pool_count, client_count, file))| {
            let fleet_efficiency = per_broker.filter(|p| *p > 0.0).map(|p| {
                let ideal = p * f64::from(fleet_size);
                if ideal > 0.0 {
                    peak / ideal
                } else {
                    0.0
                }
            });
            FleetPoint {
                fleet_size,
                peak_ops_per_sec: peak,
                pool_count,
                client_count,
                fleet_efficiency,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.fleet_size);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_ops_per_sec
                .partial_cmp(&b.peak_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_aggregate = peak_point.peak_ops_per_sec;
    let peak_fleet_size = peak_point.fleet_size;
    let per = per_broker.unwrap_or(0.0);
    let mut brokers_for_target = HashMap::new();
    for &target in TARGETS {
        let brokers = if per > 0.0 {
            (target as f64 / per).ceil() as u64
        } else {
            0
        };
        brokers_for_target.insert(target.to_string(), brokers.max(1));
    }

    let scaling_verdict = classify_fleet_scaling(&points, per_broker);

    Ok(Be4FleetCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-be4-fleet".into(),
        per_broker_peak: per_broker,
        points,
        peak_aggregate_ops_per_sec: peak_aggregate,
        peak_fleet_size,
        scaling_verdict: Some(scaling_verdict),
        brokers_for_target,
        disclaimer: "fleet_efficiency = agg_throughput / (N × n1_peak); standalone NATS per pool.".into(),
    })
}

fn parse_fleet_size(fname: &str) -> Option<u32> {
    let rest = fname.strip_prefix("bm-be4-fleet-n")?;
    rest.split('-').next()?.parse().ok()
}

fn classify_fleet_scaling(points: &[FleetPoint], n1: Option<f64>) -> String {
    let Some(n1) = n1.filter(|p| *p > 0.0) else {
        return "missing_n1_baseline".into();
    };
    if points.len() < 2 {
        return "single_point".into();
    }
    let last = points.last().expect("len >= 2");
    let ideal = n1 * f64::from(last.fleet_size);
    if ideal <= 0.0 {
        return "unknown".into();
    }
    let eff = last.peak_ops_per_sec / ideal;
    if eff >= 0.7 {
        "linear_multi_broker".into()
    } else if eff >= 0.4 {
        "sublinear".into()
    } else {
        "fleet_saturated".into()
    }
}

pub fn render_fleet_markdown(curve: &Be4FleetCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BE4 broker fleet scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak aggregate: **{:.0} ops/s** at N={}",
            curve.peak_aggregate_ops_per_sec, curve.peak_fleet_size
        ),
    ];
    if let Some(p) = curve.per_broker_peak {
        lines.push(format!("- per-broker peak (N=1): {p:.0} ops/s"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- scaling verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| N (brokers) | agg ops/s | fleet_efficiency | K | C | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .fleet_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.fleet_size, p.peak_ops_per_sec, eff, p.pool_count, p.client_count, p.report_file
        ));
    }
    lines.push(String::new());
    lines.push("**brokers_for_target** (per-broker N=1 peak as ceiling):".into());
    for (target, brokers) in &curve.brokers_for_target {
        lines.push(format!("- {target} ops/s → {brokers} standalone NATS brokers"));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn write_fleet_report(dir: &Path, name: &str, n: u32, ops: f64) {
        let body = format!(
            r#"{{
            "experiment_id": "bm-be4",
            "dimensions": {{"hardware": "aws-c6i-large", "backend": "nats"}},
            "metrics": {{"achieved_ops_per_sec": {ops}, "client_count": 256, "pool_count": {n}}},
            "bench_config": {{"publisher": {{"client_count": 256, "pool_count": {n}, "pool_layout": "DistinctPerSlot"}}}}
        }}"#
        );
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        write!(f, "{body}").unwrap();
    }

    #[test]
    fn fleet_efficiency_linear() {
        let dir = TempDir::new().unwrap();
        write_fleet_report(dir.path(), "bm-be4-fleet-n1-k1-c256-a.json", 1, 30_000.0);
        write_fleet_report(dir.path(), "bm-be4-fleet-n4-k4-c256-a.json", 4, 110_000.0);
        let curve = load_be4_fleet_curve(dir.path(), "aws-c6i-large", "nats").unwrap();
        let n4 = curve.points.iter().find(|p| p.fleet_size == 4).unwrap();
        assert!(n4.fleet_efficiency.unwrap() > 0.9);
    }
}
