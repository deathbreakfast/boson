//! BM-BD2 pool-count (shard) scaling curve — drain throughput vs K at fixed W.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bd2_common::{self, classify_sublinear, BD2_EXPERIMENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bd2ShardPoint {
    pub pool_count: u32,
    pub peak_drain_ops_per_sec: f64,
    pub worker_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_efficiency: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Bd2ShardCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub worker_count: Option<u32>,
    pub per_stream_peak: Option<f64>,
    pub points: Vec<Bd2ShardPoint>,
    pub peak_drain_ops_per_sec: f64,
    pub peak_pool_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub disclaimer: String,
}

pub fn bd2_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_bd2_shard_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!("scaling-curve-bd2-shards-{hardware}-{backend}.json"))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_shard_markdown(&curve));
    Ok(())
}

#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_bd2_shard_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Bd2ShardCurve> {
    let mut best: HashMap<u32, (f64, u32, String)> = HashMap::new();
    let mut k1_peak: Option<f64> = None;

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
            if !fname.starts_with("bm-bd2-k") {
                continue;
            }
            if fname.contains("fleet-n") {
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
            let Some(k) = bd2_common::pool_count(&v, &fname) else {
                continue;
            };
            let w = bd2_common::worker_count(&v, &fname).unwrap_or(1);
            if k == 1 {
                k1_peak = Some(k1_peak.map_or(rate, |p| p.max(rate)));
            }
            best
                .entry(k)
                .and_modify(|(best_rate, best_w, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *best_w = w;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, w, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BD2 shard reports (bm-bd2-k*) for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let per_stream = k1_peak.or_else(|| best.get(&1).map(|(r, _, _)| *r));
    let mut points: Vec<Bd2ShardPoint> = best
        .into_iter()
        .map(|(pool_count, (peak, worker_count, file))| {
            let shard_efficiency = per_stream.filter(|p| *p > 0.0).map(|p| {
                let ideal = p * f64::from(pool_count);
                if ideal > 0.0 {
                    peak / ideal
                } else {
                    0.0
                }
            });
            Bd2ShardPoint {
                pool_count,
                peak_drain_ops_per_sec: peak,
                worker_count,
                shard_efficiency,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.pool_count);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_drain_ops_per_sec
                .partial_cmp(&b.peak_drain_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_drain_ops_per_sec = peak_point.peak_drain_ops_per_sec;
    let peak_pool_count = peak_point.pool_count;
    let scaling_verdict = per_stream.and_then(|k1| {
        points.last().map(|last| {
            let ideal = k1 * f64::from(last.pool_count);
            let eff = if ideal > 0.0 {
                last.peak_drain_ops_per_sec / ideal
            } else {
                0.0
            };
            format!("shard_{}", classify_sublinear(eff))
        })
    });

    Ok(Bd2ShardCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-bd2-shard".into(),
        worker_count: points.first().map(|p| p.worker_count),
        per_stream_peak: per_stream,
        points,
        peak_drain_ops_per_sec,
        peak_pool_count,
        scaling_verdict,
        disclaimer: "shard_efficiency = drain_throughput / (K × k1_peak) on one broker.".into(),
    })
}

pub fn render_shard_markdown(curve: &Bd2ShardCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BD2 shard scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak drain: **{:.0} ops/s** at K={}",
            curve.peak_drain_ops_per_sec, curve.peak_pool_count
        ),
    ];
    if let Some(w) = curve.worker_count {
        lines.push(format!("- worker_count (W): {w}"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| K (pools) | drain ops/s | shard_efficiency | W | report |".into());
    lines.push("| --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .shard_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} |",
            p.pool_count, p.peak_drain_ops_per_sec, eff, p.worker_count, p.report_file
        ));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}
