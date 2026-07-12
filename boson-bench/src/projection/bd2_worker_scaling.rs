//! BM-BD2 worker-count scaling curve — drain throughput vs W.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bd2_common::{self, BD2_EXPERIMENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPoint {
    pub worker_count: u32,
    pub peak_drain_ops_per_sec: f64,
    pub pool_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_worker: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vs_worker_1: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Bd2WorkerCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub points: Vec<WorkerPoint>,
    pub peak_drain_ops_per_sec: f64,
    pub peak_worker_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saturation_worker_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub disclaimer: String,
}

pub fn bd2_worker_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_bd2_worker_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!("scaling-curve-bd2-workers-{hardware}-{backend}.json"))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_worker_markdown(&curve));
    Ok(())
}

#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_bd2_worker_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Bd2WorkerCurve> {
    let mut best: HashMap<u32, (f64, u32, String)> = HashMap::new();

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
            if !fname.contains("-w") || fname.contains("fleet-n") || fname.contains("-bc") {
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
            let Some(w) = bd2_common::worker_count(&v, &fname) else {
                continue;
            };
            let k = bd2_common::pool_count(&v, &fname).unwrap_or(1);
            best
                .entry(w)
                .and_modify(|(best_rate, pc, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *pc = k;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, k, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BD2 worker reports (bm-bd2-w*) for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let w1 = best.get(&1).map(|(r, _, _)| *r);
    let mut points: Vec<WorkerPoint> = best
        .into_iter()
        .map(|(worker_count, (peak, pool_count, file))| {
            let ops_per_worker = peak / f64::from(worker_count.max(1));
            let vs_worker_1 = w1.filter(|b| *b > 0.0).map(|b| peak / b);
            WorkerPoint {
                worker_count,
                peak_drain_ops_per_sec: peak,
                pool_count,
                ops_per_worker: Some(ops_per_worker),
                vs_worker_1,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.worker_count);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_drain_ops_per_sec
                .partial_cmp(&b.peak_drain_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_drain_ops_per_sec = peak_point.peak_drain_ops_per_sec;
    let peak_worker_count = peak_point.worker_count;
    let saturation_pairs: Vec<(u32, f64)> = points
        .iter()
        .map(|p| (p.worker_count, p.peak_drain_ops_per_sec))
        .collect();
    let saturation_worker_count = bd2_common::detect_saturation_w(&saturation_pairs);
    let scaling_verdict = saturation_worker_count
        .map(|w| format!("worker_saturated_at_w={w}"))
        .or_else(|| Some("worker_scaling".into()));

    Ok(Bd2WorkerCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: BD2_EXPERIMENT.into(),
        points,
        peak_drain_ops_per_sec,
        peak_worker_count,
        saturation_worker_count,
        scaling_verdict,
        disclaimer: "saturation_worker_count: marginal gain <5% when doubling W.".into(),
    })
}

pub fn render_worker_markdown(curve: &Bd2WorkerCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BD2 worker scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak drain: **{:.0} ops/s** at W={}",
            curve.peak_drain_ops_per_sec, curve.peak_worker_count
        ),
    ];
    if let Some(w) = curve.saturation_worker_count {
        lines.push(format!("- saturation (est.): W≥{w}"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| W (workers) | drain ops/s | ops/worker | vs W=1 | K | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let vs = p
            .vs_worker_1
            .map_or_else(|| "—".into(), |v| format!("{v:.2}×"));
        let opw = p
            .ops_per_worker
            .map_or_else(|| "—".into(), |v| format!("{v:.0}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.worker_count,
            p.peak_drain_ops_per_sec,
            opw,
            vs,
            p.pool_count,
            p.report_file
        ));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}
