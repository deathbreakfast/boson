//! BM-BD2 multi-bench (embed fleet) scaling curve — aggregate drain vs `bench_client_count`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::bd2_common::{self, classify_sublinear, BD2_EXPERIMENT};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Bd2MultibenchPoint {
    pub bench_client_count: u32,
    pub peak_drain_ops_per_sec: f64,
    pub fleet_size: u32,
    pub pool_count: u32,
    pub worker_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multibench_efficiency: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Bd2MultibenchCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub multibench_ladder: bool,
    pub fleet_size: u32,
    pub per_embed_peak: Option<f64>,
    pub points: Vec<Bd2MultibenchPoint>,
    pub peak_drain_ops_per_sec: f64,
    pub peak_bench_client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub disclaimer: String,
}

pub fn bd2_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_bd2_multibench_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!(
            "scaling-curve-bd2-multibench-{hardware}-{backend}.json"
        ))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_multibench_markdown(&curve));
    Ok(())
}

fn parse_bench_client_count(fname: &str, v: &Value) -> Option<u32> {
    if let Some(bc) = v
        .pointer("/dimensions/bench_client_count")
        .and_then(Value::as_u64)
    {
        return Some(bc as u32);
    }
    for bc in 1..=8 {
        if fname.contains(&format!("-bc{bc}-")) {
            return Some(bc);
        }
    }
    if fname.contains("-aggregate-") {
        return None;
    }
    Some(1)
}

#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_bd2_multibench_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Bd2MultibenchCurve> {
    let mut best: HashMap<u32, (f64, u32, u32, u32, String)> = HashMap::new();
    let mut bc1_peak: Option<f64> = None;
    let mut fleet_size = 4u32;

    if reports_dir.exists() {
        for entry in std::fs::read_dir(reports_dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let fname = path
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or("")
                .to_string();
            if !fname.contains("-aggregate-") && !fname.contains("-bc") {
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
            if v.pointer("/dimensions/hardware").and_then(Value::as_str) != Some(hardware) {
                continue;
            }
            if v.pointer("/dimensions/backend").and_then(Value::as_str) != Some(backend) {
                continue;
            }
            let is_aggregate = v
                .pointer("/dimensions/aggregate")
                .and_then(Value::as_bool)
                .unwrap_or(false)
                || fname.contains("-aggregate-");
            let bc = parse_bench_client_count(&fname, &v);
            let Some(bc) = bc else {
                continue;
            };
            if !is_aggregate && bc != 1 {
                continue;
            }
            let rate = if is_aggregate {
                v.pointer("/metrics/fleet_aggregate_ops_per_sec")
                    .or_else(|| v.pointer("/metrics/drain_ops_per_sec"))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
            } else {
                bd2_common::drain_rate(&v)
            };
            if rate <= 0.0 {
                continue;
            }
            fleet_size = v
                .pointer("/dimensions/fleet_size")
                .and_then(Value::as_u64)
                .unwrap_or(4) as u32;
            let pool_count = bd2_common::pool_count(&v, &fname).unwrap_or(fleet_size);
            let worker_count = bd2_common::worker_count(&v, &fname).unwrap_or(1);
            if bc == 1 {
                bc1_peak = Some(bc1_peak.map_or(rate, |p| p.max(rate)));
            }
            best.entry(bc)
                .and_modify(|(best_rate, fs, pc, wc, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *fs = fleet_size;
                        *pc = pool_count;
                        *wc = worker_count;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, fleet_size, pool_count, worker_count, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no multibench aggregate BM-BD2 reports for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let per_embed = bc1_peak.or_else(|| best.get(&1).map(|(r, _, _, _, _)| *r));
    let mut points: Vec<Bd2MultibenchPoint> = best
        .into_iter()
        .map(
            |(bench_client_count, (peak, fleet_size, pool_count, worker_count, file))| {
                let multibench_efficiency = per_embed.filter(|p| *p > 0.0).map(|p| {
                    let ideal = p * f64::from(bench_client_count);
                    if ideal > 0.0 {
                        peak / ideal
                    } else {
                        0.0
                    }
                });
                Bd2MultibenchPoint {
                    bench_client_count,
                    peak_drain_ops_per_sec: peak,
                    fleet_size,
                    pool_count,
                    worker_count,
                    multibench_efficiency,
                    report_file: file,
                }
            },
        )
        .collect();
    points.sort_by_key(|p| p.bench_client_count);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_drain_ops_per_sec
                .partial_cmp(&b.peak_drain_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_drain_ops_per_sec = peak_point.peak_drain_ops_per_sec;
    let peak_bench_client_count = peak_point.bench_client_count;
    let scaling_verdict = per_embed.and_then(|bc1| {
        points.last().map(|last| {
            let ideal = bc1 * f64::from(last.bench_client_count);
            let eff = if ideal > 0.0 {
                last.peak_drain_ops_per_sec / ideal
            } else {
                0.0
            };
            format!("embed_{}", classify_sublinear(eff))
        })
    });

    Ok(Bd2MultibenchCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-bd2-multibench".into(),
        multibench_ladder: true,
        fleet_size,
        per_embed_peak: per_embed,
        points,
        peak_drain_ops_per_sec,
        peak_bench_client_count,
        scaling_verdict,
        disclaimer:
            "multibench_efficiency = aggregate / (bc × bc1_peak); each bench host = one embed."
                .into(),
    })
}

pub fn render_multibench_markdown(curve: &Bd2MultibenchCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BD2 multi-bench scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak aggregate drain: **{:.0} ops/s** at bc={}",
            curve.peak_drain_ops_per_sec, curve.peak_bench_client_count
        ),
        format!("- fleet_size (brokers): {}", curve.fleet_size),
    ];
    if let Some(p) = curve.per_embed_peak {
        lines.push(format!("- per-embed peak (bc=1): {p:.0} ops/s"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| bc | drain ops/s | multibench_efficiency | K | W | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .multibench_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.bench_client_count,
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
