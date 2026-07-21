//! BM-BE4 multi-bench (embed fleet) scaling curve — aggregate vs `bench_client_count`.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TARGETS: &[u64] = &[330_000, 1_000_000, 10_000_000];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultibenchPoint {
    pub bench_client_count: u32,
    pub peak_ops_per_sec: f64,
    pub fleet_size: u32,
    pub pool_count: u32,
    pub client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub multibench_efficiency: Option<f64>,
    pub report_file: String,
}

#[derive(Debug, Serialize)]
pub struct Be4MultibenchCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub multibench_ladder: bool,
    pub fleet_size: u32,
    pub per_embed_peak: Option<f64>,
    pub points: Vec<MultibenchPoint>,
    pub peak_aggregate_ops_per_sec: f64,
    pub peak_bench_client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub embeds_for_target: HashMap<String, u64>,
    pub disclaimer: String,
}

pub fn be4_multibench_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_be4_multibench_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!(
            "scaling-curve-be4-multibench-{hardware}-{backend}.json"
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

#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_be4_multibench_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Be4MultibenchCurve> {
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
            if v.get("experiment_id").and_then(|e| e.as_str()) != Some("bm-be4") {
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
                    .or_else(|| v.pointer("/metrics/achieved_ops_per_sec"))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
            } else {
                v.pointer("/metrics/achieved_ops_per_sec")
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
            };
            if rate <= 0.0 {
                continue;
            }
            fleet_size = v
                .pointer("/dimensions/fleet_size")
                .and_then(Value::as_u64)
                .unwrap_or(4) as u32;
            let pool_count = v
                .pointer("/metrics/pool_count")
                .and_then(Value::as_u64)
                .unwrap_or_else(|| u64::from(fleet_size)) as u32;
            let client_count = v
                .pointer("/metrics/client_count")
                .and_then(Value::as_u64)
                .unwrap_or(256) as u32;
            if bc == 1 {
                bc1_peak = Some(bc1_peak.map_or(rate, |p| p.max(rate)));
            }
            best.entry(bc)
                .and_modify(|(best_rate, fs, pc, cc, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *fs = fleet_size;
                        *pc = pool_count;
                        *cc = client_count;
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert((rate, fleet_size, pool_count, client_count, fname));
        }
    }

    if best.is_empty() {
        bail!(
            "no multibench aggregate BM-BE4 reports for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let per_embed = bc1_peak.or_else(|| best.get(&1).map(|(r, _, _, _, _)| *r));
    let mut points: Vec<MultibenchPoint> = best
        .into_iter()
        .map(
            |(bench_client_count, (peak, fleet_size, pool_count, client_count, file))| {
                let multibench_efficiency = per_embed.filter(|p| *p > 0.0).map(|p| {
                    let ideal = p * f64::from(bench_client_count);
                    if ideal > 0.0 {
                        peak / ideal
                    } else {
                        0.0
                    }
                });
                MultibenchPoint {
                    bench_client_count,
                    peak_ops_per_sec: peak,
                    fleet_size,
                    pool_count,
                    client_count,
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
            a.peak_ops_per_sec
                .partial_cmp(&b.peak_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty");
    let peak_aggregate = peak_point.peak_ops_per_sec;
    let peak_bench_client_count = peak_point.bench_client_count;
    let per = per_embed.unwrap_or(0.0);
    let mut embeds_for_target = HashMap::new();
    for &target in TARGETS {
        let rate_per_embed = if per > 0.0 { per } else { peak_aggregate };
        let embeds = if rate_per_embed > 0.0 {
            (target as f64 / rate_per_embed).ceil() as u64
        } else {
            0
        };
        embeds_for_target.insert(target.to_string(), embeds.max(1));
    }

    Ok(Be4MultibenchCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-be4-multibench".into(),
        multibench_ladder: true,
        fleet_size,
        per_embed_peak: per_embed,
        points,
        peak_aggregate_ops_per_sec: peak_aggregate,
        peak_bench_client_count,
        scaling_verdict: Some(classify_multibench(
            peak_aggregate,
            peak_bench_client_count,
            per_embed,
        )),
        embeds_for_target,
        disclaimer:
            "multibench_efficiency = aggregate / (bc × bc1_peak); each bench host = one embed."
                .into(),
    })
}

fn parse_bench_client_count(fname: &str, v: &Value) -> Option<u32> {
    if let Some(bc) = v
        .pointer("/dimensions/bench_client_count")
        .and_then(Value::as_u64)
    {
        return Some(bc as u32);
    }
    if let Some(rest) = fname.split("-bc").nth(1) {
        if let Ok(bc) = rest.split('-').next()?.parse::<u32>() {
            return Some(bc);
        }
    }
    None
}

fn classify_multibench(peak_agg: f64, peak_bc: u32, bc1: Option<f64>) -> String {
    let Some(bc1) = bc1.filter(|p| *p > 0.0) else {
        return "missing_bc1_baseline".into();
    };
    let ideal = bc1 * f64::from(peak_bc);
    if ideal <= 0.0 {
        return "unknown".into();
    }
    let eff = peak_agg / ideal;
    if eff >= 0.7 {
        "linear_multi_embed".into()
    } else if eff >= 0.45 {
        "sublinear".into()
    } else {
        "embed_saturated".into()
    }
}

pub fn render_multibench_markdown(curve: &Be4MultibenchCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BE4 multi-bench scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!("- fleet_size (brokers): {}", curve.fleet_size),
        format!(
            "- peak aggregate: **{:.0} ops/s** at bc={}",
            curve.peak_aggregate_ops_per_sec, curve.peak_bench_client_count
        ),
    ];
    if let Some(p) = curve.per_embed_peak {
        lines.push(format!("- per-embed peak (bc=1): {p:.0} ops/s"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- scaling verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| bc (embeds) | agg ops/s | multibench_efficiency | K | C | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .multibench_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.bench_client_count,
            p.peak_ops_per_sec,
            eff,
            p.pool_count,
            p.client_count,
            p.report_file
        ));
    }
    lines.push(String::new());
    lines.push("**embeds_for_target** (bc=1 peak as per-embed ceiling):".into());
    for (target, embeds) in &curve.embeds_for_target {
        lines.push(format!("- {target} ops/s → {embeds} embed hosts"));
    }
    lines.push(String::new());
    lines.push(curve.disclaimer.clone());
    lines.join("\n")
}
