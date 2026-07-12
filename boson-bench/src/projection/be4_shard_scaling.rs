//! BM-BE4 pool-count (shard) scaling curve — aggregate throughput vs K at fixed C.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TARGETS: &[u64] = &[330_000, 1_000_000, 10_000_000];

/// Throughput at one pool-count (shard) level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardPoint {
    pub pool_count: u32,
    pub peak_ops_per_sec: f64,
    pub client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_layout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enqueue_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_efficiency: Option<f64>,
    pub report_file: String,
}

/// Aggregated BE4 shard sweep for one hardware/backend slice.
#[derive(Debug, Serialize)]
pub struct Be4ShardCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    pub client_count: Option<u32>,
    pub per_stream_peak: Option<f64>,
    pub points: Vec<ShardPoint>,
    pub peak_aggregate_ops_per_sec: f64,
    pub peak_pool_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_verdict: Option<String>,
    pub streams_for_target: HashMap<String, u64>,
    pub disclaimer: String,
}

/// Build shard curve JSON and optionally write to `out`.
pub fn be4_shard_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_be4_shard_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!("scaling-curve-be4-shards-{hardware}-{backend}.json"))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_shard_markdown(&curve));
    Ok(())
}

/// Load peak BM-BE4 achieved rate per pool count (K).
#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_be4_shard_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Be4ShardCurve> {
    let mut best: HashMap<u32, (f64, u32, String, Option<String>, String)> = HashMap::new();
    let mut per_stream_peak: Option<f64> = None;

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
            if !fname.starts_with("bm-be4-k") {
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
            let pool_count = v
                .pointer("/metrics/pool_count")
                .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            if pool_count == 0 {
                continue;
            }
            let client_count = v
                .pointer("/metrics/client_count")
                .or_else(|| v.pointer("/bench_config/publisher/client_count"))
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            let layout = v
                .pointer("/bench_config/publisher/pool_layout")
                .and_then(Value::as_str)
                .map(str::to_string);
            let enqueue_mode = v
                .pointer("/nats_pipeline/enqueue_mode")
                .and_then(Value::as_str)
                .map(str::to_string);

            if pool_count == 1 {
                per_stream_peak = Some(per_stream_peak.map_or(rate, |prev| prev.max(rate)));
            }

            best
                .entry(pool_count)
                .and_modify(|(best_rate, best_c, best_layout, best_mode, best_file)| {
                    if rate > *best_rate {
                        *best_rate = rate;
                        *best_c = client_count;
                        *best_layout = layout.clone().unwrap_or_default();
                        best_mode.clone_from(&enqueue_mode);
                        best_file.clone_from(&fname);
                    }
                })
                .or_insert_with(|| (
                    rate,
                    client_count,
                    layout.clone().unwrap_or_default(),
                    enqueue_mode,
                    fname,
                ));
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BE4 shard reports (bm-be4-k*) for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let k1_peak = per_stream_peak.or_else(|| best.get(&1).map(|(r, _, _, _, _)| *r));
    let mut points: Vec<ShardPoint> = best
        .into_iter()
        .map(|(pool_count, (peak, client_count, layout, enqueue_mode, file))| {
            let shard_efficiency = k1_peak.filter(|p| *p > 0.0).map(|p| {
                let ideal = p * f64::from(pool_count);
                if ideal > 0.0 {
                    peak / ideal
                } else {
                    0.0
                }
            });
            ShardPoint {
                pool_count,
                peak_ops_per_sec: peak,
                client_count,
                pool_layout: Some(layout),
                enqueue_mode,
                shard_efficiency,
                report_file: file,
            }
        })
        .collect();
    points.sort_by_key(|p| p.pool_count);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_ops_per_sec
                .partial_cmp(&b.peak_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty points");
    let peak_aggregate = peak_point.peak_ops_per_sec;
    let peak_pool_count = peak_point.pool_count;
    let common_client = points.first().map(|p| p.client_count);
    let scaling_verdict = classify_shard_scaling(&points, k1_peak);

    let per_stream = k1_peak.unwrap_or(0.0);
    let mut streams_for_target = HashMap::new();
    for &target in TARGETS {
        let streams = if per_stream > 0.0 {
            (target as f64 / per_stream).ceil() as u64
        } else {
            0
        };
        streams_for_target.insert(target.to_string(), streams.max(1));
    }

    Ok(Be4ShardCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-be4-shard".into(),
        client_count: common_client,
        per_stream_peak: k1_peak,
        points,
        peak_aggregate_ops_per_sec: peak_aggregate,
        peak_pool_count,
        scaling_verdict: Some(scaling_verdict),
        streams_for_target,
        disclaimer: "shard_efficiency = agg_throughput / (K × k1_peak); 1.0 = perfect linear multi-stream scaling on one broker.".into(),
    })
}

fn classify_shard_scaling(points: &[ShardPoint], k1_peak: Option<f64>) -> String {
    let Some(k1) = k1_peak.filter(|p| *p > 0.0) else {
        return "missing_k1_baseline".into();
    };
    if points.len() < 2 {
        return "single_point".into();
    }
    let last = points.last().expect("len >= 2");
    let ideal = k1 * f64::from(last.pool_count);
    if ideal <= 0.0 {
        return "unknown".into();
    }
    let eff = last.peak_ops_per_sec / ideal;
    if eff >= 0.85 {
        "linear_multi_stream".into()
    } else if eff >= 0.6 {
        "sublinear_broker_contention".into()
    } else {
        "broker_saturated".into()
    }
}

pub fn render_shard_markdown(curve: &Be4ShardCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BE4 shard scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak aggregate: **{:.0} ops/s** at K={}",
            curve.peak_aggregate_ops_per_sec, curve.peak_pool_count
        ),
    ];
    if let Some(c) = curve.client_count {
        lines.push(format!("- fixed client_count (C): {c}"));
    }
    if let Some(p) = curve.per_stream_peak {
        lines.push(format!("- per-stream peak (K=1): {p:.0} ops/s"));
    }
    if let Some(v) = &curve.scaling_verdict {
        lines.push(format!("- scaling verdict: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| K (streams) | agg ops/s | shard_efficiency | C | layout | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let eff = p
            .shard_efficiency
            .map_or_else(|| "—".into(), |v| format!("{v:.2}"));
        let layout = p.pool_layout.as_deref().unwrap_or("—");
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.pool_count, p.peak_ops_per_sec, eff, p.client_count, layout, p.report_file
        ));
    }
    lines.push(String::new());
    lines.push("**streams_for_target** (per-stream K=1 peak as ceiling):".into());
    for (target, streams) in &curve.streams_for_target {
        lines.push(format!("- {target} ops/s → {streams} WorkQueue streams"));
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

    fn write_shard_report(dir: &Path, name: &str, k: u32, c: u32, ops: f64) {
        let layout = if k == 1 { "Shared" } else { "DistinctPerSlot" };
        let body = format!(
            r#"{{
            "experiment_id": "bm-be4",
            "dimensions": {{"hardware": "aws-c6i-large", "backend": "nats"}},
            "metrics": {{"achieved_ops_per_sec": {ops}, "client_count": {c}, "pool_count": {k}}},
            "bench_config": {{"publisher": {{"client_count": {c}, "pool_count": {k}, "pool_layout": "{layout}"}}}}
        }}"#
        );
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        write!(f, "{body}").unwrap();
    }

    #[test]
    fn computes_shard_efficiency() {
        let dir = TempDir::new().unwrap();
        write_shard_report(dir.path(), "bm-be4-k1-c256-a.json", 1, 256, 30_000.0);
        write_shard_report(dir.path(), "bm-be4-k4-c256-a.json", 4, 256, 100_000.0);
        let curve = load_be4_shard_curve(dir.path(), "aws-c6i-large", "nats").unwrap();
        assert_eq!(curve.points.len(), 2);
        let k4 = curve.points.iter().find(|p| p.pool_count == 4).unwrap();
        assert!(k4.shard_efficiency.unwrap() > 0.8);
    }
}
