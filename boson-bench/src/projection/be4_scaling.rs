//! BM-BE4 publisher-count scaling curve (`JetStream` single-stream saturation).

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

const TARGETS: &[u64] = &[330_000, 1_000_000, 10_000_000];

/// Peak throughput at one publisher concurrency level.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherPoint {
    pub client_count: u32,
    pub peak_ops_per_sec: f64,
    pub pool_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_layout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enqueue_mode: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ops_per_client: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vs_client_1: Option<f64>,
    pub report_file: String,
}

/// Aggregated BE4 publisher sweep for one hardware/backend slice.
#[derive(Debug, Serialize)]
pub struct Be4PublisherCurve {
    pub hardware: String,
    pub backend: String,
    pub workload: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pool_layout: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_bench_peak: Option<f64>,
    pub points: Vec<PublisherPoint>,
    pub peak_ops_per_sec: f64,
    pub peak_client_count: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub saturation_client_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scaling_exponent: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bottleneck_verdict: Option<String>,
    pub streams_for_target: HashMap<String, u64>,
    pub disclaimer: String,
}

/// Build curve JSON and optionally write to `out`.
pub fn be4_publisher_curve(
    hardware: &str,
    backend: &str,
    reports_dir: &Path,
    out: Option<PathBuf>,
) -> Result<()> {
    let curve = load_be4_publisher_curve(reports_dir, hardware, backend)?;
    let out_path = out.unwrap_or_else(|| {
        reports_dir.join(format!(
            "scaling-curve-be4-publishers-{hardware}-{backend}.json"
        ))
    });
    if let Some(parent) = out_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(&out_path, serde_json::to_string_pretty(&curve)?)?;
    println!("wrote {}", out_path.display());
    println!("{}", render_be4_markdown(&curve));
    Ok(())
}

/// Load peak BM-BE4 achieved rate per client count.
#[allow(clippy::too_many_lines)] // Curve loading is one sequential filter-and-aggregate pipeline.
pub fn load_be4_publisher_curve(
    reports_dir: &Path,
    hardware: &str,
    backend: &str,
) -> Result<Be4PublisherCurve> {
    let mut best: HashMap<u32, (f64, u32, String, Option<String>, String)> = HashMap::new();
    let mut nats_bench_peak: Option<f64> = None;
    let mut common_pool_count: Option<u32> = None;
    let mut common_layout: Option<String> = None;

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
            // Publisher sweeps use bm-be4-c{C}-* filenames; ignore ad-hoc BE4 gate reports.
            if !fname.starts_with("bm-be4-c") {
                continue;
            }
            if v.pointer("/dimensions/hardware").and_then(Value::as_str) != Some(hardware) {
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
            let client_count = v
                .pointer("/metrics/client_count")
                .or_else(|| v.pointer("/bench_config/publisher/client_count"))
                .and_then(Value::as_u64)
                .unwrap_or(0) as u32;
            if client_count == 0 {
                continue;
            }
            let pool_count = v
                .pointer("/metrics/pool_count")
                .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
                .and_then(Value::as_u64)
                .unwrap_or(1) as u32;
            let layout = v
                .pointer("/bench_config/publisher/pool_layout")
                .and_then(Value::as_str)
                .map(str::to_string);
            let enqueue_mode = v
                .pointer("/nats_pipeline/enqueue_mode")
                .and_then(Value::as_str)
                .map(str::to_string);
            if let Some(bp) = v
                .pointer("/diagnostics/nats_bench_peak_ops")
                .and_then(Value::as_f64)
            {
                nats_bench_peak = Some(nats_bench_peak.map_or(bp, |prev| prev.max(bp)));
            }
            common_pool_count = Some(common_pool_count.unwrap_or(pool_count).min(pool_count));
            if common_layout.is_none() {
                common_layout.clone_from(&layout);
            }
            best.entry(client_count)
                .and_modify(
                    |(best_rate, best_pool, best_layout, best_mode, best_file)| {
                        if rate > *best_rate {
                            *best_rate = rate;
                            *best_pool = pool_count;
                            *best_layout = layout.clone().unwrap_or_default();
                            best_mode.clone_from(&enqueue_mode);
                            best_file.clone_from(&fname);
                        }
                    },
                )
                .or_insert_with(|| {
                    (
                        rate,
                        pool_count,
                        layout.clone().unwrap_or_default(),
                        enqueue_mode,
                        fname,
                    )
                });
        }
    }

    if best.is_empty() {
        bail!(
            "no BM-BE4 reports for {hardware}/{backend} in {}",
            reports_dir.display()
        );
    }

    let baseline_c1 = best.get(&1).map(|(r, _, _, _, _)| *r);
    let mut points: Vec<PublisherPoint> = best
        .into_iter()
        .map(
            |(client_count, (peak, pool_count, layout, enqueue_mode, file))| {
                let ops_per_client = peak / f64::from(client_count.max(1));
                let vs_client_1 = baseline_c1.filter(|b| *b > 0.0).map(|b| peak / b);
                PublisherPoint {
                    client_count,
                    peak_ops_per_sec: peak,
                    pool_count,
                    pool_layout: Some(layout),
                    enqueue_mode,
                    ops_per_client: Some(ops_per_client),
                    vs_client_1,
                    report_file: file,
                }
            },
        )
        .collect();
    points.sort_by_key(|p| p.client_count);

    let peak_point = points
        .iter()
        .max_by(|a, b| {
            a.peak_ops_per_sec
                .partial_cmp(&b.peak_ops_per_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        })
        .expect("non-empty points");
    let peak_ops = peak_point.peak_ops_per_sec;
    let peak_client_count = peak_point.client_count;
    let saturation_client_count = detect_saturation(&points);
    let scaling_exponent = fit_publisher_exponent(&points);
    let bottleneck_verdict = classify_bottleneck(peak_ops, nats_bench_peak, &points);

    let mut streams_for_target = HashMap::new();
    for &target in TARGETS {
        let streams = if peak_ops > 0.0 {
            (target as f64 / peak_ops).ceil() as u64
        } else {
            0
        };
        streams_for_target.insert(target.to_string(), streams.max(1));
    }

    Ok(Be4PublisherCurve {
        hardware: hardware.into(),
        backend: backend.into(),
        workload: "bm-be4".into(),
        pool_count: common_pool_count,
        pool_layout: common_layout,
        nats_bench_peak,
        points,
        peak_ops_per_sec: peak_ops,
        peak_client_count,
        saturation_client_count,
        scaling_exponent,
        bottleneck_verdict: Some(bottleneck_verdict),
        streams_for_target,
        disclaimer: "streams_for_target assumes peak_ops_per_sec is per-stream ceiling (use K=1 shared sweep); multiply by pool_count when using distinct multi-pool layout.".into(),
    })
}

fn detect_saturation(points: &[PublisherPoint]) -> Option<u32> {
    if points.len() < 2 {
        return None;
    }
    let mut prev = &points[0];
    for p in points.iter().skip(1) {
        if prev.peak_ops_per_sec > 0.0 {
            let gain = (p.peak_ops_per_sec - prev.peak_ops_per_sec) / prev.peak_ops_per_sec;
            let c_ratio = f64::from(p.client_count) / f64::from(prev.client_count.max(1));
            if c_ratio >= 1.5 && gain < 0.05 {
                return Some(prev.client_count);
            }
        }
        prev = p;
    }
    None
}

fn fit_publisher_exponent(points: &[PublisherPoint]) -> Option<f64> {
    if points.len() < 2 {
        return None;
    }
    let mut sum_log_c = 0.0;
    let mut sum_log_r = 0.0;
    let mut sum_log_c_sq = 0.0;
    let mut sum_log_c_log_r = 0.0;
    let n = points.len() as f64;
    for p in points {
        let lc = f64::from(p.client_count.max(1)).ln();
        let lr = p.peak_ops_per_sec.ln();
        sum_log_c += lc;
        sum_log_r += lr;
        sum_log_c_sq += lc * lc;
        sum_log_c_log_r += lc * lr;
    }
    let denom = n.mul_add(sum_log_c_sq, -(sum_log_c * sum_log_c));
    if denom.abs() < f64::EPSILON {
        return None;
    }
    Some(n.mul_add(sum_log_c_log_r, -(sum_log_c * sum_log_r)) / denom)
}

fn classify_bottleneck(
    peak_ops: f64,
    nats_bench: Option<f64>,
    points: &[PublisherPoint],
) -> String {
    if let Some(bench) = nats_bench {
        if bench > 0.0 && (peak_ops / bench) < 0.5 {
            return "boson_adapter".into();
        }
        if (peak_ops - bench).abs() / bench.max(1.0) < 0.15 {
            return "jetstream_single_stream".into();
        }
    }
    if let Some(c_sat) = detect_saturation(points) {
        return format!("publisher_saturation_at_c={c_sat}");
    }
    if points.len() >= 2 {
        let first = &points[0];
        let last = points.last().expect("len >= 2");
        let expected_linear = first.peak_ops_per_sec * f64::from(last.client_count)
            / f64::from(first.client_count.max(1));
        if expected_linear > 0.0 && last.peak_ops_per_sec / expected_linear > 0.85 {
            return "linear_scaling".into();
        }
    }
    "unknown".into()
}

pub fn render_be4_markdown(curve: &Be4PublisherCurve) -> String {
    let mut lines = vec![
        "# Boson BM-BE4 publisher scaling curve".into(),
        String::new(),
        format!("- hardware: `{}`", curve.hardware),
        format!("- backend: `{}`", curve.backend),
        format!(
            "- peak: **{:.0} ops/s** at C={}",
            curve.peak_ops_per_sec, curve.peak_client_count
        ),
    ];
    if let Some(k) = curve.pool_count {
        lines.push(format!("- pool_count (K): {k}"));
    }
    if let Some(l) = &curve.pool_layout {
        lines.push(format!("- pool_layout: `{l}`"));
    }
    if let Some(c) = curve.saturation_client_count {
        lines.push(format!(
            "- saturation (est.): C≥{c} (<5% gain when scaling publishers)"
        ));
    }
    if let Some(b) = curve.nats_bench_peak {
        lines.push(format!("- nats bench pub peak: {b:.0} ops/s"));
    }
    if let Some(v) = &curve.bottleneck_verdict {
        lines.push(format!("- bottleneck: `{v}`"));
    }
    lines.push(String::new());
    lines.push("| C (publishers) | peak ops/s | ops/client | vs C=1 | K | report |".into());
    lines.push("| --- | --- | --- | --- | --- | --- |".into());
    for p in &curve.points {
        let vs = p
            .vs_client_1
            .map_or_else(|| "—".into(), |v| format!("{v:.2}×"));
        let opc = p
            .ops_per_client
            .map_or_else(|| "—".into(), |v| format!("{v:.0}"));
        lines.push(format!(
            "| {} | {:.0} | {} | {} | {} | {} |",
            p.client_count, p.peak_ops_per_sec, opc, vs, p.pool_count, p.report_file
        ));
    }
    lines.push(String::new());
    lines.push("**streams_for_target** (single-stream peak as ceiling):".into());
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

    fn write_report(dir: &Path, name: &str, c: u32, ops: f64) {
        let body = format!(
            r#"{{
            "experiment_id": "bm-be4",
            "dimensions": {{"hardware": "aws-c6i-large", "backend": "nats"}},
            "metrics": {{"achieved_ops_per_sec": {ops}, "client_count": {c}, "pool_count": 1}},
            "bench_config": {{"publisher": {{"client_count": {c}, "pool_count": 1, "pool_layout": "Shared"}}}}
        }}"#
        );
        let mut f = std::fs::File::create(dir.join(name)).unwrap();
        write!(f, "{body}").unwrap();
    }

    #[test]
    fn picks_peak_per_client_count() {
        let dir = TempDir::new().unwrap();
        write_report(dir.path(), "bm-be4-c8-a.json", 8, 20_000.0);
        write_report(dir.path(), "bm-be4-c8-b.json", 8, 25_000.0);
        write_report(dir.path(), "bm-be4-c32-a.json", 32, 40_000.0);
        let curve = load_be4_publisher_curve(dir.path(), "aws-c6i-large", "nats").unwrap();
        assert_eq!(curve.points.len(), 2);
        assert!((curve.peak_ops_per_sec - 40_000.0).abs() < 1.0);
        assert_eq!(curve.peak_client_count, 32);
    }
}
