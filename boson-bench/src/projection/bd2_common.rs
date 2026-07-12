//! Shared helpers for BM-BD2 drain scaling curves.

use serde_json::Value;

pub const BD2_EXPERIMENT: &str = "bm-bd2";

pub fn drain_rate(v: &Value) -> f64 {
    v.pointer("/metrics/drain_ops_per_sec")
        .or_else(|| v.pointer("/metrics/achieved_ops_per_sec"))
        .and_then(Value::as_f64)
        .unwrap_or(0.0)
}

pub fn worker_count(v: &Value, fname: &str) -> Option<u32> {
    if let Some(w) = v
        .pointer("/metrics/worker_count")
        .or_else(|| v.pointer("/bench_config/drain/worker_count"))
        .and_then(Value::as_u64)
    {
        return Some(w as u32);
    }
    parse_tag_u32(fname, "-w", "-")
}

pub fn pool_count(v: &Value, fname: &str) -> Option<u32> {
    v.pointer("/metrics/pool_count")
        .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
        .and_then(Value::as_u64)
        .map(|n| n as u32)
        .or_else(|| parse_tag_u32(fname, "-k", "-"))
}

pub fn parse_fleet_size(fname: &str) -> Option<u32> {
    parse_tag_u32(fname, "fleet-n", "-")
}

fn parse_tag_u32(fname: &str, prefix: &str, _end: &str) -> Option<u32> {
    let idx = fname.find(prefix)?;
    let rest = &fname[idx + prefix.len()..];
    let num: String = rest.chars().take_while(char::is_ascii_digit).collect();
    num.parse().ok()
}

pub fn detect_saturation_w(points: &[(u32, f64)]) -> Option<u32> {
    if points.len() < 2 {
        return None;
    }
    let mut sorted = points.to_vec();
    sorted.sort_by_key(|(w, _)| *w);
    let mut prev = sorted[0];
    for &(w, rate) in sorted.iter().skip(1) {
        if prev.1 > 0.0 {
            let gain = (rate - prev.1) / prev.1;
            let w_ratio = f64::from(w) / f64::from(prev.0.max(1));
            if w_ratio >= 1.5 && gain < 0.05 {
                return Some(prev.0);
            }
        }
        prev = (w, rate);
    }
    None
}

pub fn classify_sublinear(efficiency: f64) -> &'static str {
    if efficiency >= 0.7 {
        "linear"
    } else if efficiency >= 0.4 {
        "sublinear"
    } else {
        "saturated"
    }
}
