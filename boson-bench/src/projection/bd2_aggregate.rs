//! Aggregate per-client BM-BD2 multibench reports into fleet totals.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};

use super::bd2_common::{self, BD2_EXPERIMENT};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct CellKey {
    fleet_size: u32,
    pool_count: u32,
    worker_count: u32,
    bench_client_count: u32,
    hardware: String,
    backend: String,
}

fn cell_key(v: &Value) -> Option<CellKey> {
    let dims = v.get("dimensions")?;
    if dims.get("aggregate").and_then(Value::as_bool).unwrap_or(false) {
        return None;
    }
    if v.get("experiment_id").and_then(|e| e.as_str()) != Some(BD2_EXPERIMENT) {
        return None;
    }
    let bench_client_count = dims
        .get("bench_client_count")
        .and_then(Value::as_u64)
        .unwrap_or(1) as u32;
    if bench_client_count <= 1 {
        return None;
    }
    Some(CellKey {
        fleet_size: dims
            .get("fleet_size")
            .and_then(Value::as_u64)
            .unwrap_or(4) as u32,
        pool_count: v
            .pointer("/metrics/pool_count")
            .or_else(|| v.pointer("/bench_config/publisher/pool_count"))
            .and_then(Value::as_u64)
            .unwrap_or(4) as u32,
        worker_count: v
            .pointer("/metrics/worker_count")
            .or_else(|| v.pointer("/bench_config/drain/worker_count"))
            .and_then(Value::as_u64)
            .unwrap_or(1) as u32,
        bench_client_count,
        hardware: dims.get("hardware")?.as_str()?.to_string(),
        backend: dims.get("backend")?.as_str()?.to_string(),
    })
}

fn client_index(v: &Value, fname: &str) -> Option<u32> {
    if let Some(idx) = v
        .pointer("/dimensions/bench_client_index")
        .and_then(Value::as_u64)
    {
        return Some(idx as u32);
    }
    for i in 0..8 {
        if fname.contains(&format!("-i{i}-")) {
            return Some(i);
        }
    }
    None
}

#[allow(clippy::too_many_lines)] // Aggregation pipeline is clearer as one sequential report transform.
pub fn aggregate_bd2(
    reports_dir: &Path,
    out_dir: Option<&Path>,
    hardware_filter: Option<&str>,
    backend_filter: Option<&str>,
    cell_prefix: Option<&str>,
) -> Result<Vec<PathBuf>> {
    let out_dir = out_dir.unwrap_or(reports_dir);
    std::fs::create_dir_all(out_dir)?;

    let mut groups: HashMap<CellKey, HashMap<u32, (Value, String)>> = HashMap::new();

    for entry in std::fs::read_dir(reports_dir)? {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("json") {
            continue;
        }
        let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if fname.contains("-aggregate-") {
            continue;
        }
        if let Some(prefix) = cell_prefix {
            if !fname.starts_with(prefix) {
                continue;
            }
        }
        let text = std::fs::read_to_string(&path)?;
        let v: Value = serde_json::from_str(&text)?;
        let Some(key) = cell_key(&v) else {
            continue;
        };
        if let Some(hw) = hardware_filter {
            if key.hardware != hw {
                continue;
            }
        }
        if let Some(be) = backend_filter {
            if key.backend != be {
                continue;
            }
        }
        let idx = client_index(&v, fname).with_context(|| {
            format!("multibench report missing bench_client_index: {fname}")
        })?;
        groups
            .entry(key)
            .or_default()
            .insert(idx, (v, fname.to_string()));
    }

    if groups.is_empty() {
        bail!(
            "no multibench BM-BD2 reports in {} (prefix={cell_prefix:?})",
            reports_dir.display()
        );
    }

    let mut written = Vec::new();
    for (key, clients) in groups {
        let count = key.bench_client_count;
        for i in 0..count {
            if !clients.contains_key(&i) {
                bail!(
                    "missing bench_client_index={i} for bc={count} fleet_size={}",
                    key.fleet_size
                );
            }
        }
        let mut total = 0.0;
        let mut per_client = Vec::new();
        for i in 0..count {
            let (v, fname) = &clients[&i];
            let rate = bd2_common::drain_rate(v);
            if rate <= 0.0 {
                bail!("zero drain rate for bench_client_index={i} ({fname})");
            }
            total += rate;
            per_client.push(json!({"bench_client_index": i, "drain_ops_per_sec": rate, "report_file": fname}));
        }

        let template = clients[&0].1.clone();
        let out_name = template
            .replace("-i0-", "-aggregate-")
            .replace(
                &format!("-k{}-", key.pool_count),
                &format!("-k{}-bc{count}-", key.pool_count),
            );
        let out_path = out_dir.join(&out_name);

        let aggregate = json!({
            "experiment_id": BD2_EXPERIMENT,
            "dimensions": {
                "backend": key.backend,
                "topology": "isolated-lab",
                "telemetry": "off",
                "hardware": key.hardware,
                "storage_topology": "nats-fleet-multibench",
                "fleet_size": key.fleet_size,
                "bench_client_count": count,
                "aggregate": true,
            },
            "metrics": {
                "drain_ops_per_sec": total,
                "achieved_ops_per_sec": total,
                "fleet_aggregate_ops_per_sec": total,
                "pool_count": key.pool_count,
                "worker_count": key.worker_count,
                "metric_kind": "drain",
            },
            "per_client": per_client,
            "pass": true,
            "status": "ok",
            "notes": format!("multibench aggregate bc={count} fleet_size={} total={total:.0} drain/s", key.fleet_size),
        });
        std::fs::write(&out_path, serde_json::to_string_pretty(&aggregate)?)?;
        println!(
            "aggregate bc={count} fleet_size={}: {:.0} drain/s -> {}",
            key.fleet_size,
            total,
            out_path.display()
        );
        written.push(out_path);
    }
    Ok(written)
}
