//! Resolve [`BenchRunConfig`] from experiment defaults plus caller overrides.

use boson_core::IdempotencyMode;

use crate::config::{BenchRunConfig, PoolLayout};

/// Optional CLI / harness overrides layered onto [`BenchRunConfig::for_experiment`].
#[derive(Debug, Clone, Default)]
pub struct BenchConfigOverrides {
    pub idempotency_mode: Option<IdempotencyMode>,
    pub client_count: Option<u32>,
    pub pool_count: Option<u32>,
    pub pool_layout: Option<PoolLayout>,
    pub prefill_count: Option<u64>,
    pub worker_count: Option<u32>,
    pub worker_poll_ms: Option<u64>,
    pub task_fanout_count: Option<u32>,
    pub storage_topology: Option<String>,
}

/// Build the effective config for one run.
///
/// Precedence (lowest → highest): experiment defaults, harness env, CLI flags.
#[must_use]
pub fn resolve_bench_config(
    experiment_id: &str,
    overrides: BenchConfigOverrides,
) -> BenchRunConfig {
    let mut cfg = BenchRunConfig::for_experiment(experiment_id);
    apply_env_overrides(&mut cfg);
    apply_overrides(&mut cfg, overrides);
    cfg
}

fn apply_overrides(cfg: &mut BenchRunConfig, o: BenchConfigOverrides) {
    if let Some(mode) = o.idempotency_mode {
        cfg.idempotency_mode = Some(mode);
    }
    if let Some(c) = o.client_count {
        cfg.publisher.client_count = c;
    }
    if let Some(k) = o.pool_count {
        cfg.publisher.pool_count = k;
        if k > 1 {
            cfg.worker_fleet.worker_pools = None;
        }
    }
    if let Some(layout) = o.pool_layout {
        cfg.publisher.pool_layout = layout;
        if layout == PoolLayout::DistinctPerSlot && cfg.publisher.pool_count > 1 {
            cfg.worker_fleet.worker_pools = None;
        }
    }
    if let Some(n) = o.prefill_count {
        cfg.drain.prefill_count = n;
    }
    if let Some(w) = o.worker_count {
        cfg.drain.worker_count = w;
    }
    if let Some(ms) = o.worker_poll_ms {
        cfg.drain.poll_interval_ms = ms;
    }
    if let Some(t) = o.task_fanout_count {
        cfg.task_fanout_count = t;
    }
    if let Some(topo) = o.storage_topology {
        cfg.storage_topology = Some(topo);
    }
}

/// Harness-only env overlay (AWS scripts, local ad-hoc). Library code does not read env.
fn apply_env_overrides(cfg: &mut BenchRunConfig) {
    if let Ok(s) = std::env::var("BOSON_BENCH_IDEMPOTENCY_MODE") {
        if let Some(mode) = IdempotencyMode::parse(&s) {
            cfg.idempotency_mode = Some(mode);
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_CLIENT_COUNT") {
        if let Ok(c) = v.parse() {
            cfg.publisher.client_count = c;
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_POOL_COUNT") {
        if let Ok(k) = v.parse() {
            cfg.publisher.pool_count = k;
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_POOL_LAYOUT") {
        cfg.publisher.pool_layout = match v.to_ascii_lowercase().as_str() {
            "distinct" | "distinct_per_slot" => PoolLayout::DistinctPerSlot,
            "shared" => PoolLayout::Shared,
            _ => cfg.publisher.pool_layout,
        };
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_PREFILL_COUNT") {
        if let Ok(n) = v.parse() {
            cfg.drain.prefill_count = n;
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_WORKER_COUNT") {
        if let Ok(w) = v.parse() {
            cfg.drain.worker_count = w;
        }
    }
    if let Ok(v) = std::env::var("BOSON_WORKER_POLL_MS") {
        if let Ok(ms) = v.parse() {
            cfg.drain.poll_interval_ms = ms;
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_TASK_COUNT") {
        if let Ok(t) = v.parse() {
            cfg.task_fanout_count = t;
        }
    }
    if let Ok(v) = std::env::var("BOSON_BENCH_STORAGE_TOPOLOGY") {
        if !v.is_empty() {
            cfg.storage_topology = Some(v);
        }
    }
}

/// Convenience for matrix subset runs (experiment defaults + env only).
#[must_use]
pub fn bench_config_for_experiment(experiment_id: &str) -> BenchRunConfig {
    resolve_bench_config(experiment_id, BenchConfigOverrides::default())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cli_worker_count_overrides_experiment_default() {
        let cfg = resolve_bench_config(
            "bm-bd1",
            BenchConfigOverrides {
                worker_count: Some(4),
                ..Default::default()
            },
        );
        assert_eq!(cfg.drain.worker_count, 4);
    }

    #[test]
    fn bd_experiment_pins_global_pool() {
        let cfg = BenchRunConfig::for_experiment("bm-bd2");
        assert_eq!(
            cfg.worker_fleet.worker_pools,
            Some(vec!["global".to_string()])
        );
    }
}
