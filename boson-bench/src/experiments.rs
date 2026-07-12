//! Map experiment IDs to scenario specs.

use anyhow::{bail, Result};
use boson_testkit::ScenarioSpec;

/// Resolved experiment plan for one bench run.
pub struct ExperimentPlan {
    /// Normalized experiment id.
    pub id: String,
    /// Scenario to execute (empty for load/scale/http-only experiments).
    pub scenario: ScenarioSpec,
    /// Optional op count override.
    pub ops: Option<u32>,
}

type ScenarioBuilder = fn(Option<u32>) -> (ScenarioSpec, Option<u32>);

fn bm_b0(ops: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (
        ScenarioSpec::enqueue_only("noop", ops.unwrap_or(5000) as usize),
        ops,
    )
}

fn bm_b1(_: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (ScenarioSpec::enqueue_and_drain("noop"), None)
}

fn bm_b2(ops: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (
        ScenarioSpec::multi_job_drain("noop", ops.unwrap_or(32) as usize),
        ops,
    )
}

fn bm_b3(_: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (ScenarioSpec::lease_contention_drain("noop"), None)
}

fn bm_b4(_: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (ScenarioSpec::retry_then_success("retryable", 2), None)
}

fn bm_b5(_: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (ScenarioSpec::run_lifecycle("noop"), None)
}

fn bm_b7(ops: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (
        ScenarioSpec::enqueue_only("noop", 0),
        Some(ops.unwrap_or(100)),
    )
}

fn bm_b12(ops: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (
        ScenarioSpec::list_and_count_at_depth("noop", ops.unwrap_or(1000) as usize),
        ops,
    )
}

fn bm_b17(ops: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (
        ScenarioSpec::enqueue_only("noop", ops.unwrap_or(5000) as usize),
        ops,
    )
}

fn enqueue_only_stub(_: Option<u32>) -> (ScenarioSpec, Option<u32>) {
    (ScenarioSpec::enqueue_only("noop", 0), None)
}

fn lookup_builder(normalized: &str) -> Option<ScenarioBuilder> {
    match normalized {
        "bm-b0" => Some(bm_b0),
        "bm-b1" | "bm-b6" => Some(bm_b1),
        "bm-b2" => Some(bm_b2),
        "bm-b3" => Some(bm_b3),
        "bm-b4" => Some(bm_b4),
        "bm-b5" => Some(bm_b5),
        "bm-b7" => Some(bm_b7),
        "bm-b8" => Some(|_| (ScenarioSpec::idempotency_smoke("noop"), None)),
        "bm-b9" => Some(|_| (ScenarioSpec::rate_limit_in_flight("limited"), None)),
        "bm-b10" => Some(|_| (ScenarioSpec::rate_limit_eps("limited_eps"), None)),
        "bm-b11" => Some(|_| (ScenarioSpec::cancel_queued_job("noop"), None)),
        "bm-b12" => Some(bm_b12),
        "bm-b13" => Some(|_| (ScenarioSpec::task_config_rate_limit("noop"), None)),
        "bm-b14" => Some(|_| (ScenarioSpec::restart_runtime_drain("noop"), None)),
        "bm-b15" => Some(|_| {
            (
                ScenarioSpec::pool_priority_drain("low_prio", "high_prio"),
                None,
            )
        }),
        "bm-b16" => Some(|_| (ScenarioSpec::handler_failure_terminal("fail"), None)),
        "bm-b17" => Some(bm_b17),
        "bm-bi1" | "bm-bf2" | "bm-be1" | "bm-be2" | "bm-be4" | "bm-bd1" | "bm-bd2"
        | "bm-bl0" | "bm-bl1" | "bm-bl2" | "bm-bl3" | "bm-bl4" | "bm-bp1" | "bm-bp2"
        | "bm-bm1" | "bm-bm2" | "bm-bm3" | "bm-bm4" => Some(enqueue_only_stub),
        _ => None,
    }
}

/// Resolve an experiment id to a scenario (see EXPERIMENTS.md).
pub fn resolve_experiment(id: &str, ops: Option<u32>) -> Result<ExperimentPlan> {
    let normalized = id.to_ascii_lowercase();
    let builder = lookup_builder(&normalized)
        .ok_or_else(|| anyhow::anyhow!("unknown experiment {id}; see boson-bench/EXPERIMENTS.md"))?;
    let (scenario, ops) = builder(ops);
    Ok(ExperimentPlan {
        id: normalized,
        scenario,
        ops,
    })
}

/// All registered experiment ids.
pub const ALL_EXPERIMENTS: &[(&str, &str)] = &[
    ("bm-b0", "Enqueue-only throughput"),
    ("bm-b1", "Enqueue + single worker noop execute"),
    ("bm-b2", "Worker concurrency (multi-job drain)"),
    ("bm-b3", "Lease contention (split-boson-server)"),
    ("bm-b4", "Retry/backoff"),
    ("bm-b5", "Run lifecycle (enqueue→running→complete)"),
    ("bm-b6", "Split-server enqueue+drain"),
    ("bm-b7", "HTTP admin enqueue"),
    ("bm-b8", "Idempotency"),
    ("bm-b9", "Rate limit in-flight"),
    ("bm-b10", "Rate limit EPS"),
    ("bm-b11", "Cancel queued job"),
    ("bm-b12", "Admin read at depth"),
    ("bm-b13", "Task config CRUD"),
    ("bm-b14", "Runtime restart recovery"),
    ("bm-b15", "Pool priority ordering"),
    ("bm-b16", "Handler failure terminal"),
    ("bm-b17", "Telemetry overhead (use --telemetry console)"),
    ("bm-bl0", "Sustained load 100 jobs/s"),
    ("bm-bl1", "Sustained load 1k jobs/s"),
    ("bm-bl2", "Sustained load 10k jobs/s"),
    ("bm-bl3", "Sustained load 100k jobs/s"),
    ("bm-bl4", "Sustained load 1M jobs/s (mem ceiling)"),
    ("bm-be1", "Enqueue capacity C=1 (no worker)"),
    ("bm-be2", "Enqueue capacity C=8 (no worker)"),
    ("bm-be4", "Enqueue capacity C=64×K=10 (no worker)"),
    ("bm-bd1", "Dequeue capacity W×ManualWorker (prefill+drain)"),
    ("bm-bd2", "Dequeue capacity W×background worker poll=0"),
    ("bm-bp1", "Multi-pool enqueue"),
    ("bm-bp2", "Multi-pool enqueue"),
    ("bm-bm1", "Multi-client enqueue"),
    ("bm-bm2", "Multi-client ceiling (C=64)"),
    ("bm-bm3", "Hot-pool contention"),
    ("bm-bm4", "Spread load C×K (Track T primary)"),
    ("bm-bi1", "Keyed enqueue (idempotency on/off)"),
    ("bm-bf2", "Task fan-out (BOSON_BENCH_TASK_COUNT)"),
];

/// Print all registered experiment ids.
pub fn list_experiments() {
    for (id, desc) in ALL_EXPERIMENTS {
        println!("{id}: {desc}");
    }
    println!("See boson-bench/EXPERIMENTS.md for full matrix");
}

/// Experiments in a named matrix subset.
pub fn subset_experiments(subset: &str) -> Result<Vec<&'static str>> {
    let ids: Vec<&str> = match subset {
        "mem-lab" => vec![
            "bm-b0", "bm-b1", "bm-b2", "bm-b3", "bm-b4", "bm-b5", "bm-b8", "bm-b9", "bm-b10",
            "bm-b11", "bm-b12", "bm-b13", "bm-b14", "bm-b15", "bm-b16",
        ],
        "mem-scale" => vec!["bm-bp1", "bm-bp2", "bm-bm1", "bm-bm2", "bm-bm3", "bm-bm4"],
        "mem-projection-inputs" => vec!["bm-bl0", "bm-bl1", "bm-bl2", "bm-bl3", "bm-bm2"],
        "embedded-lab" => vec![
            "bm-b0", "bm-b1", "bm-b2", "bm-b3", "bm-b4", "bm-b5", "bm-bl0", "bm-bl1", "bm-bl2",
            "bm-bl3",
        ],
        "tier3-capacity" => vec!["bm-be1", "bm-be2", "bm-be4", "bm-bd1", "bm-bd2"],
        "tier3-capacity-full" => vec![
            "bm-b0", "bm-be1", "bm-be2", "bm-be4", "bm-bd1", "bm-bd2",
        ],
        "scylla-lab" => vec![
            "bm-b0", "bm-b1", "bm-b2", "bm-b3", "bm-b4", "bm-b5", "bm-b7", "bm-b8", "bm-b9",
            "bm-b10", "bm-b11", "bm-b12", "bm-b13", "bm-b14", "bm-b15", "bm-b16", "bm-bl0",
            "bm-bl1", "bm-bl2", "bm-bl3",
        ],
        other => bail!(
            "unknown subset {other}; use mem-lab|mem-scale|mem-projection-inputs|embedded-lab|scylla-lab|tier3-capacity|tier3-capacity-full"
        ),
    };
    Ok(ids)
}
