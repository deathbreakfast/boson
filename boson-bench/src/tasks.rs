//! Register synthetic tasks for bench experiments from [`BenchRunConfig`].

use boson_runtime::TaskRegistry;
use boson_testkit::fixtures::{register_noop_task, register_noop_task_on_pool};

use crate::config::{BenchRunConfig, PoolLayout};
use crate::experiments::ExperimentPlan;

fn leak_str(s: String) -> &'static str {
    Box::leak(s.into_boxed_str())
}

fn leak_pool(slot: u32) -> &'static str {
    leak_str(BenchRunConfig::pool_name_for_slot(slot))
}

/// Register noop tasks for spread-load / capacity experiments.
pub fn register_publisher_tasks(registry: &mut TaskRegistry, cfg: &BenchRunConfig) {
    let pool_count = cfg.publisher.pool_count.max(1);
    if pool_count == 1 {
        register_noop_task(registry, "noop");
        return;
    }

    for i in 0..pool_count {
        let name = leak_str(format!("noop_{i}"));
        match cfg.publisher.pool_layout {
            PoolLayout::Shared => register_noop_task(registry, name),
            PoolLayout::DistinctPerSlot => register_noop_task_on_pool(registry, name, leak_pool(i)),
        }
    }
}

/// Register tasks required by an experiment plan and run config.
pub fn register_for_plan(registry: &mut TaskRegistry, plan: &ExperimentPlan, cfg: &BenchRunConfig) {
    use boson_testkit::fixtures::{
        register_fail_n_then_ok_task, register_fail_task, register_noop_task,
        register_noop_task_with_priority, register_rate_limited_eps_task,
        register_rate_limited_in_flight_task,
    };

    match plan.id.as_str() {
        "bm-b4" => register_fail_n_then_ok_task(registry, "retryable", 2),
        "bm-b9" => register_rate_limited_in_flight_task(registry, "limited"),
        "bm-b10" => register_rate_limited_eps_task(registry, "limited_eps"),
        "bm-b16" => register_fail_task(registry, "fail"),
        "bm-b15" => {
            register_noop_task_with_priority(registry, "low_prio", 10);
            register_noop_task_with_priority(registry, "high_prio", 1);
        }
        "bm-bi1" => register_noop_task(registry, "noop"),
        "bm-bf2" => {
            for i in 0..cfg.task_fanout_count.max(1) {
                register_noop_task(registry, leak_str(format!("noop_{i}")));
            }
        }
        id if id.starts_with("bm-be")
            || id.starts_with("bm-bm")
            || id.starts_with("bm-bp")
            || id.starts_with("bm-bl")
            || id.starts_with("bm-bd") =>
        {
            register_publisher_tasks(registry, cfg);
        }
        _ => register_noop_task(registry, "noop"),
    }
}
