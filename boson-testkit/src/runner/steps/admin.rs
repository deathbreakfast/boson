use std::time::Instant;

use anyhow::Result;

use super::super::state::RunState;
use super::super::{RunMode, StepTiming};

/// Assert admin list/count APIs return the expected job count (`AdminListCount` step).
pub async fn run_admin_list_count(
    step_index: usize,
    mode: RunMode,
    state: &RunState,
    timings: &mut Vec<StepTiming>,
    expected_count: u64,
) -> Result<Option<String>> {
    let start = Instant::now();
    let jobs = state.boson()?.list_jobs(None, 0, 10_000).await?;
    let count = state.boson()?.count_jobs(None).await?;
    if mode == RunMode::Benchmark {
        timings.push(StepTiming {
            step_index,
            op: "admin_read".into(),
            samples_ms: vec![start.elapsed().as_secs_f64() * 1000.0],
        });
        return Ok(None);
    }
    if count != expected_count {
        return Ok(Some(format!(
            "AdminListCount: expected count {expected_count}, got {count}"
        )));
    }
    if jobs.len() as u64 != expected_count.min(10_000) && expected_count <= 10_000 {
        return Ok(Some(format!(
            "AdminListCount: list len {} != expected {}",
            jobs.len(),
            expected_count
        )));
    }
    Ok(None)
}

/// Assert task run stats from admin APIs (`AssertTaskRunStats` step).
pub async fn run_assert_task_run_stats(
    mode: RunMode,
    state: &RunState,
    task: &str,
    runs_total: u32,
    success_count: u32,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let run_stats = state.boson()?.task_run_stats(task).await?;
    if run_stats.runs_total != runs_total {
        return Ok(Some(format!(
            "AssertTaskRunStats: task {task} expected runs_total {runs_total}, got {}",
            run_stats.runs_total
        )));
    }
    if run_stats.success_count != success_count {
        return Ok(Some(format!(
            "AssertTaskRunStats: task {task} expected success_count {success_count}, got {}",
            run_stats.success_count
        )));
    }
    Ok(None)
}
