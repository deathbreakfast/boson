use anyhow::{anyhow, Result};
use boson_core::JobStatus;

use super::super::support::{counting_hit_count, noop_hit_count};
use super::super::RunMode;
use super::super::state::RunState;

/// Assert a job's status by enqueue index (`AssertJobStatus` step).
pub async fn run_assert_job_status(
    mode: RunMode,
    state: &RunState,
    job_index: usize,
    status: JobStatus,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let Some(job_id) = state.job_ids.get(job_index) else {
        return Ok(Some(format!("job_index {job_index} out of range")));
    };
    let job = state
        .boson()
        .get_job(job_id)
        .await?
        .ok_or_else(|| anyhow!("job not found"))?;
    if job.status != status {
        return Ok(Some(format!(
            "AssertJobStatus: expected {status:?}, got {:?} for job {job_id}",
            job.status
        )));
    }
    Ok(None)
}

/// Assert the latest run outcome for a job (`AssertRunOutcome` step).
pub async fn run_assert_run_outcome(
    mode: RunMode,
    state: &RunState,
    job_index: usize,
    run_status: boson_core::RunStatus,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let Some(job_id) = state.job_ids.get(job_index) else {
        return Ok(Some(format!("job_index {job_index} out of range")));
    };
    let runs = state.boson().list_runs(Some(job_id), 0, 8).await?;
    let Some(latest) = runs.last() else {
        return Ok(Some(format!("AssertRunOutcome: no runs for job {job_id}")));
    };
    if latest.status != run_status {
        return Ok(Some(format!(
            "AssertRunOutcome: expected {run_status:?}, got {:?} for job {job_id}",
            latest.status
        )));
    }
    Ok(None)
}

/// Assert two enqueued jobs share the same id (`AssertSameJobId` step).
pub fn run_assert_same_job_id(
    mode: RunMode,
    state: &RunState,
    first_index: usize,
    second_index: usize,
) -> Option<String> {
    if mode == RunMode::Benchmark {
        return None;
    }
    let Some(first) = state.job_ids.get(first_index) else {
        return Some(format!("first_index {first_index} out of range"));
    };
    let Some(second) = state.job_ids.get(second_index) else {
        return Some(format!("second_index {second_index} out of range"));
    };
    if first != second {
        return Some(format!(
            "AssertSameJobId: expected same id, got {first} and {second}"
        ));
    }
    None
}

/// Assert two enqueued jobs have different ids (`AssertDifferentJobId` step).
pub fn run_assert_different_job_id(
    mode: RunMode,
    state: &RunState,
    first_index: usize,
    second_index: usize,
) -> Option<String> {
    if mode == RunMode::Benchmark {
        return None;
    }
    let Some(first) = state.job_ids.get(first_index) else {
        return Some(format!("first_index {first_index} out of range"));
    };
    let Some(second) = state.job_ids.get(second_index) else {
        return Some(format!("second_index {second_index} out of range"));
    };
    if first == second {
        return Some(format!(
            "AssertDifferentJobId: expected different ids, both {first}"
        ));
    }
    None
}

fn handler_hits_since_start(state: &RunState, task: &str) -> usize {
    match task {
        "noop" => noop_hit_count().saturating_sub(state.noop_hits_at_start),
        "counting" => counting_hit_count().saturating_sub(state.counting_hits_at_start),
        _ if task.starts_with("counting") => {
            counting_hit_count().saturating_sub(state.counting_hits_at_start)
        }
        _ if task.starts_with("noop") => noop_hit_count().saturating_sub(state.noop_hits_at_start),
        _ => 0,
    }
}

/// Assert handler invocation count for a fixture task (`AssertHandlerHits` step).
pub fn run_assert_handler_hits(
    mode: RunMode,
    state: &RunState,
    task: &str,
    count: usize,
) -> Option<String> {
    if mode == RunMode::Benchmark {
        return None;
    }
    let hits = handler_hits_since_start(state, task);
    if hits != count {
        return Some(format!(
            "AssertHandlerHits: task {task} expected {count} hits, got {hits}"
        ));
    }
    None
}

/// Assert total job count with optional status filter (`AssertJobCount` step).
pub async fn run_assert_job_count(
    mode: RunMode,
    state: &RunState,
    count: u64,
    status: Option<JobStatus>,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let actual = state.boson().count_jobs(status).await?;
    if actual != count {
        return Ok(Some(format!(
            "AssertJobCount: expected {count}, got {actual} (status filter {status:?})"
        )));
    }
    Ok(None)
}

/// Assert run count for a job by enqueue index (`AssertRunCount` step).
pub async fn run_assert_run_count(
    mode: RunMode,
    state: &RunState,
    job_index: usize,
    count: usize,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let Some(job_id) = state.job_ids.get(job_index) else {
        return Ok(Some(format!("job_index {job_index} out of range")));
    };
    let runs = state.boson().list_runs(Some(job_id), 0, 32).await?;
    if runs.len() != count {
        return Ok(Some(format!(
            "AssertRunCount: expected {count} runs for job {job_id}, got {}",
            runs.len()
        )));
    }
    Ok(None)
}

/// Assert `get_job` returns `None` for `job_id` (`AssertJobMissing` step).
pub async fn run_assert_job_missing(
    mode: RunMode,
    state: &RunState,
    job_id: &str,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    if state.boson().get_job(job_id).await?.is_some() {
        return Ok(Some(format!(
            "AssertJobMissing: expected no job for id {job_id}"
        )));
    }
    Ok(None)
}
