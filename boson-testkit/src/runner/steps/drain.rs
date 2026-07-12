use std::time::Instant;

use anyhow::Result;

use super::super::{RunMode, StepTiming};
use super::super::state::RunState;

/// Cancel a previously enqueued job by index (`CancelJob` step).
pub async fn run_cancel_job(state: &RunState, job_index: usize) -> Result<Option<String>> {
    let Some(job_id) = state.job_ids.get(job_index) else {
        return Ok(Some(format!("job_index {job_index} out of range")));
    };
    state.boson().cancel_job(job_id).await?;
    Ok(None)
}

/// Cancel a non-existent job id and expect [`BosonError::JobNotFound`].
pub async fn run_cancel_missing_job(state: &RunState) -> Result<Option<String>> {
    match state.boson().cancel_job("missing-job-id-for-test").await {
        Err(boson_core::BosonError::JobNotFound(_)) => Ok(None),
        Err(e) => Ok(Some(format!(
            "CancelMissingJob: expected JobNotFound, got {e}"
        ))),
        Ok(()) => Ok(Some(
            "CancelMissingJob: expected JobNotFound, cancel succeeded".into(),
        )),
    }
}

/// Drive the manual worker until idle or `max_steps` (`DrainUntilIdle` step).
pub async fn run_drain(
    step_index: usize,
    mode: RunMode,
    state: &RunState,
    timings: &mut Vec<StepTiming>,
    max_steps: usize,
) -> Result<Option<String>> {
    let start = Instant::now();
    for _ in 0..max_steps {
        if !state.manual().try_run_next().await {
            break;
        }
    }
    if mode == RunMode::Benchmark {
        timings.push(StepTiming {
            step_index,
            op: "drain".into(),
            samples_ms: vec![start.elapsed().as_secs_f64() * 1000.0],
        });
    }
    Ok(None)
}
