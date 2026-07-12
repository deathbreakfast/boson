use std::time::Instant;

use anyhow::{anyhow, Result};
use boson_core::BosonError;

use super::super::support::{empty_params, system_actor, EnqueueErrorKind};
use super::super::{RunMode, StepTiming};
use super::super::state::RunState;

/// Enqueue `count` jobs for `task` (`EnqueueN` step); records benchmark timings when applicable.
pub async fn run_enqueue(
    step_index: usize,
    mode: RunMode,
    state: &mut RunState,
    timings: &mut Vec<StepTiming>,
    task: &str,
    count: usize,
    idempotency_key: Option<&String>,
) -> Result<Option<String>> {
    let mut samples = Vec::with_capacity(count);
    for _ in 0..count {
        let start = Instant::now();
        let id = state
            .boson()
            .enqueue(
                task,
                system_actor(),
                empty_params(),
                idempotency_key.cloned(),
            )
            .await
            .map_err(|e| anyhow!("enqueue failed: {e}"))?;
        if mode == RunMode::Benchmark {
            samples.push(start.elapsed().as_secs_f64() * 1000.0);
        }
        state.job_ids.push(id);
    }
    if mode == RunMode::Benchmark && !samples.is_empty() {
        timings.push(StepTiming {
            step_index,
            op: "enqueue".into(),
            samples_ms: samples,
        });
    }
    Ok(None)
}

/// Assert enqueue fails with the expected error kind (`AssertEnqueueError` step).
pub async fn run_assert_enqueue_error(
    mode: RunMode,
    state: &RunState,
    task: &str,
    expected: EnqueueErrorKind,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    match state
        .boson()
        .enqueue(task, system_actor(), empty_params(), None)
        .await
    {
        Ok(job_id) => Ok(Some(format!(
            "AssertEnqueueError: expected {expected:?}, enqueue succeeded with job {job_id}"
        ))),
        Err(e) => {
            let matches = match expected {
                EnqueueErrorKind::TaskNotFound => {
                    matches!(e, BosonError::TaskNotFound(_))
                }
                EnqueueErrorKind::RateLimited => matches!(e, BosonError::RateLimited(_)),
            };
            if matches {
                Ok(None)
            } else {
                Ok(Some(format!(
                    "AssertEnqueueError: expected {expected:?}, got {e}"
                )))
            }
        }
    }
}

/// Update persisted task config fields (`UpsertTaskConfig` step).
pub async fn run_upsert_task_config(
    state: &RunState,
    task: &str,
    max_in_flight: Option<u32>,
    max_eps: Option<u32>,
    max_attempts: Option<u32>,
    base_delay_ms: Option<u64>,
) -> Result<Option<String>> {
    let mut config = state.boson().get_task_config(task).await?;
    if let Some(v) = max_in_flight {
        config.rate_limit_policy.max_in_flight = v;
    }
    if let Some(v) = max_eps {
        config.rate_limit_policy.max_enqueue_per_second = v;
    }
    if let Some(v) = max_attempts {
        config.retry_policy.max_attempts = v;
    }
    if let Some(v) = base_delay_ms {
        config.retry_policy.base_delay_ms = v;
    }
    state.boson().upsert_task_config(config).await?;
    Ok(None)
}
