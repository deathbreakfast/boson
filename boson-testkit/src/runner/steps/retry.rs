use anyhow::{anyhow, Result};
use boson_core::{JobStatus, TaskConfig};

use super::super::support::{empty_params, system_actor};
use super::super::RunMode;
use super::super::state::RunState;

/// Exercise retry backoff until success or terminal failure (`RetryBackoff` step).
pub async fn run_retry_backoff(
    mode: RunMode,
    state: &mut RunState,
    task: &str,
    fail_attempts: u32,
) -> Result<Option<String>> {
    if mode == RunMode::Benchmark {
        return Ok(None);
    }
    let mut config = TaskConfig::default_for(task);
    config.retry_policy.max_attempts = fail_attempts.saturating_add(1).max(1);
    config.retry_policy.base_delay_ms = 0;
    state.boson().upsert_task_config(config).await?;

    let id = state
        .boson()
        .enqueue(task, system_actor(), empty_params(), None)
        .await
        .map_err(|e| anyhow!("retry enqueue failed: {e}"))?;
    state.job_ids.push(id);

    for _ in 0..64 {
        state.manual().try_run_next().await;
        if let Some(job) = state.boson().get_job(&state.job_ids[0]).await? {
            if job.status == JobStatus::Success {
                return Ok(None);
            }
            if job.status == JobStatus::Failed {
                return Ok(Some(format!(
                    "RetryBackoff: job failed after retries (fail_attempts={fail_attempts})"
                )));
            }
        }
    }
    Ok(Some("RetryBackoff: job did not reach Success".into()))
}
