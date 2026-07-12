//! Shared run finish logic for worker backends.

use std::time::Duration;

use async_trait::async_trait;
use boson_core::{BosonError, Job, JobStatus, Result, RunStatus, TaskConfig, RetryPolicy};
use tokio::time::sleep;

use crate::telemetry;

/// Persistence and retry operations used by [`finish_job_execution`].
#[async_trait]
pub trait RunLifecycleHost: Send + Sync {
    async fn record_run_finish(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()>;

    async fn put_job(&self, job: Job);

    async fn load_task_config(&self, task_name: &str) -> Result<TaskConfig>;

    async fn schedule_retry(&self, job: Job, delay_ms: u64);
}

pub async fn finish_job_execution<H: RunLifecycleHost>(
    host: &H,
    run_id: String,
    job: Job,
    result: std::result::Result<(), BosonError>,
    duration_ms: i64,
) {
    match result {
        Ok(()) => {
            let _ = host
                .record_run_finish(&run_id, RunStatus::Success, Some(duration_ms), None)
                .await;
            telemetry::record_task_completed(&job.task_name, &job.job_id, &run_id, duration_ms);
            let mut finished = job;
            finished.status = JobStatus::Success;
            host.put_job(finished).await;
        }
        Err(e) => {
            let err_msg = e.to_string();
            let config = host.load_task_config(&job.task_name).await.ok();
            let retry_policy = config.as_ref().map(|c| &c.retry_policy);
            let should_retry = retry_policy
                .is_some_and(|r| job.attempt.cast_unsigned() < r.max_attempts);
            if should_retry {
                let policy = retry_policy.unwrap();
                let delay_ms = compute_retry_delay_ms(policy, job.attempt);
                let _ = host
                    .record_run_finish(
                        &run_id,
                        RunStatus::Failed,
                        Some(duration_ms),
                        Some(err_msg.clone()),
                    )
                    .await;
                telemetry::record_task_failed(
                    &job.task_name,
                    &job.job_id,
                    &run_id,
                    &err_msg,
                    true,
                );
                host.schedule_retry(job, delay_ms).await;
            } else {
                let _ = host
                    .record_run_finish(
                        &run_id,
                        RunStatus::Failed,
                        Some(duration_ms),
                        Some(err_msg.clone()),
                    )
                    .await;
                telemetry::record_task_failed(
                    &job.task_name,
                    &job.job_id,
                    &run_id,
                    &err_msg,
                    false,
                );
                let mut failed = job;
                failed.status = JobStatus::Failed;
                host.put_job(failed).await;
            }
        }
    }
}

pub async fn sleep_retry_delay(delay_ms: u64) {
    sleep(Duration::from_millis(delay_ms)).await;
}

fn ms_u64_to_f64(ms: u64) -> f64 {
    f64::from(u32::try_from(ms.min(u64::from(u32::MAX))).unwrap_or(u32::MAX))
}

fn round_f64_to_u64_at_most(max: u64, value: f64) -> u64 {
    if value <= 0.0 || !value.is_finite() {
        return 0;
    }
    let max = max.min(u64::from(u32::MAX));
    if value >= f64::from(u32::try_from(max).unwrap_or(u32::MAX)) {
        return max;
    }
    let target = value.round();
    let mut lo = 0u64;
    let mut hi = max;
    while lo < hi {
        let mid = lo + (hi - lo).div_ceil(2);
        if f64::from(u32::try_from(mid).unwrap_or(u32::MAX)) <= target {
            lo = mid;
        } else {
            hi = mid - 1;
        }
    }
    lo
}

fn compute_retry_delay_ms(policy: &RetryPolicy, attempt: i32) -> u64 {
    let exponent = i32::max(attempt - 1, 0);
    let scaled =
        ms_u64_to_f64(policy.base_delay_ms) * policy.backoff_multiplier.powi(exponent);
    round_f64_to_u64_at_most(policy.max_delay_ms, scaled)
}
