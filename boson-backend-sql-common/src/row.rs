//! Row mapping between SQL and boson-core DTOs.

use boson_core::{IdempotencyMode, Job, JobStatus, RateLimitPolicy, Run, RunStatus, TaskConfig};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sqlx::{ColumnIndex, Row};

use crate::error_map::map_err;

/// Rate-limit JSON plus optional idempotency (backward compatible with older rows).
#[derive(Debug, Serialize, Deserialize)]
struct StoredRateLimit {
    #[serde(flatten)]
    policy: RateLimitPolicy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    idempotency_mode: Option<IdempotencyMode>,
}

/// Serialize [`JobStatus`] to the lowercase string stored in SQL.
pub const fn job_status_to_str(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Queued => "queued",
        JobStatus::Running => "running",
        JobStatus::Success => "success",
        JobStatus::Failed => "failed",
        JobStatus::Canceled => "canceled",
    }
}

/// Parse a SQL job status string into [`JobStatus`].
pub fn parse_job_status(s: &str) -> boson_core::Result<JobStatus> {
    match s {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "success" => Ok(JobStatus::Success),
        "failed" => Ok(JobStatus::Failed),
        "canceled" => Ok(JobStatus::Canceled),
        other => Err(boson_core::BosonError::Backend(format!(
            "unknown job status: {other}"
        ))),
    }
}

/// Serialize [`RunStatus`] to the lowercase string stored in SQL.
pub const fn run_status_to_str(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Running => "running",
        RunStatus::Success => "success",
        RunStatus::Failed => "failed",
        RunStatus::Canceled => "canceled",
        RunStatus::Timeout => "timeout",
    }
}

/// Parse a SQL run status string into [`RunStatus`].
pub fn parse_run_status(s: &str) -> boson_core::Result<RunStatus> {
    match s {
        "running" => Ok(RunStatus::Running),
        "success" => Ok(RunStatus::Success),
        "failed" => Ok(RunStatus::Failed),
        "canceled" => Ok(RunStatus::Canceled),
        "timeout" => Ok(RunStatus::Timeout),
        other => Err(boson_core::BosonError::Backend(format!(
            "unknown run status: {other}"
        ))),
    }
}

/// Map a SQL row to a [`Job`].
pub fn row_to_job<'r, R>(row: &'r R) -> boson_core::Result<Job>
where
    R: Row,
    for<'i> &'i str: ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    DateTime<Utc>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let actor_json: String = row.try_get("actor_json").map_err(|e| map_err(&e))?;
    let params_json: String = row.try_get("params_json").map_err(|e| map_err(&e))?;
    let status: String = row.try_get("status").map_err(|e| map_err(&e))?;
    Ok(Job {
        job_id: row.try_get("job_id").map_err(|e| map_err(&e))?,
        task_name: row.try_get("task_name").map_err(|e| map_err(&e))?,
        actor_json: serde_json::from_str(&actor_json)?,
        params_json: serde_json::from_str(&params_json)?,
        priority: row.try_get("priority").map_err(|e| map_err(&e))?,
        pool: row.try_get("pool").map_err(|e| map_err(&e))?,
        status: parse_job_status(&status)?,
        idempotency_key: row.try_get("idempotency_key").map_err(|e| map_err(&e))?,
        created_at: row.try_get("created_at").map_err(|e| map_err(&e))?,
        signature_hash: u64::try_from(
            row.try_get::<i64, _>("signature_hash").map_err(|e| map_err(&e))?,
        )
        .unwrap_or(0),
        attempt: row.try_get("attempt").map_err(|e| map_err(&e))?,
    })
}

/// Map a SQL row to a [`Run`].
pub fn row_to_run<'r, R>(row: &'r R) -> boson_core::Result<Run>
where
    R: Row,
    for<'i> &'i str: ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    DateTime<Utc>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    Option<DateTime<Utc>>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    Option<i64>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    Option<String>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let status: String = row.try_get("status").map_err(|e| map_err(&e))?;
    Ok(Run {
        run_id: row.try_get("run_id").map_err(|e| map_err(&e))?,
        job_id: row.try_get("job_id").map_err(|e| map_err(&e))?,
        task_name: row.try_get("task_name").map_err(|e| map_err(&e))?,
        attempt: row.try_get("attempt").map_err(|e| map_err(&e))?,
        status: parse_run_status(&status)?,
        started_at: row.try_get("started_at").map_err(|e| map_err(&e))?,
        finished_at: row.try_get("finished_at").map_err(|e| map_err(&e))?,
        duration_ms: row.try_get("duration_ms").map_err(|e| map_err(&e))?,
        error_message: row.try_get("error_message").map_err(|e| map_err(&e))?,
    })
}

/// Map a SQL row to a [`TaskConfig`].
pub fn row_to_task_config<'r, R>(row: &'r R) -> boson_core::Result<TaskConfig>
where
    R: Row,
    for<'i> &'i str: ColumnIndex<R>,
    String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    DateTime<Utc>: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    let retry_json: String = row.try_get("retry_policy_json").map_err(|e| map_err(&e))?;
    let rate_json: String = row.try_get("rate_limit_policy_json").map_err(|e| map_err(&e))?;
    let stored: StoredRateLimit = serde_json::from_str(&rate_json)?;
    Ok(TaskConfig {
        task_name: row.try_get("task_name").map_err(|e| map_err(&e))?,
        priority: row.try_get("priority").map_err(|e| map_err(&e))?,
        pool: row.try_get("pool").map_err(|e| map_err(&e))?,
        retry_policy: serde_json::from_str(&retry_json)?,
        rate_limit_policy: stored.policy,
        idempotency_mode: stored.idempotency_mode,
        updated_at: row.try_get("updated_at").map_err(|e| map_err(&e))?,
    })
}

/// JSON-encode job `actor_json` and `params_json` for SQL bind parameters.
pub fn job_to_binds(job: &Job) -> boson_core::Result<(String, String)> {
    Ok((
        serde_json::to_string(&job.actor_json)?,
        serde_json::to_string(&job.params_json)?,
    ))
}

/// JSON-encode task config policies for SQL bind parameters.
pub fn task_config_to_binds(config: &TaskConfig) -> boson_core::Result<(String, String)> {
    let rate = StoredRateLimit {
        policy: config.rate_limit_policy,
        idempotency_mode: config.idempotency_mode,
    };
    Ok((
        serde_json::to_string(&config.retry_policy)?,
        serde_json::to_string(&rate)?,
    ))
}
