//! Job HTTP handlers.
//!
//! Enqueue uses a fixed system actor JSON (not caller-supplied):
//! `{"System": {"operation": "boson_api_enqueue"}}`. Map HTTP auth to your app's
//! [`ExecutionContext`](boson_core::ExecutionContext) in a custom route if needed.
//!
//! See [`Boson::enqueue`](boson_runtime::Boson::enqueue) for programmatic enqueue.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use boson_core::{BosonError, JobStatus};
use serde::{Deserialize, Serialize};

use super::response::ApiResponse;
use crate::state::BosonState;

/// Enqueue request body for `POST /jobs/enqueue`.
///
/// Maps to [`Boson::enqueue`](boson_runtime::Boson::enqueue) with a fixed system actor JSON.
#[derive(Debug, Deserialize, Serialize)]
pub struct EnqueueRequest {
    /// Registered task name (must match a [`TaskDescriptor`](boson_runtime::TaskDescriptor) in the worker registry).
    pub task_name: String,
    /// JSON task parameters passed to the handler (default `{}`).
    #[serde(default)]
    pub params: serde_json::Value,
    /// Optional idempotency key — duplicate keys return the existing non-terminal job id.
    pub idempotency_key: Option<String>,
}

/// Enqueue response body.
#[derive(Debug, Serialize, Deserialize)]
pub struct EnqueueResponse {
    /// Assigned job id (new or reused when idempotency key matches).
    pub job_id: String,
}

/// Job summary for list and detail responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct JobResponse {
    /// Unique job id.
    pub job_id: String,
    /// Task that will execute this job.
    pub task_name: String,
    /// Current status: `queued`, `running`, `success`, `failed`, or `canceled`.
    pub status: String,
    /// Job priority (lower = higher priority).
    pub priority: i32,
    /// Worker pool name for claim routing.
    pub pool: String,
    /// ISO 8601 creation timestamp.
    pub created_at: String,
}

impl From<boson_core::Job> for JobResponse {
    fn from(j: boson_core::Job) -> Self {
        Self {
            job_id: j.job_id,
            task_name: j.task_name,
            status: j.status.to_string(),
            priority: j.priority,
            pool: j.pool,
            created_at: j.created_at.to_rfc3339(),
        }
    }
}

/// Query parameters for `GET /jobs`.
#[derive(Debug, Default, Deserialize)]
pub struct ListJobsQuery {
    /// Filter by status: `queued`, `running`, `success`, `failed`, or `canceled`.
    pub status: Option<String>,
    /// Max rows to return (default `100`).
    pub limit: Option<usize>,
}

fn parse_job_status(s: &str) -> Option<JobStatus> {
    match s {
        "queued" => Some(JobStatus::Queued),
        "running" => Some(JobStatus::Running),
        "success" => Some(JobStatus::Success),
        "failed" => Some(JobStatus::Failed),
        "canceled" => Some(JobStatus::Canceled),
        _ => None,
    }
}

/// `POST /jobs/enqueue` — enqueue a background job.
///
/// Uses actor `{"System": {"operation": "boson_api_enqueue"}}`. Returns `429` when
/// [`BosonError::RateLimited`](boson_core::BosonError::RateLimited).
pub async fn enqueue(
    State(state): State<BosonState>,
    Json(req): Json<EnqueueRequest>,
) -> (StatusCode, Json<ApiResponse<EnqueueResponse>>) {
    let actor_json = serde_json::json!({"System": {"operation": "boson_api_enqueue"}});
    match state
        .boson
        .enqueue(&req.task_name, actor_json, req.params, req.idempotency_key)
        .await
    {
        Ok(job_id) => (
            StatusCode::OK,
            Json(ApiResponse::ok(EnqueueResponse { job_id })),
        ),
        Err(BosonError::RateLimited(_)) => (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse::<EnqueueResponse>::err(
                "enqueue rate limited; retry with backoff",
            )),
        ),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::<EnqueueResponse>::err(e.to_string())),
        ),
    }
}

/// `GET /jobs` — list jobs with optional [`ListJobsQuery::status`] filter.
pub async fn list_jobs(
    State(state): State<BosonState>,
    Query(q): Query<ListJobsQuery>,
) -> Json<ApiResponse<Vec<JobResponse>>> {
    let status = q.status.as_deref().and_then(parse_job_status);
    let limit = q.limit.unwrap_or(100);
    match state.boson.list_jobs(status, 0, limit).await {
        Ok(jobs) => Json(ApiResponse::ok(jobs.into_iter().map(JobResponse::from).collect())),
        Err(e) => Json(ApiResponse::err(e.to_string())),
    }
}

/// `GET /jobs/:id` — fetch one job by id.
pub async fn get_job(
    State(state): State<BosonState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<ApiResponse<JobResponse>>) {
    match state.boson.get_job(&id).await {
        Ok(Some(j)) => (StatusCode::OK, Json(ApiResponse::ok(j.into()))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::err(format!("Job '{id}' not found"))),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::err(e.to_string())),
        ),
    }
}

/// `POST /jobs/:id/cancel` — cancel an active job ([`Boson::cancel_job`](boson_runtime::Boson::cancel_job)).
pub async fn cancel_job(
    State(state): State<BosonState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<ApiResponse<()>>) {
    match state.boson.cancel_job(&id).await {
        Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(()))),
        Err(e) => (StatusCode::NOT_FOUND, Json(ApiResponse::err(e.to_string()))),
    }
}
