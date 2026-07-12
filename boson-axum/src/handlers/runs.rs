//! Run HTTP handlers.

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use super::response::ApiResponse;
use crate::state::BosonState;

/// Run summary for list and detail responses.
#[derive(Debug, Serialize, Deserialize)]
pub struct RunResponse {
    /// Unique run id (one per execution attempt).
    pub run_id: String,
    /// Parent job id.
    pub job_id: String,
    /// Task name copied from the job.
    pub task_name: String,
    /// Run status: `running`, `success`, or `failed`.
    pub status: String,
    /// 1-based attempt number on the parent job.
    pub attempt: i32,
    /// ISO 8601 start timestamp.
    pub started_at: String,
    /// ISO 8601 finish timestamp when terminal.
    pub finished_at: Option<String>,
    /// Handler duration in milliseconds when finished.
    pub duration_ms: Option<i64>,
}

impl From<boson_core::Run> for RunResponse {
    fn from(r: boson_core::Run) -> Self {
        Self {
            run_id: r.run_id,
            job_id: r.job_id,
            task_name: r.task_name,
            status: r.status.to_string(),
            attempt: r.attempt,
            started_at: r.started_at.to_rfc3339(),
            finished_at: r.finished_at.map(|t| t.to_rfc3339()),
            duration_ms: r.duration_ms,
        }
    }
}

/// Query parameters for `GET /runs`.
#[derive(Debug, Default, Deserialize)]
pub struct ListRunsQuery {
    /// Filter runs by parent job id.
    pub job_id: Option<String>,
    /// Max rows to return (default `100`).
    pub limit: Option<usize>,
}

/// `GET /runs` — list run history. Returns `200` with runs or `500` on backend error.
pub async fn list_runs(
    State(state): State<BosonState>,
    Query(q): Query<ListRunsQuery>,
) -> Json<ApiResponse<Vec<RunResponse>>> {
    let limit = q.limit.unwrap_or(100);
    match state
        .boson
        .list_runs(q.job_id.as_deref(), 0, limit)
        .await
    {
        Ok(runs) => Json(ApiResponse::ok(runs.into_iter().map(RunResponse::from).collect())),
        Err(e) => Json(ApiResponse::err(e.to_string())),
    }
}

/// `GET /runs/:id` — load one run. Returns `200`, `404` when missing, or `500` on error.
pub async fn get_run(
    State(state): State<BosonState>,
    Path(id): Path<String>,
) -> (StatusCode, Json<ApiResponse<RunResponse>>) {
    match state.boson.get_run(&id).await {
        Ok(Some(r)) => (StatusCode::OK, Json(ApiResponse::ok(r.into()))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::err(format!("Run '{id}' not found"))),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::err(e.to_string())),
        ),
    }
}
