//! Task config HTTP handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};
use boson_core::TaskConfig;
use chrono::Utc;
use serde::{Deserialize, Serialize};

use super::response::ApiResponse;
use crate::state::BosonState;

/// Task config returned by the HTTP admin API.
///
/// Mirrors persisted [`TaskConfig`](boson_core::TaskConfig) fields exposed over HTTP.
/// **`idempotency_mode` is not exposed** via this API — set it on the `#[task]` macro or
/// via [`BosonBuilder::idempotency_mode`](boson_runtime::BosonBuilder::idempotency_mode) at boot.
#[derive(Debug, Serialize, Deserialize)]
pub struct TaskConfigResponse {
    /// Task name (unique key).
    pub task_name: String,
    /// Enqueue priority override (lower = higher priority).
    pub priority: i32,
    /// Worker pool override for claim routing.
    pub pool: String,
    /// Retry policy override (max attempts, backoff).
    pub retry_policy: boson_core::RetryPolicy,
    /// Enqueue rate limit override (in-flight and per-second caps).
    pub rate_limit_policy: boson_core::RateLimitPolicy,
    /// ISO 8601 last-update timestamp.
    pub updated_at: String,
}

impl From<TaskConfig> for TaskConfigResponse {
    fn from(c: TaskConfig) -> Self {
        Self {
            task_name: c.task_name,
            priority: c.priority,
            pool: c.pool,
            retry_policy: c.retry_policy,
            rate_limit_policy: c.rate_limit_policy,
            updated_at: c.updated_at.to_rfc3339(),
        }
    }
}

/// Partial task config update (unset fields are left unchanged).
///
/// **`idempotency_mode` cannot be updated via HTTP** — use the `#[task]` attribute or runtime
/// builder instead. See [`TaskConfigResponse`] for the full list of HTTP-exposed fields.
#[derive(Debug, Deserialize, Serialize)]
pub struct UpdateTaskConfigRequest {
    pub priority: Option<i32>,
    pub pool: Option<String>,
    pub retry_policy: Option<boson_core::RetryPolicy>,
    pub rate_limit_policy: Option<boson_core::RateLimitPolicy>,
}

/// `GET /tasks/:name/config` — load persisted [`TaskConfig`](boson_core::TaskConfig).
pub async fn get_task_config(
    State(state): State<BosonState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<ApiResponse<TaskConfigResponse>>) {
    match state.boson.get_task_config(&name).await {
        Ok(c) => (StatusCode::OK, Json(ApiResponse::ok(c.into()))),
        Err(e) => (StatusCode::NOT_FOUND, Json(ApiResponse::err(e.to_string()))),
    }
}

/// `POST /tasks/:name/config` — merge partial update into task config.
pub async fn update_task_config(
    State(state): State<BosonState>,
    Path(name): Path<String>,
    Json(req): Json<UpdateTaskConfigRequest>,
) -> (StatusCode, Json<ApiResponse<TaskConfigResponse>>) {
    let mut config = match state.boson.get_task_config(&name).await {
        Ok(c) => c,
        Err(e) => {
            return (StatusCode::NOT_FOUND, Json(ApiResponse::err(e.to_string())));
        }
    };
    if let Some(p) = req.priority {
        config.priority = p;
    }
    if let Some(p) = req.pool {
        config.pool = p;
    }
    if let Some(r) = req.retry_policy {
        config.retry_policy = r;
    }
    if let Some(r) = req.rate_limit_policy {
        config.rate_limit_policy = r;
    }
    config.updated_at = Utc::now();
    match state.boson.upsert_task_config(config.clone()).await {
        Ok(()) => (StatusCode::OK, Json(ApiResponse::ok(config.into()))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::err(e.to_string())),
        ),
    }
}

/// `GET /tasks/:name/config/revisions` — **stub**: always returns an empty list.
///
/// Revision history is not persisted yet. Use [`get_task_config`] for the current config.
/// This endpoint is reserved for a future audit trail; do not rely on it for production workflows.
pub async fn get_task_config_revisions(
    State(_state): State<BosonState>,
    Path(_name): Path<String>,
) -> Json<ApiResponse<Vec<serde_json::Value>>> {
    Json(ApiResponse::ok(vec![]))
}
