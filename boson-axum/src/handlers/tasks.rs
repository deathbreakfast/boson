//! Task HTTP handlers.

use axum::{
    extract::{Path, State},
    http::StatusCode,
    Json,
};

use super::response::ApiResponse;
use crate::state::BosonState;

/// Registered task metadata from the worker registry.
#[derive(Debug, serde::Serialize)]
pub struct TaskResponse {
    /// Task name (matches `#[task(name = "...")]`).
    pub name: String,
    /// JSON schema of handler parameters.
    pub signature_json: String,
    /// Hash of the signature for change detection.
    pub signature_hash: u64,
    /// Default enqueue priority from the task descriptor.
    pub default_priority: i32,
    /// Default worker pool from the task descriptor.
    pub default_pool: String,
}

/// `GET /tasks` — list all registered tasks in the worker process.
pub async fn list_tasks(State(state): State<BosonState>) -> Json<ApiResponse<Vec<TaskResponse>>> {
    let list: Vec<TaskResponse> = state
        .boson
        .registry()
        .iter()
        .map(|d| TaskResponse {
            name: d.name.to_string(),
            signature_json: d.signature_json.to_string(),
            signature_hash: d.signature_hash,
            default_priority: d.default_priority,
            default_pool: d.default_pool.to_string(),
        })
        .collect();
    Json(ApiResponse::ok(list))
}

/// `GET /tasks/:name` — load one task descriptor. Returns `200` or `404` when not registered.
pub async fn get_task(
    State(state): State<BosonState>,
    Path(name): Path<String>,
) -> (StatusCode, Json<ApiResponse<TaskResponse>>) {
    state.boson.registry().get(&name).map_or_else(
        || {
            (
                StatusCode::NOT_FOUND,
                Json(ApiResponse::err(format!("Task '{name}' not found"))),
            )
        },
        |d| {
            (
                StatusCode::OK,
                Json(ApiResponse::ok(TaskResponse {
                    name: d.name.to_string(),
                    signature_json: d.signature_json.to_string(),
                    signature_hash: d.signature_hash,
                    default_priority: d.default_priority,
                    default_pool: d.default_pool.to_string(),
                })),
            )
        },
    )
}
