//! Job model — an enqueued unit of work.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Status of a job in the queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobStatus {
    /// Waiting in queue.
    #[default]
    Queued,
    /// Currently executing.
    Running,
    /// Completed successfully.
    Success,
    /// Failed (may retry).
    Failed,
    /// Canceled by user.
    Canceled,
}

impl std::fmt::Display for JobStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Canceled => write!(f, "canceled"),
        }
    }
}

/// An enqueued unit of work (one task invocation).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Job {
    /// Unique identifier (UUID).
    pub job_id: String,
    /// Task name (must exist in registry).
    pub task_name: String,
    /// Captured actor (identity) at enqueue time.
    pub actor_json: Value,
    /// Serialized task parameters.
    pub params_json: Value,
    /// Priority (lower = higher priority). From task config.
    pub priority: i32,
    /// Pool name for worker assignment.
    pub pool: String,
    /// Current status.
    pub status: JobStatus,
    /// Optional idempotency key.
    pub idempotency_key: Option<String>,
    /// When the job was enqueued.
    pub created_at: DateTime<Utc>,
    /// Signature hash at enqueue (for validation).
    pub signature_hash: u64,
    /// Attempt number (1-based); incremented on retry.
    pub attempt: i32,
}

impl Job {
    /// Create a new queued job (typically from enqueue).
    #[must_use]
    pub fn new(
        task_name: &str,
        actor_json: Value,
        params_json: Value,
        priority: i32,
        pool: &str,
        signature_hash: u64,
        idempotency_key: Option<String>,
    ) -> Self {
        Self {
            job_id: uuid::Uuid::new_v4().to_string(),
            task_name: task_name.to_string(),
            actor_json,
            params_json,
            priority,
            pool: pool.to_string(),
            status: JobStatus::Queued,
            idempotency_key,
            created_at: Utc::now(),
            signature_hash,
            attempt: 1,
        }
    }
}
