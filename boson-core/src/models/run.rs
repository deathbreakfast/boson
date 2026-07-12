//! Run model — one execution attempt of a job.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Status of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunStatus {
    /// Run is executing.
    #[default]
    Running,
    /// Run completed successfully.
    Success,
    /// Run failed.
    Failed,
    /// Run was canceled.
    Canceled,
    /// Run timed out.
    ///
    /// Reserved for future worker timeout support; not set by the current runtime.
    Timeout,
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Failed => write!(f, "failed"),
            Self::Canceled => write!(f, "canceled"),
            Self::Timeout => write!(f, "timeout"),
        }
    }
}

/// One execution attempt of a job.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Run {
    /// Unique identifier (UUID).
    pub run_id: String,
    /// Job this run belongs to.
    pub job_id: String,
    /// Task name (denormalized).
    pub task_name: String,
    /// Attempt number (1-based).
    pub attempt: i32,
    /// Status.
    pub status: RunStatus,
    /// When execution started.
    pub started_at: DateTime<Utc>,
    /// When execution finished (if terminal).
    pub finished_at: Option<DateTime<Utc>>,
    /// Duration in milliseconds (if finished).
    pub duration_ms: Option<i64>,
    /// Error message if failed.
    pub error_message: Option<String>,
}

impl Run {
    /// Create a new running run for a job.
    #[must_use]
    pub fn new(job_id: &str, task_name: &str, attempt: i32) -> Self {
        Self {
            run_id: uuid::Uuid::new_v4().to_string(),
            job_id: job_id.to_string(),
            task_name: task_name.to_string(),
            attempt,
            status: RunStatus::Running,
            started_at: Utc::now(),
            finished_at: None,
            duration_ms: None,
            error_message: None,
        }
    }
}
