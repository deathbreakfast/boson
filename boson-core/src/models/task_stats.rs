//! Aggregate run statistics for a registered task.

use serde::{Deserialize, Serialize};

/// Run outcome counts for one task name.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskRunStats {
    /// Total runs recorded.
    pub runs_total: u32,
    /// Successful runs.
    pub success_count: u32,
}
