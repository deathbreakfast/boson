//! Pool state and stats — the pause flag and the per-queue numbers behind the admin Queues page.

use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// How long failed jobs are kept before a host's retention sweep removes them, unless the host
/// picks another age.
///
/// Pass `Utc::now() - DEFAULT_FAILED_RETENTION` to
/// [`QueueBackend::sweep_failed`](crate::QueueBackend::sweep_failed).
pub const DEFAULT_FAILED_RETENTION: Duration = Duration::days(14);

/// Stored pause flag for one pool, with who changed it and when.
///
/// Written by [`QueueBackend::set_pool_paused`](crate::QueueBackend::set_pool_paused). Workers
/// don't claim jobs from a paused pool; jobs already running finish, and new jobs can still be
/// enqueued into it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolState {
    /// Pool name.
    pub pool: String,
    /// Whether claims from this pool are paused.
    pub paused: bool,
    /// Label of the actor that last changed the flag, as the caller passed it.
    pub changed_by: Option<String>,
    /// When the flag was last changed.
    pub changed_at: DateTime<Utc>,
}

/// Numbers for one pool, from [`QueueBackend::pool_stats`](crate::QueueBackend::pool_stats).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PoolStats {
    /// Pool name.
    pub pool: String,
    /// Jobs in `queued`.
    pub waiting: u64,
    /// Jobs in `running`.
    pub running: u64,
    /// `created_at` of the oldest queued job, if any are waiting.
    pub oldest_waiting_at: Option<DateTime<Utc>>,
    /// Jobs in a finished status (`success`, `failed`, `canceled`) with a run that finished
    /// inside the window.
    pub done_in_window: u64,
    /// Jobs created inside the window.
    pub added_in_window: u64,
    /// Whether the pool is paused.
    pub paused: bool,
}

impl PoolStats {
    /// Empty stats for a pool with nothing in it.
    #[must_use]
    pub fn empty(pool: &str) -> Self {
        Self {
            pool: pool.to_string(),
            waiting: 0,
            running: 0,
            oldest_waiting_at: None,
            done_in_window: 0,
            added_in_window: 0,
            paused: false,
        }
    }
}
