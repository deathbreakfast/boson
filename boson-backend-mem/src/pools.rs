//! Pool stats and pause state for the in-memory backend.

use boson_core::{PoolState, PoolStats};
use chrono::{Duration, Utc};

use crate::store::Inner;

/// Stats for every pool with jobs, a task config naming it, or a stored pause state.
pub fn pool_stats(inner: &Inner, window: Duration) -> Vec<PoolStats> {
    let mut stats = boson_core::tally_pool_stats(
        inner.jobs.values(),
        inner.runs.values(),
        Utc::now() - window,
    );
    let named = inner
        .task_configs
        .values()
        .map(|c| c.pool.as_str())
        .chain(inner.pool_states.keys().map(String::as_str));
    for pool in named {
        stats
            .entry(pool.to_string())
            .or_insert_with(|| PoolStats::empty(pool));
    }
    for entry in stats.values_mut() {
        entry.paused = inner.pool_paused(&entry.pool);
    }
    stats.into_values().collect()
}

/// Store the pause flag for a pool.
pub fn set_pool_paused(
    inner: &mut Inner,
    pool: &str,
    paused: bool,
    actor_label: &str,
) -> PoolState {
    let state = PoolState {
        pool: pool.to_string(),
        paused,
        changed_by: (!actor_label.is_empty()).then(|| actor_label.to_string()),
        changed_at: Utc::now(),
    };
    inner.pool_states.insert(pool.to_string(), state.clone());
    state
}

/// Stored pause state for a pool.
pub fn get_pool_state(inner: &Inner, pool: &str) -> Option<PoolState> {
    inner.pool_states.get(pool).cloned()
}
