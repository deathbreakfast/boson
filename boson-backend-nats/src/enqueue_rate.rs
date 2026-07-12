//! Process-local sliding-window limiter for enqueues per task per second.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::time::Instant;

/// Tracks recent enqueue timestamps per task name (1-second sliding window).
#[derive(Debug, Default)]
pub struct EnqueueRateLimiter {
    inner: Mutex<HashMap<String, VecDeque<Instant>>>,
}

impl EnqueueRateLimiter {
    /// New empty limiter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if this enqueue is allowed under `max_per_second` for `task_name`.
    pub fn try_record(&self, task_name: &str, max_per_second: u32) -> bool {
        if max_per_second == 0 {
            return true;
        }
        let mut guard = self.inner.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let allowed = {
            let window = guard.entry(task_name.to_string()).or_default();
            while let Some(front) = window.front().copied() {
                if now.duration_since(front).as_secs() >= 1 {
                    window.pop_front();
                } else {
                    break;
                }
            }
            if window.len() >= max_per_second as usize {
                false
            } else {
                window.push_back(now);
                true
            }
        };
        drop(guard);
        allowed
    }
}
