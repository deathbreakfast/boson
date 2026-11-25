//! Shared tally for [`QueueBackend::pool_stats`](crate::QueueBackend::pool_stats) over jobs and
//! runs held in memory.

use std::collections::{BTreeMap, HashSet};

use chrono::{DateTime, Utc};

use crate::models::{Job, JobStatus, PoolStats, Run};

/// Count waiting, running, done and added jobs per pool, keyed by pool name.
///
/// A job counts as done when it is `success`, `failed` or `canceled` and one of its runs
/// finished at or after `since`; it counts as added when it was created at or after `since`.
/// `paused` is left `false` for the caller to fill in. Only pools with at least one job appear.
#[must_use]
pub fn tally_pool_stats<'a>(
    jobs: impl IntoIterator<Item = &'a Job>,
    runs: impl IntoIterator<Item = &'a Run>,
    since: DateTime<Utc>,
) -> BTreeMap<String, PoolStats> {
    let finished: HashSet<&str> = runs
        .into_iter()
        .filter(|r| r.finished_at.is_some_and(|t| t >= since))
        .map(|r| r.job_id.as_str())
        .collect();
    let mut stats: BTreeMap<String, PoolStats> = BTreeMap::new();
    for job in jobs {
        let entry = stats
            .entry(job.pool.clone())
            .or_insert_with(|| PoolStats::empty(&job.pool));
        match job.status {
            JobStatus::Queued => {
                entry.waiting += 1;
                entry.oldest_waiting_at = Some(
                    entry
                        .oldest_waiting_at
                        .map_or(job.created_at, |t| t.min(job.created_at)),
                );
            }
            JobStatus::Running => entry.running += 1,
            JobStatus::Success | JobStatus::Failed | JobStatus::Canceled => {
                if finished.contains(job.job_id.as_str()) {
                    entry.done_in_window += 1;
                }
            }
        }
        if job.created_at >= since {
            entry.added_in_window += 1;
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Duration;
    use serde_json::json;

    #[test]
    fn tallies_per_pool() {
        let now = Utc::now();
        let mk = |pool: &str, status, age_secs| {
            let mut j = Job::new("t", json!({}), json!({}), 0, pool, 0, None);
            j.status = status;
            j.created_at = now - Duration::seconds(age_secs);
            j
        };
        let jobs = vec![
            mk("a", JobStatus::Queued, 10),
            mk("a", JobStatus::Queued, 5000),
            mk("a", JobStatus::Running, 10),
            mk("a", JobStatus::Success, 5000),
            mk("b", JobStatus::Failed, 10),
        ];
        let mut done = Run::new(&jobs[3].job_id, "t", 1);
        done.finished_at = Some(now);
        let mut old = Run::new(&jobs[4].job_id, "t", 1);
        old.finished_at = Some(now - Duration::seconds(7200));
        let stats = tally_pool_stats(&jobs, &[done, old], now - Duration::hours(1));
        let a = &stats["a"];
        assert_eq!(
            (a.waiting, a.running, a.done_in_window, a.added_in_window),
            (2, 1, 1, 2)
        );
        assert_eq!(a.oldest_waiting_at, Some(jobs[1].created_at));
        let b = &stats["b"];
        assert_eq!((b.done_in_window, b.added_in_window), (0, 1));
    }
}
