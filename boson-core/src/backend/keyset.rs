//! Keyset cursors for [`QueueBackend::list_jobs_after`](crate::QueueBackend::list_jobs_after) and
//! [`QueueBackend::list_runs_after`](crate::QueueBackend::list_runs_after).
//!
//! A cursor is the sort key of the last row a caller has already seen. The next page starts
//! strictly after that row, so paging never skips or repeats rows the way `OFFSET` can when rows
//! are inserted or change status between pages.
//!
//! [`page_jobs_after`] and [`page_runs_after`] apply the same ordering and filters in Rust. Engines
//! without an ordered index (and the default trait bodies) use them over an in-memory collection.

use std::borrow::Borrow;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::filter::{JobFilter, RunFilter};
use crate::models::{Job, Run};

/// Position of a job in `created_at ASC, job_id ASC` order.
///
/// Build one from the last job of a page with `JobKey::from(&job)` and pass it to
/// [`QueueBackend::list_jobs_after`](crate::QueueBackend::list_jobs_after) to fetch the next page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobKey {
    /// [`Job::created_at`] of the last job seen.
    pub created_at: DateTime<Utc>,
    /// [`Job::job_id`] of the last job seen; breaks ties between equal `created_at` values.
    pub id: String,
}

impl JobKey {
    /// True when `job` sorts strictly after this key (`created_at ASC, job_id ASC`).
    #[must_use]
    pub fn precedes(&self, job: &Job) -> bool {
        (job.created_at, job.job_id.as_str()) > (self.created_at, self.id.as_str())
    }
}

impl From<&Job> for JobKey {
    fn from(job: &Job) -> Self {
        Self {
            created_at: job.created_at,
            id: job.job_id.clone(),
        }
    }
}

/// Position of a run in `started_at DESC, run_id DESC` order.
///
/// Build one from the last run of a page with `RunKey::from(&run)` and pass it to
/// [`QueueBackend::list_runs_after`](crate::QueueBackend::list_runs_after) to fetch the next page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunKey {
    /// [`Run::started_at`] of the last run seen.
    pub started_at: DateTime<Utc>,
    /// [`Run::run_id`] of the last run seen; breaks ties between equal `started_at` values.
    pub id: String,
}

impl RunKey {
    /// True when `run` sorts strictly after this key (`started_at DESC, run_id DESC`).
    #[must_use]
    pub fn precedes(&self, run: &Run) -> bool {
        (run.started_at, run.run_id.as_str()) < (self.started_at, self.id.as_str())
    }
}

impl From<&Run> for RunKey {
    fn from(run: &Run) -> Self {
        Self {
            started_at: run.started_at,
            id: run.run_id.clone(),
        }
    }
}

/// One keyset page of jobs from an unordered collection.
///
/// Keeps jobs matching `filter` that sort strictly after `after`, orders them
/// `created_at ASC, job_id ASC` (byte order on the id), and returns at most `limit`.
/// Copies of the same job (as a Redis `SCAN` can yield) collapse to one.
/// Accepts owned jobs or references and returns the same element type.
#[must_use]
pub fn page_jobs_after<J, I>(
    jobs: I,
    filter: &JobFilter,
    after: Option<&JobKey>,
    limit: usize,
) -> Vec<J>
where
    J: Borrow<Job>,
    I: IntoIterator<Item = J>,
{
    let mut page: Vec<J> = jobs
        .into_iter()
        .filter(|j| {
            let j = j.borrow();
            filter.matches(j) && after.is_none_or(|k| k.precedes(j))
        })
        .collect();
    page.sort_by(|a, b| {
        let (a, b) = (a.borrow(), b.borrow());
        (a.created_at, a.job_id.as_str()).cmp(&(b.created_at, b.job_id.as_str()))
    });
    page.dedup_by(|a, b| Borrow::<Job>::borrow(&*a).job_id == Borrow::<Job>::borrow(&*b).job_id);
    page.truncate(limit);
    page
}

/// One keyset page of runs from an unordered collection.
///
/// Keeps runs matching `filter` that sort strictly after `after`, orders them
/// `started_at DESC, run_id DESC` (byte order on the id), and returns at most `limit`.
/// Copies of the same run (as a Redis `SCAN` can yield) collapse to one.
/// Accepts owned runs or references and returns the same element type.
#[must_use]
pub fn page_runs_after<R, I>(
    runs: I,
    filter: &RunFilter,
    after: Option<&RunKey>,
    limit: usize,
) -> Vec<R>
where
    R: Borrow<Run>,
    I: IntoIterator<Item = R>,
{
    let mut page: Vec<R> = runs
        .into_iter()
        .filter(|r| {
            let r = r.borrow();
            filter.matches(r) && after.is_none_or(|k| k.precedes(r))
        })
        .collect();
    page.sort_by(|a, b| {
        let (a, b) = (a.borrow(), b.borrow());
        (b.started_at, b.run_id.as_str()).cmp(&(a.started_at, a.run_id.as_str()))
    });
    page.dedup_by(|a, b| Borrow::<Run>::borrow(&*a).run_id == Borrow::<Run>::borrow(&*b).run_id);
    page.truncate(limit);
    page
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::JobStatus;
    use chrono::{Duration, TimeZone};
    use serde_json::json;

    fn base() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap()
    }

    fn job(i: usize, status: JobStatus) -> Job {
        let mut j = Job::new("t", json!({}), json!({}), 0, "global", 0, None);
        j.job_id = format!("job-{i:06}");
        // Groups of three share a timestamp so the id tie-break is exercised.
        j.created_at = base() + Duration::milliseconds(i64::try_from(i / 3).unwrap());
        j.status = status;
        j
    }

    fn run(i: usize, job_id: &str) -> Run {
        let mut r = Run::new(job_id, "t", 1);
        r.run_id = format!("run-{i:06}");
        r.started_at = base() + Duration::milliseconds(i64::try_from(i / 3).unwrap());
        r
    }

    #[test]
    fn jobs_page_in_order_with_filter_and_ties() {
        let statuses = [JobStatus::Queued, JobStatus::Success, JobStatus::Failed];
        // Insert in reverse so input order does not match output order.
        let all: Vec<Job> = (0..300).rev().map(|i| job(i, statuses[i % 3])).collect();
        let mut seen = Vec::new();
        let mut cursor: Option<JobKey> = None;
        loop {
            let page = page_jobs_after(
                all.iter(),
                &JobFilter::status(JobStatus::Success),
                cursor.as_ref(),
                7,
            );
            let Some(last) = page.last() else { break };
            cursor = Some(JobKey::from(*last));
            seen.extend(page.iter().map(|j| j.job_id.clone()));
        }
        let expected: Vec<String> = (0..300_usize)
            .filter(|i| i % 3 == 1)
            .map(|i| format!("job-{i:06}"))
            .collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn jobs_tie_break_is_strict() {
        let a = job(0, JobStatus::Queued);
        let b = job(1, JobStatus::Queued);
        assert_eq!(a.created_at, b.created_at);
        let key = JobKey::from(&a);
        assert!(!key.precedes(&a));
        assert!(key.precedes(&b));
        assert!(!JobKey::from(&b).precedes(&a));
    }

    #[test]
    fn runs_page_descending_with_filter_and_ties() {
        let all: Vec<Run> = (0..300)
            .map(|i| {
                run(
                    i,
                    if i.is_multiple_of(2) {
                        "job-a"
                    } else {
                        "job-b"
                    },
                )
            })
            .collect();
        let mut seen = Vec::new();
        let mut cursor: Option<RunKey> = None;
        loop {
            let page = page_runs_after(all.clone(), &RunFilter::job("job-a"), cursor.as_ref(), 11);
            let Some(last) = page.last() else { break };
            cursor = Some(RunKey::from(last));
            seen.extend(page.into_iter().map(|r| r.run_id));
        }
        let expected: Vec<String> = (0..300_usize)
            .rev()
            .filter(|i| i.is_multiple_of(2))
            .map(|i| format!("run-{i:06}"))
            .collect();
        assert_eq!(seen, expected);
    }

    #[test]
    fn duplicate_rows_collapse() {
        let all = vec![
            job(0, JobStatus::Queued),
            job(0, JobStatus::Queued),
            job(1, JobStatus::Queued),
        ];
        let page = page_jobs_after(all, &JobFilter::default(), None, 10);
        assert_eq!(page.len(), 2);
    }

    #[test]
    fn zero_limit_is_empty() {
        let all = vec![job(0, JobStatus::Queued)];
        assert!(page_jobs_after(all, &JobFilter::default(), None, 0).is_empty());
    }

    #[test]
    fn jobs_page_with_suffix_match_and_dropped() {
        let mut all: Vec<Job> = (0..30).map(|i| job(i, JobStatus::Failed)).collect();
        all[1].dropped_at = Some(base());
        // ids ending in "1": job-000001, job-000011, job-000021; job-000001 is dropped.
        let filter = JobFilter {
            id_match: Some("1".into()),
            ..JobFilter::default()
        };
        let page = page_jobs_after(all.iter(), &filter, None, 10);
        let ids: Vec<&str> = page.iter().map(|j| j.job_id.as_str()).collect();
        assert_eq!(ids, ["job-000011", "job-000021"]);
        let filter = JobFilter {
            include_dropped: true,
            ..filter
        };
        assert_eq!(page_jobs_after(all.iter(), &filter, None, 10).len(), 3);
    }

    #[test]
    fn keys_round_trip_through_json() {
        let key = JobKey::from(&job(4, JobStatus::Queued));
        let text = serde_json::to_string(&key).unwrap();
        assert_eq!(serde_json::from_str::<JobKey>(&text).unwrap(), key);
        let key = RunKey::from(&run(4, "job-a"));
        let text = serde_json::to_string(&key).unwrap();
        assert_eq!(serde_json::from_str::<RunKey>(&text).unwrap(), key);
    }
}
