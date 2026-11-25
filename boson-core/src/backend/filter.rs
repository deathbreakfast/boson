//! Filters for [`QueueBackend::list_jobs_after`](crate::QueueBackend::list_jobs_after),
//! [`QueueBackend::list_runs_after`](crate::QueueBackend::list_runs_after) and their count
//! methods.
//!
//! Every field narrows the scan; a default filter matches everything except dropped jobs.
//! Time ranges are half open: `*_from` is inclusive and `*_to` exclusive. `id_match` matches the
//! whole id or its trailing characters, so a short id copied from a table finds its row.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::models::{Job, JobStatus, Run, RunStatus};

/// Which jobs a jobs scan or count covers.
///
/// # Examples
///
/// ```
/// use boson_core::{JobFilter, JobStatus};
///
/// // Failed `invoice_sync` jobs whose id ends in `3f9a`.
/// let filter = JobFilter {
///     statuses: vec![JobStatus::Failed],
///     task: Some("invoice_sync".into()),
///     id_match: Some("3f9a".into()),
///     ..JobFilter::default()
/// };
/// # let _ = filter;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobFilter {
    /// Statuses to keep; empty keeps every status.
    #[serde(default)]
    pub statuses: Vec<JobStatus>,
    /// Exact task name.
    #[serde(default)]
    pub task: Option<String>,
    /// Exact pool name.
    #[serde(default)]
    pub pool: Option<String>,
    /// Keep jobs with `created_at >= created_from`.
    #[serde(default)]
    pub created_from: Option<DateTime<Utc>>,
    /// Keep jobs with `created_at < created_to`.
    #[serde(default)]
    pub created_to: Option<DateTime<Utc>>,
    /// Whole job id or its trailing characters. Case-sensitive; empty is ignored.
    #[serde(default)]
    pub id_match: Option<String>,
    /// Also keep jobs with [`Job::dropped_at`] set. `false` by default, so dropped jobs stay out
    /// of the lists unless a caller asks for them.
    #[serde(default)]
    pub include_dropped: bool,
}

impl JobFilter {
    /// Filter on a single status.
    #[must_use]
    pub fn status(status: JobStatus) -> Self {
        Self {
            statuses: vec![status],
            ..Self::default()
        }
    }

    /// The `id_match` value when it is set and not empty.
    #[must_use]
    pub fn id_suffix(&self) -> Option<&str> {
        self.id_match.as_deref().filter(|m| !m.is_empty())
    }

    /// True when `job` passes every field of the filter.
    #[must_use]
    pub fn matches(&self, job: &Job) -> bool {
        (self.statuses.is_empty() || self.statuses.contains(&job.status))
            && self.task.as_deref().is_none_or(|t| job.task_name == t)
            && self.pool.as_deref().is_none_or(|p| job.pool == p)
            && self.created_from.is_none_or(|t| job.created_at >= t)
            && self.created_to.is_none_or(|t| job.created_at < t)
            && self.id_suffix().is_none_or(|m| job.job_id.ends_with(m))
            && (self.include_dropped || job.dropped_at.is_none())
    }
}

/// Which runs a runs scan or count covers.
///
/// # Examples
///
/// ```
/// use boson_core::{RunFilter, RunStatus};
///
/// // Runs that failed or timed out for one job.
/// let filter = RunFilter {
///     statuses: vec![RunStatus::Failed, RunStatus::Timeout],
///     job_id: Some("6c1e…".into()),
///     ..RunFilter::default()
/// };
/// # let _ = filter;
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunFilter {
    /// Statuses to keep; empty keeps every status.
    #[serde(default)]
    pub statuses: Vec<RunStatus>,
    /// Exact task name.
    #[serde(default)]
    pub task: Option<String>,
    /// Exact job id (the runs of one job).
    #[serde(default)]
    pub job_id: Option<String>,
    /// Keep runs with `started_at >= started_from`.
    #[serde(default)]
    pub started_from: Option<DateTime<Utc>>,
    /// Keep runs with `started_at < started_to`.
    #[serde(default)]
    pub started_to: Option<DateTime<Utc>>,
    /// Whole run id or its trailing characters. Case-sensitive; empty is ignored.
    #[serde(default)]
    pub id_match: Option<String>,
}

impl RunFilter {
    /// Filter on one job's runs.
    #[must_use]
    pub fn job(job_id: &str) -> Self {
        Self {
            job_id: Some(job_id.to_string()),
            ..Self::default()
        }
    }

    /// The `id_match` value when it is set and not empty.
    #[must_use]
    pub fn id_suffix(&self) -> Option<&str> {
        self.id_match.as_deref().filter(|m| !m.is_empty())
    }

    /// True when `run` passes every field of the filter.
    #[must_use]
    pub fn matches(&self, run: &Run) -> bool {
        (self.statuses.is_empty() || self.statuses.contains(&run.status))
            && self.task.as_deref().is_none_or(|t| run.task_name == t)
            && self.job_id.as_deref().is_none_or(|j| run.job_id == j)
            && self.started_from.is_none_or(|t| run.started_at >= t)
            && self.started_to.is_none_or(|t| run.started_at < t)
            && self.id_suffix().is_none_or(|m| run.run_id.ends_with(m))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone};
    use serde_json::json;

    fn job(id: &str, status: JobStatus) -> Job {
        let mut j = Job::new("t", json!({}), json!({}), 0, "global", 0, None);
        j.job_id = id.to_string();
        j.status = status;
        j.created_at = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        j
    }

    #[test]
    fn default_matches_all_but_dropped() {
        let mut j = job("abc", JobStatus::Failed);
        assert!(JobFilter::default().matches(&j));
        j.dropped_at = Some(Utc::now());
        assert!(!JobFilter::default().matches(&j));
        let all = JobFilter {
            include_dropped: true,
            ..JobFilter::default()
        };
        assert!(all.matches(&j));
    }

    #[test]
    fn id_match_is_whole_or_suffix() {
        let j = job("job-123abc", JobStatus::Queued);
        let with = |m: &str| JobFilter {
            id_match: Some(m.to_string()),
            ..JobFilter::default()
        };
        assert!(with("job-123abc").matches(&j));
        assert!(with("3abc").matches(&j));
        assert!(with("").matches(&j));
        assert!(!with("job-1").matches(&j));
        assert!(!with("3ABC").matches(&j));
    }

    #[test]
    fn time_range_is_half_open() {
        let j = job("a", JobStatus::Queued);
        let at = j.created_at;
        let range = |from, to| JobFilter {
            created_from: Some(from),
            created_to: Some(to),
            ..JobFilter::default()
        };
        assert!(range(at, at + Duration::seconds(1)).matches(&j));
        assert!(!range(at - Duration::seconds(1), at).matches(&j));
    }

    #[test]
    fn statuses_task_and_pool() {
        let j = job("a", JobStatus::Running);
        let f = JobFilter {
            statuses: vec![JobStatus::Queued, JobStatus::Running],
            task: Some("t".into()),
            pool: Some("global".into()),
            ..JobFilter::default()
        };
        assert!(f.matches(&j));
        assert!(!JobFilter::status(JobStatus::Queued).matches(&j));
        let other_pool = JobFilter {
            pool: Some("alerts".into()),
            ..JobFilter::default()
        };
        assert!(!other_pool.matches(&j));
    }

    #[test]
    fn run_filter_fields() {
        let mut r = Run::new("job-a", "t", 1);
        r.run_id = "run-xyz".into();
        r.status = RunStatus::Timeout;
        let f = RunFilter {
            statuses: vec![RunStatus::Timeout],
            task: Some("t".into()),
            job_id: Some("job-a".into()),
            started_from: Some(r.started_at),
            started_to: Some(r.started_at + Duration::seconds(1)),
            id_match: Some("xyz".into()),
        };
        assert!(f.matches(&r));
        assert!(!RunFilter::job("job-b").matches(&r));
        let failed = RunFilter {
            statuses: vec![RunStatus::Failed],
            ..RunFilter::default()
        };
        assert!(!failed.matches(&r));
    }

    #[test]
    fn filters_deserialize_from_empty_object() {
        let f: JobFilter = serde_json::from_str("{}").unwrap();
        assert_eq!(f, JobFilter::default());
        let f: RunFilter = serde_json::from_str("{}").unwrap();
        assert_eq!(f, RunFilter::default());
    }
}
