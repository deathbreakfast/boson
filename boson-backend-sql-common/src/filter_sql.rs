//! `WHERE` clauses for [`JobFilter`] and [`RunFilter`], shared by the keyset and count queries.

use boson_core::{JobFilter, JobKey, RunFilter, RunKey};
use chrono::{DateTime, Utc};

use crate::row::{job_status_to_str, run_status_to_str};
use crate::SqlDialect;

/// One bind value for a built clause, in placeholder order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Bind {
    /// Text column value.
    Text(String),
    /// Timestamp column value.
    Time(DateTime<Utc>),
    /// Integer value.
    Int(i64),
}

/// `AND`-joined clauses with `?` placeholders and their binds.
#[derive(Debug, Default)]
pub struct Where {
    clauses: Vec<String>,
    /// Bind values in placeholder order.
    pub binds: Vec<Bind>,
}

impl Where {
    fn push(&mut self, clause: impl Into<String>, binds: impl IntoIterator<Item = Bind>) {
        self.clauses.push(clause.into());
        self.binds.extend(binds);
    }

    /// `" WHERE a AND b"`, or empty when there are no clauses.
    #[must_use]
    pub fn sql(&self) -> String {
        if self.clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", self.clauses.join(" AND "))
        }
    }
}

/// `col IN (?, ?, …)` with one placeholder per value.
fn in_list(col: &str, n: usize) -> String {
    format!("{col} IN ({})", vec!["?"; n].join(", "))
}

/// Whole-id-or-suffix match on `col`, exact and case-sensitive on both engines.
///
/// `SQLite`'s `LIKE` ignores ASCII case, so both dialects compare the last N characters instead.
fn push_suffix(w: &mut Where, dialect: SqlDialect, col: &str, suffix: &str) {
    let n = i64::try_from(suffix.chars().count()).unwrap_or(i64::MAX);
    let (clause, len) = match dialect {
        SqlDialect::Sqlite => (format!("substr({col}, ?) = ?"), -n),
        SqlDialect::Postgres => (format!("right({col}, CAST(? AS INTEGER)) = ?"), n),
    };
    w.push(clause, [Bind::Int(len), Bind::Text(suffix.to_string())]);
}

/// Clauses for a [`JobFilter`].
#[must_use]
pub fn job_where(dialect: SqlDialect, filter: &JobFilter) -> Where {
    let mut w = Where::default();
    if !filter.statuses.is_empty() {
        w.push(
            in_list("status", filter.statuses.len()),
            filter
                .statuses
                .iter()
                .map(|s| Bind::Text(job_status_to_str(*s).to_string())),
        );
    }
    if let Some(task) = &filter.task {
        w.push("task_name = ?", [Bind::Text(task.clone())]);
    }
    if let Some(pool) = &filter.pool {
        w.push("pool = ?", [Bind::Text(pool.clone())]);
    }
    if let Some(from) = filter.created_from {
        w.push("created_at >= ?", [Bind::Time(from)]);
    }
    if let Some(to) = filter.created_to {
        w.push("created_at < ?", [Bind::Time(to)]);
    }
    if let Some(suffix) = filter.id_suffix() {
        push_suffix(&mut w, dialect, "job_id", suffix);
    }
    if !filter.include_dropped {
        w.push("dropped_at IS NULL", []);
    }
    w
}

/// Clauses for a [`RunFilter`].
#[must_use]
pub fn run_where(dialect: SqlDialect, filter: &RunFilter) -> Where {
    let mut w = Where::default();
    if !filter.statuses.is_empty() {
        w.push(
            in_list("status", filter.statuses.len()),
            filter
                .statuses
                .iter()
                .map(|s| Bind::Text(run_status_to_str(*s).to_string())),
        );
    }
    if let Some(task) = &filter.task {
        w.push("task_name = ?", [Bind::Text(task.clone())]);
    }
    if let Some(job_id) = &filter.job_id {
        w.push("job_id = ?", [Bind::Text(job_id.clone())]);
    }
    if let Some(from) = filter.started_from {
        w.push("started_at >= ?", [Bind::Time(from)]);
    }
    if let Some(to) = filter.started_to {
        w.push("started_at < ?", [Bind::Time(to)]);
    }
    if let Some(suffix) = filter.id_suffix() {
        push_suffix(&mut w, dialect, "run_id", suffix);
    }
    w
}

/// Job filter plus the keyset predicate for `created_at ASC, job_id ASC`.
///
/// The predicate is spelled as an expanded `OR` instead of a row-value comparison so the same
/// text runs on `SQLite` and Postgres. The redundant `created_at >= ?` bound lets the planner
/// seek into `boson_job_created_at_id` (or `boson_job_status_created_at_id`) instead of scanning
/// from the start.
#[must_use]
pub fn jobs_after_where(dialect: SqlDialect, filter: &JobFilter, after: Option<&JobKey>) -> Where {
    let mut w = job_where(dialect, filter);
    if let Some(key) = after {
        w.push(
            "created_at >= ? AND (created_at > ? OR (created_at = ? AND job_id > ?))",
            [
                Bind::Time(key.created_at),
                Bind::Time(key.created_at),
                Bind::Time(key.created_at),
                Bind::Text(key.id.clone()),
            ],
        );
    }
    w
}

/// Run filter plus the keyset predicate for `started_at DESC, run_id DESC`.
///
/// Same shape as [`jobs_after_where`]; the redundant `started_at <= ?` bound lets the planner
/// seek into `boson_run_started_at_id`, scanned backward.
#[must_use]
pub fn runs_after_where(dialect: SqlDialect, filter: &RunFilter, after: Option<&RunKey>) -> Where {
    let mut w = run_where(dialect, filter);
    if let Some(key) = after {
        w.push(
            "started_at <= ? AND (started_at < ? OR (started_at = ? AND run_id < ?))",
            [
                Bind::Time(key.started_at),
                Bind::Time(key.started_at),
                Bind::Time(key.started_at),
                Bind::Text(key.id.clone()),
            ],
        );
    }
    w
}

/// Bind every [`Bind`] onto a `sqlx` query, in order.
#[macro_export]
macro_rules! bind_all {
    ($q:expr, $binds:expr) => {{
        let mut q = $q;
        for b in $binds {
            q = match b {
                $crate::filter_sql::Bind::Text(s) => q.bind(s.clone()),
                $crate::filter_sql::Bind::Time(t) => q.bind(*t),
                $crate::filter_sql::Bind::Int(i) => q.bind(*i),
            };
        }
        q
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use boson_core::{JobStatus, RunStatus};

    #[test]
    fn default_job_filter_hides_dropped() {
        let w = job_where(SqlDialect::Sqlite, &JobFilter::default());
        assert_eq!(w.sql(), " WHERE dropped_at IS NULL");
        assert_eq!(w.binds, Vec::new());
        let all = JobFilter {
            include_dropped: true,
            ..JobFilter::default()
        };
        assert_eq!(job_where(SqlDialect::Sqlite, &all).sql(), "");
    }

    #[test]
    fn job_filter_every_field() {
        let t = Utc::now();
        let filter = JobFilter {
            statuses: vec![JobStatus::Failed, JobStatus::Canceled],
            task: Some("t".into()),
            pool: Some("p".into()),
            created_from: Some(t),
            created_to: Some(t),
            id_match: Some("ab".into()),
            include_dropped: true,
        };
        let key = JobKey {
            created_at: t,
            id: "k".into(),
        };
        let w = jobs_after_where(SqlDialect::Sqlite, &filter, Some(&key));
        assert_eq!(
            w.sql(),
            " WHERE status IN (?, ?) AND task_name = ? AND pool = ? AND created_at >= ? \
             AND created_at < ? AND substr(job_id, ?) = ? AND created_at >= ? \
             AND (created_at > ? OR (created_at = ? AND job_id > ?))"
        );
        assert_eq!(w.binds.len(), 12);
        assert_eq!(w.binds[6], Bind::Int(-2));
        let pg = job_where(SqlDialect::Postgres, &filter);
        assert!(pg.sql().contains("right(job_id, CAST(? AS INTEGER)) = ?"));
        assert_eq!(pg.binds[6], Bind::Int(2));
    }

    #[test]
    fn run_filter_every_field() {
        let t = Utc::now();
        let filter = RunFilter {
            statuses: vec![RunStatus::Timeout],
            task: Some("t".into()),
            job_id: Some("j".into()),
            started_from: Some(t),
            started_to: Some(t),
            id_match: Some("xyz".into()),
        };
        let w = runs_after_where(SqlDialect::Postgres, &filter, None);
        assert_eq!(
            w.sql(),
            " WHERE status IN (?) AND task_name = ? AND job_id = ? AND started_at >= ? \
             AND started_at < ? AND right(run_id, CAST(? AS INTEGER)) = ?"
        );
        assert_eq!(w.binds[0], Bind::Text("timeout".into()));
        assert_eq!(
            runs_after_where(SqlDialect::Sqlite, &RunFilter::default(), None).sql(),
            ""
        );
    }
}
