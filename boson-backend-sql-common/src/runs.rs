//! Run persistence for the SQL queue backend.

use boson_core::{Result, Run, RunStatus, TaskRunStats};
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::row::{row_to_run, run_status_to_str};
use crate::{
    bind_sql, sql_execute, sql_fetch_all_map, sql_fetch_one_map, sql_fetch_optional_map,
    SqlQueueBackend,
};

impl SqlQueueBackend {
    pub(crate) async fn upsert_run_impl(&self, run: &Run) -> Result<()> {
        let sql = bind_sql(
            self.dialect,
            "INSERT INTO boson_run
             (run_id, job_id, task_name, attempt, status, started_at, finished_at, duration_ms, error_message)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT (run_id) DO UPDATE SET
              job_id = excluded.job_id,
              task_name = excluded.task_name,
              attempt = excluded.attempt,
              status = excluded.status,
              started_at = excluded.started_at,
              finished_at = excluded.finished_at,
              duration_ms = excluded.duration_ms,
              error_message = excluded.error_message",
        );
        sql_execute!(self, &sql, |q| {
            q.bind(&run.run_id)
                .bind(&run.job_id)
                .bind(&run.task_name)
                .bind(run.attempt)
                .bind(run_status_to_str(run.status))
                .bind(run.started_at)
                .bind(run.finished_at)
                .bind(run.duration_ms)
                .bind(&run.error_message)
        })
    }

    pub(crate) async fn get_run_impl(&self, run_id: &str) -> Result<Option<Run>> {
        let sql = bind_sql(self.dialect, "SELECT * FROM boson_run WHERE run_id = ?");
        sql_fetch_optional_map!(
            self,
            &sql,
            |q| q.bind(run_id),
            |r| row_to_run(&r)
        )
    }

    pub(crate) async fn list_runs_impl(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        if let Some(job_id) = job_id_filter {
            let sql = bind_sql(
                self.dialect,
                "SELECT * FROM boson_run WHERE job_id = ? ORDER BY started_at DESC LIMIT ? OFFSET ?",
            );
            sql_fetch_all_map!(self, &sql, |q| {
                q.bind(job_id)
                    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                    .bind(i64::try_from(offset).unwrap_or(i64::MAX))
            }, |r| row_to_run(r))
        } else {
            let sql = bind_sql(
                self.dialect,
                "SELECT * FROM boson_run ORDER BY started_at DESC LIMIT ? OFFSET ?",
            );
            sql_fetch_all_map!(self, &sql, |q| q.bind(i64::try_from(limit).unwrap_or(i64::MAX)).bind(i64::try_from(offset).unwrap_or(i64::MAX)), |r| {
                row_to_run(r)
            })
        }
    }

    pub(crate) async fn finish_run_impl(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        let sql = bind_sql(
            self.dialect,
            "UPDATE boson_run SET status = ?, finished_at = ?, duration_ms = ?, error_message = ?
             WHERE run_id = ?",
        );
        let finished_at = Utc::now();
        sql_execute!(self, &sql, |q| {
            q.bind(run_status_to_str(status))
                .bind(finished_at)
                .bind(duration_ms)
                .bind(error_message)
                .bind(run_id)
        })
    }

    pub(crate) async fn count_runs_impl(&self, job_id_filter: Option<&str>) -> Result<u64> {
        if let Some(job_id) = job_id_filter {
            let sql = bind_sql(
                self.dialect,
                "SELECT COUNT(*) AS cnt FROM boson_run WHERE job_id = ?",
            );
            sql_fetch_one_map!(
                self,
                &sql,
                |q| q.bind(job_id),
                |r| Ok(u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX))
            )
        } else {
            let sql = bind_sql(self.dialect, "SELECT COUNT(*) AS cnt FROM boson_run");
            sql_fetch_one_map!(self, &sql, |q| q, |r| {
                Ok(u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX))
            })
        }
    }

    pub(crate) async fn count_runs_since_impl(&self, since: DateTime<Utc>) -> Result<u64> {
        let sql = bind_sql(
            self.dialect,
            "SELECT COUNT(*) AS cnt FROM boson_run WHERE started_at >= ?",
        );
        sql_fetch_one_map!(
            self,
            &sql,
            |q| q.bind(since),
            |r| Ok(u64::try_from(r.get::<i64, _>("cnt")).unwrap_or(u64::MAX))
        )
    }

    pub(crate) async fn task_run_stats_impl(&self, task_name: &str) -> Result<TaskRunStats> {
        let total_sql = bind_sql(
            self.dialect,
            "SELECT COUNT(*) AS cnt FROM boson_run WHERE task_name = ?",
        );
        let success_sql = bind_sql(
            self.dialect,
            "SELECT COUNT(*) AS cnt FROM boson_run WHERE task_name = ? AND status = 'success'",
        );
        let runs_total = sql_fetch_one_map!(
            self,
            &total_sql,
            |q| q.bind(task_name),
            |r| Ok::<u32, boson_core::BosonError>(u32::try_from(r.get::<i64, _>("cnt")).unwrap_or(u32::MAX))
        )?;
        let success_count = sql_fetch_one_map!(
            self,
            &success_sql,
            |q| q.bind(task_name),
            |r| Ok::<u32, boson_core::BosonError>(u32::try_from(r.get::<i64, _>("cnt")).unwrap_or(u32::MAX))
        )?;
        Ok(TaskRunStats {
            runs_total,
            success_count,
        })
    }
}
