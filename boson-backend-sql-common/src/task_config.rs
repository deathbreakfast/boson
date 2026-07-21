//! Task config persistence for the SQL queue backend.

use boson_core::{Result, TaskConfig};

use crate::row::{row_to_task_config, task_config_to_binds};
use crate::{bind_sql, sql_execute, sql_fetch_optional_map, SqlQueueBackend};

impl SqlQueueBackend {
    pub(crate) async fn get_task_config_impl(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        let sql = bind_sql(
            self.dialect,
            "SELECT * FROM boson_task_config WHERE task_name = ?",
        );
        sql_fetch_optional_map!(self, &sql, |q| q.bind(task_name), |r| row_to_task_config(
            &r
        ))
    }

    pub(crate) async fn upsert_task_config_impl(&self, config: &TaskConfig) -> Result<()> {
        let (retry_json, rate_json) = task_config_to_binds(config)?;
        let sql = bind_sql(
            self.dialect,
            "INSERT INTO boson_task_config
             (task_name, priority, pool, retry_policy_json, rate_limit_policy_json, updated_at)
             VALUES (?, ?, ?, ?, ?, ?)
             ON CONFLICT (task_name) DO UPDATE SET
              priority = excluded.priority,
              pool = excluded.pool,
              retry_policy_json = excluded.retry_policy_json,
              rate_limit_policy_json = excluded.rate_limit_policy_json,
              updated_at = excluded.updated_at",
        );
        sql_execute!(self, &sql, |q| {
            q.bind(&config.task_name)
                .bind(config.priority)
                .bind(&config.pool)
                .bind(retry_json)
                .bind(rate_json)
                .bind(config.updated_at)
        })
    }
}
