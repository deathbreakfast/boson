//! Pool stats and pause state for the SQL queue backend.

use std::collections::BTreeMap;

use boson_core::{PoolState, PoolStats, Result};
use chrono::{DateTime, Utc};
use sqlx::Row;

use crate::{bind_sql, sql_execute, sql_fetch_all_map, sql_fetch_optional_map, SqlQueueBackend};

impl SqlQueueBackend {
    pub(crate) async fn pool_stats_impl(&self, window: chrono::Duration) -> Result<Vec<PoolStats>> {
        let since = Utc::now() - window;
        let mut stats: BTreeMap<String, PoolStats> = BTreeMap::new();

        let named = bind_sql(
            self.dialect,
            "SELECT DISTINCT pool FROM boson_task_config
             UNION SELECT DISTINCT pool FROM boson_job",
        );
        let pools = sql_fetch_all_map!(self, &named, |q| q, |r| {
            Ok::<String, boson_core::BosonError>(r.get("pool"))
        })?;
        for pool in pools {
            stats.insert(pool.clone(), PoolStats::empty(&pool));
        }

        let active = bind_sql(
            self.dialect,
            "SELECT pool, status, COUNT(*) AS cnt, MIN(created_at) AS oldest FROM boson_job
             WHERE status IN ('queued', 'running') GROUP BY pool, status",
        );
        let rows = sql_fetch_all_map!(self, &active, |q| q, |r| {
            Ok::<_, boson_core::BosonError>((
                r.get::<String, _>("pool"),
                r.get::<String, _>("status"),
                count(r.get::<i64, _>("cnt")),
                r.get::<Option<DateTime<Utc>>, _>("oldest"),
            ))
        })?;
        for (pool, status, cnt, oldest) in rows {
            let entry = entry(&mut stats, &pool);
            if status == "queued" {
                entry.waiting = cnt;
                entry.oldest_waiting_at = oldest;
            } else {
                entry.running = cnt;
            }
        }

        let added = bind_sql(
            self.dialect,
            "SELECT pool, COUNT(*) AS cnt FROM boson_job WHERE created_at >= ? GROUP BY pool",
        );
        for (pool, cnt) in self.pool_counts(&added, since).await? {
            entry(&mut stats, &pool).added_in_window = cnt;
        }

        let done = bind_sql(
            self.dialect,
            "SELECT j.pool AS pool, COUNT(DISTINCT j.job_id) AS cnt
             FROM boson_run r JOIN boson_job j ON j.job_id = r.job_id
             WHERE r.finished_at >= ? AND j.status IN ('success', 'failed', 'canceled')
             GROUP BY j.pool",
        );
        for (pool, cnt) in self.pool_counts(&done, since).await? {
            entry(&mut stats, &pool).done_in_window = cnt;
        }

        for state in self.list_pool_states().await? {
            entry(&mut stats, &state.pool).paused = state.paused;
        }
        Ok(stats.into_values().collect())
    }

    /// `(pool, cnt)` rows from a grouped count with one timestamp bind.
    async fn pool_counts(&self, sql: &str, since: DateTime<Utc>) -> Result<Vec<(String, u64)>> {
        sql_fetch_all_map!(self, sql, |q| q.bind(since), |r| {
            Ok::<_, boson_core::BosonError>((
                r.get::<String, _>("pool"),
                count(r.get::<i64, _>("cnt")),
            ))
        })
    }

    async fn list_pool_states(&self) -> Result<Vec<PoolState>> {
        let sql = bind_sql(self.dialect, "SELECT * FROM boson_pool_state");
        sql_fetch_all_map!(self, &sql, |q| q, |r| {
            Ok::<_, boson_core::BosonError>(PoolState {
                pool: r.get("pool"),
                paused: r.get("paused"),
                changed_by: r.get("changed_by"),
                changed_at: r.get("changed_at"),
            })
        })
    }

    pub(crate) async fn set_pool_paused_impl(
        &self,
        pool: &str,
        paused: bool,
        actor_label: &str,
    ) -> Result<PoolState> {
        let state = PoolState {
            pool: pool.to_string(),
            paused,
            changed_by: (!actor_label.is_empty()).then(|| actor_label.to_string()),
            changed_at: Utc::now(),
        };
        let sql = bind_sql(
            self.dialect,
            "INSERT INTO boson_pool_state (pool, paused, changed_by, changed_at)
             VALUES (?, ?, ?, ?)
             ON CONFLICT (pool) DO UPDATE SET
              paused = excluded.paused,
              changed_by = excluded.changed_by,
              changed_at = excluded.changed_at",
        );
        sql_execute!(self, &sql, |q| {
            q.bind(&state.pool)
                .bind(state.paused)
                .bind(&state.changed_by)
                .bind(state.changed_at)
        })?;
        Ok(state)
    }

    pub(crate) async fn get_pool_state_impl(&self, pool: &str) -> Result<Option<PoolState>> {
        let sql = bind_sql(
            self.dialect,
            "SELECT * FROM boson_pool_state WHERE pool = ?",
        );
        sql_fetch_optional_map!(self, &sql, |q| q.bind(pool), |r| {
            Ok::<_, boson_core::BosonError>(PoolState {
                pool: r.get("pool"),
                paused: r.get("paused"),
                changed_by: r.get("changed_by"),
                changed_at: r.get("changed_at"),
            })
        })
    }
}

fn entry<'a>(stats: &'a mut BTreeMap<String, PoolStats>, pool: &str) -> &'a mut PoolStats {
    stats
        .entry(pool.to_string())
        .or_insert_with(|| PoolStats::empty(pool))
}

fn count(n: i64) -> u64 {
    u64::try_from(n).unwrap_or(0)
}
