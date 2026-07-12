//! `ScyllaDB` [`QueueBackend`] for Boson.
//!
//! ## Local builds
//!
//! Prefer a single Cargo job on resource-constrained hosts:
//!
//! ```bash
//! export CARGO_BUILD_JOBS=1
//! export CARGO_TARGET_DIR=target-boson-scylla
//! ```
//!
//! Multi-node / multi-worker Scylla E2E must run against CI or cloud contact points —
//! not a local multi-node Docker cluster.

mod bootstrap;
mod config;
mod enqueue_rate;
mod error_map;
mod schema;

pub use bootstrap::{
    install_default_scylla_backend, isolated_keyspace, scylla_test_contact_points,
};

use std::collections::HashSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use futures::future::join_all;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;
use scylla::client::PoolSize;
use scylla::DeserializeRow;
use uuid::Uuid;

use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run,
    RunStatus, TaskConfig, TaskRunStats,
};

use config::{expiry_bucket, shard_for_job};
use enqueue_rate::EnqueueRateLimiter;
use error_map::map_err;

pub use config::ScyllaQueueConfig;

/// Scylla-backed queue backend.
pub struct ScyllaQueueBackend {
    session: Arc<Session>,
    keyspace: String,
    ready_shard_count: u32,
    shard_concurrency: u32,
    parallel_writes: bool,
    enqueue_rate: EnqueueRateLimiter,
    list_cursor: AtomicU64,
    // prepared statements
    insert_job: scylla::statement::prepared::PreparedStatement,
    select_job: scylla::statement::prepared::PreparedStatement,
    update_job_claim: scylla::statement::prepared::PreparedStatement,
    update_job_status: scylla::statement::prepared::PreparedStatement,
    update_job_revert: scylla::statement::prepared::PreparedStatement,
    insert_ready: scylla::statement::prepared::PreparedStatement,
    select_ready: scylla::statement::prepared::PreparedStatement,
    delete_ready: scylla::statement::prepared::PreparedStatement,
    insert_idempotency: scylla::statement::prepared::PreparedStatement,
    upsert_idempotency: scylla::statement::prepared::PreparedStatement,
    select_idempotency: scylla::statement::prepared::PreparedStatement,
    insert_lease: scylla::statement::prepared::PreparedStatement,
    steal_lease: scylla::statement::prepared::PreparedStatement,
    select_lease: scylla::statement::prepared::PreparedStatement,
    update_lease_ttl: scylla::statement::prepared::PreparedStatement,
    delete_lease: scylla::statement::prepared::PreparedStatement,
    insert_lease_by_id: scylla::statement::prepared::PreparedStatement,
    select_lease_by_id: scylla::statement::prepared::PreparedStatement,
    delete_lease_by_id: scylla::statement::prepared::PreparedStatement,
    insert_lease_expiry: scylla::statement::prepared::PreparedStatement,
    delete_lease_expiry: scylla::statement::prepared::PreparedStatement,
    select_lease_expiry: scylla::statement::prepared::PreparedStatement,
    insert_status_idx: scylla::statement::prepared::PreparedStatement,
    delete_status_idx: scylla::statement::prepared::PreparedStatement,
    select_status_idx: scylla::statement::prepared::PreparedStatement,
    insert_run: scylla::statement::prepared::PreparedStatement,
    select_run: scylla::statement::prepared::PreparedStatement,
    update_run_finish: scylla::statement::prepared::PreparedStatement,
    insert_run_by_job: scylla::statement::prepared::PreparedStatement,
    select_run_by_job: scylla::statement::prepared::PreparedStatement,
    insert_task_config: scylla::statement::prepared::PreparedStatement,
    select_task_config: scylla::statement::prepared::PreparedStatement,
}


#[derive(DeserializeRow)]
struct JobRow {
    job_id: String,
    task_name: String,
    actor_json: String,
    params_json: String,
    priority: i32,
    pool: String,
    status: String,
    idempotency_key: Option<String>,
    created_at: i64,
    signature_hash: i64,
    attempt: i32,
}

#[derive(DeserializeRow)]
struct ReadyRow {
    priority: i32,
    created_at: i64,
    job_id: String,
}

#[derive(DeserializeRow)]
struct IdRow {
    job_id: String,
}

#[derive(DeserializeRow)]
struct LeaseRow {
    #[allow(dead_code)]
    lease_id: String,
    #[allow(dead_code)]
    worker_id: String,
    expires_at: i64,
}

#[derive(DeserializeRow)]
struct LeaseExpiryRow {
    expires_at: i64,
    job_id: String,
    lease_id: String,
}

#[derive(DeserializeRow)]
struct StatusIdxRow {
    #[allow(dead_code)]
    created_at: i64,
    job_id: String,
}

#[derive(DeserializeRow)]
struct RunRow {
    run_id: String,
    job_id: String,
    task_name: String,
    attempt: i32,
    status: String,
    started_at: i64,
    finished_at: Option<i64>,
    duration_ms: Option<i64>,
    error_message: Option<String>,
}

#[derive(DeserializeRow)]
struct RunIdRow {
    run_id: String,
}

#[derive(DeserializeRow)]
struct TaskConfigRow {
    task_name: String,
    priority: i32,
    pool: String,
    retry_policy_json: String,
    rate_limit_policy_json: String,
    idempotency_mode: Option<String>,
    updated_at: i64,
}

const fn to_millis(dt: DateTime<Utc>) -> i64 {
    dt.timestamp_millis()
}

fn from_millis(ms: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(ms).unwrap_or_else(Utc::now)
}

impl ScyllaQueueBackend {
    /// Connect to a Scylla cluster and bootstrap schema.
    ///
    /// # Errors
    ///
    /// Returns an error when the session cannot connect or schema bootstrap fails.
    pub async fn connect(config: ScyllaQueueConfig) -> Result<Self> {
        let mut builder = SessionBuilder::new();
        for cp in &config.contact_points {
            builder = builder.known_node(cp.as_str());
        }
        if let Some(dc) = &config.datacenter {
            builder = builder.prefer_datacenter(dc.clone());
        }
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            builder = builder.user(user.clone(), pass.clone());
        }
        if let Some(n) = config.pool_per_shard.and_then(|n| NonZeroUsize::new(n.max(1) as usize)) {
            builder = builder.pool_size(PoolSize::PerShard(n));
        }
        let session = Box::pin(builder.build()).await.map_err(map_err)?;
        Self::from_session(Arc::new(session), &config).await
    }

    /// Wrap an existing session (schema bootstrap runs).
    ///
    /// # Errors
    ///
    /// Returns an error when schema bootstrap or prepare fails.
    #[allow(clippy::cognitive_complexity, clippy::too_many_lines)]
    pub async fn from_session(session: Arc<Session>, config: &ScyllaQueueConfig) -> Result<Self> {
        schema::ensure_schema(&session, &config.keyspace, config.replication_factor).await?;
        let keyspace = config.keyspace.clone();
        let prefix = format!("{keyspace}.");
        let q = |sql: &str| sql.replace("boson.", &prefix);

        let prepare = |sql: String| {
            let session = Arc::clone(&session);
            async move { session.prepare(sql).await.map_err(map_err) }
        };

        Ok(Self {
            session: Arc::clone(&session),
            keyspace,
            ready_shard_count: config.ready_shard_count.max(1),
            shard_concurrency: config.shard_concurrency.max(1),
            parallel_writes: config.parallel_writes,
            enqueue_rate: EnqueueRateLimiter::new(),
            list_cursor: AtomicU64::new(0),
            insert_job: prepare(q(
                "INSERT INTO boson.boson_job (job_id, task_name, actor_json, params_json, priority, pool, status, idempotency_key, created_at, signature_hash, attempt) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ))
            .await?,
            select_job: prepare(q(
                "SELECT job_id, task_name, actor_json, params_json, priority, pool, status, idempotency_key, created_at, signature_hash, attempt FROM boson.boson_job WHERE job_id = ?",
            ))
            .await?,
            update_job_claim: prepare(q(
                "UPDATE boson.boson_job SET status = ? WHERE job_id = ? IF status = ?",
            ))
            .await?,
            update_job_status: prepare(q(
                "UPDATE boson.boson_job SET status = ?, attempt = ? WHERE job_id = ?",
            ))
            .await?,
            update_job_revert: prepare(q(
                "UPDATE boson.boson_job SET status = ? WHERE job_id = ? IF status = ?",
            ))
            .await?,
            insert_ready: prepare(q(
                "INSERT INTO boson.boson_ready (pool, shard, priority, created_at, job_id) VALUES (?, ?, ?, ?, ?)",
            ))
            .await?,
            select_ready: prepare(q(
                "SELECT priority, created_at, job_id FROM boson.boson_ready WHERE pool = ? AND shard = ? LIMIT ?",
            ))
            .await?,
            delete_ready: prepare(q(
                "DELETE FROM boson.boson_ready WHERE pool = ? AND shard = ? AND priority = ? AND created_at = ? AND job_id = ?",
            ))
            .await?,
            insert_idempotency: prepare(q(
                "INSERT INTO boson.boson_idempotency (idempotency_key, job_id) VALUES (?, ?) IF NOT EXISTS",
            ))
            .await?,
            upsert_idempotency: prepare(q(
                "INSERT INTO boson.boson_idempotency (idempotency_key, job_id) VALUES (?, ?)",
            ))
            .await?,
            select_idempotency: prepare(q(
                "SELECT job_id FROM boson.boson_idempotency WHERE idempotency_key = ?",
            ))
            .await?,
            insert_lease: prepare(q(
                "INSERT INTO boson.boson_lease (job_id, lease_id, worker_id, expires_at) VALUES (?, ?, ?, ?) IF NOT EXISTS",
            ))
            .await?,
            steal_lease: prepare(q(
                "UPDATE boson.boson_lease SET lease_id = ?, worker_id = ?, expires_at = ? WHERE job_id = ? IF expires_at < ?",
            ))
            .await?,
            select_lease: prepare(q(
                "SELECT lease_id, worker_id, expires_at FROM boson.boson_lease WHERE job_id = ?",
            ))
            .await?,
            update_lease_ttl: prepare(q(
                "UPDATE boson.boson_lease SET expires_at = ? WHERE job_id = ?",
            ))
            .await?,
            delete_lease: prepare(q("DELETE FROM boson.boson_lease WHERE job_id = ?")).await?,
            insert_lease_by_id: prepare(q(
                "INSERT INTO boson.boson_lease_by_id (lease_id, job_id) VALUES (?, ?)",
            ))
            .await?,
            select_lease_by_id: prepare(q(
                "SELECT job_id FROM boson.boson_lease_by_id WHERE lease_id = ?",
            ))
            .await?,
            delete_lease_by_id: prepare(q(
                "DELETE FROM boson.boson_lease_by_id WHERE lease_id = ?",
            ))
            .await?,
            insert_lease_expiry: prepare(q(
                "INSERT INTO boson.boson_lease_by_expiry (bucket, expires_at, job_id, lease_id) VALUES (?, ?, ?, ?)",
            ))
            .await?,
            delete_lease_expiry: prepare(q(
                "DELETE FROM boson.boson_lease_by_expiry WHERE bucket = ? AND expires_at = ? AND job_id = ?",
            ))
            .await?,
            select_lease_expiry: prepare(q(
                "SELECT expires_at, job_id, lease_id FROM boson.boson_lease_by_expiry WHERE bucket = ?",
            ))
            .await?,
            insert_status_idx: prepare(q(
                "INSERT INTO boson.boson_job_by_status (status, created_at, job_id) VALUES (?, ?, ?)",
            ))
            .await?,
            delete_status_idx: prepare(q(
                "DELETE FROM boson.boson_job_by_status WHERE status = ? AND created_at = ? AND job_id = ?",
            ))
            .await?,
            select_status_idx: prepare(q(
                "SELECT created_at, job_id FROM boson.boson_job_by_status WHERE status = ? LIMIT ?",
            ))
            .await?,
            insert_run: prepare(q(
                "INSERT INTO boson.boson_run (run_id, job_id, task_name, attempt, status, started_at, finished_at, duration_ms, error_message) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ))
            .await?,
            select_run: prepare(q(
                "SELECT run_id, job_id, task_name, attempt, status, started_at, finished_at, duration_ms, error_message FROM boson.boson_run WHERE run_id = ?",
            ))
            .await?,
            update_run_finish: prepare(q(
                "UPDATE boson.boson_run SET status = ?, finished_at = ?, duration_ms = ?, error_message = ? WHERE run_id = ?",
            ))
            .await?,
            insert_run_by_job: prepare(q(
                "INSERT INTO boson.boson_run_by_job (job_id, started_at, run_id) VALUES (?, ?, ?)",
            ))
            .await?,
            select_run_by_job: prepare(q(
                "SELECT run_id FROM boson.boson_run_by_job WHERE job_id = ? LIMIT ?",
            ))
            .await?,
            insert_task_config: prepare(q(
                "INSERT INTO boson.boson_task_config (task_name, priority, pool, retry_policy_json, rate_limit_policy_json, idempotency_mode, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            ))
            .await?,
            select_task_config: prepare(q(
                "SELECT task_name, priority, pool, retry_policy_json, rate_limit_policy_json, idempotency_mode, updated_at FROM boson.boson_task_config WHERE task_name = ?",
            ))
            .await?,
        })
    }

    /// Underlying driver session.
    #[must_use]
    pub const fn session(&self) -> &Arc<Session> {
        &self.session
    }

    /// Configured keyspace name.
    #[must_use]
    pub fn keyspace(&self) -> &str {
        &self.keyspace
    }

    /// Ready-queue shard count.
    #[must_use]
    pub const fn ready_shard_count(&self) -> u32 {
        self.ready_shard_count
    }

    async fn exec(
        &self,
        stmt: &scylla::statement::prepared::PreparedStatement,
        values: impl scylla::serialize::row::SerializeRow,
    ) -> Result<scylla::response::query_result::QueryResult> {
        self.session
            .execute_unpaged(stmt, values)
            .await
            .map_err(map_err)
    }

    fn job_from_row(row: JobRow) -> Result<Job> {
        Ok(Job {
            job_id: row.job_id,
            task_name: row.task_name,
            actor_json: serde_json::from_str(&row.actor_json)?,
            params_json: serde_json::from_str(&row.params_json)?,
            priority: row.priority,
            pool: row.pool,
            status: parse_job_status(&row.status)?,
            idempotency_key: row.idempotency_key,
            created_at: from_millis(row.created_at),
            signature_hash: u64::try_from(row.signature_hash).unwrap_or(0),
            attempt: row.attempt,
        })
    }

    async fn load_job(&self, job_id: &str) -> Result<Option<Job>> {
        let rows = self.exec(&self.select_job, (job_id,)).await?;
        maybe_first_row::<JobRow>(rows)
            .map(Self::job_from_row)
            .transpose()
    }

    async fn insert_job_body(&self, job: &Job) -> Result<()> {
        let actor = serde_json::to_string(&job.actor_json)?;
        let params = serde_json::to_string(&job.params_json)?;
        self.exec(
            &self.insert_job,
            (
                job.job_id.as_str(),
                job.task_name.as_str(),
                actor.as_str(),
                params.as_str(),
                job.priority,
                job.pool.as_str(),
                job.status.to_string(),
                job.idempotency_key.as_deref(),
                to_millis(job.created_at),
                i64::try_from(job.signature_hash).unwrap_or(i64::MAX),
                job.attempt,
            ),
        )
        .await?;
        Ok(())
    }

    async fn insert_status_for_job(&self, job: &Job) -> Result<()> {
        self.exec(
            &self.insert_status_idx,
            (
                job.status.to_string(),
                to_millis(job.created_at),
                job.job_id.as_str(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn insert_job_row(&self, job: &Job) -> Result<()> {
        if self.parallel_writes {
            let (job_res, status_res) =
                tokio::join!(self.insert_job_body(job), self.insert_status_for_job(job));
            job_res?;
            status_res?;
        } else {
            self.insert_job_body(job).await?;
            self.insert_status_for_job(job).await?;
        }
        Ok(())
    }

    async fn insert_ready_row(&self, job: &Job) -> Result<()> {
        let shard = shard_for_job(&job.job_id, self.ready_shard_count);
        self.exec(
            &self.insert_ready,
            (
                job.pool.as_str(),
                shard,
                job.priority,
                to_millis(job.created_at),
                job.job_id.as_str(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn delete_ready_row(&self, job: &Job) -> Result<()> {
        let shard = shard_for_job(&job.job_id, self.ready_shard_count);
        self.exec(
            &self.delete_ready,
            (
                job.pool.as_str(),
                shard,
                job.priority,
                to_millis(job.created_at),
                job.job_id.as_str(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn move_status_idx(
        &self,
        job_id: &str,
        created_at: DateTime<Utc>,
        from: JobStatus,
        to: JobStatus,
    ) -> Result<()> {
        let ms = to_millis(created_at);
        let from_s = from.to_string();
        let to_s = to.to_string();
        if self.parallel_writes {
            let (del_res, ins_res) = tokio::join!(
                self.exec(&self.delete_status_idx, (from_s.as_str(), ms, job_id)),
                self.exec(&self.insert_status_idx, (to_s.as_str(), ms, job_id)),
            );
            del_res?;
            ins_res?;
        } else {
            self.exec(&self.delete_status_idx, (from_s.as_str(), ms, job_id))
                .await?;
            self.exec(&self.insert_status_idx, (to_s.as_str(), ms, job_id))
                .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl QueueBackend for ScyllaQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        if let Some(existing) = self.load_job(&job.job_id).await? {
            if existing.status != job.status {
                self.move_status_idx(
                    &job.job_id,
                    existing.created_at,
                    existing.status,
                    job.status,
                )
                .await?;
                if existing.status == JobStatus::Queued {
                    let _ = self.delete_ready_row(&existing).await;
                }
            } else if existing.status == JobStatus::Queued {
                // Same status but fields may change (e.g. attempt on retry); refresh ready PK.
                let _ = self.delete_ready_row(&existing).await;
            }
        }
        self.insert_job_row(job).await?;
        if job.status == JobStatus::Queued {
            self.insert_ready_row(job).await?;
        }
        Ok(())
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        let idempotency = task_config.resolved_idempotency_mode(IdempotencyMode::Lwt);
        let mut job = job;
        if idempotency == IdempotencyMode::Lwt {
            if let Some(ref key) = job.idempotency_key {
                if !key.is_empty() {
                    let result = self
                        .exec(&self.insert_idempotency, (key.as_str(), job.job_id.as_str()))
                        .await?;
                    if !lwt_applied(result) {
                        let existing = self.exec(&self.select_idempotency, (key.as_str(),)).await?;
                        let Some(row) = maybe_first_row::<IdRow>(existing) else {
                            return Err(BosonError::Backend(
                                "idempotency insert not applied but row missing".into(),
                            ));
                        };
                        // Reuse only while the prior job is still active (mem/SQL parity).
                        if let Some(prior) = self.load_job(&row.job_id).await? {
                            if matches!(prior.status, JobStatus::Queued | JobStatus::Running) {
                                return Ok((row.job_id, JobEnqueueDisposition::ReusedIdempotent));
                            }
                        }
                        // Terminal or missing: point the key at the new job id.
                        self.exec(
                            &self.upsert_idempotency,
                            (key.as_str(), job.job_id.as_str()),
                        )
                        .await?;
                    }
                }
            }
        } else {
            job.idempotency_key = None;
        }

        let policy = &task_config.rate_limit_policy;
        if policy.max_in_flight > 0 {
            let count = self.count_active_jobs_for_task(&job.task_name).await?;
            if count >= policy.max_in_flight {
                return Err(BosonError::RateLimited(job.task_name.clone()));
            }
        }
        if policy.max_enqueue_per_second > 0
            && !self
                .enqueue_rate
                .try_record(&job.task_name, policy.max_enqueue_per_second)
        {
            return Err(BosonError::RateLimited(job.task_name.clone()));
        }

        let job_id = job.job_id.clone();
        if self.parallel_writes {
            let (job_res, status_res, ready_res) = tokio::join!(
                self.insert_job_body(&job),
                self.insert_status_for_job(&job),
                self.insert_ready_row(&job),
            );
            job_res?;
            status_res?;
            ready_res?;
        } else {
            self.insert_job_row(&job).await?;
            self.insert_ready_row(&job).await?;
        }
        Ok((job_id, JobEnqueueDisposition::InsertedNew))
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.load_job(job_id).await
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let statuses: Vec<JobStatus> = status_filter.map_or_else(
            || {
                vec![
                    JobStatus::Queued,
                    JobStatus::Running,
                    JobStatus::Success,
                    JobStatus::Failed,
                    JobStatus::Canceled,
                ]
            },
            |status| vec![status],
        );
        let mut jobs = Vec::new();
        let fetch = offset.saturating_add(limit).max(1);
        for status in statuses {
            let rows = self
                .exec(
                    &self.select_status_idx,
                    (
                        status.to_string(),
                        i32::try_from(fetch).unwrap_or(i32::MAX),
                    ),
                )
                .await?;
            for idx in collect_rows::<StatusIdxRow>(rows) {
                if let Some(job) = self.load_job(&idx.job_id).await? {
                    jobs.push(job);
                }
            }
        }
        jobs.sort_by_key(|j| j.created_at);
        Ok(jobs.into_iter().skip(offset).take(limit).collect())
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        let Some(job) = self.load_job(job_id).await? else {
            return Err(BosonError::JobNotFound(job_id.to_string()));
        };
        if !matches!(job.status, JobStatus::Queued | JobStatus::Running) {
            return Ok(());
        }
        let from = job.status;
        self.exec(
            &self.update_job_status,
            (
                JobStatus::Canceled.to_string(),
                job.attempt,
                job_id,
            ),
        )
        .await?;
        self.move_status_idx(job_id, job.created_at, from, JobStatus::Canceled)
            .await?;
        if from == JobStatus::Queued {
            self.delete_ready_row(&job).await?;
        }
        Ok(())
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        let result = self
            .exec(
                &self.update_job_claim,
                (
                    JobStatus::Running.to_string(),
                    job_id,
                    JobStatus::Queued.to_string(),
                ),
            )
            .await?;
        if !lwt_applied(result) {
            return Ok(None);
        }
        let Some(job) = self.load_job(job_id).await? else {
            return Ok(None);
        };
        if self.parallel_writes {
            let (status_res, ready_res) = tokio::join!(
                self.move_status_idx(
                    job_id,
                    job.created_at,
                    JobStatus::Queued,
                    JobStatus::Running
                ),
                self.delete_ready_row(&job),
            );
            status_res?;
            let _ = ready_res;
        } else {
            self.move_status_idx(job_id, job.created_at, JobStatus::Queued, JobStatus::Running)
                .await?;
            let _ = self.delete_ready_row(&job).await;
        }
        Ok(Some(job))
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        let Some(job) = self.load_job(job_id).await? else {
            return Ok(());
        };
        if job.status != JobStatus::Running {
            return Ok(());
        }
        let result = self
            .exec(
                &self.update_job_revert,
                (
                    JobStatus::Queued.to_string(),
                    job_id,
                    JobStatus::Running.to_string(),
                ),
            )
            .await?;
        if lwt_applied(result) {
            self.move_status_idx(job_id, job.created_at, JobStatus::Running, JobStatus::Queued)
                .await?;
            let mut queued = job;
            queued.status = JobStatus::Queued;
            self.insert_ready_row(&queued).await?;
        }
        Ok(())
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        // Sample ready shards for pool discovery (admin path).
        let mut pools = HashSet::new();
        let sample = self.ready_shard_count.min(16);
        for shard in 0..i32::try_from(sample).unwrap_or(1) {
            // We do not have pool list without scanning; use a known default plus any from status index.
            let _ = shard;
        }
        let rows = self
            .exec(
                &self.select_status_idx,
                (JobStatus::Queued.to_string(), 10_000_i32),
            )
            .await?;
        for idx in collect_rows::<StatusIdxRow>(rows) {
            if let Some(job) = self.load_job(&idx.job_id).await? {
                pools.insert(job.pool);
            }
        }
        let mut out: Vec<String> = pools.into_iter().collect();
        out.sort();
        Ok(out)
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        let limit = limit.max(1);
        let shard_count = self.ready_shard_count;
        // Scan every shard so priority order and claim candidates are correct. Sampling a
        // subset of 256 shards routinely missed ready jobs (empty list / stuck Queued).
        let start = self.list_cursor.fetch_add(1, Ordering::Relaxed);
        let per_shard = i32::try_from(limit).unwrap_or(i32::MAX);
        let concurrency = self.shard_concurrency.max(1) as usize;
        let shards: Vec<i32> = (0..shard_count)
            .map(|i| {
                i32::try_from((start + u64::from(i)) % u64::from(shard_count)).unwrap_or(0)
            })
            .collect();

        let mut candidates: Vec<(i32, i64, String)> = Vec::new();
        for chunk in shards.chunks(concurrency) {
            let futs: Vec<_> = chunk
                .iter()
                .map(|&shard| self.exec(&self.select_ready, (pool, shard, per_shard)))
                .collect();
            for result in join_all(futs).await {
                let rows = result?;
                for row in collect_rows::<ReadyRow>(rows) {
                    candidates.push((row.priority, row.created_at, row.job_id));
                }
            }
        }
        candidates.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        candidates.dedup_by(|a, b| a.2 == b.2);
        let job_ids: Vec<String> = candidates
            .into_iter()
            .take(limit)
            .map(|(_, _, id)| id)
            .collect();
        let load_futs: Vec<_> = job_ids.iter().map(|id| self.load_job(id)).collect();
        let mut jobs = Vec::new();
        for result in join_all(load_futs).await {
            if let Some(job) = result? {
                if job.status == JobStatus::Queued && job.pool == pool {
                    jobs.push(job);
                }
            }
        }
        Ok(jobs)
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        let jobs = self.list_jobs(status_filter, 0, 100_000).await?;
        Ok(u64::try_from(jobs.len()).unwrap_or(u64::MAX))
    }

    async fn count_jobs_for_task(
        &self,
        task_name: &str,
        status: Option<JobStatus>,
    ) -> Result<u64> {
        let jobs = self.list_jobs(status, 0, 100_000).await?;
        let count = jobs.iter().filter(|j| j.task_name == task_name).count();
        Ok(u64::try_from(count).unwrap_or(u64::MAX))
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        let queued = self.count_jobs_for_task(task_name, Some(JobStatus::Queued)).await?;
        let running = self
            .count_jobs_for_task(task_name, Some(JobStatus::Running))
            .await?;
        Ok(u32::try_from(queued.saturating_add(running)).unwrap_or(u32::MAX))
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        let rows = self.exec(&self.select_idempotency, (key,)).await?;
        Ok(maybe_first_row::<IdRow>(rows).map(|r| r.job_id))
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        self.exec(
            &self.insert_run,
            (
                run.run_id.as_str(),
                run.job_id.as_str(),
                run.task_name.as_str(),
                run.attempt,
                run.status.to_string(),
                to_millis(run.started_at),
                run.finished_at.map(to_millis),
                run.duration_ms,
                run.error_message.as_deref(),
            ),
        )
        .await?;
        self.exec(
            &self.insert_run_by_job,
            (
                run.job_id.as_str(),
                to_millis(run.started_at),
                run.run_id.as_str(),
            ),
        )
        .await?;
        Ok(())
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        let rows = self.exec(&self.select_run, (run_id,)).await?;
        Ok(maybe_first_row::<RunRow>(rows).map(run_from_row).transpose()?)
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let Some(job_id) = job_id_filter else {
            return Ok(vec![]);
        };
        let fetch = offset.saturating_add(limit).max(1);
        let rows = self
            .exec(
                &self.select_run_by_job,
                (job_id, i32::try_from(fetch).unwrap_or(i32::MAX)),
            )
            .await?;
        let mut runs = Vec::new();
        for id in collect_rows::<RunIdRow>(rows) {
            if let Some(run) = self.get_run(&id.run_id).await? {
                runs.push(run);
            }
        }
        runs.sort_by_key(|r| r.started_at);
        Ok(runs.into_iter().skip(offset).take(limit).collect())
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        let finished_at = to_millis(Utc::now());
        self.exec(
            &self.update_run_finish,
            (
                status.to_string(),
                finished_at,
                duration_ms,
                error_message.as_deref(),
                run_id,
            ),
        )
        .await?;
        Ok(())
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        let runs = self.list_runs(job_id_filter, 0, 100_000).await?;
        Ok(u64::try_from(runs.len()).unwrap_or(u64::MAX))
    }

    async fn count_runs_since(&self, _since: DateTime<Utc>) -> Result<u64> {
        Ok(0)
    }

    async fn task_run_stats(&self, _task_name: &str) -> Result<TaskRunStats> {
        Ok(TaskRunStats {
            runs_total: 0,
            success_count: 0,
        })
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        let rows = self.exec(&self.select_task_config, (task_name,)).await?;
        let Some(row) = maybe_first_row::<TaskConfigRow>(rows) else {
            return Ok(None);
        };
        Ok(Some(TaskConfig {
            task_name: row.task_name,
            priority: row.priority,
            pool: row.pool,
            retry_policy: serde_json::from_str(&row.retry_policy_json)?,
            rate_limit_policy: serde_json::from_str(&row.rate_limit_policy_json)?,
            idempotency_mode: row
                .idempotency_mode
                .as_deref()
                .and_then(IdempotencyMode::parse),
            updated_at: from_millis(row.updated_at),
        }))
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        let mode = config.idempotency_mode.map(|m| match m {
            IdempotencyMode::Lwt => "lwt",
            IdempotencyMode::None => "none",
        });
        self.exec(
            &self.insert_task_config,
            (
                config.task_name.as_str(),
                config.priority,
                config.pool.as_str(),
                serde_json::to_string(&config.retry_policy)?,
                serde_json::to_string(&config.rate_limit_policy)?,
                mode,
                to_millis(config.updated_at),
            ),
        )
        .await?;
        Ok(())
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        let now_ms = to_millis(Utc::now());
        let expires_at = to_millis(Utc::now() + Duration::seconds(ttl_secs));
        let lease_id = Uuid::new_v4().to_string();
        let insert = self
            .exec(
                &self.insert_lease,
                (job_id, lease_id.as_str(), worker_id, expires_at),
            )
            .await?;
        if lwt_applied(insert) {
            self.record_lease(job_id, &lease_id, expires_at).await?;
            return Ok(Some(lease_id));
        }
        let steal = self
            .exec(
                &self.steal_lease,
                (
                    lease_id.as_str(),
                    worker_id,
                    expires_at,
                    job_id,
                    now_ms,
                ),
            )
            .await?;
        if lwt_applied(steal) {
            self.record_lease(job_id, &lease_id, expires_at).await?;
            return Ok(Some(lease_id));
        }
        Ok(None)
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        let Some(job_id) = self.job_id_for_lease(lease_id).await? else {
            return Ok(());
        };
        let expires_at = to_millis(Utc::now() + Duration::seconds(ttl_secs));
        self.exec(&self.update_lease_ttl, (expires_at, job_id.as_str()))
            .await?;
        self.insert_expiry_index(&job_id, lease_id, expires_at)
            .await?;
        Ok(())
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        let Some(job_id) = self.job_id_for_lease(lease_id).await? else {
            return Ok(());
        };
        if let Some(lease) = self.load_lease(&job_id).await? {
            let bucket = expiry_bucket(lease.expires_at / 1000);
            let _ = self
                .exec(
                    &self.delete_lease_expiry,
                    (bucket, lease.expires_at, job_id.as_str()),
                )
                .await;
        }
        self.exec(&self.delete_lease, (job_id.as_str(),)).await?;
        self.exec(&self.delete_lease_by_id, (lease_id,)).await?;
        Ok(())
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        let now_ms = to_millis(Utc::now());
        let bucket = expiry_bucket(now_ms / 1000);
        let mut pairs = Vec::new();
        for b in [bucket - 1, bucket, bucket + 1] {
            if b < 0 {
                continue;
            }
            let rows = self.exec(&self.select_lease_expiry, (b,)).await?;
            for row in collect_rows::<LeaseExpiryRow>(rows) {
                if row.expires_at <= now_ms {
                    pairs.push((row.lease_id, row.job_id));
                }
            }
        }
        Ok(pairs)
    }
}

impl ScyllaQueueBackend {
    async fn insert_expiry_index(
        &self,
        job_id: &str,
        lease_id: &str,
        expires_at_ms: i64,
    ) -> Result<()> {
        let bucket = expiry_bucket(expires_at_ms / 1000);
        self.exec(
            &self.insert_lease_expiry,
            (bucket, expires_at_ms, job_id, lease_id),
        )
        .await?;
        Ok(())
    }

    async fn record_lease(
        &self,
        job_id: &str,
        lease_id: &str,
        expires_at_ms: i64,
    ) -> Result<()> {
        self.exec(&self.insert_lease_by_id, (lease_id, job_id))
            .await?;
        self.insert_expiry_index(job_id, lease_id, expires_at_ms)
            .await?;
        Ok(())
    }

    async fn job_id_for_lease(&self, lease_id: &str) -> Result<Option<String>> {
        let rows = self.exec(&self.select_lease_by_id, (lease_id,)).await?;
        Ok(maybe_first_row::<IdRow>(rows).map(|r| r.job_id))
    }

    async fn load_lease(&self, job_id: &str) -> Result<Option<LeaseRow>> {
        let rows = self.exec(&self.select_lease, (job_id,)).await?;
        Ok(maybe_first_row::<LeaseRow>(rows))
    }
}

impl fmt::Debug for ScyllaQueueBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScyllaQueueBackend")
            .field("keyspace", &self.keyspace)
            .field("ready_shard_count", &self.ready_shard_count)
            .finish_non_exhaustive()
    }
}

fn parse_job_status(s: &str) -> Result<JobStatus> {
    match s {
        "queued" => Ok(JobStatus::Queued),
        "running" => Ok(JobStatus::Running),
        "success" => Ok(JobStatus::Success),
        "failed" => Ok(JobStatus::Failed),
        "canceled" => Ok(JobStatus::Canceled),
        other => Err(BosonError::Backend(format!("unknown job status: {other}"))),
    }
}

fn parse_run_status(s: &str) -> Result<RunStatus> {
    match s {
        "running" => Ok(RunStatus::Running),
        "success" => Ok(RunStatus::Success),
        "failed" => Ok(RunStatus::Failed),
        "canceled" => Ok(RunStatus::Canceled),
        "timeout" => Ok(RunStatus::Timeout),
        other => Err(BosonError::Backend(format!("unknown run status: {other}"))),
    }
}

fn run_from_row(row: RunRow) -> Result<Run> {
    Ok(Run {
        run_id: row.run_id,
        job_id: row.job_id,
        task_name: row.task_name,
        attempt: row.attempt,
        status: parse_run_status(&row.status)?,
        started_at: from_millis(row.started_at),
        finished_at: row.finished_at.map(from_millis),
        duration_ms: row.duration_ms,
        error_message: row.error_message,
    })
}

fn lwt_applied(result: scylla::response::query_result::QueryResult) -> bool {
    use scylla::deserialize::row::ColumnIterator;
    use scylla::deserialize::value::DeserializeValue;

    // Scylla LWT responses always include `[applied]` plus primary-key / IF columns.
    // Derive-based row types reject unknown columns, so read the first column only.
    let Ok(rows) = result.into_rows_result() else {
        return false;
    };
    let Ok(Some(mut cols)) = rows.maybe_first_row::<ColumnIterator<'_, '_>>() else {
        return false;
    };
    let Some(Ok(first)) = cols.next() else {
        return false;
    };
    bool::deserialize(first.spec.typ(), first.slice).unwrap_or(false)
}

fn maybe_first_row<R>(result: scylla::response::query_result::QueryResult) -> Option<R>
where
    R: for<'frame> scylla::deserialize::row::DeserializeRow<'frame, 'frame>,
{
    result
        .into_rows_result()
        .ok()
        .and_then(|rows| rows.maybe_first_row::<R>().ok().flatten())
}

fn collect_rows<R>(result: scylla::response::query_result::QueryResult) -> Vec<R>
where
    R: for<'frame> scylla::deserialize::row::DeserializeRow<'frame, 'frame>,
{
    result
        .into_rows_result()
        .ok()
        .and_then(|rows| {
            rows.rows::<R>()
                .ok()
                .map(|iter| iter.filter_map(std::result::Result::ok).collect())
        })
        .unwrap_or_default()
}

// Silence unused import warnings for Hash/Hasher if any.
#[allow(dead_code)]
fn _hash_str(s: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    h.finish()
}
