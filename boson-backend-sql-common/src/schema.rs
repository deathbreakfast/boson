//! Bootstrap SQL tables and indexes for the queue backend.

use boson_core::Result;

use crate::{SqlDialect, SqlQueueBackend};

const JOB_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS boson_job (
    job_id TEXT PRIMARY KEY,
    task_name TEXT NOT NULL,
    actor_json TEXT NOT NULL,
    params_json TEXT NOT NULL,
    priority INTEGER NOT NULL,
    pool TEXT NOT NULL,
    status TEXT NOT NULL,
    idempotency_key TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    signature_hash BIGINT NOT NULL,
    attempt INTEGER NOT NULL
)";

const RUN_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS boson_run (
    run_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    task_name TEXT NOT NULL,
    attempt INTEGER NOT NULL,
    status TEXT NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    finished_at TIMESTAMPTZ,
    duration_ms BIGINT,
    error_message TEXT
)";

const TASK_CONFIG_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS boson_task_config (
    task_name TEXT PRIMARY KEY,
    priority INTEGER NOT NULL,
    pool TEXT NOT NULL,
    retry_policy_json TEXT NOT NULL,
    rate_limit_policy_json TEXT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
)";

const LEASE_TABLE: &str = r"
CREATE TABLE IF NOT EXISTS boson_lease (
    lease_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL
)";

/// Create tables and indexes if missing.
pub async fn ensure_schema(backend: &SqlQueueBackend) -> Result<()> {
    let lock = backend.dialect() == SqlDialect::Postgres;
    if lock {
        backend.run_ddl("SELECT pg_advisory_lock(872349012)").await?;
    }
    let result = ensure_schema_tables(backend).await;
    if lock {
        backend.run_ddl("SELECT pg_advisory_unlock(872349012)").await?;
    }
    result
}

async fn ensure_schema_tables(backend: &SqlQueueBackend) -> Result<()> {
    for ddl in [JOB_TABLE, RUN_TABLE, TASK_CONFIG_TABLE, LEASE_TABLE] {
        backend.run_ddl(ddl).await?;
    }

    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_job_status ON boson_job (status)")
        .await?;
    backend
        .run_ddl(
            "CREATE INDEX IF NOT EXISTS boson_job_pool_queued ON boson_job (pool, priority, created_at) WHERE status = 'queued'",
        )
        .await?;
    backend
        .run_ddl(
            "CREATE INDEX IF NOT EXISTS boson_job_task_status ON boson_job (task_name, status)",
        )
        .await?;
    backend
        .run_ddl(
            "CREATE UNIQUE INDEX IF NOT EXISTS boson_job_idempotency_active ON boson_job (idempotency_key) WHERE idempotency_key IS NOT NULL AND status IN ('queued', 'running')",
        )
        .await?;

    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_run_job_id ON boson_run (job_id)")
        .await?;
    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_run_task_name ON boson_run (task_name)")
        .await?;
    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_run_started_at ON boson_run (started_at)")
        .await?;

    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_lease_job_id ON boson_lease (job_id)")
        .await?;
    backend
        .run_ddl("CREATE INDEX IF NOT EXISTS boson_lease_expires_at ON boson_lease (expires_at)")
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::SqlQueueBackend;

    #[tokio::test]
    async fn schema_idempotent_sqlite() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db");
        let url = format!("sqlite://{}?mode=rwc", path.display());
        let b1 = SqlQueueBackend::connect_sqlite(&url).await.unwrap();
        let b2 = SqlQueueBackend::connect_sqlite(&url).await.unwrap();
        drop(b1);
        drop(b2);
    }
}
