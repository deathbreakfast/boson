//! CQL schema bootstrap for the Boson queue.

use scylla::client::session::Session;

use crate::error_map::{into_result, map_err};

#[allow(clippy::too_many_lines)]
pub async fn ensure_schema(
    session: &Session,
    keyspace: &str,
    replication_factor: u32,
) -> boson_core::Result<()> {
    let rf = replication_factor.max(1);
    into_result(
        session
            .query_unpaged(
                format!(
                    "CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH replication = \
                     {{'class': 'SimpleStrategy', 'replication_factor': {rf}}}"
                ),
                &[],
            )
            .await,
    )?;
    session
        .use_keyspace(keyspace, false)
        .await
        .map_err(map_err)?;

    for ddl in [
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_job (
                job_id text PRIMARY KEY,
                task_name text,
                actor_json text,
                params_json text,
                priority int,
                pool text,
                status text,
                idempotency_key text,
                created_at bigint,
                signature_hash bigint,
                attempt int
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_ready (
                pool text,
                shard int,
                priority int,
                created_at bigint,
                job_id text,
                PRIMARY KEY ((pool, shard), priority, created_at, job_id)
            ) WITH CLUSTERING ORDER BY (priority ASC, created_at ASC, job_id ASC)"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_lease (
                job_id text PRIMARY KEY,
                lease_id text,
                worker_id text,
                expires_at bigint
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_lease_by_id (
                lease_id text PRIMARY KEY,
                job_id text
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_lease_by_expiry (
                bucket int,
                expires_at bigint,
                job_id text,
                lease_id text,
                PRIMARY KEY ((bucket), expires_at, job_id)
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_idempotency (
                idempotency_key text PRIMARY KEY,
                job_id text
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_run (
                run_id text PRIMARY KEY,
                job_id text,
                task_name text,
                attempt int,
                status text,
                started_at bigint,
                finished_at bigint,
                duration_ms bigint,
                error_message text
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_task_config (
                task_name text PRIMARY KEY,
                priority int,
                pool text,
                retry_policy_json text,
                rate_limit_policy_json text,
                idempotency_mode text,
                updated_at bigint
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_job_by_status (
                status text,
                created_at bigint,
                job_id text,
                PRIMARY KEY ((status), created_at, job_id)
            )"
        ),
        format!(
            "CREATE TABLE IF NOT EXISTS {keyspace}.boson_run_by_job (
                job_id text,
                started_at bigint,
                run_id text,
                PRIMARY KEY ((job_id), started_at, run_id)
            )"
        ),
    ] {
        into_result(session.query_unpaged(ddl, &[]).await)?;
    }
    Ok(())
}
