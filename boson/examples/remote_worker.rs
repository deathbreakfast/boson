//! Mode 2 worker process — claims and runs jobs from a shared `SQLite` database.
//!
//! Pair with `remote_enqueue` against the same `BOSON_SQLITE_PATH`.
//!
//! ```bash
//! export BOSON_SQLITE_PATH=/tmp/boson-remote.db
//! cargo run -p uf-boson --example remote_worker --features sqlite &
//! cargo run -p uf-boson --example remote_enqueue --features sqlite
//! ```
//!
//! See the crate docs:
//! [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::sync::Arc;
use std::time::Duration;

use boson::{Boson, JsonExecutionContextFactory, SqliteQueueBackend};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
    let worker_id = std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "remote-worker-1".into());
    let lease_ttl: i64 = std::env::var("BOSON_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let backend = SqliteQueueBackend::new(&path).await?;
    let _boson = Boson::builder()
        .queue_backend(Arc::new(backend))
        .execution_context_factory(JsonExecutionContextFactory)
        .worker_id(worker_id.clone())
        .lease_ttl_secs(lease_ttl)
        .auto_registry()
        .build()?;

    println!("worker {worker_id} listening (path={path}, lease_ttl_secs={lease_ttl})");

    // Keep the process alive so the background worker loop can drain jobs.
    let run_secs: u64 = std::env::var("BOSON_WORKER_RUN_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);
    tokio::time::sleep(Duration::from_secs(run_secs)).await;
    println!("worker exiting after {run_secs}s");
    Ok(())
}
