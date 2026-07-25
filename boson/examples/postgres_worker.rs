//! Remote-worker process — claims and runs jobs from a shared `PostgreSQL` database.
//!
//! Pair with `postgres_enqueue` against the same `DATABASE_URL`.
//!
//! ```bash
//! export DATABASE_URL=postgres://localhost/boson
//!
//! # Terminal 1 — worker A
//! BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example postgres_worker --features postgres
//!
//! # Terminal 2 — worker B (optional)
//! BOSON_WORKER_ID=worker-b cargo run -p uf-boson --example postgres_worker --features postgres
//!
//! # Terminal 3 — enqueue
//! cargo run -p uf-boson --example postgres_enqueue --features postgres
//! ```
//!
//! Stop workers with Ctrl-C. For scripted smoke, set `BOSON_WORKER_RUN_SECS`.
//!
//! See the crate docs:
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::sync::Arc;
use std::time::Duration;

use boson::{Boson, JsonExecutionContextFactory, PostgresQueueBackend};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("BOSON_POSTGRES_URL"))
        .unwrap_or_else(|_| "postgres://localhost/boson".into());
    let worker_id = std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "postgres-worker-1".into());
    let lease_ttl: i64 = std::env::var("BOSON_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let backend = PostgresQueueBackend::connect(&url).await?;
    let _boson = Boson::builder()
        .queue_backend(Arc::new(backend))
        .execution_context_factory(JsonExecutionContextFactory)
        .worker_id(worker_id.clone())
        .lease_ttl_secs(lease_ttl)
        .auto_registry()
        .build()?;

    println!("worker {worker_id} listening (lease_ttl_secs={lease_ttl})");

    // Keep the process alive so the background worker loop can drain jobs.
    if let Ok(s) = std::env::var("BOSON_WORKER_RUN_SECS") {
        let run_secs: u64 = s
            .parse()
            .map_err(|e| anyhow::anyhow!("BOSON_WORKER_RUN_SECS: {e}"))?;
        tokio::time::sleep(Duration::from_secs(run_secs)).await;
        println!("worker exiting after {run_secs}s");
    } else {
        println!("Ctrl-C to stop");
        tokio::signal::ctrl_c().await?;
        println!("worker shutting down");
    }
    Ok(())
}
