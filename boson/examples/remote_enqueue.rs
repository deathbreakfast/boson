//! Mode 2 enqueue-only process — writes jobs to a shared `SQLite` database.
//!
//! Pair with `remote_worker` against the same `BOSON_SQLITE_PATH`.
//!
//! ```bash
//! export BOSON_SQLITE_PATH=/tmp/boson-remote.db
//! cargo run -p uf-boson --example remote_worker --features sqlite &
//! cargo run -p uf-boson --example remote_enqueue --features sqlite
//! ```
//!
//! See the crate docs:
//! [Mode 2](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::sync::Arc;
use std::time::Duration;

use boson::{configure, Boson, JsonExecutionContextFactory, SqliteQueueBackend};
use remote_shared_task::{RemotePing, RemotePingParams};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let path = std::env::var("BOSON_SQLITE_PATH").unwrap_or_else(|_| "/tmp/boson-remote.db".into());
    let backend = SqliteQueueBackend::new(&path).await?;

    let boson = Boson::builder()
        .queue_backend(Arc::new(backend))
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .without_worker()
        .build()?;
    configure(boson);

    let job_id = RemotePing::send_with(
        serde_json::json!({"System": {"operation": "remote-demo"}}),
        RemotePingParams {
            message: "hello from enqueue host".into(),
        },
    )
    .await?;
    println!("enqueued job_id={job_id} (path={path})");

    // Give the worker a moment when both are started together in scripts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
