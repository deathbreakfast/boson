//! Remote-worker enqueue-only process — writes jobs to a shared `PostgreSQL` database.
//!
//! Pair with `postgres_worker` against the same `DATABASE_URL`.
//!
//! ```bash
//! export DATABASE_URL=postgres://localhost/boson
//!
//! # Terminal 1 — worker first
//! cargo run -p uf-boson --example postgres_worker --features postgres
//!
//! # Terminal 2 — enqueue
//! cargo run -p uf-boson --example postgres_enqueue --features postgres
//! ```
//!
//! See the crate docs:
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::sync::Arc;
use std::time::Duration;

use boson::{configure, Boson, JsonExecutionContextFactory, PostgresQueueBackend};
use remote_shared_task::{RemotePing, RemotePingParams};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("DATABASE_URL")
        .or_else(|_| std::env::var("BOSON_POSTGRES_URL"))
        .unwrap_or_else(|_| "postgres://localhost/boson".into());
    let backend = PostgresQueueBackend::connect(&url).await?;

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
            message: "hello from postgres enqueue host".into(),
        },
    )
    .await?;
    println!("enqueued job_id={job_id}");

    // Give the worker a moment when both are started together in scripts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
