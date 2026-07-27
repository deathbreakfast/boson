//! Remote-worker enqueue-only process — writes jobs to a shared Redis fleet queue.
//!
//! Not a `uf-boson` feature — depends on `boson-backend-redis` directly (path dev-dependency in
//! this crate; production apps add `boson-backend-redis` to `[dependencies]`).
//!
//! Pair with `redis_fleet_worker` against the same `BOSON_REDIS_URL`.
//!
//! ```bash
//! docker run -d --name boson-redis -p 6379:6379 redis:7
//! export BOSON_REDIS_URL=redis://127.0.0.1:6379
//!
//! # Terminal 1 — worker first
//! cargo run -p uf-boson --example redis_fleet_worker
//!
//! # Terminal 2 — enqueue
//! cargo run -p uf-boson --example redis_fleet_enqueue
//! ```
//!
//! See the crate docs:
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::sync::Arc;
use std::time::Duration;

use boson::{configure, Boson, JsonExecutionContextFactory};
use boson_backend_redis::{RedisQueueBackend, RedisQueueConfig};
use remote_shared_task::{RemotePing, RemotePingParams};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("BOSON_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let backend = RedisQueueBackend::connect(RedisQueueConfig {
        url: url.clone(),
        ..RedisQueueConfig::default()
    })
    .await?;

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
            message: "hello from redis fleet enqueue host".into(),
        },
    )
    .await?;
    println!("enqueued job_id={job_id} (url={url})");

    // Give the worker a moment when both are started together in scripts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
