//! Remote-worker enqueue-only process — writes jobs to a shared NATS `JetStream` `WorkQueue`.
//!
//! Not a `uf-boson` feature — depends on `boson-backend-nats` directly (path dev-dependency in
//! this crate; production apps add `boson-backend-nats` to `[dependencies]`). Set
//! `BOSON_NATS_QUEUE_MODE=workqueue` so [`boson_backend_nats::connect_auto`] selects the
//! `WorkQueue` stream backend instead of the default KV backend.
//!
//! Pair with `nats_workqueue_worker` against the same `BOSON_NATS_URL` — see that example for why
//! the worker needs `BOSON_WORKER_POOLS` pinned in `WorkQueue` mode.
//!
//! ```bash
//! docker run -d --name boson-nats -p 4222:4222 nats:2.10 -js
//! export BOSON_NATS_URL=nats://127.0.0.1:4222
//! export BOSON_NATS_QUEUE_MODE=workqueue
//!
//! # Terminal 1 — worker first
//! BOSON_WORKER_POOLS=global cargo run -p uf-boson --example nats_workqueue_worker
//!
//! # Terminal 2 — enqueue
//! cargo run -p uf-boson --example nats_workqueue_enqueue
//! ```
//!
//! See the crate docs:
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::time::Duration;

use boson::{configure, Boson, JsonExecutionContextFactory};
use boson_backend_nats::connect_auto;
use remote_shared_task::{RemotePing, RemotePingParams};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("BOSON_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let backend = connect_auto(&url).await?;

    let boson = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .without_worker()
        .build()?;
    configure(boson);

    let job_id = RemotePing::send_with(
        serde_json::json!({"System": {"operation": "remote-demo"}}),
        RemotePingParams {
            message: "hello from nats workqueue enqueue host".into(),
        },
    )
    .await?;
    println!("enqueued job_id={job_id} (url={url})");

    // Give the worker a moment when both are started together in scripts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    Ok(())
}
