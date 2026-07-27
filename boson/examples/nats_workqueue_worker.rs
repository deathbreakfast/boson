//! Remote-worker process — claims and runs jobs from a shared NATS `JetStream` `WorkQueue`.
//!
//! Not a `uf-boson` feature — depends on `boson-backend-nats` directly (path dev-dependency in
//! this crate; production apps add `boson-backend-nats` to `[dependencies]`). Set
//! `BOSON_NATS_QUEUE_MODE=workqueue` so [`boson_backend_nats::connect_auto`] selects the
//! `WorkQueue` stream backend instead of the default KV backend.
//!
//! **`WorkQueue` pool discovery is per-process** — `distinct_pools_queued` only reports pools
//! this backend instance has itself published to, so a worker in a *different* process from the
//! enqueue host never discovers pools on its own. Pin `BOSON_WORKER_POOLS` (comma-separated) to
//! every pool your tasks use — the `#[task]` default is `global` (see `remote_ping`).
//!
//! Pair with `nats_workqueue_enqueue` against the same `BOSON_NATS_URL`.
//!
//! ```bash
//! docker run -d --name boson-nats -p 4222:4222 nats:2.10 -js
//! export BOSON_NATS_URL=nats://127.0.0.1:4222
//! export BOSON_NATS_QUEUE_MODE=workqueue
//! export BOSON_WORKER_POOLS=global
//!
//! # Terminal 1 — worker A
//! BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example nats_workqueue_worker
//!
//! # Terminal 2 — worker B (optional)
//! BOSON_WORKER_ID=worker-b cargo run -p uf-boson --example nats_workqueue_worker
//!
//! # Terminal 3 — enqueue
//! cargo run -p uf-boson --example nats_workqueue_enqueue
//! ```
//!
//! Stop workers with Ctrl-C. For scripted smoke, set `BOSON_WORKER_RUN_SECS`.
//!
//! See the crate docs:
//! [Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#remote-worker-two-binaries).

#![allow(clippy::print_stdout)] // Examples print status to the console.

#[path = "shared/remote_ping.rs"]
mod remote_shared_task;

use std::time::Duration;

use boson::{Boson, JsonExecutionContextFactory};
use boson_backend_nats::connect_auto;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("BOSON_NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
    let worker_id = std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "nats-worker-1".into());
    let lease_ttl: i64 = std::env::var("BOSON_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let backend = connect_auto(&url).await?;
    let _boson = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(JsonExecutionContextFactory)
        .worker_id(worker_id.clone())
        .lease_ttl_secs(lease_ttl)
        .auto_registry()
        .build()?;

    println!("worker {worker_id} listening (url={url}, lease_ttl_secs={lease_ttl})");

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
