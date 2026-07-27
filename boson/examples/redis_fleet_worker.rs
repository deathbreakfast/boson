//! Remote-worker process — claims and runs jobs from a shared Redis fleet queue.
//!
//! Not a `uf-boson` feature — depends on `boson-backend-redis` directly (path dev-dependency in
//! this crate; production apps add `boson-backend-redis` to `[dependencies]`).
//!
//! Pair with `redis_fleet_enqueue` against the same `BOSON_REDIS_URL`.
//!
//! ```bash
//! docker run -d --name boson-redis -p 6379:6379 redis:7
//! export BOSON_REDIS_URL=redis://127.0.0.1:6379
//!
//! # Terminal 1 — worker A
//! BOSON_WORKER_ID=worker-a cargo run -p uf-boson --example redis_fleet_worker
//!
//! # Terminal 2 — worker B (optional)
//! BOSON_WORKER_ID=worker-b cargo run -p uf-boson --example redis_fleet_worker
//!
//! # Terminal 3 — enqueue
//! cargo run -p uf-boson --example redis_fleet_enqueue
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

use boson::{Boson, JsonExecutionContextFactory};
use boson_backend_redis::{RedisQueueBackend, RedisQueueConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let url = std::env::var("BOSON_REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
    let worker_id = std::env::var("BOSON_WORKER_ID").unwrap_or_else(|_| "redis-worker-1".into());
    let lease_ttl: i64 = std::env::var("BOSON_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(30);

    let backend = RedisQueueBackend::connect(RedisQueueConfig {
        url: url.clone(),
        ..RedisQueueConfig::default()
    })
    .await?;
    let _boson = Boson::builder()
        .queue_backend(Arc::new(backend))
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
