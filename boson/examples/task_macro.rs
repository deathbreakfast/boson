//! First boot plus first `#[task]` — boot a worker, enqueue, and run one job in-process.
//!
//! 1. Define a handler with [`task`].
//! 2. Boot [`Boson`] with [`JsonExecutionContextFactory`] and [`BosonBuilder::auto_registry`].
//! 3. Call [`configure`] so [`Greet::send_with`] can enqueue.
//! 4. Drive [`ManualWorker::try_run_next`] to execute the job.
//!
//! Run: `cargo run -p uf-boson --example task_macro --features mem`

#![allow(clippy::print_stdout)] // Examples print status to the console.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use boson::{configure, task, Boson, JsonExecutionContextFactory, MemQueueBackend};

static GREET_RUNS: AtomicUsize = AtomicUsize::new(0);

#[task(name = "greet")]
async fn greet(ctx: Box<dyn boson::ExecutionContext>, name: String) -> boson_core::Result<()> {
    GREET_RUNS.fetch_add(1, Ordering::SeqCst);
    println!("greet {} (actor={})", name, ctx.label());
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (boson, manual) = Boson::builder()
        .queue_backend(Arc::new(MemQueueBackend::new()))
        .execution_context_factory(JsonExecutionContextFactory)
        .auto_registry()
        .without_worker()
        .build_manual()?;

    configure(boson.clone());

    Greet::send_with(
        serde_json::json!({"System": {"operation": "demo"}}),
        GreetParams {
            name: "world".into(),
        },
    )
    .await?;

    assert!(manual.try_run_next().await);
    assert_eq!(GREET_RUNS.load(Ordering::SeqCst), 1);
    Ok(())
}
