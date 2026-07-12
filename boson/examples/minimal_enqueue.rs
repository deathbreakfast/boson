//! Manual task registration without the `#[task]` macro.
//!
//! Run: `cargo run -p boson --example minimal_enqueue --features mem`

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use boson::{
    Boson, ExecutionContext, JsonExecutionContextFactory, MemQueueBackend, TaskDescriptor,
    TaskRegistry,
};
use boson::prelude::Result as BosonResult;

fn echo_task(
    ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = BosonResult<()>> + Send + 'static>> {
    Box::pin(async move {
        println!("task ran (actor={})", ctx.label());
        Ok(())
    })
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut registry = TaskRegistry::new();
    let desc: &'static TaskDescriptor =
        Box::leak(Box::new(TaskDescriptor::new("echo", echo_task)));
    registry.register(desc);

    let boson = Boson::builder()
        .queue_backend(Arc::new(MemQueueBackend::new()))
        .execution_context_factory(JsonExecutionContextFactory)
        .registry(Arc::new(registry))
        .build()?;

    let job_id = boson
        .enqueue(
            "echo",
            serde_json::json!({"System": {"operation": "demo"}}),
            serde_json::json!({}),
            None,
        )
        .await?;
    println!("enqueued job {job_id}");
    Ok(())
}
