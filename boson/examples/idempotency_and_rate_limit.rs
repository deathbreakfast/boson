//! Idempotency keys and enqueue rate limits.
//!
//! Run: `cargo run -p uf-boson --example idempotency_and_rate_limit --features mem`

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use boson::{
    Boson, BosonError, ExecutionContext, JsonExecutionContextFactory, MemQueueBackend,
    RateLimitPolicy, TaskConfig, TaskDescriptor, TaskRegistry,
};
use boson::prelude::Result as BosonResult;

fn noop_task(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = BosonResult<()>> + Send + 'static>> {
    Box::pin(async { Ok(()) })
}

fn register_task(registry: &mut TaskRegistry, name: &'static str) {
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::new(name, noop_task)));
    registry.register(desc);
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut registry = TaskRegistry::new();
    register_task(&mut registry, "limited");
    let registry = Arc::new(registry);

    let (boson, manual) = Boson::builder()
        .queue_backend(Arc::new(MemQueueBackend::new()))
        .execution_context_factory(JsonExecutionContextFactory)
        .registry(registry)
        .without_worker()
        .build_manual()?;

    // Idempotency: second enqueue with the same key returns the original job id.
    let key = Some("checkout-42".into());
    let first = boson
        .enqueue(
            "limited",
            serde_json::json!({"System": {}}),
            serde_json::json!({}),
            key.clone(),
        )
        .await?;
    let second = boson
        .enqueue(
            "limited",
            serde_json::json!({"System": {}}),
            serde_json::json!({}),
            key,
        )
        .await?;
    assert_eq!(first, second, "idempotent enqueue should reuse job id");
    println!("idempotency: both enqueues returned job {first}");

    // Drain the idempotent job so rate-limit counts start clean.
    while manual.try_run_next().await {}

    // Rate limit: cap in-flight jobs for this task at 1.
    let mut config = TaskConfig::default_for("limited");
    config.rate_limit_policy = RateLimitPolicy {
        max_in_flight: 1,
        max_enqueue_per_second: 0,
    };
    boson.upsert_task_config(config).await?;

    let _ = boson
        .enqueue(
            "limited",
            serde_json::json!({"System": {}}),
            serde_json::json!({}),
            None,
        )
        .await?;
    match boson
        .enqueue(
            "limited",
            serde_json::json!({"System": {}}),
            serde_json::json!({}),
            None,
        )
        .await
    {
        Err(BosonError::RateLimited(_)) => println!("rate limit: second enqueue rejected as expected"),
        Ok(id) => anyhow::bail!("expected RateLimited, got job {id}"),
        Err(e) => anyhow::bail!("expected RateLimited, got {e}"),
    }

    Ok(())
}
