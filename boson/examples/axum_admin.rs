//! Mount the Boson HTTP admin API on Axum.
//!
//! Run: `cargo run -p boson --example axum_admin --features mem,axum`
//!
//! Then: `curl -X POST http://127.0.0.1:3000/api/boson/jobs/enqueue -H 'Content-Type: application/json' -d '{"task_name":"echo"}'`

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{extract::FromRef, Router};
use boson::{
    boson_router, Boson, BosonState, ExecutionContext, JsonExecutionContextFactory,
    MemQueueBackend, NEST_PATH, TaskDescriptor, TaskRegistry,
};
use boson::prelude::Result as BosonResult;

fn echo_task(
    _ctx: Box<dyn ExecutionContext>,
    _params: serde_json::Value,
) -> Pin<Box<dyn Future<Output = BosonResult<()>> + Send + 'static>> {
    Box::pin(async { Ok(()) })
}

#[derive(Clone)]
struct AppState {
    boson: BosonState,
}

impl FromRef<AppState> for BosonState {
    fn from_ref(state: &AppState) -> Self {
        state.boson.clone()
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut registry = TaskRegistry::new();
    let desc: &'static TaskDescriptor =
        Box::leak(Box::new(TaskDescriptor::new("echo", echo_task)));
    registry.register(desc);

    let boson = Arc::new(
        Boson::builder()
            .queue_backend(Arc::new(MemQueueBackend::new()))
            .execution_context_factory(JsonExecutionContextFactory)
            .registry(Arc::new(registry))
            .build()?,
    );

    let app = Router::new()
        .nest(NEST_PATH, boson_router())
        .with_state(AppState {
            boson: BosonState::new(boson),
        });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://127.0.0.1:3000{NEST_PATH}");
    // Default: bind-only smoke so CI `cargo run --example` exits. Set BOSON_EXAMPLE_SERVE=1 to keep serving.
    if std::env::var_os("BOSON_EXAMPLE_SERVE").is_none() {
        return Ok(());
    }
    axum::serve(listener, app).await?;
    Ok(())
}
