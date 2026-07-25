//! Mount the Boson HTTP admin API on Axum with optional admin auth.
//!
//! Run: `cargo run -p uf-boson --example axum_admin --features mem,axum`
//!
//! Auth (recommended for any reachable port):
//! - `BOSON_REQUIRE_ADMIN_AUTH=1`
//! - `BOSON_ADMIN_TOKEN=lab-token` (example verifier header `x-boson-admin-token`)
//!
//! Then:
//! ```bash
//! curl -X POST http://127.0.0.1:3000/api/boson/jobs/enqueue \
//!   -H 'Content-Type: application/json' \
//!   -H 'x-boson-admin-token: lab-token' \
//!   -d '{"task_name":"echo"}'
//! ```
//!
//! HTTP enqueue persists a non-System actor (`Service/boson_api`). Set `BOSON_EXAMPLE_SERVE=1`
//! to keep listening (default exits after bind for CI).

#![allow(clippy::print_stdout)] // Examples print status to the console.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{extract::FromRef, Router};
use boson::prelude::Result as BosonResult;
use boson::{
    boson_router, Boson, BosonState, ExecutionContext, JsonExecutionContextFactory,
    MemQueueBackend, StaticTokenAdminAuth, TaskDescriptor, TaskRegistry, NEST_PATH,
};

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
    let desc: &'static TaskDescriptor = Box::leak(Box::new(TaskDescriptor::new("echo", echo_task)));
    registry.register(desc);

    let boson = Arc::new(
        Boson::builder()
            .queue_backend(Arc::new(MemQueueBackend::new()))
            .execution_context_factory(JsonExecutionContextFactory)
            .registry(Arc::new(registry))
            .build()?,
    );

    let mut builder = BosonState::builder(Arc::clone(&boson));
    if let Ok(token) = std::env::var("BOSON_ADMIN_TOKEN") {
        if !token.is_empty() {
            builder = builder
                .admin_auth(Arc::new(StaticTokenAdminAuth::new(token)))
                .require_admin_auth(true);
        }
    }
    let boson_state = builder.build().map_err(anyhow::Error::msg)?;

    let app = Router::new()
        .nest(NEST_PATH, boson_router())
        .with_state(AppState { boson: boson_state });

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!("listening on http://127.0.0.1:3000{NEST_PATH}");
    // Default: bind-only smoke so CI `cargo run --example` exits. Set BOSON_EXAMPLE_SERVE=1 to keep serving.
    if std::env::var_os("BOSON_EXAMPLE_SERVE").is_none() {
        return Ok(());
    }
    axum::serve(listener, app).await?;
    Ok(())
}
