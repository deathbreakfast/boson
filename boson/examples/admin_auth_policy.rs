//! Fail-closed Boson admin auth — every `/api/boson` request must present a valid token.
//!
//! Unlike `axum_admin` (auth optional via `BOSON_ADMIN_TOKEN`), this example always installs
//! `StaticTokenAdminAuth` and sets `require_admin_auth(true)` — no env opt-out — then proves the
//! policy inline before it ever binds a socket:
//! - a request without `x-boson-admin-token` is rejected with `401 Unauthorized`
//! - a request with the correct token succeeds with `200 OK`
//!
//! Run: `cargo run -p uf-boson --example admin_auth_policy --features mem,axum`
//!
//! Set `BOSON_EXAMPLE_SERVE=1` to keep listening after the proof, then try it yourself:
//! ```bash
//! curl -i http://127.0.0.1:3000/api/boson/jobs/enqueue \
//!   -X POST -H 'Content-Type: application/json' -d '{"task_name":"echo"}'
//! # -> 401 Unauthorized (no token)
//!
//! curl -i http://127.0.0.1:3000/api/boson/jobs/enqueue \
//!   -X POST -H 'Content-Type: application/json' -H 'x-boson-admin-token: lab-token' \
//!   -d '{"task_name":"echo"}'
//! # -> 200 OK
//! ```
//!
//! **Production:** prefer a host-owned verifier and `BOSON_REQUIRE_ADMIN_AUTH=1` over a static
//! shared token — see repository [`SECURITY.md`](../../SECURITY.md).

#![allow(clippy::print_stdout)] // Examples print status to the console.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::body::Body;
use axum::extract::FromRef;
use axum::http::{Request, StatusCode};
use axum::Router;
use boson::prelude::Result as BosonResult;
use boson::{
    boson_router, Boson, BosonState, ExecutionContext, JsonExecutionContextFactory,
    MemQueueBackend, StaticTokenAdminAuth, TaskDescriptor, TaskRegistry, NEST_PATH,
};
use tower::ServiceExt;

const ADMIN_TOKEN: &str = "lab-token";

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

fn enqueue_request(token: Option<&str>) -> anyhow::Result<Request<Body>> {
    let mut builder = Request::builder()
        .method("POST")
        .uri("/api/boson/jobs/enqueue")
        .header("content-type", "application/json");
    if let Some(token) = token {
        builder = builder.header("x-boson-admin-token", token);
    }
    Ok(builder.body(Body::from(r#"{"task_name":"echo"}"#))?)
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

    // Fail-closed: always install a verifier and require it — no env opt-out.
    let boson_state = BosonState::builder(Arc::clone(&boson))
        .admin_auth(Arc::new(StaticTokenAdminAuth::new(ADMIN_TOKEN)))
        .require_admin_auth(true)
        .build()
        .map_err(anyhow::Error::msg)?;

    let app = Router::new()
        .nest(NEST_PATH, boson_router())
        .with_state(AppState { boson: boson_state });

    let unauthorized = app.clone().oneshot(enqueue_request(None)?).await?;
    println!("no token -> {}", unauthorized.status());
    anyhow::ensure!(
        unauthorized.status() == StatusCode::UNAUTHORIZED,
        "expected 401 without a token, got {}",
        unauthorized.status()
    );

    let authorized = app
        .clone()
        .oneshot(enqueue_request(Some(ADMIN_TOKEN))?)
        .await?;
    println!("valid token -> {}", authorized.status());
    anyhow::ensure!(
        authorized.status() == StatusCode::OK,
        "expected 200 with a valid token, got {}",
        authorized.status()
    );

    println!("fail-closed admin auth proven: 401 without token, 200 with token");

    // Default: proof-only smoke so CI `cargo run --example` exits. Set BOSON_EXAMPLE_SERVE=1 to
    // keep serving for manual curl testing.
    if std::env::var_os("BOSON_EXAMPLE_SERVE").is_none() {
        return Ok(());
    }

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000").await?;
    println!(
        "listening on http://127.0.0.1:3000{NEST_PATH} (send x-boson-admin-token: {ADMIN_TOKEN})"
    );
    axum::serve(listener, app).await?;
    Ok(())
}
