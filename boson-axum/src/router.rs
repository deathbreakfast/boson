//! Axum router for Boson HTTP API.

use axum::{
    extract::FromRef,
    routing::{get, post},
    Router,
};

use super::handlers;
use super::state::BosonState;

/// Nest path for the Boson API router (`/api/boson`).
///
/// Pass to [`Router::nest`](axum::Router::nest) with [`boson_router`]. See the crate-level
/// [Example](crate#example) and the [`boson`](https://docs.rs/uf-boson) crate
/// [§ 5](https://docs.rs/uf-boson/latest/boson/index.html#5-mount-http-admin-optional).
pub const NEST_PATH: &str = "/api/boson";

/// Create the Boson API router (mount at [`NEST_PATH`]).
///
/// Host apps nest this under [`NEST_PATH`] and provide [`BosonState`] via [`FromRef`].
/// Runnable: `cargo run -p uf-boson --example axum_admin --features mem,axum`.
pub fn boson_router<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
    BosonState: FromRef<S>,
{
    Router::new()
        .route("/jobs/enqueue", post(handlers::enqueue))
        .route("/jobs", get(handlers::list_jobs))
        .route("/jobs/{id}", get(handlers::get_job))
        .route("/jobs/{id}/cancel", post(handlers::cancel_job))
        .route("/tasks", get(handlers::list_tasks))
        .route("/tasks/{name}", get(handlers::get_task))
        .route(
            "/tasks/{name}/config",
            get(handlers::get_task_config).post(handlers::update_task_config),
        )
        .route(
            "/tasks/{name}/config/revisions",
            get(handlers::get_task_config_revisions),
        )
        .route("/runs", get(handlers::list_runs))
        .route("/runs/{id}", get(handlers::get_run))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use boson_backend_mem::MemQueueBackend;
    use boson_core::{ExecutionContext, ExecutionContextFactory, IdentityError};
    use boson_runtime::Boson;

    struct TestCtx {
        actor_json: serde_json::Value,
    }
    impl ExecutionContext for TestCtx {
        fn label(&self) -> &'static str {
            "test"
        }

        fn actor_json(&self) -> &serde_json::Value {
            &self.actor_json
        }
    }

    struct TestFactory;
    impl ExecutionContextFactory for TestFactory {
        fn build(
            &self,
            actor_json: &serde_json::Value,
        ) -> Result<Box<dyn ExecutionContext>, IdentityError> {
            Ok(Box::new(TestCtx {
                actor_json: actor_json.clone(),
            }))
        }
    }

    #[test]
    fn router_creation() {
        #[derive(Clone)]
        struct AppState {
            boson: BosonState,
        }
        impl FromRef<AppState> for BosonState {
            fn from_ref(state: &AppState) -> Self {
                state.boson.clone()
            }
        }
        let boson = Arc::new(
            Boson::builder()
                .queue_backend(Arc::new(MemQueueBackend::new()))
                .execution_context_factory(TestFactory)
                .without_worker()
                .build()
                .expect("build"),
        );
        let _router = boson_router::<AppState>();
        let _ = AppState {
            boson: BosonState::new(boson),
        };
    }
}
