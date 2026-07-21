//! HTTP admin enqueue benchmark (BM-B7).

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use axum::{
    body::Body,
    extract::FromRef,
    http::{Request, StatusCode},
    Router,
};
use boson_axum::{boson_router, BosonState, NEST_PATH};
use boson_backend_mem::MemQueueBackend;
use boson_runtime::{Boson, TaskRegistry};
use boson_telemetry::{install_ops_log, NoOpsLog};
use boson_testkit::{fixtures::register_noop_task, StubExecutionContextFactory};
use http_body_util::BodyExt;
use tower::ServiceExt;

use crate::report::ReportMetrics;
use crate::stats::MetricStats;

#[derive(Clone)]
struct AppState {
    boson: BosonState,
}

impl FromRef<AppState> for BosonState {
    fn from_ref(state: &AppState) -> Self {
        state.boson.clone()
    }
}

/// Run HTTP enqueue benchmark and return timing metrics.
pub async fn run_http_enqueue(ops: u32) -> Result<ReportMetrics> {
    let _ = boson_backend_mem::install_default_mem_backend();
    install_ops_log(Arc::new(NoOpsLog));

    let mut registry = TaskRegistry::new();
    register_noop_task(&mut registry, "noop");
    let registry = Arc::new(registry);
    let backend = Arc::new(MemQueueBackend::new());
    let (boson, _manual) = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(StubExecutionContextFactory)
        .registry(registry)
        .without_worker()
        .build_manual()?;
    let boson = Arc::new(boson);
    let state = AppState {
        boson: BosonState::new(Arc::clone(&boson)),
    };
    let router = Router::new()
        .nest(NEST_PATH, boson_router::<AppState>())
        .with_state(state);

    let mut samples = Vec::with_capacity(ops as usize);
    for _ in 0..ops {
        let body = serde_json::json!({
            "task_name": "noop",
            "actor_json": {"System": {"operation": "bench"}},
            "params_json": {},
        });
        let req = Request::builder()
            .method("POST")
            .uri(format!("{NEST_PATH}/jobs/enqueue"))
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))?;
        let start = Instant::now();
        let response = router.clone().oneshot(req).await?;
        if response.status() != StatusCode::OK {
            anyhow::bail!("HTTP enqueue returned {}", response.status());
        }
        let _body = response.into_body().collect().await?;
        samples.push(start.elapsed().as_secs_f64() * 1000.0);
    }

    let metric_stats = MetricStats::summarize(samples);
    Ok(ReportMetrics {
        enqueue_ms: Some(metric_stats),
        p99_ms: Some(metric_stats.p99),
        ..Default::default()
    })
}
