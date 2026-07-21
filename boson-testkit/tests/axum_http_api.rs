//! HTTP integration tests for the Boson Axum API.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::print_stdout,
    clippy::print_stderr
)] // Integration-test helpers are not covered by clippy.toml allow-*-in-tests.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::FromRef,
    http::{Request, StatusCode},
    Router,
};
use boson_axum::{boson_router, BosonState, NEST_PATH};
use boson_backend_mem::{install_default_mem_backend, MemQueueBackend};
use boson_runtime::{Boson, ManualWorker, TaskRegistry};
use boson_telemetry::{install_ops_log, NoOpsLog};
use boson_testkit::{
    fixtures::{register_noop_task, register_rate_limited_in_flight_task},
    StubExecutionContextFactory,
};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use tower::ServiceExt;

#[derive(Clone)]
struct AppState {
    boson: BosonState,
}

impl FromRef<AppState> for BosonState {
    fn from_ref(state: &AppState) -> Self {
        state.boson.clone()
    }
}

struct HttpTestApp {
    router: Router,
    boson: Arc<Boson>,
    manual: ManualWorker,
}

impl HttpTestApp {
    fn new(register: impl FnOnce(&mut TaskRegistry)) -> Self {
        let _ = install_default_mem_backend();
        install_ops_log(Arc::new(NoOpsLog));
        let mut registry = TaskRegistry::new();
        register(&mut registry);
        let registry = Arc::new(registry);
        let backend = Arc::new(MemQueueBackend::new());
        let (boson, manual) = Boson::builder()
            .queue_backend(backend)
            .execution_context_factory(StubExecutionContextFactory)
            .registry(registry)
            .without_worker()
            .build_manual()
            .expect("build_manual");
        let boson = Arc::new(boson);
        let state = AppState {
            boson: BosonState::new(Arc::clone(&boson)),
        };
        let router = Router::new()
            .nest(NEST_PATH, boson_router::<AppState>())
            .with_state(state);
        Self {
            router,
            boson,
            manual,
        }
    }

    async fn request(&self, req: Request<Body>) -> (StatusCode, Value) {
        let response = self.router.clone().oneshot(req).await.expect("oneshot");
        let status = response.status();
        let body = response
            .into_body()
            .collect()
            .await
            .expect("collect body")
            .to_bytes();
        let json: Value = serde_json::from_slice(&body).expect("json body");
        (status, json)
    }

    async fn drain_one(&self) {
        assert!(
            self.manual.try_run_next().await,
            "expected one job to drain"
        );
    }
}

fn enqueue_request(task_name: &str) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(format!("{NEST_PATH}/jobs/enqueue"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "task_name": task_name,
                "params": {},
            })
            .to_string(),
        ))
        .expect("request")
}

async fn enqueue_via_http(app: &HttpTestApp, task_name: &str) -> (StatusCode, Value) {
    app.request(enqueue_request(task_name)).await
}

#[tokio::test(flavor = "multi_thread")]
async fn post_enqueue_returns_job_id() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (status, body) = enqueue_via_http(&app, "noop").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let job_id = body["data"]["job_id"].as_str().expect("job_id");
    assert!(!job_id.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn post_enqueue_unknown_task_400() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (status, body) = enqueue_via_http(&app, "missing").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(body["success"], false);
    assert!(body["error"].as_str().is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn post_enqueue_rate_limited_429() {
    let app = HttpTestApp::new(|registry| {
        register_rate_limited_in_flight_task(registry, "limited");
    });
    let (status1, body1) = enqueue_via_http(&app, "limited").await;
    assert_eq!(status1, StatusCode::OK);
    let job_id = body1["data"]["job_id"].as_str().expect("job_id");
    assert!(app.boson.get_job(job_id).await.expect("get_job").is_some());

    let (status2, body2) = enqueue_via_http(&app, "limited").await;
    assert_eq!(status2, StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(body2["success"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_job_found() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (_, body) = enqueue_via_http(&app, "noop").await;
    let job_id = body["data"]["job_id"].as_str().expect("job_id");

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/jobs/{job_id}"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["job_id"], job_id);
    assert_eq!(body["data"]["status"], "queued");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_job_not_found_404() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/jobs/missing-job-id"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["success"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn post_cancel_queued_job() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (_, body) = enqueue_via_http(&app, "noop").await;
    let job_id = body["data"]["job_id"].as_str().expect("job_id");

    let req = Request::builder()
        .method("POST")
        .uri(format!("{NEST_PATH}/jobs/{job_id}/cancel"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);

    let job = app
        .boson
        .get_job(job_id)
        .await
        .expect("get_job")
        .expect("job");
    assert_eq!(job.status.to_string(), "canceled");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_list_jobs_status_filter() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    enqueue_via_http(&app, "noop").await;

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/jobs?status=queued"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let jobs = body["data"].as_array().expect("jobs");
    assert_eq!(jobs.len(), 1);
    assert_eq!(jobs[0]["status"], "queued");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_runs_for_job() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (_, body) = enqueue_via_http(&app, "noop").await;
    let job_id = body["data"]["job_id"].as_str().expect("job_id");
    app.drain_one().await;

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/runs?job_id={job_id}"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let runs = body["data"].as_array().expect("runs");
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["job_id"], job_id);
    assert_eq!(runs[0]["status"], "success");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_list_tasks() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["success"], true);
    let tasks = body["data"].as_array().expect("tasks");
    assert!(tasks.iter().any(|t| t["name"] == "noop"));
}

#[tokio::test(flavor = "multi_thread")]
async fn get_task_found() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks/noop"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["name"], "noop");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_task_not_found_404() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks/missing"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["success"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_task_config_after_enqueue() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    enqueue_via_http(&app, "noop").await;

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks/noop/config"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["task_name"], "noop");
}

#[tokio::test(flavor = "multi_thread")]
async fn get_task_config_not_found_404() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks/missing/config"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["success"], false);
}

#[tokio::test(flavor = "multi_thread")]
async fn post_task_config_updates_rate_limit() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    enqueue_via_http(&app, "noop").await;

    let req = Request::builder()
        .method("POST")
        .uri(format!("{NEST_PATH}/tasks/noop/config"))
        .header("content-type", "application/json")
        .body(Body::from(
            json!({
                "rate_limit_policy": {
                    "max_in_flight": 1,
                    "max_enqueue_per_second": 0
                }
            })
            .to_string(),
        ))
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["rate_limit_policy"]["max_in_flight"], 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_task_config_revisions_empty_stub() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    enqueue_via_http(&app, "noop").await;

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/tasks/noop/config/revisions"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"].as_array().expect("revisions").len(), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_run_by_id() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let (_, body) = enqueue_via_http(&app, "noop").await;
    let job_id = body["data"]["job_id"].as_str().expect("job_id");
    app.drain_one().await;

    let runs_req = Request::builder()
        .uri(format!("{NEST_PATH}/runs?job_id={job_id}"))
        .body(Body::empty())
        .expect("request");
    let (_, runs_body) = app.request(runs_req).await;
    let run_id = runs_body["data"][0]["run_id"].as_str().expect("run_id");

    let req = Request::builder()
        .uri(format!("{NEST_PATH}/runs/{run_id}"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["data"]["run_id"], run_id);
}

#[tokio::test(flavor = "multi_thread")]
async fn get_run_not_found_404() {
    let app = HttpTestApp::new(|registry| register_noop_task(registry, "noop"));
    let req = Request::builder()
        .uri(format!("{NEST_PATH}/runs/missing-run-id"))
        .body(Body::empty())
        .expect("request");
    let (status, body) = app.request(req).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["success"], false);
}
