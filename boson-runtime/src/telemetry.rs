//! Runtime self-metrics via [`OpsLog`](boson_telemetry::OpsLog).

use boson_telemetry::ops_log;
use serde_json::json;

/// Record a successful task enqueue.
pub fn record_task_enqueued(task_name: &str, runtime_label: &str) {
    ops_log().record_counter(
        "boson_tasks_enqueued",
        &[("task_name", task_name), ("runtime", runtime_label)],
        1.0,
    );
}

/// Record task execution start.
pub fn record_task_started(task_name: &str, job_id: &str, run_id: &str, runtime_label: &str) {
    ops_log().log_event(
        "boson_task_log",
        &json!({
            "event": "started",
            "task_name": task_name,
            "job_id": job_id,
            "run_id": run_id,
            "runtime": runtime_label,
        }),
    );
}

/// Record successful task completion.
pub fn record_task_completed(task_name: &str, job_id: &str, run_id: &str, duration_ms: i64) {
    ops_log().record_counter("boson_tasks_completed", &[("task_name", task_name)], 1.0);
    ops_log().record_counter(
        "boson_task_duration_ms",
        &[("task_name", task_name)],
        f64::from(i32::try_from(duration_ms.max(0)).unwrap_or(i32::MAX)),
    );
    ops_log().log_event(
        "boson_task_log",
        &json!({
            "event": "completed",
            "task_name": task_name,
            "job_id": job_id,
            "run_id": run_id,
            "duration_ms": duration_ms,
        }),
    );
}

/// Record task failure (terminal or retry scheduled).
pub fn record_task_failed(
    task_name: &str,
    job_id: &str,
    run_id: &str,
    message: &str,
    will_retry: bool,
) {
    ops_log().record_counter("boson_tasks_failed", &[("task_name", task_name)], 1.0);
    ops_log().log_event(
        "boson_handler_error",
        &json!({
            "task_name": task_name,
            "job_id": job_id,
            "run_id": run_id,
            "message": message,
            "will_retry": will_retry,
        }),
    );
}

/// Record handler-side error before run finish.
pub fn record_handler_error(task_name: &str, job_id: &str, run_id: &str, message: &str) {
    ops_log().log_event(
        "boson_handler_error",
        &json!({
            "task_name": task_name,
            "job_id": job_id,
            "run_id": run_id,
            "message": message,
        }),
    );
}

/// Record a backend `upsert_job` failure without aborting the worker.
pub fn log_job_upsert_failed(job_id: &str, task_name: &str, error: &str) {
    ops_log().log_event(
        "boson_runtime_log",
        &json!({
            "event": "job_upsert_failed",
            "job_id": job_id,
            "task_name": task_name,
            "error": error,
        }),
    );
}

/// Record runtime ready at boot.
pub fn log_runtime_ready(runtime_label: &str) {
    ops_log().log_event(
        "boson_runtime_log",
        &json!({
            "event": "ready",
            "runtime": runtime_label,
        }),
    );
}

/// Record expired lease reclaims.
pub fn log_lease_reclaim(count: usize, runtime_label: &str) {
    if count == 0 {
        return;
    }
    ops_log().log_event(
        "boson_runtime_log",
        &json!({
            "event": "lease_reclaim",
            "count": count,
            "runtime": runtime_label,
        }),
    );
}
