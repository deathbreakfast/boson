use super::OpsLog;

/// stderr structured lines (default dev adapter).
///
/// # Examples
///
/// ```
/// use boson_telemetry::{ConsoleOpsLog, OpsLog};
///
/// let log = ConsoleOpsLog;
/// log.record_counter("boson_tasks_enqueued", &[("task_name", "ping")], 1.0);
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct ConsoleOpsLog;

impl OpsLog for ConsoleOpsLog {
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        eprintln!("[boson-telemetry] counter {name}={value} {labels:?}");
    }

    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64) {
        eprintln!("[boson-telemetry] gauge {name}={value} {labels:?}");
    }

    fn log_event(&self, name: &str, payload: &serde_json::Value) {
        eprintln!("[boson-telemetry] event {name} {payload}");
    }
}
