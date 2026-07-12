//! Operations telemetry for Boson workers.
//!
//! Install an [`OpsLog`] adapter at worker boot via [`BosonBuilder::ops_log`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.ops_log).
//!
//! ## Entry points
//!
//! - [`OpsLog`] — counters, gauges, structured events (see trait for emitted names)
//! - [`install_ops_log`] — process-wide install at boot
//! - [`NoOpsLog`] / [`ConsoleOpsLog`] — built-in adapters
//! - [`ops_log_from_env`] — select adapter via `BOSON_TELEMETRY`

mod console;
mod global;
mod noop;

pub use console::ConsoleOpsLog;
pub use global::{install_ops_log, ops_log, ops_log_from_env};
pub use noop::NoOpsLog;

/// Structured ops metrics and events for enqueue, runs, leases, and runtime health.
///
/// The runtime emits the following by default (labels vary by call site):
///
/// **Counters**
///
/// | Name | When |
/// |------|------|
/// | `boson_tasks_enqueued` | Job enqueued (`task_name`, `runtime` labels) |
/// | `boson_tasks_completed` | Handler succeeded (`task_name` label) |
/// | `boson_task_duration_ms` | Handler succeeded — duration as counter value |
/// | `boson_tasks_failed` | Handler failed (`task_name` label) |
///
/// **Events** (`log_event` name → payload highlights)
///
/// | Name | When |
/// |------|------|
/// | `boson_task_log` | Run started or completed (`event`, `task_name`, `job_id`, `run_id`, …) |
/// | `boson_handler_error` | Handler error (`task_name`, `job_id`, `message`, optional `will_retry`) |
/// | `boson_runtime_log` | Runtime ready or lease reclaim (`event`, `runtime`, …) |
///
/// Install a custom adapter with [`install_ops_log`] to forward these to your metrics stack.
/// Use [`ConsoleOpsLog`], [`NoOpsLog`], or a custom [`OpsLog`] implementation.
pub trait OpsLog: Send + Sync {
    /// Increment a counter with optional labels.
    fn record_counter(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Set a gauge with optional labels.
    fn record_gauge(&self, name: &str, labels: &[(&str, &str)], value: f64);

    /// Emit a structured diagnostic event (see trait-level table for runtime event names).
    fn log_event(&self, name: &str, payload: &serde_json::Value);
}

#[cfg(test)]
mod tests {
    use super::{ConsoleOpsLog, NoOpsLog, OpsLog};

    #[test]
    fn noop_ops_log_is_silent() {
        let log = NoOpsLog;
        log.record_counter("c", &[], 1.0);
        log.record_gauge("g", &[], 2.0);
        log.log_event("e", &serde_json::json!({}));
    }

    #[test]
    fn console_ops_log_does_not_panic() {
        let log = ConsoleOpsLog;
        log.record_counter("boson_tasks_enqueued", &[("task_name", "t")], 1.0);
    }
}
