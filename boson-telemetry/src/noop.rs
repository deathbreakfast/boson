use super::OpsLog;

/// Zero-cost no-op (benchmark `telemetry=off` and minimal CI).
#[derive(Debug, Default, Clone, Copy)]
pub struct NoOpsLog;

impl OpsLog for NoOpsLog {
    fn record_counter(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}

    fn record_gauge(&self, _name: &str, _labels: &[(&str, &str)], _value: f64) {}

    fn log_event(&self, _name: &str, _payload: &serde_json::Value) {}
}
