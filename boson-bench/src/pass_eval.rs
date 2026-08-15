//! Pass/fail evaluation against pre-registered criteria.

use crate::report::ReportMetrics;

const MAX_ERROR_RATE: f64 = 0.001;

/// Evaluate pass criteria for a completed run.
pub fn evaluate(
    experiment_id: &str,
    metrics: &ReportMetrics,
    run_error: Option<&str>,
) -> (bool, String) {
    if let Some(err) = run_error {
        return (false, format!("FAIL: {err}"));
    }

    if experiment_id == "bm-bc1" {
        return evaluate_bc1(metrics);
    }

    if let Some(rate) = metrics.error_rate {
        if rate >= MAX_ERROR_RATE {
            return (false, format!("err={rate:.4}% FAIL (>=0.1%)"));
        }
    }

    let notes = match experiment_id {
        id if id.starts_with("bm-be") => {
            let achieved = metrics.achieved_ops_per_sec.unwrap_or(0.0);
            let p99 = metrics.p99_ms.unwrap_or(0.0);
            let err = metrics.error_rate.unwrap_or(0.0) * 100.0;
            format!("enqueue {achieved:.0}/s p99={p99:.3}ms err={err:.4}% PASS")
        }
        id if id.starts_with("bm-bd") => {
            let drain = metrics.drain_ops_per_sec.unwrap_or(0.0);
            let workers = metrics.worker_count.unwrap_or(0);
            let n = metrics.prefill_count.unwrap_or(0);
            format!("drain {drain:.0}/s W={workers} N={n} PASS")
        }
        id if id.starts_with("bm-bl")
            || id.starts_with("bm-bm")
            || id.starts_with("bm-bp")
            || id == "bm-bi1"
            || id == "bm-bf2" =>
        {
            let achieved = metrics.achieved_ops_per_sec.unwrap_or(0.0);
            let p99 = metrics.p99_ms.unwrap_or(0.0);
            let err = metrics.error_rate.unwrap_or(0.0) * 100.0;
            format!("soak/enqueue {achieved:.0}/s p99={p99:.3}ms err={err:.4}% PASS")
        }
        "bm-b0" | "bm-b1" | "bm-b5" => {
            let enq = metrics.enqueue_ms.map_or_else(
                || "no enqueue samples".into(),
                |s| format!("p50={:.3}ms p99={:.3}ms", s.p50, s.p99),
            );
            format!("{enq} PASS")
        }
        _ => "PASS".into(),
    };

    (true, notes)
}

fn evaluate_bc1(metrics: &ReportMetrics) -> (bool, String) {
    if metrics.lease_ttl_secs.unwrap_or(0) <= 0 {
        return (false, "FAIL: disabled leases".into());
    }
    if metrics.run_rows_enabled == Some(false) {
        return (false, "FAIL: skipped run persistence".into());
    }
    if metrics.duplicate_executions.unwrap_or(0) > 0 {
        return (
            false,
            format!(
                "FAIL: duplicate handler execution ({})",
                metrics.duplicate_executions.unwrap_or(0)
            ),
        );
    }
    if metrics.residual_backlog.unwrap_or(0) > 0 {
        return (
            false,
            format!(
                "FAIL: residual backlog ({})",
                metrics.residual_backlog.unwrap_or(0)
            ),
        );
    }
    if metrics.error_rate.unwrap_or(0.0) > 0.0 {
        return (
            false,
            format!(
                "FAIL: terminal failures (err={:.4})",
                metrics.error_rate.unwrap_or(0.0)
            ),
        );
    }
    let completed = metrics.completed_ops_per_sec.unwrap_or(0.0);
    if completed <= 0.0 {
        return (false, "FAIL: no terminal Success".into());
    }
    let workers = metrics.worker_count.unwrap_or(0);
    (true, format!("completed {completed:.1}/s W={workers} PASS"))
}

/// Pass criteria description for report JSON.
pub fn pass_criteria_for(experiment_id: &str) -> &'static str {
    match experiment_id {
        id if id.starts_with("bm-be") => "enqueue error rate < 0.1%",
        id if id.starts_with("bm-bd") => "prefilled queue drains without error",
        "bm-bc1" => {
            "terminal Success only; no duplicates, backlog, disabled leases, or skipped run rows"
        }
        id if id.starts_with("bm-bl")
            || id.starts_with("bm-bm")
            || id.starts_with("bm-bp")
            || id == "bm-bi1"
            || id == "bm-bf2" =>
        {
            "soak/enqueue error rate < 0.1%"
        }
        "bm-b0" => "enqueue completes without error",
        "bm-b1" | "bm-b5" => "job reaches success",
        _ => "scenario completes without error",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bc1_ok() -> ReportMetrics {
        ReportMetrics {
            completed_ops_per_sec: Some(12.0),
            achieved_ops_per_sec: Some(12.0),
            error_rate: Some(0.0),
            worker_count: Some(32),
            duplicate_executions: Some(0),
            residual_backlog: Some(0),
            lease_ttl_secs: Some(30),
            run_rows_enabled: Some(true),
            ..Default::default()
        }
    }

    #[test]
    fn fails_on_high_error_rate() {
        let metrics = ReportMetrics {
            error_rate: Some(0.01),
            ..Default::default()
        };
        let (pass, _) = evaluate("bm-bl1", &metrics, None);
        assert!(!pass);
    }

    #[test]
    fn bc1_happy_pass() {
        let (pass, notes) = evaluate("bm-bc1", &bc1_ok(), None);
        assert!(pass, "{notes}");
        assert!(notes.contains("completed"));
    }

    #[test]
    fn bc1_fails_on_timeout_residual_backlog() {
        let mut metrics = bc1_ok();
        metrics.residual_backlog = Some(4);
        let (pass, notes) = evaluate("bm-bc1", &metrics, None);
        assert!(!pass);
        assert!(notes.contains("backlog"), "{notes}");
    }

    #[test]
    fn bc1_fails_on_duplicate_hit() {
        let mut metrics = bc1_ok();
        metrics.duplicate_executions = Some(2);
        let (pass, notes) = evaluate("bm-bc1", &metrics, None);
        assert!(!pass);
        assert!(notes.contains("duplicate"), "{notes}");
    }

    #[test]
    fn bc1_fails_on_disabled_lease() {
        let mut metrics = bc1_ok();
        metrics.lease_ttl_secs = Some(0);
        let (pass, notes) = evaluate("bm-bc1", &metrics, None);
        assert!(!pass);
        assert!(notes.contains("lease"), "{notes}");
    }

    #[test]
    fn bc1_fails_on_skipped_run_rows() {
        let mut metrics = bc1_ok();
        metrics.run_rows_enabled = Some(false);
        let (pass, notes) = evaluate("bm-bc1", &metrics, None);
        assert!(!pass);
        assert!(notes.contains("run persistence"), "{notes}");
    }

    #[test]
    fn bc1_fails_on_terminal_failures() {
        let mut metrics = bc1_ok();
        metrics.error_rate = Some(0.05);
        let (pass, notes) = evaluate("bm-bc1", &metrics, None);
        assert!(!pass);
        assert!(notes.contains("terminal"), "{notes}");
    }

    #[test]
    fn bc1_timeout_run_error_is_fail_not_panic() {
        let (pass, notes) = evaluate("bm-bc1", &bc1_ok(), Some("drain timeout"));
        assert!(!pass);
        assert!(notes.starts_with("FAIL:"), "{notes}");
    }
}
