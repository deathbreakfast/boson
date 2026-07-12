//! Pass/fail evaluation against pre-registered criteria.

use crate::report::ReportMetrics;

const MAX_ERROR_RATE: f64 = 0.001;

/// Evaluate pass criteria for a completed run.
pub fn evaluate(experiment_id: &str, metrics: &ReportMetrics, run_error: Option<&str>) -> (bool, String) {
    if let Some(err) = run_error {
        return (false, format!("FAIL: {err}"));
    }

    if let Some(rate) = metrics.error_rate {
        if rate >= MAX_ERROR_RATE {
            return (
                false,
                format!("err={rate:.4}% FAIL (>=0.1%)"),
            );
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
            let enq = metrics
                .enqueue_ms.map_or_else(|| "no enqueue samples".into(), |s| format!("p50={:.3}ms p99={:.3}ms", s.p50, s.p99));
            format!("{enq} PASS")
        }
        _ => "PASS".into(),
    };

    (true, notes)
}

/// Pass criteria description for report JSON.
pub fn pass_criteria_for(experiment_id: &str) -> &'static str {
    match experiment_id {
        id if id.starts_with("bm-be") => "enqueue error rate < 0.1%",
        id if id.starts_with("bm-bd") => "prefilled queue drains without error",
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

    #[test]
    fn fails_on_high_error_rate() {
        let metrics = ReportMetrics {
            error_rate: Some(0.01),
            ..Default::default()
        };
        let (pass, _) = evaluate("bm-bl1", &metrics, None);
        assert!(!pass);
    }
}
