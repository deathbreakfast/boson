//! Isolated enqueue capacity experiments BM-BE* (no background worker).

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use boson_runtime::Boson;

use crate::config::BenchRunConfig;
use crate::report::ReportMetrics;
use crate::scale::run_enqueue_clients;

/// Concurrent enqueue with no worker draining (BM-BE1/BE2/BE4).
pub async fn run_enqueue_capacity(
    boson: Arc<Boson>,
    cfg: &BenchRunConfig,
) -> Result<ReportMetrics> {
    let publisher = &cfg.publisher;
    let duration = Duration::from_secs(publisher.duration_secs);
    let start = Instant::now();
    let mut metrics =
        run_enqueue_clients(boson, publisher, duration, start).await?;
    metrics.metric_kind = Some("enqueue".into());
    metrics.client_count = Some(publisher.client_count);
    metrics.pool_count = Some(publisher.pool_count);
    Ok(metrics)
}
