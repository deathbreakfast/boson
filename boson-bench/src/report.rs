//! JSON report schema for benchmark runs.

use serde::{Deserialize, Serialize};

use crate::config::BenchRunConfig;
use crate::hardware::HardwareDetail;
use crate::resource_profile::ResourceProfile;
use crate::stats::MetricStats;

/// Matrix dimensions embedded in each report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportDimensions {
    /// Backend adapter slug.
    pub backend: String,
    /// Topology slug.
    pub topology: String,
    /// Telemetry adapter slug.
    pub telemetry: String,
    /// Hardware profile slug.
    pub hardware: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional storage topology for distributed campaigns.
    pub storage_topology: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Multi-bench client index (0-based) when running distributed embed fleet.
    pub bench_client_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Total bench clients in a multi-bench cell.
    pub bench_client_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Standalone NATS broker count for fleet campaigns.
    pub fleet_size: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// True when this report sums per-client multibench runs.
    pub aggregate: Option<bool>,
}

/// Flexible metrics block — scenario vs load vs scale experiments.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ReportMetrics {
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Target ops/s for load tiers.
    pub target_ops_per_sec: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Achieved ops/s.
    pub achieved_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Error rate 0.0–1.0.
    pub error_rate: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Enqueue timing stats.
    pub enqueue_ms: Option<MetricStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Drain timing stats.
    pub drain_ms: Option<MetricStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Admin read timing stats.
    pub admin_read_ms: Option<MetricStats>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Combined p99 ms (load/scale).
    pub p99_ms: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Client count for scale experiments.
    pub client_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Pool count for scale experiments.
    pub pool_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Worker count for drain experiments.
    pub worker_count: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Prefill job count for drain experiments.
    pub prefill_count: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Explicit drain throughput (jobs/s).
    pub drain_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Sum of per-client achieved rates in a multibench aggregate report.
    pub fleet_aggregate_ops_per_sec: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Metric kind: `enqueue`, `drain`, or soak default.
    pub metric_kind: Option<String>,
}

/// NATS enqueue pipeline dimensions captured at run time.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsPipelineDimensions {
    pub enqueue_mode: String,
    pub sync_ack: String,
    pub max_inflight: String,
}

/// Optional diagnostics block (raw NATS baseline, etc.).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReportDiagnostics {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nats_bench_peak_ops: Option<f64>,
}

/// One benchmark run written to stdout or `--report`.
#[derive(Debug, Serialize)]
pub struct BenchReport {
    /// Experiment id (e.g. `bm-b0`).
    pub experiment_id: String,
    /// Matrix dimensions.
    pub dimensions: ReportDimensions,
    /// Scenario id executed (when applicable).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scenario_id: Option<String>,
    /// Captured hardware metadata.
    pub hardware_detail: HardwareDetail,
    /// Experiment-specific metrics.
    pub metrics: ReportMetrics,
    /// Resolved bench knobs for this run (sweep reproducibility).
    pub bench_config: BenchRunConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Cloud resource sampling.
    pub resource_profile: Option<ResourceProfile>,
    /// Pre-registered pass criteria summary.
    pub pass_criteria: String,
    /// Whether pass criteria met.
    pub pass: bool,
    /// `ok` or `fail`.
    pub status: &'static str,
    /// Human-readable summary.
    pub notes: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// NATS `WorkQueue` pipeline settings when backend is nats.
    pub nats_pipeline: Option<NatsPipelineDimensions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Optional diagnostics (e.g. raw nats bench baseline).
    pub diagnostics: Option<ReportDiagnostics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    /// Error message when status is `fail`.
    pub error: Option<String>,
}

/// Standard report filename for an experiment run.
pub fn report_filename(experiment_id: &str, matrix_slug: &str, hardware: &str) -> String {
    format!("{experiment_id}-{matrix_slug}-{hardware}.json")
}

impl BenchReport {
    /// Standard report filename.
    pub fn filename(&self) -> String {
        report_filename(
            &self.experiment_id,
            &format!(
                "{}-{}-{}",
                self.dimensions.backend, self.dimensions.topology, self.dimensions.telemetry
            ),
            &self.dimensions.hardware,
        )
    }
}

/// Build report path under the profiling directory.
pub fn default_reports_dir() -> std::path::PathBuf {
    std::path::PathBuf::from("profiling/boson-bench/reports")
}

/// Write a report JSON file.
pub fn write_report(path: &std::path::Path, report: &BenchReport) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, serde_json::to_string_pretty(report)?)?;
    Ok(())
}
