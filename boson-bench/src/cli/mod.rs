//! CLI argument definitions and dispatch for `boson-bench`.

mod bench_config;
mod dispatch;
mod hardware_cmd;
mod matrix_cmd;
mod projections_cmd;
mod run_experiment;

pub use dispatch::dispatch;

use std::path::PathBuf;

use clap::{Parser, Subcommand};

/// Root CLI parser.
#[derive(Parser)]
#[command(name = "boson-bench", about = "Boson synthetic benchmark runner")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

/// Bench subcommands (see [`EXPERIMENTS.md`](../../EXPERIMENTS.md)).
#[derive(Subcommand)]
pub enum Command {
    /// List registered experiment IDs (see EXPERIMENTS.md).
    Experiments,
    /// Run one experiment id against a matrix slice.
    Run {
        #[arg(long, default_value = "bm-b0")]
        experiment: String,
        #[arg(long, default_value = "mem")]
        backend: String,
        #[arg(long, default_value = "isolated-lab")]
        topology: String,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long)]
        hardware: Option<String>,
        #[arg(long)]
        ops: Option<u32>,
        #[arg(long, default_value = "0")]
        warmup: u32,
        #[arg(long)]
        report: Option<PathBuf>,
        /// Idempotency mode override (`lwt` / `none`).
        #[arg(long)]
        idempotency_mode: Option<String>,
        /// Concurrent enqueue clients (BM-BE/BM/BP).
        #[arg(long)]
        client_count: Option<u32>,
        /// Pool slot count K (spread-load / partition sweep).
        #[arg(long)]
        pool_count: Option<u32>,
        /// Pool layout: `shared` (hot partition) or `distinct` (`pool_0..pool_{K-1}`).
        #[arg(long, value_enum)]
        pool_layout: Option<run_experiment::PoolLayoutArg>,
        /// Prefill job count (BM-BD*).
        #[arg(long)]
        prefill_count: Option<u64>,
        /// Parallel drain workers (BM-BD*).
        #[arg(long)]
        worker_count: Option<u32>,
        /// Background worker poll interval ms (BM-BD2).
        #[arg(long)]
        worker_poll_ms: Option<u64>,
        /// Task fan-out count (BM-BF2).
        #[arg(long)]
        task_fanout_count: Option<u32>,
        /// Storage topology label for reports (e.g. `redis-1`).
        #[arg(long)]
        storage_topology: Option<String>,
    },
    /// Run a sustained load tier (BM-BL*).
    RunLoad {
        #[arg(long)]
        experiment: String,
        #[arg(long, default_value = "mem")]
        backend: String,
        #[arg(long, default_value = "isolated-lab")]
        topology: String,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long)]
        hardware: Option<String>,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Run a scale-out experiment (BM-BP* / BM-BM*).
    RunScale {
        #[arg(long)]
        experiment: String,
        #[arg(long, default_value = "mem")]
        backend: String,
        #[arg(long, default_value = "isolated-lab")]
        topology: String,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long)]
        hardware: Option<String>,
        #[arg(long)]
        report: Option<PathBuf>,
    },
    /// Run a matrix subset.
    Matrix {
        #[arg(long)]
        subset: String,
        #[arg(long, default_value = "mem")]
        backend: String,
        #[arg(long, default_value = "isolated-lab")]
        topology: String,
        #[arg(long, default_value = "off")]
        telemetry: String,
        #[arg(long)]
        hardware: Option<String>,
        #[arg(long, default_value = "0")]
        warmup: u32,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
    },
    /// Print fleet projection toward 10M jobs/s.
    ProjectFleet {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "mem")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Print storage-node scaling curve from BM-BM4 reports.
    ProjectScalingCurve {
        #[arg(long, default_value = "aws-t3-medium")]
        hardware: String,
        #[arg(long, default_value = "scylla")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BE4 publisher-count scaling curve (`JetStream` single-stream saturation).
    Be4PublisherCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BE4 pool-count (shard) scaling curve at fixed publisher count.
    Be4ShardCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BE4 broker fleet scaling curve (N standalone NATS nodes).
    Be4FleetCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Aggregate per-client multibench BM-BE4 reports.
    Be4Aggregate {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        cell_prefix: Option<String>,
    },
    /// BM-BE4 multi-bench scaling curve (embed fleet aggregate).
    Be4MultibenchCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BD2 worker-count scaling curve (drain throughput vs W).
    Bd2WorkerCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BD2 pool-count (shard) scaling curve at fixed W.
    Bd2ShardCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// BM-BD2 broker fleet scaling curve (N standalone NATS nodes).
    Bd2FleetCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Aggregate per-client multibench BM-BD2 reports.
    Bd2Aggregate {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        cell_prefix: Option<String>,
    },
    /// BM-BD2 multi-bench scaling curve (embed fleet aggregate drain).
    Bd2MultibenchCurve {
        #[arg(long, default_value = "aws-c6i-large")]
        hardware: String,
        #[arg(long, default_value = "nats")]
        backend: String,
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    /// Print experiment results summary from JSON reports.
    FillResults {
        #[arg(long, default_value = "profiling/boson-bench/reports")]
        reports_dir: PathBuf,
    },
    /// Capture and print hardware profile JSON.
    Hardware {
        #[arg(long, default_value = "aws-c6i-large")]
        profile: String,
    },
}
