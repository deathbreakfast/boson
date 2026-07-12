//! Synthetic Boson benchmarks (BM-B* / BM-BL* / BM-BP* / BM-BM*).
//!
//! Registry: [`EXPERIMENTS.md`](EXPERIMENTS.md)

#![allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]

mod cli;
mod config;
mod drain;
mod enqueue;
mod experiments;
mod fill_results;
mod hardware;
mod http_bench;
mod load;
mod matrix;
mod pass_eval;
mod projection;
mod report;
mod resource_profile;
mod runner;
mod scale;
mod stats;
mod tasks;

use anyhow::Result;
use clap::Parser;

use cli::Cli;

#[tokio::main]
#[allow(clippy::large_futures)] // CLI dispatch holds large async backends (Scylla/NATS).
async fn main() -> Result<()> {
    cli::dispatch(Cli::parse().command).await
}
