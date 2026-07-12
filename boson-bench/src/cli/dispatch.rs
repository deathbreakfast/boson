//! Dispatch parsed CLI commands to bench runners.

use anyhow::Result;

use crate::cli::Command;
use crate::experiments;
use crate::fill_results;

use super::hardware_cmd::dispatch_hardware;
use super::matrix_cmd::dispatch_matrix;
use super::projections_cmd::dispatch_projections;
use super::run_experiment::dispatch_run;

/// Execute one parsed CLI command.
#[allow(clippy::large_futures)] // CLI dispatch holds large async backends (Scylla/NATS).
pub async fn dispatch(command: Command) -> Result<()> {
    match command {
        Command::Experiments => experiments::list_experiments(),
        c @ (Command::Run { .. } | Command::RunLoad { .. } | Command::RunScale { .. }) => {
            dispatch_run(c).await?;
        }
        c @ Command::Matrix { .. } => dispatch_matrix(c).await?,
        c @ (Command::ProjectFleet { .. }
        | Command::ProjectScalingCurve { .. }
        | Command::Be4PublisherCurve { .. }
        | Command::Be4ShardCurve { .. }
        | Command::Be4FleetCurve { .. }
        | Command::Be4Aggregate { .. }
        | Command::Be4MultibenchCurve { .. }
        | Command::Bd2WorkerCurve { .. }
        | Command::Bd2ShardCurve { .. }
        | Command::Bd2FleetCurve { .. }
        | Command::Bd2Aggregate { .. }
        | Command::Bd2MultibenchCurve { .. }) => {
            dispatch_projections(c)?;
        }
        Command::FillResults { reports_dir } => fill_results::fill_results(&reports_dir)?,
        Command::Hardware { profile: _ } => dispatch_hardware()?,
    }
    Ok(())
}
