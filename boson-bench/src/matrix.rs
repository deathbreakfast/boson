//! CLI matrix parsing for benchmark runs.

use anyhow::{bail, Result};
use boson_testkit::matrix::{BackendAdapter, MatrixSpec, TelemetryAdapter, Topology};

/// Parse CLI flags into a [`MatrixSpec`].
pub fn matrix_from_cli(backend: &str, topology: &str, telemetry: &str) -> Result<MatrixSpec> {
    Ok(MatrixSpec {
        backend: parse_backend(backend)?,
        topology: parse_topology(topology)?,
        telemetry: parse_telemetry(telemetry)?,
    })
}

fn parse_backend(s: &str) -> Result<BackendAdapter> {
    match s.to_ascii_lowercase().as_str() {
        "mem" => Ok(BackendAdapter::Mem),
        "sqlite" => Ok(BackendAdapter::Sqlite),
        "postgres" => Ok(BackendAdapter::Postgres),
        "scylla" => Ok(BackendAdapter::Scylla),
        "redis" => Ok(BackendAdapter::Redis),
        "nats" => Ok(BackendAdapter::Nats),
        other => bail!("unknown backend {other}; see boson-bench/EXPERIMENTS.md"),
    }
}

fn parse_topology(s: &str) -> Result<Topology> {
    match s.to_ascii_lowercase().as_str() {
        "isolated-lab" | "isolated_lab" => Ok(Topology::IsolatedLab),
        "split-boson-server" | "split_boson_server" => Ok(Topology::SplitBosonServer),
        "server-apps-remote" | "server_apps_remote" => Ok(Topology::ServerAppsRemote),
        other => bail!(
            "unknown topology {other}; use isolated-lab|split-boson-server|server-apps-remote"
        ),
    }
}

fn parse_telemetry(s: &str) -> Result<TelemetryAdapter> {
    match s.to_ascii_lowercase().as_str() {
        "off" => Ok(TelemetryAdapter::Off),
        "console" => Ok(TelemetryAdapter::Console),
        other => bail!("unknown telemetry {other}; expected off or console"),
    }
}
