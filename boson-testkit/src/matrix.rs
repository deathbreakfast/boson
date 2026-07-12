//! Matrix dimensions for correctness and bench runs.

use serde::{Deserialize, Serialize};

/// Storage / queue backend adapter for a matrix row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum BackendAdapter {
    /// In-memory [`QueueBackend`](boson_core::QueueBackend).
    Mem,
    /// Embedded `SQLite` — third-party adapter crate.
    Sqlite,
    /// External Postgres — third-party adapter crate.
    Postgres,
    /// Native `ScyllaDB` CQL — third-party adapter crate.
    Scylla,
    /// Redis (ZSET ready queue) — Tier 3 adapter.
    Redis,
    /// NATS `JetStream` KV — Tier 3 adapter.
    Nats,
}

/// Deployment topology label for test and bench matrix rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Topology {
    /// Isolated lab (testkit only).
    IsolatedLab,
    /// Split boson-server workers with run leases.
    SplitBosonServer,
    /// Server apps with remote coordinator (HTTP client harness).
    ServerAppsRemote,
}

/// Telemetry adapter for a matrix row.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TelemetryAdapter {
    /// No telemetry output.
    Off,
    /// Log telemetry to the console.
    Console,
}

/// Full matrix specification (aligned with boson-bench EXPERIMENTS.md).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MatrixSpec {
    /// Queue backend adapter.
    pub backend: BackendAdapter,
    /// Host topology.
    pub topology: Topology,
    /// Ops log adapter.
    pub telemetry: TelemetryAdapter,
}

impl Default for MatrixSpec {
    fn default() -> Self {
        Self::ci_mem_isolated_lab()
    }
}

impl MatrixSpec {
    /// CI default: mem backend, isolated-lab topology, telemetry off.
    #[must_use]
    pub const fn ci_mem_isolated_lab() -> Self {
        Self {
            backend: BackendAdapter::Mem,
            topology: Topology::IsolatedLab,
            telemetry: TelemetryAdapter::Off,
        }
    }

    /// Back-compat alias for CI default slice.
    #[must_use]
    pub const fn ci_mem_local() -> Self {
        Self::ci_mem_isolated_lab()
    }

    /// Split-worker topology with leases (`#[ignore]` in e2e).
    #[must_use]
    pub const fn ci_mem_split_boson_server() -> Self {
        Self {
            backend: BackendAdapter::Mem,
            topology: Topology::SplitBosonServer,
            telemetry: TelemetryAdapter::Off,
        }
    }

    /// Console telemetry smoke (`#[ignore]` in e2e).
    #[must_use]
    pub const fn ci_mem_isolated_lab_console() -> Self {
        Self {
            backend: BackendAdapter::Mem,
            topology: Topology::IsolatedLab,
            telemetry: TelemetryAdapter::Console,
        }
    }

    /// Back-compat alias for console telemetry row.
    #[must_use]
    pub const fn ci_mem_local_console() -> Self {
        Self::ci_mem_isolated_lab_console()
    }

    /// Isolated-lab matrix row for any storage backend.
    #[must_use]
    pub const fn isolated_lab(backend: BackendAdapter) -> Self {
        Self {
            backend,
            topology: Topology::IsolatedLab,
            telemetry: TelemetryAdapter::Off,
        }
    }

    /// Split-boson-server matrix row for any storage backend.
    #[must_use]
    pub const fn split_boson_server(backend: BackendAdapter) -> Self {
        Self {
            backend,
            topology: Topology::SplitBosonServer,
            telemetry: TelemetryAdapter::Off,
        }
    }

    /// Isolated-lab with console telemetry for any storage backend.
    #[must_use]
    pub const fn isolated_lab_console(backend: BackendAdapter) -> Self {
        Self {
            backend,
            topology: Topology::IsolatedLab,
            telemetry: TelemetryAdapter::Console,
        }
    }

    /// CI sqlite smoke row.
    #[must_use]
    pub const fn ci_sqlite_isolated_lab() -> Self {
        Self::isolated_lab(BackendAdapter::Sqlite)
    }

    /// CI postgres smoke row.
    #[must_use]
    pub const fn ci_postgres_isolated_lab() -> Self {
        Self::isolated_lab(BackendAdapter::Postgres)
    }

    /// CI sqlite split-server row.
    #[must_use]
    pub const fn ci_sqlite_split_boson_server() -> Self {
        Self::split_boson_server(BackendAdapter::Sqlite)
    }

    /// CI postgres split-server row.
    #[must_use]
    pub const fn ci_postgres_split_boson_server() -> Self {
        Self::split_boson_server(BackendAdapter::Postgres)
    }

    /// Worker id for lease claims (env override supported).
    #[must_use]
    pub fn worker_id(&self) -> String {
        std::env::var("INSTANCE_ID")
            .or_else(|_| std::env::var("BOSON_WORKER_ID"))
            .unwrap_or_else(|_| "testkit-worker-1".to_string())
    }

    /// Lease TTL derived from topology.
    #[must_use]
    pub fn lease_ttl_secs(&self) -> i64 {
        match self.topology {
            Topology::SplitBosonServer => std::env::var("BOSON_LEASE_TTL_SECS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(120),
            _ => 0,
        }
    }

    /// Runtime/telemetry label for this matrix row.
    #[must_use]
    pub const fn runtime_label(&self) -> &'static str {
        topology_slug(self.topology)
    }

    /// Stable string id for report filenames.
    #[must_use]
    pub fn report_slug(&self) -> String {
        format!(
            "{}-{}-{}",
            backend_slug(self.backend),
            topology_slug(self.topology),
            telemetry_slug(self.telemetry),
        )
    }

    /// Backend dimension slug.
    #[must_use]
    pub const fn backend_name(&self) -> &'static str {
        backend_slug(self.backend)
    }

    /// Topology dimension slug.
    #[must_use]
    pub const fn topology_name(&self) -> &'static str {
        topology_slug(self.topology)
    }

    /// Telemetry dimension slug.
    #[must_use]
    pub const fn telemetry_name(&self) -> &'static str {
        telemetry_slug(self.telemetry)
    }
}

const fn backend_slug(b: BackendAdapter) -> &'static str {
    match b {
        BackendAdapter::Mem => "mem",
        BackendAdapter::Sqlite => "sqlite",
        BackendAdapter::Postgres => "postgres",
        BackendAdapter::Scylla => "scylla",
        BackendAdapter::Redis => "redis",
        BackendAdapter::Nats => "nats",
    }
}

const fn topology_slug(t: Topology) -> &'static str {
    match t {
        Topology::IsolatedLab => "isolated-lab",
        Topology::SplitBosonServer => "split-boson-server",
        Topology::ServerAppsRemote => "server-apps-remote",
    }
}

const fn telemetry_slug(t: TelemetryAdapter) -> &'static str {
    match t {
        TelemetryAdapter::Off => "off",
        TelemetryAdapter::Console => "console",
    }
}

/// Backends exercised by the full e2e scenario catalog (PR smoke uses [`smoke_storage_backends`]).
#[must_use]
pub const fn e2e_storage_backends() -> &'static [BackendAdapter] {
    &[
        BackendAdapter::Mem,
        BackendAdapter::Sqlite,
        BackendAdapter::Postgres,
        BackendAdapter::Scylla,
        BackendAdapter::Redis,
        BackendAdapter::Nats,
    ]
}

/// Backends active on every PR without service containers.
#[must_use]
pub const fn smoke_storage_backends() -> &'static [BackendAdapter] {
    &[BackendAdapter::Mem, BackendAdapter::Sqlite]
}

/// Isolated-lab matrix for a backend dimension.
#[must_use]
pub const fn matrix_isolated_lab(backend: BackendAdapter) -> MatrixSpec {
    MatrixSpec::isolated_lab(backend)
}

/// Split-boson-server matrix for a backend dimension.
#[must_use]
pub const fn matrix_split_boson_server(backend: BackendAdapter) -> MatrixSpec {
    MatrixSpec::split_boson_server(backend)
}

/// Isolated-lab + console telemetry for a backend dimension.
#[must_use]
pub const fn matrix_isolated_lab_console(backend: BackendAdapter) -> MatrixSpec {
    MatrixSpec::isolated_lab_console(backend)
}
