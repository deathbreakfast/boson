//! Worker lease and identity settings composed at build time.

/// Resolved worker settings for claim, lease, and telemetry labels.
///
/// Usually constructed by [`BosonBuilder`](crate::BosonBuilder) via [`worker_id`](crate::BosonBuilder::worker_id)
/// and [`lease_ttl_secs`](crate::BosonBuilder::lease_ttl_secs). Defaults: worker id from
/// `INSTANCE_ID` / `BOSON_WORKER_ID` / `boson-worker-1`, lease TTL `0` (Mode 1 embedded, no
/// distributed leases).
///
/// Getting started:
/// [Mode 2 — Remote worker](https://docs.rs/uf-boson/latest/boson/index.html#mode-2--remote-worker-two-binaries).
///
/// # Example — multiple worker processes (Mode 2)
///
/// Run one Boson instance per process against **shared** persistence (Postgres, `SQLite` path,
/// Redis, or NATS — not [`MemQueueBackend`](https://docs.rs/boson-backend-mem), which is
/// in-process only). Each process needs a unique [`worker_id`](Self::worker_id) and a positive
/// [`lease_ttl_secs`](Self::lease_ttl_secs) so
/// [`QueueBackend::try_claim_run_lease`](boson_core::QueueBackend::try_claim_run_lease) prevents
/// double execution:
///
/// ```rust,ignore
/// use std::sync::Arc;
///
/// use boson_backend_postgres::PostgresQueueBackend;
/// use boson_core::JsonExecutionContextFactory;
/// use boson_runtime::Boson;
///
/// # async fn boot() -> boson_core::Result<()> {
/// let url = std::env::var("DATABASE_URL")?;
/// let backend = PostgresQueueBackend::connect(&url).await?;
/// let _boson = Boson::builder()
///     .queue_backend(Arc::new(backend))
///     .execution_context_factory(JsonExecutionContextFactory)
///     .worker_id("worker-a")
///     .lease_ttl_secs(30)
///     .auto_registry()
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct WorkerSettings {
    /// Worker identity for distributed lease claims.
    pub worker_id: String,
    /// Run lease TTL in seconds; `0` disables lease coordination.
    pub lease_ttl_secs: i64,
    /// Telemetry/runtime label (topology slug or host-provided).
    pub runtime_label: String,
    /// When set, poll only these pools (shared-nothing fleet pinning).
    pub worker_pools: Option<Vec<String>>,
    /// Delay between worker poll ticks in milliseconds (default 50).
    pub worker_poll_interval_ms: u64,
    /// Skip persisting run rows on the hot path (bench / throughput mode).
    pub skip_run_persistence: bool,
}

impl WorkerSettings {
    /// Default embedded monolith: no leases, label `embedded`.
    #[must_use]
    pub fn embedded() -> Self {
        Self {
            worker_id: resolve_worker_id_from_env(),
            lease_ttl_secs: 0,
            runtime_label: "embedded".into(),
            worker_pools: resolve_worker_pools_from_env(),
            worker_poll_interval_ms: resolve_worker_poll_interval_from_env(),
            skip_run_persistence: resolve_skip_run_persistence_from_env(),
        }
    }

    /// Build settings from optional builder overrides.
    pub fn resolve(
        worker_id: Option<String>,
        lease_ttl_secs: Option<i64>,
        runtime_label: Option<String>,
        worker_pools: Option<Vec<String>>,
        worker_poll_interval_ms: Option<u64>,
    ) -> Self {
        Self {
            worker_id: worker_id.unwrap_or_else(resolve_worker_id_from_env),
            lease_ttl_secs: lease_ttl_secs.unwrap_or_else(resolve_lease_ttl_from_env),
            runtime_label: runtime_label.unwrap_or_else(|| "embedded".to_string()),
            worker_pools: worker_pools.or_else(resolve_worker_pools_from_env),
            worker_poll_interval_ms: worker_poll_interval_ms
                .unwrap_or_else(resolve_worker_poll_interval_from_env),
            skip_run_persistence: resolve_skip_run_persistence_from_env(),
        }
    }

    /// Pools this worker should poll. When pinned, uses [`Self::worker_pools`]; otherwise backend discovery.
    #[must_use]
    pub fn pools_to_poll(&self, discovered: Vec<String>) -> Vec<String> {
        match &self.worker_pools {
            Some(pools) if !pools.is_empty() => pools.clone(),
            _ => discovered,
        }
    }
}

fn resolve_worker_id_from_env() -> String {
    std::env::var("INSTANCE_ID")
        .or_else(|_| std::env::var("BOSON_WORKER_ID"))
        .unwrap_or_else(|_| "boson-worker-1".to_string())
}

fn resolve_lease_ttl_from_env() -> i64 {
    std::env::var("BOSON_LEASE_TTL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0)
}

fn resolve_worker_pools_from_env() -> Option<Vec<String>> {
    std::env::var("BOSON_WORKER_POOLS")
        .ok()
        .map(|s| {
            s.split(',')
                .map(str::trim)
                .filter(|p| !p.is_empty())
                .map(ToString::to_string)
                .collect()
        })
        .filter(|pools: &Vec<String>| !pools.is_empty())
}

fn resolve_worker_poll_interval_from_env() -> u64 {
    std::env::var("BOSON_WORKER_POLL_MS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(50)
}

fn resolve_skip_run_persistence_from_env() -> bool {
    std::env::var("BOSON_SKIP_RUN_ROWS")
        .ok()
        .is_some_and(|v| matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
}
