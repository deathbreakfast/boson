//! Install queue backends and build Boson for a matrix row.

mod env_guard;

use std::sync::Arc;

use anyhow::{bail, Result};
use boson_backend_mem::{install_default_mem_backend, MemQueueBackend};
use boson_backend_nats::connect_auto;
use boson_backend_postgres::{install_isolated_postgres_backend, postgres_test_url};
use boson_backend_redis::{RedisQueueBackend, RedisQueueConfig};
use boson_backend_scylla::{
    isolated_keyspace, scylla_test_contact_points, ScyllaQueueBackend, ScyllaQueueConfig,
};
use boson_backend_sqlite::SqliteQueueBackend;
use boson_core::{ExecutionContextFactory, IdempotencyMode, QueueBackend};
use boson_runtime::{spawn_worker, Boson, BosonBuilder, ManualWorker, TaskRegistry, WorkerSettings};
use boson_telemetry::{install_ops_log, ConsoleOpsLog, NoOpsLog};

use crate::identity::StubExecutionContextFactory;
use crate::matrix::{BackendAdapter, MatrixSpec, TelemetryAdapter};

/// Holds bootstrap state for one matrix row.
pub struct BootstrapSession {
    matrix: MatrixSpec,
    backend: Option<Arc<dyn QueueBackend>>,
    registry: Arc<TaskRegistry>,
    ready: bool,
    sqlite_temp: Option<tempfile::TempDir>,
    postgres_schema: Option<String>,
    /// Runtime default idempotency policy (`None` = builder default `Lwt`).
    idempotency_mode: Option<IdempotencyMode>,
    scylla_config: Option<ScyllaQueueConfig>,
    redis_config: Option<RedisQueueConfig>,
    env_guard: Option<env_guard::EnvGuard>,
}

impl std::fmt::Debug for BootstrapSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BootstrapSession")
            .field("matrix", &self.matrix)
            .field("ready", &self.ready)
            .finish_non_exhaustive()
    }
}

impl BootstrapSession {
    /// Start a session for the given matrix dimensions.
    #[must_use]
    pub fn new(matrix: MatrixSpec) -> Self {
        Self {
            matrix,
            backend: None,
            registry: Arc::new(TaskRegistry::new()),
            ready: false,
            sqlite_temp: None,
            postgres_schema: None,
            idempotency_mode: None,
            scylla_config: None,
            redis_config: None,
            env_guard: None,
        }
    }

    /// Override Scylla connection/tuning for this session (call before [`install`](Self::install)).
    #[must_use]
    pub fn with_scylla_config(mut self, config: ScyllaQueueConfig) -> Self {
        self.scylla_config = Some(config);
        self
    }

    /// Override Redis connection settings for this session (call before [`install`](Self::install)).
    #[must_use]
    pub fn with_redis_config(mut self, config: RedisQueueConfig) -> Self {
        self.redis_config = Some(config);
        self
    }

    /// Override the runtime default [`IdempotencyMode`] (call before [`install`](Self::install)).
    #[must_use]
    pub const fn with_idempotency_mode(mut self, mode: IdempotencyMode) -> Self {
        self.idempotency_mode = Some(mode);
        self
    }

    /// Install a pre-built backend (out-of-tree adapters) and matrix telemetry.
    ///
    /// # Errors
    ///
    /// Returns an error if telemetry setup fails.
    pub fn install_backend(&mut self, backend: Arc<dyn QueueBackend>) -> Result<()> {
        boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(Arc::clone(
            &backend,
        )));
        self.backend = Some(backend);
        match self.matrix.telemetry {
            TelemetryAdapter::Off => install_ops_log(Arc::new(NoOpsLog)),
            TelemetryAdapter::Console => install_ops_log(Arc::new(ConsoleOpsLog)),
        }
        self.ready = true;
        Ok(())
    }

    /// Matrix dimensions for this session.
    #[must_use]
    pub const fn matrix(&self) -> &MatrixSpec {
        &self.matrix
    }

    /// Whether [`install`](Self::install) completed successfully.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Mutable task registry (register synthetic tasks before install).
    ///
    /// # Panics
    ///
    /// Panics if the registry is shared (not uniquely owned) before install.
    pub fn registry_mut(&mut self) -> &mut TaskRegistry {
        Arc::get_mut(&mut self.registry).expect("unique registry before build")
    }

    /// Shared registry after install.
    #[must_use]
    pub fn registry(&self) -> Arc<TaskRegistry> {
        Arc::clone(&self.registry)
    }

    /// Shared queue backend after install.
    #[must_use]
    pub fn backend(&self) -> Option<Arc<dyn QueueBackend>> {
        self.backend.clone()
    }

    /// Install queue backend and telemetry for the matrix row.
    ///
    /// # Errors
    ///
    /// Returns an error if the backend or telemetry adapter is unsupported, or if
    /// `SQLite`/Postgres setup fails.
    // Backend-specific setup is clearer as one exhaustive adapter dispatch.
    #[allow(clippy::too_many_lines)]
    pub async fn install(&mut self) -> Result<()> {
        match self.matrix.backend {
            BackendAdapter::Mem => {
                let _ = install_default_mem_backend();
                self.backend = Some(Arc::new(MemQueueBackend::new()));
            }
            BackendAdapter::Sqlite => {
                let temp = tempfile::tempdir()?;
                let path = temp.path().join("boson.db");
                let backend: Arc<dyn QueueBackend> =
                    Arc::new(SqliteQueueBackend::new(&path).await?);
                boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
                    Arc::clone(&backend),
                ));
                self.backend = Some(backend);
                self.sqlite_temp = Some(temp);
            }
            BackendAdapter::Postgres => {
                let url = postgres_test_url();
                let (backend, schema) = install_isolated_postgres_backend(&url).await?;
                let dyn_backend: Arc<dyn QueueBackend> = backend;
                boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
                    Arc::clone(&dyn_backend),
                ));
                self.backend = Some(dyn_backend);
                self.postgres_schema = Some(schema);
            }
            BackendAdapter::Scylla => {
                let points = scylla_test_contact_points().ok_or_else(|| {
                    anyhow::anyhow!(
                        "BOSON_TEST_SCYLLA_CONTACT_POINTS not set (cloud/CI Scylla only — not local multi-node Docker)"
                    )
                })?;
                let keyspace = isolated_keyspace("boson_test");
                let config = self
                    .scylla_config
                    .clone()
                    .unwrap_or_else(|| ScyllaQueueConfig {
                        contact_points: points,
                        keyspace,
                        ..Default::default()
                    });
                let backend = Arc::new(ScyllaQueueBackend::connect(config).await?);
                let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
                boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
                    Arc::clone(&dyn_backend),
                ));
                self.backend = Some(dyn_backend);
            }
            BackendAdapter::Redis => {
                let fleet_urls = std::env::var("BOSON_REDIS_URLS").ok().filter(|s| {
                    s.split(',').filter(|p| !p.trim().is_empty()).count() > 1
                });
                let pool_routing = std::env::var("BOSON_REDIS_POOL_ROUTING")
                    .ok()
                    .filter(|s| !s.trim().is_empty());
                let backend = if fleet_urls.is_some() || pool_routing.is_some() {
                    boson_backend_redis::connect_fleet_from_env().await?
                } else {
                    let url = std::env::var("BOSON_TEST_REDIS_URL")
                        .or_else(|_| std::env::var("BOSON_BENCH_REDIS_URL"))
                        .unwrap_or_else(|_| "redis://127.0.0.1:6379".into());
                    let keyspace = boson_backend_redis::keys::Keyspace::isolated("boson_e2e");
                    Arc::new(
                        RedisQueueBackend::connect_with_keyspace(&url, keyspace).await?,
                    ) as Arc<dyn QueueBackend>
                };
                let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend);
                boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
                    Arc::clone(&dyn_backend),
                ));
                self.backend = Some(dyn_backend);
            }
            BackendAdapter::Nats => {
                self.env_guard = Some(env_guard::EnvGuard::set(
                    "BOSON_NATS_KEY_PREFIX",
                    &boson_backend_nats::keys::Keyspace::isolated_prefix("boson_e2e"),
                ));
                let fleet_urls = std::env::var("BOSON_NATS_URLS").ok().filter(|s| {
                    s.split(',').filter(|p| !p.trim().is_empty()).count() > 1
                });
                let pool_routing = std::env::var("BOSON_NATS_POOL_ROUTING")
                    .ok()
                    .filter(|s| !s.trim().is_empty());
                let backend = if fleet_urls.is_some() || pool_routing.is_some() {
                    boson_backend_nats::connect_fleet_from_env().await?
                } else {
                    let url = std::env::var("BOSON_TEST_NATS_URL")
                        .unwrap_or_else(|_| "nats://127.0.0.1:4222".into());
                    connect_auto(&url).await?
                };
                let dyn_backend: Arc<dyn QueueBackend> = Arc::clone(&backend) as Arc<dyn QueueBackend>;
                boson_core::QueueRouter::set_global(boson_core::QueueRouter::with_default(
                    Arc::clone(&dyn_backend),
                ));
                self.backend = Some(dyn_backend);
            }
        }
        match self.matrix.telemetry {
            TelemetryAdapter::Off => install_ops_log(Arc::new(NoOpsLog)),
            TelemetryAdapter::Console => install_ops_log(Arc::new(ConsoleOpsLog)),
        }
        self.ready = true;
        Ok(())
    }

    fn require_ready(&self) -> Result<()> {
        if self.ready {
            Ok(())
        } else {
            bail!("BootstrapSession::install must succeed before build")
        }
    }

    fn configure_builder(&self, builder: BosonBuilder) -> BosonBuilder {
        let mut builder = builder
            .worker_id(self.matrix.worker_id())
            .lease_ttl_secs(self.matrix.lease_ttl_secs())
            .runtime_label(self.matrix.runtime_label());
        if let Some(mode) = self.idempotency_mode {
            builder = builder.idempotency_mode(mode);
        }
        builder
    }

    /// Build a [`Boson`] with background worker for the session matrix.
    ///
    /// # Errors
    ///
    /// Returns an error if install has not completed, no queue backend is available,
    /// or Boson construction fails.
    pub fn build_boson(&self) -> Result<Boson> {
        self.require_ready()?;
        let backend = self
            .backend
            .clone()
            .or_else(|| {
                boson_core::default_backend_from_global()
                    .ok()
                    .map(|b| b as Arc<dyn QueueBackend>)
            })
            .ok_or_else(|| anyhow::anyhow!("no queue backend installed"))?;
        self.configure_builder(
            Boson::builder()
                .queue_backend(backend)
                .execution_context_factory(StubExecutionContextFactory)
                .registry(Arc::clone(&self.registry)),
        )
        .build()
        .map_err(Into::into)
    }

    /// Build Boson without worker plus manual driver (deterministic tests).
    ///
    /// # Errors
    ///
    /// Returns an error if install has not completed, no queue backend is available,
    /// or Boson construction fails.
    pub fn build_boson_manual(&self) -> Result<(Boson, ManualWorker)> {
        self.require_ready()?;
        let backend = self
            .backend
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no queue backend"))?;
        self.configure_builder(
            Boson::builder()
                .queue_backend(backend)
                .execution_context_factory(StubExecutionContextFactory)
                .registry(Arc::clone(&self.registry))
                .without_worker(),
        )
        .build_manual()
        .map_err(Into::into)
    }

    /// Spawn W background workers sharing this session's backend and registry.
    ///
    /// # Errors
    ///
    /// Returns an error if install has not completed or no queue backend is available.
    pub fn spawn_background_workers(&self, count: u32, poll_interval_ms: u64) -> Result<()> {
        self.require_ready()?;
        let backend = self
            .backend
            .clone()
            .ok_or_else(|| anyhow::anyhow!("no queue backend"))?;
        let registry = self.registry();
        let identity: Arc<dyn ExecutionContextFactory> = Arc::new(StubExecutionContextFactory);
        for i in 0..count {
            spawn_worker(
                Arc::clone(&backend),
                Arc::clone(&registry),
                Arc::clone(&identity),
                WorkerSettings {
                    worker_id: format!("{}-{i}", self.matrix.worker_id()),
                    lease_ttl_secs: self.matrix.lease_ttl_secs(),
                    runtime_label: self.matrix.runtime_label().to_string(),
                    worker_pools: None,
                    worker_poll_interval_ms: poll_interval_ms,
                    skip_run_persistence: false,
                },
            );
        }
        Ok(())
    }
}
