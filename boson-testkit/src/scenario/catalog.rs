//! Shared happy/sad correctness catalog for all storage backends.
//!
//! Adapter authors do not redefine scenarios. Add a [`BackendAdapter`] and
//! bootstrap arm, append to `e2e_storage_backends`, and the matrix macros
//! expand the suite automatically.

use boson_core::IdempotencyMode;

use crate::fixtures::{
    register_counting_task, register_fail_exhaustion_task, register_fail_n_then_ok_task,
    register_fail_task, register_noop_task, register_noop_task_with_priority,
    register_rate_limited_eps_task, register_rate_limited_in_flight_task, reset_counting_hits,
    reset_noop_hits,
};
use crate::matrix::{
    matrix_isolated_lab, matrix_isolated_lab_console, matrix_split_boson_server, BackendAdapter,
    MatrixSpec,
};
use crate::scenario::ScenarioSpec;
use crate::BootstrapSession;

/// Happy vs sad path label for catalog entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PathKind {
    /// Expected success / policy-compliant behavior.
    Happy,
    /// Expected rejection, failure, or error path.
    Sad,
}

/// Topology dimension for a catalog entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatalogTopology {
    /// Isolated lab (no leases).
    IsolatedLab,
    /// Split boson-server (leases on).
    SplitBosonServer,
    /// Isolated lab with console telemetry.
    IsolatedLabConsole,
}

impl CatalogTopology {
    const fn matrix(self, backend: BackendAdapter) -> MatrixSpec {
        match self {
            Self::IsolatedLab => matrix_isolated_lab(backend),
            Self::SplitBosonServer => matrix_split_boson_server(backend),
            Self::IsolatedLabConsole => matrix_isolated_lab_console(backend),
        }
    }
}

/// Which synthetic tasks to register before install.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RegisterKind {
    /// Single `noop` task.
    Noop,
    /// `counting` task (hit counter).
    Counting,
    /// Always-fail task (`max_attempts = 1`).
    Fail,
    /// Always-fail with retries (`max_attempts = 3`) for exhaustion.
    FailExhaustion,
    /// Fail twice then succeed.
    Retryable,
    /// Rate-limited in-flight task `limited`.
    RateLimitedInFlight,
    /// Rate-limited EPS task `limited_eps`.
    RateLimitedEps,
    /// Low/high priority noop pair (`low`, `high`).
    PriorityPair,
    /// No tasks (unknown-task sad path).
    None,
    /// Noop registered with signature hash `1` (signature versioning tests).
    SignatureNoop,
}

impl RegisterKind {
    fn apply(self, session: &mut BootstrapSession) {
        let registry = session
            .registry_mut()
            .expect("unique registry before install");
        match self {
            Self::Noop => register_noop_task(registry, "noop"),
            Self::Counting => {
                reset_counting_hits();
                register_counting_task(registry, "counting");
            }
            Self::Fail => register_fail_task(registry, "fail"),
            Self::FailExhaustion => register_fail_exhaustion_task(registry, "fail_exhaust", 3),
            Self::Retryable => register_fail_n_then_ok_task(registry, "retryable", 2),
            Self::RateLimitedInFlight => {
                register_rate_limited_in_flight_task(registry, "limited");
            }
            Self::RateLimitedEps => register_rate_limited_eps_task(registry, "limited_eps"),
            Self::PriorityPair => {
                reset_noop_hits();
                reset_counting_hits();
                // Lower priority number = higher priority. `counting` (1) before `noop` (5).
                register_noop_task_with_priority(registry, "noop", 5);
                register_counting_task(registry, "counting");
            }
            Self::None => {}
            Self::SignatureNoop => {
                crate::fixtures::register_noop_task_with_signature_hash(registry, "noop", 1);
            }
        }
    }
}

/// One row in the shared correctness catalog.
#[derive(Debug, Clone, Copy)]
pub struct CatalogEntry {
    /// Stable id (matches test name prefix and macro entry).
    pub id: &'static str,
    /// Happy or sad path.
    pub path: PathKind,
    /// Topology for bootstrap.
    pub topology: CatalogTopology,
    /// Task registration.
    pub register: RegisterKind,
    /// Optional runtime idempotency default override.
    pub idempotency_mode: Option<IdempotencyMode>,
    /// Scenario factory.
    pub spec: fn() -> ScenarioSpec,
}

/// Full happy/sad correctness catalog (all backends).
#[must_use]
// Keeping the declarative catalog together makes entries easy to audit and extend.
#[allow(clippy::too_many_lines)]
pub fn correctness_catalog() -> &'static [CatalogEntry] {
    &[
        CatalogEntry {
            id: "enqueue_and_drain",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::enqueue_and_drain("noop"),
        },
        CatalogEntry {
            id: "enqueue_only",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::enqueue_only("noop", 1),
        },
        CatalogEntry {
            id: "multi_job_drain",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Counting,
            idempotency_mode: None,
            spec: || ScenarioSpec::multi_job_drain("counting", 5),
        },
        CatalogEntry {
            id: "enqueue_unknown_task",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::None,
            idempotency_mode: None,
            spec: ScenarioSpec::enqueue_unknown_task,
        },
        CatalogEntry {
            id: "rate_limit_in_flight",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::RateLimitedInFlight,
            idempotency_mode: None,
            spec: || ScenarioSpec::rate_limit_in_flight("limited"),
        },
        CatalogEntry {
            id: "rate_limit_eps",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::RateLimitedEps,
            idempotency_mode: None,
            spec: || ScenarioSpec::rate_limit_eps("limited_eps"),
        },
        CatalogEntry {
            id: "task_config_rate_limit",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::task_config_rate_limit("noop"),
        },
        CatalogEntry {
            id: "idempotency_smoke",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::idempotency_smoke("noop"),
        },
        CatalogEntry {
            id: "idempotency_reuse_while_queued",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::idempotency_reuse_while_queued("noop"),
        },
        CatalogEntry {
            id: "idempotency_after_terminal",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::idempotency_after_terminal("noop"),
        },
        CatalogEntry {
            id: "idempotency_none_allows_dup",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: Some(IdempotencyMode::None),
            spec: || ScenarioSpec::idempotency_none_allows_dup("noop"),
        },
        CatalogEntry {
            id: "run_lifecycle",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::run_lifecycle("noop"),
        },
        CatalogEntry {
            id: "handler_failure_terminal",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Fail,
            idempotency_mode: None,
            spec: || ScenarioSpec::handler_failure_terminal("fail"),
        },
        CatalogEntry {
            id: "retry_then_success",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Retryable,
            idempotency_mode: None,
            spec: || ScenarioSpec::retry_then_success("retryable", 2),
        },
        CatalogEntry {
            id: "retry_exhaustion",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::FailExhaustion,
            idempotency_mode: None,
            spec: || ScenarioSpec::retry_exhaustion("fail_exhaust"),
        },
        CatalogEntry {
            id: "cancel_queued_job",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::cancel_queued_job("noop"),
        },
        CatalogEntry {
            id: "cancel_missing_job",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: ScenarioSpec::cancel_missing_job,
        },
        CatalogEntry {
            id: "restart_runtime_drain",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::restart_runtime_drain("noop"),
        },
        CatalogEntry {
            id: "pool_priority_drain",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::PriorityPair,
            idempotency_mode: None,
            spec: || ScenarioSpec::pool_priority_drain("noop", "counting"),
        },
        CatalogEntry {
            id: "list_and_count_jobs",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::list_and_count_jobs("noop"),
        },
        CatalogEntry {
            id: "get_job_not_found",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: ScenarioSpec::get_job_not_found,
        },
        CatalogEntry {
            id: "enqueue_and_drain_split",
            path: PathKind::Happy,
            topology: CatalogTopology::SplitBosonServer,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::enqueue_and_drain("noop"),
        },
        CatalogEntry {
            id: "lease_contention_drain",
            path: PathKind::Sad,
            topology: CatalogTopology::SplitBosonServer,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::lease_contention_drain("noop"),
        },
        CatalogEntry {
            id: "multi_job_drain_split",
            path: PathKind::Happy,
            topology: CatalogTopology::SplitBosonServer,
            register: RegisterKind::Counting,
            idempotency_mode: None,
            spec: || ScenarioSpec::multi_job_drain("counting", 16),
        },
        CatalogEntry {
            id: "restart_runtime_drain_split",
            path: PathKind::Happy,
            topology: CatalogTopology::SplitBosonServer,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::restart_runtime_drain("noop"),
        },
        CatalogEntry {
            id: "enqueue_and_drain_console",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLabConsole,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::enqueue_and_drain("noop"),
        },
        CatalogEntry {
            id: "task_run_stats",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::task_run_stats("noop"),
        },
        CatalogEntry {
            id: "list_jobs_pagination",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Noop,
            idempotency_mode: None,
            spec: || ScenarioSpec::list_and_count_at_depth("noop", 5),
        },
        CatalogEntry {
            id: "retry_run_count",
            path: PathKind::Happy,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::Retryable,
            idempotency_mode: None,
            spec: || ScenarioSpec::retry_run_count("retryable", 2),
        },
        CatalogEntry {
            id: "signature_mismatch",
            path: PathKind::Sad,
            topology: CatalogTopology::IsolatedLab,
            register: RegisterKind::SignatureNoop,
            idempotency_mode: None,
            spec: || ScenarioSpec::signature_mismatch(),
        },
    ]
}

/// Whether the backend's service env is set (postgres / scylla). Mem and sqlite are always ready.
#[must_use]
pub fn backend_service_ready(backend: BackendAdapter) -> bool {
    match backend {
        BackendAdapter::Mem | BackendAdapter::Sqlite => true,
        BackendAdapter::Postgres => {
            std::env::var("BOSON_TEST_POSTGRES_URL").is_ok()
                || std::env::var("BOSON_BENCH_POSTGRES_URL").is_ok()
        }
        BackendAdapter::Scylla => boson_backend_scylla::scylla_test_contact_points().is_some(),
        BackendAdapter::Redis => {
            std::env::var("BOSON_TEST_REDIS_URL").is_ok()
                || std::env::var("BOSON_BENCH_REDIS_URL").is_ok()
        }
        BackendAdapter::Nats => {
            std::env::var("BOSON_TEST_NATS_URL").is_ok()
                || std::env::var("BOSON_BENCH_NATS_URL").is_ok()
                || std::env::var("BOSON_NATS_URLS").is_ok()
        }
    }
}

/// Look up a catalog entry by id and run it against `backend`.
///
/// Skips (no panic) when the backend service env is unset.
///
/// # Panics
///
/// Panics if `id` is not in [`correctness_catalog`], bootstrap fails when the service is ready,
/// or the scenario reports an error.
pub async fn run_named_catalog_entry(backend: BackendAdapter, id: &str) {
    if !backend_service_ready(backend) {
        eprintln!(
            "catalog entry {id}/{}: service env not set — skipping",
            backend_slug(backend)
        );
        return;
    }
    let entry = correctness_catalog()
        .iter()
        .find(|e| e.id == id)
        .unwrap_or_else(|| panic!("unknown catalog entry id: {id}"));
    run_catalog_entry(backend, entry).await;
}

/// Run one catalog entry against `backend`.
///
/// # Panics
///
/// Panics on bootstrap or scenario failure.
pub async fn run_catalog_entry(backend: BackendAdapter, entry: &CatalogEntry) {
    let matrix = entry.topology.matrix(backend);
    let mut session = BootstrapSession::new(matrix);
    if let Some(mode) = entry.idempotency_mode {
        session = session.with_idempotency_mode(mode);
    }
    entry.register.apply(&mut session);
    session
        .install()
        .await
        .unwrap_or_else(|e| panic!("bootstrap {}/{}: {e}", entry.id, backend_slug(backend)));
    let spec = (entry.spec)();
    let result = crate::ScenarioRunner::new(&session)
        .run(&spec, crate::RunMode::Correctness)
        .await
        .unwrap_or_else(|e| panic!("scenario run {}/{}: {e}", entry.id, backend_slug(backend)));
    assert!(
        result.error.is_none(),
        "scenario {}/{} failed: {:?}",
        entry.id,
        backend_slug(backend),
        result.error
    );
}

const fn backend_slug(backend: BackendAdapter) -> &'static str {
    match backend {
        BackendAdapter::Mem => "mem",
        BackendAdapter::Sqlite => "sqlite",
        BackendAdapter::Postgres => "postgres",
        BackendAdapter::Scylla => "scylla",
        BackendAdapter::Redis => "redis",
        BackendAdapter::Nats => "nats",
    }
}
