//! Shared matrix, scenarios, and bootstrap for boson-e2e and boson-bench.
//!
//! ## Entry points
//!
//! - [`BootstrapSession`] — install runtime + backend for a [`MatrixSpec`]
//! - [`ScenarioRunner`] — execute declarative [`ScenarioSpec`] steps
//! - [`ScenarioSpec`] / [`ScenarioStep`] — shared scenario catalog for e2e and bench
//! - [`correctness_catalog`] / [`matrix_scenario_suite!`] — happy/sad matrix for all backends
//! - [`backend_contract_suite!`] — `QueueBackend` contract suite for adapter crates
//! - [`MatrixSpec`] — backend × deployment layout × telemetry dimensions
//!
//! Used by `boson-e2e` for correctness runs and `boson-bench` for performance campaigns.
//! See `boson-bench/EXPERIMENTS.md` for benchmark matrix details.

pub mod backend_contract;
pub mod bootstrap;
pub mod fixtures;
pub mod identity;
#[macro_use]
pub mod macros;
pub mod matrix;
pub mod runner;
pub mod scenario;

#[doc(hidden)]
pub use paste as __paste;

pub use backend_contract::BackendEnv;
pub use bootstrap::BootstrapSession;
pub use fixtures::{assert_task_registered, system_actor, TaskPolicy};
pub use identity::StubExecutionContextFactory;
pub use matrix::{
    e2e_storage_backends, matrix_isolated_lab, matrix_isolated_lab_console,
    matrix_split_boson_server, smoke_storage_backends, BackendAdapter, MatrixSpec,
};
pub use runner::{RunMode, ScenarioResult, ScenarioRunner, StepTiming};
pub use scenario::{
    backend_service_ready, correctness_catalog, run_catalog_entry, run_named_catalog_entry,
    CatalogEntry, CatalogTopology, EnqueueErrorKind, PathKind, RegisterKind, ScenarioSpec,
    ScenarioStep,
};
