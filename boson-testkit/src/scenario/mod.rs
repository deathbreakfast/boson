//! Declarative scenario steps shared by e2e (assert) and bench (measure).
//!
//! - [`EnqueueErrorKind`] and [`ScenarioStep`] — serde-tagged step definitions
//! - [`ScenarioSpec`] — scenario id + step list with factory helpers
//! - [`catalog`] — shared happy/sad correctness matrix for all backends

pub mod catalog;
mod factories;
mod spec;
mod step;

pub use catalog::{
    backend_service_ready, correctness_catalog, run_catalog_entry, run_named_catalog_entry,
    CatalogEntry, CatalogTopology, PathKind, RegisterKind,
};
pub use spec::ScenarioSpec;
pub use step::{EnqueueErrorKind, ScenarioStep};
