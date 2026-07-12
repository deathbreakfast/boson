use serde::{Deserialize, Serialize};

use super::step::ScenarioStep;

/// Ordered scenario consumed by both drivers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScenarioSpec {
    /// Stable scenario identifier.
    pub id: String,
    /// Ordered steps.
    pub steps: Vec<ScenarioStep>,
}
