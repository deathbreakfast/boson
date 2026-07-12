//! Stub identity for testkit scenarios.

use boson_core::{ExecutionContext, ExecutionContextFactory, IdentityError};

/// Test double that accepts any actor JSON.
#[derive(Debug, Default, Clone, Copy)]
pub struct StubExecutionContextFactory;

struct StubContext {
    label: String,
    actor_json: serde_json::Value,
}

impl ExecutionContext for StubContext {
    fn label(&self) -> &str {
        &self.label
    }

    fn actor_json(&self) -> &serde_json::Value {
        &self.actor_json
    }
}

impl ExecutionContextFactory for StubExecutionContextFactory {
    fn build(
        &self,
        actor_json: &serde_json::Value,
    ) -> Result<Box<dyn ExecutionContext>, IdentityError> {
        Ok(Box::new(StubContext {
            label: actor_json.to_string(),
            actor_json: actor_json.clone(),
        }))
    }
}
