//! Optional policy for validating opaque `actor_json` at enqueue time.
//!
//! Boson stores actor JSON as opaque data and reconstructs handler context via
//! [`ExecutionContextFactory`](crate::ExecutionContextFactory). Hosts that map JSON to
//! privileged identities (for example a `System` shape) should install an
//! [`ActorJsonPolicy`] so untrusted enqueue paths cannot mint elevated actors.

use serde_json::Value;

use crate::error::{BosonError, Result};

/// Trust level for an enqueue call site.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EnqueueTrust {
    /// In-process / bootstrapped callers (may mint elevated actor shapes when policy allows).
    Internal,
    /// HTTP admin or other externally reachable enqueue surfaces.
    External,
}

/// Validates `actor_json` before a job is persisted.
///
/// # Errors
///
/// Implementations return [`BosonError::InvalidConfig`] when the actor is rejected.
pub trait ActorJsonPolicy: Send + Sync {
    /// Validate actor JSON for the given trust level.
    ///
    /// # Errors
    ///
    /// Returns an error when the actor must not be stored.
    fn validate(&self, trust: EnqueueTrust, actor_json: &Value) -> Result<()>;
}

/// Rejects well-known System-shaped actors on [`EnqueueTrust::External`] paths.
///
/// Recognizes `{"System": ...}` object keys (case-sensitive), matching common UF actor JSON.
#[derive(Debug, Default, Clone, Copy)]
pub struct RejectExternalSystemActor;

impl ActorJsonPolicy for RejectExternalSystemActor {
    fn validate(&self, trust: EnqueueTrust, actor_json: &Value) -> Result<()> {
        if trust == EnqueueTrust::External && actor_json.get("System").is_some() {
            return Err(BosonError::InvalidConfig(
                "external enqueue cannot use System-shaped actor_json".into(),
            ));
        }
        Ok(())
    }
}

/// Default HTTP / external enqueue actor (non-System service marker).
///
/// Shape: `{"Service":{"name":"boson_api"}}`. Hosts that elevate privileges from actor JSON
/// must not treat this marker as System.
#[must_use]
pub fn default_http_enqueue_actor() -> Value {
    serde_json::json!({"Service": {"name": "boson_api"}})
}

/// True when `actor_json` uses the well-known System object key.
#[must_use]
pub fn is_system_shaped_actor(actor_json: &Value) -> bool {
    actor_json.get("System").is_some()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reject_external_system() {
        let policy = RejectExternalSystemActor;
        let system = serde_json::json!({"System": {"operation": "x"}});
        assert!(policy
            .validate(EnqueueTrust::External, &system)
            .unwrap_err()
            .to_string()
            .contains("System"));
        assert!(policy.validate(EnqueueTrust::Internal, &system).is_ok());
    }

    #[test]
    fn allow_external_service() {
        let policy = RejectExternalSystemActor;
        let service = default_http_enqueue_actor();
        assert!(!is_system_shaped_actor(&service));
        assert!(policy.validate(EnqueueTrust::External, &service).is_ok());
    }
}
