//! Identity reconstruction at the task handler boundary.
//!
//! When a job is enqueued, Boson stores `actor_json` on the job record. When a worker dispatches
//! the job, it calls [`ExecutionContextFactory::build`] to reconstruct handler context from that
//! JSON. Task handlers (including those defined with `#[boson::task]`) receive the result as
//! `Box<dyn ExecutionContext>`.
//!
//! # In task handlers
//!
//! Use [`ExecutionContext::label`] for logs and [`ExecutionContext::actor_json`] when the handler
//! only needs the captured actor payload. You do not configure the factory inside the handler.
//!
//! # Choosing a factory (integrator)
//!
//! Install the factory once at worker boot via
//! [`BosonBuilder::execution_context_factory`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.execution_context_factory).
//! See the [`boson`](https://docs.rs/uf-boson) crate
//! [Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started) for a full boot example.
//!
//! | Approach | When to use |
//! |----------|-------------|
//! | [`JsonExecutionContextFactory`] | Examples, smoke tests, and handlers that only need [`ExecutionContext::label`] and [`ExecutionContext::actor_json`] |
//! | Custom [`ExecutionContextFactory`] | Production apps that map actor JSON to sessions, permissions, database access, or other application identity |
//!
//! # Custom factory sketch
//!
//! ```rust,no_run
//! use boson_core::{ExecutionContext, ExecutionContextFactory, IdentityError};
//! use serde_json::Value;
//!
//! struct AppContext {
//!     actor_json: Value,
//! }
//!
//! impl ExecutionContext for AppContext {
//!     fn label(&self) -> &str {
//!         "app"
//!     }
//!     fn actor_json(&self) -> &Value {
//!         &self.actor_json
//!     }
//! }
//!
//! struct AppFactory;
//!
//! impl ExecutionContextFactory for AppFactory {
//!     fn build(&self, actor_json: &Value) -> Result<Box<dyn ExecutionContext>, IdentityError> {
//!         // Validate actor_json and construct application-specific context.
//!         Ok(Box::new(AppContext {
//!             actor_json: actor_json.clone(),
//!         }))
//!     }
//! }
//! ```

use serde_json::Value;

use crate::error::IdentityError;

/// Opaque execution context for task handlers.
///
/// The runtime passes this as the first argument to `#[boson::task]` handlers and to registered
/// invoke functions. Use [`label`](Self::label) for logs and [`actor_json`](Self::actor_json) when
/// the handler only needs the captured actor payload.
pub trait ExecutionContext: Send {
    /// Debug label for logs and tests.
    fn label(&self) -> &str;

    /// Actor JSON captured at enqueue time and restored at dispatch.
    fn actor_json(&self) -> &Value;
}

/// Builds handler execution context from captured actor JSON at enqueue time.
///
/// Implement this trait (or use [`JsonExecutionContextFactory`]) and pass the factory to
/// [`BosonBuilder::execution_context_factory`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.execution_context_factory) when booting the runtime.
///
/// # Example
///
/// Most apps start with [`JsonExecutionContextFactory`]. Custom factories validate `actor_json` and
/// attach sessions or permissions — see [Custom factory sketch](crate::identity#custom-factory-sketch).
///
/// ```ignore
/// use std::sync::Arc;
///
/// use boson_backend_mem::MemQueueBackend;
/// use boson_core::JsonExecutionContextFactory;
/// use boson_runtime::Boson;
///
/// # fn main() -> boson_core::Result<()> {
/// let _boson = Boson::builder()
///     .queue_backend(Arc::new(MemQueueBackend::new()))
///     .execution_context_factory(JsonExecutionContextFactory)
///     .build()?;
/// # Ok(())
/// # }
/// ```
pub trait ExecutionContextFactory: Send + Sync {
    /// Build context for task dispatch from stored `actor_json`.
    ///
    /// # Errors
    ///
    /// Returns [`IdentityError`] when actor JSON cannot be mapped to application context.
    fn build(&self, actor_json: &Value) -> Result<Box<dyn ExecutionContext>, IdentityError>;
}

/// Default factory that wraps actor JSON in a labeled [`ExecutionContext`].
///
/// Suitable for examples and handlers that only need [`ExecutionContext::label`] and
/// [`ExecutionContext::actor_json`]. For application-specific identity (database sessions,
/// permission checks, typed actors), implement [`ExecutionContextFactory`] instead.
///
/// The default [`label`](ExecutionContext::label) keeps short actor JSON as-is (up to 64
/// characters) and replaces longer payloads with a stable `json:<hash>` form so logs stay
/// compact. The full actor payload remains available via [`actor_json`](ExecutionContext::actor_json).
///
/// # Example
///
/// Pass to [`BosonBuilder::execution_context_factory`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.BosonBuilder.html#method.execution_context_factory) at worker boot:
///
/// ```ignore
/// use std::sync::Arc;
///
/// use boson_backend_mem::MemQueueBackend;
/// use boson_core::JsonExecutionContextFactory;
/// use boson_runtime::Boson;
///
/// # fn main() -> boson_core::Result<()> {
/// let _boson = Boson::builder()
///     .queue_backend(Arc::new(MemQueueBackend::new()))
///     .execution_context_factory(JsonExecutionContextFactory)
///     .auto_registry()
///     .build()?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct JsonExecutionContextFactory;

struct JsonContext {
    actor_json: Value,
    label: String,
}

impl ExecutionContext for JsonContext {
    fn label(&self) -> &str {
        &self.label
    }

    fn actor_json(&self) -> &Value {
        &self.actor_json
    }
}

impl ExecutionContextFactory for JsonExecutionContextFactory {
    fn build(&self, actor_json: &Value) -> Result<Box<dyn ExecutionContext>, IdentityError> {
        Ok(Box::new(JsonContext {
            actor_json: actor_json.clone(),
            label: compact_actor_label(actor_json),
        }))
    }
}

/// Compact log label for actor JSON: keep short payloads; hash long ones.
fn compact_actor_label(actor_json: &Value) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    const MAX_LEN: usize = 64;
    let full = actor_json.to_string();
    if full.len() <= MAX_LEN {
        return full;
    }
    let mut hasher = DefaultHasher::new();
    full.hash(&mut hasher);
    format!("json:{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestContext {
        label: String,
        actor_json: Value,
    }

    impl ExecutionContext for TestContext {
        fn label(&self) -> &str {
            &self.label
        }

        fn actor_json(&self) -> &Value {
            &self.actor_json
        }
    }

    struct TestFactory;

    impl ExecutionContextFactory for TestFactory {
        fn build(&self, actor_json: &Value) -> Result<Box<dyn ExecutionContext>, IdentityError> {
            if actor_json.get("System").is_some() {
                Ok(Box::new(TestContext {
                    label: "system".into(),
                    actor_json: actor_json.clone(),
                }))
            } else {
                Err(IdentityError::InvalidActor("missing System".into()))
            }
        }
    }

    #[test]
    fn factory_builds_context() {
        let factory = TestFactory;
        let actor = serde_json::json!({"System": {"operation": "t"}});
        let ctx = factory.build(&actor).expect("ok");
        assert_eq!(ctx.label(), "system");
        assert_eq!(ctx.actor_json(), &actor);
    }

    #[test]
    fn json_factory_stores_actor_json() {
        let factory = JsonExecutionContextFactory;
        let actor = serde_json::json!({"user": "alice"});
        let ctx = factory.build(&actor).expect("ok");
        assert_eq!(ctx.actor_json(), &actor);
        assert!(ctx.label().contains("alice"));
    }

    #[test]
    fn json_factory_compacts_long_labels() {
        let factory = JsonExecutionContextFactory;
        let actor = serde_json::json!({
            "user": "alice",
            "note": "x".repeat(80),
        });
        let ctx = factory.build(&actor).expect("ok");
        assert_eq!(ctx.actor_json(), &actor);
        assert!(ctx.label().starts_with("json:"));
        assert!(ctx.label().len() < 40);
    }
}
