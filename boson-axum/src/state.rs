//! Shared Axum state for Boson handlers.

use std::sync::Arc;

use boson_runtime::Boson;
use serde_json::Value as JsonValue;

use crate::auth::{require_admin_auth_from_env, AdminAuth};

/// Callback that supplies `actor_json` for HTTP enqueue (overrides the default service marker).
pub type HttpEnqueueActorProvider = Arc<dyn Fn() -> JsonValue + Send + Sync>;

/// Extractable state holding a [`Boson`] runtime and optional admin auth.
///
/// Construct with [`BosonState::new`] or [`BosonState::builder`].
#[derive(Clone)]
pub struct BosonState {
    /// Boson runtime for admin and enqueue operations.
    pub boson: Arc<Boson>,
    /// Optional host verifier for admin routes.
    pub admin_auth: Option<Arc<dyn AdminAuth>>,
    /// When true, requests are rejected if [`admin_auth`](Self::admin_auth) is `None`.
    pub require_admin_auth: bool,
    /// Optional override for HTTP enqueue actor JSON.
    pub http_enqueue_actor: Option<HttpEnqueueActorProvider>,
}

impl BosonState {
    /// Create state from a shared Boson instance (no admin auth; require-flag from env).
    ///
    /// ```rust,no_run
    /// use std::sync::Arc;
    /// use boson_axum::BosonState;
    /// use boson_runtime::Boson;
    ///
    /// # fn demo(boson: Boson) {
    /// let state = BosonState::new(Arc::new(boson));
    /// # let _ = state;
    /// # }
    /// ```
    #[must_use]
    pub fn new(boson: Arc<Boson>) -> Self {
        Self {
            boson,
            admin_auth: None,
            require_admin_auth: require_admin_auth_from_env(),
            http_enqueue_actor: None,
        }
    }

    /// Builder for authenticated / customized admin mounts.
    #[must_use]
    pub fn builder(boson: Arc<Boson>) -> BosonStateBuilder {
        BosonStateBuilder {
            boson,
            admin_auth: None,
            require_admin_auth: require_admin_auth_from_env(),
            http_enqueue_actor: None,
        }
    }

    /// Actor JSON used for `POST /jobs/enqueue`.
    #[must_use]
    pub fn enqueue_actor_json(&self) -> JsonValue {
        if let Some(ref provider) = self.http_enqueue_actor {
            return provider();
        }
        boson_core::default_http_enqueue_actor()
    }
}

/// Build [`BosonState`] with admin auth and actor overrides.
pub struct BosonStateBuilder {
    boson: Arc<Boson>,
    admin_auth: Option<Arc<dyn AdminAuth>>,
    require_admin_auth: bool,
    http_enqueue_actor: Option<HttpEnqueueActorProvider>,
}

impl BosonStateBuilder {
    /// Install a host [`AdminAuth`] verifier.
    #[must_use]
    pub fn admin_auth(mut self, auth: Arc<dyn AdminAuth>) -> Self {
        self.admin_auth = Some(auth);
        self
    }

    /// Force require-admin-auth (overrides env when set).
    #[must_use]
    pub const fn require_admin_auth(mut self, require: bool) -> Self {
        self.require_admin_auth = require;
        self
    }

    /// Override HTTP enqueue actor JSON.
    #[must_use]
    pub fn http_enqueue_actor(
        mut self,
        provider: impl Fn() -> JsonValue + Send + Sync + 'static,
    ) -> Self {
        self.http_enqueue_actor = Some(Arc::new(provider));
        self
    }

    /// Build state.
    ///
    /// # Errors
    ///
    /// Returns an error when `require_admin_auth` is set and no verifier was installed.
    pub fn build(self) -> Result<BosonState, String> {
        if self.require_admin_auth && self.admin_auth.is_none() {
            return Err(
                "BOSON_REQUIRE_ADMIN_AUTH is set but no AdminAuth verifier was configured".into(),
            );
        }
        Ok(BosonState {
            boson: self.boson,
            admin_auth: self.admin_auth,
            require_admin_auth: self.require_admin_auth,
            http_enqueue_actor: self.http_enqueue_actor,
        })
    }
}
