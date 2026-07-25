//! HTTP admin API under `/api/boson`.
//!
//! Requires a booted [`boson_runtime::Boson`] — see
//! [Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started) on the
//! [`boson`](https://docs.rs/uf-boson) crate before mounting this router
//! ([§ 5](https://docs.rs/uf-boson/latest/boson/index.html#5-mount-http-admin-optional)).
//!
//! ## Entry points
//!
//! - [`boson_router`] — mount under [`NEST_PATH`] (`/api/boson`)
//! - [`BosonState`] / [`BosonStateBuilder`] — shared Axum state, optional [`AdminAuth`]
//! - [`AdminAuth`], [`StaticTokenAdminAuth`], [`REQUIRE_ADMIN_AUTH_ENV`] — host auth seam
//! - [`MAX_LIST_LIMIT`] — hard cap for list query `limit`
//!
//! ## Owns / does not own
//!
//! **Owns:** route table, DTO shaping (no `actor_json` / `params_json` on wire), list caps,
//! default non-System HTTP enqueue actor, optional admin auth middleware.
//!
//! **Does not own:** Soliton HMAC, mTLS, session cookies — hosts implement [`AdminAuth`].
//!
//! ## Handlers
//!
//! | Route | Module | Purpose |
//! |-------|--------|---------|
//! | `/tasks` | `handlers::tasks` | List and inspect registered tasks |
//! | `/jobs` | `handlers::jobs` | Enqueue, list, cancel jobs |
//! | `/runs` | `handlers::runs` | Inspect run history |
//! | `/tasks/{name}/config` | `handlers::config` | Task config read/update (no `idempotency_mode`) |
//! | `/tasks/{name}/config/revisions` | `handlers::config` | **Stub** — always returns `[]`; revision history not implemented |
//!
//! See `examples/axum_admin.rs` in the `boson` crate for a runnable server.
//!
//! ## Example — completed setup with admin auth
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use axum::{extract::FromRef, Router};
//! use boson_axum::{
//!     boson_router, BosonState, StaticTokenAdminAuth, NEST_PATH,
//! };
//! use boson_runtime::Boson;
//!
//! #[derive(Clone)]
//! struct AppState {
//!     boson: BosonState,
//! }
//!
//! impl FromRef<AppState> for BosonState {
//!     fn from_ref(state: &AppState) -> Self {
//!         state.boson.clone()
//!     }
//! }
//!
//! fn mount(boson: Boson) -> Result<Router<AppState>, String> {
//!     let state = BosonState::builder(Arc::new(boson))
//!         .admin_auth(Arc::new(StaticTokenAdminAuth::new("lab-token")))
//!         .require_admin_auth(true)
//!         .build()?;
//!     Ok(Router::new()
//!         .nest(NEST_PATH, boson_router())
//!         .with_state(AppState { boson: state }))
//! }
//! ```

mod auth;
mod handlers;
mod limits;
mod router;
mod state;

pub use auth::{
    require_admin_auth_from_env, AdminAuth, AdminAuthError, AllowAllAdminAuth, RequireAdmin,
    StaticTokenAdminAuth, REQUIRE_ADMIN_AUTH_ENV,
};
pub use limits::{
    clamp_list_limit, clamp_retry_policy, DEFAULT_LIST_LIMIT, MAX_LIST_LIMIT, MAX_RETRY_ATTEMPTS,
    MAX_RETRY_DELAY_MS, MIN_RETRY_DELAY_MS,
};
pub use router::{boson_router, NEST_PATH};
pub use state::{BosonState, BosonStateBuilder, HttpEnqueueActorProvider};
