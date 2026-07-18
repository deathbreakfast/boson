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
//! - [`BosonState`] — shared Axum state holding [`boson_runtime::Boson`]
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
//! ## Example
//!
//! ```rust,no_run
//! use std::sync::Arc;
//!
//! use axum::{extract::FromRef, Router};
//! use boson_axum::{boson_router, BosonState, NEST_PATH};
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
//! fn mount(boson: Boson) -> Router<AppState> {
//!     Router::new()
//!         .nest(NEST_PATH, boson_router())
//!         .with_state(AppState {
//!             boson: BosonState::new(Arc::new(boson)),
//!         })
//! }
//! ```

mod handlers;
mod router;
mod state;

pub use router::{boson_router, NEST_PATH};
pub use state::BosonState;
