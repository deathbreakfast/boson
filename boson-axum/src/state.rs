//! Shared Axum state for Boson handlers.

use std::sync::Arc;

use boson_runtime::Boson;

/// Extractable state holding a [`Boson`] runtime.
///
/// Construct with [`BosonState::new`] after [`BosonBuilder::build`](boson_runtime::BosonBuilder::build)
/// and inject via [`FromRef`](axum::extract::FromRef) (see the [crate example](crate#example)).
#[derive(Clone)]
pub struct BosonState {
    /// Boson runtime for admin and enqueue operations.
    pub boson: Arc<Boson>,
}

impl BosonState {
    /// Create state from a shared Boson instance.
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
    pub const fn new(boson: Arc<Boson>) -> Self {
        Self { boson }
    }
}
