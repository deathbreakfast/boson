//! Shared Axum state for Boson handlers.

use std::sync::Arc;

use boson_runtime::Boson;

/// Extractable state holding a [`Boson`] runtime.
#[derive(Clone)]
pub struct BosonState {
    /// Boson runtime for admin and enqueue operations.
    pub boson: Arc<Boson>,
}

impl BosonState {
    /// Create state from a shared Boson instance.
    #[must_use]
    pub const fn new(boson: Arc<Boson>) -> Self {
        Self { boson }
    }
}
