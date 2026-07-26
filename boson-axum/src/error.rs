//! Typed errors for Boson Axum state construction.

use thiserror::Error;

/// Errors from [`BosonStateBuilder::build`](crate::BosonStateBuilder::build).
#[derive(Debug, Clone, PartialEq, Eq, Error)]
pub enum BosonAxumError {
    /// `require_admin_auth` is set but no [`AdminAuth`](crate::AdminAuth) verifier was installed.
    #[error("BOSON_REQUIRE_ADMIN_AUTH is set but no AdminAuth verifier was configured")]
    MissingAdminAuth,
}
