//! Map sqlx errors to [`BosonError`](boson_core::BosonError).

use boson_core::BosonError;

/// Convert a `sqlx` error into [`BosonError::Backend`](boson_core::BosonError::Backend).
pub fn map_err(e: &sqlx::Error) -> BosonError {
    BosonError::Backend(e.to_string())
}
