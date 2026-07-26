//! Error helpers for the in-memory backend.

use boson_core::BosonError;

/// Lock poison mapped to backend error.
pub fn lock_err() -> BosonError {
    BosonError::backend("memory backend lock poisoned")
}
