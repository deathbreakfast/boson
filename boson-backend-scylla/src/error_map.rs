//! Map Scylla driver errors to [`boson_core::BosonError`].

use boson_core::BosonError;

pub fn map_err(err: impl std::fmt::Display) -> BosonError {
    BosonError::Backend(err.to_string())
}

pub fn into_result<T>(
    result: std::result::Result<T, impl std::fmt::Display>,
) -> boson_core::Result<T> {
    result.map_err(map_err)
}
