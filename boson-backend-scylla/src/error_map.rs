//! Map Scylla driver errors to [`boson_core::BosonError`].

use boson_core::{map_backend_connect_err_source, redact_credentials_in_text, BosonError};

/// Map a Display-only failure (opaque adapter text).
pub fn map_err(err: impl std::fmt::Display) -> BosonError {
    BosonError::backend(format!(
        "scylla backend: {}",
        redact_credentials_in_text(&err.to_string())
    ))
}

/// Map a typed Scylla/`std` error, preserving [`std::error::Error::source`].
pub fn map_err_source(err: impl std::error::Error + Send + Sync + 'static) -> BosonError {
    let message = format!(
        "scylla backend: {}",
        redact_credentials_in_text(&err.to_string())
    );
    BosonError::backend_source(message, err)
}

/// Connect failure preserving the underlying cause.
pub fn map_connect_err_source(
    contact_points: &str,
    err: impl std::error::Error + Send + Sync + 'static,
) -> BosonError {
    map_backend_connect_err_source("scylla connect", contact_points, err)
}

pub fn into_result<T>(
    result: std::result::Result<T, impl std::fmt::Display>,
) -> boson_core::Result<T> {
    result.map_err(map_err)
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use boson_core::map_backend_connect_err;

    use super::{map_err, map_err_source};

    #[test]
    fn map_err_redacts_embedded_url() {
        let err = map_err("session failed scylla://user:secret@node:9042");
        let msg = err.to_string();
        assert!(msg.contains("scylla://***@node:9042"), "msg: {msg}");
        assert!(!msg.contains("secret"), "msg: {msg}");
    }

    #[test]
    fn map_err_source_preserves_cause() {
        #[derive(Debug)]
        struct SessionBoom;
        impl std::fmt::Display for SessionBoom {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("session boom")
            }
        }
        impl StdError for SessionBoom {}

        let err = map_err_source(SessionBoom);
        assert!(err
            .source()
            .is_some_and(|s| s.to_string().contains("session boom")));
    }

    #[test]
    fn map_connect_err_redacts_passworded_endpoint() {
        let err = map_backend_connect_err(
            "scylla connect",
            "scylla://admin:hunter2@127.0.0.1:9042",
            "unable to connect scylla://admin:hunter2@127.0.0.1:9042",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("scylla connect scylla://***@127.0.0.1:9042"),
            "msg: {msg}"
        );
        assert!(!msg.contains("hunter2"), "msg: {msg}");
    }

    #[test]
    fn map_connect_err_sad_path_surfaces_failure() {
        let err = map_backend_connect_err(
            "scylla connect",
            "127.0.0.1:9042",
            "no contact points reachable",
        );
        let msg = err.to_string();
        assert!(msg.contains("scylla connect 127.0.0.1:9042"), "msg: {msg}");
        assert!(msg.contains("no contact points reachable"), "msg: {msg}");
    }
}
