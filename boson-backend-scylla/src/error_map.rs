//! Map Scylla driver errors to [`boson_core::BosonError`].

use boson_core::{map_backend_connect_err, redact_credentials_in_text, BosonError};

pub fn map_err(err: impl std::fmt::Display) -> BosonError {
    BosonError::Backend(format!(
        "scylla backend: {}",
        redact_credentials_in_text(&err.to_string())
    ))
}

/// Connect failure labeled with redacted contact-point text.
pub fn map_connect_err(contact_points: &str, err: impl std::fmt::Display) -> BosonError {
    map_backend_connect_err("scylla connect", contact_points, err)
}

pub fn into_result<T>(
    result: std::result::Result<T, impl std::fmt::Display>,
) -> boson_core::Result<T> {
    result.map_err(map_err)
}

#[cfg(test)]
mod tests {
    use super::{map_connect_err, map_err};

    #[test]
    fn map_err_redacts_embedded_url() {
        let err = map_err("session failed scylla://user:secret@node:9042");
        let msg = err.to_string();
        assert!(msg.contains("scylla://***@node:9042"), "msg: {msg}");
        assert!(!msg.contains("secret"), "msg: {msg}");
    }

    #[test]
    fn map_connect_err_redacts_passworded_endpoint() {
        let err = map_connect_err(
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
        let err = map_connect_err("127.0.0.1:9042", "no contact points reachable");
        let msg = err.to_string();
        assert!(msg.contains("scylla connect 127.0.0.1:9042"), "msg: {msg}");
        assert!(msg.contains("no contact points reachable"), "msg: {msg}");
    }
}
