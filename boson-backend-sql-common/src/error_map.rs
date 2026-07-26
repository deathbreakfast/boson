//! Map sqlx errors to [`BosonError`](boson_core::BosonError).

use boson_core::{redact_credentials_in_text, redact_endpoint, BosonError};

/// Owned `sqlx` error text used as [`std::error::Error::source`] when the original
/// `sqlx::Error` cannot be moved (call sites typically pass `&sqlx::Error`).
#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct SqlBackendCause(String);

/// Convert a `sqlx` error into [`BosonError::Backend`](boson_core::BosonError::Backend).
///
/// Display text redacts URL userinfo when present. The redacted display text is also
/// preserved as [`std::error::Error::source`].
pub fn map_err(e: &sqlx::Error) -> BosonError {
    let detail = redact_credentials_in_text(&e.to_string());
    let message = format!("sql backend: {detail}");
    BosonError::backend_source(message, SqlBackendCause(detail))
}

/// Connect failure labeled with a redacted endpoint (Postgres / `SQLite`).
pub fn map_connect_err(kind: &str, url: &str, e: &sqlx::Error) -> BosonError {
    let detail = redact_credentials_in_text(&e.to_string());
    let message = format!("{kind} connect {}: {detail}", redact_endpoint(url));
    BosonError::backend_source(message, SqlBackendCause(detail))
}

#[cfg(test)]
mod tests {
    use std::error::Error as StdError;

    use super::{map_connect_err, map_err};

    #[test]
    fn map_err_redacts_embedded_userinfo() {
        // Simulate a Display that embeds a passworded URL (sqlx often does).
        let msg = "error communicating with database: postgres://u:secret@localhost/db";
        let redacted = boson_core::redact_credentials_in_text(msg);
        assert!(redacted.contains("postgres://***@localhost/db"));
        assert!(!redacted.contains("secret"));
        // PoolTimedOut has no URL; ensure map_err still surfaces a backend error.
        let mapped = map_err(&sqlx::Error::PoolTimedOut);
        assert!(mapped.to_string().contains("sql backend"));
        assert!(mapped.to_string().contains("timed out") || mapped.to_string().contains("Timeout"));
        assert!(mapped.source().is_some());
    }

    #[test]
    fn map_connect_err_redacts_userinfo() {
        let mapped = map_connect_err(
            "postgres",
            "postgres://user:secret@host/db",
            &sqlx::Error::PoolTimedOut,
        );
        let msg = mapped.to_string();
        assert!(msg.contains("postgres://***@host/db"), "msg: {msg}");
        assert!(!msg.contains("secret"), "msg: {msg}");
        assert!(mapped.source().is_some());
    }
}
