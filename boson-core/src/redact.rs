//! Redact credentials from connection endpoints and error text.

use std::fmt;

use crate::error::BosonError;

/// Remove URL userinfo before placing an endpoint in an error or log message.
///
/// Comma-separated multi-endpoint strings are redacted per segment (Redis / NATS style).
#[must_use]
pub fn redact_endpoint(endpoint: &str) -> String {
    endpoint
        .split(',')
        .map(redact_single_endpoint)
        .collect::<Vec<_>>()
        .join(",")
}

fn redact_single_endpoint(endpoint: &str) -> String {
    let Some(scheme_end) = endpoint.find("://") else {
        return endpoint.to_string();
    };
    let authority_start = scheme_end + 3;
    let authority = &endpoint[authority_start..];
    let Some(userinfo_end) = authority.find('@') else {
        return endpoint.to_string();
    };
    let userinfo_end = authority_start + userinfo_end;
    let host_start = userinfo_end + 1;
    if endpoint[authority_start..userinfo_end].contains(['/', '?', '#']) {
        return endpoint.to_string();
    }
    format!(
        "{}***@{}",
        &endpoint[..authority_start],
        &endpoint[host_start..]
    )
}

/// Length of a URL-like prefix starting at `s` (scheme through host/path until whitespace).
fn consume_endpoint_prefix(s: &str) -> usize {
    let Some(scheme_end) = s.find("://") else {
        return s.len();
    };
    let after_scheme = scheme_end + 3;
    let rest = &s[after_scheme..];
    let end_rel = rest
        .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ')' || c == ']')
        .unwrap_or(rest.len());
    after_scheme + end_rel
}

/// Redact `scheme://userinfo@host` substrings embedded in free-form error text.
#[must_use]
pub fn redact_credentials_in_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    while i < text.len() {
        if let Some(rel) = text[i..].find("://") {
            let abs = i + rel;
            let scheme_start = text[..abs]
                .rfind(|c: char| !(c.is_ascii_alphanumeric() || c == '+' || c == '.' || c == '-'))
                .map_or(i, |j| j + 1);
            out.push_str(&text[i..scheme_start]);
            let consumed = consume_endpoint_prefix(&text[scheme_start..]);
            let endpoint = &text[scheme_start..scheme_start + consumed];
            out.push_str(&redact_endpoint(endpoint));
            i = scheme_start + consumed;
        } else {
            out.push_str(&text[i..]);
            break;
        }
    }
    out
}

/// Backend connect failure labeled with a redacted endpoint and redacted source text.
///
/// `label` is a short prefix such as `"redis connect"` or `"nats connect"`.
/// Use for all adapter connect paths so URL userinfo never lands in [`BosonError::Backend`].
#[must_use]
pub fn map_backend_connect_err(label: &str, endpoint: &str, err: impl fmt::Display) -> BosonError {
    let detail = redact_credentials_in_text(&err.to_string());
    BosonError::Backend(format!("{label} {}: {detail}", redact_endpoint(endpoint)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_url_userinfo() {
        let redacted = redact_endpoint("redis://user:secret@host:6379/0");
        assert_eq!(redacted, "redis://***@host:6379/0");
        assert!(!redacted.contains("secret"));
    }

    #[test]
    fn leaves_urls_without_userinfo() {
        assert_eq!(
            redact_endpoint("nats://127.0.0.1:4222"),
            "nats://127.0.0.1:4222"
        );
    }

    #[test]
    fn redacts_embedded_url_in_error_text() {
        let raw = "connection failed: postgres://u:p@localhost:5432/db refused";
        let redacted = redact_credentials_in_text(raw);
        assert!(redacted.contains("postgres://***@localhost:5432/db"));
        assert!(!redacted.contains("u:p@"));
    }

    #[test]
    fn map_backend_connect_err_redacts_label_and_source() {
        let err = map_backend_connect_err(
            "redis connect",
            "redis://user:secret@host:6379",
            "dial redis://user:secret@host:6379 timed out",
        );
        let msg = err.to_string();
        assert!(
            msg.contains("redis connect redis://***@host:6379"),
            "msg: {msg}"
        );
        assert!(!msg.contains("secret"), "msg: {msg}");
        assert!(msg.contains("redis://***@host:6379"), "msg: {msg}");
    }

    #[test]
    fn map_backend_connect_err_sad_path_still_surfaces_failure() {
        let err = map_backend_connect_err("nats connect", "plain-host:4222", "broker down");
        let msg = err.to_string();
        assert!(msg.contains("nats connect plain-host:4222"), "msg: {msg}");
        assert!(msg.contains("broker down"), "msg: {msg}");
    }
}
