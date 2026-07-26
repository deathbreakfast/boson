//! Sanitize operator-visible error strings (runs, telemetry).

use crate::redact::redact_credentials_in_text;

/// Maximum length of persisted / logged handler error messages.
pub const MAX_ERROR_MESSAGE_CHARS: usize = 512;

/// Truncate and strip obvious secret-looking substrings from an error message.
///
/// Also redacts URL userinfo via [`redact_credentials_in_text`]. Callers must still avoid
/// embedding params/actor JSON in errors. Used before persisting run `error_message` and
/// recording telemetry.
#[must_use]
pub fn sanitize_error_message(raw: &str) -> String {
    let mut out = redact_credentials_in_text(&raw.replace('\0', ""));
    for needle in [
        "password=",
        "Password=",
        "secret=",
        "Secret=",
        "token=",
        "Token=",
        "Bearer ",
        "authorization:",
    ] {
        if let Some(idx) = out.find(needle) {
            let end = (idx + needle.len() + 8).min(out.len());
            out.replace_range(idx..end, &format!("{needle}[redacted]"));
        }
    }
    if out.chars().count() > MAX_ERROR_MESSAGE_CHARS {
        out = out.chars().take(MAX_ERROR_MESSAGE_CHARS).collect();
        out.push('…');
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncates_long_messages() {
        let long = "x".repeat(800);
        let s = sanitize_error_message(&long);
        assert!(s.chars().count() <= MAX_ERROR_MESSAGE_CHARS + 1);
        assert!(s.ends_with('…'));
    }

    #[test]
    fn redacts_password_prefix() {
        let s = sanitize_error_message("db failed password=hunter2 more");
        assert!(s.contains("[redacted]"));
        assert!(!s.contains("hunter2"));
    }

    #[test]
    fn redacts_url_userinfo_in_handler_errors() {
        let s = sanitize_error_message("connect redis://user:secret@host:6379 failed");
        assert!(s.contains("redis://***@host:6379"));
        assert!(!s.contains("secret"));
    }
}
