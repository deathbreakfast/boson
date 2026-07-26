//! Optional admin authentication for `/api/boson`.
//!
//! Boson does not ship Soliton/HMAC. Hosts implement [`AdminAuth`] and attach it via
//! [`BosonStateBuilder`](crate::BosonStateBuilder). When [`require_admin_auth_from_env`] is true
//! and no verifier is configured, requests are rejected (fail closed).
//!
//! Handlers take [`RequireAdmin`] as an extractor so auth runs with the host's `FromRef` state.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use axum::{
    extract::{FromRef, FromRequestParts},
    http::{request::Parts, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::state::BosonState;

/// Environment variable: when `1`/`true`/`yes`, admin routes require a configured [`AdminAuth`].
pub const REQUIRE_ADMIN_AUTH_ENV: &str = "BOSON_REQUIRE_ADMIN_AUTH";

/// Rejection from [`AdminAuth::authorize`].
#[derive(Debug, Clone)]
pub struct AdminAuthError {
    message: String,
}

impl AdminAuthError {
    /// Create an authorization error with a safe (non-secret) message.
    #[must_use]
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }

    /// Operator-safe message.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Host-supplied verifier for Boson admin HTTP.
pub trait AdminAuth: Send + Sync {
    /// Authorize one request from its HTTP parts (headers, URI, method).
    ///
    /// # Errors
    ///
    /// Return [`AdminAuthError`] when the caller must not access admin routes.
    fn authorize<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdminAuthError>> + Send + 'a>>;
}

/// Parse a require-admin-auth flag string (`1` / `true` / `yes`, case-insensitive).
#[must_use]
pub fn parse_require_admin_auth(value: &str) -> bool {
    let v = value.trim().to_ascii_lowercase();
    matches!(v.as_str(), "1" | "true" | "yes")
}

/// Read [`REQUIRE_ADMIN_AUTH_ENV`]: `1`, `true`, or `yes` (case-insensitive) ⇒ required.
#[must_use]
pub fn require_admin_auth_from_env() -> bool {
    std::env::var(REQUIRE_ADMIN_AUTH_ENV).is_ok_and(|v| parse_require_admin_auth(&v))
}

fn unauthorized(message: impl Into<String>) -> Response {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "success": false,
            "data": null,
            "error": message.into(),
        })),
    )
        .into_response()
}

/// Extractor that enforces [`BosonState::admin_auth`] / require-flag before the handler runs.
pub struct RequireAdmin;

impl<S> FromRequestParts<S> for RequireAdmin
where
    S: Send + Sync,
    BosonState: FromRef<S>,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let boson_state = BosonState::from_ref(state);
        match (&boson_state.admin_auth, boson_state.require_admin_auth) {
            (Some(auth), _) => match auth.authorize(parts).await {
                Ok(()) => Ok(Self),
                Err(e) => Err(unauthorized(e.message().to_string())),
            },
            (None, true) => Err(unauthorized(
                "admin auth required but no AdminAuth verifier configured",
            )),
            (None, false) => Ok(Self),
        }
    }
}

/// Shared always-allow verifier for local tests (not for production).
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllAdminAuth;

impl AdminAuth for AllowAllAdminAuth {
    fn authorize<'a>(
        &'a self,
        _parts: &'a Parts,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdminAuthError>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

/// Header-based verifier: require `x-boson-admin-token` equal to the configured secret.
///
/// Intended for tests and simple lab setups. Prefer host mTLS/HMAC in production.
#[derive(Debug, Clone)]
pub struct StaticTokenAdminAuth {
    token: Arc<str>,
}

impl StaticTokenAdminAuth {
    /// Create a verifier that accepts a single shared token.
    #[must_use]
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: Arc::from(token.into()),
        }
    }
}

impl AdminAuth for StaticTokenAdminAuth {
    fn authorize<'a>(
        &'a self,
        parts: &'a Parts,
    ) -> Pin<Box<dyn Future<Output = Result<(), AdminAuthError>> + Send + 'a>> {
        let expected = Arc::clone(&self.token);
        let header = parts
            .headers
            .get("x-boson-admin-token")
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned);
        Box::pin(async move {
            match header {
                Some(ref got) if got.as_str() == expected.as_ref() => Ok(()),
                _ => Err(AdminAuthError::new(
                    "missing or invalid x-boson-admin-token",
                )),
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use axum::http::Request;

    use super::*;

    fn parts_with_token(token: Option<&str>) -> Parts {
        let mut builder = Request::builder().uri("/");
        if let Some(t) = token {
            builder = builder.header("x-boson-admin-token", t);
        }
        builder.body(()).expect("request").into_parts().0
    }

    #[test]
    fn parse_require_admin_auth_truthy_and_falsy() {
        for v in ["1", "true", "YES", " True "] {
            assert!(parse_require_admin_auth(v), "expected truthy: {v}");
        }
        for v in ["0", "false", "no", ""] {
            assert!(!parse_require_admin_auth(v), "expected falsy: {v}");
        }
    }

    #[tokio::test]
    async fn static_token_accepts_matching_header() {
        let auth = StaticTokenAdminAuth::new("lab-secret");
        let parts = parts_with_token(Some("lab-secret"));
        assert!(auth.authorize(&parts).await.is_ok());
    }

    #[tokio::test]
    async fn static_token_rejects_missing_or_wrong_header() {
        let auth = StaticTokenAdminAuth::new("lab-secret");
        assert!(auth.authorize(&parts_with_token(None)).await.is_err());
        assert!(auth
            .authorize(&parts_with_token(Some("nope")))
            .await
            .is_err());
    }
}
