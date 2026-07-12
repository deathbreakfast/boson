//! Common API response wrapper.

use serde::{Deserialize, Serialize};

/// Standard API response envelope.
#[derive(Debug, Serialize, Deserialize)]
pub struct ApiResponse<T> {
    /// Whether the request succeeded.
    pub success: bool,
    /// Payload on success.
    pub data: Option<T>,
    /// Error message on failure.
    pub error: Option<String>,
}

impl<T> ApiResponse<T> {
    /// Success response.
    pub const fn ok(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
        }
    }

    /// Error response.
    pub fn err(msg: impl Into<String>) -> Self {
        Self {
            success: false,
            data: None,
            error: Some(msg.into()),
        }
    }
}
