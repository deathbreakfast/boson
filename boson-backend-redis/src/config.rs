//! Connection settings for [`crate::RedisQueueBackend`].

/// Connection settings for [`crate::RedisQueueBackend`].
#[derive(Debug, Clone)]
pub struct RedisQueueConfig {
    /// Redis URL (e.g. `redis://127.0.0.1:6379`).
    pub url: String,
    /// Key namespace prefix (default `boson`).
    pub key_prefix: String,
}

impl Default for RedisQueueConfig {
    fn default() -> Self {
        Self {
            url: "redis://127.0.0.1:6379".into(),
            key_prefix: "boson".into(),
        }
    }
}
