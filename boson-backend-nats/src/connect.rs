//! NATS client connection helpers (single or comma-separated URLs).

use boson_core::{map_backend_connect_err, BosonError, Result};

/// Connect to one or more NATS servers (`url` may be comma-separated).
///
/// Error labels redact URL userinfo via [`map_backend_connect_err`].
pub async fn connect_nats(urls: &str) -> Result<async_nats::Client> {
    let servers: Vec<&str> = urls
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .collect();
    if servers.is_empty() {
        return Err(BosonError::Backend("NATS URL empty after parsing".into()));
    }
    if servers.len() == 1 {
        async_nats::connect(servers[0])
            .await
            .map_err(|e| map_backend_connect_err("nats connect", servers[0], e))
    } else {
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(servers)
            .await
            .map_err(|e| map_backend_connect_err("nats connect", urls, e))
    }
}
