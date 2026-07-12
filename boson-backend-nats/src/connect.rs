//! NATS client connection helpers (single or comma-separated URLs).

use boson_core::{BosonError, Result};

/// Connect to one or more NATS servers (`url` may be comma-separated).
pub async fn connect_nats(urls: &str) -> Result<async_nats::Client> {
    let servers: Vec<&str> = urls.split(',').map(str::trim).filter(|s| !s.is_empty()).collect();
    if servers.is_empty() {
        return Err(BosonError::Backend(
            "NATS URL empty after parsing".into(),
        ));
    }
    if servers.len() == 1 {
        async_nats::connect(servers[0])
            .await
            .map_err(|e| BosonError::Backend(format!("nats connect {}: {e}", servers[0])))
    } else {
        async_nats::ConnectOptions::new()
            .retry_on_initial_connect()
            .connect(servers)
            .await
            .map_err(|e| BosonError::Backend(format!("nats connect {urls}: {e}")))
    }
}
