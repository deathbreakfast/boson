//! Pipelined `JetStream` publish with optional ack wait.

use std::sync::Arc;

use async_nats::jetstream::context::PublishAckFuture;
use boson_core::{redact_credentials_in_text, BosonError, Result};
use boson_telemetry::ops_log;
use bytes::Bytes;
use serde_json::json;
use tokio::sync::Semaphore;

use crate::config::NatsEnqueueConfig;

/// Limits concurrent in-flight `JetStream` publishes for one backend.
pub struct PublishPipeline {
    semaphore: Arc<Semaphore>,
    sync_ack: bool,
}

impl PublishPipeline {
    #[must_use]
    pub fn new(config: NatsEnqueueConfig) -> Self {
        let max = config.max_inflight.max(1) as usize;
        Self {
            semaphore: Arc::new(Semaphore::new(max)),
            sync_ack: config.sync_ack,
        }
    }

    /// Publish and optionally await `JetStream` ack.
    ///
    /// # Errors
    ///
    /// Returns an error when publish or ack fails.
    pub async fn publish(
        &self,
        jetstream: &async_nats::jetstream::Context,
        subject: String,
        body: Bytes,
    ) -> Result<()> {
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
            BosonError::backend_source(
                format!(
                    "nats publish pipeline: {}",
                    redact_credentials_in_text(&e.to_string())
                ),
                e,
            )
        })?;

        let ack_future: PublishAckFuture =
            jetstream
                .publish(subject.clone(), body)
                .await
                .map_err(|e| {
                    BosonError::backend_source(
                        format!(
                            "nats publish {subject}: {}",
                            redact_credentials_in_text(&e.to_string())
                        ),
                        e,
                    )
                })?;

        if self.sync_ack {
            ack_future.await.map_err(|e| {
                BosonError::backend_source(
                    format!(
                        "nats publish ack {subject}: {}",
                        redact_credentials_in_text(&e.to_string())
                    ),
                    e,
                )
            })?;
            drop(permit);
        } else {
            tokio::spawn(async move {
                if let Err(e) = ack_future.await {
                    let detail = redact_credentials_in_text(&e.to_string());
                    let log = ops_log();
                    log.record_counter("boson_nats_publish_ack_failed", &[], 1.0);
                    log.log_event(
                        "boson_nats_publish_ack_failed",
                        &json!({
                            "subject": subject,
                            "message": detail,
                        }),
                    );
                }
                drop(permit);
            });
        }
        Ok(())
    }
}
