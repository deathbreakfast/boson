//! Pipelined `JetStream` publish with optional ack wait.

use std::sync::Arc;

use async_nats::jetstream::context::PublishAckFuture;
use boson_core::{BosonError, Result};
use bytes::Bytes;
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
        let permit = self
            .semaphore
            .clone()
            .acquire_owned()
            .await
            .map_err(|e| BosonError::Backend(format!("nats publish pipeline: {e}")))?;

        let ack_future: PublishAckFuture = jetstream
            .publish(subject.clone(), body)
            .await
            .map_err(|e| BosonError::Backend(format!("nats publish {subject}: {e}")))?;

        if self.sync_ack {
            ack_future
                .await
                .map_err(|e| BosonError::Backend(format!("nats publish ack {subject}: {e}")))?;
            drop(permit);
        } else {
            tokio::spawn(async move {
                let _ = ack_future.await;
                drop(permit);
            });
        }
        Ok(())
    }
}
