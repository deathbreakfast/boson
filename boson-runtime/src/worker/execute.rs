//! Execute a claimed job via the task registry.

use std::sync::Arc;

use boson_core::{BosonError, ExecutionContextFactory, Job, QueueBackend, Result};

use crate::registry::TaskRegistry;

/// Run the registered handler for one claimed job.
pub async fn execute_job(
    registry: &TaskRegistry,
    identity: &Arc<dyn ExecutionContextFactory>,
    job: &Job,
) -> Result<()> {
    let descriptor = registry.get_or_err(&job.task_name)?;
    if job.signature_hash != descriptor.signature_hash {
        return Err(BosonError::SignatureMismatch {
            expected: job.signature_hash.to_string(),
            actual: descriptor.signature_hash.to_string(),
        });
    }
    let ctx = identity
        .build(&job.actor_json)
        .map_err(|e| BosonError::Internal(e.to_string()))?;
    (descriptor.invoke)(ctx, job.params_json.clone()).await
}

/// Persist run start. Job status is already `Running` from [`try_claim_job`](QueueBackend::try_claim_job).
pub async fn record_run_start(
    backend: &Arc<dyn QueueBackend>,
    run: &boson_core::Run,
) -> Result<()> {
    backend.upsert_run(run).await
}
