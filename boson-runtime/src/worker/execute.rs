//! Execute a claimed job via the task registry.

use std::sync::Arc;
use std::time::Duration;

use boson_core::{BosonError, ExecutionContextFactory, Job, JobStatus, QueueBackend, Result};
use tokio::time::sleep;

use crate::registry::TaskRegistry;

/// Run the registered handler for one claimed job.
///
/// Polls job status while the handler runs; if the job becomes [`JobStatus::Canceled`],
/// the handler future is dropped (cooperative cancel) and this returns a cancel error.
pub async fn execute_job(
    registry: &TaskRegistry,
    identity: &Arc<dyn ExecutionContextFactory>,
    backend: &Arc<dyn QueueBackend>,
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
        .map_err(|e| BosonError::internal_source("execution context build failed", e))?;
    let invoke = (descriptor.invoke)(ctx, job.params_json.clone());
    let job_id = job.job_id.clone();
    let backend = Arc::clone(backend);
    let cancel_watch = async move {
        loop {
            sleep(Duration::from_millis(50)).await;
            match backend.get_job(&job_id).await {
                Ok(Some(j)) if j.status == JobStatus::Canceled => return,
                Ok(None) | Err(_) => return,
                _ => {}
            }
        }
    };
    tokio::select! {
        result = invoke => result,
        () = cancel_watch => Err(BosonError::internal(
            "job canceled during execution",
        )),
    }
}

/// Persist run start. Job status is already `Running` from [`try_claim_job`](QueueBackend::try_claim_job).
pub async fn record_run_start(
    backend: &Arc<dyn QueueBackend>,
    run: &boson_core::Run,
) -> Result<()> {
    backend.upsert_run(run).await
}
