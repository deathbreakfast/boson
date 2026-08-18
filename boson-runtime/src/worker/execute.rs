//! Execute a claimed job via the task registry.

use std::sync::Arc;
use std::time::Duration;

use boson_core::{
    BosonError, ExecutionContext, ExecutionContextFactory, Job, JobStatus, QueueBackend, Result,
};
use serde_json::Value;
use tokio::time::sleep;

use crate::registry::TaskRegistry;

struct DispatchAttemptContext {
    inner: Box<dyn ExecutionContext>,
    attempt: u32,
}

impl ExecutionContext for DispatchAttemptContext {
    fn label(&self) -> &str {
        self.inner.label()
    }

    fn actor_json(&self) -> &Value {
        self.inner.actor_json()
    }

    fn attempt(&self) -> u32 {
        self.attempt
    }
}

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
    let inner = identity
        .build(&job.actor_json)
        .map_err(|e| BosonError::internal_source("execution context build failed", e))?;
    let ctx = Box::new(DispatchAttemptContext {
        inner,
        attempt: job.attempt.cast_unsigned(),
    });
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    struct InnerContext {
        actor_json: Value,
    }

    impl ExecutionContext for InnerContext {
        fn label(&self) -> &'static str {
            "inner"
        }

        fn actor_json(&self) -> &Value {
            &self.actor_json
        }
    }

    #[test]
    fn dispatch_context_forwards_job_attempt() {
        let inner = Box::new(InnerContext {
            actor_json: json!({"System": {"operation": "test"}}),
        });
        let ctx = DispatchAttemptContext { inner, attempt: 2 };
        assert_eq!(ctx.label(), "inner");
        assert_eq!(ctx.attempt(), 2);
        assert_eq!(inner_attempt_default(), 1);
    }

    fn inner_attempt_default() -> u32 {
        InnerContext {
            actor_json: json!({}),
        }
        .attempt()
    }
}
