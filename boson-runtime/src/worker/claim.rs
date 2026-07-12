//! Claim the next runnable job from a pool.

use std::sync::Arc;

use boson_core::{Job, QueueBackend, Result};

/// Claim one job from a pool, optionally acquiring a run lease first when `lease_ttl_secs > 0`.
pub async fn claim_next_job(
    backend: &Arc<dyn QueueBackend>,
    pool: &str,
    worker_id: &str,
    lease_ttl_secs: i64,
) -> Result<Option<(Job, Option<String>)>> {
    if lease_ttl_secs == 0 {
        if let Ok(Some(job)) = backend.pop_claim_from_pool(pool).await {
            return Ok(Some((job, None)));
        }
    }
    let candidates = backend.list_queued_for_pool_sorted(pool, 8).await?;
    for job in candidates {
        let job_id = job.job_id.clone();
        let lease_id = if lease_ttl_secs > 0 {
            backend
                .try_claim_run_lease(&job_id, worker_id, lease_ttl_secs)
                .await?
        } else {
            None
        };
        if lease_ttl_secs > 0 && lease_id.is_none() {
            continue;
        }
        match backend.try_claim_job(&job_id).await? {
            Some(claimed) => return Ok(Some((claimed, lease_id))),
            None => {
                if let Some(ref lid) = lease_id {
                    let _ = backend.release_lease(lid).await;
                }
            }
        }
    }
    Ok(None)
}
