use anyhow::{anyhow, Result};
use boson_core::JobStatus;

use super::super::state::RunState;

/// Verify lease exclusivity between two workers (`SimulateLeaseContention` step).
pub async fn run_simulate_lease_contention(
    state: &RunState,
    ttl_secs: u32,
) -> Result<Option<String>> {
    let job_id = match state.job_ids.first() {
        Some(id) => id.clone(),
        None => return Ok(Some("SimulateLeaseContention: no enqueued job".into())),
    };
    let backend = state.boson()?.queue_backend();
    let ttl = i64::from(ttl_secs);
    let lease_a = backend
        .try_claim_run_lease(&job_id, "worker-a", ttl)
        .await
        .map_err(|e| anyhow!("lease claim a: {e}"))?;
    if lease_a.is_none() {
        return Ok(Some(
            "SimulateLeaseContention: worker-a could not claim lease".into(),
        ));
    }
    let lease_b = backend
        .try_claim_run_lease(&job_id, "worker-b", ttl)
        .await
        .map_err(|e| anyhow!("lease claim b: {e}"))?;
    if lease_b.is_some() {
        return Ok(Some(
            "SimulateLeaseContention: worker-b claimed lease while worker-a holds it".into(),
        ));
    }
    if let Some(lid) = lease_a {
        let _ = backend.release_lease(&lid).await;
    }
    Ok(None)
}

/// Force a job into `Running` with an already-expired lease (reclaim fixture).
pub async fn run_mark_running_with_expired_lease(
    state: &RunState,
    job_index: usize,
) -> Result<Option<String>> {
    let Some(job_id) = state.job_ids.get(job_index).cloned() else {
        return Ok(Some(format!(
            "MarkRunningWithExpiredLease: missing job_index={job_index}"
        )));
    };
    let backend = state.boson()?.queue_backend();
    let Some(mut job) = backend
        .get_job(&job_id)
        .await
        .map_err(|e| anyhow!("get_job: {e}"))?
    else {
        return Ok(Some(format!(
            "MarkRunningWithExpiredLease: job {job_id} not found"
        )));
    };
    job.status = JobStatus::Running;
    backend
        .upsert_job(&job)
        .await
        .map_err(|e| anyhow!("upsert running: {e}"))?;
    let lease = backend
        .try_claim_run_lease(&job_id, "reclaim-fixture", -1)
        .await
        .map_err(|e| anyhow!("expired lease claim: {e}"))?;
    if lease.is_none() {
        return Ok(Some(
            "MarkRunningWithExpiredLease: could not claim expired lease".into(),
        ));
    }
    Ok(None)
}

/// Apply reaper semantics: release expired leases and revert jobs to queued.
pub async fn run_force_reclaim_expired_leases(state: &RunState) -> Result<Option<String>> {
    let backend = state.boson()?.queue_backend();
    let pairs = backend
        .expired_lease_job_pairs()
        .await
        .map_err(|e| anyhow!("expired_lease_job_pairs: {e}"))?;
    if pairs.is_empty() {
        return Ok(Some(
            "ForceReclaimExpiredLeases: no expired leases to reclaim".into(),
        ));
    }
    for (lease_id, job_id) in pairs {
        let _ = backend.release_lease(&lease_id).await;
        let _ = backend.revert_job_to_queued(&job_id).await;
    }
    Ok(None)
}
