use anyhow::{anyhow, Result};

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
