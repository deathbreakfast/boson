//! Job persistence helpers for the in-memory backend.

use boson_core::{
    BosonError, IdempotencyMode, Job, JobEnqueueDisposition, JobStatus, Result, TaskConfig,
};

use crate::enqueue_rate::EnqueueRateLimiter;
use crate::store::Inner;

/// Persist or replace a job row.
pub fn upsert_job(inner: &mut Inner, job: &Job) {
    inner.jobs.insert(job.job_id.clone(), job.clone());
}

/// Find non-terminal job id by idempotency key.
pub fn find_nonterminal_by_idempotency_key(inner: &Inner, key: &str) -> Option<String> {
    if key.is_empty() {
        return None;
    }
    for job in inner.jobs.values() {
        if job.idempotency_key.as_deref() == Some(key)
            && matches!(job.status, JobStatus::Queued | JobStatus::Running)
        {
            return Some(job.job_id.clone());
        }
    }
    None
}

/// Count active (`queued` + `running`) jobs for one task.
pub fn count_active_jobs_for_task(inner: &Inner, task_name: &str) -> u32 {
    let count = inner
        .jobs
        .values()
        .filter(|j| {
            j.task_name == task_name
                && matches!(j.status, JobStatus::Queued | JobStatus::Running)
        })
        .count();
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// Enforce policies and insert a job.
pub fn enqueue_with_policies(
    inner: &mut Inner,
    rate_limiter: &EnqueueRateLimiter,
    job: &Job,
    task_config: &TaskConfig,
) -> Result<(String, JobEnqueueDisposition)> {
    let idempotency = task_config.resolved_idempotency_mode(IdempotencyMode::Lwt);
    let mut job = job.clone();
    if idempotency == IdempotencyMode::Lwt {
        if let Some(ref key) = job.idempotency_key {
            if !key.is_empty() {
                if let Some(existing) = find_nonterminal_by_idempotency_key(inner, key) {
                    return Ok((existing, JobEnqueueDisposition::ReusedIdempotent));
                }
            }
        }
    } else {
        job.idempotency_key = None;
    }

    let policy = &task_config.rate_limit_policy;
    if policy.max_in_flight > 0 {
        let count = count_active_jobs_for_task(inner, &job.task_name);
        if count >= policy.max_in_flight {
            return Err(BosonError::RateLimited(job.task_name.clone()));
        }
    }

    if policy.max_enqueue_per_second > 0
        && !rate_limiter.try_record(&job.task_name, policy.max_enqueue_per_second)
    {
        return Err(BosonError::RateLimited(job.task_name.clone()));
    }

    let job_id = job.job_id.clone();
    upsert_job(inner, &job);
    Ok((job_id, JobEnqueueDisposition::InsertedNew))
}

/// Load one job.
pub fn get_job(inner: &Inner, job_id: &str) -> Option<Job> {
    inner.jobs.get(job_id).cloned()
}

/// List jobs with optional status filter and pagination.
pub fn list_jobs(
    inner: &Inner,
    status_filter: Option<JobStatus>,
    offset: usize,
    limit: usize,
) -> Vec<Job> {
    let mut jobs: Vec<Job> = inner
        .jobs
        .values()
        .filter(|j| status_filter.is_none_or(|s| j.status == s))
        .cloned()
        .collect();
    jobs.sort_by_key(|j| j.created_at);
    jobs.into_iter().skip(offset).take(limit).collect()
}

/// Cancel a job if still active.
pub fn cancel_job_if_active(inner: &mut Inner, job_id: &str) -> Result<()> {
    let Some(job) = inner.jobs.get_mut(job_id) else {
        return Err(BosonError::JobNotFound(job_id.to_string()));
    };
    if matches!(job.status, JobStatus::Queued | JobStatus::Running) {
        job.status = JobStatus::Canceled;
    }
    Ok(())
}

/// Atomically claim a queued job.
pub fn try_claim_job(inner: &mut Inner, job_id: &str) -> Option<Job> {
    let job = inner.jobs.get_mut(job_id)?;
    if job.status != JobStatus::Queued {
        return None;
    }
    job.status = JobStatus::Running;
    Some(job.clone())
}

/// Revert a running job to queued.
pub fn revert_job_to_queued(inner: &mut Inner, job_id: &str) {
    let Some(job) = inner.jobs.get_mut(job_id) else {
        return;
    };
    if job.status == JobStatus::Running {
        job.status = JobStatus::Queued;
    }
}

/// Distinct pool names among queued jobs.
pub fn distinct_pools_queued(inner: &Inner) -> Vec<String> {
    let mut pools: Vec<String> = inner
        .jobs
        .values()
        .filter(|j| j.status == JobStatus::Queued)
        .map(|j| j.pool.clone())
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();
    pools.sort();
    pools
}

/// Queued jobs for one pool sorted by priority then created time.
pub fn list_queued_for_pool_sorted(inner: &Inner, pool: &str, limit: usize) -> Vec<Job> {
    let mut jobs: Vec<Job> = inner
        .jobs
        .values()
        .filter(|j| j.status == JobStatus::Queued && j.pool == pool)
        .cloned()
        .collect();
    jobs.sort_by(|a, b| a.priority.cmp(&b.priority).then_with(|| a.created_at.cmp(&b.created_at)));
    jobs.truncate(limit);
    jobs
}

/// Atomically claim the next queued job from a pool.
pub fn pop_claim_from_pool(inner: &mut Inner, pool: &str) -> Option<Job> {
    let next = list_queued_for_pool_sorted(inner, pool, 1)
        .into_iter()
        .next()?;
    try_claim_job(inner, &next.job_id)
}

/// Count jobs optionally filtered by status.
pub fn count_jobs(inner: &Inner, status_filter: Option<JobStatus>) -> u64 {
    let count = inner
        .jobs
        .values()
        .filter(|j| status_filter.is_none_or(|s| j.status == s))
        .count();
    u64::try_from(count).unwrap_or(u64::MAX)
}

/// Count jobs for one task optionally filtered by status.
pub fn count_jobs_for_task(
    inner: &Inner,
    task_name: &str,
    status: Option<JobStatus>,
) -> u64 {
    let count = inner
        .jobs
        .values()
        .filter(|j| {
            j.task_name == task_name && status.is_none_or(|s| j.status == s)
        })
        .count();
    u64::try_from(count).unwrap_or(u64::MAX)
}
