//! Multi-broker `NATS` fleet: route logical pools to standalone `NATS` nodes.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_trait::async_trait;
use boson_core::{
    BosonError, Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run, RunStatus,
    TaskConfig, TaskRunStats,
};
use chrono::{DateTime, Utc};

use crate::config::NatsEnqueueConfig;
use crate::keys::Keyspace;
use crate::workqueue::NatsWorkQueueBackend;

/// Routes `pool_i` → `NATS` broker `urls[i % N]` (standalone nodes, not RAFT).
pub struct PoolRoutedBackend {
    backends: Vec<Arc<NatsWorkQueueBackend>>,
    pool_index: HashMap<String, usize>,
}

impl std::fmt::Debug for PoolRoutedBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolRoutedBackend")
            .field("brokers", &self.backends.len())
            .field("pool_routes", &self.pool_index.len())
            .finish()
    }
}

impl PoolRoutedBackend {
    fn backend_for_pool(&self, pool: &str) -> Result<&Arc<NatsWorkQueueBackend>> {
        if let Some(&idx) = self.pool_index.get(pool) {
            return self.backends.get(idx).ok_or_else(|| {
                BosonError::Backend(format!("no backend for pool {pool}"))
            });
        }
        let idx = pool_slot_index(pool, self.backends.len());
        self.backends.get(idx).ok_or_else(|| {
            BosonError::Backend(format!("no backend index {idx} for pool {pool}"))
        })
    }

    async fn find_job_backend(&self, job_id: &str) -> Result<Option<(&Arc<NatsWorkQueueBackend>, Job)>> {
        for backend in &self.backends {
            if let Some(job) = backend.get_job(job_id).await? {
                return Ok(Some((backend, job)));
            }
        }
        Ok(None)
    }

    async fn merge_distinct_pools(&self) -> Result<Vec<String>> {
        let mut pools = HashSet::new();
        for backend in &self.backends {
            for pool in backend.distinct_pools_queued().await? {
                pools.insert(pool);
            }
        }
        let mut out: Vec<String> = pools.into_iter().collect();
        out.sort();
        Ok(out)
    }
}

/// Parse `pool_0`, `pool_1`, … for bench distinct layout; default 0.
fn pool_slot_index(pool: &str, n: usize) -> usize {
    if n == 0 {
        return 0;
    }
    if let Some(rest) = pool.strip_prefix("pool_") {
        if let Ok(i) = rest.parse::<usize>() {
            return i % n;
        }
    }
    0
}

/// Parse fleet URLs from `BOSON_NATS_URLS` or `BOSON_NATS_POOL_ROUTING`.
pub fn fleet_urls_from_env() -> Result<Vec<String>> {
    if let Ok(routing) = std::env::var("BOSON_NATS_POOL_ROUTING") {
        if !routing.trim().is_empty() {
            let mut urls = Vec::new();
            for part in routing.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                let (_, url) = part.split_once('=').ok_or_else(|| {
                    BosonError::Backend(format!("invalid BOSON_NATS_POOL_ROUTING segment: {part}"))
                })?;
                urls.push(url.trim().to_string());
            }
            if !urls.is_empty() {
                return Ok(urls);
            }
        }
    }
    if let Ok(urls) = std::env::var("BOSON_NATS_URLS") {
        let list: Vec<String> = urls
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if !list.is_empty() {
            return Ok(list);
        }
    }
    Err(BosonError::Backend(
        "BOSON_NATS_URLS or BOSON_NATS_POOL_ROUTING required for fleet".into(),
    ))
}

/// Connect one `WorkQueue` backend per fleet URL.
///
/// # Errors
///
/// Returns an error when fleet routing is invalid or a backend cannot connect.
pub async fn connect_fleet_from_env() -> Result<Arc<dyn QueueBackend>> {
    let urls = fleet_urls_from_env()?;
    let keyspace = Keyspace::from_env();
    let config = NatsEnqueueConfig::from_env();

    let mut pool_index = HashMap::new();
    if let Ok(routing) = std::env::var("BOSON_NATS_POOL_ROUTING") {
        for part in routing.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }
            if let Some((pool, url)) = part.split_once('=') {
                let pool = pool.trim().to_string();
                let url = url.trim();
                if let Some(idx) = urls.iter().position(|u| u == url) {
                    pool_index.insert(pool, idx);
                }
            }
        }
    }

    let mut backends = Vec::with_capacity(urls.len());
    for url in &urls {
        backends.push(Arc::new(
            NatsWorkQueueBackend::connect_with_config(url, keyspace.clone(), config).await?,
        ));
    }

    if backends.len() == 1 {
        let Some(backend) = backends.pop() else {
            return Err(BosonError::Backend(
                "fleet backend unexpectedly missing".into(),
            ));
        };
        return Ok(backend as Arc<dyn QueueBackend>);
    }

    Ok(Arc::new(PoolRoutedBackend {
        backends,
        pool_index,
    }))
}

#[async_trait]
impl QueueBackend for PoolRoutedBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        self.backend_for_pool(&job.pool)?.upsert_job(job).await
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        self.backend_for_pool(&job.pool)?
            .enqueue_with_policies(job, task_config)
            .await
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        for backend in &self.backends {
            if let Some(job) = backend.get_job(job_id).await? {
                return Ok(Some(job));
            }
        }
        Ok(None)
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        let mut jobs = Vec::new();
        for backend in &self.backends {
            jobs.extend(backend.list_jobs(status_filter, 0, usize::MAX).await?);
        }
        jobs.sort_by_key(|j| j.created_at);
        Ok(jobs.into_iter().skip(offset).take(limit).collect())
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        if let Some((backend, _)) = self.find_job_backend(job_id).await? {
            backend.cancel_job_if_active(job_id).await
        } else {
            Err(BosonError::JobNotFound(job_id.to_string()))
        }
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        if let Some((backend, _)) = self.find_job_backend(job_id).await? {
            backend.try_claim_job(job_id).await
        } else {
            Ok(None)
        }
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        if let Some((backend, _)) = self.find_job_backend(job_id).await? {
            backend.revert_job_to_queued(job_id).await
        } else {
            Ok(())
        }
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        self.merge_distinct_pools().await
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        self.backend_for_pool(pool)?
            .list_queued_for_pool_sorted(pool, limit)
            .await
    }

    async fn pop_claim_from_pool(&self, pool: &str) -> Result<Option<Job>> {
        self.backend_for_pool(pool)?.pop_claim_from_pool(pool).await
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        let mut total = 0u64;
        for backend in &self.backends {
            total = total.saturating_add(backend.count_jobs(status_filter).await?);
        }
        Ok(total)
    }

    async fn count_jobs_for_task(
        &self,
        task_name: &str,
        status: Option<JobStatus>,
    ) -> Result<u64> {
        let mut total = 0u64;
        for backend in &self.backends {
            total = total.saturating_add(backend.count_jobs_for_task(task_name, status).await?);
        }
        Ok(total)
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        let mut total = 0u32;
        for backend in &self.backends {
            total = total.saturating_add(backend.count_active_jobs_for_task(task_name).await?);
        }
        Ok(total)
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        for backend in &self.backends {
            if let Some(id) = backend.find_nonterminal_by_idempotency_key(key).await? {
                return Ok(Some(id));
            }
        }
        Ok(None)
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        if let Some((backend, job)) = self.find_job_backend(&run.job_id).await? {
            backend.upsert_run(run).await?;
            let _ = job;
            Ok(())
        } else {
            self.backends[0].upsert_run(run).await
        }
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        for backend in &self.backends {
            if let Some(run) = backend.get_run(run_id).await? {
                return Ok(Some(run));
            }
        }
        Ok(None)
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        let mut runs = Vec::new();
        for backend in &self.backends {
            runs.extend(backend.list_runs(job_id_filter, 0, usize::MAX).await?);
        }
        runs.sort_by_key(|r| r.started_at);
        Ok(runs.into_iter().skip(offset).take(limit).collect())
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        for backend in &self.backends {
            if backend.get_run(run_id).await?.is_some() {
                return backend
                    .finish_run(run_id, status, duration_ms, error_message)
                    .await;
            }
        }
        Ok(())
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        let mut total = 0u64;
        for backend in &self.backends {
            total = total.saturating_add(backend.count_runs(job_id_filter).await?);
        }
        Ok(total)
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        let mut total = 0u64;
        for backend in &self.backends {
            total = total.saturating_add(backend.count_runs_since(since).await?);
        }
        Ok(total)
    }

    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        let mut total = TaskRunStats {
            runs_total: 0,
            success_count: 0,
        };
        for backend in &self.backends {
            let s = backend.task_run_stats(task_name).await?;
            total.runs_total = total.runs_total.saturating_add(s.runs_total);
            total.success_count = total.success_count.saturating_add(s.success_count);
        }
        Ok(total)
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        for backend in &self.backends {
            if let Some(cfg) = backend.get_task_config(task_name).await? {
                return Ok(Some(cfg));
            }
        }
        Ok(None)
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        for backend in &self.backends {
            backend.upsert_task_config(config).await?;
        }
        Ok(())
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        if let Some((backend, _)) = self.find_job_backend(job_id).await? {
            backend.try_claim_run_lease(job_id, worker_id, ttl_secs).await
        } else {
            self.backends[0]
                .try_claim_run_lease(job_id, worker_id, ttl_secs)
                .await
        }
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        for backend in &self.backends {
            if backend.extend_lease(lease_id, ttl_secs).await.is_ok() {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        for backend in &self.backends {
            backend.release_lease(lease_id).await?;
        }
        Ok(())
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        let mut out = Vec::new();
        for backend in &self.backends {
            out.extend(backend.expired_lease_job_pairs().await?);
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pool_slot_index_maps_distinct_pools() {
        assert_eq!(pool_slot_index("pool_0", 4), 0);
        assert_eq!(pool_slot_index("pool_3", 4), 3);
        assert_eq!(pool_slot_index("pool_5", 4), 1);
        assert_eq!(pool_slot_index("global", 4), 0);
    }
}
