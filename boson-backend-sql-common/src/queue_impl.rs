use async_trait::async_trait;
use boson_core::{
    Job, JobEnqueueDisposition, JobStatus, QueueBackend, Result, Run, RunStatus, TaskConfig,
    TaskRunStats,
};
use chrono::{DateTime, Utc};

use crate::SqlQueueBackend;

#[async_trait]
impl QueueBackend for SqlQueueBackend {
    async fn upsert_job(&self, job: &Job) -> Result<()> {
        self.upsert_job_impl(job).await
    }

    async fn enqueue_with_policies(
        &self,
        job: Job,
        task_config: &TaskConfig,
    ) -> Result<(String, JobEnqueueDisposition)> {
        self.enqueue_with_policies_impl(&self.enqueue_rate, job, task_config)
            .await
    }

    async fn get_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.get_job_impl(job_id).await
    }

    async fn list_jobs(
        &self,
        status_filter: Option<JobStatus>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Job>> {
        self.list_jobs_impl(status_filter, offset, limit).await
    }

    async fn cancel_job_if_active(&self, job_id: &str) -> Result<()> {
        self.cancel_job_if_active_impl(job_id).await
    }

    async fn try_claim_job(&self, job_id: &str) -> Result<Option<Job>> {
        self.try_claim_job_impl(job_id).await
    }

    async fn revert_job_to_queued(&self, job_id: &str) -> Result<()> {
        self.revert_job_to_queued_impl(job_id).await
    }

    async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
        self.distinct_pools_queued_impl().await
    }

    async fn list_queued_for_pool_sorted(&self, pool: &str, limit: usize) -> Result<Vec<Job>> {
        self.list_queued_for_pool_sorted_impl(pool, limit).await
    }

    async fn count_jobs(&self, status_filter: Option<JobStatus>) -> Result<u64> {
        self.count_jobs_impl(status_filter).await
    }

    async fn count_jobs_for_task(&self, task_name: &str, status: Option<JobStatus>) -> Result<u64> {
        self.count_jobs_for_task_impl(task_name, status).await
    }

    async fn count_active_jobs_for_task(&self, task_name: &str) -> Result<u32> {
        self.count_active_jobs_for_task_impl(task_name).await
    }

    async fn find_nonterminal_by_idempotency_key(&self, key: &str) -> Result<Option<String>> {
        self.find_nonterminal_by_idempotency_key_impl(key).await
    }

    async fn upsert_run(&self, run: &Run) -> Result<()> {
        self.upsert_run_impl(run).await
    }

    async fn get_run(&self, run_id: &str) -> Result<Option<Run>> {
        self.get_run_impl(run_id).await
    }

    async fn list_runs(
        &self,
        job_id_filter: Option<&str>,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<Run>> {
        self.list_runs_impl(job_id_filter, offset, limit).await
    }

    async fn finish_run(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        self.finish_run_impl(run_id, status, duration_ms, error_message)
            .await
    }

    async fn count_runs(&self, job_id_filter: Option<&str>) -> Result<u64> {
        self.count_runs_impl(job_id_filter).await
    }

    async fn count_runs_since(&self, since: DateTime<Utc>) -> Result<u64> {
        self.count_runs_since_impl(since).await
    }

    async fn task_run_stats(&self, task_name: &str) -> Result<TaskRunStats> {
        self.task_run_stats_impl(task_name).await
    }

    async fn get_task_config(&self, task_name: &str) -> Result<Option<TaskConfig>> {
        self.get_task_config_impl(task_name).await
    }

    async fn upsert_task_config(&self, config: &TaskConfig) -> Result<()> {
        self.upsert_task_config_impl(config).await
    }

    async fn try_claim_run_lease(
        &self,
        job_id: &str,
        worker_id: &str,
        ttl_secs: i64,
    ) -> Result<Option<String>> {
        self.try_claim_run_lease_impl(job_id, worker_id, ttl_secs)
            .await
    }

    async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> Result<()> {
        self.extend_lease_impl(lease_id, ttl_secs).await
    }

    async fn release_lease(&self, lease_id: &str) -> Result<()> {
        self.release_lease_impl(lease_id).await
    }

    async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
        self.expired_lease_job_pairs_impl().await
    }
}
