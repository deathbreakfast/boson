//! Generates a [`QueueBackend`](boson_core::QueueBackend) impl that forwards to an inner backend field.

/// Delegate every [`QueueBackend`](boson_core::QueueBackend) method to `$inner`.
#[macro_export]
macro_rules! delegate_queue_backend {
    ($wrapper:ty, $inner:ident) => {
        #[::async_trait::async_trait]
        impl ::boson_core::QueueBackend for $wrapper {
            async fn upsert_job(&self, job: &::boson_core::Job) -> ::boson_core::Result<()> {
                self.$inner.upsert_job(job).await
            }

            async fn enqueue_with_policies(
                &self,
                job: ::boson_core::Job,
                task_config: &::boson_core::TaskConfig,
            ) -> ::boson_core::Result<(String, ::boson_core::JobEnqueueDisposition)> {
                self.$inner.enqueue_with_policies(job, task_config).await
            }

            async fn get_job(&self, job_id: &str) -> ::boson_core::Result<Option<::boson_core::Job>> {
                self.$inner.get_job(job_id).await
            }

            async fn list_jobs(
                &self,
                status_filter: Option<::boson_core::JobStatus>,
                offset: usize,
                limit: usize,
            ) -> ::boson_core::Result<Vec<::boson_core::Job>> {
                self.$inner.list_jobs(status_filter, offset, limit).await
            }

            async fn cancel_job_if_active(&self, job_id: &str) -> ::boson_core::Result<()> {
                self.$inner.cancel_job_if_active(job_id).await
            }

            async fn try_claim_job(
                &self,
                job_id: &str,
            ) -> ::boson_core::Result<Option<::boson_core::Job>> {
                self.$inner.try_claim_job(job_id).await
            }

            async fn revert_job_to_queued(&self, job_id: &str) -> ::boson_core::Result<()> {
                self.$inner.revert_job_to_queued(job_id).await
            }

            async fn distinct_pools_queued(&self) -> ::boson_core::Result<Vec<String>> {
                self.$inner.distinct_pools_queued().await
            }

            async fn list_queued_for_pool_sorted(
                &self,
                pool: &str,
                limit: usize,
            ) -> ::boson_core::Result<Vec<::boson_core::Job>> {
                self.$inner.list_queued_for_pool_sorted(pool, limit).await
            }

            async fn count_jobs(
                &self,
                status_filter: Option<::boson_core::JobStatus>,
            ) -> ::boson_core::Result<u64> {
                self.$inner.count_jobs(status_filter).await
            }

            async fn count_jobs_for_task(
                &self,
                task_name: &str,
                status: Option<::boson_core::JobStatus>,
            ) -> ::boson_core::Result<u64> {
                self.$inner.count_jobs_for_task(task_name, status).await
            }

            async fn count_active_jobs_for_task(
                &self,
                task_name: &str,
            ) -> ::boson_core::Result<u32> {
                self.$inner.count_active_jobs_for_task(task_name).await
            }

            async fn find_nonterminal_by_idempotency_key(
                &self,
                key: &str,
            ) -> ::boson_core::Result<Option<String>> {
                self.$inner.find_nonterminal_by_idempotency_key(key).await
            }

            async fn upsert_run(&self, run: &::boson_core::Run) -> ::boson_core::Result<()> {
                self.$inner.upsert_run(run).await
            }

            async fn get_run(&self, run_id: &str) -> ::boson_core::Result<Option<::boson_core::Run>> {
                self.$inner.get_run(run_id).await
            }

            async fn list_runs(
                &self,
                job_id_filter: Option<&str>,
                offset: usize,
                limit: usize,
            ) -> ::boson_core::Result<Vec<::boson_core::Run>> {
                self.$inner.list_runs(job_id_filter, offset, limit).await
            }

            async fn finish_run(
                &self,
                run_id: &str,
                status: ::boson_core::RunStatus,
                duration_ms: Option<i64>,
                error_message: Option<String>,
            ) -> ::boson_core::Result<()> {
                self.$inner
                    .finish_run(run_id, status, duration_ms, error_message)
                    .await
            }

            async fn count_runs(&self, job_id_filter: Option<&str>) -> ::boson_core::Result<u64> {
                self.$inner.count_runs(job_id_filter).await
            }

            async fn count_runs_since(
                &self,
                since: ::chrono::DateTime<::chrono::Utc>,
            ) -> ::boson_core::Result<u64> {
                self.$inner.count_runs_since(since).await
            }

            async fn task_run_stats(
                &self,
                task_name: &str,
            ) -> ::boson_core::Result<::boson_core::TaskRunStats> {
                self.$inner.task_run_stats(task_name).await
            }

            async fn get_task_config(
                &self,
                task_name: &str,
            ) -> ::boson_core::Result<Option<::boson_core::TaskConfig>> {
                self.$inner.get_task_config(task_name).await
            }

            async fn upsert_task_config(
                &self,
                config: &::boson_core::TaskConfig,
            ) -> ::boson_core::Result<()> {
                self.$inner.upsert_task_config(config).await
            }

            async fn try_claim_run_lease(
                &self,
                job_id: &str,
                worker_id: &str,
                ttl_secs: i64,
            ) -> ::boson_core::Result<Option<String>> {
                self.$inner
                    .try_claim_run_lease(job_id, worker_id, ttl_secs)
                    .await
            }

            async fn extend_lease(&self, lease_id: &str, ttl_secs: i64) -> ::boson_core::Result<()> {
                self.$inner.extend_lease(lease_id, ttl_secs).await
            }

            async fn release_lease(&self, lease_id: &str) -> ::boson_core::Result<()> {
                self.$inner.release_lease(lease_id).await
            }

            async fn expired_lease_job_pairs(
                &self,
            ) -> ::boson_core::Result<Vec<(String, String)>> {
                self.$inner.expired_lease_job_pairs().await
            }
        }
    };
}
