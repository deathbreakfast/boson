//! Background worker loop and lifecycle host implementation.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use boson_core::{
    ExecutionContextFactory, Job, JobStatus, QueueBackend, Result, Run, RunStatus, TaskConfig,
};
use chrono::Utc;
use tokio::time::sleep;

use super::claim::claim_next_job;
use super::config::WorkerSettings;
use super::execute::{execute_job, record_run_start};
use super::lifecycle::{finish_job_execution, sleep_retry_delay, RunLifecycleHost};
use crate::registry::TaskRegistry;
use crate::telemetry;

/// Handle to a spawned worker task (optional join).
#[derive(Debug)]
pub struct WorkerHandle {
    _label: String,
}

pub struct WorkerEngine {
    pub(crate) backend: Arc<dyn QueueBackend>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) identity: Arc<dyn ExecutionContextFactory>,
    pub(crate) worker: WorkerSettings,
}

impl WorkerEngine {
    pub(crate) async fn drive_run(self: &Arc<Self>, job: Job, lease_id: Option<String>) {
        if self.worker.skip_run_persistence && lease_id.is_none() {
            self.drive_run_without_run_rows(job).await;
            return;
        }
        let run = Run::new(&job.job_id, &job.task_name, job.attempt);
        let run_id = run.run_id.clone();
        if record_run_start(&self.backend, &run).await.is_err() {
            telemetry::record_handler_error(
                &job.task_name,
                &job.job_id,
                &run_id,
                "failed to persist run start",
            );
            if let Some(ref lid) = lease_id {
                let _ = self.backend.release_lease(lid).await;
            }
            let _ = self.backend.revert_job_to_queued(&job.job_id).await;
            return;
        }
        telemetry::record_task_started(
            &job.task_name,
            &job.job_id,
            &run_id,
            &self.worker.runtime_label,
        );
        let start = Utc::now();
        let result = execute_job(&self.registry, &self.identity, &job).await;
        if let Err(ref e) = result {
            telemetry::record_handler_error(
                &job.task_name,
                &job.job_id,
                &run_id,
                &e.to_string(),
            );
        }
        let duration_ms = (Utc::now() - start).num_milliseconds();
        finish_job_execution(self.as_ref(), run_id, job, result, duration_ms).await;
        if let Some(ref lid) = lease_id {
            let _ = self.backend.release_lease(lid).await;
        }
    }

    async fn drive_run_without_run_rows(self: &Arc<Self>, job: Job) {
        let run_id = format!("fast-{}", job.job_id);
        telemetry::record_task_started(
            &job.task_name,
            &job.job_id,
            &run_id,
            &self.worker.runtime_label,
        );
        let start = Utc::now();
        let result = execute_job(&self.registry, &self.identity, &job).await;
        let duration_ms = (Utc::now() - start).num_milliseconds();
        match result {
            Ok(()) => {
                telemetry::record_task_completed(&job.task_name, &job.job_id, &run_id, duration_ms);
                let mut finished = job;
                finished.status = JobStatus::Success;
                self.upsert_job(finished).await;
            }
            Err(e) => {
                telemetry::record_handler_error(
                    &job.task_name,
                    &job.job_id,
                    &run_id,
                    &e.to_string(),
                );
                telemetry::record_task_failed(
                    &job.task_name,
                    &job.job_id,
                    &run_id,
                    &e.to_string(),
                    false,
                );
                let _ = self.backend.revert_job_to_queued(&job.job_id).await;
            }
        }
    }

    async fn upsert_job(&self, job: Job) {
        if let Err(e) = self.backend.upsert_job(&job).await {
            telemetry::log_job_upsert_failed(&job.job_id, &job.task_name, &e.to_string());
        }
    }

    async fn tick(self: &Arc<Self>) {
        let discovered = self
            .backend
            .distinct_pools_queued()
            .await
            .unwrap_or_default();
        let pools = self.worker.pools_to_poll(discovered);
        for pool in pools {
            if let Ok(Some((job, lease_id))) = claim_next_job(
                &self.backend,
                &pool,
                &self.worker.worker_id,
                self.worker.lease_ttl_secs,
            )
            .await
            {
                self.drive_run(job, lease_id).await;
            }
        }
    }

    async fn reap_expired_leases(self: Arc<Self>) {
        if self.worker.lease_ttl_secs <= 0 {
            return;
        }
        loop {
            sleep(Duration::from_secs(15)).await;
            let pairs = self
                .backend
                .expired_lease_job_pairs()
                .await
                .unwrap_or_default();
            let count = pairs.len();
            for (lease_id, job_id) in pairs {
                let _ = self.backend.release_lease(&lease_id).await;
                let _ = self.backend.revert_job_to_queued(&job_id).await;
            }
            telemetry::log_lease_reclaim(count, &self.worker.runtime_label);
        }
    }
}

#[async_trait]
impl RunLifecycleHost for WorkerEngine {
    async fn record_run_finish(
        &self,
        run_id: &str,
        status: RunStatus,
        duration_ms: Option<i64>,
        error_message: Option<String>,
    ) -> Result<()> {
        self.backend
            .finish_run(run_id, status, duration_ms, error_message)
            .await
    }

    async fn put_job(&self, job: Job) {
        self.upsert_job(job).await;
    }

    async fn load_task_config(&self, task_name: &str) -> Result<TaskConfig> {
        if let Some(c) = self.backend.get_task_config(task_name).await? {
            return Ok(c);
        }
        Ok(self.registry.get_or_err(task_name)?.to_task_config())
    }

    async fn schedule_retry(&self, mut job: Job, delay_ms: u64) {
        sleep_retry_delay(delay_ms).await;
        job.attempt += 1;
        job.status = JobStatus::Queued;
        self.upsert_job(job).await;
    }
}

/// Spawn background worker loop.
pub fn spawn_worker(
    backend: Arc<dyn QueueBackend>,
    registry: Arc<TaskRegistry>,
    identity: Arc<dyn ExecutionContextFactory>,
    worker: WorkerSettings,
) -> WorkerHandle {
    let label = worker.runtime_label.clone();
    let engine = Arc::new(WorkerEngine {
        backend,
        registry,
        identity,
        worker,
    });
    if engine.worker.lease_ttl_secs > 0 {
        let reaper = Arc::clone(&engine);
        tokio::spawn(async move {
            reaper.reap_expired_leases().await;
        });
    }
    tokio::spawn(async move {
        let poll_ms = engine.worker.worker_poll_interval_ms;
        loop {
            engine.tick().await;
            if poll_ms > 0 {
                sleep(Duration::from_millis(poll_ms)).await;
            } else {
                tokio::task::yield_now().await;
            }
        }
    });
    WorkerHandle { _label: label }
}
