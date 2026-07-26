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
        let heartbeat = self.spawn_lease_heartbeat(lease_id.as_deref());
        let start = Utc::now();
        let result = execute_job(&self.registry, &self.identity, &self.backend, &job).await;
        if let Some(handle) = heartbeat {
            handle.abort();
        }
        let result = self.apply_cancel_if_needed(&job.job_id, result).await;
        if let Err(ref e) = result {
            let msg = boson_core::sanitize_error_message(&e.to_string());
            telemetry::record_handler_error(&job.task_name, &job.job_id, &run_id, &msg);
        }
        let duration_ms = (Utc::now() - start).num_milliseconds();
        finish_job_execution(self.as_ref(), run_id, job, result, duration_ms).await;
        if let Some(ref lid) = lease_id {
            let _ = self.backend.release_lease(lid).await;
        }
    }

    fn spawn_lease_heartbeat(&self, lease_id: Option<&str>) -> Option<tokio::task::JoinHandle<()>> {
        let lease_id = lease_id?.to_string();
        let ttl = self.worker.lease_ttl_secs;
        if ttl <= 0 {
            return None;
        }
        let backend = Arc::clone(&self.backend);
        let interval_secs = u64::try_from((ttl / 3).max(1)).unwrap_or(1);
        Some(tokio::spawn(async move {
            loop {
                sleep(Duration::from_secs(interval_secs)).await;
                if backend.extend_lease(&lease_id, ttl).await.is_err() {
                    break;
                }
            }
        }))
    }

    /// If the job was canceled while running, convert Ok into a cancel error so finish
    /// does not overwrite status to Success.
    async fn apply_cancel_if_needed(&self, job_id: &str, result: Result<()>) -> Result<()> {
        result?;
        match self.backend.get_job(job_id).await {
            Ok(Some(j)) if j.status == JobStatus::Canceled => Err(
                boson_core::BosonError::internal("job canceled during execution"),
            ),
            _ => Ok(()),
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
        let result = execute_job(&self.registry, &self.identity, &self.backend, &job).await;
        let result = self.apply_cancel_if_needed(&job.job_id, result).await;
        let duration_ms = (Utc::now() - start).num_milliseconds();
        match result {
            Ok(()) => {
                telemetry::record_task_completed(&job.task_name, &job.job_id, &run_id, duration_ms);
                let mut finished = job;
                finished.status = JobStatus::Success;
                self.upsert_job(finished).await;
            }
            Err(e) => {
                let msg = boson_core::sanitize_error_message(&e.to_string());
                telemetry::record_handler_error(&job.task_name, &job.job_id, &run_id, &msg);
                if msg.contains("job canceled") {
                    let mut canceled = job;
                    canceled.status = JobStatus::Canceled;
                    self.upsert_job(canceled).await;
                    return;
                }
                telemetry::record_task_failed(&job.task_name, &job.job_id, &run_id, &msg, false);
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
