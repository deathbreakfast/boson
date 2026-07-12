use boson_core::{JobStatus, RunStatus};
use serde::{Deserialize, Serialize};

/// Expected enqueue error for [`ScenarioStep::AssertEnqueueError`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EnqueueErrorKind {
    /// Task not registered.
    TaskNotFound,
    /// Rate limit or in-flight cap blocked enqueue.
    RateLimited,
}

/// One step in a declarative scenario.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "step", rename_all = "snake_case")]
pub enum ScenarioStep {
    /// Enqueue `count` jobs for `task`.
    EnqueueN {
        /// Registered task name.
        task: String,
        /// Number of jobs.
        count: usize,
        /// Optional shared idempotency key.
        #[serde(default)]
        idempotency_key: Option<String>,
    },
    /// Expect the next enqueue for `task` to fail.
    AssertEnqueueError {
        /// Task name passed to enqueue.
        task: String,
        /// Expected error kind.
        error: EnqueueErrorKind,
    },
    /// Override persisted task config before enqueue.
    UpsertTaskConfig {
        /// Task name.
        task: String,
        /// Max in-flight jobs (`None` = leave unchanged).
        #[serde(default)]
        max_in_flight: Option<u32>,
        /// Max enqueues per second (`None` = leave unchanged).
        #[serde(default)]
        max_enqueue_per_second: Option<u32>,
        /// Retry max attempts (`None` = leave unchanged).
        #[serde(default)]
        max_attempts: Option<u32>,
        /// Retry base delay ms (`None` = leave unchanged).
        #[serde(default)]
        base_delay_ms: Option<u64>,
    },
    /// Cancel a previously enqueued job.
    CancelJob {
        /// Job id index from prior enqueue steps.
        job_index: usize,
    },
    /// Cancel a job id that does not exist — expect [`boson_core::BosonError::JobNotFound`].
    CancelMissingJob,
    /// Assert `get_job` returns `None` for an unknown id.
    AssertJobMissing {
        /// Job id that must not exist.
        job_id: String,
    },
    /// Run manual worker until queue is idle or `max_steps` reached.
    DrainUntilIdle {
        /// Safety cap on worker iterations.
        max_steps: usize,
    },
    /// Assert job status.
    AssertJobStatus {
        /// Job id from a prior enqueue step (index into captured ids).
        job_index: usize,
        /// Expected terminal or active status.
        status: JobStatus,
    },
    /// Assert the latest run for a job reached the expected status.
    AssertRunOutcome {
        /// Job id index from prior enqueue steps.
        job_index: usize,
        /// Expected run terminal status.
        run_status: RunStatus,
    },
    /// Two enqueue steps reused the same job id (idempotency).
    AssertSameJobId {
        /// Index of the first enqueue step.
        first_index: usize,
        /// Index of the second enqueue step.
        second_index: usize,
    },
    /// Two enqueue steps produced different job ids.
    AssertDifferentJobId {
        /// Index of the first enqueue step.
        first_index: usize,
        /// Index of the second enqueue step.
        second_index: usize,
    },
    /// Assert synthetic handler invocation count.
    AssertHandlerHits {
        /// Task name (`noop`, `counting`, etc.).
        task: String,
        /// Expected hit count.
        count: usize,
    },
    /// Assert job count with optional status filter.
    AssertJobCount {
        /// Expected count.
        count: u64,
        /// Optional status filter.
        #[serde(default)]
        status: Option<JobStatus>,
    },
    /// Assert number of runs for a job.
    AssertRunCount {
        /// Job id index from prior enqueue steps.
        job_index: usize,
        /// Expected run row count.
        count: usize,
    },
    /// Tear down and rebuild Boson on the same backend.
    RestartRuntime,
    /// Lease contention: second worker cannot claim while first holds lease.
    SimulateLeaseContention {
        /// Unused — reserved for multi-worker harness.
        workers: u32,
        /// Lease TTL seconds for contention probe.
        ttl_secs: u32,
    },
    /// Retry/backoff: enqueue, drain until success after transient failures.
    RetryBackoff {
        /// Registered fail-then-ok task name.
        task: String,
        /// Fail attempts before success.
        fail_attempts: u32,
    },
    /// Remote HTTP enqueue (requires host coordinator wiring).
    RemoteEnqueue {
        /// When true, enqueue via HTTP instead of in-process API.
        via_http: bool,
    },
    /// List jobs and count at depth (benchmark admin read path).
    AdminListCount {
        /// Expected queued job count.
        expected_count: u64,
    },
    /// Assert aggregate run stats for a task name.
    AssertTaskRunStats {
        /// Registered task name.
        task: String,
        /// Expected total runs.
        runs_total: u32,
        /// Expected successful runs.
        success_count: u32,
    },
    /// Rebuild the worker with an updated task signature hash (same backend).
    ReregisterTaskSignature {
        /// Task to re-register.
        task: String,
        /// New signature hash on the descriptor.
        signature_hash: u64,
    },
}
