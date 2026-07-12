use boson_core::{JobStatus, RunStatus};

use crate::scenario::{ScenarioSpec, ScenarioStep};

impl ScenarioSpec {
    /// Enqueue, drain, and assert run lifecycle (BM-B5).
    #[must_use]
    pub fn run_lifecycle(task: &str) -> Self {
        Self {
            id: "run_lifecycle".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Success,
                },
                ScenarioStep::AssertRunOutcome {
                    job_index: 0,
                    run_status: RunStatus::Success,
                },
            ],
        }
    }

    /// Fail task with `max_attempts=1` → terminal failure.
    #[must_use]
    pub fn handler_failure_terminal(task: &str) -> Self {
        Self {
            id: "handler_failure_terminal".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Failed,
                },
                ScenarioStep::AssertRunOutcome {
                    job_index: 0,
                    run_status: RunStatus::Failed,
                },
            ],
        }
    }

    /// Fail N times then succeed via retry policy.
    #[must_use]
    pub fn retry_then_success(task: &str, fail_attempts: u32) -> Self {
        Self {
            id: "retry_then_success".into(),
            steps: vec![ScenarioStep::RetryBackoff {
                task: task.to_string(),
                fail_attempts,
            }],
        }
    }

    /// Cancel a queued job before drain.
    #[must_use]
    pub fn cancel_queued_job(task: &str) -> Self {
        Self {
            id: "cancel_queued_job".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::CancelJob { job_index: 0 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Canceled,
                },
            ],
        }
    }

    /// Cancel an unknown job id — expect `JobNotFound` (no panic).
    #[must_use]
    pub fn cancel_missing_job() -> Self {
        Self {
            id: "cancel_missing_job".into(),
            steps: vec![ScenarioStep::CancelMissingJob],
        }
    }

    /// `get_job` for a missing id returns `None`.
    #[must_use]
    pub fn get_job_not_found() -> Self {
        Self {
            id: "get_job_not_found".into(),
            steps: vec![ScenarioStep::AssertJobMissing {
                job_id: "missing-job-id".into(),
            }],
        }
    }

    /// Always-failing task exhausts retries → terminal `Failed`.
    #[must_use]
    pub fn retry_exhaustion(task: &str) -> Self {
        Self {
            id: "retry_exhaustion".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 64 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Failed,
                },
            ],
        }
    }

    /// Enqueue, restart runtime, drain on same backend.
    #[must_use]
    pub fn restart_runtime_drain(task: &str) -> Self {
        Self {
            id: "restart_runtime_drain".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::RestartRuntime,
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Success,
                },
            ],
        }
    }

    /// List/count admin smoke after enqueue.
    #[must_use]
    pub fn list_and_count_jobs(task: &str) -> Self {
        Self {
            id: "list_and_count_jobs".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::AssertJobCount {
                    count: 1,
                    status: Some(JobStatus::Queued),
                },
            ],
        }
    }

    /// Enqueue `depth` jobs then exercise list/count admin APIs (BM-B12).
    #[must_use]
    pub fn list_and_count_at_depth(task: &str, depth: usize) -> Self {
        Self {
            id: format!("list_and_count_at_depth_{depth}"),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: depth,
                    idempotency_key: None,
                },
                ScenarioStep::AdminListCount {
                    expected_count: depth as u64,
                },
            ],
        }
    }

    /// Enqueue low-priority then high-priority job; drain both (BM-B15).
    #[must_use]
    pub fn pool_priority_drain(low_task: &str, high_task: &str) -> Self {
        Self {
            id: "pool_priority_drain".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: low_task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::EnqueueN {
                    task: high_task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertHandlerHits {
                    task: high_task.to_string(),
                    count: 1,
                },
                ScenarioStep::AssertHandlerHits {
                    task: low_task.to_string(),
                    count: 1,
                },
            ],
        }
    }

    /// Enqueue, lease contention probe, drain.
    #[must_use]
    pub fn lease_contention_drain(task: &str) -> Self {
        Self {
            id: "lease_contention_drain".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::SimulateLeaseContention {
                    workers: 2,
                    ttl_secs: 120,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Success,
                },
            ],
        }
    }

    /// Enqueue, drain, and assert task run stats via admin APIs.
    #[must_use]
    pub fn task_run_stats(task: &str) -> Self {
        Self {
            id: "task_run_stats".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertTaskRunStats {
                    task: task.to_string(),
                    runs_total: 1,
                    success_count: 1,
                },
            ],
        }
    }

    /// Retry with transient failures then assert run attempt count.
    #[must_use]
    pub fn retry_run_count(task: &str, fail_attempts: u32) -> Self {
        Self {
            id: "retry_run_count".into(),
            steps: vec![
                ScenarioStep::RetryBackoff {
                    task: task.to_string(),
                    fail_attempts,
                },
                ScenarioStep::AssertRunCount {
                    job_index: 0,
                    count: usize::try_from(fail_attempts.saturating_add(1)).unwrap_or(usize::MAX),
                },
            ],
        }
    }

    /// Enqueue under signature v1, bump registry hash, expect terminal failure on drain.
    #[must_use]
    pub fn signature_mismatch() -> Self {
        Self {
            id: "signature_mismatch".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: "noop".into(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::ReregisterTaskSignature {
                    task: "noop".into(),
                    signature_hash: 2,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Failed,
                },
            ],
        }
    }
}
