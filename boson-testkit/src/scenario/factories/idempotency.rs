use boson_core::JobStatus;

use crate::scenario::{ScenarioSpec, ScenarioStep};

impl ScenarioSpec {
    /// Enqueue twice with the same idempotency key (BM-B8 partial).
    #[must_use]
    pub fn idempotency_smoke(task: &str) -> Self {
        Self {
            id: "idempotency_smoke".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem".into()),
                },
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem".into()),
                },
                ScenarioStep::AssertSameJobId {
                    first_index: 0,
                    second_index: 1,
                },
            ],
        }
    }

    /// Second enqueue with same key after terminal success creates a new job.
    #[must_use]
    pub fn idempotency_after_terminal(task: &str) -> Self {
        Self {
            id: "idempotency_after_terminal".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-terminal".into()),
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Success,
                },
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-terminal".into()),
                },
                ScenarioStep::AssertDifferentJobId {
                    first_index: 0,
                    second_index: 1,
                },
            ],
        }
    }

    /// Idempotency reuse while prior job still queued, then drain.
    #[must_use]
    pub fn idempotency_reuse_while_queued(task: &str) -> Self {
        Self {
            id: "idempotency_reuse_while_queued".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-queued".into()),
                },
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-queued".into()),
                },
                ScenarioStep::AssertSameJobId {
                    first_index: 0,
                    second_index: 1,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: JobStatus::Success,
                },
            ],
        }
    }

    /// With [`IdempotencyMode::None`](boson_core::IdempotencyMode::None), same key inserts two jobs.
    ///
    /// Requires the session builder default `idempotency_mode` to be `None`.
    #[must_use]
    pub fn idempotency_none_allows_dup(task: &str) -> Self {
        Self {
            id: "idempotency_none_allows_dup".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-none".into()),
                },
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: Some("testkit-idem-none".into()),
                },
                ScenarioStep::AssertDifferentJobId {
                    first_index: 0,
                    second_index: 1,
                },
            ],
        }
    }
}
