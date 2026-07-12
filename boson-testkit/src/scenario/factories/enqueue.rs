use crate::scenario::{ScenarioSpec, ScenarioStep};

impl ScenarioSpec {
    /// Enqueue one job and drain until success.
    #[must_use]
    pub fn enqueue_and_drain(task: &str) -> Self {
        Self {
            id: format!("enqueue_and_drain_{task}"),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle { max_steps: 32 },
                ScenarioStep::AssertJobStatus {
                    job_index: 0,
                    status: boson_core::JobStatus::Success,
                },
                ScenarioStep::AssertHandlerHits {
                    task: task.to_string(),
                    count: 1,
                },
            ],
        }
    }

    /// Enqueue-only workload (BM-B0).
    #[must_use]
    pub fn enqueue_only(task: &str, count: usize) -> Self {
        Self {
            id: "enqueue_only".into(),
            steps: vec![ScenarioStep::EnqueueN {
                task: task.to_string(),
                count,
                idempotency_key: None,
            }],
        }
    }

    /// Enqueue N jobs and drain all to success.
    #[must_use]
    pub fn multi_job_drain(task: &str, count: usize) -> Self {
        Self {
            id: format!("multi_job_drain_{count}"),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count,
                    idempotency_key: None,
                },
                ScenarioStep::DrainUntilIdle {
                    max_steps: count * 8,
                },
                ScenarioStep::AssertHandlerHits {
                    task: task.to_string(),
                    count,
                },
            ],
        }
    }

    /// Enqueue unknown task — expect `TaskNotFound`.
    #[must_use]
    pub fn enqueue_unknown_task() -> Self {
        Self {
            id: "enqueue_unknown_task".into(),
            steps: vec![ScenarioStep::AssertEnqueueError {
                task: "no_such_task".into(),
                error: crate::scenario::EnqueueErrorKind::TaskNotFound,
            }],
        }
    }
}
