use crate::scenario::{EnqueueErrorKind, ScenarioSpec, ScenarioStep};

impl ScenarioSpec {
    /// Rate limit via `max_in_flight=1` on registered task.
    #[must_use]
    pub fn rate_limit_in_flight(task: &str) -> Self {
        Self {
            id: "rate_limit_in_flight".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::AssertEnqueueError {
                    task: task.to_string(),
                    error: EnqueueErrorKind::RateLimited,
                },
            ],
        }
    }

    /// Rate limit via `max_enqueue_per_second=1`.
    #[must_use]
    pub fn rate_limit_eps(task: &str) -> Self {
        Self {
            id: "rate_limit_eps".into(),
            steps: vec![
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::AssertEnqueueError {
                    task: task.to_string(),
                    error: EnqueueErrorKind::RateLimited,
                },
            ],
        }
    }

    /// Upsert task config then hit rate limit.
    #[must_use]
    pub fn task_config_rate_limit(task: &str) -> Self {
        Self {
            id: "task_config_rate_limit".into(),
            steps: vec![
                ScenarioStep::UpsertTaskConfig {
                    task: task.to_string(),
                    max_in_flight: Some(1),
                    max_enqueue_per_second: None,
                    max_attempts: None,
                    base_delay_ms: None,
                },
                ScenarioStep::EnqueueN {
                    task: task.to_string(),
                    count: 1,
                    idempotency_key: None,
                },
                ScenarioStep::AssertEnqueueError {
                    task: task.to_string(),
                    error: EnqueueErrorKind::RateLimited,
                },
            ],
        }
    }
}
