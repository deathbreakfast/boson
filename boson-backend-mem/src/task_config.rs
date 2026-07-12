//! Task config persistence helpers for the in-memory backend.

use boson_core::TaskConfig;

use crate::store::Inner;

/// Load task config by name.
pub fn get_task_config(inner: &Inner, task_name: &str) -> Option<TaskConfig> {
    inner.task_configs.get(task_name).cloned()
}

/// Persist task config.
pub fn upsert_task_config(inner: &mut Inner, config: &TaskConfig) {
    inner
        .task_configs
        .insert(config.task_name.clone(), config.clone());
}
