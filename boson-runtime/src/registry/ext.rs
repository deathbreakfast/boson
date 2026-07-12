//! Extended methods on [`TaskRegistry`](super::TaskRegistry).

use boson_core::{BosonError, Result};

use super::TaskDescriptor;
use super::TaskRegistry;

impl TaskRegistry {
    /// Look up a task by name, returning an error if not found.
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::TaskNotFound`](boson_core::BosonError::TaskNotFound) when the name is absent.
    pub fn get_or_err(&self, name: &str) -> Result<&'static TaskDescriptor> {
        self.get(name)
            .ok_or_else(|| BosonError::TaskNotFound(name.to_string()))
    }

    /// Task names in sorted order.
    #[must_use]
    pub fn sorted_task_names(&self) -> Vec<&str> {
        let mut names = self.list();
        names.sort_unstable();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_registry() {
        let registry = TaskRegistry::new();
        assert!(registry.is_empty());
        assert!(registry.get("missing").is_none());
    }

    #[test]
    fn auto_discover_runs() {
        let _registry = TaskRegistry::auto_discover();
    }

    #[test]
    fn get_or_err_not_found() {
        let registry = TaskRegistry::new();
        let err = registry.get_or_err("nonexistent").unwrap_err();
        assert!(matches!(err, BosonError::TaskNotFound(_)));
    }
}
