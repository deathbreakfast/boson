//! Maps logical backend names to concrete [`QueueBackend`](crate::QueueBackend) implementations.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use crate::backend::{QueueBackend, DEFAULT_BACKEND_NAME};
use crate::error::{BosonError, Result};

static GLOBAL_ROUTER: OnceLock<Arc<QueueRouter>> = OnceLock::new();

/// Registry for named queue backends (multi-backend hosts).
#[derive(Debug)]
pub struct QueueRouter {
    backends: RwLock<HashMap<String, Arc<dyn QueueBackend>>>,
}

impl Default for QueueRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl QueueRouter {
    /// Empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self {
            backends: RwLock::new(HashMap::new()),
        }
    }

    /// Register a single default backend under [`DEFAULT_BACKEND_NAME`](crate::backend::DEFAULT_BACKEND_NAME).
    pub fn with_default(backend: Arc<dyn QueueBackend>) -> Self {
        let mut router = Self::new();
        router.register(DEFAULT_BACKEND_NAME, backend);
        router
    }

    /// Register during initial setup (mutable bootstrap).
    ///
    /// # Panics
    ///
    /// Panics if the internal lock is poisoned.
    pub fn register(&mut self, name: &str, backend: Arc<dyn QueueBackend>) {
        self.backends
            .write()
            .expect("queue router lock not poisoned")
            .insert(name.to_string(), backend);
    }

    /// Register after [`Self::set_global`] (runtime registration).
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::Internal`](crate::BosonError::Internal) if the lock is poisoned.
    pub fn register_runtime(&self, name: &str, backend: Arc<dyn QueueBackend>) -> Result<()> {
        self.backends
            .write()
            .map_err(|_| BosonError::Internal("queue router lock poisoned".into()))?
            .insert(name.to_string(), backend);
        Ok(())
    }

    /// Resolve a backend by logical name.
    ///
    /// # Errors
    ///
    /// Returns [`BosonError::Internal`](crate::BosonError::Internal) if the lock is poisoned,
    /// or [`BosonError::UnknownBackend`](crate::BosonError::UnknownBackend) when `name` is not registered.
    pub fn resolve(&self, name: &str) -> Result<Arc<dyn QueueBackend>> {
        self.backends
            .read()
            .map_err(|_| BosonError::Internal("queue router lock poisoned".into()))?
            .get(name)
            .cloned()
            .ok_or_else(|| BosonError::UnknownBackend(name.to_string()))
    }

    /// Install the process-global router (call once at host boot).
    pub fn set_global(router: Self) {
        let _ = GLOBAL_ROUTER.set(Arc::new(router));
    }

    /// Global router (panics if [`Self::set_global`] was not called).
    ///
    /// # Panics
    ///
    /// Panics when [`Self::set_global`] was not called before this function.
    pub fn global() -> Arc<Self> {
        GLOBAL_ROUTER
            .get()
            .cloned()
            .expect("QueueRouter::set_global was not called")
    }

    /// Optional global router.
    pub fn try_global() -> Option<Arc<Self>> {
        GLOBAL_ROUTER.get().cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::JobEnqueueDisposition;
    use crate::models::{Job, TaskConfig};
    use async_trait::async_trait;
    use chrono::{DateTime, Utc};

    struct StubBackend;

    impl std::fmt::Debug for StubBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("StubBackend")
        }
    }

    #[async_trait]
    impl QueueBackend for StubBackend {
        async fn upsert_job(&self, _job: &Job) -> Result<()> {
            Ok(())
        }

        async fn enqueue_with_policies(
            &self,
            job: Job,
            _task_config: &TaskConfig,
        ) -> Result<(String, JobEnqueueDisposition)> {
            Ok((job.job_id, JobEnqueueDisposition::InsertedNew))
        }

        async fn get_job(&self, _job_id: &str) -> Result<Option<Job>> {
            Ok(None)
        }

        async fn list_jobs(
            &self,
            _status_filter: Option<crate::models::JobStatus>,
            _offset: usize,
            _limit: usize,
        ) -> Result<Vec<Job>> {
            Ok(vec![])
        }

        async fn cancel_job_if_active(&self, _job_id: &str) -> Result<()> {
            Ok(())
        }

        async fn try_claim_job(&self, _job_id: &str) -> Result<Option<Job>> {
            Ok(None)
        }

        async fn revert_job_to_queued(&self, _job_id: &str) -> Result<()> {
            Ok(())
        }

        async fn distinct_pools_queued(&self) -> Result<Vec<String>> {
            Ok(vec![])
        }

        async fn list_queued_for_pool_sorted(
            &self,
            _pool: &str,
            _limit: usize,
        ) -> Result<Vec<Job>> {
            Ok(vec![])
        }

        async fn count_jobs(
            &self,
            _status_filter: Option<crate::models::JobStatus>,
        ) -> Result<u64> {
            Ok(0)
        }

        async fn count_jobs_for_task(
            &self,
            _task_name: &str,
            _status: Option<crate::models::JobStatus>,
        ) -> Result<u64> {
            Ok(0)
        }

        async fn count_active_jobs_for_task(&self, _task_name: &str) -> Result<u32> {
            Ok(0)
        }

        async fn find_nonterminal_by_idempotency_key(&self, _key: &str) -> Result<Option<String>> {
            Ok(None)
        }

        async fn upsert_run(&self, _run: &crate::models::Run) -> Result<()> {
            Ok(())
        }

        async fn get_run(&self, _run_id: &str) -> Result<Option<crate::models::Run>> {
            Ok(None)
        }

        async fn list_runs(
            &self,
            _job_id_filter: Option<&str>,
            _offset: usize,
            _limit: usize,
        ) -> Result<Vec<crate::models::Run>> {
            Ok(vec![])
        }

        async fn finish_run(
            &self,
            _run_id: &str,
            _status: crate::models::RunStatus,
            _duration_ms: Option<i64>,
            _error_message: Option<String>,
        ) -> Result<()> {
            Ok(())
        }

        async fn count_runs(&self, _job_id_filter: Option<&str>) -> Result<u64> {
            Ok(0)
        }

        async fn count_runs_since(&self, _since: DateTime<Utc>) -> Result<u64> {
            Ok(0)
        }

        async fn task_run_stats(&self, _task_name: &str) -> Result<crate::models::TaskRunStats> {
            Ok(crate::models::TaskRunStats {
                runs_total: 0,
                success_count: 0,
            })
        }

        async fn get_task_config(&self, _task_name: &str) -> Result<Option<TaskConfig>> {
            Ok(None)
        }

        async fn upsert_task_config(&self, _config: &TaskConfig) -> Result<()> {
            Ok(())
        }

        async fn try_claim_run_lease(
            &self,
            _job_id: &str,
            _worker_id: &str,
            _ttl_secs: i64,
        ) -> Result<Option<String>> {
            Ok(None)
        }

        async fn extend_lease(&self, _lease_id: &str, _ttl_secs: i64) -> Result<()> {
            Ok(())
        }

        async fn release_lease(&self, _lease_id: &str) -> Result<()> {
            Ok(())
        }

        async fn expired_lease_job_pairs(&self) -> Result<Vec<(String, String)>> {
            Ok(vec![])
        }
    }

    #[test]
    fn register_and_resolve() {
        let mut router = QueueRouter::new();
        let backend: Arc<dyn QueueBackend> = Arc::new(StubBackend);
        router.register("test", Arc::clone(&backend));
        let resolved = router.resolve("test").expect("resolve");
        assert!(Arc::ptr_eq(&resolved, &backend));
    }

    #[test]
    fn unknown_backend_errors() {
        let router = QueueRouter::new();
        let err = router.resolve("missing").unwrap_err();
        assert!(matches!(err, BosonError::UnknownBackend(_)));
    }
}
