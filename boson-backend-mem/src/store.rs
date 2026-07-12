//! In-memory store backing [`MemQueueBackend`](crate::MemQueueBackend).

use std::collections::HashMap;

use boson_core::{Job, Run, TaskConfig};
use chrono::{DateTime, Utc};

use crate::error::lock_err;

/// One distributed worker lease row.
#[derive(Debug, Clone)]
pub struct LeaseRecord {
    /// Job id stored as lease task id (distributed worker convention).
    pub job_id: String,
    /// When the lease expires (UTC).
    pub expires_at: DateTime<Utc>,
}

/// Process-local queue state (not durable).
#[derive(Debug, Default)]
pub struct Inner {
    /// Jobs keyed by id.
    pub jobs: HashMap<String, Job>,
    /// Runs keyed by id.
    pub runs: HashMap<String, Run>,
    /// Task configs keyed by task name.
    pub task_configs: HashMap<String, TaskConfig>,
    /// Leases keyed by lease id.
    pub leases: HashMap<String, LeaseRecord>,
}

impl Inner {
    /// Empty store.
    pub fn new() -> Self {
        Self::default()
    }
}

/// Acquire a read lock, mapping poison to [`BosonError::Backend`](boson_core::BosonError::Backend).
pub fn read<T, R>(
    lock: &std::sync::RwLock<T>,
    f: impl FnOnce(&T) -> R,
) -> boson_core::Result<R> {
    let guard = lock.read().map_err(|_| lock_err())?;
    Ok(f(&guard))
}

/// Acquire a write lock, mapping poison to [`BosonError::Backend`](boson_core::BosonError::Backend).
pub fn write<T, R>(
    lock: &std::sync::RwLock<T>,
    f: impl FnOnce(&mut T) -> R,
) -> boson_core::Result<R> {
    let mut guard = lock.write().map_err(|_| lock_err())?;
    Ok(f(&mut guard))
}

/// Like [`write`], but the closure may fail.
pub fn write_fallible<T, R>(
    lock: &std::sync::RwLock<T>,
    f: impl FnOnce(&mut T) -> boson_core::Result<R>,
) -> boson_core::Result<R> {
    let mut guard = lock.write().map_err(|_| lock_err())?;
    f(&mut guard)
}
