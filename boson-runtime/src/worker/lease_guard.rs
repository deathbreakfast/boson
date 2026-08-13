//! Abort lease heartbeats (and similar renew tasks) on drop.

use tokio::task::JoinHandle;

/// Aborts the inner [`JoinHandle`] when dropped.
///
/// Ensures orphaned lease heartbeats stop when a handler panics or returns early,
/// so the lease can expire and the reaper can reclaim the job.
pub(crate) struct AbortOnDrop(Option<JoinHandle<()>>);

impl AbortOnDrop {
    pub(crate) fn new(handle: Option<JoinHandle<()>>) -> Self {
        Self(handle)
    }
}

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        if let Some(handle) = self.0.take() {
            handle.abort();
        }
    }
}
