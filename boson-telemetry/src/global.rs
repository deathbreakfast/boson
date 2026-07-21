//! Process-wide [`OpsLog`] install.

use std::sync::{Arc, OnceLock, RwLock};

use super::{ConsoleOpsLog, NoOpsLog, OpsLog};

static GLOBAL: OnceLock<RwLock<Option<Arc<dyn OpsLog>>>> = OnceLock::new();

fn slot() -> &'static RwLock<Option<Arc<dyn OpsLog>>> {
    GLOBAL.get_or_init(|| RwLock::new(None))
}

/// Install the process-wide ops log (typically at server boot before Boson runtime).
///
/// Recovers from a poisoned lock by taking the inner value.
pub fn install_ops_log(log: Arc<dyn OpsLog>) {
    let mut guard = slot()
        .write()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    *guard = Some(log);
}

/// Resolved ops log — [`NoOpsLog`] until [`install_ops_log`].
#[must_use]
pub fn ops_log() -> Arc<dyn OpsLog> {
    slot()
        .read()
        .ok()
        .and_then(|g| g.clone())
        .unwrap_or_else(|| Arc::new(NoOpsLog))
}

/// Resolve from `BOSON_TELEMETRY` (`off` | `console`; default `console`).
#[must_use]
pub fn ops_log_from_env() -> Arc<dyn OpsLog> {
    match std::env::var("BOSON_TELEMETRY")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .as_deref()
    {
        Some("off" | "0" | "false" | "none") => Arc::new(NoOpsLog),
        _ => Arc::new(ConsoleOpsLog),
    }
}
