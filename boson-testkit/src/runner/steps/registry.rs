use std::sync::Arc;

use anyhow::Result;
use boson_runtime::{Boson, TaskRegistry};
use boson_telemetry::{install_ops_log, NoOpsLog};

use super::super::state::RunState;
use crate::fixtures::register_noop_task_with_signature_hash;
use crate::identity::StubExecutionContextFactory;
use crate::BootstrapSession;

/// Re-register a task with a new signature hash and rebuild the in-process worker.
pub async fn run_reregister_task_signature(
    session: &BootstrapSession,
    state: &mut RunState,
    task: &str,
    signature_hash: u64,
) -> Result<Option<String>> {
    let backend = session
        .backend()
        .ok_or_else(|| anyhow::anyhow!("no queue backend for reregister"))?;
    state.boson = None;
    state.manual = None;

    let mut registry = TaskRegistry::new();
    if task != "noop" {
        return Ok(Some(format!(
            "ReregisterTaskSignature: unsupported task {task} (testkit noop only)"
        )));
    }
    register_noop_task_with_signature_hash(&mut registry, "noop", signature_hash);
    let registry = Arc::new(registry);
    install_ops_log(Arc::new(NoOpsLog));

    let (boson, manual) = Boson::builder()
        .queue_backend(backend)
        .execution_context_factory(StubExecutionContextFactory)
        .registry(registry)
        .without_worker()
        .build_manual()?;

    state.boson = Some(boson);
    state.manual = Some(manual);
    Ok(None)
}
