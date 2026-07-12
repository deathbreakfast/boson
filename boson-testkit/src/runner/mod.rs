//! Shared scenario executor for e2e (correctness) and bench (timings).

mod dispatch;
mod state;
mod steps;
mod support;

use anyhow::{bail, Result};
use tokio::sync::Mutex;

use crate::bootstrap::BootstrapSession;
use crate::scenario::ScenarioSpec;

/// Serializes correctness runs that share fixture handler hit counters.
static CORRECTNESS_RUN_LOCK: Mutex<()> = Mutex::const_new(());

/// Driver mode: assert on outcomes vs collect timings only.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunMode {
    /// Failures populate [`ScenarioResult::error`].
    Correctness,
    /// Record per-step timings; assertions are skipped.
    Benchmark,
}

/// Per-step timing samples (milliseconds).
#[derive(Debug, Clone)]
pub struct StepTiming {
    /// Index in the scenario step list.
    pub step_index: usize,
    /// Operation label (`enqueue`, `drain`, …).
    pub op: String,
    /// One sample per operation (enqueue records one per job).
    pub samples_ms: Vec<f64>,
}

/// Outcome of running one [`ScenarioSpec`].
#[derive(Debug, Clone)]
pub struct ScenarioResult {
    /// Scenario id from the spec.
    pub scenario_id: String,
    /// Matrix slug for reports.
    pub matrix_slug: String,
    /// Jobs captured from enqueue steps.
    pub jobs_enqueued: u32,
    /// Benchmark timings (empty in correctness-only runs without timed steps).
    pub step_timings: Vec<StepTiming>,
    /// Set when a correctness assertion fails or a step is unsupported.
    pub error: Option<String>,
}

/// Executes declarative scenarios against a bootstrapped session.
pub struct ScenarioRunner<'a> {
    pub(crate) session: &'a BootstrapSession,
}

impl<'a> ScenarioRunner<'a> {
    /// Borrow a session that has completed [`BootstrapSession::install`].
    #[must_use]
    pub const fn new(session: &'a BootstrapSession) -> Self {
        Self { session }
    }

    /// Run all steps in `spec`.
    ///
    /// # Errors
    ///
    /// Returns an error if the session is not installed, state construction fails,
    /// or a step encounters a backend error.
    pub async fn run(&self, spec: &ScenarioSpec, mode: RunMode) -> Result<ScenarioResult> {
        let _correctness_guard = if mode == RunMode::Correctness {
            Some(CORRECTNESS_RUN_LOCK.lock().await)
        } else {
            None
        };

        if !self.session.is_ready() {
            bail!("BootstrapSession::install must succeed before running scenarios");
        }

        let matrix_slug = self.session.matrix().report_slug();
        let mut state = self.build_state()?;
        let mut step_timings = Vec::new();

        for (step_index, step) in spec.steps.iter().enumerate() {
            if let Some(err) = self
                .run_step(step_index, step, mode, &mut state, &mut step_timings)
                .await?
            {
                return Ok(ScenarioResult {
                    scenario_id: spec.id.clone(),
                    matrix_slug,
                    jobs_enqueued: u32::try_from(state.job_ids.len()).unwrap_or(u32::MAX),
                    step_timings,
                    error: Some(err),
                });
            }
        }

        Ok(ScenarioResult {
            scenario_id: spec.id.clone(),
            matrix_slug,
            jobs_enqueued: u32::try_from(state.job_ids.len()).unwrap_or(u32::MAX),
            step_timings,
            error: None,
        })
    }
}
