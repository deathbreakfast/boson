use anyhow::Result;

use super::support::ScenarioStep;
use super::{RunMode, ScenarioRunner, StepTiming};
use super::state::RunState;
use super::steps::{
    run_admin_list_count, run_assert_different_job_id, run_assert_enqueue_error,
    run_assert_handler_hits, run_assert_job_count, run_assert_job_missing, run_assert_job_status,
    run_assert_run_count, run_assert_run_outcome, run_assert_same_job_id, run_assert_task_run_stats,
    run_cancel_job, run_cancel_missing_job, run_drain, run_enqueue, run_retry_backoff,
    run_reregister_task_signature, run_simulate_lease_contention, run_upsert_task_config,
};

impl ScenarioRunner<'_> {
    #[allow(clippy::too_many_lines)] // step dispatch match table
    pub(crate) async fn run_step(
        &self,
        step_index: usize,
        step: &ScenarioStep,
        mode: RunMode,
        state: &mut RunState,
        timings: &mut Vec<StepTiming>,
    ) -> Result<Option<String>> {
        match step {
            ScenarioStep::EnqueueN {
                task,
                count,
                idempotency_key,
            } => {
                run_enqueue(
                    step_index,
                    mode,
                    state,
                    timings,
                    task,
                    *count,
                    idempotency_key.as_ref(),
                )
                .await
            }
            ScenarioStep::AssertEnqueueError { task, error } => {
                run_assert_enqueue_error(mode, state, task, *error).await
            }
            ScenarioStep::UpsertTaskConfig {
                task,
                max_in_flight,
                max_enqueue_per_second,
                max_attempts,
                base_delay_ms,
            } => {
                run_upsert_task_config(
                    state,
                    task,
                    *max_in_flight,
                    *max_enqueue_per_second,
                    *max_attempts,
                    *base_delay_ms,
                )
                .await
            }
            ScenarioStep::CancelJob { job_index } => run_cancel_job(state, *job_index).await,
            ScenarioStep::CancelMissingJob => run_cancel_missing_job(state).await,
            ScenarioStep::AssertJobMissing { job_id } => {
                run_assert_job_missing(mode, state, job_id).await
            }
            ScenarioStep::DrainUntilIdle { max_steps } => {
                run_drain(step_index, mode, state, timings, *max_steps).await
            }
            ScenarioStep::AssertJobStatus { job_index, status } => {
                run_assert_job_status(mode, state, *job_index, *status).await
            }
            ScenarioStep::AssertRunOutcome {
                job_index,
                run_status,
            } => run_assert_run_outcome(mode, state, *job_index, *run_status).await,
            ScenarioStep::AssertSameJobId {
                first_index,
                second_index,
            } => Ok(run_assert_same_job_id(mode, state, *first_index, *second_index)),
            ScenarioStep::AssertDifferentJobId {
                first_index,
                second_index,
            } => Ok(run_assert_different_job_id(
                mode,
                state,
                *first_index,
                *second_index,
            )),
            ScenarioStep::AssertHandlerHits { task, count } => {
                Ok(run_assert_handler_hits(mode, state, task, *count))
            }
            ScenarioStep::AssertJobCount { count, status } => {
                run_assert_job_count(mode, state, *count, *status).await
            }
            ScenarioStep::AssertRunCount { job_index, count } => {
                run_assert_run_count(mode, state, *job_index, *count).await
            }
            ScenarioStep::RestartRuntime => {
                self.rebuild_state(state)?;
                Ok(None)
            }
            ScenarioStep::SimulateLeaseContention { ttl_secs, .. } => {
                run_simulate_lease_contention(state, *ttl_secs).await
            }
            ScenarioStep::RetryBackoff { task, fail_attempts } => {
                run_retry_backoff(mode, state, task, *fail_attempts).await
            }
            ScenarioStep::RemoteEnqueue { .. } => {
                Ok(Some("RemoteEnqueue requires host HTTP coordinator wiring".into()))
            }
            ScenarioStep::AdminListCount { expected_count } => {
                run_admin_list_count(step_index, mode, state, timings, *expected_count).await
            }
            ScenarioStep::AssertTaskRunStats {
                task,
                runs_total,
                success_count,
            } => {
                run_assert_task_run_stats(mode, state, task, *runs_total, *success_count).await
            }
            ScenarioStep::ReregisterTaskSignature {
                task,
                signature_hash,
            } => {
                run_reregister_task_signature(self.session, state, task, *signature_hash).await
            }
        }
    }
}
