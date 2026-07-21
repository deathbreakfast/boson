use anyhow::Result;
use boson_runtime::{Boson, ManualWorker};

use super::support::{counting_hit_count, noop_hit_count};
use super::ScenarioRunner;

pub struct RunState {
    pub(crate) boson: Option<Boson>,
    pub(crate) manual: Option<ManualWorker>,
    pub(crate) job_ids: Vec<String>,
    pub(crate) noop_hits_at_start: usize,
    pub(crate) counting_hits_at_start: usize,
}

impl RunState {
    pub(crate) fn boson(&self) -> Result<&Boson> {
        self.boson
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("boson runtime not initialized"))
    }

    pub(crate) fn manual(&self) -> Result<&ManualWorker> {
        self.manual
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("manual worker not initialized"))
    }
}

impl ScenarioRunner<'_> {
    pub(crate) fn build_state(&self) -> Result<RunState> {
        let (boson, manual) = self.session.build_boson_manual()?;
        Ok(RunState {
            boson: Some(boson),
            manual: Some(manual),
            job_ids: Vec::new(),
            noop_hits_at_start: noop_hit_count(),
            counting_hits_at_start: counting_hit_count(),
        })
    }

    pub(crate) fn rebuild_state(&self, state: &mut RunState) -> Result<()> {
        state.boson = None;
        state.manual = None;
        let (boson, manual) = self.session.build_boson_manual()?;
        state.boson = Some(boson);
        state.manual = Some(manual);
        Ok(())
    }
}
