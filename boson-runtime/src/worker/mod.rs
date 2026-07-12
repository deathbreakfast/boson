//! Background worker loop: claim queued jobs, dispatch registered handlers, finish runs, retry.
//!
//! [`BosonBuilder::build`](crate::BosonBuilder::build) calls [`spawn_worker`] automatically. For
//! tests, use [`BosonBuilder::build_manual`](crate::BosonBuilder::build_manual) and
//! [`ManualWorker::try_run_next`].

mod claim;
mod config;
mod execute;
mod lifecycle;
mod loop_;
mod manual;

pub use config::WorkerSettings;
pub use loop_::spawn_worker;
pub use manual::ManualWorker;
