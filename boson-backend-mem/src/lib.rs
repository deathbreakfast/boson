//! In-memory [`QueueBackend`](boson_core::QueueBackend) adapter for tests and local development.
//!
//! ## Entry points
//!
//! - [`MemQueueBackend`] — in-process queue persistence
//! - [`install_default_mem_backend`] — register on global [`QueueRouter`](boson_core::QueueRouter)
//!
//! Pair with [`Boson`](https://docs.rs/boson-runtime/latest/boson_runtime/struct.Boson.html) at worker boot.
//! Useful as a reference when implementing [`QueueBackend`](boson_core::QueueBackend) — see **How to implement** on the trait.

mod bootstrap;
mod enqueue_rate;
mod error;
mod jobs;
mod leases;
mod mem_queue_backend;
mod runs;
mod store;
mod task_config;

pub use bootstrap::install_default_mem_backend;
pub use mem_queue_backend::MemQueueBackend;
