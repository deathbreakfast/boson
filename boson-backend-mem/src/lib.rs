//! In-memory [`QueueBackend`](boson_core::QueueBackend) adapter for tests and local development.
//!
//! **Topology:** Mode 1 embedded only — state lives in this process and cannot be shared with a
//! remote worker. For Mode 2 (enqueue host + worker binary), use `SQLite`, Postgres, Redis, or NATS.
//! See the [`boson`](https://docs.rs/uf-boson) crate
//! [Getting started](https://docs.rs/uf-boson/latest/boson/index.html#getting-started).
//!
//! ## Entry points
//!
//! - [`MemQueueBackend`] — in-process queue persistence
//! - [`install_default_mem_backend`] — register on global [`QueueRouter`](boson_core::QueueRouter)
//!
//! Useful as a reference when implementing [`QueueBackend`](boson_core::QueueBackend) — see
//! **How to implement** on the trait.

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
