//! Bench run configuration — public knobs for capacity, scale, and partition sweeps.
//!
//! Callers construct [`BenchRunConfig`] explicitly (or via [`BenchRunConfig::for_experiment`]).
//! The CLI may overlay flags or env onto these defaults; library modules read the struct only.

use boson_core::IdempotencyMode;
use serde::{Deserialize, Serialize};

/// How multi-client publishers map to queue pools.
///
/// Task name selects the handler; **pool** selects the backend partition (Redis ZSET,
/// Scylla `(pool, shard)`, etc.). [`PoolLayout::DistinctPerSlot`] registers `noop_i` on
/// `pool_i` so spread-load experiments hit distinct write paths.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum PoolLayout {
    /// All registered noop tasks enqueue to pool `global` (single hot partition).
    #[default]
    Shared,
    /// Slot `i` uses task `noop_i` (or `noop` when K=1) on pool `pool_i`.
    DistinctPerSlot,
}

/// Concurrent publisher settings (BM-BE*, BM-BM*, BM-BP*).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PublisherConfig {
    /// Number of concurrent enqueue client tasks.
    pub client_count: u32,
    /// Number of pool slots (1 = hot partition).
    pub pool_count: u32,
    /// Whether slots share one pool or use `pool_0..pool_{K-1}`.
    pub pool_layout: PoolLayout,
    /// Timed enqueue window in seconds.
    pub duration_secs: u64,
}

impl Default for PublisherConfig {
    fn default() -> Self {
        Self {
            client_count: 8,
            pool_count: 1,
            pool_layout: PoolLayout::Shared,
            duration_secs: 15,
        }
    }
}

/// Prefill + parallel drain settings (BM-BD*).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DrainConfig {
    /// Jobs enqueued before drain starts.
    pub prefill_count: u64,
    /// Parallel worker count (manual or background).
    pub worker_count: u32,
    /// Background worker poll interval ms (`0` = yield only).
    pub poll_interval_ms: u64,
    /// Max seconds to wait for queue empty / handler hits.
    pub timeout_secs: u64,
}

impl Default for DrainConfig {
    fn default() -> Self {
        Self {
            prefill_count: 10_000,
            worker_count: 10,
            poll_interval_ms: 0,
            timeout_secs: 600,
        }
    }
}

/// Worker fleet pinning for multi-pool drain (optional sweep dimension).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerFleetConfig {
    /// When set, each background/manual worker polls only these pools (disjoint subsets).
    pub worker_pools: Option<Vec<String>>,
}

/// Resolved configuration for one bench run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchRunConfig {
    /// Runtime idempotency for capacity runs (`None` = matrix default).
    pub idempotency_mode: Option<IdempotencyMode>,
    /// Publisher / scale knobs.
    pub publisher: PublisherConfig,
    /// Dequeue capacity knobs.
    pub drain: DrainConfig,
    /// BM-BF2 task fan-out count (`noop_0..`).
    pub task_fanout_count: u32,
    /// Optional storage topology label for report dimensions.
    pub storage_topology: Option<String>,
    /// Optional worker pool pinning.
    pub worker_fleet: WorkerFleetConfig,
}

impl Default for BenchRunConfig {
    fn default() -> Self {
        Self {
            idempotency_mode: None,
            publisher: PublisherConfig::default(),
            drain: DrainConfig::default(),
            task_fanout_count: 1,
            storage_topology: None,
            worker_fleet: WorkerFleetConfig::default(),
        }
    }
}

impl BenchRunConfig {
    /// Defaults tuned for BM-BE1/BE2/BE4/BM-BD* and related sweeps.
    #[must_use]
    pub fn for_experiment(experiment_id: &str) -> Self {
        let id = experiment_id.to_ascii_lowercase();
        let mut cfg = Self::default();

        match id.as_str() {
            "bm-be1" => {
                cfg.idempotency_mode = Some(IdempotencyMode::None);
                cfg.publisher = PublisherConfig {
                    client_count: 1,
                    pool_count: 1,
                    pool_layout: PoolLayout::Shared,
                    duration_secs: 15,
                };
            }
            "bm-be2" => {
                cfg.idempotency_mode = Some(IdempotencyMode::None);
                cfg.publisher = PublisherConfig {
                    client_count: 8,
                    pool_count: 1,
                    pool_layout: PoolLayout::Shared,
                    duration_secs: 15,
                };
            }
            "bm-be4" => {
                cfg.idempotency_mode = Some(IdempotencyMode::None);
                cfg.publisher = PublisherConfig {
                    client_count: 64,
                    pool_count: 10,
                    pool_layout: PoolLayout::DistinctPerSlot,
                    duration_secs: 15,
                };
            }
            "bm-bd1" | "bm-bd2" => {
                cfg.idempotency_mode = Some(IdempotencyMode::None);
                cfg.publisher.pool_count = 1;
                cfg.publisher.pool_layout = PoolLayout::Shared;
                cfg.worker_fleet.worker_pools = Some(vec!["global".to_string()]);
            }
            "bm-bm2" | "bm-bm4" => {
                cfg.publisher.client_count = 64;
                cfg.publisher.pool_count = 10;
                cfg.publisher.pool_layout = PoolLayout::DistinctPerSlot;
            }
            "bm-bm3" => {
                cfg.publisher.pool_count = 1;
                cfg.publisher.pool_layout = PoolLayout::Shared;
            }
            "bm-bp1" | "bm-bp2" => {
                cfg.publisher.pool_count = 10;
                cfg.publisher.pool_layout = PoolLayout::DistinctPerSlot;
            }
            _ => {}
        }

        cfg
    }

    /// Pool name for publisher slot `i` when [`PoolLayout::DistinctPerSlot`].
    #[must_use]
    pub fn pool_name_for_slot(slot: u32) -> String {
        format!("pool_{slot}")
    }

    /// Task name for publisher client `client` given publisher config.
    #[must_use]
    pub fn task_name_for_client(&self, client: u32) -> String {
        let pool_count = self.publisher.pool_count.max(1);
        if pool_count == 1 {
            "noop".to_string()
        } else {
            format!("noop_{}", client % pool_count)
        }
    }
}
