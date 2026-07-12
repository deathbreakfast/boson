//! Fleet projection model for 10M jobs/s decomposition.

use serde::Serialize;

use super::TARGET_OPS_PER_SEC;

/// Inputs collected from BM-BL* / BM-BM* report JSONs.
#[derive(Debug, Default, Clone)]
pub struct FleetProjectionInputs {
    /// Hardware profile slug.
    pub hardware: String,
    /// Backend adapter slug.
    pub backend: String,
    /// Single-pool sustained ceiling ops/s.
    pub per_partition_ceiling: Option<f64>,
    /// Multi-client aggregate ops/s.
    pub aggregate_ops_per_sec: Option<f64>,
    /// Peak BM-BM4 cluster throughput.
    pub cluster_peak_ops_per_sec: Option<f64>,
    /// Storage node count from Track T campaign.
    pub storage_node_count: Option<u8>,
    /// Hourly USD for cost projection.
    pub hourly_usd: f64,
}

/// Computed fleet projection toward 10M jobs/s aggregate.
#[derive(Debug, Serialize)]
pub struct FleetProjection {
    pub hardware: String,
    pub backend: String,
    pub per_partition_ops_per_sec: Option<f64>,
    pub aggregate_ops_per_sec: Option<f64>,
    pub cluster_peak_ops_per_sec: Option<f64>,
    pub storage_node_count: Option<u8>,
    pub partitions_for_10e6: Option<u64>,
    pub nodes_required: Option<u64>,
    pub cost_per_million_ops_usd: Option<f64>,
    pub target_ops_per_sec: u64,
    pub disclaimer: String,
}

/// Compute fleet projection from measured load-tier ceilings.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss, clippy::cast_precision_loss)]
pub fn compute(inputs: &FleetProjectionInputs) -> FleetProjection {
    let ceiling = inputs
        .per_partition_ceiling
        .or(inputs.aggregate_ops_per_sec)
        .or(inputs.cluster_peak_ops_per_sec)
        .filter(|r| *r > 0.0);

    let partitions = ceiling.map(|r| (TARGET_OPS_PER_SEC / r).ceil() as u64);
    let nodes = partitions.map(|p| p.max(1));
    let fleet_aggregate = ceiling.zip(partitions).map(|(r, p)| r * p as f64);

    let cost_per_m = fleet_aggregate.and_then(|agg| {
        if agg > 0.0 && inputs.hourly_usd > 0.0 {
            let nodes_f = nodes.unwrap_or(1) as f64;
            Some((inputs.hourly_usd * nodes_f / agg) * (1_000_000.0 / 3600.0))
        } else {
            None
        }
    });

    FleetProjection {
        hardware: inputs.hardware.clone(),
        backend: inputs.backend.clone(),
        per_partition_ops_per_sec: inputs.per_partition_ceiling,
        aggregate_ops_per_sec: inputs.aggregate_ops_per_sec,
        cluster_peak_ops_per_sec: inputs.cluster_peak_ops_per_sec,
        storage_node_count: inputs.storage_node_count,
        partitions_for_10e6: partitions,
        nodes_required: nodes,
        cost_per_million_ops_usd: cost_per_m,
        target_ops_per_sec: TARGET_OPS_PER_SEC as u64,
        disclaimer: "Aspirational decomposition — not a performance claim.".into(),
    }
}
