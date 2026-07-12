//! Markdown rendering for fleet projections.

use super::model::FleetProjection;

/// Render projection as markdown for stdout.
pub fn render_markdown(p: &FleetProjection) -> String {
    format!(
        "## Fleet projection ({}/{})\n\n\
         - Target: **{} jobs/s**\n\
         - Per-partition ceiling: {:?} ops/s\n\
         - Partitions for target: {:?}\n\
         - Nodes required (1 part/node): {:?}\n\
         - $/M ops (compute): {:?}\n\n\
         {}",
        p.hardware,
        p.backend,
        p.target_ops_per_sec,
        p.per_partition_ops_per_sec,
        p.partitions_for_10e6,
        p.nodes_required,
        p.cost_per_million_ops_usd,
        p.disclaimer,
    )
}
