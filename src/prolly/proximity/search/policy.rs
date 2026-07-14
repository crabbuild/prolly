use super::super::AdaptiveQuality;

pub(crate) struct AdaptiveContext {
    pub(crate) results: usize,
    pub(crate) k: usize,
    pub(crate) frontier_bound: f64,
    pub(crate) worst_score: f64,
    pub(crate) overlapping_clusters: usize,
    pub(crate) logical_level: u8,
    pub(crate) last_fanout: usize,
    pub(crate) cluster_count: usize,
}

pub(crate) fn adaptive_should_stop(quality: AdaptiveQuality, context: AdaptiveContext) -> bool {
    if context.results < context.k {
        return false;
    }
    let scale = context.worst_score.abs().max(1.0);
    let normalized_gap = (context.frontier_bound - context.worst_score) / scale;
    let (gap_floor, overlap_multiplier, minimum_clusters) = match quality {
        AdaptiveQuality::Fast => (-0.50, 1, 2),
        AdaptiveQuality::Balanced => (-0.25, 2, 8),
        AdaptiveQuality::HighRecall => (-0.10, 4, 32),
    };
    let overlap_limit = context
        .last_fanout
        .max(1)
        .saturating_mul(overlap_multiplier)
        .min(context.cluster_count);
    context.cluster_count >= minimum_clusters
        && context.logical_level <= 2
        && normalized_gap >= gap_floor
        && context.overlapping_clusters <= overlap_limit
}
