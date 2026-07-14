use super::super::AdaptiveQuality;

pub(crate) fn adaptive_should_stop(
    quality: AdaptiveQuality,
    results: usize,
    k: usize,
    frontier_bound: f64,
    worst_score: f64,
    overlapping_clusters: usize,
    logical_level: u8,
    last_fanout: usize,
    cluster_count: usize,
) -> bool {
    if results < k {
        return false;
    }
    let scale = worst_score.abs().max(1.0);
    let normalized_gap = (frontier_bound - worst_score) / scale;
    let (gap_floor, overlap_multiplier, minimum_clusters) = match quality {
        AdaptiveQuality::Fast => (-0.50, 1, 2),
        AdaptiveQuality::Balanced => (-0.25, 2, 8),
        AdaptiveQuality::HighRecall => (-0.10, 4, 32),
    };
    let overlap_limit = last_fanout
        .max(1)
        .saturating_mul(overlap_multiplier)
        .min(cluster_count);
    cluster_count >= minimum_clusters
        && logical_level <= 2
        && normalized_gap >= gap_floor
        && overlapping_clusters <= overlap_limit
}
