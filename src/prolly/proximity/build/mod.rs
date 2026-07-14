mod canonical;
mod parallel;

pub use parallel::BuildParallelism;

/// Canonical logical work reported independently of worker count.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximityBuildStats {
    pub distance_evaluations: usize,
    pub proximity_objects: usize,
    pub proximity_objects_written: usize,
}
