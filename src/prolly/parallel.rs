//! Runtime tuning for canonical batch mutation execution.
//!
//! Parallel configuration may change bounded I/O scheduling, but never the
//! chunking algorithm or resulting root.

use std::ops::Range;

use super::batch::{self, BatchApplyResult};
use super::error::{Error, Mutation};
use super::store::Store;
use super::{Prolly, Tree};

/// Runtime-only parallelism preferences for canonical batch writes.
#[derive(Clone, Debug)]
pub struct ParallelConfig {
    /// Maximum worker/read width. Zero selects the implementation default.
    pub max_threads: usize,
    /// Mutation count below which sequential scheduling is preferred.
    pub parallelism_threshold: usize,
}

impl Default for ParallelConfig {
    fn default() -> Self {
        Self {
            max_threads: 0,
            parallelism_threshold: 100,
        }
    }
}

impl ParallelConfig {
    /// Create explicit runtime parallelism preferences.
    pub fn new(max_threads: usize, parallelism_threshold: usize) -> Self {
        Self {
            max_threads,
            parallelism_threshold,
        }
    }

    /// Force sequential scheduling without selecting a different writer.
    pub fn sequential() -> Self {
        Self {
            max_threads: 1,
            parallelism_threshold: usize::MAX,
        }
    }
}

/// Scheduling decisions derived from a caller's runtime preferences.
///
/// The policy only controls how independent work is partitioned. It never
/// selects a boundary algorithm or changes canonical assembly order.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ExecutionPolicy {
    width: usize,
    wave_size: usize,
    enabled: bool,
}

impl ExecutionPolicy {
    pub(crate) fn from_config(
        config: &ParallelConfig,
        effective_mutations: usize,
        independent_work: usize,
    ) -> Self {
        let pool_width = rayon::current_num_threads().max(1);
        let configured = if config.max_threads == 0 {
            pool_width
        } else {
            config.max_threads.min(pool_width).max(1)
        };
        let width = configured.min(independent_work.max(1));
        let enabled = width > 1
            && independent_work > 1
            && effective_mutations >= config.parallelism_threshold;
        let width = if enabled { width } else { 1 };
        Self {
            width,
            wave_size: width.saturating_mul(4).max(1),
            enabled,
        }
    }

    pub(crate) fn automatic(effective_mutations: usize, independent_work: usize) -> Self {
        Self::from_config(
            &ParallelConfig::default(),
            effective_mutations,
            independent_work,
        )
    }

    pub(crate) fn sequential() -> Self {
        Self {
            width: 1,
            wave_size: 1,
            enabled: false,
        }
    }

    pub(crate) fn enabled(self) -> bool {
        self.enabled
    }

    pub(crate) fn width(self) -> usize {
        self.width
    }

    pub(crate) fn wave_size(self) -> usize {
        self.wave_size
    }

    pub(crate) fn limit_to(self, independent_work: usize) -> Self {
        let width = self.width.min(independent_work.max(1));
        let enabled = self.enabled && width > 1 && independent_work > 1;
        let width = if enabled { width } else { 1 };
        Self {
            width,
            wave_size: width.saturating_mul(4).max(1),
            enabled,
        }
    }

    pub(crate) fn ranges(self, len: usize) -> Vec<Range<usize>> {
        if len == 0 {
            return Vec::new();
        }
        let partitions = self.width.min(len).max(1);
        let chunk = len.div_ceil(partitions);
        (0..len)
            .step_by(chunk)
            .map(|start| start..(start + chunk).min(len))
            .collect()
    }
}

pub(crate) fn parallel_batch_with_stats<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    config: &ParallelConfig,
) -> Result<BatchApplyResult, Error> {
    batch::apply_with_stats_configured(prolly, tree, mutations, config)
}

#[cfg(test)]
mod tests {
    use super::{ExecutionPolicy, ParallelConfig};

    #[test]
    fn execution_policy_honors_threshold_and_width() {
        let sequential = ExecutionPolicy::from_config(&ParallelConfig::new(8, 100), 99, 64);
        assert!(!sequential.enabled());
        assert_eq!(sequential.width(), 1);

        let parallel = ExecutionPolicy::from_config(&ParallelConfig::new(2, 1), 100, 8);
        assert!(parallel.enabled() || rayon::current_num_threads() == 1);
        assert!(parallel.width() <= 2);
        assert_eq!(parallel.wave_size(), parallel.width() * 4);

        let limited = parallel.limit_to(1);
        assert!(!limited.enabled());
        assert_eq!(limited.width(), 1);

        assert_eq!(ExecutionPolicy::sequential().width(), 1);
        assert_eq!(ExecutionPolicy::automatic(0, 0).width(), 1);
    }

    #[test]
    fn execution_policy_ranges_cover_input_once_in_order() {
        let policy = ExecutionPolicy::from_config(&ParallelConfig::new(4, 1), 17, 17);
        let ranges = policy.ranges(17);
        let covered = ranges.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(covered, (0..17).collect::<Vec<_>>());
        assert!(policy.ranges(0).is_empty());
    }
}
