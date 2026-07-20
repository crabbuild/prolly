//! Runtime tuning for canonical batch mutation execution.
//!
//! Parallel configuration may change bounded I/O scheduling, but never the
//! chunking algorithm or resulting root.

use std::cell::Cell;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};

#[cfg(test)]
use rayon::prelude::*;

static ACTIVE_CANONICAL_WRITES: AtomicUsize = AtomicUsize::new(0);
thread_local! {
    static CANONICAL_WRITE_DEPTH: Cell<usize> = const { Cell::new(0) };
}

pub(crate) struct CanonicalWriteConcurrencyGuard;

impl CanonicalWriteConcurrencyGuard {
    pub(crate) fn enter() -> Self {
        CANONICAL_WRITE_DEPTH.with(|depth| depth.set(depth.get().saturating_add(1)));
        ACTIVE_CANONICAL_WRITES.fetch_add(1, Ordering::AcqRel);
        Self
    }
}

impl Drop for CanonicalWriteConcurrencyGuard {
    fn drop(&mut self) {
        ACTIVE_CANONICAL_WRITES.fetch_sub(1, Ordering::AcqRel);
        CANONICAL_WRITE_DEPTH.with(|depth| depth.set(depth.get().saturating_sub(1)));
    }
}

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
    read_width: usize,
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
        let active_writes = CANONICAL_WRITE_DEPTH.with(|depth| {
            if depth.get() == 0 {
                1
            } else {
                ACTIVE_CANONICAL_WRITES.load(Ordering::Acquire).max(1)
            }
        });
        let saturated_by_callers = active_writes.saturating_mul(3) > pool_width;
        let configured = if saturated_by_callers {
            1
        } else if config.max_threads == 0 {
            pool_width
        } else {
            config.max_threads.min(pool_width).max(1)
        };
        let width = configured.min(independent_work.max(1));
        let enabled = width > 1
            && independent_work > 1
            && effective_mutations >= config.parallelism_threshold;
        let width = if enabled { width } else { 1 };
        let read_width = if enabled {
            if config.max_threads == 0 {
                16.max(pool_width).min(independent_work.max(1))
            } else {
                width
            }
        } else {
            1
        };
        Self {
            width,
            read_width,
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

    #[allow(dead_code)]
    pub(crate) fn sequential() -> Self {
        Self {
            width: 1,
            read_width: 1,
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

    pub(crate) fn read_width(self) -> usize {
        self.read_width
    }

    pub(crate) fn wave_size(self) -> usize {
        self.wave_size
    }

    pub(crate) fn limit_to(self, independent_work: usize) -> Self {
        let width = self.width.min(independent_work.max(1));
        let enabled = self.enabled && width > 1 && independent_work > 1;
        let width = if enabled { width } else { 1 };
        let read_width = if enabled {
            self.read_width.min(independent_work.max(1))
        } else {
            1
        };
        Self {
            width,
            read_width,
            wave_size: width.saturating_mul(4).max(1),
            enabled,
        }
    }

    pub(crate) fn ranges(self, len: usize) -> Vec<Range<usize>> {
        indexed_ranges(len, self.width)
    }
}

/// Split indexed work into exactly `min(width, len)` balanced, non-empty
/// ranges. The helper is also used by configured route decoding so an
/// explicit CPU width cannot escape through a nested Rayon iterator.
pub(crate) fn indexed_ranges(len: usize, width: usize) -> Vec<Range<usize>> {
    if len == 0 {
        return Vec::new();
    }
    let partitions = width.min(len).max(1);
    let base = len / partitions;
    let remainder = len % partitions;
    (0..partitions)
        .map(|partition| {
            let start = partition * base + partition.min(remainder);
            let width = base + usize::from(partition < remainder);
            start..start + width
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn map_indexed_ranges<T, F>(len: usize, width: usize, map: F) -> Vec<T>
where
    T: Send,
    F: Fn(Range<usize>) -> T + Send + Sync,
{
    indexed_ranges(len, width)
        .into_par_iter()
        .map(map)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Barrier};

    use super::{
        map_indexed_ranges, CanonicalWriteConcurrencyGuard, ExecutionPolicy, ParallelConfig,
    };

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
        assert_eq!(ranges.len(), policy.width());
        assert!(ranges
            .windows(2)
            .all(|pair| pair[0].len().abs_diff(pair[1].len()) <= 1));
        let covered = ranges.into_iter().flatten().collect::<Vec<_>>();
        assert_eq!(covered, (0..17).collect::<Vec<_>>());
        assert!(policy.ranges(0).is_empty());

        let nearly_one_per_partition =
            ExecutionPolicy::from_config(&ParallelConfig::new(12, 1), 13, 13);
        assert_eq!(nearly_one_per_partition.ranges(13).len(), 12);
    }

    #[test]
    fn execution_policy_disables_inner_work_when_callers_saturate_the_pool() {
        let guards = (0..rayon::current_num_threads().max(1))
            .map(|_| CanonicalWriteConcurrencyGuard::enter())
            .collect::<Vec<_>>();
        let policy = ExecutionPolicy::from_config(&ParallelConfig::new(0, 1), 1_000, 1_000);

        assert_eq!(policy.width(), 1);
        assert!(!policy.enabled());
        drop(guards);
    }

    #[test]
    fn indexed_range_executor_never_exceeds_explicit_cpu_width() {
        rayon::ThreadPoolBuilder::new()
            .num_threads(8)
            .build()
            .unwrap()
            .install(|| {
                let barrier = Arc::new(Barrier::new(4));
                let active = Arc::new(AtomicUsize::new(0));
                let maximum = Arc::new(AtomicUsize::new(0));
                let ranges = map_indexed_ranges(100, 4, {
                    let barrier = barrier.clone();
                    let active = active.clone();
                    let maximum = maximum.clone();
                    move |range| {
                        let now = active.fetch_add(1, Ordering::AcqRel) + 1;
                        maximum.fetch_max(now, Ordering::AcqRel);
                        barrier.wait();
                        active.fetch_sub(1, Ordering::AcqRel);
                        range
                    }
                });

                assert_eq!(ranges.len(), 4);
                assert_eq!(maximum.load(Ordering::Acquire), 4);
            });
    }
}
