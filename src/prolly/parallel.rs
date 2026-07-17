//! Runtime tuning for canonical batch mutation execution.
//!
//! Parallel configuration may change bounded I/O scheduling, but never the
//! chunking algorithm or resulting root.

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

pub(crate) fn parallel_batch_with_stats<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
    _config: &ParallelConfig,
) -> Result<BatchApplyResult, Error> {
    batch::apply_with_stats(prolly, tree, mutations)
}
