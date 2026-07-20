use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::prolly::error::Error;

pub(crate) const DEFAULT_READ_PARALLELISM: usize = 16;
const DEFAULT_MAX_IN_FLIGHT_BYTES: usize = 64 * 1024 * 1024;
pub(crate) const DEFAULT_NODE_CACHE_MAX_NODES: usize = 16_384;
pub(crate) const DEFAULT_NODE_CACHE_MAX_BYTES: usize = 256 * 1024 * 1024;

/// Bounded runtime settings that do not participate in persisted tree identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExecutionConfig {
    read_parallelism: NonZeroUsize,
    max_in_flight_bytes: NonZeroUsize,
    node_cache_max_nodes: NonZeroUsize,
    node_cache_max_bytes: NonZeroUsize,
}

impl ExecutionConfig {
    /// Construct validated, finite execution limits.
    pub fn try_new(
        read_parallelism: usize,
        max_in_flight_bytes: usize,
        node_cache_max_nodes: usize,
        node_cache_max_bytes: usize,
    ) -> Result<Self, Error> {
        Ok(Self {
            read_parallelism: nonzero("read_parallelism", read_parallelism)?,
            max_in_flight_bytes: nonzero("max_in_flight_bytes", max_in_flight_bytes)?,
            node_cache_max_nodes: nonzero("node_cache_max_nodes", node_cache_max_nodes)?,
            node_cache_max_bytes: nonzero("node_cache_max_bytes", node_cache_max_bytes)?,
        })
    }

    pub fn read_parallelism(&self) -> NonZeroUsize {
        self.read_parallelism
    }

    pub fn max_in_flight_bytes(&self) -> NonZeroUsize {
        self.max_in_flight_bytes
    }

    pub fn node_cache_max_nodes(&self) -> NonZeroUsize {
        self.node_cache_max_nodes
    }

    pub fn node_cache_max_bytes(&self) -> NonZeroUsize {
        self.node_cache_max_bytes
    }
}

impl Default for ExecutionConfig {
    fn default() -> Self {
        Self {
            read_parallelism: NonZeroUsize::new(DEFAULT_READ_PARALLELISM).unwrap(),
            max_in_flight_bytes: NonZeroUsize::new(DEFAULT_MAX_IN_FLIGHT_BYTES).unwrap(),
            node_cache_max_nodes: NonZeroUsize::new(DEFAULT_NODE_CACHE_MAX_NODES).unwrap(),
            node_cache_max_bytes: NonZeroUsize::new(DEFAULT_NODE_CACHE_MAX_BYTES).unwrap(),
        }
    }
}

fn nonzero(field: &'static str, value: usize) -> Result<NonZeroUsize, Error> {
    NonZeroUsize::new(value).ok_or(Error::InvalidExecutionConfig { field, value })
}

/// Counters produced by exactly one public engine operation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct OperationStats {
    pub(crate) cache_hits: u64,
    pub(crate) cache_misses: u64,
    pub(crate) nodes_read: u64,
    pub(crate) bytes_read: u64,
    pub(crate) nodes_written: u64,
    pub(crate) bytes_written: u64,
    pub(crate) peak_in_flight_reads: usize,
}

/// Per-operation limits, cancellation state, and metrics.
#[allow(dead_code)]
pub(crate) struct OperationContext {
    pub(crate) limits: ExecutionConfig,
    stats: OperationStats,
    cancelled: AtomicBool,
}

#[allow(dead_code)]
impl OperationContext {
    pub(crate) fn new(limits: ExecutionConfig) -> Self {
        Self {
            limits,
            stats: OperationStats::default(),
            cancelled: AtomicBool::new(false),
        }
    }

    pub(crate) fn record_cache_hit(&mut self) {
        self.stats.cache_hits = self.stats.cache_hits.saturating_add(1);
    }

    pub(crate) fn record_cache_miss(&mut self) {
        self.stats.cache_misses = self.stats.cache_misses.saturating_add(1);
    }

    pub(crate) fn record_read(&mut self, bytes: usize) {
        self.stats.nodes_read = self.stats.nodes_read.saturating_add(1);
        self.stats.bytes_read = self.stats.bytes_read.saturating_add(bytes as u64);
    }

    pub(crate) fn record_write(&mut self, bytes: usize) {
        self.stats.nodes_written = self.stats.nodes_written.saturating_add(1);
        self.stats.bytes_written = self.stats.bytes_written.saturating_add(bytes as u64);
    }

    pub(crate) fn observe_in_flight_reads(&mut self, reads: usize) {
        self.stats.peak_in_flight_reads = self.stats.peak_in_flight_reads.max(reads);
    }

    pub(crate) fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    pub(crate) fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    pub(crate) fn finish(self) -> OperationStats {
        self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operation_stats_are_local_and_exact() {
        let mut operation = OperationContext::new(ExecutionConfig::default());
        operation.record_cache_hit();
        operation.record_cache_miss();
        operation.record_read(17);
        operation.record_write(23);
        operation.observe_in_flight_reads(2);
        operation.observe_in_flight_reads(1);

        let stats = operation.finish();
        assert_eq!(stats.cache_hits, 1);
        assert_eq!(stats.cache_misses, 1);
        assert_eq!(stats.nodes_read, 1);
        assert_eq!(stats.bytes_read, 17);
        assert_eq!(stats.nodes_written, 1);
        assert_eq!(stats.bytes_written, 23);
        assert_eq!(stats.peak_in_flight_reads, 2);
    }

    #[test]
    fn cancellation_is_operation_local() {
        let first = OperationContext::new(ExecutionConfig::default());
        let second = OperationContext::new(ExecutionConfig::default());
        first.cancel();
        assert!(first.is_cancelled());
        assert!(!second.is_cancelled());
    }
}
