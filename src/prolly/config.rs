//! Configuration for Prolly Trees

use serde::{Deserialize, Serialize};

use super::encoding::Encoding;
use super::engine::execution::{
    DEFAULT_NODE_CACHE_MAX_BYTES, DEFAULT_NODE_CACHE_MAX_NODES, DEFAULT_READ_PARALLELISM,
};
use super::format::{BoundaryRule, ChunkingSpec, NodeLayoutSpec, TreeFormat};

/// Runtime-only tuning that never participates in persisted tree identity.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RuntimeConfig {
    /// Optional maximum number of decoded nodes retained in each manager cache.
    pub node_cache_max_nodes: Option<usize>,
    /// Optional maximum serialized-node bytes retained in each manager cache.
    pub node_cache_max_bytes: Option<usize>,
    /// Preferred maximum number of concurrent ordered reads.
    pub read_parallelism: usize,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            node_cache_max_nodes: Some(DEFAULT_NODE_CACHE_MAX_NODES),
            node_cache_max_bytes: Some(DEFAULT_NODE_CACHE_MAX_BYTES),
            read_parallelism: DEFAULT_READ_PARALLELISM,
        }
    }
}

/// Tree configuration
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Config {
    /// Persisted settings that determine shape and content IDs.
    pub format: TreeFormat,
    /// Cache and I/O tuning local to this manager.
    pub runtime: RuntimeConfig,
}

impl Config {
    /// Create a new ConfigBuilder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Minimum configured chunk measure before probabilistic cuts.
    pub fn min_chunk_size(&self) -> usize {
        self.format.chunking.min as usize
    }

    /// Maximum configured chunk measure.
    pub fn max_chunk_size(&self) -> usize {
        self.format.chunking.max as usize
    }

    /// Hash-threshold factor, or the target measure for non-threshold policies.
    pub fn chunking_factor(&self) -> u32 {
        match self.format.chunking.rule {
            BoundaryRule::HashThreshold { factor } => factor,
            _ => self.format.chunking.target.min(u64::from(u32::MAX)) as u32,
        }
    }

    /// Boundary hash seed.
    pub fn hash_seed(&self) -> u64 {
        self.format.chunking.hash_seed
    }

    /// Value encoding descriptor.
    pub fn encoding(&self) -> &Encoding {
        &self.format.value_encoding
    }
}

/// Builder for Config
#[derive(Default)]
pub struct ConfigBuilder {
    config: Config,
}

impl ConfigBuilder {
    /// Set the minimum chunk size
    pub fn min_chunk_size(mut self, size: usize) -> Self {
        self.config.format.chunking.min = size as u64;
        self.config.format.chunking.target = self.config.format.chunking.target.max(size as u64);
        self.config.format.chunking.max = self.config.format.chunking.max.max(size as u64);
        self
    }

    /// Set the maximum chunk size
    pub fn max_chunk_size(mut self, size: usize) -> Self {
        self.config.format.chunking.max = size as u64;
        self.config.format.chunking.target = self.config.format.chunking.target.min(size as u64);
        self.config.format.chunking.min = self.config.format.chunking.min.min(size as u64);
        self
    }

    /// Set the chunking factor
    pub fn chunking_factor(mut self, factor: u32) -> Self {
        self.config.format.chunking.rule = BoundaryRule::HashThreshold { factor };
        self
    }

    /// Set the hash seed
    pub fn hash_seed(mut self, seed: u64) -> Self {
        self.config.format.chunking.hash_seed = seed;
        self
    }

    /// Set the encoding type
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.config.format.value_encoding = encoding;
        self
    }

    /// Select the complete persisted chunking policy.
    pub fn chunking(mut self, chunking: ChunkingSpec) -> Self {
        self.config.format.chunking = chunking;
        self
    }

    /// Select the physical node layout.
    pub fn node_layout(mut self, layout: NodeLayoutSpec) -> Self {
        self.config.format.node_layout = layout;
        self
    }

    /// Replace the complete persisted tree format.
    pub fn format(mut self, format: TreeFormat) -> Self {
        self.config.format = format;
        self
    }

    /// Set the maximum number of decoded nodes retained in each manager cache.
    ///
    /// Use `0` to disable node caching. Omit this setting to keep the cache
    /// unbounded.
    pub fn node_cache_max_nodes(mut self, max_nodes: usize) -> Self {
        self.config.runtime.node_cache_max_nodes = Some(max_nodes);
        self
    }

    /// Set the maximum serialized-node bytes retained in each manager cache.
    ///
    /// Use `0` to disable node caching. Omit this setting to keep the cache
    /// unbounded by bytes.
    pub fn node_cache_max_bytes(mut self, max_bytes: usize) -> Self {
        self.config.runtime.node_cache_max_bytes = Some(max_bytes);
        self
    }

    /// Keep each manager's decoded-node cache unbounded.
    pub fn unbounded_node_cache(mut self) -> Self {
        self.config.runtime.node_cache_max_nodes = None;
        self.config.runtime.node_cache_max_bytes = None;
        self
    }

    /// Set preferred ordered-read parallelism.
    pub fn read_parallelism(mut self, parallelism: usize) -> Self {
        self.config.runtime.read_parallelism = parallelism.max(1);
        self
    }

    /// Build the Config
    pub fn build(self) -> Config {
        self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config, Config::default());
        assert_eq!(config.runtime, RuntimeConfig::default());
    }

    #[test]
    fn test_config_builder() {
        let config = Config::builder()
            .min_chunk_size(2)
            .max_chunk_size(100)
            .chunking_factor(64)
            .hash_seed(42)
            .encoding(Encoding::Cbor)
            .node_cache_max_nodes(128)
            .node_cache_max_bytes(1_048_576)
            .build();

        assert_eq!(config.min_chunk_size(), 2);
        assert_eq!(config.max_chunk_size(), 100);
        assert_eq!(config.chunking_factor(), 64);
        assert_eq!(config.hash_seed(), 42);
        assert_eq!(config.encoding(), &Encoding::Cbor);
        assert_eq!(config.runtime.node_cache_max_nodes, Some(128));
        assert_eq!(config.runtime.node_cache_max_bytes, Some(1_048_576));

        let config = Config::builder()
            .node_cache_max_nodes(128)
            .node_cache_max_bytes(1_048_576)
            .unbounded_node_cache()
            .build();
        assert_eq!(config.runtime.node_cache_max_nodes, None);
        assert_eq!(config.runtime.node_cache_max_bytes, None);
    }
}
