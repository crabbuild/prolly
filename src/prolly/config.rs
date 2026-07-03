//! Configuration for Prolly Trees

use serde::{Deserialize, Serialize};

use super::encoding::{
    Encoding, DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE,
    DEFAULT_MIN_CHUNK_SIZE,
};

/// Tree configuration
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    /// Min entries before considering boundaries
    pub min_chunk_size: usize,
    /// Max entries in a node
    pub max_chunk_size: usize,
    /// Chunking factor (higher = larger nodes on average)
    pub chunking_factor: u32,
    /// Hash seed for boundary detection
    pub hash_seed: u64,
    /// Default value encoding
    pub encoding: Encoding,
    /// Optional maximum number of decoded nodes retained in each manager cache.
    ///
    /// `None` keeps the cache unbounded, matching the historical behavior.
    /// `Some(0)` disables node caching.
    pub node_cache_max_nodes: Option<usize>,
    /// Optional maximum serialized-node bytes retained in each manager cache.
    ///
    /// The byte budget uses each node's compact serialized length as a stable
    /// approximation of decoded-node cache pressure. `None` keeps the cache
    /// unbounded by bytes. `Some(0)` disables node caching.
    pub node_cache_max_bytes: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            min_chunk_size: DEFAULT_MIN_CHUNK_SIZE,
            max_chunk_size: DEFAULT_MAX_CHUNK_SIZE,
            chunking_factor: DEFAULT_CHUNKING_FACTOR,
            hash_seed: DEFAULT_HASH_SEED,
            encoding: Encoding::Raw,
            node_cache_max_nodes: None,
            node_cache_max_bytes: None,
        }
    }
}

impl Config {
    /// Create a new ConfigBuilder
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
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
        self.config.min_chunk_size = size;
        self
    }

    /// Set the maximum chunk size
    pub fn max_chunk_size(mut self, size: usize) -> Self {
        self.config.max_chunk_size = size;
        self
    }

    /// Set the chunking factor
    pub fn chunking_factor(mut self, factor: u32) -> Self {
        self.config.chunking_factor = factor;
        self
    }

    /// Set the hash seed
    pub fn hash_seed(mut self, seed: u64) -> Self {
        self.config.hash_seed = seed;
        self
    }

    /// Set the encoding type
    pub fn encoding(mut self, encoding: Encoding) -> Self {
        self.config.encoding = encoding;
        self
    }

    /// Set the maximum number of decoded nodes retained in each manager cache.
    ///
    /// Use `0` to disable node caching. Omit this setting to keep the cache
    /// unbounded.
    pub fn node_cache_max_nodes(mut self, max_nodes: usize) -> Self {
        self.config.node_cache_max_nodes = Some(max_nodes);
        self
    }

    /// Set the maximum serialized-node bytes retained in each manager cache.
    ///
    /// Use `0` to disable node caching. Omit this setting to keep the cache
    /// unbounded by bytes.
    pub fn node_cache_max_bytes(mut self, max_bytes: usize) -> Self {
        self.config.node_cache_max_bytes = Some(max_bytes);
        self
    }

    /// Keep each manager's decoded-node cache unbounded.
    pub fn unbounded_node_cache(mut self) -> Self {
        self.config.node_cache_max_nodes = None;
        self.config.node_cache_max_bytes = None;
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
        assert_eq!(config.min_chunk_size, DEFAULT_MIN_CHUNK_SIZE);
        assert_eq!(config.max_chunk_size, DEFAULT_MAX_CHUNK_SIZE);
        assert_eq!(config.chunking_factor, DEFAULT_CHUNKING_FACTOR);
        assert_eq!(config.hash_seed, DEFAULT_HASH_SEED);
        assert_eq!(config.encoding, Encoding::Raw);
        assert_eq!(config.node_cache_max_nodes, None);
        assert_eq!(config.node_cache_max_bytes, None);
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

        assert_eq!(config.min_chunk_size, 2);
        assert_eq!(config.max_chunk_size, 100);
        assert_eq!(config.chunking_factor, 64);
        assert_eq!(config.hash_seed, 42);
        assert_eq!(config.encoding, Encoding::Cbor);
        assert_eq!(config.node_cache_max_nodes, Some(128));
        assert_eq!(config.node_cache_max_bytes, Some(1_048_576));

        let config = Config::builder()
            .node_cache_max_nodes(128)
            .node_cache_max_bytes(1_048_576)
            .unbounded_node_cache()
            .build();
        assert_eq!(config.node_cache_max_nodes, None);
        assert_eq!(config.node_cache_max_bytes, None);
    }
}
