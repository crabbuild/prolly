//! Built-in persisted chunking policies.

use super::encoding::{
    DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_MIN_CHUNK_SIZE,
};
use super::format::{BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec, HashAlgorithm};

/// Entry-count chunking with boundaries derived from keys and values.
pub fn entry_count_key_value_hash() -> ChunkingSpec {
    ChunkingSpec {
        measure: ChunkMeasure::EntryCount,
        input: BoundaryInput::KeyValue,
        hash: HashAlgorithm::XxHash64,
        rule: BoundaryRule::HashThreshold {
            factor: DEFAULT_CHUNKING_FACTOR,
        },
        min: DEFAULT_MIN_CHUNK_SIZE as u64,
        target: DEFAULT_CHUNKING_FACTOR as u64,
        max: DEFAULT_MAX_CHUNK_SIZE as u64,
        hash_seed: DEFAULT_HASH_SEED,
        level_salt: false,
        hard_max_node_bytes: 16 * 1024 * 1024,
    }
}

/// Entry-count chunking with value-stable, key-only boundaries.
pub fn entry_count_key_hash() -> ChunkingSpec {
    ChunkingSpec::default()
}

/// Logical-byte chunking with key-only bounded Weibull boundaries.
pub fn logical_bytes_key_weibull() -> ChunkingSpec {
    ChunkingSpec {
        measure: ChunkMeasure::LogicalBytes,
        input: BoundaryInput::Key,
        hash: HashAlgorithm::XxHash64,
        rule: BoundaryRule::Weibull { shape: 2 },
        min: 4 * 1024,
        target: 16 * 1024,
        max: 64 * 1024,
        hash_seed: DEFAULT_HASH_SEED,
        level_salt: true,
        hard_max_node_bytes: 16 * 1024 * 1024,
    }
}

/// Logical-byte chunking with a rolling content hash.
pub fn logical_bytes_rolling_hash() -> ChunkingSpec {
    ChunkingSpec {
        measure: ChunkMeasure::LogicalBytes,
        input: BoundaryInput::Key,
        hash: HashAlgorithm::XxHash64,
        rule: BoundaryRule::RollingBuzHash { window: 64 },
        min: 4 * 1024,
        target: 16 * 1024,
        max: 64 * 1024,
        hash_seed: DEFAULT_HASH_SEED,
        level_salt: true,
        hard_max_node_bytes: 16 * 1024 * 1024,
    }
}
