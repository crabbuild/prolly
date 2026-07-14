//! Deterministic, content-addressed approximate nearest-neighbor maps.

mod builder;
mod cache;
mod codec;
mod descriptor;
mod map;
mod mutation;
mod node;
mod record;
mod vector;

use super::cid::Cid;
use super::error::Error;
use super::tree::Tree;

pub use map::ProximityMap;

const MIN_PROXIMITY_NODE_BYTES: u32 = 64;

/// Distance function committed into a proximity-map descriptor.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DistanceMetric {
    /// Squared Euclidean distance.
    L2Squared,
}

impl DistanceMetric {
    pub(crate) fn id(self) -> u8 {
        match self {
            Self::L2Squared => 1,
        }
    }

    pub(crate) fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            1 => Ok(Self::L2Squared),
            _ => Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: format!("unknown distance metric id {id}"),
            }),
        }
    }
}

/// Shape-affecting configuration for a proximity map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProximityConfig {
    /// Number of finite `f32` components in every stored and query vector.
    pub dimensions: u32,
    /// Persisted distance metric.
    pub metric: DistanceMetric,
    /// Number of leading hash bits consumed per promotion level.
    pub log_chunk_size: u8,
    /// Seed for deterministic key promotion.
    pub level_hash_seed: u64,
    /// Hard encoded-byte limit for one proximity node.
    pub max_node_bytes: u32,
}

impl ProximityConfig {
    /// Construct a production-oriented squared-L2 configuration.
    pub fn new(dimensions: u32) -> Self {
        Self {
            dimensions,
            metric: DistanceMetric::L2Squared,
            log_chunk_size: 8,
            level_hash_seed: 0,
            max_node_bytes: 4 * 1024 * 1024,
        }
    }

    /// Validate all shape-affecting fields before reading or writing nodes.
    pub fn validate(&self) -> Result<(), Error> {
        if self.dimensions == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "dimensions must be greater than zero".to_owned(),
            });
        }
        if !(1..=63).contains(&self.log_chunk_size) {
            return Err(Error::InvalidProximityConfig {
                reason: "log_chunk_size must be in 1..=63".to_owned(),
            });
        }
        if self.max_node_bytes < MIN_PROXIMITY_NODE_BYTES {
            return Err(Error::InvalidProximityConfig {
                reason: format!("max_node_bytes must be at least {MIN_PROXIMITY_NODE_BYTES}"),
            });
        }
        Ok(())
    }
}

/// Persisted handle for the exact directory and derived proximity hierarchy.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityTree {
    pub directory: Tree,
    pub proximity_root: Cid,
    pub descriptor: Cid,
    pub count: u64,
    pub config: ProximityConfig,
}

/// One logical record supplied to a bulk build.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityRecord {
    pub key: Vec<u8>,
    pub vector: Vec<f32>,
    pub value: Vec<u8>,
}

/// Vector and application value returned by an exact-key lookup.
pub type ExactProximityRecord = (Vec<f32>, Vec<u8>);

/// One immutable-map mutation. `None` deletes the key.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityMutation {
    pub key: Vec<u8>,
    pub value: Option<(Vec<f32>, Vec<u8>)>,
}

/// Per-query quality and resource controls.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SearchOptions {
    pub k: usize,
    pub beam_width: usize,
    pub max_nodes: Option<usize>,
    pub max_distance_evaluations: Option<usize>,
}

impl SearchOptions {
    pub fn new(k: usize) -> Self {
        Self {
            k,
            beam_width: k.max(32),
            max_nodes: None,
            max_distance_evaluations: None,
        }
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.k == 0 {
            return Err(Error::InvalidProximitySearch {
                reason: "k must be greater than zero".to_owned(),
            });
        }
        if self.beam_width < self.k {
            return Err(Error::InvalidProximitySearch {
                reason: "beam_width must be at least k".to_owned(),
            });
        }
        if self.max_nodes == Some(0) {
            return Err(Error::InvalidProximitySearch {
                reason: "max_nodes must be greater than zero".to_owned(),
            });
        }
        if self.max_distance_evaluations == Some(0) {
            return Err(Error::InvalidProximitySearch {
                reason: "max_distance_evaluations must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

/// One resolved nearest-neighbor result.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbor {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub distance: f32,
}

/// Observable resource use for one search.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximitySearchStats {
    pub levels_visited: usize,
    pub nodes_read: usize,
    pub bytes_read: usize,
    pub distance_evaluations: usize,
    pub budget_exhausted: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    pub neighbors: Vec<Neighbor>,
    pub stats: ProximitySearchStats,
}

/// Observable copy-on-write work for one mutation batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximityMutationStats {
    pub nodes_read: usize,
    pub nodes_written: usize,
    pub nodes_reused: usize,
    pub records_rebuilt: usize,
    pub distance_evaluations: usize,
    pub full_proximity_rebuild: bool,
}

/// Full structural verification summary.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximityVerification {
    pub record_count: u64,
    pub proximity_node_count: usize,
    pub maximum_level: u8,
    pub maximum_node_bytes: usize,
    pub distance_checks: usize,
}
