//! Deterministic, content-addressed approximate nearest-neighbor maps.

mod builder;
mod cache;
mod distance;
mod map;
mod mutation;
mod storage;
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
    /// One minus the dot product of canonical unit vectors.
    Cosine,
    /// Negated dot product, preserving lower-is-better ordering.
    InnerProduct,
}

impl DistanceMetric {
    pub(crate) fn id(self) -> u8 {
        match self {
            Self::L2Squared => 1,
            Self::Cosine => 2,
            Self::InnerProduct => 3,
        }
    }

    pub(crate) fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            1 => Ok(Self::L2Squared),
            2 => Ok(Self::Cosine),
            3 => Ok(Self::InnerProduct),
            _ => Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: format!("unknown distance metric id {id}"),
            }),
        }
    }
}

/// Deterministic hierarchy promotion configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HierarchyConfig {
    /// Number of leading hash bits consumed per promotion level.
    pub log_chunk_size: u8,
    /// Seed for deterministic key promotion.
    pub level_hash_seed: u64,
}

impl Default for HierarchyConfig {
    fn default() -> Self {
        Self {
            log_chunk_size: 8,
            level_hash_seed: 0,
        }
    }
}

/// Canonical physical-node paging configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OverflowConfig {
    /// Minimum encoded bytes before a content boundary may close a page.
    pub min_page_bytes: u32,
    /// Preferred encoded page size used for capacity planning.
    pub target_page_bytes: u32,
    /// Hard encoded-byte limit for one physical proximity object.
    pub max_page_bytes: u32,
    /// Seed for deterministic overflow boundaries.
    pub hash_seed: u64,
}

impl Default for OverflowConfig {
    fn default() -> Self {
        Self {
            min_page_bytes: 4 * 1024,
            target_page_bytes: 64 * 1024,
            max_page_bytes: 4 * 1024 * 1024,
            hash_seed: 0,
        }
    }
}

/// Canonical inline versus external representative-vector policy.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VectorStorageConfig {
    /// Maximum canonical vector bytes stored inline in a PRXN object.
    pub inline_threshold_bytes: u32,
}

impl Default for VectorStorageConfig {
    fn default() -> Self {
        Self {
            inline_threshold_bytes: 64 * 1024,
        }
    }
}

/// Node-local symmetric signed-int8 routing configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScalarQuantizationConfig {
    /// Consecutive dimensions sharing one canonical scale.
    pub group_size: u32,
}

/// Shape-affecting configuration for a proximity map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProximityConfig {
    /// Number of finite `f32` components in every stored and query vector.
    pub dimensions: u32,
    /// Persisted distance metric.
    pub metric: DistanceMetric,
    /// Promotion and hierarchy shape.
    pub hierarchy: HierarchyConfig,
    /// Physical node paging and limits.
    pub overflow: OverflowConfig,
    /// Representative vector storage policy.
    pub vector_storage: VectorStorageConfig,
    /// Optional committed local routing quantization.
    pub scalar_quantization: Option<ScalarQuantizationConfig>,
}

impl ProximityConfig {
    /// Construct a production-oriented squared-L2 configuration.
    pub fn new(dimensions: u32) -> Self {
        Self {
            dimensions,
            metric: DistanceMetric::L2Squared,
            hierarchy: HierarchyConfig::default(),
            overflow: OverflowConfig::default(),
            vector_storage: VectorStorageConfig::default(),
            scalar_quantization: None,
        }
    }

    /// Validate all shape-affecting fields before reading or writing nodes.
    pub fn validate(&self) -> Result<(), Error> {
        if self.dimensions == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "dimensions must be greater than zero".to_owned(),
            });
        }
        if !(1..=63).contains(&self.hierarchy.log_chunk_size) {
            return Err(Error::InvalidProximityConfig {
                reason: "log_chunk_size must be in 1..=63".to_owned(),
            });
        }
        if self.overflow.max_page_bytes < MIN_PROXIMITY_NODE_BYTES {
            return Err(Error::InvalidProximityConfig {
                reason: format!("max_page_bytes must be at least {MIN_PROXIMITY_NODE_BYTES}"),
            });
        }
        if self.overflow.min_page_bytes == 0
            || self.overflow.min_page_bytes > self.overflow.target_page_bytes
            || self.overflow.target_page_bytes > self.overflow.max_page_bytes
        {
            return Err(Error::InvalidProximityConfig {
                reason: "overflow page sizes must satisfy 0 < min <= target <= max".to_owned(),
            });
        }
        if self.vector_storage.inline_threshold_bytes == 0
            || self.vector_storage.inline_threshold_bytes > self.overflow.max_page_bytes
        {
            return Err(Error::InvalidProximityConfig {
                reason: "inline_threshold_bytes must be in 1..=max_page_bytes".to_owned(),
            });
        }
        if self
            .scalar_quantization
            .as_ref()
            .is_some_and(|config| config.group_size == 0)
        {
            return Err(Error::InvalidProximityConfig {
                reason: "scalar quantization group_size must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

/// Deterministic approximate-search quality policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdaptiveQuality {
    Fast,
    Balanced,
    HighRecall,
}

/// Logical search termination policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchPolicy {
    Exact,
    FixedBudget,
    Adaptive(AdaptiveQuality),
}

/// Search execution backend.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchBackend {
    Native,
    Hnsw,
    Auto,
}

/// Honest completion state for a search result.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SearchCompletion {
    Exact,
    ApproximatePolicySatisfied,
    BudgetExhausted,
    Cancelled,
    DeadlineExceeded,
}

/// Deterministic logical search resource limits.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SearchBudget {
    pub max_nodes: Option<usize>,
    pub max_committed_bytes: Option<usize>,
    pub max_distance_evaluations: Option<usize>,
    pub max_frontier_entries: Option<usize>,
}

impl SearchBudget {
    /// Reject zero limits, which cannot perform a meaningful search.
    pub fn validate(&self) -> Result<(), Error> {
        let limits = [
            ("max_nodes", self.max_nodes),
            ("max_committed_bytes", self.max_committed_bytes),
            ("max_distance_evaluations", self.max_distance_evaluations),
            ("max_frontier_entries", self.max_frontier_entries),
        ];
        if let Some((name, _)) = limits.into_iter().find(|(_, value)| *value == Some(0)) {
            return Err(Error::InvalidProximitySearch {
                reason: format!("{name} must be greater than zero"),
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
