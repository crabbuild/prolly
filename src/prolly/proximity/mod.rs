//! Deterministic, content-addressed approximate nearest-neighbor maps.

pub(crate) mod accelerator;
mod build;
mod builder;
mod cache;
mod distance;
mod map;
mod mutation;
mod proof;
mod search;
pub(crate) mod storage;
mod vector;

use super::cid::Cid;
use super::error::Error;
use super::tree::Tree;

pub use accelerator::catalog::{
    AcceleratorCatalog, AcceleratorCatalogEntry, CatalogAcceleratorKind,
};
pub use accelerator::composite::{
    CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBaseKind,
    CompositeBuildLimits, CompositeBuildOrRebuildOutcome, CompositeBuildOutcome,
    CompositeBuildStats, CompositeRebuildOptions, FullRebuildReason,
};
pub use accelerator::hnsw::{
    HnswBuildLimits, HnswBuildStats, HnswConfig, HnswIndex, HnswRoutingVectorEncoding,
};
pub use accelerator::pq::{
    ProductQuantizationBuildLimits, ProductQuantizationBuildStats, ProductQuantizationConfig,
    ProductQuantizationQuality, ProductQuantizer,
};
pub use accelerator::AcceleratorSet;
pub use accelerator::{
    AsyncAcceleratorCatalog, AsyncAcceleratorSet, AsyncCompositeAccelerator, AsyncHnswIndex,
    AsyncProductQuantizer,
};
pub use build::{BuildParallelism, ProximityBuildStats};
pub use map::{ProximityMap, ProximityReadSession};
pub use proof::{
    ProximityMembershipProof, ProximityMembershipVerification, ProximityProofFilter,
    ProximitySearchClaim, ProximitySearchEvent, ProximitySearchProof, ProximitySearchRequest,
    ProximitySearchVerification, ProximityStructuralProof, ProximityStructuralVerification,
};
pub use search::{
    ApproximatePreference, HnswSearchOptions, PlannerPolicy, PqSearchOptions, ProximityFilter,
    SearchIo, SearchOptions, SearchPlan, SearchPlanSummary, SearchRequest, SearchRuntime,
    SearchRuntimePolicy, StoreCacheNamespace, SEARCH_PLAN_FORMAT_VERSION,
};
pub use search::{AsyncIoConfig, AsyncProximityMap, AsyncSearchControl, CancellationToken};

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
    ProductQuantized,
    Hnsw,
    Composite,
    Auto,
}

/// Runtime-only deterministic query distance implementation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryKernel {
    /// Canonical scalar accumulation used by persisted construction and mutation.
    ScalarDeterministic,
    /// Runtime-detected fixed-lane products with canonical scalar-order reduction.
    SimdDeterministic,
    /// Prefer deterministic SIMD and fall back to the canonical scalar kernel.
    AutoDeterministic,
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

/// Safe borrowed view of little-endian persisted vector components.
#[derive(Clone, Copy, Debug)]
pub struct ProximityVectorRef<'a> {
    bytes: &'a [u8],
    dimensions: u32,
}

impl<'a> ProximityVectorRef<'a> {
    pub(crate) fn from_encoded(vector: storage::EncodedVectorRef<'a>) -> Self {
        Self {
            bytes: vector.bytes,
            dimensions: vector.dimensions,
        }
    }

    pub fn dimensions(&self) -> usize {
        self.dimensions as usize
    }

    /// Canonical little-endian component bytes retained by this record view.
    /// Language adapters use this to expose callback-scoped vector views
    /// without materializing an intermediate `Vec<f32>`.
    pub fn as_le_bytes(&self) -> &'a [u8] {
        self.bytes
    }

    pub fn component(&self, index: usize) -> Option<f32> {
        if index >= self.dimensions() {
            return None;
        }
        let start = index.checked_mul(4)?;
        let bytes: [u8; 4] = self.bytes.get(start..start + 4)?.try_into().ok()?;
        Some(f32::from_bits(u32::from_le_bytes(bytes)))
    }

    pub fn iter(&self) -> impl ExactSizeIterator<Item = f32> + '_ {
        self.bytes.chunks_exact(4).map(|bytes| {
            f32::from_bits(u32::from_le_bytes(
                bytes.try_into().expect("validated vector component"),
            ))
        })
    }

    pub fn copy_to_slice(&self, output: &mut [f32]) -> Result<(), Error> {
        if output.len() != self.dimensions() {
            return Err(Error::InvalidProximityObject {
                kind: "record",
                reason: format!(
                    "output dimensions {} do not match record dimensions {}",
                    output.len(),
                    self.dimensions()
                ),
            });
        }
        for (slot, component) in output.iter_mut().zip(self.iter()) {
            *slot = component;
        }
        Ok(())
    }

    pub fn to_vec(&self) -> Vec<f32> {
        self.iter().collect()
    }
}

/// Callback-scoped exact proximity record.
#[derive(Clone, Copy, Debug)]
pub struct ProximityRecordRef<'a> {
    pub vector: ProximityVectorRef<'a>,
    pub value: &'a [u8],
}

impl ProximityRecordRef<'_> {
    pub fn to_owned(self) -> ExactProximityRecord {
        (self.vector.to_vec(), self.value.to_vec())
    }
}

/// One immutable-map mutation. `None` deletes the key.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximityMutation {
    pub key: Vec<u8>,
    pub value: Option<(Vec<f32>, Vec<u8>)>,
}

/// One resolved nearest-neighbor result.
#[derive(Clone, Debug, PartialEq)]
pub struct Neighbor {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub distance: f64,
}

/// Observable resource use for one search.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximitySearchStats {
    pub levels_visited: usize,
    pub nodes_read: usize,
    pub bytes_read: usize,
    pub physical_bytes_read: usize,
    pub committed_bytes: usize,
    pub distance_evaluations: usize,
    pub quantized_distance_evaluations: usize,
    pub reranked_candidates: usize,
    pub frontier_peak: usize,
    /// Maximum number of retained authoritative directory records awaiting
    /// final top-k materialization.
    pub candidate_handles_peak: usize,
    /// Maximum unique packed-leaf bytes retained by authoritative candidates.
    pub candidate_retained_bytes_peak: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct SearchResult {
    pub neighbors: Vec<Neighbor>,
    pub stats: ProximitySearchStats,
    pub completion: SearchCompletion,
    pub plan: search::SearchPlanSummary,
}

/// Observable copy-on-write work for one mutation batch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProximityMutationStats {
    pub directory_entries_scanned: usize,
    pub directory_nodes_read: usize,
    pub directory_nodes_rebuilt: usize,
    pub directory_nodes_written: usize,
    pub directory_nodes_reused: usize,
    pub directory_levels_rebuilt: usize,
    pub directory_right_edge_rebuilt: bool,
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
    pub external_vector_count: usize,
    pub quantized_node_count: usize,
    pub scalar_quantizer_count: usize,
    pub overflow_page_count: usize,
    pub overflow_directory_count: usize,
    pub maximum_level: u8,
    pub maximum_node_bytes: usize,
    pub distance_checks: usize,
}
