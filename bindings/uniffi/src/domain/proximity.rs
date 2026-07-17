#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{default_config, ProllyEngine};

    #[test]
    fn immutable_proximity_workflow_is_portable() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let map = engine
            .build_proximity_map(
                ProximityConfigRecord::new(2),
                vec![
                    ProximityRecordRecord {
                        key: b"a".to_vec(),
                        vector: vec![0.0, 0.0],
                        value: b"alpha".to_vec(),
                    },
                    ProximityRecordRecord {
                        key: b"b".to_vec(),
                        vector: vec![1.0, 1.0],
                        value: b"beta".to_vec(),
                    },
                ],
                None,
            )
            .unwrap();
        assert_eq!(map.count().unwrap(), 2);
        assert_eq!(map.get(b"a".to_vec()).unwrap().unwrap().value, b"alpha");
        let session = map.read_session().unwrap();
        assert!(session.contains_key(b"b".to_vec()).unwrap());
        assert_eq!(session.get(b"b".to_vec()).unwrap().unwrap().value, b"beta");
        assert_ne!(session.fast_handle(), 0);
        assert_eq!(
            session
                .search(ProximitySearchRequestRecord::exact(vec![0.1, 0.1], 1))
                .unwrap()
                .neighbors[0]
                .key,
            b"a"
        );
        let result = map
            .search(ProximitySearchRequestRecord::exact(vec![0.1, 0.1], 1))
            .unwrap();
        assert_eq!(result.neighbors[0].key, b"a");
        let updated = map
            .mutate(vec![ProximityMutationRecord {
                key: b"a".to_vec(),
                vector: None,
                value: None,
            }])
            .unwrap();
        assert!(updated.map.get(b"a".to_vec()).unwrap().is_none());
        assert!(map.get(b"a".to_vec()).unwrap().is_some());
        assert_eq!(updated.map.verify().unwrap().record_count, 1);
        let membership = map.prove_membership(b"a".to_vec()).unwrap();
        let verified =
            verify_proximity_membership_proof(membership, Some(map.descriptor())).unwrap();
        assert!(verified.record.is_some());
        let structure = map
            .prove_structure(ContentGraphLimitsRecord::defaults())
            .unwrap();
        assert_eq!(
            verify_proximity_structure_proof(
                structure,
                Some(map.descriptor()),
                ContentGraphLimitsRecord::defaults(),
            )
            .unwrap()
            .summary
            .record_count,
            2
        );
        let search_proof = map
            .prove_search(
                ProximitySearchRequestRecord::exact(vec![0.1, 0.1], 1),
                ContentGraphLimitsRecord::defaults(),
            )
            .unwrap();
        let search_verification = search_proof
            .verify(Some(map.descriptor()), ContentGraphLimitsRecord::defaults())
            .unwrap();
        assert_eq!(search_verification.result.neighbors[0].key, b"a");
        assert_eq!(
            search_verification.claim.kind,
            ProximitySearchClaimKindRecord::ExactL2Optimal
        );
        assert!(search_verification.replayed_events > 0);
    }

    #[test]
    fn portable_proximity_fixture_is_valid_json() {
        let fixture = include_str!("../../../../conformance/binding-proximity-fixtures.v1.json");
        let parsed: serde_json::Value = serde_json::from_str(fixture).unwrap();
        assert_eq!(parsed["schema_version"], 1);
        assert_eq!(parsed["packed_page"]["magic"], "PRPG");
        assert_eq!(parsed["packed_page"]["version"], 2);
        assert_eq!(parsed["packed_page"]["kind"], 7);
        assert_eq!(parsed["packed_page"]["distance"], "f64-le");
    }
}
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prolly::{
    AdaptiveQuality, BuildParallelism, ContentGraphLimits, ContentObjectKind, DistanceMetric,
    HierarchyConfig, HnswSearchOptions, Neighbor, OverflowConfig, PlannerPolicy, PqSearchOptions,
    ProximityConfig, ProximityFilter, ProximityMap, ProximityMembershipProof, ProximityMutation,
    ProximityMutationStats, ProximityRecord, ProximitySearchClaim, ProximitySearchProof,
    ProximitySearchStats, ProximityStructuralProof, ProximityVerification, QueryKernel,
    ScalarQuantizationConfig, SearchBackend, SearchBudget, SearchCompletion, SearchOptions,
    SearchPolicy, SearchRequest, SearchResult, TypedContentObject, TypedContentRoot,
    VectorStorageConfig,
};

use crate::{BindingEngine, KeyProofRecord, ProllyBindingError, ProllyEngine};
#[cfg(feature = "sqlite")]
use prolly_store_sqlite::SqliteStore;

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ContentObjectKindRecord {
    OrderedNode,
    ProximityDescriptor,
    ProximityNode,
    OverflowDirectory,
    OverflowPage,
    ExternalVector,
    ScalarQuantization,
    ProductQuantization,
    HnswManifest,
    HnswPage,
    CompositeAccelerator,
    AcceleratorCatalog,
}

impl From<ContentObjectKind> for ContentObjectKindRecord {
    fn from(value: ContentObjectKind) -> Self {
        match value {
            ContentObjectKind::OrderedNode => Self::OrderedNode,
            ContentObjectKind::ProximityDescriptor => Self::ProximityDescriptor,
            ContentObjectKind::ProximityNode => Self::ProximityNode,
            ContentObjectKind::OverflowDirectory => Self::OverflowDirectory,
            ContentObjectKind::OverflowPage => Self::OverflowPage,
            ContentObjectKind::ExternalVector => Self::ExternalVector,
            ContentObjectKind::ScalarQuantization => Self::ScalarQuantization,
            ContentObjectKind::ProductQuantization => Self::ProductQuantization,
            ContentObjectKind::HnswManifest => Self::HnswManifest,
            ContentObjectKind::HnswPage => Self::HnswPage,
            ContentObjectKind::CompositeAccelerator => Self::CompositeAccelerator,
            ContentObjectKind::AcceleratorCatalog => Self::AcceleratorCatalog,
        }
    }
}

impl From<ContentObjectKindRecord> for ContentObjectKind {
    fn from(value: ContentObjectKindRecord) -> Self {
        match value {
            ContentObjectKindRecord::OrderedNode => Self::OrderedNode,
            ContentObjectKindRecord::ProximityDescriptor => Self::ProximityDescriptor,
            ContentObjectKindRecord::ProximityNode => Self::ProximityNode,
            ContentObjectKindRecord::OverflowDirectory => Self::OverflowDirectory,
            ContentObjectKindRecord::OverflowPage => Self::OverflowPage,
            ContentObjectKindRecord::ExternalVector => Self::ExternalVector,
            ContentObjectKindRecord::ScalarQuantization => Self::ScalarQuantization,
            ContentObjectKindRecord::ProductQuantization => Self::ProductQuantization,
            ContentObjectKindRecord::HnswManifest => Self::HnswManifest,
            ContentObjectKindRecord::HnswPage => Self::HnswPage,
            ContentObjectKindRecord::CompositeAccelerator => Self::CompositeAccelerator,
            ContentObjectKindRecord::AcceleratorCatalog => Self::AcceleratorCatalog,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ContentGraphLimitsRecord {
    pub max_objects: u64,
    pub max_depth: u64,
    pub max_bytes: u64,
    pub max_references_per_object: u64,
}

impl ContentGraphLimitsRecord {
    pub fn defaults() -> Self {
        let value = ContentGraphLimits::default();
        Self {
            max_objects: value.max_objects as u64,
            max_depth: value.max_depth as u64,
            max_bytes: value.max_bytes as u64,
            max_references_per_object: value.max_references_per_object as u64,
        }
    }
}

impl TryFrom<ContentGraphLimitsRecord> for ContentGraphLimits {
    type Error = ProllyBindingError;

    fn try_from(value: ContentGraphLimitsRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            max_objects: to_usize(value.max_objects, "max_objects")?,
            max_depth: to_usize(value.max_depth, "max_depth")?,
            max_bytes: to_usize(value.max_bytes, "max_bytes")?,
            max_references_per_object: to_usize(
                value.max_references_per_object,
                "max_references_per_object",
            )?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TypedContentObjectRecord {
    pub kind: ContentObjectKindRecord,
    pub cid: Vec<u8>,
    pub dimensions: Option<u32>,
    pub bytes: Vec<u8>,
    pub depth: u64,
}

impl From<TypedContentObject> for TypedContentObjectRecord {
    fn from(value: TypedContentObject) -> Self {
        Self {
            kind: value.root.kind.into(),
            cid: value.root.cid.0.to_vec(),
            dimensions: value.root.dimensions,
            bytes: value.bytes,
            depth: value.depth as u64,
        }
    }
}

impl TryFrom<TypedContentObjectRecord> for TypedContentObject {
    type Error = ProllyBindingError;

    fn try_from(value: TypedContentObjectRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: TypedContentRoot {
                kind: value.kind.into(),
                cid: crate::cid_from_vec(value.cid)?,
                dimensions: value.dimensions,
            },
            bytes: value.bytes,
            depth: to_usize(value.depth, "depth")?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum DistanceMetricRecord {
    L2Squared,
    Cosine,
    InnerProduct,
}

impl From<DistanceMetricRecord> for DistanceMetric {
    fn from(value: DistanceMetricRecord) -> Self {
        match value {
            DistanceMetricRecord::L2Squared => Self::L2Squared,
            DistanceMetricRecord::Cosine => Self::Cosine,
            DistanceMetricRecord::InnerProduct => Self::InnerProduct,
        }
    }
}

impl From<DistanceMetric> for DistanceMetricRecord {
    fn from(value: DistanceMetric) -> Self {
        match value {
            DistanceMetric::L2Squared => Self::L2Squared,
            DistanceMetric::Cosine => Self::Cosine,
            DistanceMetric::InnerProduct => Self::InnerProduct,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityConfigRecord {
    pub dimensions: u32,
    pub metric: DistanceMetricRecord,
    pub log_chunk_size: u8,
    pub level_hash_seed: u64,
    pub min_page_bytes: u32,
    pub target_page_bytes: u32,
    pub max_page_bytes: u32,
    pub overflow_hash_seed: u64,
    pub inline_threshold_bytes: u32,
    pub scalar_quantization_group_size: Option<u32>,
}

impl ProximityConfigRecord {
    pub fn new(dimensions: u32) -> Self {
        ProximityConfig::new(dimensions).into()
    }
}

impl From<ProximityConfig> for ProximityConfigRecord {
    fn from(value: ProximityConfig) -> Self {
        Self {
            dimensions: value.dimensions,
            metric: value.metric.into(),
            log_chunk_size: value.hierarchy.log_chunk_size,
            level_hash_seed: value.hierarchy.level_hash_seed,
            min_page_bytes: value.overflow.min_page_bytes,
            target_page_bytes: value.overflow.target_page_bytes,
            max_page_bytes: value.overflow.max_page_bytes,
            overflow_hash_seed: value.overflow.hash_seed,
            inline_threshold_bytes: value.vector_storage.inline_threshold_bytes,
            scalar_quantization_group_size: value
                .scalar_quantization
                .map(|config| config.group_size),
        }
    }
}

impl TryFrom<ProximityConfigRecord> for ProximityConfig {
    type Error = ProllyBindingError;

    fn try_from(value: ProximityConfigRecord) -> Result<Self, Self::Error> {
        let config = Self {
            dimensions: value.dimensions,
            metric: value.metric.into(),
            hierarchy: HierarchyConfig {
                log_chunk_size: value.log_chunk_size,
                level_hash_seed: value.level_hash_seed,
            },
            overflow: OverflowConfig {
                min_page_bytes: value.min_page_bytes,
                target_page_bytes: value.target_page_bytes,
                max_page_bytes: value.max_page_bytes,
                hash_seed: value.overflow_hash_seed,
            },
            vector_storage: VectorStorageConfig {
                inline_threshold_bytes: value.inline_threshold_bytes,
            },
            scalar_quantization: value
                .scalar_quantization_group_size
                .map(|group_size| ScalarQuantizationConfig { group_size }),
        };
        config.validate()?;
        Ok(config)
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximityRecordRecord {
    pub key: Vec<u8>,
    pub vector: Vec<f32>,
    pub value: Vec<u8>,
}

#[uniffi::export(with_foreign)]
pub trait ProximityRecordVisitorCallback: Send + Sync {
    fn visit(&self, record: ProximityRecordRecord) -> bool;
}

impl From<ProximityRecordRecord> for ProximityRecord {
    fn from(value: ProximityRecordRecord) -> Self {
        Self {
            key: value.key,
            vector: value.vector,
            value: value.value,
        }
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ExactProximityRecordRecord {
    pub vector: Vec<f32>,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityMembershipProofRecord {
    pub descriptor: Vec<u8>,
    pub descriptor_bytes: Vec<u8>,
    pub directory_proof: KeyProofRecord,
    pub record_bytes: Option<Vec<u8>>,
}

impl From<ProximityMembershipProof> for ProximityMembershipProofRecord {
    fn from(value: ProximityMembershipProof) -> Self {
        Self {
            descriptor: value.descriptor.0.to_vec(),
            descriptor_bytes: value.descriptor_bytes,
            directory_proof: value.directory_proof.into(),
            record_bytes: value.record_bytes,
        }
    }
}

impl TryFrom<ProximityMembershipProofRecord> for ProximityMembershipProof {
    type Error = ProllyBindingError;

    fn try_from(value: ProximityMembershipProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            descriptor: crate::cid_from_vec(value.descriptor)?,
            descriptor_bytes: value.descriptor_bytes,
            directory_proof: value.directory_proof.try_into()?,
            record_bytes: value.record_bytes,
        })
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximityMembershipVerificationRecord {
    pub descriptor: Vec<u8>,
    pub key: Vec<u8>,
    pub record: Option<ExactProximityRecordRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityStructuralProofRecord {
    pub descriptor: Vec<u8>,
    pub objects: Vec<TypedContentObjectRecord>,
}

impl From<ProximityStructuralProof> for ProximityStructuralProofRecord {
    fn from(value: ProximityStructuralProof) -> Self {
        Self {
            descriptor: value.descriptor.0.to_vec(),
            objects: value.objects.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<ProximityStructuralProofRecord> for ProximityStructuralProof {
    type Error = ProllyBindingError;

    fn try_from(value: ProximityStructuralProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            descriptor: crate::cid_from_vec(value.descriptor)?,
            objects: value
                .objects
                .into_iter()
                .map(TryInto::try_into)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximityMutationRecord {
    pub key: Vec<u8>,
    pub vector: Option<Vec<f32>>,
    pub value: Option<Vec<u8>>,
}

fn proximity_mutation(
    value: ProximityMutationRecord,
) -> Result<ProximityMutation, ProllyBindingError> {
    let pair = match (value.vector, value.value) {
        (Some(vector), Some(value)) => Some((vector, value)),
        (None, None) => None,
        _ => {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "proximity mutation vector and value must both be present or absent"
                    .to_string(),
            })
        }
    };
    Ok(ProximityMutation {
        key: value.key,
        value: pair,
    })
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum AdaptiveQualityRecord {
    Fast,
    Balanced,
    HighRecall,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SearchPolicyKind {
    Exact,
    FixedBudget,
    Adaptive,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SearchBackendRecord {
    Native,
    ProductQuantized,
    Hnsw,
    Composite,
    Auto,
}

impl From<SearchBackendRecord> for SearchBackend {
    fn from(value: SearchBackendRecord) -> Self {
        match value {
            SearchBackendRecord::Native => Self::Native,
            SearchBackendRecord::ProductQuantized => Self::ProductQuantized,
            SearchBackendRecord::Hnsw => Self::Hnsw,
            SearchBackendRecord::Composite => Self::Composite,
            SearchBackendRecord::Auto => Self::Auto,
        }
    }
}

impl From<SearchBackend> for SearchBackendRecord {
    fn from(value: SearchBackend) -> Self {
        match value {
            SearchBackend::Native => Self::Native,
            SearchBackend::ProductQuantized => Self::ProductQuantized,
            SearchBackend::Hnsw => Self::Hnsw,
            SearchBackend::Composite => Self::Composite,
            SearchBackend::Auto => Self::Auto,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum QueryKernelRecord {
    ScalarDeterministic,
    SimdDeterministic,
    AutoDeterministic,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ProximityFilterKind {
    All,
    KeyRange,
    Prefix,
    EligibleKeys,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityFilterRecord {
    pub kind: ProximityFilterKind,
    pub start: Option<Vec<u8>>,
    pub range_end: Option<Vec<u8>>,
    pub prefix: Option<Vec<u8>>,
    pub eligible_keys: Vec<Vec<u8>>,
}

impl Default for ProximityFilterRecord {
    fn default() -> Self {
        Self {
            kind: ProximityFilterKind::All,
            start: None,
            range_end: None,
            prefix: None,
            eligible_keys: Vec::new(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct SearchBudgetRecord {
    pub max_nodes: Option<u64>,
    pub max_committed_bytes: Option<u64>,
    pub max_distance_evaluations: Option<u64>,
    pub max_frontier_entries: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximitySearchRequestRecord {
    pub query: Vec<f32>,
    pub k: u64,
    pub policy: SearchPolicyKind,
    pub adaptive_quality: Option<AdaptiveQualityRecord>,
    pub budget: SearchBudgetRecord,
    pub filter: ProximityFilterRecord,
    pub kernel: QueryKernelRecord,
    pub backend: SearchBackendRecord,
    pub hnsw_ef_search: Option<u32>,
    pub pq_rerank_multiplier: Option<u16>,
}

impl ProximitySearchRequestRecord {
    pub fn exact(query: Vec<f32>, k: u64) -> Self {
        Self {
            query,
            k,
            policy: SearchPolicyKind::Exact,
            adaptive_quality: None,
            budget: SearchBudgetRecord::default(),
            filter: ProximityFilterRecord::default(),
            kernel: QueryKernelRecord::AutoDeterministic,
            backend: SearchBackendRecord::Native,
            hnsw_ef_search: None,
            pq_rerank_multiplier: None,
        }
    }
}

fn to_usize(value: u64, field: &str) -> Result<usize, ProllyBindingError> {
    usize::try_from(value).map_err(|_| ProllyBindingError::InvalidArgument {
        reason: format!("{field} does not fit this platform"),
    })
}

fn optional_usize(value: Option<u64>, field: &str) -> Result<Option<usize>, ProllyBindingError> {
    value.map(|value| to_usize(value, field)).transpose()
}

fn proximity_search_request(
    request: &ProximitySearchRequestRecord,
) -> Result<SearchRequest<'_>, ProllyBindingError> {
    let k = to_usize(request.k, "k")?;
    let policy = match request.policy {
        SearchPolicyKind::Exact => SearchPolicy::Exact,
        SearchPolicyKind::FixedBudget => SearchPolicy::FixedBudget,
        SearchPolicyKind::Adaptive => SearchPolicy::Adaptive(
            match request
                .adaptive_quality
                .ok_or_else(|| ProllyBindingError::InvalidArgument {
                    reason: "adaptive search requires adaptive_quality".to_string(),
                })? {
                AdaptiveQualityRecord::Fast => AdaptiveQuality::Fast,
                AdaptiveQualityRecord::Balanced => AdaptiveQuality::Balanced,
                AdaptiveQualityRecord::HighRecall => AdaptiveQuality::HighRecall,
            },
        ),
    };
    let budget = SearchBudget {
        max_nodes: optional_usize(request.budget.max_nodes, "max_nodes")?,
        max_committed_bytes: optional_usize(
            request.budget.max_committed_bytes,
            "max_committed_bytes",
        )?,
        max_distance_evaluations: optional_usize(
            request.budget.max_distance_evaluations,
            "max_distance_evaluations",
        )?,
        max_frontier_entries: optional_usize(
            request.budget.max_frontier_entries,
            "max_frontier_entries",
        )?,
    };
    let filter = match &request.filter.kind {
        ProximityFilterKind::All => ProximityFilter::All,
        ProximityFilterKind::KeyRange => ProximityFilter::KeyRange {
            start: request.filter.start.as_deref(),
            end: request.filter.range_end.as_deref(),
        },
        ProximityFilterKind::Prefix => {
            ProximityFilter::Prefix(request.filter.prefix.as_deref().ok_or_else(|| {
                ProllyBindingError::InvalidArgument {
                    reason: "prefix filter requires prefix".to_string(),
                }
            })?)
        }
        ProximityFilterKind::EligibleKeys => {
            ProximityFilter::EligibleKeys(&request.filter.eligible_keys)
        }
    };
    let kernel = match request.kernel {
        QueryKernelRecord::ScalarDeterministic => QueryKernel::ScalarDeterministic,
        QueryKernelRecord::SimdDeterministic => QueryKernel::SimdDeterministic,
        QueryKernelRecord::AutoDeterministic => QueryKernel::AutoDeterministic,
    };
    Ok(SearchRequest {
        query: &request.query,
        k,
        policy,
        budget,
        filter,
        kernel,
        options: SearchOptions {
            backend: request.backend.into(),
            planner: PlannerPolicy::default(),
            hnsw: HnswSearchOptions {
                ef_search: request.hnsw_ef_search,
            },
            pq: PqSearchOptions {
                rerank_multiplier: request.pq_rerank_multiplier,
            },
        },
    })
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximityNeighborRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub distance: f64,
}

impl From<Neighbor> for ProximityNeighborRecord {
    fn from(value: Neighbor) -> Self {
        Self {
            key: value.key,
            value: value.value,
            distance: value.distance,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SearchCompletionRecord {
    Exact,
    ApproximatePolicySatisfied,
    BudgetExhausted,
    Cancelled,
    DeadlineExceeded,
}

impl From<SearchCompletion> for SearchCompletionRecord {
    fn from(value: SearchCompletion) -> Self {
        match value {
            SearchCompletion::Exact => Self::Exact,
            SearchCompletion::ApproximatePolicySatisfied => Self::ApproximatePolicySatisfied,
            SearchCompletion::BudgetExhausted => Self::BudgetExhausted,
            SearchCompletion::Cancelled => Self::Cancelled,
            SearchCompletion::DeadlineExceeded => Self::DeadlineExceeded,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximitySearchStatsRecord {
    pub levels_visited: u64,
    pub nodes_read: u64,
    pub bytes_read: u64,
    pub physical_bytes_read: u64,
    pub committed_bytes: u64,
    pub distance_evaluations: u64,
    pub quantized_distance_evaluations: u64,
    pub reranked_candidates: u64,
    pub frontier_peak: u64,
    pub candidate_handles_peak: u64,
    pub candidate_retained_bytes_peak: u64,
}

impl From<ProximitySearchStats> for ProximitySearchStatsRecord {
    fn from(value: ProximitySearchStats) -> Self {
        Self {
            levels_visited: value.levels_visited as u64,
            nodes_read: value.nodes_read as u64,
            bytes_read: value.bytes_read as u64,
            physical_bytes_read: value.physical_bytes_read as u64,
            committed_bytes: value.committed_bytes as u64,
            distance_evaluations: value.distance_evaluations as u64,
            quantized_distance_evaluations: value.quantized_distance_evaluations as u64,
            reranked_candidates: value.reranked_candidates as u64,
            frontier_peak: value.frontier_peak as u64,
            candidate_handles_peak: value.candidate_handles_peak as u64,
            candidate_retained_bytes_peak: value.candidate_retained_bytes_peak as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximitySearchResultRecord {
    pub neighbors: Vec<ProximityNeighborRecord>,
    pub stats: ProximitySearchStatsRecord,
    pub completion: SearchCompletionRecord,
    pub backend: SearchBackendRecord,
    pub plan_format_version: u8,
}

impl From<SearchResult> for ProximitySearchResultRecord {
    fn from(value: SearchResult) -> Self {
        Self {
            neighbors: value.neighbors.into_iter().map(Into::into).collect(),
            stats: value.stats.into(),
            completion: value.completion.into(),
            backend: value.plan.backend.into(),
            plan_format_version: value.plan.format_version,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ProximitySearchClaimKindRecord {
    ExactL2Optimal,
    HonestExecution,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximitySearchClaimRecord {
    pub kind: ProximitySearchClaimKindRecord,
    pub terminal_lower_bound: Option<f64>,
}

impl From<ProximitySearchClaim> for ProximitySearchClaimRecord {
    fn from(value: ProximitySearchClaim) -> Self {
        match value {
            ProximitySearchClaim::ExactL2Optimal {
                terminal_lower_bound,
            } => Self {
                kind: ProximitySearchClaimKindRecord::ExactL2Optimal,
                terminal_lower_bound: Some(terminal_lower_bound),
            },
            ProximitySearchClaim::HonestExecution => Self {
                kind: ProximitySearchClaimKindRecord::HonestExecution,
                terminal_lower_bound: None,
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct ProximitySearchVerificationRecord {
    pub result: ProximitySearchResultRecord,
    pub claim: ProximitySearchClaimRecord,
    pub replayed_events: u64,
}

#[derive(uniffi::Object)]
pub struct BindingProximitySearchProof {
    inner: ProximitySearchProof,
}

#[uniffi::export]
impl BindingProximitySearchProof {
    pub fn source_descriptor(&self) -> Vec<u8> {
        self.inner.source.descriptor.0.to_vec()
    }

    pub fn verify(
        &self,
        expected_descriptor: Option<Vec<u8>>,
        limits: ContentGraphLimitsRecord,
    ) -> Result<ProximitySearchVerificationRecord, ProllyBindingError> {
        let limits = ContentGraphLimits::try_from(limits)?;
        let verified = match expected_descriptor {
            Some(expected) => self
                .inner
                .verify_for_source(&crate::cid_from_vec(expected)?, &limits)?,
            None => self.inner.verify(&limits)?,
        };
        Ok(ProximitySearchVerificationRecord {
            result: verified.result.into(),
            claim: verified.claim.into(),
            replayed_events: verified.replayed_events as u64,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityMutationStatsRecord {
    pub directory_entries_scanned: u64,
    pub directory_nodes_read: u64,
    pub directory_nodes_rebuilt: u64,
    pub directory_nodes_written: u64,
    pub directory_nodes_reused: u64,
    pub directory_levels_rebuilt: u64,
    pub directory_right_edge_rebuilt: bool,
    pub nodes_read: u64,
    pub nodes_written: u64,
    pub nodes_reused: u64,
    pub records_rebuilt: u64,
    pub distance_evaluations: u64,
    pub full_proximity_rebuild: bool,
}

impl From<ProximityMutationStats> for ProximityMutationStatsRecord {
    fn from(value: ProximityMutationStats) -> Self {
        Self {
            directory_entries_scanned: value.directory_entries_scanned as u64,
            directory_nodes_read: value.directory_nodes_read as u64,
            directory_nodes_rebuilt: value.directory_nodes_rebuilt as u64,
            directory_nodes_written: value.directory_nodes_written as u64,
            directory_nodes_reused: value.directory_nodes_reused as u64,
            directory_levels_rebuilt: value.directory_levels_rebuilt as u64,
            directory_right_edge_rebuilt: value.directory_right_edge_rebuilt,
            nodes_read: value.nodes_read as u64,
            nodes_written: value.nodes_written as u64,
            nodes_reused: value.nodes_reused as u64,
            records_rebuilt: value.records_rebuilt as u64,
            distance_evaluations: value.distance_evaluations as u64,
            full_proximity_rebuild: value.full_proximity_rebuild,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityVerificationRecord {
    pub record_count: u64,
    pub proximity_node_count: u64,
    pub external_vector_count: u64,
    pub quantized_node_count: u64,
    pub scalar_quantizer_count: u64,
    pub overflow_page_count: u64,
    pub overflow_directory_count: u64,
    pub maximum_level: u8,
    pub maximum_node_bytes: u64,
    pub distance_checks: u64,
}

impl From<ProximityVerification> for ProximityVerificationRecord {
    fn from(value: ProximityVerification) -> Self {
        Self {
            record_count: value.record_count,
            proximity_node_count: value.proximity_node_count as u64,
            external_vector_count: value.external_vector_count as u64,
            quantized_node_count: value.quantized_node_count as u64,
            scalar_quantizer_count: value.scalar_quantizer_count as u64,
            overflow_page_count: value.overflow_page_count as u64,
            overflow_directory_count: value.overflow_directory_count as u64,
            maximum_level: value.maximum_level,
            maximum_node_bytes: value.maximum_node_bytes as u64,
            distance_checks: value.distance_checks as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProximityStructuralVerificationRecord {
    pub descriptor: Vec<u8>,
    pub object_count: u64,
    pub summary: ProximityVerificationRecord,
}

macro_rules! with_proximity_map {
    ($self:expr, $map:ident, $body:block) => {{
        let descriptor = crate::cid_from_vec($self.descriptor.clone())?;
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let $map = ProximityMap::load(engine.store().clone(), descriptor)?;
                $body
            }
            BindingEngine::File(engine) => {
                let $map = ProximityMap::load(engine.store().clone(), descriptor)?;
                $body
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let $map = ProximityMap::load(engine.store().clone(), descriptor)?;
                $body
            }
            BindingEngine::Host(engine) => {
                let $map = ProximityMap::load(engine.store().clone(), descriptor)?;
                $body
            }
        }
    }};
}

enum BindingProximitySessionMap {
    Memory(ProximityMap<Arc<prolly::MemStore>>),
    File(ProximityMap<Arc<prolly::FileNodeStore>>),
    #[cfg(feature = "sqlite")]
    Sqlite(ProximityMap<Arc<SqliteStore>>),
    Host(ProximityMap<Arc<crate::HostStore>>),
}

macro_rules! with_proximity_session {
    ($self:expr, $map:ident, $body:block) => {{
        match &$self.inner {
            BindingProximitySessionMap::Memory($map) => $body,
            BindingProximitySessionMap::File($map) => $body,
            #[cfg(feature = "sqlite")]
            BindingProximitySessionMap::Sqlite($map) => $body,
            BindingProximitySessionMap::Host($map) => $body,
        }
    }};
}

#[derive(uniffi::Object)]
pub struct BindingProximityReadSession {
    inner: BindingProximitySessionMap,
    fast_handle: AtomicU64,
}

#[uniffi::export]
impl BindingProximityReadSession {
    pub fn fast_handle(&self) -> u64 {
        self.fast_handle.load(Ordering::Acquire)
    }

    pub fn get(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<ExactProximityRecordRecord>, ProllyBindingError> {
        with_proximity_session!(self, map, {
            Ok(map
                .get(&key)?
                .map(|(vector, value)| ExactProximityRecordRecord { vector, value }))
        })
    }

    pub fn contains_key(&self, key: Vec<u8>) -> Result<bool, ProllyBindingError> {
        with_proximity_session!(self, map, { map.contains_key(&key).map_err(Into::into) })
    }

    pub fn search(
        &self,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        let request = proximity_search_request(&request)?;
        with_proximity_session!(self, map, {
            map.search(request).map(Into::into).map_err(Into::into)
        })
    }

    pub fn scan_records(
        &self,
        visitor: Arc<dyn ProximityRecordVisitorCallback>,
    ) -> Result<u64, ProllyBindingError> {
        with_proximity_session!(self, map, {
            let outcome = map.scan_records_until(|key, record| {
                if visitor.visit(ProximityRecordRecord {
                    key: key.to_vec(),
                    vector: record.vector.to_vec(),
                    value: record.value.to_vec(),
                }) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            })?;
            Ok(outcome.visited)
        })
    }
}

impl Drop for BindingProximityReadSession {
    fn drop(&mut self) {
        crate::fast_abi::unregister_proximity_map(self.fast_handle.load(Ordering::Relaxed));
    }
}

#[derive(uniffi::Object)]
pub struct BindingProximityMap {
    engine: Arc<ProllyEngine>,
    descriptor: Vec<u8>,
    fast_handle: AtomicU64,
}

impl BindingProximityMap {
    fn from_descriptor(engine: Arc<ProllyEngine>, descriptor: prolly::Cid) -> Arc<Self> {
        let map = Arc::new(Self {
            engine,
            descriptor: descriptor.0.to_vec(),
            fast_handle: AtomicU64::new(0),
        });
        let handle = crate::fast_abi::register_proximity_map(&map);
        map.fast_handle.store(handle, Ordering::Release);
        map
    }
}

#[derive(Clone, uniffi::Record)]
pub struct ProximityMutationResultRecord {
    pub map: Arc<BindingProximityMap>,
    pub stats: ProximityMutationStatsRecord,
}

#[uniffi::export]
impl BindingProximityMap {
    pub fn descriptor(&self) -> Vec<u8> {
        self.descriptor.clone()
    }

    pub fn fast_handle(&self) -> u64 {
        self.fast_handle.load(Ordering::Acquire)
    }

    pub fn read_session(&self) -> Result<Arc<BindingProximityReadSession>, ProllyBindingError> {
        let descriptor = crate::cid_from_vec(self.descriptor.clone())?;
        let inner =
            match &self.engine.inner {
                BindingEngine::Memory(engine) => BindingProximitySessionMap::Memory(
                    ProximityMap::load(engine.store().clone(), descriptor)?,
                ),
                BindingEngine::File(engine) => BindingProximitySessionMap::File(
                    ProximityMap::load(engine.store().clone(), descriptor)?,
                ),
                #[cfg(feature = "sqlite")]
                BindingEngine::Sqlite(engine) => BindingProximitySessionMap::Sqlite(
                    ProximityMap::load(engine.store().clone(), descriptor)?,
                ),
                BindingEngine::Host(engine) => BindingProximitySessionMap::Host(
                    ProximityMap::load(engine.store().clone(), descriptor)?,
                ),
            };
        let session = Arc::new(BindingProximityReadSession {
            inner,
            fast_handle: AtomicU64::new(0),
        });
        let handle = crate::fast_abi::register_proximity_session(&session);
        session.fast_handle.store(handle, Ordering::Release);
        Ok(session)
    }

    pub fn count(&self) -> Result<u64, ProllyBindingError> {
        with_proximity_map!(self, map, { Ok(map.tree().count) })
    }

    pub fn config(&self) -> Result<ProximityConfigRecord, ProllyBindingError> {
        with_proximity_map!(self, map, { Ok(map.tree().config.clone().into()) })
    }

    pub fn get(
        &self,
        key: Vec<u8>,
    ) -> Result<Option<ExactProximityRecordRecord>, ProllyBindingError> {
        with_proximity_map!(self, map, {
            Ok(map
                .get(&key)?
                .map(|(vector, value)| ExactProximityRecordRecord { vector, value }))
        })
    }

    pub fn contains_key(&self, key: Vec<u8>) -> Result<bool, ProllyBindingError> {
        with_proximity_map!(self, map, { map.contains_key(&key).map_err(Into::into) })
    }

    pub fn scan_records(
        &self,
        visitor: Arc<dyn ProximityRecordVisitorCallback>,
    ) -> Result<u64, ProllyBindingError> {
        with_proximity_map!(self, map, {
            let outcome = map.scan_records_until(|key, record| {
                if visitor.visit(ProximityRecordRecord {
                    key: key.to_vec(),
                    vector: record.vector.to_vec(),
                    value: record.value.to_vec(),
                }) {
                    ControlFlow::Continue(())
                } else {
                    ControlFlow::Break(())
                }
            })?;
            Ok(outcome.visited)
        })
    }

    pub fn clear_content_cache(&self) -> Result<(), ProllyBindingError> {
        with_proximity_map!(self, map, { map.clear_content_cache().map_err(Into::into) })
    }

    pub fn search(
        &self,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        let request = proximity_search_request(&request)?;
        with_proximity_map!(self, map, {
            map.search(request).map(Into::into).map_err(Into::into)
        })
    }

    pub fn prove_search(
        &self,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord,
    ) -> Result<Arc<BindingProximitySearchProof>, ProllyBindingError> {
        let limits = ContentGraphLimits::try_from(limits)?;
        let request = proximity_search_request(&request)?;
        with_proximity_map!(self, map, {
            Ok(Arc::new(BindingProximitySearchProof {
                inner: map.prove_search(request, &limits)?,
            }))
        })
    }

    pub fn mutate(
        &self,
        mutations: Vec<ProximityMutationRecord>,
    ) -> Result<ProximityMutationResultRecord, ProllyBindingError> {
        let mutations = mutations
            .into_iter()
            .map(proximity_mutation)
            .collect::<Result<Vec<_>, _>>()?;
        with_proximity_map!(self, map, {
            let (updated, stats) = map.mutate_batch(mutations)?;
            Ok(ProximityMutationResultRecord {
                map: Self::from_descriptor(self.engine.clone(), updated.tree().descriptor.clone()),
                stats: stats.into(),
            })
        })
    }

    pub fn rebuild(
        &self,
        mutations: Vec<ProximityMutationRecord>,
    ) -> Result<Arc<BindingProximityMap>, ProllyBindingError> {
        let mutations = mutations
            .into_iter()
            .map(proximity_mutation)
            .collect::<Result<Vec<_>, _>>()?;
        with_proximity_map!(self, map, {
            let updated = map.rebuild_batch(mutations)?;
            Ok(Self::from_descriptor(
                self.engine.clone(),
                updated.tree().descriptor.clone(),
            ))
        })
    }

    pub fn prove_membership(
        &self,
        key: Vec<u8>,
    ) -> Result<ProximityMembershipProofRecord, ProllyBindingError> {
        with_proximity_map!(self, map, {
            map.prove_membership(&key)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn prove_structure(
        &self,
        limits: ContentGraphLimitsRecord,
    ) -> Result<ProximityStructuralProofRecord, ProllyBindingError> {
        let limits = ContentGraphLimits::try_from(limits)?;
        with_proximity_map!(self, map, {
            map.prove_structure(&limits)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn verify(&self) -> Result<ProximityVerificationRecord, ProllyBindingError> {
        with_proximity_map!(self, map, {
            map.verify().map(Into::into).map_err(Into::into)
        })
    }
}

#[uniffi::export]
pub fn verify_proximity_membership_proof(
    proof: ProximityMembershipProofRecord,
    expected_descriptor: Option<Vec<u8>>,
) -> Result<ProximityMembershipVerificationRecord, ProllyBindingError> {
    let proof = ProximityMembershipProof::try_from(proof)?;
    let verified = match expected_descriptor {
        Some(expected) => proof.verify_for(&crate::cid_from_vec(expected)?)?,
        None => proof.verify()?,
    };
    Ok(ProximityMembershipVerificationRecord {
        descriptor: verified.descriptor.0.to_vec(),
        key: verified.key,
        record: verified
            .record
            .map(|(vector, value)| ExactProximityRecordRecord { vector, value }),
    })
}

#[uniffi::export]
pub fn verify_proximity_structure_proof(
    proof: ProximityStructuralProofRecord,
    expected_descriptor: Option<Vec<u8>>,
    limits: ContentGraphLimitsRecord,
) -> Result<ProximityStructuralVerificationRecord, ProllyBindingError> {
    let proof = ProximityStructuralProof::try_from(proof)?;
    let limits = ContentGraphLimits::try_from(limits)?;
    let verified = match expected_descriptor {
        Some(expected) => proof.verify_for(&crate::cid_from_vec(expected)?, &limits)?,
        None => proof.verify(&limits)?,
    };
    Ok(ProximityStructuralVerificationRecord {
        descriptor: verified.descriptor.0.to_vec(),
        object_count: verified.object_count as u64,
        summary: verified.summary.into(),
    })
}

impl Drop for BindingProximityMap {
    fn drop(&mut self) {
        crate::fast_abi::unregister_proximity_map(self.fast_handle.load(Ordering::Relaxed));
    }
}

pub(crate) fn build_proximity_map(
    engine: Arc<ProllyEngine>,
    config: ProximityConfigRecord,
    records: Vec<ProximityRecordRecord>,
    threads: Option<u64>,
) -> Result<Arc<BindingProximityMap>, ProllyBindingError> {
    let config = ProximityConfig::try_from(config)?;
    let records = records.into_iter().map(Into::into).collect::<Vec<_>>();
    let parallelism = BuildParallelism::new(
        threads
            .map(|threads| to_usize(threads, "threads"))
            .transpose()?
            .unwrap_or(1),
    )?;
    let descriptor = match &engine.inner {
        BindingEngine::Memory(inner) => ProximityMap::build_with_parallelism(
            inner.store().clone(),
            config,
            records,
            parallelism,
        )?
        .0
        .tree()
        .descriptor
        .clone(),
        BindingEngine::File(inner) => ProximityMap::build_with_parallelism(
            inner.store().clone(),
            config,
            records,
            parallelism,
        )?
        .0
        .tree()
        .descriptor
        .clone(),
        #[cfg(feature = "sqlite")]
        BindingEngine::Sqlite(inner) => ProximityMap::build_with_parallelism(
            inner.store().clone(),
            config,
            records,
            parallelism,
        )?
        .0
        .tree()
        .descriptor
        .clone(),
        BindingEngine::Host(inner) => ProximityMap::build_with_parallelism(
            inner.store().clone(),
            config,
            records,
            parallelism,
        )?
        .0
        .tree()
        .descriptor
        .clone(),
    };
    Ok(BindingProximityMap::from_descriptor(engine, descriptor))
}

pub(crate) fn load_proximity_map(
    engine: Arc<ProllyEngine>,
    descriptor: Vec<u8>,
) -> Result<Arc<BindingProximityMap>, ProllyBindingError> {
    let descriptor = crate::cid_from_vec(descriptor)?;
    match &engine.inner {
        BindingEngine::Memory(inner) => {
            ProximityMap::load(inner.store().clone(), descriptor.clone())?;
        }
        BindingEngine::File(inner) => {
            ProximityMap::load(inner.store().clone(), descriptor.clone())?;
        }
        #[cfg(feature = "sqlite")]
        BindingEngine::Sqlite(inner) => {
            ProximityMap::load(inner.store().clone(), descriptor.clone())?;
        }
        BindingEngine::Host(inner) => {
            ProximityMap::load(inner.store().clone(), descriptor.clone())?;
        }
    }
    Ok(BindingProximityMap::from_descriptor(engine, descriptor))
}

#[uniffi::export]
pub fn default_proximity_config(dimensions: u32) -> ProximityConfigRecord {
    ProximityConfigRecord::new(dimensions)
}

#[uniffi::export]
pub fn default_content_graph_limits() -> ContentGraphLimitsRecord {
    ContentGraphLimitsRecord::defaults()
}

#[uniffi::export]
pub fn exact_proximity_search_request(query: Vec<f32>, k: u64) -> ProximitySearchRequestRecord {
    ProximitySearchRequestRecord::exact(query, k)
}
