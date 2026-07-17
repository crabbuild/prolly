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

    #[test]
    fn hnsw_accelerator_lifecycle_is_portable_and_source_bound() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let records = (0..16)
            .map(|index| ProximityRecordRecord {
                key: format!("vector-{index:02}").into_bytes(),
                vector: vec![index as f32, 0.0],
                value: format!("value-{index:02}").into_bytes(),
            })
            .collect();
        let map = engine
            .build_proximity_map(ProximityConfigRecord::new(2), records, None)
            .unwrap();
        let built = map
            .build_hnsw(default_hnsw_config(), default_hnsw_build_limits())
            .unwrap();
        assert_eq!(built.stats.records, 16);
        assert!(built.index.is_canonical());
        assert_eq!(built.index.source_descriptor(), map.descriptor());

        let mut request = ProximitySearchRequestRecord::exact(vec![0.0, 0.0], 3);
        request.policy = SearchPolicyKind::FixedBudget;
        request.backend = SearchBackendRecord::Hnsw;
        let result = built.index.search(map.clone(), request.clone()).unwrap();
        assert_eq!(result.backend, SearchBackendRecord::Hnsw);
        assert_eq!(result.neighbors[0].key, b"vector-00");
        let proof = built
            .index
            .prove_search(map.clone(), request, ContentGraphLimitsRecord::defaults())
            .unwrap();
        assert_eq!(
            proof
                .verify(Some(map.descriptor()), ContentGraphLimitsRecord::defaults())
                .unwrap()
                .result
                .backend,
            SearchBackendRecord::Hnsw
        );
        let loaded = map.load_hnsw(built.index.manifest()).unwrap();
        assert_eq!(loaded.manifest(), built.index.manifest());
    }

    #[test]
    fn pq_accelerator_lifecycle_is_portable_bounded_and_source_bound() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let records = (0..16)
            .map(|index| ProximityRecordRecord {
                key: format!("vector-{index:02}").into_bytes(),
                vector: vec![index as f32, (index % 3) as f32, 0.0, 1.0],
                value: format!("value-{index:02}").into_bytes(),
            })
            .collect();
        let map = engine
            .build_proximity_map(ProximityConfigRecord::new(4), records, None)
            .unwrap();
        let config = ProductQuantizationConfigRecord {
            subquantizers: 2,
            centroids_per_subquantizer: 4,
            training_iterations: 2,
            rerank_multiplier: 4,
            seed: u64::MAX,
            max_training_vectors: 16,
        };
        let built = map
            .build_pq(config.clone(), 2, default_pq_build_limits())
            .unwrap();
        assert_eq!(built.stats.encoded_vectors, 16);
        assert_eq!(built.index.config(), config);
        assert_eq!(built.index.source_descriptor(), map.descriptor());
        assert!(built.index.quality().mean_squared_error.is_finite());

        let mut request = ProximitySearchRequestRecord::exact(vec![0.0, 0.0, 0.0, 1.0], 3);
        request.policy = SearchPolicyKind::FixedBudget;
        request.backend = SearchBackendRecord::ProductQuantized;
        let result = built.index.search(map.clone(), request.clone()).unwrap();
        assert_eq!(result.backend, SearchBackendRecord::ProductQuantized);
        assert_eq!(result.neighbors[0].key, b"vector-00");
        let proof = built
            .index
            .prove_search(map.clone(), request, ContentGraphLimitsRecord::defaults())
            .unwrap();
        assert_eq!(
            proof
                .verify(Some(map.descriptor()), ContentGraphLimitsRecord::defaults())
                .unwrap()
                .result
                .backend,
            SearchBackendRecord::ProductQuantized
        );
        let loaded = map.load_pq(built.index.manifest()).unwrap();
        assert_eq!(loaded.manifest(), built.index.manifest());
    }

    #[test]
    fn composite_and_catalog_lifecycle_is_portable_bounded_and_source_bound() {
        let engine = Arc::new(ProllyEngine::memory(default_config()).unwrap());
        let records = (0..16)
            .map(|index| ProximityRecordRecord {
                key: format!("vector-{index:02}").into_bytes(),
                vector: vec![index as f32, 0.0],
                value: format!("value-{index:02}").into_bytes(),
            })
            .collect();
        let base = engine
            .build_proximity_map(ProximityConfigRecord::new(2), records, None)
            .unwrap();
        let hnsw = base
            .build_hnsw(default_hnsw_config(), default_hnsw_build_limits())
            .unwrap()
            .index;
        let current = base
            .mutate(vec![ProximityMutationRecord {
                key: b"vector-00".to_vec(),
                vector: Some(vec![0.25, 0.0]),
                value: Some(b"updated".to_vec()),
            }])
            .unwrap()
            .map;
        let outcome = current
            .build_composite_hnsw(
                base.clone(),
                hnsw.clone(),
                default_composite_accelerator_config(),
                default_composite_build_limits(),
            )
            .unwrap();
        assert!(outcome.reasons.is_empty());
        assert_eq!(outcome.stats.vector_updated_records, 1);
        let composite = outcome.accelerator.unwrap();
        assert_eq!(composite.current_source_descriptor(), current.descriptor());
        assert_eq!(composite.base_source_descriptor(), base.descriptor());
        assert_eq!(composite.base_kind(), CompositeBaseKindRecord::Hnsw);
        assert_eq!(composite.delta_count(), 1);
        assert_eq!(composite.shadow_count(), 1);

        let mut request = ProximitySearchRequestRecord::exact(vec![0.0, 0.0], 3);
        request.policy = SearchPolicyKind::FixedBudget;
        request.backend = SearchBackendRecord::Composite;
        assert_eq!(
            composite
                .search(current.clone(), request.clone())
                .unwrap()
                .backend,
            SearchBackendRecord::Composite
        );
        let proof = composite
            .prove_search(
                current.clone(),
                request.clone(),
                ContentGraphLimitsRecord::defaults(),
            )
            .unwrap();
        assert_eq!(
            proof
                .verify(
                    Some(current.descriptor()),
                    ContentGraphLimitsRecord::defaults(),
                )
                .unwrap()
                .result
                .backend,
            SearchBackendRecord::Composite
        );
        let loaded_composite = current.load_composite(composite.manifest()).unwrap();
        assert_eq!(loaded_composite.manifest(), composite.manifest());

        let catalog = current
            .build_accelerator_catalog(None, None, Some(composite.clone()))
            .unwrap();
        assert_eq!(catalog.source_descriptor(), current.descriptor());
        assert_eq!(catalog.entries().len(), 1);
        assert_eq!(
            catalog.entries()[0].kind,
            CatalogAcceleratorKindRecord::Composite
        );
        assert_eq!(
            catalog
                .search(current.clone(), request.clone())
                .unwrap()
                .backend,
            SearchBackendRecord::Composite
        );
        assert_eq!(
            catalog
                .prove_search(
                    current.clone(),
                    request,
                    ContentGraphLimitsRecord::defaults(),
                )
                .unwrap()
                .verify(
                    Some(current.descriptor()),
                    ContentGraphLimitsRecord::defaults(),
                )
                .unwrap()
                .result
                .backend,
            SearchBackendRecord::Composite
        );
        let loaded_catalog = current
            .load_accelerator_catalog(catalog.manifest())
            .unwrap();
        assert_eq!(loaded_catalog.manifest(), catalog.manifest());

        let mut forced = default_composite_accelerator_config();
        forced.max_delta_records = 0;
        let rebuilt = current
            .build_or_rebuild_composite_hnsw(
                base,
                hnsw,
                forced,
                default_composite_build_limits(),
                default_composite_rebuild_options(),
            )
            .unwrap();
        assert_eq!(rebuilt.kind, CompositeBuildOrRebuildKindRecord::HnswRebuilt);
        assert!(rebuilt.hnsw.is_some());
        assert!(!rebuilt.reasons.is_empty());
    }
}
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use prolly::{
    AcceleratorCatalog, AcceleratorCatalogEntry, AcceleratorSet, AdaptiveQuality, BuildParallelism,
    CatalogAcceleratorKind, CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase,
    CompositeBaseKind, CompositeBuildLimits, CompositeBuildOrRebuildOutcome, CompositeBuildOutcome,
    CompositeBuildStats, CompositeRebuildOptions, ContentGraphLimits, ContentObjectKind,
    DistanceMetric, FullRebuildReason, HierarchyConfig, HnswBuildLimits, HnswBuildStats,
    HnswConfig, HnswIndex, HnswRoutingVectorEncoding, HnswSearchOptions, Neighbor, OverflowConfig,
    PlannerPolicy, PqSearchOptions, ProductQuantizationBuildLimits, ProductQuantizationBuildStats,
    ProductQuantizationConfig, ProductQuantizationQuality, ProductQuantizer, ProximityConfig,
    ProximityFilter, ProximityMap, ProximityMembershipProof, ProximityMutation,
    ProximityMutationStats, ProximityRecord, ProximitySearchClaim, ProximitySearchProof,
    ProximitySearchStats, ProximityStructuralProof, ProximityVerification, QueryKernel,
    ScalarQuantizationConfig, SearchBackend, SearchBudget, SearchCompletion, SearchIo,
    SearchOptions, SearchPolicy, SearchRequest, SearchResult, SearchRuntime, Store,
    TypedContentObject, TypedContentRoot, VectorStorageConfig,
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum HnswRoutingVectorEncodingRecord {
    FullF32,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HnswConfigRecord {
    pub max_connections: u16,
    pub ef_construction: u32,
    pub ef_search: u32,
    pub level_bits: u8,
    pub overfetch_multiplier: u32,
    pub seed: u64,
    pub routing_vector_encoding: HnswRoutingVectorEncodingRecord,
}

impl From<HnswConfig> for HnswConfigRecord {
    fn from(value: HnswConfig) -> Self {
        Self {
            max_connections: value.max_connections,
            ef_construction: value.ef_construction,
            ef_search: value.ef_search,
            level_bits: value.level_bits,
            overfetch_multiplier: value.overfetch_multiplier,
            seed: value.seed,
            routing_vector_encoding: HnswRoutingVectorEncodingRecord::FullF32,
        }
    }
}

impl From<HnswConfigRecord> for HnswConfig {
    fn from(value: HnswConfigRecord) -> Self {
        let routing_vector_encoding = match value.routing_vector_encoding {
            HnswRoutingVectorEncodingRecord::FullF32 => HnswRoutingVectorEncoding::FullF32,
        };
        Self {
            max_connections: value.max_connections,
            ef_construction: value.ef_construction,
            ef_search: value.ef_search,
            level_bits: value.level_bits,
            overfetch_multiplier: value.overfetch_multiplier,
            seed: value.seed,
            routing_vector_encoding,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HnswBuildLimitsRecord {
    pub max_records: Option<u64>,
    pub max_owned_bytes: Option<u64>,
    pub max_distance_evaluations: Option<u64>,
    pub worker_threads: u64,
    pub max_encoded_graph_bytes: Option<u64>,
}

impl TryFrom<HnswBuildLimitsRecord> for HnswBuildLimits {
    type Error = ProllyBindingError;

    fn try_from(value: HnswBuildLimitsRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            max_records: optional_usize(value.max_records, "max_records")?,
            max_owned_bytes: optional_usize(value.max_owned_bytes, "max_owned_bytes")?,
            max_distance_evaluations: optional_usize(
                value.max_distance_evaluations,
                "max_distance_evaluations",
            )?,
            worker_threads: to_usize(value.worker_threads, "worker_threads")?,
            max_encoded_graph_bytes: optional_usize(
                value.max_encoded_graph_bytes,
                "max_encoded_graph_bytes",
            )?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HnswBuildStatsRecord {
    pub records: u64,
    pub distance_evaluations: u64,
    pub directed_edges: u64,
    pub maximum_level: u8,
    pub owned_bytes: u64,
    pub encoded_graph_bytes: u64,
}

impl From<HnswBuildStats> for HnswBuildStatsRecord {
    fn from(value: HnswBuildStats) -> Self {
        Self {
            records: value.records as u64,
            distance_evaluations: value.distance_evaluations as u64,
            directed_edges: value.directed_edges as u64,
            maximum_level: value.maximum_level,
            owned_bytes: value.owned_bytes as u64,
            encoded_graph_bytes: value.encoded_graph_bytes as u64,
        }
    }
}

#[uniffi::export]
pub fn default_hnsw_config() -> HnswConfigRecord {
    HnswConfig::default().into()
}

#[uniffi::export]
pub fn default_hnsw_build_limits() -> HnswBuildLimitsRecord {
    let value = HnswBuildLimits::default();
    HnswBuildLimitsRecord {
        max_records: value.max_records.map(|value| value as u64),
        max_owned_bytes: value.max_owned_bytes.map(|value| value as u64),
        max_distance_evaluations: value.max_distance_evaluations.map(|value| value as u64),
        worker_threads: value.worker_threads as u64,
        max_encoded_graph_bytes: value.max_encoded_graph_bytes.map(|value| value as u64),
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProductQuantizationConfigRecord {
    pub subquantizers: u32,
    pub centroids_per_subquantizer: u16,
    pub training_iterations: u16,
    pub rerank_multiplier: u32,
    pub seed: u64,
    pub max_training_vectors: u64,
}

impl TryFrom<ProductQuantizationConfigRecord> for ProductQuantizationConfig {
    type Error = ProllyBindingError;

    fn try_from(value: ProductQuantizationConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            subquantizers: value.subquantizers,
            centroids_per_subquantizer: value.centroids_per_subquantizer,
            training_iterations: value.training_iterations,
            rerank_multiplier: value.rerank_multiplier,
            seed: value.seed,
            max_training_vectors: to_usize(value.max_training_vectors, "max_training_vectors")?,
        })
    }
}

impl From<ProductQuantizationConfig> for ProductQuantizationConfigRecord {
    fn from(value: ProductQuantizationConfig) -> Self {
        Self {
            subquantizers: value.subquantizers,
            centroids_per_subquantizer: value.centroids_per_subquantizer,
            training_iterations: value.training_iterations,
            rerank_multiplier: value.rerank_multiplier,
            seed: value.seed,
            max_training_vectors: value.max_training_vectors as u64,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProductQuantizationBuildLimitsRecord {
    pub max_training_vectors: Option<u64>,
    pub max_training_bytes: Option<u64>,
    pub max_temporary_code_bytes: Option<u64>,
    pub max_distance_evaluations: Option<u64>,
    pub max_encoded_output_bytes: Option<u64>,
    pub max_worker_threads: Option<u64>,
}

impl TryFrom<ProductQuantizationBuildLimitsRecord> for ProductQuantizationBuildLimits {
    type Error = ProllyBindingError;

    fn try_from(value: ProductQuantizationBuildLimitsRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            max_training_vectors: optional_usize(
                value.max_training_vectors,
                "max_training_vectors",
            )?,
            max_training_bytes: optional_usize(value.max_training_bytes, "max_training_bytes")?,
            max_temporary_code_bytes: optional_usize(
                value.max_temporary_code_bytes,
                "max_temporary_code_bytes",
            )?,
            max_distance_evaluations: optional_usize(
                value.max_distance_evaluations,
                "max_distance_evaluations",
            )?,
            max_encoded_output_bytes: optional_usize(
                value.max_encoded_output_bytes,
                "max_encoded_output_bytes",
            )?,
            max_worker_threads: optional_usize(value.max_worker_threads, "max_worker_threads")?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProductQuantizationBuildStatsRecord {
    pub training_distance_evaluations: u64,
    pub encoding_distance_evaluations: u64,
    pub encoded_vectors: u64,
    pub training_vectors: u64,
    pub training_bytes: u64,
    pub encoded_output_bytes: u64,
}

impl From<ProductQuantizationBuildStats> for ProductQuantizationBuildStatsRecord {
    fn from(value: ProductQuantizationBuildStats) -> Self {
        Self {
            training_distance_evaluations: value.training_distance_evaluations as u64,
            encoding_distance_evaluations: value.encoding_distance_evaluations as u64,
            encoded_vectors: value.encoded_vectors as u64,
            training_vectors: value.training_vectors as u64,
            training_bytes: value.training_bytes as u64,
            encoded_output_bytes: value.encoded_output_bytes as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, uniffi::Record)]
pub struct ProductQuantizationQualityRecord {
    pub mean_squared_error: f64,
    pub maximum_squared_error: f64,
}

impl From<ProductQuantizationQuality> for ProductQuantizationQualityRecord {
    fn from(value: ProductQuantizationQuality) -> Self {
        Self {
            mean_squared_error: value.mean_squared_error,
            maximum_squared_error: value.maximum_squared_error,
        }
    }
}

#[uniffi::export]
pub fn default_pq_config() -> ProductQuantizationConfigRecord {
    ProductQuantizationConfig::default().into()
}

#[uniffi::export]
pub fn default_pq_build_limits() -> ProductQuantizationBuildLimitsRecord {
    let value = ProductQuantizationBuildLimits::default();
    ProductQuantizationBuildLimitsRecord {
        max_training_vectors: value.max_training_vectors.map(|value| value as u64),
        max_training_bytes: value.max_training_bytes.map(|value| value as u64),
        max_temporary_code_bytes: value.max_temporary_code_bytes.map(|value| value as u64),
        max_distance_evaluations: value.max_distance_evaluations.map(|value| value as u64),
        max_encoded_output_bytes: value.max_encoded_output_bytes.map(|value| value as u64),
        max_worker_threads: value.max_worker_threads.map(|value| value as u64),
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CompositeBaseKindRecord {
    Hnsw,
    ProductQuantized,
}

impl From<CompositeBaseKind> for CompositeBaseKindRecord {
    fn from(value: CompositeBaseKind) -> Self {
        match value {
            CompositeBaseKind::Hnsw => Self::Hnsw,
            CompositeBaseKind::ProductQuantized => Self::ProductQuantized,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompositeAcceleratorConfigRecord {
    pub max_delta_records: u64,
    pub max_shadow_records: u64,
    pub max_delta_ratio_ppm: u32,
    pub max_shadow_ratio_ppm: u32,
    pub base_overfetch_multiplier: u32,
}

impl TryFrom<CompositeAcceleratorConfigRecord> for CompositeAcceleratorConfig {
    type Error = ProllyBindingError;

    fn try_from(value: CompositeAcceleratorConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            max_delta_records: to_usize(value.max_delta_records, "max_delta_records")?,
            max_shadow_records: to_usize(value.max_shadow_records, "max_shadow_records")?,
            max_delta_ratio_ppm: value.max_delta_ratio_ppm,
            max_shadow_ratio_ppm: value.max_shadow_ratio_ppm,
            base_overfetch_multiplier: value.base_overfetch_multiplier,
        })
    }
}

impl From<CompositeAcceleratorConfig> for CompositeAcceleratorConfigRecord {
    fn from(value: CompositeAcceleratorConfig) -> Self {
        Self {
            max_delta_records: value.max_delta_records as u64,
            max_shadow_records: value.max_shadow_records as u64,
            max_delta_ratio_ppm: value.max_delta_ratio_ppm,
            max_shadow_ratio_ppm: value.max_shadow_ratio_ppm,
            base_overfetch_multiplier: value.base_overfetch_multiplier,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompositeBuildLimitsRecord {
    pub max_diff_entries: Option<u64>,
    pub max_owned_bytes: Option<u64>,
    pub max_encoded_output_bytes: Option<u64>,
    pub max_distance_evaluations: Option<u64>,
}

impl TryFrom<CompositeBuildLimitsRecord> for CompositeBuildLimits {
    type Error = ProllyBindingError;

    fn try_from(value: CompositeBuildLimitsRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            max_diff_entries: optional_usize(value.max_diff_entries, "max_diff_entries")?,
            max_owned_bytes: optional_usize(value.max_owned_bytes, "max_owned_bytes")?,
            max_encoded_output_bytes: optional_usize(
                value.max_encoded_output_bytes,
                "max_encoded_output_bytes",
            )?,
            max_distance_evaluations: optional_usize(
                value.max_distance_evaluations,
                "max_distance_evaluations",
            )?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompositeBuildStatsRecord {
    pub diff_entries: u64,
    pub inserted_records: u64,
    pub vector_updated_records: u64,
    pub value_only_records: u64,
    pub deleted_records: u64,
    pub delta_records: u64,
    pub shadow_records: u64,
    pub owned_bytes_peak: u64,
    pub encoded_output_bytes: u64,
    pub distance_evaluations: u64,
}

impl From<CompositeBuildStats> for CompositeBuildStatsRecord {
    fn from(value: CompositeBuildStats) -> Self {
        Self {
            diff_entries: value.diff_entries as u64,
            inserted_records: value.inserted_records as u64,
            vector_updated_records: value.vector_updated_records as u64,
            value_only_records: value.value_only_records as u64,
            deleted_records: value.deleted_records as u64,
            delta_records: value.delta_records as u64,
            shadow_records: value.shadow_records as u64,
            owned_bytes_peak: value.owned_bytes_peak as u64,
            encoded_output_bytes: value.encoded_output_bytes as u64,
            distance_evaluations: value.distance_evaluations as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum FullRebuildReasonKindRecord {
    DeltaRecords,
    ShadowRecords,
    DeltaRatio,
    ShadowRatio,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct FullRebuildReasonRecord {
    pub kind: FullRebuildReasonKindRecord,
    pub actual: u64,
    pub maximum: u64,
}

impl From<FullRebuildReason> for FullRebuildReasonRecord {
    fn from(value: FullRebuildReason) -> Self {
        match value {
            FullRebuildReason::DeltaRecords { actual, maximum } => Self {
                kind: FullRebuildReasonKindRecord::DeltaRecords,
                actual: actual as u64,
                maximum: maximum as u64,
            },
            FullRebuildReason::ShadowRecords { actual, maximum } => Self {
                kind: FullRebuildReasonKindRecord::ShadowRecords,
                actual: actual as u64,
                maximum: maximum as u64,
            },
            FullRebuildReason::DeltaRatio {
                actual_ppm,
                maximum_ppm,
            } => Self {
                kind: FullRebuildReasonKindRecord::DeltaRatio,
                actual: u64::from(actual_ppm),
                maximum: u64::from(maximum_ppm),
            },
            FullRebuildReason::ShadowRatio {
                actual_ppm,
                maximum_ppm,
            } => Self {
                kind: FullRebuildReasonKindRecord::ShadowRatio,
                actual: u64::from(actual_ppm),
                maximum: u64::from(maximum_ppm),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CompositeRebuildOptionsRecord {
    pub hnsw_limits: HnswBuildLimitsRecord,
    pub pq_worker_threads: u64,
    pub pq_limits: ProductQuantizationBuildLimitsRecord,
}

impl TryFrom<CompositeRebuildOptionsRecord> for CompositeRebuildOptions {
    type Error = ProllyBindingError;

    fn try_from(value: CompositeRebuildOptionsRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            hnsw_limits: value.hnsw_limits.try_into()?,
            pq_parallelism: BuildParallelism::new(to_usize(
                value.pq_worker_threads,
                "pq_worker_threads",
            )?)?,
            pq_limits: value.pq_limits.try_into()?,
        })
    }
}

#[uniffi::export]
pub fn default_composite_accelerator_config() -> CompositeAcceleratorConfigRecord {
    CompositeAcceleratorConfig::default().into()
}

#[uniffi::export]
pub fn default_composite_build_limits() -> CompositeBuildLimitsRecord {
    CompositeBuildLimitsRecord {
        max_diff_entries: None,
        max_owned_bytes: None,
        max_encoded_output_bytes: None,
        max_distance_evaluations: None,
    }
}

#[uniffi::export]
pub fn default_composite_rebuild_options() -> CompositeRebuildOptionsRecord {
    CompositeRebuildOptionsRecord {
        hnsw_limits: default_hnsw_build_limits(),
        pq_worker_threads: 1,
        pq_limits: default_pq_build_limits(),
    }
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

macro_rules! with_proximity_store_map {
    ($self:expr, $store:ident, $map:ident, $body:block) => {{
        let descriptor = crate::cid_from_vec($self.descriptor.clone())?;
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let $store = engine.store().clone();
                let $map = ProximityMap::load($store.clone(), descriptor)?;
                $body
            }
            BindingEngine::File(engine) => {
                let $store = engine.store().clone();
                let $map = ProximityMap::load($store.clone(), descriptor)?;
                $body
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let $store = engine.store().clone();
                let $map = ProximityMap::load($store.clone(), descriptor)?;
                $body
            }
            BindingEngine::Host(engine) => {
                let $store = engine.store().clone();
                let $map = ProximityMap::load($store.clone(), descriptor)?;
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

impl BindingProximityReadSession {
    pub(crate) fn scan_records_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'record> FnMut(&[u8], prolly::ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<prolly::ScanOutcome<B>, ProllyBindingError> {
        with_proximity_session!(self, map, {
            map.scan_records_range_until(start, end, visit)
                .map_err(Into::into)
        })
    }
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
    pub(crate) fn scan_records_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'record> FnMut(&[u8], prolly::ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<prolly::ScanOutcome<B>, ProllyBindingError> {
        with_proximity_map!(self, map, {
            map.scan_records_range_until(start, end, visit)
                .map_err(Into::into)
        })
    }
}

#[derive(uniffi::Object)]
pub struct BindingHnswIndex {
    engine: Arc<ProllyEngine>,
    manifest: Vec<u8>,
    source_descriptor: Vec<u8>,
    config: HnswConfigRecord,
    canonical: bool,
}

#[derive(uniffi::Object)]
pub struct BindingProductQuantizer {
    engine: Arc<ProllyEngine>,
    manifest: Vec<u8>,
    source_descriptor: Vec<u8>,
    config: ProductQuantizationConfigRecord,
    quality: ProductQuantizationQualityRecord,
}

#[derive(uniffi::Object)]
pub struct BindingCompositeAccelerator {
    engine: Arc<ProllyEngine>,
    manifest: Vec<u8>,
    current_source_descriptor: Vec<u8>,
    base_source_descriptor: Vec<u8>,
    base_kind: CompositeBaseKindRecord,
    delta_count: u64,
    shadow_count: u64,
    config: CompositeAcceleratorConfigRecord,
    build_stats: CompositeBuildStatsRecord,
}

#[derive(Clone, uniffi::Record)]
pub struct CompositeBuildOutcomeRecord {
    pub accelerator: Option<Arc<BindingCompositeAccelerator>>,
    pub reasons: Vec<FullRebuildReasonRecord>,
    pub stats: CompositeBuildStatsRecord,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CompositeBuildOrRebuildKindRecord {
    Composite,
    NoAcceleratorRequired,
    HnswRebuilt,
    ProductQuantizedRebuilt,
}

#[derive(Clone, uniffi::Record)]
pub struct CompositeBuildOrRebuildOutcomeRecord {
    pub kind: CompositeBuildOrRebuildKindRecord,
    pub composite: Option<Arc<BindingCompositeAccelerator>>,
    pub hnsw: Option<Arc<BindingHnswIndex>>,
    pub pq: Option<Arc<BindingProductQuantizer>>,
    pub reasons: Vec<FullRebuildReasonRecord>,
    pub composite_stats: CompositeBuildStatsRecord,
    pub hnsw_stats: Option<HnswBuildStatsRecord>,
    pub pq_stats: Option<ProductQuantizationBuildStatsRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CatalogAcceleratorKindRecord {
    Hnsw,
    ProductQuantized,
    Composite,
}

impl From<CatalogAcceleratorKind> for CatalogAcceleratorKindRecord {
    fn from(value: CatalogAcceleratorKind) -> Self {
        match value {
            CatalogAcceleratorKind::Hnsw => Self::Hnsw,
            CatalogAcceleratorKind::ProductQuantized => Self::ProductQuantized,
            CatalogAcceleratorKind::Composite => Self::Composite,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AcceleratorCatalogEntryRecord {
    pub kind: CatalogAcceleratorKindRecord,
    pub configuration_fingerprint: Vec<u8>,
    pub manifest: Vec<u8>,
}

impl From<AcceleratorCatalogEntry> for AcceleratorCatalogEntryRecord {
    fn from(value: AcceleratorCatalogEntry) -> Self {
        Self {
            kind: value.kind.into(),
            configuration_fingerprint: value.configuration_fingerprint.0.to_vec(),
            manifest: value.manifest.0.to_vec(),
        }
    }
}

#[derive(uniffi::Object)]
pub struct BindingAcceleratorCatalog {
    engine: Arc<ProllyEngine>,
    manifest: Vec<u8>,
    source_descriptor: Vec<u8>,
    entries: Vec<AcceleratorCatalogEntryRecord>,
}

fn binding_hnsw_index<S>(engine: Arc<ProllyEngine>, index: &HnswIndex<S>) -> Arc<BindingHnswIndex>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    Arc::new(BindingHnswIndex {
        engine,
        manifest: index.manifest_cid().0.to_vec(),
        source_descriptor: index.source_descriptor().0.to_vec(),
        config: index.config().clone().into(),
        canonical: index.is_canonical(),
    })
}

fn binding_product_quantizer<S>(
    engine: Arc<ProllyEngine>,
    index: &ProductQuantizer<S>,
) -> Arc<BindingProductQuantizer>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    Arc::new(BindingProductQuantizer {
        engine,
        manifest: index.manifest_cid().0.to_vec(),
        source_descriptor: index.source_descriptor().0.to_vec(),
        config: index.config().clone().into(),
        quality: index.quality().into(),
    })
}

fn binding_composite_accelerator<S>(
    engine: Arc<ProllyEngine>,
    index: &CompositeAccelerator<S>,
) -> Arc<BindingCompositeAccelerator>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    Arc::new(BindingCompositeAccelerator {
        engine,
        manifest: index.manifest_cid().0.to_vec(),
        current_source_descriptor: index.current_source_descriptor().0.to_vec(),
        base_source_descriptor: index.base_source_descriptor().0.to_vec(),
        base_kind: index.base_kind().into(),
        delta_count: index.delta_count(),
        shadow_count: index.shadow_count(),
        config: index.config().clone().into(),
        build_stats: index.build_stats().clone().into(),
    })
}

fn composite_build_outcome<S>(
    engine: Arc<ProllyEngine>,
    outcome: CompositeBuildOutcome<S>,
) -> CompositeBuildOutcomeRecord
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    match outcome {
        CompositeBuildOutcome::Composite { accelerator, stats } => CompositeBuildOutcomeRecord {
            accelerator: Some(binding_composite_accelerator(engine, &accelerator)),
            reasons: Vec::new(),
            stats: stats.into(),
        },
        CompositeBuildOutcome::FullRebuildRequired { reasons, stats } => {
            CompositeBuildOutcomeRecord {
                accelerator: None,
                reasons: reasons.into_iter().map(Into::into).collect(),
                stats: stats.into(),
            }
        }
    }
}

fn composite_rebuild_outcome<S>(
    engine: Arc<ProllyEngine>,
    outcome: CompositeBuildOrRebuildOutcome<S>,
) -> CompositeBuildOrRebuildOutcomeRecord
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    match outcome {
        CompositeBuildOrRebuildOutcome::Composite { accelerator, stats } => {
            CompositeBuildOrRebuildOutcomeRecord {
                kind: CompositeBuildOrRebuildKindRecord::Composite,
                composite: Some(binding_composite_accelerator(engine, &accelerator)),
                hnsw: None,
                pq: None,
                reasons: Vec::new(),
                composite_stats: stats.into(),
                hnsw_stats: None,
                pq_stats: None,
            }
        }
        CompositeBuildOrRebuildOutcome::NoAcceleratorRequired {
            reasons,
            composite_stats,
        } => CompositeBuildOrRebuildOutcomeRecord {
            kind: CompositeBuildOrRebuildKindRecord::NoAcceleratorRequired,
            composite: None,
            hnsw: None,
            pq: None,
            reasons: reasons.into_iter().map(Into::into).collect(),
            composite_stats: composite_stats.into(),
            hnsw_stats: None,
            pq_stats: None,
        },
        CompositeBuildOrRebuildOutcome::HnswRebuilt {
            accelerator,
            reasons,
            composite_stats,
            rebuild_stats,
        } => CompositeBuildOrRebuildOutcomeRecord {
            kind: CompositeBuildOrRebuildKindRecord::HnswRebuilt,
            composite: None,
            hnsw: Some(binding_hnsw_index(engine, &accelerator)),
            pq: None,
            reasons: reasons.into_iter().map(Into::into).collect(),
            composite_stats: composite_stats.into(),
            hnsw_stats: Some(rebuild_stats.into()),
            pq_stats: None,
        },
        CompositeBuildOrRebuildOutcome::ProductQuantizedRebuilt {
            accelerator,
            reasons,
            composite_stats,
            rebuild_stats,
        } => CompositeBuildOrRebuildOutcomeRecord {
            kind: CompositeBuildOrRebuildKindRecord::ProductQuantizedRebuilt,
            composite: None,
            hnsw: None,
            pq: Some(binding_product_quantizer(engine, &accelerator)),
            reasons: reasons.into_iter().map(Into::into).collect(),
            composite_stats: composite_stats.into(),
            hnsw_stats: None,
            pq_stats: Some(rebuild_stats.into()),
        },
    }
}

fn build_composite_hnsw<S>(
    engine: Arc<ProllyEngine>,
    store: S,
    base_source: prolly::Cid,
    current_source: prolly::Cid,
    base_manifest: prolly::Cid,
    config: CompositeAcceleratorConfig,
    limits: CompositeBuildLimits,
) -> Result<CompositeBuildOutcomeRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let base_map = ProximityMap::load(store.clone(), base_source)?;
    let current_map = ProximityMap::load(store.clone(), current_source)?;
    let base = CompositeBase::Hnsw(HnswIndex::load(store, base_manifest)?);
    Ok(composite_build_outcome(
        engine,
        CompositeAccelerator::build(&base_map, &current_map, base, config, limits)?,
    ))
}

fn build_composite_pq<S>(
    engine: Arc<ProllyEngine>,
    store: S,
    base_source: prolly::Cid,
    current_source: prolly::Cid,
    base_manifest: prolly::Cid,
    config: CompositeAcceleratorConfig,
    limits: CompositeBuildLimits,
) -> Result<CompositeBuildOutcomeRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let base_map = ProximityMap::load(store.clone(), base_source)?;
    let current_map = ProximityMap::load(store.clone(), current_source)?;
    let base = CompositeBase::ProductQuantized(ProductQuantizer::load(store, base_manifest)?);
    Ok(composite_build_outcome(
        engine,
        CompositeAccelerator::build(&base_map, &current_map, base, config, limits)?,
    ))
}

fn rebuild_composite_hnsw<S>(
    engine: Arc<ProllyEngine>,
    store: S,
    base_source: prolly::Cid,
    current_source: prolly::Cid,
    base_manifest: prolly::Cid,
    config: CompositeAcceleratorConfig,
    limits: CompositeBuildLimits,
    rebuild: CompositeRebuildOptions,
) -> Result<CompositeBuildOrRebuildOutcomeRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let base_map = ProximityMap::load(store.clone(), base_source)?;
    let current_map = ProximityMap::load(store.clone(), current_source)?;
    let base = CompositeBase::Hnsw(HnswIndex::load(store, base_manifest)?);
    Ok(composite_rebuild_outcome(
        engine,
        CompositeAccelerator::build_or_rebuild(
            &base_map,
            &current_map,
            base,
            config,
            limits,
            rebuild,
        )?,
    ))
}

fn rebuild_composite_pq<S>(
    engine: Arc<ProllyEngine>,
    store: S,
    base_source: prolly::Cid,
    current_source: prolly::Cid,
    base_manifest: prolly::Cid,
    config: CompositeAcceleratorConfig,
    limits: CompositeBuildLimits,
    rebuild: CompositeRebuildOptions,
) -> Result<CompositeBuildOrRebuildOutcomeRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let base_map = ProximityMap::load(store.clone(), base_source)?;
    let current_map = ProximityMap::load(store.clone(), current_source)?;
    let base = CompositeBase::ProductQuantized(ProductQuantizer::load(store, base_manifest)?);
    Ok(composite_rebuild_outcome(
        engine,
        CompositeAccelerator::build_or_rebuild(
            &base_map,
            &current_map,
            base,
            config,
            limits,
            rebuild,
        )?,
    ))
}

fn search_hnsw<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
) -> Result<ProximitySearchResultRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let index = HnswIndex::load(store.clone(), manifest.clone())?;
    let map = ProximityMap::load(store, source.clone())?;
    index
        .search(&map, request)
        .map(Into::into)
        .map_err(Into::into)
}

fn prove_hnsw_search<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
    limits: &ContentGraphLimits,
) -> Result<ProximitySearchProof, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let index = HnswIndex::load(store.clone(), manifest.clone())?;
    let map = ProximityMap::load(store, source.clone())?;
    index
        .prove_search(&map, request, limits)
        .map_err(Into::into)
}

fn search_pq<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
) -> Result<ProximitySearchResultRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let index = ProductQuantizer::load(store.clone(), manifest.clone())?;
    let map = ProximityMap::load(store, source.clone())?;
    index
        .search(&map, request)
        .map(Into::into)
        .map_err(Into::into)
}

fn prove_pq_search<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
    limits: &ContentGraphLimits,
) -> Result<ProximitySearchProof, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let index = ProductQuantizer::load(store.clone(), manifest.clone())?;
    let map = ProximityMap::load(store, source.clone())?;
    index
        .prove_search(&map, request, limits)
        .map_err(Into::into)
}

fn binding_accelerator_catalog<S>(
    engine: Arc<ProllyEngine>,
    catalog: &AcceleratorCatalog<S>,
) -> Arc<BindingAcceleratorCatalog>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    Arc::new(BindingAcceleratorCatalog {
        engine,
        manifest: catalog.manifest_cid().0.to_vec(),
        source_descriptor: catalog.source_descriptor().0.to_vec(),
        entries: catalog.entries().iter().cloned().map(Into::into).collect(),
    })
}

fn search_composite<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
) -> Result<ProximitySearchResultRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let map = ProximityMap::load(store.clone(), source.clone())?;
    let composite = CompositeAccelerator::load(store.clone(), manifest.clone())?;
    let accelerators = AcceleratorSet::empty().with_composite(map.tree(), composite)?;
    let search_io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    map.search_with(&accelerators, &search_io, request)
        .map(Into::into)
        .map_err(Into::into)
}

fn prove_composite_search<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
    limits: &ContentGraphLimits,
) -> Result<ProximitySearchProof, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let index = CompositeAccelerator::load(store.clone(), manifest.clone())?;
    let map = ProximityMap::load(store, source.clone())?;
    index
        .prove_search(&map, request, limits)
        .map_err(Into::into)
}

fn search_catalog<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
) -> Result<ProximitySearchResultRecord, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let map = ProximityMap::load(store.clone(), source.clone())?;
    let catalog = AcceleratorCatalog::load(store.clone(), manifest.clone(), map.tree())?;
    let search_io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
    map.search_with(catalog.accelerators(), &search_io, request)
        .map(Into::into)
        .map_err(Into::into)
}

fn prove_catalog_search<S>(
    store: S,
    manifest: &prolly::Cid,
    source: &prolly::Cid,
    request: SearchRequest<'_>,
    limits: &ContentGraphLimits,
) -> Result<ProximitySearchProof, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let map = ProximityMap::load(store.clone(), source.clone())?;
    let catalog = AcceleratorCatalog::load(store, manifest.clone(), map.tree())?;
    catalog
        .prove_search(&map, request, limits)
        .map_err(Into::into)
}

fn build_catalog<S>(
    engine: Arc<ProllyEngine>,
    store: S,
    source: prolly::Cid,
    hnsw: Option<prolly::Cid>,
    pq: Option<prolly::Cid>,
    composite: Option<prolly::Cid>,
) -> Result<Arc<BindingAcceleratorCatalog>, ProllyBindingError>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let map = ProximityMap::load(store.clone(), source)?;
    let mut accelerators = AcceleratorSet::empty();
    if let Some(manifest) = hnsw {
        accelerators =
            accelerators.with_hnsw(map.tree(), HnswIndex::load(store.clone(), manifest)?)?;
    }
    if let Some(manifest) = pq {
        accelerators =
            accelerators.with_pq(map.tree(), ProductQuantizer::load(store.clone(), manifest)?)?;
    }
    if let Some(manifest) = composite {
        accelerators = accelerators.with_composite(
            map.tree(),
            CompositeAccelerator::load(store.clone(), manifest)?,
        )?;
    }
    let catalog = AcceleratorCatalog::build(store, map.tree(), accelerators)?;
    Ok(binding_accelerator_catalog(engine, &catalog))
}

#[uniffi::export]
impl BindingHnswIndex {
    pub fn manifest(&self) -> Vec<u8> {
        self.manifest.clone()
    }

    pub fn source_descriptor(&self) -> Vec<u8> {
        self.source_descriptor.clone()
    }

    pub fn config(&self) -> HnswConfigRecord {
        self.config.clone()
    }

    pub fn is_canonical(&self) -> bool {
        self.canonical
    }

    pub fn search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "HNSW index and proximity map belong to different engines".to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                search_hnsw(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::File(engine) => {
                search_hnsw(engine.store().clone(), &manifest, &source, request)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                search_hnsw(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::Host(engine) => {
                search_hnsw(engine.store().clone(), &manifest, &source, request)
            }
        }
    }

    pub fn prove_search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord,
    ) -> Result<Arc<BindingProximitySearchProof>, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "HNSW index and proximity map belong to different engines".to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        let limits = ContentGraphLimits::try_from(limits)?;
        let proof = match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                prove_hnsw_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::File(engine) => {
                prove_hnsw_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                prove_hnsw_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::Host(engine) => {
                prove_hnsw_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
        }?;
        Ok(Arc::new(BindingProximitySearchProof { inner: proof }))
    }
}

#[uniffi::export]
impl BindingProductQuantizer {
    pub fn manifest(&self) -> Vec<u8> {
        self.manifest.clone()
    }

    pub fn source_descriptor(&self) -> Vec<u8> {
        self.source_descriptor.clone()
    }

    pub fn config(&self) -> ProductQuantizationConfigRecord {
        self.config.clone()
    }

    pub fn quality(&self) -> ProductQuantizationQualityRecord {
        self.quality
    }

    pub fn search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "product quantizer and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                search_pq(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::File(engine) => {
                search_pq(engine.store().clone(), &manifest, &source, request)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                search_pq(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::Host(engine) => {
                search_pq(engine.store().clone(), &manifest, &source, request)
            }
        }
    }

    pub fn prove_search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord,
    ) -> Result<Arc<BindingProximitySearchProof>, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "product quantizer and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        let limits = ContentGraphLimits::try_from(limits)?;
        let proof = match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                prove_pq_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::File(engine) => {
                prove_pq_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                prove_pq_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::Host(engine) => {
                prove_pq_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
        }?;
        Ok(Arc::new(BindingProximitySearchProof { inner: proof }))
    }
}

#[uniffi::export]
impl BindingCompositeAccelerator {
    pub fn manifest(&self) -> Vec<u8> {
        self.manifest.clone()
    }

    pub fn current_source_descriptor(&self) -> Vec<u8> {
        self.current_source_descriptor.clone()
    }

    pub fn base_source_descriptor(&self) -> Vec<u8> {
        self.base_source_descriptor.clone()
    }

    pub fn base_kind(&self) -> CompositeBaseKindRecord {
        self.base_kind
    }

    pub fn delta_count(&self) -> u64 {
        self.delta_count
    }

    pub fn shadow_count(&self) -> u64 {
        self.shadow_count
    }

    pub fn config(&self) -> CompositeAcceleratorConfigRecord {
        self.config.clone()
    }

    pub fn build_stats(&self) -> CompositeBuildStatsRecord {
        self.build_stats.clone()
    }

    pub fn search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite accelerator and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                search_composite(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::File(engine) => {
                search_composite(engine.store().clone(), &manifest, &source, request)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                search_composite(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::Host(engine) => {
                search_composite(engine.store().clone(), &manifest, &source, request)
            }
        }
    }

    pub fn prove_search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord,
    ) -> Result<Arc<BindingProximitySearchProof>, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite accelerator and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        let limits = ContentGraphLimits::try_from(limits)?;
        let proof = match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                prove_composite_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::File(engine) => {
                prove_composite_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                prove_composite_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::Host(engine) => {
                prove_composite_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
        }?;
        Ok(Arc::new(BindingProximitySearchProof { inner: proof }))
    }
}

#[uniffi::export]
impl BindingAcceleratorCatalog {
    pub fn manifest(&self) -> Vec<u8> {
        self.manifest.clone()
    }

    pub fn source_descriptor(&self) -> Vec<u8> {
        self.source_descriptor.clone()
    }

    pub fn entries(&self) -> Vec<AcceleratorCatalogEntryRecord> {
        self.entries.clone()
    }

    pub fn search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
    ) -> Result<ProximitySearchResultRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "accelerator catalog and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                search_catalog(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::File(engine) => {
                search_catalog(engine.store().clone(), &manifest, &source, request)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                search_catalog(engine.store().clone(), &manifest, &source, request)
            }
            BindingEngine::Host(engine) => {
                search_catalog(engine.store().clone(), &manifest, &source, request)
            }
        }
    }

    pub fn prove_search(
        &self,
        map: Arc<BindingProximityMap>,
        request: ProximitySearchRequestRecord,
        limits: ContentGraphLimitsRecord,
    ) -> Result<Arc<BindingProximitySearchProof>, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &map.engine) {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "accelerator catalog and proximity map belong to different engines"
                    .to_string(),
            });
        }
        let manifest = crate::cid_from_vec(self.manifest.clone())?;
        let source = crate::cid_from_vec(map.descriptor())?;
        let request = proximity_search_request(&request)?;
        let limits = ContentGraphLimits::try_from(limits)?;
        let proof = match &self.engine.inner {
            BindingEngine::Memory(engine) => {
                prove_catalog_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::File(engine) => {
                prove_catalog_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                prove_catalog_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
            BindingEngine::Host(engine) => {
                prove_catalog_search(engine.store().clone(), &manifest, &source, request, &limits)
            }
        }?;
        Ok(Arc::new(BindingProximitySearchProof { inner: proof }))
    }
}

#[derive(Clone, uniffi::Record)]
pub struct HnswBuildResultRecord {
    pub index: Arc<BindingHnswIndex>,
    pub stats: HnswBuildStatsRecord,
}

#[derive(Clone, uniffi::Record)]
pub struct ProductQuantizationBuildResultRecord {
    pub index: Arc<BindingProductQuantizer>,
    pub stats: ProductQuantizationBuildStatsRecord,
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

    pub fn build_hnsw(
        &self,
        config: HnswConfigRecord,
        limits: HnswBuildLimitsRecord,
    ) -> Result<HnswBuildResultRecord, ProllyBindingError> {
        let config: HnswConfig = config.into();
        let limits = HnswBuildLimits::try_from(limits)?;
        with_proximity_map!(self, map, {
            let (index, stats) = HnswIndex::build_with_limits(&map, config, limits)?;
            Ok(HnswBuildResultRecord {
                index: Arc::new(BindingHnswIndex {
                    engine: self.engine.clone(),
                    manifest: index.manifest_cid().0.to_vec(),
                    source_descriptor: index.source_descriptor().0.to_vec(),
                    config: index.config().clone().into(),
                    canonical: index.is_canonical(),
                }),
                stats: stats.into(),
            })
        })
    }

    pub fn load_hnsw(
        &self,
        manifest: Vec<u8>,
    ) -> Result<Arc<BindingHnswIndex>, ProllyBindingError> {
        let manifest = crate::cid_from_vec(manifest)?;
        with_proximity_store_map!(self, store, map, {
            let index = HnswIndex::load(store, manifest)?;
            if index.source_descriptor() != &map.tree().descriptor {
                return Err(ProllyBindingError::InvalidArgument {
                    reason: "HNSW index is bound to a different source descriptor".to_string(),
                });
            }
            Ok(Arc::new(BindingHnswIndex {
                engine: self.engine.clone(),
                manifest: index.manifest_cid().0.to_vec(),
                source_descriptor: index.source_descriptor().0.to_vec(),
                config: index.config().clone().into(),
                canonical: index.is_canonical(),
            }))
        })
    }

    pub fn build_pq(
        &self,
        config: ProductQuantizationConfigRecord,
        worker_threads: u64,
        limits: ProductQuantizationBuildLimitsRecord,
    ) -> Result<ProductQuantizationBuildResultRecord, ProllyBindingError> {
        let config = ProductQuantizationConfig::try_from(config)?;
        let parallelism = BuildParallelism::new(to_usize(worker_threads, "worker_threads")?)?;
        let limits = ProductQuantizationBuildLimits::try_from(limits)?;
        with_proximity_map!(self, map, {
            let (index, stats) =
                ProductQuantizer::build_with_limits(&map, config, parallelism, limits)?;
            Ok(ProductQuantizationBuildResultRecord {
                index: Arc::new(BindingProductQuantizer {
                    engine: self.engine.clone(),
                    manifest: index.manifest_cid().0.to_vec(),
                    source_descriptor: index.source_descriptor().0.to_vec(),
                    config: index.config().clone().into(),
                    quality: index.quality().into(),
                }),
                stats: stats.into(),
            })
        })
    }

    pub fn load_pq(
        &self,
        manifest: Vec<u8>,
    ) -> Result<Arc<BindingProductQuantizer>, ProllyBindingError> {
        let manifest = crate::cid_from_vec(manifest)?;
        with_proximity_store_map!(self, store, map, {
            let index = ProductQuantizer::load(store, manifest)?;
            if index.source_descriptor() != &map.tree().descriptor {
                return Err(ProllyBindingError::InvalidArgument {
                    reason: "product quantizer is bound to a different source descriptor"
                        .to_string(),
                });
            }
            Ok(Arc::new(BindingProductQuantizer {
                engine: self.engine.clone(),
                manifest: index.manifest_cid().0.to_vec(),
                source_descriptor: index.source_descriptor().0.to_vec(),
                config: index.config().clone().into(),
                quality: index.quality().into(),
            }))
        })
    }

    pub fn build_composite_hnsw(
        &self,
        base_map: Arc<BindingProximityMap>,
        base: Arc<BindingHnswIndex>,
        config: CompositeAcceleratorConfigRecord,
        limits: CompositeBuildLimitsRecord,
    ) -> Result<CompositeBuildOutcomeRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &base_map.engine) || !Arc::ptr_eq(&self.engine, &base.engine)
        {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite inputs belong to different engines".to_string(),
            });
        }
        let base_source = crate::cid_from_vec(base_map.descriptor())?;
        let current_source = crate::cid_from_vec(self.descriptor())?;
        let base_manifest = crate::cid_from_vec(base.manifest())?;
        let config = config.try_into()?;
        let limits = limits.try_into()?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => build_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            BindingEngine::File(engine) => build_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => build_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            BindingEngine::Host(engine) => build_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
        }
    }

    pub fn build_composite_pq(
        &self,
        base_map: Arc<BindingProximityMap>,
        base: Arc<BindingProductQuantizer>,
        config: CompositeAcceleratorConfigRecord,
        limits: CompositeBuildLimitsRecord,
    ) -> Result<CompositeBuildOutcomeRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &base_map.engine) || !Arc::ptr_eq(&self.engine, &base.engine)
        {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite inputs belong to different engines".to_string(),
            });
        }
        let base_source = crate::cid_from_vec(base_map.descriptor())?;
        let current_source = crate::cid_from_vec(self.descriptor())?;
        let base_manifest = crate::cid_from_vec(base.manifest())?;
        let config = config.try_into()?;
        let limits = limits.try_into()?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => build_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            BindingEngine::File(engine) => build_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => build_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
            BindingEngine::Host(engine) => build_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
            ),
        }
    }

    pub fn build_or_rebuild_composite_hnsw(
        &self,
        base_map: Arc<BindingProximityMap>,
        base: Arc<BindingHnswIndex>,
        config: CompositeAcceleratorConfigRecord,
        limits: CompositeBuildLimitsRecord,
        rebuild: CompositeRebuildOptionsRecord,
    ) -> Result<CompositeBuildOrRebuildOutcomeRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &base_map.engine) || !Arc::ptr_eq(&self.engine, &base.engine)
        {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite inputs belong to different engines".to_string(),
            });
        }
        let base_source = crate::cid_from_vec(base_map.descriptor())?;
        let current_source = crate::cid_from_vec(self.descriptor())?;
        let base_manifest = crate::cid_from_vec(base.manifest())?;
        let config = config.try_into()?;
        let limits = limits.try_into()?;
        let rebuild = rebuild.try_into()?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => rebuild_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            BindingEngine::File(engine) => rebuild_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => rebuild_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            BindingEngine::Host(engine) => rebuild_composite_hnsw(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
        }
    }

    pub fn build_or_rebuild_composite_pq(
        &self,
        base_map: Arc<BindingProximityMap>,
        base: Arc<BindingProductQuantizer>,
        config: CompositeAcceleratorConfigRecord,
        limits: CompositeBuildLimitsRecord,
        rebuild: CompositeRebuildOptionsRecord,
    ) -> Result<CompositeBuildOrRebuildOutcomeRecord, ProllyBindingError> {
        if !Arc::ptr_eq(&self.engine, &base_map.engine) || !Arc::ptr_eq(&self.engine, &base.engine)
        {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "composite inputs belong to different engines".to_string(),
            });
        }
        let base_source = crate::cid_from_vec(base_map.descriptor())?;
        let current_source = crate::cid_from_vec(self.descriptor())?;
        let base_manifest = crate::cid_from_vec(base.manifest())?;
        let config = config.try_into()?;
        let limits = limits.try_into()?;
        let rebuild = rebuild.try_into()?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => rebuild_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            BindingEngine::File(engine) => rebuild_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => rebuild_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
            BindingEngine::Host(engine) => rebuild_composite_pq(
                self.engine.clone(),
                engine.store().clone(),
                base_source,
                current_source,
                base_manifest,
                config,
                limits,
                rebuild,
            ),
        }
    }

    pub fn load_composite(
        &self,
        manifest: Vec<u8>,
    ) -> Result<Arc<BindingCompositeAccelerator>, ProllyBindingError> {
        let manifest = crate::cid_from_vec(manifest)?;
        with_proximity_store_map!(self, store, map, {
            let index = CompositeAccelerator::load(store, manifest)?;
            if index.current_source_descriptor() != &map.tree().descriptor {
                return Err(ProllyBindingError::InvalidArgument {
                    reason: "composite accelerator is bound to a different source descriptor"
                        .to_string(),
                });
            }
            Ok(binding_composite_accelerator(self.engine.clone(), &index))
        })
    }

    pub fn build_accelerator_catalog(
        &self,
        hnsw: Option<Arc<BindingHnswIndex>>,
        pq: Option<Arc<BindingProductQuantizer>>,
        composite: Option<Arc<BindingCompositeAccelerator>>,
    ) -> Result<Arc<BindingAcceleratorCatalog>, ProllyBindingError> {
        for belongs in [
            hnsw.as_ref()
                .map(|value| Arc::ptr_eq(&self.engine, &value.engine)),
            pq.as_ref()
                .map(|value| Arc::ptr_eq(&self.engine, &value.engine)),
            composite
                .as_ref()
                .map(|value| Arc::ptr_eq(&self.engine, &value.engine)),
        ]
        .into_iter()
        .flatten()
        {
            if !belongs {
                return Err(ProllyBindingError::InvalidArgument {
                    reason: "catalog inputs belong to different engines".to_string(),
                });
            }
        }
        let source = crate::cid_from_vec(self.descriptor())?;
        let hnsw = hnsw
            .map(|value| crate::cid_from_vec(value.manifest()))
            .transpose()?;
        let pq = pq
            .map(|value| crate::cid_from_vec(value.manifest()))
            .transpose()?;
        let composite = composite
            .map(|value| crate::cid_from_vec(value.manifest()))
            .transpose()?;
        match &self.engine.inner {
            BindingEngine::Memory(engine) => build_catalog(
                self.engine.clone(),
                engine.store().clone(),
                source,
                hnsw,
                pq,
                composite,
            ),
            BindingEngine::File(engine) => build_catalog(
                self.engine.clone(),
                engine.store().clone(),
                source,
                hnsw,
                pq,
                composite,
            ),
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => build_catalog(
                self.engine.clone(),
                engine.store().clone(),
                source,
                hnsw,
                pq,
                composite,
            ),
            BindingEngine::Host(engine) => build_catalog(
                self.engine.clone(),
                engine.store().clone(),
                source,
                hnsw,
                pq,
                composite,
            ),
        }
    }

    pub fn load_accelerator_catalog(
        &self,
        manifest: Vec<u8>,
    ) -> Result<Arc<BindingAcceleratorCatalog>, ProllyBindingError> {
        let manifest = crate::cid_from_vec(manifest)?;
        with_proximity_store_map!(self, store, map, {
            let catalog = AcceleratorCatalog::load(store, manifest, map.tree())?;
            Ok(binding_accelerator_catalog(self.engine.clone(), &catalog))
        })
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
