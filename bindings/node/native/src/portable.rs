use super::{
    to_napi_error, NativeProllyEngine, NodeDiffPageRecord, NodeDiffRecord, NodeEntryRecord,
    NodeMultiKeyProofVerificationRecord, NodeMutationRecord, NodeRangeCursorRecord,
    NodeRangePageProofVerificationRecord, NodeRangePageRecord, NodeRangeProofVerificationRecord,
    NodeReverseCursorRecord, NodeReversePageRecord, NodeTreeRecord,
};
use napi::bindgen_prelude::{Buffer, Env, Error, Float32Array, FunctionRef, Result, Status};
use napi::JsObject;
use napi_derive::napi;
use prolly_bindings::{
    default_content_graph_limits, default_proximity_config, exact_proximity_search_request,
    verify_key_proof, verify_multi_key_proof, verify_proximity_membership_proof,
    verify_proximity_structure_proof, verify_range_page_proof, verify_range_proof,
    ActiveIndexHealthRecord, BindingIndexRegistry, BindingIndexedMap, BindingIndexedSnapshot,
    BindingMapComparison, BindingMapSnapshot, BindingProximityMap, BindingProximityReadSession,
    BindingProximitySearchProof, BindingSecondaryIndexSnapshot, BindingVersionedMap,
    DistanceMetricRecord, ExactProximityRecordRecord, IndexBuildResultRecord, IndexEntryRecord,
    IndexMatchRecord, IndexPageRecord, IndexProjectionRecord, IndexVerificationRecord,
    IndexedMapHealthRecord, IndexedMapMetricsRecord, IndexedRetentionRecord,
    IndexedSnapshotIdRecord, IndexedSourceRecord, IndexedUpdateKind, IndexedUpdateRecord,
    IndexedVersionRecord, KeyProofRecord, MapUpdateKind, MapUpdateRecord, MapVersionRecord,
    MultiKeyProofRecord, ProllyBindingError, ProllyReadSession, ProvedRangePageRecord,
    ProximityConfigRecord, ProximityMembershipProofRecord, ProximityMutationRecord,
    ProximityMutationStatsRecord, ProximityNeighborRecord, ProximityRecordRecord,
    ProximitySearchClaimKindRecord, ProximitySearchResultRecord, ProximityStructuralProofRecord,
    ProximityVerificationRecord, RangeProofRecord, SearchBackendRecord, SearchCompletionRecord,
    SecondaryIndexExtractorCallback, VersionPruneRecord,
};
use std::sync::Arc;

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeFastPageResult {
    status: i32,
    terminal: u8,
    reserved: [u8; 3],
    record_count: u32,
    lease_handle: u64,
    data_ptr: *const u8,
    data_len: u64,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct NodeFastScanOpenResult {
    status: i32,
    reserved: u32,
    scan_handle: u64,
}

extern "C" {
    fn prolly_fast_read_session_scan_open(
        session_handle: u64,
        start_ptr: *const u8,
        start_len: usize,
        end_ptr: *const u8,
        end_len: usize,
        has_end: u8,
    ) -> NodeFastScanOpenResult;
    fn prolly_fast_read_session_scan_next(
        session_handle: u64,
        scan_handle: u64,
        max_records: u32,
        max_arena_bytes: u64,
    ) -> NodeFastPageResult;
    fn prolly_fast_scan_close(scan_handle: u64);
    fn prolly_fast_page_release(lease_handle: u64);
}

struct NodeFastScanGuard(u64);

impl Drop for NodeFastScanGuard {
    fn drop(&mut self) {
        unsafe { prolly_fast_scan_close(self.0) };
    }
}

struct NodeFastPageGuard(u64);

impl Drop for NodeFastPageGuard {
    fn drop(&mut self) {
        unsafe { prolly_fast_page_release(self.0) };
    }
}

#[napi(object)]
pub struct NodePortableMapVersion {
    pub id: Buffer,
    pub tree: NodeTreeRecord,
    pub created_at_millis: Option<String>,
    pub is_head: bool,
}

impl From<MapVersionRecord> for NodePortableMapVersion {
    fn from(value: MapVersionRecord) -> Self {
        Self {
            id: Buffer::from(value.id),
            tree: value.tree.into(),
            created_at_millis: value.created_at_millis.map(|value| value.to_string()),
            is_head: value.is_head,
        }
    }
}

#[napi(object)]
pub struct NodePortableMapUpdate {
    pub kind: String,
    pub previous: Option<Buffer>,
    pub current: Option<NodePortableMapVersion>,
}

impl From<MapUpdateRecord> for NodePortableMapUpdate {
    fn from(value: MapUpdateRecord) -> Self {
        let kind = match value.kind {
            MapUpdateKind::Applied => "applied",
            MapUpdateKind::Unchanged => "unchanged",
            MapUpdateKind::Conflict => "conflict",
        };
        Self {
            kind: kind.to_string(),
            previous: value.previous.map(Buffer::from),
            current: value.current.map(Into::into),
        }
    }
}

#[napi(object)]
pub struct NodePortableVersionPrune {
    pub retained: Vec<Buffer>,
    pub removed: Vec<Buffer>,
}

impl From<VersionPruneRecord> for NodePortableVersionPrune {
    fn from(value: VersionPruneRecord) -> Self {
        Self {
            retained: value.retained.into_iter().map(Buffer::from).collect(),
            removed: value.removed.into_iter().map(Buffer::from).collect(),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexEntry {
    pub term: Buffer,
    pub projection: Option<Buffer>,
}

#[napi(object)]
pub struct NodePortableIndexExtractRequest {
    pub primary_key: Buffer,
    pub source_value: Buffer,
}

#[napi(object)]
pub struct NodePortableIndexedVersion {
    pub source_version: Buffer,
    pub catalog_version: Option<Buffer>,
    pub index_count: String,
}

impl From<IndexedVersionRecord> for NodePortableIndexedVersion {
    fn from(value: IndexedVersionRecord) -> Self {
        Self {
            source_version: Buffer::from(value.source_version),
            catalog_version: value.catalog_version.map(Buffer::from),
            index_count: value.index_count.to_string(),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedSnapshotId {
    pub source_version: Buffer,
    pub catalog_version: Buffer,
}

impl From<IndexedSnapshotIdRecord> for NodePortableIndexedSnapshotId {
    fn from(value: IndexedSnapshotIdRecord) -> Self {
        Self {
            source_version: Buffer::from(value.source_version),
            catalog_version: Buffer::from(value.catalog_version),
        }
    }
}

impl From<NodePortableIndexedSnapshotId> for IndexedSnapshotIdRecord {
    fn from(value: NodePortableIndexedSnapshotId) -> Self {
        Self {
            source_version: value.source_version.to_vec(),
            catalog_version: value.catalog_version.to_vec(),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedUpdate {
    pub kind: String,
    pub previous_source_version: Option<Buffer>,
    pub current: Option<NodePortableIndexedVersion>,
}

impl From<IndexedUpdateRecord> for NodePortableIndexedUpdate {
    fn from(value: IndexedUpdateRecord) -> Self {
        let kind = match value.kind {
            IndexedUpdateKind::Applied => "applied",
            IndexedUpdateKind::Unchanged => "unchanged",
            IndexedUpdateKind::Conflict => "conflict",
        };
        Self {
            kind: kind.to_string(),
            previous_source_version: value.previous_source_version.map(Buffer::from),
            current: value.current.map(Into::into),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexBuildResult {
    pub source_version: Buffer,
    pub index_version: Buffer,
    pub catalog_version: Buffer,
    pub generation: String,
    pub entries: String,
    pub attempts: String,
    pub activated: bool,
}

impl From<IndexBuildResultRecord> for NodePortableIndexBuildResult {
    fn from(value: IndexBuildResultRecord) -> Self {
        Self {
            source_version: Buffer::from(value.source_version),
            index_version: Buffer::from(value.index_version),
            catalog_version: Buffer::from(value.catalog_version),
            generation: value.generation.to_string(),
            entries: value.entries.to_string(),
            attempts: value.attempts.to_string(),
            activated: value.activated,
        }
    }
}

fn index_projection_name(value: IndexProjectionRecord) -> String {
    match value {
        IndexProjectionRecord::KeysOnly => "keys_only",
        IndexProjectionRecord::Include => "include",
        IndexProjectionRecord::All => "all",
    }
    .to_string()
}

#[napi(object)]
pub struct NodePortableActiveIndexHealth {
    pub name: Buffer,
    pub generation: String,
    pub fingerprint: Buffer,
    pub projection: String,
    pub index_map_id: Buffer,
    pub index_version: Buffer,
}

impl From<ActiveIndexHealthRecord> for NodePortableActiveIndexHealth {
    fn from(value: ActiveIndexHealthRecord) -> Self {
        Self {
            name: Buffer::from(value.name),
            generation: value.generation.to_string(),
            fingerprint: Buffer::from(value.fingerprint),
            projection: index_projection_name(value.projection),
            index_map_id: Buffer::from(value.index_map_id),
            index_version: Buffer::from(value.index_version),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedMapHealth {
    pub source_map_id: Buffer,
    pub source_version: Option<Buffer>,
    pub catalog_version: Option<Buffer>,
    pub active_indexes: Vec<NodePortableActiveIndexHealth>,
    pub supports_transactions: bool,
}

impl From<IndexedMapHealthRecord> for NodePortableIndexedMapHealth {
    fn from(value: IndexedMapHealthRecord) -> Self {
        Self {
            source_map_id: Buffer::from(value.source_map_id),
            source_version: value.source_version.map(Buffer::from),
            catalog_version: value.catalog_version.map(Buffer::from),
            active_indexes: value.active_indexes.into_iter().map(Into::into).collect(),
            supports_transactions: value.supports_transactions,
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexVerification {
    pub name: Buffer,
    pub source_version: Buffer,
    pub expected_index_version: Buffer,
    pub actual_index_version: Buffer,
    pub expected_entries: String,
    pub actual_entries: String,
    pub semantic_differences: String,
    pub valid: bool,
    pub canonical: bool,
}

impl From<IndexVerificationRecord> for NodePortableIndexVerification {
    fn from(value: IndexVerificationRecord) -> Self {
        Self {
            name: Buffer::from(value.name),
            source_version: Buffer::from(value.source_version),
            expected_index_version: Buffer::from(value.expected_index_version),
            actual_index_version: Buffer::from(value.actual_index_version),
            expected_entries: value.expected_entries.to_string(),
            actual_entries: value.actual_entries.to_string(),
            semantic_differences: value.semantic_differences.to_string(),
            valid: value.valid,
            canonical: value.canonical,
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedMapMetrics {
    pub normalized_source_mutations: String,
    pub records_extracted: String,
    pub terms_emitted: String,
    pub projected_bytes: String,
    pub physical_upserts: String,
    pub physical_deletes: String,
    pub unchanged_emissions_skipped: String,
    pub source_nodes_written: String,
    pub index_nodes_written: String,
    pub catalog_nodes_written: String,
    pub retries: String,
    pub build_attempts: String,
    pub verification_outcomes: String,
    pub retained_roots: String,
}

impl From<IndexedMapMetricsRecord> for NodePortableIndexedMapMetrics {
    fn from(value: IndexedMapMetricsRecord) -> Self {
        Self {
            normalized_source_mutations: value.normalized_source_mutations.to_string(),
            records_extracted: value.records_extracted.to_string(),
            terms_emitted: value.terms_emitted.to_string(),
            projected_bytes: value.projected_bytes.to_string(),
            physical_upserts: value.physical_upserts.to_string(),
            physical_deletes: value.physical_deletes.to_string(),
            unchanged_emissions_skipped: value.unchanged_emissions_skipped.to_string(),
            source_nodes_written: value.source_nodes_written.to_string(),
            index_nodes_written: value.index_nodes_written.to_string(),
            catalog_nodes_written: value.catalog_nodes_written.to_string(),
            retries: value.retries.to_string(),
            build_attempts: value.build_attempts.to_string(),
            verification_outcomes: value.verification_outcomes.to_string(),
            retained_roots: value.retained_roots.to_string(),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedRetention {
    pub retained_source_versions: Vec<Buffer>,
    pub removed_source_versions: Vec<Buffer>,
    pub retained_index_versions: Vec<Buffer>,
    pub removed_index_versions: Vec<Buffer>,
    pub removed_catalog_versions: Vec<Buffer>,
    pub removed_checkpoint_records: String,
    pub removed_named_roots: Vec<Buffer>,
}

impl From<IndexedRetentionRecord> for NodePortableIndexedRetention {
    fn from(value: IndexedRetentionRecord) -> Self {
        let buffers = |items: Vec<Vec<u8>>| items.into_iter().map(Buffer::from).collect();
        Self {
            retained_source_versions: buffers(value.retained_source_versions),
            removed_source_versions: buffers(value.removed_source_versions),
            retained_index_versions: buffers(value.retained_index_versions),
            removed_index_versions: buffers(value.removed_index_versions),
            removed_catalog_versions: buffers(value.removed_catalog_versions),
            removed_checkpoint_records: value.removed_checkpoint_records.to_string(),
            removed_named_roots: buffers(value.removed_named_roots),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexMatch {
    pub term: Buffer,
    pub primary_key: Buffer,
    pub projection: Option<Buffer>,
}

impl From<IndexMatchRecord> for NodePortableIndexMatch {
    fn from(value: IndexMatchRecord) -> Self {
        Self {
            term: Buffer::from(value.term),
            primary_key: Buffer::from(value.primary_key),
            projection: value.projection.map(Buffer::from),
        }
    }
}

#[napi(object)]
pub struct NodePortableIndexedSource {
    pub term: Buffer,
    pub primary_key: Buffer,
    pub projection: Option<Buffer>,
    pub source_value: Buffer,
}

#[napi(object)]
pub struct NodePortableIndexPage {
    pub matches: Vec<NodePortableIndexMatch>,
    pub next_cursor: Option<Buffer>,
}

impl From<IndexPageRecord> for NodePortableIndexPage {
    fn from(value: IndexPageRecord) -> Self {
        Self {
            matches: value.matches.into_iter().map(Into::into).collect(),
            next_cursor: value.next_cursor.map(Buffer::from),
        }
    }
}

impl From<IndexedSourceRecord> for NodePortableIndexedSource {
    fn from(value: IndexedSourceRecord) -> Self {
        Self {
            term: Buffer::from(value.term),
            primary_key: Buffer::from(value.primary_key),
            projection: value.projection.map(Buffer::from),
            source_value: Buffer::from(value.source_value),
        }
    }
}

#[napi(object)]
pub struct NodePortableProximityRecord {
    pub key: Buffer,
    pub vector: Float32Array,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodePortableExactProximityRecord {
    pub vector: Vec<f64>,
    pub value: Buffer,
}

impl From<ExactProximityRecordRecord> for NodePortableExactProximityRecord {
    fn from(value: ExactProximityRecordRecord) -> Self {
        Self {
            vector: value.vector.into_iter().map(f64::from).collect(),
            value: Buffer::from(value.value),
        }
    }
}

#[napi(object)]
pub struct NodePortableProximityConfig {
    pub dimensions: u32,
    pub metric: String,
    pub log_chunk_size: u32,
    pub level_hash_seed: String,
    pub min_page_bytes: u32,
    pub target_page_bytes: u32,
    pub max_page_bytes: u32,
    pub overflow_hash_seed: String,
    pub inline_threshold_bytes: u32,
    pub scalar_quantization_group_size: Option<u32>,
}

impl From<ProximityConfigRecord> for NodePortableProximityConfig {
    fn from(value: ProximityConfigRecord) -> Self {
        Self {
            dimensions: value.dimensions,
            metric: match value.metric {
                DistanceMetricRecord::L2Squared => "l2_squared",
                DistanceMetricRecord::Cosine => "cosine",
                DistanceMetricRecord::InnerProduct => "inner_product",
            }
            .to_string(),
            log_chunk_size: u32::from(value.log_chunk_size),
            level_hash_seed: value.level_hash_seed.to_string(),
            min_page_bytes: value.min_page_bytes,
            target_page_bytes: value.target_page_bytes,
            max_page_bytes: value.max_page_bytes,
            overflow_hash_seed: value.overflow_hash_seed.to_string(),
            inline_threshold_bytes: value.inline_threshold_bytes,
            scalar_quantization_group_size: value.scalar_quantization_group_size,
        }
    }
}

#[napi(object)]
pub struct NodePortableProximityMutation {
    pub key: Buffer,
    pub vector: Option<Float32Array>,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodePortableProximityMutationStats {
    pub directory_entries_scanned: String,
    pub directory_nodes_read: String,
    pub directory_nodes_rebuilt: String,
    pub directory_nodes_written: String,
    pub directory_nodes_reused: String,
    pub directory_levels_rebuilt: String,
    pub directory_right_edge_rebuilt: bool,
    pub records_rebuilt: String,
    pub nodes_read: String,
    pub nodes_written: String,
    pub nodes_reused: String,
    pub distance_evaluations: String,
    pub full_proximity_rebuild: bool,
}

impl From<ProximityMutationStatsRecord> for NodePortableProximityMutationStats {
    fn from(value: ProximityMutationStatsRecord) -> Self {
        Self {
            directory_entries_scanned: value.directory_entries_scanned.to_string(),
            directory_nodes_read: value.directory_nodes_read.to_string(),
            directory_nodes_rebuilt: value.directory_nodes_rebuilt.to_string(),
            directory_nodes_written: value.directory_nodes_written.to_string(),
            directory_nodes_reused: value.directory_nodes_reused.to_string(),
            directory_levels_rebuilt: value.directory_levels_rebuilt.to_string(),
            directory_right_edge_rebuilt: value.directory_right_edge_rebuilt,
            records_rebuilt: value.records_rebuilt.to_string(),
            nodes_read: value.nodes_read.to_string(),
            nodes_written: value.nodes_written.to_string(),
            nodes_reused: value.nodes_reused.to_string(),
            distance_evaluations: value.distance_evaluations.to_string(),
            full_proximity_rebuild: value.full_proximity_rebuild,
        }
    }
}

#[napi]
pub struct NodePortableProximityMutationResult {
    map: Arc<BindingProximityMap>,
    stats: ProximityMutationStatsRecord,
}

#[napi]
impl NodePortableProximityMutationResult {
    #[napi]
    pub fn map(&self) -> NativePortableProximityMap {
        NativePortableProximityMap {
            inner: Arc::clone(&self.map),
        }
    }

    #[napi]
    pub fn stats(&self) -> NodePortableProximityMutationStats {
        self.stats.clone().into()
    }
}

#[napi(object)]
pub struct NodePortableProximityVerification {
    pub record_count: String,
    pub proximity_node_count: String,
    pub external_vector_count: String,
    pub quantized_node_count: String,
    pub scalar_quantizer_count: String,
    pub overflow_page_count: String,
    pub overflow_directory_count: String,
    pub maximum_level: u32,
    pub maximum_node_bytes: String,
    pub distance_checks: String,
}

impl From<ProximityVerificationRecord> for NodePortableProximityVerification {
    fn from(value: ProximityVerificationRecord) -> Self {
        Self {
            record_count: value.record_count.to_string(),
            proximity_node_count: value.proximity_node_count.to_string(),
            external_vector_count: value.external_vector_count.to_string(),
            quantized_node_count: value.quantized_node_count.to_string(),
            scalar_quantizer_count: value.scalar_quantizer_count.to_string(),
            overflow_page_count: value.overflow_page_count.to_string(),
            overflow_directory_count: value.overflow_directory_count.to_string(),
            maximum_level: u32::from(value.maximum_level),
            maximum_node_bytes: value.maximum_node_bytes.to_string(),
            distance_checks: value.distance_checks.to_string(),
        }
    }
}

#[napi(object)]
pub struct NodePortableStructuralVerification {
    pub descriptor: Buffer,
    pub object_count: String,
    pub summary: NodePortableProximityVerification,
}

#[napi(object)]
pub struct NodePortableNeighbor {
    pub key: Buffer,
    pub value: Buffer,
    pub distance: f64,
}

impl From<ProximityNeighborRecord> for NodePortableNeighbor {
    fn from(value: ProximityNeighborRecord) -> Self {
        Self {
            key: Buffer::from(value.key),
            value: Buffer::from(value.value),
            distance: value.distance,
        }
    }
}

#[napi(object)]
pub struct NodePortableSearchResult {
    pub neighbors: Vec<NodePortableNeighbor>,
    pub completion: String,
    pub backend: String,
}

#[napi(object)]
pub struct NodePortableSearchProofVerification {
    pub result: NodePortableSearchResult,
    pub claim: String,
    pub terminal_lower_bound: Option<f64>,
    pub replayed_events: String,
}

#[napi(object)]
pub struct NodePortableProofVerification {
    pub valid: bool,
    pub exists: bool,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodePortableMaintenanceSummary {
    pub item_count: String,
    pub byte_count: String,
}

#[napi(object)]
pub struct NodePortableReadScanOutcome {
    pub visited: String,
    pub stopped: bool,
}

impl From<ProximitySearchResultRecord> for NodePortableSearchResult {
    fn from(value: ProximitySearchResultRecord) -> Self {
        let completion = match value.completion {
            SearchCompletionRecord::Exact => "exact",
            SearchCompletionRecord::ApproximatePolicySatisfied => "approximate_policy_satisfied",
            SearchCompletionRecord::BudgetExhausted => "budget_exhausted",
            SearchCompletionRecord::Cancelled => "cancelled",
            SearchCompletionRecord::DeadlineExceeded => "deadline_exceeded",
        };
        let backend = match value.backend {
            SearchBackendRecord::Native => "native",
            SearchBackendRecord::ProductQuantized => "product_quantized",
            SearchBackendRecord::Hnsw => "hnsw",
            SearchBackendRecord::Composite => "composite",
            SearchBackendRecord::Auto => "auto",
        };
        Self {
            neighbors: value.neighbors.into_iter().map(Into::into).collect(),
            completion: completion.to_string(),
            backend: backend.to_string(),
        }
    }
}

type NodePortableIndexExtractor =
    FunctionRef<NodePortableIndexExtractRequest, Vec<NodePortableIndexEntry>>;

struct NodeIndexExtractor {
    env: Env,
    callback: NodePortableIndexExtractor,
}

// FunctionRef is a persistent Node reference. Index extraction is currently
// invoked synchronously on the JavaScript thread by this adapter.
unsafe impl Send for NodeIndexExtractor {}
unsafe impl Sync for NodeIndexExtractor {}

impl SecondaryIndexExtractorCallback for NodeIndexExtractor {
    fn extract(
        &self,
        primary_key: Vec<u8>,
        source_value: Vec<u8>,
    ) -> std::result::Result<Vec<IndexEntryRecord>, ProllyBindingError> {
        let function = self
            .callback
            .borrow_back(&self.env)
            .map_err(binding_callback_error)?;
        function
            .call(NodePortableIndexExtractRequest {
                primary_key: Buffer::from(primary_key),
                source_value: Buffer::from(source_value),
            })
            .map(|entries| {
                entries
                    .into_iter()
                    .map(|entry| IndexEntryRecord {
                        term: entry.term.to_vec(),
                        projection: entry.projection.map(|value| value.to_vec()),
                    })
                    .collect()
            })
            .map_err(binding_callback_error)
    }
}

fn binding_callback_error(error: impl ToString) -> ProllyBindingError {
    ProllyBindingError::Internal {
        reason: error.to_string(),
    }
}

#[napi]
pub struct NativePortableVersionedMap {
    inner: Arc<BindingVersionedMap>,
}

#[napi]
impl NativePortableVersionedMap {
    #[napi]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id())
    }

    #[napi(js_name = "isInitialized")]
    pub fn is_initialized(&self) -> Result<bool> {
        self.inner.is_initialized().map_err(to_napi_error)
    }

    #[napi]
    pub fn initialize(&self) -> Result<NodePortableMapVersion> {
        self.inner
            .initialize()
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn head(&self) -> Result<Option<NodePortableMapVersion>> {
        self.inner
            .head()
            .map(|value| value.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "headId")]
    pub fn head_id(&self) -> Result<Option<Buffer>> {
        self.inner
            .head_id()
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn version(&self, id: Buffer) -> Result<Option<NodePortableMapVersion>> {
        self.inner
            .version(id.to_vec())
            .map(|value| value.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn versions(&self) -> Result<Vec<NodePortableMapVersion>> {
        self.inner
            .versions()
            .map(|values| values.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "containsKey")]
    pub fn contains_key(&self, key: Buffer) -> Result<bool> {
        self.inner.contains_key(key.to_vec()).map_err(to_napi_error)
    }

    #[napi(js_name = "getMany")]
    pub fn get_many(&self, keys: Vec<Buffer>) -> Result<Vec<Option<Buffer>>> {
        self.inner
            .get_many(keys.into_iter().map(|value| value.to_vec()).collect())
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(Buffer::from))
                    .collect()
            })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getAt")]
    pub fn get_at(&self, id: Buffer, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get_at(id.to_vec(), key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getManyAt")]
    pub fn get_many_at(&self, id: Buffer, keys: Vec<Buffer>) -> Result<Vec<Option<Buffer>>> {
        self.inner
            .get_many_at(
                id.to_vec(),
                keys.into_iter().map(|value| value.to_vec()).collect(),
            )
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(Buffer::from))
                    .collect()
            })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn put(&self, key: Buffer, value: Buffer) -> Result<NodePortableMapVersion> {
        self.inner
            .put(key.to_vec(), value.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn apply(&self, mutations: Vec<NodeMutationRecord>) -> Result<NodePortableMapVersion> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .apply(mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "applyIf")]
    pub fn apply_if(
        &self,
        expected: Option<Buffer>,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodePortableMapUpdate> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .apply_if(expected.map(|value| value.to_vec()), mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "putIf")]
    pub fn put_if(
        &self,
        expected: Option<Buffer>,
        key: Buffer,
        value: Buffer,
    ) -> Result<NodePortableMapUpdate> {
        self.inner
            .put_if(
                expected.map(|value| value.to_vec()),
                key.to_vec(),
                value.to_vec(),
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "deleteIf")]
    pub fn delete_if(
        &self,
        expected: Option<Buffer>,
        key: Buffer,
    ) -> Result<NodePortableMapUpdate> {
        self.inner
            .delete_if(expected.map(|value| value.to_vec()), key.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn delete(&self, key: Buffer) -> Result<NodePortableMapVersion> {
        self.inner
            .delete(key.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn snapshot(&self) -> Result<Option<NativePortableMapSnapshot>> {
        self.inner
            .snapshot()
            .map(|value| value.map(|inner| NativePortableMapSnapshot { inner }))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "snapshotAt")]
    pub fn snapshot_at(&self, id: Buffer) -> Result<Option<NativePortableMapSnapshot>> {
        self.inner
            .snapshot_at(id.to_vec())
            .map(|value| value.map(|inner| NativePortableMapSnapshot { inner }))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn compare(&self, base: Buffer, target: Buffer) -> Result<NativePortableMapComparison> {
        self.inner
            .compare(base.to_vec(), target.to_vec())
            .map(|inner| NativePortableMapComparison { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "compareToHead")]
    pub fn compare_to_head(&self, base: Buffer) -> Result<NativePortableMapComparison> {
        self.inner
            .compare_to_head(base.to_vec())
            .map(|inner| NativePortableMapComparison { inner })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn backup(&self) -> Result<Buffer> {
        self.inner.backup().map(Buffer::from).map_err(to_napi_error)
    }

    #[napi(js_name = "restoreBackup")]
    pub fn restore_backup(&self, bytes: Buffer) -> Result<NodePortableMapVersion> {
        self.inner
            .restore_backup(bytes.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "keepLast")]
    pub fn keep_last(&self, count: u32) -> Result<NodePortableVersionPrune> {
        self.inner
            .keep_last(u64::from(count))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "verifyCatalog")]
    pub fn verify_catalog(&self) -> Result<NodePortableMaintenanceSummary> {
        self.inner
            .verify_catalog()
            .map(|value| NodePortableMaintenanceSummary {
                item_count: value.version_count.to_string(),
                byte_count: value.reachable_bytes.to_string(),
            })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planGc")]
    pub fn plan_gc(&self) -> Result<NodePortableMaintenanceSummary> {
        self.inner
            .plan_gc()
            .map(|value| NodePortableMaintenanceSummary {
                item_count: value.reachability.live_nodes.to_string(),
                byte_count: value.reclaimable_bytes.to_string(),
            })
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableMapComparison {
    inner: Arc<BindingMapComparison>,
}

#[napi]
impl NativePortableMapComparison {
    #[napi]
    pub fn base(&self) -> NodePortableMapVersion {
        self.inner.base().into()
    }

    #[napi]
    pub fn target(&self) -> NodePortableMapVersion {
        self.inner.target().into()
    }

    #[napi]
    pub fn diff(&self) -> Result<Vec<NodeDiffRecord>> {
        self.inner
            .diff()
            .map(|diffs| diffs.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "diffPage")]
    pub fn diff_page(
        &self,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeDiffPageRecord> {
        let limit = limit.parse::<u64>().map_err(|_| {
            Error::new(
                Status::InvalidArg,
                "diff page limit must be an unsigned 64-bit integer",
            )
        })?;
        self.inner
            .diff_page(
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableMapSnapshot {
    inner: Arc<BindingMapSnapshot>,
}

#[napi]
impl NativePortableMapSnapshot {
    #[napi]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id())
    }

    #[napi]
    pub fn version(&self) -> NodePortableMapVersion {
        self.inner.version().into()
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getMany")]
    pub fn get_many(&self, keys: Vec<Buffer>) -> Result<Vec<Option<Buffer>>> {
        self.inner
            .get_many(keys.into_iter().map(|key| key.to_vec()).collect())
            .map(|values| {
                values
                    .into_iter()
                    .map(|value| value.map(Buffer::from))
                    .collect()
            })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "containsKey")]
    pub fn contains_key(&self, key: Buffer) -> Result<bool> {
        self.inner.contains_key(key.to_vec()).map_err(to_napi_error)
    }

    #[napi(js_name = "firstEntry")]
    pub fn first_entry(&self) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .first_entry()
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "lastEntry")]
    pub fn last_entry(&self) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .last_entry()
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "lowerBound")]
    pub fn lower_bound(&self, key: Buffer) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .lower_bound(key.to_vec())
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "upperBound")]
    pub fn upper_bound(&self, key: Buffer) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .upper_bound(key.to_vec())
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn range(&self, start: Buffer, end: Option<Buffer>) -> Result<Vec<NodeEntryRecord>> {
        self.inner
            .range(start.to_vec(), end.map(|value| value.to_vec()))
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn prefix(&self, prefix: Buffer) -> Result<Vec<NodeEntryRecord>> {
        self.inner
            .prefix(prefix.to_vec())
            .map(|entries| entries.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "rangePage")]
    pub fn range_page(
        &self,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeRangePageRecord> {
        self.inner
            .range_page(
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                parse_index_page_limit(&limit)?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "prefixPage")]
    pub fn prefix_page(
        &self,
        prefix: Buffer,
        cursor: Option<NodeRangeCursorRecord>,
        limit: String,
    ) -> Result<NodeRangePageRecord> {
        self.inner
            .prefix_page(
                prefix.to_vec(),
                cursor.map(Into::into),
                parse_index_page_limit(&limit)?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "reversePage")]
    pub fn reverse_page(
        &self,
        cursor: Option<NodeReverseCursorRecord>,
        start: Buffer,
        limit: String,
    ) -> Result<NodeReversePageRecord> {
        self.inner
            .reverse_page(
                cursor.map(Into::into),
                start.to_vec(),
                parse_index_page_limit(&limit)?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "prefixReversePage")]
    pub fn prefix_reverse_page(
        &self,
        prefix: Buffer,
        cursor: Option<NodeReverseCursorRecord>,
        limit: String,
    ) -> Result<NodeReversePageRecord> {
        self.inner
            .prefix_reverse_page(
                prefix.to_vec(),
                cursor.map(Into::into),
                parse_index_page_limit(&limit)?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveKey")]
    pub fn prove_key(&self, key: Buffer) -> Result<NativePortableKeyProof> {
        self.inner
            .prove_key(key.to_vec())
            .map(|inner| NativePortableKeyProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveKeys")]
    pub fn prove_keys(&self, keys: Vec<Buffer>) -> Result<NativePortableMultiKeyProof> {
        self.inner
            .prove_keys(keys.into_iter().map(|key| key.to_vec()).collect())
            .map(|inner| NativePortableMultiKeyProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveRange")]
    pub fn prove_range(
        &self,
        start: Buffer,
        end: Option<Buffer>,
    ) -> Result<NativePortableRangeProof> {
        self.inner
            .prove_range(start.to_vec(), end.map(|value| value.to_vec()))
            .map(|inner| NativePortableRangeProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "provePrefix")]
    pub fn prove_prefix(&self, prefix: Buffer) -> Result<NativePortableRangeProof> {
        self.inner
            .prove_prefix(prefix.to_vec())
            .map(|inner| NativePortableRangeProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveRangePage")]
    pub fn prove_range_page(
        &self,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NativePortableProvedRangePage> {
        self.inner
            .prove_range_page(
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                parse_index_page_limit(&limit)?,
            )
            .map(|inner| NativePortableProvedRangePage { inner })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn stats(&self) -> Result<NodePortableMaintenanceSummary> {
        self.inner
            .stats()
            .map(|value| NodePortableMaintenanceSummary {
                item_count: value.total_key_value_pairs.to_string(),
                byte_count: value.total_tree_size_bytes.to_string(),
            })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn export(&self) -> Result<NodePortableMaintenanceSummary> {
        self.inner
            .export()
            .map(|value| NodePortableMaintenanceSummary {
                item_count: value.nodes.len().to_string(),
                byte_count: value
                    .nodes
                    .iter()
                    .map(|node| node.bytes.len() as u64)
                    .sum::<u64>()
                    .to_string(),
            })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn read(&self) -> Result<NativePortableReadSession> {
        self.inner
            .read_session()
            .map(|inner| NativePortableReadSession { inner })
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableReadSession {
    inner: Arc<ProllyReadSession>,
}

#[napi]
impl NativePortableReadSession {
    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "scanRangePages")]
    pub fn scan_range_pages(
        &self,
        env: Env,
        start: Buffer,
        end: Option<Buffer>,
        visit: FunctionRef<JsObject, u32>,
    ) -> Result<NodePortableReadScanOutcome> {
        let session_handle = self.inner.fast_handle();
        if session_handle == 0 {
            return Err(Error::new(
                Status::GenericFailure,
                "native retained read session is closed".to_string(),
            ));
        }
        let (end_ptr, end_len, has_end) = match end.as_ref() {
            Some(end) => (end.as_ptr(), end.len(), 1),
            None => (std::ptr::null(), 0, 0),
        };
        let opened = unsafe {
            prolly_fast_read_session_scan_open(
                session_handle,
                start.as_ptr(),
                start.len(),
                end_ptr,
                end_len,
                has_end,
            )
        };
        if opened.status != 0 || opened.scan_handle == 0 {
            return Err(Error::new(
                Status::GenericFailure,
                format!(
                    "native retained scan open failed with status {}",
                    opened.status
                ),
            ));
        }
        let _scan = NodeFastScanGuard(opened.scan_handle);
        let function = visit.borrow_back(&env)?;
        let mut visited = 0_u64;
        loop {
            let page = unsafe {
                prolly_fast_read_session_scan_next(
                    session_handle,
                    opened.scan_handle,
                    4096,
                    4 * 1024 * 1024,
                )
            };
            if page.status != 0 {
                return Err(Error::new(
                    Status::GenericFailure,
                    format!(
                        "native retained scan read failed with status {}",
                        page.status
                    ),
                ));
            }
            let _page = NodeFastPageGuard(page.lease_handle);
            if page.data_ptr.is_null() {
                return Err(Error::new(
                    Status::GenericFailure,
                    "native retained scan returned a null page".to_string(),
                ));
            }
            let length = usize::try_from(page.data_len).map_err(|_| {
                Error::new(
                    Status::GenericFailure,
                    "native retained scan page exceeds the host address space".to_string(),
                )
            })?;
            let borrowed = unsafe {
                env.create_buffer_with_borrowed_data(
                    page.data_ptr.cast_mut(),
                    length,
                    (),
                    |(), _env| {},
                )?
            };
            let mut argument = env.create_object()?;
            argument.set_named_property("bytes", borrowed.into_unknown())?;
            argument.set_named_property("recordCount", page.record_count)?;
            argument.set_named_property("terminal", page.terminal != 0)?;
            let consumed = function.call(argument)?;
            if consumed > page.record_count {
                return Err(Error::new(
                    Status::InvalidArg,
                    "packed scan visitor consumed more records than the page contains".to_string(),
                ));
            }
            visited = visited.checked_add(u64::from(consumed)).ok_or_else(|| {
                Error::new(
                    Status::GenericFailure,
                    "packed scan visit count overflow".to_string(),
                )
            })?;
            if consumed < page.record_count {
                return Ok(NodePortableReadScanOutcome {
                    visited: visited.to_string(),
                    stopped: true,
                });
            }
            if page.terminal != 0 {
                return Ok(NodePortableReadScanOutcome {
                    visited: visited.to_string(),
                    stopped: false,
                });
            }
            if page.record_count == 0 {
                return Err(Error::new(
                    Status::GenericFailure,
                    "non-terminal packed scan page made no progress".to_string(),
                ));
            }
        }
    }
}

#[napi]
pub struct NativePortableKeyProof {
    inner: KeyProofRecord,
}

#[napi]
impl NativePortableKeyProof {
    #[napi]
    pub fn verify(&self) -> Result<NodePortableProofVerification> {
        verify_key_proof(self.inner.clone())
            .map(|value| NodePortableProofVerification {
                valid: value.valid,
                exists: value.exists,
                value: value.value.map(Buffer::from),
            })
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableMultiKeyProof {
    inner: MultiKeyProofRecord,
}

#[napi]
impl NativePortableMultiKeyProof {
    #[napi]
    pub fn verify(&self) -> Result<NodeMultiKeyProofVerificationRecord> {
        verify_multi_key_proof(self.inner.clone())
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableRangeProof {
    inner: RangeProofRecord,
}

#[napi]
impl NativePortableRangeProof {
    #[napi]
    pub fn verify(&self) -> Result<NodeRangeProofVerificationRecord> {
        verify_range_proof(self.inner.clone())
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableProvedRangePage {
    inner: ProvedRangePageRecord,
}

#[napi]
impl NativePortableProvedRangePage {
    #[napi]
    pub fn page(&self) -> NodeRangePageRecord {
        self.inner.page.clone().into()
    }

    #[napi]
    pub fn verify(&self) -> Result<NodeRangePageProofVerificationRecord> {
        verify_range_page_proof(self.inner.proof.clone())
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableIndexRegistry {
    inner: Arc<BindingIndexRegistry>,
    env: Env,
}

#[napi]
impl NativePortableIndexRegistry {
    #[napi(constructor)]
    pub fn new(env: Env) -> Self {
        Self {
            inner: Arc::new(BindingIndexRegistry::new()),
            env,
        }
    }

    #[napi]
    pub fn register(
        &self,
        name: Buffer,
        generation: String,
        extractor_id: String,
        projection: String,
        extractor: NodePortableIndexExtractor,
    ) -> Result<()> {
        let generation = generation.parse::<u64>().map_err(|error| {
            Error::new(
                Status::InvalidArg,
                format!("invalid index generation: {error}"),
            )
        })?;
        let projection = match projection.as_str() {
            "keys_only" => IndexProjectionRecord::KeysOnly,
            "include" => IndexProjectionRecord::Include,
            "all" => IndexProjectionRecord::All,
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "projection must be keys_only, include, or all".to_string(),
                ))
            }
        };
        self.inner
            .register(
                name.to_vec(),
                generation,
                extractor_id,
                projection,
                None,
                Arc::new(NodeIndexExtractor {
                    env: self.env,
                    callback: extractor,
                }),
            )
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableIndexedMap {
    inner: Arc<BindingIndexedMap>,
}

#[napi]
impl NativePortableIndexedMap {
    #[napi]
    pub fn id(&self) -> Buffer {
        Buffer::from(self.inner.id())
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn put(&self, key: Buffer, value: Buffer) -> Result<NodePortableIndexedVersion> {
        self.inner
            .put(key.to_vec(), value.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn apply(&self, mutations: Vec<NodeMutationRecord>) -> Result<NodePortableIndexedVersion> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .apply(mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "applyIf")]
    pub fn apply_if(
        &self,
        expected_source: Option<Buffer>,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodePortableIndexedUpdate> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .apply_if(expected_source.map(|value| value.to_vec()), mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn delete(&self, key: Buffer) -> Result<NodePortableIndexedVersion> {
        self.inner
            .delete(key.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "ensureIndex")]
    pub fn ensure_index(&self, name: Buffer) -> Result<NodePortableIndexBuildResult> {
        self.inner
            .ensure_index(name.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn snapshot(&self) -> Result<NativePortableIndexedSnapshot> {
        self.inner
            .snapshot()
            .map(|inner| NativePortableIndexedSnapshot { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "snapshotAt")]
    pub fn snapshot_at(&self, source_version: Buffer) -> Result<NativePortableIndexedSnapshot> {
        self.inner
            .snapshot_at(source_version.to_vec())
            .map(|inner| NativePortableIndexedSnapshot { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "snapshotById")]
    pub fn snapshot_by_id(
        &self,
        snapshot_id: NodePortableIndexedSnapshotId,
    ) -> Result<NativePortableIndexedSnapshot> {
        self.inner
            .snapshot_by_id(snapshot_id.into())
            .map(|inner| NativePortableIndexedSnapshot { inner })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn health(&self) -> Result<NodePortableIndexedMapHealth> {
        self.inner.health().map(Into::into).map_err(to_napi_error)
    }

    #[napi]
    pub fn metrics(&self) -> Result<NodePortableIndexedMapMetrics> {
        self.inner.metrics().map(Into::into).map_err(to_napi_error)
    }

    #[napi(js_name = "verifyIndex")]
    pub fn verify_index(
        &self,
        name: Buffer,
        source_version: Buffer,
    ) -> Result<NodePortableIndexVerification> {
        self.inner
            .verify_index(name.to_vec(), source_version.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "verifyAll")]
    pub fn verify_all(&self, source_version: Buffer) -> Result<Vec<NodePortableIndexVerification>> {
        self.inner
            .verify_all(source_version.to_vec())
            .map(|items| items.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "repairIndex")]
    pub fn repair_index(
        &self,
        name: Buffer,
        source_version: Buffer,
    ) -> Result<NodePortableIndexVerification> {
        self.inner
            .repair_index(name.to_vec(), source_version.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "deactivateIndex")]
    pub fn deactivate_index(&self, name: Buffer) -> Result<NodePortableIndexedVersion> {
        self.inner
            .deactivate_index(name.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "exportCurrent")]
    pub fn export_current(&self) -> Result<Buffer> {
        self.inner
            .export_current()
            .map(Buffer::from)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "importCurrent")]
    pub fn import_current(
        &self,
        bundle: Buffer,
        expected_source: Option<Buffer>,
    ) -> Result<NodePortableIndexedVersion> {
        self.inner
            .import_current(bundle.to_vec(), expected_source.map(|value| value.to_vec()))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "keepLast")]
    pub fn keep_last(&self, count: String) -> Result<NodePortableIndexedRetention> {
        let count = count
            .parse::<u64>()
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        self.inner
            .keep_last(count)
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableIndexedSnapshot {
    inner: Arc<BindingIndexedSnapshot>,
}

#[napi]
impl NativePortableIndexedSnapshot {
    #[napi]
    pub fn id(&self) -> NodePortableIndexedSnapshotId {
        self.inner.id().into()
    }

    #[napi]
    pub fn index(&self, name: Buffer) -> Result<NativePortableSecondaryIndex> {
        self.inner
            .index(name.to_vec())
            .map(|inner| NativePortableSecondaryIndex { inner })
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableSecondaryIndex {
    inner: Arc<BindingSecondaryIndexSnapshot>,
}

#[napi]
impl NativePortableSecondaryIndex {
    #[napi]
    pub fn name(&self) -> Buffer {
        Buffer::from(self.inner.name())
    }

    #[napi(js_name = "fastHandle")]
    pub fn fast_handle(&self) -> String {
        self.inner.fast_handle().to_string()
    }

    #[napi]
    pub fn exact(&self, term: Buffer) -> Result<Vec<NodePortableIndexMatch>> {
        self.inner
            .exact(term.to_vec())
            .map(|values| values.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn prefix(&self, prefix: Buffer) -> Result<Vec<NodePortableIndexMatch>> {
        self.inner
            .prefix(prefix.to_vec())
            .map(|values| values.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn range(&self, start: Buffer, end: Option<Buffer>) -> Result<Vec<NodePortableIndexMatch>> {
        self.inner
            .range(start.to_vec(), end.map(|value| value.to_vec()))
            .map(|values| values.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn records(&self, term: Buffer) -> Result<Vec<NodePortableIndexedSource>> {
        self.inner
            .records(term.to_vec())
            .map(|values| values.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "exactPage")]
    pub fn exact_page(
        &self,
        term: Buffer,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = limit.parse::<u64>().map_err(|error| {
            Error::new(Status::InvalidArg, format!("invalid page limit: {error}"))
        })?;
        self.inner
            .exact_page(term.to_vec(), cursor.map(|value| value.to_vec()), limit)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "exactReversePage")]
    pub fn exact_reverse_page(
        &self,
        term: Buffer,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = parse_index_page_limit(&limit)?;
        self.inner
            .exact_reverse_page(term.to_vec(), cursor.map(|value| value.to_vec()), limit)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "prefixPage")]
    pub fn prefix_page(
        &self,
        prefix: Buffer,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = parse_index_page_limit(&limit)?;
        self.inner
            .prefix_page(prefix.to_vec(), cursor.map(|value| value.to_vec()), limit)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "prefixReversePage")]
    pub fn prefix_reverse_page(
        &self,
        prefix: Buffer,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = parse_index_page_limit(&limit)?;
        self.inner
            .prefix_reverse_page(prefix.to_vec(), cursor.map(|value| value.to_vec()), limit)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "rangePage")]
    pub fn range_page(
        &self,
        start: Buffer,
        end: Option<Buffer>,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = parse_index_page_limit(&limit)?;
        self.inner
            .range_page(
                start.to_vec(),
                end.map(|value| value.to_vec()),
                cursor.map(|value| value.to_vec()),
                limit,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "rangeReversePage")]
    pub fn range_reverse_page(
        &self,
        start: Buffer,
        end: Option<Buffer>,
        cursor: Option<Buffer>,
        limit: String,
    ) -> Result<NodePortableIndexPage> {
        let limit = parse_index_page_limit(&limit)?;
        self.inner
            .range_reverse_page(
                start.to_vec(),
                end.map(|value| value.to_vec()),
                cursor.map(|value| value.to_vec()),
                limit,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }
}

fn parse_index_page_limit(limit: &str) -> Result<u64> {
    limit
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, format!("invalid page limit: {error}")))
}

#[napi]
pub struct NativePortableProximityMap {
    inner: Arc<BindingProximityMap>,
}

#[napi]
impl NativePortableProximityMap {
    #[napi]
    pub fn read(&self) -> Result<NativePortableProximityReadSession> {
        self.inner
            .read_session()
            .map(|inner| NativePortableProximityReadSession { inner })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn count(&self) -> Result<String> {
        self.inner
            .count()
            .map(|value| value.to_string())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn config(&self) -> Result<NodePortableProximityConfig> {
        self.inner.config().map(Into::into).map_err(to_napi_error)
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<NodePortableExactProximityRecord>> {
        self.inner
            .get(key.to_vec())
            .map(|record| record.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn contains(&self, key: Buffer) -> Result<bool> {
        self.inner.contains_key(key.to_vec()).map_err(to_napi_error)
    }

    #[napi]
    pub fn search(&self, query: Float32Array, k: String) -> Result<NodePortableSearchResult> {
        let k = k.parse::<u64>().map_err(|error| {
            Error::new(Status::InvalidArg, format!("invalid top-k value: {error}"))
        })?;
        self.inner
            .search(exact_proximity_search_request(query.to_vec(), k))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn descriptor(&self) -> Buffer {
        Buffer::from(self.inner.descriptor())
    }

    #[napi]
    pub fn verify(&self) -> Result<NodePortableProximityVerification> {
        self.inner.verify().map(Into::into).map_err(to_napi_error)
    }

    #[napi]
    pub fn mutate(
        &self,
        mutations: Vec<NodePortableProximityMutation>,
    ) -> Result<NodePortableProximityMutationResult> {
        self.inner
            .mutate(
                mutations
                    .into_iter()
                    .map(|value| ProximityMutationRecord {
                        key: value.key.to_vec(),
                        vector: value.vector.map(|vector| vector.to_vec()),
                        value: value.value.map(|value| value.to_vec()),
                    })
                    .collect(),
            )
            .map(|value| NodePortableProximityMutationResult {
                map: value.map,
                stats: value.stats,
            })
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn rebuild(
        &self,
        mutations: Vec<NodePortableProximityMutation>,
    ) -> Result<NativePortableProximityMap> {
        self.inner
            .rebuild(
                mutations
                    .into_iter()
                    .map(|value| ProximityMutationRecord {
                        key: value.key.to_vec(),
                        vector: value.vector.map(|vector| vector.to_vec()),
                        value: value.value.map(|value| value.to_vec()),
                    })
                    .collect(),
            )
            .map(|inner| NativePortableProximityMap { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveMembership")]
    pub fn prove_membership(&self, key: Buffer) -> Result<NativePortableProximityProof> {
        self.inner
            .prove_membership(key.to_vec())
            .map(|inner| NativePortableProximityProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveStructure")]
    pub fn prove_structure(&self) -> Result<NativePortableProximityStructuralProof> {
        self.inner
            .prove_structure(default_content_graph_limits())
            .map(|inner| NativePortableProximityStructuralProof { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveSearch")]
    pub fn prove_search(
        &self,
        query: Float32Array,
        k: String,
    ) -> Result<NativePortableProximitySearchProof> {
        let k = k.parse::<u64>().map_err(|error| {
            Error::new(Status::InvalidArg, format!("invalid top-k value: {error}"))
        })?;
        self.inner
            .prove_search(
                exact_proximity_search_request(query.to_vec(), k),
                default_content_graph_limits(),
            )
            .map(|inner| NativePortableProximitySearchProof { inner })
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableProximityStructuralProof {
    inner: ProximityStructuralProofRecord,
}

#[napi]
impl NativePortableProximityStructuralProof {
    #[napi]
    pub fn verify(
        &self,
        expected_descriptor: Option<Buffer>,
    ) -> Result<NodePortableStructuralVerification> {
        verify_proximity_structure_proof(
            self.inner.clone(),
            expected_descriptor.map(|value| value.to_vec()),
            default_content_graph_limits(),
        )
        .map(|value| NodePortableStructuralVerification {
            descriptor: Buffer::from(value.descriptor),
            object_count: value.object_count.to_string(),
            summary: value.summary.into(),
        })
        .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableProximityReadSession {
    inner: Arc<BindingProximityReadSession>,
}

#[napi]
impl NativePortableProximityReadSession {
    #[napi]
    pub fn search(&self, query: Float32Array, k: String) -> Result<NodePortableSearchResult> {
        let k = k.parse::<u64>().map_err(|error| {
            Error::new(Status::InvalidArg, format!("invalid top-k value: {error}"))
        })?;
        self.inner
            .search(exact_proximity_search_request(query.to_vec(), k))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<NodePortableExactProximityRecord>> {
        self.inner
            .get(key.to_vec())
            .map(|record| record.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn contains(&self, key: Buffer) -> Result<bool> {
        self.inner.contains_key(key.to_vec()).map_err(to_napi_error)
    }

    #[napi(js_name = "fastHandle")]
    pub fn fast_handle(&self) -> String {
        self.inner.fast_handle().to_string()
    }
}

#[napi]
pub struct NativePortableProximityProof {
    inner: ProximityMembershipProofRecord,
}

#[napi]
impl NativePortableProximityProof {
    #[napi]
    pub fn verify(&self, expected_descriptor: Option<Buffer>) -> Result<Option<Buffer>> {
        verify_proximity_membership_proof(
            self.inner.clone(),
            expected_descriptor.map(|value| value.to_vec()),
        )
        .map(|value| value.record.map(|record| Buffer::from(record.value)))
        .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativePortableProximitySearchProof {
    inner: Arc<BindingProximitySearchProof>,
}

#[napi]
impl NativePortableProximitySearchProof {
    #[napi]
    pub fn verify(
        &self,
        expected_descriptor: Option<Buffer>,
    ) -> Result<NodePortableSearchProofVerification> {
        self.inner
            .verify(
                expected_descriptor.map(|value| value.to_vec()),
                default_content_graph_limits(),
            )
            .map(|value| NodePortableSearchProofVerification {
                result: value.result.into(),
                claim: match value.claim.kind {
                    ProximitySearchClaimKindRecord::ExactL2Optimal => "exact_l2_optimal",
                    ProximitySearchClaimKindRecord::HonestExecution => "honest_execution",
                }
                .to_string(),
                terminal_lower_bound: value.claim.terminal_lower_bound,
                replayed_events: value.replayed_events.to_string(),
            })
            .map_err(to_napi_error)
    }
}

#[napi]
impl NativeProllyEngine {
    #[napi(js_name = "versionedMap")]
    pub fn portable_versioned_map(&self, id: Buffer) -> Result<NativePortableVersionedMap> {
        self.inner
            .versioned_map(id.to_vec())
            .map(|inner| NativePortableVersionedMap { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "indexedMap")]
    pub fn portable_indexed_map(
        &self,
        id: Buffer,
        registry: &NativePortableIndexRegistry,
    ) -> Result<NativePortableIndexedMap> {
        self.inner
            .indexed_map(id.to_vec(), Arc::clone(&registry.inner))
            .map(|inner| NativePortableIndexedMap { inner })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "buildProximity")]
    pub fn portable_build_proximity(
        &self,
        dimensions: u32,
        records: Vec<NodePortableProximityRecord>,
    ) -> Result<NativePortableProximityMap> {
        let records = records
            .into_iter()
            .map(|record| ProximityRecordRecord {
                key: record.key.to_vec(),
                vector: record.vector.to_vec(),
                value: record.value.to_vec(),
            })
            .collect();
        self.inner
            .build_proximity_map(default_proximity_config(dimensions), records, None)
            .map(|inner| NativePortableProximityMap { inner })
            .map_err(to_napi_error)
    }
}
