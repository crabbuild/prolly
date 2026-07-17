use super::{to_napi_error, NativeProllyEngine, NodeTreeRecord};
use napi::bindgen_prelude::{Buffer, Env, Error, Float32Array, FunctionRef, Result, Status};
use napi_derive::napi;
use prolly_bindings::{
    default_content_graph_limits, default_proximity_config, exact_proximity_search_request,
    verify_key_proof, verify_proximity_membership_proof, BindingIndexRegistry, BindingIndexedMap,
    BindingIndexedSnapshot, BindingMapSnapshot, BindingProximityMap, BindingProximitySearchProof,
    BindingSecondaryIndexSnapshot, BindingVersionedMap, IndexBuildResultRecord, IndexEntryRecord,
    IndexMatchRecord, IndexPageRecord, IndexProjectionRecord, IndexedSourceRecord,
    IndexedVersionRecord, KeyProofRecord, MapVersionRecord, ProllyBindingError, ProllyReadSession,
    ProximityMembershipProofRecord, ProximityNeighborRecord, ProximityRecordRecord,
    ProximitySearchClaimKindRecord, ProximitySearchResultRecord, SearchBackendRecord,
    SearchCompletionRecord, SecondaryIndexExtractorCallback,
};
use std::sync::Arc;

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
    pub fn initialize(&self) -> Result<NodePortableMapVersion> {
        self.inner
            .initialize()
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
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

    #[napi]
    pub fn backup(&self) -> Result<Buffer> {
        self.inner.backup().map(Buffer::from).map_err(to_napi_error)
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
pub struct NativePortableMapSnapshot {
    inner: Arc<BindingMapSnapshot>,
}

#[napi]
impl NativePortableMapSnapshot {
    #[napi]
    pub fn get(&self, key: Buffer) -> Result<Option<Buffer>> {
        self.inner
            .get(key.to_vec())
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveKey")]
    pub fn prove_key(&self, key: Buffer) -> Result<NativePortableKeyProof> {
        self.inner
            .prove_key(key.to_vec())
            .map(|inner| NativePortableKeyProof { inner })
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

    #[napi]
    pub fn metrics(&self) -> Result<NodePortableMaintenanceSummary> {
        self.inner
            .metrics()
            .map(|value| NodePortableMaintenanceSummary {
                item_count: value.build_attempts.to_string(),
                byte_count: value.projected_bytes.to_string(),
            })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "verifyIndex")]
    pub fn verify_index(&self, name: Buffer, source_version: Buffer) -> Result<bool> {
        self.inner
            .verify_index(name.to_vec(), source_version.to_vec())
            .map(|value| value.valid)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "exportCurrent")]
    pub fn export_current(&self) -> Result<Buffer> {
        self.inner
            .export_current()
            .map(Buffer::from)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "keepLast")]
    pub fn keep_last(&self, count: String) -> Result<String> {
        let count = count
            .parse::<u64>()
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        self.inner
            .keep_last(count)
            .map(|value| value.retained_source_versions.len().to_string())
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
}

#[napi]
pub struct NativePortableProximityMap {
    inner: Arc<BindingProximityMap>,
}

#[napi]
impl NativePortableProximityMap {
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
    pub fn verify(&self) -> Result<String> {
        self.inner
            .verify()
            .map(|value| value.record_count.to_string())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "proveMembership")]
    pub fn prove_membership(&self, key: Buffer) -> Result<NativePortableProximityProof> {
        self.inner
            .prove_membership(key.to_vec())
            .map(|inner| NativePortableProximityProof { inner })
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
