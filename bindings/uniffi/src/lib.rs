use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};

#[cfg(feature = "sqlite")]
use prolly::SqliteStore;
use prolly::{
    self, is_boundary_config as core_is_boundary_config, AuthenticatedProofBundleVerification,
    AuthenticatedProofEnvelope, AuthenticatedProofEnvelopeVerification, BatchApplyResult,
    BatchApplyStats, BatchOp, BlobGcPlan, BlobGcReachability, BlobGcSweep, BlobRef, BlobStore,
    BlobStoreScan, ChangedSpan, ChangedSpanHint, Cid, Config, Conflict, CrdtConfig, CursorWindow,
    DeletePolicy, Diff, DiffPageProof, DiffPageProofVerification, DiffTraversalStats, Encoding,
    FileBlobStore, FileNodeStore, GcPlan, GcReachability, GcSweep, KeyBuilder, KeyProof,
    KeyProofVerification, LargeValueConfig, ManifestStore, ManifestStoreScan, ManifestUpdate,
    MemBlobStore, MemStore, MergeFallbackReason, MergeFastPath, MergePolicyFn,
    MergePolicyRegistry as CoreMergePolicyRegistry, MergeResolutionKind as CoreMergeResolutionKind,
    MergeReuseReason, MergeTrace, MergeTraceEvent, MergeTraceStage, MissingNodeCopy,
    MissingNodePlan, MultiKeyProof, MultiKeyProofVerification, MultiValueSet, Mutation,
    NamedRootManifest, NamedRootRetention, NamedRootUpdate, Node, NodeStoreScan, ParallelConfig,
    Prolly, ProllyMetricsSnapshot, ProofBundleSummary, ProofBundleVerification, ProvedDiffPage,
    ProvedRangePage, RangeCursor, RangePageProof, RangePageProofVerification, RangeProof,
    RangeProofVerification, Resolver, ReverseCursor, RootManifest,
    SnapshotBundle as CoreSnapshotBundle, SnapshotBundleNode as CoreSnapshotBundleNode,
    SnapshotBundleSummary, SnapshotBundleVerification, SnapshotNamespace, SnapshotRoot,
    SnapshotSelection, StatsComparison, StatsDiff, StatsPercentageChange, Store,
    StructuralDiffCursor, StructuralDiffMarker, StructuralDiffPage, TimestampedValue, Tombstone,
    Tree, TreeDebugComparedNode, TreeDebugComparison, TreeDebugComparisonLevel, TreeDebugLevel,
    TreeDebugNode, TreeDebugNodeStatus, TreeDebugView, TreeStats, ValueRef, VersionedValue,
};
use serde::Serialize;
use thiserror::Error;

type MemoryEngine = Prolly<Arc<MemStore>>;
type FileEngine = Prolly<Arc<FileNodeStore>>;
#[cfg(feature = "sqlite")]
type SqliteEngine = Prolly<Arc<SqliteStore>>;
type HostEngine = Prolly<Arc<HostStore>>;

enum BindingEngine {
    Memory(MemoryEngine),
    File(FileEngine),
    #[cfg(feature = "sqlite")]
    Sqlite(SqliteEngine),
    Host(HostEngine),
}

type MemoryBlobStore = Arc<MemBlobStore>;
type FileBlobStoreHandle = Arc<FileBlobStore>;

enum BindingBlobStore {
    Memory(MemoryBlobStore),
    File(FileBlobStoreHandle),
}

macro_rules! with_engine {
    ($self:expr, $engine:ident, $body:block) => {
        match &$self.inner {
            BindingEngine::Memory($engine) => $body,
            BindingEngine::File($engine) => $body,
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite($engine) => $body,
            BindingEngine::Host($engine) => $body,
        }
    };
}

macro_rules! with_blob_store {
    ($self:expr, $store:ident, $body:block) => {
        match &$self.inner {
            BindingBlobStore::Memory($store) => $body,
            BindingBlobStore::File($store) => $body,
        }
    };
}

macro_rules! with_engine_and_blob_store {
    ($engine_holder:expr, $blob_holder:expr, $engine:ident, $blob_store:ident, $body:block) => {
        match (&$engine_holder.inner, &$blob_holder.inner) {
            (BindingEngine::Memory($engine), BindingBlobStore::Memory($blob_store)) => $body,
            (BindingEngine::Memory($engine), BindingBlobStore::File($blob_store)) => $body,
            (BindingEngine::File($engine), BindingBlobStore::Memory($blob_store)) => $body,
            (BindingEngine::File($engine), BindingBlobStore::File($blob_store)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($engine), BindingBlobStore::Memory($blob_store)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($engine), BindingBlobStore::File($blob_store)) => $body,
            (BindingEngine::Host($engine), BindingBlobStore::Memory($blob_store)) => $body,
            (BindingEngine::Host($engine), BindingBlobStore::File($blob_store)) => $body,
        }
    };
}

macro_rules! with_engine_pair {
    ($self:expr, $other:expr, $source:ident, $destination:ident, $body:block) => {
        match (&$self.inner, &$other.inner) {
            (BindingEngine::Memory($source), BindingEngine::Memory($destination)) => $body,
            (BindingEngine::Memory($source), BindingEngine::File($destination)) => $body,
            (BindingEngine::Memory($source), BindingEngine::Host($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Memory($source), BindingEngine::Sqlite($destination)) => $body,
            (BindingEngine::File($source), BindingEngine::Memory($destination)) => $body,
            (BindingEngine::File($source), BindingEngine::File($destination)) => $body,
            (BindingEngine::File($source), BindingEngine::Host($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::File($source), BindingEngine::Sqlite($destination)) => $body,
            (BindingEngine::Host($source), BindingEngine::Memory($destination)) => $body,
            (BindingEngine::Host($source), BindingEngine::File($destination)) => $body,
            (BindingEngine::Host($source), BindingEngine::Host($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Host($source), BindingEngine::Sqlite($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($source), BindingEngine::Memory($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($source), BindingEngine::File($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($source), BindingEngine::Sqlite($destination)) => $body,
            #[cfg(feature = "sqlite")]
            (BindingEngine::Sqlite($source), BindingEngine::Host($destination)) => $body,
        }
    };
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum EncodingKind {
    Raw,
    Cbor,
    Json,
    Custom,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct EncodingRecord {
    pub kind: EncodingKind,
    pub custom_name: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ConfigRecord {
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub chunking_factor: u32,
    pub hash_seed: u64,
    pub encoding: EncodingRecord,
    pub node_cache_max_nodes: Option<u64>,
    pub node_cache_max_bytes: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ParallelConfigRecord {
    pub max_threads: u64,
    pub parallelism_threshold: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TreeRecord {
    pub root: Option<Vec<u8>>,
    pub config: ConfigRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotBundleNodeRecord {
    pub cid: Vec<u8>,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotBundleRecord {
    pub format_version: u32,
    pub tree: TreeRecord,
    pub nodes: Vec<SnapshotBundleNodeRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotBundleSummaryRecord {
    pub format_version: u32,
    pub root: Option<Vec<u8>>,
    pub node_count: u64,
    pub byte_count: u64,
    pub min_node_bytes: u64,
    pub max_node_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotBundleVerificationRecord {
    pub valid: bool,
    pub summary: SnapshotBundleSummaryRecord,
    pub reachable_nodes: u64,
    pub reachable_bytes: u64,
    pub missing_cids: Vec<Vec<u8>>,
    pub extra_cids: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct EntryRecord {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MutationKind {
    Upsert,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MutationRecord {
    pub kind: MutationKind,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BatchApplyStatsRecord {
    pub input_mutations: u64,
    pub effective_mutations: u64,
    pub preprocess_input_sorted: bool,
    pub affected_leaves: u64,
    pub changed_leaves: u64,
    pub sparse_leaf_applies: u64,
    pub written_nodes: u64,
    pub written_bytes: u64,
    pub used_append_fast_path: bool,
    pub used_batched_route: bool,
    pub used_coalesced_rebuild: bool,
    pub used_deferred_rebalancing: bool,
    pub used_bottom_up_rebuild: bool,
    pub cache_written_nodes: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BatchApplyResultRecord {
    pub tree: TreeRecord,
    pub stats: BatchApplyStatsRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TreeStatsLevelU64Record {
    pub level: u8,
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeStatsLevelF64Record {
    pub level: u8,
    pub value: f64,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeStatsRecord {
    pub num_nodes: u64,
    pub num_leaves: u64,
    pub num_internal_nodes: u64,
    pub tree_height: u8,
    pub total_key_value_pairs: u64,
    pub total_tree_size_bytes: u64,
    pub avg_node_size_bytes: f64,
    pub min_node_size_bytes: u64,
    pub max_node_size_bytes: u64,
    pub avg_entries_per_node: f64,
    pub nodes_per_level: Vec<TreeStatsLevelU64Record>,
    pub avg_node_size_per_level: Vec<TreeStatsLevelF64Record>,
    pub avg_entries_per_level: Vec<TreeStatsLevelF64Record>,
    pub min_entries_per_level: Vec<TreeStatsLevelU64Record>,
    pub max_entries_per_level: Vec<TreeStatsLevelU64Record>,
    pub avg_fanout: f64,
    pub min_fanout: u64,
    pub max_fanout: u64,
    pub avg_fill_factor: f64,
    pub avg_leaf_fill_factor: f64,
    pub avg_internal_fill_factor: f64,
    pub avg_key_size_bytes: f64,
    pub avg_value_size_bytes: f64,
    pub min_key_size_bytes: u64,
    pub max_key_size_bytes: u64,
    pub min_value_size_bytes: u64,
    pub max_value_size_bytes: u64,
    pub total_keys_size_bytes: u64,
    pub total_values_size_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct StatsDiffRecord {
    pub num_nodes_diff: i64,
    pub num_leaves_diff: i64,
    pub num_internal_nodes_diff: i64,
    pub tree_height_diff: i8,
    pub total_key_value_pairs_diff: i64,
    pub total_tree_size_bytes_diff: i64,
    pub avg_node_size_bytes_diff: f64,
    pub min_node_size_bytes_diff: i64,
    pub max_node_size_bytes_diff: i64,
    pub avg_entries_per_node_diff: f64,
    pub avg_fanout_diff: f64,
    pub min_fanout_diff: i64,
    pub max_fanout_diff: i64,
    pub avg_fill_factor_diff: f64,
    pub avg_leaf_fill_factor_diff: f64,
    pub avg_internal_fill_factor_diff: f64,
    pub avg_key_size_bytes_diff: f64,
    pub avg_value_size_bytes_diff: f64,
    pub min_key_size_bytes_diff: i64,
    pub max_key_size_bytes_diff: i64,
    pub min_value_size_bytes_diff: i64,
    pub max_value_size_bytes_diff: i64,
    pub total_keys_size_bytes_diff: i64,
    pub total_values_size_bytes_diff: i64,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct StatsPercentageChangeRecord {
    pub num_nodes_pct: f64,
    pub num_leaves_pct: f64,
    pub num_internal_nodes_pct: f64,
    pub tree_height_pct: f64,
    pub total_key_value_pairs_pct: f64,
    pub total_tree_size_bytes_pct: f64,
    pub avg_node_size_bytes_pct: f64,
    pub min_node_size_bytes_pct: f64,
    pub max_node_size_bytes_pct: f64,
    pub avg_entries_per_node_pct: f64,
    pub avg_fanout_pct: f64,
    pub min_fanout_pct: f64,
    pub max_fanout_pct: f64,
    pub avg_fill_factor_pct: f64,
    pub avg_leaf_fill_factor_pct: f64,
    pub avg_internal_fill_factor_pct: f64,
    pub avg_key_size_bytes_pct: f64,
    pub avg_value_size_bytes_pct: f64,
    pub min_key_size_bytes_pct: f64,
    pub max_key_size_bytes_pct: f64,
    pub min_value_size_bytes_pct: f64,
    pub max_value_size_bytes_pct: f64,
    pub total_keys_size_bytes_pct: f64,
    pub total_values_size_bytes_pct: f64,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct StatsComparisonRecord {
    pub before: TreeStatsRecord,
    pub after: TreeStatsRecord,
    pub absolute: StatsDiffRecord,
    pub percentage: StatsPercentageChangeRecord,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugNodeRecord {
    pub cid: Vec<u8>,
    pub leaf: bool,
    pub level: u8,
    pub entry_count: u64,
    pub max_entries: u64,
    pub fill_factor: f64,
    pub encoded_bytes: u64,
    pub first_key: Option<Vec<u8>>,
    pub last_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugLevelRecord {
    pub level: u8,
    pub nodes: Vec<TreeDebugNodeRecord>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugViewRecord {
    pub levels: Vec<TreeDebugLevelRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum TreeDebugNodeStatusKind {
    Shared,
    LeftOnly,
    RightOnly,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugComparedNodeRecord {
    pub status: TreeDebugNodeStatusKind,
    pub node: TreeDebugNodeRecord,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugComparisonLevelRecord {
    pub level: u8,
    pub shared_nodes: u64,
    pub left_only_nodes: u64,
    pub right_only_nodes: u64,
    pub shared_bytes: u64,
    pub left_only_bytes: u64,
    pub right_only_bytes: u64,
    pub nodes: Vec<TreeDebugComparedNodeRecord>,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct TreeDebugComparisonRecord {
    pub shared_nodes: u64,
    pub left_only_nodes: u64,
    pub right_only_nodes: u64,
    pub shared_bytes: u64,
    pub left_only_bytes: u64,
    pub right_only_bytes: u64,
    pub levels: Vec<TreeDebugComparisonLevelRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeTraceEventKind {
    FastPath,
    StructuralMergeStarted,
    ReusedSubtree,
    RewrittenNode,
    ResolverCalled,
    Fallback,
    DiffTraversal,
    BatchMerge,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeFastPathKind {
    BranchesEqual,
    LeftUnchanged,
    RightUnchanged,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeReuseReasonKind {
    BranchesEqual,
    LeftUnchanged,
    RightUnchanged,
    UnchangedAfterMerge,
    MatchesLeft,
    MatchesRight,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeTraceStageKind {
    Structural,
    Batch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeTraceResolutionKind {
    Value,
    Delete,
    Unresolved,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MergeFallbackReasonKind {
    MissingRoot,
    ShapeMismatch,
    NodeLengthMismatch,
    ChildFallback,
    DeleteResolution,
    DiffBatch,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MergeTraceEventRecord {
    pub kind: MergeTraceEventKind,
    pub fast_path: Option<MergeFastPathKind>,
    pub cid: Option<Vec<u8>>,
    pub reuse_reason: Option<MergeReuseReasonKind>,
    pub level: Option<u64>,
    pub entries: Option<u64>,
    pub first_key: Option<Vec<u8>>,
    pub last_key: Option<Vec<u8>>,
    pub stage: Option<MergeTraceStageKind>,
    pub key: Option<Vec<u8>>,
    pub resolution: Option<MergeTraceResolutionKind>,
    pub fallback_reason: Option<MergeFallbackReasonKind>,
    pub diff_stats: Option<DiffTraversalStatsRecord>,
    pub right_changes: Option<u64>,
    pub mutations: Option<u64>,
    pub append_only: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MergeTraceRecord {
    pub events: Vec<MergeTraceEventRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum DiffKind {
    Added,
    Removed,
    Changed,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DiffRecord {
    pub kind: DiffKind,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub old_value: Option<Vec<u8>>,
    pub new_value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangeCursorRecord {
    pub after_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangePageRecord {
    pub entries: Vec<EntryRecord>,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ReverseCursorRecord {
    pub before_key: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ReversePageRecord {
    pub entries: Vec<EntryRecord>,
    pub next_cursor: Option<ReverseCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CursorWindowRecord {
    pub position_key: Option<Vec<u8>>,
    pub position_value: Option<Vec<u8>>,
    pub found: bool,
    pub entries: Vec<EntryRecord>,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DiffPageRecord {
    pub diffs: Vec<DiffRecord>,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ConflictRecord {
    pub key: Vec<u8>,
    pub base: Option<Vec<u8>>,
    pub left: Option<Vec<u8>>,
    pub right: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ResolutionKind {
    Value,
    Delete,
    Unresolved,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ResolutionRecord {
    pub kind: ResolutionKind,
    pub value: Option<Vec<u8>>,
}

#[uniffi::export(with_foreign)]
pub trait MergeResolverCallback: Send + Sync {
    fn resolve(&self, conflict: ConflictRecord) -> ResolutionRecord;
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CrdtResolutionKind {
    Value,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CrdtResolutionRecord {
    pub kind: CrdtResolutionKind,
    pub value: Option<Vec<u8>>,
}

#[uniffi::export(with_foreign)]
pub trait CrdtResolverCallback: Send + Sync {
    fn resolve(&self, conflict: ConflictRecord) -> CrdtResolutionRecord;
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreBytesResultRecord {
    pub value: Option<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreUnitResultRecord {
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreBoolResultRecord {
    pub value: bool,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreBatchGetResultRecord {
    pub values: Vec<Option<Vec<u8>>>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreListBytesResultRecord {
    pub values: Vec<Vec<u8>>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreRootResultRecord {
    pub value: Option<RootManifestRecord>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreNamedRootManifestRecord {
    pub name: Vec<u8>,
    pub manifest: RootManifestRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreListRootsResultRecord {
    pub values: Vec<HostStoreNamedRootManifestRecord>,
    pub error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct HostStoreRootCasResultRecord {
    pub applied: bool,
    pub current: Option<RootManifestRecord>,
    pub error: Option<String>,
}

#[uniffi::export(with_foreign)]
pub trait HostStoreCallback: Send + Sync {
    fn get(&self, key: Vec<u8>) -> HostStoreBytesResultRecord;
    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> HostStoreUnitResultRecord;
    fn delete(&self, key: Vec<u8>) -> HostStoreUnitResultRecord;
    fn batch(&self, ops: Vec<MutationRecord>) -> HostStoreUnitResultRecord;
    fn batch_get_ordered(&self, keys: Vec<Vec<u8>>) -> HostStoreBatchGetResultRecord;
    fn prefers_batch_reads(&self) -> HostStoreBoolResultRecord;
    fn supports_hints(&self) -> HostStoreBoolResultRecord;
    fn get_hint(&self, namespace: Vec<u8>, key: Vec<u8>) -> HostStoreBytesResultRecord;
    fn put_hint(
        &self,
        namespace: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> HostStoreUnitResultRecord;
    fn list_node_cids(&self) -> HostStoreListBytesResultRecord;
    fn get_root(&self, name: Vec<u8>) -> HostStoreRootResultRecord;
    fn put_root(&self, name: Vec<u8>, manifest: RootManifestRecord) -> HostStoreUnitResultRecord;
    fn delete_root(&self, name: Vec<u8>) -> HostStoreUnitResultRecord;
    fn compare_and_swap_root(
        &self,
        name: Vec<u8>,
        expected: Option<RootManifestRecord>,
        replacement: Option<RootManifestRecord>,
    ) -> HostStoreRootCasResultRecord;
    fn list_roots(&self) -> HostStoreListRootsResultRecord;
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ConflictPageRecord {
    pub conflicts: Vec<ConflictRecord>,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MergeExplanationRecord {
    pub result: Option<TreeRecord>,
    pub error: Option<String>,
    pub trace_json: String,
    pub trace: MergeTraceRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangeBoundsRecord {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NodeRecord {
    pub keys: Vec<Vec<u8>>,
    pub vals: Vec<Vec<u8>>,
    pub leaf: bool,
    pub level: u8,
    pub min_chunk_size: u64,
    pub max_chunk_size: u64,
    pub chunking_factor: u32,
    pub hash_seed: u64,
    pub encoding: EncodingRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyProofRecord {
    pub root: Option<Vec<u8>>,
    pub key: Vec<u8>,
    pub path: Vec<NodeRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct KeyProofVerificationRecord {
    pub valid: bool,
    pub exists: bool,
    pub absence: bool,
    pub root: Option<Vec<u8>>,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MultiKeyProofRecord {
    pub root: Option<Vec<u8>>,
    pub keys: Vec<Vec<u8>>,
    pub path: Vec<NodeRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MultiKeyProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Vec<u8>>,
    pub results: Vec<KeyProofVerificationRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangeProofRecord {
    pub root: Option<Vec<u8>>,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub path: Vec<NodeRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangeProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Vec<u8>>,
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
    pub entries: Vec<EntryRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangePageProofRecord {
    pub root: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub path: Vec<NodeRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RangePageProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub entries: Vec<EntryRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProvedRangePageRecord {
    pub page: RangePageRecord,
    pub proof: RangePageProofRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DiffPageProofRecord {
    pub base: RangePageProofRecord,
    pub other: RangePageProofRecord,
    pub lookahead_base: Option<KeyProofRecord>,
    pub lookahead_other: Option<KeyProofRecord>,
    pub requested_end: Option<Vec<u8>>,
    pub limit: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DiffPageProofVerificationRecord {
    pub valid: bool,
    pub base_valid: bool,
    pub other_valid: bool,
    pub lookahead_valid: bool,
    pub base_root: Option<Vec<u8>>,
    pub other_root: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub requested_end: Option<Vec<u8>>,
    pub proof_end: Option<Vec<u8>>,
    pub limit: u64,
    pub diffs: Vec<DiffRecord>,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProvedDiffPageRecord {
    pub page: DiffPageRecord,
    pub proof: DiffPageProofRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProofBundleSummaryRecord {
    pub version: u64,
    pub kind: String,
    pub root: Option<Vec<u8>>,
    pub other_root: Option<Vec<u8>>,
    pub key_count: u64,
    pub path_node_count: u64,
    pub start: Option<Vec<u8>>,
    pub end: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
    pub requested_end: Option<Vec<u8>>,
    pub limit: Option<u64>,
    pub has_lookahead: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ProofBundleVerificationRecord {
    pub summary: ProofBundleSummaryRecord,
    pub valid: bool,
    pub exists_count: u64,
    pub absence_count: u64,
    pub entry_count: u64,
    pub diff_count: u64,
    pub next_cursor: Option<RangeCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AuthenticatedProofEnvelopeRecord {
    pub algorithm: String,
    pub key_id: Vec<u8>,
    pub proof_bundle: Vec<u8>,
    pub context: Vec<u8>,
    pub issued_at_millis: Option<u64>,
    pub expires_at_millis: Option<u64>,
    pub nonce: Vec<u8>,
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AuthenticatedProofEnvelopeVerificationRecord {
    pub valid: bool,
    pub signature_valid: bool,
    pub time_valid: bool,
    pub not_yet_valid: bool,
    pub expired: bool,
    pub algorithm: String,
    pub key_id: Vec<u8>,
    pub proof_bundle: Vec<u8>,
    pub context: Vec<u8>,
    pub issued_at_millis: Option<u64>,
    pub expires_at_millis: Option<u64>,
    pub nonce: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct AuthenticatedProofBundleVerificationRecord {
    pub valid: bool,
    pub envelope: AuthenticatedProofEnvelopeVerificationRecord,
    pub proof: Option<ProofBundleVerificationRecord>,
    pub proof_error: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct VersionedValueRecord {
    pub schema: String,
    pub version: u64,
    pub encoding: EncodingRecord,
    pub payload: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BlobRefRecord {
    pub cid: Vec<u8>,
    pub len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct LargeValueConfigRecord {
    pub inline_threshold: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BlobGcReachabilityRecord {
    pub live_blobs: Vec<BlobRefRecord>,
    pub live_blob_count: u64,
    pub live_blob_bytes: u64,
    pub scanned_nodes: u64,
    pub scanned_values: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BlobGcPlanRecord {
    pub reachability: BlobGcReachabilityRecord,
    pub candidate_blobs: u64,
    pub reclaimable_blobs: Vec<BlobRefRecord>,
    pub reclaimable_blob_count: u64,
    pub reclaimable_blob_bytes: u64,
    pub missing_candidates: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct BlobGcSweepRecord {
    pub plan: BlobGcPlanRecord,
    pub deleted_blobs: u64,
    pub deleted_blob_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum ValueRefKind {
    Inline,
    Blob,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ValueRefRecord {
    pub kind: ValueRefKind,
    pub value: Option<Vec<u8>>,
    pub blob: Option<BlobRefRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct RootManifestRecord {
    pub tree: TreeRecord,
    pub created_at_millis: Option<u64>,
    pub updated_at_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedRootRecord {
    pub name: Vec<u8>,
    pub tree: TreeRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedRootManifestRecord {
    pub name: Vec<u8>,
    pub manifest: RootManifestRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedRootSelectionRecord {
    pub roots: Vec<NamedRootRecord>,
    pub missing_names: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedRootUpdateRecord {
    pub applied: bool,
    pub conflict: bool,
    pub current: Option<TreeRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum SnapshotNamespaceKind {
    Branch,
    Tag,
    Checkpoint,
    Custom,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotNamespaceRecord {
    pub kind: SnapshotNamespaceKind,
    pub custom_prefix: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotRecord {
    pub id: Vec<u8>,
    pub name: Vec<u8>,
    pub tree: TreeRecord,
    pub created_at_millis: Option<u64>,
    pub updated_at_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SnapshotSelectionRecord {
    pub snapshots: Vec<SnapshotRecord>,
    pub missing_ids: Vec<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ChangedSpanRecord {
    pub start: Vec<u8>,
    pub end: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ChangedSpanHintRecord {
    pub base_root: Option<Vec<u8>>,
    pub changed_root: Option<Vec<u8>>,
    pub spans: Vec<ChangedSpanRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum NamedRootRetentionKind {
    All,
    Exact,
    Prefix,
    NewestByName,
    UpdatedSince,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct NamedRootRetentionRecord {
    pub kind: NamedRootRetentionKind,
    pub names: Vec<Vec<u8>>,
    pub prefix: Vec<u8>,
    pub count: Option<u64>,
    pub min_updated_at_millis: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct JsonDocumentRecord {
    pub json: String,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct DiffTraversalStatsRecord {
    pub compared_nodes: u64,
    pub reused_subtrees: u64,
    pub added_subtrees: u64,
    pub removed_subtrees: u64,
    pub collected_fallbacks: u64,
    pub emitted_diffs: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum StructuralDiffMarkerKind {
    Compare,
    Added,
    Removed,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StructuralDiffMarkerRecord {
    pub kind: StructuralDiffMarkerKind,
    pub base_cid: Option<Vec<u8>>,
    pub other_cid: Option<Vec<u8>>,
    pub span_end: Option<Vec<u8>>,
    pub cid: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StructuralDiffCursorRecord {
    pub base_root: Option<Vec<u8>>,
    pub other_root: Option<Vec<u8>>,
    pub markers: Vec<StructuralDiffMarkerRecord>,
    pub pending: Vec<DiffRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StructuralDiffPageRecord {
    pub diffs: Vec<DiffRecord>,
    pub next_cursor_json: Option<String>,
    pub stats: DiffTraversalStatsRecord,
    pub next_cursor: Option<StructuralDiffCursorRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MetricsRecord {
    pub node_cache_hits: u64,
    pub node_cache_misses: u64,
    pub node_cache_evictions: u64,
    pub nodes_read: u64,
    pub bytes_read: u64,
    pub nodes_written: u64,
    pub bytes_written: u64,
    pub store_get_calls: u64,
    pub store_batch_get_calls: u64,
    pub store_batch_get_keys: u64,
    pub store_put_calls: u64,
    pub store_batch_put_calls: u64,
    pub store_batch_put_nodes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CacheStatsRecord {
    pub cached_nodes: u64,
    pub cached_bytes: u64,
    pub pinned_nodes: u64,
    pub pinned_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct GcReachabilityRecord {
    pub live_cids: Vec<Vec<u8>>,
    pub live_nodes: u64,
    pub live_bytes: u64,
    pub leaf_nodes: u64,
    pub internal_nodes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct GcPlanRecord {
    pub reachability: GcReachabilityRecord,
    pub candidate_nodes: u64,
    pub reclaimable_cids: Vec<Vec<u8>>,
    pub reclaimable_nodes: u64,
    pub reclaimable_bytes: u64,
    pub missing_candidates: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct GcSweepRecord {
    pub plan: GcPlanRecord,
    pub deleted_nodes: u64,
    pub deleted_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MissingNodePlanRecord {
    pub required_cids: Vec<Vec<u8>>,
    pub required_nodes: u64,
    pub required_bytes: u64,
    pub missing_cids: Vec<Vec<u8>>,
    pub missing_nodes: u64,
    pub missing_bytes: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MissingNodeCopyRecord {
    pub plan: MissingNodePlanRecord,
    pub copied_nodes: u64,
    pub copied_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CrdtMergeStrategyKind {
    LastWriterWins,
    MultiValue,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum CrdtDeletePolicyKind {
    DeleteWins,
    UpdateWins,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct CrdtConfigRecord {
    pub strategy: CrdtMergeStrategyKind,
    pub delete_policy: CrdtDeletePolicyKind,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TimestampedValueRecord {
    pub value: Vec<u8>,
    pub timestamp: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TombstoneMetadataRecord {
    pub key: String,
    pub value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct TombstoneRecord {
    pub actor: Vec<u8>,
    pub timestamp_millis: u64,
    pub causal_metadata: Vec<TombstoneMetadataRecord>,
}

#[derive(Debug, Error, uniffi::Error)]
pub enum ProllyBindingError {
    #[error("{message}")]
    InvalidArgument { message: String },
    #[error("{message}")]
    InvalidCid { message: String },
    #[error("{message}")]
    InvalidNode { message: String },
    #[error("{message}")]
    NotFound { message: String },
    #[error("{message}")]
    Conflict { message: String },
    #[error("{message}")]
    Store { message: String },
    #[error("{message}")]
    Serialization { message: String },
    #[error("{message}")]
    Internal { message: String },
}

impl From<prolly::Error> for ProllyBindingError {
    fn from(error: prolly::Error) -> Self {
        match error {
            prolly::Error::NotFound(cid) => Self::NotFound {
                message: format!("node not found: {}", hex_bytes(cid.as_bytes())),
            },
            prolly::Error::InvalidNode => Self::InvalidNode {
                message: "invalid node structure".to_string(),
            },
            prolly::Error::Deserialize(message) | prolly::Error::Serialize(message) => {
                Self::Serialization { message }
            }
            prolly::Error::Store(error) => Self::Store {
                message: error.to_string(),
            },
            prolly::Error::CidMismatch { expected, actual } => Self::InvalidCid {
                message: format!(
                    "content CID mismatch: expected {}, got {}",
                    hex_bytes(expected.as_bytes()),
                    hex_bytes(actual.as_bytes())
                ),
            },
            prolly::Error::Conflict(conflict) => Self::Conflict {
                message: format!("merge conflict at key: {}", hex_bytes(&conflict.key)),
            },
            prolly::Error::BufferFull => Self::InvalidArgument {
                message: "mutation buffer is full".to_string(),
            },
            prolly::Error::UnsortedInput { previous, next } => Self::InvalidArgument {
                message: format!(
                    "sorted input keys are out of order: previous={} next={}",
                    hex_bytes(&previous),
                    hex_bytes(&next)
                ),
            },
            prolly::Error::MissingNamedRoots { names } => Self::InvalidArgument {
                message: format!("missing named roots for retention policy: {names:?}"),
            },
            prolly::Error::InvalidSnapshotBundle(message) => Self::InvalidArgument { message },
        }
    }
}

#[derive(uniffi::Object)]
pub struct ProllyBlobStore {
    inner: BindingBlobStore,
}

#[uniffi::export]
impl ProllyBlobStore {
    #[uniffi::constructor]
    pub fn memory() -> Self {
        Self {
            inner: BindingBlobStore::Memory(Arc::new(MemBlobStore::new())),
        }
    }

    #[uniffi::constructor]
    pub fn file(path: String) -> Result<Self, ProllyBindingError> {
        let store = Arc::new(FileBlobStore::open(path).map_err(store_error)?);
        Ok(Self {
            inner: BindingBlobStore::File(store),
        })
    }

    pub fn put_blob(&self, bytes: Vec<u8>) -> Result<BlobRefRecord, ProllyBindingError> {
        with_blob_store!(self, store, {
            store
                .put_blob(&bytes)
                .map(BlobRefRecord::from)
                .map_err(store_error)
        })
    }

    pub fn get_blob(
        &self,
        reference: BlobRefRecord,
    ) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        let reference = BlobRef::try_from(reference)?;
        with_blob_store!(self, store, {
            store.get_blob(&reference).map_err(store_error)
        })
    }

    pub fn delete_blob(&self, reference: BlobRefRecord) -> Result<(), ProllyBindingError> {
        let reference = BlobRef::try_from(reference)?;
        with_blob_store!(self, store, {
            store.delete_blob(&reference).map_err(store_error)
        })
    }

    pub fn list_blob_refs(&self) -> Result<Vec<BlobRefRecord>, ProllyBindingError> {
        with_blob_store!(self, store, {
            store
                .list_blob_refs()
                .map(blob_ref_records)
                .map_err(store_error)
        })
    }

    pub fn blob_count(&self) -> Result<u64, ProllyBindingError> {
        with_blob_store!(self, store, {
            let count = store.list_blob_refs().map_err(store_error)?.len();
            to_u64(count, "blob_count")
        })
    }
}

#[derive(uniffi::Object)]
pub struct MergePolicyRegistry {
    inner: Mutex<CoreMergePolicyRegistry>,
}

#[uniffi::export]
impl MergePolicyRegistry {
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(CoreMergePolicyRegistry::new()),
        }
    }

    pub fn len(&self) -> Result<u64, ProllyBindingError> {
        let registry = self.lock()?;
        to_u64(registry.len(), "merge_policy_rule_count")
    }

    pub fn is_empty(&self) -> Result<bool, ProllyBindingError> {
        Ok(self.lock()?.is_empty())
    }

    pub fn has_default(&self) -> Result<bool, ProllyBindingError> {
        Ok(self.lock()?.has_default())
    }

    pub fn set_default_resolver_name(&self, name: String) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_name(name)?;
        self.lock()?
            .set_default(move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn set_default_resolver(
        &self,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_callback(resolver);
        self.lock()?
            .set_default(move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_prefix_resolver_name(
        &self,
        prefix: Vec<u8>,
        name: String,
    ) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_name(name)?;
        self.lock()?
            .push_prefix(prefix, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_exact_resolver_name(
        &self,
        key: Vec<u8>,
        name: String,
    ) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_name(name)?;
        self.lock()?
            .push_exact(key, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_prefix_resolver(
        &self,
        prefix: Vec<u8>,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_callback(resolver);
        self.lock()?
            .push_prefix(prefix, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_exact_resolver(
        &self,
        key: Vec<u8>,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<(), ProllyBindingError> {
        let policy = policy_fn_from_callback(resolver);
        self.lock()?
            .push_exact(key, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }
}

impl MergePolicyRegistry {
    fn lock(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, CoreMergePolicyRegistry>, ProllyBindingError> {
        self.inner.lock().map_err(|error| {
            internal_error(format!("merge policy registry lock poisoned: {error}"))
        })
    }

    fn as_resolver(&self) -> Result<Resolver, ProllyBindingError> {
        Ok(self.lock()?.as_resolver())
    }

    pub fn set_default_host_resolver<F>(&self, resolver: F) -> Result<(), ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let policy = policy_fn_from_host_callback(resolver);
        self.lock()?
            .set_default(move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_prefix_host_resolver<F>(
        &self,
        prefix: Vec<u8>,
        resolver: F,
    ) -> Result<(), ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let policy = policy_fn_from_host_callback(resolver);
        self.lock()?
            .push_prefix(prefix, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }

    pub fn push_exact_host_resolver<F>(
        &self,
        key: Vec<u8>,
        resolver: F,
    ) -> Result<(), ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let policy = policy_fn_from_host_callback(resolver);
        self.lock()?
            .push_exact(key, move |conflict| (policy.as_ref())(conflict));
        Ok(())
    }
}

#[derive(Clone)]
struct HostStore {
    callback: Arc<dyn HostStoreCallback>,
}

#[derive(Clone, Debug)]
struct HostStoreError(String);

impl std::fmt::Display for HostStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for HostStoreError {}

impl HostStore {
    fn new(callback: Arc<dyn HostStoreCallback>) -> Self {
        Self { callback }
    }

    fn unit(record: HostStoreUnitResultRecord) -> Result<(), HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None => Ok(()),
        }
    }

    fn bytes(record: HostStoreBytesResultRecord) -> Result<Option<Vec<u8>>, HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None => Ok(record.value),
        }
    }

    fn byte_list(record: HostStoreListBytesResultRecord) -> Result<Vec<Vec<u8>>, HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None => Ok(record.values),
        }
    }

    fn boolean(record: HostStoreBoolResultRecord) -> Result<bool, HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None => Ok(record.value),
        }
    }

    fn batch_get_values(
        record: HostStoreBatchGetResultRecord,
        expected_len: usize,
    ) -> Result<Vec<Option<Vec<u8>>>, HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None if record.values.len() == expected_len => Ok(record.values),
            None => Err(HostStoreError(format!(
                "host store batch_get_ordered returned {} values for {expected_len} keys",
                record.values.len()
            ))),
        }
    }

    fn root(record: HostStoreRootResultRecord) -> Result<Option<RootManifest>, HostStoreError> {
        match record.error {
            Some(error) => Err(HostStoreError(error)),
            None => record
                .value
                .map(RootManifest::try_from)
                .transpose()
                .map_err(HostStoreError::from),
        }
    }

    fn mutation_from_batch_op(op: &BatchOp<'_>) -> MutationRecord {
        match op {
            BatchOp::Upsert { key, value } => MutationRecord {
                kind: MutationKind::Upsert,
                key: (*key).to_vec(),
                value: Some((*value).to_vec()),
            },
            BatchOp::Delete { key } => MutationRecord {
                kind: MutationKind::Delete,
                key: (*key).to_vec(),
                value: None,
            },
        }
    }

    fn manifest_record(manifest: &RootManifest) -> RootManifestRecord {
        manifest.clone().into()
    }
}

impl From<ProllyBindingError> for HostStoreError {
    fn from(error: ProllyBindingError) -> Self {
        Self(error.to_string())
    }
}

impl Store for HostStore {
    type Error = HostStoreError;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Self::bytes(self.callback.get(key.to_vec()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Self::unit(self.callback.put(key.to_vec(), value.to_vec()))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Self::unit(self.callback.delete(key.to_vec()))
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let records = ops.iter().map(Self::mutation_from_batch_op).collect();
        Self::unit(self.callback.batch(records))
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let ordered = self.batch_get_ordered(keys)?;
        let mut results = HashMap::with_capacity(ordered.len());
        for (key, value) in keys.iter().zip(ordered) {
            if let Some(value) = value {
                results.insert((*key).to_vec(), value);
            }
        }
        Ok(results)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let key_records = keys.iter().map(|key| (*key).to_vec()).collect();
        Self::batch_get_values(self.callback.batch_get_ordered(key_records), keys.len())
    }

    fn prefers_batch_reads(&self) -> bool {
        Self::boolean(self.callback.prefers_batch_reads()).unwrap_or(false)
    }

    fn supports_hints(&self) -> bool {
        Self::boolean(self.callback.supports_hints()).unwrap_or(false)
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Self::bytes(self.callback.get_hint(namespace.to_vec(), key.to_vec()))
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Self::unit(
            self.callback
                .put_hint(namespace.to_vec(), key.to_vec(), value.to_vec()),
        )
    }
}

impl NodeStoreScan for HostStore {
    type Error = HostStoreError;

    fn list_node_cids(&self) -> Result<Vec<Cid>, Self::Error> {
        let mut cids = Self::byte_list(self.callback.list_node_cids())?
            .into_iter()
            .map(|bytes| cid_from_vec(bytes).map_err(|error| HostStoreError(error.to_string())))
            .collect::<Result<Vec<_>, _>>()?;
        cids.sort_by(|left, right| left.as_bytes().cmp(right.as_bytes()));
        Ok(cids)
    }
}

impl ManifestStore for HostStore {
    type Error = HostStoreError;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        Self::root(self.callback.get_root(name.to_vec()))
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        Self::unit(
            self.callback
                .put_root(name.to_vec(), Self::manifest_record(manifest)),
        )
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        Self::unit(self.callback.delete_root(name.to_vec()))
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let expected = expected.map(Self::manifest_record);
        let replacement = new.map(Self::manifest_record);
        let record = self
            .callback
            .compare_and_swap_root(name.to_vec(), expected, replacement);
        if let Some(error) = record.error {
            return Err(HostStoreError(error));
        }
        if record.applied {
            return Ok(ManifestUpdate::Applied);
        }
        let current = record
            .current
            .map(RootManifest::try_from)
            .transpose()
            .map_err(HostStoreError::from)?;
        Ok(ManifestUpdate::Conflict { current })
    }
}

impl ManifestStoreScan for HostStore {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        let record = self.callback.list_roots();
        if let Some(error) = record.error {
            return Err(HostStoreError(error));
        }
        let mut roots = record
            .values
            .into_iter()
            .map(|root| {
                RootManifest::try_from(root.manifest)
                    .map(|manifest| NamedRootManifest::new(root.name, manifest))
                    .map_err(HostStoreError::from)
            })
            .collect::<Result<Vec<_>, _>>()?;
        roots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(roots)
    }
}

#[derive(uniffi::Object)]
pub struct ProllyEngine {
    inner: BindingEngine,
}

#[uniffi::export]
impl ProllyEngine {
    #[uniffi::constructor]
    pub fn memory(config: ConfigRecord) -> Result<Self, ProllyBindingError> {
        let config = config.try_into()?;
        let store = Arc::new(MemStore::new());
        Ok(Self {
            inner: BindingEngine::Memory(Prolly::new(store, config)),
        })
    }

    #[uniffi::constructor]
    pub fn file(path: String, config: ConfigRecord) -> Result<Self, ProllyBindingError> {
        let config = config.try_into()?;
        let store = Arc::new(FileNodeStore::open(path).map_err(store_error)?);
        Ok(Self {
            inner: BindingEngine::File(Prolly::new(store, config)),
        })
    }

    #[uniffi::constructor]
    pub fn custom_store(
        callback: Arc<dyn HostStoreCallback>,
        config: ConfigRecord,
    ) -> Result<Self, ProllyBindingError> {
        let config = config.try_into()?;
        let store = Arc::new(HostStore::new(callback));
        Ok(Self {
            inner: BindingEngine::Host(Prolly::new(store, config)),
        })
    }

    pub fn create(&self) -> TreeRecord {
        with_engine!(self, engine, { engine.create().into() })
    }

    pub fn get(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine.get(&tree, &key).map_err(Into::into)
        })
    }

    pub fn first_entry(&self, tree: TreeRecord) -> Result<Option<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .first_entry(&tree)
                .map(|entry| entry.map(entry_record))
                .map_err(Into::into)
        })
    }

    pub fn last_entry(&self, tree: TreeRecord) -> Result<Option<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .last_entry(&tree)
                .map(|entry| entry.map(entry_record))
                .map_err(Into::into)
        })
    }

    pub fn lower_bound(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<Option<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .lower_bound(&tree, &key)
                .map(|entry| entry.map(entry_record))
                .map_err(Into::into)
        })
    }

    pub fn upper_bound(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<Option<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .upper_bound(&tree, &key)
                .map(|entry| entry.map(entry_record))
                .map_err(Into::into)
        })
    }

    pub fn get_many(
        &self,
        tree: TreeRecord,
        keys: Vec<Vec<u8>>,
    ) -> Result<Vec<Option<Vec<u8>>>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine.get_many(&tree, &keys).map_err(Into::into)
        })
    }

    pub fn prove_key(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<KeyProofRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .prove_key(&tree, &key)
                .map(KeyProofRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prove_keys(
        &self,
        tree: TreeRecord,
        keys: Vec<Vec<u8>>,
    ) -> Result<MultiKeyProofRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .prove_keys(&tree, &keys)
                .map(MultiKeyProofRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prove_range(
        &self,
        tree: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<RangeProofRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .prove_range(&tree, &start, end.as_deref())
                .map(RangeProofRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prove_prefix(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
    ) -> Result<RangeProofRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .prove_prefix(&tree, &prefix)
                .map(RangeProofRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prove_range_page(
        &self,
        tree: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<ProvedRangePageRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .prove_range_page(&tree, &cursor, end.as_deref(), limit)
                .map(ProvedRangePageRecord::try_from)?
        })
    }

    pub fn prove_diff_page(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<ProvedDiffPageRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .prove_diff_page(&base, &other, &cursor, end.as_deref(), limit)
                .map(ProvedDiffPageRecord::try_from)?
        })
    }

    pub fn get_value_ref(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<Option<ValueRefRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .get_value_ref(&tree, &key)
                .map(|value_ref| value_ref.map(ValueRefRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn get_large_value(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        tree: TreeRecord,
        key: Vec<u8>,
    ) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            engine
                .get_large_value(store, &tree, &key)
                .map_err(Into::into)
        })
    }

    pub fn put(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .put(&tree, key, value)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn put_large_value(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        tree: TreeRecord,
        key: Vec<u8>,
        value: Vec<u8>,
        config: LargeValueConfigRecord,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let config = LargeValueConfig::try_from(config)?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            engine
                .put_large_value(store, &tree, key, value, config)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn delete(&self, tree: TreeRecord, key: Vec<u8>) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .delete(&tree, &key)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn batch(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_engine!(self, engine, {
            engine
                .batch(&tree, mutations)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn batch_with_stats(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
    ) -> Result<BatchApplyResultRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_engine!(self, engine, {
            engine
                .batch_with_stats(&tree, mutations)
                .map(BatchApplyResultRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn build_from_entries(
        &self,
        entries: Vec<EntryRecord>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<Vec<_>>();
        with_engine!(self, engine, {
            engine
                .build_from_entries(entries)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn build_from_sorted_entries(
        &self,
        entries: Vec<EntryRecord>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let entries = entries
            .into_iter()
            .map(|entry| (entry.key, entry.value))
            .collect::<Vec<_>>();
        with_engine!(self, engine, {
            engine
                .build_from_sorted_entries(entries)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn append_batch(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_engine!(self, engine, {
            engine
                .append_batch(&tree, mutations)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn append_batch_with_stats(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
    ) -> Result<BatchApplyResultRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_engine!(self, engine, {
            engine
                .append_batch_with_stats(&tree, mutations)
                .map(BatchApplyResultRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn parallel_batch(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
        config: ParallelConfigRecord,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let config = ParallelConfig::try_from(config)?;
        with_engine!(self, engine, {
            engine
                .parallel_batch(&tree, mutations, &config)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn parallel_batch_with_stats(
        &self,
        tree: TreeRecord,
        mutations: Vec<MutationRecord>,
        config: ParallelConfigRecord,
    ) -> Result<BatchApplyResultRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        let config = ParallelConfig::try_from(config)?;
        with_engine!(self, engine, {
            engine
                .parallel_batch_with_stats(&tree, mutations, &config)
                .map(BatchApplyResultRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn range(
        &self,
        tree: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let iter = engine.range(&tree, &start, end.as_deref())?;
            iter.map(|entry| {
                entry
                    .map(|(key, value)| EntryRecord { key, value })
                    .map_err(Into::into)
            })
            .collect()
        })
    }

    pub fn prefix(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
    ) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let iter = engine.prefix(&tree, &prefix)?;
            iter.map(|entry| entry.map(entry_record).map_err(Into::into))
                .collect()
        })
    }

    pub fn prefix_page(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
        cursor: Option<RangeCursorRecord>,
        limit: u64,
    ) -> Result<RangePageRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .prefix_page(&tree, &prefix, &cursor, limit)
                .map(RangePageRecord::try_from)?
        })
    }

    pub fn range_after(
        &self,
        tree: TreeRecord,
        after_key: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let iter = engine.range_after(&tree, &after_key, end.as_deref())?;
            iter.map(|entry| {
                entry
                    .map(|(key, value)| EntryRecord { key, value })
                    .map_err(Into::into)
            })
            .collect()
        })
    }

    pub fn range_from_cursor(
        &self,
        tree: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        with_engine!(self, engine, {
            let iter = engine.range_from_cursor(&tree, &cursor, end.as_deref())?;
            iter.map(|entry| {
                entry
                    .map(|(key, value)| EntryRecord { key, value })
                    .map_err(Into::into)
            })
            .collect()
        })
    }

    pub fn range_diff(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<DiffRecord>, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        with_engine!(self, engine, {
            engine
                .range_diff(&base, &other, &start, end.as_deref())?
                .into_iter()
                .map(DiffRecord::try_from)
                .collect()
        })
    }

    pub fn diff_from_cursor(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
    ) -> Result<Vec<DiffRecord>, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        with_engine!(self, engine, {
            engine
                .diff_from_cursor(&base, &other, &cursor, end.as_deref())?
                .into_iter()
                .map(DiffRecord::try_from)
                .collect()
        })
    }

    pub fn range_page(
        &self,
        tree: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<RangePageRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .range_page(&tree, &cursor, end.as_deref(), limit)
                .map(RangePageRecord::try_from)?
        })
    }

    pub fn reverse_page(
        &self,
        tree: TreeRecord,
        cursor: Option<ReverseCursorRecord>,
        start: Vec<u8>,
        limit: u64,
    ) -> Result<ReversePageRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(ReverseCursor::from)
            .unwrap_or_else(ReverseCursor::end);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .reverse_page(&tree, &cursor, &start, limit)
                .map(ReversePageRecord::try_from)?
        })
    }

    pub fn prefix_reverse_page(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
        cursor: Option<ReverseCursorRecord>,
        limit: u64,
    ) -> Result<ReversePageRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let cursor = cursor
            .map(ReverseCursor::from)
            .unwrap_or_else(ReverseCursor::end);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .prefix_reverse_page(&tree, &prefix, &cursor, limit)
                .map(ReversePageRecord::try_from)?
        })
    }

    pub fn cursor_window(
        &self,
        tree: TreeRecord,
        key: Vec<u8>,
        end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<CursorWindowRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .cursor_window(&tree, &key, end.as_deref(), limit)
                .map(CursorWindowRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn diff(
        &self,
        base: TreeRecord,
        other: TreeRecord,
    ) -> Result<Vec<DiffRecord>, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        with_engine!(self, engine, {
            engine
                .diff(&base, &other)?
                .into_iter()
                .map(DiffRecord::try_from)
                .collect()
        })
    }

    pub fn diff_page(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<DiffPageRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        let cursor = cursor
            .map(RangeCursor::from)
            .unwrap_or_else(RangeCursor::start);
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            engine
                .diff_page(&base, &other, &cursor, end.as_deref(), limit)
                .map(DiffPageRecord::try_from)?
        })
    }

    pub fn conflict_page(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        cursor: Option<RangeCursorRecord>,
        limit: u64,
    ) -> Result<ConflictPageRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let after_key = cursor.and_then(|cursor| cursor.after_key);
        let limit = to_usize(limit, "limit")?;
        if limit == 0 {
            return Ok(ConflictPageRecord {
                conflicts: Vec::new(),
                next_cursor: after_key.map(|after_key| RangeCursorRecord {
                    after_key: Some(after_key),
                }),
            });
        }
        with_engine!(self, engine, {
            let mut conflicts = Vec::with_capacity(limit);
            let mut last_emitted_key = None;
            let mut has_more = false;
            for conflict in engine.stream_conflicts(&base, &left, &right)? {
                let conflict = conflict?;
                if after_key
                    .as_ref()
                    .is_some_and(|after_key| conflict.key.as_slice() <= after_key.as_slice())
                {
                    continue;
                }
                if conflicts.len() == limit {
                    has_more = true;
                    break;
                }
                last_emitted_key = Some(conflict.key.clone());
                conflicts.push(ConflictRecord::from(conflict));
            }
            Ok(ConflictPageRecord {
                conflicts,
                next_cursor: if has_more {
                    last_emitted_key.map(|after_key| RangeCursorRecord {
                        after_key: Some(after_key),
                    })
                } else {
                    None
                },
            })
        })
    }

    pub fn merge(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: Option<String>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_name(resolver)?;
        with_engine!(self, engine, {
            engine
                .merge(&base, &left, &right, resolver)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_explain(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: Option<String>,
    ) -> Result<MergeExplanationRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_name(resolver)?;
        with_engine!(self, engine, {
            let explanation = engine.merge_explain(&base, &left, &right, resolver);
            MergeExplanationRecord::try_from(explanation)
        })
    }

    pub fn merge_with_resolver(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge(&base, &left, &right, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_explain_with_resolver(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<MergeExplanationRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_callback(resolver);
        with_engine!(self, engine, {
            let explanation = engine.merge_explain(&base, &left, &right, Some(resolver));
            MergeExplanationRecord::try_from(explanation)
        })
    }

    pub fn merge_with_policy(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        policy: Arc<MergePolicyRegistry>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = policy.as_resolver()?;
        with_engine!(self, engine, {
            engine
                .merge(&base, &left, &right, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_explain_with_policy(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        policy: Arc<MergePolicyRegistry>,
    ) -> Result<MergeExplanationRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = policy.as_resolver()?;
        with_engine!(self, engine, {
            let explanation = engine.merge_explain(&base, &left, &right, Some(resolver));
            MergeExplanationRecord::try_from(explanation)
        })
    }

    pub fn merge_range(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        resolver: Option<String>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_name(resolver)?;
        with_engine!(self, engine, {
            engine
                .merge_range(&base, &left, &right, &start, end.as_deref(), resolver)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_range_with_resolver(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge_range(&base, &left, &right, &start, end.as_deref(), Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_range_with_policy(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        policy: Arc<MergePolicyRegistry>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = policy.as_resolver()?;
        with_engine!(self, engine, {
            engine
                .merge_range(&base, &left, &right, &start, end.as_deref(), Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_prefix(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: Vec<u8>,
        resolver: Option<String>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_name(resolver)?;
        with_engine!(self, engine, {
            engine
                .merge_prefix(&base, &left, &right, &prefix, resolver)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_prefix_with_policy(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: Vec<u8>,
        policy: Arc<MergePolicyRegistry>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = policy.as_resolver()?;
        with_engine!(self, engine, {
            engine
                .merge_prefix(&base, &left, &right, &prefix, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_prefix_with_resolver(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: Vec<u8>,
        resolver: Arc<dyn MergeResolverCallback>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge_prefix(&base, &left, &right, &prefix, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn crdt_merge(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        config: CrdtConfigRecord,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let config = CrdtConfig::from(config);
        with_engine!(self, engine, {
            engine
                .crdt_merge(&base, &left, &right, &config)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn crdt_merge_with_resolver(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        delete_policy: CrdtDeletePolicyKind,
        resolver: Arc<dyn CrdtResolverCallback>,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let config = crdt_config_from_callback(delete_policy, resolver);
        with_engine!(self, engine, {
            engine
                .crdt_merge(&base, &left, &right, &config)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn structural_diff_page(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        cursor_json: Option<String>,
        limit: u64,
    ) -> Result<StructuralDiffPageRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        let cursor = cursor_json
            .map(|json| serde_json::from_str::<StructuralDiffCursor>(&json).map_err(json_error))
            .transpose()?;
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            let page = engine.structural_diff_page(&base, &other, cursor.as_ref(), limit)?;
            StructuralDiffPageRecord::try_from(page)
        })
    }

    pub fn structural_diff_page_with_cursor(
        &self,
        base: TreeRecord,
        other: TreeRecord,
        cursor: Option<StructuralDiffCursorRecord>,
        limit: u64,
    ) -> Result<StructuralDiffPageRecord, ProllyBindingError> {
        let base = base.try_into()?;
        let other = other.try_into()?;
        let cursor = cursor.map(StructuralDiffCursor::try_from).transpose()?;
        let limit = to_usize(limit, "limit")?;
        with_engine!(self, engine, {
            let page = engine.structural_diff_page(&base, &other, cursor.as_ref(), limit)?;
            StructuralDiffPageRecord::try_from(page)
        })
    }

    pub fn collect_stats_json(
        &self,
        tree: TreeRecord,
    ) -> Result<JsonDocumentRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let stats = engine.collect_stats(&tree)?;
            json_document(&stats)
        })
    }

    pub fn collect_stats(&self, tree: TreeRecord) -> Result<TreeStatsRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .collect_stats(&tree)
                .map(TreeStatsRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn stats_diff_json(
        &self,
        before: TreeRecord,
        after: TreeRecord,
    ) -> Result<JsonDocumentRecord, ProllyBindingError> {
        let before = before.try_into()?;
        let after = after.try_into()?;
        with_engine!(self, engine, {
            let stats = engine.stats_diff(&before, &after)?;
            json_document(&stats)
        })
    }

    pub fn stats_diff(
        &self,
        before: TreeRecord,
        after: TreeRecord,
    ) -> Result<StatsComparisonRecord, ProllyBindingError> {
        let before = before.try_into()?;
        let after = after.try_into()?;
        with_engine!(self, engine, {
            engine
                .stats_diff(&before, &after)
                .map(StatsComparisonRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn debug_tree_json(
        &self,
        tree: TreeRecord,
    ) -> Result<JsonDocumentRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let view = engine.debug_tree(&tree)?;
            json_document(&view)
        })
    }

    pub fn debug_tree(&self, tree: TreeRecord) -> Result<TreeDebugViewRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .debug_tree(&tree)
                .map(TreeDebugViewRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn debug_tree_text(&self, tree: TreeRecord) -> Result<String, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .debug_tree(&tree)
                .map(|view| view.to_text())
                .map_err(Into::into)
        })
    }

    pub fn debug_compare_trees_json(
        &self,
        left: TreeRecord,
        right: TreeRecord,
    ) -> Result<JsonDocumentRecord, ProllyBindingError> {
        let left = left.try_into()?;
        let right = right.try_into()?;
        with_engine!(self, engine, {
            let comparison = engine.debug_compare_trees(&left, &right)?;
            json_document(&comparison)
        })
    }

    pub fn debug_compare_trees(
        &self,
        left: TreeRecord,
        right: TreeRecord,
    ) -> Result<TreeDebugComparisonRecord, ProllyBindingError> {
        let left = left.try_into()?;
        let right = right.try_into()?;
        with_engine!(self, engine, {
            engine
                .debug_compare_trees(&left, &right)
                .map(TreeDebugComparisonRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn debug_compare_trees_text(
        &self,
        left: TreeRecord,
        right: TreeRecord,
    ) -> Result<String, ProllyBindingError> {
        let left = left.try_into()?;
        let right = right.try_into()?;
        with_engine!(self, engine, {
            engine
                .debug_compare_trees(&left, &right)
                .map(|comparison| comparison.to_text())
                .map_err(Into::into)
        })
    }

    pub fn mark_reachable(
        &self,
        roots: Vec<TreeRecord>,
    ) -> Result<GcReachabilityRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine!(self, engine, {
            let reachability = engine.mark_reachable(&roots)?;
            GcReachabilityRecord::try_from(reachability)
        })
    }

    pub fn plan_gc(
        &self,
        roots: Vec<TreeRecord>,
        candidate_cids: Vec<Vec<u8>>,
    ) -> Result<GcPlanRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        let candidate_cids = cids_from_records(candidate_cids)?;
        with_engine!(self, engine, {
            let plan = engine.plan_gc(&roots, &candidate_cids)?;
            GcPlanRecord::try_from(plan)
        })
    }

    pub fn sweep_gc(
        &self,
        roots: Vec<TreeRecord>,
        candidate_cids: Vec<Vec<u8>>,
    ) -> Result<GcSweepRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        let candidate_cids = cids_from_records(candidate_cids)?;
        with_engine!(self, engine, {
            let sweep = engine.sweep_gc(&roots, &candidate_cids)?;
            GcSweepRecord::try_from(sweep)
        })
    }

    pub fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
        with_engine!(self, engine, {
            engine
                .store()
                .list_node_cids()
                .map(cid_records)
                .map_err(store_error)
        })
    }

    pub fn plan_store_gc(
        &self,
        roots: Vec<TreeRecord>,
    ) -> Result<GcPlanRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine!(self, engine, {
            let plan = engine.plan_store_gc(&roots)?;
            GcPlanRecord::try_from(plan)
        })
    }

    pub fn sweep_store_gc(
        &self,
        roots: Vec<TreeRecord>,
    ) -> Result<GcSweepRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine!(self, engine, {
            let sweep = engine.sweep_store_gc(&roots)?;
            GcSweepRecord::try_from(sweep)
        })
    }

    pub fn mark_reachable_blobs(
        &self,
        roots: Vec<TreeRecord>,
    ) -> Result<BlobGcReachabilityRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine!(self, engine, {
            let reachability = engine.mark_reachable_blobs(&roots)?;
            BlobGcReachabilityRecord::try_from(reachability)
        })
    }

    pub fn plan_blob_gc(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        roots: Vec<TreeRecord>,
        candidate_blobs: Vec<BlobRefRecord>,
    ) -> Result<BlobGcPlanRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        let candidate_blobs = blob_refs_from_records(candidate_blobs)?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            let plan = engine.plan_blob_gc(store, &roots, &candidate_blobs)?;
            BlobGcPlanRecord::try_from(plan)
        })
    }

    pub fn sweep_blob_gc(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        roots: Vec<TreeRecord>,
        candidate_blobs: Vec<BlobRefRecord>,
    ) -> Result<BlobGcSweepRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        let candidate_blobs = blob_refs_from_records(candidate_blobs)?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            let sweep = engine.sweep_blob_gc(store, &roots, &candidate_blobs)?;
            BlobGcSweepRecord::try_from(sweep)
        })
    }

    pub fn plan_blob_store_gc(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        roots: Vec<TreeRecord>,
    ) -> Result<BlobGcPlanRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            let plan = engine.plan_blob_store_gc(store, &roots)?;
            BlobGcPlanRecord::try_from(plan)
        })
    }

    pub fn sweep_blob_store_gc(
        &self,
        blob_store: Arc<ProllyBlobStore>,
        roots: Vec<TreeRecord>,
    ) -> Result<BlobGcSweepRecord, ProllyBindingError> {
        let roots = trees_from_records(roots)?;
        with_engine_and_blob_store!(self, blob_store, engine, store, {
            let sweep = engine.sweep_blob_store_gc(store, &roots)?;
            BlobGcSweepRecord::try_from(sweep)
        })
    }

    pub fn plan_missing_nodes(
        &self,
        tree: TreeRecord,
        destination: Arc<ProllyEngine>,
    ) -> Result<MissingNodePlanRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine_pair!(self, destination, source, destination_engine, {
            let plan = source.plan_missing_nodes(&tree, destination_engine.store())?;
            MissingNodePlanRecord::try_from(plan)
        })
    }

    pub fn copy_missing_nodes(
        &self,
        tree: TreeRecord,
        destination: Arc<ProllyEngine>,
    ) -> Result<MissingNodeCopyRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine_pair!(self, destination, source, destination_engine, {
            let copy = source.copy_missing_nodes(&tree, destination_engine.store())?;
            MissingNodeCopyRecord::try_from(copy)
        })
    }

    pub fn export_snapshot(
        &self,
        tree: TreeRecord,
    ) -> Result<SnapshotBundleRecord, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let bundle = engine.export_snapshot(&tree)?;
            SnapshotBundleRecord::try_from(bundle)
        })
    }

    pub fn import_snapshot(
        &self,
        bundle: SnapshotBundleRecord,
    ) -> Result<TreeRecord, ProllyBindingError> {
        let bundle = CoreSnapshotBundle::try_from(bundle)?;
        with_engine!(self, engine, {
            engine
                .import_snapshot(&bundle)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn cache_stats(&self) -> Result<CacheStatsRecord, ProllyBindingError> {
        with_engine!(self, engine, { cache_stats(engine) })
    }

    pub fn clear_cache(&self) {
        with_engine!(self, engine, { engine.clear_cache() })
    }

    pub fn pin_tree_root(&self, tree: TreeRecord) -> Result<u64, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let count = engine.pin_tree_root(&tree)?;
            to_u64(count, "pinned root count")
        })
    }

    pub fn pin_tree_path(&self, tree: TreeRecord, key: Vec<u8>) -> Result<u64, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            let count = engine.pin_tree_path(&tree, &key)?;
            to_u64(count, "pinned path count")
        })
    }

    pub fn unpin_all_cache_nodes(&self) -> Result<u64, ProllyBindingError> {
        with_engine!(self, engine, {
            to_u64(engine.unpin_all_cache_nodes(), "unpinned count")
        })
    }

    pub fn metrics(&self) -> MetricsRecord {
        with_engine!(self, engine, { engine.metrics().into() })
    }

    pub fn reset_metrics(&self) {
        with_engine!(self, engine, { engine.reset_metrics() })
    }

    pub fn publish_prefix_path_hint(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
    ) -> Result<bool, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .publish_prefix_path_hint(&tree, &prefix)
                .map_err(Into::into)
        })
    }

    pub fn hydrate_prefix_path_hint(
        &self,
        tree: TreeRecord,
        prefix: Vec<u8>,
    ) -> Result<bool, ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .hydrate_prefix_path_hint(&tree, &prefix)
                .map_err(Into::into)
        })
    }

    pub fn publish_changed_spans_hint(
        &self,
        base: TreeRecord,
        changed: TreeRecord,
        spans: Vec<ChangedSpanRecord>,
    ) -> Result<bool, ProllyBindingError> {
        let base = base.try_into()?;
        let changed = changed.try_into()?;
        let spans = spans.into_iter().map(ChangedSpan::from);
        with_engine!(self, engine, {
            engine
                .publish_changed_spans_hint(&base, &changed, spans)
                .map_err(Into::into)
        })
    }

    pub fn load_changed_spans_hint(
        &self,
        base: TreeRecord,
        changed: TreeRecord,
    ) -> Result<Option<ChangedSpanHintRecord>, ProllyBindingError> {
        let base = base.try_into()?;
        let changed = changed.try_into()?;
        with_engine!(self, engine, {
            engine
                .load_changed_spans_hint(&base, &changed)
                .map(|hint| hint.map(ChangedSpanHintRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn load_named_root(&self, name: Vec<u8>) -> Result<Option<TreeRecord>, ProllyBindingError> {
        with_engine!(self, engine, {
            engine
                .load_named_root(&name)
                .map(|tree| tree.map(TreeRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn load_named_roots(
        &self,
        names: Vec<Vec<u8>>,
    ) -> Result<NamedRootSelectionRecord, ProllyBindingError> {
        with_engine!(self, engine, {
            engine
                .load_named_roots(names)
                .map(NamedRootSelectionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn load_retained_named_roots(
        &self,
        retention: NamedRootRetentionRecord,
    ) -> Result<NamedRootSelectionRecord, ProllyBindingError> {
        let retention = NamedRootRetention::try_from(retention)?;
        with_engine!(self, engine, {
            engine
                .load_retained_named_roots(&retention)
                .map(NamedRootSelectionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn plan_store_gc_for_retention(
        &self,
        retention: NamedRootRetentionRecord,
    ) -> Result<GcPlanRecord, ProllyBindingError> {
        let retention = NamedRootRetention::try_from(retention)?;
        with_engine!(self, engine, {
            let plan = engine.plan_store_gc_for_retention(&retention)?;
            GcPlanRecord::try_from(plan)
        })
    }

    pub fn sweep_store_gc_for_retention(
        &self,
        retention: NamedRootRetentionRecord,
    ) -> Result<GcSweepRecord, ProllyBindingError> {
        let retention = NamedRootRetention::try_from(retention)?;
        with_engine!(self, engine, {
            let sweep = engine.sweep_store_gc_for_retention(&retention)?;
            GcSweepRecord::try_from(sweep)
        })
    }

    pub fn list_named_roots(&self) -> Result<Vec<NamedRootRecord>, ProllyBindingError> {
        with_engine!(self, engine, {
            engine
                .list_named_roots()
                .map(|roots| roots.into_iter().map(NamedRootRecord::from).collect())
                .map_err(Into::into)
        })
    }

    pub fn list_named_root_manifests(
        &self,
    ) -> Result<Vec<NamedRootManifestRecord>, ProllyBindingError> {
        with_engine!(self, engine, {
            engine
                .list_named_root_manifests()
                .map(|roots| {
                    roots
                        .into_iter()
                        .map(NamedRootManifestRecord::from)
                        .collect()
                })
                .map_err(Into::into)
        })
    }

    pub fn publish_named_root(
        &self,
        name: Vec<u8>,
        tree: TreeRecord,
    ) -> Result<(), ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine.publish_named_root(&name, &tree).map_err(Into::into)
        })
    }

    pub fn publish_named_root_at_millis(
        &self,
        name: Vec<u8>,
        tree: TreeRecord,
        timestamp_millis: u64,
    ) -> Result<(), ProllyBindingError> {
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .publish_named_root_at_millis(&name, &tree, timestamp_millis)
                .map_err(Into::into)
        })
    }

    pub fn delete_named_root(&self, name: Vec<u8>) -> Result<(), ProllyBindingError> {
        with_engine!(self, engine, {
            engine.delete_named_root(&name).map_err(Into::into)
        })
    }

    pub fn compare_and_swap_named_root(
        &self,
        name: Vec<u8>,
        expected: Option<TreeRecord>,
        replacement: Option<TreeRecord>,
    ) -> Result<NamedRootUpdateRecord, ProllyBindingError> {
        let expected = expected.map(Tree::try_from).transpose()?;
        let replacement = replacement.map(Tree::try_from).transpose()?;
        with_engine!(self, engine, {
            engine
                .compare_and_swap_named_root(&name, expected.as_ref(), replacement.as_ref())
                .map(NamedRootUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn compare_and_swap_named_root_at_millis(
        &self,
        name: Vec<u8>,
        expected: Option<TreeRecord>,
        replacement: Option<TreeRecord>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdateRecord, ProllyBindingError> {
        let expected = expected.map(Tree::try_from).transpose()?;
        let replacement = replacement.map(Tree::try_from).transpose()?;
        with_engine!(self, engine, {
            engine
                .compare_and_swap_named_root_at_millis(
                    &name,
                    expected.as_ref(),
                    replacement.as_ref(),
                    timestamp_millis,
                )
                .map(NamedRootUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn load_snapshot(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
    ) -> Result<Option<TreeRecord>, ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .load(&id)
                .map(|tree| tree.map(TreeRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn load_snapshots(
        &self,
        namespace: SnapshotNamespaceRecord,
        ids: Vec<Vec<u8>>,
    ) -> Result<SnapshotSelectionRecord, ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .load_many(ids)
                .map(SnapshotSelectionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn list_snapshots(
        &self,
        namespace: SnapshotNamespaceRecord,
    ) -> Result<Vec<SnapshotRecord>, ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .list()
                .map(|snapshots| snapshots.into_iter().map(SnapshotRecord::from).collect())
                .map_err(Into::into)
        })
    }

    pub fn publish_snapshot(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
        tree: TreeRecord,
    ) -> Result<(), ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .publish(&id, &tree)
                .map_err(Into::into)
        })
    }

    pub fn publish_snapshot_at_millis(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
        tree: TreeRecord,
        timestamp_millis: u64,
    ) -> Result<(), ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        let tree = tree.try_into()?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .publish_at_millis(&id, &tree, timestamp_millis)
                .map_err(Into::into)
        })
    }

    pub fn delete_snapshot(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
    ) -> Result<(), ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .delete(&id)
                .map_err(Into::into)
        })
    }

    pub fn compare_and_swap_snapshot(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
        expected: Option<TreeRecord>,
        replacement: Option<TreeRecord>,
    ) -> Result<NamedRootUpdateRecord, ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        let expected = expected.map(Tree::try_from).transpose()?;
        let replacement = replacement.map(Tree::try_from).transpose()?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .compare_and_swap(&id, expected.as_ref(), replacement.as_ref())
                .map(NamedRootUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn compare_and_swap_snapshot_at_millis(
        &self,
        namespace: SnapshotNamespaceRecord,
        id: Vec<u8>,
        expected: Option<TreeRecord>,
        replacement: Option<TreeRecord>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdateRecord, ProllyBindingError> {
        let namespace = SnapshotNamespace::try_from(namespace)?;
        let expected = expected.map(Tree::try_from).transpose()?;
        let replacement = replacement.map(Tree::try_from).transpose()?;
        with_engine!(self, engine, {
            engine
                .snapshots(namespace.clone())
                .compare_and_swap_at_millis(
                    &id,
                    expected.as_ref(),
                    replacement.as_ref(),
                    timestamp_millis,
                )
                .map(NamedRootUpdateRecord::from)
                .map_err(Into::into)
        })
    }
}

impl ProllyEngine {
    pub fn merge_with_host_resolver<F>(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: F,
    ) -> Result<TreeRecord, ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_host_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge(&base, &left, &right, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_explain_with_host_resolver<F>(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        resolver: F,
    ) -> Result<MergeExplanationRecord, ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_host_callback(resolver);
        with_engine!(self, engine, {
            let explanation = engine.merge_explain(&base, &left, &right, Some(resolver));
            MergeExplanationRecord::try_from(explanation)
        })
    }

    pub fn merge_range_with_host_resolver<F>(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        start: Vec<u8>,
        end: Option<Vec<u8>>,
        resolver: F,
    ) -> Result<TreeRecord, ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_host_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge_range(&base, &left, &right, &start, end.as_deref(), Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn merge_prefix_with_host_resolver<F>(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        prefix: Vec<u8>,
        resolver: F,
    ) -> Result<TreeRecord, ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
    {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let resolver = resolver_from_host_callback(resolver);
        with_engine!(self, engine, {
            engine
                .merge_prefix(&base, &left, &right, &prefix, Some(resolver))
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn crdt_merge_with_host_resolver<F>(
        &self,
        base: TreeRecord,
        left: TreeRecord,
        right: TreeRecord,
        delete_policy: CrdtDeletePolicyKind,
        resolver: F,
    ) -> Result<TreeRecord, ProllyBindingError>
    where
        F: Fn(ConflictRecord) -> CrdtResolutionRecord + 'static,
    {
        let base = base.try_into()?;
        let left = left.try_into()?;
        let right = right.try_into()?;
        let config = crdt_config_from_host_callback(delete_policy, resolver);
        with_engine!(self, engine, {
            engine
                .crdt_merge(&base, &left, &right, &config)
                .map(TreeRecord::from)
                .map_err(Into::into)
        })
    }
}

#[cfg(feature = "sqlite")]
#[uniffi::export]
impl ProllyEngine {
    #[uniffi::constructor]
    pub fn sqlite(path: String, config: ConfigRecord) -> Result<Self, ProllyBindingError> {
        let config = config.try_into()?;
        let store = Arc::new(SqliteStore::open(path).map_err(store_error)?);
        Ok(Self {
            inner: BindingEngine::Sqlite(Prolly::new(store, config)),
        })
    }

    #[uniffi::constructor]
    pub fn sqlite_in_memory(config: ConfigRecord) -> Result<Self, ProllyBindingError> {
        let config = config.try_into()?;
        let store = Arc::new(SqliteStore::open_in_memory().map_err(store_error)?);
        Ok(Self {
            inner: BindingEngine::Sqlite(Prolly::new(store, config)),
        })
    }
}

#[uniffi::export]
pub fn default_config() -> ConfigRecord {
    Config::default().into()
}

#[uniffi::export]
pub fn encoding_raw() -> EncodingRecord {
    Encoding::Raw.into()
}

#[uniffi::export]
pub fn encoding_cbor() -> EncodingRecord {
    Encoding::Cbor.into()
}

#[uniffi::export]
pub fn encoding_json() -> EncodingRecord {
    Encoding::Json.into()
}

#[uniffi::export]
pub fn encoding_custom(name: String) -> EncodingRecord {
    Encoding::Custom(name).into()
}

#[uniffi::export]
pub fn tree_config(
    min_chunk_size: u64,
    max_chunk_size: u64,
    chunking_factor: u32,
    hash_seed: u64,
    encoding: EncodingRecord,
    node_cache_max_nodes: Option<u64>,
    node_cache_max_bytes: Option<u64>,
) -> Result<ConfigRecord, ProllyBindingError> {
    let record = ConfigRecord {
        min_chunk_size,
        max_chunk_size,
        chunking_factor,
        hash_seed,
        encoding,
        node_cache_max_nodes,
        node_cache_max_bytes,
    };
    Config::try_from(record.clone())?;
    Ok(record)
}

#[uniffi::export]
pub fn default_large_value_config() -> LargeValueConfigRecord {
    LargeValueConfig::default().into()
}

#[uniffi::export]
pub fn large_value_config(
    inline_threshold: u64,
) -> Result<LargeValueConfigRecord, ProllyBindingError> {
    let record = LargeValueConfigRecord { inline_threshold };
    LargeValueConfig::try_from(record.clone())?;
    Ok(record)
}

#[uniffi::export]
pub fn default_parallel_config() -> ParallelConfigRecord {
    ParallelConfig::default().into()
}

#[uniffi::export]
pub fn parallel_config(
    max_threads: u64,
    parallelism_threshold: u64,
) -> Result<ParallelConfigRecord, ProllyBindingError> {
    let record = ParallelConfigRecord {
        max_threads,
        parallelism_threshold,
    };
    ParallelConfig::try_from(record.clone())?;
    Ok(record)
}

#[uniffi::export]
pub fn parallel_config_sequential() -> ParallelConfigRecord {
    ParallelConfig::sequential().into()
}

#[uniffi::export]
pub fn cid_from_bytes(bytes: Vec<u8>) -> Vec<u8> {
    Cid::from_bytes(&bytes).as_bytes().to_vec()
}

#[uniffi::export]
pub fn node_from_bytes(bytes: Vec<u8>) -> Result<NodeRecord, ProllyBindingError> {
    Node::from_bytes(&bytes)
        .map(NodeRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn node_to_bytes(node: NodeRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let node = Node::try_from(node)?;
    Ok(node.to_bytes())
}

#[uniffi::export]
pub fn node_cid(node: NodeRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let node = Node::try_from(node)?;
    Ok(node.cid().as_bytes().to_vec())
}

#[uniffi::export]
pub fn verify_key_proof(
    proof: KeyProofRecord,
) -> Result<KeyProofVerificationRecord, ProllyBindingError> {
    let proof = KeyProof::try_from(proof)?;
    Ok(prolly::verify_key_proof(&proof).into())
}

#[uniffi::export]
pub fn verify_multi_key_proof(
    proof: MultiKeyProofRecord,
) -> Result<MultiKeyProofVerificationRecord, ProllyBindingError> {
    let proof = MultiKeyProof::try_from(proof)?;
    Ok(prolly::verify_multi_key_proof(&proof).into())
}

#[uniffi::export]
pub fn verify_range_proof(
    proof: RangeProofRecord,
) -> Result<RangeProofVerificationRecord, ProllyBindingError> {
    let proof = RangeProof::try_from(proof)?;
    Ok(prolly::verify_range_proof(&proof).into())
}

#[uniffi::export]
pub fn verify_range_page_proof(
    proof: RangePageProofRecord,
) -> Result<RangePageProofVerificationRecord, ProllyBindingError> {
    let proof = RangePageProof::try_from(proof)?;
    Ok(prolly::verify_range_page_proof(&proof).into())
}

#[uniffi::export]
pub fn verify_diff_page_proof(
    proof: DiffPageProofRecord,
) -> Result<DiffPageProofVerificationRecord, ProllyBindingError> {
    let proof = DiffPageProof::try_from(proof)?;
    Ok(prolly::verify_diff_page_proof(&proof).try_into()?)
}

#[uniffi::export]
pub fn inspect_proof_bundle(
    bytes: Vec<u8>,
) -> Result<ProofBundleSummaryRecord, ProllyBindingError> {
    prolly::inspect_proof_bundle(&bytes).map(ProofBundleSummaryRecord::try_from)?
}

#[uniffi::export]
pub fn verify_proof_bundle(
    bytes: Vec<u8>,
) -> Result<ProofBundleVerificationRecord, ProllyBindingError> {
    prolly::verify_proof_bundle(&bytes).map(ProofBundleVerificationRecord::try_from)?
}

#[uniffi::export]
pub fn sign_proof_bundle_hmac_sha256(
    proof_bundle: Vec<u8>,
    key_id: Vec<u8>,
    secret: Vec<u8>,
    context: Vec<u8>,
    issued_at_millis: Option<u64>,
    expires_at_millis: Option<u64>,
    nonce: Vec<u8>,
) -> Result<AuthenticatedProofEnvelopeRecord, ProllyBindingError> {
    prolly::sign_proof_bundle_hmac_sha256(
        proof_bundle,
        key_id,
        &secret,
        context,
        issued_at_millis,
        expires_at_millis,
        nonce,
    )
    .map(AuthenticatedProofEnvelopeRecord::from)
    .map_err(Into::into)
}

#[uniffi::export]
pub fn verify_authenticated_proof_envelope(
    envelope: AuthenticatedProofEnvelopeRecord,
    secret: Vec<u8>,
    now_millis: Option<u64>,
) -> AuthenticatedProofEnvelopeVerificationRecord {
    let envelope = AuthenticatedProofEnvelope::from(envelope);
    prolly::verify_authenticated_proof_envelope(&envelope, &secret, now_millis).into()
}

#[uniffi::export]
pub fn verify_authenticated_proof_bundle(
    envelope_bytes: Vec<u8>,
    secret: Vec<u8>,
    now_millis: Option<u64>,
) -> Result<AuthenticatedProofBundleVerificationRecord, ProllyBindingError> {
    prolly::verify_authenticated_proof_bundle(&envelope_bytes, &secret, now_millis)
        .map(AuthenticatedProofBundleVerificationRecord::try_from)?
}

#[uniffi::export]
pub fn authenticated_proof_envelope_to_bytes(
    envelope: AuthenticatedProofEnvelopeRecord,
) -> Result<Vec<u8>, ProllyBindingError> {
    let envelope = AuthenticatedProofEnvelope::from(envelope);
    envelope.to_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn authenticated_proof_envelope_from_bytes(
    bytes: Vec<u8>,
) -> Result<AuthenticatedProofEnvelopeRecord, ProllyBindingError> {
    AuthenticatedProofEnvelope::from_bytes(&bytes)
        .map(AuthenticatedProofEnvelopeRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn key_proof_path_node_bytes(
    proof: KeyProofRecord,
) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    let proof = KeyProof::try_from(proof)?;
    Ok(proof.path_node_bytes())
}

#[uniffi::export]
pub fn key_proof_to_bytes(proof: KeyProofRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let proof = KeyProof::try_from(proof)?;
    proof.to_bundle_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn key_proof_from_bytes(bytes: Vec<u8>) -> Result<KeyProofRecord, ProllyBindingError> {
    KeyProof::from_bundle_bytes(&bytes)
        .map(KeyProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn multi_key_proof_path_node_bytes(
    proof: MultiKeyProofRecord,
) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    let proof = MultiKeyProof::try_from(proof)?;
    Ok(proof.path_node_bytes())
}

#[uniffi::export]
pub fn multi_key_proof_to_bytes(proof: MultiKeyProofRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let proof = MultiKeyProof::try_from(proof)?;
    proof.to_bundle_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn multi_key_proof_from_bytes(
    bytes: Vec<u8>,
) -> Result<MultiKeyProofRecord, ProllyBindingError> {
    MultiKeyProof::from_bundle_bytes(&bytes)
        .map(MultiKeyProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn range_proof_path_node_bytes(
    proof: RangeProofRecord,
) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    let proof = RangeProof::try_from(proof)?;
    Ok(proof.path_node_bytes())
}

#[uniffi::export]
pub fn range_proof_to_bytes(proof: RangeProofRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let proof = RangeProof::try_from(proof)?;
    proof.to_bundle_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn range_proof_from_bytes(bytes: Vec<u8>) -> Result<RangeProofRecord, ProllyBindingError> {
    RangeProof::from_bundle_bytes(&bytes)
        .map(RangeProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn range_page_proof_path_node_bytes(
    proof: RangePageProofRecord,
) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    let proof = RangePageProof::try_from(proof)?;
    Ok(proof.path_node_bytes())
}

#[uniffi::export]
pub fn range_page_proof_to_bytes(
    proof: RangePageProofRecord,
) -> Result<Vec<u8>, ProllyBindingError> {
    let proof = RangePageProof::try_from(proof)?;
    proof.to_bundle_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn range_page_proof_from_bytes(
    bytes: Vec<u8>,
) -> Result<RangePageProofRecord, ProllyBindingError> {
    RangePageProof::from_bundle_bytes(&bytes)
        .map(RangePageProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn diff_page_proof_to_bytes(proof: DiffPageProofRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let proof = DiffPageProof::try_from(proof)?;
    proof.to_bundle_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn diff_page_proof_from_bytes(
    bytes: Vec<u8>,
) -> Result<DiffPageProofRecord, ProllyBindingError> {
    DiffPageProof::from_bundle_bytes(&bytes).map(DiffPageProofRecord::try_from)?
}

#[uniffi::export]
pub fn key_proof_from_node_bytes(
    root: Option<Vec<u8>>,
    key: Vec<u8>,
    path_node_bytes: Vec<Vec<u8>>,
) -> Result<KeyProofRecord, ProllyBindingError> {
    let root = root.map(cid_from_vec).transpose()?;
    KeyProof::from_node_bytes(root, key, path_node_bytes)
        .map(KeyProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn multi_key_proof_from_node_bytes(
    root: Option<Vec<u8>>,
    keys: Vec<Vec<u8>>,
    path_node_bytes: Vec<Vec<u8>>,
) -> Result<MultiKeyProofRecord, ProllyBindingError> {
    let root = root.map(cid_from_vec).transpose()?;
    MultiKeyProof::from_node_bytes(root, keys, path_node_bytes)
        .map(MultiKeyProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn range_proof_from_node_bytes(
    root: Option<Vec<u8>>,
    start: Vec<u8>,
    end: Option<Vec<u8>>,
    path_node_bytes: Vec<Vec<u8>>,
) -> Result<RangeProofRecord, ProllyBindingError> {
    let root = root.map(cid_from_vec).transpose()?;
    RangeProof::from_node_bytes(root, start, end, path_node_bytes)
        .map(RangeProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn range_page_proof_from_node_bytes(
    root: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
    path_node_bytes: Vec<Vec<u8>>,
) -> Result<RangePageProofRecord, ProllyBindingError> {
    let root = root.map(cid_from_vec).transpose()?;
    RangePageProof::from_node_bytes(root, after, end, path_node_bytes)
        .map(RangePageProofRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn is_boundary_config(
    config: ConfigRecord,
    count: u64,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<bool, ProllyBindingError> {
    let config = Config::try_from(config)?;
    let count = usize::try_from(count).map_err(|_| invalid_argument("count is too large"))?;
    Ok(core_is_boundary_config(&config, count, &key, &value))
}

#[uniffi::export]
pub fn prefix_end(prefix: Vec<u8>) -> Option<Vec<u8>> {
    prolly::prefix_end(prefix)
}

#[uniffi::export]
pub fn prefix_range(prefix: Vec<u8>) -> RangeBoundsRecord {
    let (start, end) = prolly::prefix_range(prefix);
    RangeBoundsRecord { start, end }
}

#[uniffi::export]
pub fn range_cursor_start() -> RangeCursorRecord {
    RangeCursor::start().into()
}

#[uniffi::export]
pub fn range_cursor_after_key(key: Vec<u8>) -> RangeCursorRecord {
    RangeCursor::after_key(key).into()
}

#[uniffi::export]
pub fn reverse_cursor_end() -> ReverseCursorRecord {
    ReverseCursor::end().into()
}

#[uniffi::export]
pub fn reverse_cursor_before_key(key: Vec<u8>) -> ReverseCursorRecord {
    ReverseCursor::before_key(key).into()
}

#[uniffi::export]
pub fn upsert_mutation(key: Vec<u8>, value: Vec<u8>) -> MutationRecord {
    Mutation::Upsert { key, val: value }.into()
}

#[uniffi::export]
pub fn delete_mutation(key: Vec<u8>) -> MutationRecord {
    Mutation::Delete { key }.into()
}

#[uniffi::export]
pub fn resolution_value(value: Vec<u8>) -> ResolutionRecord {
    prolly::Resolution::value(value).into()
}

#[uniffi::export]
pub fn resolution_delete() -> ResolutionRecord {
    prolly::Resolution::delete().into()
}

#[uniffi::export]
pub fn resolution_unresolved() -> ResolutionRecord {
    prolly::Resolution::unresolved().into()
}

#[uniffi::export]
pub fn resolve_prefer_left(conflict: ConflictRecord) -> ResolutionRecord {
    prolly::resolver::prefer_left(&conflict.into()).into()
}

#[uniffi::export]
pub fn resolve_prefer_right(conflict: ConflictRecord) -> ResolutionRecord {
    prolly::resolver::prefer_right(&conflict.into()).into()
}

#[uniffi::export]
pub fn resolve_delete_wins(conflict: ConflictRecord) -> ResolutionRecord {
    prolly::resolver::delete_wins(&conflict.into()).into()
}

#[uniffi::export]
pub fn resolve_update_wins(conflict: ConflictRecord) -> ResolutionRecord {
    prolly::resolver::update_wins(&conflict.into()).into()
}

#[uniffi::export]
pub fn crdt_resolution_value(value: Vec<u8>) -> CrdtResolutionRecord {
    prolly::CrdtResolution::value(value).into()
}

#[uniffi::export]
pub fn crdt_resolution_delete() -> CrdtResolutionRecord {
    prolly::CrdtResolution::delete().into()
}

#[uniffi::export]
pub fn snapshot_namespace_branch() -> SnapshotNamespaceRecord {
    SnapshotNamespace::Branch.into()
}

#[uniffi::export]
pub fn snapshot_namespace_tag() -> SnapshotNamespaceRecord {
    SnapshotNamespace::Tag.into()
}

#[uniffi::export]
pub fn snapshot_namespace_checkpoint() -> SnapshotNamespaceRecord {
    SnapshotNamespace::Checkpoint.into()
}

#[uniffi::export]
pub fn snapshot_namespace_custom(prefix: Vec<u8>) -> SnapshotNamespaceRecord {
    SnapshotNamespace::Custom(prefix).into()
}

#[uniffi::export]
pub fn snapshot_root_name(
    namespace: SnapshotNamespaceRecord,
    id: Vec<u8>,
) -> Result<Vec<u8>, ProllyBindingError> {
    let namespace = SnapshotNamespace::try_from(namespace)?;
    Ok(prolly::snapshot_root_name(&namespace, id))
}

#[uniffi::export]
pub fn snapshot_id_from_name(
    namespace: SnapshotNamespaceRecord,
    name: Vec<u8>,
) -> Result<Option<Vec<u8>>, ProllyBindingError> {
    let namespace = SnapshotNamespace::try_from(namespace)?;
    Ok(prolly::snapshot_id_from_name(&namespace, name))
}

#[uniffi::export]
pub fn u64_key(value: u64) -> Vec<u8> {
    prolly::u64_key(value).to_vec()
}

#[uniffi::export]
pub fn u128_key(value: String) -> Result<Vec<u8>, ProllyBindingError> {
    let value = value
        .parse::<u128>()
        .map_err(|error| invalid_argument(error.to_string()))?;
    Ok(prolly::u128_key(value).to_vec())
}

#[uniffi::export]
pub fn i64_key(value: i64) -> Vec<u8> {
    prolly::i64_key(value).to_vec()
}

#[uniffi::export]
pub fn i128_key(value: String) -> Result<Vec<u8>, ProllyBindingError> {
    let value = value
        .parse::<i128>()
        .map_err(|error| invalid_argument(error.to_string()))?;
    Ok(prolly::i128_key(value).to_vec())
}

#[uniffi::export]
pub fn timestamp_millis_key(value: u64) -> Vec<u8> {
    prolly::timestamp_millis_key(value).to_vec()
}

#[uniffi::export]
pub fn encode_segment(segment: Vec<u8>) -> Vec<u8> {
    prolly::encode_segment(segment)
}

#[uniffi::export]
pub fn key_from_segments(segments: Vec<Vec<u8>>) -> Vec<u8> {
    let mut builder = KeyBuilder::new();
    for segment in segments {
        builder = builder.push_segment(segment);
    }
    builder.finish()
}

#[uniffi::export]
pub fn key_from_prefixed_segments(prefix: Vec<u8>, segments: Vec<Vec<u8>>) -> Vec<u8> {
    let mut builder = KeyBuilder::from_prefix(prefix);
    for segment in segments {
        builder = builder.push_segment(segment);
    }
    builder.finish()
}

#[uniffi::export]
pub fn changed_span(start: Vec<u8>, end: Option<Vec<u8>>) -> ChangedSpanRecord {
    ChangedSpan::new(start, end).into()
}

#[uniffi::export]
pub fn changed_span_from_key(key: Vec<u8>) -> ChangedSpanRecord {
    ChangedSpan::from_key(key).into()
}

#[uniffi::export]
pub fn changed_span_for_prefix(prefix: Vec<u8>) -> ChangedSpanRecord {
    ChangedSpan::for_prefix(prefix).into()
}

#[uniffi::export]
pub fn retain_all_named_roots() -> NamedRootRetentionRecord {
    NamedRootRetention::all().into()
}

#[uniffi::export]
pub fn retain_exact_named_roots(names: Vec<Vec<u8>>) -> NamedRootRetentionRecord {
    NamedRootRetention::exact(names).into()
}

#[uniffi::export]
pub fn retain_named_root_prefix(prefix: Vec<u8>) -> NamedRootRetentionRecord {
    NamedRootRetention::prefix(prefix).into()
}

#[uniffi::export]
pub fn retain_newest_named_roots(prefix: Vec<u8>, count: u64) -> NamedRootRetentionRecord {
    NamedRootRetentionRecord {
        kind: NamedRootRetentionKind::NewestByName,
        names: Vec::new(),
        prefix,
        count: Some(count),
        min_updated_at_millis: None,
    }
}

#[uniffi::export]
pub fn retain_named_roots_updated_since(
    prefix: Vec<u8>,
    min_updated_at_millis: u64,
) -> NamedRootRetentionRecord {
    NamedRootRetention::updated_since(prefix, min_updated_at_millis).into()
}

#[uniffi::export]
pub fn decode_segments(key: Vec<u8>) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    prolly::decode_segments(&key).map_err(|error| ProllyBindingError::InvalidArgument {
        message: error.to_string(),
    })
}

#[uniffi::export]
pub fn debug_key(key: Vec<u8>) -> String {
    prolly::debug_key(&key)
}

#[uniffi::export]
pub fn versioned_value_to_bytes(
    record: VersionedValueRecord,
) -> Result<Vec<u8>, ProllyBindingError> {
    let value = VersionedValue::try_from(record)?;
    value.to_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn versioned_value_from_bytes(
    bytes: Vec<u8>,
) -> Result<VersionedValueRecord, ProllyBindingError> {
    VersionedValue::from_bytes(&bytes)
        .map(VersionedValueRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn versioned_value_matches_schema(
    record: VersionedValueRecord,
    schema: String,
    version: u64,
) -> Result<bool, ProllyBindingError> {
    let value = VersionedValue::try_from(record)?;
    Ok(value.matches_schema(&schema, version))
}

#[uniffi::export]
pub fn versioned_value_require_schema(
    record: VersionedValueRecord,
    schema: String,
    version: u64,
) -> Result<(), ProllyBindingError> {
    let value = VersionedValue::try_from(record)?;
    value.require_schema(&schema, version).map_err(Into::into)
}

#[uniffi::export]
pub fn versioned_value_bytes_matches_schema(
    bytes: Vec<u8>,
    schema: String,
    version: u64,
) -> Result<bool, ProllyBindingError> {
    VersionedValue::from_bytes(&bytes)
        .map(|value| value.matches_schema(&schema, version))
        .map_err(Into::into)
}

#[uniffi::export]
pub fn versioned_value_bytes_require_schema(
    bytes: Vec<u8>,
    schema: String,
    version: u64,
) -> Result<(), ProllyBindingError> {
    VersionedValue::from_bytes(&bytes)
        .and_then(|value| value.require_schema(&schema, version))
        .map_err(Into::into)
}

#[uniffi::export]
pub fn value_ref_to_bytes(record: ValueRefRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let value_ref = ValueRef::try_from(record)?;
    Ok(value_ref.to_bytes())
}

#[uniffi::export]
pub fn value_ref_from_bytes(bytes: Vec<u8>) -> Result<ValueRefRecord, ProllyBindingError> {
    ValueRef::from_bytes(&bytes)
        .map(ValueRefRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn value_ref_from_stored_bytes(bytes: Vec<u8>) -> Result<ValueRefRecord, ProllyBindingError> {
    ValueRef::from_stored_bytes(&bytes)
        .map(ValueRefRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn value_ref_inline_requires_escape(value: Vec<u8>) -> bool {
    ValueRef::inline_requires_escape(&value)
}

#[uniffi::export]
pub fn blob_ref_validate_bytes(
    reference: BlobRefRecord,
    bytes: Vec<u8>,
) -> Result<(), ProllyBindingError> {
    let reference = BlobRef::try_from(reference)?;
    reference.validate_bytes(&bytes).map_err(Into::into)
}

#[uniffi::export]
pub fn root_manifest_to_bytes(record: RootManifestRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let manifest = RootManifest::try_from(record)?;
    manifest.to_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn root_manifest_from_bytes(bytes: Vec<u8>) -> Result<RootManifestRecord, ProllyBindingError> {
    RootManifest::from_bytes(&bytes)
        .map(RootManifestRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn snapshot_bundle_to_bytes(
    record: SnapshotBundleRecord,
) -> Result<Vec<u8>, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::try_from(record)?;
    bundle.to_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn snapshot_bundle_from_bytes(
    bytes: Vec<u8>,
) -> Result<SnapshotBundleRecord, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::from_bytes(&bytes)?;
    SnapshotBundleRecord::try_from(bundle)
}

#[uniffi::export]
pub fn snapshot_bundle_digest(record: SnapshotBundleRecord) -> Result<Vec<u8>, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::try_from(record)?;
    bundle
        .digest()
        .map(|cid| cid.as_bytes().to_vec())
        .map_err(Into::into)
}

#[uniffi::export]
pub fn snapshot_bundle_digest_bytes(bytes: Vec<u8>) -> Result<Vec<u8>, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::from_bytes(&bytes)?;
    bundle
        .digest()
        .map(|cid| cid.as_bytes().to_vec())
        .map_err(Into::into)
}

#[uniffi::export]
pub fn snapshot_bundle_summary(
    record: SnapshotBundleRecord,
) -> Result<SnapshotBundleSummaryRecord, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::try_from(record)?;
    bundle.summary().map(TryInto::try_into)?
}

#[uniffi::export]
pub fn snapshot_bundle_summary_from_bytes(
    bytes: Vec<u8>,
) -> Result<SnapshotBundleSummaryRecord, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::from_bytes(&bytes)?;
    bundle.summary().map(TryInto::try_into)?
}

#[uniffi::export]
pub fn verify_snapshot_bundle(
    record: SnapshotBundleRecord,
) -> Result<SnapshotBundleVerificationRecord, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::try_from(record)?;
    bundle.verify().map(TryInto::try_into)?
}

#[uniffi::export]
pub fn verify_snapshot_bundle_bytes(
    bytes: Vec<u8>,
) -> Result<SnapshotBundleVerificationRecord, ProllyBindingError> {
    let bundle = CoreSnapshotBundle::from_bytes(&bytes)?;
    bundle.verify().map(TryInto::try_into)?
}

#[uniffi::export]
pub fn crdt_config_lww(delete_policy: CrdtDeletePolicyKind) -> CrdtConfigRecord {
    CrdtConfigRecord {
        strategy: CrdtMergeStrategyKind::LastWriterWins,
        delete_policy,
    }
}

#[uniffi::export]
pub fn crdt_config_multi_value(delete_policy: CrdtDeletePolicyKind) -> CrdtConfigRecord {
    CrdtConfigRecord {
        strategy: CrdtMergeStrategyKind::MultiValue,
        delete_policy,
    }
}

#[uniffi::export]
pub fn timestamped_value_to_bytes(record: TimestampedValueRecord) -> Vec<u8> {
    TimestampedValue::from(record).to_bytes()
}

#[uniffi::export]
pub fn timestamped_value_from_bytes(
    bytes: Vec<u8>,
) -> Result<TimestampedValueRecord, ProllyBindingError> {
    TimestampedValue::from_bytes(&bytes)
        .map(TimestampedValueRecord::from)
        .ok_or_else(|| invalid_argument("timestamped value must be at least 8 bytes"))
}

#[uniffi::export]
pub fn timestamped_value_now(value: Vec<u8>) -> TimestampedValueRecord {
    TimestampedValue::now(value).into()
}

#[uniffi::export]
pub fn multi_value_set_to_bytes(values: Vec<Vec<u8>>) -> Vec<u8> {
    MultiValueSet::from_values(values).to_bytes()
}

#[uniffi::export]
pub fn multi_value_set_from_bytes(bytes: Vec<u8>) -> Result<Vec<Vec<u8>>, ProllyBindingError> {
    MultiValueSet::from_bytes(&bytes)
        .map(|set| set.values)
        .ok_or_else(|| invalid_argument("invalid multi-value set envelope"))
}

#[uniffi::export]
pub fn multi_value_set_merge(left: Vec<Vec<u8>>, right: Vec<Vec<u8>>) -> Vec<Vec<u8>> {
    MultiValueSet::from_values(left)
        .merge(&MultiValueSet::from_values(right))
        .values
}

#[uniffi::export]
pub fn tombstone_to_bytes(record: TombstoneRecord) -> Result<Vec<u8>, ProllyBindingError> {
    Tombstone::try_from(record)?.to_bytes().map_err(Into::into)
}

#[uniffi::export]
pub fn tombstone_from_bytes(bytes: Vec<u8>) -> Result<TombstoneRecord, ProllyBindingError> {
    Tombstone::from_bytes(&bytes)
        .map(TombstoneRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn tombstone_from_stored_bytes(
    bytes: Vec<u8>,
) -> Result<Option<TombstoneRecord>, ProllyBindingError> {
    Tombstone::from_stored_bytes(&bytes)
        .map(|tombstone| tombstone.map(TombstoneRecord::from))
        .map_err(Into::into)
}

#[uniffi::export]
pub fn is_tombstone_value(bytes: Vec<u8>) -> bool {
    prolly::is_tombstone_value(&bytes)
}

#[uniffi::export]
pub fn tombstone_upsert_mutation(
    key: Vec<u8>,
    tombstone: TombstoneRecord,
) -> Result<MutationRecord, ProllyBindingError> {
    let tombstone = Tombstone::try_from(tombstone)?;
    prolly::tombstone_upsert(key, &tombstone)
        .map(MutationRecord::from)
        .map_err(Into::into)
}

#[uniffi::export]
pub fn tombstone_compaction_mutation(
    key: Vec<u8>,
    stored_value: Vec<u8>,
) -> Result<Option<MutationRecord>, ProllyBindingError> {
    prolly::tombstone_compaction(key, &stored_value)
        .map(|mutation| mutation.map(MutationRecord::from))
        .map_err(Into::into)
}

impl From<Encoding> for EncodingRecord {
    fn from(value: Encoding) -> Self {
        match value {
            Encoding::Raw => Self {
                kind: EncodingKind::Raw,
                custom_name: None,
            },
            Encoding::Cbor => Self {
                kind: EncodingKind::Cbor,
                custom_name: None,
            },
            Encoding::Json => Self {
                kind: EncodingKind::Json,
                custom_name: None,
            },
            Encoding::Custom(name) => Self {
                kind: EncodingKind::Custom,
                custom_name: Some(name),
            },
        }
    }
}

impl TryFrom<EncodingRecord> for Encoding {
    type Error = ProllyBindingError;

    fn try_from(value: EncodingRecord) -> Result<Self, Self::Error> {
        match value.kind {
            EncodingKind::Raw => Ok(Self::Raw),
            EncodingKind::Cbor => Ok(Self::Cbor),
            EncodingKind::Json => Ok(Self::Json),
            EncodingKind::Custom => value
                .custom_name
                .map(Self::Custom)
                .ok_or_else(|| invalid_argument("custom encoding requires custom_name")),
        }
    }
}

impl From<Config> for ConfigRecord {
    fn from(value: Config) -> Self {
        Self {
            min_chunk_size: value.min_chunk_size as u64,
            max_chunk_size: value.max_chunk_size as u64,
            chunking_factor: value.chunking_factor,
            hash_seed: value.hash_seed,
            encoding: value.encoding.into(),
            node_cache_max_nodes: value.node_cache_max_nodes.map(|value| value as u64),
            node_cache_max_bytes: value.node_cache_max_bytes.map(|value| value as u64),
        }
    }
}

impl TryFrom<ConfigRecord> for Config {
    type Error = ProllyBindingError;

    fn try_from(value: ConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            min_chunk_size: to_usize(value.min_chunk_size, "min_chunk_size")?,
            max_chunk_size: to_usize(value.max_chunk_size, "max_chunk_size")?,
            chunking_factor: value.chunking_factor,
            hash_seed: value.hash_seed,
            encoding: value.encoding.try_into()?,
            node_cache_max_nodes: value
                .node_cache_max_nodes
                .map(|value| to_usize(value, "node_cache_max_nodes"))
                .transpose()?,
            node_cache_max_bytes: value
                .node_cache_max_bytes
                .map(|value| to_usize(value, "node_cache_max_bytes"))
                .transpose()?,
        })
    }
}

impl From<ParallelConfig> for ParallelConfigRecord {
    fn from(value: ParallelConfig) -> Self {
        Self {
            max_threads: value.max_threads as u64,
            parallelism_threshold: value.parallelism_threshold as u64,
        }
    }
}

impl TryFrom<ParallelConfigRecord> for ParallelConfig {
    type Error = ProllyBindingError;

    fn try_from(value: ParallelConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(
            to_usize(value.max_threads, "max_threads")?,
            to_usize(value.parallelism_threshold, "parallelism_threshold")?,
        ))
    }
}

impl From<Tree> for TreeRecord {
    fn from(value: Tree) -> Self {
        Self {
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            config: value.config.into(),
        }
    }
}

impl TryFrom<TreeRecord> for Tree {
    type Error = ProllyBindingError;

    fn try_from(value: TreeRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: value.root.map(cid_from_vec).transpose()?,
            config: value.config.try_into()?,
        })
    }
}

impl TryFrom<CoreSnapshotBundle> for SnapshotBundleRecord {
    type Error = ProllyBindingError;

    fn try_from(value: CoreSnapshotBundle) -> Result<Self, Self::Error> {
        Ok(Self {
            format_version: value.format_version,
            tree: value.tree.into(),
            nodes: value.nodes.into_iter().map(Into::into).collect(),
        })
    }
}

impl TryFrom<SnapshotBundleRecord> for CoreSnapshotBundle {
    type Error = ProllyBindingError;

    fn try_from(value: SnapshotBundleRecord) -> Result<Self, Self::Error> {
        let bundle = CoreSnapshotBundle {
            format_version: value.format_version,
            tree: value.tree.try_into()?,
            nodes: value
                .nodes
                .into_iter()
                .map(CoreSnapshotBundleNode::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        };
        bundle.validate_format_version()?;
        Ok(bundle)
    }
}

impl TryFrom<SnapshotBundleSummary> for SnapshotBundleSummaryRecord {
    type Error = ProllyBindingError;

    fn try_from(value: SnapshotBundleSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            format_version: value.format_version,
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            node_count: to_u64(value.node_count, "node_count")?,
            byte_count: to_u64(value.byte_count, "byte_count")?,
            min_node_bytes: to_u64(value.min_node_bytes, "min_node_bytes")?,
            max_node_bytes: to_u64(value.max_node_bytes, "max_node_bytes")?,
        })
    }
}

impl TryFrom<SnapshotBundleVerification> for SnapshotBundleVerificationRecord {
    type Error = ProllyBindingError;

    fn try_from(value: SnapshotBundleVerification) -> Result<Self, Self::Error> {
        Ok(Self {
            valid: value.valid,
            summary: value.summary.try_into()?,
            reachable_nodes: to_u64(value.reachable_nodes, "reachable_nodes")?,
            reachable_bytes: to_u64(value.reachable_bytes, "reachable_bytes")?,
            missing_cids: cid_records(value.missing_cids),
            extra_cids: cid_records(value.extra_cids),
        })
    }
}

impl From<CoreSnapshotBundleNode> for SnapshotBundleNodeRecord {
    fn from(value: CoreSnapshotBundleNode) -> Self {
        Self {
            cid: value.cid.as_bytes().to_vec(),
            bytes: value.bytes,
        }
    }
}

impl TryFrom<SnapshotBundleNodeRecord> for CoreSnapshotBundleNode {
    type Error = ProllyBindingError;

    fn try_from(value: SnapshotBundleNodeRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            cid: cid_from_vec(value.cid)?,
            bytes: value.bytes,
        })
    }
}

impl From<Node> for NodeRecord {
    fn from(value: Node) -> Self {
        Self {
            keys: value.keys,
            vals: value.vals,
            leaf: value.leaf,
            level: value.level,
            min_chunk_size: value.min_chunk_size as u64,
            max_chunk_size: value.max_chunk_size as u64,
            chunking_factor: value.chunking_factor,
            hash_seed: value.hash_seed,
            encoding: value.encoding.into(),
        }
    }
}

impl TryFrom<NodeRecord> for Node {
    type Error = ProllyBindingError;

    fn try_from(value: NodeRecord) -> Result<Self, Self::Error> {
        if value.keys.len() != value.vals.len() {
            return Err(ProllyBindingError::InvalidNode {
                message: "node keys and vals must have the same length".to_string(),
            });
        }

        Ok(Self {
            keys: value.keys,
            vals: value.vals,
            leaf: value.leaf,
            level: value.level,
            min_chunk_size: to_usize(value.min_chunk_size, "min_chunk_size")?,
            max_chunk_size: to_usize(value.max_chunk_size, "max_chunk_size")?,
            chunking_factor: value.chunking_factor,
            hash_seed: value.hash_seed,
            encoding: value.encoding.try_into()?,
        })
    }
}

impl From<KeyProof> for KeyProofRecord {
    fn from(value: KeyProof) -> Self {
        Self {
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            key: value.key,
            path: value.path.into_iter().map(NodeRecord::from).collect(),
        }
    }
}

impl TryFrom<KeyProofRecord> for KeyProof {
    type Error = ProllyBindingError;

    fn try_from(value: KeyProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: value.root.map(cid_from_vec).transpose()?,
            key: value.key,
            path: value
                .path
                .into_iter()
                .map(Node::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<KeyProofVerification> for KeyProofVerificationRecord {
    fn from(value: KeyProofVerification) -> Self {
        let exists = value.exists();
        let absence = value.is_absence();
        Self {
            valid: value.valid,
            exists,
            absence,
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            key: value.key,
            value: value.value,
        }
    }
}

impl From<MultiKeyProof> for MultiKeyProofRecord {
    fn from(value: MultiKeyProof) -> Self {
        Self {
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            keys: value.keys,
            path: value.path.into_iter().map(NodeRecord::from).collect(),
        }
    }
}

impl TryFrom<MultiKeyProofRecord> for MultiKeyProof {
    type Error = ProllyBindingError;

    fn try_from(value: MultiKeyProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: value.root.map(cid_from_vec).transpose()?,
            keys: value.keys,
            path: value
                .path
                .into_iter()
                .map(Node::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<MultiKeyProofVerification> for MultiKeyProofVerificationRecord {
    fn from(value: MultiKeyProofVerification) -> Self {
        Self {
            valid: value.valid,
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            results: value
                .results
                .into_iter()
                .map(KeyProofVerificationRecord::from)
                .collect(),
        }
    }
}

impl From<RangeProof> for RangeProofRecord {
    fn from(value: RangeProof) -> Self {
        Self {
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            start: value.start,
            end: value.end,
            path: value.path.into_iter().map(NodeRecord::from).collect(),
        }
    }
}

impl TryFrom<RangeProofRecord> for RangeProof {
    type Error = ProllyBindingError;

    fn try_from(value: RangeProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: value.root.map(cid_from_vec).transpose()?,
            start: value.start,
            end: value.end,
            path: value
                .path
                .into_iter()
                .map(Node::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<RangeProofVerification> for RangeProofVerificationRecord {
    fn from(value: RangeProofVerification) -> Self {
        Self {
            valid: value.valid,
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            start: value.start,
            end: value.end,
            entries: value
                .entries
                .into_iter()
                .map(|(key, value)| EntryRecord { key, value })
                .collect(),
        }
    }
}

impl From<RangePageProof> for RangePageProofRecord {
    fn from(value: RangePageProof) -> Self {
        Self {
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            after: value.after,
            end: value.end,
            path: value.path.into_iter().map(NodeRecord::from).collect(),
        }
    }
}

impl TryFrom<RangePageProofRecord> for RangePageProof {
    type Error = ProllyBindingError;

    fn try_from(value: RangePageProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            root: value.root.map(cid_from_vec).transpose()?,
            after: value.after,
            end: value.end,
            path: value
                .path
                .into_iter()
                .map(Node::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<RangePageProofVerification> for RangePageProofVerificationRecord {
    fn from(value: RangePageProofVerification) -> Self {
        Self {
            valid: value.valid,
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            after: value.after,
            end: value.end,
            entries: value
                .entries
                .into_iter()
                .map(|(key, value)| EntryRecord { key, value })
                .collect(),
        }
    }
}

impl TryFrom<ProvedRangePage> for ProvedRangePageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: ProvedRangePage) -> Result<Self, Self::Error> {
        Ok(Self {
            page: RangePageRecord::try_from(value.page)?,
            proof: RangePageProofRecord::from(value.proof),
        })
    }
}

impl TryFrom<DiffPageProof> for DiffPageProofRecord {
    type Error = ProllyBindingError;

    fn try_from(value: DiffPageProof) -> Result<Self, Self::Error> {
        Ok(Self {
            base: RangePageProofRecord::from(value.base),
            other: RangePageProofRecord::from(value.other),
            lookahead_base: value.lookahead_base.map(KeyProofRecord::from),
            lookahead_other: value.lookahead_other.map(KeyProofRecord::from),
            requested_end: value.requested_end,
            limit: to_u64(value.limit, "limit")?,
        })
    }
}

impl TryFrom<DiffPageProofRecord> for DiffPageProof {
    type Error = ProllyBindingError;

    fn try_from(value: DiffPageProofRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            base: RangePageProof::try_from(value.base)?,
            other: RangePageProof::try_from(value.other)?,
            lookahead_base: value.lookahead_base.map(KeyProof::try_from).transpose()?,
            lookahead_other: value.lookahead_other.map(KeyProof::try_from).transpose()?,
            requested_end: value.requested_end,
            limit: to_usize(value.limit, "limit")?,
        })
    }
}

impl TryFrom<DiffPageProofVerification> for DiffPageProofVerificationRecord {
    type Error = ProllyBindingError;

    fn try_from(value: DiffPageProofVerification) -> Result<Self, Self::Error> {
        Ok(Self {
            valid: value.valid,
            base_valid: value.base_valid,
            other_valid: value.other_valid,
            lookahead_valid: value.lookahead_valid,
            base_root: value.base_root.map(|cid| cid.as_bytes().to_vec()),
            other_root: value.other_root.map(|cid| cid.as_bytes().to_vec()),
            after: value.after,
            requested_end: value.requested_end,
            proof_end: value.proof_end,
            limit: to_u64(value.limit, "limit")?,
            diffs: value
                .diffs
                .into_iter()
                .map(DiffRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: value.next_cursor.map(RangeCursorRecord::from),
        })
    }
}

impl TryFrom<ProvedDiffPage> for ProvedDiffPageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: ProvedDiffPage) -> Result<Self, Self::Error> {
        Ok(Self {
            page: DiffPageRecord::try_from(value.page)?,
            proof: DiffPageProofRecord::try_from(value.proof)?,
        })
    }
}

impl TryFrom<ProofBundleSummary> for ProofBundleSummaryRecord {
    type Error = ProllyBindingError;

    fn try_from(value: ProofBundleSummary) -> Result<Self, Self::Error> {
        Ok(Self {
            version: value.version,
            kind: value.kind.as_str().to_string(),
            root: value.root.map(|cid| cid.as_bytes().to_vec()),
            other_root: value.other_root.map(|cid| cid.as_bytes().to_vec()),
            key_count: to_u64(value.key_count, "key_count")?,
            path_node_count: to_u64(value.path_node_count, "path_node_count")?,
            start: value.start,
            end: value.end,
            after: value.after,
            requested_end: value.requested_end,
            limit: value
                .limit
                .map(|limit| to_u64(limit, "limit"))
                .transpose()?,
            has_lookahead: value.has_lookahead,
        })
    }
}

impl TryFrom<ProofBundleVerification> for ProofBundleVerificationRecord {
    type Error = ProllyBindingError;

    fn try_from(value: ProofBundleVerification) -> Result<Self, Self::Error> {
        Ok(Self {
            summary: ProofBundleSummaryRecord::try_from(value.summary)?,
            valid: value.valid,
            exists_count: to_u64(value.exists_count, "exists_count")?,
            absence_count: to_u64(value.absence_count, "absence_count")?,
            entry_count: to_u64(value.entry_count, "entry_count")?,
            diff_count: to_u64(value.diff_count, "diff_count")?,
            next_cursor: value.next_cursor.map(RangeCursorRecord::from),
        })
    }
}

impl From<AuthenticatedProofEnvelope> for AuthenticatedProofEnvelopeRecord {
    fn from(value: AuthenticatedProofEnvelope) -> Self {
        Self {
            algorithm: value.algorithm,
            key_id: value.key_id,
            proof_bundle: value.proof_bundle,
            context: value.context,
            issued_at_millis: value.issued_at_millis,
            expires_at_millis: value.expires_at_millis,
            nonce: value.nonce,
            signature: value.signature,
        }
    }
}

impl From<AuthenticatedProofEnvelopeRecord> for AuthenticatedProofEnvelope {
    fn from(value: AuthenticatedProofEnvelopeRecord) -> Self {
        Self {
            algorithm: value.algorithm,
            key_id: value.key_id,
            proof_bundle: value.proof_bundle,
            context: value.context,
            issued_at_millis: value.issued_at_millis,
            expires_at_millis: value.expires_at_millis,
            nonce: value.nonce,
            signature: value.signature,
        }
    }
}

impl From<AuthenticatedProofEnvelopeVerification> for AuthenticatedProofEnvelopeVerificationRecord {
    fn from(value: AuthenticatedProofEnvelopeVerification) -> Self {
        Self {
            valid: value.valid,
            signature_valid: value.signature_valid,
            time_valid: value.time_valid,
            not_yet_valid: value.not_yet_valid,
            expired: value.expired,
            algorithm: value.algorithm,
            key_id: value.key_id,
            proof_bundle: value.proof_bundle,
            context: value.context,
            issued_at_millis: value.issued_at_millis,
            expires_at_millis: value.expires_at_millis,
            nonce: value.nonce,
        }
    }
}

impl TryFrom<MutationRecord> for Mutation {
    type Error = ProllyBindingError;

    fn try_from(value: MutationRecord) -> Result<Self, Self::Error> {
        match value.kind {
            MutationKind::Upsert => Ok(Self::Upsert {
                key: value.key,
                val: value
                    .value
                    .ok_or_else(|| invalid_argument("upsert mutation requires value"))?,
            }),
            MutationKind::Delete => Ok(Self::Delete { key: value.key }),
        }
    }
}

impl From<Mutation> for MutationRecord {
    fn from(value: Mutation) -> Self {
        match value {
            Mutation::Upsert { key, val } => Self {
                kind: MutationKind::Upsert,
                key,
                value: Some(val),
            },
            Mutation::Delete { key } => Self {
                kind: MutationKind::Delete,
                key,
                value: None,
            },
        }
    }
}

impl From<BatchApplyStats> for BatchApplyStatsRecord {
    fn from(value: BatchApplyStats) -> Self {
        Self {
            input_mutations: value.input_mutations as u64,
            effective_mutations: value.effective_mutations as u64,
            preprocess_input_sorted: value.preprocess_input_sorted,
            affected_leaves: value.affected_leaves as u64,
            changed_leaves: value.changed_leaves as u64,
            sparse_leaf_applies: value.sparse_leaf_applies as u64,
            written_nodes: value.written_nodes as u64,
            written_bytes: value.written_bytes as u64,
            used_append_fast_path: value.used_append_fast_path,
            used_batched_route: value.used_batched_route,
            used_coalesced_rebuild: value.used_coalesced_rebuild,
            used_deferred_rebalancing: value.used_deferred_rebalancing,
            used_bottom_up_rebuild: value.used_bottom_up_rebuild,
            cache_written_nodes: value.cache_written_nodes,
        }
    }
}

impl From<BatchApplyResult> for BatchApplyResultRecord {
    fn from(value: BatchApplyResult) -> Self {
        Self {
            tree: value.tree.into(),
            stats: value.stats.into(),
        }
    }
}

fn level_u64_records(
    values: std::collections::BTreeMap<u8, usize>,
) -> Vec<TreeStatsLevelU64Record> {
    values
        .into_iter()
        .map(|(level, value)| TreeStatsLevelU64Record {
            level,
            value: value as u64,
        })
        .collect()
}

fn level_f64_records(values: std::collections::BTreeMap<u8, f64>) -> Vec<TreeStatsLevelF64Record> {
    values
        .into_iter()
        .map(|(level, value)| TreeStatsLevelF64Record { level, value })
        .collect()
}

impl From<TreeStats> for TreeStatsRecord {
    fn from(value: TreeStats) -> Self {
        Self {
            num_nodes: value.num_nodes as u64,
            num_leaves: value.num_leaves as u64,
            num_internal_nodes: value.num_internal_nodes as u64,
            tree_height: value.tree_height,
            total_key_value_pairs: value.total_key_value_pairs as u64,
            total_tree_size_bytes: value.total_tree_size_bytes as u64,
            avg_node_size_bytes: value.avg_node_size_bytes,
            min_node_size_bytes: value.min_node_size_bytes as u64,
            max_node_size_bytes: value.max_node_size_bytes as u64,
            avg_entries_per_node: value.avg_entries_per_node,
            nodes_per_level: level_u64_records(value.nodes_per_level),
            avg_node_size_per_level: level_f64_records(value.avg_node_size_per_level),
            avg_entries_per_level: level_f64_records(value.avg_entries_per_level),
            min_entries_per_level: level_u64_records(value.min_entries_per_level),
            max_entries_per_level: level_u64_records(value.max_entries_per_level),
            avg_fanout: value.avg_fanout,
            min_fanout: value.min_fanout as u64,
            max_fanout: value.max_fanout as u64,
            avg_fill_factor: value.avg_fill_factor,
            avg_leaf_fill_factor: value.avg_leaf_fill_factor,
            avg_internal_fill_factor: value.avg_internal_fill_factor,
            avg_key_size_bytes: value.avg_key_size_bytes,
            avg_value_size_bytes: value.avg_value_size_bytes,
            min_key_size_bytes: value.min_key_size_bytes as u64,
            max_key_size_bytes: value.max_key_size_bytes as u64,
            min_value_size_bytes: value.min_value_size_bytes as u64,
            max_value_size_bytes: value.max_value_size_bytes as u64,
            total_keys_size_bytes: value.total_keys_size_bytes as u64,
            total_values_size_bytes: value.total_values_size_bytes as u64,
        }
    }
}

impl From<StatsDiff> for StatsDiffRecord {
    fn from(value: StatsDiff) -> Self {
        Self {
            num_nodes_diff: value.num_nodes_diff,
            num_leaves_diff: value.num_leaves_diff,
            num_internal_nodes_diff: value.num_internal_nodes_diff,
            tree_height_diff: value.tree_height_diff,
            total_key_value_pairs_diff: value.total_key_value_pairs_diff,
            total_tree_size_bytes_diff: value.total_tree_size_bytes_diff,
            avg_node_size_bytes_diff: value.avg_node_size_bytes_diff,
            min_node_size_bytes_diff: value.min_node_size_bytes_diff,
            max_node_size_bytes_diff: value.max_node_size_bytes_diff,
            avg_entries_per_node_diff: value.avg_entries_per_node_diff,
            avg_fanout_diff: value.avg_fanout_diff,
            min_fanout_diff: value.min_fanout_diff,
            max_fanout_diff: value.max_fanout_diff,
            avg_fill_factor_diff: value.avg_fill_factor_diff,
            avg_leaf_fill_factor_diff: value.avg_leaf_fill_factor_diff,
            avg_internal_fill_factor_diff: value.avg_internal_fill_factor_diff,
            avg_key_size_bytes_diff: value.avg_key_size_bytes_diff,
            avg_value_size_bytes_diff: value.avg_value_size_bytes_diff,
            min_key_size_bytes_diff: value.min_key_size_bytes_diff,
            max_key_size_bytes_diff: value.max_key_size_bytes_diff,
            min_value_size_bytes_diff: value.min_value_size_bytes_diff,
            max_value_size_bytes_diff: value.max_value_size_bytes_diff,
            total_keys_size_bytes_diff: value.total_keys_size_bytes_diff,
            total_values_size_bytes_diff: value.total_values_size_bytes_diff,
        }
    }
}

impl From<StatsPercentageChange> for StatsPercentageChangeRecord {
    fn from(value: StatsPercentageChange) -> Self {
        Self {
            num_nodes_pct: value.num_nodes_pct,
            num_leaves_pct: value.num_leaves_pct,
            num_internal_nodes_pct: value.num_internal_nodes_pct,
            tree_height_pct: value.tree_height_pct,
            total_key_value_pairs_pct: value.total_key_value_pairs_pct,
            total_tree_size_bytes_pct: value.total_tree_size_bytes_pct,
            avg_node_size_bytes_pct: value.avg_node_size_bytes_pct,
            min_node_size_bytes_pct: value.min_node_size_bytes_pct,
            max_node_size_bytes_pct: value.max_node_size_bytes_pct,
            avg_entries_per_node_pct: value.avg_entries_per_node_pct,
            avg_fanout_pct: value.avg_fanout_pct,
            min_fanout_pct: value.min_fanout_pct,
            max_fanout_pct: value.max_fanout_pct,
            avg_fill_factor_pct: value.avg_fill_factor_pct,
            avg_leaf_fill_factor_pct: value.avg_leaf_fill_factor_pct,
            avg_internal_fill_factor_pct: value.avg_internal_fill_factor_pct,
            avg_key_size_bytes_pct: value.avg_key_size_bytes_pct,
            avg_value_size_bytes_pct: value.avg_value_size_bytes_pct,
            min_key_size_bytes_pct: value.min_key_size_bytes_pct,
            max_key_size_bytes_pct: value.max_key_size_bytes_pct,
            min_value_size_bytes_pct: value.min_value_size_bytes_pct,
            max_value_size_bytes_pct: value.max_value_size_bytes_pct,
            total_keys_size_bytes_pct: value.total_keys_size_bytes_pct,
            total_values_size_bytes_pct: value.total_values_size_bytes_pct,
        }
    }
}

impl From<StatsComparison> for StatsComparisonRecord {
    fn from(value: StatsComparison) -> Self {
        Self {
            before: value.before.into(),
            after: value.after.into(),
            absolute: value.absolute.into(),
            percentage: value.percentage.into(),
        }
    }
}

impl From<TreeDebugNode> for TreeDebugNodeRecord {
    fn from(value: TreeDebugNode) -> Self {
        Self {
            cid: value.cid.as_bytes().to_vec(),
            leaf: value.leaf,
            level: value.level,
            entry_count: value.entry_count as u64,
            max_entries: value.max_entries as u64,
            fill_factor: value.fill_factor,
            encoded_bytes: value.encoded_bytes as u64,
            first_key: value.first_key,
            last_key: value.last_key,
        }
    }
}

impl From<TreeDebugLevel> for TreeDebugLevelRecord {
    fn from(value: TreeDebugLevel) -> Self {
        Self {
            level: value.level,
            nodes: value
                .nodes
                .into_iter()
                .map(TreeDebugNodeRecord::from)
                .collect(),
        }
    }
}

impl From<TreeDebugView> for TreeDebugViewRecord {
    fn from(value: TreeDebugView) -> Self {
        Self {
            levels: value
                .levels
                .into_iter()
                .map(TreeDebugLevelRecord::from)
                .collect(),
        }
    }
}

impl From<TreeDebugNodeStatus> for TreeDebugNodeStatusKind {
    fn from(value: TreeDebugNodeStatus) -> Self {
        match value {
            TreeDebugNodeStatus::Shared => Self::Shared,
            TreeDebugNodeStatus::LeftOnly => Self::LeftOnly,
            TreeDebugNodeStatus::RightOnly => Self::RightOnly,
        }
    }
}

impl From<TreeDebugComparedNode> for TreeDebugComparedNodeRecord {
    fn from(value: TreeDebugComparedNode) -> Self {
        Self {
            status: value.status.into(),
            node: value.node.into(),
        }
    }
}

impl From<TreeDebugComparisonLevel> for TreeDebugComparisonLevelRecord {
    fn from(value: TreeDebugComparisonLevel) -> Self {
        Self {
            level: value.level,
            shared_nodes: value.shared_nodes as u64,
            left_only_nodes: value.left_only_nodes as u64,
            right_only_nodes: value.right_only_nodes as u64,
            shared_bytes: value.shared_bytes as u64,
            left_only_bytes: value.left_only_bytes as u64,
            right_only_bytes: value.right_only_bytes as u64,
            nodes: value
                .nodes
                .into_iter()
                .map(TreeDebugComparedNodeRecord::from)
                .collect(),
        }
    }
}

impl From<TreeDebugComparison> for TreeDebugComparisonRecord {
    fn from(value: TreeDebugComparison) -> Self {
        Self {
            shared_nodes: value.shared_nodes as u64,
            left_only_nodes: value.left_only_nodes as u64,
            right_only_nodes: value.right_only_nodes as u64,
            shared_bytes: value.shared_bytes as u64,
            left_only_bytes: value.left_only_bytes as u64,
            right_only_bytes: value.right_only_bytes as u64,
            levels: value
                .levels
                .into_iter()
                .map(TreeDebugComparisonLevelRecord::from)
                .collect(),
        }
    }
}

impl TryFrom<MergeTrace> for MergeTraceRecord {
    type Error = ProllyBindingError;

    fn try_from(value: MergeTrace) -> Result<Self, Self::Error> {
        Ok(Self {
            events: value
                .events
                .into_iter()
                .map(MergeTraceEventRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<MergeTraceEvent> for MergeTraceEventRecord {
    type Error = ProllyBindingError;

    fn try_from(value: MergeTraceEvent) -> Result<Self, Self::Error> {
        let mut record = Self {
            kind: MergeTraceEventKind::StructuralMergeStarted,
            fast_path: None,
            cid: None,
            reuse_reason: None,
            level: None,
            entries: None,
            first_key: None,
            last_key: None,
            stage: None,
            key: None,
            resolution: None,
            fallback_reason: None,
            diff_stats: None,
            right_changes: None,
            mutations: None,
            append_only: None,
        };

        match value {
            MergeTraceEvent::FastPath { reason } => {
                record.kind = MergeTraceEventKind::FastPath;
                record.fast_path = Some(MergeFastPathKind::from(reason));
            }
            MergeTraceEvent::StructuralMergeStarted => {
                record.kind = MergeTraceEventKind::StructuralMergeStarted;
            }
            MergeTraceEvent::ReusedSubtree { cid, reason } => {
                record.kind = MergeTraceEventKind::ReusedSubtree;
                record.cid = Some(cid.as_bytes().to_vec());
                record.reuse_reason = Some(MergeReuseReasonKind::from(reason));
            }
            MergeTraceEvent::RewrittenNode {
                cid,
                level,
                entries,
                first_key,
                last_key,
            } => {
                record.kind = MergeTraceEventKind::RewrittenNode;
                record.cid = Some(cid.as_bytes().to_vec());
                record.level = Some(level.into());
                record.entries = Some(to_u64(entries, "entries")?);
                record.first_key = first_key;
                record.last_key = last_key;
            }
            MergeTraceEvent::ResolverCalled {
                stage,
                key,
                resolution,
            } => {
                record.kind = MergeTraceEventKind::ResolverCalled;
                record.stage = Some(MergeTraceStageKind::from(stage));
                record.key = Some(key);
                record.resolution = Some(MergeTraceResolutionKind::from(resolution));
            }
            MergeTraceEvent::Fallback { reason } => {
                record.kind = MergeTraceEventKind::Fallback;
                record.fallback_reason = Some(MergeFallbackReasonKind::from(reason));
            }
            MergeTraceEvent::DiffTraversal { stats } => {
                record.kind = MergeTraceEventKind::DiffTraversal;
                record.diff_stats = Some(DiffTraversalStatsRecord::try_from(stats)?);
            }
            MergeTraceEvent::BatchMerge {
                right_changes,
                mutations,
                append_only,
            } => {
                record.kind = MergeTraceEventKind::BatchMerge;
                record.right_changes = Some(to_u64(right_changes, "right_changes")?);
                record.mutations = Some(to_u64(mutations, "mutations")?);
                record.append_only = Some(append_only);
            }
        }

        Ok(record)
    }
}

impl From<MergeFastPath> for MergeFastPathKind {
    fn from(value: MergeFastPath) -> Self {
        match value {
            MergeFastPath::BranchesEqual => Self::BranchesEqual,
            MergeFastPath::LeftUnchanged => Self::LeftUnchanged,
            MergeFastPath::RightUnchanged => Self::RightUnchanged,
        }
    }
}

impl From<MergeReuseReason> for MergeReuseReasonKind {
    fn from(value: MergeReuseReason) -> Self {
        match value {
            MergeReuseReason::BranchesEqual => Self::BranchesEqual,
            MergeReuseReason::LeftUnchanged => Self::LeftUnchanged,
            MergeReuseReason::RightUnchanged => Self::RightUnchanged,
            MergeReuseReason::UnchangedAfterMerge => Self::UnchangedAfterMerge,
            MergeReuseReason::MatchesLeft => Self::MatchesLeft,
            MergeReuseReason::MatchesRight => Self::MatchesRight,
        }
    }
}

impl From<MergeTraceStage> for MergeTraceStageKind {
    fn from(value: MergeTraceStage) -> Self {
        match value {
            MergeTraceStage::Structural => Self::Structural,
            MergeTraceStage::Batch => Self::Batch,
        }
    }
}

impl From<CoreMergeResolutionKind> for MergeTraceResolutionKind {
    fn from(value: CoreMergeResolutionKind) -> Self {
        match value {
            CoreMergeResolutionKind::Value => Self::Value,
            CoreMergeResolutionKind::Delete => Self::Delete,
            CoreMergeResolutionKind::Unresolved => Self::Unresolved,
        }
    }
}

impl From<MergeFallbackReason> for MergeFallbackReasonKind {
    fn from(value: MergeFallbackReason) -> Self {
        match value {
            MergeFallbackReason::MissingRoot => Self::MissingRoot,
            MergeFallbackReason::ShapeMismatch => Self::ShapeMismatch,
            MergeFallbackReason::NodeLengthMismatch => Self::NodeLengthMismatch,
            MergeFallbackReason::ChildFallback => Self::ChildFallback,
            MergeFallbackReason::DeleteResolution => Self::DeleteResolution,
            MergeFallbackReason::DiffBatch => Self::DiffBatch,
        }
    }
}

impl TryFrom<Diff> for DiffRecord {
    type Error = ProllyBindingError;

    fn try_from(value: Diff) -> Result<Self, Self::Error> {
        Ok(match value {
            Diff::Added { key, val } => Self {
                kind: DiffKind::Added,
                key,
                value: Some(val),
                old_value: None,
                new_value: None,
            },
            Diff::Removed { key, val } => Self {
                kind: DiffKind::Removed,
                key,
                value: Some(val),
                old_value: None,
                new_value: None,
            },
            Diff::Changed { key, old, new } => Self {
                kind: DiffKind::Changed,
                key,
                value: None,
                old_value: Some(old),
                new_value: Some(new),
            },
        })
    }
}

impl TryFrom<DiffRecord> for Diff {
    type Error = ProllyBindingError;

    fn try_from(value: DiffRecord) -> Result<Self, Self::Error> {
        match value.kind {
            DiffKind::Added => Ok(Self::Added {
                key: value.key,
                val: value
                    .value
                    .ok_or_else(|| invalid_argument("added diff requires value"))?,
            }),
            DiffKind::Removed => Ok(Self::Removed {
                key: value.key,
                val: value
                    .value
                    .ok_or_else(|| invalid_argument("removed diff requires value"))?,
            }),
            DiffKind::Changed => Ok(Self::Changed {
                key: value.key,
                old: value
                    .old_value
                    .ok_or_else(|| invalid_argument("changed diff requires old_value"))?,
                new: value
                    .new_value
                    .ok_or_else(|| invalid_argument("changed diff requires new_value"))?,
            }),
        }
    }
}

fn entry_record((key, value): (Vec<u8>, Vec<u8>)) -> EntryRecord {
    EntryRecord { key, value }
}

impl From<Conflict> for ConflictRecord {
    fn from(value: Conflict) -> Self {
        Self::from(&value)
    }
}

impl From<&Conflict> for ConflictRecord {
    fn from(value: &Conflict) -> Self {
        Self {
            key: value.key.clone(),
            base: value.base.clone(),
            left: value.left.clone(),
            right: value.right.clone(),
        }
    }
}

impl From<ConflictRecord> for Conflict {
    fn from(value: ConflictRecord) -> Self {
        Self {
            key: value.key,
            base: value.base,
            left: value.left,
            right: value.right,
        }
    }
}

impl From<ResolutionRecord> for prolly::Resolution {
    fn from(value: ResolutionRecord) -> Self {
        match value.kind {
            ResolutionKind::Value => value
                .value
                .map(prolly::Resolution::value)
                .unwrap_or_else(prolly::Resolution::unresolved),
            ResolutionKind::Delete => prolly::Resolution::delete(),
            ResolutionKind::Unresolved => prolly::Resolution::unresolved(),
        }
    }
}

impl From<prolly::Resolution> for ResolutionRecord {
    fn from(value: prolly::Resolution) -> Self {
        match value {
            prolly::Resolution::Value(value) => Self {
                kind: ResolutionKind::Value,
                value: Some(value),
            },
            prolly::Resolution::Delete => Self {
                kind: ResolutionKind::Delete,
                value: None,
            },
            prolly::Resolution::Unresolved => Self {
                kind: ResolutionKind::Unresolved,
                value: None,
            },
        }
    }
}

impl From<CrdtResolutionRecord> for prolly::CrdtResolution {
    fn from(value: CrdtResolutionRecord) -> Self {
        match value.kind {
            CrdtResolutionKind::Value => {
                prolly::CrdtResolution::value(value.value.unwrap_or_default())
            }
            CrdtResolutionKind::Delete => prolly::CrdtResolution::delete(),
        }
    }
}

impl From<prolly::CrdtResolution> for CrdtResolutionRecord {
    fn from(value: prolly::CrdtResolution) -> Self {
        match value {
            prolly::CrdtResolution::Value(value) => Self {
                kind: CrdtResolutionKind::Value,
                value: Some(value),
            },
            prolly::CrdtResolution::Delete => Self {
                kind: CrdtResolutionKind::Delete,
                value: None,
            },
        }
    }
}

impl From<RangeCursor> for RangeCursorRecord {
    fn from(value: RangeCursor) -> Self {
        Self {
            after_key: value.after().map(<[u8]>::to_vec),
        }
    }
}

impl From<RangeCursorRecord> for RangeCursor {
    fn from(value: RangeCursorRecord) -> Self {
        value
            .after_key
            .map(RangeCursor::after_key)
            .unwrap_or_else(RangeCursor::start)
    }
}

impl TryFrom<prolly::RangePage> for RangePageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: prolly::RangePage) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|(key, value)| EntryRecord { key, value })
                .collect(),
            next_cursor: value.next_cursor.map(RangeCursorRecord::from),
        })
    }
}

impl From<ReverseCursor> for ReverseCursorRecord {
    fn from(value: ReverseCursor) -> Self {
        Self {
            before_key: value.before().map(<[u8]>::to_vec),
        }
    }
}

impl From<ReverseCursorRecord> for ReverseCursor {
    fn from(value: ReverseCursorRecord) -> Self {
        value
            .before_key
            .map(ReverseCursor::before_key)
            .unwrap_or_else(ReverseCursor::end)
    }
}

impl TryFrom<prolly::ReversePage> for ReversePageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: prolly::ReversePage) -> Result<Self, Self::Error> {
        Ok(Self {
            entries: value
                .entries
                .into_iter()
                .map(|(key, value)| EntryRecord { key, value })
                .collect(),
            next_cursor: value.next_cursor.map(ReverseCursorRecord::from),
        })
    }
}

impl From<CursorWindow> for CursorWindowRecord {
    fn from(value: CursorWindow) -> Self {
        Self {
            position_key: value.position_key,
            position_value: value.position_value,
            found: value.found,
            entries: value
                .entries
                .into_iter()
                .map(|(key, value)| EntryRecord { key, value })
                .collect(),
            next_cursor: value.next_cursor.map(RangeCursorRecord::from),
        }
    }
}

impl TryFrom<prolly::DiffPage> for DiffPageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: prolly::DiffPage) -> Result<Self, Self::Error> {
        Ok(Self {
            diffs: value
                .diffs
                .into_iter()
                .map(DiffRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor: value.next_cursor.map(RangeCursorRecord::from),
        })
    }
}

impl TryFrom<prolly::MergeExplanation> for MergeExplanationRecord {
    type Error = ProllyBindingError;

    fn try_from(value: prolly::MergeExplanation) -> Result<Self, Self::Error> {
        let trace_json = serde_json::to_string(&value.trace).map_err(json_error)?;
        let trace = MergeTraceRecord::try_from(value.trace)?;
        let (result, error) = match value.result {
            Ok(tree) => (Some(TreeRecord::from(tree)), None),
            Err(error) => (None, Some(error.to_string())),
        };

        Ok(Self {
            result,
            error,
            trace_json,
            trace,
        })
    }
}

impl TryFrom<AuthenticatedProofBundleVerification> for AuthenticatedProofBundleVerificationRecord {
    type Error = ProllyBindingError;

    fn try_from(value: AuthenticatedProofBundleVerification) -> Result<Self, Self::Error> {
        Ok(Self {
            valid: value.valid,
            envelope: AuthenticatedProofEnvelopeVerificationRecord::from(value.envelope),
            proof: value
                .proof
                .map(ProofBundleVerificationRecord::try_from)
                .transpose()?,
            proof_error: value.proof_error,
        })
    }
}

impl From<VersionedValue> for VersionedValueRecord {
    fn from(value: VersionedValue) -> Self {
        Self {
            schema: value.schema,
            version: value.version,
            encoding: value.encoding.into(),
            payload: value.payload,
        }
    }
}

impl TryFrom<VersionedValueRecord> for VersionedValue {
    type Error = ProllyBindingError;

    fn try_from(value: VersionedValueRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            schema: value.schema,
            version: value.version,
            encoding: value.encoding.try_into()?,
            payload: value.payload,
        })
    }
}

impl From<BlobRef> for BlobRefRecord {
    fn from(value: BlobRef) -> Self {
        Self {
            cid: value.cid.as_bytes().to_vec(),
            len: value.len,
        }
    }
}

impl TryFrom<BlobRefRecord> for BlobRef {
    type Error = ProllyBindingError;

    fn try_from(value: BlobRefRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            cid: cid_from_vec(value.cid)?,
            len: value.len,
        })
    }
}

impl From<LargeValueConfig> for LargeValueConfigRecord {
    fn from(value: LargeValueConfig) -> Self {
        Self {
            inline_threshold: value.inline_threshold as u64,
        }
    }
}

impl TryFrom<LargeValueConfigRecord> for LargeValueConfig {
    type Error = ProllyBindingError;

    fn try_from(value: LargeValueConfigRecord) -> Result<Self, Self::Error> {
        Ok(Self::new(to_usize(
            value.inline_threshold,
            "inline_threshold",
        )?))
    }
}

impl From<ValueRef> for ValueRefRecord {
    fn from(value: ValueRef) -> Self {
        match value {
            ValueRef::Inline(value) => Self {
                kind: ValueRefKind::Inline,
                value: Some(value),
                blob: None,
            },
            ValueRef::Blob(blob) => Self {
                kind: ValueRefKind::Blob,
                value: None,
                blob: Some(blob.into()),
            },
        }
    }
}

impl TryFrom<ValueRefRecord> for ValueRef {
    type Error = ProllyBindingError;

    fn try_from(value: ValueRefRecord) -> Result<Self, Self::Error> {
        match value.kind {
            ValueRefKind::Inline => {
                Ok(Self::Inline(value.value.ok_or_else(|| {
                    invalid_argument("inline value ref requires value")
                })?))
            }
            ValueRefKind::Blob => Ok(Self::Blob(
                value
                    .blob
                    .ok_or_else(|| invalid_argument("blob value ref requires blob"))?
                    .try_into()?,
            )),
        }
    }
}

impl From<RootManifest> for RootManifestRecord {
    fn from(value: RootManifest) -> Self {
        let created_at_millis = value.created_at_millis;
        let updated_at_millis = value.updated_at_millis;
        Self {
            tree: value.into_tree().into(),
            created_at_millis,
            updated_at_millis,
        }
    }
}

impl TryFrom<RootManifestRecord> for RootManifest {
    type Error = ProllyBindingError;

    fn try_from(value: RootManifestRecord) -> Result<Self, Self::Error> {
        let tree = Tree::try_from(value.tree)?;
        Ok(Self::from_tree_with_timestamps_millis(
            &tree,
            value.created_at_millis,
            value.updated_at_millis,
        ))
    }
}

impl From<prolly::NamedRoot> for NamedRootRecord {
    fn from(value: prolly::NamedRoot) -> Self {
        Self {
            name: value.name,
            tree: value.tree.into(),
        }
    }
}

impl From<NamedRootManifest> for NamedRootManifestRecord {
    fn from(value: NamedRootManifest) -> Self {
        Self {
            name: value.name,
            manifest: value.manifest.into(),
        }
    }
}

impl From<prolly::NamedRootSelection> for NamedRootSelectionRecord {
    fn from(value: prolly::NamedRootSelection) -> Self {
        Self {
            roots: value.roots.into_iter().map(NamedRootRecord::from).collect(),
            missing_names: value.missing_names,
        }
    }
}

impl From<NamedRootUpdate> for NamedRootUpdateRecord {
    fn from(value: NamedRootUpdate) -> Self {
        match value {
            NamedRootUpdate::Applied => Self {
                applied: true,
                conflict: false,
                current: None,
            },
            NamedRootUpdate::Conflict { current } => Self {
                applied: false,
                conflict: true,
                current: current.map(TreeRecord::from),
            },
        }
    }
}

impl From<SnapshotNamespace> for SnapshotNamespaceRecord {
    fn from(value: SnapshotNamespace) -> Self {
        match value {
            SnapshotNamespace::Branch => Self {
                kind: SnapshotNamespaceKind::Branch,
                custom_prefix: None,
            },
            SnapshotNamespace::Tag => Self {
                kind: SnapshotNamespaceKind::Tag,
                custom_prefix: None,
            },
            SnapshotNamespace::Checkpoint => Self {
                kind: SnapshotNamespaceKind::Checkpoint,
                custom_prefix: None,
            },
            SnapshotNamespace::Custom(prefix) => Self {
                kind: SnapshotNamespaceKind::Custom,
                custom_prefix: Some(prefix),
            },
        }
    }
}

impl TryFrom<SnapshotNamespaceRecord> for SnapshotNamespace {
    type Error = ProllyBindingError;

    fn try_from(value: SnapshotNamespaceRecord) -> Result<Self, Self::Error> {
        Ok(match value.kind {
            SnapshotNamespaceKind::Branch => Self::Branch,
            SnapshotNamespaceKind::Tag => Self::Tag,
            SnapshotNamespaceKind::Checkpoint => Self::Checkpoint,
            SnapshotNamespaceKind::Custom => Self::Custom(
                value
                    .custom_prefix
                    .ok_or_else(|| invalid_argument("custom snapshot namespace requires prefix"))?,
            ),
        })
    }
}

impl From<SnapshotRoot> for SnapshotRecord {
    fn from(value: SnapshotRoot) -> Self {
        Self {
            id: value.id,
            name: value.name,
            tree: value.tree.into(),
            created_at_millis: value.created_at_millis,
            updated_at_millis: value.updated_at_millis,
        }
    }
}

impl From<SnapshotSelection> for SnapshotSelectionRecord {
    fn from(value: SnapshotSelection) -> Self {
        Self {
            snapshots: value
                .snapshots
                .into_iter()
                .map(SnapshotRecord::from)
                .collect(),
            missing_ids: value.missing_ids,
        }
    }
}

impl From<ChangedSpanRecord> for ChangedSpan {
    fn from(value: ChangedSpanRecord) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<ChangedSpan> for ChangedSpanRecord {
    fn from(value: ChangedSpan) -> Self {
        Self {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<ChangedSpanHint> for ChangedSpanHintRecord {
    fn from(value: ChangedSpanHint) -> Self {
        Self {
            base_root: value.base_root.map(|cid| cid.as_bytes().to_vec()),
            changed_root: value.changed_root.map(|cid| cid.as_bytes().to_vec()),
            spans: value
                .spans
                .into_iter()
                .map(ChangedSpanRecord::from)
                .collect(),
        }
    }
}

impl From<NamedRootRetention> for NamedRootRetentionRecord {
    fn from(value: NamedRootRetention) -> Self {
        match value {
            NamedRootRetention::All => Self {
                kind: NamedRootRetentionKind::All,
                names: Vec::new(),
                prefix: Vec::new(),
                count: None,
                min_updated_at_millis: None,
            },
            NamedRootRetention::Exact { names } => Self {
                kind: NamedRootRetentionKind::Exact,
                names,
                prefix: Vec::new(),
                count: None,
                min_updated_at_millis: None,
            },
            NamedRootRetention::Prefix { prefix } => Self {
                kind: NamedRootRetentionKind::Prefix,
                names: Vec::new(),
                prefix,
                count: None,
                min_updated_at_millis: None,
            },
            NamedRootRetention::NewestByName { prefix, count } => Self {
                kind: NamedRootRetentionKind::NewestByName,
                names: Vec::new(),
                prefix,
                count: Some(count as u64),
                min_updated_at_millis: None,
            },
            NamedRootRetention::UpdatedSince {
                prefix,
                min_updated_at_millis,
            } => Self {
                kind: NamedRootRetentionKind::UpdatedSince,
                names: Vec::new(),
                prefix,
                count: None,
                min_updated_at_millis: Some(min_updated_at_millis),
            },
        }
    }
}

impl TryFrom<NamedRootRetentionRecord> for NamedRootRetention {
    type Error = ProllyBindingError;

    fn try_from(value: NamedRootRetentionRecord) -> Result<Self, Self::Error> {
        Ok(match value.kind {
            NamedRootRetentionKind::All => Self::All,
            NamedRootRetentionKind::Exact => Self::Exact { names: value.names },
            NamedRootRetentionKind::Prefix => Self::Prefix {
                prefix: value.prefix,
            },
            NamedRootRetentionKind::NewestByName => Self::NewestByName {
                prefix: value.prefix,
                count: to_usize(
                    value.count.ok_or_else(|| {
                        invalid_argument("newest-by-name retention requires count")
                    })?,
                    "retention count",
                )?,
            },
            NamedRootRetentionKind::UpdatedSince => Self::UpdatedSince {
                prefix: value.prefix,
                min_updated_at_millis: value.min_updated_at_millis.ok_or_else(|| {
                    invalid_argument("updated-since retention requires min_updated_at_millis")
                })?,
            },
        })
    }
}

impl TryFrom<StructuralDiffPage> for StructuralDiffPageRecord {
    type Error = ProllyBindingError;

    fn try_from(value: StructuralDiffPage) -> Result<Self, Self::Error> {
        let next_cursor_json = value
            .next_cursor
            .as_ref()
            .map(serde_json::to_string)
            .transpose()
            .map_err(json_error)?;
        Ok(Self {
            diffs: value
                .diffs
                .into_iter()
                .map(DiffRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            next_cursor_json,
            stats: DiffTraversalStatsRecord::try_from(value.stats)?,
            next_cursor: value
                .next_cursor
                .map(StructuralDiffCursorRecord::try_from)
                .transpose()?,
        })
    }
}

impl TryFrom<StructuralDiffCursor> for StructuralDiffCursorRecord {
    type Error = ProllyBindingError;

    fn try_from(value: StructuralDiffCursor) -> Result<Self, Self::Error> {
        Ok(Self {
            base_root: value.base_root.map(|cid| cid.as_bytes().to_vec()),
            other_root: value.other_root.map(|cid| cid.as_bytes().to_vec()),
            markers: value
                .markers
                .into_iter()
                .map(StructuralDiffMarkerRecord::from)
                .collect(),
            pending: value
                .pending
                .into_iter()
                .map(DiffRecord::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl TryFrom<StructuralDiffCursorRecord> for StructuralDiffCursor {
    type Error = ProllyBindingError;

    fn try_from(value: StructuralDiffCursorRecord) -> Result<Self, Self::Error> {
        Ok(Self {
            base_root: value.base_root.map(cid_from_vec).transpose()?,
            other_root: value.other_root.map(cid_from_vec).transpose()?,
            markers: value
                .markers
                .into_iter()
                .map(StructuralDiffMarker::try_from)
                .collect::<Result<Vec<_>, _>>()?,
            pending: value
                .pending
                .into_iter()
                .map(Diff::try_from)
                .collect::<Result<Vec<_>, _>>()?,
        })
    }
}

impl From<StructuralDiffMarker> for StructuralDiffMarkerRecord {
    fn from(value: StructuralDiffMarker) -> Self {
        match value {
            StructuralDiffMarker::Compare {
                base_cid,
                other_cid,
                span_end,
            } => Self {
                kind: StructuralDiffMarkerKind::Compare,
                base_cid: Some(base_cid.as_bytes().to_vec()),
                other_cid: Some(other_cid.as_bytes().to_vec()),
                span_end,
                cid: None,
            },
            StructuralDiffMarker::Added { cid } => Self {
                kind: StructuralDiffMarkerKind::Added,
                base_cid: None,
                other_cid: None,
                span_end: None,
                cid: Some(cid.as_bytes().to_vec()),
            },
            StructuralDiffMarker::Removed { cid } => Self {
                kind: StructuralDiffMarkerKind::Removed,
                base_cid: None,
                other_cid: None,
                span_end: None,
                cid: Some(cid.as_bytes().to_vec()),
            },
        }
    }
}

impl TryFrom<StructuralDiffMarkerRecord> for StructuralDiffMarker {
    type Error = ProllyBindingError;

    fn try_from(value: StructuralDiffMarkerRecord) -> Result<Self, Self::Error> {
        match value.kind {
            StructuralDiffMarkerKind::Compare => Ok(Self::Compare {
                base_cid: cid_from_vec(value.base_cid.ok_or_else(|| {
                    invalid_argument("compare structural diff marker requires base_cid")
                })?)?,
                other_cid: cid_from_vec(value.other_cid.ok_or_else(|| {
                    invalid_argument("compare structural diff marker requires other_cid")
                })?)?,
                span_end: value.span_end,
            }),
            StructuralDiffMarkerKind::Added => Ok(Self::Added {
                cid: cid_from_vec(value.cid.ok_or_else(|| {
                    invalid_argument("added structural diff marker requires cid")
                })?)?,
            }),
            StructuralDiffMarkerKind::Removed => Ok(Self::Removed {
                cid: cid_from_vec(value.cid.ok_or_else(|| {
                    invalid_argument("removed structural diff marker requires cid")
                })?)?,
            }),
        }
    }
}

impl TryFrom<DiffTraversalStats> for DiffTraversalStatsRecord {
    type Error = ProllyBindingError;

    fn try_from(value: DiffTraversalStats) -> Result<Self, Self::Error> {
        Ok(Self {
            compared_nodes: to_u64(value.compared_nodes, "compared_nodes")?,
            reused_subtrees: to_u64(value.reused_subtrees, "reused_subtrees")?,
            added_subtrees: to_u64(value.added_subtrees, "added_subtrees")?,
            removed_subtrees: to_u64(value.removed_subtrees, "removed_subtrees")?,
            collected_fallbacks: to_u64(value.collected_fallbacks, "collected_fallbacks")?,
            emitted_diffs: to_u64(value.emitted_diffs, "emitted_diffs")?,
        })
    }
}

impl From<ProllyMetricsSnapshot> for MetricsRecord {
    fn from(value: ProllyMetricsSnapshot) -> Self {
        Self {
            node_cache_hits: value.node_cache_hits,
            node_cache_misses: value.node_cache_misses,
            node_cache_evictions: value.node_cache_evictions,
            nodes_read: value.nodes_read,
            bytes_read: value.bytes_read,
            nodes_written: value.nodes_written,
            bytes_written: value.bytes_written,
            store_get_calls: value.store_get_calls,
            store_batch_get_calls: value.store_batch_get_calls,
            store_batch_get_keys: value.store_batch_get_keys,
            store_put_calls: value.store_put_calls,
            store_batch_put_calls: value.store_batch_put_calls,
            store_batch_put_nodes: value.store_batch_put_nodes,
        }
    }
}

impl TryFrom<GcReachability> for GcReachabilityRecord {
    type Error = ProllyBindingError;

    fn try_from(value: GcReachability) -> Result<Self, Self::Error> {
        Ok(Self {
            live_cids: cid_records(value.live_cids),
            live_nodes: to_u64(value.live_nodes, "live_nodes")?,
            live_bytes: to_u64(value.live_bytes, "live_bytes")?,
            leaf_nodes: to_u64(value.leaf_nodes, "leaf_nodes")?,
            internal_nodes: to_u64(value.internal_nodes, "internal_nodes")?,
        })
    }
}

impl TryFrom<GcPlan> for GcPlanRecord {
    type Error = ProllyBindingError;

    fn try_from(value: GcPlan) -> Result<Self, Self::Error> {
        let GcPlan {
            reachability,
            candidate_nodes,
            reclaimable_cids,
            reclaimable_nodes,
            reclaimable_bytes,
            missing_candidates,
        } = value;

        Ok(Self {
            reachability: GcReachabilityRecord::try_from(reachability)?,
            candidate_nodes: to_u64(candidate_nodes, "candidate_nodes")?,
            reclaimable_cids: cid_records(reclaimable_cids),
            reclaimable_nodes: to_u64(reclaimable_nodes, "reclaimable_nodes")?,
            reclaimable_bytes: to_u64(reclaimable_bytes, "reclaimable_bytes")?,
            missing_candidates: to_u64(missing_candidates, "missing_candidates")?,
        })
    }
}

impl TryFrom<GcSweep> for GcSweepRecord {
    type Error = ProllyBindingError;

    fn try_from(value: GcSweep) -> Result<Self, Self::Error> {
        Ok(Self {
            plan: GcPlanRecord::try_from(value.plan)?,
            deleted_nodes: to_u64(value.deleted_nodes, "deleted_nodes")?,
            deleted_bytes: to_u64(value.deleted_bytes, "deleted_bytes")?,
        })
    }
}

impl TryFrom<BlobGcReachability> for BlobGcReachabilityRecord {
    type Error = ProllyBindingError;

    fn try_from(value: BlobGcReachability) -> Result<Self, Self::Error> {
        Ok(Self {
            live_blobs: blob_ref_records(value.live_blobs),
            live_blob_count: to_u64(value.live_blob_count, "live_blob_count")?,
            live_blob_bytes: value.live_blob_bytes,
            scanned_nodes: to_u64(value.scanned_nodes, "scanned_nodes")?,
            scanned_values: to_u64(value.scanned_values, "scanned_values")?,
        })
    }
}

impl TryFrom<BlobGcPlan> for BlobGcPlanRecord {
    type Error = ProllyBindingError;

    fn try_from(value: BlobGcPlan) -> Result<Self, Self::Error> {
        let BlobGcPlan {
            reachability,
            candidate_blobs,
            reclaimable_blobs,
            reclaimable_blob_count,
            reclaimable_blob_bytes,
            missing_candidates,
        } = value;

        Ok(Self {
            reachability: BlobGcReachabilityRecord::try_from(reachability)?,
            candidate_blobs: to_u64(candidate_blobs, "candidate_blobs")?,
            reclaimable_blobs: blob_ref_records(reclaimable_blobs),
            reclaimable_blob_count: to_u64(reclaimable_blob_count, "reclaimable_blob_count")?,
            reclaimable_blob_bytes,
            missing_candidates: to_u64(missing_candidates, "missing_candidates")?,
        })
    }
}

impl TryFrom<BlobGcSweep> for BlobGcSweepRecord {
    type Error = ProllyBindingError;

    fn try_from(value: BlobGcSweep) -> Result<Self, Self::Error> {
        Ok(Self {
            plan: BlobGcPlanRecord::try_from(value.plan)?,
            deleted_blobs: to_u64(value.deleted_blobs, "deleted_blobs")?,
            deleted_blob_bytes: value.deleted_blob_bytes,
        })
    }
}

impl TryFrom<MissingNodePlan> for MissingNodePlanRecord {
    type Error = ProllyBindingError;

    fn try_from(value: MissingNodePlan) -> Result<Self, Self::Error> {
        Ok(Self {
            required_cids: cid_records(value.required_cids),
            required_nodes: to_u64(value.required_nodes, "required_nodes")?,
            required_bytes: to_u64(value.required_bytes, "required_bytes")?,
            missing_cids: cid_records(value.missing_cids),
            missing_nodes: to_u64(value.missing_nodes, "missing_nodes")?,
            missing_bytes: to_u64(value.missing_bytes, "missing_bytes")?,
        })
    }
}

impl TryFrom<MissingNodeCopy> for MissingNodeCopyRecord {
    type Error = ProllyBindingError;

    fn try_from(value: MissingNodeCopy) -> Result<Self, Self::Error> {
        Ok(Self {
            plan: MissingNodePlanRecord::try_from(value.plan)?,
            copied_nodes: to_u64(value.copied_nodes, "copied_nodes")?,
            copied_bytes: to_u64(value.copied_bytes, "copied_bytes")?,
        })
    }
}

impl From<CrdtDeletePolicyKind> for DeletePolicy {
    fn from(value: CrdtDeletePolicyKind) -> Self {
        match value {
            CrdtDeletePolicyKind::DeleteWins => Self::DeleteWins,
            CrdtDeletePolicyKind::UpdateWins => Self::UpdateWins,
        }
    }
}

impl From<CrdtConfigRecord> for CrdtConfig {
    fn from(value: CrdtConfigRecord) -> Self {
        let config = match value.strategy {
            CrdtMergeStrategyKind::LastWriterWins => Self::lww(),
            CrdtMergeStrategyKind::MultiValue => Self::multi_value(),
        };
        config.with_delete_policy(value.delete_policy.into())
    }
}

impl From<TimestampedValue> for TimestampedValueRecord {
    fn from(value: TimestampedValue) -> Self {
        Self {
            value: value.value,
            timestamp: value.timestamp,
        }
    }
}

impl From<TimestampedValueRecord> for TimestampedValue {
    fn from(value: TimestampedValueRecord) -> Self {
        Self::new(value.value, value.timestamp)
    }
}

impl TryFrom<TombstoneRecord> for Tombstone {
    type Error = ProllyBindingError;

    fn try_from(value: TombstoneRecord) -> Result<Self, Self::Error> {
        let mut causal_metadata = BTreeMap::new();
        for entry in value.causal_metadata {
            if causal_metadata
                .insert(entry.key.clone(), entry.value)
                .is_some()
            {
                return Err(invalid_argument(format!(
                    "duplicate tombstone metadata key {:?}",
                    entry.key
                )));
            }
        }

        Ok(Self {
            actor: value.actor,
            timestamp_millis: value.timestamp_millis,
            causal_metadata,
        })
    }
}

impl From<Tombstone> for TombstoneRecord {
    fn from(value: Tombstone) -> Self {
        Self {
            actor: value.actor,
            timestamp_millis: value.timestamp_millis,
            causal_metadata: value
                .causal_metadata
                .into_iter()
                .map(|(key, value)| TombstoneMetadataRecord { key, value })
                .collect(),
        }
    }
}

fn cid_from_vec(bytes: Vec<u8>) -> Result<Cid, ProllyBindingError> {
    let bytes: [u8; 32] =
        bytes
            .try_into()
            .map_err(|bytes: Vec<u8>| ProllyBindingError::InvalidCid {
                message: format!("CID must be exactly 32 bytes, got {}", bytes.len()),
            })?;
    Ok(Cid(bytes))
}

fn to_usize(value: u64, field: &'static str) -> Result<usize, ProllyBindingError> {
    usize::try_from(value).map_err(|_| invalid_argument(format!("{field} is too large")))
}

fn to_u64(value: usize, field: &'static str) -> Result<u64, ProllyBindingError> {
    u64::try_from(value).map_err(|_| invalid_argument(format!("{field} is too large")))
}

fn trees_from_records(records: Vec<TreeRecord>) -> Result<Vec<Tree>, ProllyBindingError> {
    records.into_iter().map(Tree::try_from).collect()
}

fn cids_from_records(records: Vec<Vec<u8>>) -> Result<Vec<Cid>, ProllyBindingError> {
    records.into_iter().map(cid_from_vec).collect()
}

fn blob_refs_from_records(records: Vec<BlobRefRecord>) -> Result<Vec<BlobRef>, ProllyBindingError> {
    records.into_iter().map(BlobRef::try_from).collect()
}

fn blob_ref_records(references: Vec<BlobRef>) -> Vec<BlobRefRecord> {
    references.into_iter().map(BlobRefRecord::from).collect()
}

fn cid_records(cids: Vec<Cid>) -> Vec<Vec<u8>> {
    cids.into_iter()
        .map(|cid| cid.as_bytes().to_vec())
        .collect()
}

fn json_document(value: &impl Serialize) -> Result<JsonDocumentRecord, ProllyBindingError> {
    serde_json::to_string(value)
        .map(|json| JsonDocumentRecord { json })
        .map_err(json_error)
}

fn json_error(error: serde_json::Error) -> ProllyBindingError {
    ProllyBindingError::Serialization {
        message: error.to_string(),
    }
}

fn cache_stats<S: prolly::Store>(
    engine: &Prolly<S>,
) -> Result<CacheStatsRecord, ProllyBindingError> {
    Ok(CacheStatsRecord {
        cached_nodes: to_u64(engine.cache_len(), "cached_nodes")?,
        cached_bytes: to_u64(engine.cache_bytes_len(), "cached_bytes")?,
        pinned_nodes: to_u64(engine.cache_pinned_len(), "pinned_nodes")?,
        pinned_bytes: to_u64(engine.cache_pinned_bytes_len(), "pinned_bytes")?,
    })
}

fn invalid_argument(message: impl Into<String>) -> ProllyBindingError {
    ProllyBindingError::InvalidArgument {
        message: message.into(),
    }
}

fn internal_error(message: impl Into<String>) -> ProllyBindingError {
    ProllyBindingError::Internal {
        message: message.into(),
    }
}

fn store_error(error: impl std::error::Error) -> ProllyBindingError {
    ProllyBindingError::Store {
        message: error.to_string(),
    }
}

fn resolver_from_name(name: Option<String>) -> Result<Option<Resolver>, ProllyBindingError> {
    let Some(name) = name else {
        return Ok(None);
    };

    let resolver: Resolver = match name.as_str() {
        "prefer_left" => Box::new(prolly::resolver::prefer_left),
        "prefer_right" => Box::new(prolly::resolver::prefer_right),
        "delete_wins" => Box::new(prolly::resolver::delete_wins),
        "update_wins" => Box::new(prolly::resolver::update_wins),
        other => {
            return Err(ProllyBindingError::InvalidArgument {
                message: format!(
                    "unknown resolver {other:?}; expected prefer_left, prefer_right, delete_wins, or update_wins"
                ),
            });
        }
    };
    Ok(Some(resolver))
}

fn resolver_from_callback(callback: Arc<dyn MergeResolverCallback>) -> Resolver {
    Box::new(move |conflict| callback.resolve(ConflictRecord::from(conflict)).into())
}

fn resolver_from_host_callback<F>(callback: F) -> Resolver
where
    F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
{
    Box::new(move |conflict| callback(ConflictRecord::from(conflict)).into())
}

fn policy_fn_from_name(name: String) -> Result<MergePolicyFn, ProllyBindingError> {
    let policy: MergePolicyFn = match name.as_str() {
        "prefer_left" => Arc::new(prolly::resolver::prefer_left),
        "prefer_right" => Arc::new(prolly::resolver::prefer_right),
        "delete_wins" => Arc::new(prolly::resolver::delete_wins),
        "update_wins" => Arc::new(prolly::resolver::update_wins),
        other => {
            return Err(ProllyBindingError::InvalidArgument {
                message: format!(
                    "unknown resolver {other:?}; expected prefer_left, prefer_right, delete_wins, or update_wins"
                ),
            });
        }
    };
    Ok(policy)
}

fn policy_fn_from_callback(callback: Arc<dyn MergeResolverCallback>) -> MergePolicyFn {
    Arc::new(move |conflict| callback.resolve(ConflictRecord::from(conflict)).into())
}

fn policy_fn_from_host_callback<F>(callback: F) -> MergePolicyFn
where
    F: Fn(ConflictRecord) -> ResolutionRecord + 'static,
{
    let callback = Arc::new(HostMergePolicyCallback(callback));
    Arc::new(move |conflict| callback.resolve(ConflictRecord::from(conflict)).into())
}

struct HostMergePolicyCallback<F>(F);

// The host adapter is used by the synchronous Node wrapper and invokes the
// callback inline during merge. Generated UniFFI callbacks still use the
// stricter `MergeResolverCallback: Send + Sync` path above.
unsafe impl<F> Send for HostMergePolicyCallback<F> {}
unsafe impl<F> Sync for HostMergePolicyCallback<F> {}

impl<F> HostMergePolicyCallback<F>
where
    F: Fn(ConflictRecord) -> ResolutionRecord,
{
    fn resolve(&self, conflict: ConflictRecord) -> ResolutionRecord {
        (self.0)(conflict)
    }
}

fn crdt_config_from_callback(
    delete_policy: CrdtDeletePolicyKind,
    callback: Arc<dyn CrdtResolverCallback>,
) -> CrdtConfig {
    CrdtConfig::custom(move |conflict| callback.resolve(ConflictRecord::from(conflict)).into())
        .with_delete_policy(delete_policy.into())
}

fn crdt_config_from_host_callback<F>(delete_policy: CrdtDeletePolicyKind, callback: F) -> CrdtConfig
where
    F: Fn(ConflictRecord) -> CrdtResolutionRecord + 'static,
{
    let callback = Arc::new(HostCrdtCallback(callback));
    CrdtConfig::custom(move |conflict| callback.resolve(ConflictRecord::from(conflict)).into())
        .with_delete_policy(delete_policy.into())
}

struct HostCrdtCallback<F>(F);

// The host adapter is used by the synchronous Node wrapper and invokes the
// callback inline during merge. Generated UniFFI callbacks still use the
// stricter `CrdtResolverCallback: Send + Sync` path above.
unsafe impl<F> Send for HostCrdtCallback<F> {}
unsafe impl<F> Sync for HostCrdtCallback<F> {}

impl<F> HostCrdtCallback<F>
where
    F: Fn(ConflictRecord) -> CrdtResolutionRecord,
{
    fn resolve(&self, conflict: ConflictRecord) -> CrdtResolutionRecord {
        (self.0)(conflict)
    }
}

fn hex_bytes(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

uniffi::setup_scaffolding!("prolly");

#[cfg(test)]
mod tests {
    use super::*;

    struct JoinResolver;

    impl MergeResolverCallback for JoinResolver {
        fn resolve(&self, conflict: ConflictRecord) -> ResolutionRecord {
            match (conflict.left, conflict.right) {
                (Some(mut left), Some(right)) => {
                    left.extend_from_slice(b"|");
                    left.extend_from_slice(&right);
                    ResolutionRecord {
                        kind: ResolutionKind::Value,
                        value: Some(left),
                    }
                }
                (Some(value), None) | (None, Some(value)) => ResolutionRecord {
                    kind: ResolutionKind::Value,
                    value: Some(value),
                },
                (None, None) => ResolutionRecord {
                    kind: ResolutionKind::Delete,
                    value: None,
                },
            }
        }
    }

    struct CrdtJoinResolver;

    impl CrdtResolverCallback for CrdtJoinResolver {
        fn resolve(&self, conflict: ConflictRecord) -> CrdtResolutionRecord {
            match (conflict.left, conflict.right) {
                (Some(mut left), Some(right)) => {
                    left.extend_from_slice(b"|");
                    left.extend_from_slice(&right);
                    CrdtResolutionRecord {
                        kind: CrdtResolutionKind::Value,
                        value: Some(left),
                    }
                }
                (Some(value), None) | (None, Some(value)) => CrdtResolutionRecord {
                    kind: CrdtResolutionKind::Value,
                    value: Some(value),
                },
                (None, None) => CrdtResolutionRecord {
                    kind: CrdtResolutionKind::Delete,
                    value: None,
                },
            }
        }
    }

    struct CrdtDeleteResolver;

    impl CrdtResolverCallback for CrdtDeleteResolver {
        fn resolve(&self, _conflict: ConflictRecord) -> CrdtResolutionRecord {
            CrdtResolutionRecord {
                kind: CrdtResolutionKind::Delete,
                value: None,
            }
        }
    }

    #[derive(Default)]
    struct TestHostStore {
        nodes: Mutex<BTreeMap<Vec<u8>, Vec<u8>>>,
        hints: Mutex<BTreeMap<(Vec<u8>, Vec<u8>), Vec<u8>>>,
        roots: Mutex<BTreeMap<Vec<u8>, RootManifestRecord>>,
    }

    impl TestHostStore {
        fn unit() -> HostStoreUnitResultRecord {
            HostStoreUnitResultRecord { error: None }
        }

        fn bytes(value: Option<Vec<u8>>) -> HostStoreBytesResultRecord {
            HostStoreBytesResultRecord { value, error: None }
        }
    }

    impl HostStoreCallback for TestHostStore {
        fn get(&self, key: Vec<u8>) -> HostStoreBytesResultRecord {
            Self::bytes(self.nodes.lock().unwrap().get(&key).cloned())
        }

        fn put(&self, key: Vec<u8>, value: Vec<u8>) -> HostStoreUnitResultRecord {
            self.nodes.lock().unwrap().insert(key, value);
            Self::unit()
        }

        fn delete(&self, key: Vec<u8>) -> HostStoreUnitResultRecord {
            self.nodes.lock().unwrap().remove(&key);
            Self::unit()
        }

        fn batch(&self, ops: Vec<MutationRecord>) -> HostStoreUnitResultRecord {
            let mut nodes = self.nodes.lock().unwrap();
            for op in ops {
                match op.kind {
                    MutationKind::Upsert => {
                        nodes.insert(op.key, op.value.unwrap_or_default());
                    }
                    MutationKind::Delete => {
                        nodes.remove(&op.key);
                    }
                }
            }
            Self::unit()
        }

        fn batch_get_ordered(&self, keys: Vec<Vec<u8>>) -> HostStoreBatchGetResultRecord {
            let nodes = self.nodes.lock().unwrap();
            HostStoreBatchGetResultRecord {
                values: keys.iter().map(|key| nodes.get(key).cloned()).collect(),
                error: None,
            }
        }

        fn prefers_batch_reads(&self) -> HostStoreBoolResultRecord {
            HostStoreBoolResultRecord {
                value: true,
                error: None,
            }
        }

        fn supports_hints(&self) -> HostStoreBoolResultRecord {
            HostStoreBoolResultRecord {
                value: true,
                error: None,
            }
        }

        fn get_hint(&self, namespace: Vec<u8>, key: Vec<u8>) -> HostStoreBytesResultRecord {
            Self::bytes(self.hints.lock().unwrap().get(&(namespace, key)).cloned())
        }

        fn put_hint(
            &self,
            namespace: Vec<u8>,
            key: Vec<u8>,
            value: Vec<u8>,
        ) -> HostStoreUnitResultRecord {
            self.hints.lock().unwrap().insert((namespace, key), value);
            Self::unit()
        }

        fn list_node_cids(&self) -> HostStoreListBytesResultRecord {
            HostStoreListBytesResultRecord {
                values: self.nodes.lock().unwrap().keys().cloned().collect(),
                error: None,
            }
        }

        fn get_root(&self, name: Vec<u8>) -> HostStoreRootResultRecord {
            HostStoreRootResultRecord {
                value: self.roots.lock().unwrap().get(&name).cloned(),
                error: None,
            }
        }

        fn put_root(
            &self,
            name: Vec<u8>,
            manifest: RootManifestRecord,
        ) -> HostStoreUnitResultRecord {
            self.roots.lock().unwrap().insert(name, manifest);
            Self::unit()
        }

        fn delete_root(&self, name: Vec<u8>) -> HostStoreUnitResultRecord {
            self.roots.lock().unwrap().remove(&name);
            Self::unit()
        }

        fn compare_and_swap_root(
            &self,
            name: Vec<u8>,
            expected: Option<RootManifestRecord>,
            replacement: Option<RootManifestRecord>,
        ) -> HostStoreRootCasResultRecord {
            let mut roots = self.roots.lock().unwrap();
            let current = roots.get(&name).cloned();
            if current == expected {
                match replacement {
                    Some(replacement) => {
                        roots.insert(name, replacement);
                    }
                    None => {
                        roots.remove(&name);
                    }
                }
                HostStoreRootCasResultRecord {
                    applied: true,
                    current: None,
                    error: None,
                }
            } else {
                HostStoreRootCasResultRecord {
                    applied: false,
                    current,
                    error: None,
                }
            }
        }

        fn list_roots(&self) -> HostStoreListRootsResultRecord {
            HostStoreListRootsResultRecord {
                values: self
                    .roots
                    .lock()
                    .unwrap()
                    .iter()
                    .map(|(name, manifest)| HostStoreNamedRootManifestRecord {
                        name: name.clone(),
                        manifest: manifest.clone(),
                    })
                    .collect(),
                error: None,
            }
        }
    }

    #[test]
    fn memory_engine_round_trips_basic_operations() {
        let engine = ProllyEngine::memory(default_config()).unwrap();
        let tree = engine.create();
        let tree = engine.put(tree, b"a".to_vec(), b"1".to_vec()).unwrap();
        let tree = engine.put(tree, b"b".to_vec(), b"2".to_vec()).unwrap();

        assert_eq!(
            engine.get(tree.clone(), b"a".to_vec()).unwrap(),
            Some(b"1".to_vec())
        );
        assert_eq!(
            engine
                .range(tree.clone(), Vec::new(), None)
                .unwrap()
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );

        let other = engine.delete(tree.clone(), b"a".to_vec()).unwrap();
        assert_eq!(engine.diff(tree, other).unwrap().len(), 1);
    }

    #[test]
    fn config_helpers_match_rust_constructors() {
        assert_eq!(encoding_raw(), Encoding::Raw.into());
        assert_eq!(encoding_cbor(), Encoding::Cbor.into());
        assert_eq!(encoding_json(), Encoding::Json.into());
        assert_eq!(
            encoding_custom("postcard".to_string()),
            Encoding::Custom("postcard".to_string()).into()
        );

        let config = tree_config(
            2,
            64,
            32,
            7,
            encoding_custom("postcard".to_string()),
            Some(16),
            Some(4096),
        )
        .unwrap();
        assert_eq!(
            Config::try_from(config.clone()).unwrap(),
            Config::builder()
                .min_chunk_size(2)
                .max_chunk_size(64)
                .chunking_factor(32)
                .hash_seed(7)
                .encoding(Encoding::Custom("postcard".to_string()))
                .node_cache_max_nodes(16)
                .node_cache_max_bytes(4096)
                .build()
        );
        assert_eq!(config.node_cache_max_nodes, Some(16));
        assert_eq!(config.node_cache_max_bytes, Some(4096));

        assert_eq!(
            large_value_config(8).unwrap(),
            LargeValueConfig::new(8).into()
        );
        assert_eq!(
            parallel_config(2, 24).unwrap(),
            ParallelConfig::new(2, 24).into()
        );
        assert_eq!(
            parallel_config_sequential(),
            ParallelConfig::sequential().into()
        );
    }

    #[test]
    fn node_bytes_and_cid_round_trip() {
        let node = NodeRecord {
            keys: vec![b"a".to_vec()],
            vals: vec![b"1".to_vec()],
            leaf: true,
            level: 0,
            min_chunk_size: 4,
            max_chunk_size: 1024,
            chunking_factor: 128,
            hash_seed: 0,
            encoding: EncodingRecord {
                kind: EncodingKind::Raw,
                custom_name: None,
            },
        };

        let bytes = node_to_bytes(node.clone()).unwrap();
        let decoded = node_from_bytes(bytes.clone()).unwrap();
        assert_eq!(decoded, node);
        assert_eq!(node_cid(decoded).unwrap(), cid_from_bytes(bytes));
    }

    #[test]
    fn composite_key_helpers_build_segment_safe_keys() {
        let key = key_from_segments(vec![b"tenant".to_vec(), b"42".to_vec(), vec![0]]);
        let expected = [
            b"tenant".as_slice(),
            &[0, 0],
            b"42".as_slice(),
            &[0, 0],
            &[0, 0xff, 0, 0],
        ]
        .concat();
        assert_eq!(key, expected);
        assert_eq!(
            decode_segments(key.clone()).unwrap(),
            vec![b"tenant".to_vec(), b"42".to_vec(), vec![0]]
        );

        let prefixed = key_from_prefixed_segments(key, vec![u64_key(7)]);
        assert_eq!(
            decode_segments(prefixed).unwrap(),
            vec![b"tenant".to_vec(), b"42".to_vec(), vec![0], u64_key(7)]
        );
    }

    #[test]
    fn changed_span_helpers_match_rust_constructors() {
        assert_eq!(
            changed_span(b"a".to_vec(), Some(b"z".to_vec())),
            ChangedSpanRecord {
                start: b"a".to_vec(),
                end: Some(b"z".to_vec()),
            }
        );
        assert_eq!(
            changed_span_from_key(b"k".to_vec()),
            ChangedSpanRecord {
                start: b"k".to_vec(),
                end: Some(b"k\0".to_vec()),
            }
        );
        assert_eq!(
            changed_span_for_prefix(b"k".to_vec()),
            ChangedSpanRecord {
                start: b"k".to_vec(),
                end: Some(b"l".to_vec()),
            }
        );
        assert_eq!(
            changed_span_for_prefix(Vec::new()),
            ChangedSpanRecord {
                start: Vec::new(),
                end: None,
            }
        );
    }

    #[test]
    fn named_root_retention_helpers_match_rust_constructors() {
        assert_eq!(
            retain_all_named_roots(),
            NamedRootRetentionRecord::from(NamedRootRetention::all())
        );
        assert_eq!(
            retain_exact_named_roots(vec![b"main".to_vec(), b"staging".to_vec()]),
            NamedRootRetentionRecord::from(NamedRootRetention::exact([
                b"main".as_slice(),
                b"staging".as_slice()
            ]))
        );
        assert_eq!(
            retain_named_root_prefix(b"refs/heads/".to_vec()),
            NamedRootRetentionRecord::from(NamedRootRetention::prefix(b"refs/heads/"))
        );
        assert_eq!(
            retain_newest_named_roots(b"checkpoint/".to_vec(), 3),
            NamedRootRetentionRecord::from(NamedRootRetention::newest_by_name(b"checkpoint/", 3))
        );
        assert_eq!(
            retain_named_roots_updated_since(b"checkpoint/".to_vec(), 42),
            NamedRootRetentionRecord::from(NamedRootRetention::updated_since(b"checkpoint/", 42))
        );
    }

    #[test]
    fn range_cursor_helpers_match_rust_constructors() {
        assert_eq!(range_cursor_start(), RangeCursorRecord { after_key: None });
        assert_eq!(
            range_cursor_after_key(b"k".to_vec()),
            RangeCursorRecord::from(RangeCursor::after_key(b"k".to_vec()))
        );
        assert_eq!(
            reverse_cursor_end(),
            ReverseCursorRecord { before_key: None }
        );
        assert_eq!(
            reverse_cursor_before_key(b"k".to_vec()),
            ReverseCursorRecord::from(ReverseCursor::before_key(b"k".to_vec()))
        );
    }

    #[test]
    fn mutation_helpers_match_rust_constructors() {
        assert_eq!(
            upsert_mutation(b"k".to_vec(), b"value".to_vec()),
            MutationRecord::from(Mutation::Upsert {
                key: b"k".to_vec(),
                val: b"value".to_vec(),
            })
        );
        assert_eq!(
            delete_mutation(b"k".to_vec()),
            MutationRecord::from(Mutation::Delete { key: b"k".to_vec() })
        );
    }

    #[test]
    fn resolution_helpers_match_rust_constructors() {
        assert_eq!(
            resolution_value(b"value".to_vec()),
            ResolutionRecord::from(prolly::Resolution::value(b"value".to_vec()))
        );
        assert_eq!(
            resolution_delete(),
            ResolutionRecord::from(prolly::Resolution::delete())
        );
        assert_eq!(
            resolution_unresolved(),
            ResolutionRecord::from(prolly::Resolution::unresolved())
        );
        assert_eq!(
            crdt_resolution_value(b"value".to_vec()),
            CrdtResolutionRecord::from(prolly::CrdtResolution::value(b"value".to_vec()))
        );
        assert_eq!(
            crdt_resolution_delete(),
            CrdtResolutionRecord::from(prolly::CrdtResolution::delete())
        );
    }

    #[test]
    fn built_in_resolver_helpers_match_rust_resolvers() {
        let update_conflict = ConflictRecord {
            key: b"k".to_vec(),
            base: Some(b"base".to_vec()),
            left: Some(b"left".to_vec()),
            right: Some(b"right".to_vec()),
        };
        assert_eq!(
            resolve_prefer_left(update_conflict.clone()),
            ResolutionRecord::from(prolly::resolver::prefer_left(
                &update_conflict.clone().into()
            ))
        );
        assert_eq!(
            resolve_prefer_right(update_conflict.clone()),
            ResolutionRecord::from(prolly::resolver::prefer_right(
                &update_conflict.clone().into()
            ))
        );
        assert_eq!(
            resolve_delete_wins(update_conflict.clone()).kind,
            ResolutionKind::Unresolved
        );

        let delete_conflict = ConflictRecord {
            key: b"k".to_vec(),
            base: Some(b"base".to_vec()),
            left: None,
            right: Some(b"right".to_vec()),
        };
        assert_eq!(
            resolve_delete_wins(delete_conflict.clone()),
            ResolutionRecord::from(prolly::resolver::delete_wins(
                &delete_conflict.clone().into()
            ))
        );
        assert_eq!(
            resolve_update_wins(delete_conflict.clone()),
            ResolutionRecord::from(prolly::resolver::update_wins(&delete_conflict.into()))
        );
    }

    #[test]
    fn key_proofs_verify_through_binding_records() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let entries = (0..10)
            .map(|idx| EntryRecord {
                key: format!("k{idx:02}").into_bytes(),
                value: format!("v{idx:02}").into_bytes(),
            })
            .collect::<Vec<_>>();
        let tree = engine.build_from_sorted_entries(entries).unwrap();

        let proof = engine.prove_key(tree.clone(), b"k05".to_vec()).unwrap();
        assert_eq!(proof.root, tree.root);
        assert!(proof.path.len() > 1);

        let verified = verify_key_proof(proof.clone()).unwrap();
        assert!(verified.valid);
        assert!(verified.exists);
        assert!(!verified.absence);
        assert_eq!(verified.value, Some(b"v05".to_vec()));

        let encoded = key_proof_path_node_bytes(proof.clone()).unwrap();
        let decoded =
            key_proof_from_node_bytes(proof.root.clone(), proof.key.clone(), encoded).unwrap();
        assert_eq!(
            verify_key_proof(decoded).unwrap().value,
            Some(b"v05".to_vec())
        );

        let bundled = key_proof_to_bytes(proof.clone()).unwrap();
        assert_eq!(bundled, key_proof_to_bytes(proof.clone()).unwrap());
        let key_summary = inspect_proof_bundle(bundled.clone()).unwrap();
        assert_eq!(key_summary.kind, "key");
        assert_eq!(key_summary.root, tree.root);
        assert_eq!(key_summary.key_count, 1);
        assert_eq!(key_summary.path_node_count, proof.path.len() as u64);
        let key_bundle_verified = verify_proof_bundle(bundled.clone()).unwrap();
        assert!(key_bundle_verified.valid);
        assert_eq!(key_bundle_verified.summary.kind, "key");
        assert_eq!(key_bundle_verified.exists_count, 1);
        assert_eq!(key_bundle_verified.absence_count, 0);
        let decoded_bundle = key_proof_from_bytes(bundled).unwrap();
        assert_eq!(
            verify_key_proof(decoded_bundle).unwrap().value,
            Some(b"v05".to_vec())
        );

        let absent = engine.prove_key(tree.clone(), b"k05a".to_vec()).unwrap();
        let verified_absence = verify_key_proof(absent).unwrap();
        assert!(verified_absence.valid);
        assert!(!verified_absence.exists);
        assert!(verified_absence.absence);
        assert_eq!(verified_absence.value, None);

        let mut tampered = proof;
        let leaf = tampered.path.last_mut().unwrap();
        let value_index = leaf
            .keys
            .iter()
            .position(|key| key.as_slice() == b"k05")
            .unwrap();
        leaf.vals[value_index] = b"tampered".to_vec();
        assert!(
            !verify_proof_bundle(key_proof_to_bytes(tampered.clone()).unwrap())
                .unwrap()
                .valid
        );
        assert!(!verify_key_proof(tampered).unwrap().valid);

        let multi = engine
            .prove_keys(
                tree.clone(),
                vec![b"k01".to_vec(), b"k05a".to_vec(), b"k08".to_vec()],
            )
            .unwrap();
        assert_eq!(multi.root, tree.root);
        assert!(multi.path.len() > 1);

        let multi_verified = verify_multi_key_proof(multi.clone()).unwrap();
        assert!(multi_verified.valid);
        assert_eq!(multi_verified.results.len(), 3);
        assert_eq!(multi_verified.results[0].value, Some(b"v01".to_vec()));
        assert!(multi_verified.results[1].absence);
        assert_eq!(multi_verified.results[2].value, Some(b"v08".to_vec()));

        let encoded_multi = multi_key_proof_path_node_bytes(multi.clone()).unwrap();
        let decoded_multi =
            multi_key_proof_from_node_bytes(multi.root.clone(), multi.keys.clone(), encoded_multi)
                .unwrap();
        assert_eq!(
            verify_multi_key_proof(decoded_multi).unwrap().results[2].value,
            Some(b"v08".to_vec())
        );

        let bundled_multi = multi_key_proof_to_bytes(multi.clone()).unwrap();
        assert_eq!(
            bundled_multi,
            multi_key_proof_to_bytes(multi.clone()).unwrap()
        );
        let multi_summary = inspect_proof_bundle(bundled_multi.clone()).unwrap();
        assert_eq!(multi_summary.kind, "multi_key");
        assert_eq!(multi_summary.key_count, 3);
        assert_eq!(multi_summary.path_node_count, multi.path.len() as u64);
        let multi_bundle_verified = verify_proof_bundle(bundled_multi.clone()).unwrap();
        assert!(multi_bundle_verified.valid);
        assert_eq!(multi_bundle_verified.summary.kind, "multi_key");
        assert_eq!(multi_bundle_verified.exists_count, 2);
        assert_eq!(multi_bundle_verified.absence_count, 1);
        let decoded_multi_bundle = multi_key_proof_from_bytes(bundled_multi).unwrap();
        assert_eq!(
            verify_multi_key_proof(decoded_multi_bundle)
                .unwrap()
                .results[2]
                .value,
            Some(b"v08".to_vec())
        );

        let range = engine
            .prove_range(tree.clone(), b"k02".to_vec(), Some(b"k06".to_vec()))
            .unwrap();
        assert_eq!(range.root, tree.root);
        assert!(range.path.len() > 1);
        let range_verified = verify_range_proof(range.clone()).unwrap();
        assert!(range_verified.valid);
        assert_eq!(range_verified.entries.len(), 4);
        assert_eq!(range_verified.entries[0].key, b"k02".to_vec());
        assert_eq!(range_verified.entries[3].value, b"v05".to_vec());

        let encoded_range = range_proof_path_node_bytes(range.clone()).unwrap();
        let decoded_range = range_proof_from_node_bytes(
            range.root.clone(),
            range.start.clone(),
            range.end.clone(),
            encoded_range,
        )
        .unwrap();
        assert_eq!(
            verify_range_proof(decoded_range).unwrap().entries[3].value,
            b"v05".to_vec()
        );

        let bundled_range = range_proof_to_bytes(range.clone()).unwrap();
        assert_eq!(bundled_range, range_proof_to_bytes(range.clone()).unwrap());
        let range_summary = inspect_proof_bundle(bundled_range.clone()).unwrap();
        assert_eq!(range_summary.kind, "range");
        assert_eq!(range_summary.start, Some(b"k02".to_vec()));
        assert_eq!(range_summary.end, Some(b"k06".to_vec()));
        let range_bundle_verified = verify_proof_bundle(bundled_range.clone()).unwrap();
        assert!(range_bundle_verified.valid);
        assert_eq!(range_bundle_verified.summary.kind, "range");
        assert_eq!(range_bundle_verified.entry_count, 4);
        let decoded_range_bundle = range_proof_from_bytes(bundled_range).unwrap();
        assert_eq!(
            verify_range_proof(decoded_range_bundle).unwrap().entries[3].value,
            b"v05".to_vec()
        );

        let prefix = engine.prove_prefix(tree.clone(), b"k0".to_vec()).unwrap();
        let prefix_verified = verify_range_proof(prefix.clone()).unwrap();
        assert!(prefix_verified.valid);
        assert_eq!(prefix.start, b"k0".to_vec());
        assert_eq!(prefix.end, Some(b"k1".to_vec()));
        assert_eq!(prefix_verified.entries.len(), 10);
        assert_eq!(prefix_verified.entries[9].value, b"v09".to_vec());

        let proved_page = engine
            .prove_range_page(
                tree.clone(),
                Some(RangeCursorRecord {
                    after_key: Some(b"k03".to_vec()),
                }),
                None,
                3,
            )
            .unwrap();
        assert_eq!(proved_page.page.entries.len(), 3);
        assert_eq!(proved_page.page.entries[0].key, b"k04".to_vec());
        assert_eq!(
            proved_page.page.next_cursor.as_ref().unwrap().after_key,
            Some(b"k06".to_vec())
        );
        assert_eq!(proved_page.proof.after, Some(b"k03".to_vec()));
        assert_eq!(proved_page.proof.end, Some(b"k07".to_vec()));
        let page_verified = verify_range_page_proof(proved_page.proof.clone()).unwrap();
        assert!(page_verified.valid);
        assert_eq!(page_verified.entries, proved_page.page.entries);

        let encoded_page = range_page_proof_path_node_bytes(proved_page.proof.clone()).unwrap();
        let decoded_page = range_page_proof_from_node_bytes(
            proved_page.proof.root.clone(),
            proved_page.proof.after.clone(),
            proved_page.proof.end.clone(),
            encoded_page,
        )
        .unwrap();
        assert_eq!(
            verify_range_page_proof(decoded_page).unwrap().entries,
            proved_page.page.entries
        );

        let bundled_page = range_page_proof_to_bytes(proved_page.proof.clone()).unwrap();
        assert_eq!(
            bundled_page,
            range_page_proof_to_bytes(proved_page.proof.clone()).unwrap()
        );
        let page_summary = inspect_proof_bundle(bundled_page.clone()).unwrap();
        assert_eq!(page_summary.kind, "range_page");
        assert_eq!(page_summary.after, Some(b"k03".to_vec()));
        assert_eq!(page_summary.end, proved_page.proof.end);
        let page_bundle_verified = verify_proof_bundle(bundled_page.clone()).unwrap();
        assert!(page_bundle_verified.valid);
        assert_eq!(page_bundle_verified.summary.kind, "range_page");
        assert_eq!(
            page_bundle_verified.entry_count,
            proved_page.page.entries.len() as u64
        );
        let decoded_page_bundle = range_page_proof_from_bytes(bundled_page).unwrap();
        assert_eq!(
            verify_range_page_proof(decoded_page_bundle)
                .unwrap()
                .entries,
            proved_page.page.entries
        );

        let other = engine.delete(tree.clone(), b"k02".to_vec()).unwrap();
        let other = engine
            .put(other, b"k04".to_vec(), b"v04x".to_vec())
            .unwrap();
        let other = engine
            .put(other, b"k05a".to_vec(), b"bonus".to_vec())
            .unwrap();
        let proved_diff_page = engine
            .prove_diff_page(tree.clone(), other.clone(), None, None, 2)
            .unwrap();
        assert_eq!(proved_diff_page.page.diffs.len(), 2);
        assert_eq!(proved_diff_page.page.diffs[0].kind, DiffKind::Removed);
        assert_eq!(proved_diff_page.page.diffs[0].key, b"k02".to_vec());
        assert_eq!(proved_diff_page.page.diffs[1].kind, DiffKind::Changed);
        assert_eq!(
            proved_diff_page
                .page
                .next_cursor
                .as_ref()
                .unwrap()
                .after_key,
            Some(b"k04".to_vec())
        );
        assert_eq!(proved_diff_page.proof.base.end, Some(b"k05a".to_vec()));
        assert_eq!(proved_diff_page.proof.limit, 2);
        assert_eq!(
            proved_diff_page.proof.lookahead_base.as_ref().unwrap().key,
            b"k05a".to_vec()
        );
        let diff_page_verified = verify_diff_page_proof(proved_diff_page.proof.clone()).unwrap();
        assert!(diff_page_verified.valid);
        assert!(diff_page_verified.lookahead_valid);
        assert_eq!(diff_page_verified.diffs, proved_diff_page.page.diffs);
        assert_eq!(
            diff_page_verified.next_cursor,
            proved_diff_page.page.next_cursor
        );

        let bundled_diff_page = diff_page_proof_to_bytes(proved_diff_page.proof.clone()).unwrap();
        assert_eq!(
            bundled_diff_page,
            diff_page_proof_to_bytes(proved_diff_page.proof.clone()).unwrap()
        );
        let diff_summary = inspect_proof_bundle(bundled_diff_page.clone()).unwrap();
        assert_eq!(diff_summary.kind, "diff_page");
        assert_eq!(diff_summary.root, tree.root);
        assert_eq!(diff_summary.other_root, other.root);
        assert_eq!(diff_summary.limit, Some(2));
        assert!(diff_summary.has_lookahead);
        let diff_bundle_verified = verify_proof_bundle(bundled_diff_page.clone()).unwrap();
        assert!(diff_bundle_verified.valid);
        assert_eq!(diff_bundle_verified.summary.kind, "diff_page");
        assert_eq!(diff_bundle_verified.diff_count, 2);
        assert_eq!(
            diff_bundle_verified.next_cursor,
            proved_diff_page.page.next_cursor
        );
        let decoded_diff_page = diff_page_proof_from_bytes(bundled_diff_page).unwrap();
        assert_eq!(decoded_diff_page, proved_diff_page.proof);
        assert_eq!(
            verify_diff_page_proof(decoded_diff_page).unwrap().diffs,
            proved_diff_page.page.diffs
        );

        let signed = sign_proof_bundle_hmac_sha256(
            key_proof_to_bytes(engine.prove_key(tree.clone(), b"k05".to_vec()).unwrap()).unwrap(),
            b"binding-key".to_vec(),
            b"shared secret".to_vec(),
            b"tenant=t1".to_vec(),
            Some(1_700_000_000_000),
            Some(1_700_000_100_000),
            b"nonce-1".to_vec(),
        )
        .unwrap();
        assert_eq!(signed.key_id, b"binding-key".to_vec());
        assert_eq!(signed.context, b"tenant=t1".to_vec());
        assert_eq!(signed.signature.len(), 32);

        let signed_bytes = authenticated_proof_envelope_to_bytes(signed.clone()).unwrap();
        assert_eq!(
            signed_bytes,
            authenticated_proof_envelope_to_bytes(signed.clone()).unwrap()
        );
        let decoded_signed = authenticated_proof_envelope_from_bytes(signed_bytes.clone()).unwrap();
        assert_eq!(decoded_signed, signed);

        let envelope_verified = verify_authenticated_proof_envelope(
            decoded_signed.clone(),
            b"shared secret".to_vec(),
            Some(1_700_000_050_000),
        );
        assert!(envelope_verified.valid);
        assert!(envelope_verified.signature_valid);
        assert!(envelope_verified.time_valid);
        assert_eq!(envelope_verified.key_id, b"binding-key".to_vec());
        assert_eq!(envelope_verified.context, b"tenant=t1".to_vec());
        assert_eq!(
            verify_key_proof(key_proof_from_bytes(envelope_verified.proof_bundle).unwrap())
                .unwrap()
                .value,
            Some(b"v05".to_vec())
        );

        let authenticated_bundle = verify_authenticated_proof_bundle(
            signed_bytes.clone(),
            b"shared secret".to_vec(),
            Some(1_700_000_050_000),
        )
        .unwrap();
        assert!(authenticated_bundle.valid);
        assert!(authenticated_bundle.envelope.valid);
        assert_eq!(authenticated_bundle.proof_error, None);
        assert_eq!(
            authenticated_bundle
                .proof
                .as_ref()
                .map(|proof| proof.exists_count),
            Some(1)
        );

        let wrong_secret = verify_authenticated_proof_envelope(
            decoded_signed.clone(),
            b"wrong secret".to_vec(),
            Some(1_700_000_050_000),
        );
        assert!(!wrong_secret.valid);
        assert!(!wrong_secret.signature_valid);
        let wrong_secret_bundle = verify_authenticated_proof_bundle(
            signed_bytes.clone(),
            b"wrong secret".to_vec(),
            Some(1_700_000_050_000),
        )
        .unwrap();
        assert!(!wrong_secret_bundle.valid);
        assert!(wrong_secret_bundle.proof.is_none());

        let expired = verify_authenticated_proof_envelope(
            decoded_signed,
            b"shared secret".to_vec(),
            Some(1_700_000_100_000),
        );
        assert!(!expired.valid);
        assert!(expired.expired);
        let expired_bundle = verify_authenticated_proof_bundle(
            signed_bytes,
            b"shared secret".to_vec(),
            Some(1_700_000_100_000),
        )
        .unwrap();
        assert!(!expired_bundle.valid);
        assert!(expired_bundle.envelope.expired);
        assert!(expired_bundle.proof.is_none());

        let malformed_signed = sign_proof_bundle_hmac_sha256(
            vec![0, 1, 2, 3],
            b"binding-key".to_vec(),
            b"shared secret".to_vec(),
            b"tenant=t1".to_vec(),
            None,
            None,
            b"nonce-2".to_vec(),
        )
        .unwrap();
        let malformed_signed_bytes =
            authenticated_proof_envelope_to_bytes(malformed_signed).unwrap();
        let malformed_bundle = verify_authenticated_proof_bundle(
            malformed_signed_bytes,
            b"shared secret".to_vec(),
            None,
        )
        .unwrap();
        assert!(!malformed_bundle.valid);
        assert!(malformed_bundle.envelope.valid);
        assert!(malformed_bundle.proof.is_none());
        assert!(malformed_bundle.proof_error.is_some());
    }

    #[test]
    fn file_engine_persists_nodes_across_reopen() {
        let path = temp_path("file-engine");
        let _ = std::fs::remove_dir_all(&path);

        let tree = {
            let engine =
                ProllyEngine::file(path.to_string_lossy().into_owned(), small_config()).unwrap();
            let tree = engine.create();
            engine.put(tree, b"k".to_vec(), b"v".to_vec()).unwrap()
        };

        let reopened =
            ProllyEngine::file(path.to_string_lossy().into_owned(), small_config()).unwrap();
        assert_eq!(
            reopened.get(tree, b"k".to_vec()).unwrap(),
            Some(b"v".to_vec())
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn custom_store_callbacks_drive_engine_store_manifest_hints_and_sync() {
        let store = Arc::new(TestHostStore::default());
        let engine = Arc::new(ProllyEngine::custom_store(store, small_config()).unwrap());
        let tree = engine
            .batch(
                engine.create(),
                vec![upsert(b"a", b"1"), upsert(b"b", b"2")],
            )
            .unwrap();

        assert_eq!(
            engine
                .get_many(tree.clone(), vec![b"a".to_vec(), b"missing".to_vec()])
                .unwrap(),
            vec![Some(b"1".to_vec()), None]
        );
        assert_eq!(
            engine.first_entry(tree.clone()).unwrap(),
            Some(EntryRecord {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            })
        );
        assert_eq!(
            engine.last_entry(tree.clone()).unwrap(),
            Some(EntryRecord {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            })
        );
        assert_eq!(
            engine.lower_bound(tree.clone(), b"aa".to_vec()).unwrap(),
            Some(EntryRecord {
                key: b"b".to_vec(),
                value: b"2".to_vec(),
            })
        );
        assert_eq!(
            engine.upper_bound(tree.clone(), b"b".to_vec()).unwrap(),
            None
        );
        assert_eq!(
            engine.prefix(tree.clone(), b"a".to_vec()).unwrap(),
            vec![EntryRecord {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }]
        );
        let prefix_page = engine
            .prefix_page(tree.clone(), b"a".to_vec(), None, 1)
            .unwrap();
        assert_eq!(
            prefix_page.entries,
            vec![EntryRecord {
                key: b"a".to_vec(),
                value: b"1".to_vec(),
            }]
        );
        assert!(prefix_page.next_cursor.is_some());
        assert!(engine
            .publish_prefix_path_hint(tree.clone(), b"a".to_vec())
            .unwrap());
        assert!(engine
            .hydrate_prefix_path_hint(tree.clone(), b"a".to_vec())
            .unwrap());

        engine
            .publish_named_root_at_millis(b"main".to_vec(), tree.clone(), 42)
            .unwrap();
        assert_eq!(
            engine.load_named_root(b"main".to_vec()).unwrap(),
            Some(tree.clone())
        );
        assert_eq!(engine.list_named_roots().unwrap().len(), 1);
        let manifests = engine.list_named_root_manifests().unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].name, b"main".to_vec());
        assert_eq!(manifests[0].manifest.tree, tree);
        assert_eq!(manifests[0].manifest.created_at_millis, Some(42));
        assert_eq!(manifests[0].manifest.updated_at_millis, Some(42));
        assert_eq!(
            engine
                .load_retained_named_roots(all_named_root_retention())
                .unwrap()
                .roots
                .len(),
            1
        );

        let cids = engine.list_node_cids().unwrap();
        assert!(!cids.is_empty());
        let plan = engine.plan_store_gc(vec![tree.clone()]).unwrap();
        assert_eq!(plan.reclaimable_nodes, 0);
        let retained_plan = engine
            .plan_store_gc_for_retention(all_named_root_retention())
            .unwrap();
        assert_eq!(retained_plan.reclaimable_nodes, 0);

        let conflict = engine
            .compare_and_swap_named_root(b"main".to_vec(), None, Some(tree.clone()))
            .unwrap();
        assert!(!conflict.applied);
        assert!(conflict.conflict);
        assert!(conflict.current.is_some());

        let destination = Arc::new(
            ProllyEngine::custom_store(Arc::new(TestHostStore::default()), small_config()).unwrap(),
        );
        let missing = engine
            .plan_missing_nodes(tree.clone(), destination.clone())
            .unwrap();
        assert_eq!(missing.missing_nodes, missing.required_nodes);
        let copied = engine
            .copy_missing_nodes(tree.clone(), destination.clone())
            .unwrap();
        assert_eq!(copied.copied_nodes, missing.missing_nodes);
        assert_eq!(
            destination.get(tree.clone(), b"b".to_vec()).unwrap(),
            Some(b"2".to_vec())
        );

        let delete = engine
            .compare_and_swap_named_root(b"main".to_vec(), Some(tree), None)
            .unwrap();
        assert!(delete.applied);
        assert!(engine.load_named_root(b"main".to_vec()).unwrap().is_none());
    }

    #[test]
    fn paging_merge_and_named_roots_work_through_binding_records() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let empty = engine.create();
        let tree = engine
            .batch(
                empty.clone(),
                vec![upsert(b"a", b"1"), upsert(b"b", b"2"), upsert(b"c", b"3")],
            )
            .unwrap();

        let first_page = engine
            .range_page(tree.clone(), None, None, 2)
            .expect("range page");
        assert_eq!(first_page.entries.len(), 2);
        assert!(first_page.next_cursor.is_some());

        let after_a = engine
            .range_after(tree.clone(), b"a".to_vec(), None)
            .expect("range after");
        assert_eq!(
            after_a
                .iter()
                .map(|entry| (entry.key.clone(), entry.value.clone()))
                .collect::<Vec<_>>(),
            vec![
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec())
            ]
        );

        let from_cursor = engine
            .range_from_cursor(
                tree.clone(),
                Some(RangeCursorRecord {
                    after_key: Some(b"a".to_vec()),
                }),
                None,
            )
            .expect("range from cursor");
        assert_eq!(from_cursor, after_a);

        let window = engine
            .cursor_window(tree.clone(), b"bb".to_vec(), None, 1)
            .expect("cursor window");
        assert_eq!(window.position_key, Some(b"b".to_vec()));
        assert_eq!(window.position_value, Some(b"2".to_vec()));
        assert!(!window.found);
        assert_eq!(window.entries.len(), 1);
        assert_eq!(window.entries[0].key, b"c".to_vec());
        assert_eq!(window.entries[0].value, b"3".to_vec());
        assert_eq!(
            window.next_cursor,
            Some(RangeCursorRecord {
                after_key: Some(b"c".to_vec())
            })
        );

        let exact_probe = engine
            .cursor_window(tree.clone(), b"b".to_vec(), None, 0)
            .expect("exact cursor probe");
        assert!(exact_probe.found);
        assert_eq!(exact_probe.position_key, Some(b"b".to_vec()));
        assert!(exact_probe.entries.is_empty());
        assert!(exact_probe.next_cursor.is_none());

        let second_page = engine
            .range_page(tree.clone(), first_page.next_cursor, None, 2)
            .expect("second range page");
        assert_eq!(
            second_page
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![(b"c".to_vec(), b"3".to_vec())]
        );
        assert!(second_page.next_cursor.is_none());

        let reverse_first = engine
            .reverse_page(tree.clone(), None, Vec::new(), 2)
            .expect("reverse page");
        assert_eq!(
            reverse_first
                .entries
                .iter()
                .map(|entry| (entry.key.clone(), entry.value.clone()))
                .collect::<Vec<_>>(),
            vec![
                (b"c".to_vec(), b"3".to_vec()),
                (b"b".to_vec(), b"2".to_vec())
            ]
        );
        assert_eq!(
            reverse_first.next_cursor,
            Some(ReverseCursorRecord {
                before_key: Some(b"b".to_vec())
            })
        );
        let reverse_second = engine
            .reverse_page(tree.clone(), reverse_first.next_cursor, Vec::new(), 2)
            .expect("second reverse page");
        assert_eq!(
            reverse_second
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![(b"a".to_vec(), b"1".to_vec())]
        );
        assert!(reverse_second.next_cursor.is_none());

        let prefix_reverse = engine
            .prefix_reverse_page(tree.clone(), b"b".to_vec(), None, 4)
            .expect("prefix reverse page");
        assert_eq!(
            prefix_reverse
                .entries
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![(b"b".to_vec(), b"2".to_vec())]
        );
        assert!(prefix_reverse.next_cursor.is_none());

        let diff_page = engine
            .diff_page(empty.clone(), tree.clone(), None, None, 1)
            .expect("diff page");
        assert_eq!(diff_page.diffs.len(), 1);
        assert!(diff_page.next_cursor.is_some());

        let changed_for_cursor = engine
            .batch(tree.clone(), vec![upsert(b"b", b"22"), upsert(b"c", b"33")])
            .expect("changed tree for diff cursor");
        let resumed_diffs = engine
            .diff_from_cursor(
                tree.clone(),
                changed_for_cursor,
                Some(RangeCursorRecord {
                    after_key: Some(b"a".to_vec()),
                }),
                Some(b"c".to_vec()),
            )
            .expect("diff from cursor");
        assert_eq!(resumed_diffs.len(), 1);
        assert_eq!(resumed_diffs[0].kind, DiffKind::Changed);
        assert_eq!(resumed_diffs[0].key, b"b".to_vec());

        let parallel = engine
            .parallel_batch(
                tree.clone(),
                vec![upsert(b"d", b"4"), upsert(b"e", b"5")],
                ParallelConfigRecord {
                    max_threads: 1,
                    parallelism_threshold: 1,
                },
            )
            .unwrap();
        assert_eq!(
            engine.get(parallel, b"e".to_vec()).unwrap(),
            Some(b"5".to_vec())
        );
        let parallel_stats = engine
            .parallel_batch_with_stats(
                tree.clone(),
                vec![upsert(b"f", b"6"), upsert(b"g", b"7")],
                ParallelConfigRecord {
                    max_threads: 1,
                    parallelism_threshold: 1,
                },
            )
            .unwrap();
        assert_eq!(parallel_stats.stats.input_mutations, 2);
        assert_eq!(parallel_stats.stats.effective_mutations, 2);
        assert!(parallel_stats.stats.written_nodes > 0);
        assert_eq!(
            engine.get(parallel_stats.tree, b"g".to_vec()).unwrap(),
            Some(b"7".to_vec())
        );

        let base = engine.put(empty, b"k".to_vec(), b"base".to_vec()).unwrap();
        let left = engine
            .put(base.clone(), b"k".to_vec(), b"left".to_vec())
            .unwrap();
        let right = engine
            .put(base.clone(), b"k".to_vec(), b"right".to_vec())
            .unwrap();
        let explanation = engine
            .merge_explain(
                base.clone(),
                left.clone(),
                right.clone(),
                Some("prefer_right".to_string()),
            )
            .unwrap();
        assert!(explanation.result.is_some());
        assert!(explanation.error.is_none());
        assert!(explanation.trace_json.contains("events"));
        assert!(!explanation.trace.events.is_empty());
        assert!(explanation
            .trace
            .events
            .iter()
            .any(|event| event.kind == MergeTraceEventKind::ResolverCalled
                && event.resolution == Some(MergeTraceResolutionKind::Value)));
        let merged = engine
            .merge(base, left, right, Some("prefer_right".to_string()))
            .unwrap();
        assert_eq!(
            engine.get(merged.clone(), b"k".to_vec()).unwrap(),
            Some(b"right".to_vec())
        );

        engine
            .publish_named_root_at_millis(b"main".to_vec(), merged.clone(), 42)
            .unwrap();
        assert_eq!(
            engine.load_named_root(b"main".to_vec()).unwrap(),
            Some(merged.clone())
        );
        assert_eq!(engine.list_named_roots().unwrap().len(), 1);
        let manifests = engine.list_named_root_manifests().unwrap();
        assert_eq!(manifests.len(), 1);
        assert_eq!(manifests[0].name, b"main".to_vec());
        assert_eq!(manifests[0].manifest.tree, merged);
        assert_eq!(manifests[0].manifest.created_at_millis, Some(42));
        assert_eq!(manifests[0].manifest.updated_at_millis, Some(42));
        let retention = all_named_root_retention();
        let retained = engine.load_retained_named_roots(retention.clone()).unwrap();
        assert_eq!(retained.roots.len(), 1);
        assert_eq!(
            engine
                .plan_store_gc_for_retention(retention)
                .unwrap()
                .reachability
                .live_nodes,
            1
        );

        let update = engine
            .compare_and_swap_named_root(b"main".to_vec(), Some(merged), None)
            .unwrap();
        assert!(update.applied);
        assert!(!update.conflict);
        assert!(engine.load_named_root(b"main".to_vec()).unwrap().is_none());
    }

    #[test]
    fn snapshot_namespace_methods_work_through_binding_records() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let empty = engine.create();
        let main = engine
            .put(empty.clone(), b"k".to_vec(), b"main".to_vec())
            .unwrap();
        let feature = engine
            .put(main.clone(), b"k2".to_vec(), b"feature".to_vec())
            .unwrap();
        let branch = snapshot_namespace_branch();
        let tag = snapshot_namespace_tag();
        let custom = snapshot_namespace_custom(b"workspace/root/".to_vec());

        assert_eq!(
            snapshot_root_name(branch.clone(), b"main".to_vec()).unwrap(),
            b"refs/heads/main".to_vec()
        );
        assert_eq!(
            snapshot_id_from_name(branch.clone(), b"refs/heads/main".to_vec()).unwrap(),
            Some(b"main".to_vec())
        );
        assert_eq!(
            snapshot_root_name(custom.clone(), b"latest".to_vec()).unwrap(),
            b"workspace/root/latest".to_vec()
        );

        engine
            .publish_snapshot_at_millis(branch.clone(), b"main".to_vec(), main.clone(), 100)
            .unwrap();
        engine
            .publish_snapshot_at_millis(branch.clone(), b"feature".to_vec(), feature.clone(), 200)
            .unwrap();
        engine
            .publish_snapshot_at_millis(tag.clone(), b"v1".to_vec(), main.clone(), 300)
            .unwrap();

        assert_eq!(
            engine
                .load_snapshot(branch.clone(), b"main".to_vec())
                .unwrap(),
            Some(main.clone())
        );

        let listed = engine.list_snapshots(branch.clone()).unwrap();
        assert_eq!(
            listed
                .iter()
                .map(|snapshot| snapshot.id.clone())
                .collect::<Vec<_>>(),
            vec![b"feature".to_vec(), b"main".to_vec()]
        );
        assert_eq!(listed[0].updated_at_millis, Some(200));
        assert_eq!(engine.list_snapshots(tag.clone()).unwrap().len(), 1);

        let selection = engine
            .load_snapshots(branch.clone(), vec![b"main".to_vec(), b"missing".to_vec()])
            .unwrap();
        assert_eq!(selection.snapshots.len(), 1);
        assert_eq!(selection.snapshots[0].id, b"main".to_vec());
        assert_eq!(selection.missing_ids, vec![b"missing".to_vec()]);

        let conflict = engine
            .compare_and_swap_snapshot(
                branch.clone(),
                b"main".to_vec(),
                Some(feature.clone()),
                Some(empty.clone()),
            )
            .unwrap();
        assert!(conflict.conflict);
        assert_eq!(conflict.current, Some(main.clone()));

        let update = engine
            .compare_and_swap_snapshot_at_millis(
                branch.clone(),
                b"main".to_vec(),
                Some(main.clone()),
                Some(feature.clone()),
                400,
            )
            .unwrap();
        assert!(update.applied);
        assert_eq!(
            engine
                .load_snapshot(branch.clone(), b"main".to_vec())
                .unwrap(),
            Some(feature)
        );

        engine
            .delete_snapshot(branch.clone(), b"feature".to_vec())
            .unwrap();
        assert!(engine
            .load_snapshot(branch, b"feature".to_vec())
            .unwrap()
            .is_none());
    }

    #[test]
    fn custom_merge_resolver_callback_drives_merge_variants() {
        let engine = ProllyEngine::memory(default_config()).unwrap();
        let empty = engine.create();
        let base = engine.put(empty, b"k".to_vec(), b"base".to_vec()).unwrap();
        let left = engine
            .put(base.clone(), b"k".to_vec(), b"left".to_vec())
            .unwrap();
        let right = engine
            .put(base.clone(), b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let merged = engine
            .merge_with_resolver(
                base.clone(),
                left.clone(),
                right.clone(),
                Arc::new(JoinResolver),
            )
            .unwrap();
        assert_eq!(
            engine.get(merged.clone(), b"k".to_vec()).unwrap(),
            Some(b"left|right".to_vec())
        );

        let explanation = engine
            .merge_explain_with_resolver(
                base.clone(),
                left.clone(),
                right.clone(),
                Arc::new(JoinResolver),
            )
            .unwrap();
        assert!(explanation.result.is_some());
        assert!(explanation.error.is_none());
        assert!(explanation.trace_json.contains("ResolverCalled"));
        assert!(explanation
            .trace
            .events
            .iter()
            .any(|event| event.kind == MergeTraceEventKind::ResolverCalled
                && event.resolution == Some(MergeTraceResolutionKind::Value)));

        let ranged = engine
            .merge_range_with_resolver(
                base.clone(),
                left.clone(),
                right.clone(),
                b"k".to_vec(),
                Some(b"l".to_vec()),
                Arc::new(JoinResolver),
            )
            .unwrap();
        assert_eq!(
            engine.get(ranged, b"k".to_vec()).unwrap(),
            Some(b"left|right".to_vec())
        );

        let prefixed = engine
            .merge_prefix_with_resolver(base, left, right, b"k".to_vec(), Arc::new(JoinResolver))
            .unwrap();
        assert_eq!(
            engine.get(prefixed, b"k".to_vec()).unwrap(),
            Some(b"left|right".to_vec())
        );
    }

    #[test]
    fn merge_policy_registry_drives_merge_variants() {
        let engine = ProllyEngine::memory(default_config()).unwrap();
        let empty = engine.create();
        let base = engine
            .batch(
                empty,
                vec![
                    upsert(b"doc/title", b"base-doc"),
                    upsert(b"settings/theme", b"base-theme"),
                    upsert(b"z", b"base-z"),
                ],
            )
            .unwrap();
        let left = engine
            .batch(
                base.clone(),
                vec![
                    upsert(b"doc/title", b"left-doc"),
                    upsert(b"settings/theme", b"left-theme"),
                    upsert(b"z", b"left-z"),
                ],
            )
            .unwrap();
        let right = engine
            .batch(
                base.clone(),
                vec![
                    upsert(b"doc/title", b"right-doc"),
                    upsert(b"settings/theme", b"right-theme"),
                    upsert(b"z", b"right-z"),
                ],
            )
            .unwrap();

        let policy = Arc::new(MergePolicyRegistry::new());
        policy
            .set_default_resolver_name("prefer_left".to_string())
            .unwrap();
        policy
            .push_prefix_resolver(b"doc/".to_vec(), Arc::new(JoinResolver))
            .unwrap();
        policy
            .push_prefix_resolver_name(b"settings/".to_vec(), "prefer_left".to_string())
            .unwrap();
        policy
            .push_exact_resolver_name(b"settings/theme".to_vec(), "prefer_right".to_string())
            .unwrap();
        assert_eq!(policy.len().unwrap(), 3);
        assert!(!policy.is_empty().unwrap());
        assert!(policy.has_default().unwrap());

        let merged = engine
            .merge_with_policy(base.clone(), left.clone(), right.clone(), policy.clone())
            .unwrap();
        assert_eq!(
            engine.get(merged.clone(), b"doc/title".to_vec()).unwrap(),
            Some(b"left-doc|right-doc".to_vec())
        );
        assert_eq!(
            engine
                .get(merged.clone(), b"settings/theme".to_vec())
                .unwrap(),
            Some(b"right-theme".to_vec())
        );
        assert_eq!(
            engine.get(merged.clone(), b"z".to_vec()).unwrap(),
            Some(b"left-z".to_vec())
        );

        let explanation = engine
            .merge_explain_with_policy(base.clone(), left.clone(), right.clone(), policy.clone())
            .unwrap();
        assert!(explanation.result.is_some());
        assert!(explanation.error.is_none());

        let ranged = engine
            .merge_range_with_policy(
                base.clone(),
                left.clone(),
                right.clone(),
                b"doc/".to_vec(),
                Some(b"doc0".to_vec()),
                policy.clone(),
            )
            .unwrap();
        assert_eq!(
            engine.get(ranged, b"doc/title".to_vec()).unwrap(),
            Some(b"left-doc|right-doc".to_vec())
        );

        let prefixed = engine
            .merge_prefix_with_policy(base, left, right, b"settings/".to_vec(), policy)
            .unwrap();
        assert_eq!(
            engine.get(prefixed, b"settings/theme".to_vec()).unwrap(),
            Some(b"right-theme".to_vec())
        );
    }

    #[test]
    fn bulk_build_and_append_batch_use_rust_bulk_paths() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let unsorted_entries = vec![entry(b"c", b"3"), entry(b"a", b"1"), entry(b"b", b"2")];

        let built = engine
            .build_from_entries(unsorted_entries)
            .expect("unsorted build");
        assert_eq!(
            engine
                .range(built.clone(), Vec::new(), None)
                .unwrap()
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (b"a".to_vec(), b"1".to_vec()),
                (b"b".to_vec(), b"2".to_vec()),
                (b"c".to_vec(), b"3".to_vec())
            ]
        );

        let sorted_entries = vec![entry(b"a", b"1"), entry(b"b", b"2"), entry(b"c", b"3")];
        let sorted = engine
            .build_from_sorted_entries(sorted_entries)
            .expect("sorted build");
        assert_eq!(built.root, sorted.root);

        let sorted_error = engine
            .build_from_sorted_entries(vec![entry(b"b", b"2"), entry(b"a", b"1")])
            .unwrap_err();
        assert!(matches!(
            sorted_error,
            ProllyBindingError::InvalidArgument { .. }
        ));

        let batch_result = engine
            .batch_with_stats(
                engine.create(),
                vec![upsert(b"b", b"2"), upsert(b"a", b"1"), upsert(b"b", b"22")],
            )
            .expect("batch with stats");
        assert_eq!(
            engine
                .get(batch_result.tree.clone(), b"b".to_vec())
                .unwrap(),
            Some(b"22".to_vec())
        );
        assert_eq!(batch_result.stats.input_mutations, 3);
        assert_eq!(batch_result.stats.effective_mutations, 2);
        assert!(!batch_result.stats.preprocess_input_sorted);
        assert!(batch_result.stats.written_nodes > 0);

        let append_result = engine
            .append_batch_with_stats(
                built,
                vec![upsert(b"d", b"4"), upsert(b"e", b"5"), upsert(b"d", b"44")],
            )
            .expect("append batch with stats");
        let appended = append_result.tree;
        assert_eq!(append_result.stats.input_mutations, 3);
        assert_eq!(append_result.stats.effective_mutations, 2);
        assert!(!append_result.stats.preprocess_input_sorted);
        assert!(append_result.stats.used_append_fast_path);
        assert!(append_result.stats.written_nodes > 0);
        assert_eq!(
            engine.get(appended.clone(), b"d".to_vec()).unwrap(),
            Some(b"44".to_vec())
        );
        assert_eq!(
            engine
                .range(appended, b"d".to_vec(), None)
                .unwrap()
                .into_iter()
                .map(|entry| (entry.key, entry.value))
                .collect::<Vec<_>>(),
            vec![
                (b"d".to_vec(), b"44".to_vec()),
                (b"e".to_vec(), b"5".to_vec())
            ]
        );
    }

    #[test]
    fn conflict_page_reports_and_pages_merge_conflicts() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let empty = engine.create();
        let base = engine
            .batch(
                empty,
                vec![upsert(b"a", b"base-a"), upsert(b"b", b"base-b")],
            )
            .unwrap();
        let left = engine
            .batch(
                base.clone(),
                vec![upsert(b"a", b"left-a"), upsert(b"b", b"left-b")],
            )
            .unwrap();
        let right = engine
            .batch(
                base.clone(),
                vec![upsert(b"a", b"right-a"), upsert(b"b", b"right-b")],
            )
            .unwrap();

        let first = engine
            .conflict_page(base.clone(), left.clone(), right.clone(), None, 1)
            .unwrap();
        assert_eq!(first.conflicts.len(), 1);
        assert_eq!(first.conflicts[0].key, b"a".to_vec());
        assert_eq!(first.conflicts[0].base, Some(b"base-a".to_vec()));
        assert_eq!(first.conflicts[0].left, Some(b"left-a".to_vec()));
        assert_eq!(first.conflicts[0].right, Some(b"right-a".to_vec()));
        assert!(first.next_cursor.is_some());

        let second = engine
            .conflict_page(
                base.clone(),
                left.clone(),
                right.clone(),
                first.next_cursor,
                1,
            )
            .unwrap();
        assert_eq!(second.conflicts.len(), 1);
        assert_eq!(second.conflicts[0].key, b"b".to_vec());
        assert!(second.next_cursor.is_none());

        let all = engine.conflict_page(base, left, right, None, 10).unwrap();
        assert_eq!(all.conflicts.len(), 2);
        assert!(all.next_cursor.is_none());
    }

    #[test]
    fn root_manifest_bytes_round_trip() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let tree = engine
            .put(engine.create(), b"k".to_vec(), b"v".to_vec())
            .unwrap();
        let manifest = RootManifestRecord {
            tree,
            created_at_millis: Some(10),
            updated_at_millis: Some(20),
        };

        let bytes = root_manifest_to_bytes(manifest.clone()).unwrap();
        assert_eq!(root_manifest_from_bytes(bytes).unwrap(), manifest);
    }

    #[test]
    fn versioned_value_schema_guards_work() {
        let value = VersionedValueRecord {
            schema: "example".to_string(),
            version: 1,
            encoding: EncodingRecord {
                kind: EncodingKind::Raw,
                custom_name: None,
            },
            payload: b"payload".to_vec(),
        };
        let bytes = versioned_value_to_bytes(value.clone()).unwrap();

        assert!(versioned_value_matches_schema(value.clone(), "example".to_string(), 1).unwrap());
        assert!(!versioned_value_matches_schema(value.clone(), "example".to_string(), 2).unwrap());
        versioned_value_require_schema(value.clone(), "example".to_string(), 1).unwrap();
        assert!(versioned_value_require_schema(value, "other".to_string(), 1).is_err());

        assert!(
            versioned_value_bytes_matches_schema(bytes.clone(), "example".to_string(), 1).unwrap()
        );
        assert!(
            !versioned_value_bytes_matches_schema(bytes.clone(), "example".to_string(), 2).unwrap()
        );
        versioned_value_bytes_require_schema(bytes.clone(), "example".to_string(), 1).unwrap();
        assert!(versioned_value_bytes_require_schema(bytes, "other".to_string(), 1).is_err());
    }

    #[test]
    fn blob_store_and_large_value_helpers_work() {
        let engine = ProllyEngine::memory(small_config()).unwrap();
        let blob_store = Arc::new(ProllyBlobStore::memory());

        let direct_ref = blob_store.put_blob(b"direct".to_vec()).unwrap();
        blob_ref_validate_bytes(direct_ref.clone(), b"direct".to_vec()).unwrap();
        assert!(blob_ref_validate_bytes(direct_ref.clone(), b"wrong".to_vec()).is_err());
        assert_eq!(
            blob_store.get_blob(direct_ref.clone()).unwrap(),
            Some(b"direct".to_vec())
        );
        blob_store.delete_blob(direct_ref).unwrap();
        assert_eq!(blob_store.blob_count().unwrap(), 0);

        let tree = engine
            .put_large_value(
                blob_store.clone(),
                engine.create(),
                b"big".to_vec(),
                b"large payload".to_vec(),
                LargeValueConfigRecord {
                    inline_threshold: 4,
                },
            )
            .unwrap();
        let value_ref = engine
            .get_value_ref(tree.clone(), b"big".to_vec())
            .unwrap()
            .unwrap();
        assert_eq!(value_ref.kind, ValueRefKind::Blob);
        let raw_inline = value_ref_from_stored_bytes(b"plain".to_vec()).unwrap();
        assert_eq!(raw_inline.kind, ValueRefKind::Inline);
        assert_eq!(raw_inline.value, Some(b"plain".to_vec()));
        assert!(!value_ref_inline_requires_escape(b"plain".to_vec()));

        let stored_ref_bytes = value_ref_to_bytes(value_ref.clone()).unwrap();
        let stored_ref = value_ref_from_stored_bytes(stored_ref_bytes.clone()).unwrap();
        assert_eq!(stored_ref.kind, ValueRefKind::Blob);
        assert!(value_ref_inline_requires_escape(stored_ref_bytes));
        assert_eq!(
            engine
                .get_large_value(blob_store.clone(), tree.clone(), b"big".to_vec())
                .unwrap(),
            Some(b"large payload".to_vec())
        );

        let reachable = engine.mark_reachable_blobs(vec![tree.clone()]).unwrap();
        assert_eq!(reachable.live_blob_count, 1);
        assert_eq!(
            engine
                .plan_blob_gc(
                    blob_store.clone(),
                    vec![tree.clone()],
                    reachable.live_blobs.clone()
                )
                .unwrap()
                .reclaimable_blob_count,
            0
        );

        let orphan = blob_store.put_blob(b"orphan".to_vec()).unwrap();
        let candidates = blob_store.list_blob_refs().unwrap();
        assert!(candidates.contains(&orphan));
        let plan = engine
            .plan_blob_store_gc(blob_store.clone(), vec![tree.clone()])
            .unwrap();
        assert_eq!(plan.reclaimable_blob_count, 1);
        assert_eq!(
            engine
                .sweep_blob_store_gc(blob_store.clone(), vec![tree.clone()])
                .unwrap()
                .deleted_blobs,
            1
        );
        assert_eq!(blob_store.blob_count().unwrap(), 1);
    }

    #[test]
    fn file_blob_store_persists_blobs_across_reopen() {
        let path = temp_path("file-blob-store");
        let _ = std::fs::remove_dir_all(&path);

        let reference = {
            let blob_store = ProllyBlobStore::file(path.to_string_lossy().into_owned()).unwrap();
            blob_store.put_blob(b"blob-payload".to_vec()).unwrap()
        };

        let reopened = ProllyBlobStore::file(path.to_string_lossy().into_owned()).unwrap();
        assert_eq!(
            reopened.get_blob(reference).unwrap(),
            Some(b"blob-payload".to_vec())
        );

        let _ = std::fs::remove_dir_all(path);
    }

    #[test]
    fn custom_crdt_resolver_callback_drives_merge() {
        let engine = ProllyEngine::memory(default_config()).unwrap();
        let empty = engine.create();
        let base = engine
            .put(empty.clone(), b"k".to_vec(), b"base".to_vec())
            .unwrap();
        let left = engine
            .put(base.clone(), b"k".to_vec(), b"left".to_vec())
            .unwrap();
        let right = engine
            .put(base.clone(), b"k".to_vec(), b"right".to_vec())
            .unwrap();

        let merged = engine
            .crdt_merge_with_resolver(
                base.clone(),
                left,
                right,
                CrdtDeletePolicyKind::UpdateWins,
                Arc::new(CrdtJoinResolver),
            )
            .unwrap();
        assert_eq!(
            engine.get(merged, b"k".to_vec()).unwrap(),
            Some(b"left|right".to_vec())
        );

        let delete_left = engine.delete(base.clone(), b"k".to_vec()).unwrap();
        let update_right = engine
            .put(base.clone(), b"k".to_vec(), b"right".to_vec())
            .unwrap();
        let deleted = engine
            .crdt_merge_with_resolver(
                base,
                delete_left,
                update_right,
                CrdtDeletePolicyKind::UpdateWins,
                Arc::new(CrdtDeleteResolver),
            )
            .unwrap();
        assert_eq!(engine.get(deleted, b"k".to_vec()).unwrap(), None);
    }

    #[test]
    fn inspection_sync_gc_crdt_and_tombstone_helpers_work() {
        let engine = Arc::new(ProllyEngine::memory(small_config()).unwrap());
        let empty = engine.create();

        let base_value = timestamped_value_to_bytes(TimestampedValueRecord {
            value: b"base".to_vec(),
            timestamp: 1,
        });
        let left_value = timestamped_value_to_bytes(TimestampedValueRecord {
            value: b"left".to_vec(),
            timestamp: 2,
        });
        let right_value = timestamped_value_to_bytes(TimestampedValueRecord {
            value: b"right".to_vec(),
            timestamp: 3,
        });

        let base = engine
            .put(empty.clone(), b"k".to_vec(), base_value)
            .unwrap();
        let left = engine.put(base.clone(), b"k".to_vec(), left_value).unwrap();
        let right = engine
            .put(base.clone(), b"k".to_vec(), right_value)
            .unwrap();
        let merged = engine
            .crdt_merge(
                base.clone(),
                left,
                right,
                crdt_config_lww(CrdtDeletePolicyKind::UpdateWins),
            )
            .unwrap();

        let merged_value = engine.get(merged.clone(), b"k".to_vec()).unwrap().unwrap();
        assert_eq!(
            timestamped_value_from_bytes(merged_value).unwrap(),
            TimestampedValueRecord {
                value: b"right".to_vec(),
                timestamp: 3,
            }
        );

        let structural_page = engine
            .structural_diff_page(empty.clone(), merged.clone(), None, 1)
            .unwrap();
        assert_eq!(structural_page.diffs.len(), 1);
        assert_eq!(structural_page.stats.emitted_diffs, 1);
        let cursor_page = engine
            .structural_diff_page(empty.clone(), merged.clone(), None, 0)
            .unwrap();
        assert!(cursor_page.next_cursor.is_some());
        assert!(cursor_page.next_cursor_json.is_some());
        let resumed_page = engine
            .structural_diff_page_with_cursor(
                empty.clone(),
                merged.clone(),
                cursor_page.next_cursor.clone(),
                1,
            )
            .unwrap();
        assert_eq!(resumed_page.diffs.len(), 1);
        assert!(resumed_page.next_cursor.is_none());
        assert_eq!(
            engine
                .range_diff(
                    empty.clone(),
                    merged.clone(),
                    b"k".to_vec(),
                    Some(b"l".to_vec())
                )
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            engine
                .get_value_ref(merged.clone(), b"k".to_vec())
                .unwrap()
                .unwrap()
                .kind,
            ValueRefKind::Inline
        );

        let stats = engine.collect_stats_json(merged.clone()).unwrap();
        assert!(stats.json.contains("\"num_nodes\""));
        let typed_stats = engine.collect_stats(merged.clone()).unwrap();
        assert!(typed_stats.num_nodes > 0);
        assert_eq!(typed_stats.total_key_value_pairs, 1);
        assert!(typed_stats
            .nodes_per_level
            .iter()
            .any(|entry| entry.level == 0));

        let diff_stats = engine
            .stats_diff_json(empty.clone(), merged.clone())
            .unwrap();
        assert!(diff_stats.json.contains("\"absolute\""));
        let typed_diff_stats = engine.stats_diff(empty.clone(), merged.clone()).unwrap();
        assert_eq!(typed_diff_stats.before.total_key_value_pairs, 0);
        assert_eq!(typed_diff_stats.after.total_key_value_pairs, 1);
        assert_eq!(typed_diff_stats.absolute.total_key_value_pairs_diff, 1);
        assert!(typed_diff_stats.percentage.total_key_value_pairs_pct >= 0.0);
        assert!(engine
            .debug_tree_text(merged.clone())
            .unwrap()
            .contains("level"));
        let debug_tree = engine.debug_tree(merged.clone()).unwrap();
        assert!(!debug_tree.levels.is_empty());
        assert!(debug_tree
            .levels
            .iter()
            .flat_map(|level| level.nodes.iter())
            .any(|node| node.leaf && node.entry_count > 0));
        assert!(engine
            .debug_compare_trees_json(empty.clone(), merged.clone())
            .unwrap()
            .json
            .contains("\"right_only_nodes\""));
        let debug_comparison = engine
            .debug_compare_trees(empty.clone(), merged.clone())
            .unwrap();
        assert_eq!(debug_comparison.left_only_nodes, 0);
        assert!(debug_comparison.right_only_nodes > 0);
        assert!(debug_comparison
            .levels
            .iter()
            .flat_map(|level| level.nodes.iter())
            .any(|node| node.status == TreeDebugNodeStatusKind::RightOnly));

        let reachability = engine.mark_reachable(vec![merged.clone()]).unwrap();
        assert!(reachability.live_nodes > 0);
        let live_cids = reachability.live_cids.clone();
        let node_cids = engine.list_node_cids().unwrap();
        assert!(!node_cids.is_empty());
        let gc_plan = engine
            .plan_gc(vec![merged.clone()], live_cids.clone())
            .unwrap();
        assert_eq!(gc_plan.reclaimable_nodes, 0);
        assert_eq!(
            engine
                .sweep_gc(vec![merged.clone()], live_cids)
                .unwrap()
                .deleted_nodes,
            0
        );
        let store_gc_plan = engine.plan_store_gc(vec![merged.clone()]).unwrap();
        assert!(store_gc_plan.candidate_nodes >= reachability.live_nodes);
        assert_eq!(
            engine
                .sweep_store_gc(vec![merged.clone()])
                .unwrap()
                .deleted_nodes,
            store_gc_plan.reclaimable_nodes
        );

        let destination = Arc::new(ProllyEngine::memory(small_config()).unwrap());
        let missing = engine
            .plan_missing_nodes(merged.clone(), destination.clone())
            .unwrap();
        assert!(missing.missing_nodes > 0);
        let copied = engine
            .copy_missing_nodes(merged.clone(), destination.clone())
            .unwrap();
        assert_eq!(copied.copied_nodes, missing.missing_nodes);
        assert_eq!(
            destination.get(merged.clone(), b"k".to_vec()).unwrap(),
            engine.get(merged.clone(), b"k".to_vec()).unwrap()
        );
        let snapshot_bundle = engine.export_snapshot(merged.clone()).unwrap();
        assert_eq!(snapshot_bundle.format_version, 1);
        assert_eq!(snapshot_bundle.tree, merged);
        assert_eq!(snapshot_bundle.nodes.len() as u64, reachability.live_nodes);
        let snapshot_bundle_bytes = snapshot_bundle_to_bytes(snapshot_bundle.clone()).unwrap();
        let snapshot_digest = snapshot_bundle_digest(snapshot_bundle.clone()).unwrap();
        assert_eq!(
            snapshot_digest,
            cid_from_bytes(snapshot_bundle_bytes.clone())
        );
        assert_eq!(
            snapshot_bundle_digest_bytes(snapshot_bundle_bytes.clone()).unwrap(),
            snapshot_digest
        );
        let snapshot_summary = snapshot_bundle_summary(snapshot_bundle.clone()).unwrap();
        assert_eq!(snapshot_summary.format_version, 1);
        assert_eq!(snapshot_summary.root, snapshot_bundle.tree.root);
        assert_eq!(snapshot_summary.node_count, reachability.live_nodes);
        assert_eq!(snapshot_summary.byte_count, reachability.live_bytes);
        assert_eq!(
            snapshot_bundle_summary_from_bytes(snapshot_bundle_bytes.clone()).unwrap(),
            snapshot_summary
        );
        let snapshot_verification = verify_snapshot_bundle(snapshot_bundle.clone()).unwrap();
        assert!(snapshot_verification.valid);
        assert_eq!(snapshot_verification.summary, snapshot_summary);
        assert!(snapshot_verification.missing_cids.is_empty());
        assert!(snapshot_verification.extra_cids.is_empty());
        assert_eq!(
            verify_snapshot_bundle_bytes(snapshot_bundle_bytes.clone()).unwrap(),
            snapshot_verification
        );
        assert_eq!(
            snapshot_bundle_from_bytes(snapshot_bundle_bytes.clone()).unwrap(),
            snapshot_bundle
        );
        let mut incomplete_snapshot_bundle = snapshot_bundle.clone();
        incomplete_snapshot_bundle.nodes.pop();
        let incomplete_snapshot_verification =
            verify_snapshot_bundle(incomplete_snapshot_bundle).unwrap();
        assert!(!incomplete_snapshot_verification.valid);
        assert!(!incomplete_snapshot_verification.missing_cids.is_empty());
        let imported_destination = Arc::new(ProllyEngine::memory(small_config()).unwrap());
        let imported_tree = imported_destination
            .import_snapshot(snapshot_bundle_from_bytes(snapshot_bundle_bytes).unwrap())
            .unwrap();
        assert_eq!(
            imported_destination
                .get(imported_tree, b"k".to_vec())
                .unwrap(),
            engine.get(merged.clone(), b"k".to_vec()).unwrap()
        );

        let _ = engine.pin_tree_root(merged.clone()).unwrap();
        let _ = engine.pin_tree_path(merged.clone(), b"k".to_vec()).unwrap();
        assert!(engine.cache_stats().unwrap().cached_nodes > 0);
        let _ = engine.unpin_all_cache_nodes().unwrap();
        assert!(engine.metrics().nodes_written > 0);
        engine.reset_metrics();
        assert_eq!(engine.metrics().nodes_written, 0);
        assert!(!engine
            .publish_prefix_path_hint(merged.clone(), b"k".to_vec())
            .unwrap());
        assert!(!engine
            .hydrate_prefix_path_hint(merged.clone(), b"k".to_vec())
            .unwrap());
        assert!(!engine
            .publish_changed_spans_hint(
                empty.clone(),
                merged.clone(),
                vec![ChangedSpanRecord {
                    start: b"k".to_vec(),
                    end: Some(b"l".to_vec()),
                }],
            )
            .unwrap());
        assert!(engine
            .load_changed_spans_hint(empty.clone(), merged.clone())
            .unwrap()
            .is_none());

        let tombstone = TombstoneRecord {
            actor: b"actor-1".to_vec(),
            timestamp_millis: 123,
            causal_metadata: vec![TombstoneMetadataRecord {
                key: "clock".to_string(),
                value: b"7".to_vec(),
            }],
        };
        let tombstone_bytes = tombstone_to_bytes(tombstone.clone()).unwrap();
        assert!(is_tombstone_value(tombstone_bytes.clone()));
        assert_eq!(
            tombstone_from_stored_bytes(tombstone_bytes.clone()).unwrap(),
            Some(tombstone.clone())
        );
        assert_eq!(
            tombstone_from_bytes(tombstone_bytes.clone()).unwrap(),
            tombstone
        );
        assert_eq!(
            tombstone_upsert_mutation(b"deleted".to_vec(), tombstone.clone())
                .unwrap()
                .kind,
            MutationKind::Upsert
        );
        assert_eq!(
            tombstone_compaction_mutation(b"deleted".to_vec(), tombstone_bytes)
                .unwrap()
                .unwrap()
                .kind,
            MutationKind::Delete
        );

        assert_eq!(
            multi_value_set_from_bytes(multi_value_set_to_bytes(vec![
                b"b".to_vec(),
                b"a".to_vec(),
                b"a".to_vec(),
            ]))
            .unwrap(),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
        assert_eq!(
            multi_value_set_merge(vec![b"b".to_vec()], vec![b"a".to_vec(), b"b".to_vec()]),
            vec![b"a".to_vec(), b"b".to_vec()]
        );
    }

    #[cfg(feature = "sqlite")]
    #[test]
    fn sqlite_engine_persists_nodes_across_reopen() {
        let path = temp_path("sqlite-engine");
        let _ = std::fs::remove_file(&path);

        let tree = {
            let engine =
                ProllyEngine::sqlite(path.to_string_lossy().into_owned(), small_config()).unwrap();
            let tree = engine.create();
            engine.put(tree, b"k".to_vec(), b"v".to_vec()).unwrap()
        };

        let reopened =
            ProllyEngine::sqlite(path.to_string_lossy().into_owned(), small_config()).unwrap();
        assert_eq!(
            reopened.get(tree, b"k".to_vec()).unwrap(),
            Some(b"v".to_vec())
        );

        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(path.with_extension("db-wal"));
        let _ = std::fs::remove_file(path.with_extension("db-shm"));
    }

    fn small_config() -> ConfigRecord {
        ConfigRecord {
            min_chunk_size: 2,
            max_chunk_size: 4,
            chunking_factor: 2,
            hash_seed: 0,
            encoding: EncodingRecord {
                kind: EncodingKind::Raw,
                custom_name: None,
            },
            node_cache_max_nodes: None,
            node_cache_max_bytes: None,
        }
    }

    fn temp_path(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or_default();
        std::env::temp_dir().join(format!(
            "prolly-bindings-{label}-{}-{nanos}.db",
            std::process::id()
        ))
    }

    fn upsert(key: &[u8], value: &[u8]) -> MutationRecord {
        MutationRecord {
            kind: MutationKind::Upsert,
            key: key.to_vec(),
            value: Some(value.to_vec()),
        }
    }

    fn entry(key: &[u8], value: &[u8]) -> EntryRecord {
        EntryRecord {
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    fn all_named_root_retention() -> NamedRootRetentionRecord {
        NamedRootRetentionRecord {
            kind: NamedRootRetentionKind::All,
            names: Vec::new(),
            prefix: Vec::new(),
            count: None,
            min_updated_at_millis: None,
        }
    }
}
