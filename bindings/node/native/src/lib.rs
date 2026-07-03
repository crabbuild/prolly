use napi::bindgen_prelude::{
    Buffer, Env, Error, FromNapiValue, FunctionRef, JsValuesTupleIntoVec, Result, Status,
};
use napi::JsUnknown;
use napi_derive::napi;
use prolly_bindings::{
    authenticated_proof_envelope_from_bytes, authenticated_proof_envelope_to_bytes,
    blob_ref_validate_bytes, changed_span, changed_span_for_prefix, changed_span_from_key,
    cid_from_bytes, crdt_config_lww, crdt_config_multi_value, crdt_resolution_delete,
    crdt_resolution_value, debug_key, decode_segments, default_config, default_large_value_config,
    default_parallel_config, delete_mutation, diff_page_proof_from_bytes, diff_page_proof_to_bytes,
    encode_segment, encoding_cbor, encoding_custom, encoding_json, encoding_raw, i128_key, i64_key,
    inspect_proof_bundle, is_boundary_config, is_tombstone_value, key_from_prefixed_segments,
    key_from_segments, key_proof_from_bytes, key_proof_from_node_bytes, key_proof_path_node_bytes,
    key_proof_to_bytes, large_value_config, multi_key_proof_from_bytes,
    multi_key_proof_from_node_bytes, multi_key_proof_path_node_bytes, multi_key_proof_to_bytes,
    multi_value_set_from_bytes, multi_value_set_merge, multi_value_set_to_bytes, node_cid,
    node_from_bytes, node_to_bytes, parallel_config, parallel_config_sequential, prefix_end,
    prefix_range, range_cursor_after_key, range_cursor_start, range_page_proof_from_bytes,
    range_page_proof_from_node_bytes, range_page_proof_path_node_bytes, range_page_proof_to_bytes,
    range_proof_from_bytes, range_proof_from_node_bytes, range_proof_path_node_bytes,
    range_proof_to_bytes, resolution_delete, resolution_unresolved, resolution_value,
    resolve_delete_wins, resolve_prefer_left, resolve_prefer_right, resolve_update_wins,
    retain_all_named_roots, retain_exact_named_roots, retain_named_root_prefix,
    retain_named_roots_updated_since, retain_newest_named_roots, reverse_cursor_before_key,
    reverse_cursor_end, root_manifest_from_bytes, root_manifest_to_bytes,
    sign_proof_bundle_hmac_sha256, snapshot_bundle_digest, snapshot_bundle_digest_bytes,
    snapshot_bundle_from_bytes, snapshot_bundle_summary, snapshot_bundle_summary_from_bytes,
    snapshot_bundle_to_bytes, snapshot_id_from_name, snapshot_namespace_branch,
    snapshot_namespace_checkpoint, snapshot_namespace_custom, snapshot_namespace_tag,
    snapshot_root_name, timestamp_millis_key, timestamped_value_from_bytes, timestamped_value_now,
    timestamped_value_to_bytes, tombstone_compaction_mutation, tombstone_from_bytes,
    tombstone_from_stored_bytes, tombstone_to_bytes, tombstone_upsert_mutation, tree_config,
    u128_key, u64_key, upsert_mutation, value_ref_from_bytes, value_ref_from_stored_bytes,
    value_ref_inline_requires_escape, value_ref_to_bytes, verify_authenticated_proof_bundle,
    verify_authenticated_proof_envelope, verify_diff_page_proof, verify_key_proof,
    verify_multi_key_proof, verify_proof_bundle, verify_range_page_proof, verify_range_proof,
    verify_snapshot_bundle, verify_snapshot_bundle_bytes, versioned_value_bytes_matches_schema,
    versioned_value_bytes_require_schema, versioned_value_from_bytes, versioned_value_to_bytes,
    AuthenticatedProofBundleVerificationRecord as BindingAuthenticatedProofBundleVerificationRecord,
    AuthenticatedProofEnvelopeRecord as BindingAuthenticatedProofEnvelopeRecord,
    AuthenticatedProofEnvelopeVerificationRecord as BindingAuthenticatedProofEnvelopeVerificationRecord,
    BatchApplyResultRecord as BindingBatchApplyResultRecord,
    BatchApplyStatsRecord as BindingBatchApplyStatsRecord,
    BlobGcPlanRecord as BindingBlobGcPlanRecord,
    BlobGcReachabilityRecord as BindingBlobGcReachabilityRecord,
    BlobGcSweepRecord as BindingBlobGcSweepRecord, BlobRefRecord as BindingBlobRefRecord,
    CacheStatsRecord as BindingCacheStatsRecord,
    ChangedSpanHintRecord as BindingChangedSpanHintRecord,
    ChangedSpanRecord as BindingChangedSpanRecord, ConfigRecord,
    ConflictPageRecord as BindingConflictPageRecord, ConflictRecord as BindingConflictRecord,
    CrdtConfigRecord as BindingCrdtConfigRecord, CrdtDeletePolicyKind, CrdtMergeStrategyKind,
    CrdtResolutionKind as BindingCrdtResolutionKind,
    CrdtResolutionRecord as BindingCrdtResolutionRecord,
    CursorWindowRecord as BindingCursorWindowRecord, DiffKind,
    DiffPageProofRecord as BindingDiffPageProofRecord,
    DiffPageProofVerificationRecord as BindingDiffPageProofVerificationRecord,
    DiffPageRecord as BindingDiffPageRecord, DiffRecord as BindingDiffRecord,
    DiffTraversalStatsRecord as BindingDiffTraversalStatsRecord, EncodingKind, EncodingRecord,
    EntryRecord as BindingEntryRecord, GcPlanRecord as BindingGcPlanRecord,
    GcReachabilityRecord as BindingGcReachabilityRecord, GcSweepRecord as BindingGcSweepRecord,
    HostStoreBatchGetResultRecord as BindingHostStoreBatchGetResultRecord,
    HostStoreBoolResultRecord as BindingHostStoreBoolResultRecord,
    HostStoreBytesResultRecord as BindingHostStoreBytesResultRecord,
    HostStoreCallback as BindingHostStoreCallback,
    HostStoreListBytesResultRecord as BindingHostStoreListBytesResultRecord,
    HostStoreListRootsResultRecord as BindingHostStoreListRootsResultRecord,
    HostStoreNamedRootManifestRecord as BindingHostStoreNamedRootManifestRecord,
    HostStoreRootCasResultRecord as BindingHostStoreRootCasResultRecord,
    HostStoreRootResultRecord as BindingHostStoreRootResultRecord,
    HostStoreUnitResultRecord as BindingHostStoreUnitResultRecord,
    KeyProofRecord as BindingKeyProofRecord,
    KeyProofVerificationRecord as BindingKeyProofVerificationRecord,
    LargeValueConfigRecord as BindingLargeValueConfigRecord,
    MergeExplanationRecord as BindingMergeExplanationRecord,
    MergeFallbackReasonKind as BindingMergeFallbackReasonKind,
    MergeFastPathKind as BindingMergeFastPathKind,
    MergePolicyRegistry as BindingMergePolicyRegistry,
    MergeReuseReasonKind as BindingMergeReuseReasonKind,
    MergeTraceEventKind as BindingMergeTraceEventKind,
    MergeTraceEventRecord as BindingMergeTraceEventRecord,
    MergeTraceRecord as BindingMergeTraceRecord,
    MergeTraceResolutionKind as BindingMergeTraceResolutionKind,
    MergeTraceStageKind as BindingMergeTraceStageKind, MetricsRecord as BindingMetricsRecord,
    MissingNodeCopyRecord as BindingMissingNodeCopyRecord,
    MissingNodePlanRecord as BindingMissingNodePlanRecord,
    MultiKeyProofRecord as BindingMultiKeyProofRecord,
    MultiKeyProofVerificationRecord as BindingMultiKeyProofVerificationRecord, MutationKind,
    MutationRecord, NamedRootManifestRecord as BindingNamedRootManifestRecord,
    NamedRootRecord as BindingNamedRootRecord, NamedRootRetentionKind, NamedRootRetentionRecord,
    NamedRootSelectionRecord as BindingNamedRootSelectionRecord,
    NamedRootUpdateRecord as BindingNamedRootUpdateRecord,
    ParallelConfigRecord as BindingParallelConfigRecord, ProllyBindingError, ProllyBlobStore,
    ProllyEngine, ProofBundleSummaryRecord as BindingProofBundleSummaryRecord,
    ProofBundleVerificationRecord as BindingProofBundleVerificationRecord,
    ProvedDiffPageRecord as BindingProvedDiffPageRecord,
    ProvedRangePageRecord as BindingProvedRangePageRecord,
    RangeBoundsRecord as BindingRangeBoundsRecord, RangeCursorRecord,
    RangePageProofRecord as BindingRangePageProofRecord,
    RangePageProofVerificationRecord as BindingRangePageProofVerificationRecord,
    RangePageRecord as BindingRangePageRecord, RangeProofRecord as BindingRangeProofRecord,
    RangeProofVerificationRecord as BindingRangeProofVerificationRecord,
    ResolutionKind as BindingResolutionKind, ResolutionRecord as BindingResolutionRecord,
    ReverseCursorRecord, ReversePageRecord as BindingReversePageRecord,
    RootManifestRecord as BindingRootManifestRecord,
    SnapshotBundleNodeRecord as BindingSnapshotBundleNodeRecord,
    SnapshotBundleRecord as BindingSnapshotBundleRecord,
    SnapshotBundleSummaryRecord as BindingSnapshotBundleSummaryRecord,
    SnapshotBundleVerificationRecord as BindingSnapshotBundleVerificationRecord,
    SnapshotNamespaceKind, SnapshotNamespaceRecord, SnapshotRecord as BindingSnapshotRecord,
    SnapshotSelectionRecord as BindingSnapshotSelectionRecord,
    StructuralDiffCursorRecord as BindingStructuralDiffCursorRecord,
    StructuralDiffMarkerKind as BindingStructuralDiffMarkerKind,
    StructuralDiffMarkerRecord as BindingStructuralDiffMarkerRecord,
    StructuralDiffPageRecord as BindingStructuralDiffPageRecord,
    TimestampedValueRecord as BindingTimestampedValueRecord,
    TombstoneMetadataRecord as BindingTombstoneMetadataRecord,
    TombstoneRecord as BindingTombstoneRecord, TreeRecord, ValueRefKind,
    ValueRefRecord as BindingValueRefRecord,
};
use serde::Deserialize;
use std::sync::{Arc, Mutex};

#[napi(object)]
pub struct NodeTreeRecord {
    pub root: Option<Buffer>,
    pub config: Option<NodeConfigRecord>,
}

#[napi(object)]
pub struct NodeEntryRecord {
    pub key: Buffer,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodeDiffRecord {
    pub kind: String,
    pub key: Buffer,
    pub value: Option<Buffer>,
    pub old: Option<Buffer>,
    pub new_value: Option<Buffer>,
}

#[napi(object)]
pub struct NodeMutationRecord {
    pub kind: String,
    pub key: Buffer,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodeEncodingRecord {
    pub kind: String,
    pub custom_name: Option<String>,
}

#[napi(object)]
pub struct NodeConfigRecord {
    pub min_chunk_size: String,
    pub max_chunk_size: String,
    pub chunking_factor: u32,
    pub hash_seed: String,
    pub encoding: NodeEncodingRecord,
    pub node_cache_max_nodes: Option<String>,
    pub node_cache_max_bytes: Option<String>,
}

#[napi(object)]
pub struct NodeParallelConfigRecord {
    pub max_threads: String,
    pub parallelism_threshold: String,
}

#[napi(object)]
pub struct NodeSnapshotBundleNodeRecord {
    pub cid: Buffer,
    pub bytes: Buffer,
}

#[napi(object)]
pub struct NodeSnapshotBundleRecord {
    pub format_version: u32,
    pub tree: NodeTreeRecord,
    pub nodes: Vec<NodeSnapshotBundleNodeRecord>,
}

#[napi(object)]
pub struct NodeSnapshotBundleSummaryRecord {
    pub format_version: u32,
    pub root: Option<Buffer>,
    pub node_count: String,
    pub byte_count: String,
    pub min_node_bytes: String,
    pub max_node_bytes: String,
}

#[napi(object)]
pub struct NodeSnapshotBundleVerificationRecord {
    pub valid: bool,
    pub summary: NodeSnapshotBundleSummaryRecord,
    pub reachable_nodes: String,
    pub reachable_bytes: String,
    pub missing_cids: Vec<Buffer>,
    pub extra_cids: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeBatchApplyStatsRecord {
    pub input_mutations: String,
    pub effective_mutations: String,
    pub preprocess_input_sorted: bool,
    pub affected_leaves: String,
    pub changed_leaves: String,
    pub sparse_leaf_applies: String,
    pub written_nodes: String,
    pub written_bytes: String,
    pub used_append_fast_path: bool,
    pub used_batched_route: bool,
    pub used_coalesced_rebuild: bool,
    pub used_deferred_rebalancing: bool,
    pub used_bottom_up_rebuild: bool,
    pub cache_written_nodes: bool,
}

#[napi(object)]
pub struct NodeBatchApplyResultRecord {
    pub tree: NodeTreeRecord,
    pub stats: NodeBatchApplyStatsRecord,
}

#[napi(object)]
pub struct NodeHostStoreEmptyRequest {}

#[napi(object)]
pub struct NodeHostStoreKeyRequest {
    pub key: Buffer,
}

#[napi(object)]
pub struct NodeHostStorePutRequest {
    pub key: Buffer,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodeHostStoreBatchRequest {
    pub ops: Vec<NodeMutationRecord>,
}

#[napi(object)]
pub struct NodeHostStoreBatchGetRequest {
    pub keys: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeHostStoreHintRequest {
    pub namespace: Buffer,
    pub key: Buffer,
}

#[napi(object)]
pub struct NodeHostStorePutHintRequest {
    pub namespace: Buffer,
    pub key: Buffer,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodeHostStoreRootRequest {
    pub name: Buffer,
}

#[napi(object)]
pub struct NodeHostStorePutRootRequest {
    pub name: Buffer,
    pub manifest: Buffer,
}

#[napi(object)]
pub struct NodeHostStoreCasRootRequest {
    pub name: Buffer,
    pub expected: Option<Buffer>,
    pub replacement: Option<Buffer>,
}

#[napi(object)]
pub struct NodeHostStoreBytesResult {
    pub value: Option<Buffer>,
    pub ok: bool,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreUnitResult {
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreBoolResult {
    pub value: bool,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreBatchGetResult {
    pub values: Vec<NodeHostStoreBytesResult>,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreListBytesResult {
    pub values: Vec<Buffer>,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreRootResult {
    pub value: Option<Buffer>,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreNamedRootManifest {
    pub name: Buffer,
    pub manifest: Buffer,
}

#[napi(object)]
pub struct NodeHostStoreListRootsResult {
    pub values: Vec<NodeHostStoreNamedRootManifest>,
    pub error: Option<String>,
}

#[napi(object)]
pub struct NodeHostStoreCasResult {
    pub applied: bool,
    pub current: Option<Buffer>,
    pub error: Option<String>,
}

pub struct NodeHostStoreCallbacks {
    pub get: FunctionRef<NodeHostStoreKeyRequest, NodeHostStoreBytesResult>,
    pub put: FunctionRef<NodeHostStorePutRequest, NodeHostStoreUnitResult>,
    pub delete: FunctionRef<NodeHostStoreKeyRequest, NodeHostStoreUnitResult>,
    pub batch: FunctionRef<NodeHostStoreBatchRequest, NodeHostStoreUnitResult>,
    pub batch_get_ordered: FunctionRef<NodeHostStoreBatchGetRequest, NodeHostStoreBatchGetResult>,
    pub prefers_batch_reads: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreBoolResult>,
    pub supports_hints: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreBoolResult>,
    pub get_hint: FunctionRef<NodeHostStoreHintRequest, NodeHostStoreBytesResult>,
    pub put_hint: FunctionRef<NodeHostStorePutHintRequest, NodeHostStoreUnitResult>,
    pub list_node_cids: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreListBytesResult>,
    pub get_root: FunctionRef<NodeHostStoreRootRequest, NodeHostStoreRootResult>,
    pub put_root: FunctionRef<NodeHostStorePutRootRequest, NodeHostStoreUnitResult>,
    pub delete_root: FunctionRef<NodeHostStoreRootRequest, NodeHostStoreUnitResult>,
    pub compare_and_swap_root: FunctionRef<NodeHostStoreCasRootRequest, NodeHostStoreCasResult>,
    pub list_roots: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreListRootsResult>,
}

#[napi(object)]
pub struct NodeRangeCursorRecord {
    pub after_key: Option<Buffer>,
}

#[napi(object)]
pub struct NodeReverseCursorRecord {
    pub before_key: Option<Buffer>,
}

#[napi(object)]
pub struct NodeRangeBoundsRecord {
    pub start: Buffer,
    pub end: Option<Buffer>,
}

#[napi(object)]
pub struct NodeRangePageRecord {
    pub entries: Vec<NodeEntryRecord>,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeReversePageRecord {
    pub entries: Vec<NodeEntryRecord>,
    pub next_cursor: Option<NodeReverseCursorRecord>,
}

#[napi(object)]
pub struct NodeCursorWindowRecord {
    pub position_key: Option<Buffer>,
    pub position_value: Option<Buffer>,
    pub found: bool,
    pub entries: Vec<NodeEntryRecord>,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeDiffPageRecord {
    pub diffs: Vec<NodeDiffRecord>,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeConflictRecord {
    pub key: Buffer,
    pub base: Option<Buffer>,
    pub left: Option<Buffer>,
    pub right: Option<Buffer>,
}

#[napi(object)]
pub struct NodeResolutionRecord {
    pub kind: String,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodeCrdtResolutionRecord {
    pub kind: String,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodeConflictPageRecord {
    pub conflicts: Vec<NodeConflictRecord>,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeDiffTraversalStatsRecord {
    pub compared_nodes: String,
    pub reused_subtrees: String,
    pub added_subtrees: String,
    pub removed_subtrees: String,
    pub collected_fallbacks: String,
    pub emitted_diffs: String,
}

#[napi(object)]
pub struct NodeStructuralDiffPageRecord {
    pub diffs: Vec<NodeDiffRecord>,
    pub next_cursor_json: Option<String>,
    pub stats: NodeDiffTraversalStatsRecord,
    pub next_cursor: Option<NodeStructuralDiffCursorRecord>,
}

#[napi(object)]
pub struct NodeStructuralDiffCursorRecord {
    pub base_root: Option<Buffer>,
    pub other_root: Option<Buffer>,
    pub markers: Vec<NodeStructuralDiffMarkerRecord>,
    pub pending: Vec<NodeDiffRecord>,
}

#[napi(object)]
pub struct NodeStructuralDiffMarkerRecord {
    pub kind: String,
    pub base_cid: Option<Buffer>,
    pub other_cid: Option<Buffer>,
    pub span_end: Option<Buffer>,
    pub cid: Option<Buffer>,
}

#[napi(object)]
pub struct NodeMergeTraceRecord {
    pub events: Vec<NodeMergeTraceEventRecord>,
}

#[napi(object)]
pub struct NodeMergeTraceEventRecord {
    pub kind: String,
    pub fast_path: Option<String>,
    pub cid: Option<Buffer>,
    pub reuse_reason: Option<String>,
    pub level: Option<String>,
    pub entries: Option<String>,
    pub first_key: Option<Buffer>,
    pub last_key: Option<Buffer>,
    pub stage: Option<String>,
    pub key: Option<Buffer>,
    pub resolution: Option<String>,
    pub fallback_reason: Option<String>,
    pub diff_stats: Option<NodeDiffTraversalStatsRecord>,
    pub right_changes: Option<String>,
    pub mutations: Option<String>,
    pub append_only: Option<bool>,
}

#[napi(object)]
pub struct NodeMergeExplanationRecord {
    pub result: Option<NodeTreeRecord>,
    pub error: Option<String>,
    pub trace_json: String,
    pub trace: NodeMergeTraceRecord,
}

#[napi(object)]
pub struct NodeNamedRootRecord {
    pub name: Buffer,
    pub tree: NodeTreeRecord,
}

#[napi(object)]
pub struct NodeRootManifestRecord {
    pub tree: NodeTreeRecord,
    pub created_at_millis: Option<String>,
    pub updated_at_millis: Option<String>,
}

#[napi(object)]
pub struct NodeNamedRootManifestRecord {
    pub name: Buffer,
    pub manifest: NodeRootManifestRecord,
}

#[napi(object)]
pub struct NodeNamedRootSelectionRecord {
    pub roots: Vec<NodeNamedRootRecord>,
    pub missing_names: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeNamedRootUpdateRecord {
    pub applied: bool,
    pub conflict: bool,
    pub current: Option<NodeTreeRecord>,
}

#[napi(object)]
pub struct NodeSnapshotNamespaceRecord {
    pub kind: String,
    pub custom_prefix: Option<Buffer>,
}

#[napi(object)]
pub struct NodeSnapshotRecord {
    pub id: Buffer,
    pub name: Buffer,
    pub tree: NodeTreeRecord,
    pub created_at_millis: Option<String>,
    pub updated_at_millis: Option<String>,
}

#[napi(object)]
pub struct NodeSnapshotSelectionRecord {
    pub snapshots: Vec<NodeSnapshotRecord>,
    pub missing_ids: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeKeyProofRecord {
    pub root: Option<Buffer>,
    pub key: Buffer,
    pub path_node_bytes: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeKeyProofVerificationRecord {
    pub valid: bool,
    pub exists: bool,
    pub absence: bool,
    pub root: Option<Buffer>,
    pub key: Buffer,
    pub value: Option<Buffer>,
}

#[napi(object)]
pub struct NodeMultiKeyProofRecord {
    pub root: Option<Buffer>,
    pub keys: Vec<Buffer>,
    pub path_node_bytes: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeMultiKeyProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Buffer>,
    pub results: Vec<NodeKeyProofVerificationRecord>,
}

#[napi(object)]
pub struct NodeRangeProofRecord {
    pub root: Option<Buffer>,
    pub start: Buffer,
    pub end: Option<Buffer>,
    pub path_node_bytes: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeRangeProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Buffer>,
    pub start: Buffer,
    pub end: Option<Buffer>,
    pub entries: Vec<NodeEntryRecord>,
}

#[napi(object)]
pub struct NodeRangePageProofRecord {
    pub root: Option<Buffer>,
    pub after: Option<Buffer>,
    pub end: Option<Buffer>,
    pub path_node_bytes: Vec<Buffer>,
}

#[napi(object)]
pub struct NodeRangePageProofVerificationRecord {
    pub valid: bool,
    pub root: Option<Buffer>,
    pub after: Option<Buffer>,
    pub end: Option<Buffer>,
    pub entries: Vec<NodeEntryRecord>,
}

#[napi(object)]
pub struct NodeProvedRangePageRecord {
    pub page: NodeRangePageRecord,
    pub proof: NodeRangePageProofRecord,
}

#[napi(object)]
pub struct NodeDiffPageProofRecord {
    pub base: NodeRangePageProofRecord,
    pub other: NodeRangePageProofRecord,
    pub lookahead_base: Option<NodeKeyProofRecord>,
    pub lookahead_other: Option<NodeKeyProofRecord>,
    pub requested_end: Option<Buffer>,
    pub limit: String,
}

#[napi(object)]
pub struct NodeDiffPageProofVerificationRecord {
    pub valid: bool,
    pub base_valid: bool,
    pub other_valid: bool,
    pub lookahead_valid: bool,
    pub base_root: Option<Buffer>,
    pub other_root: Option<Buffer>,
    pub after: Option<Buffer>,
    pub requested_end: Option<Buffer>,
    pub proof_end: Option<Buffer>,
    pub limit: String,
    pub diffs: Vec<NodeDiffRecord>,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeProvedDiffPageRecord {
    pub page: NodeDiffPageRecord,
    pub proof: NodeDiffPageProofRecord,
}

#[napi(object)]
pub struct NodeProofBundleSummaryRecord {
    pub version: String,
    pub kind: String,
    pub root: Option<Buffer>,
    pub other_root: Option<Buffer>,
    pub key_count: String,
    pub path_node_count: String,
    pub start: Option<Buffer>,
    pub end: Option<Buffer>,
    pub after: Option<Buffer>,
    pub requested_end: Option<Buffer>,
    pub limit: Option<String>,
    pub has_lookahead: bool,
}

#[napi(object)]
pub struct NodeProofBundleVerificationRecord {
    pub summary: NodeProofBundleSummaryRecord,
    pub valid: bool,
    pub exists_count: String,
    pub absence_count: String,
    pub entry_count: String,
    pub diff_count: String,
    pub next_cursor: Option<NodeRangeCursorRecord>,
}

#[napi(object)]
pub struct NodeAuthenticatedProofEnvelopeRecord {
    pub algorithm: String,
    pub key_id: Buffer,
    pub proof_bundle: Buffer,
    pub context: Buffer,
    pub issued_at_millis: Option<String>,
    pub expires_at_millis: Option<String>,
    pub nonce: Buffer,
    pub signature: Buffer,
}

#[napi(object)]
pub struct NodeAuthenticatedProofEnvelopeVerificationRecord {
    pub valid: bool,
    pub signature_valid: bool,
    pub time_valid: bool,
    pub not_yet_valid: bool,
    pub expired: bool,
    pub algorithm: String,
    pub key_id: Buffer,
    pub proof_bundle: Buffer,
    pub context: Buffer,
    pub issued_at_millis: Option<String>,
    pub expires_at_millis: Option<String>,
    pub nonce: Buffer,
}

#[napi(object)]
pub struct NodeAuthenticatedProofBundleVerificationRecord {
    pub valid: bool,
    pub envelope: NodeAuthenticatedProofEnvelopeVerificationRecord,
    pub proof: Option<NodeProofBundleVerificationRecord>,
    pub proof_error: Option<String>,
}

#[napi(object)]
pub struct NodeNamedRootRetentionRecord {
    pub kind: String,
    pub names: Vec<Buffer>,
    pub prefix: Option<Buffer>,
    pub count: Option<String>,
    pub min_updated_at_millis: Option<String>,
}

#[napi(object)]
pub struct NodeCacheStatsRecord {
    pub cached_nodes: String,
    pub cached_bytes: String,
    pub pinned_nodes: String,
    pub pinned_bytes: String,
}

#[napi(object)]
pub struct NodeMetricsRecord {
    pub node_cache_hits: String,
    pub node_cache_misses: String,
    pub node_cache_evictions: String,
    pub nodes_read: String,
    pub bytes_read: String,
    pub nodes_written: String,
    pub bytes_written: String,
    pub store_get_calls: String,
    pub store_batch_get_calls: String,
    pub store_batch_get_keys: String,
    pub store_put_calls: String,
    pub store_batch_put_calls: String,
    pub store_batch_put_nodes: String,
}

#[napi(object)]
pub struct NodeChangedSpanRecord {
    pub start: Buffer,
    pub end: Option<Buffer>,
}

#[napi(object)]
pub struct NodeChangedSpanHintRecord {
    pub base_root: Option<Buffer>,
    pub changed_root: Option<Buffer>,
    pub spans: Vec<NodeChangedSpanRecord>,
}

#[napi(object)]
pub struct NodeGcReachabilityRecord {
    pub live_cids: Vec<Buffer>,
    pub live_nodes: String,
    pub live_bytes: String,
    pub leaf_nodes: String,
    pub internal_nodes: String,
}

#[napi(object)]
pub struct NodeGcPlanRecord {
    pub reachability: NodeGcReachabilityRecord,
    pub candidate_nodes: String,
    pub reclaimable_cids: Vec<Buffer>,
    pub reclaimable_nodes: String,
    pub reclaimable_bytes: String,
    pub missing_candidates: String,
}

#[napi(object)]
pub struct NodeGcSweepRecord {
    pub plan: NodeGcPlanRecord,
    pub deleted_nodes: String,
    pub deleted_bytes: String,
}

#[napi(object)]
pub struct NodeMissingNodePlanRecord {
    pub required_cids: Vec<Buffer>,
    pub required_nodes: String,
    pub required_bytes: String,
    pub missing_cids: Vec<Buffer>,
    pub missing_nodes: String,
    pub missing_bytes: String,
}

#[napi(object)]
pub struct NodeMissingNodeCopyRecord {
    pub plan: NodeMissingNodePlanRecord,
    pub copied_nodes: String,
    pub copied_bytes: String,
}

#[napi(object)]
pub struct NodeBlobRefRecord {
    pub cid: Buffer,
    pub len: String,
}

#[napi(object)]
pub struct NodeLargeValueConfigRecord {
    pub inline_threshold: String,
}

#[napi(object)]
pub struct NodeValueRefRecord {
    pub kind: String,
    pub value: Option<Buffer>,
    pub blob: Option<NodeBlobRefRecord>,
}

#[napi(object)]
pub struct NodeBlobGcReachabilityRecord {
    pub live_blobs: Vec<NodeBlobRefRecord>,
    pub live_blob_count: String,
    pub live_blob_bytes: String,
    pub scanned_nodes: String,
    pub scanned_values: String,
}

#[napi(object)]
pub struct NodeBlobGcPlanRecord {
    pub reachability: NodeBlobGcReachabilityRecord,
    pub candidate_blobs: String,
    pub reclaimable_blobs: Vec<NodeBlobRefRecord>,
    pub reclaimable_blob_count: String,
    pub reclaimable_blob_bytes: String,
    pub missing_candidates: String,
}

#[napi(object)]
pub struct NodeBlobGcSweepRecord {
    pub plan: NodeBlobGcPlanRecord,
    pub deleted_blobs: String,
    pub deleted_blob_bytes: String,
}

#[napi(object)]
pub struct NodeCrdtConfigRecord {
    pub strategy: String,
    pub delete_policy: String,
}

#[napi(object)]
pub struct NodeTimestampedValueRecord {
    pub value: Buffer,
    pub timestamp: String,
}

#[napi(object)]
pub struct NodeTombstoneMetadataRecord {
    pub key: String,
    pub value: Buffer,
}

#[napi(object)]
pub struct NodeTombstoneRecord {
    pub actor: Buffer,
    pub timestamp_millis: String,
    pub causal_metadata: Vec<NodeTombstoneMetadataRecord>,
}

#[napi]
pub struct NativeProllyBlobStore {
    inner: Arc<ProllyBlobStore>,
}

#[napi]
impl NativeProllyBlobStore {
    #[napi(factory)]
    pub fn memory() -> Self {
        Self {
            inner: Arc::new(ProllyBlobStore::memory()),
        }
    }

    #[napi(factory)]
    pub fn file(path: String) -> Result<Self> {
        ProllyBlobStore::file(path)
            .map(|store| Self {
                inner: Arc::new(store),
            })
            .map_err(to_napi_error)
    }

    #[napi(js_name = "putBlob")]
    pub fn put_blob(&self, bytes: Buffer) -> Result<NodeBlobRefRecord> {
        self.inner
            .put_blob(bytes.to_vec())
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getBlob")]
    pub fn get_blob(&self, reference: NodeBlobRefRecord) -> Result<Option<Buffer>> {
        self.inner
            .get_blob(reference.try_into()?)
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "deleteBlob")]
    pub fn delete_blob(&self, reference: NodeBlobRefRecord) -> Result<()> {
        self.inner
            .delete_blob(reference.try_into()?)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "listBlobRefs")]
    pub fn list_blob_refs(&self) -> Result<Vec<NodeBlobRefRecord>> {
        self.inner
            .list_blob_refs()
            .map(|refs| refs.into_iter().map(Into::into).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "blobCount")]
    pub fn blob_count(&self) -> Result<String> {
        self.inner
            .blob_count()
            .map(|count| count.to_string())
            .map_err(to_napi_error)
    }
}

#[napi]
pub struct NativeMergePolicyRegistry {
    inner: Arc<BindingMergePolicyRegistry>,
    callback_error: Arc<Mutex<Option<String>>>,
}

#[napi]
impl NativeMergePolicyRegistry {
    #[napi(constructor)]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(BindingMergePolicyRegistry::new()),
            callback_error: Arc::new(Mutex::new(None)),
        }
    }

    #[napi]
    pub fn len(&self) -> Result<String> {
        self.inner
            .len()
            .map(|count| count.to_string())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "isEmpty")]
    pub fn is_empty(&self) -> Result<bool> {
        self.inner.is_empty().map_err(to_napi_error)
    }

    #[napi(js_name = "hasDefault")]
    pub fn has_default(&self) -> Result<bool> {
        self.inner.has_default().map_err(to_napi_error)
    }

    #[napi(js_name = "setDefaultResolverName")]
    pub fn set_default_resolver_name(&self, name: String) -> Result<()> {
        self.inner
            .set_default_resolver_name(name)
            .map_err(to_napi_error)
    }

    #[napi(
        js_name = "setDefaultResolver",
        ts_args_type = "resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn set_default_resolver(&self, env: Env, resolver: NodeResolverFunction) -> Result<()> {
        self.inner
            .set_default_host_resolver(js_resolver(env, resolver, Arc::clone(&self.callback_error)))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "pushPrefixResolverName")]
    pub fn push_prefix_resolver_name(&self, prefix: Buffer, name: String) -> Result<()> {
        self.inner
            .push_prefix_resolver_name(prefix.to_vec(), name)
            .map_err(to_napi_error)
    }

    #[napi(
        js_name = "pushPrefixResolver",
        ts_args_type = "prefix: Buffer, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn push_prefix_resolver(
        &self,
        env: Env,
        prefix: Buffer,
        resolver: NodeResolverFunction,
    ) -> Result<()> {
        self.inner
            .push_prefix_host_resolver(
                prefix.to_vec(),
                js_resolver(env, resolver, Arc::clone(&self.callback_error)),
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "pushExactResolverName")]
    pub fn push_exact_resolver_name(&self, key: Buffer, name: String) -> Result<()> {
        self.inner
            .push_exact_resolver_name(key.to_vec(), name)
            .map_err(to_napi_error)
    }

    #[napi(
        js_name = "pushExactResolver",
        ts_args_type = "key: Buffer, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn push_exact_resolver(
        &self,
        env: Env,
        key: Buffer,
        resolver: NodeResolverFunction,
    ) -> Result<()> {
        self.inner
            .push_exact_host_resolver(
                key.to_vec(),
                js_resolver(env, resolver, Arc::clone(&self.callback_error)),
            )
            .map_err(to_napi_error)
    }
}

impl NativeMergePolicyRegistry {
    fn clear_callback_error(&self) {
        if let Ok(mut guard) = self.callback_error.lock() {
            *guard = None;
        }
    }

    fn take_callback_error(&self) -> Option<Error> {
        take_callback_error(&self.callback_error)
    }
}

#[napi]
pub struct NativeHostStore {
    inner: Arc<NodeHostStore>,
}

#[napi]
impl NativeHostStore {
    #[napi(constructor)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        env: Env,
        get: FunctionRef<NodeHostStoreKeyRequest, NodeHostStoreBytesResult>,
        put: FunctionRef<NodeHostStorePutRequest, NodeHostStoreUnitResult>,
        delete: FunctionRef<NodeHostStoreKeyRequest, NodeHostStoreUnitResult>,
        batch: FunctionRef<NodeHostStoreBatchRequest, NodeHostStoreUnitResult>,
        batch_get_ordered: FunctionRef<NodeHostStoreBatchGetRequest, NodeHostStoreBatchGetResult>,
        prefers_batch_reads: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreBoolResult>,
        supports_hints: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreBoolResult>,
        get_hint: FunctionRef<NodeHostStoreHintRequest, NodeHostStoreBytesResult>,
        put_hint: FunctionRef<NodeHostStorePutHintRequest, NodeHostStoreUnitResult>,
        list_node_cids: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreListBytesResult>,
        get_root: FunctionRef<NodeHostStoreRootRequest, NodeHostStoreRootResult>,
        put_root: FunctionRef<NodeHostStorePutRootRequest, NodeHostStoreUnitResult>,
        delete_root: FunctionRef<NodeHostStoreRootRequest, NodeHostStoreUnitResult>,
        compare_and_swap_root: FunctionRef<NodeHostStoreCasRootRequest, NodeHostStoreCasResult>,
        list_roots: FunctionRef<NodeHostStoreEmptyRequest, NodeHostStoreListRootsResult>,
    ) -> Self {
        Self {
            inner: Arc::new(NodeHostStore {
                env,
                callbacks: NodeHostStoreCallbacks {
                    get,
                    put,
                    delete,
                    batch,
                    batch_get_ordered,
                    prefers_batch_reads,
                    supports_hints,
                    get_hint,
                    put_hint,
                    list_node_cids,
                    get_root,
                    put_root,
                    delete_root,
                    compare_and_swap_root,
                    list_roots,
                },
            }),
        }
    }
}

struct NodeHostStore {
    env: Env,
    callbacks: NodeHostStoreCallbacks,
}

unsafe impl Send for NodeHostStore {}
unsafe impl Sync for NodeHostStore {}

impl NodeHostStore {
    fn call<Arg, Ret>(
        &self,
        callback: &FunctionRef<Arg, Ret>,
        arg: Arg,
    ) -> std::result::Result<Ret, String>
    where
        Arg: JsValuesTupleIntoVec,
        Ret: FromNapiValue,
    {
        let function = callback
            .borrow_back(&self.env)
            .map_err(|error| error.to_string())?;
        function.call(arg).map_err(|error| error.to_string())
    }

    fn unit_from_error(error: String) -> BindingHostStoreUnitResultRecord {
        BindingHostStoreUnitResultRecord { error: Some(error) }
    }

    fn bytes_error(error: String) -> BindingHostStoreBytesResultRecord {
        BindingHostStoreBytesResultRecord {
            value: None,
            error: Some(error),
        }
    }

    fn manifest_to_buffer(
        manifest: BindingRootManifestRecord,
    ) -> std::result::Result<Buffer, String> {
        root_manifest_to_bytes(manifest)
            .map(Buffer::from)
            .map_err(|error| error.to_string())
    }

    fn manifest_from_buffer(
        buffer: Buffer,
    ) -> std::result::Result<BindingRootManifestRecord, String> {
        root_manifest_from_bytes(buffer.to_vec()).map_err(|error| error.to_string())
    }

    fn optional_manifest_from_buffer(
        buffer: Option<Buffer>,
    ) -> std::result::Result<Option<BindingRootManifestRecord>, String> {
        buffer.map(Self::manifest_from_buffer).transpose()
    }
}

impl BindingHostStoreCallback for NodeHostStore {
    fn get(&self, key: Vec<u8>) -> BindingHostStoreBytesResultRecord {
        match self.call(
            &self.callbacks.get,
            NodeHostStoreKeyRequest {
                key: Buffer::from(key),
            },
        ) {
            Ok(result) => BindingHostStoreBytesResultRecord {
                value: if result.ok {
                    result.value.map(|value| value.to_vec())
                } else {
                    None
                },
                error: result.error,
            },
            Err(error) => Self::bytes_error(error),
        }
    }

    fn put(&self, key: Vec<u8>, value: Vec<u8>) -> BindingHostStoreUnitResultRecord {
        match self.call(
            &self.callbacks.put,
            NodeHostStorePutRequest {
                key: Buffer::from(key),
                value: Buffer::from(value),
            },
        ) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn delete(&self, key: Vec<u8>) -> BindingHostStoreUnitResultRecord {
        match self.call(
            &self.callbacks.delete,
            NodeHostStoreKeyRequest {
                key: Buffer::from(key),
            },
        ) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn batch(&self, ops: Vec<MutationRecord>) -> BindingHostStoreUnitResultRecord {
        let ops = ops.into_iter().map(Into::into).collect();
        match self.call(&self.callbacks.batch, NodeHostStoreBatchRequest { ops }) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn batch_get_ordered(&self, keys: Vec<Vec<u8>>) -> BindingHostStoreBatchGetResultRecord {
        let keys = keys.into_iter().map(Buffer::from).collect();
        match self.call(
            &self.callbacks.batch_get_ordered,
            NodeHostStoreBatchGetRequest { keys },
        ) {
            Ok(result) => BindingHostStoreBatchGetResultRecord {
                values: result
                    .values
                    .into_iter()
                    .map(|value| {
                        if value.error.is_some() || !value.ok {
                            None
                        } else {
                            value.value.map(|value| value.to_vec())
                        }
                    })
                    .collect(),
                error: result.error,
            },
            Err(error) => BindingHostStoreBatchGetResultRecord {
                values: Vec::new(),
                error: Some(error),
            },
        }
    }

    fn prefers_batch_reads(&self) -> BindingHostStoreBoolResultRecord {
        match self.call(
            &self.callbacks.prefers_batch_reads,
            NodeHostStoreEmptyRequest {},
        ) {
            Ok(result) => BindingHostStoreBoolResultRecord {
                value: result.value,
                error: result.error,
            },
            Err(error) => BindingHostStoreBoolResultRecord {
                value: false,
                error: Some(error),
            },
        }
    }

    fn supports_hints(&self) -> BindingHostStoreBoolResultRecord {
        match self.call(&self.callbacks.supports_hints, NodeHostStoreEmptyRequest {}) {
            Ok(result) => BindingHostStoreBoolResultRecord {
                value: result.value,
                error: result.error,
            },
            Err(error) => BindingHostStoreBoolResultRecord {
                value: false,
                error: Some(error),
            },
        }
    }

    fn get_hint(&self, namespace: Vec<u8>, key: Vec<u8>) -> BindingHostStoreBytesResultRecord {
        match self.call(
            &self.callbacks.get_hint,
            NodeHostStoreHintRequest {
                namespace: Buffer::from(namespace),
                key: Buffer::from(key),
            },
        ) {
            Ok(result) => BindingHostStoreBytesResultRecord {
                value: if result.ok {
                    result.value.map(|value| value.to_vec())
                } else {
                    None
                },
                error: result.error,
            },
            Err(error) => Self::bytes_error(error),
        }
    }

    fn put_hint(
        &self,
        namespace: Vec<u8>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> BindingHostStoreUnitResultRecord {
        match self.call(
            &self.callbacks.put_hint,
            NodeHostStorePutHintRequest {
                namespace: Buffer::from(namespace),
                key: Buffer::from(key),
                value: Buffer::from(value),
            },
        ) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn list_node_cids(&self) -> BindingHostStoreListBytesResultRecord {
        match self.call(&self.callbacks.list_node_cids, NodeHostStoreEmptyRequest {}) {
            Ok(result) => BindingHostStoreListBytesResultRecord {
                values: result
                    .values
                    .into_iter()
                    .map(|value| value.to_vec())
                    .collect(),
                error: result.error,
            },
            Err(error) => BindingHostStoreListBytesResultRecord {
                values: Vec::new(),
                error: Some(error),
            },
        }
    }

    fn get_root(&self, name: Vec<u8>) -> BindingHostStoreRootResultRecord {
        match self.call(
            &self.callbacks.get_root,
            NodeHostStoreRootRequest {
                name: Buffer::from(name),
            },
        ) {
            Ok(result) => match result.value.map(Self::manifest_from_buffer).transpose() {
                Ok(value) => BindingHostStoreRootResultRecord {
                    value,
                    error: result.error,
                },
                Err(error) => BindingHostStoreRootResultRecord {
                    value: None,
                    error: Some(error),
                },
            },
            Err(error) => BindingHostStoreRootResultRecord {
                value: None,
                error: Some(error),
            },
        }
    }

    fn put_root(
        &self,
        name: Vec<u8>,
        manifest: BindingRootManifestRecord,
    ) -> BindingHostStoreUnitResultRecord {
        let manifest = match Self::manifest_to_buffer(manifest) {
            Ok(manifest) => manifest,
            Err(error) => return Self::unit_from_error(error),
        };
        match self.call(
            &self.callbacks.put_root,
            NodeHostStorePutRootRequest {
                name: Buffer::from(name),
                manifest,
            },
        ) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn delete_root(&self, name: Vec<u8>) -> BindingHostStoreUnitResultRecord {
        match self.call(
            &self.callbacks.delete_root,
            NodeHostStoreRootRequest {
                name: Buffer::from(name),
            },
        ) {
            Ok(result) => BindingHostStoreUnitResultRecord {
                error: result.error,
            },
            Err(error) => Self::unit_from_error(error),
        }
    }

    fn compare_and_swap_root(
        &self,
        name: Vec<u8>,
        expected: Option<BindingRootManifestRecord>,
        replacement: Option<BindingRootManifestRecord>,
    ) -> BindingHostStoreRootCasResultRecord {
        let expected = match expected.map(Self::manifest_to_buffer).transpose() {
            Ok(expected) => expected,
            Err(error) => {
                return BindingHostStoreRootCasResultRecord {
                    applied: false,
                    current: None,
                    error: Some(error),
                }
            }
        };
        let replacement = match replacement.map(Self::manifest_to_buffer).transpose() {
            Ok(replacement) => replacement,
            Err(error) => {
                return BindingHostStoreRootCasResultRecord {
                    applied: false,
                    current: None,
                    error: Some(error),
                }
            }
        };
        match self.call(
            &self.callbacks.compare_and_swap_root,
            NodeHostStoreCasRootRequest {
                name: Buffer::from(name),
                expected,
                replacement,
            },
        ) {
            Ok(result) => match Self::optional_manifest_from_buffer(result.current) {
                Ok(current) => BindingHostStoreRootCasResultRecord {
                    applied: result.applied,
                    current,
                    error: result.error,
                },
                Err(error) => BindingHostStoreRootCasResultRecord {
                    applied: false,
                    current: None,
                    error: Some(error),
                },
            },
            Err(error) => BindingHostStoreRootCasResultRecord {
                applied: false,
                current: None,
                error: Some(error),
            },
        }
    }

    fn list_roots(&self) -> BindingHostStoreListRootsResultRecord {
        match self.call(&self.callbacks.list_roots, NodeHostStoreEmptyRequest {}) {
            Ok(result) => {
                let mut values = Vec::with_capacity(result.values.len());
                for root in result.values {
                    match Self::manifest_from_buffer(root.manifest) {
                        Ok(manifest) => values.push(BindingHostStoreNamedRootManifestRecord {
                            name: root.name.to_vec(),
                            manifest,
                        }),
                        Err(error) => {
                            return BindingHostStoreListRootsResultRecord {
                                values: Vec::new(),
                                error: Some(error),
                            }
                        }
                    }
                }
                BindingHostStoreListRootsResultRecord {
                    values,
                    error: result.error,
                }
            }
            Err(error) => BindingHostStoreListRootsResultRecord {
                values: Vec::new(),
                error: Some(error),
            },
        }
    }
}

#[napi]
pub struct NativeProllyEngine {
    inner: Arc<ProllyEngine>,
    config: ConfigRecord,
}

#[napi]
impl NativeProllyEngine {
    #[napi(factory)]
    pub fn memory() -> Result<Self> {
        let config = default_config();
        let inner = ProllyEngine::memory(config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "memoryWithConfigJson")]
    pub fn memory_with_config_json(config_json: String) -> Result<Self> {
        let config = config_from_json(&config_json)?;
        let inner = ProllyEngine::memory(config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "customStore")]
    pub fn custom_store(store: &NativeHostStore) -> Result<Self> {
        let config = default_config();
        let inner = ProllyEngine::custom_store(store.inner.clone(), config.clone())
            .map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "customStoreWithConfigJson")]
    pub fn custom_store_with_config_json(
        store: &NativeHostStore,
        config_json: String,
    ) -> Result<Self> {
        let config = config_from_json(&config_json)?;
        let inner = ProllyEngine::custom_store(store.inner.clone(), config.clone())
            .map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory)]
    pub fn file(path: String) -> Result<Self> {
        let config = default_config();
        let inner = ProllyEngine::file(path, config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "fileWithConfigJson")]
    pub fn file_with_config_json(path: String, config_json: String) -> Result<Self> {
        let config = config_from_json(&config_json)?;
        let inner = ProllyEngine::file(path, config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory)]
    pub fn sqlite(path: String) -> Result<Self> {
        let config = default_config();
        let inner = ProllyEngine::sqlite(path, config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "sqliteWithConfigJson")]
    pub fn sqlite_with_config_json(path: String, config_json: String) -> Result<Self> {
        let config = config_from_json(&config_json)?;
        let inner = ProllyEngine::sqlite(path, config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "sqliteInMemory")]
    pub fn sqlite_in_memory() -> Result<Self> {
        let config = default_config();
        let inner = ProllyEngine::sqlite_in_memory(config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi(factory, js_name = "sqliteInMemoryWithConfigJson")]
    pub fn sqlite_in_memory_with_config_json(config_json: String) -> Result<Self> {
        let config = config_from_json(&config_json)?;
        let inner = ProllyEngine::sqlite_in_memory(config.clone()).map_err(to_napi_error)?;
        Ok(Self {
            inner: Arc::new(inner),
            config,
        })
    }

    #[napi]
    pub fn create(&self) -> NodeTreeRecord {
        self.inner.create().into()
    }

    #[napi]
    pub fn put(&self, tree: NodeTreeRecord, key: Buffer, value: Buffer) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .put(
                tree.into_tree(self.config.clone()),
                key.to_vec(),
                value.to_vec(),
            )
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi]
    pub fn delete(&self, tree: NodeTreeRecord, key: Buffer) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .delete(tree.into_tree(self.config.clone()), key.to_vec())
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi]
    pub fn get(&self, tree: NodeTreeRecord, key: Buffer) -> Result<Option<Buffer>> {
        let value = self
            .inner
            .get(tree.into_tree(self.config.clone()), key.to_vec())
            .map_err(to_napi_error)?;
        Ok(value.map(Buffer::from))
    }

    #[napi(js_name = "getValueRef")]
    pub fn get_value_ref(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
    ) -> Result<Option<NodeValueRefRecord>> {
        self.inner
            .get_value_ref(tree.into_tree(self.config.clone()), key.to_vec())
            .map(|value| value.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getLargeValue")]
    pub fn get_large_value(
        &self,
        blob_store: &NativeProllyBlobStore,
        tree: NodeTreeRecord,
        key: Buffer,
    ) -> Result<Option<Buffer>> {
        self.inner
            .get_large_value(
                blob_store.inner.clone(),
                tree.into_tree(self.config.clone()),
                key.to_vec(),
            )
            .map(|value| value.map(Buffer::from))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "putLargeValue")]
    pub fn put_large_value(
        &self,
        blob_store: &NativeProllyBlobStore,
        tree: NodeTreeRecord,
        key: Buffer,
        value: Buffer,
        config: NodeLargeValueConfigRecord,
    ) -> Result<NodeTreeRecord> {
        self.inner
            .put_large_value(
                blob_store.inner.clone(),
                tree.into_tree(self.config.clone()),
                key.to_vec(),
                value.to_vec(),
                config.try_into()?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "getMany")]
    pub fn get_many(&self, tree: NodeTreeRecord, keys: Vec<Buffer>) -> Result<Vec<Option<Buffer>>> {
        let values = self
            .inner
            .get_many(
                tree.into_tree(self.config.clone()),
                keys.into_iter().map(|key| key.to_vec()).collect(),
            )
            .map_err(to_napi_error)?;
        Ok(values
            .into_iter()
            .map(|value| value.map(Buffer::from))
            .collect())
    }

    #[napi(js_name = "proveKey")]
    pub fn prove_key(&self, tree: NodeTreeRecord, key: Buffer) -> Result<NodeKeyProofRecord> {
        let proof = self
            .inner
            .prove_key(tree.into_tree(self.config.clone()), key.to_vec())
            .map_err(to_napi_error)?;
        NodeKeyProofRecord::try_from_binding(proof)
    }

    #[napi(js_name = "proveKeys")]
    pub fn prove_keys(
        &self,
        tree: NodeTreeRecord,
        keys: Vec<Buffer>,
    ) -> Result<NodeMultiKeyProofRecord> {
        let proof = self
            .inner
            .prove_keys(
                tree.into_tree(self.config.clone()),
                keys.into_iter().map(|key| key.to_vec()).collect(),
            )
            .map_err(to_napi_error)?;
        NodeMultiKeyProofRecord::try_from_binding(proof)
    }

    #[napi(js_name = "proveRange")]
    pub fn prove_range(
        &self,
        tree: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
    ) -> Result<NodeRangeProofRecord> {
        let proof = self
            .inner
            .prove_range(
                tree.into_tree(self.config.clone()),
                start.to_vec(),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        NodeRangeProofRecord::try_from_binding(proof)
    }

    #[napi(js_name = "provePrefix")]
    pub fn prove_prefix(
        &self,
        tree: NodeTreeRecord,
        prefix: Buffer,
    ) -> Result<NodeRangeProofRecord> {
        let proof = self
            .inner
            .prove_prefix(tree.into_tree(self.config.clone()), prefix.to_vec())
            .map_err(to_napi_error)?;
        NodeRangeProofRecord::try_from_binding(proof)
    }

    #[napi(js_name = "proveRangePage")]
    pub fn prove_range_page(
        &self,
        tree: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeProvedRangePageRecord> {
        let limit = parse_u64(&limit)?;
        let proof_page = self
            .inner
            .prove_range_page(
                tree.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map_err(to_napi_error)?;
        NodeProvedRangePageRecord::try_from_binding(proof_page)
    }

    #[napi(js_name = "proveDiffPage")]
    pub fn prove_diff_page(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeProvedDiffPageRecord> {
        let limit = parse_u64(&limit)?;
        let proof_page = self
            .inner
            .prove_diff_page(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map_err(to_napi_error)?;
        NodeProvedDiffPageRecord::try_from_binding(proof_page)
    }

    #[napi]
    pub fn batch(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodeTreeRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let tree = self
            .inner
            .batch(tree.into_tree(self.config.clone()), mutations)
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi(js_name = "batchWithStats")]
    pub fn batch_with_stats(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodeBatchApplyResultRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .batch_with_stats(tree.into_tree(self.config.clone()), mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "parallelBatch")]
    pub fn parallel_batch(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
        config: NodeParallelConfigRecord,
    ) -> Result<NodeTreeRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let config = config.try_into()?;
        self.inner
            .parallel_batch(tree.into_tree(self.config.clone()), mutations, config)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "parallelBatchWithStats")]
    pub fn parallel_batch_with_stats(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
        config: NodeParallelConfigRecord,
    ) -> Result<NodeBatchApplyResultRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        let config = config.try_into()?;
        self.inner
            .parallel_batch_with_stats(tree.into_tree(self.config.clone()), mutations, config)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "buildFromEntries")]
    pub fn build_from_entries(&self, entries: Vec<NodeEntryRecord>) -> Result<NodeTreeRecord> {
        let entries = entries
            .into_iter()
            .map(Into::into)
            .collect::<Vec<BindingEntryRecord>>();
        self.inner
            .build_from_entries(entries)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "buildFromSortedEntries")]
    pub fn build_from_sorted_entries(
        &self,
        entries: Vec<NodeEntryRecord>,
    ) -> Result<NodeTreeRecord> {
        let entries = entries
            .into_iter()
            .map(Into::into)
            .collect::<Vec<BindingEntryRecord>>();
        self.inner
            .build_from_sorted_entries(entries)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "appendBatch")]
    pub fn append_batch(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodeTreeRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .append_batch(tree.into_tree(self.config.clone()), mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "appendBatchWithStats")]
    pub fn append_batch_with_stats(
        &self,
        tree: NodeTreeRecord,
        mutations: Vec<NodeMutationRecord>,
    ) -> Result<NodeBatchApplyResultRecord> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>>>()?;
        self.inner
            .append_batch_with_stats(tree.into_tree(self.config.clone()), mutations)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "firstEntry")]
    pub fn first_entry(&self, tree: NodeTreeRecord) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .first_entry(tree.into_tree(self.config.clone()))
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "lastEntry")]
    pub fn last_entry(&self, tree: NodeTreeRecord) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .last_entry(tree.into_tree(self.config.clone()))
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "lowerBound")]
    pub fn lower_bound(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
    ) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .lower_bound(tree.into_tree(self.config.clone()), key.to_vec())
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "upperBound")]
    pub fn upper_bound(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
    ) -> Result<Option<NodeEntryRecord>> {
        self.inner
            .upper_bound(tree.into_tree(self.config.clone()), key.to_vec())
            .map(|entry| entry.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn range(
        &self,
        tree: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
    ) -> Result<Vec<NodeEntryRecord>> {
        let entries = self
            .inner
            .range(
                tree.into_tree(self.config.clone()),
                start.to_vec(),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    #[napi]
    pub fn prefix(&self, tree: NodeTreeRecord, prefix: Buffer) -> Result<Vec<NodeEntryRecord>> {
        let entries = self
            .inner
            .prefix(tree.into_tree(self.config.clone()), prefix.to_vec())
            .map_err(to_napi_error)?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "prefixPage")]
    pub fn prefix_page(
        &self,
        tree: NodeTreeRecord,
        prefix: Buffer,
        cursor: Option<NodeRangeCursorRecord>,
        limit: String,
    ) -> Result<NodeRangePageRecord> {
        let page = self
            .inner
            .prefix_page(
                tree.into_tree(self.config.clone()),
                prefix.to_vec(),
                cursor.map(Into::into),
                parse_u64(&limit)?,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi(js_name = "prefixReversePage")]
    pub fn prefix_reverse_page(
        &self,
        tree: NodeTreeRecord,
        prefix: Buffer,
        cursor: Option<NodeReverseCursorRecord>,
        limit: String,
    ) -> Result<NodeReversePageRecord> {
        let page = self
            .inner
            .prefix_reverse_page(
                tree.into_tree(self.config.clone()),
                prefix.to_vec(),
                cursor.map(Into::into),
                parse_u64(&limit)?,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi(js_name = "rangeAfter")]
    pub fn range_after(
        &self,
        tree: NodeTreeRecord,
        after_key: Buffer,
        end: Option<Buffer>,
    ) -> Result<Vec<NodeEntryRecord>> {
        let entries = self
            .inner
            .range_after(
                tree.into_tree(self.config.clone()),
                after_key.to_vec(),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "rangeFromCursor")]
    pub fn range_from_cursor(
        &self,
        tree: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
    ) -> Result<Vec<NodeEntryRecord>> {
        let entries = self
            .inner
            .range_from_cursor(
                tree.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        Ok(entries.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "rangePage")]
    pub fn range_page(
        &self,
        tree: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeRangePageRecord> {
        let limit = parse_u64(&limit)?;
        let page = self
            .inner
            .range_page(
                tree.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi(js_name = "reversePage")]
    pub fn reverse_page(
        &self,
        tree: NodeTreeRecord,
        cursor: Option<NodeReverseCursorRecord>,
        start: Buffer,
        limit: String,
    ) -> Result<NodeReversePageRecord> {
        let limit = parse_u64(&limit)?;
        let page = self
            .inner
            .reverse_page(
                tree.into_tree(self.config.clone()),
                cursor.map(Into::into),
                start.to_vec(),
                limit,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi(js_name = "cursorWindow")]
    pub fn cursor_window(
        &self,
        tree: NodeTreeRecord,
        key: Buffer,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeCursorWindowRecord> {
        let limit = parse_u64(&limit)?;
        let window = self
            .inner
            .cursor_window(
                tree.into_tree(self.config.clone()),
                key.to_vec(),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map_err(to_napi_error)?;
        Ok(window.into())
    }

    #[napi]
    pub fn diff(&self, base: NodeTreeRecord, other: NodeTreeRecord) -> Result<Vec<NodeDiffRecord>> {
        let diffs = self
            .inner
            .diff(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
            )
            .map_err(to_napi_error)?;
        Ok(diffs.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "rangeDiff")]
    pub fn range_diff(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
    ) -> Result<Vec<NodeDiffRecord>> {
        let diffs = self
            .inner
            .range_diff(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                start.to_vec(),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        Ok(diffs.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "diffFromCursor")]
    pub fn diff_from_cursor(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
    ) -> Result<Vec<NodeDiffRecord>> {
        let diffs = self
            .inner
            .diff_from_cursor(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
            )
            .map_err(to_napi_error)?;
        Ok(diffs.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "diffPage")]
    pub fn diff_page(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        end: Option<Buffer>,
        limit: String,
    ) -> Result<NodeDiffPageRecord> {
        let limit = parse_u64(&limit)?;
        let page = self
            .inner
            .diff_page(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                cursor.map(Into::into),
                end.map(|value| value.to_vec()),
                limit,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi(js_name = "conflictPage")]
    pub fn conflict_page(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        cursor: Option<NodeRangeCursorRecord>,
        limit: String,
    ) -> Result<NodeConflictPageRecord> {
        let limit = parse_u64(&limit)?;
        let page = self
            .inner
            .conflict_page(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                cursor.map(Into::into),
                limit,
            )
            .map_err(to_napi_error)?;
        Ok(page.into())
    }

    #[napi]
    pub fn merge(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        resolver: Option<String>,
    ) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .merge(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                resolver,
            )
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi(
        js_name = "mergeWithResolver",
        ts_args_type = "base: NodeTreeRecord, left: NodeTreeRecord, right: NodeTreeRecord, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn merge_with_resolver(
        &self,
        env: Env,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        resolver: NodeResolverFunction,
    ) -> Result<NodeTreeRecord> {
        let callback_error = Arc::new(Mutex::new(None));
        let tree = self.inner.merge_with_host_resolver(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            js_resolver(env, resolver, Arc::clone(&callback_error)),
        );
        if let Some(error) = take_callback_error(&callback_error) {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergeWithPolicy")]
    pub fn merge_with_policy(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        policy: &NativeMergePolicyRegistry,
    ) -> Result<NodeTreeRecord> {
        policy.clear_callback_error();
        let tree = self.inner.merge_with_policy(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            policy.inner.clone(),
        );
        if let Some(error) = policy.take_callback_error() {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "crdtMerge")]
    pub fn crdt_merge(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        config: NodeCrdtConfigRecord,
    ) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .crdt_merge(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                config.try_into()?,
            )
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi(
        js_name = "crdtMergeWithResolver",
        ts_args_type = "base: NodeTreeRecord, left: NodeTreeRecord, right: NodeTreeRecord, deletePolicy: string, resolver: (conflict: NodeConflictRecord) => NodeCrdtResolutionRecord"
    )]
    pub fn crdt_merge_with_resolver(
        &self,
        env: Env,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        delete_policy: String,
        resolver: NodeCrdtResolverFunction,
    ) -> Result<NodeTreeRecord> {
        let delete_policy = crdt_delete_policy_from_str(&delete_policy)?;
        let callback_error = Arc::new(Mutex::new(None));
        let tree = self.inner.crdt_merge_with_host_resolver(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            delete_policy,
            js_crdt_resolver(env, resolver, Arc::clone(&callback_error)),
        );
        if let Some(error) = take_callback_error(&callback_error) {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergeExplain")]
    pub fn merge_explain(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        resolver: Option<String>,
    ) -> Result<NodeMergeExplanationRecord> {
        let explanation = self
            .inner
            .merge_explain(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                resolver,
            )
            .map_err(to_napi_error)?;
        Ok(explanation.into())
    }

    #[napi(
        js_name = "mergeExplainWithResolver",
        ts_args_type = "base: NodeTreeRecord, left: NodeTreeRecord, right: NodeTreeRecord, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn merge_explain_with_resolver(
        &self,
        env: Env,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        resolver: NodeResolverFunction,
    ) -> Result<NodeMergeExplanationRecord> {
        let callback_error = Arc::new(Mutex::new(None));
        let explanation = self.inner.merge_explain_with_host_resolver(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            js_resolver(env, resolver, Arc::clone(&callback_error)),
        );
        if let Some(error) = take_callback_error(&callback_error) {
            return Err(error);
        }
        Ok(explanation.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergeExplainWithPolicy")]
    pub fn merge_explain_with_policy(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        policy: &NativeMergePolicyRegistry,
    ) -> Result<NodeMergeExplanationRecord> {
        policy.clear_callback_error();
        let explanation = self.inner.merge_explain_with_policy(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            policy.inner.clone(),
        );
        if let Some(error) = policy.take_callback_error() {
            return Err(error);
        }
        Ok(explanation.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergeRange")]
    pub fn merge_range(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
        resolver: Option<String>,
    ) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .merge_range(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                start.to_vec(),
                end.map(|value| value.to_vec()),
                resolver,
            )
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi(
        js_name = "mergeRangeWithResolver",
        ts_args_type = "base: NodeTreeRecord, left: NodeTreeRecord, right: NodeTreeRecord, start: Buffer, end: Buffer | null | undefined, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn merge_range_with_resolver(
        &self,
        env: Env,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
        resolver: NodeResolverFunction,
    ) -> Result<NodeTreeRecord> {
        let callback_error = Arc::new(Mutex::new(None));
        let tree = self.inner.merge_range_with_host_resolver(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            start.to_vec(),
            end.map(|value| value.to_vec()),
            js_resolver(env, resolver, Arc::clone(&callback_error)),
        );
        if let Some(error) = take_callback_error(&callback_error) {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergeRangeWithPolicy")]
    pub fn merge_range_with_policy(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        start: Buffer,
        end: Option<Buffer>,
        policy: &NativeMergePolicyRegistry,
    ) -> Result<NodeTreeRecord> {
        policy.clear_callback_error();
        let tree = self.inner.merge_range_with_policy(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            start.to_vec(),
            end.map(|value| value.to_vec()),
            policy.inner.clone(),
        );
        if let Some(error) = policy.take_callback_error() {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergePrefix")]
    pub fn merge_prefix(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        prefix: Buffer,
        resolver: Option<String>,
    ) -> Result<NodeTreeRecord> {
        let tree = self
            .inner
            .merge_prefix(
                base.into_tree(self.config.clone()),
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
                prefix.to_vec(),
                resolver,
            )
            .map_err(to_napi_error)?;
        Ok(tree.into())
    }

    #[napi(
        js_name = "mergePrefixWithResolver",
        ts_args_type = "base: NodeTreeRecord, left: NodeTreeRecord, right: NodeTreeRecord, prefix: Buffer, resolver: (conflict: NodeConflictRecord) => NodeResolutionRecord"
    )]
    pub fn merge_prefix_with_resolver(
        &self,
        env: Env,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        prefix: Buffer,
        resolver: NodeResolverFunction,
    ) -> Result<NodeTreeRecord> {
        let callback_error = Arc::new(Mutex::new(None));
        let tree = self.inner.merge_prefix_with_host_resolver(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            prefix.to_vec(),
            js_resolver(env, resolver, Arc::clone(&callback_error)),
        );
        if let Some(error) = take_callback_error(&callback_error) {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "mergePrefixWithPolicy")]
    pub fn merge_prefix_with_policy(
        &self,
        base: NodeTreeRecord,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
        prefix: Buffer,
        policy: &NativeMergePolicyRegistry,
    ) -> Result<NodeTreeRecord> {
        policy.clear_callback_error();
        let tree = self.inner.merge_prefix_with_policy(
            base.into_tree(self.config.clone()),
            left.into_tree(self.config.clone()),
            right.into_tree(self.config.clone()),
            prefix.to_vec(),
            policy.inner.clone(),
        );
        if let Some(error) = policy.take_callback_error() {
            return Err(error);
        }
        Ok(tree.map_err(to_napi_error)?.into())
    }

    #[napi(js_name = "loadNamedRoot")]
    pub fn load_named_root(&self, name: Buffer) -> Result<Option<NodeTreeRecord>> {
        let tree = self
            .inner
            .load_named_root(name.to_vec())
            .map_err(to_napi_error)?;
        Ok(tree.map(Into::into))
    }

    #[napi(js_name = "loadNamedRoots")]
    pub fn load_named_roots(&self, names: Vec<Buffer>) -> Result<NodeNamedRootSelectionRecord> {
        let selection = self
            .inner
            .load_named_roots(names.into_iter().map(|name| name.to_vec()).collect())
            .map_err(to_napi_error)?;
        Ok(selection.into())
    }

    #[napi(js_name = "loadRetainedNamedRoots")]
    pub fn load_retained_named_roots(
        &self,
        retention: NodeNamedRootRetentionRecord,
    ) -> Result<NodeNamedRootSelectionRecord> {
        let selection = self
            .inner
            .load_retained_named_roots(retention.try_into()?)
            .map_err(to_napi_error)?;
        Ok(selection.into())
    }

    #[napi(js_name = "listNamedRoots")]
    pub fn list_named_roots(&self) -> Result<Vec<NodeNamedRootRecord>> {
        let roots = self.inner.list_named_roots().map_err(to_napi_error)?;
        Ok(roots.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "listNamedRootManifests")]
    pub fn list_named_root_manifests(&self) -> Result<Vec<NodeNamedRootManifestRecord>> {
        let roots = self
            .inner
            .list_named_root_manifests()
            .map_err(to_napi_error)?;
        Ok(roots.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "publishNamedRoot")]
    pub fn publish_named_root(&self, name: Buffer, tree: NodeTreeRecord) -> Result<()> {
        self.inner
            .publish_named_root(name.to_vec(), tree.into_tree(self.config.clone()))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "publishNamedRootAtMillis")]
    pub fn publish_named_root_at_millis(
        &self,
        name: Buffer,
        tree: NodeTreeRecord,
        timestamp_millis: String,
    ) -> Result<()> {
        let timestamp_millis = parse_u64(&timestamp_millis)?;
        self.inner
            .publish_named_root_at_millis(
                name.to_vec(),
                tree.into_tree(self.config.clone()),
                timestamp_millis,
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "deleteNamedRoot")]
    pub fn delete_named_root(&self, name: Buffer) -> Result<()> {
        self.inner
            .delete_named_root(name.to_vec())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "compareAndSwapNamedRoot")]
    pub fn compare_and_swap_named_root(
        &self,
        name: Buffer,
        expected: Option<NodeTreeRecord>,
        replacement: Option<NodeTreeRecord>,
    ) -> Result<NodeNamedRootUpdateRecord> {
        let update = self
            .inner
            .compare_and_swap_named_root(
                name.to_vec(),
                expected.map(|tree| tree.into_tree(self.config.clone())),
                replacement.map(|tree| tree.into_tree(self.config.clone())),
            )
            .map_err(to_napi_error)?;
        Ok(update.into())
    }

    #[napi(js_name = "publishSnapshot")]
    pub fn publish_snapshot(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
        tree: NodeTreeRecord,
    ) -> Result<()> {
        self.inner
            .publish_snapshot(
                namespace.try_into()?,
                id.to_vec(),
                tree.into_tree(self.config.clone()),
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "publishSnapshotAtMillis")]
    pub fn publish_snapshot_at_millis(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
        tree: NodeTreeRecord,
        timestamp_millis: String,
    ) -> Result<()> {
        let timestamp_millis = parse_u64(&timestamp_millis)?;
        self.inner
            .publish_snapshot_at_millis(
                namespace.try_into()?,
                id.to_vec(),
                tree.into_tree(self.config.clone()),
                timestamp_millis,
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "loadSnapshot")]
    pub fn load_snapshot(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
    ) -> Result<Option<NodeTreeRecord>> {
        let tree = self
            .inner
            .load_snapshot(namespace.try_into()?, id.to_vec())
            .map_err(to_napi_error)?;
        Ok(tree.map(Into::into))
    }

    #[napi(js_name = "loadSnapshots")]
    pub fn load_snapshots(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        ids: Vec<Buffer>,
    ) -> Result<NodeSnapshotSelectionRecord> {
        let selection = self
            .inner
            .load_snapshots(
                namespace.try_into()?,
                ids.into_iter().map(|id| id.to_vec()).collect(),
            )
            .map_err(to_napi_error)?;
        Ok(selection.into())
    }

    #[napi(js_name = "listSnapshots")]
    pub fn list_snapshots(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
    ) -> Result<Vec<NodeSnapshotRecord>> {
        let snapshots = self
            .inner
            .list_snapshots(namespace.try_into()?)
            .map_err(to_napi_error)?;
        Ok(snapshots.into_iter().map(Into::into).collect())
    }

    #[napi(js_name = "deleteSnapshot")]
    pub fn delete_snapshot(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
    ) -> Result<()> {
        self.inner
            .delete_snapshot(namespace.try_into()?, id.to_vec())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "compareAndSwapSnapshot")]
    pub fn compare_and_swap_snapshot(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
        expected: Option<NodeTreeRecord>,
        replacement: Option<NodeTreeRecord>,
    ) -> Result<NodeNamedRootUpdateRecord> {
        let update = self
            .inner
            .compare_and_swap_snapshot(
                namespace.try_into()?,
                id.to_vec(),
                expected.map(|tree| tree.into_tree(self.config.clone())),
                replacement.map(|tree| tree.into_tree(self.config.clone())),
            )
            .map_err(to_napi_error)?;
        Ok(update.into())
    }

    #[napi(js_name = "compareAndSwapSnapshotAtMillis")]
    pub fn compare_and_swap_snapshot_at_millis(
        &self,
        namespace: NodeSnapshotNamespaceRecord,
        id: Buffer,
        expected: Option<NodeTreeRecord>,
        replacement: Option<NodeTreeRecord>,
        timestamp_millis: String,
    ) -> Result<NodeNamedRootUpdateRecord> {
        let timestamp_millis = parse_u64(&timestamp_millis)?;
        let update = self
            .inner
            .compare_and_swap_snapshot_at_millis(
                namespace.try_into()?,
                id.to_vec(),
                expected.map(|tree| tree.into_tree(self.config.clone())),
                replacement.map(|tree| tree.into_tree(self.config.clone())),
                timestamp_millis,
            )
            .map_err(to_napi_error)?;
        Ok(update.into())
    }

    #[napi(js_name = "collectStatsJson")]
    pub fn collect_stats_json(&self, tree: NodeTreeRecord) -> Result<String> {
        self.inner
            .collect_stats_json(tree.into_tree(self.config.clone()))
            .map(|document| document.json)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "collectStats")]
    pub fn collect_stats(&self, env: Env, tree: NodeTreeRecord) -> Result<JsUnknown> {
        let json = self.collect_stats_json(tree)?;
        let value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        env.to_js_value(&value)
    }

    #[napi(js_name = "statsDiffJson")]
    pub fn stats_diff_json(&self, before: NodeTreeRecord, after: NodeTreeRecord) -> Result<String> {
        self.inner
            .stats_diff_json(
                before.into_tree(self.config.clone()),
                after.into_tree(self.config.clone()),
            )
            .map(|document| document.json)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "statsDiff")]
    pub fn stats_diff(
        &self,
        env: Env,
        before: NodeTreeRecord,
        after: NodeTreeRecord,
    ) -> Result<JsUnknown> {
        let json = self.stats_diff_json(before, after)?;
        let value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        env.to_js_value(&value)
    }

    #[napi(js_name = "debugTreeJson")]
    pub fn debug_tree_json(&self, tree: NodeTreeRecord) -> Result<String> {
        self.inner
            .debug_tree_json(tree.into_tree(self.config.clone()))
            .map(|document| document.json)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "debugTree")]
    pub fn debug_tree(&self, env: Env, tree: NodeTreeRecord) -> Result<JsUnknown> {
        let json = self.debug_tree_json(tree)?;
        let value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        env.to_js_value(&value)
    }

    #[napi(js_name = "debugTreeText")]
    pub fn debug_tree_text(&self, tree: NodeTreeRecord) -> Result<String> {
        self.inner
            .debug_tree_text(tree.into_tree(self.config.clone()))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "debugCompareTreesJson")]
    pub fn debug_compare_trees_json(
        &self,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
    ) -> Result<String> {
        self.inner
            .debug_compare_trees_json(
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
            )
            .map(|document| document.json)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "debugCompareTrees")]
    pub fn debug_compare_trees(
        &self,
        env: Env,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
    ) -> Result<JsUnknown> {
        let json = self.debug_compare_trees_json(left, right)?;
        let value: serde_json::Value = serde_json::from_str(&json)
            .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
        env.to_js_value(&value)
    }

    #[napi(js_name = "debugCompareTreesText")]
    pub fn debug_compare_trees_text(
        &self,
        left: NodeTreeRecord,
        right: NodeTreeRecord,
    ) -> Result<String> {
        self.inner
            .debug_compare_trees_text(
                left.into_tree(self.config.clone()),
                right.into_tree(self.config.clone()),
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "cacheStats")]
    pub fn cache_stats(&self) -> Result<NodeCacheStatsRecord> {
        self.inner
            .cache_stats()
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "clearCache")]
    pub fn clear_cache(&self) {
        self.inner.clear_cache();
    }

    #[napi(js_name = "pinTreeRoot")]
    pub fn pin_tree_root(&self, tree: NodeTreeRecord) -> Result<String> {
        self.inner
            .pin_tree_root(tree.into_tree(self.config.clone()))
            .map(|value| value.to_string())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "pinTreePath")]
    pub fn pin_tree_path(&self, tree: NodeTreeRecord, key: Buffer) -> Result<String> {
        self.inner
            .pin_tree_path(tree.into_tree(self.config.clone()), key.to_vec())
            .map(|value| value.to_string())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "unpinAllCacheNodes")]
    pub fn unpin_all_cache_nodes(&self) -> Result<String> {
        self.inner
            .unpin_all_cache_nodes()
            .map(|value| value.to_string())
            .map_err(to_napi_error)
    }

    #[napi]
    pub fn metrics(&self) -> NodeMetricsRecord {
        self.inner.metrics().into()
    }

    #[napi(js_name = "resetMetrics")]
    pub fn reset_metrics(&self) {
        self.inner.reset_metrics();
    }

    #[napi(js_name = "publishPrefixPathHint")]
    pub fn publish_prefix_path_hint(&self, tree: NodeTreeRecord, prefix: Buffer) -> Result<bool> {
        self.inner
            .publish_prefix_path_hint(tree.into_tree(self.config.clone()), prefix.to_vec())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "hydratePrefixPathHint")]
    pub fn hydrate_prefix_path_hint(&self, tree: NodeTreeRecord, prefix: Buffer) -> Result<bool> {
        self.inner
            .hydrate_prefix_path_hint(tree.into_tree(self.config.clone()), prefix.to_vec())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "publishChangedSpansHint")]
    pub fn publish_changed_spans_hint(
        &self,
        base: NodeTreeRecord,
        changed: NodeTreeRecord,
        spans: Vec<NodeChangedSpanRecord>,
    ) -> Result<bool> {
        self.inner
            .publish_changed_spans_hint(
                base.into_tree(self.config.clone()),
                changed.into_tree(self.config.clone()),
                spans.into_iter().map(Into::into).collect(),
            )
            .map_err(to_napi_error)
    }

    #[napi(js_name = "loadChangedSpansHint")]
    pub fn load_changed_spans_hint(
        &self,
        base: NodeTreeRecord,
        changed: NodeTreeRecord,
    ) -> Result<Option<NodeChangedSpanHintRecord>> {
        self.inner
            .load_changed_spans_hint(
                base.into_tree(self.config.clone()),
                changed.into_tree(self.config.clone()),
            )
            .map(|hint| hint.map(Into::into))
            .map_err(to_napi_error)
    }

    #[napi(js_name = "structuralDiffPage")]
    pub fn structural_diff_page(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        cursor_json: Option<String>,
        limit: String,
    ) -> Result<NodeStructuralDiffPageRecord> {
        let limit = parse_u64(&limit)?;
        self.inner
            .structural_diff_page(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                cursor_json,
                limit,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "structuralDiffPageWithCursor")]
    pub fn structural_diff_page_with_cursor(
        &self,
        base: NodeTreeRecord,
        other: NodeTreeRecord,
        cursor: Option<NodeStructuralDiffCursorRecord>,
        limit: String,
    ) -> Result<NodeStructuralDiffPageRecord> {
        let limit = parse_u64(&limit)?;
        let cursor = cursor
            .map(BindingStructuralDiffCursorRecord::try_from)
            .transpose()?;
        self.inner
            .structural_diff_page_with_cursor(
                base.into_tree(self.config.clone()),
                other.into_tree(self.config.clone()),
                cursor,
                limit,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "markReachable")]
    pub fn mark_reachable(&self, roots: Vec<NodeTreeRecord>) -> Result<NodeGcReachabilityRecord> {
        self.inner
            .mark_reachable(self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "markReachableBlobs")]
    pub fn mark_reachable_blobs(
        &self,
        roots: Vec<NodeTreeRecord>,
    ) -> Result<NodeBlobGcReachabilityRecord> {
        self.inner
            .mark_reachable_blobs(self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "listNodeCids")]
    pub fn list_node_cids(&self) -> Result<Vec<Buffer>> {
        self.inner
            .list_node_cids()
            .map(|cids| cids.into_iter().map(Buffer::from).collect())
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planGc")]
    pub fn plan_gc(
        &self,
        roots: Vec<NodeTreeRecord>,
        candidate_cids: Vec<Buffer>,
    ) -> Result<NodeGcPlanRecord> {
        self.inner
            .plan_gc(self.trees(roots), buffers(candidate_cids))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "sweepGc")]
    pub fn sweep_gc(
        &self,
        roots: Vec<NodeTreeRecord>,
        candidate_cids: Vec<Buffer>,
    ) -> Result<NodeGcSweepRecord> {
        self.inner
            .sweep_gc(self.trees(roots), buffers(candidate_cids))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planStoreGc")]
    pub fn plan_store_gc(&self, roots: Vec<NodeTreeRecord>) -> Result<NodeGcPlanRecord> {
        self.inner
            .plan_store_gc(self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "sweepStoreGc")]
    pub fn sweep_store_gc(&self, roots: Vec<NodeTreeRecord>) -> Result<NodeGcSweepRecord> {
        self.inner
            .sweep_store_gc(self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planStoreGcForRetention")]
    pub fn plan_store_gc_for_retention(
        &self,
        retention: NodeNamedRootRetentionRecord,
    ) -> Result<NodeGcPlanRecord> {
        self.inner
            .plan_store_gc_for_retention(retention.try_into()?)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "sweepStoreGcForRetention")]
    pub fn sweep_store_gc_for_retention(
        &self,
        retention: NodeNamedRootRetentionRecord,
    ) -> Result<NodeGcSweepRecord> {
        self.inner
            .sweep_store_gc_for_retention(retention.try_into()?)
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planBlobGc")]
    pub fn plan_blob_gc(
        &self,
        blob_store: &NativeProllyBlobStore,
        roots: Vec<NodeTreeRecord>,
        candidate_blobs: Vec<NodeBlobRefRecord>,
    ) -> Result<NodeBlobGcPlanRecord> {
        self.inner
            .plan_blob_gc(
                blob_store.inner.clone(),
                self.trees(roots),
                candidate_blobs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>>>()?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "sweepBlobGc")]
    pub fn sweep_blob_gc(
        &self,
        blob_store: &NativeProllyBlobStore,
        roots: Vec<NodeTreeRecord>,
        candidate_blobs: Vec<NodeBlobRefRecord>,
    ) -> Result<NodeBlobGcSweepRecord> {
        self.inner
            .sweep_blob_gc(
                blob_store.inner.clone(),
                self.trees(roots),
                candidate_blobs
                    .into_iter()
                    .map(TryInto::try_into)
                    .collect::<Result<Vec<_>>>()?,
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planBlobStoreGc")]
    pub fn plan_blob_store_gc(
        &self,
        blob_store: &NativeProllyBlobStore,
        roots: Vec<NodeTreeRecord>,
    ) -> Result<NodeBlobGcPlanRecord> {
        self.inner
            .plan_blob_store_gc(blob_store.inner.clone(), self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "sweepBlobStoreGc")]
    pub fn sweep_blob_store_gc(
        &self,
        blob_store: &NativeProllyBlobStore,
        roots: Vec<NodeTreeRecord>,
    ) -> Result<NodeBlobGcSweepRecord> {
        self.inner
            .sweep_blob_store_gc(blob_store.inner.clone(), self.trees(roots))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "planMissingNodes")]
    pub fn plan_missing_nodes(
        &self,
        tree: NodeTreeRecord,
        destination: &NativeProllyEngine,
    ) -> Result<NodeMissingNodePlanRecord> {
        self.inner
            .plan_missing_nodes(
                tree.into_tree(self.config.clone()),
                destination.inner.clone(),
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "copyMissingNodes")]
    pub fn copy_missing_nodes(
        &self,
        tree: NodeTreeRecord,
        destination: &NativeProllyEngine,
    ) -> Result<NodeMissingNodeCopyRecord> {
        self.inner
            .copy_missing_nodes(
                tree.into_tree(self.config.clone()),
                destination.inner.clone(),
            )
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "exportSnapshot")]
    pub fn export_snapshot(&self, tree: NodeTreeRecord) -> Result<NodeSnapshotBundleRecord> {
        self.inner
            .export_snapshot(tree.into_tree(self.config.clone()))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    #[napi(js_name = "importSnapshot")]
    pub fn import_snapshot(&self, bundle: NodeSnapshotBundleRecord) -> Result<NodeTreeRecord> {
        self.inner
            .import_snapshot(bundle.into_binding(self.config.clone()))
            .map(Into::into)
            .map_err(to_napi_error)
    }

    fn trees(&self, trees: Vec<NodeTreeRecord>) -> Vec<TreeRecord> {
        trees
            .into_iter()
            .map(|tree| tree.into_tree(self.config.clone()))
            .collect()
    }
}

impl NodeTreeRecord {
    fn into_tree(self, config: ConfigRecord) -> TreeRecord {
        TreeRecord {
            root: self.root.map(|root| root.to_vec()),
            config: self
                .config
                .and_then(|config| config.try_into().ok())
                .unwrap_or(config),
        }
    }
}

impl From<TreeRecord> for NodeTreeRecord {
    fn from(tree: TreeRecord) -> Self {
        Self {
            root: tree.root.map(Buffer::from),
            config: Some(tree.config.into()),
        }
    }
}

impl From<BindingEntryRecord> for NodeEntryRecord {
    fn from(entry: BindingEntryRecord) -> Self {
        Self {
            key: Buffer::from(entry.key),
            value: Buffer::from(entry.value),
        }
    }
}

impl From<NodeEntryRecord> for BindingEntryRecord {
    fn from(entry: NodeEntryRecord) -> Self {
        Self {
            key: entry.key.to_vec(),
            value: entry.value.to_vec(),
        }
    }
}

impl From<BindingDiffRecord> for NodeDiffRecord {
    fn from(diff: BindingDiffRecord) -> Self {
        let kind = match diff.kind {
            DiffKind::Added => "added",
            DiffKind::Removed => "removed",
            DiffKind::Changed => "changed",
        }
        .to_string();
        Self {
            kind,
            key: Buffer::from(diff.key),
            value: diff.value.map(Buffer::from),
            old: diff.old_value.map(Buffer::from),
            new_value: diff.new_value.map(Buffer::from),
        }
    }
}

impl TryFrom<NodeDiffRecord> for BindingDiffRecord {
    type Error = Error;

    fn try_from(diff: NodeDiffRecord) -> Result<Self> {
        let kind = match diff.kind.as_str() {
            "added" => DiffKind::Added,
            "removed" => DiffKind::Removed,
            "changed" => DiffKind::Changed,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown diff kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            kind,
            key: diff.key.to_vec(),
            value: diff.value.map(|value| value.to_vec()),
            old_value: diff.old.map(|value| value.to_vec()),
            new_value: diff.new_value.map(|value| value.to_vec()),
        })
    }
}

impl TryFrom<NodeMutationRecord> for MutationRecord {
    type Error = Error;

    fn try_from(value: NodeMutationRecord) -> Result<Self> {
        let kind = match value.kind.as_str() {
            "upsert" => MutationKind::Upsert,
            "delete" => MutationKind::Delete,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown mutation kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            kind,
            key: value.key.to_vec(),
            value: value.value.map(|value| value.to_vec()),
        })
    }
}

impl From<MutationRecord> for NodeMutationRecord {
    fn from(value: MutationRecord) -> Self {
        let kind = match value.kind {
            MutationKind::Upsert => "upsert",
            MutationKind::Delete => "delete",
        }
        .to_string();
        Self {
            kind,
            key: Buffer::from(value.key),
            value: value.value.map(Buffer::from),
        }
    }
}

impl From<EncodingRecord> for NodeEncodingRecord {
    fn from(value: EncodingRecord) -> Self {
        let kind = match value.kind {
            EncodingKind::Raw => "raw",
            EncodingKind::Cbor => "cbor",
            EncodingKind::Json => "json",
            EncodingKind::Custom => "custom",
        }
        .to_string();
        Self {
            kind,
            custom_name: value.custom_name,
        }
    }
}

impl TryFrom<NodeEncodingRecord> for EncodingRecord {
    type Error = Error;

    fn try_from(value: NodeEncodingRecord) -> Result<Self> {
        let kind = match value.kind.as_str() {
            "raw" => EncodingKind::Raw,
            "cbor" => EncodingKind::Cbor,
            "json" => EncodingKind::Json,
            "custom" => EncodingKind::Custom,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown encoding kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            kind,
            custom_name: value.custom_name,
        })
    }
}

impl From<ConfigRecord> for NodeConfigRecord {
    fn from(config: ConfigRecord) -> Self {
        Self {
            min_chunk_size: config.min_chunk_size.to_string(),
            max_chunk_size: config.max_chunk_size.to_string(),
            chunking_factor: config.chunking_factor,
            hash_seed: config.hash_seed.to_string(),
            encoding: config.encoding.into(),
            node_cache_max_nodes: config.node_cache_max_nodes.map(|value| value.to_string()),
            node_cache_max_bytes: config.node_cache_max_bytes.map(|value| value.to_string()),
        }
    }
}

impl TryFrom<NodeConfigRecord> for ConfigRecord {
    type Error = Error;

    fn try_from(value: NodeConfigRecord) -> Result<Self> {
        Ok(Self {
            min_chunk_size: parse_u64(&value.min_chunk_size)?,
            max_chunk_size: parse_u64(&value.max_chunk_size)?,
            chunking_factor: value.chunking_factor,
            hash_seed: parse_u64(&value.hash_seed)?,
            encoding: value.encoding.try_into()?,
            node_cache_max_nodes: value
                .node_cache_max_nodes
                .as_deref()
                .map(parse_u64)
                .transpose()?,
            node_cache_max_bytes: value
                .node_cache_max_bytes
                .as_deref()
                .map(parse_u64)
                .transpose()?,
        })
    }
}

impl From<BindingParallelConfigRecord> for NodeParallelConfigRecord {
    fn from(config: BindingParallelConfigRecord) -> Self {
        Self {
            max_threads: config.max_threads.to_string(),
            parallelism_threshold: config.parallelism_threshold.to_string(),
        }
    }
}

impl TryFrom<NodeParallelConfigRecord> for BindingParallelConfigRecord {
    type Error = Error;

    fn try_from(value: NodeParallelConfigRecord) -> Result<Self> {
        Ok(Self {
            max_threads: parse_u64(&value.max_threads)?,
            parallelism_threshold: parse_u64(&value.parallelism_threshold)?,
        })
    }
}

impl From<BindingBatchApplyStatsRecord> for NodeBatchApplyStatsRecord {
    fn from(stats: BindingBatchApplyStatsRecord) -> Self {
        Self {
            input_mutations: stats.input_mutations.to_string(),
            effective_mutations: stats.effective_mutations.to_string(),
            preprocess_input_sorted: stats.preprocess_input_sorted,
            affected_leaves: stats.affected_leaves.to_string(),
            changed_leaves: stats.changed_leaves.to_string(),
            sparse_leaf_applies: stats.sparse_leaf_applies.to_string(),
            written_nodes: stats.written_nodes.to_string(),
            written_bytes: stats.written_bytes.to_string(),
            used_append_fast_path: stats.used_append_fast_path,
            used_batched_route: stats.used_batched_route,
            used_coalesced_rebuild: stats.used_coalesced_rebuild,
            used_deferred_rebalancing: stats.used_deferred_rebalancing,
            used_bottom_up_rebuild: stats.used_bottom_up_rebuild,
            cache_written_nodes: stats.cache_written_nodes,
        }
    }
}

impl From<BindingBatchApplyResultRecord> for NodeBatchApplyResultRecord {
    fn from(result: BindingBatchApplyResultRecord) -> Self {
        Self {
            tree: result.tree.into(),
            stats: result.stats.into(),
        }
    }
}

impl From<BindingCrdtConfigRecord> for NodeCrdtConfigRecord {
    fn from(value: BindingCrdtConfigRecord) -> Self {
        let strategy = match value.strategy {
            CrdtMergeStrategyKind::LastWriterWins => "last_writer_wins",
            CrdtMergeStrategyKind::MultiValue => "multi_value",
        }
        .to_string();
        let delete_policy = match value.delete_policy {
            CrdtDeletePolicyKind::DeleteWins => "delete_wins",
            CrdtDeletePolicyKind::UpdateWins => "update_wins",
        }
        .to_string();
        Self {
            strategy,
            delete_policy,
        }
    }
}

impl TryFrom<NodeCrdtConfigRecord> for BindingCrdtConfigRecord {
    type Error = Error;

    fn try_from(value: NodeCrdtConfigRecord) -> Result<Self> {
        Ok(Self {
            strategy: crdt_merge_strategy_from_str(&value.strategy)?,
            delete_policy: crdt_delete_policy_from_str(&value.delete_policy)?,
        })
    }
}

impl From<BindingTimestampedValueRecord> for NodeTimestampedValueRecord {
    fn from(value: BindingTimestampedValueRecord) -> Self {
        Self {
            value: Buffer::from(value.value),
            timestamp: value.timestamp.to_string(),
        }
    }
}

impl TryFrom<NodeTimestampedValueRecord> for BindingTimestampedValueRecord {
    type Error = Error;

    fn try_from(value: NodeTimestampedValueRecord) -> Result<Self> {
        Ok(Self {
            value: value.value.to_vec(),
            timestamp: parse_u64(&value.timestamp)?,
        })
    }
}

impl From<BindingTombstoneMetadataRecord> for NodeTombstoneMetadataRecord {
    fn from(value: BindingTombstoneMetadataRecord) -> Self {
        Self {
            key: value.key,
            value: Buffer::from(value.value),
        }
    }
}

impl From<NodeTombstoneMetadataRecord> for BindingTombstoneMetadataRecord {
    fn from(value: NodeTombstoneMetadataRecord) -> Self {
        Self {
            key: value.key,
            value: value.value.to_vec(),
        }
    }
}

impl From<BindingTombstoneRecord> for NodeTombstoneRecord {
    fn from(value: BindingTombstoneRecord) -> Self {
        Self {
            actor: Buffer::from(value.actor),
            timestamp_millis: value.timestamp_millis.to_string(),
            causal_metadata: value.causal_metadata.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<NodeTombstoneRecord> for BindingTombstoneRecord {
    type Error = Error;

    fn try_from(value: NodeTombstoneRecord) -> Result<Self> {
        Ok(Self {
            actor: value.actor.to_vec(),
            timestamp_millis: parse_u64(&value.timestamp_millis)?,
            causal_metadata: value.causal_metadata.into_iter().map(Into::into).collect(),
        })
    }
}

impl From<RangeCursorRecord> for NodeRangeCursorRecord {
    fn from(cursor: RangeCursorRecord) -> Self {
        Self {
            after_key: cursor.after_key.map(Buffer::from),
        }
    }
}

impl From<NodeRangeCursorRecord> for RangeCursorRecord {
    fn from(cursor: NodeRangeCursorRecord) -> Self {
        Self {
            after_key: cursor.after_key.map(|value| value.to_vec()),
        }
    }
}

impl From<ReverseCursorRecord> for NodeReverseCursorRecord {
    fn from(cursor: ReverseCursorRecord) -> Self {
        Self {
            before_key: cursor.before_key.map(Buffer::from),
        }
    }
}

impl From<NodeReverseCursorRecord> for ReverseCursorRecord {
    fn from(cursor: NodeReverseCursorRecord) -> Self {
        Self {
            before_key: cursor.before_key.map(|value| value.to_vec()),
        }
    }
}

impl From<BindingRangeBoundsRecord> for NodeRangeBoundsRecord {
    fn from(bounds: BindingRangeBoundsRecord) -> Self {
        Self {
            start: Buffer::from(bounds.start),
            end: bounds.end.map(Buffer::from),
        }
    }
}

impl From<BindingRangePageRecord> for NodeRangePageRecord {
    fn from(page: BindingRangePageRecord) -> Self {
        Self {
            entries: page.entries.into_iter().map(Into::into).collect(),
            next_cursor: page.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingReversePageRecord> for NodeReversePageRecord {
    fn from(page: BindingReversePageRecord) -> Self {
        Self {
            entries: page.entries.into_iter().map(Into::into).collect(),
            next_cursor: page.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingCursorWindowRecord> for NodeCursorWindowRecord {
    fn from(window: BindingCursorWindowRecord) -> Self {
        Self {
            position_key: window.position_key.map(Buffer::from),
            position_value: window.position_value.map(Buffer::from),
            found: window.found,
            entries: window.entries.into_iter().map(Into::into).collect(),
            next_cursor: window.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingDiffPageRecord> for NodeDiffPageRecord {
    fn from(page: BindingDiffPageRecord) -> Self {
        Self {
            diffs: page.diffs.into_iter().map(Into::into).collect(),
            next_cursor: page.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingConflictRecord> for NodeConflictRecord {
    fn from(conflict: BindingConflictRecord) -> Self {
        Self {
            key: Buffer::from(conflict.key),
            base: conflict.base.map(Buffer::from),
            left: conflict.left.map(Buffer::from),
            right: conflict.right.map(Buffer::from),
        }
    }
}

impl From<NodeConflictRecord> for BindingConflictRecord {
    fn from(conflict: NodeConflictRecord) -> Self {
        Self {
            key: conflict.key.to_vec(),
            base: conflict.base.map(|value| value.to_vec()),
            left: conflict.left.map(|value| value.to_vec()),
            right: conflict.right.map(|value| value.to_vec()),
        }
    }
}

impl From<NodeResolutionRecord> for BindingResolutionRecord {
    fn from(resolution: NodeResolutionRecord) -> Self {
        let kind = match resolution.kind.as_str() {
            "value" => BindingResolutionKind::Value,
            "delete" => BindingResolutionKind::Delete,
            "unresolved" => BindingResolutionKind::Unresolved,
            _ => BindingResolutionKind::Unresolved,
        };
        Self {
            kind,
            value: resolution.value.map(|value| value.to_vec()),
        }
    }
}

impl From<BindingResolutionRecord> for NodeResolutionRecord {
    fn from(resolution: BindingResolutionRecord) -> Self {
        let kind = match resolution.kind {
            BindingResolutionKind::Value => "value",
            BindingResolutionKind::Delete => "delete",
            BindingResolutionKind::Unresolved => "unresolved",
        };
        Self {
            kind: kind.to_string(),
            value: resolution.value.map(Buffer::from),
        }
    }
}

impl From<NodeCrdtResolutionRecord> for BindingCrdtResolutionRecord {
    fn from(resolution: NodeCrdtResolutionRecord) -> Self {
        let kind = match resolution.kind.as_str() {
            "value" => BindingCrdtResolutionKind::Value,
            "delete" => BindingCrdtResolutionKind::Delete,
            _ => BindingCrdtResolutionKind::Delete,
        };
        Self {
            kind,
            value: resolution.value.map(|value| value.to_vec()),
        }
    }
}

impl From<BindingCrdtResolutionRecord> for NodeCrdtResolutionRecord {
    fn from(resolution: BindingCrdtResolutionRecord) -> Self {
        let kind = match resolution.kind {
            BindingCrdtResolutionKind::Value => "value",
            BindingCrdtResolutionKind::Delete => "delete",
        };
        Self {
            kind: kind.to_string(),
            value: resolution.value.map(Buffer::from),
        }
    }
}

fn unresolved_resolution() -> BindingResolutionRecord {
    BindingResolutionRecord {
        kind: BindingResolutionKind::Unresolved,
        value: None,
    }
}

fn delete_crdt_resolution() -> BindingCrdtResolutionRecord {
    BindingCrdtResolutionRecord {
        kind: BindingCrdtResolutionKind::Delete,
        value: None,
    }
}

impl From<BindingConflictPageRecord> for NodeConflictPageRecord {
    fn from(page: BindingConflictPageRecord) -> Self {
        Self {
            conflicts: page.conflicts.into_iter().map(Into::into).collect(),
            next_cursor: page.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingDiffTraversalStatsRecord> for NodeDiffTraversalStatsRecord {
    fn from(stats: BindingDiffTraversalStatsRecord) -> Self {
        Self {
            compared_nodes: stats.compared_nodes.to_string(),
            reused_subtrees: stats.reused_subtrees.to_string(),
            added_subtrees: stats.added_subtrees.to_string(),
            removed_subtrees: stats.removed_subtrees.to_string(),
            collected_fallbacks: stats.collected_fallbacks.to_string(),
            emitted_diffs: stats.emitted_diffs.to_string(),
        }
    }
}

impl From<BindingStructuralDiffPageRecord> for NodeStructuralDiffPageRecord {
    fn from(page: BindingStructuralDiffPageRecord) -> Self {
        Self {
            diffs: page.diffs.into_iter().map(Into::into).collect(),
            next_cursor_json: page.next_cursor_json,
            stats: page.stats.into(),
            next_cursor: page.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingStructuralDiffCursorRecord> for NodeStructuralDiffCursorRecord {
    fn from(cursor: BindingStructuralDiffCursorRecord) -> Self {
        Self {
            base_root: cursor.base_root.map(Buffer::from),
            other_root: cursor.other_root.map(Buffer::from),
            markers: cursor.markers.into_iter().map(Into::into).collect(),
            pending: cursor.pending.into_iter().map(Into::into).collect(),
        }
    }
}

impl TryFrom<NodeStructuralDiffCursorRecord> for BindingStructuralDiffCursorRecord {
    type Error = Error;

    fn try_from(cursor: NodeStructuralDiffCursorRecord) -> Result<Self> {
        Ok(Self {
            base_root: cursor.base_root.map(|value| value.to_vec()),
            other_root: cursor.other_root.map(|value| value.to_vec()),
            markers: cursor
                .markers
                .into_iter()
                .map(BindingStructuralDiffMarkerRecord::try_from)
                .collect::<Result<Vec<_>>>()?,
            pending: cursor
                .pending
                .into_iter()
                .map(BindingDiffRecord::try_from)
                .collect::<Result<Vec<_>>>()?,
        })
    }
}

impl From<BindingStructuralDiffMarkerRecord> for NodeStructuralDiffMarkerRecord {
    fn from(marker: BindingStructuralDiffMarkerRecord) -> Self {
        Self {
            kind: structural_diff_marker_kind(marker.kind).to_string(),
            base_cid: marker.base_cid.map(Buffer::from),
            other_cid: marker.other_cid.map(Buffer::from),
            span_end: marker.span_end.map(Buffer::from),
            cid: marker.cid.map(Buffer::from),
        }
    }
}

impl TryFrom<NodeStructuralDiffMarkerRecord> for BindingStructuralDiffMarkerRecord {
    type Error = Error;

    fn try_from(marker: NodeStructuralDiffMarkerRecord) -> Result<Self> {
        Ok(Self {
            kind: binding_structural_diff_marker_kind(&marker.kind)?,
            base_cid: marker.base_cid.map(|value| value.to_vec()),
            other_cid: marker.other_cid.map(|value| value.to_vec()),
            span_end: marker.span_end.map(|value| value.to_vec()),
            cid: marker.cid.map(|value| value.to_vec()),
        })
    }
}

impl From<BindingMergeTraceRecord> for NodeMergeTraceRecord {
    fn from(trace: BindingMergeTraceRecord) -> Self {
        Self {
            events: trace.events.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<BindingMergeTraceEventRecord> for NodeMergeTraceEventRecord {
    fn from(event: BindingMergeTraceEventRecord) -> Self {
        Self {
            kind: merge_trace_event_kind(event.kind).to_string(),
            fast_path: event
                .fast_path
                .map(|kind| merge_fast_path_kind(kind).to_string()),
            cid: event.cid.map(Buffer::from),
            reuse_reason: event
                .reuse_reason
                .map(|kind| merge_reuse_reason_kind(kind).to_string()),
            level: event.level.map(|value| value.to_string()),
            entries: event.entries.map(|value| value.to_string()),
            first_key: event.first_key.map(Buffer::from),
            last_key: event.last_key.map(Buffer::from),
            stage: event
                .stage
                .map(|kind| merge_trace_stage_kind(kind).to_string()),
            key: event.key.map(Buffer::from),
            resolution: event
                .resolution
                .map(|kind| merge_trace_resolution_kind(kind).to_string()),
            fallback_reason: event
                .fallback_reason
                .map(|kind| merge_fallback_reason_kind(kind).to_string()),
            diff_stats: event.diff_stats.map(Into::into),
            right_changes: event.right_changes.map(|value| value.to_string()),
            mutations: event.mutations.map(|value| value.to_string()),
            append_only: event.append_only,
        }
    }
}

fn merge_trace_event_kind(kind: BindingMergeTraceEventKind) -> &'static str {
    match kind {
        BindingMergeTraceEventKind::FastPath => "fast_path",
        BindingMergeTraceEventKind::StructuralMergeStarted => "structural_merge_started",
        BindingMergeTraceEventKind::ReusedSubtree => "reused_subtree",
        BindingMergeTraceEventKind::RewrittenNode => "rewritten_node",
        BindingMergeTraceEventKind::ResolverCalled => "resolver_called",
        BindingMergeTraceEventKind::Fallback => "fallback",
        BindingMergeTraceEventKind::DiffTraversal => "diff_traversal",
        BindingMergeTraceEventKind::BatchMerge => "batch_merge",
    }
}

fn merge_fast_path_kind(kind: BindingMergeFastPathKind) -> &'static str {
    match kind {
        BindingMergeFastPathKind::BranchesEqual => "branches_equal",
        BindingMergeFastPathKind::LeftUnchanged => "left_unchanged",
        BindingMergeFastPathKind::RightUnchanged => "right_unchanged",
    }
}

fn merge_reuse_reason_kind(kind: BindingMergeReuseReasonKind) -> &'static str {
    match kind {
        BindingMergeReuseReasonKind::BranchesEqual => "branches_equal",
        BindingMergeReuseReasonKind::LeftUnchanged => "left_unchanged",
        BindingMergeReuseReasonKind::RightUnchanged => "right_unchanged",
        BindingMergeReuseReasonKind::UnchangedAfterMerge => "unchanged_after_merge",
        BindingMergeReuseReasonKind::MatchesLeft => "matches_left",
        BindingMergeReuseReasonKind::MatchesRight => "matches_right",
    }
}

fn merge_trace_stage_kind(kind: BindingMergeTraceStageKind) -> &'static str {
    match kind {
        BindingMergeTraceStageKind::Structural => "structural",
        BindingMergeTraceStageKind::Batch => "batch",
    }
}

fn merge_trace_resolution_kind(kind: BindingMergeTraceResolutionKind) -> &'static str {
    match kind {
        BindingMergeTraceResolutionKind::Value => "value",
        BindingMergeTraceResolutionKind::Delete => "delete",
        BindingMergeTraceResolutionKind::Unresolved => "unresolved",
    }
}

fn merge_fallback_reason_kind(kind: BindingMergeFallbackReasonKind) -> &'static str {
    match kind {
        BindingMergeFallbackReasonKind::MissingRoot => "missing_root",
        BindingMergeFallbackReasonKind::ShapeMismatch => "shape_mismatch",
        BindingMergeFallbackReasonKind::NodeLengthMismatch => "node_length_mismatch",
        BindingMergeFallbackReasonKind::ChildFallback => "child_fallback",
        BindingMergeFallbackReasonKind::DeleteResolution => "delete_resolution",
        BindingMergeFallbackReasonKind::DiffBatch => "diff_batch",
    }
}

impl From<BindingMergeExplanationRecord> for NodeMergeExplanationRecord {
    fn from(explanation: BindingMergeExplanationRecord) -> Self {
        Self {
            result: explanation.result.map(Into::into),
            error: explanation.error,
            trace_json: explanation.trace_json,
            trace: explanation.trace.into(),
        }
    }
}

impl From<BindingNamedRootRecord> for NodeNamedRootRecord {
    fn from(root: BindingNamedRootRecord) -> Self {
        Self {
            name: Buffer::from(root.name),
            tree: root.tree.into(),
        }
    }
}

impl From<BindingRootManifestRecord> for NodeRootManifestRecord {
    fn from(manifest: BindingRootManifestRecord) -> Self {
        Self {
            tree: manifest.tree.into(),
            created_at_millis: manifest
                .created_at_millis
                .map(|timestamp| timestamp.to_string()),
            updated_at_millis: manifest
                .updated_at_millis
                .map(|timestamp| timestamp.to_string()),
        }
    }
}

impl From<BindingNamedRootManifestRecord> for NodeNamedRootManifestRecord {
    fn from(root: BindingNamedRootManifestRecord) -> Self {
        Self {
            name: Buffer::from(root.name),
            manifest: root.manifest.into(),
        }
    }
}

impl From<BindingNamedRootSelectionRecord> for NodeNamedRootSelectionRecord {
    fn from(selection: BindingNamedRootSelectionRecord) -> Self {
        Self {
            roots: selection.roots.into_iter().map(Into::into).collect(),
            missing_names: selection
                .missing_names
                .into_iter()
                .map(Buffer::from)
                .collect(),
        }
    }
}

impl From<BindingNamedRootUpdateRecord> for NodeNamedRootUpdateRecord {
    fn from(update: BindingNamedRootUpdateRecord) -> Self {
        Self {
            applied: update.applied,
            conflict: update.conflict,
            current: update.current.map(Into::into),
        }
    }
}

impl From<SnapshotNamespaceRecord> for NodeSnapshotNamespaceRecord {
    fn from(namespace: SnapshotNamespaceRecord) -> Self {
        let kind = match namespace.kind {
            SnapshotNamespaceKind::Branch => "branch",
            SnapshotNamespaceKind::Tag => "tag",
            SnapshotNamespaceKind::Checkpoint => "checkpoint",
            SnapshotNamespaceKind::Custom => "custom",
        };
        Self {
            kind: kind.to_string(),
            custom_prefix: namespace.custom_prefix.map(Buffer::from),
        }
    }
}

impl TryFrom<NodeSnapshotNamespaceRecord> for SnapshotNamespaceRecord {
    type Error = Error;

    fn try_from(value: NodeSnapshotNamespaceRecord) -> Result<Self> {
        let kind = match value.kind.as_str() {
            "branch" => SnapshotNamespaceKind::Branch,
            "tag" => SnapshotNamespaceKind::Tag,
            "checkpoint" => SnapshotNamespaceKind::Checkpoint,
            "custom" => SnapshotNamespaceKind::Custom,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown snapshot namespace kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            kind,
            custom_prefix: value.custom_prefix.map(|prefix| prefix.to_vec()),
        })
    }
}

impl From<BindingSnapshotRecord> for NodeSnapshotRecord {
    fn from(snapshot: BindingSnapshotRecord) -> Self {
        Self {
            id: Buffer::from(snapshot.id),
            name: Buffer::from(snapshot.name),
            tree: snapshot.tree.into(),
            created_at_millis: snapshot
                .created_at_millis
                .map(|timestamp| timestamp.to_string()),
            updated_at_millis: snapshot
                .updated_at_millis
                .map(|timestamp| timestamp.to_string()),
        }
    }
}

impl From<BindingSnapshotSelectionRecord> for NodeSnapshotSelectionRecord {
    fn from(selection: BindingSnapshotSelectionRecord) -> Self {
        Self {
            snapshots: selection.snapshots.into_iter().map(Into::into).collect(),
            missing_ids: selection
                .missing_ids
                .into_iter()
                .map(Buffer::from)
                .collect(),
        }
    }
}

impl NodeKeyProofRecord {
    fn try_from_binding(proof: BindingKeyProofRecord) -> Result<Self> {
        let root = proof.root.clone().map(Buffer::from);
        let key = Buffer::from(proof.key.clone());
        let path_node_bytes = key_proof_path_node_bytes(proof)
            .map_err(to_napi_error)?
            .into_iter()
            .map(Buffer::from)
            .collect();
        Ok(Self {
            root,
            key,
            path_node_bytes,
        })
    }

    fn into_binding(self) -> Result<BindingKeyProofRecord> {
        key_proof_from_node_bytes(
            self.root.map(|root| root.to_vec()),
            self.key.to_vec(),
            self.path_node_bytes
                .into_iter()
                .map(|node| node.to_vec())
                .collect(),
        )
        .map_err(to_napi_error)
    }
}

impl From<BindingKeyProofVerificationRecord> for NodeKeyProofVerificationRecord {
    fn from(verification: BindingKeyProofVerificationRecord) -> Self {
        Self {
            valid: verification.valid,
            exists: verification.exists,
            absence: verification.absence,
            root: verification.root.map(Buffer::from),
            key: Buffer::from(verification.key),
            value: verification.value.map(Buffer::from),
        }
    }
}

impl NodeMultiKeyProofRecord {
    fn try_from_binding(proof: BindingMultiKeyProofRecord) -> Result<Self> {
        let root = proof.root.clone().map(Buffer::from);
        let keys = proof.keys.iter().cloned().map(Buffer::from).collect();
        let path_node_bytes = multi_key_proof_path_node_bytes(proof)
            .map_err(to_napi_error)?
            .into_iter()
            .map(Buffer::from)
            .collect();
        Ok(Self {
            root,
            keys,
            path_node_bytes,
        })
    }

    fn into_binding(self) -> Result<BindingMultiKeyProofRecord> {
        multi_key_proof_from_node_bytes(
            self.root.map(|root| root.to_vec()),
            self.keys.into_iter().map(|key| key.to_vec()).collect(),
            self.path_node_bytes
                .into_iter()
                .map(|node| node.to_vec())
                .collect(),
        )
        .map_err(to_napi_error)
    }
}

impl From<BindingMultiKeyProofVerificationRecord> for NodeMultiKeyProofVerificationRecord {
    fn from(verification: BindingMultiKeyProofVerificationRecord) -> Self {
        Self {
            valid: verification.valid,
            root: verification.root.map(Buffer::from),
            results: verification.results.into_iter().map(Into::into).collect(),
        }
    }
}

impl NodeRangeProofRecord {
    fn try_from_binding(proof: BindingRangeProofRecord) -> Result<Self> {
        let root = proof.root.clone().map(Buffer::from);
        let start = Buffer::from(proof.start.clone());
        let end = proof.end.clone().map(Buffer::from);
        let path_node_bytes = range_proof_path_node_bytes(proof)
            .map_err(to_napi_error)?
            .into_iter()
            .map(Buffer::from)
            .collect();
        Ok(Self {
            root,
            start,
            end,
            path_node_bytes,
        })
    }

    fn into_binding(self) -> Result<BindingRangeProofRecord> {
        range_proof_from_node_bytes(
            self.root.map(|root| root.to_vec()),
            self.start.to_vec(),
            self.end.map(|value| value.to_vec()),
            self.path_node_bytes
                .into_iter()
                .map(|node| node.to_vec())
                .collect(),
        )
        .map_err(to_napi_error)
    }
}

impl From<BindingRangeProofVerificationRecord> for NodeRangeProofVerificationRecord {
    fn from(verification: BindingRangeProofVerificationRecord) -> Self {
        Self {
            valid: verification.valid,
            root: verification.root.map(Buffer::from),
            start: Buffer::from(verification.start),
            end: verification.end.map(Buffer::from),
            entries: verification.entries.into_iter().map(Into::into).collect(),
        }
    }
}

impl NodeRangePageProofRecord {
    fn try_from_binding(proof: BindingRangePageProofRecord) -> Result<Self> {
        let root = proof.root.clone().map(Buffer::from);
        let after = proof.after.clone().map(Buffer::from);
        let end = proof.end.clone().map(Buffer::from);
        let path_node_bytes = range_page_proof_path_node_bytes(proof)
            .map_err(to_napi_error)?
            .into_iter()
            .map(Buffer::from)
            .collect();
        Ok(Self {
            root,
            after,
            end,
            path_node_bytes,
        })
    }

    fn into_binding(self) -> Result<BindingRangePageProofRecord> {
        range_page_proof_from_node_bytes(
            self.root.map(|root| root.to_vec()),
            self.after.map(|value| value.to_vec()),
            self.end.map(|value| value.to_vec()),
            self.path_node_bytes
                .into_iter()
                .map(|node| node.to_vec())
                .collect(),
        )
        .map_err(to_napi_error)
    }
}

impl From<BindingRangePageProofVerificationRecord> for NodeRangePageProofVerificationRecord {
    fn from(verification: BindingRangePageProofVerificationRecord) -> Self {
        Self {
            valid: verification.valid,
            root: verification.root.map(Buffer::from),
            after: verification.after.map(Buffer::from),
            end: verification.end.map(Buffer::from),
            entries: verification.entries.into_iter().map(Into::into).collect(),
        }
    }
}

impl NodeProvedRangePageRecord {
    fn try_from_binding(value: BindingProvedRangePageRecord) -> Result<Self> {
        Ok(Self {
            page: value.page.into(),
            proof: NodeRangePageProofRecord::try_from_binding(value.proof)?,
        })
    }
}

impl NodeDiffPageProofRecord {
    fn try_from_binding(proof: BindingDiffPageProofRecord) -> Result<Self> {
        Ok(Self {
            base: NodeRangePageProofRecord::try_from_binding(proof.base)?,
            other: NodeRangePageProofRecord::try_from_binding(proof.other)?,
            lookahead_base: proof
                .lookahead_base
                .map(NodeKeyProofRecord::try_from_binding)
                .transpose()?,
            lookahead_other: proof
                .lookahead_other
                .map(NodeKeyProofRecord::try_from_binding)
                .transpose()?,
            requested_end: proof.requested_end.map(Buffer::from),
            limit: proof.limit.to_string(),
        })
    }

    fn into_binding(self) -> Result<BindingDiffPageProofRecord> {
        Ok(BindingDiffPageProofRecord {
            base: self.base.into_binding()?,
            other: self.other.into_binding()?,
            lookahead_base: self
                .lookahead_base
                .map(NodeKeyProofRecord::into_binding)
                .transpose()?,
            lookahead_other: self
                .lookahead_other
                .map(NodeKeyProofRecord::into_binding)
                .transpose()?,
            requested_end: self.requested_end.map(|value| value.to_vec()),
            limit: parse_u64(&self.limit)?,
        })
    }
}

impl From<BindingDiffPageProofVerificationRecord> for NodeDiffPageProofVerificationRecord {
    fn from(value: BindingDiffPageProofVerificationRecord) -> Self {
        Self {
            valid: value.valid,
            base_valid: value.base_valid,
            other_valid: value.other_valid,
            lookahead_valid: value.lookahead_valid,
            base_root: value.base_root.map(Buffer::from),
            other_root: value.other_root.map(Buffer::from),
            after: value.after.map(Buffer::from),
            requested_end: value.requested_end.map(Buffer::from),
            proof_end: value.proof_end.map(Buffer::from),
            limit: value.limit.to_string(),
            diffs: value.diffs.into_iter().map(Into::into).collect(),
            next_cursor: value.next_cursor.map(Into::into),
        }
    }
}

impl NodeProvedDiffPageRecord {
    fn try_from_binding(value: BindingProvedDiffPageRecord) -> Result<Self> {
        Ok(Self {
            page: value.page.into(),
            proof: NodeDiffPageProofRecord::try_from_binding(value.proof)?,
        })
    }
}

impl From<BindingProofBundleSummaryRecord> for NodeProofBundleSummaryRecord {
    fn from(value: BindingProofBundleSummaryRecord) -> Self {
        Self {
            version: value.version.to_string(),
            kind: value.kind,
            root: value.root.map(Buffer::from),
            other_root: value.other_root.map(Buffer::from),
            key_count: value.key_count.to_string(),
            path_node_count: value.path_node_count.to_string(),
            start: value.start.map(Buffer::from),
            end: value.end.map(Buffer::from),
            after: value.after.map(Buffer::from),
            requested_end: value.requested_end.map(Buffer::from),
            limit: value.limit.map(|limit| limit.to_string()),
            has_lookahead: value.has_lookahead,
        }
    }
}

impl From<BindingProofBundleVerificationRecord> for NodeProofBundleVerificationRecord {
    fn from(value: BindingProofBundleVerificationRecord) -> Self {
        Self {
            summary: value.summary.into(),
            valid: value.valid,
            exists_count: value.exists_count.to_string(),
            absence_count: value.absence_count.to_string(),
            entry_count: value.entry_count.to_string(),
            diff_count: value.diff_count.to_string(),
            next_cursor: value.next_cursor.map(Into::into),
        }
    }
}

impl From<BindingAuthenticatedProofEnvelopeRecord> for NodeAuthenticatedProofEnvelopeRecord {
    fn from(value: BindingAuthenticatedProofEnvelopeRecord) -> Self {
        Self {
            algorithm: value.algorithm,
            key_id: Buffer::from(value.key_id),
            proof_bundle: Buffer::from(value.proof_bundle),
            context: Buffer::from(value.context),
            issued_at_millis: value
                .issued_at_millis
                .map(|timestamp| timestamp.to_string()),
            expires_at_millis: value
                .expires_at_millis
                .map(|timestamp| timestamp.to_string()),
            nonce: Buffer::from(value.nonce),
            signature: Buffer::from(value.signature),
        }
    }
}

impl TryFrom<NodeAuthenticatedProofEnvelopeRecord> for BindingAuthenticatedProofEnvelopeRecord {
    type Error = Error;

    fn try_from(value: NodeAuthenticatedProofEnvelopeRecord) -> Result<Self> {
        Ok(Self {
            algorithm: value.algorithm,
            key_id: value.key_id.to_vec(),
            proof_bundle: value.proof_bundle.to_vec(),
            context: value.context.to_vec(),
            issued_at_millis: value
                .issued_at_millis
                .as_deref()
                .map(parse_u64)
                .transpose()?,
            expires_at_millis: value
                .expires_at_millis
                .as_deref()
                .map(parse_u64)
                .transpose()?,
            nonce: value.nonce.to_vec(),
            signature: value.signature.to_vec(),
        })
    }
}

impl From<BindingAuthenticatedProofEnvelopeVerificationRecord>
    for NodeAuthenticatedProofEnvelopeVerificationRecord
{
    fn from(value: BindingAuthenticatedProofEnvelopeVerificationRecord) -> Self {
        Self {
            valid: value.valid,
            signature_valid: value.signature_valid,
            time_valid: value.time_valid,
            not_yet_valid: value.not_yet_valid,
            expired: value.expired,
            algorithm: value.algorithm,
            key_id: Buffer::from(value.key_id),
            proof_bundle: Buffer::from(value.proof_bundle),
            context: Buffer::from(value.context),
            issued_at_millis: value
                .issued_at_millis
                .map(|timestamp| timestamp.to_string()),
            expires_at_millis: value
                .expires_at_millis
                .map(|timestamp| timestamp.to_string()),
            nonce: Buffer::from(value.nonce),
        }
    }
}

impl From<BindingAuthenticatedProofBundleVerificationRecord>
    for NodeAuthenticatedProofBundleVerificationRecord
{
    fn from(value: BindingAuthenticatedProofBundleVerificationRecord) -> Self {
        Self {
            valid: value.valid,
            envelope: value.envelope.into(),
            proof: value.proof.map(Into::into),
            proof_error: value.proof_error,
        }
    }
}

impl TryFrom<NodeNamedRootRetentionRecord> for NamedRootRetentionRecord {
    type Error = Error;

    fn try_from(value: NodeNamedRootRetentionRecord) -> Result<Self> {
        let kind = match value.kind.as_str() {
            "all" => NamedRootRetentionKind::All,
            "exact" => NamedRootRetentionKind::Exact,
            "prefix" => NamedRootRetentionKind::Prefix,
            "newest_by_name" => NamedRootRetentionKind::NewestByName,
            "updated_since" => NamedRootRetentionKind::UpdatedSince,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown named root retention kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            kind,
            names: value.names.into_iter().map(|name| name.to_vec()).collect(),
            prefix: value
                .prefix
                .map(|prefix| prefix.to_vec())
                .unwrap_or_default(),
            count: value.count.as_deref().map(parse_u64).transpose()?,
            min_updated_at_millis: value
                .min_updated_at_millis
                .as_deref()
                .map(parse_u64)
                .transpose()?,
        })
    }
}

impl From<NamedRootRetentionRecord> for NodeNamedRootRetentionRecord {
    fn from(value: NamedRootRetentionRecord) -> Self {
        let kind = match value.kind {
            NamedRootRetentionKind::All => "all",
            NamedRootRetentionKind::Exact => "exact",
            NamedRootRetentionKind::Prefix => "prefix",
            NamedRootRetentionKind::NewestByName => "newest_by_name",
            NamedRootRetentionKind::UpdatedSince => "updated_since",
        };
        Self {
            kind: kind.to_string(),
            names: value.names.into_iter().map(Buffer::from).collect(),
            prefix: Some(Buffer::from(value.prefix)),
            count: value.count.map(|count| count.to_string()),
            min_updated_at_millis: value
                .min_updated_at_millis
                .map(|min_updated_at_millis| min_updated_at_millis.to_string()),
        }
    }
}

impl From<BindingCacheStatsRecord> for NodeCacheStatsRecord {
    fn from(stats: BindingCacheStatsRecord) -> Self {
        Self {
            cached_nodes: stats.cached_nodes.to_string(),
            cached_bytes: stats.cached_bytes.to_string(),
            pinned_nodes: stats.pinned_nodes.to_string(),
            pinned_bytes: stats.pinned_bytes.to_string(),
        }
    }
}

impl From<BindingMetricsRecord> for NodeMetricsRecord {
    fn from(metrics: BindingMetricsRecord) -> Self {
        Self {
            node_cache_hits: metrics.node_cache_hits.to_string(),
            node_cache_misses: metrics.node_cache_misses.to_string(),
            node_cache_evictions: metrics.node_cache_evictions.to_string(),
            nodes_read: metrics.nodes_read.to_string(),
            bytes_read: metrics.bytes_read.to_string(),
            nodes_written: metrics.nodes_written.to_string(),
            bytes_written: metrics.bytes_written.to_string(),
            store_get_calls: metrics.store_get_calls.to_string(),
            store_batch_get_calls: metrics.store_batch_get_calls.to_string(),
            store_batch_get_keys: metrics.store_batch_get_keys.to_string(),
            store_put_calls: metrics.store_put_calls.to_string(),
            store_batch_put_calls: metrics.store_batch_put_calls.to_string(),
            store_batch_put_nodes: metrics.store_batch_put_nodes.to_string(),
        }
    }
}

impl From<NodeChangedSpanRecord> for BindingChangedSpanRecord {
    fn from(span: NodeChangedSpanRecord) -> Self {
        Self {
            start: span.start.to_vec(),
            end: span.end.map(|value| value.to_vec()),
        }
    }
}

impl From<BindingChangedSpanRecord> for NodeChangedSpanRecord {
    fn from(span: BindingChangedSpanRecord) -> Self {
        Self {
            start: Buffer::from(span.start),
            end: span.end.map(Buffer::from),
        }
    }
}

impl From<BindingChangedSpanHintRecord> for NodeChangedSpanHintRecord {
    fn from(hint: BindingChangedSpanHintRecord) -> Self {
        Self {
            base_root: hint.base_root.map(Buffer::from),
            changed_root: hint.changed_root.map(Buffer::from),
            spans: hint.spans.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<BindingGcReachabilityRecord> for NodeGcReachabilityRecord {
    fn from(reachability: BindingGcReachabilityRecord) -> Self {
        Self {
            live_cids: reachability
                .live_cids
                .into_iter()
                .map(Buffer::from)
                .collect(),
            live_nodes: reachability.live_nodes.to_string(),
            live_bytes: reachability.live_bytes.to_string(),
            leaf_nodes: reachability.leaf_nodes.to_string(),
            internal_nodes: reachability.internal_nodes.to_string(),
        }
    }
}

impl From<BindingGcPlanRecord> for NodeGcPlanRecord {
    fn from(plan: BindingGcPlanRecord) -> Self {
        Self {
            reachability: plan.reachability.into(),
            candidate_nodes: plan.candidate_nodes.to_string(),
            reclaimable_cids: plan
                .reclaimable_cids
                .into_iter()
                .map(Buffer::from)
                .collect(),
            reclaimable_nodes: plan.reclaimable_nodes.to_string(),
            reclaimable_bytes: plan.reclaimable_bytes.to_string(),
            missing_candidates: plan.missing_candidates.to_string(),
        }
    }
}

impl From<BindingGcSweepRecord> for NodeGcSweepRecord {
    fn from(sweep: BindingGcSweepRecord) -> Self {
        Self {
            plan: sweep.plan.into(),
            deleted_nodes: sweep.deleted_nodes.to_string(),
            deleted_bytes: sweep.deleted_bytes.to_string(),
        }
    }
}

impl From<BindingMissingNodePlanRecord> for NodeMissingNodePlanRecord {
    fn from(plan: BindingMissingNodePlanRecord) -> Self {
        Self {
            required_cids: plan.required_cids.into_iter().map(Buffer::from).collect(),
            required_nodes: plan.required_nodes.to_string(),
            required_bytes: plan.required_bytes.to_string(),
            missing_cids: plan.missing_cids.into_iter().map(Buffer::from).collect(),
            missing_nodes: plan.missing_nodes.to_string(),
            missing_bytes: plan.missing_bytes.to_string(),
        }
    }
}

impl From<BindingMissingNodeCopyRecord> for NodeMissingNodeCopyRecord {
    fn from(copy: BindingMissingNodeCopyRecord) -> Self {
        Self {
            plan: copy.plan.into(),
            copied_nodes: copy.copied_nodes.to_string(),
            copied_bytes: copy.copied_bytes.to_string(),
        }
    }
}

impl From<BindingSnapshotBundleNodeRecord> for NodeSnapshotBundleNodeRecord {
    fn from(node: BindingSnapshotBundleNodeRecord) -> Self {
        Self {
            cid: Buffer::from(node.cid),
            bytes: Buffer::from(node.bytes),
        }
    }
}

impl From<NodeSnapshotBundleNodeRecord> for BindingSnapshotBundleNodeRecord {
    fn from(node: NodeSnapshotBundleNodeRecord) -> Self {
        Self {
            cid: node.cid.to_vec(),
            bytes: node.bytes.to_vec(),
        }
    }
}

impl From<BindingSnapshotBundleRecord> for NodeSnapshotBundleRecord {
    fn from(bundle: BindingSnapshotBundleRecord) -> Self {
        Self {
            format_version: bundle.format_version,
            tree: bundle.tree.into(),
            nodes: bundle.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

impl NodeSnapshotBundleRecord {
    fn into_binding(self, config: ConfigRecord) -> BindingSnapshotBundleRecord {
        BindingSnapshotBundleRecord {
            format_version: self.format_version,
            tree: self.tree.into_tree(config),
            nodes: self.nodes.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<BindingSnapshotBundleSummaryRecord> for NodeSnapshotBundleSummaryRecord {
    fn from(summary: BindingSnapshotBundleSummaryRecord) -> Self {
        Self {
            format_version: summary.format_version,
            root: summary.root.map(Buffer::from),
            node_count: summary.node_count.to_string(),
            byte_count: summary.byte_count.to_string(),
            min_node_bytes: summary.min_node_bytes.to_string(),
            max_node_bytes: summary.max_node_bytes.to_string(),
        }
    }
}

impl From<BindingSnapshotBundleVerificationRecord> for NodeSnapshotBundleVerificationRecord {
    fn from(verification: BindingSnapshotBundleVerificationRecord) -> Self {
        Self {
            valid: verification.valid,
            summary: verification.summary.into(),
            reachable_nodes: verification.reachable_nodes.to_string(),
            reachable_bytes: verification.reachable_bytes.to_string(),
            missing_cids: verification
                .missing_cids
                .into_iter()
                .map(Buffer::from)
                .collect(),
            extra_cids: verification
                .extra_cids
                .into_iter()
                .map(Buffer::from)
                .collect(),
        }
    }
}

impl From<BindingBlobRefRecord> for NodeBlobRefRecord {
    fn from(reference: BindingBlobRefRecord) -> Self {
        Self {
            cid: Buffer::from(reference.cid),
            len: reference.len.to_string(),
        }
    }
}

impl TryFrom<NodeBlobRefRecord> for BindingBlobRefRecord {
    type Error = Error;

    fn try_from(value: NodeBlobRefRecord) -> Result<Self> {
        Ok(Self {
            cid: value.cid.to_vec(),
            len: parse_u64(&value.len)?,
        })
    }
}

impl From<BindingLargeValueConfigRecord> for NodeLargeValueConfigRecord {
    fn from(config: BindingLargeValueConfigRecord) -> Self {
        Self {
            inline_threshold: config.inline_threshold.to_string(),
        }
    }
}

impl TryFrom<NodeLargeValueConfigRecord> for BindingLargeValueConfigRecord {
    type Error = Error;

    fn try_from(value: NodeLargeValueConfigRecord) -> Result<Self> {
        Ok(Self {
            inline_threshold: parse_u64(&value.inline_threshold)?,
        })
    }
}

impl From<BindingValueRefRecord> for NodeValueRefRecord {
    fn from(reference: BindingValueRefRecord) -> Self {
        let kind = match reference.kind {
            ValueRefKind::Inline => "inline",
            ValueRefKind::Blob => "blob",
        }
        .to_string();
        Self {
            kind,
            value: reference.value.map(Buffer::from),
            blob: reference.blob.map(Into::into),
        }
    }
}

impl From<BindingBlobGcReachabilityRecord> for NodeBlobGcReachabilityRecord {
    fn from(reachability: BindingBlobGcReachabilityRecord) -> Self {
        Self {
            live_blobs: reachability
                .live_blobs
                .into_iter()
                .map(Into::into)
                .collect(),
            live_blob_count: reachability.live_blob_count.to_string(),
            live_blob_bytes: reachability.live_blob_bytes.to_string(),
            scanned_nodes: reachability.scanned_nodes.to_string(),
            scanned_values: reachability.scanned_values.to_string(),
        }
    }
}

impl From<BindingBlobGcPlanRecord> for NodeBlobGcPlanRecord {
    fn from(plan: BindingBlobGcPlanRecord) -> Self {
        Self {
            reachability: plan.reachability.into(),
            candidate_blobs: plan.candidate_blobs.to_string(),
            reclaimable_blobs: plan.reclaimable_blobs.into_iter().map(Into::into).collect(),
            reclaimable_blob_count: plan.reclaimable_blob_count.to_string(),
            reclaimable_blob_bytes: plan.reclaimable_blob_bytes.to_string(),
            missing_candidates: plan.missing_candidates.to_string(),
        }
    }
}

impl From<BindingBlobGcSweepRecord> for NodeBlobGcSweepRecord {
    fn from(sweep: BindingBlobGcSweepRecord) -> Self {
        Self {
            plan: sweep.plan.into(),
            deleted_blobs: sweep.deleted_blobs.to_string(),
            deleted_blob_bytes: sweep.deleted_blob_bytes.to_string(),
        }
    }
}

#[napi(js_name = "cidFromBytes")]
pub fn cid_from_bytes_native(bytes: Buffer) -> Buffer {
    Buffer::from(cid_from_bytes(bytes.to_vec()))
}

#[napi(js_name = "nodeBytesRoundTrip")]
pub fn node_bytes_round_trip(bytes: Buffer) -> Result<Buffer> {
    let node = node_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    node_to_bytes(node).map(Buffer::from).map_err(to_napi_error)
}

#[napi(js_name = "nodeCidFromBytes")]
pub fn node_cid_from_bytes(bytes: Buffer) -> Result<Buffer> {
    let node = node_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    node_cid(node).map(Buffer::from).map_err(to_napi_error)
}

#[napi(js_name = "verifyKeyProof")]
pub fn verify_key_proof_native(
    proof: NodeKeyProofRecord,
) -> Result<NodeKeyProofVerificationRecord> {
    let proof = proof.into_binding()?;
    verify_key_proof(proof)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifyMultiKeyProof")]
pub fn verify_multi_key_proof_native(
    proof: NodeMultiKeyProofRecord,
) -> Result<NodeMultiKeyProofVerificationRecord> {
    let proof = proof.into_binding()?;
    verify_multi_key_proof(proof)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifyRangeProof")]
pub fn verify_range_proof_native(
    proof: NodeRangeProofRecord,
) -> Result<NodeRangeProofVerificationRecord> {
    let proof = proof.into_binding()?;
    verify_range_proof(proof)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifyRangePageProof")]
pub fn verify_range_page_proof_native(
    proof: NodeRangePageProofRecord,
) -> Result<NodeRangePageProofVerificationRecord> {
    let proof = proof.into_binding()?;
    verify_range_page_proof(proof)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifyDiffPageProof")]
pub fn verify_diff_page_proof_native(
    proof: NodeDiffPageProofRecord,
) -> Result<NodeDiffPageProofVerificationRecord> {
    let proof = proof.into_binding()?;
    verify_diff_page_proof(proof)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "keyProofToBytes")]
pub fn key_proof_to_bytes_native(proof: NodeKeyProofRecord) -> Result<Buffer> {
    let proof = proof.into_binding()?;
    key_proof_to_bytes(proof)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "keyProofFromBytes")]
pub fn key_proof_from_bytes_native(bytes: Buffer) -> Result<NodeKeyProofRecord> {
    let proof = key_proof_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    NodeKeyProofRecord::try_from_binding(proof)
}

#[napi(js_name = "multiKeyProofToBytes")]
pub fn multi_key_proof_to_bytes_native(proof: NodeMultiKeyProofRecord) -> Result<Buffer> {
    let proof = proof.into_binding()?;
    multi_key_proof_to_bytes(proof)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "multiKeyProofFromBytes")]
pub fn multi_key_proof_from_bytes_native(bytes: Buffer) -> Result<NodeMultiKeyProofRecord> {
    let proof = multi_key_proof_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    NodeMultiKeyProofRecord::try_from_binding(proof)
}

#[napi(js_name = "rangeProofToBytes")]
pub fn range_proof_to_bytes_native(proof: NodeRangeProofRecord) -> Result<Buffer> {
    let proof = proof.into_binding()?;
    range_proof_to_bytes(proof)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "rangeProofFromBytes")]
pub fn range_proof_from_bytes_native(bytes: Buffer) -> Result<NodeRangeProofRecord> {
    let proof = range_proof_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    NodeRangeProofRecord::try_from_binding(proof)
}

#[napi(js_name = "rangePageProofToBytes")]
pub fn range_page_proof_to_bytes_native(proof: NodeRangePageProofRecord) -> Result<Buffer> {
    let proof = proof.into_binding()?;
    range_page_proof_to_bytes(proof)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "rangePageProofFromBytes")]
pub fn range_page_proof_from_bytes_native(bytes: Buffer) -> Result<NodeRangePageProofRecord> {
    let proof = range_page_proof_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    NodeRangePageProofRecord::try_from_binding(proof)
}

#[napi(js_name = "diffPageProofToBytes")]
pub fn diff_page_proof_to_bytes_native(proof: NodeDiffPageProofRecord) -> Result<Buffer> {
    let proof = proof.into_binding()?;
    diff_page_proof_to_bytes(proof)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "diffPageProofFromBytes")]
pub fn diff_page_proof_from_bytes_native(bytes: Buffer) -> Result<NodeDiffPageProofRecord> {
    let proof = diff_page_proof_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    NodeDiffPageProofRecord::try_from_binding(proof)
}

#[napi(js_name = "inspectProofBundle")]
pub fn inspect_proof_bundle_native(bytes: Buffer) -> Result<NodeProofBundleSummaryRecord> {
    inspect_proof_bundle(bytes.to_vec())
        .map(NodeProofBundleSummaryRecord::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifyProofBundle")]
pub fn verify_proof_bundle_native(bytes: Buffer) -> Result<NodeProofBundleVerificationRecord> {
    verify_proof_bundle(bytes.to_vec())
        .map(NodeProofBundleVerificationRecord::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "signProofBundleHmacSha256")]
pub fn sign_proof_bundle_hmac_sha256_native(
    proof_bundle: Buffer,
    key_id: Buffer,
    secret: Buffer,
    context: Buffer,
    issued_at_millis: Option<String>,
    expires_at_millis: Option<String>,
    nonce: Buffer,
) -> Result<NodeAuthenticatedProofEnvelopeRecord> {
    sign_proof_bundle_hmac_sha256(
        proof_bundle.to_vec(),
        key_id.to_vec(),
        secret.to_vec(),
        context.to_vec(),
        issued_at_millis.as_deref().map(parse_u64).transpose()?,
        expires_at_millis.as_deref().map(parse_u64).transpose()?,
        nonce.to_vec(),
    )
    .map(Into::into)
    .map_err(to_napi_error)
}

#[napi(js_name = "verifyAuthenticatedProofEnvelope")]
pub fn verify_authenticated_proof_envelope_native(
    envelope: NodeAuthenticatedProofEnvelopeRecord,
    secret: Buffer,
    now_millis: Option<String>,
) -> Result<NodeAuthenticatedProofEnvelopeVerificationRecord> {
    let envelope = BindingAuthenticatedProofEnvelopeRecord::try_from(envelope)?;
    Ok(verify_authenticated_proof_envelope(
        envelope,
        secret.to_vec(),
        now_millis.as_deref().map(parse_u64).transpose()?,
    )
    .into())
}

#[napi(js_name = "verifyAuthenticatedProofBundle")]
pub fn verify_authenticated_proof_bundle_native(
    envelope_bytes: Buffer,
    secret: Buffer,
    now_millis: Option<String>,
) -> Result<NodeAuthenticatedProofBundleVerificationRecord> {
    verify_authenticated_proof_bundle(
        envelope_bytes.to_vec(),
        secret.to_vec(),
        now_millis.as_deref().map(parse_u64).transpose()?,
    )
    .map(NodeAuthenticatedProofBundleVerificationRecord::from)
    .map_err(to_napi_error)
}

#[napi(js_name = "authenticatedProofEnvelopeToBytes")]
pub fn authenticated_proof_envelope_to_bytes_native(
    envelope: NodeAuthenticatedProofEnvelopeRecord,
) -> Result<Buffer> {
    let envelope = BindingAuthenticatedProofEnvelopeRecord::try_from(envelope)?;
    authenticated_proof_envelope_to_bytes(envelope)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "authenticatedProofEnvelopeFromBytes")]
pub fn authenticated_proof_envelope_from_bytes_native(
    bytes: Buffer,
) -> Result<NodeAuthenticatedProofEnvelopeRecord> {
    authenticated_proof_envelope_from_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "keyProofFromNodeBytes")]
pub fn key_proof_from_node_bytes_native(
    root: Option<Buffer>,
    key: Buffer,
    path_node_bytes: Vec<Buffer>,
) -> Result<NodeKeyProofRecord> {
    let proof = key_proof_from_node_bytes(
        root.map(|root| root.to_vec()),
        key.to_vec(),
        path_node_bytes
            .into_iter()
            .map(|node| node.to_vec())
            .collect(),
    )
    .map_err(to_napi_error)?;
    NodeKeyProofRecord::try_from_binding(proof)
}

#[napi(js_name = "multiKeyProofFromNodeBytes")]
pub fn multi_key_proof_from_node_bytes_native(
    root: Option<Buffer>,
    keys: Vec<Buffer>,
    path_node_bytes: Vec<Buffer>,
) -> Result<NodeMultiKeyProofRecord> {
    let proof = multi_key_proof_from_node_bytes(
        root.map(|root| root.to_vec()),
        keys.into_iter().map(|key| key.to_vec()).collect(),
        path_node_bytes
            .into_iter()
            .map(|node| node.to_vec())
            .collect(),
    )
    .map_err(to_napi_error)?;
    NodeMultiKeyProofRecord::try_from_binding(proof)
}

#[napi(js_name = "rangeProofFromNodeBytes")]
pub fn range_proof_from_node_bytes_native(
    root: Option<Buffer>,
    start: Buffer,
    end: Option<Buffer>,
    path_node_bytes: Vec<Buffer>,
) -> Result<NodeRangeProofRecord> {
    let proof = range_proof_from_node_bytes(
        root.map(|root| root.to_vec()),
        start.to_vec(),
        end.map(|value| value.to_vec()),
        path_node_bytes
            .into_iter()
            .map(|node| node.to_vec())
            .collect(),
    )
    .map_err(to_napi_error)?;
    NodeRangeProofRecord::try_from_binding(proof)
}

#[napi(js_name = "rangePageProofFromNodeBytes")]
pub fn range_page_proof_from_node_bytes_native(
    root: Option<Buffer>,
    after: Option<Buffer>,
    end: Option<Buffer>,
    path_node_bytes: Vec<Buffer>,
) -> Result<NodeRangePageProofRecord> {
    let proof = range_page_proof_from_node_bytes(
        root.map(|root| root.to_vec()),
        after.map(|value| value.to_vec()),
        end.map(|value| value.to_vec()),
        path_node_bytes
            .into_iter()
            .map(|node| node.to_vec())
            .collect(),
    )
    .map_err(to_napi_error)?;
    NodeRangePageProofRecord::try_from_binding(proof)
}

#[napi(js_name = "isBoundaryConfigJson")]
pub fn is_boundary_config_json(
    config_json: String,
    count: String,
    key: Buffer,
    value: Buffer,
) -> Result<bool> {
    let config = config_from_json(&config_json)?;
    let count = count
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    is_boundary_config(config, count, key.to_vec(), value.to_vec()).map_err(to_napi_error)
}

#[napi(js_name = "prefixEnd")]
pub fn prefix_end_native(prefix: Buffer) -> Option<Buffer> {
    prefix_end(prefix.to_vec()).map(Buffer::from)
}

#[napi(js_name = "prefixRange")]
pub fn prefix_range_native(prefix: Buffer) -> NodeRangeBoundsRecord {
    prefix_range(prefix.to_vec()).into()
}

#[napi(js_name = "rangeCursorStart")]
pub fn range_cursor_start_native() -> NodeRangeCursorRecord {
    range_cursor_start().into()
}

#[napi(js_name = "rangeCursorAfterKey")]
pub fn range_cursor_after_key_native(key: Buffer) -> NodeRangeCursorRecord {
    range_cursor_after_key(key.to_vec()).into()
}

#[napi(js_name = "reverseCursorEnd")]
pub fn reverse_cursor_end_native() -> NodeReverseCursorRecord {
    reverse_cursor_end().into()
}

#[napi(js_name = "reverseCursorBeforeKey")]
pub fn reverse_cursor_before_key_native(key: Buffer) -> NodeReverseCursorRecord {
    reverse_cursor_before_key(key.to_vec()).into()
}

#[napi(js_name = "upsertMutation")]
pub fn upsert_mutation_native(key: Buffer, value: Buffer) -> NodeMutationRecord {
    upsert_mutation(key.to_vec(), value.to_vec()).into()
}

#[napi(js_name = "deleteMutation")]
pub fn delete_mutation_native(key: Buffer) -> NodeMutationRecord {
    delete_mutation(key.to_vec()).into()
}

#[napi(js_name = "resolutionValue")]
pub fn resolution_value_native(value: Buffer) -> NodeResolutionRecord {
    resolution_value(value.to_vec()).into()
}

#[napi(js_name = "resolutionDelete")]
pub fn resolution_delete_native() -> NodeResolutionRecord {
    resolution_delete().into()
}

#[napi(js_name = "resolutionUnresolved")]
pub fn resolution_unresolved_native() -> NodeResolutionRecord {
    resolution_unresolved().into()
}

#[napi(js_name = "resolvePreferLeft")]
pub fn resolve_prefer_left_native(conflict: NodeConflictRecord) -> NodeResolutionRecord {
    resolve_prefer_left(conflict.into()).into()
}

#[napi(js_name = "resolvePreferRight")]
pub fn resolve_prefer_right_native(conflict: NodeConflictRecord) -> NodeResolutionRecord {
    resolve_prefer_right(conflict.into()).into()
}

#[napi(js_name = "resolveDeleteWins")]
pub fn resolve_delete_wins_native(conflict: NodeConflictRecord) -> NodeResolutionRecord {
    resolve_delete_wins(conflict.into()).into()
}

#[napi(js_name = "resolveUpdateWins")]
pub fn resolve_update_wins_native(conflict: NodeConflictRecord) -> NodeResolutionRecord {
    resolve_update_wins(conflict.into()).into()
}

#[napi(js_name = "crdtResolutionValue")]
pub fn crdt_resolution_value_native(value: Buffer) -> NodeCrdtResolutionRecord {
    crdt_resolution_value(value.to_vec()).into()
}

#[napi(js_name = "crdtResolutionDelete")]
pub fn crdt_resolution_delete_native() -> NodeCrdtResolutionRecord {
    crdt_resolution_delete().into()
}

#[napi(js_name = "u64Key")]
pub fn u64_key_native(value: String) -> Result<Buffer> {
    let value = value
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    Ok(Buffer::from(u64_key(value)))
}

#[napi(js_name = "u128Key")]
pub fn u128_key_native(value: String) -> Result<Buffer> {
    u128_key(value).map(Buffer::from).map_err(to_napi_error)
}

#[napi(js_name = "i64Key")]
pub fn i64_key_native(value: String) -> Result<Buffer> {
    let value = value
        .parse::<i64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    Ok(Buffer::from(i64_key(value)))
}

#[napi(js_name = "i128Key")]
pub fn i128_key_native(value: String) -> Result<Buffer> {
    i128_key(value).map(Buffer::from).map_err(to_napi_error)
}

#[napi(js_name = "timestampMillisKey")]
pub fn timestamp_millis_key_native(value: String) -> Result<Buffer> {
    let value = value
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    Ok(Buffer::from(timestamp_millis_key(value)))
}

#[napi(js_name = "encodeSegment")]
pub fn encode_segment_native(segment: Buffer) -> Buffer {
    Buffer::from(encode_segment(segment.to_vec()))
}

#[napi(js_name = "keyFromSegments")]
pub fn key_from_segments_native(segments: Vec<Buffer>) -> Buffer {
    let segments = segments
        .into_iter()
        .map(|segment| segment.to_vec())
        .collect();
    Buffer::from(key_from_segments(segments))
}

#[napi(js_name = "keyFromPrefixedSegments")]
pub fn key_from_prefixed_segments_native(prefix: Buffer, segments: Vec<Buffer>) -> Buffer {
    let segments = segments
        .into_iter()
        .map(|segment| segment.to_vec())
        .collect();
    Buffer::from(key_from_prefixed_segments(prefix.to_vec(), segments))
}

#[napi(js_name = "changedSpan")]
pub fn changed_span_native(start: Buffer, end: Option<Buffer>) -> NodeChangedSpanRecord {
    changed_span(start.to_vec(), end.map(|value| value.to_vec())).into()
}

#[napi(js_name = "changedSpanFromKey")]
pub fn changed_span_from_key_native(key: Buffer) -> NodeChangedSpanRecord {
    changed_span_from_key(key.to_vec()).into()
}

#[napi(js_name = "changedSpanForPrefix")]
pub fn changed_span_for_prefix_native(prefix: Buffer) -> NodeChangedSpanRecord {
    changed_span_for_prefix(prefix.to_vec()).into()
}

#[napi(js_name = "retainAllNamedRoots")]
pub fn retain_all_named_roots_native() -> NodeNamedRootRetentionRecord {
    retain_all_named_roots().into()
}

#[napi(js_name = "retainExactNamedRoots")]
pub fn retain_exact_named_roots_native(names: Vec<Buffer>) -> NodeNamedRootRetentionRecord {
    let names = names.into_iter().map(|name| name.to_vec()).collect();
    retain_exact_named_roots(names).into()
}

#[napi(js_name = "retainNamedRootPrefix")]
pub fn retain_named_root_prefix_native(prefix: Buffer) -> NodeNamedRootRetentionRecord {
    retain_named_root_prefix(prefix.to_vec()).into()
}

#[napi(js_name = "retainNewestNamedRoots")]
pub fn retain_newest_named_roots_native(
    prefix: Buffer,
    count: String,
) -> Result<NodeNamedRootRetentionRecord> {
    let count = count
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    Ok(retain_newest_named_roots(prefix.to_vec(), count).into())
}

#[napi(js_name = "retainNamedRootsUpdatedSince")]
pub fn retain_named_roots_updated_since_native(
    prefix: Buffer,
    min_updated_at_millis: String,
) -> Result<NodeNamedRootRetentionRecord> {
    let min_updated_at_millis = min_updated_at_millis
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    Ok(retain_named_roots_updated_since(prefix.to_vec(), min_updated_at_millis).into())
}

#[napi(js_name = "decodeSegments")]
pub fn decode_segments_native(key: Buffer) -> Result<Vec<Buffer>> {
    decode_segments(key.to_vec())
        .map(|segments| segments.into_iter().map(Buffer::from).collect())
        .map_err(to_napi_error)
}

#[napi(js_name = "debugKey")]
pub fn debug_key_native(key: Buffer) -> String {
    debug_key(key.to_vec())
}

#[napi(js_name = "snapshotNamespaceBranch")]
pub fn snapshot_namespace_branch_native() -> NodeSnapshotNamespaceRecord {
    snapshot_namespace_branch().into()
}

#[napi(js_name = "snapshotNamespaceTag")]
pub fn snapshot_namespace_tag_native() -> NodeSnapshotNamespaceRecord {
    snapshot_namespace_tag().into()
}

#[napi(js_name = "snapshotNamespaceCheckpoint")]
pub fn snapshot_namespace_checkpoint_native() -> NodeSnapshotNamespaceRecord {
    snapshot_namespace_checkpoint().into()
}

#[napi(js_name = "snapshotNamespaceCustom")]
pub fn snapshot_namespace_custom_native(prefix: Buffer) -> NodeSnapshotNamespaceRecord {
    snapshot_namespace_custom(prefix.to_vec()).into()
}

#[napi(js_name = "snapshotRootName")]
pub fn snapshot_root_name_native(
    namespace: NodeSnapshotNamespaceRecord,
    id: Buffer,
) -> Result<Buffer> {
    snapshot_root_name(namespace.try_into()?, id.to_vec())
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotIdFromName")]
pub fn snapshot_id_from_name_native(
    namespace: NodeSnapshotNamespaceRecord,
    name: Buffer,
) -> Result<Option<Buffer>> {
    snapshot_id_from_name(namespace.try_into()?, name.to_vec())
        .map(|id| id.map(Buffer::from))
        .map_err(to_napi_error)
}

#[napi(js_name = "versionedValueBytesRoundTrip")]
pub fn versioned_value_bytes_round_trip(bytes: Buffer) -> Result<Buffer> {
    let record = versioned_value_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    versioned_value_to_bytes(record)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "versionedValueBytesMatchesSchema")]
pub fn versioned_value_bytes_matches_schema_native(
    bytes: Buffer,
    schema: String,
    version: String,
) -> Result<bool> {
    let version = parse_u64(&version)?;
    versioned_value_bytes_matches_schema(bytes.to_vec(), schema, version).map_err(to_napi_error)
}

#[napi(js_name = "versionedValueBytesRequireSchema")]
pub fn versioned_value_bytes_require_schema_native(
    bytes: Buffer,
    schema: String,
    version: String,
) -> Result<()> {
    let version = parse_u64(&version)?;
    versioned_value_bytes_require_schema(bytes.to_vec(), schema, version).map_err(to_napi_error)
}

#[napi(js_name = "valueRefBytesRoundTrip")]
pub fn value_ref_bytes_round_trip(bytes: Buffer) -> Result<Buffer> {
    let record = value_ref_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    value_ref_to_bytes(record)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "valueRefFromStoredBytes")]
pub fn value_ref_from_stored_bytes_native(bytes: Buffer) -> Result<NodeValueRefRecord> {
    value_ref_from_stored_bytes(bytes.to_vec())
        .map(NodeValueRefRecord::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "valueRefInlineRequiresEscape")]
pub fn value_ref_inline_requires_escape_native(value: Buffer) -> bool {
    value_ref_inline_requires_escape(value.to_vec())
}

#[napi(js_name = "blobRefValidateBytes")]
pub fn blob_ref_validate_bytes_native(reference: NodeBlobRefRecord, bytes: Buffer) -> Result<()> {
    blob_ref_validate_bytes(reference.try_into()?, bytes.to_vec()).map_err(to_napi_error)
}

#[napi(js_name = "rootManifestBytesRoundTrip")]
pub fn root_manifest_bytes_round_trip(bytes: Buffer) -> Result<Buffer> {
    let record = root_manifest_from_bytes(bytes.to_vec()).map_err(to_napi_error)?;
    root_manifest_to_bytes(record)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleToBytes")]
pub fn snapshot_bundle_to_bytes_native(bundle: NodeSnapshotBundleRecord) -> Result<Buffer> {
    snapshot_bundle_to_bytes(bundle.into_binding(default_config()))
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleFromBytes")]
pub fn snapshot_bundle_from_bytes_native(bytes: Buffer) -> Result<NodeSnapshotBundleRecord> {
    snapshot_bundle_from_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleDigest")]
pub fn snapshot_bundle_digest_native(bundle: NodeSnapshotBundleRecord) -> Result<Buffer> {
    snapshot_bundle_digest(bundle.into_binding(default_config()))
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleDigestBytes")]
pub fn snapshot_bundle_digest_bytes_native(bytes: Buffer) -> Result<Buffer> {
    snapshot_bundle_digest_bytes(bytes.to_vec())
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleSummary")]
pub fn snapshot_bundle_summary_native(
    bundle: NodeSnapshotBundleRecord,
) -> Result<NodeSnapshotBundleSummaryRecord> {
    snapshot_bundle_summary(bundle.into_binding(default_config()))
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "snapshotBundleSummaryFromBytes")]
pub fn snapshot_bundle_summary_from_bytes_native(
    bytes: Buffer,
) -> Result<NodeSnapshotBundleSummaryRecord> {
    snapshot_bundle_summary_from_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifySnapshotBundle")]
pub fn verify_snapshot_bundle_native(
    bundle: NodeSnapshotBundleRecord,
) -> Result<NodeSnapshotBundleVerificationRecord> {
    verify_snapshot_bundle(bundle.into_binding(default_config()))
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "verifySnapshotBundleBytes")]
pub fn verify_snapshot_bundle_bytes_native(
    bytes: Buffer,
) -> Result<NodeSnapshotBundleVerificationRecord> {
    verify_snapshot_bundle_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "defaultConfig")]
pub fn default_config_native() -> NodeConfigRecord {
    default_config().into()
}

#[napi(js_name = "encodingRaw")]
pub fn encoding_raw_native() -> NodeEncodingRecord {
    encoding_raw().into()
}

#[napi(js_name = "encodingCbor")]
pub fn encoding_cbor_native() -> NodeEncodingRecord {
    encoding_cbor().into()
}

#[napi(js_name = "encodingJson")]
pub fn encoding_json_native() -> NodeEncodingRecord {
    encoding_json().into()
}

#[napi(js_name = "encodingCustom")]
pub fn encoding_custom_native(name: String) -> NodeEncodingRecord {
    encoding_custom(name).into()
}

#[napi(js_name = "treeConfig")]
pub fn tree_config_native(
    min_chunk_size: String,
    max_chunk_size: String,
    chunking_factor: u32,
    hash_seed: String,
    encoding: NodeEncodingRecord,
    node_cache_max_nodes: Option<String>,
    node_cache_max_bytes: Option<String>,
) -> Result<NodeConfigRecord> {
    tree_config(
        parse_u64(&min_chunk_size)?,
        parse_u64(&max_chunk_size)?,
        chunking_factor,
        parse_u64(&hash_seed)?,
        encoding.try_into()?,
        node_cache_max_nodes.as_deref().map(parse_u64).transpose()?,
        node_cache_max_bytes.as_deref().map(parse_u64).transpose()?,
    )
    .map(Into::into)
    .map_err(to_napi_error)
}

#[napi(js_name = "defaultLargeValueConfig")]
pub fn default_large_value_config_native() -> NodeLargeValueConfigRecord {
    default_large_value_config().into()
}

#[napi(js_name = "largeValueConfig")]
pub fn large_value_config_native(inline_threshold: String) -> Result<NodeLargeValueConfigRecord> {
    large_value_config(parse_u64(&inline_threshold)?)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "defaultParallelConfig")]
pub fn default_parallel_config_native() -> NodeParallelConfigRecord {
    default_parallel_config().into()
}

#[napi(js_name = "parallelConfig")]
pub fn parallel_config_native(
    max_threads: String,
    parallelism_threshold: String,
) -> Result<NodeParallelConfigRecord> {
    parallel_config(parse_u64(&max_threads)?, parse_u64(&parallelism_threshold)?)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "parallelConfigSequential")]
pub fn parallel_config_sequential_native() -> NodeParallelConfigRecord {
    parallel_config_sequential().into()
}

#[napi(js_name = "crdtConfigLww")]
pub fn crdt_config_lww_native(delete_policy: String) -> Result<NodeCrdtConfigRecord> {
    Ok(crdt_config_lww(crdt_delete_policy_from_str(&delete_policy)?).into())
}

#[napi(js_name = "crdtConfigMultiValue")]
pub fn crdt_config_multi_value_native(delete_policy: String) -> Result<NodeCrdtConfigRecord> {
    Ok(crdt_config_multi_value(crdt_delete_policy_from_str(&delete_policy)?).into())
}

#[napi(js_name = "timestampedValueToBytes")]
pub fn timestamped_value_to_bytes_native(record: NodeTimestampedValueRecord) -> Result<Buffer> {
    Ok(Buffer::from(timestamped_value_to_bytes(record.try_into()?)))
}

#[napi(js_name = "timestampedValueFromBytes")]
pub fn timestamped_value_from_bytes_native(bytes: Buffer) -> Result<NodeTimestampedValueRecord> {
    timestamped_value_from_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "timestampedValueNow")]
pub fn timestamped_value_now_native(value: Buffer) -> NodeTimestampedValueRecord {
    timestamped_value_now(value.to_vec()).into()
}

#[napi(js_name = "multiValueSetToBytes")]
pub fn multi_value_set_to_bytes_native(values: Vec<Buffer>) -> Buffer {
    Buffer::from(multi_value_set_to_bytes(buffers(values)))
}

#[napi(js_name = "multiValueSetFromBytes")]
pub fn multi_value_set_from_bytes_native(bytes: Buffer) -> Result<Vec<Buffer>> {
    multi_value_set_from_bytes(bytes.to_vec())
        .map(|values| values.into_iter().map(Buffer::from).collect())
        .map_err(to_napi_error)
}

#[napi(js_name = "multiValueSetMerge")]
pub fn multi_value_set_merge_native(left: Vec<Buffer>, right: Vec<Buffer>) -> Vec<Buffer> {
    multi_value_set_merge(buffers(left), buffers(right))
        .into_iter()
        .map(Buffer::from)
        .collect()
}

#[napi(js_name = "tombstoneToBytes")]
pub fn tombstone_to_bytes_native(record: NodeTombstoneRecord) -> Result<Buffer> {
    tombstone_to_bytes(record.try_into()?)
        .map(Buffer::from)
        .map_err(to_napi_error)
}

#[napi(js_name = "tombstoneFromBytes")]
pub fn tombstone_from_bytes_native(bytes: Buffer) -> Result<NodeTombstoneRecord> {
    tombstone_from_bytes(bytes.to_vec())
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "tombstoneFromStoredBytes")]
pub fn tombstone_from_stored_bytes_native(bytes: Buffer) -> Result<Option<NodeTombstoneRecord>> {
    tombstone_from_stored_bytes(bytes.to_vec())
        .map(|record| record.map(Into::into))
        .map_err(to_napi_error)
}

#[napi(js_name = "isTombstoneValue")]
pub fn is_tombstone_value_native(bytes: Buffer) -> bool {
    is_tombstone_value(bytes.to_vec())
}

#[napi(js_name = "tombstoneUpsertMutation")]
pub fn tombstone_upsert_mutation_native(
    key: Buffer,
    tombstone: NodeTombstoneRecord,
) -> Result<NodeMutationRecord> {
    tombstone_upsert_mutation(key.to_vec(), tombstone.try_into()?)
        .map(Into::into)
        .map_err(to_napi_error)
}

#[napi(js_name = "tombstoneCompactionMutation")]
pub fn tombstone_compaction_mutation_native(
    key: Buffer,
    stored_value: Buffer,
) -> Result<Option<NodeMutationRecord>> {
    tombstone_compaction_mutation(key.to_vec(), stored_value.to_vec())
        .map(|mutation| mutation.map(Into::into))
        .map_err(to_napi_error)
}

type NodeResolverFunction = FunctionRef<NodeConflictRecord, NodeResolutionRecord>;
type NodeCrdtResolverFunction = FunctionRef<NodeConflictRecord, NodeCrdtResolutionRecord>;

fn js_resolver(
    env: Env,
    resolver: NodeResolverFunction,
    callback_error: Arc<Mutex<Option<String>>>,
) -> impl Fn(BindingConflictRecord) -> BindingResolutionRecord + 'static {
    move |conflict| {
        if callback_error
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(true)
        {
            return unresolved_resolution();
        }

        let function = match resolver.borrow_back(&env) {
            Ok(function) => function,
            Err(error) => {
                if let Ok(mut guard) = callback_error.lock() {
                    *guard = Some(error.to_string());
                }
                return unresolved_resolution();
            }
        };

        match function.call(NodeConflictRecord::from(conflict)) {
            Ok(resolution) => resolution.into(),
            Err(error) => {
                if let Ok(mut guard) = callback_error.lock() {
                    *guard = Some(error.to_string());
                }
                unresolved_resolution()
            }
        }
    }
}

fn js_crdt_resolver(
    env: Env,
    resolver: NodeCrdtResolverFunction,
    callback_error: Arc<Mutex<Option<String>>>,
) -> impl Fn(BindingConflictRecord) -> BindingCrdtResolutionRecord + 'static {
    move |conflict| {
        if callback_error
            .lock()
            .map(|guard| guard.is_some())
            .unwrap_or(true)
        {
            return delete_crdt_resolution();
        }

        let function = match resolver.borrow_back(&env) {
            Ok(function) => function,
            Err(error) => {
                if let Ok(mut guard) = callback_error.lock() {
                    *guard = Some(error.to_string());
                }
                return delete_crdt_resolution();
            }
        };

        match function.call(NodeConflictRecord::from(conflict)) {
            Ok(resolution) => resolution.into(),
            Err(error) => {
                if let Ok(mut guard) = callback_error.lock() {
                    *guard = Some(error.to_string());
                }
                delete_crdt_resolution()
            }
        }
    }
}

fn take_callback_error(callback_error: &Arc<Mutex<Option<String>>>) -> Option<Error> {
    callback_error
        .lock()
        .ok()
        .and_then(|mut guard| guard.take())
        .map(|message| Error::new(Status::GenericFailure, message))
}

fn to_napi_error(error: ProllyBindingError) -> Error {
    Error::new(Status::GenericFailure, error.to_string())
}

fn parse_u64(value: &str) -> Result<u64> {
    value
        .parse::<u64>()
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))
}

fn structural_diff_marker_kind(kind: BindingStructuralDiffMarkerKind) -> &'static str {
    match kind {
        BindingStructuralDiffMarkerKind::Compare => "compare",
        BindingStructuralDiffMarkerKind::Added => "added",
        BindingStructuralDiffMarkerKind::Removed => "removed",
    }
}

fn binding_structural_diff_marker_kind(value: &str) -> Result<BindingStructuralDiffMarkerKind> {
    match value {
        "compare" => Ok(BindingStructuralDiffMarkerKind::Compare),
        "added" => Ok(BindingStructuralDiffMarkerKind::Added),
        "removed" => Ok(BindingStructuralDiffMarkerKind::Removed),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown structural diff marker kind {other:?}"),
        )),
    }
}

fn crdt_delete_policy_from_str(value: &str) -> Result<CrdtDeletePolicyKind> {
    match value {
        "delete_wins" => Ok(CrdtDeletePolicyKind::DeleteWins),
        "update_wins" => Ok(CrdtDeletePolicyKind::UpdateWins),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown CRDT delete policy {other:?}"),
        )),
    }
}

fn crdt_merge_strategy_from_str(value: &str) -> Result<CrdtMergeStrategyKind> {
    match value {
        "last_writer_wins" => Ok(CrdtMergeStrategyKind::LastWriterWins),
        "multi_value" => Ok(CrdtMergeStrategyKind::MultiValue),
        other => Err(Error::new(
            Status::InvalidArg,
            format!("unknown CRDT merge strategy {other:?}"),
        )),
    }
}

fn buffers(values: Vec<Buffer>) -> Vec<Vec<u8>> {
    values.into_iter().map(|value| value.to_vec()).collect()
}

fn config_from_json(config_json: &str) -> Result<ConfigRecord> {
    let fixture: FixtureConfig = serde_json::from_str(config_json)
        .map_err(|error| Error::new(Status::InvalidArg, error.to_string()))?;
    fixture.try_into()
}

#[derive(Deserialize)]
struct FixtureConfig {
    min_chunk_size: u64,
    max_chunk_size: u64,
    chunking_factor: u32,
    hash_seed: u64,
    encoding: FixtureEncoding,
    node_cache_max_nodes: Option<u64>,
    node_cache_max_bytes: Option<u64>,
}

#[derive(Deserialize)]
struct FixtureEncoding {
    kind: String,
    custom_name: Option<String>,
}

impl TryFrom<FixtureConfig> for ConfigRecord {
    type Error = Error;

    fn try_from(value: FixtureConfig) -> Result<Self> {
        let kind = match value.encoding.kind.as_str() {
            "raw" => EncodingKind::Raw,
            "cbor" => EncodingKind::Cbor,
            "json" => EncodingKind::Json,
            "custom" => EncodingKind::Custom,
            other => {
                return Err(Error::new(
                    Status::InvalidArg,
                    format!("unknown encoding kind {other:?}"),
                ))
            }
        };
        Ok(Self {
            min_chunk_size: value.min_chunk_size,
            max_chunk_size: value.max_chunk_size,
            chunking_factor: value.chunking_factor,
            hash_seed: value.hash_seed,
            encoding: EncodingRecord {
                kind,
                custom_name: value.encoding.custom_name,
            },
            node_cache_max_nodes: value.node_cache_max_nodes,
            node_cache_max_bytes: value.node_cache_max_bytes,
        })
    }
}
