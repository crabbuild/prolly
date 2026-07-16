//! # Prolly Trees
//!
//! A Rust implementation of Prolly Trees - content-addressable ordered search indexes
//! that combine the efficiency of B+ trees with deterministic merging capabilities.
//!
//! ## Features
//!
//! - **Ordered key-value storage**: Keys are sorted lexicographically (byte comparison)
//! - **Content-addressable nodes**: Each node has a unique CID (Content Identifier) derived from its content
//! - **Deterministic structure**: Same content always produces the same tree structure
//! - **Efficient diff/merge**: Compare trees by comparing root hashes, skip identical subtrees
//! - **Pluggable storage**: Implement the [`Store`] trait for custom backends
//!
//! ## Quick Start
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config};
//!
//! // Create a store and tree manager
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! // Create an empty tree
//! let tree = prolly.create();
//!
//! // Insert key-value pairs (returns a new tree - immutable)
//! let tree = prolly.put(&tree, b"name".to_vec(), b"Alice".to_vec()).unwrap();
//! let tree = prolly.put(&tree, b"age".to_vec(), b"30".to_vec()).unwrap();
//!
//! // Retrieve values
//! let name = prolly.get(&tree, b"name").unwrap();
//! assert_eq!(name, Some(b"Alice".to_vec()));
//!
//! // Delete keys
//! let tree = prolly.delete(&tree, b"age").unwrap();
//! assert!(prolly.get(&tree, b"age").unwrap().is_none());
//! ```
//!
//! ## Range Iteration
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//! let mut tree = prolly.create();
//!
//! // Insert some data
//! tree = prolly.put(&tree, b"a".to_vec(), b"1".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"b".to_vec(), b"2".to_vec()).unwrap();
//! tree = prolly.put(&tree, b"c".to_vec(), b"3".to_vec()).unwrap();
//!
//! // Iterate over all keys
//! for result in prolly.range(&tree, &[], None).unwrap() {
//!     let (key, val) = result.unwrap();
//!     println!("{:?} -> {:?}", String::from_utf8_lossy(&key), String::from_utf8_lossy(&val));
//! }
//!
//! // Iterate over a specific range [b, c)
//! for result in prolly.range(&tree, b"b", Some(b"c")).unwrap() {
//!     let (key, val) = result.unwrap();
//!     // Only yields "b" -> "2"
//! }
//! ```
//!
//! ## Diff and Merge
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Diff};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! // Create base tree
//! let base = prolly.create();
//! let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();
//!
//! // Create two divergent branches
//! let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
//! let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();
//!
//! // Compute diff
//! let diffs = prolly.diff(&base, &left).unwrap();
//! // diffs contains: Added { key: b"b", val: b"2" }
//!
//! // Three-way merge (no conflicts since changes are disjoint)
//! let merged = prolly.merge(&base, &left, &right, None).unwrap();
//!
//! // Merged tree has all keys: a, b, c
//! assert!(prolly.get(&merged, b"a").unwrap().is_some());
//! assert!(prolly.get(&merged, b"b").unwrap().is_some());
//! assert!(prolly.get(&merged, b"c").unwrap().is_some());
//! ```
//!
//! ## Batch Building
//!
//! For bulk loading data, use [`BatchBuilder`] for parallel tree construction:
//!
//! ```rust
//! use prolly::{BatchBuilder, MemStore, Config, Prolly};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemStore::new());
//! let config = Config::default();
//!
//! // Build tree from many entries in parallel
//! let mut builder = BatchBuilder::new(store.clone(), config.clone());
//! for i in 0..1000 {
//!     builder.add(format!("key{:04}", i).into_bytes(), format!("val{}", i).into_bytes());
//! }
//! let tree = builder.build().unwrap();
//!
//! // Use the tree with Prolly
//! let prolly = Prolly::new(store, config);
//! let val = prolly.get(&tree, b"key0042").unwrap();
//! assert!(val.is_some());
//! ```
//!
//! ## Named Roots
//!
//! Use named-root helpers when an application needs durable names for immutable
//! tree snapshots:
//!
//! A named root is a mutable pointer, not a live view. `put`, `delete`, `batch`,
//! and `merge` return new immutable [`Tree`] handles and do not automatically
//! advance any name. Publish the replacement tree explicitly, preferably with
//! `compare_and_swap_named_root` when another writer could update the same
//! name.
//!
//! ```rust
//! use prolly::{Config, MemStore, Prolly};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemStore::new());
//! let prolly = Prolly::new(store.clone(), Config::default());
//! let tree = prolly.create();
//! let tree = prolly.put(&tree, b"name".to_vec(), b"Trail".to_vec()).unwrap();
//!
//! let update = prolly
//!     .compare_and_swap_named_root(b"main", None, Some(&tree))
//!     .unwrap();
//! assert!(update.is_applied());
//!
//! let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
//! assert_eq!(prolly.get(&loaded, b"name").unwrap(), Some(b"Trail".to_vec()));
//! ```
//!
//! ## Custom Storage Backend
//!
//! Implement the [`Store`] trait for custom storage:
//!
//! ```rust
//! use prolly::{Store, BatchOp};
//!
//! struct MyStore {
//!     // Your storage implementation
//! }
//!
//! impl Store for MyStore {
//!     type Error = std::io::Error;
//!
//!     fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
//!         // Implement get
//!         Ok(None)
//!     }
//!
//!     fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
//!         // Implement put
//!         Ok(())
//!     }
//!
//!     fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
//!         // Implement delete
//!         Ok(())
//!     }
//!
//!     fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
//!         // Implement batch operations
//!         Ok(())
//!     }
//! }
//! ```
//!
//! ## Configuration
//!
//! Customize tree behavior with [`Config`]:
//!
//! ```rust
//! use prolly::{Config, Encoding};
//!
//! let config = Config::builder()
//!     .min_chunk_size(4)        // Min entries before considering split
//!     .max_chunk_size(1024)     // Max entries per node
//!     .chunking_factor(128)     // Controls average node size
//!     .hash_seed(42)            // Seed for boundary detection
//!     .encoding(Encoding::Raw)  // Value encoding type
//!     .node_cache_max_nodes(50_000) // Optional decoded-node cache cap
//!     .node_cache_max_bytes(256 * 1024 * 1024) // Optional serialized-byte cap
//!     .build();
//! ```
//!
//! ## Advanced Extensibility
//!
//! The library provides extensible traits for advanced use cases:
//!
//! ### Streaming Diff
//!
//! Use [`StreamingDiffer`] for memory-efficient diff operations on large trees:
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, Diff};
//! use std::sync::Arc;
//!
//! let store = Arc::new(MemStore::new());
//! let prolly = Prolly::new(store.clone(), Config::default());
//!
//! let base = prolly.create();
//! let other = prolly.put(&base, b"key".to_vec(), b"val".to_vec()).unwrap();
//!
//! // Stream differences lazily (memory-efficient for large trees)
//! for diff_result in prolly.stream_diff(&base, &other).unwrap() {
//!     match diff_result {
//!         Ok(diff) => println!("{:?}", diff),
//!         Err(e) => eprintln!("Error: {}", e),
//!     }
//! }
//! ```
//!
//! ### CRDT Merge
//!
//! Use [`ConflictFreeMerger`] for automatic conflict resolution:
//!
//! ```rust
//! use prolly::{CrdtConfig, CrdtResolution, MergeStrategy, DeletePolicy, TimestampedValue};
//!
//! // Last-Writer-Wins strategy
//! let lww_config = CrdtConfig::lww();
//!
//! // Multi-Value strategy (preserves all concurrent values)
//! let mv_config = CrdtConfig::multi_value();
//!
//! // Custom merge function
//! let custom_config = CrdtConfig::custom(|conflict| {
//!     match &conflict.left {
//!         Some(value) => CrdtResolution::value(value.clone()),
//!         None => CrdtResolution::delete(),
//!     }
//! });
//! ```
//!
//! ### Parallel Processing
//!
//! Use [`ParallelRebalancer`] for multi-threaded batch operations:
//!
//! ```rust
//! use prolly::{ParallelConfig, DefaultParallelRebalancer};
//!
//! // Configure parallel processing
//! let config = ParallelConfig {
//!     max_threads: 4,           // Use 4 threads (0 = auto)
//!     parallelism_threshold: 50, // Parallelize when > 50 items
//! };
//!
//! let rebalancer = DefaultParallelRebalancer::new();
//! ```

mod prolly;

// Re-export public API from prolly module
pub use prolly::batch::{
    append_batch, BatchApplyResult, BatchApplyStats, BatchWriter, BatchWriterConfig, MutationBuffer,
};
#[cfg(feature = "async-store")]
pub use prolly::blob::{AsyncBlobStore, SyncBlobStoreAsAsync};
pub use prolly::blob::{
    BlobRef, BlobStore, BlobStoreScan, FileBlobStore, FileBlobStoreError, LargeValueConfig,
    MemBlobStore, MemBlobStoreError, ValueRef, DEFAULT_INLINE_VALUE_THRESHOLD,
};
#[cfg(feature = "tokio")]
pub use prolly::blob::{TokioBlockingBlobStore, TokioBlockingBlobStoreError};
pub use prolly::boundary::{is_boundary, is_boundary_config, BoundaryDetector};
pub use prolly::builder::{BatchBuilder, SortedBatchBuilder};
pub use prolly::canonical::CanonicalWriteStats;
pub use prolly::canonical_splice::{canonical_splice, CanonicalSpliceStats};
pub use prolly::chunking;
pub use prolly::cid::Cid;
pub use prolly::config::{Config, ConfigBuilder, RuntimeConfig};
pub use prolly::content_graph::{
    compare_and_swap_named_content_root, compare_and_swap_named_content_root_with_limits,
    content_references, copy_and_publish_content_graph, copy_content_graph,
    load_named_content_root, load_named_content_root_with_limits, plan_content_gc,
    put_named_content_root, put_named_content_root_with_limits, sweep_content_gc,
    sweep_content_gc_with_invalidator, walk_content_graph, ContentGcPlan, ContentGcSweep,
    ContentGraphCopy, ContentGraphLimits, ContentGraphWalk, ContentManifestUpdate,
    ContentObjectKind, ContentRootManifest, ContentRootPublication, TypedContentObject,
    TypedContentRoot,
};
pub use prolly::crdt::{
    ConflictFreeMerger, CrdtConfig, CrdtResolution, CustomMergeFn, DefaultConflictFreeMerger,
    DeletePolicy, MergeStrategy, MultiValueSet, TimestampExtractor, TimestampedValue,
};
pub use prolly::cursor::{Cursor, CursorIterator, DiffCursor};
pub use prolly::debug::{
    TreeDebugComparedNode, TreeDebugComparison, TreeDebugComparisonLevel, TreeDebugLevel,
    TreeDebugNode, TreeDebugNodeStatus, TreeDebugView,
};
#[cfg(feature = "async-store")]
pub use prolly::diff::{AsyncConflictIter, AsyncDiffIter};
pub use prolly::diff::{
    DiffPage, DiffTraversalStats, MergeExplanation, MergeFallbackReason, MergeFastPath,
    MergeResolutionKind, MergeReuseReason, MergeTrace, MergeTraceEvent, MergeTraceStage,
    StructuralDiffCursor, StructuralDiffMarker, StructuralDiffPage,
};
pub use prolly::encoding::Encoding;
pub use prolly::error::{resolver, Conflict, Diff, Error, Mutation, Resolution, Resolver};
pub use prolly::format::{
    BoundaryInput, BoundaryRule, ChunkMeasure, ChunkingSpec, HashAlgorithm, NodeLayoutSpec,
    TreeFormat,
};
pub use prolly::gc::{
    BlobGcPlan, BlobGcReachability, BlobGcSweep, GcPlan, GcReachability, GcSweep,
};
pub use prolly::key::{
    debug_key, decode_segments, encode_segment, encode_segment_prefix, i128_key, i64_key,
    prefix_end, prefix_range, timestamp_millis_key, u128_key, u64_key, KeyBuilder, KeyDecodeError,
};
#[cfg(feature = "async-store")]
pub use prolly::manifest::{AsyncManifestStore, AsyncManifestStoreScan};
pub use prolly::manifest::{
    ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRoot, NamedRootManifest,
    NamedRootRetention, NamedRootSelection, NamedRootUpdate, RootManifest,
};
pub use prolly::node::{Node, NodeBuilder};
pub use prolly::parallel::{DefaultParallelRebalancer, ParallelConfig, ParallelRebalancer};
pub use prolly::patch::{LogicalPatch, StructuralEdit, StructuralPatch};
pub use prolly::policy::{
    MergePolicyFn, MergePolicyRegistry, MergePolicyRule, MergePolicyRuleLabel,
};
pub use prolly::proof::{
    inspect_proof_bundle, sign_proof_bundle_hmac_sha256, verify_authenticated_proof_bundle,
    verify_authenticated_proof_envelope, verify_diff_page_proof, verify_key_proof,
    verify_multi_key_proof, verify_proof_bundle, verify_range_page_proof, verify_range_proof,
    AuthenticatedProofBundleVerification, AuthenticatedProofEnvelope,
    AuthenticatedProofEnvelopeVerification, DiffPageProof, DiffPageProofVerification, KeyProof,
    KeyProofVerification, MultiKeyProof, MultiKeyProofVerification, ProofBundleKind,
    ProofBundleSummary, ProofBundleVerification, ProvedDiffPage, ProvedRangePage, RangePageProof,
    RangePageProofVerification, RangeProof, RangeProofVerification,
};
pub use prolly::proximity::{
    AcceleratorCatalog, AcceleratorCatalogEntry, AcceleratorSet, AdaptiveQuality,
    ApproximatePreference, BuildParallelism, CatalogAcceleratorKind, CompositeAccelerator,
    CompositeAcceleratorConfig, CompositeBase, CompositeBaseKind, CompositeBuildLimits,
    CompositeBuildOrRebuildOutcome, CompositeBuildOutcome, CompositeBuildStats,
    CompositeRebuildOptions, DistanceMetric, ExactProximityRecord, FullRebuildReason,
    HierarchyConfig, HnswBuildLimits, HnswBuildStats, HnswConfig, HnswIndex,
    HnswRoutingVectorEncoding, HnswSearchOptions, Neighbor, OverflowConfig, PlannerPolicy,
    PqSearchOptions, ProductQuantizationBuildLimits, ProductQuantizationBuildStats,
    ProductQuantizationConfig, ProductQuantizationQuality, ProductQuantizer, ProximityBuildStats,
    ProximityConfig, ProximityFilter, ProximityMap, ProximityMembershipProof,
    ProximityMembershipVerification, ProximityMutation, ProximityMutationStats,
    ProximityProofFilter, ProximityRecord, ProximitySearchClaim, ProximitySearchEvent,
    ProximitySearchProof, ProximitySearchRequest, ProximitySearchStats,
    ProximitySearchVerification, ProximityStructuralProof, ProximityStructuralVerification,
    ProximityTree, ProximityVerification, QueryKernel, ScalarQuantizationConfig, SearchBackend,
    SearchBudget, SearchCompletion, SearchIo, SearchOptions, SearchPlan, SearchPlanSummary,
    SearchPolicy, SearchRequest, SearchResult, SearchRuntime, SearchRuntimePolicy,
    StoreCacheNamespace, VectorStorageConfig, SEARCH_PLAN_FORMAT_VERSION,
};
#[cfg(feature = "async-store")]
pub use prolly::proximity::{
    AsyncAcceleratorCatalog, AsyncAcceleratorSet, AsyncCompositeAccelerator, AsyncHnswIndex,
    AsyncIoConfig, AsyncProductQuantizer, AsyncProximityMap, AsyncSearchControl, CancellationToken,
};
pub use prolly::range::{
    CursorWindow, RangeCursor, RangeIter, RangePage, ReverseCursor, ReversePage,
};
pub use prolly::secondary_index::{
    catalog_checkpoint_key, catalog_checkpoints_prefix, catalog_current_key,
    catalog_descriptor_key, catalog_format_key, catalog_map_id, catalog_retired_key,
    control_record_key, control_root_name, decode_physical_index_key, descriptor_fingerprint,
    index_map_id, physical_index_key, term_bounds_exact, term_bounds_prefix, term_bounds_range,
    ActiveIndexControl, ActiveIndexHealth, DecodedPhysicalIndexKey, IndexBuildResult,
    IndexCheckpoint, IndexControl, IndexProjection, IndexValue, IndexVerification,
    IndexedHeadRecord, IndexedMap, IndexedMapEditor, IndexedMapHealth, IndexedMapMetricsSnapshot,
    IndexedMapUpdate, IndexedRetentionResult, IndexedSnapshot, IndexedSnapshotBundle,
    IndexedSnapshotBundleIndex, IndexedSnapshotBundleSummary, IndexedSnapshotBundleVerification,
    IndexedSnapshotId, IndexedSourceRecord, IndexedVersion, ProjectedIndexEntry, SecondaryIndex,
    SecondaryIndexBuilder, SecondaryIndexCursor, SecondaryIndexDescriptor, SecondaryIndexDirection,
    SecondaryIndexEntry, SecondaryIndexError, SecondaryIndexExtractor, SecondaryIndexLimits,
    SecondaryIndexMatch, SecondaryIndexPage, SecondaryIndexRegistry, SecondaryIndexSnapshot,
    TermBounds, INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION, INDEX_PHYSICAL_LAYOUT_VERSION,
    SECONDARY_INDEX_FORMAT_VERSION,
};
pub use prolly::snapshot::{
    snapshot_id_from_name, snapshot_root_name, SnapshotManager, SnapshotNamespace, SnapshotRoot,
    SnapshotSelection, SNAPSHOT_BRANCH_PREFIX, SNAPSHOT_CHECKPOINT_PREFIX, SNAPSHOT_TAG_PREFIX,
};
pub use prolly::streaming::{DefaultStreamingDiffer, StreamingDiffer};
pub use prolly::{ChangedSpan, ChangedSpanHint, KeyValue, Prolly, ProllyMetricsSnapshot};

#[cfg(feature = "async-store")]
pub use prolly::range::{AsyncRangeIter, AsyncRangePage, AsyncReversePage};
#[cfg(feature = "async-store")]
pub use prolly::remote::{
    conformance as remote_conformance, RemoteAdapterError, RemoteBatchOp, RemoteManifestUpdate,
    RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition, RemoteRootWrite, RemoteStoreBackend,
    RemoteStoreConfig, RemoteTransactionConflict, RemoteTransactionUpdate,
};
pub use prolly::stats::{StatsComparison, StatsDiff, StatsPercentageChange, TreeStats};
#[cfg(feature = "async-store")]
pub use prolly::store::{AsyncStore, SyncStoreAsAsync};
pub use prolly::store::{
    BatchOp, FileNodeStore, FileNodeStoreError, MemStore, MemStoreError, NodeStoreScan, Store,
};
#[cfg(feature = "tokio")]
pub use prolly::store::{TokioBlockingStore, TokioBlockingStoreError};
pub use prolly::sync::{
    MissingNodeCopy, MissingNodePlan, SnapshotBundle, SnapshotBundleNode, SnapshotBundleSummary,
    SnapshotBundleVerification, SNAPSHOT_BUNDLE_FORMAT_VERSION,
};
pub use prolly::tombstone::{
    is_tombstone_value, tombstone_compaction, tombstone_upsert, Tombstone,
};
#[cfg(feature = "async-store")]
pub use prolly::transaction::{
    AsyncProllyTransaction, AsyncTransactionOverlayStore, AsyncTransactionalStore,
};
pub use prolly::transaction::{
    OwnedProllyTransaction, OwnedTransactionOverlayStore, ProllyTransaction, RootCondition,
    RootWrite, TransactionConflict, TransactionNodeWrite, TransactionOverlayError,
    TransactionUpdate, TransactionalStore,
};
pub use prolly::tree::Tree;
pub use prolly::value::{
    decode_cbor, decode_json, encode_cbor, encode_json, CborCodec, JsonCodec, ValueCodec,
    VersionedCborCodec, VersionedJsonCodec, VersionedValue,
};
#[cfg(feature = "async-store")]
pub use prolly::versioned_map::{AsyncMapChangeSubscription, AsyncMapSnapshot, AsyncVersionedMap};
pub use prolly::versioned_map::{
    BytesKeyCodec, KeyCodec, MapBackupVersion, MapCatalogVerification, MapChangeEvent,
    MapChangeSubscription, MapComparison, MapMerge, MapReverseIter, MapSnapshot, MapVersion,
    MapVersionId, ProofAuthentication, StringKeyCodec, TypedMigrationResult, TypedVersionedMap,
    VersionPruneResult, VersionedMap, VersionedMapBackup, VersionedMapBatchResult,
    VersionedMapEditor, VersionedMapUpdate, VersionedMapsTransaction,
    DEFAULT_VERSIONED_MAP_RETRIES, VERSIONED_MAP_ROOT_PREFIX,
};
pub use prolly::write_session::{PendingValue, Savepoint, WriteSession};
#[cfg(feature = "async-store")]
pub use prolly::AsyncProlly;

// Re-export constants
pub use prolly::encoding::{
    DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_MIN_CHUNK_SIZE,
    INIT_LEVEL,
};
