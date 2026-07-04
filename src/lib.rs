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
//! let tree = prolly.put(&tree, b"name".to_vec(), b"CrabDB".to_vec()).unwrap();
//!
//! let update = prolly
//!     .compare_and_swap_named_root(b"main", None, Some(&tree))
//!     .unwrap();
//! assert!(update.is_applied());
//!
//! let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
//! assert_eq!(prolly.get(&loaded, b"name").unwrap(), Some(b"CrabDB".to_vec()));
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
pub use prolly::boundary::{is_boundary, is_boundary_config};
pub use prolly::builder::{BatchBuilder, SortedBatchBuilder};
pub use prolly::cid::Cid;
pub use prolly::config::{Config, ConfigBuilder};
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
pub use prolly::gc::{
    BlobGcPlan, BlobGcReachability, BlobGcSweep, GcPlan, GcReachability, GcSweep,
};
pub use prolly::key::{
    debug_key, decode_segments, encode_segment, i128_key, i64_key, prefix_end, prefix_range,
    timestamp_millis_key, u128_key, u64_key, KeyBuilder, KeyDecodeError,
};
#[cfg(feature = "async-store")]
pub use prolly::manifest::{AsyncManifestStore, AsyncManifestStoreScan};
pub use prolly::manifest::{
    ManifestStore, ManifestStoreScan, ManifestUpdate, NamedRoot, NamedRootManifest,
    NamedRootRetention, NamedRootSelection, NamedRootUpdate, RootManifest,
};
pub use prolly::node::{Node, NodeBuilder};
pub use prolly::parallel::{DefaultParallelRebalancer, ParallelConfig, ParallelRebalancer};
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
pub use prolly::range::{
    CursorWindow, RangeCursor, RangeIter, RangePage, ReverseCursor, ReversePage,
};
pub use prolly::snapshot::{
    snapshot_id_from_name, snapshot_root_name, SnapshotManager, SnapshotNamespace, SnapshotRoot,
    SnapshotSelection, SNAPSHOT_BRANCH_PREFIX, SNAPSHOT_CHECKPOINT_PREFIX, SNAPSHOT_TAG_PREFIX,
};
pub use prolly::streaming::{DefaultStreamingDiffer, StreamingDiffer};
pub use prolly::{ChangedSpan, ChangedSpanHint, Prolly, ProllyMetricsSnapshot};

#[cfg(feature = "async-store")]
pub use prolly::range::{AsyncRangeIter, AsyncRangePage, AsyncReversePage};
#[cfg(feature = "async-store")]
pub use prolly::remote::{
    conformance as remote_conformance, RemoteAdapterError, RemoteBatchOp, RemoteManifestUpdate,
    RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend, RemoteStoreConfig,
};
pub use prolly::stats::{StatsComparison, StatsDiff, StatsPercentageChange, TreeStats};
#[cfg(feature = "async-store")]
pub use prolly::store::{AsyncStore, SyncStoreAsAsync};
pub use prolly::store::{
    BatchOp, FileNodeStore, FileNodeStoreError, MemStore, MemStoreError, NodeStoreScan, Store,
};
#[cfg(feature = "rocksdb")]
pub use prolly::store::{CompressionType, RocksDBConfig, RocksDBStore, RocksDBStoreError};
#[cfg(feature = "pglite")]
pub use prolly::store::{PgliteStore, PgliteStoreConfig, PgliteStoreError};
#[cfg(feature = "slatedb")]
pub use prolly::store::{SlateDbStore, SlateDbStoreConfig, SlateDbStoreError};
#[cfg(feature = "sqlite")]
pub use prolly::store::{SqliteStore, SqliteStoreConfig, SqliteStoreError};
#[cfg(feature = "tokio")]
pub use prolly::store::{TokioBlockingStore, TokioBlockingStoreError};
pub use prolly::sync::{
    MissingNodeCopy, MissingNodePlan, SnapshotBundle, SnapshotBundleNode, SnapshotBundleSummary,
    SnapshotBundleVerification, SNAPSHOT_BUNDLE_FORMAT_VERSION,
};
pub use prolly::tombstone::{
    is_tombstone_value, tombstone_compaction, tombstone_upsert, Tombstone,
};
pub use prolly::tree::Tree;
pub use prolly::value::{
    decode_cbor, decode_json, encode_cbor, encode_json, CborCodec, JsonCodec, ValueCodec,
    VersionedCborCodec, VersionedJsonCodec, VersionedValue,
};
#[cfg(feature = "async-store")]
pub use prolly::AsyncProlly;

// Re-export constants
pub use prolly::encoding::{
    DEFAULT_CHUNKING_FACTOR, DEFAULT_HASH_SEED, DEFAULT_MAX_CHUNK_SIZE, DEFAULT_MIN_CHUNK_SIZE,
    INIT_LEVEL,
};
