//! Strict, synchronous secondary indexes for [`VersionedMap`](super::versioned_map::VersionedMap).
//!
//! The low-level tree remains index-agnostic. This module defines the runtime
//! contracts used by the `IndexedMap` coordinator and canonical collection state.

mod async_coordinator;
mod async_snapshot;
mod budget;
mod bundle;
mod coordinator;
mod definition;
mod publication;
mod snapshot;
mod state;
mod storage;
mod workspace;

pub use async_coordinator::{AsyncIndexedMap, AsyncIndexedSourceView};
pub use async_snapshot::{
    AsyncIndexedSnapshot, AsyncSecondaryIndexQuery, AsyncSecondaryIndexSnapshot,
};
pub use budget::{MaintenanceBudget, MutationBudget, QueryBudget, TransferBudget};
pub use bundle::{
    IndexedSnapshotBundle, IndexedSnapshotBundleIndex, IndexedSnapshotBundleSummary,
    IndexedSnapshotBundleVerification, INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION,
};
pub use coordinator::{
    ActiveIndexHealth, IndexBuildResult, IndexVerification, IndexedMap, IndexedMapEditor,
    IndexedMapHealth, IndexedMapMetricsSnapshot, IndexedMapUpdate, IndexedRetentionResult,
    IndexedVersion, SnapshotPinGuard,
};
pub use definition::{
    IndexProjection, SecondaryIndex, SecondaryIndexBuilder, SecondaryIndexEntry,
    SecondaryIndexEntryRef, SecondaryIndexError, SecondaryIndexExtractor, SecondaryIndexLimits,
    SecondaryIndexRegistry, StreamingSecondaryIndexExtractor,
};
pub use publication::{AsyncIndexedStore, IndexedStore};
pub use snapshot::{
    IndexedSnapshot, IndexedSourceRecord, IndexedSourceRecordRef, ProjectedIndexEntry,
    SecondaryIndexCursor, SecondaryIndexDirection, SecondaryIndexMatch, SecondaryIndexMatchRef,
    SecondaryIndexPage, SecondaryIndexQuery, SecondaryIndexSnapshot,
};
pub use state::{
    indexed_collection_root_name, CollectionIndexPolicy, IndexDescriptor, IndexSemanticLimits,
    IndexSnapshotRef, IndexedCollectionState, IndexedSnapshotId, IndexedSnapshotRecord,
    SnapshotPin, SourceSnapshotRef, INDEXED_COLLECTION_FORMAT,
};
pub use storage::{
    decode_physical_index_key, decode_physical_index_key_ref, physical_index_key,
    term_bounds_exact, term_bounds_prefix, term_bounds_range, DecodedPhysicalIndexKey,
    DecodedPhysicalIndexKeyRef, IndexValue, IndexValueRef, TermBounds,
    INDEX_PHYSICAL_LAYOUT_VERSION,
};
