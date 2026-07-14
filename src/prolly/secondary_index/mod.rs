//! Strict, synchronous secondary indexes for [`VersionedMap`](super::versioned_map::VersionedMap).
//!
//! The low-level tree remains index-agnostic. This module defines the runtime
//! contracts used by the `IndexedMap` coordinator and its persisted catalog.

mod coordinator;
mod definition;
mod snapshot;
mod storage;

pub use coordinator::{
    ActiveIndexHealth, IndexBuildResult, IndexVerification, IndexedMap, IndexedMapEditor,
    IndexedMapHealth, IndexedMapMetricsSnapshot, IndexedMapUpdate, IndexedVersion,
};
pub use definition::{
    IndexProjection, SecondaryIndex, SecondaryIndexBuilder, SecondaryIndexEntry,
    SecondaryIndexError, SecondaryIndexExtractor, SecondaryIndexLimits, SecondaryIndexRegistry,
};
pub use snapshot::{
    IndexedSnapshot, IndexedSnapshotId, SecondaryIndexCursor, SecondaryIndexDirection,
    SecondaryIndexMatch, SecondaryIndexPage, SecondaryIndexSnapshot,
};
pub use storage::{
    catalog_checkpoint_key, catalog_current_key, catalog_descriptor_key, catalog_format_key,
    catalog_map_id, catalog_retired_key, control_record_key, control_root_name,
    decode_physical_index_key, descriptor_fingerprint, index_map_id, physical_index_key,
    term_bounds_exact, term_bounds_prefix, term_bounds_range, ActiveIndexControl,
    DecodedPhysicalIndexKey, IndexCheckpoint, IndexControl, IndexValue, IndexedHeadRecord,
    SecondaryIndexDescriptor, TermBounds, INDEX_PHYSICAL_LAYOUT_VERSION,
    SECONDARY_INDEX_FORMAT_VERSION,
};
