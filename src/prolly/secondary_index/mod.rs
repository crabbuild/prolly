//! Strict, synchronous secondary indexes for [`VersionedMap`](super::versioned_map::VersionedMap).
//!
//! The low-level tree remains index-agnostic. This module defines the runtime
//! contracts used by the `IndexedMap` coordinator and its persisted catalog.

mod definition;

pub use definition::{
    IndexProjection, SecondaryIndex, SecondaryIndexBuilder, SecondaryIndexEntry,
    SecondaryIndexError, SecondaryIndexExtractor, SecondaryIndexLimits, SecondaryIndexRegistry,
};
