//! Error types for Prolly Trees

use super::cid::Cid;
use serde::{Deserialize, Serialize};

/// A mutation to apply to the tree
///
/// Represents a single operation in a batch mutation: either an upsert (insert or update)
/// or a delete operation.
///
#[derive(Clone, Debug, PartialEq)]
pub enum Mutation {
    /// Insert or update a key-value pair
    Upsert { key: Vec<u8>, val: Vec<u8> },
    /// Delete a key
    Delete { key: Vec<u8> },
}

impl Mutation {
    /// Get the key for this mutation
    ///
    pub fn key(&self) -> &[u8] {
        match self {
            Mutation::Upsert { key, .. } => key,
            Mutation::Delete { key } => key,
        }
    }

    /// Check if this is a delete mutation
    pub fn is_delete(&self) -> bool {
        matches!(self, Mutation::Delete { .. })
    }
}

/// Difference between two trees
///
/// Represents a single change between a base tree and another tree.
///
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Diff {
    /// Entry exists in the new tree but not in the base tree
    Added { key: Vec<u8>, val: Vec<u8> },
    /// Entry exists in the base tree but not in the new tree
    Removed { key: Vec<u8>, val: Vec<u8> },
    /// Entry exists in both trees but with different values
    Changed {
        key: Vec<u8>,
        old: Vec<u8>,
        new: Vec<u8>,
    },
}

impl Diff {
    /// Borrow the key affected by this diff entry.
    pub fn key(&self) -> &[u8] {
        match self {
            Diff::Added { key, .. } | Diff::Removed { key, .. } | Diff::Changed { key, .. } => key,
        }
    }
}

/// Merge conflict information
///
/// Contains all the information needed to resolve a conflict during a three-way merge.
///
#[derive(Clone, Debug)]
pub struct Conflict {
    /// The key where the conflict occurred
    pub key: Vec<u8>,
    /// The value in the base tree (None if key didn't exist in base)
    pub base: Option<Vec<u8>>,
    /// The value in the left tree (None if the key is absent)
    pub left: Option<Vec<u8>>,
    /// The value in the right tree (None if the key is absent)
    pub right: Option<Vec<u8>>,
}

/// Resolution for a standard three-way merge conflict.
///
/// `Value` keeps the key with the provided value, `Delete` removes the key,
/// and `Unresolved` returns [`Error::Conflict`] to the caller.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// Keep the key with this value.
    Value(Vec<u8>),
    /// Delete the key from the merged tree.
    Delete,
    /// Leave the conflict unresolved.
    Unresolved,
}

impl Resolution {
    /// Resolve the conflict to a concrete value.
    pub fn value(value: impl Into<Vec<u8>>) -> Self {
        Self::Value(value.into())
    }

    /// Resolve the conflict by deleting the key.
    pub fn delete() -> Self {
        Self::Delete
    }

    /// Leave the conflict unresolved.
    pub fn unresolved() -> Self {
        Self::Unresolved
    }
}

/// Conflict resolution strategy
///
/// A function that takes a conflict and returns an explicit resolution.
/// If [`Resolution::Unresolved`] is returned, the merge will fail with a
/// `Conflict` error.
///
///
/// # Example
/// ```
/// use prolly::{Resolution, Resolver};
///
/// // Always prefer the left value
/// let prefer_left: Resolver = Box::new(|conflict| {
///     match &conflict.left {
///         Some(value) => Resolution::value(value.clone()),
///         None => Resolution::delete(),
///     }
/// });
///
/// // Always prefer the right value
/// let prefer_right: Resolver = Box::new(|conflict| {
///     match &conflict.right {
///         Some(value) => Resolution::value(value.clone()),
///         None => Resolution::delete(),
///     }
/// });
///
/// // Concatenate values
/// let concat: Resolver = Box::new(|conflict| {
///     match (&conflict.left, &conflict.right) {
///         (Some(left), Some(right)) => {
///             let mut result = left.clone();
///             result.extend(right);
///             Resolution::value(result)
///         }
///         _ => Resolution::unresolved(),
///     }
/// });
/// ```
pub type Resolver = Box<dyn Fn(&Conflict) -> Resolution>;

/// Ready-made standard merge resolvers.
pub mod resolver {
    use super::{Conflict, Resolution};

    /// Prefer the left side. If left deleted the key, delete it.
    pub fn prefer_left(conflict: &Conflict) -> Resolution {
        match &conflict.left {
            Some(value) => Resolution::value(value.clone()),
            None => Resolution::delete(),
        }
    }

    /// Prefer the right side. If right deleted the key, delete it.
    pub fn prefer_right(conflict: &Conflict) -> Resolution {
        match &conflict.right {
            Some(value) => Resolution::value(value.clone()),
            None => Resolution::delete(),
        }
    }

    /// Delete the key when either side deleted it; otherwise leave unresolved.
    pub fn delete_wins(conflict: &Conflict) -> Resolution {
        if conflict.left.is_none() || conflict.right.is_none() {
            Resolution::delete()
        } else {
            Resolution::unresolved()
        }
    }

    /// Keep the updated side for delete/update conflicts; otherwise leave unresolved.
    pub fn update_wins(conflict: &Conflict) -> Resolution {
        match (&conflict.left, &conflict.right) {
            (Some(value), None) | (None, Some(value)) => Resolution::value(value.clone()),
            _ => Resolution::unresolved(),
        }
    }
}

use super::secondary_index::IndexProjection;
use super::transaction::TransactionConflict;
use super::versioned_map::MapVersionId;

/// Prolly tree errors
#[derive(Debug)]
pub enum Error {
    /// Node not found in store
    NotFound(Cid),
    /// Invalid node structure
    InvalidNode,
    /// Deserialization failed
    Deserialize(String),
    /// Serialization failed
    Serialize(String),
    /// Storage error
    Store(Box<dyn std::error::Error + Send + Sync>),
    /// Stored bytes did not hash to the CID they were stored under.
    CidMismatch { expected: Cid, actual: Cid },
    /// Merge conflict - occurs when both trees modify the same key differently
    /// and no resolver is provided or the resolver returns `Resolution::Unresolved`
    ///
    Conflict(Conflict),
    /// Mutation buffer is full - adding a mutation would exceed the buffer size limit
    BufferFull,
    /// Sorted bulk loading received keys out of order.
    UnsortedInput { previous: Vec<u8>, next: Vec<u8> },
    /// A GC retention policy referenced named roots that were not present.
    MissingNamedRoots { names: Vec<Vec<u8>> },
    /// A portable snapshot bundle is malformed or not self-contained.
    InvalidSnapshotBundle(String),
    /// The configured store does not support strict atomic transactions.
    UnsupportedTransactions { store: &'static str },
    /// A transaction could not commit because a validated named root changed.
    TransactionConflict(TransactionConflict),
    /// A built-in versioned-map catalog is missing or internally inconsistent.
    InvalidVersionedMap(String),
    /// A runtime secondary-index definition is invalid.
    InvalidIndexDefinition { reason: String },
    /// Persisted active index semantics have no matching runtime extractor.
    IndexRuntimeDefinitionMissing { name: Vec<u8>, generation: u64 },
    /// Runtime and persisted descriptor fingerprints disagree.
    IndexDefinitionMismatch {
        name: Vec<u8>,
        persisted: Cid,
        runtime: Cid,
    },
    /// A managed source map must be mutated through `IndexedMap`.
    IndexesRequireIndexedMap {
        map_id: Vec<u8>,
        active_indexes: Vec<Vec<u8>>,
    },
    /// The requested operation has no safe indexed implementation in v1.
    IndexOperationUnsupported { operation: &'static str },
    /// An application extractor rejected one source record.
    IndexExtractionFailed {
        name: Vec<u8>,
        primary_key: Vec<u8>,
        reason: String,
    },
    /// An extractor emission is incompatible with its projection mode.
    IndexProjectionMismatch {
        name: Vec<u8>,
        mode: IndexProjection,
        primary_key: Vec<u8>,
    },
    /// One source record emitted different projections for the same term.
    ConflictingIndexProjection {
        name: Vec<u8>,
        primary_key: Vec<u8>,
        term: Vec<u8>,
    },
    /// Repeated source movement prevented index activation.
    IndexBuildConflictLimitExceeded { name: Vec<u8>, attempts: usize },
    /// No exact checkpoint exists for an index at the selected source version.
    IndexUnavailableAtVersion {
        name: Vec<u8>,
        source_version: MapVersionId,
    },
    /// A persisted checkpoint disagrees with the selected source or index root.
    IndexCheckpointMismatch {
        name: Vec<u8>,
        source_version: MapVersionId,
        reason: String,
    },
    /// A cursor belongs to a different immutable indexed snapshot.
    IndexCursorVersionMismatch { expected: String, actual: String },
    /// Index work exceeded a configured resource bound.
    IndexResourceLimitExceeded {
        resource: &'static str,
        limit: usize,
        actual: usize,
    },
    /// A current indexed-snapshot bundle is malformed or inconsistent.
    InvalidIndexedSnapshotBundle { reason: String },
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::NotFound(cid) => write!(f, "node not found: {:?}", cid),
            Error::InvalidNode => write!(f, "invalid node structure"),
            Error::Deserialize(e) => write!(f, "deserialize error: {}", e),
            Error::Serialize(e) => write!(f, "serialize error: {}", e),
            Error::Store(e) => write!(f, "storage error: {}", e),
            Error::CidMismatch { expected, actual } => {
                write!(
                    f,
                    "content CID mismatch: expected {:?}, got {:?}",
                    expected, actual
                )
            }
            Error::Conflict(c) => write!(f, "merge conflict at key: {:?}", c.key),
            Error::BufferFull => write!(f, "mutation buffer is full"),
            Error::UnsortedInput { previous, next } => write!(
                f,
                "sorted input keys are out of order: previous={:?} next={:?}",
                previous, next
            ),
            Error::MissingNamedRoots { names } => {
                write!(f, "missing named roots for retention policy: {:?}", names)
            }
            Error::InvalidSnapshotBundle(message) => {
                write!(f, "invalid snapshot bundle: {message}")
            }
            Error::UnsupportedTransactions { store } => {
                write!(f, "store does not support strict transactions: {store}")
            }
            Error::TransactionConflict(conflict) => {
                write!(
                    f,
                    "transaction conflict for named root: {:?}",
                    conflict.name
                )
            }
            Error::InvalidVersionedMap(message) => {
                write!(f, "invalid versioned map: {message}")
            }
            Error::InvalidIndexDefinition { reason } => {
                write!(f, "invalid secondary index definition: {reason}")
            }
            Error::IndexRuntimeDefinitionMissing { name, generation } => write!(
                f,
                "runtime secondary index definition missing: name={name:?} generation={generation}"
            ),
            Error::IndexDefinitionMismatch {
                name,
                persisted,
                runtime,
            } => write!(
                f,
                "secondary index definition mismatch: name={name:?} persisted={persisted:?} runtime={runtime:?}"
            ),
            Error::IndexesRequireIndexedMap {
                map_id,
                active_indexes,
            } => write!(
                f,
                "managed map requires IndexedMap coordinator: map_id={map_id:?} active_indexes={active_indexes:?}"
            ),
            Error::IndexOperationUnsupported { operation } => {
                write!(f, "indexed map operation is unsupported in v1: {operation}")
            }
            Error::IndexExtractionFailed {
                name,
                primary_key,
                reason,
            } => write!(
                f,
                "secondary index extraction failed: name={name:?} primary_key={primary_key:?}: {reason}"
            ),
            Error::IndexProjectionMismatch {
                name,
                mode,
                primary_key,
            } => write!(
                f,
                "secondary index projection mismatch: name={name:?} mode={mode:?} primary_key={primary_key:?}"
            ),
            Error::ConflictingIndexProjection {
                name,
                primary_key,
                term,
            } => write!(
                f,
                "conflicting secondary index projection: name={name:?} primary_key={primary_key:?} term={term:?}"
            ),
            Error::IndexBuildConflictLimitExceeded { name, attempts } => write!(
                f,
                "secondary index build conflict limit exceeded: name={name:?} attempts={attempts}"
            ),
            Error::IndexUnavailableAtVersion {
                name,
                source_version,
            } => write!(
                f,
                "secondary index unavailable at source version: name={name:?} source_version={source_version}"
            ),
            Error::IndexCheckpointMismatch {
                name,
                source_version,
                reason,
            } => write!(
                f,
                "secondary index checkpoint mismatch: name={name:?} source_version={source_version}: {reason}"
            ),
            Error::IndexCursorVersionMismatch { expected, actual } => write!(
                f,
                "secondary index cursor snapshot mismatch: expected={expected} actual={actual}"
            ),
            Error::IndexResourceLimitExceeded {
                resource,
                limit,
                actual,
            } => write!(
                f,
                "secondary index resource limit exceeded: resource={resource} limit={limit} actual={actual}"
            ),
            Error::InvalidIndexedSnapshotBundle { reason } => {
                write!(f, "invalid indexed snapshot bundle: {reason}")
            }
        }
    }
}

impl std::error::Error for Error {}
