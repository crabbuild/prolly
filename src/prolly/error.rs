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

use super::transaction::TransactionConflict;

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
    /// A proximity-map shape configuration is invalid.
    InvalidProximityConfig { reason: String },
    /// Persisted proximity bytes use a format version this build does not read.
    UnsupportedProximityVersion { found: u8, required: u8 },
    /// A vector is incompatible with the proximity-map configuration.
    InvalidProximityVector { reason: String },
    /// A proximity build or mutation contains the same logical key twice.
    DuplicateProximityKey { key: Vec<u8> },
    /// Proximity search options are invalid.
    InvalidProximitySearch { reason: String },
    /// A persisted proximity record, node, or descriptor is malformed.
    InvalidProximityObject { kind: &'static str, reason: String },
    /// One canonical proximity node exceeds the configured hard byte limit.
    ProximityNodeTooLarge {
        level: u8,
        entries: usize,
        encoded_bytes: usize,
        limit: usize,
    },
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
            Error::InvalidProximityConfig { reason } => {
                write!(f, "invalid proximity configuration: {reason}")
            }
            Error::UnsupportedProximityVersion { found, required } => write!(
                f,
                "unsupported proximity format version: found={found} required={required}"
            ),
            Error::InvalidProximityVector { reason } => {
                write!(f, "invalid proximity vector: {reason}")
            }
            Error::DuplicateProximityKey { key } => {
                write!(f, "duplicate proximity key: {key:?}")
            }
            Error::InvalidProximitySearch { reason } => {
                write!(f, "invalid proximity search options: {reason}")
            }
            Error::InvalidProximityObject { kind, reason } => {
                write!(f, "invalid proximity {kind}: {reason}")
            }
            Error::ProximityNodeTooLarge {
                level,
                entries,
                encoded_bytes,
                limit,
            } => write!(
                f,
                "proximity node exceeds byte limit: level={level} entries={entries} bytes={encoded_bytes} limit={limit}"
            ),
        }
    }
}

impl std::error::Error for Error {}
