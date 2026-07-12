//! CRDT-style conflict-free merge operations for Prolly trees
//!
//! This module provides automatic conflict resolution during merges using
//! CRDT (Conflict-free Replicated Data Type) semantics. Unlike the standard
//! merge which can return conflicts, CRDT merges always succeed by applying
//! deterministic resolution strategies.
//!
//! # Overview
//!
//! CRDT merges are beneficial when:
//! - Building distributed systems where concurrent modifications are common
//! - Automatic conflict resolution is preferred over manual intervention
//! - Eventual consistency is acceptable
//!
//! # Merge Strategies
//!
//! - **Last-Writer-Wins (LWW)**: Value with higher timestamp wins
//! - **Multi-Value (MV)**: Preserve all concurrent values as a set
//! - **Custom**: User-provided merge function for domain-specific resolution
//!
//! # Example: Last-Writer-Wins Merge
//!
//! ```rust
//! use prolly::{Prolly, MemStore, Config, CrdtConfig, MergeStrategy, DeletePolicy};
//! use prolly::{ConflictFreeMerger, DefaultConflictFreeMerger, TimestampedValue};
//!
//! let store = MemStore::new();
//! let prolly = Prolly::new(store, Config::default());
//!
//! let base = prolly.create();
//!
//! // Use timestamped values for LWW
//! let base = prolly.put(&base, b"key".to_vec(),
//!     TimestampedValue::new(b"original".to_vec(), 100).to_bytes()).unwrap();
//!
//! // Create divergent branches with different timestamps
//! let left = prolly.put(&base, b"key".to_vec(),
//!     TimestampedValue::new(b"left_value".to_vec(), 200).to_bytes()).unwrap();
//! let right = prolly.put(&base, b"key".to_vec(),
//!     TimestampedValue::new(b"right_value".to_vec(), 150).to_bytes()).unwrap();
//!
//! // CRDT merge - left wins because it has higher timestamp (200 > 150)
//! let merger = DefaultConflictFreeMerger::new();
//! let config = CrdtConfig::lww();
//! // let merged = merger.crdt_merge(&store, &base, &left, &right, &config).unwrap();
//! ```
//!
//! # Example: Multi-Value Merge
//!
//! ```rust
//! use prolly::{CrdtConfig, MergeStrategy, MultiValueSet};
//!
//! // Configure for multi-value merge
//! let config = CrdtConfig::multi_value();
//!
//! // When conflicts occur, both values are preserved in a MultiValueSet
//! let mv = MultiValueSet::from_values(vec![b"value1".to_vec(), b"value2".to_vec()]);
//! assert_eq!(mv.len(), 2);
//! ```
//!
//! # Example: Custom Merge Function
//!
//! ```rust
//! use prolly::{CrdtConfig, CrdtResolution};
//!
//! // Custom merge: concatenate values
//! let config = CrdtConfig::custom(|conflict| {
//!     match (&conflict.left, &conflict.right) {
//!         (Some(left), Some(right)) => {
//!             let mut result = left.clone();
//!             result.extend(right);
//!             CrdtResolution::value(result)
//!         }
//!         _ => CrdtResolution::delete(),
//!     }
//! });
//! ```

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Data Models
// ============================================================================

/// A value with an embedded timestamp for LWW ordering.
///
/// Used with the Last-Writer-Wins merge strategy to determine which
/// value should win when concurrent modifications occur.
///
/// # Serialization Format
/// Values are serialized as: `[value bytes][8-byte big-endian timestamp]`
///
/// # Example
/// ```rust
/// use prolly::TimestampedValue;
///
/// let tv = TimestampedValue::now(b"hello".to_vec());
/// let bytes = tv.to_bytes();
/// let restored = TimestampedValue::from_bytes(&bytes).unwrap();
/// assert_eq!(restored.value, b"hello");
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimestampedValue {
    /// The actual value bytes
    pub value: Vec<u8>,
    /// Timestamp in microseconds since epoch
    pub timestamp: u64,
}

impl TimestampedValue {
    /// Create a new timestamped value with the given timestamp.
    pub fn new(value: Vec<u8>, timestamp: u64) -> Self {
        Self { value, timestamp }
    }

    /// Create a new timestamped value with current time.
    pub fn now(value: Vec<u8>) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;
        Self { value, timestamp }
    }

    /// Serialize to bytes (value + 8-byte timestamp suffix).
    ///
    /// Format: `[value bytes][8-byte big-endian timestamp]`
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.value.clone();
        bytes.extend(&self.timestamp.to_be_bytes());
        bytes
    }

    /// Deserialize from bytes.
    ///
    /// Returns `None` if the byte slice is too short (< 8 bytes).
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 8 {
            return None;
        }
        let (value, ts_bytes) = bytes.split_at(bytes.len() - 8);
        let timestamp = u64::from_be_bytes(ts_bytes.try_into().ok()?);
        Some(Self {
            value: value.to_vec(),
            timestamp,
        })
    }
}

/// A set of concurrent values for MV-Register semantics.
///
/// Used with the Multi-Value merge strategy to preserve all concurrent
/// values when conflicts occur, allowing the application to resolve
/// them later or present all options to the user.
///
/// # Serialization Format
/// Values are serialized as: `[4-byte count][4-byte len1][value1][4-byte len2][value2]...`
///
/// # Example
/// ```rust
/// use prolly::MultiValueSet;
///
/// let mv = MultiValueSet::single(b"value1".to_vec());
/// let mv2 = MultiValueSet::single(b"value2".to_vec());
/// let merged = mv.merge(&mv2);
/// assert_eq!(merged.values.len(), 2);
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultiValueSet {
    /// All concurrent values (sorted for deterministic ordering)
    pub values: Vec<Vec<u8>>,
}

impl MultiValueSet {
    /// Create from a single value.
    pub fn single(value: Vec<u8>) -> Self {
        Self {
            values: vec![value],
        }
    }

    /// Create from multiple values.
    pub fn from_values(mut values: Vec<Vec<u8>>) -> Self {
        // Sort and deduplicate for deterministic ordering
        values.sort();
        values.dedup();
        Self { values }
    }

    /// Merge two sets, combining all unique values.
    ///
    /// The resulting set contains all values from both sets,
    /// sorted lexicographically for deterministic ordering.
    pub fn merge(&self, other: &Self) -> Self {
        let mut values = Vec::with_capacity(self.values.len().saturating_add(other.values.len()));
        values.extend(self.values.iter().cloned());
        values.extend(other.values.iter().cloned());
        Self::from_values(values)
    }

    /// Serialize to bytes (length-prefixed values).
    ///
    /// Format: `[4-byte count][4-byte len][value]...`
    pub fn to_bytes(&self) -> Vec<u8> {
        // Keep persisted bytes canonical even if callers constructed the
        // public `values` field directly rather than through `from_values`.
        let canonical = Self::from_values(self.values.clone());
        let mut bytes = Vec::new();
        bytes.extend(&(canonical.values.len() as u32).to_be_bytes());
        for v in &canonical.values {
            bytes.extend(&(v.len() as u32).to_be_bytes());
            bytes.extend(v);
        }
        bytes
    }

    /// Deserialize from bytes.
    ///
    /// Returns `None` if the byte slice is malformed.
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 {
            return None;
        }
        let count = u32::from_be_bytes(bytes[..4].try_into().ok()?) as usize;
        // Every encoded value needs at least its four-byte length. Reject an
        // impossible count before allocating from untrusted bytes.
        if count > (bytes.len() - 4) / 4 {
            return None;
        }
        let mut values = Vec::with_capacity(count);
        let mut offset = 4;
        for _ in 0..count {
            if offset + 4 > bytes.len() {
                return None;
            }
            let len = u32::from_be_bytes(bytes[offset..offset + 4].try_into().ok()?) as usize;
            offset += 4;
            let end = offset.checked_add(len)?;
            if end > bytes.len() {
                return None;
            }
            values.push(bytes[offset..end].to_vec());
            offset = end;
        }
        // A valid prefix followed by unrelated bytes must not be accepted as a
        // complete set during MV merges.
        if offset != bytes.len() {
            return None;
        }
        Some(Self::from_values(values))
    }

    /// Check if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Get the number of values in the set.
    pub fn len(&self) -> usize {
        self.values.len()
    }
}

// ============================================================================
// Merge Strategy and Configuration
// ============================================================================

/// Conflict-free custom resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CrdtResolution {
    /// Keep the key with this value.
    Value(Vec<u8>),
    /// Delete the key from the merged tree.
    Delete,
}

impl CrdtResolution {
    /// Resolve the conflict to a concrete value.
    pub fn value(value: impl Into<Vec<u8>>) -> Self {
        Self::Value(value.into())
    }

    /// Resolve the conflict by deleting the key.
    pub fn delete() -> Self {
        Self::Delete
    }
}

/// Custom merge function type.
///
/// Takes a full conflict, including delete/absence state, and returns a
/// conflict-free value-or-delete resolution.
pub type CustomMergeFn = Arc<dyn Fn(&Conflict) -> CrdtResolution + Send + Sync>;

/// Merge strategy for conflict-free merging.
///
/// Determines how conflicts are resolved when both branches modify the same key.
#[derive(Clone, Default)]
pub enum MergeStrategy {
    /// Last-Writer-Wins: value with higher timestamp wins.
    ///
    /// Requires values to be serialized using [`TimestampedValue`] format,
    /// or a custom timestamp extractor to be provided in [`CrdtConfig`].
    #[default]
    LastWriterWins,

    /// Multi-Value: preserve all concurrent values as a set.
    ///
    /// When conflicts occur, both values are combined into a [`MultiValueSet`].
    /// This allows the application to resolve conflicts later or present
    /// all options to the user.
    MultiValue,

    /// Custom: use provided merge function.
    ///
    /// The function receives the full conflict and returns a value-or-delete
    /// resolution. It must be pure and deterministic: the merge engine may
    /// evaluate the same conflict again when a structural fast path falls back
    /// to the general diff/batch path. Symmetric results are also required if
    /// callers expect convergence when left and right are exchanged.
    Custom(CustomMergeFn),
}

impl std::fmt::Debug for MergeStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MergeStrategy::LastWriterWins => write!(f, "LastWriterWins"),
            MergeStrategy::MultiValue => write!(f, "MultiValue"),
            MergeStrategy::Custom(_) => write!(f, "Custom(<fn>)"),
        }
    }
}

/// Policy for delete vs update conflicts.
///
/// Determines what happens when one branch deletes a key while
/// another branch modifies it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum DeletePolicy {
    /// Deletes win over updates.
    ///
    /// If one branch deletes a key and another modifies it,
    /// the key will be absent from the merged result.
    DeleteWins,

    /// Updates win over deletes.
    ///
    /// If one branch deletes a key and another modifies it,
    /// the key will be present with the updated value.
    #[default]
    UpdateWins,
}

/// Timestamp extractor function type.
///
/// Extracts a timestamp from a value for LWW comparison.
pub type TimestampExtractor = Arc<dyn Fn(&[u8]) -> u64 + Send + Sync>;

/// Configuration for conflict-free merging.
///
/// Controls how the CRDT merger resolves conflicts between concurrent
/// modifications.
///
/// # Example
/// ```rust
/// use prolly::{CrdtConfig, MergeStrategy, DeletePolicy};
///
/// let config = CrdtConfig {
///     strategy: MergeStrategy::LastWriterWins,
///     delete_policy: DeletePolicy::UpdateWins,
///     timestamp_extractor: None,
/// };
/// ```
#[derive(Clone)]
pub struct CrdtConfig {
    /// The merge strategy to use for resolving conflicts.
    pub strategy: MergeStrategy,

    /// Policy for delete vs update conflicts.
    pub delete_policy: DeletePolicy,

    /// Function to extract timestamp from value (for LWW).
    ///
    /// If `None`, values are assumed to be in [`TimestampedValue`] format.
    /// If extraction fails, timestamp defaults to 0.
    pub timestamp_extractor: Option<TimestampExtractor>,
}

impl std::fmt::Debug for CrdtConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CrdtConfig")
            .field("strategy", &self.strategy)
            .field("delete_policy", &self.delete_policy)
            .field(
                "timestamp_extractor",
                &self.timestamp_extractor.as_ref().map(|_| "<fn>"),
            )
            .finish()
    }
}

impl Default for CrdtConfig {
    fn default() -> Self {
        Self {
            strategy: MergeStrategy::LastWriterWins,
            delete_policy: DeletePolicy::UpdateWins,
            timestamp_extractor: None,
        }
    }
}

impl CrdtConfig {
    /// Create a new CrdtConfig with LWW strategy.
    pub fn lww() -> Self {
        Self {
            strategy: MergeStrategy::LastWriterWins,
            ..Default::default()
        }
    }

    /// Create a new CrdtConfig with MV strategy.
    pub fn multi_value() -> Self {
        Self {
            strategy: MergeStrategy::MultiValue,
            ..Default::default()
        }
    }

    /// Create a new CrdtConfig with a custom merge function.
    ///
    /// The callback must be pure and deterministic and may be invoked more than
    /// once for a conflict while the engine changes execution paths.
    pub fn custom<F>(merge_fn: F) -> Self
    where
        F: Fn(&Conflict) -> CrdtResolution + Send + Sync + 'static,
    {
        Self {
            strategy: MergeStrategy::Custom(Arc::new(merge_fn)),
            ..Default::default()
        }
    }

    /// Set the delete policy.
    pub fn with_delete_policy(mut self, policy: DeletePolicy) -> Self {
        self.delete_policy = policy;
        self
    }

    /// Set a custom timestamp extractor for LWW strategy.
    pub fn with_timestamp_extractor<F>(mut self, extractor: F) -> Self
    where
        F: Fn(&[u8]) -> u64 + Send + Sync + 'static,
    {
        self.timestamp_extractor = Some(Arc::new(extractor));
        self
    }
}

// ============================================================================
// ConflictFreeMerger Trait
// ============================================================================

use super::error::{Conflict, Error, Resolution, Resolver};
use super::store::Store;
use super::tree::Tree;
use super::Prolly;

/// Trait for conflict-free merge operations using CRDT semantics.
///
/// Unlike the standard merge which can return `Error::Conflict`, implementations
/// of this trait guarantee that merges always succeed by applying deterministic
/// resolution strategies. This makes it suitable for distributed systems where
/// concurrent modifications are common and automatic resolution is preferred.
///
/// This API performs a conflict-free *three-way* merge over immutable tree
/// snapshots. The built-in LWW and multi-value resolvers are symmetric and
/// deterministic for a fixed base. This is not an operation-log CRDT: callers
/// must still retain ancestry, and custom resolvers are responsible for their
/// own determinism and symmetry.
///
/// # Guarantees
///
/// - **Never returns `Error::Conflict`**: All conflicts are automatically resolved
/// - **Deterministic built-ins**: Same inputs produce the same output
/// - **Preserves non-conflicting changes**: Changes to different keys are always preserved
///
/// # Type Parameters
/// * `S` - The storage backend type implementing [`Store`]
///
/// # Example
///
/// ```rust
/// use prolly::{Prolly, MemStore, Config, ConflictFreeMerger, DefaultConflictFreeMerger, CrdtConfig};
///
/// let store = MemStore::new();
/// let prolly = Prolly::new(store, Config::default());
///
/// let base = prolly.create();
/// let base = prolly.put(&base, b"key".to_vec(), b"value".to_vec()).unwrap();
///
/// // Create divergent branches
/// let left = prolly.put(&base, b"key".to_vec(), b"left".to_vec()).unwrap();
/// let right = prolly.put(&base, b"key".to_vec(), b"right".to_vec()).unwrap();
///
/// // CRDT merge never fails
/// let merger = DefaultConflictFreeMerger;
/// let config = CrdtConfig::default();
/// // merged = merger.crdt_merge(&store, &base, &left, &right, &config).unwrap();
/// ```
pub trait ConflictFreeMerger<S: Store> {
    /// Merge two trees without conflicts using CRDT semantics.
    ///
    /// Performs a three-way merge using `base` as the common ancestor.
    /// All conflicts are automatically resolved using the configured strategy.
    ///
    /// # Arguments
    /// * `store` - The storage backend
    /// * `base` - The common ancestor tree
    /// * `left` - The left branch tree
    /// * `right` - The right branch tree
    /// * `config` - CRDT configuration specifying merge strategy and policies
    ///
    /// # Returns
    /// * `Ok(Tree)` - The merged tree (never returns `Error::Conflict`)
    /// * `Err(Error)` - Only on storage or deserialization errors
    ///
    /// # Conflict Resolution
    ///
    /// When both branches modify the same key differently:
    /// - **LWW**: Value with higher timestamp wins; ties broken lexicographically
    /// - **MV**: Both values are combined into a [`MultiValueSet`]
    /// - **Custom**: User-provided function determines the result
    ///
    /// When one branch deletes and another modifies:
    /// - **DeleteWins**: Key is absent from result
    /// - **UpdateWins**: Key is present with the updated value
    fn crdt_merge(
        &self,
        store: &S,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        config: &CrdtConfig,
    ) -> Result<Tree, Error>;
}

/// Default conflict-free merger implementation.
///
/// Implements CRDT-style merging with support for:
/// - Last-Writer-Wins (LWW) strategy with timestamp comparison
/// - Multi-Value (MV) strategy preserving all concurrent values
/// - Custom merge functions for domain-specific resolution
/// - Configurable delete policies
///
/// # Example
///
/// ```rust
/// use prolly::{DefaultConflictFreeMerger, CrdtConfig, MergeStrategy};
///
/// let merger = DefaultConflictFreeMerger::new();
/// let config = CrdtConfig::lww();
/// // Use with crdt_merge method
/// ```
#[derive(Debug, Default, Clone, Copy)]
pub struct DefaultConflictFreeMerger;

impl DefaultConflictFreeMerger {
    /// Create a new DefaultConflictFreeMerger.
    pub fn new() -> Self {
        Self
    }

    /// Extract timestamp from a value using the configured extractor or default format.
    fn extract_timestamp(value: &[u8], config: &CrdtConfig) -> u64 {
        if let Some(ref extractor) = config.timestamp_extractor {
            extractor(value)
        } else {
            // Default: assume TimestampedValue format (last 8 bytes are timestamp)
            TimestampedValue::from_bytes(value)
                .map(|tv| tv.timestamp)
                .unwrap_or(0)
        }
    }

    /// Resolve a conflict using the LWW strategy.
    ///
    /// The value with the higher timestamp wins. If timestamps are equal,
    /// uses lexicographic comparison as a deterministic tiebreaker.
    fn resolve_lww(left: &[u8], right: &[u8], config: &CrdtConfig) -> Vec<u8> {
        let left_ts = Self::extract_timestamp(left, config);
        let right_ts = Self::extract_timestamp(right, config);

        if left_ts > right_ts {
            left.to_vec()
        } else if right_ts > left_ts {
            right.to_vec()
        } else {
            // Timestamps equal - use lexicographic comparison as tiebreaker
            if left >= right {
                left.to_vec()
            } else {
                right.to_vec()
            }
        }
    }

    /// Resolve a conflict using the MV strategy.
    ///
    /// Combines both values into a MultiValueSet.
    fn resolve_mv(left: &[u8], right: &[u8]) -> Vec<u8> {
        // Try to parse existing MultiValueSets, or create new ones
        let left_set =
            MultiValueSet::from_bytes(left).unwrap_or_else(|| MultiValueSet::single(left.to_vec()));
        let right_set = MultiValueSet::from_bytes(right)
            .unwrap_or_else(|| MultiValueSet::single(right.to_vec()));

        left_set.merge(&right_set).to_bytes()
    }

    /// Resolve a conflict using a custom merge function.
    fn resolve_custom(conflict: &Conflict, merge_fn: &CustomMergeFn) -> Option<Vec<u8>> {
        match merge_fn(conflict) {
            CrdtResolution::Value(value) => Some(value),
            CrdtResolution::Delete => None,
        }
    }

    /// Resolve a delete vs update conflict based on the configured policy.
    ///
    /// Returns `Some(value)` if the key should exist, `None` if it should be deleted.
    fn resolve_delete_conflict(existing_value: &[u8], policy: &DeletePolicy) -> Option<Vec<u8>> {
        match policy {
            DeletePolicy::DeleteWins => None,
            DeletePolicy::UpdateWins => Some(existing_value.to_vec()),
        }
    }

    /// Resolve a conflict between two changes.
    ///
    /// # Arguments
    /// * `key` - The key where the conflict occurred
    /// * `base` - The base branch value (Some for present, None for absent)
    /// * `left` - The left branch value (Some for update, None for delete)
    /// * `right` - The right branch value (Some for update, None for delete)
    /// * `config` - CRDT configuration
    ///
    /// # Returns
    /// * `Some(value)` - The resolved value to use
    /// * `None` - The key should be deleted
    fn resolve_conflict(
        key: &[u8],
        base: &Option<Vec<u8>>,
        left: &Option<Vec<u8>>,
        right: &Option<Vec<u8>>,
        config: &CrdtConfig,
    ) -> Option<Vec<u8>> {
        if let MergeStrategy::Custom(f) = &config.strategy {
            let conflict = Conflict {
                key: key.to_vec(),
                base: base.clone(),
                left: left.clone(),
                right: right.clone(),
            };
            return Self::resolve_custom(&conflict, f);
        }

        match (left, right) {
            // Both have values - use strategy to resolve
            (Some(l), Some(r)) => {
                let resolved = match &config.strategy {
                    MergeStrategy::LastWriterWins => Self::resolve_lww(l, r, config),
                    MergeStrategy::MultiValue => Self::resolve_mv(l, r),
                    MergeStrategy::Custom(_) => unreachable!("custom strategy returned above"),
                };
                Some(resolved)
            }

            // Left has value, right deleted
            (Some(l), None) => Self::resolve_delete_conflict(l, &config.delete_policy),

            // Left deleted, right has value
            (None, Some(r)) => Self::resolve_delete_conflict(r, &config.delete_policy),

            // Both deleted - key should be deleted
            (None, None) => None,
        }
    }
}

/// Resolve one CRDT conflict using the default strategy implementation.
///
/// Manager-level and trait-level merge paths share this function so sync,
/// async, explained, and direct-store merges cannot drift semantically.
pub(crate) fn resolve_conflict(config: &CrdtConfig, conflict: &Conflict) -> Option<Vec<u8>> {
    DefaultConflictFreeMerger::resolve_conflict(
        &conflict.key,
        &conflict.base,
        &conflict.left,
        &conflict.right,
        config,
    )
}

/// Build a standard merge resolver from CRDT configuration.
pub(crate) fn resolver(config: &CrdtConfig) -> Resolver {
    let config = config.clone();
    Box::new(move |conflict| {
        resolve_conflict(&config, conflict)
            .map(Resolution::value)
            .unwrap_or_else(Resolution::delete)
    })
}

impl<S: Store> ConflictFreeMerger<S> for DefaultConflictFreeMerger {
    fn crdt_merge(
        &self,
        store: &S,
        base: &Tree,
        left: &Tree,
        right: &Tree,
        config: &CrdtConfig,
    ) -> Result<Tree, Error> {
        // Reuse the production structural/diff/batch merge engine. A borrowed
        // Store implementation avoids taking ownership while retaining the
        // backend's optimized batch operations.
        let prolly = Prolly::new(store, base.config.clone());
        prolly.merge(base, left, right, Some(resolver(config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // TimestampedValue tests
    #[test]
    fn test_timestamped_value_new() {
        let tv = TimestampedValue::new(b"hello".to_vec(), 12345);
        assert_eq!(tv.value, b"hello");
        assert_eq!(tv.timestamp, 12345);
    }

    #[test]
    fn test_timestamped_value_now() {
        let tv = TimestampedValue::now(b"hello".to_vec());
        assert_eq!(tv.value, b"hello");
        assert!(tv.timestamp > 0);
    }

    #[test]
    fn test_timestamped_value_roundtrip() {
        let original = TimestampedValue::new(b"test value".to_vec(), 1234567890);
        let bytes = original.to_bytes();
        let restored = TimestampedValue::from_bytes(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_timestamped_value_empty_value() {
        let tv = TimestampedValue::new(vec![], 100);
        let bytes = tv.to_bytes();
        let restored = TimestampedValue::from_bytes(&bytes).unwrap();
        assert_eq!(restored.value, Vec::<u8>::new());
        assert_eq!(restored.timestamp, 100);
    }

    #[test]
    fn test_timestamped_value_from_bytes_too_short() {
        let bytes = vec![1, 2, 3]; // Less than 8 bytes
        assert!(TimestampedValue::from_bytes(&bytes).is_none());
    }

    // MultiValueSet tests
    #[test]
    fn test_multi_value_set_single() {
        let mv = MultiValueSet::single(b"value".to_vec());
        assert_eq!(mv.values.len(), 1);
        assert_eq!(mv.values[0], b"value");
    }

    #[test]
    fn test_multi_value_set_from_values() {
        let mv = MultiValueSet::from_values(vec![
            b"c".to_vec(),
            b"a".to_vec(),
            b"b".to_vec(),
            b"a".to_vec(), // duplicate
        ]);
        // Should be sorted and deduplicated
        assert_eq!(mv.values, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }

    #[test]
    fn test_multi_value_set_merge() {
        let mv1 = MultiValueSet::single(b"a".to_vec());
        let mv2 = MultiValueSet::single(b"b".to_vec());
        let merged = mv1.merge(&mv2);
        assert_eq!(merged.values, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn test_multi_value_set_merge_with_overlap() {
        let mv1 = MultiValueSet::from_values(vec![b"a".to_vec(), b"b".to_vec()]);
        let mv2 = MultiValueSet::from_values(vec![b"b".to_vec(), b"c".to_vec()]);
        let merged = mv1.merge(&mv2);
        assert_eq!(
            merged.values,
            vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]
        );
    }

    #[test]
    fn test_multi_value_set_roundtrip() {
        let original = MultiValueSet::from_values(vec![
            b"value1".to_vec(),
            b"value2".to_vec(),
            b"value3".to_vec(),
        ]);
        let bytes = original.to_bytes();
        let restored = MultiValueSet::from_bytes(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_multi_value_set_empty_roundtrip() {
        let original = MultiValueSet { values: vec![] };
        let bytes = original.to_bytes();
        let restored = MultiValueSet::from_bytes(&bytes).unwrap();
        assert_eq!(original, restored);
    }

    #[test]
    fn test_multi_value_set_from_bytes_too_short() {
        let bytes = vec![1, 2]; // Less than 4 bytes
        assert!(MultiValueSet::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_multi_value_set_from_bytes_truncated() {
        // Count says 1 value, but no value data
        let bytes = vec![0, 0, 0, 1, 0, 0, 0, 5]; // count=1, len=5, but no value
        assert!(MultiValueSet::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_multi_value_set_rejects_trailing_bytes() {
        let mut bytes = MultiValueSet::single(b"value".to_vec()).to_bytes();
        bytes.extend_from_slice(b"trailing");

        assert!(MultiValueSet::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_multi_value_set_rejects_impossible_count_before_allocating() {
        let bytes = u32::MAX.to_be_bytes();

        assert!(MultiValueSet::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_multi_value_set_serialization_is_canonical() {
        let non_canonical = MultiValueSet {
            values: vec![b"b".to_vec(), b"a".to_vec(), b"b".to_vec()],
        };

        let decoded = MultiValueSet::from_bytes(&non_canonical.to_bytes()).unwrap();
        assert_eq!(decoded.values, vec![b"a".to_vec(), b"b".to_vec()]);
    }

    // CrdtConfig tests
    #[test]
    fn test_crdt_config_default() {
        let config = CrdtConfig::default();
        assert!(matches!(config.strategy, MergeStrategy::LastWriterWins));
        assert_eq!(config.delete_policy, DeletePolicy::UpdateWins);
        assert!(config.timestamp_extractor.is_none());
    }

    #[test]
    fn test_crdt_config_lww() {
        let config = CrdtConfig::lww();
        assert!(matches!(config.strategy, MergeStrategy::LastWriterWins));
    }

    #[test]
    fn test_crdt_config_multi_value() {
        let config = CrdtConfig::multi_value();
        assert!(matches!(config.strategy, MergeStrategy::MultiValue));
    }

    #[test]
    fn test_crdt_config_custom() {
        let config = CrdtConfig::custom(|conflict| {
            CrdtResolution::value(conflict.left.clone().expect("left value"))
        });
        assert!(matches!(config.strategy, MergeStrategy::Custom(_)));
    }

    #[test]
    fn test_crdt_config_with_delete_policy() {
        let config = CrdtConfig::default().with_delete_policy(DeletePolicy::DeleteWins);
        assert_eq!(config.delete_policy, DeletePolicy::DeleteWins);
    }

    #[test]
    fn test_crdt_config_with_timestamp_extractor() {
        let config = CrdtConfig::default().with_timestamp_extractor(|bytes| {
            if bytes.len() >= 8 {
                u64::from_be_bytes(bytes[bytes.len() - 8..].try_into().unwrap())
            } else {
                0
            }
        });
        assert!(config.timestamp_extractor.is_some());
    }

    #[test]
    fn test_delete_policy_default() {
        let policy = DeletePolicy::default();
        assert_eq!(policy, DeletePolicy::UpdateWins);
    }

    #[test]
    fn test_merge_strategy_default() {
        let strategy = MergeStrategy::default();
        assert!(matches!(strategy, MergeStrategy::LastWriterWins));
    }

    #[test]
    fn test_merge_strategy_debug() {
        assert_eq!(
            format!("{:?}", MergeStrategy::LastWriterWins),
            "LastWriterWins"
        );
        assert_eq!(format!("{:?}", MergeStrategy::MultiValue), "MultiValue");
        let custom = MergeStrategy::Custom(Arc::new(|conflict: &Conflict| {
            CrdtResolution::value(conflict.left.clone().expect("left value"))
        }));
        assert_eq!(format!("{:?}", custom), "Custom(<fn>)");
    }

    // LWW Resolution tests
    #[test]
    fn test_resolve_lww_higher_timestamp_wins() {
        let config = CrdtConfig::default();
        let old_value = TimestampedValue::new(b"old".to_vec(), 100);
        let new_value = TimestampedValue::new(b"new".to_vec(), 200);

        let result = DefaultConflictFreeMerger::resolve_lww(
            &old_value.to_bytes(),
            &new_value.to_bytes(),
            &config,
        );

        let restored = TimestampedValue::from_bytes(&result).unwrap();
        assert_eq!(restored.value, b"new");
        assert_eq!(restored.timestamp, 200);
    }

    #[test]
    fn test_resolve_lww_equal_timestamps_lexicographic_tiebreaker() {
        let config = CrdtConfig::default();
        let value_a = TimestampedValue::new(b"aaa".to_vec(), 100);
        let value_b = TimestampedValue::new(b"bbb".to_vec(), 100);

        // When timestamps are equal, the lexicographically larger value wins
        let result = DefaultConflictFreeMerger::resolve_lww(
            &value_a.to_bytes(),
            &value_b.to_bytes(),
            &config,
        );

        let restored = TimestampedValue::from_bytes(&result).unwrap();
        // value_b.to_bytes() > value_a.to_bytes() lexicographically
        assert_eq!(restored.value, b"bbb");
    }

    #[test]
    fn test_resolve_lww_with_custom_extractor() {
        // Custom extractor that reads timestamp from first 8 bytes
        let config = CrdtConfig::default().with_timestamp_extractor(|bytes| {
            if bytes.len() >= 8 {
                u64::from_be_bytes(bytes[..8].try_into().unwrap())
            } else {
                0
            }
        });

        // Create values with timestamp at the beginning
        let mut old_value = 100u64.to_be_bytes().to_vec();
        old_value.extend(b"old");

        let mut new_value = 200u64.to_be_bytes().to_vec();
        new_value.extend(b"new");

        let result = DefaultConflictFreeMerger::resolve_lww(&old_value, &new_value, &config);
        assert_eq!(result, new_value);
    }

    // MV Resolution tests
    #[test]
    fn test_resolve_mv_combines_values() {
        let result = DefaultConflictFreeMerger::resolve_mv(b"left", b"right");
        let mv = MultiValueSet::from_bytes(&result).unwrap();

        assert_eq!(mv.values.len(), 2);
        assert!(mv.values.contains(&b"left".to_vec()));
        assert!(mv.values.contains(&b"right".to_vec()));
    }

    #[test]
    fn test_resolve_mv_merges_existing_sets() {
        let left_set = MultiValueSet::from_values(vec![b"a".to_vec(), b"b".to_vec()]);
        let right_set = MultiValueSet::from_values(vec![b"c".to_vec(), b"d".to_vec()]);

        let result =
            DefaultConflictFreeMerger::resolve_mv(&left_set.to_bytes(), &right_set.to_bytes());
        let mv = MultiValueSet::from_bytes(&result).unwrap();

        assert_eq!(mv.values.len(), 4);
        assert!(mv.values.contains(&b"a".to_vec()));
        assert!(mv.values.contains(&b"b".to_vec()));
        assert!(mv.values.contains(&b"c".to_vec()));
        assert!(mv.values.contains(&b"d".to_vec()));
    }

    // Custom merge function tests
    #[test]
    fn test_resolve_custom_function() {
        let merge_fn: CustomMergeFn = Arc::new(|conflict| {
            // Concatenate left and right
            let mut result = conflict.left.clone().expect("left value");
            result.extend(conflict.right.as_ref().expect("right value"));
            CrdtResolution::value(result)
        });

        let conflict = Conflict {
            key: b"key".to_vec(),
            base: Some(b"base".to_vec()),
            left: Some(b"left".to_vec()),
            right: Some(b"right".to_vec()),
        };
        let result = DefaultConflictFreeMerger::resolve_custom(&conflict, &merge_fn);

        assert_eq!(result, Some(b"leftright".to_vec()));
    }

    #[test]
    fn test_resolve_custom_function_can_delete() {
        let merge_fn: CustomMergeFn = Arc::new(|_| CrdtResolution::delete());
        let conflict = Conflict {
            key: b"key".to_vec(),
            base: Some(b"base".to_vec()),
            left: None,
            right: Some(b"right".to_vec()),
        };
        let result = DefaultConflictFreeMerger::resolve_custom(&conflict, &merge_fn);

        assert!(result.is_none());
    }

    // Delete policy tests
    #[test]
    fn test_resolve_delete_conflict_delete_wins() {
        let result =
            DefaultConflictFreeMerger::resolve_delete_conflict(b"value", &DeletePolicy::DeleteWins);
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_delete_conflict_update_wins() {
        let result =
            DefaultConflictFreeMerger::resolve_delete_conflict(b"value", &DeletePolicy::UpdateWins);
        assert_eq!(result, Some(b"value".to_vec()));
    }

    // Full conflict resolution tests
    #[test]
    fn test_resolve_conflict_both_values_lww() {
        let config = CrdtConfig::lww();
        let old_value = TimestampedValue::new(b"old".to_vec(), 100);
        let new_value = TimestampedValue::new(b"new".to_vec(), 200);

        let result = DefaultConflictFreeMerger::resolve_conflict(
            b"key",
            &Some(b"base".to_vec()),
            &Some(old_value.to_bytes()),
            &Some(new_value.to_bytes()),
            &config,
        );

        assert!(result.is_some());
        let restored = TimestampedValue::from_bytes(&result.unwrap()).unwrap();
        assert_eq!(restored.value, b"new");
    }

    #[test]
    fn test_resolve_conflict_left_deleted_update_wins() {
        let config = CrdtConfig::default().with_delete_policy(DeletePolicy::UpdateWins);

        let result = DefaultConflictFreeMerger::resolve_conflict(
            b"key",
            &Some(b"base".to_vec()),
            &None,
            &Some(b"right_value".to_vec()),
            &config,
        );

        assert_eq!(result, Some(b"right_value".to_vec()));
    }

    #[test]
    fn test_resolve_conflict_left_deleted_delete_wins() {
        let config = CrdtConfig::default().with_delete_policy(DeletePolicy::DeleteWins);

        let result = DefaultConflictFreeMerger::resolve_conflict(
            b"key",
            &Some(b"base".to_vec()),
            &None,
            &Some(b"right_value".to_vec()),
            &config,
        );

        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_conflict_both_deleted() {
        let config = CrdtConfig::default();

        let result =
            DefaultConflictFreeMerger::resolve_conflict(b"key", &None, &None, &None, &config);

        assert!(result.is_none());
    }

    // Integration tests with actual trees
    #[test]
    fn test_crdt_merge_no_conflicts() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        // Create base tree
        let base = prolly.create();
        let base = prolly.put(&base, b"a".to_vec(), b"1".to_vec()).unwrap();

        // Create divergent branches with non-conflicting changes
        let left = prolly.put(&base, b"b".to_vec(), b"2".to_vec()).unwrap();
        let right = prolly.put(&base, b"c".to_vec(), b"3".to_vec()).unwrap();

        // CRDT merge
        let merger = DefaultConflictFreeMerger::new();
        let crdt_config = CrdtConfig::default();
        let merged = merger
            .crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config)
            .unwrap();

        // Verify all keys present
        assert_eq!(prolly.get(&merged, b"a").unwrap(), Some(b"1".to_vec()));
        assert_eq!(prolly.get(&merged, b"b").unwrap(), Some(b"2".to_vec()));
        assert_eq!(prolly.get(&merged, b"c").unwrap(), Some(b"3".to_vec()));
    }

    #[test]
    fn test_crdt_merge_reuses_production_root_fast_path() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;

        let store = MemStore::new();
        let prolly = Prolly::new(&store, Config::default());
        let base = prolly
            .put(&prolly.create(), b"key".to_vec(), b"base".to_vec())
            .unwrap();
        let right = prolly
            .put(&base, b"key".to_vec(), b"right".to_vec())
            .unwrap();

        let merged = DefaultConflictFreeMerger::new()
            .crdt_merge(&store, &base, &base, &right, &CrdtConfig::lww())
            .unwrap();

        assert_eq!(merged.root, right.root);
    }

    #[test]
    fn test_crdt_merge_with_lww_conflict() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        // Create base tree
        let base = prolly.create();
        let base_value = TimestampedValue::new(b"base".to_vec(), 100);
        let base = prolly
            .put(&base, b"key".to_vec(), base_value.to_bytes())
            .unwrap();

        // Create conflicting branches
        let left_value = TimestampedValue::new(b"left".to_vec(), 200);
        let left = prolly
            .put(&base, b"key".to_vec(), left_value.to_bytes())
            .unwrap();

        let right_value = TimestampedValue::new(b"right".to_vec(), 300);
        let right = prolly
            .put(&base, b"key".to_vec(), right_value.to_bytes())
            .unwrap();

        // CRDT merge with LWW - right should win (higher timestamp)
        let merger = DefaultConflictFreeMerger::new();
        let crdt_config = CrdtConfig::lww();
        let merged = merger
            .crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config)
            .unwrap();

        let result = prolly.get(&merged, b"key").unwrap().unwrap();
        let restored = TimestampedValue::from_bytes(&result).unwrap();
        assert_eq!(restored.value, b"right");
        assert_eq!(restored.timestamp, 300);
    }

    #[test]
    fn test_crdt_merge_with_mv_conflict() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        // Create base tree
        let base = prolly.create();
        let base = prolly
            .put(&base, b"key".to_vec(), b"base".to_vec())
            .unwrap();

        // Create conflicting branches
        let left = prolly
            .put(&base, b"key".to_vec(), b"left".to_vec())
            .unwrap();
        let right = prolly
            .put(&base, b"key".to_vec(), b"right".to_vec())
            .unwrap();

        // CRDT merge with MV - both values should be preserved
        let merger = DefaultConflictFreeMerger::new();
        let crdt_config = CrdtConfig::multi_value();
        let merged = merger
            .crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config)
            .unwrap();

        let result = prolly.get(&merged, b"key").unwrap().unwrap();
        let mv = MultiValueSet::from_bytes(&result).unwrap();

        assert_eq!(mv.values.len(), 2);
        assert!(mv.values.contains(&b"left".to_vec()));
        assert!(mv.values.contains(&b"right".to_vec()));
    }

    #[test]
    fn test_crdt_merge_with_custom_value_resolution() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        let base = prolly.create();
        let base = prolly
            .put(&base, b"key".to_vec(), b"base".to_vec())
            .unwrap();
        let left = prolly
            .put(&base, b"key".to_vec(), b"left".to_vec())
            .unwrap();
        let right = prolly
            .put(&base, b"key".to_vec(), b"right".to_vec())
            .unwrap();

        let crdt_config = CrdtConfig::custom(|conflict| {
            assert_eq!(conflict.base.as_deref(), Some(b"base".as_slice()));
            let mut value = conflict.left.clone().expect("left value");
            value.extend_from_slice(b"+");
            value.extend_from_slice(conflict.right.as_ref().expect("right value"));
            CrdtResolution::value(value)
        });
        let merger = DefaultConflictFreeMerger::new();
        let merged = merger
            .crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config)
            .unwrap();

        assert_eq!(
            prolly.get(&merged, b"key").unwrap(),
            Some(b"left+right".to_vec())
        );
    }

    #[test]
    fn test_crdt_merge_with_custom_delete_resolution() {
        use crate::prolly::config::Config;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        let base = prolly.create();
        let base = prolly
            .put(&base, b"key".to_vec(), b"base".to_vec())
            .unwrap();
        let left = prolly.delete(&base, b"key").unwrap();
        let right = prolly
            .put(&base, b"key".to_vec(), b"right".to_vec())
            .unwrap();

        let crdt_config = CrdtConfig::custom(|conflict| {
            assert!(conflict.left.is_none());
            assert_eq!(conflict.right.as_deref(), Some(b"right".as_slice()));
            CrdtResolution::delete()
        });
        let merger = DefaultConflictFreeMerger::new();
        let merged = merger
            .crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config)
            .unwrap();

        assert_eq!(prolly.get(&merged, b"key").unwrap(), None);
    }

    #[test]
    fn test_crdt_merge_never_returns_conflict_error() {
        use crate::prolly::config::Config;
        use crate::prolly::error::Error;
        use crate::prolly::store::MemStore;
        use crate::prolly::Prolly;
        use std::sync::Arc;

        let store = Arc::new(MemStore::new());
        let config = Config::default();
        let prolly = Prolly::new(store.clone(), config);

        // Create base tree
        let base = prolly.create();
        let base = prolly
            .put(&base, b"key".to_vec(), b"base".to_vec())
            .unwrap();

        // Create conflicting branches
        let left = prolly
            .put(&base, b"key".to_vec(), b"left".to_vec())
            .unwrap();
        let right = prolly
            .put(&base, b"key".to_vec(), b"right".to_vec())
            .unwrap();

        // CRDT merge should never return Conflict error
        let merger = DefaultConflictFreeMerger::new();
        let crdt_config = CrdtConfig::default();
        let result = merger.crdt_merge(store.as_ref(), &base, &left, &right, &crdt_config);

        // Should succeed, not return Conflict
        assert!(result.is_ok());
        assert!(!matches!(result, Err(Error::Conflict(_))));
    }
}
