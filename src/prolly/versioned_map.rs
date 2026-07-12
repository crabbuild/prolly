//! A low-friction, linear version catalog for application indexes.
//!
//! [`VersionedMap`] combines immutable prolly trees, named roots, and strict
//! transactions into one application-facing handle. Every successful logical
//! update atomically advances a mutable head and records an immutable,
//! content-derived version root. It intentionally stops short of commit
//! ancestry, branches, authors, and reflogs; those belong in a repository/VCS
//! layer.

use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::HashSet;
use std::fmt;
use std::marker::PhantomData;

use super::cid::Cid;
use super::error::{Diff, Error, Mutation};
use super::manifest::{ManifestStore, ManifestStoreScan, NamedRootRetention, RootManifest};
use super::range::{CursorWindow, RangeCursor, RangeIter, RangePage, ReverseCursor, ReversePage};
use super::stats::TreeStats;
use super::store::Store;
use super::transaction::{TransactionConflict, TransactionUpdate, TransactionalStore};
use super::tree::Tree;
use super::{current_unix_time_millis, Prolly};

/// Root namespace reserved for built-in versioned maps.
pub const VERSIONED_MAP_ROOT_PREFIX: &[u8] = b"maps/versioned/";

/// Maximum optimistic attempts made by the convenience mutation methods.
pub const DEFAULT_VERSIONED_MAP_RETRIES: usize = 8;

const HEAD_SUFFIX: &[u8] = b"/head";
const VERSIONS_SUFFIX: &[u8] = b"/versions/";
const VERSIONED_MAP_BACKUP_FORMAT_VERSION: u64 = 1;

/// Metadata attached when authenticating a snapshot proof bundle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProofAuthentication {
    /// Application key identifier used to select the verification secret.
    pub key_id: Vec<u8>,
    /// Application-specific domain separation bytes.
    pub context: Vec<u8>,
    /// Optional envelope issue time.
    pub issued_at_millis: Option<u64>,
    /// Optional envelope expiration time.
    pub expires_at_millis: Option<u64>,
    /// Optional replay-prevention nonce.
    pub nonce: Vec<u8>,
}

impl ProofAuthentication {
    /// Start proof authentication metadata for one verification key.
    pub fn new(key_id: impl Into<Vec<u8>>) -> Self {
        Self {
            key_id: key_id.into(),
            ..Self::default()
        }
    }

    /// Set application-specific domain separation bytes.
    pub fn with_context(mut self, context: impl Into<Vec<u8>>) -> Self {
        self.context = context.into();
        self
    }

    /// Set optional issue and expiration times.
    pub fn with_validity(
        mut self,
        issued_at_millis: Option<u64>,
        expires_at_millis: Option<u64>,
    ) -> Self {
        self.issued_at_millis = issued_at_millis;
        self.expires_at_millis = expires_at_millis;
        self
    }

    /// Set replay-prevention nonce bytes.
    pub fn with_nonce(mut self, nonce: impl Into<Vec<u8>>) -> Self {
        self.nonce = nonce.into();
        self
    }
}

/// Content-derived identifier for one index snapshot.
///
/// The identifier hashes the complete timestamp-free [`RootManifest`], so it
/// includes both the root CID and the tree configuration. Empty trees therefore
/// also have a stable version identifier.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct MapVersionId(Cid);

impl MapVersionId {
    /// Compute the stable identifier for a tree handle.
    pub fn for_tree(tree: &Tree) -> Result<Self, Error> {
        let bytes = RootManifest::from_tree(tree).to_bytes()?;
        Ok(Self(Cid::from_bytes(&bytes)))
    }

    /// Borrow the underlying 32-byte content identifier.
    pub fn as_cid(&self) -> &Cid {
        &self.0
    }

    /// Consume this identifier and return its underlying CID.
    pub fn into_cid(self) -> Cid {
        self.0
    }
}

impl fmt::Display for MapVersionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0.as_bytes() {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// One durable snapshot in a versioned map.
#[derive(Clone, Debug, PartialEq)]
pub struct MapVersion {
    /// Content-derived version identifier.
    pub id: MapVersionId,
    /// Immutable tree handle for this version.
    pub tree: Tree,
    /// Creation timestamp recorded by the version root, when available.
    pub created_at_millis: Option<u64>,
    /// Whether this snapshot is the index's current head.
    pub is_head: bool,
}

/// Result of pruning immutable version roots from a managed map.
///
/// Pruning removes catalog roots, not content-addressed nodes. Run the normal
/// retention-aware GC flow afterward to reclaim nodes no longer reachable from
/// the remaining versions.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct VersionPruneResult {
    /// Versions retained by the policy, including the current head.
    pub retained: Vec<MapVersionId>,
    /// Version roots removed from the catalog.
    pub removed: Vec<MapVersionId>,
}

/// Managed-map publication plus engine batch execution statistics.
#[derive(Clone, Debug)]
pub struct VersionedMapBatchResult {
    /// Published managed-map version.
    pub version: MapVersion,
    /// Route, rewrite, and node-write counters from the engine batch.
    pub stats: super::batch::BatchApplyStats,
}

/// One version and its self-contained node bundle in a map catalog backup.
#[derive(Clone, Debug, PartialEq)]
pub struct MapBackupVersion {
    /// Stable content-derived version identifier.
    pub id: MapVersionId,
    /// Original catalog creation timestamp.
    pub created_at_millis: Option<u64>,
    /// Complete, independently verifiable tree snapshot.
    pub bundle: super::sync::SnapshotBundle,
}

/// Portable backup of one complete managed-map catalog.
#[derive(Clone, Debug, PartialEq)]
pub struct VersionedMapBackup {
    /// Application map identifier.
    pub map_id: Vec<u8>,
    /// Version that was head when the backup was created.
    pub head: MapVersionId,
    /// Every cataloged immutable version.
    pub versions: Vec<MapBackupVersion>,
}

#[derive(Serialize, Deserialize)]
struct VersionedMapBackupWire {
    version: u64,
    map_id: Vec<u8>,
    head: MapVersionId,
    versions: Vec<MapBackupVersionWire>,
}

#[derive(Serialize, Deserialize)]
struct MapBackupVersionWire {
    id: MapVersionId,
    created_at_millis: Option<u64>,
    bundle: Vec<u8>,
}

impl VersionedMapBackup {
    /// Verify version IDs, bundle completeness, uniqueness, and head presence.
    pub fn verify(&self) -> Result<(), Error> {
        let mut seen = HashSet::with_capacity(self.versions.len());
        let mut found_head = false;
        for version in &self.versions {
            if !seen.insert(version.id.clone()) {
                return Err(Error::InvalidVersionedMap(format!(
                    "backup contains duplicate version {}",
                    version.id
                )));
            }
            let verification = version.bundle.verify()?;
            if !verification.valid {
                return Err(Error::InvalidVersionedMap(format!(
                    "backup version {} is not self-contained",
                    version.id
                )));
            }
            let actual = MapVersionId::for_tree(&version.bundle.tree)?;
            if actual != version.id {
                return Err(Error::InvalidVersionedMap(format!(
                    "backup version {} contains tree {}",
                    version.id, actual
                )));
            }
            found_head |= version.id == self.head;
        }
        if !found_head {
            return Err(Error::InvalidVersionedMap(format!(
                "backup head {} is absent from its catalog",
                self.head
            )));
        }
        Ok(())
    }

    /// Serialize this backup as deterministic versioned CBOR.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.verify()?;
        let wire = VersionedMapBackupWire {
            version: VERSIONED_MAP_BACKUP_FORMAT_VERSION,
            map_id: self.map_id.clone(),
            head: self.head.clone(),
            versions: self
                .versions
                .iter()
                .map(|version| {
                    Ok(MapBackupVersionWire {
                        id: version.id.clone(),
                        created_at_millis: version.created_at_millis,
                        bundle: version.bundle.to_bytes()?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        };
        serde_cbor::ser::to_vec_packed(&wire).map_err(|err| Error::Serialize(err.to_string()))
    }

    /// Decode and fully verify a portable backup.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire: VersionedMapBackupWire =
            serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))?;
        if wire.version != VERSIONED_MAP_BACKUP_FORMAT_VERSION {
            return Err(Error::InvalidVersionedMap(format!(
                "unsupported backup format version {}",
                wire.version
            )));
        }
        let backup = Self {
            map_id: wire.map_id,
            head: wire.head,
            versions: wire
                .versions
                .into_iter()
                .map(|version| {
                    Ok(MapBackupVersion {
                        id: version.id,
                        created_at_millis: version.created_at_millis,
                        bundle: super::sync::SnapshotBundle::from_bytes(&version.bundle)?,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        };
        backup.verify()?;
        Ok(backup)
    }
}

impl VersionPruneResult {
    /// Number of immutable version roots removed.
    pub fn removed_count(&self) -> usize {
        self.removed.len()
    }

    /// Whether pruning left the catalog unchanged.
    pub fn is_unchanged(&self) -> bool {
        self.removed.is_empty()
    }
}

impl MapVersion {
    fn new(tree: Tree, created_at_millis: Option<u64>, is_head: bool) -> Result<Self, Error> {
        Ok(Self {
            id: MapVersionId::for_tree(&tree)?,
            tree,
            created_at_millis,
            is_head,
        })
    }
}

/// An immutable, version-pinned view over one managed map snapshot.
///
/// A snapshot owns its [`MapVersion`] handle and borrows the engine. All reads
/// stay on that tree even when another writer advances the managed map's head.
/// This is the preferred surface for request-scoped reads, long scans, proofs,
/// export, and diagnostics.
pub struct MapSnapshot<'a, S: Store> {
    prolly: &'a Prolly<S>,
    version: MapVersion,
}

/// Lazy descending iterator backed by bounded reverse pages.
pub struct MapReverseIter<'a, S: Store> {
    prolly: &'a Prolly<S>,
    tree: Tree,
    start: Vec<u8>,
    prefix: Option<Vec<u8>>,
    cursor: ReverseCursor,
    page_size: usize,
    buffered: std::vec::IntoIter<(Vec<u8>, Vec<u8>)>,
    finished: bool,
}

impl<'a, S: Store> MapReverseIter<'a, S> {
    fn new(
        prolly: &'a Prolly<S>,
        tree: Tree,
        start: Vec<u8>,
        prefix: Option<Vec<u8>>,
        page_size: usize,
    ) -> Self {
        Self {
            prolly,
            tree,
            start,
            prefix,
            cursor: ReverseCursor::end(),
            page_size: page_size.max(1),
            buffered: Vec::new().into_iter(),
            finished: false,
        }
    }
}

impl<S: Store> Iterator for MapReverseIter<'_, S> {
    type Item = Result<(Vec<u8>, Vec<u8>), Error>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(entry) = self.buffered.next() {
                return Some(Ok(entry));
            }
            if self.finished {
                return None;
            }
            let page = match &self.prefix {
                Some(prefix) => self.prolly.prefix_reverse_page(
                    &self.tree,
                    prefix,
                    &self.cursor,
                    self.page_size,
                ),
                None => {
                    self.prolly
                        .reverse_page(&self.tree, &self.cursor, &self.start, self.page_size)
                }
            };
            match page {
                Ok(page) => {
                    self.finished = page.next_cursor.is_none();
                    if let Some(cursor) = page.next_cursor {
                        self.cursor = cursor;
                    }
                    self.buffered = page.entries.into_iter();
                }
                Err(error) => {
                    self.finished = true;
                    return Some(Err(error));
                }
            }
        }
    }
}

/// A version-pinned comparison between two snapshots of the same managed map.
pub struct MapComparison<'a, S: Store> {
    prolly: &'a Prolly<S>,
    base: MapVersion,
    target: MapVersion,
}

/// A three-way merge pinned to a base, current head, and candidate version.
pub struct MapMerge<'a, S: Store> {
    prolly: &'a Prolly<S>,
    map_id: Vec<u8>,
    base: MapVersion,
    head: MapVersion,
    candidate: MapVersion,
}

impl<'a, S: Store> MapMerge<'a, S> {
    fn new(
        prolly: &'a Prolly<S>,
        map_id: Vec<u8>,
        base: MapVersion,
        head: MapVersion,
        candidate: MapVersion,
    ) -> Self {
        Self {
            prolly,
            map_id,
            base,
            head,
            candidate,
        }
    }

    /// Common ancestor selected for the merge.
    pub fn base(&self) -> &MapVersion {
        &self.base
    }

    /// Head that must still be current when publishing.
    pub fn head(&self) -> &MapVersion {
        &self.head
    }

    /// Candidate version whose changes are being merged.
    pub fn candidate(&self) -> &MapVersion {
        &self.candidate
    }

    /// Lazily stream conflicts without constructing a merged tree.
    pub fn stream_conflicts<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Result<super::error::Conflict, Error>> + 's>, Error> {
        self.prolly
            .stream_conflicts(&self.base.tree, &self.head.tree, &self.candidate.tree)
    }

    /// Build the merged tree without moving head.
    pub fn merge(&self, resolver: Option<super::error::Resolver>) -> Result<Tree, Error> {
        self.prolly.merge(
            &self.base.tree,
            &self.head.tree,
            &self.candidate.tree,
            resolver,
        )
    }

    /// Build a merged tree using a prefix/exact-key policy registry.
    pub fn merge_with_policy(
        &self,
        policies: &super::policy::MergePolicyRegistry,
    ) -> Result<Tree, Error> {
        self.merge(Some(policies.as_resolver()))
    }

    /// Build a conflict-free merged tree using CRDT semantics.
    pub fn crdt_merge(&self, config: &super::crdt::CrdtConfig) -> Result<Tree, Error> {
        self.prolly.crdt_merge(
            &self.base.tree,
            &self.head.tree,
            &self.candidate.tree,
            config,
        )
    }

    /// Build a conflict-free merged tree and retain engine diagnostics.
    pub fn crdt_merge_explain(
        &self,
        config: &super::crdt::CrdtConfig,
    ) -> super::diff::MergeExplanation {
        self.prolly.crdt_merge_explain(
            &self.base.tree,
            &self.head.tree,
            &self.candidate.tree,
            config,
        )
    }

    /// Merge and publish only if the pinned head is still current.
    pub fn publish(
        &self,
        resolver: Option<super::error::Resolver>,
    ) -> Result<VersionedMapUpdate, Error>
    where
        S: ManifestStore + TransactionalStore,
    {
        let merged = self.merge(resolver)?;
        let map = VersionedMap::new(self.prolly, &self.map_id);
        map.publish_tree_if(Some(&self.head.id), &merged, current_unix_time_millis())
    }

    /// Merge with policies and CAS-publish the result.
    pub fn publish_with_policy(
        &self,
        policies: &super::policy::MergePolicyRegistry,
    ) -> Result<VersionedMapUpdate, Error>
    where
        S: ManifestStore + TransactionalStore,
    {
        self.publish(Some(policies.as_resolver()))
    }

    /// CRDT-merge and CAS-publish the result.
    pub fn publish_crdt(
        &self,
        config: &super::crdt::CrdtConfig,
    ) -> Result<VersionedMapUpdate, Error>
    where
        S: ManifestStore + TransactionalStore,
    {
        let merged = self.crdt_merge(config)?;
        let map = VersionedMap::new(self.prolly, &self.map_id);
        map.publish_tree_if(Some(&self.head.id), &merged, current_unix_time_millis())
    }
}

impl<'a, S: Store> MapComparison<'a, S> {
    fn new(prolly: &'a Prolly<S>, base: MapVersion, target: MapVersion) -> Self {
        Self {
            prolly,
            base,
            target,
        }
    }

    /// Baseline version.
    pub fn base(&self) -> &MapVersion {
        &self.base
    }

    /// Target version.
    pub fn target(&self) -> &MapVersion {
        &self.target
    }

    /// Collect every logical difference.
    pub fn diff(&self) -> Result<Vec<Diff>, Error> {
        self.prolly.diff(&self.base.tree, &self.target.tree)
    }

    /// Lazily stream logical differences.
    pub fn stream_diff<'s>(
        &'s self,
    ) -> Result<Box<dyn Iterator<Item = Result<Diff, Error>> + 's>, Error> {
        self.prolly.stream_diff(&self.base.tree, &self.target.tree)
    }

    /// Read one resumable key-cursor diff page.
    pub fn diff_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<super::diff::DiffPage, Error> {
        self.prolly
            .diff_page(&self.base.tree, &self.target.tree, cursor, end, limit)
    }

    /// Read one structural diff page while preserving the CID frontier.
    pub fn structural_diff_page(
        &self,
        cursor: Option<&super::diff::StructuralDiffCursor>,
        limit: usize,
    ) -> Result<super::diff::StructuralDiffPage, Error> {
        self.prolly
            .structural_diff_page(&self.base.tree, &self.target.tree, cursor, limit)
    }

    /// Read and prove one bounded diff page.
    pub fn prove_diff_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<super::proof::ProvedDiffPage, Error> {
        self.prolly
            .prove_diff_page(&self.base.tree, &self.target.tree, cursor, end, limit)
    }

    /// Compare shape, entry counts, and serialized size.
    pub fn stats(&self) -> Result<super::stats::StatsComparison, Error> {
        self.prolly.stats_diff(&self.base.tree, &self.target.tree)
    }

    /// Compare shared and rewritten tree nodes.
    pub fn debug_view(&self) -> Result<super::debug::TreeDebugComparison, Error> {
        self.prolly
            .debug_compare_trees(&self.base.tree, &self.target.tree)
    }

    /// Publish correctness-optional changed-span hints for this transition.
    pub fn publish_changed_spans<I>(&self, spans: I) -> Result<bool, Error>
    where
        I: IntoIterator<Item = super::ChangedSpan>,
    {
        self.prolly
            .publish_changed_spans_hint(&self.base.tree, &self.target.tree, spans)
    }

    /// Load correctness-optional changed-span hints for this transition.
    pub fn changed_spans(&self) -> Result<Option<super::ChangedSpanHint>, Error> {
        self.prolly
            .load_changed_spans_hint(&self.base.tree, &self.target.tree)
    }
}

impl<'a, S: Store> MapSnapshot<'a, S> {
    fn new(prolly: &'a Prolly<S>, version: MapVersion) -> Self {
        Self { prolly, version }
    }

    /// Metadata and tree handle for this pinned version.
    pub fn version(&self) -> &MapVersion {
        &self.version
    }

    /// Stable content-derived identifier for this pinned version.
    pub fn id(&self) -> &MapVersionId {
        &self.version.id
    }

    /// Immutable tree handle used by this snapshot.
    pub fn tree(&self) -> &Tree {
        &self.version.tree
    }

    /// Read one key.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.prolly.get(self.tree(), key)
    }

    /// Read the stored inline/blob reference without resolving blob bytes.
    pub fn get_value_ref(&self, key: &[u8]) -> Result<Option<super::blob::ValueRef>, Error> {
        self.prolly.get_value_ref(self.tree(), key)
    }

    /// Read one value and resolve offloaded blob content when necessary.
    pub fn get_large_value<B: super::blob::BlobStore>(
        &self,
        blob_store: &B,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        self.prolly.get_large_value(blob_store, self.tree(), key)
    }

    /// Check whether one key exists.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get(key)?.is_some())
    }

    /// Read several keys while preserving caller order and duplicates.
    pub fn get_many<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.prolly.get_many(self.tree(), keys)
    }

    /// Return the first entry in key order.
    pub fn first_entry(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        self.prolly.first_entry(self.tree())
    }

    /// Return the last entry in key order.
    pub fn last_entry(&self) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        self.prolly.last_entry(self.tree())
    }

    /// Return the first entry whose key is greater than or equal to `key`.
    pub fn lower_bound(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        self.prolly.lower_bound(self.tree(), key)
    }

    /// Return the first entry whose key is strictly greater than `key`.
    pub fn upper_bound(&self, key: &[u8]) -> Result<Option<(Vec<u8>, Vec<u8>)>, Error> {
        self.prolly.upper_bound(self.tree(), key)
    }

    /// Lazily stream a half-open key range from this immutable snapshot.
    pub fn range<'s>(
        &'s self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeIter<'s, S>, Error> {
        self.prolly.range(self.tree(), start, end)
    }

    /// Explicit alias for [`MapSnapshot::range`] emphasizing lazy large scans.
    pub fn stream_range<'s>(
        &'s self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeIter<'s, S>, Error> {
        self.range(start, end)
    }

    /// Lazily stream every entry under `prefix`.
    pub fn prefix<'s>(&'s self, prefix: &[u8]) -> Result<RangeIter<'s, S>, Error> {
        self.prolly.prefix(self.tree(), prefix)
    }

    /// Explicit alias for [`MapSnapshot::prefix`] emphasizing lazy large scans.
    pub fn stream_prefix<'s>(&'s self, prefix: &[u8]) -> Result<RangeIter<'s, S>, Error> {
        self.prefix(prefix)
    }

    /// Read one forward cursor page.
    pub fn range_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<RangePage, Error> {
        self.prolly.range_page(self.tree(), cursor, end, limit)
    }

    /// Read one prefix-bounded forward cursor page.
    pub fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: &RangeCursor,
        limit: usize,
    ) -> Result<RangePage, Error> {
        self.prolly.prefix_page(self.tree(), prefix, cursor, limit)
    }

    /// Read one reverse cursor page from the end of `[start, +inf)`.
    pub fn reverse_page(
        &self,
        cursor: &ReverseCursor,
        start: &[u8],
        limit: usize,
    ) -> Result<ReversePage, Error> {
        self.prolly.reverse_page(self.tree(), cursor, start, limit)
    }

    /// Read one reverse cursor page inside `prefix`.
    pub fn prefix_reverse_page(
        &self,
        prefix: &[u8],
        cursor: &ReverseCursor,
        limit: usize,
    ) -> Result<ReversePage, Error> {
        self.prolly
            .prefix_reverse_page(self.tree(), prefix, cursor, limit)
    }

    /// Lazily scan `[start, +inf)` in descending key order.
    ///
    /// `page_size` bounds each internal store traversal and is clamped to one.
    pub fn reverse_scan(&self, start: &[u8], page_size: usize) -> MapReverseIter<'a, S> {
        MapReverseIter::new(
            self.prolly,
            self.tree().clone(),
            start.to_vec(),
            None,
            page_size,
        )
    }

    /// Lazily scan one prefix in descending key order.
    pub fn prefix_reverse_scan(&self, prefix: &[u8], page_size: usize) -> MapReverseIter<'a, S> {
        MapReverseIter::new(
            self.prolly,
            self.tree().clone(),
            prefix.to_vec(),
            Some(prefix.to_vec()),
            page_size,
        )
    }

    /// Seek to `key` and return a bounded forward window.
    pub fn cursor_window(
        &self,
        key: &[u8],
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<CursorWindow, Error> {
        self.prolly.cursor_window(self.tree(), key, end, limit)
    }

    /// Collect structural and serialized-size statistics for this snapshot.
    pub fn stats(&self) -> Result<TreeStats, Error> {
        self.prolly.collect_stats(self.tree())
    }

    /// Return a deterministic diagnostic view grouped by tree level.
    pub fn debug_view(&self) -> Result<super::debug::TreeDebugView, Error> {
        self.prolly.debug_tree(self.tree())
    }

    /// Build a self-contained proof of one key's presence or absence.
    pub fn prove_key(&self, key: &[u8]) -> Result<super::proof::KeyProof, Error> {
        self.prolly.prove_key(self.tree(), key)
    }

    /// Build one shared proof for several keys.
    pub fn prove_keys<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<super::proof::MultiKeyProof, Error> {
        self.prolly.prove_keys(self.tree(), keys)
    }

    /// Build a complete proof for `[start, end)`.
    pub fn prove_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<super::proof::RangeProof, Error> {
        self.prolly.prove_range(self.tree(), start, end)
    }

    /// Build a complete proof for all entries under `prefix`.
    pub fn prove_prefix(&self, prefix: &[u8]) -> Result<super::proof::RangeProof, Error> {
        self.prolly.prove_prefix(self.tree(), prefix)
    }

    /// Read and prove one cursor page.
    pub fn prove_range_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<super::proof::ProvedRangePage, Error> {
        self.prolly
            .prove_range_page(self.tree(), cursor, end, limit)
    }

    /// Authenticate canonical proof bundle bytes with HMAC-SHA256.
    pub fn authenticate_proof_bundle(
        &self,
        proof_bundle: impl Into<Vec<u8>>,
        secret: &[u8],
        authentication: ProofAuthentication,
    ) -> Result<super::proof::AuthenticatedProofEnvelope, Error> {
        super::proof::sign_proof_bundle_hmac_sha256(
            proof_bundle,
            authentication.key_id,
            secret,
            authentication.context,
            authentication.issued_at_millis,
            authentication.expires_at_millis,
            authentication.nonce,
        )
    }

    /// Export this tree and every reachable node as a portable verified bundle.
    pub fn export(&self) -> Result<super::sync::SnapshotBundle, Error> {
        self.prolly.export_snapshot(self.tree())
    }

    /// Plan which nodes another store is missing for this snapshot.
    pub fn plan_missing_nodes<D: Store>(
        &self,
        destination: &D,
    ) -> Result<super::sync::MissingNodePlan, Error> {
        self.prolly.plan_missing_nodes(self.tree(), destination)
    }

    /// Copy this snapshot's missing nodes into another store.
    pub fn copy_missing_nodes<D: Store>(
        &self,
        destination: &D,
    ) -> Result<super::sync::MissingNodeCopy, Error> {
        self.prolly.copy_missing_nodes(self.tree(), destination)
    }

    /// Copy and publish this snapshot as the head of another managed map.
    pub fn push_to<D>(&self, destination: &VersionedMap<'_, D>) -> Result<MapVersion, Error>
    where
        D: Store + ManifestStore + TransactionalStore,
    {
        let bundle = self.export()?;
        destination.import_as_head(&bundle)
    }

    /// Pin this snapshot's root in the engine cache.
    pub fn pin_root(&self) -> Result<usize, Error> {
        self.prolly.pin_tree_root(self.tree())
    }

    /// Pin the root-to-leaf path for one hot key or prefix.
    pub fn pin_path(&self, key: &[u8]) -> Result<usize, Error> {
        self.prolly.pin_tree_path(self.tree(), key)
    }

    /// Persist a correctness-optional hot-prefix path hint.
    pub fn publish_prefix_hint(&self, prefix: &[u8]) -> Result<bool, Error> {
        self.prolly.publish_prefix_path_hint(self.tree(), prefix)
    }

    /// Hydrate the engine cache from a previously published prefix hint.
    pub fn hydrate_prefix_hint(&self, prefix: &[u8]) -> Result<bool, Error> {
        self.prolly.hydrate_prefix_path_hint(self.tree(), prefix)
    }
}

/// Outcome of a compare-and-update operation.
#[derive(Clone, Debug, PartialEq)]
pub enum VersionedMapUpdate {
    /// The update committed and produced this version.
    Applied {
        /// Previous head, or `None` when the index was initialized.
        previous: Option<MapVersionId>,
        /// New current version.
        current: MapVersion,
    },
    /// The requested mutations did not change the current tree.
    Unchanged {
        /// Current version, or `None` for an absent index and an empty edit.
        current: Option<MapVersion>,
    },
    /// The caller's expected head did not match the current head.
    Conflict {
        /// Current head at the time the conflict was observed.
        current: Option<MapVersion>,
    },
}

impl VersionedMapUpdate {
    /// Return the resulting current version for applied or unchanged updates.
    pub fn current(&self) -> Option<&MapVersion> {
        match self {
            Self::Applied { current, .. } => Some(current),
            Self::Unchanged { current } | Self::Conflict { current } => current.as_ref(),
        }
    }

    /// Whether a new head was committed.
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }

    /// Whether the caller's expected head was stale.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }
}

/// Mutation collector used by [`VersionedMap::edit`].
#[derive(Clone, Debug, Default)]
pub struct VersionedMapEditor {
    mutations: Vec<Mutation>,
}

impl VersionedMapEditor {
    /// Create an empty edit.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert or replace a key.
    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.mutations.push(Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        });
        self
    }

    /// Delete a key.
    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.mutations.push(Mutation::Delete { key: key.into() });
        self
    }

    /// Append an already constructed mutation.
    pub fn push(&mut self, mutation: Mutation) -> &mut Self {
        self.mutations.push(mutation);
        self
    }

    /// Number of collected mutations.
    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    /// Whether no mutations have been collected.
    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }

    fn into_mutations(self) -> Vec<Mutation> {
        self.mutations
    }
}

/// One strict transaction spanning any number of managed maps.
///
/// Use this to atomically update authoritative maps, secondary indexes, and
/// materialized views. All original heads are validated together and every new
/// node, immutable version root, and head movement commits as one backend
/// transaction.
pub struct VersionedMapsTransaction<'tx, 'engine, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    tx: &'tx super::transaction::ProllyTransaction<'engine, S>,
    timestamp_millis: u64,
}

/// Codec for converting typed application keys to and from ordered map bytes.
pub trait KeyCodec<K> {
    /// Encode one typed key into its order-preserving byte representation.
    fn encode_key(&self, key: &K) -> Result<Vec<u8>, Error>;

    /// Decode one stored key.
    fn decode_key(&self, bytes: &[u8]) -> Result<K, Error>;
}

/// Identity codec for byte-vector keys.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BytesKeyCodec;

impl KeyCodec<Vec<u8>> for BytesKeyCodec {
    fn encode_key(&self, key: &Vec<u8>) -> Result<Vec<u8>, Error> {
        Ok(key.clone())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(bytes.to_vec())
    }
}

/// UTF-8 codec for string keys. Ordering follows UTF-8 byte order.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StringKeyCodec;

impl KeyCodec<String> for StringKeyCodec {
    fn encode_key(&self, key: &String) -> Result<Vec<u8>, Error> {
        Ok(key.as_bytes().to_vec())
    }

    fn decode_key(&self, bytes: &[u8]) -> Result<String, Error> {
        String::from_utf8(bytes.to_vec()).map_err(|err| Error::Deserialize(err.to_string()))
    }
}

/// Typed facade over a byte-oriented [`VersionedMap`].
pub struct TypedVersionedMap<'a, S: Store, K, V, KC, VC> {
    inner: VersionedMap<'a, S>,
    key_codec: KC,
    value_codec: VC,
    marker: PhantomData<fn() -> (K, V)>,
}

/// Result of rewriting every typed value through a schema migration.
#[derive(Clone, Debug)]
pub struct TypedMigrationResult {
    /// Managed-map publication outcome.
    pub update: VersionedMapUpdate,
    /// Values decoded from the source schema.
    pub scanned_values: usize,
    /// Values rewritten into the target schema.
    pub rewritten_values: usize,
}

/// One observed managed-map head transition.
#[derive(Clone, Debug, PartialEq)]
pub struct MapChangeEvent {
    /// Previously observed head, or `None` when observation began uninitialized.
    pub previous: Option<MapVersionId>,
    /// Newly observed head.
    pub current: MapVersion,
    /// Logical changes from `previous` to `current`.
    pub diffs: Vec<Diff>,
}

/// Resumable in-process change subscription driven by explicit polling.
pub struct MapChangeSubscription<'a, S: Store> {
    map: VersionedMap<'a, S>,
    last_seen: Option<MapVersionId>,
}

impl<'a, S> MapChangeSubscription<'a, S>
where
    S: Store + ManifestStore,
{
    /// Last head observed by this subscription.
    pub fn last_seen(&self) -> Option<&MapVersionId> {
        self.last_seen.as_ref()
    }

    /// Poll once, returning `None` when head has not changed.
    pub fn poll(&mut self) -> Result<Option<MapChangeEvent>, Error> {
        let Some(current) = self.map.head()? else {
            return Ok(None);
        };
        if self.last_seen.as_ref() == Some(&current.id) {
            return Ok(None);
        }
        let previous_tree = match &self.last_seen {
            Some(id) => {
                self.map
                    .version(id)?
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(format!(
                            "subscription resume version {} was pruned",
                            id
                        ))
                    })?
                    .tree
            }
            None => self.map.prolly.create(),
        };
        let diffs = self.map.prolly.diff(&previous_tree, &current.tree)?;
        let previous = self.last_seen.replace(current.id.clone());
        Ok(Some(MapChangeEvent {
            previous,
            current,
            diffs,
        }))
    }
}

impl<'a, S: Store, K, V, KC, VC> TypedVersionedMap<'a, S, K, V, KC, VC> {
    fn new(inner: VersionedMap<'a, S>, key_codec: KC, value_codec: VC) -> Self {
        Self {
            inner,
            key_codec,
            value_codec,
            marker: PhantomData,
        }
    }

    /// Borrow the byte-oriented managed map.
    pub fn raw(&self) -> &VersionedMap<'a, S> {
        &self.inner
    }
}

impl<S, K, V, KC, VC> TypedVersionedMap<'_, S, K, V, KC, VC>
where
    S: Store + ManifestStore,
    V: serde::de::DeserializeOwned,
    KC: KeyCodec<K>,
    VC: super::value::ValueCodec,
{
    /// Read and schema-validate one typed value from head.
    pub fn get(&self, key: &K) -> Result<Option<V>, Error> {
        let key = self.key_codec.encode_key(key)?;
        self.inner
            .get(&key)?
            .map(|bytes| self.value_codec.decode(&bytes))
            .transpose()
    }

    /// Read and schema-validate one typed value from a historical version.
    pub fn get_at(&self, id: &MapVersionId, key: &K) -> Result<Option<V>, Error> {
        let key = self.key_codec.encode_key(key)?;
        self.inner
            .get_at(id, &key)?
            .map(|bytes| self.value_codec.decode(&bytes))
            .transpose()
    }

    /// Decode all entries in key order.
    pub fn entries(&self) -> Result<Vec<(K, V)>, Error> {
        let Some(snapshot) = self.inner.snapshot()? else {
            return Ok(Vec::new());
        };
        snapshot
            .range(&[], None)?
            .map(|entry| {
                let (key, value) = entry?;
                Ok((
                    self.key_codec.decode_key(&key)?,
                    self.value_codec.decode(&value)?,
                ))
            })
            .collect()
    }
}

impl<S, K, V, KC, VC> TypedVersionedMap<'_, S, K, V, KC, VC>
where
    S: Store + ManifestStore + TransactionalStore,
    V: serde::Serialize,
    KC: KeyCodec<K>,
    VC: super::value::ValueCodec,
{
    /// Encode and publish one typed value.
    pub fn put(&self, key: &K, value: &V) -> Result<MapVersion, Error> {
        self.inner.put(
            self.key_codec.encode_key(key)?,
            self.value_codec.encode(value)?,
        )
    }

    /// Conditionally encode and publish one typed value.
    pub fn put_if(
        &self,
        expected: Option<&MapVersionId>,
        key: &K,
        value: &V,
    ) -> Result<VersionedMapUpdate, Error> {
        self.inner.put_if(
            expected,
            self.key_codec.encode_key(key)?,
            self.value_codec.encode(value)?,
        )
    }

    /// Delete one typed key.
    pub fn delete(&self, key: &K) -> Result<MapVersion, Error> {
        self.inner.delete(self.key_codec.encode_key(key)?)
    }

    /// Rewrite every value from `source_codec` through `migrate` and CAS-publish.
    pub fn migrate_from<Old, OVC>(
        &self,
        expected: &MapVersionId,
        source_codec: &OVC,
        mut migrate: impl FnMut(Old) -> Result<V, Error>,
    ) -> Result<TypedMigrationResult, Error>
    where
        Old: serde::de::DeserializeOwned,
        OVC: super::value::ValueCodec,
    {
        let snapshot = self.inner.snapshot_at(expected)?.ok_or_else(|| {
            Error::InvalidVersionedMap(format!("unknown migration source version {expected}"))
        })?;
        let mut mutations = Vec::new();
        let mut scanned_values = 0usize;
        for entry in snapshot.range(&[], None)? {
            let (key, bytes) = entry?;
            let old: Old = source_codec.decode(&bytes)?;
            let value = migrate(old)?;
            mutations.push(Mutation::Upsert {
                key,
                val: self.value_codec.encode(&value)?,
            });
            scanned_values += 1;
        }
        let update = self.inner.apply_if(Some(expected), mutations)?;
        Ok(TypedMigrationResult {
            update,
            scanned_values,
            rewritten_values: scanned_values,
        })
    }
}

impl<'tx, 'engine, S> VersionedMapsTransaction<'tx, 'engine, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    fn new(
        tx: &'tx super::transaction::ProllyTransaction<'engine, S>,
        timestamp_millis: u64,
    ) -> Self {
        Self {
            tx,
            timestamp_millis,
        }
    }

    /// Load the staged or original head for one map.
    pub fn head(&self, map_id: impl AsRef<[u8]>) -> Result<Option<MapVersion>, Error> {
        let (_, head_name, _) = versioned_map_names(map_id.as_ref());
        self.tx
            .load_named_root(&head_name)?
            .map(|tree| {
                Ok(MapVersion {
                    id: MapVersionId::for_tree(&tree)?,
                    tree,
                    created_at_millis: None,
                    is_head: true,
                })
            })
            .transpose()
    }

    /// Read one key from a staged or original map head.
    pub fn get(&self, map_id: impl AsRef<[u8]>, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.head(map_id)? {
            Some(head) => self.tx.get(&head.tree, key),
            None => Ok(None),
        }
    }

    /// Apply a logical mutation batch to one map inside this transaction.
    pub fn apply(
        &self,
        map_id: impl AsRef<[u8]>,
        mutations: Vec<Mutation>,
    ) -> Result<MapVersion, Error> {
        let map_id = map_id.as_ref();
        let (_, head_name, versions_prefix) = versioned_map_names(map_id);
        let current = self.tx.load_named_root(&head_name)?;
        let base = current.clone().unwrap_or_else(|| self.tx.create());
        let next = self.tx.batch(&base, mutations)?;
        if current.as_ref() == Some(&next) {
            return Ok(MapVersion {
                id: MapVersionId::for_tree(&next)?,
                tree: next,
                created_at_millis: None,
                is_head: true,
            });
        }

        let id = MapVersionId::for_tree(&next)?;
        let mut version_name = versions_prefix;
        version_name.extend_from_slice(id.as_cid().as_bytes());
        match self.tx.load_named_root(&version_name)? {
            Some(existing) if existing != next => {
                return Err(Error::InvalidVersionedMap(format!(
                    "content identifier collision for transaction version {}",
                    id
                )));
            }
            Some(_) => {}
            None => {
                self.tx
                    .publish_named_root_at_millis(&version_name, &next, self.timestamp_millis)?
            }
        }
        self.tx
            .publish_named_root_at_millis(&head_name, &next, self.timestamp_millis)?;
        Ok(MapVersion {
            id,
            tree: next,
            created_at_millis: Some(self.timestamp_millis),
            is_head: true,
        })
    }

    /// Conditionally apply mutations when the staged/original head matches.
    pub fn apply_if(
        &self,
        map_id: impl AsRef<[u8]>,
        expected: Option<&MapVersionId>,
        mutations: Vec<Mutation>,
    ) -> Result<VersionedMapUpdate, Error> {
        let current = self.head(map_id.as_ref())?;
        if current.as_ref().map(|version| &version.id) != expected {
            return Ok(VersionedMapUpdate::Conflict { current });
        }
        let previous = current.map(|version| version.id);
        let current = self.apply(map_id, mutations)?;
        if previous.as_ref() == Some(&current.id) {
            Ok(VersionedMapUpdate::Unchanged {
                current: Some(current),
            })
        } else {
            Ok(VersionedMapUpdate::Applied { previous, current })
        }
    }

    /// Put one key in one managed map.
    pub fn put(
        &self,
        map_id: impl AsRef<[u8]>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<MapVersion, Error> {
        self.apply(
            map_id,
            vec![Mutation::Upsert {
                key: key.into(),
                val: value.into(),
            }],
        )
    }

    /// Delete one key from one managed map.
    pub fn delete(
        &self,
        map_id: impl AsRef<[u8]>,
        key: impl Into<Vec<u8>>,
    ) -> Result<MapVersion, Error> {
        self.apply(map_id, vec![Mutation::Delete { key: key.into() }])
    }

    /// Collect and apply several edits to one managed map.
    pub fn edit(
        &self,
        map_id: impl AsRef<[u8]>,
        edit: impl FnOnce(&mut VersionedMapEditor),
    ) -> Result<MapVersion, Error> {
        let mut editor = VersionedMapEditor::new();
        edit(&mut editor);
        self.apply(map_id, editor.into_mutations())
    }
}

/// Built-in versioned map facade over a [`Prolly`] engine.
///
/// Index names are hex-encoded before being placed in the named-root namespace,
/// so arbitrary application bytes cannot collide with another index's roots.
pub struct VersionedMap<'a, S: Store> {
    prolly: &'a Prolly<S>,
    id: Vec<u8>,
    root_prefix: Vec<u8>,
    head_name: Vec<u8>,
    versions_prefix: Vec<u8>,
}

impl<'a, S: Store> VersionedMap<'a, S> {
    /// Create a handle for `id` using an existing engine.
    pub fn new(prolly: &'a Prolly<S>, id: impl AsRef<[u8]>) -> Self {
        let id = id.as_ref().to_vec();
        let (root_prefix, head_name, versions_prefix) = versioned_map_names(&id);

        Self {
            prolly,
            id,
            root_prefix,
            head_name,
            versions_prefix,
        }
    }

    /// Application-provided index identifier.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    /// Full durable named-root key used for the current head.
    pub fn head_name(&self) -> &[u8] {
        &self.head_name
    }

    /// Prefix containing the immutable version roots.
    pub fn versions_prefix(&self) -> &[u8] {
        &self.versions_prefix
    }

    /// Add typed, schema-aware key and value codecs to this managed map.
    pub fn typed<K, V, KC, VC>(
        &self,
        key_codec: KC,
        value_codec: VC,
    ) -> TypedVersionedMap<'a, S, K, V, KC, VC> {
        TypedVersionedMap::new(
            VersionedMap::new(self.prolly, &self.id),
            key_codec,
            value_codec,
        )
    }

    /// Retention policy that keeps this index's head and complete version catalog.
    pub fn retention_policy(&self) -> NamedRootRetention {
        let mut isolated_prefix = self.root_prefix.clone();
        isolated_prefix.push(b'/');
        NamedRootRetention::prefix(isolated_prefix)
    }

    /// Pin the current head to an immutable request-scoped snapshot.
    pub fn snapshot(&self) -> Result<Option<MapSnapshot<'a, S>>, Error>
    where
        S: ManifestStore,
    {
        self.head()
            .map(|version| version.map(|version| MapSnapshot::new(self.prolly, version)))
    }

    /// Pin one cataloged historical version to an immutable snapshot.
    pub fn snapshot_at(&self, id: &MapVersionId) -> Result<Option<MapSnapshot<'a, S>>, Error>
    where
        S: ManifestStore,
    {
        self.version(id)
            .map(|version| version.map(|version| MapSnapshot::new(self.prolly, version)))
    }

    /// Pin two cataloged versions for repeatable comparison operations.
    pub fn compare(
        &self,
        base: &MapVersionId,
        target: &MapVersionId,
    ) -> Result<MapComparison<'a, S>, Error>
    where
        S: ManifestStore,
    {
        let base = self
            .version(base)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {base}")))?;
        let target = self
            .version(target)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {target}")))?;
        Ok(MapComparison::new(self.prolly, base, target))
    }

    /// Pin one historical version and the current head for comparison.
    pub fn compare_to_head(&self, base: &MapVersionId) -> Result<MapComparison<'a, S>, Error>
    where
        S: ManifestStore,
    {
        let base = self
            .version(base)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {base}")))?;
        let target = self.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("map has not been initialized".to_string())
        })?;
        Ok(MapComparison::new(self.prolly, base, target))
    }

    /// Pin a three-way merge between `base`, current head, and `candidate`.
    pub fn prepare_merge(
        &self,
        base: &MapVersionId,
        candidate: &MapVersionId,
    ) -> Result<MapMerge<'a, S>, Error>
    where
        S: ManifestStore,
    {
        let base = self
            .version(base)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {base}")))?;
        let candidate = self.version(candidate)?.ok_or_else(|| {
            Error::InvalidVersionedMap(format!("unknown map version {candidate}"))
        })?;
        let head = self.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("map has not been initialized".to_string())
        })?;
        Ok(MapMerge::new(
            self.prolly,
            self.id.clone(),
            base,
            head,
            candidate,
        ))
    }

    fn version_name(&self, id: &MapVersionId) -> Vec<u8> {
        let mut name = self.versions_prefix.clone();
        name.extend_from_slice(id.as_cid().as_bytes());
        name
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore,
{
    /// Start observing future head transitions from the current head.
    pub fn subscribe(&self) -> Result<MapChangeSubscription<'_, S>, Error> {
        Ok(MapChangeSubscription {
            map: VersionedMap::new(self.prolly, &self.id),
            last_seen: self.head_id()?,
        })
    }

    /// Resume observing head transitions from a previously persisted version.
    pub fn subscribe_from(&self, last_seen: Option<MapVersionId>) -> MapChangeSubscription<'_, S> {
        MapChangeSubscription {
            map: VersionedMap::new(self.prolly, &self.id),
            last_seen,
        }
    }

    /// Whether this managed map has a published head.
    pub fn is_initialized(&self) -> Result<bool, Error> {
        Ok(self.head()?.is_some())
    }

    /// Load only the current content-derived version identifier.
    pub fn head_id(&self) -> Result<Option<MapVersionId>, Error> {
        Ok(self.head()?.map(|version| version.id))
    }

    /// Load the current version, or `None` when the index has not been initialized.
    pub fn head(&self) -> Result<Option<MapVersion>, Error> {
        let manifest = self
            .prolly
            .store()
            .get_root(&self.head_name)
            .map_err(|err| Error::Store(Box::new(err)))?;
        manifest
            .map(|manifest| {
                MapVersion::new(
                    manifest.to_tree(),
                    manifest.updated_at_millis.or(manifest.created_at_millis),
                    true,
                )
            })
            .transpose()
    }

    /// Read a key from the current version. An absent index behaves like an empty map.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.head()? {
            Some(version) => self.prolly.get(&version.tree, key),
            None => Ok(None),
        }
    }

    /// Read one value from head and resolve offloaded blob content.
    pub fn get_large_value<B: super::blob::BlobStore>(
        &self,
        blob_store: &B,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>, Error> {
        match self.snapshot()? {
            Some(snapshot) => snapshot.get_large_value(blob_store, key),
            None => Ok(None),
        }
    }

    /// Check whether a key exists in the current version.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get(key)?.is_some())
    }

    /// Read several keys from one resolved current snapshot.
    ///
    /// Results preserve caller order and duplicate keys.
    pub fn get_many<K: AsRef<[u8]>>(&self, keys: &[K]) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let tree = self
            .head()?
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        self.prolly.get_many(&tree, keys)
    }

    /// Iterate over a range in the current version.
    ///
    /// An absent index behaves like an empty map. The iterator remains pinned
    /// to the resolved immutable snapshot even if another writer advances head.
    pub fn range<'a>(
        &'a self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeIter<'a, S>, Error> {
        let tree = self
            .head()?
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        self.prolly.range(&tree, start, end)
    }

    /// Iterate over keys with `prefix` in the current version.
    pub fn prefix<'a>(&'a self, prefix: &[u8]) -> Result<RangeIter<'a, S>, Error> {
        let tree = self
            .head()?
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        self.prolly.prefix(&tree, prefix)
    }

    /// Read a cursor page from the current version.
    ///
    /// The caller should keep using the same map version while consuming a
    /// cursor. Use [`VersionedMap::range_page_at`] when the head may advance
    /// between requests and repeatable pagination is required.
    pub fn range_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<RangePage, Error> {
        let tree = self
            .head()?
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        self.prolly.range_page(&tree, cursor, end, limit)
    }

    /// Read a prefix-bounded cursor page from the current version.
    pub fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: &RangeCursor,
        limit: usize,
    ) -> Result<RangePage, Error> {
        let tree = self
            .head()?
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        self.prolly.prefix_page(&tree, prefix, cursor, limit)
    }

    /// Load a version by its stable identifier.
    pub fn version(&self, id: &MapVersionId) -> Result<Option<MapVersion>, Error> {
        let manifest = self
            .prolly
            .store()
            .get_root(&self.version_name(id))
            .map_err(|err| Error::Store(Box::new(err)))?;
        let Some(manifest) = manifest else {
            return Ok(None);
        };
        let tree = manifest.to_tree();
        let actual = MapVersionId::for_tree(&tree)?;
        if actual != *id {
            return Err(Error::InvalidVersionedMap(format!(
                "version root {} points to content {}",
                id, actual
            )));
        }
        let is_head = self.head()?.map(|head| head.id == *id).unwrap_or(false);
        Ok(Some(MapVersion {
            id: actual,
            tree,
            created_at_millis: manifest.created_at_millis,
            is_head,
        }))
    }

    /// Read a key from a specific version.
    pub fn get_at(&self, id: &MapVersionId, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly.get(&version.tree, key)
    }

    /// Read several keys from a specific immutable version.
    pub fn get_many_at<K: AsRef<[u8]>>(
        &self,
        id: &MapVersionId,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly.get_many(&version.tree, keys)
    }

    /// Iterate over a range in a specific cataloged version.
    pub fn range_at<'a>(
        &'a self,
        id: &MapVersionId,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<RangeIter<'a, S>, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly.range(&version.tree, start, end)
    }

    /// Iterate over keys with `prefix` in a specific immutable version.
    pub fn prefix_at<'a>(
        &'a self,
        id: &MapVersionId,
        prefix: &[u8],
    ) -> Result<RangeIter<'a, S>, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly.prefix(&version.tree, prefix)
    }

    /// Read a cursor page from a specific immutable version.
    pub fn range_page_at(
        &self,
        id: &MapVersionId,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<RangePage, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly.range_page(&version.tree, cursor, end, limit)
    }

    /// Read a prefix-bounded cursor page from a specific immutable version.
    pub fn prefix_page_at(
        &self,
        id: &MapVersionId,
        prefix: &[u8],
        cursor: &RangeCursor,
        limit: usize,
    ) -> Result<RangePage, Error> {
        let version = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        self.prolly
            .prefix_page(&version.tree, prefix, cursor, limit)
    }

    /// Diff two cataloged versions.
    pub fn diff(&self, base: &MapVersionId, target: &MapVersionId) -> Result<Vec<Diff>, Error> {
        let base = self
            .version(base)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {base}")))?;
        let target = self
            .version(target)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {target}")))?;
        self.prolly.diff(&base.tree, &target.tree)
    }

    /// Diff a cataloged version against the current head.
    pub fn changes_since(&self, base: &MapVersionId) -> Result<Vec<Diff>, Error> {
        let base = self
            .version(base)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {base}")))?;
        let head = self.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("map has not been initialized".to_string())
        })?;
        self.prolly.diff(&base.tree, &head.tree)
    }
}

/// Async counterpart to [`VersionedMap`] for remote and browser stores.
#[cfg(feature = "async-store")]
pub struct AsyncVersionedMap<'a, S: super::store::AsyncStore> {
    prolly: &'a super::AsyncProlly<S>,
    id: Vec<u8>,
    head_name: Vec<u8>,
    versions_prefix: Vec<u8>,
}

/// Immutable version-pinned async read view.
#[cfg(feature = "async-store")]
pub struct AsyncMapSnapshot<'a, S: super::store::AsyncStore> {
    prolly: &'a super::AsyncProlly<S>,
    version: MapVersion,
}

/// Resumable async change subscription driven by explicit polling.
#[cfg(feature = "async-store")]
pub struct AsyncMapChangeSubscription<'a, S: super::store::AsyncStore> {
    map: AsyncVersionedMap<'a, S>,
    last_seen: Option<MapVersionId>,
}

#[cfg(feature = "async-store")]
impl<'a, S: super::store::AsyncStore> AsyncVersionedMap<'a, S> {
    /// Create an async managed-map handle.
    pub fn new(prolly: &'a super::AsyncProlly<S>, id: impl AsRef<[u8]>) -> Self {
        let id = id.as_ref().to_vec();
        let (_, head_name, versions_prefix) = versioned_map_names(&id);
        Self {
            prolly,
            id,
            head_name,
            versions_prefix,
        }
    }

    /// Application map identifier.
    pub fn id(&self) -> &[u8] {
        &self.id
    }

    fn version_name(&self, id: &MapVersionId) -> Vec<u8> {
        let mut name = self.versions_prefix.clone();
        name.extend_from_slice(id.as_cid().as_bytes());
        name
    }
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncVersionedMap<'a, S>
where
    S: super::store::AsyncStore + super::manifest::AsyncManifestStore,
    <S as super::store::AsyncStore>::Error: Send + Sync,
    <S as super::manifest::AsyncManifestStore>::Error: Send + Sync,
{
    /// Load current async head.
    pub async fn head(&self) -> Result<Option<MapVersion>, Error> {
        self.prolly
            .load_named_root(&self.head_name)
            .await?
            .map(|tree| {
                Ok(MapVersion {
                    id: MapVersionId::for_tree(&tree)?,
                    tree,
                    created_at_millis: None,
                    is_head: true,
                })
            })
            .transpose()
    }

    /// Load a cataloged immutable version.
    pub async fn version(&self, id: &MapVersionId) -> Result<Option<MapVersion>, Error> {
        self.prolly
            .load_named_root(&self.version_name(id))
            .await?
            .map(|tree| {
                let actual = MapVersionId::for_tree(&tree)?;
                if actual != *id {
                    return Err(Error::InvalidVersionedMap(format!(
                        "catalog root does not match async version {id}"
                    )));
                }
                Ok(MapVersion {
                    id: actual,
                    tree,
                    created_at_millis: None,
                    is_head: false,
                })
            })
            .transpose()
    }

    /// Pin current head for repeatable async reads.
    pub async fn snapshot(&self) -> Result<Option<AsyncMapSnapshot<'a, S>>, Error> {
        Ok(self.head().await?.map(|version| AsyncMapSnapshot {
            prolly: self.prolly,
            version,
        }))
    }

    /// Pin one cataloged historical version for repeatable async reads.
    pub async fn snapshot_at(
        &self,
        id: &MapVersionId,
    ) -> Result<Option<AsyncMapSnapshot<'a, S>>, Error> {
        Ok(self.version(id).await?.map(|version| AsyncMapSnapshot {
            prolly: self.prolly,
            version,
        }))
    }

    /// Start observing future async head transitions from the current head.
    pub async fn subscribe(&self) -> Result<AsyncMapChangeSubscription<'a, S>, Error> {
        Ok(AsyncMapChangeSubscription {
            map: AsyncVersionedMap::new(self.prolly, &self.id),
            last_seen: self.head().await?.map(|version| version.id),
        })
    }

    /// Resume async observation from a previously persisted version.
    pub fn subscribe_from(
        &self,
        last_seen: Option<MapVersionId>,
    ) -> AsyncMapChangeSubscription<'a, S> {
        AsyncMapChangeSubscription {
            map: AsyncVersionedMap::new(self.prolly, &self.id),
            last_seen,
        }
    }

    /// Read one key from current head.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        match self.snapshot().await? {
            Some(snapshot) => snapshot.get(key).await,
            None => Ok(None),
        }
    }
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncMapChangeSubscription<'a, S>
where
    S: super::store::AsyncStore + super::manifest::AsyncManifestStore,
    <S as super::store::AsyncStore>::Error: Send + Sync,
    <S as super::manifest::AsyncManifestStore>::Error: Send + Sync,
{
    /// Last head observed by this subscription.
    pub fn last_seen(&self) -> Option<&MapVersionId> {
        self.last_seen.as_ref()
    }

    /// Poll once, returning `None` when the async head has not changed.
    pub async fn poll(&mut self) -> Result<Option<MapChangeEvent>, Error> {
        let Some(current) = self.map.head().await? else {
            return Ok(None);
        };
        if self.last_seen.as_ref() == Some(&current.id) {
            return Ok(None);
        }
        let previous_tree = match &self.last_seen {
            Some(id) => {
                self.map
                    .version(id)
                    .await?
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(format!(
                            "async subscription resume version {} was pruned",
                            id
                        ))
                    })?
                    .tree
            }
            None => self.map.prolly.create(),
        };
        let diffs = self.map.prolly.diff(&previous_tree, &current.tree).await?;
        let previous = self.last_seen.replace(current.id.clone());
        Ok(Some(MapChangeEvent {
            previous,
            current,
            diffs,
        }))
    }
}

#[cfg(feature = "async-store")]
impl<'a, S> AsyncMapSnapshot<'a, S>
where
    S: super::store::AsyncStore,
    <S as super::store::AsyncStore>::Error: Send + Sync,
{
    /// Pinned version metadata.
    pub fn version(&self) -> &MapVersion {
        &self.version
    }

    /// Immutable pinned tree.
    pub fn tree(&self) -> &Tree {
        &self.version.tree
    }

    /// Read one key.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.prolly.get(self.tree(), key).await
    }

    /// Read several keys while preserving order and duplicates.
    pub async fn get_many<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<Vec<Option<Vec<u8>>>, Error> {
        self.prolly.get_many(self.tree(), keys).await
    }

    /// Lazily stream a key range.
    pub async fn range<'s>(
        &'s self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<super::range::AsyncRangeIter<'s, S>, Error> {
        self.prolly.range(self.tree(), start, end).await
    }

    /// Lazily stream one key prefix.
    pub async fn prefix<'s>(
        &'s self,
        prefix: &[u8],
    ) -> Result<super::range::AsyncRangeIter<'s, S>, Error> {
        self.prolly.prefix(self.tree(), prefix).await
    }

    /// Read one forward cursor page.
    pub async fn range_page(
        &self,
        cursor: &RangeCursor,
        end: Option<&[u8]>,
        limit: usize,
    ) -> Result<RangePage, Error> {
        self.prolly
            .range_page(self.tree(), cursor, end, limit)
            .await
    }

    /// Read one prefix cursor page.
    pub async fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: &RangeCursor,
        limit: usize,
    ) -> Result<RangePage, Error> {
        self.prolly
            .prefix_page(self.tree(), prefix, cursor, limit)
            .await
    }

    /// Collect tree statistics asynchronously.
    pub async fn stats(&self) -> Result<TreeStats, Error> {
        self.prolly.collect_stats(self.tree()).await
    }

    /// Prove one key asynchronously.
    pub async fn prove_key(&self, key: &[u8]) -> Result<super::proof::KeyProof, Error> {
        self.prolly.prove_key(self.tree(), key).await
    }

    /// Prove several keys asynchronously.
    pub async fn prove_keys<K: AsRef<[u8]>>(
        &self,
        keys: &[K],
    ) -> Result<super::proof::MultiKeyProof, Error> {
        self.prolly.prove_keys(self.tree(), keys).await
    }

    /// Prove a complete range asynchronously.
    pub async fn prove_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
    ) -> Result<super::proof::RangeProof, Error> {
        self.prolly.prove_range(self.tree(), start, end).await
    }

    /// Prove a complete prefix asynchronously.
    pub async fn prove_prefix(&self, prefix: &[u8]) -> Result<super::proof::RangeProof, Error> {
        self.prolly.prove_prefix(self.tree(), prefix).await
    }
}

#[cfg(feature = "async-store")]
impl<S> AsyncVersionedMap<'_, S>
where
    S: super::store::AsyncStore
        + super::manifest::AsyncManifestStore
        + super::transaction::AsyncTransactionalStore,
    <S as super::store::AsyncStore>::Error: Send + Sync,
    <S as super::manifest::AsyncManifestStore>::Error: Send + Sync,
{
    /// Atomically apply a batch and retry optimistic head conflicts.
    pub async fn apply(&self, mutations: Vec<Mutation>) -> Result<MapVersion, Error> {
        let timestamp_millis = current_unix_time_millis();
        let mut last_conflict = None;
        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let tx = self.prolly.begin_transaction()?;
            let current = tx.load_named_root(&self.head_name).await?;
            let base = current.clone().unwrap_or_else(|| tx.create());
            let next = tx.batch(&base, mutations.clone()).await?;
            if current.as_ref() == Some(&next) {
                tx.rollback();
                return Ok(MapVersion {
                    id: MapVersionId::for_tree(&next)?,
                    tree: next,
                    created_at_millis: None,
                    is_head: true,
                });
            }
            let id = MapVersionId::for_tree(&next)?;
            let version_name = self.version_name(&id);
            match tx.load_named_root(&version_name).await? {
                Some(existing) if existing != next => {
                    tx.rollback();
                    return Err(Error::InvalidVersionedMap(format!(
                        "content identifier collision for async version {}",
                        id
                    )));
                }
                Some(_) => {}
                None => {
                    tx.publish_named_root_at_millis(&version_name, &next, timestamp_millis)
                        .await?;
                }
            }
            tx.publish_named_root_at_millis(&self.head_name, &next, timestamp_millis)
                .await?;
            match tx.commit().await? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(MapVersion {
                        id,
                        tree: next,
                        created_at_millis: Some(timestamp_millis),
                        is_head: true,
                    });
                }
                TransactionUpdate::Conflict(conflict) => last_conflict = Some(conflict),
            }
        }
        Err(Error::TransactionConflict(
            last_conflict.expect("retry loop records a conflict before exhaustion"),
        ))
    }

    /// Put one key asynchronously.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<MapVersion, Error> {
        self.apply(vec![Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        }])
        .await
    }

    /// Delete one key asynchronously.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<MapVersion, Error> {
        self.apply(vec![Mutation::Delete { key: key.into() }]).await
    }

    /// Collect and apply several asynchronous managed-map edits.
    pub async fn edit(
        &self,
        edit: impl FnOnce(&mut VersionedMapEditor),
    ) -> Result<MapVersion, Error> {
        let mut editor = VersionedMapEditor::new();
        edit(&mut editor);
        self.apply(editor.into_mutations()).await
    }
}

#[cfg(feature = "async-store")]
impl<S: super::store::AsyncStore> super::AsyncProlly<S> {
    /// Open an async managed map.
    pub fn versioned_map(&self, id: impl AsRef<[u8]>) -> AsyncVersionedMap<'_, S> {
        AsyncVersionedMap::new(self, id)
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan,
{
    /// List cataloged versions newest first.
    pub fn versions(&self) -> Result<Vec<MapVersion>, Error> {
        let head_id = self.head()?.map(|head| head.id);
        let mut versions = self
            .prolly
            .list_named_root_manifests()?
            .into_iter()
            .filter_map(|named| {
                let suffix = named.name.strip_prefix(self.versions_prefix.as_slice())?;
                if suffix.len() != 32 {
                    return Some(Err(Error::InvalidVersionedMap(format!(
                        "invalid version root name under {:?}",
                        self.versions_prefix
                    ))));
                }
                let tree = named.manifest.to_tree();
                let actual = match MapVersionId::for_tree(&tree) {
                    Ok(id) => id,
                    Err(err) => return Some(Err(err)),
                };
                if actual.as_cid().as_bytes() != suffix {
                    return Some(Err(Error::InvalidVersionedMap(format!(
                        "version catalog key does not match tree content: {}",
                        actual
                    ))));
                }
                Some(Ok(MapVersion {
                    is_head: head_id.as_ref() == Some(&actual),
                    id: actual,
                    tree,
                    created_at_millis: named.manifest.created_at_millis,
                }))
            })
            .collect::<Result<Vec<_>, _>>()?;

        versions.sort_by(|left, right| {
            right
                .created_at_millis
                .cmp(&left.created_at_millis)
                .then_with(|| {
                    left.id
                        .as_cid()
                        .as_bytes()
                        .cmp(right.id.as_cid().as_bytes())
                })
        });
        Ok(versions)
    }

    /// Export every cataloged version and the current head as one portable backup.
    pub fn backup(&self) -> Result<VersionedMapBackup, Error> {
        let head = self.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("map has not been initialized".to_string())
        })?;
        let versions = self
            .versions()?
            .into_iter()
            .map(|version| {
                Ok(MapBackupVersion {
                    id: version.id,
                    created_at_millis: version.created_at_millis,
                    bundle: self.prolly.export_snapshot(&version.tree)?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let backup = VersionedMapBackup {
            map_id: self.id.clone(),
            head: head.id,
            versions,
        };
        backup.verify()?;
        Ok(backup)
    }
}

enum UpdateAttempt {
    Applied {
        previous: Option<MapVersionId>,
        current: MapVersion,
    },
    Unchanged(Option<MapVersion>),
    Conflict(TransactionConflict),
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Initialize an empty index, or return its existing head.
    pub fn initialize(&self) -> Result<MapVersion, Error> {
        self.apply(Vec::new())
    }

    /// Import one verified portable snapshot and atomically make it head.
    pub fn import_as_head(
        &self,
        bundle: &super::sync::SnapshotBundle,
    ) -> Result<MapVersion, Error> {
        self.import_as_head_at_millis(bundle, current_unix_time_millis())
    }

    /// Import one verified portable snapshot with an explicit catalog timestamp.
    pub fn import_as_head_at_millis(
        &self,
        bundle: &super::sync::SnapshotBundle,
        timestamp_millis: u64,
    ) -> Result<MapVersion, Error> {
        if !bundle.verify()?.valid {
            return Err(Error::InvalidVersionedMap(
                "snapshot bundle is not self-contained".to_string(),
            ));
        }
        if bundle.tree.config != *self.prolly.config() {
            return Err(Error::InvalidVersionedMap(
                "snapshot config does not match the managed map engine".to_string(),
            ));
        }
        let tree = self.prolly.import_snapshot(bundle)?;
        let id = MapVersionId::for_tree(&tree)?;
        let version_name = self.version_name(&id);
        let mut last_conflict = None;

        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let tx = self.prolly.begin_transaction()?;
            let current = tx.load_named_root(&self.head_name)?;
            if current.as_ref() == Some(&tree) {
                tx.rollback();
                return Ok(MapVersion {
                    id,
                    tree,
                    created_at_millis: Some(timestamp_millis),
                    is_head: true,
                });
            }
            match tx.load_named_root(&version_name)? {
                Some(existing) if existing != tree => {
                    tx.rollback();
                    return Err(Error::InvalidVersionedMap(format!(
                        "content identifier collision for imported version {}",
                        id
                    )));
                }
                Some(_) => {}
                None => tx.publish_named_root_at_millis(&version_name, &tree, timestamp_millis)?,
            }
            tx.publish_named_root_at_millis(&self.head_name, &tree, timestamp_millis)?;
            match tx.commit()? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(MapVersion {
                        id,
                        tree,
                        created_at_millis: Some(timestamp_millis),
                        is_head: true,
                    });
                }
                TransactionUpdate::Conflict(conflict) => last_conflict = Some(conflict),
            }
        }

        Err(Error::TransactionConflict(
            last_conflict.expect("retry loop records a conflict before exhaustion"),
        ))
    }

    /// Restore a complete portable catalog into an uninitialized managed map.
    pub fn restore_backup(&self, backup: &VersionedMapBackup) -> Result<MapVersion, Error> {
        backup.verify()?;
        if backup.map_id != self.id {
            return Err(Error::InvalidVersionedMap(format!(
                "backup map id {:?} does not match target {:?}",
                backup.map_id, self.id
            )));
        }
        for version in &backup.versions {
            if version.bundle.tree.config != *self.prolly.config() {
                return Err(Error::InvalidVersionedMap(format!(
                    "backup version {} uses a different tree config",
                    version.id
                )));
            }
            self.prolly.import_snapshot(&version.bundle)?;
        }

        let tx = self.prolly.begin_transaction()?;
        if tx.load_named_root(&self.head_name)?.is_some() {
            tx.rollback();
            return Err(Error::InvalidVersionedMap(
                "restore target is already initialized".to_string(),
            ));
        }
        let mut restored_head = None;
        for version in &backup.versions {
            let tree = &version.bundle.tree;
            let name = self.version_name(&version.id);
            match tx.load_named_root(&name)? {
                Some(existing) if existing != *tree => {
                    tx.rollback();
                    return Err(Error::InvalidVersionedMap(format!(
                        "target version root {} contains different content",
                        version.id
                    )));
                }
                Some(_) => {}
                None => tx.publish_named_root_at_millis(
                    &name,
                    tree,
                    version
                        .created_at_millis
                        .unwrap_or_else(current_unix_time_millis),
                )?,
            }
            if version.id == backup.head {
                restored_head = Some(MapVersion {
                    id: version.id.clone(),
                    tree: tree.clone(),
                    created_at_millis: version.created_at_millis,
                    is_head: true,
                });
            }
        }
        let restored_head = restored_head.expect("verified backup contains its head");
        tx.publish_named_root_at_millis(
            &self.head_name,
            &restored_head.tree,
            restored_head
                .created_at_millis
                .unwrap_or_else(current_unix_time_millis),
        )?;
        match tx.commit()? {
            TransactionUpdate::Applied { .. } => Ok(restored_head),
            TransactionUpdate::Conflict(conflict) => Err(Error::TransactionConflict(conflict)),
        }
    }

    /// Apply a mutation batch atomically, retrying optimistic conflicts.
    pub fn apply(&self, mutations: Vec<Mutation>) -> Result<MapVersion, Error> {
        self.apply_at_millis(mutations, current_unix_time_millis())
    }

    /// Apply a mutation batch with an explicit timestamp.
    pub fn apply_at_millis(
        &self,
        mutations: Vec<Mutation>,
        timestamp_millis: u64,
    ) -> Result<MapVersion, Error> {
        let mut last_conflict = None;
        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            match self.try_apply(&mutations, None, timestamp_millis)? {
                UpdateAttempt::Applied { current, .. } => return Ok(current),
                UpdateAttempt::Unchanged(Some(current)) => return Ok(current),
                UpdateAttempt::Unchanged(None) => {
                    return Err(Error::InvalidVersionedMap(
                        "empty update did not initialize the index".to_string(),
                    ));
                }
                UpdateAttempt::Conflict(conflict) => last_conflict = Some(conflict),
            }
        }
        Err(Error::TransactionConflict(
            last_conflict.expect("retry loop records a conflict before exhaustion"),
        ))
    }

    /// Apply mutations only when `expected` still identifies the current head.
    pub fn apply_if(
        &self,
        expected: Option<&MapVersionId>,
        mutations: Vec<Mutation>,
    ) -> Result<VersionedMapUpdate, Error> {
        self.apply_if_at_millis(expected, mutations, current_unix_time_millis())
    }

    /// Apply a conditional mutation batch with an explicit timestamp.
    pub fn apply_if_at_millis(
        &self,
        expected: Option<&MapVersionId>,
        mutations: Vec<Mutation>,
        timestamp_millis: u64,
    ) -> Result<VersionedMapUpdate, Error> {
        match self.try_apply(&mutations, Some(expected), timestamp_millis)? {
            UpdateAttempt::Applied { previous, current } => {
                Ok(VersionedMapUpdate::Applied { previous, current })
            }
            UpdateAttempt::Unchanged(current) => Ok(VersionedMapUpdate::Unchanged { current }),
            UpdateAttempt::Conflict(_) => Ok(VersionedMapUpdate::Conflict {
                current: self.head()?,
            }),
        }
    }

    /// Conditionally insert or replace one key.
    pub fn put_if(
        &self,
        expected: Option<&MapVersionId>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<VersionedMapUpdate, Error> {
        self.apply_if(
            expected,
            vec![Mutation::Upsert {
                key: key.into(),
                val: value.into(),
            }],
        )
    }

    /// Conditionally delete one key.
    pub fn delete_if(
        &self,
        expected: Option<&MapVersionId>,
        key: impl Into<Vec<u8>>,
    ) -> Result<VersionedMapUpdate, Error> {
        self.apply_if(expected, vec![Mutation::Delete { key: key.into() }])
    }

    /// Collect several mutations and apply them only when `expected` is current.
    pub fn edit_if(
        &self,
        expected: Option<&MapVersionId>,
        edit: impl FnOnce(&mut VersionedMapEditor),
    ) -> Result<VersionedMapUpdate, Error> {
        let mut editor = VersionedMapEditor::new();
        edit(&mut editor);
        self.apply_if(expected, editor.into_mutations())
    }

    /// Insert or replace one key and return the new current version.
    pub fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<MapVersion, Error> {
        self.apply(vec![Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        }])
    }

    /// Insert one value, offloading large bytes, and retry head conflicts.
    pub fn put_large_value<B: super::blob::BlobStore>(
        &self,
        blob_store: &B,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        config: super::blob::LargeValueConfig,
    ) -> Result<MapVersion, Error> {
        let key = key.into();
        let value = value.into();
        let mut last_conflict = None;
        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let current = self.head()?;
            let expected = current.as_ref().map(|version| &version.id);
            let tree = current
                .as_ref()
                .map(|version| version.tree.clone())
                .unwrap_or_else(|| self.prolly.create());
            let next = self.prolly.put_large_value(
                blob_store,
                &tree,
                key.clone(),
                value.clone(),
                config.clone(),
            )?;
            match self.publish_tree_if(expected, &next, current_unix_time_millis())? {
                VersionedMapUpdate::Applied { current, .. } => return Ok(current),
                VersionedMapUpdate::Unchanged {
                    current: Some(current),
                } => return Ok(current),
                VersionedMapUpdate::Conflict { current } => {
                    last_conflict = current;
                }
                VersionedMapUpdate::Unchanged { current: None } => {}
            }
        }
        Err(Error::InvalidVersionedMap(format!(
            "large-value update exhausted retries at head {:?}",
            last_conflict.map(|version| version.id)
        )))
    }

    /// Conditionally insert one inline/blob-backed value.
    pub fn put_large_value_if<B: super::blob::BlobStore>(
        &self,
        blob_store: &B,
        expected: Option<&MapVersionId>,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        config: super::blob::LargeValueConfig,
    ) -> Result<VersionedMapUpdate, Error> {
        let current = self.head()?;
        if current.as_ref().map(|version| &version.id) != expected {
            return Ok(VersionedMapUpdate::Conflict { current });
        }
        let tree = current
            .map(|version| version.tree)
            .unwrap_or_else(|| self.prolly.create());
        let next =
            self.prolly
                .put_large_value(blob_store, &tree, key.into(), value.into(), config)?;
        self.publish_tree_if(expected, &next, current_unix_time_millis())
    }

    /// Delete one key and return the new current version.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<MapVersion, Error> {
        self.apply(vec![Mutation::Delete { key: key.into() }])
    }

    /// Collect several mutations in a compact closure and commit them once.
    pub fn edit(&self, edit: impl FnOnce(&mut VersionedMapEditor)) -> Result<MapVersion, Error> {
        let mut editor = VersionedMapEditor::new();
        edit(&mut editor);
        self.apply(editor.into_mutations())
    }

    /// Apply append-oriented mutations using the engine's right-edge fast path.
    pub fn append(&self, mutations: Vec<Mutation>) -> Result<MapVersion, Error> {
        let mut last_head = None;
        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let current = self.head()?;
            let expected = current.as_ref().map(|version| &version.id);
            let tree = current
                .as_ref()
                .map(|version| version.tree.clone())
                .unwrap_or_else(|| self.prolly.create());
            let next = self.prolly.append_batch(&tree, mutations.clone())?;
            match self.publish_tree_if(expected, &next, current_unix_time_millis())? {
                VersionedMapUpdate::Applied { current, .. }
                | VersionedMapUpdate::Unchanged {
                    current: Some(current),
                } => return Ok(current),
                VersionedMapUpdate::Conflict { current } => last_head = current,
                VersionedMapUpdate::Unchanged { current: None } => {}
            }
        }
        Err(Error::InvalidVersionedMap(format!(
            "append exhausted retries at head {:?}",
            last_head.map(|version| version.id)
        )))
    }

    /// Apply a route-planned parallel mutation batch and publish its statistics.
    pub fn parallel_apply(
        &self,
        mutations: Vec<Mutation>,
        config: &super::parallel::ParallelConfig,
    ) -> Result<VersionedMapBatchResult, Error> {
        let mut last_head = None;
        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let current = self.head()?;
            let expected = current.as_ref().map(|version| &version.id);
            let tree = current
                .as_ref()
                .map(|version| version.tree.clone())
                .unwrap_or_else(|| self.prolly.create());
            let applied =
                self.prolly
                    .parallel_batch_with_stats(&tree, mutations.clone(), config)?;
            match self.publish_tree_if(expected, &applied.tree, current_unix_time_millis())? {
                VersionedMapUpdate::Applied { current, .. }
                | VersionedMapUpdate::Unchanged {
                    current: Some(current),
                } => {
                    return Ok(VersionedMapBatchResult {
                        version: current,
                        stats: applied.stats,
                    });
                }
                VersionedMapUpdate::Conflict { current } => last_head = current,
                VersionedMapUpdate::Unchanged { current: None } => {}
            }
        }
        Err(Error::InvalidVersionedMap(format!(
            "parallel batch exhausted retries at head {:?}",
            last_head.map(|version| version.id)
        )))
    }

    /// Move the head to an existing version without deleting newer snapshots.
    ///
    /// Versions identify unique tree states rather than update events, so a
    /// rollback moves the head but does not create a duplicate catalog entry.
    pub fn rollback_to(&self, id: &MapVersionId) -> Result<MapVersion, Error> {
        let target = self
            .version(id)?
            .ok_or_else(|| Error::InvalidVersionedMap(format!("unknown map version {id}")))?;
        let timestamp_millis = current_unix_time_millis();
        let mut last_conflict = None;

        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let tx = self.prolly.begin_transaction()?;
            let current = tx.load_named_root(&self.head_name)?;
            if current.as_ref() == Some(&target.tree) {
                return Ok(MapVersion {
                    is_head: true,
                    ..target
                });
            }
            tx.publish_named_root_at_millis(&self.head_name, &target.tree, timestamp_millis)?;
            match tx.commit()? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(MapVersion {
                        is_head: true,
                        ..target
                    });
                }
                TransactionUpdate::Conflict(conflict) => last_conflict = Some(conflict),
            }
        }

        Err(Error::TransactionConflict(
            last_conflict.expect("retry loop records a conflict before exhaustion"),
        ))
    }

    fn try_apply(
        &self,
        mutations: &[Mutation],
        expected: Option<Option<&MapVersionId>>,
        timestamp_millis: u64,
    ) -> Result<UpdateAttempt, Error> {
        let tx = self.prolly.begin_transaction()?;
        let current_tree = tx.load_named_root(&self.head_name)?;
        let current_id = current_tree
            .as_ref()
            .map(MapVersionId::for_tree)
            .transpose()?;

        if let Some(expected) = expected {
            if current_id.as_ref() != expected {
                tx.rollback();
                return Ok(UpdateAttempt::Conflict(TransactionConflict::new(
                    self.head_name.clone(),
                    None,
                    None,
                )));
            }
        }

        let base = current_tree.clone().unwrap_or_else(|| tx.create());
        let next = tx.batch(&base, mutations.to_vec())?;
        if current_tree.as_ref() == Some(&next) {
            let current = Some(MapVersion {
                id: current_id
                    .clone()
                    .expect("an unchanged existing tree has a version id"),
                tree: next,
                created_at_millis: None,
                is_head: true,
            });
            return match tx.commit()? {
                TransactionUpdate::Applied { .. } => Ok(UpdateAttempt::Unchanged(current)),
                TransactionUpdate::Conflict(conflict) => Ok(UpdateAttempt::Conflict(conflict)),
            };
        }

        let next_id = MapVersionId::for_tree(&next)?;
        let version_name = self.version_name(&next_id);
        match tx.load_named_root(&version_name)? {
            Some(existing) if existing != next => {
                tx.rollback();
                return Err(Error::InvalidVersionedMap(format!(
                    "content identifier collision for version {}",
                    next_id
                )));
            }
            Some(_) => {}
            None => {
                tx.publish_named_root_at_millis(&version_name, &next, timestamp_millis)?;
            }
        }
        tx.publish_named_root_at_millis(&self.head_name, &next, timestamp_millis)?;

        match tx.commit()? {
            TransactionUpdate::Applied { .. } => Ok(UpdateAttempt::Applied {
                previous: current_id,
                current: MapVersion {
                    id: next_id,
                    tree: next,
                    created_at_millis: Some(timestamp_millis),
                    is_head: true,
                },
            }),
            TransactionUpdate::Conflict(conflict) => Ok(UpdateAttempt::Conflict(conflict)),
        }
    }

    fn publish_tree_if(
        &self,
        expected: Option<&MapVersionId>,
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<VersionedMapUpdate, Error> {
        let tx = self.prolly.begin_transaction()?;
        let current_tree = tx.load_named_root(&self.head_name)?;
        let current_id = current_tree
            .as_ref()
            .map(MapVersionId::for_tree)
            .transpose()?;
        if current_id.as_ref() != expected {
            tx.rollback();
            return Ok(VersionedMapUpdate::Conflict {
                current: self.head()?,
            });
        }
        if current_tree.as_ref() == Some(tree) {
            let current = self.head()?;
            return match tx.commit()? {
                TransactionUpdate::Applied { .. } => Ok(VersionedMapUpdate::Unchanged { current }),
                TransactionUpdate::Conflict(_) => Ok(VersionedMapUpdate::Conflict {
                    current: self.head()?,
                }),
            };
        }

        let id = MapVersionId::for_tree(tree)?;
        let version_name = self.version_name(&id);
        match tx.load_named_root(&version_name)? {
            Some(existing) if existing != *tree => {
                tx.rollback();
                return Err(Error::InvalidVersionedMap(format!(
                    "content identifier collision for merged version {}",
                    id
                )));
            }
            Some(_) => {}
            None => tx.publish_named_root_at_millis(&version_name, tree, timestamp_millis)?,
        }
        tx.publish_named_root_at_millis(&self.head_name, tree, timestamp_millis)?;
        match tx.commit()? {
            TransactionUpdate::Applied { .. } => Ok(VersionedMapUpdate::Applied {
                previous: current_id,
                current: MapVersion {
                    id,
                    tree: tree.clone(),
                    created_at_millis: Some(timestamp_millis),
                    is_head: true,
                },
            }),
            TransactionUpdate::Conflict(_) => Ok(VersionedMapUpdate::Conflict {
                current: self.head()?,
            }),
        }
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan + TransactionalStore,
{
    /// Keep the newest `keep_latest` cataloged versions plus the current head.
    ///
    /// The head is always retained, even after rollback when it is older than
    /// the newest catalog entries. `keep_latest == 0` therefore keeps exactly
    /// the current head. This operation removes immutable version root names in
    /// one strict transaction; it does not delete content-addressed nodes.
    pub fn prune_versions(&self, keep_latest: usize) -> Result<VersionPruneResult, Error> {
        self.keep_last(keep_latest)
    }

    /// Retain the newest `count` versions plus the current head.
    pub fn keep_last(&self, count: usize) -> Result<VersionPruneResult, Error> {
        self.prune_with(|versions| {
            Ok(versions
                .iter()
                .take(count)
                .map(|version| version.id.clone())
                .collect())
        })
    }

    /// Retain versions newer than `max_age` plus the current head.
    pub fn keep_for(&self, max_age: std::time::Duration) -> Result<VersionPruneResult, Error> {
        self.keep_for_at(current_unix_time_millis(), max_age)
    }

    /// Deterministic form of [`VersionedMap::keep_for`] with an explicit clock.
    pub fn keep_for_at(
        &self,
        now_millis: u64,
        max_age: std::time::Duration,
    ) -> Result<VersionPruneResult, Error> {
        let age_millis = max_age.as_millis().min(u128::from(u64::MAX)) as u64;
        let cutoff = now_millis.saturating_sub(age_millis);
        self.prune_with(|versions| {
            Ok(versions
                .iter()
                .filter(|version| {
                    version
                        .created_at_millis
                        .map(|created| created >= cutoff)
                        .unwrap_or(true)
                })
                .map(|version| version.id.clone())
                .collect())
        })
    }

    /// Retain an explicit version set plus the current head.
    ///
    /// Missing requested IDs are rejected so a typo cannot silently discard
    /// more history than intended.
    pub fn keep_versions<I, V>(&self, ids: I) -> Result<VersionPruneResult, Error>
    where
        I: IntoIterator<Item = V>,
        V: Borrow<MapVersionId>,
    {
        let requested = ids
            .into_iter()
            .map(|id| id.borrow().clone())
            .collect::<HashSet<_>>();
        self.prune_with(|versions| {
            let present = versions
                .iter()
                .map(|version| version.id.clone())
                .collect::<HashSet<_>>();
            let missing = requested.difference(&present).collect::<Vec<_>>();
            if !missing.is_empty() {
                return Err(Error::InvalidVersionedMap(format!(
                    "retention requested unknown versions: {:?}",
                    missing
                )));
            }
            Ok(requested.clone())
        })
    }

    fn prune_with(
        &self,
        select: impl Fn(&[MapVersion]) -> Result<HashSet<MapVersionId>, Error>,
    ) -> Result<VersionPruneResult, Error> {
        let mut last_conflict = None;

        for _ in 0..DEFAULT_VERSIONED_MAP_RETRIES {
            let tx = self.prolly.begin_transaction()?;
            let Some(head_tree) = tx.load_named_root(&self.head_name)? else {
                tx.rollback();
                let versions = self.versions()?;
                if versions.is_empty() {
                    return Ok(VersionPruneResult::default());
                }
                return Err(Error::InvalidVersionedMap(
                    "version roots exist without a current head".to_string(),
                ));
            };
            let head_id = MapVersionId::for_tree(&head_tree)?;
            let versions = self.versions()?;
            if !versions.iter().any(|version| version.id == head_id) {
                tx.rollback();
                return Err(Error::InvalidVersionedMap(format!(
                    "current head {} is absent from the version catalog",
                    head_id
                )));
            }

            let mut retained_ids = select(&versions)?;
            retained_ids.insert(head_id);

            let retained = versions
                .iter()
                .filter(|version| retained_ids.contains(&version.id))
                .map(|version| version.id.clone())
                .collect::<Vec<_>>();
            let removed = versions
                .iter()
                .filter(|version| !retained_ids.contains(&version.id))
                .map(|version| version.id.clone())
                .collect::<Vec<_>>();

            if removed.is_empty() {
                tx.rollback();
                return Ok(VersionPruneResult { retained, removed });
            }

            for id in &removed {
                let name = self.version_name(id);
                if tx.load_named_root(&name)?.is_some() {
                    tx.delete_named_root(&name)?;
                }
            }

            match tx.commit()? {
                TransactionUpdate::Applied { .. } => {
                    return Ok(VersionPruneResult { retained, removed });
                }
                TransactionUpdate::Conflict(conflict) => last_conflict = Some(conflict),
            }
        }

        Err(Error::TransactionConflict(
            last_conflict.expect("retry loop records a conflict before exhaustion"),
        ))
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + TransactionalStore + Clone + Send + Sync,
{
    /// Build sorted input with the streaming builder and initialize an absent map.
    pub fn initialize_sorted<I, K, V>(&self, entries: I) -> Result<VersionedMapUpdate, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        self.rebuild_sorted_if(None, entries)
    }

    /// Stream sorted input into a candidate tree, then CAS-replace head.
    pub fn rebuild_sorted_if<I, K, V>(
        &self,
        expected: Option<&MapVersionId>,
        entries: I,
    ) -> Result<VersionedMapUpdate, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let current = self.head()?;
        if current.as_ref().map(|version| &version.id) != expected {
            return Ok(VersionedMapUpdate::Conflict { current });
        }
        let mut builder = super::builder::SortedBatchBuilder::new(
            self.prolly.store().clone(),
            self.prolly.config().clone(),
        );
        for (key, value) in entries {
            builder.add(key.into(), value.into())?;
        }
        let tree = builder.build()?;
        self.publish_tree_if(expected, &tree, current_unix_time_millis())
    }

    /// Build arbitrary iterator input in parallel, then CAS-replace head.
    pub fn rebuild_from_iter_if<I, K, V>(
        &self,
        expected: Option<&MapVersionId>,
        entries: I,
    ) -> Result<VersionedMapUpdate, Error>
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<Vec<u8>>,
        V: Into<Vec<u8>>,
    {
        let current = self.head()?;
        if current.as_ref().map(|version| &version.id) != expected {
            return Ok(VersionedMapUpdate::Conflict { current });
        }
        let mut builder = super::builder::BatchBuilder::new(
            self.prolly.store().clone(),
            self.prolly.config().clone(),
        );
        for (key, value) in entries {
            builder.add(key.into(), value.into());
        }
        let tree = builder.build()?;
        self.publish_tree_if(expected, &tree, current_unix_time_millis())
    }
}

/// Successful integrity audit of one complete managed-map catalog.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MapCatalogVerification {
    /// Current head identifier.
    pub head: MapVersionId,
    /// Number of immutable catalog entries.
    pub version_count: usize,
    /// Unique nodes reachable from all retained versions.
    pub reachable_nodes: usize,
    /// Serialized bytes reachable from all retained versions.
    pub reachable_bytes: usize,
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan,
{
    /// Verify root names, version IDs, head membership, and every reachable node.
    pub fn verify_catalog(&self) -> Result<MapCatalogVerification, Error> {
        let head = self.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("map has not been initialized".to_string())
        })?;
        let versions = self.versions()?;
        if !versions.iter().any(|version| version.id == head.id) {
            return Err(Error::InvalidVersionedMap(format!(
                "current head {} is absent from the version catalog",
                head.id
            )));
        }
        let trees = versions
            .iter()
            .map(|version| version.tree.clone())
            .collect::<Vec<_>>();
        let reachable = self.prolly.mark_reachable(&trees)?;
        Ok(MapCatalogVerification {
            head: head.id,
            version_count: versions.len(),
            reachable_nodes: reachable.live_nodes,
            reachable_bytes: reachable.live_bytes,
        })
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan + super::store::NodeStoreScan,
{
    /// Dry-run node GC after applying this map's retention policy.
    ///
    /// Node storage is shared and content-addressed, so the safety boundary is
    /// necessarily store-wide: every remaining named root is retained. This
    /// prevents maintenance on one map from deleting another map's nodes.
    pub fn plan_gc(&self) -> Result<super::gc::GcPlan, Error> {
        self.prolly
            .plan_store_gc_for_retention(&NamedRootRetention::all())
    }

    /// Sweep store-wide nodes unreachable from every remaining named root.
    pub fn sweep_gc(&self) -> Result<super::gc::GcSweep, Error> {
        self.prolly
            .sweep_store_gc_for_retention(&NamedRootRetention::all())
    }
}

impl<S> VersionedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan,
{
    /// Plan blob GC while retaining blobs from every remaining named root.
    ///
    /// Blob stores are commonly shared by several maps, so limiting reachability
    /// to this map would make a per-map maintenance call unsafe for its peers.
    pub fn plan_blob_gc<B: super::blob::BlobStoreScan>(
        &self,
        blob_store: &B,
    ) -> Result<super::gc::BlobGcPlan, Error> {
        let roots = self
            .prolly
            .load_retained_named_roots(&NamedRootRetention::all())?
            .trees();
        self.prolly.plan_blob_store_gc(blob_store, &roots)
    }

    /// Sweep blobs unreachable from every remaining named root in the store.
    pub fn sweep_blob_gc<B: super::blob::BlobStoreScan>(
        &self,
        blob_store: &B,
    ) -> Result<super::gc::BlobGcSweep, Error> {
        let roots = self
            .prolly
            .load_retained_named_roots(&NamedRootRetention::all())?
            .trees();
        self.prolly.sweep_blob_store_gc(blob_store, &roots)
    }
}

impl<S: Store> Prolly<S> {
    /// Open a built-in versioned map identified by arbitrary application bytes.
    pub fn versioned_map(&self, id: impl AsRef<[u8]>) -> VersionedMap<'_, S> {
        VersionedMap::new(self, id)
    }
}

impl<S> Prolly<S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Atomically update any number of managed maps in one strict transaction.
    pub fn versioned_maps_transaction<T>(
        &self,
        run: impl FnOnce(&mut VersionedMapsTransaction<'_, '_, S>) -> Result<T, Error>,
    ) -> Result<T, Error> {
        let timestamp_millis = current_unix_time_millis();
        self.transaction(|tx| {
            let mut maps = VersionedMapsTransaction::new(tx, timestamp_millis);
            run(&mut maps)
        })
    }
}

fn append_hex(output: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    output.reserve(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize]);
        output.push(HEX[(byte & 0x0f) as usize]);
    }
}

fn versioned_map_names(id: &[u8]) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
    let mut root_prefix = VERSIONED_MAP_ROOT_PREFIX.to_vec();
    append_hex(&mut root_prefix, id);

    let mut head_name = root_prefix.clone();
    head_name.extend_from_slice(HEAD_SUFFIX);

    let mut versions_prefix = root_prefix.clone();
    versions_prefix.extend_from_slice(VERSIONS_SUFFIX);
    (root_prefix, head_name, versions_prefix)
}
