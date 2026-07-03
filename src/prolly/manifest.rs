//! Named root manifest support.
//!
//! Content-addressed tree nodes identify immutable snapshots, but applications
//! also need durable names such as `main`, `workspace/123/latest`, or
//! `agent-run/abc/checkpoint/42`. The manifest layer records those names
//! separately from node storage and provides compare-and-swap updates for
//! concurrent writers.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;

use super::cid::Cid;
use super::config::Config;
use super::error::Error;
use super::tree::Tree;

const ROOT_MANIFEST_VERSION: u64 = 1;

#[derive(Serialize, Deserialize)]
struct RootManifestWire {
    version: u64,
    root: Option<Cid>,
    config: Config,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    created_at_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    updated_at_millis: Option<u64>,
}

/// Durable named-root payload.
///
/// A root manifest stores the complete tree handle needed to reopen a named
/// snapshot. The root CID alone is not enough because chunking and encoding
/// config determine how the tree should be interpreted.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RootManifest {
    /// Root node CID, or `None` for the empty tree.
    pub root: Option<Cid>,
    /// Tree configuration associated with this root.
    pub config: Config,
    /// Optional Unix timestamp in milliseconds when this named root was created.
    pub created_at_millis: Option<u64>,
    /// Optional Unix timestamp in milliseconds when this named root was updated.
    pub updated_at_millis: Option<u64>,
}

impl RootManifest {
    /// Create a new manifest from a root CID and config.
    pub fn new(root: Option<Cid>, config: Config) -> Self {
        Self {
            root,
            config,
            created_at_millis: None,
            updated_at_millis: None,
        }
    }

    /// Set optional creation and update timestamps in Unix milliseconds.
    pub fn with_timestamps_millis(
        mut self,
        created_at_millis: Option<u64>,
        updated_at_millis: Option<u64>,
    ) -> Self {
        self.created_at_millis = created_at_millis;
        self.updated_at_millis = updated_at_millis;
        self
    }

    /// Set the creation timestamp in Unix milliseconds.
    pub fn with_created_at_millis(mut self, created_at_millis: u64) -> Self {
        self.created_at_millis = Some(created_at_millis);
        self
    }

    /// Set the update timestamp in Unix milliseconds.
    pub fn with_updated_at_millis(mut self, updated_at_millis: u64) -> Self {
        self.updated_at_millis = Some(updated_at_millis);
        self
    }

    /// Create a manifest from a tree and timestamp metadata.
    pub fn from_tree_with_timestamps_millis(
        tree: &Tree,
        created_at_millis: Option<u64>,
        updated_at_millis: Option<u64>,
    ) -> Self {
        Self {
            root: tree.root.clone(),
            config: tree.config.clone(),
            created_at_millis,
            updated_at_millis,
        }
    }

    /// Create a manifest from an existing tree handle.
    pub fn from_tree(tree: &Tree) -> Self {
        Self {
            root: tree.root.clone(),
            config: tree.config.clone(),
            created_at_millis: None,
            updated_at_millis: None,
        }
    }

    /// Convert this manifest into a tree handle.
    pub fn into_tree(self) -> Tree {
        Tree {
            root: self.root,
            config: self.config,
        }
    }

    /// Convert this manifest into a cloned tree handle.
    pub fn to_tree(&self) -> Tree {
        Tree {
            root: self.root.clone(),
            config: self.config.clone(),
        }
    }

    /// Serialize this manifest to a versioned, deterministic binary payload.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let wire = RootManifestWire {
            version: ROOT_MANIFEST_VERSION,
            root: self.root.clone(),
            config: self.config.clone(),
            created_at_millis: self.created_at_millis,
            updated_at_millis: self.updated_at_millis,
        };
        serde_cbor::ser::to_vec_packed(&wire).map_err(|err| Error::Deserialize(err.to_string()))
    }

    /// Decode a manifest from bytes produced by [`RootManifest::to_bytes`].
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire: RootManifestWire =
            serde_cbor::from_slice(bytes).map_err(|err| Error::Deserialize(err.to_string()))?;
        if wire.version != ROOT_MANIFEST_VERSION {
            return Err(Error::Deserialize(format!(
                "unsupported root manifest version: {}",
                wire.version
            )));
        }
        Ok(Self {
            root: wire.root,
            config: wire.config,
            created_at_millis: wire.created_at_millis,
            updated_at_millis: wire.updated_at_millis,
        })
    }
}

impl From<Tree> for RootManifest {
    fn from(tree: Tree) -> Self {
        Self {
            root: tree.root,
            config: tree.config,
            created_at_millis: None,
            updated_at_millis: None,
        }
    }
}

impl From<&Tree> for RootManifest {
    fn from(tree: &Tree) -> Self {
        Self::from_tree(tree)
    }
}

impl From<RootManifest> for Tree {
    fn from(manifest: RootManifest) -> Self {
        manifest.into_tree()
    }
}

/// A named root manifest returned by manifest-store scans.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedRootManifest {
    /// Durable name of the root manifest.
    pub name: Vec<u8>,
    /// Manifest stored under `name`.
    pub manifest: RootManifest,
}

impl NamedRootManifest {
    /// Create a named manifest entry.
    pub fn new(name: Vec<u8>, manifest: RootManifest) -> Self {
        Self { name, manifest }
    }

    /// Convert this manifest entry into a tree-oriented named root.
    pub fn into_named_root(self) -> NamedRoot {
        NamedRoot {
            name: self.name,
            tree: self.manifest.into_tree(),
        }
    }

    /// Convert this manifest entry into a cloned tree-oriented named root.
    pub fn to_named_root(&self) -> NamedRoot {
        NamedRoot {
            name: self.name.clone(),
            tree: self.manifest.to_tree(),
        }
    }
}

/// A named root loaded through a [`crate::Prolly`] manager.
#[derive(Clone, Debug, PartialEq)]
pub struct NamedRoot {
    /// Durable name of the root.
    pub name: Vec<u8>,
    /// Tree handle stored under `name`.
    pub tree: Tree,
}

impl NamedRoot {
    /// Create a named tree root.
    pub fn new(name: Vec<u8>, tree: Tree) -> Self {
        Self { name, tree }
    }
}

/// Result of loading roots for a retention policy.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct NamedRootSelection {
    /// Roots selected by the retention policy.
    pub roots: Vec<NamedRoot>,
    /// Exact names requested by the policy that were not present.
    pub missing_names: Vec<Vec<u8>>,
}

impl NamedRootSelection {
    /// Create a new selection from roots and missing exact names.
    pub fn new(roots: Vec<NamedRoot>, missing_names: Vec<Vec<u8>>) -> Self {
        Self {
            roots,
            missing_names,
        }
    }

    /// Whether every exact name requested by the retention policy was present.
    pub fn is_complete(&self) -> bool {
        self.missing_names.is_empty()
    }

    /// Clone selected tree handles for GC APIs.
    pub fn trees(&self) -> Vec<Tree> {
        self.roots.iter().map(|root| root.tree.clone()).collect()
    }

    /// Consume the selection and return tree handles for GC APIs.
    pub fn into_trees(self) -> Vec<Tree> {
        self.roots.into_iter().map(|root| root.tree).collect()
    }
}

/// Policy for selecting named roots to retain during garbage collection.
///
/// `NewestByName` keeps the lexicographically greatest names matching `prefix`.
/// This works well for names that include sortable sequence numbers or
/// timestamps, for example `checkpoint/000042` or
/// `checkpoint/2026-07-01T12:00:00Z`.
#[derive(Clone, Debug, PartialEq)]
pub enum NamedRootRetention {
    /// Retain every named root in the manifest store.
    All,
    /// Retain an explicit list of root names.
    Exact {
        /// Exact names to load.
        names: Vec<Vec<u8>>,
    },
    /// Retain every named root whose name starts with `prefix`.
    Prefix {
        /// Name prefix to retain.
        prefix: Vec<u8>,
    },
    /// Retain the lexicographically newest `count` roots with a prefix.
    NewestByName {
        /// Name prefix to retain.
        prefix: Vec<u8>,
        /// Maximum number of roots to retain.
        count: usize,
    },
    /// Retain roots with `updated_at_millis >= min_updated_at_millis`.
    UpdatedSince {
        /// Optional name prefix to retain.
        prefix: Vec<u8>,
        /// Minimum update timestamp in Unix milliseconds.
        min_updated_at_millis: u64,
    },
}

impl NamedRootRetention {
    /// Retain every named root in the manifest store.
    pub fn all() -> Self {
        Self::All
    }

    /// Retain an explicit list of named roots.
    pub fn exact<I, N>(names: I) -> Self
    where
        I: IntoIterator<Item = N>,
        N: AsRef<[u8]>,
    {
        Self::Exact {
            names: names
                .into_iter()
                .map(|name| name.as_ref().to_vec())
                .collect(),
        }
    }

    /// Retain every named root whose name starts with `prefix`.
    pub fn prefix(prefix: impl AsRef<[u8]>) -> Self {
        Self::Prefix {
            prefix: prefix.as_ref().to_vec(),
        }
    }

    /// Retain the lexicographically newest `count` roots with `prefix`.
    pub fn newest_by_name(prefix: impl AsRef<[u8]>, count: usize) -> Self {
        Self::NewestByName {
            prefix: prefix.as_ref().to_vec(),
            count,
        }
    }

    /// Retain roots updated at or after `min_updated_at_millis`.
    pub fn updated_since(prefix: impl AsRef<[u8]>, min_updated_at_millis: u64) -> Self {
        Self::UpdatedSince {
            prefix: prefix.as_ref().to_vec(),
            min_updated_at_millis,
        }
    }

    /// Retain roots updated within `max_age` before `now_millis`.
    ///
    /// `now_millis` is explicit so tests, replay, and distributed systems can
    /// choose their own clock source. The cutoff saturates at zero when
    /// `max_age` is larger than `now_millis`.
    pub fn updated_within(prefix: impl AsRef<[u8]>, now_millis: u64, max_age: Duration) -> Self {
        Self::updated_within_millis(prefix, now_millis, duration_millis_saturating(max_age))
    }

    /// Retain roots updated within `window_millis` before `now_millis`.
    pub fn updated_within_millis(
        prefix: impl AsRef<[u8]>,
        now_millis: u64,
        window_millis: u64,
    ) -> Self {
        Self::updated_since(prefix, now_millis.saturating_sub(window_millis))
    }

    /// Retain roots updated within `days` calendar-day windows before `now_millis`.
    ///
    /// This is a convenience wrapper for retention rules such as "keep roots
    /// updated in the last 7 days". A day is treated as exactly 86,400,000
    /// milliseconds.
    pub fn updated_within_days(prefix: impl AsRef<[u8]>, now_millis: u64, days: u64) -> Self {
        Self::updated_within_millis(prefix, now_millis, days.saturating_mul(86_400_000))
    }
}

fn duration_millis_saturating(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

pub(crate) fn sort_named_root_manifests(roots: &mut [NamedRootManifest]) {
    roots.sort_by(|left, right| left.name.cmp(&right.name));
}

/// Result of a named-root compare-and-swap update.
#[derive(Clone, Debug, PartialEq)]
pub enum ManifestUpdate {
    /// The expected manifest matched and the update was applied.
    Applied,
    /// The expected manifest did not match the current manifest.
    Conflict {
        /// Current manifest stored under the requested name.
        current: Option<RootManifest>,
    },
}

impl ManifestUpdate {
    /// Whether the update was applied.
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied)
    }

    /// Whether the update failed because the current manifest differed.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }

    /// Current manifest for conflicts, or `None` for applied updates.
    pub fn current(&self) -> Option<&RootManifest> {
        match self {
            Self::Applied => None,
            Self::Conflict { current } => current.as_ref(),
        }
    }
}

/// Result of a named-root compare-and-swap through a [`crate::Prolly`] manager.
#[derive(Clone, Debug, PartialEq)]
pub enum NamedRootUpdate {
    /// The expected tree matched and the update was applied.
    Applied,
    /// The expected tree did not match the current named root.
    Conflict {
        /// Current tree stored under the requested name.
        current: Option<Tree>,
    },
}

impl NamedRootUpdate {
    /// Whether the update was applied.
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied)
    }

    /// Whether the update failed because the current tree differed.
    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }

    /// Current tree for conflicts, or `None` for applied updates.
    pub fn current(&self) -> Option<&Tree> {
        match self {
            Self::Applied => None,
            Self::Conflict { current } => current.as_ref(),
        }
    }
}

impl From<ManifestUpdate> for NamedRootUpdate {
    fn from(update: ManifestUpdate) -> Self {
        match update {
            ManifestUpdate::Applied => Self::Applied,
            ManifestUpdate::Conflict { current } => Self::Conflict {
                current: current.map(RootManifest::into_tree),
            },
        }
    }
}

/// Storage for named root manifests.
///
/// This trait is separate from [`crate::Store`] so content-addressed node stores
/// remain simple. Backends that can update a named root atomically should
/// implement compare-and-swap directly in their native transaction mechanism.
pub trait ManifestStore: Send + Sync {
    /// Error type for manifest operations.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Load a named root manifest.
    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error>;

    /// Unconditionally insert or replace a named root manifest.
    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error>;

    /// Delete a named root manifest. Deleting a missing name is not an error.
    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error>;

    /// Atomically update a named root if the current manifest matches
    /// `expected`.
    ///
    /// `expected == None` means the name must be absent. `new == None` deletes
    /// the name when the compare succeeds.
    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error>;
}

/// Manifest stores that can enumerate durable named roots.
///
/// This trait is separate from [`ManifestStore`] because point lookups and CAS
/// updates are enough for simple applications, while store-wide garbage
/// collection needs an explicit listing capability. Implementations must return
/// roots sorted by raw name bytes for deterministic retention planning.
pub trait ManifestStoreScan: ManifestStore {
    /// List all durable named root manifests.
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error>;
}

impl<T: ManifestStore> ManifestStore for Arc<T> {
    type Error = T::Error;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        (**self).get_root(name)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        (**self).put_root(name, manifest)
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        (**self).delete_root(name)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        (**self).compare_and_swap_root(name, expected, new)
    }
}

impl<T: ManifestStoreScan> ManifestStoreScan for Arc<T> {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        (**self).list_roots()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, Encoding};
    use std::time::Duration;

    #[derive(Serialize)]
    struct LegacyRootManifestWire {
        version: u64,
        root: Option<Cid>,
        config: Config,
    }

    #[test]
    fn manifest_round_trips_through_bytes() {
        let cid = Cid::from_bytes(b"root");
        let manifest = RootManifest::new(
            Some(cid),
            Config::builder()
                .min_chunk_size(2)
                .max_chunk_size(8)
                .chunking_factor(4)
                .hash_seed(99)
                .encoding(Encoding::Json)
                .node_cache_max_nodes(128)
                .build(),
        )
        .with_timestamps_millis(Some(100), Some(200));

        let bytes = manifest.to_bytes().unwrap();
        assert_eq!(RootManifest::from_bytes(&bytes).unwrap(), manifest);
    }

    #[test]
    fn manifest_reads_legacy_bytes_without_timestamps() {
        let legacy = LegacyRootManifestWire {
            version: ROOT_MANIFEST_VERSION,
            root: Some(Cid::from_bytes(b"legacy-root")),
            config: Config::default(),
        };
        let bytes = serde_cbor::ser::to_vec_packed(&legacy).unwrap();
        let manifest = RootManifest::from_bytes(&bytes).unwrap();

        assert_eq!(manifest.root, legacy.root);
        assert_eq!(manifest.config, legacy.config);
        assert_eq!(manifest.created_at_millis, None);
        assert_eq!(manifest.updated_at_millis, None);
    }

    #[test]
    fn manifest_converts_to_and_from_tree() {
        let tree = Tree {
            root: Some(Cid::from_bytes(b"root")),
            config: Config::default(),
        };

        let manifest = RootManifest::from_tree(&tree);
        assert_eq!(manifest.to_tree(), tree);
        assert_eq!(Tree::from(manifest), tree);
    }

    #[test]
    fn named_root_update_converts_conflict_to_tree() {
        let tree = Tree {
            root: Some(Cid::from_bytes(b"root")),
            config: Config::default(),
        };
        let update = ManifestUpdate::Conflict {
            current: Some(RootManifest::from_tree(&tree)),
        };

        assert_eq!(
            NamedRootUpdate::from(update),
            NamedRootUpdate::Conflict {
                current: Some(tree)
            }
        );
    }

    #[test]
    fn retention_duration_helpers_build_cutoffs() {
        assert_eq!(
            NamedRootRetention::updated_within(b"checkpoint/", 1_000, Duration::from_millis(250)),
            NamedRootRetention::updated_since(b"checkpoint/", 750)
        );
        assert_eq!(
            NamedRootRetention::updated_within_millis(b"checkpoint/", 100, 250),
            NamedRootRetention::updated_since(b"checkpoint/", 0)
        );
        assert_eq!(
            NamedRootRetention::updated_within_days(b"checkpoint/", 172_800_050, 1),
            NamedRootRetention::updated_since(b"checkpoint/", 86_400_050)
        );
        assert_eq!(
            NamedRootRetention::updated_within_days(b"checkpoint/", 42, u64::MAX),
            NamedRootRetention::updated_since(b"checkpoint/", 0)
        );
    }
}
