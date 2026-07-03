//! Snapshot and branch-style helpers built on named roots.
//!
//! Named roots are intentionally byte-oriented and low level. This module adds a
//! small convention layer for common embedded-library workflows: branch heads,
//! tags, checkpoints, and custom root namespaces. The helpers do not add new
//! storage semantics; they produce deterministic root names and delegate all
//! durability and compare-and-swap behavior to the manifest store.

use super::error::Error;
use super::manifest::{
    ManifestStore, ManifestStoreScan, NamedRootSelection, NamedRootUpdate, RootManifest,
};
use super::store::Store;
use super::tree::Tree;
use super::Prolly;

/// Root-name prefix used for branch snapshots.
pub const SNAPSHOT_BRANCH_PREFIX: &[u8] = b"refs/heads/";
/// Root-name prefix used for immutable release/tag snapshots.
pub const SNAPSHOT_TAG_PREFIX: &[u8] = b"refs/tags/";
/// Root-name prefix used for checkpoint snapshots.
pub const SNAPSHOT_CHECKPOINT_PREFIX: &[u8] = b"refs/checkpoints/";

/// Namespace used to derive stable named-root keys for snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotNamespace {
    /// Mutable branch-style snapshots, stored under `refs/heads/`.
    Branch,
    /// Tag-style snapshots, stored under `refs/tags/`.
    Tag,
    /// Checkpoint snapshots, stored under `refs/checkpoints/`.
    Checkpoint,
    /// Application-defined root namespace.
    Custom(Vec<u8>),
}

impl SnapshotNamespace {
    /// Branch snapshot namespace.
    pub fn branch() -> Self {
        Self::Branch
    }

    /// Tag snapshot namespace.
    pub fn tag() -> Self {
        Self::Tag
    }

    /// Checkpoint snapshot namespace.
    pub fn checkpoint() -> Self {
        Self::Checkpoint
    }

    /// Custom snapshot namespace.
    pub fn custom(prefix: impl Into<Vec<u8>>) -> Self {
        Self::Custom(prefix.into())
    }

    /// Return the raw named-root prefix for this namespace.
    pub fn prefix(&self) -> &[u8] {
        match self {
            Self::Branch => SNAPSHOT_BRANCH_PREFIX,
            Self::Tag => SNAPSHOT_TAG_PREFIX,
            Self::Checkpoint => SNAPSHOT_CHECKPOINT_PREFIX,
            Self::Custom(prefix) => prefix,
        }
    }

    /// Build the durable named-root key for `id` in this namespace.
    pub fn root_name(&self, id: impl AsRef<[u8]>) -> Vec<u8> {
        snapshot_root_name(self, id)
    }

    /// Return the snapshot id if `name` belongs to this namespace.
    pub fn id_from_name(&self, name: impl AsRef<[u8]>) -> Option<Vec<u8>> {
        snapshot_id_from_name(self, name)
    }
}

/// Build the durable named-root key for `id` in `namespace`.
pub fn snapshot_root_name(namespace: &SnapshotNamespace, id: impl AsRef<[u8]>) -> Vec<u8> {
    let prefix = namespace.prefix();
    let id = id.as_ref();
    let mut name = Vec::with_capacity(prefix.len() + id.len());
    name.extend_from_slice(prefix);
    name.extend_from_slice(id);
    name
}

/// Return the snapshot id if `name` belongs to `namespace`.
pub fn snapshot_id_from_name(
    namespace: &SnapshotNamespace,
    name: impl AsRef<[u8]>,
) -> Option<Vec<u8>> {
    let prefix = namespace.prefix();
    let name = name.as_ref();
    name.strip_prefix(prefix).map(<[u8]>::to_vec)
}

/// A snapshot root with namespace-local id and manifest metadata.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotRoot {
    /// Namespace-local snapshot id.
    pub id: Vec<u8>,
    /// Full durable named-root key.
    pub name: Vec<u8>,
    /// Tree stored under this snapshot.
    pub tree: Tree,
    /// Optional creation timestamp from the root manifest.
    pub created_at_millis: Option<u64>,
    /// Optional update timestamp from the root manifest.
    pub updated_at_millis: Option<u64>,
}

impl SnapshotRoot {
    fn from_manifest(
        namespace: &SnapshotNamespace,
        name: Vec<u8>,
        manifest: RootManifest,
    ) -> Option<Self> {
        let id = snapshot_id_from_name(namespace, &name)?;
        let created_at_millis = manifest.created_at_millis;
        let updated_at_millis = manifest.updated_at_millis;
        Some(Self {
            id,
            name,
            tree: manifest.into_tree(),
            created_at_millis,
            updated_at_millis,
        })
    }
}

/// Result of loading several snapshots by namespace-local id.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SnapshotSelection {
    /// Snapshots that were present.
    pub snapshots: Vec<SnapshotRoot>,
    /// Namespace-local ids that were requested but absent.
    pub missing_ids: Vec<Vec<u8>>,
}

/// Convenience API for branch, tag, checkpoint, or custom snapshot roots.
pub struct SnapshotManager<'a, S: Store> {
    prolly: &'a Prolly<S>,
    namespace: SnapshotNamespace,
}

impl<'a, S: Store> SnapshotManager<'a, S> {
    /// Create a manager for `namespace` using an existing [`Prolly`] manager.
    pub fn new(prolly: &'a Prolly<S>, namespace: SnapshotNamespace) -> Self {
        Self { prolly, namespace }
    }

    /// Return the namespace used by this manager.
    pub fn namespace(&self) -> &SnapshotNamespace {
        &self.namespace
    }

    /// Build a durable named-root key from a namespace-local id.
    pub fn root_name(&self, id: impl AsRef<[u8]>) -> Vec<u8> {
        self.namespace.root_name(id)
    }

    /// Load one snapshot by namespace-local id.
    pub fn load(&self, id: impl AsRef<[u8]>) -> Result<Option<Tree>, Error>
    where
        S: ManifestStore,
    {
        self.prolly.load_named_root(&self.root_name(id))
    }

    /// Load several snapshots by namespace-local id.
    pub fn load_many<I, Id>(&self, ids: I) -> Result<SnapshotSelection, Error>
    where
        S: ManifestStore,
        I: IntoIterator<Item = Id>,
        Id: AsRef<[u8]>,
    {
        let ids = ids
            .into_iter()
            .map(|id| id.as_ref().to_vec())
            .collect::<Vec<_>>();
        let names = ids.iter().map(|id| self.root_name(id)).collect::<Vec<_>>();
        let selection = self.prolly.load_named_roots(names)?;
        Ok(self.selection_from_named_roots(selection))
    }

    /// Publish or replace a snapshot.
    pub fn publish(&self, id: impl AsRef<[u8]>, tree: &Tree) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        self.prolly.publish_named_root(&self.root_name(id), tree)
    }

    /// Publish or replace a snapshot with an explicit timestamp.
    pub fn publish_at_millis(
        &self,
        id: impl AsRef<[u8]>,
        tree: &Tree,
        timestamp_millis: u64,
    ) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        self.prolly
            .publish_named_root_at_millis(&self.root_name(id), tree, timestamp_millis)
    }

    /// Delete a snapshot. Deleting a missing snapshot is not an error.
    pub fn delete(&self, id: impl AsRef<[u8]>) -> Result<(), Error>
    where
        S: ManifestStore,
    {
        self.prolly.delete_named_root(&self.root_name(id))
    }

    /// Atomically update a snapshot when the current tree matches `expected`.
    pub fn compare_and_swap(
        &self,
        id: impl AsRef<[u8]>,
        expected: Option<&Tree>,
        replacement: Option<&Tree>,
    ) -> Result<NamedRootUpdate, Error>
    where
        S: ManifestStore,
    {
        self.prolly
            .compare_and_swap_named_root(&self.root_name(id), expected, replacement)
    }

    /// Atomically update a snapshot with an explicit timestamp.
    pub fn compare_and_swap_at_millis(
        &self,
        id: impl AsRef<[u8]>,
        expected: Option<&Tree>,
        replacement: Option<&Tree>,
        timestamp_millis: u64,
    ) -> Result<NamedRootUpdate, Error>
    where
        S: ManifestStore,
    {
        self.prolly.compare_and_swap_named_root_at_millis(
            &self.root_name(id),
            expected,
            replacement,
            timestamp_millis,
        )
    }

    /// List all snapshots in this namespace.
    pub fn list(&self) -> Result<Vec<SnapshotRoot>, Error>
    where
        S: ManifestStoreScan,
    {
        let mut snapshots = self
            .prolly
            .list_named_root_manifests()?
            .into_iter()
            .filter_map(|root| {
                SnapshotRoot::from_manifest(&self.namespace, root.name, root.manifest)
            })
            .collect::<Vec<_>>();
        snapshots.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(snapshots)
    }

    fn selection_from_named_roots(&self, selection: NamedRootSelection) -> SnapshotSelection {
        let snapshots = selection
            .roots
            .into_iter()
            .filter_map(|root| {
                let id = self.namespace.id_from_name(&root.name)?;
                Some(SnapshotRoot {
                    id,
                    name: root.name,
                    tree: root.tree,
                    created_at_millis: None,
                    updated_at_millis: None,
                })
            })
            .collect::<Vec<_>>();
        let missing_ids = selection
            .missing_names
            .into_iter()
            .filter_map(|name| self.namespace.id_from_name(name))
            .collect::<Vec<_>>();
        SnapshotSelection {
            snapshots,
            missing_ids,
        }
    }
}

impl<S: Store> Prolly<S> {
    /// Return a snapshot manager for the provided namespace.
    pub fn snapshots(&self, namespace: SnapshotNamespace) -> SnapshotManager<'_, S> {
        SnapshotManager::new(self, namespace)
    }

    /// Return a branch snapshot manager using `refs/heads/` names.
    pub fn branch_snapshots(&self) -> SnapshotManager<'_, S> {
        self.snapshots(SnapshotNamespace::Branch)
    }

    /// Return a tag snapshot manager using `refs/tags/` names.
    pub fn tag_snapshots(&self) -> SnapshotManager<'_, S> {
        self.snapshots(SnapshotNamespace::Tag)
    }

    /// Return a checkpoint snapshot manager using `refs/checkpoints/` names.
    pub fn checkpoint_snapshots(&self) -> SnapshotManager<'_, S> {
        self.snapshots(SnapshotNamespace::Checkpoint)
    }
}
