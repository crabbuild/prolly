use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::Instant;

use serde::{Deserialize, Serialize};

use super::super::cid::Cid;
use super::super::error::Error;
use super::super::manifest::NamedRootUpdate;
use super::super::node::Node;
use super::super::store::{MemStore, Store};
use super::super::sync::{verify_node_bytes, SnapshotBundleNode};
use super::super::tree::Tree;
use super::super::versioned_map::MapVersionId;
use super::super::Prolly;
use super::budget::TransferBudget;
use super::coordinator::{IndexedMap, IndexedVersion};
use super::publication::IndexedStore;
use super::state::{IndexDescriptor, IndexedCollectionState};

pub const INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION: u32 = 1;
const BUNDLE_MAGIC: &[u8; 8] = b"PIBNDL01";

/// One active descriptor/tree selection carried by a canonical collection bundle.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedSnapshotBundleIndex {
    pub descriptor: IndexDescriptor,
    pub tree: Tree,
}

/// Self-contained transport for one canonical indexed-collection state closure.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedSnapshotBundle {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub state_version: MapVersionId,
    pub state_tree: Tree,
    pub indexes: Vec<IndexedSnapshotBundleIndex>,
    pub nodes: Vec<SnapshotBundleNode>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedSnapshotBundleSummary {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub state_version: MapVersionId,
    pub index_count: usize,
    pub node_count: usize,
    pub byte_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedSnapshotBundleVerification {
    pub valid: bool,
    pub summary: IndexedSnapshotBundleSummary,
    pub reachable_nodes: usize,
}

#[derive(Serialize, Deserialize)]
struct BundleWire(
    u32,
    Vec<u8>,
    MapVersionId,
    MapVersionId,
    Tree,
    Vec<IndexWire>,
    Vec<NodeWire>,
);

#[derive(Serialize, Deserialize)]
struct IndexWire(Vec<u8>, Tree);

#[derive(Serialize, Deserialize)]
struct NodeWire(Vec<u8>, Vec<u8>);

impl IndexedSnapshotBundle {
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn byte_count(&self) -> usize {
        self.nodes.iter().map(|node| node.bytes.len()).sum()
    }

    pub fn digest(&self) -> Result<Cid, Error> {
        self.to_bytes().map(|bytes| Cid::from_bytes(&bytes))
    }

    pub fn summary(&self) -> Result<IndexedSnapshotBundleSummary, Error> {
        self.verify().map(|verified| verified.summary)
    }

    pub fn inspect(bytes: &[u8]) -> Result<IndexedSnapshotBundleSummary, Error> {
        Self::from_bytes(bytes)?.summary()
    }

    pub fn verify(&self) -> Result<IndexedSnapshotBundleVerification, Error> {
        self.verify_with_budget(&TransferBudget::default())
    }

    pub fn verify_with_budget(
        &self,
        budget: &TransferBudget,
    ) -> Result<IndexedSnapshotBundleVerification, Error> {
        budget.validate()?;
        let started = Instant::now();
        if self.format_version != INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION
            || self.source_map_id.is_empty()
            || MapVersionId::for_tree(&self.state_tree)? != self.state_version
        {
            return Err(invalid_bundle(
                "invalid format, source, or state-tree identity",
            ));
        }
        let mut node_map = BTreeMap::<Vec<u8>, &[u8]>::new();
        let mut decoded_bytes = 0usize;
        for node in &self.nodes {
            if node_map.len() == budget.max_nodes {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "bundle_nodes",
                    limit: budget.max_nodes,
                    actual: node_map.len().saturating_add(1),
                });
            }
            decoded_bytes = decoded_bytes.saturating_add(node.bytes.len());
            if decoded_bytes > budget.max_decoded_bytes
                || decoded_bytes > budget.max_accounted_memory_bytes
                || started.elapsed() > budget.max_elapsed
            {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "bundle_decoded_bytes",
                    limit: budget
                        .max_decoded_bytes
                        .min(budget.max_accounted_memory_bytes),
                    actual: decoded_bytes,
                });
            }
            verify_node_bytes(&node.cid, &node.bytes)
                .map_err(|error| invalid_bundle(error.to_string()))?;
            if node_map
                .insert(node.cid.as_bytes().to_vec(), node.bytes.as_slice())
                .is_some()
            {
                return Err(invalid_bundle("bundle contains a duplicate node CID"));
            }
        }

        let memory = Arc::new(MemStore::new());
        let entries = self
            .nodes
            .iter()
            .map(|node| (node.cid.as_bytes(), node.bytes.as_slice()))
            .collect::<Vec<_>>();
        memory
            .batch_put(&entries)
            .map_err(|error| invalid_bundle(error.to_string()))?;
        let reader = Prolly::new(memory, self.state_tree.config.clone());
        let state = IndexedCollectionState::from_tree(&reader, &self.state_tree)
            .map_err(|error| invalid_bundle(error.to_string()))?;
        if state.source_map_id != self.source_map_id {
            return Err(invalid_bundle("state belongs to a different source"));
        }
        let head = state
            .head_snapshot()
            .map_err(|error| invalid_bundle(error.to_string()))?;
        if MapVersionId::for_tree(&head.source.tree)? != self.source_version {
            return Err(invalid_bundle("head source version does not match bundle"));
        }

        let mut expected_indexes = Vec::with_capacity(head.indexes.len());
        for selected in &head.indexes {
            let descriptor = state
                .descriptors
                .get(&(
                    selected.name.clone(),
                    selected.descriptor_fingerprint.clone(),
                ))
                .ok_or_else(|| invalid_bundle("head index descriptor is missing"))?;
            expected_indexes.push(IndexedSnapshotBundleIndex {
                descriptor: descriptor.clone(),
                tree: selected.tree.clone(),
            });
        }
        if expected_indexes != self.indexes {
            return Err(invalid_bundle("head index metadata does not match state"));
        }

        let mut reachable = BTreeSet::new();
        let mut verification_work = 0usize;
        for tree in
            std::iter::once(&self.state_tree).chain(state.snapshots.values().flat_map(|snapshot| {
                std::iter::once(&snapshot.source.tree)
                    .chain(snapshot.indexes.iter().map(|index| &index.tree))
            }))
        {
            collect_reachable(tree, &node_map, &mut reachable)?;
            verification_work = verification_work.saturating_add(reachable.len());
            if verification_work > budget.max_verification_work {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "bundle_verification_work",
                    limit: budget.max_verification_work,
                    actual: verification_work,
                });
            }
        }
        if reachable != node_map.keys().cloned().collect() {
            return Err(invalid_bundle(
                "bundle node closure has missing or unreferenced nodes",
            ));
        }
        Ok(IndexedSnapshotBundleVerification {
            valid: true,
            summary: IndexedSnapshotBundleSummary {
                format_version: self.format_version,
                source_map_id: self.source_map_id.clone(),
                source_version: self.source_version.clone(),
                state_version: self.state_version.clone(),
                index_count: self.indexes.len(),
                node_count: self.node_count(),
                byte_count: self.byte_count(),
            },
            reachable_nodes: reachable.len(),
        })
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.verify()?;
        let mut nodes = self.nodes.iter().collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
        let payload = serde_cbor::ser::to_vec_packed(&BundleWire(
            self.format_version,
            self.source_map_id.clone(),
            self.source_version.clone(),
            self.state_version.clone(),
            self.state_tree.clone(),
            self.indexes
                .iter()
                .map(|index| {
                    Ok(IndexWire(
                        serde_cbor::to_vec(&index.descriptor)
                            .map_err(|error| Error::Serialize(error.to_string()))?,
                        index.tree.clone(),
                    ))
                })
                .collect::<Result<Vec<_>, Error>>()?,
            nodes
                .into_iter()
                .map(|node| NodeWire(node.cid.as_bytes().to_vec(), node.bytes.clone()))
                .collect(),
        ))
        .map_err(|error| Error::Serialize(error.to_string()))?;
        let mut bytes = Vec::with_capacity(12 + payload.len());
        bytes.extend_from_slice(BUNDLE_MAGIC);
        bytes.extend_from_slice(&INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION.to_be_bytes());
        bytes.extend_from_slice(&payload);
        if bytes.len() > TransferBudget::default().max_encoded_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "bundle_encoded_bytes",
                limit: TransferBudget::default().max_encoded_bytes,
                actual: bytes.len(),
            });
        }
        Ok(bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() > TransferBudget::default().max_encoded_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "bundle_encoded_bytes",
                limit: TransferBudget::default().max_encoded_bytes,
                actual: bytes.len(),
            });
        }
        if bytes.len() < 12 || &bytes[..8] != BUNDLE_MAGIC {
            return Err(invalid_bundle("invalid indexed bundle envelope"));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed header"));
        if version != INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION {
            return Err(invalid_bundle("unsupported indexed bundle format"));
        }
        let mut decoder = serde_cbor::Deserializer::from_slice(&bytes[12..]);
        let BundleWire(
            format_version,
            source_map_id,
            source_version,
            state_version,
            state_tree,
            indexes,
            nodes,
        ) = BundleWire::deserialize(&mut decoder)
            .map_err(|error| invalid_bundle(error.to_string()))?;
        decoder
            .end()
            .map_err(|error| invalid_bundle(error.to_string()))?;
        let bundle = Self {
            format_version,
            source_map_id,
            source_version,
            state_version,
            state_tree,
            indexes: indexes
                .into_iter()
                .map(|IndexWire(bytes, tree)| {
                    let mut decoder = serde_cbor::Deserializer::from_slice(&bytes);
                    let descriptor = IndexDescriptor::deserialize(&mut decoder)
                        .map_err(|error| invalid_bundle(error.to_string()))?;
                    decoder
                        .end()
                        .map_err(|error| invalid_bundle(error.to_string()))?;
                    Ok(IndexedSnapshotBundleIndex { descriptor, tree })
                })
                .collect::<Result<Vec<_>, Error>>()?,
            nodes: nodes
                .into_iter()
                .map(|NodeWire(cid, bytes)| {
                    let cid: [u8; 32] = cid
                        .try_into()
                        .map_err(|_| invalid_bundle("invalid node CID length"))?;
                    Ok(SnapshotBundleNode {
                        cid: Cid(cid),
                        bytes,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
        };
        bundle.verify()?;
        Ok(bundle)
    }
}

impl<S: IndexedStore> IndexedMap<'_, S> {
    pub fn export_current(&self) -> Result<IndexedSnapshotBundle, Error> {
        self.export_current_with_budget(&TransferBudget::default())
    }

    pub fn export_current_with_budget(
        &self,
        budget: &TransferBudget,
    ) -> Result<IndexedSnapshotBundle, Error> {
        budget.validate()?;
        let loaded = self.load_state()?;
        let head = loaded.state.head_snapshot()?;
        let mut nodes = BTreeMap::new();
        for tree in std::iter::once(&loaded.tree).chain(loaded.state.snapshots.values().flat_map(
            |snapshot| {
                std::iter::once(&snapshot.source.tree)
                    .chain(snapshot.indexes.iter().map(|index| &index.tree))
            },
        )) {
            add_tree_nodes(self.prolly, tree, &mut nodes, budget)?;
        }
        let indexes = head
            .indexes
            .iter()
            .map(|selected| {
                let descriptor = loaded
                    .state
                    .descriptors
                    .get(&(
                        selected.name.clone(),
                        selected.descriptor_fingerprint.clone(),
                    ))
                    .cloned()
                    .ok_or_else(|| invalid_bundle("head descriptor is missing"))?;
                Ok(IndexedSnapshotBundleIndex {
                    descriptor,
                    tree: selected.tree.clone(),
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let bundle = IndexedSnapshotBundle {
            format_version: INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION,
            source_map_id: self.source_map_id.clone(),
            source_version: MapVersionId::for_tree(&head.source.tree)?,
            state_version: MapVersionId::for_tree(&loaded.tree)?,
            state_tree: loaded.tree,
            indexes,
            nodes: nodes.into_values().collect(),
        };
        bundle.verify()?;
        Ok(bundle)
    }

    pub fn import_current(
        &self,
        bundle: &IndexedSnapshotBundle,
        expected_source: Option<&MapVersionId>,
    ) -> Result<IndexedVersion, Error> {
        self.import_current_with_budget(bundle, expected_source, &TransferBudget::default())
    }

    pub fn import_current_with_budget(
        &self,
        bundle: &IndexedSnapshotBundle,
        expected_source: Option<&MapVersionId>,
        budget: &TransferBudget,
    ) -> Result<IndexedVersion, Error> {
        budget.validate()?;
        bundle.verify()?;
        if bundle.source_map_id != self.source_map_id
            || bundle.state_tree.config != *self.prolly.config()
            || bundle.node_count() > budget.max_nodes
            || bundle.byte_count() > budget.max_encoded_bytes
        {
            return Err(invalid_bundle(
                "bundle ownership, configuration, or transfer budget mismatch",
            ));
        }
        for index in &bundle.indexes {
            self.runtime_definition_for_descriptor(&index.descriptor)?
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: index.descriptor.name.clone(),
                    generation: index.descriptor.generation,
                })?;
        }
        let loaded = self.load_state()?;
        let current = MapVersionId::for_tree(&loaded.state.head_snapshot()?.source.tree)?;
        let logically_absent =
            loaded.state.head_snapshot()?.source.entry_count == 0 && loaded.state.active.is_empty();
        if expected_source.map_or(!logically_absent, |expected| expected != &current) {
            return Err(Error::InvalidVersionedMap(
                "indexed bundle import source expectation conflict".to_string(),
            ));
        }
        let entries = bundle
            .nodes
            .iter()
            .map(|node| (node.cid.as_bytes(), node.bytes.as_slice()))
            .collect::<Vec<_>>();
        self.prolly
            .store()
            .batch_put(&entries)
            .map_err(|error| Error::Store(Box::new(error)))?;
        self.prolly
            .store()
            .confirm_indexed_publication(&[&bundle.state_tree])?;
        match self.prolly.compare_and_swap_named_root(
            &super::state::indexed_collection_root_name(&self.source_map_id)?,
            Some(&loaded.tree),
            Some(&bundle.state_tree),
        )? {
            NamedRootUpdate::Applied => {
                let imported = self.load_state()?;
                self.current_version(&imported)
            }
            NamedRootUpdate::Conflict { .. } => Err(Error::InvalidVersionedMap(
                "indexed bundle import CAS conflict".to_string(),
            )),
        }
    }
}

fn add_tree_nodes<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    nodes: &mut BTreeMap<Vec<u8>, SnapshotBundleNode>,
    budget: &TransferBudget,
) -> Result<(), Error> {
    for node in prolly.export_snapshot(tree)?.nodes {
        nodes.entry(node.cid.as_bytes().to_vec()).or_insert(node);
        if nodes.len() > budget.max_nodes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "bundle_nodes",
                limit: budget.max_nodes,
                actual: nodes.len(),
            });
        }
        let bytes = nodes.values().map(|node| node.bytes.len()).sum::<usize>();
        if bytes > budget.max_encoded_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "bundle_bytes",
                limit: budget.max_encoded_bytes,
                actual: bytes,
            });
        }
    }
    Ok(())
}

fn collect_reachable(
    tree: &Tree,
    nodes: &BTreeMap<Vec<u8>, &[u8]>,
    reachable: &mut BTreeSet<Vec<u8>>,
) -> Result<(), Error> {
    let mut queue = VecDeque::new();
    if let Some(root) = &tree.root {
        queue.push_back(root.as_bytes().to_vec());
    }
    while let Some(cid) = queue.pop_front() {
        if !reachable.insert(cid.clone()) {
            continue;
        }
        let bytes = nodes
            .get(&cid)
            .ok_or_else(|| invalid_bundle("bundle is missing a reachable node"))?;
        let node = Node::from_bytes(bytes).map_err(|error| invalid_bundle(error.to_string()))?;
        if !node.leaf {
            for child in node.vals {
                let child: [u8; 32] = child
                    .try_into()
                    .map_err(|_| invalid_bundle("internal node has an invalid child CID"))?;
                queue.push_back(child.to_vec());
            }
        }
    }
    Ok(())
}

fn invalid_bundle(reason: impl Into<String>) -> Error {
    Error::InvalidIndexedSnapshotBundle {
        reason: reason.into(),
    }
}
