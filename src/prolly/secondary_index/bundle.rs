use super::super::cid::Cid;
use super::super::error::Error;
use super::super::manifest::ManifestStore;
use super::super::node::Node;
use super::super::store::{MemStore, Store};
use super::super::sync::{verify_node_bytes, SnapshotBundleNode};
use super::super::transaction::{TransactionConflict, TransactionalStore};
use super::super::tree::Tree;
use super::super::versioned_map::{IndexMaintenancePermit, MapVersionId};
use super::super::Prolly;
use super::coordinator::{require_non_conflict, IndexedMap, IndexedVersion};
use super::storage::{
    catalog_checkpoint_key, catalog_current_key, catalog_descriptor_key, catalog_map_id,
    control_record_key, control_root_name, ActiveIndexControl, IndexCheckpoint, IndexControl,
    IndexedHeadRecord, SecondaryIndexDescriptor,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

pub const INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION: u32 = 1;
const BUNDLE_MAGIC: &[u8; 8] = b"PIBNDL01";

/// One descriptor/checkpoint/tree selection carried by an indexed bundle.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedSnapshotBundleIndex {
    pub descriptor: SecondaryIndexDescriptor,
    pub checkpoint: IndexCheckpoint,
    pub tree: Tree,
}

/// Self-contained, canonical transport for one current coordinated snapshot.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedSnapshotBundle {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub catalog_map_id: Vec<u8>,
    pub catalog_version: MapVersionId,
    pub source_tree: Tree,
    pub catalog_tree: Tree,
    pub control: Option<IndexControl>,
    pub indexes: Vec<IndexedSnapshotBundleIndex>,
    pub nodes: Vec<SnapshotBundleNode>,
}

/// Compact validated metadata for an indexed snapshot bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedSnapshotBundleSummary {
    pub format_version: u32,
    pub source_map_id: Vec<u8>,
    pub source_version: MapVersionId,
    pub catalog_version: MapVersionId,
    pub index_count: usize,
    pub node_count: usize,
    pub byte_count: usize,
}

/// Complete reachability result for a verified indexed snapshot bundle.
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
    Vec<u8>,
    MapVersionId,
    Tree,
    Tree,
    Option<Vec<u8>>,
    Vec<IndexWire>,
    Vec<NodeWire>,
);

#[derive(Serialize, Deserialize)]
struct IndexWire(Vec<u8>, Vec<u8>, Tree);

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
        self.verify().map(|verification| verification.summary)
    }

    /// Decode, fully verify, and return only compact bundle metadata.
    pub fn inspect(bytes: &[u8]) -> Result<IndexedSnapshotBundleSummary, Error> {
        Self::from_bytes(bytes)?.summary()
    }

    /// Verify hashes, exact reachability, ownership, persisted records, and tree IDs.
    pub fn verify(&self) -> Result<IndexedSnapshotBundleVerification, Error> {
        if self.format_version != INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION {
            return Err(invalid_bundle(format!(
                "unsupported indexed bundle format {}",
                self.format_version
            )));
        }
        if self.source_map_id.is_empty()
            || self.catalog_map_id != catalog_map_id(&self.source_map_id)
        {
            return Err(invalid_bundle("invalid source or catalog ownership"));
        }
        if MapVersionId::for_tree(&self.source_tree)? != self.source_version
            || MapVersionId::for_tree(&self.catalog_tree)? != self.catalog_version
        {
            return Err(invalid_bundle("source or catalog tree version mismatch"));
        }
        if self.source_tree.config != self.catalog_tree.config
            || self
                .indexes
                .iter()
                .any(|index| index.tree.config != self.source_tree.config)
        {
            return Err(invalid_bundle("bundle tree configurations disagree"));
        }

        let mut previous_name: Option<&[u8]> = None;
        let mut expected_control = Vec::with_capacity(self.indexes.len());
        let mut checkpoints = Vec::with_capacity(self.indexes.len());
        for index in &self.indexes {
            if previous_name.is_some_and(|previous| previous >= index.descriptor.name.as_slice()) {
                return Err(invalid_bundle(
                    "bundle indexes must be strictly sorted by name",
                ));
            }
            previous_name = Some(&index.descriptor.name);
            index.descriptor.validate()?;
            let checkpoint = &index.checkpoint;
            if checkpoint.source_map_id != self.source_map_id
                || checkpoint.source_version != self.source_version
                || checkpoint.index_name != index.descriptor.name
                || checkpoint.generation != index.descriptor.generation
                || checkpoint.definition_fingerprint != index.descriptor.fingerprint
                || MapVersionId::for_tree(&index.tree)? != checkpoint.index_version
            {
                return Err(invalid_bundle(
                    "descriptor, checkpoint, source, or index tree mismatch",
                ));
            }
            expected_control.push(ActiveIndexControl {
                name: checkpoint.index_name.clone(),
                fingerprint: checkpoint.definition_fingerprint.clone(),
            });
            checkpoints.push(checkpoint.clone());
        }
        match (&self.control, self.indexes.is_empty()) {
            (None, true) => {}
            (Some(control), false)
                if control.source_map_id == self.source_map_id
                    && control.catalog_map_id == self.catalog_map_id
                    && control.active == expected_control => {}
            _ => {
                return Err(invalid_bundle(
                    "control state does not match active indexes",
                ))
            }
        }

        let mut node_map = BTreeMap::<Vec<u8>, &[u8]>::new();
        for node in &self.nodes {
            verify_node_bytes(&node.cid, &node.bytes)
                .map_err(|error| invalid_bundle(error.to_string()))?;
            if node_map
                .insert(node.cid.as_bytes().to_vec(), &node.bytes)
                .is_some()
            {
                return Err(invalid_bundle("bundle contains duplicate node CIDs"));
            }
        }
        let mut reachable = BTreeSet::<Vec<u8>>::new();
        for tree in std::iter::once(&self.source_tree)
            .chain(std::iter::once(&self.catalog_tree))
            .chain(self.indexes.iter().map(|index| &index.tree))
        {
            collect_reachable(tree, &node_map, &mut reachable)?;
        }
        let provided = node_map.keys().cloned().collect::<BTreeSet<_>>();
        if reachable != provided {
            let missing = reachable.difference(&provided).count();
            let extra = provided.difference(&reachable).count();
            return Err(invalid_bundle(format!(
                "bundle node closure mismatch: {missing} missing, {extra} extra"
            )));
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
        let reader = Prolly::new(memory, self.source_tree.config.clone());
        validate_catalog_tree(&reader, self, &checkpoints)?;

        let summary = IndexedSnapshotBundleSummary {
            format_version: self.format_version,
            source_map_id: self.source_map_id.clone(),
            source_version: self.source_version.clone(),
            catalog_version: self.catalog_version.clone(),
            index_count: self.indexes.len(),
            node_count: self.nodes.len(),
            byte_count: self.byte_count(),
        };
        Ok(IndexedSnapshotBundleVerification {
            valid: true,
            summary,
            reachable_nodes: reachable.len(),
        })
    }

    /// Encode deterministic bytes with fixed field positions and canonical ordering.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.verify()?;
        let mut nodes = self.nodes.iter().collect::<Vec<_>>();
        nodes.sort_by(|left, right| left.cid.as_bytes().cmp(right.cid.as_bytes()));
        let payload = serde_cbor::ser::to_vec_packed(&BundleWire(
            self.format_version,
            self.source_map_id.clone(),
            self.source_version.clone(),
            self.catalog_map_id.clone(),
            self.catalog_version.clone(),
            self.source_tree.clone(),
            self.catalog_tree.clone(),
            self.control
                .as_ref()
                .map(IndexControl::to_bytes)
                .transpose()?,
            self.indexes
                .iter()
                .map(|index| {
                    Ok(IndexWire(
                        index.descriptor.to_bytes()?,
                        index.checkpoint.to_bytes()?,
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
        Ok(bytes)
    }

    /// Decode canonical bytes and reject unsupported versions or trailing data.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 12 || &bytes[..8] != BUNDLE_MAGIC {
            return Err(invalid_bundle("invalid indexed bundle envelope"));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed bundle header"));
        if version != INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION {
            return Err(invalid_bundle(format!(
                "unsupported indexed bundle bytes version {version}"
            )));
        }
        let mut deserializer = serde_cbor::Deserializer::from_slice(&bytes[12..]);
        let BundleWire(
            format_version,
            source_map_id,
            source_version,
            catalog_map_id,
            catalog_version,
            source_tree,
            catalog_tree,
            control,
            indexes,
            nodes,
        ) = BundleWire::deserialize(&mut deserializer)
            .map_err(|error| invalid_bundle(error.to_string()))?;
        deserializer
            .end()
            .map_err(|error| invalid_bundle(format!("trailing bundle bytes: {error}")))?;
        let bundle = Self {
            format_version,
            source_map_id,
            source_version,
            catalog_map_id,
            catalog_version,
            source_tree,
            catalog_tree,
            control: control
                .map(|bytes| IndexControl::from_bytes(&bytes))
                .transpose()?,
            indexes: indexes
                .into_iter()
                .map(|IndexWire(descriptor, checkpoint, tree)| {
                    Ok(IndexedSnapshotBundleIndex {
                        descriptor: SecondaryIndexDescriptor::from_bytes(&descriptor)?,
                        checkpoint: IndexCheckpoint::from_bytes(&checkpoint)?,
                        tree,
                    })
                })
                .collect::<Result<Vec<_>, Error>>()?,
            nodes: nodes
                .into_iter()
                .map(|NodeWire(cid, bytes)| {
                    let cid: [u8; 32] = cid
                        .try_into()
                        .map_err(|_| invalid_bundle("bundle node CID is not 32 bytes"))?;
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

impl<S> IndexedMap<'_, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Export the exact catalog-selected current source and all active index trees.
    pub fn export_current(&self) -> Result<IndexedSnapshotBundle, Error> {
        let snapshot = self.snapshot()?;
        let mut indexes = Vec::new();
        let mut nodes = BTreeMap::<Vec<u8>, SnapshotBundleNode>::new();
        add_tree_nodes(self.prolly, snapshot.source().tree(), &mut nodes)?;
        add_tree_nodes(self.prolly, snapshot.catalog().tree(), &mut nodes)?;
        for index in snapshot.indexes() {
            add_tree_nodes(self.prolly, index.tree(), &mut nodes)?;
            indexes.push(IndexedSnapshotBundleIndex {
                descriptor: index.descriptor().clone(),
                checkpoint: index.checkpoint().clone(),
                tree: index.tree().clone(),
            });
        }
        let bundle = IndexedSnapshotBundle {
            format_version: INDEXED_SNAPSHOT_BUNDLE_FORMAT_VERSION,
            source_map_id: self.source_map_id.clone(),
            source_version: snapshot.id().source_version.clone(),
            catalog_map_id: catalog_map_id(&self.source_map_id),
            catalog_version: snapshot.id().catalog_version.clone(),
            source_tree: snapshot.source().tree().clone(),
            catalog_tree: snapshot.catalog().tree().clone(),
            control: self.load_control()?,
            indexes,
            nodes: nodes.into_values().collect(),
        };
        enforce_bundle_limits(self, &bundle)?;
        bundle.verify()?;
        Ok(bundle)
    }

    /// Verify and atomically install a current indexed snapshot for this exact source ID.
    ///
    /// `expected_source == None` means the destination source must be absent.
    pub fn import_current(
        &self,
        bundle: &IndexedSnapshotBundle,
        expected_source: Option<&MapVersionId>,
    ) -> Result<IndexedVersion, Error> {
        bundle.verify()?;
        if bundle.source_map_id != self.source_map_id {
            return Err(invalid_bundle(
                "bundle belongs to a different source map ID",
            ));
        }
        if bundle.source_tree.config != *self.prolly.config() {
            return Err(invalid_bundle(
                "bundle tree configuration does not match destination engine",
            ));
        }
        for index in &bundle.indexes {
            let runtime = self
                .runtime_definition_for_descriptor(&index.descriptor)?
                .ok_or_else(|| {
                    if let Some(runtime) = self.runtime_definition(&index.descriptor.name) {
                        let runtime_descriptor =
                            SecondaryIndexDescriptor::from_runtime(&self.source_map_id, &runtime)
                                .expect("validated runtime descriptor");
                        Error::IndexDefinitionMismatch {
                            name: index.descriptor.name.clone(),
                            persisted: index.descriptor.fingerprint.clone(),
                            runtime: runtime_descriptor.fingerprint,
                        }
                    } else {
                        Error::IndexRuntimeDefinitionMissing {
                            name: index.descriptor.name.clone(),
                            generation: index.descriptor.generation,
                        }
                    }
                })?;
            let _ = runtime;
        }
        enforce_bundle_limits(self, bundle)?;

        let current_source = self.source().head()?;
        if current_source.as_ref().map(|version| &version.id) != expected_source {
            return Err(Error::transaction_conflict(TransactionConflict::new(
                self.source().head_name().to_vec(),
                None,
                None,
            )));
        }
        let catalog_id = catalog_map_id(&self.source_map_id);
        let catalog_head = self.prolly.versioned_map(&catalog_id).head()?;
        let hidden_heads = bundle
            .indexes
            .iter()
            .map(|index| {
                self.prolly
                    .versioned_map(&index.checkpoint.index_map_id)
                    .head()
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let permit_fingerprint = self
            .load_control()?
            .map(|control| control.fingerprint())
            .transpose()?
            .or_else(|| {
                bundle
                    .control
                    .as_ref()
                    .and_then(|control| control.fingerprint().ok())
            })
            .unwrap_or_else(|| Cid::from_bytes(b"inactive-indexed-import"));
        let node_entries = bundle
            .nodes
            .iter()
            .map(|node| (node.cid.as_bytes(), node.bytes.as_slice()))
            .collect::<Vec<_>>();
        let (source, catalog, _) = self.prolly.versioned_maps_transaction(|maps| {
            maps.stage_index_nodes(&node_entries)?;
            let source_permit =
                IndexMaintenancePermit::new(self.source_map_id.clone(), permit_fingerprint.clone());
            let source_update = maps.publish_tree_index_maintenance(
                &source_permit,
                expected_source,
                &bundle.source_tree,
            )?;
            let source = require_non_conflict(source_update, self.source().head_name())?;
            let mut published_indexes = Vec::with_capacity(bundle.indexes.len());
            for (index, existing) in bundle.indexes.iter().zip(&hidden_heads) {
                let permit = IndexMaintenancePermit::new(
                    index.checkpoint.index_map_id.clone(),
                    permit_fingerprint.clone(),
                );
                let update = maps.publish_tree_index_maintenance(
                    &permit,
                    existing.as_ref().map(|version| &version.id),
                    &index.tree,
                )?;
                published_indexes.push(require_non_conflict(
                    update,
                    &index.checkpoint.index_map_id,
                )?);
            }
            let catalog_permit =
                IndexMaintenancePermit::new(catalog_id.clone(), permit_fingerprint.clone());
            let catalog_update = maps.publish_tree_index_maintenance(
                &catalog_permit,
                catalog_head.as_ref().map(|version| &version.id),
                &bundle.catalog_tree,
            )?;
            let catalog = require_non_conflict(catalog_update, &catalog_id)?;
            match &bundle.control {
                Some(control) => {
                    let tree = maps.raw_transaction().put(
                        &maps.raw_transaction().create(),
                        control_record_key(),
                        control.to_bytes()?,
                    )?;
                    maps.raw_transaction()
                        .publish_named_root(&control_root_name(&self.source_map_id), &tree)?;
                }
                None => maps
                    .raw_transaction()
                    .delete_named_root(&control_root_name(&self.source_map_id))?,
            }
            Ok((source, catalog, published_indexes))
        })?;
        Ok(IndexedVersion {
            source,
            catalog: Some(catalog),
            indexes: bundle
                .indexes
                .iter()
                .map(|index| index.checkpoint.clone())
                .collect(),
        })
    }
}

fn add_tree_nodes<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    nodes: &mut BTreeMap<Vec<u8>, SnapshotBundleNode>,
) -> Result<(), Error> {
    for node in prolly.export_snapshot(tree)?.nodes {
        match nodes.get(node.cid.as_bytes()) {
            Some(existing) if existing.bytes != node.bytes => {
                return Err(invalid_bundle("same CID has conflicting node bytes"));
            }
            Some(_) => {}
            None => {
                nodes.insert(node.cid.as_bytes().to_vec(), node);
            }
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

fn validate_catalog_tree<S: Store>(
    reader: &Prolly<S>,
    bundle: &IndexedSnapshotBundle,
    checkpoints: &[IndexCheckpoint],
) -> Result<(), Error> {
    // Use direct immutable reads: no named roots are installed in this verifier.
    let format = reader
        .get(&bundle.catalog_tree, &super::storage::catalog_format_key())?
        .ok_or_else(|| invalid_bundle("catalog format record is missing"))?;
    if format != super::storage::SECONDARY_INDEX_FORMAT_VERSION.to_be_bytes() {
        return Err(invalid_bundle("catalog format record is unsupported"));
    }
    let expected_current = IndexedHeadRecord {
        source_version: bundle.source_version.clone(),
        indexes: checkpoints.to_vec(),
    }
    .to_bytes()?;
    if reader.get(&bundle.catalog_tree, &catalog_current_key())? != Some(expected_current) {
        return Err(invalid_bundle(
            "catalog current record does not match bundle",
        ));
    }
    for index in &bundle.indexes {
        if reader.get(
            &bundle.catalog_tree,
            &catalog_descriptor_key(&index.descriptor.name, index.descriptor.generation),
        )? != Some(index.descriptor.to_bytes()?)
        {
            return Err(invalid_bundle("catalog descriptor record mismatch"));
        }
        if reader.get(
            &bundle.catalog_tree,
            &catalog_checkpoint_key(
                &bundle.source_version,
                &index.checkpoint.index_name,
                index.checkpoint.generation,
            ),
        )? != Some(index.checkpoint.to_bytes()?)
        {
            return Err(invalid_bundle("catalog checkpoint record mismatch"));
        }
    }
    Ok(())
}

fn enforce_bundle_limits<S>(
    indexed: &IndexedMap<'_, S>,
    bundle: &IndexedSnapshotBundle,
) -> Result<(), Error>
where
    S: Store + ManifestStore + TransactionalStore,
{
    let mut max_nodes = usize::MAX;
    let mut max_bytes = usize::MAX;
    for index in &bundle.indexes {
        let definition = indexed
            .runtime_definition_for_descriptor(&index.descriptor)?
            .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                name: index.descriptor.name.clone(),
                generation: index.descriptor.generation,
            })?;
        max_nodes = max_nodes.min(definition.limits().max_bundle_nodes);
        max_bytes = max_bytes.min(definition.limits().max_bundle_bytes);
    }
    if bundle.node_count() > max_nodes {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "bundle_nodes",
            limit: max_nodes,
            actual: bundle.node_count(),
        });
    }
    if bundle.byte_count() > max_bytes {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "bundle_bytes",
            limit: max_bytes,
            actual: bundle.byte_count(),
        });
    }
    Ok(())
}

fn invalid_bundle(reason: impl Into<String>) -> Error {
    Error::InvalidIndexedSnapshotBundle {
        reason: reason.into(),
    }
}
