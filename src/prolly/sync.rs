//! Store-to-store synchronization helpers for content-addressed tree nodes.

use super::cid::Cid;
use super::error::Error;
use super::node::Node;
use super::tree::Tree;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};

/// Current in-memory format version for portable tree snapshot bundles.
pub const SNAPSHOT_BUNDLE_FORMAT_VERSION: u32 = 1;

const SNAPSHOT_BUNDLE_BYTES_VERSION: u32 = SNAPSHOT_BUNDLE_FORMAT_VERSION;

/// Dry-run plan for making a destination store able to read one source tree.
///
/// `required_cids` are all node CIDs reachable from the tree root. `missing_cids`
/// are the subset not currently present in the destination store. All CIDs are
/// sorted by raw CID bytes for deterministic transport planning.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MissingNodePlan {
    /// All reachable node CIDs required by the tree.
    pub required_cids: Vec<Cid>,
    /// Number of reachable nodes required by the tree.
    pub required_nodes: usize,
    /// Serialized bytes for all reachable nodes in the source tree.
    pub required_bytes: usize,
    /// Required node CIDs not present in the destination store.
    pub missing_cids: Vec<Cid>,
    /// Number of missing destination nodes.
    pub missing_nodes: usize,
    /// Serialized bytes for missing nodes as read from the source store.
    pub missing_bytes: usize,
}

impl MissingNodePlan {
    /// Whether the destination already has every required node.
    pub fn is_empty(&self) -> bool {
        self.missing_cids.is_empty()
    }

    /// Return all required CIDs in deterministic byte order.
    pub fn required_cids(&self) -> &[Cid] {
        &self.required_cids
    }

    /// Return missing CIDs in deterministic byte order.
    pub fn missing_cids(&self) -> &[Cid] {
        &self.missing_cids
    }
}

/// Result of copying missing nodes from one store to another.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MissingNodeCopy {
    /// Dry-run plan used for this copy.
    pub plan: MissingNodePlan,
    /// Number of nodes written to the destination store.
    pub copied_nodes: usize,
    /// Serialized bytes written to the destination store.
    pub copied_bytes: usize,
}

/// One content-addressed node included in a portable tree snapshot bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBundleNode {
    /// CID bytes the node is stored under.
    pub cid: Cid,
    /// Serialized node bytes whose SHA-256 CID must equal `cid`.
    pub bytes: Vec<u8>,
}

/// Self-contained transport bundle for one tree and its reachable node bytes.
///
/// The bundle is intended for import/export between stores, processes, and
/// language bindings. `nodes` should contain exactly the node CIDs reachable
/// from `tree.root`, sorted by raw CID bytes for deterministic transport.
#[derive(Clone, Debug, PartialEq)]
pub struct SnapshotBundle {
    /// Bundle schema version. Currently always `1`.
    pub format_version: u32,
    /// Tree handle the imported store will be able to read.
    pub tree: Tree,
    /// Reachable serialized nodes for the tree.
    pub nodes: Vec<SnapshotBundleNode>,
}

/// Compact metadata for a validated snapshot bundle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBundleSummary {
    /// Bundle schema version.
    pub format_version: u32,
    /// Tree root CID, or `None` for an empty tree.
    pub root: Option<Cid>,
    /// Number of unique node CIDs included in the bundle.
    pub node_count: usize,
    /// Total serialized bytes across unique bundled nodes.
    pub byte_count: usize,
    /// Smallest serialized node payload in the bundle.
    pub min_node_bytes: usize,
    /// Largest serialized node payload in the bundle.
    pub max_node_bytes: usize,
}

/// Result of verifying a snapshot bundle as a self-contained tree.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotBundleVerification {
    /// True when the bundle has exactly the reachable node set for its tree.
    pub valid: bool,
    /// Validated bundle metadata.
    pub summary: SnapshotBundleSummary,
    /// Number of reachable CIDs discovered from the tree root.
    pub reachable_nodes: usize,
    /// Serialized bytes for reachable bundled nodes.
    pub reachable_bytes: usize,
    /// Reachable CIDs absent from the bundle.
    pub missing_cids: Vec<Cid>,
    /// Bundled CIDs not reachable from the tree root.
    pub extra_cids: Vec<Cid>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotBundleWire {
    version: u32,
    tree: Tree,
    nodes: Vec<SnapshotBundleNodeWire>,
}

#[derive(Serialize, Deserialize)]
struct SnapshotBundleNodeWire {
    cid: Vec<u8>,
    bytes: Vec<u8>,
}

impl SnapshotBundle {
    /// Create a versioned snapshot bundle from a tree and reachable node bytes.
    pub fn new(tree: Tree, nodes: Vec<SnapshotBundleNode>) -> Self {
        Self {
            format_version: SNAPSHOT_BUNDLE_FORMAT_VERSION,
            tree,
            nodes,
        }
    }

    /// Number of serialized nodes in the bundle.
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Total serialized node bytes in the bundle.
    pub fn byte_count(&self) -> usize {
        self.nodes.iter().map(|node| node.bytes.len()).sum()
    }

    /// Return the SHA-256 digest of this bundle's canonical byte encoding.
    ///
    /// The digest is stable for semantically equivalent bundles: node entries
    /// are canonicalized by CID before encoding, so caller-side ordering does
    /// not affect the result.
    pub fn digest(&self) -> Result<Cid, Error> {
        self.to_bytes().map(|bytes| Cid::from_bytes(&bytes))
    }

    /// Return validated metadata for this bundle without importing it.
    ///
    /// The summary canonicalizes node order, deduplicates identical repeated
    /// nodes, and verifies each node byte payload against its CID.
    pub fn summary(&self) -> Result<SnapshotBundleSummary, Error> {
        self.validate_format_version()?;
        let nodes = canonical_snapshot_nodes(&self.nodes)?;
        Ok(snapshot_bundle_summary(self, &nodes))
    }

    /// Verify that this bundle is complete and contains no unreachable nodes.
    ///
    /// This check is read-only: it validates version, canonicalizes nodes,
    /// verifies every node byte payload by CID, decodes reachable nodes from
    /// the supplied bytes, and compares the reachable CID set with the bundled
    /// CID set.
    pub fn verify(&self) -> Result<SnapshotBundleVerification, Error> {
        self.validate_format_version()?;
        let nodes = canonical_snapshot_nodes(&self.nodes)?;
        let summary = snapshot_bundle_summary(self, &nodes);
        let nodes_by_cid = nodes
            .iter()
            .map(|node| (node.cid.as_bytes().to_vec(), node.bytes.as_slice()))
            .collect::<BTreeMap<_, _>>();
        let reachability = reachable_snapshot_nodes(&self.tree, &nodes_by_cid)?;
        let provided_cids = nodes_by_cid.keys().cloned().collect::<Vec<_>>();

        let missing_cids = reachability
            .reachable_cids
            .iter()
            .filter(|cid| !nodes_by_cid.contains_key(*cid))
            .cloned()
            .map(cid_from_wire_bytes)
            .collect::<Result<Vec<_>, Error>>()?;
        let extra_cids = provided_cids
            .iter()
            .filter(|cid| !reachability.reachable_cids.contains(*cid))
            .cloned()
            .map(cid_from_wire_bytes)
            .collect::<Result<Vec<_>, Error>>()?;

        Ok(SnapshotBundleVerification {
            valid: missing_cids.is_empty() && extra_cids.is_empty(),
            summary,
            reachable_nodes: reachability.reachable_cids.len(),
            reachable_bytes: reachability.reachable_bytes,
            missing_cids,
            extra_cids,
        })
    }

    /// Validate that the bundle version is supported by this crate.
    pub fn validate_format_version(&self) -> Result<(), Error> {
        if self.format_version == SNAPSHOT_BUNDLE_FORMAT_VERSION {
            Ok(())
        } else {
            Err(Error::InvalidSnapshotBundle(format!(
                "unsupported format version {}",
                self.format_version
            )))
        }
    }

    /// Serialize this bundle as deterministic, versioned bytes.
    ///
    /// The encoded form canonicalizes node order by CID bytes and deduplicates
    /// repeated identical nodes. It rejects unsupported bundle versions,
    /// malformed CIDs, and node bytes whose content hash does not match the
    /// supplied CID.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate_format_version()?;
        let nodes = canonical_snapshot_nodes(&self.nodes)?;
        let wire = SnapshotBundleWire {
            version: SNAPSHOT_BUNDLE_BYTES_VERSION,
            tree: self.tree.clone(),
            nodes: nodes
                .into_iter()
                .map(|node| SnapshotBundleNodeWire {
                    cid: node.cid.as_bytes().to_vec(),
                    bytes: node.bytes,
                })
                .collect(),
        };
        serde_cbor::ser::to_vec_packed(&wire).map_err(|err| Error::Serialize(err.to_string()))
    }

    /// Decode a deterministic, versioned snapshot bundle byte payload.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let wire: SnapshotBundleWire =
            serde_cbor::from_slice(bytes).map_err(snapshot_bundle_deserialize)?;
        if wire.version != SNAPSHOT_BUNDLE_BYTES_VERSION {
            return Err(Error::InvalidSnapshotBundle(format!(
                "unsupported bytes version {}",
                wire.version
            )));
        }
        let nodes = wire
            .nodes
            .into_iter()
            .map(|node| {
                let cid = cid_from_wire_bytes(node.cid)?;
                verify_node_bytes(&cid, &node.bytes)?;
                Ok(SnapshotBundleNode {
                    cid,
                    bytes: node.bytes,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(Self {
            format_version: SNAPSHOT_BUNDLE_FORMAT_VERSION,
            tree: wire.tree,
            nodes: canonical_snapshot_nodes(&nodes)?,
        })
    }
}

struct SnapshotBundleReachability {
    reachable_cids: Vec<Vec<u8>>,
    reachable_bytes: usize,
}

fn snapshot_bundle_summary(
    bundle: &SnapshotBundle,
    nodes: &[SnapshotBundleNode],
) -> SnapshotBundleSummary {
    let byte_count = nodes.iter().map(|node| node.bytes.len()).sum();
    let min_node_bytes = nodes
        .iter()
        .map(|node| node.bytes.len())
        .min()
        .unwrap_or_default();
    let max_node_bytes = nodes
        .iter()
        .map(|node| node.bytes.len())
        .max()
        .unwrap_or_default();

    SnapshotBundleSummary {
        format_version: bundle.format_version,
        root: bundle.tree.root.clone(),
        node_count: nodes.len(),
        byte_count,
        min_node_bytes,
        max_node_bytes,
    }
}

fn reachable_snapshot_nodes(
    tree: &Tree,
    nodes_by_cid: &BTreeMap<Vec<u8>, &[u8]>,
) -> Result<SnapshotBundleReachability, Error> {
    let mut seen = BTreeMap::<Vec<u8>, ()>::new();
    let mut missing = BTreeMap::<Vec<u8>, ()>::new();
    let mut frontier = VecDeque::new();
    let mut reachable_bytes = 0usize;

    if let Some(root) = &tree.root {
        frontier.push_back(root.as_bytes().to_vec());
    }

    while let Some(cid) = frontier.pop_front() {
        if seen.contains_key(&cid) {
            continue;
        }
        seen.insert(cid.clone(), ());

        let Some(bytes) = nodes_by_cid.get(&cid) else {
            missing.insert(cid, ());
            continue;
        };

        let node = Node::from_bytes(bytes)?;
        if node.keys.len() != node.vals.len() {
            return Err(Error::InvalidNode);
        }
        reachable_bytes += bytes.len();

        if !node.leaf {
            for child in &node.vals {
                let cid = child
                    .as_slice()
                    .try_into()
                    .map(Cid)
                    .map_err(|_| Error::InvalidNode)?;
                if !seen.contains_key(cid.as_bytes()) {
                    frontier.push_back(cid.as_bytes().to_vec());
                }
            }
        }
    }

    let mut reachable_cids = seen.into_keys().collect::<Vec<_>>();
    for cid in missing.into_keys() {
        if !reachable_cids.contains(&cid) {
            reachable_cids.push(cid);
        }
    }
    reachable_cids.sort();

    Ok(SnapshotBundleReachability {
        reachable_cids,
        reachable_bytes,
    })
}

pub(crate) fn verify_node_bytes(expected: &Cid, bytes: &[u8]) -> Result<(), Error> {
    let actual = Cid::from_bytes(bytes);
    if &actual == expected {
        Ok(())
    } else {
        Err(Error::CidMismatch {
            expected: expected.clone(),
            actual,
        })
    }
}

fn canonical_snapshot_nodes(
    nodes: &[SnapshotBundleNode],
) -> Result<Vec<SnapshotBundleNode>, Error> {
    let mut by_cid: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for node in nodes {
        verify_node_bytes(&node.cid, &node.bytes)?;
        let cid = node.cid.as_bytes().to_vec();
        if let Some(existing) = by_cid.get(&cid) {
            if existing != &node.bytes {
                return Err(Error::InvalidSnapshotBundle(format!(
                    "bundle contains conflicting duplicate node CID {}",
                    hex_bytes(&cid)
                )));
            }
            continue;
        }
        by_cid.insert(cid, node.bytes.clone());
    }
    by_cid
        .into_iter()
        .map(|(cid, bytes)| {
            Ok(SnapshotBundleNode {
                cid: cid_from_wire_bytes(cid)?,
                bytes,
            })
        })
        .collect()
}

fn cid_from_wire_bytes(bytes: Vec<u8>) -> Result<Cid, Error> {
    let cid: [u8; 32] = bytes.try_into().map_err(|bytes: Vec<u8>| {
        Error::InvalidSnapshotBundle(format!("CID must be exactly 32 bytes, got {}", bytes.len()))
    })?;
    Ok(Cid(cid))
}

fn snapshot_bundle_deserialize(error: serde_cbor::Error) -> Error {
    Error::InvalidSnapshotBundle(format!("could not decode bundle bytes: {error}"))
}

fn hex_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}
