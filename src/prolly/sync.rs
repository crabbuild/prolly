//! Store-to-store synchronization helpers for content-addressed tree nodes.

use super::cid::Cid;
use super::error::Error;

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
