//! Public surface for localized tree mutation.

use super::error::{Error, Mutation};
use super::store::Store;
use super::tree::Tree;
use super::write;
use super::Prolly;

/// Observable logical and physical work performed by [`splice`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpliceStats {
    pub entries_scanned: usize,
    pub nodes_read: usize,
    pub nodes_rebuilt: usize,
    pub nodes_written: usize,
    pub nodes_reused: usize,
    pub levels_rebuilt: usize,
    pub right_edge_rebuilt: bool,
    pub root_changed: bool,
}

/// Apply logical mutations through the localized writer.
pub fn splice<S>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
) -> Result<(Tree, SpliceStats), Error>
where
    S: Store,
    S::Error: Send + Sync,
{
    if tree.config != *prolly.config() {
        return Err(Error::SpliceConfigMismatch);
    }
    let old_root = tree.root.clone();
    let (tree, stats) = write::apply(prolly, tree, mutations)?;
    let nodes_written = stats.nodes_written as usize;
    Ok((
        tree.clone(),
        SpliceStats {
            entries_scanned: stats.entries_streamed as usize,
            nodes_read: stats.nodes_read as usize,
            nodes_rebuilt: nodes_written,
            nodes_written,
            nodes_reused: stats.nodes_reused as usize,
            levels_rebuilt: stats.resync_distance_nodes as usize,
            right_edge_rebuilt: stats.used_key_stable_fast_path,
            root_changed: old_root != tree.root,
        },
    ))
}
