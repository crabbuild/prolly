//! Public surface for localized tree mutation.

use super::error::{Error, Mutation};
use super::store::Store;
use super::tree::Tree;
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
    let old_root = tree.root.clone();
    let result = prolly.batch_with_stats(tree, mutations)?;
    let tree = result.tree;
    let stats = result.stats;
    let nodes_written = stats.written_nodes;
    Ok((
        tree.clone(),
        SpliceStats {
            entries_scanned: stats.entries_streamed,
            nodes_read: stats.nodes_read,
            nodes_rebuilt: nodes_written,
            nodes_written,
            nodes_reused: stats.nodes_reused,
            levels_rebuilt: stats.resync_distance_nodes,
            right_edge_rebuilt: stats.used_key_stable_fast_path,
            root_changed: old_root != tree.root,
        },
    ))
}
