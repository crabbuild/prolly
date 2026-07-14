//! Public compatibility surface for localized canonical mutation.

use super::canonical;
use super::error::{Error, Mutation};
use super::store::Store;
use super::tree::Tree;
use super::Prolly;

/// Observable logical and physical work performed by [`canonical_splice`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CanonicalSpliceStats {
    pub entries_scanned: usize,
    pub nodes_read: usize,
    pub nodes_rebuilt: usize,
    pub nodes_written: usize,
    pub nodes_reused: usize,
    pub levels_rebuilt: usize,
    pub right_edge_rebuilt: bool,
    pub root_changed: bool,
}

/// Apply logical mutations through the canonical localized writer.
pub fn canonical_splice<S>(
    prolly: &Prolly<S>,
    tree: &Tree,
    mutations: Vec<Mutation>,
) -> Result<(Tree, CanonicalSpliceStats), Error>
where
    S: Store,
    S::Error: Send + Sync,
{
    if tree.config != *prolly.config() {
        return Err(Error::CanonicalSpliceConfigMismatch);
    }
    let old_root = tree.root.clone();
    let (tree, stats) = canonical::apply(prolly, tree, mutations)?;
    let nodes_written = stats.nodes_written as usize;
    Ok((
        tree.clone(),
        CanonicalSpliceStats {
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
