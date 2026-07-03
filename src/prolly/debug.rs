//! Debug views for inspecting Prolly tree shape and structural sharing.

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};

use super::key::debug_key;
#[cfg(feature = "async-store")]
use super::store::AsyncStore;
#[cfg(feature = "async-store")]
use super::AsyncProlly;
use super::{child_cid_at, Cid, Error, Prolly, Store, Tree};

/// Inspectable node metadata for debug tooling.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugNode {
    /// Content identifier for this node.
    pub cid: Cid,
    /// Whether this node stores leaf values instead of child CIDs.
    pub leaf: bool,
    /// Tree level where `0` is the leaf level.
    pub level: u8,
    /// Number of entries in the node.
    pub entry_count: usize,
    /// Maximum entries configured for this node.
    pub max_entries: usize,
    /// `entry_count / max_entries`, or `0.0` when `max_entries` is zero.
    pub fill_factor: f64,
    /// Compact encoded byte size of the node.
    pub encoded_bytes: usize,
    /// First separator or leaf key in this node.
    pub first_key: Option<Vec<u8>>,
    /// Last separator or leaf key in this node.
    pub last_key: Option<Vec<u8>>,
}

/// Debug metadata for all nodes at one tree level.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugLevel {
    /// Tree level where `0` is the leaf level.
    pub level: u8,
    /// Nodes at this level in deterministic traversal order.
    pub nodes: Vec<TreeDebugNode>,
}

/// Debug view of a tree grouped by level.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugView {
    /// Levels ordered from root to leaves.
    pub levels: Vec<TreeDebugLevel>,
}

/// Whether a compared subtree is shared or unique to one side.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TreeDebugNodeStatus {
    /// The same CID is reachable from both compared roots.
    Shared,
    /// The CID is reachable only from the left tree.
    LeftOnly,
    /// The CID is reachable only from the right tree.
    RightOnly,
}

/// A compared node annotated with its sharing status.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugComparedNode {
    /// Sharing status for this node.
    pub status: TreeDebugNodeStatus,
    /// Node metadata.
    pub node: TreeDebugNode,
}

/// Per-level structural sharing summary.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugComparisonLevel {
    /// Tree level where `0` is the leaf level.
    pub level: u8,
    /// Nodes reachable from both sides at this level.
    pub shared_nodes: usize,
    /// Nodes reachable only from the left side at this level.
    pub left_only_nodes: usize,
    /// Nodes reachable only from the right side at this level.
    pub right_only_nodes: usize,
    /// Encoded bytes for shared nodes at this level, counted once.
    pub shared_bytes: usize,
    /// Encoded bytes for left-only nodes at this level.
    pub left_only_bytes: usize,
    /// Encoded bytes for right-only nodes at this level.
    pub right_only_bytes: usize,
    /// Compared nodes at this level.
    pub nodes: Vec<TreeDebugComparedNode>,
}

/// Structural sharing comparison between two tree roots.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
pub struct TreeDebugComparison {
    /// Unique CIDs reachable from both roots.
    pub shared_nodes: usize,
    /// Unique CIDs reachable only from the left root.
    pub left_only_nodes: usize,
    /// Unique CIDs reachable only from the right root.
    pub right_only_nodes: usize,
    /// Encoded bytes for shared nodes, counted once.
    pub shared_bytes: usize,
    /// Encoded bytes for left-only nodes.
    pub left_only_bytes: usize,
    /// Encoded bytes for right-only nodes.
    pub right_only_bytes: usize,
    /// Per-level summaries ordered from root to leaves.
    pub levels: Vec<TreeDebugComparisonLevel>,
}

impl TreeDebugNode {
    fn from_node(cid: Cid, node: &super::Node) -> Self {
        let entry_count = node.len();
        let max_entries = node.max_chunk_size;
        let fill_factor = if max_entries == 0 {
            0.0
        } else {
            entry_count as f64 / max_entries as f64
        };

        Self {
            cid,
            leaf: node.leaf,
            level: node.level,
            entry_count,
            max_entries,
            fill_factor,
            encoded_bytes: node.encoded_len(),
            first_key: node.keys.first().cloned(),
            last_key: node.keys.last().cloned(),
        }
    }
}

impl TreeDebugView {
    /// Return a deterministic, human-readable tree-level rendering.
    pub fn to_text(&self) -> String {
        if self.levels.is_empty() {
            return "empty tree".to_string();
        }

        let mut lines = Vec::new();
        for (idx, level) in self.levels.iter().enumerate() {
            let label = if idx == 0 { " (root)" } else { "" };
            lines.push(format!(
                "level {}{}: nodes={}",
                level.level,
                label,
                level.nodes.len()
            ));
            for node in &level.nodes {
                lines.push(format_node_line("  ", None, node));
            }
        }
        lines.join("\n")
    }
}

impl TreeDebugComparison {
    /// Return a deterministic, human-readable structural sharing rendering.
    pub fn to_text(&self) -> String {
        if self.shared_nodes == 0 && self.left_only_nodes == 0 && self.right_only_nodes == 0 {
            return "empty comparison".to_string();
        }

        let mut lines = vec![format!(
            "shared={} ({} bytes), left_only={} ({} bytes), right_only={} ({} bytes)",
            self.shared_nodes,
            self.shared_bytes,
            self.left_only_nodes,
            self.left_only_bytes,
            self.right_only_nodes,
            self.right_only_bytes
        )];

        for level in &self.levels {
            lines.push(format!(
                "level {}: shared={} left_only={} right_only={}",
                level.level, level.shared_nodes, level.left_only_nodes, level.right_only_nodes
            ));
            for compared in &level.nodes {
                lines.push(format_node_line(
                    "  ",
                    Some(compared.status.as_str()),
                    &compared.node,
                ));
            }
        }

        lines.join("\n")
    }
}

impl TreeDebugNodeStatus {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Shared => "shared",
            Self::LeftOnly => "left",
            Self::RightOnly => "right",
        }
    }
}

pub(crate) fn collect_tree_debug_view<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
) -> Result<TreeDebugView, Error> {
    let Some(root_cid) = &tree.root else {
        return Ok(TreeDebugView::default());
    };

    let mut grouped = BTreeMap::new();
    let mut seen = HashSet::new();
    let mut frontier = vec![root_cid.clone()];

    while !frontier.is_empty() {
        let nodes = prolly.load_many_ordered_with_parallelism(&frontier, 1)?;
        let mut next_frontier = Vec::new();

        for (cid, node) in frontier.iter().cloned().zip(nodes) {
            if !seen.insert(cid.clone()) {
                continue;
            }
            if node.keys.len() != node.vals.len() {
                return Err(Error::InvalidNode);
            }

            grouped
                .entry(node.level)
                .or_insert_with(Vec::new)
                .push(TreeDebugNode::from_node(cid, &node));

            if !node.leaf {
                next_frontier.reserve(node.vals.len());
                for idx in 0..node.len() {
                    next_frontier.push(child_cid_at(&node, idx)?);
                }
            }
        }

        frontier = next_frontier;
    }

    Ok(view_from_grouped(grouped))
}

pub(crate) fn compare_tree_debug_views<S: Store>(
    prolly: &Prolly<S>,
    left: &Tree,
    right: &Tree,
) -> Result<TreeDebugComparison, Error> {
    let left = collect_tree_debug_view(prolly, left)?;
    let right = collect_tree_debug_view(prolly, right)?;
    Ok(compare_views(left, right))
}

#[cfg(feature = "async-store")]
pub(crate) async fn collect_tree_debug_view_async<S>(
    prolly: &AsyncProlly<S>,
    tree: &Tree,
) -> Result<TreeDebugView, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let Some(root_cid) = &tree.root else {
        return Ok(TreeDebugView::default());
    };

    let mut grouped = BTreeMap::new();
    let mut seen = HashSet::new();
    let mut frontier = vec![root_cid.clone()];

    while !frontier.is_empty() {
        let nodes = prolly.load_child_frontier_ordered(&frontier).await?;
        let mut next_frontier = Vec::new();

        for (cid, node) in frontier.iter().cloned().zip(nodes) {
            if !seen.insert(cid.clone()) {
                continue;
            }
            if node.keys.len() != node.vals.len() {
                return Err(Error::InvalidNode);
            }

            grouped
                .entry(node.level)
                .or_insert_with(Vec::new)
                .push(TreeDebugNode::from_node(cid, &node));

            if !node.leaf {
                next_frontier.reserve(node.vals.len());
                for idx in 0..node.len() {
                    next_frontier.push(child_cid_at(&node, idx)?);
                }
            }
        }

        frontier = next_frontier;
    }

    Ok(view_from_grouped(grouped))
}

#[cfg(feature = "async-store")]
pub(crate) async fn compare_tree_debug_views_async<S>(
    prolly: &AsyncProlly<S>,
    left: &Tree,
    right: &Tree,
) -> Result<TreeDebugComparison, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let left = collect_tree_debug_view_async(prolly, left).await?;
    let right = collect_tree_debug_view_async(prolly, right).await?;
    Ok(compare_views(left, right))
}

fn view_from_grouped(grouped: BTreeMap<u8, Vec<TreeDebugNode>>) -> TreeDebugView {
    let mut levels: Vec<_> = grouped
        .into_iter()
        .map(|(level, nodes)| TreeDebugLevel { level, nodes })
        .collect();
    levels.reverse();
    TreeDebugView { levels }
}

fn compare_views(left: TreeDebugView, right: TreeDebugView) -> TreeDebugComparison {
    let left_nodes = flatten_view(left);
    let right_nodes = flatten_view(right);
    let mut grouped: BTreeMap<u8, TreeDebugComparisonLevel> = BTreeMap::new();
    let mut comparison = TreeDebugComparison::default();

    let mut left_cids: Vec<_> = left_nodes.keys().cloned().collect();
    left_cids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    for cid in left_cids {
        let node = left_nodes
            .get(&cid)
            .expect("CID collected from map keys must exist")
            .clone();
        if right_nodes.contains_key(&cid) {
            comparison.shared_nodes += 1;
            comparison.shared_bytes += node.encoded_bytes;
            push_compared_node(&mut grouped, TreeDebugNodeStatus::Shared, node);
        } else {
            comparison.left_only_nodes += 1;
            comparison.left_only_bytes += node.encoded_bytes;
            push_compared_node(&mut grouped, TreeDebugNodeStatus::LeftOnly, node);
        }
    }

    let mut right_cids: Vec<_> = right_nodes.keys().cloned().collect();
    right_cids.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));

    for cid in right_cids {
        if left_nodes.contains_key(&cid) {
            continue;
        }
        let node = right_nodes
            .get(&cid)
            .expect("CID collected from map keys must exist")
            .clone();
        comparison.right_only_nodes += 1;
        comparison.right_only_bytes += node.encoded_bytes;
        push_compared_node(&mut grouped, TreeDebugNodeStatus::RightOnly, node);
    }

    comparison.levels = grouped.into_values().collect();
    comparison.levels.reverse();
    comparison
}

fn flatten_view(view: TreeDebugView) -> HashMap<Cid, TreeDebugNode> {
    view.levels
        .into_iter()
        .flat_map(|level| level.nodes)
        .map(|node| (node.cid.clone(), node))
        .collect()
}

fn push_compared_node(
    grouped: &mut BTreeMap<u8, TreeDebugComparisonLevel>,
    status: TreeDebugNodeStatus,
    node: TreeDebugNode,
) {
    let level = grouped
        .entry(node.level)
        .or_insert_with(|| TreeDebugComparisonLevel {
            level: node.level,
            ..TreeDebugComparisonLevel::default()
        });

    match status {
        TreeDebugNodeStatus::Shared => {
            level.shared_nodes += 1;
            level.shared_bytes += node.encoded_bytes;
        }
        TreeDebugNodeStatus::LeftOnly => {
            level.left_only_nodes += 1;
            level.left_only_bytes += node.encoded_bytes;
        }
        TreeDebugNodeStatus::RightOnly => {
            level.right_only_nodes += 1;
            level.right_only_bytes += node.encoded_bytes;
        }
    }

    level.nodes.push(TreeDebugComparedNode { status, node });
    level.nodes.sort_by(|a, b| {
        status_rank(&a.status)
            .cmp(&status_rank(&b.status))
            .then_with(|| a.node.cid.as_bytes().cmp(b.node.cid.as_bytes()))
    });
}

fn status_rank(status: &TreeDebugNodeStatus) -> u8 {
    match status {
        TreeDebugNodeStatus::Shared => 0,
        TreeDebugNodeStatus::LeftOnly => 1,
        TreeDebugNodeStatus::RightOnly => 2,
    }
}

fn format_node_line(prefix: &str, status: Option<&str>, node: &TreeDebugNode) -> String {
    let kind = if node.leaf { "L" } else { "I" };
    let status = status.map(|value| format!("{value} ")).unwrap_or_default();
    format!(
        "{prefix}{status}{kind} {} entries={}/{} fill={:.1}% bytes={} keys={}..{}",
        short_cid(&node.cid),
        node.entry_count,
        node.max_entries,
        node.fill_factor * 100.0,
        node.encoded_bytes,
        format_optional_key(node.first_key.as_deref()),
        format_optional_key(node.last_key.as_deref())
    )
}

fn format_optional_key(key: Option<&[u8]>) -> String {
    key.map(debug_key).unwrap_or_else(|| "-".to_string())
}

fn short_cid(cid: &Cid) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(12);
    for byte in cid.as_bytes().iter().take(6) {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}
