//! Canonical, localized copy-on-write mutation for ordered prolly trees.

use super::boundary::is_hash_boundary_config;
use super::builder::chunk_ranges_from_hash_boundaries;
use super::cid::Cid;
use super::config::Config;
use super::error::{Error, Mutation};
use super::node::Node;
use super::store::Store;
use super::tree::Tree;
use super::Prolly;
use std::collections::{HashMap, HashSet};

type LeafEntry = (Vec<u8>, Vec<u8>);
type NextLeafEntry = Result<Option<LeafEntry>, Error>;

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

#[derive(Clone, Debug, PartialEq, Eq)]
struct NodeSummary {
    cid: Cid,
    first_key: Vec<u8>,
}

#[derive(Clone, Debug)]
struct BuiltNode {
    summary: NodeSummary,
    bytes: Vec<u8>,
}

struct LevelSplice {
    summaries: Vec<NodeSummary>,
    built: Vec<BuiltNode>,
    reused: usize,
    rebuilt_right_edge: bool,
}

/// Apply sorted-unique logical mutations while preserving clean-build CIDs.
///
/// Leaf reconstruction begins at the canonical start of the first affected
/// leaf. Once all mutations are consumed, a byte-identical old leaf
/// resynchronizes the stream and the remaining leaf suffix is reused. Parent
/// levels are deterministically rechunked from compact child summaries; only
/// missing content is written.
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
    let mutations = normalize_mutations(mutations)?;
    if mutations.is_empty() {
        return Ok((tree.clone(), CanonicalSpliceStats::default()));
    }

    let mut stats = CanonicalSpliceStats::default();
    let mut old_cids = HashSet::new();
    let mut leaves = Vec::new();
    let mut old_levels = Vec::<Vec<NodeSummary>>::new();
    if let Some(root) = &tree.root {
        collect_leaf_summaries(
            prolly,
            root,
            &mut leaves,
            &mut old_levels,
            &mut old_cids,
            &mut stats,
        )?;
    }

    let first_key = mutations[0].key();
    let affected = leaves
        .partition_point(|leaf| leaf.first_key.as_slice() <= first_key)
        .saturating_sub(1);
    let mut summaries = leaves[..affected.min(leaves.len())].to_vec();
    stats.nodes_reused += summaries.len();

    let old_lookup: HashMap<(Vec<u8>, Cid), usize> = leaves
        .iter()
        .enumerate()
        .skip(affected)
        .map(|(index, leaf)| ((leaf.first_key.clone(), leaf.cid.clone()), index))
        .collect();
    let mut stream = MergedStream::new(prolly, &leaves, affected, mutations, &mut stats)?;
    let mut current = new_node(&tree.config, true, 0);
    let mut built_leaves = Vec::new();
    let mut synchronized = false;
    let mut suffix_reused = 0usize;

    while let Some((key, value)) = stream.next()? {
        let boundary = is_hash_boundary_config(&tree.config, &key, &value);
        current.keys.push(key);
        current.vals.push(value);
        let count = current.len();
        if count >= tree.config.min_chunk_size && (count >= tree.config.max_chunk_size || boundary)
        {
            let built = finish_node(std::mem::replace(
                &mut current,
                new_node(&tree.config, true, 0),
            ));
            let matched = old_lookup
                .get(&(built.summary.first_key.clone(), built.summary.cid.clone()))
                .copied();
            summaries.push(built.summary.clone());
            built_leaves.push(built);
            if stream.mutations_consumed() {
                if let Some(index) = matched {
                    summaries.extend_from_slice(&leaves[index + 1..]);
                    suffix_reused += leaves.len().saturating_sub(index + 1);
                    synchronized = true;
                    break;
                }
            }
        }
    }

    if !synchronized && !current.is_empty() {
        let built = finish_node(current);
        let matched = old_lookup
            .get(&(built.summary.first_key.clone(), built.summary.cid.clone()))
            .copied();
        summaries.push(built.summary.clone());
        built_leaves.push(built);
        if stream.mutations_consumed() {
            if let Some(index) = matched {
                summaries.extend_from_slice(&leaves[index + 1..]);
                suffix_reused += leaves.len().saturating_sub(index + 1);
                synchronized = true;
            }
        }
    }
    drop(stream);
    stats.nodes_reused += suffix_reused;

    stats.levels_rebuilt = usize::from(!built_leaves.is_empty());
    stats.right_edge_rebuilt = !synchronized && (!leaves.is_empty() || !built_leaves.is_empty());
    persist_nodes(prolly.store(), &built_leaves, &old_cids, &mut stats)?;

    if summaries == leaves {
        stats.root_changed = false;
        return Ok((tree.clone(), stats));
    }

    let mut old_children = leaves;
    let mut level = 0u8;
    while summaries.len() > 1 {
        level = level.checked_add(1).ok_or(Error::InvalidNode)?;
        let old_parents = old_levels
            .get(usize::from(level))
            .map(Vec::as_slice)
            .unwrap_or_default();
        let splice =
            splice_internal_level(&tree.config, level, &summaries, &old_children, old_parents)?;
        stats.nodes_reused += splice.reused;
        stats.right_edge_rebuilt |= splice.rebuilt_right_edge;
        stats.levels_rebuilt += usize::from(!splice.built.is_empty());
        persist_nodes(prolly.store(), &splice.built, &old_cids, &mut stats)?;
        if splice.summaries == old_parents {
            stats.root_changed = false;
            return Ok((tree.clone(), stats));
        }
        old_children = old_parents.to_vec();
        summaries = splice.summaries;
    }

    let root = summaries.into_iter().next().map(|summary| summary.cid);
    stats.root_changed = root != tree.root;
    Ok((
        Tree {
            root,
            config: tree.config.clone(),
        },
        stats,
    ))
}

fn normalize_mutations(mut mutations: Vec<Mutation>) -> Result<Vec<Mutation>, Error> {
    mutations.sort_by(|left, right| left.key().cmp(right.key()));
    for pair in mutations.windows(2) {
        if pair[0].key() == pair[1].key() {
            return Err(Error::DuplicateCanonicalMutation {
                key: pair[0].key().to_vec(),
            });
        }
    }
    Ok(mutations)
}

fn collect_leaf_summaries<S: Store>(
    prolly: &Prolly<S>,
    cid: &Cid,
    leaves: &mut Vec<NodeSummary>,
    old_levels: &mut Vec<Vec<NodeSummary>>,
    old_cids: &mut HashSet<Cid>,
    stats: &mut CanonicalSpliceStats,
) -> Result<(), Error>
where
    S::Error: Send + Sync,
{
    let node = prolly.load(cid)?;
    stats.nodes_read += 1;
    old_cids.insert(cid.clone());
    validate_node(&node)?;
    if node.leaf {
        if let Some(first_key) = node.keys.first() {
            leaves.push(NodeSummary {
                cid: cid.clone(),
                first_key: first_key.clone(),
            });
        }
        return Ok(());
    }
    let first_key = node.keys.first().cloned().ok_or(Error::InvalidNode)?;
    let level = usize::from(node.level);
    if old_levels.len() <= level {
        old_levels.resize_with(level + 1, Vec::new);
    }
    old_levels[level].push(NodeSummary {
        cid: cid.clone(),
        first_key,
    });
    if node.level == 1 {
        for (key, value) in node.keys.iter().zip(&node.vals) {
            let child = decode_child_cid(value)?;
            old_cids.insert(child.clone());
            leaves.push(NodeSummary {
                cid: child,
                first_key: key.clone(),
            });
        }
        return Ok(());
    }
    for value in &node.vals {
        collect_leaf_summaries(
            prolly,
            &decode_child_cid(value)?,
            leaves,
            old_levels,
            old_cids,
            stats,
        )?;
    }
    Ok(())
}

fn splice_internal_level(
    config: &Config,
    level: u8,
    new_children: &[NodeSummary],
    old_children: &[NodeSummary],
    old_nodes: &[NodeSummary],
) -> Result<LevelSplice, Error> {
    if new_children == old_children {
        return Ok(LevelSplice {
            summaries: old_nodes.to_vec(),
            built: Vec::new(),
            reused: old_nodes.len(),
            rebuilt_right_edge: false,
        });
    }

    let common_prefix = new_children
        .iter()
        .zip(old_children)
        .take_while(|(new, old)| new == old)
        .count();
    let first_changed_key = new_children
        .get(common_prefix)
        .or_else(|| old_children.get(common_prefix))
        .map(|summary| summary.first_key.as_slice())
        .ok_or(Error::InvalidNode)?;
    let affected_node = if old_nodes.is_empty() {
        0
    } else {
        old_nodes
            .partition_point(|node| node.first_key.as_slice() <= first_changed_key)
            .saturating_sub(1)
    };
    let start = if old_nodes.is_empty() {
        0
    } else {
        old_children
            .iter()
            .position(|child| child.first_key == old_nodes[affected_node].first_key)
            .ok_or(Error::InvalidNode)?
    };

    let mut common_suffix = 0usize;
    while common_suffix < new_children.len().min(old_children.len()) - common_prefix
        && new_children[new_children.len() - common_suffix - 1]
            == old_children[old_children.len() - common_suffix - 1]
    {
        common_suffix += 1;
    }
    let changed_end = new_children.len().saturating_sub(common_suffix);
    let old_lookup: HashMap<(Vec<u8>, Cid), usize> = old_nodes
        .iter()
        .enumerate()
        .skip(affected_node)
        .map(|(index, node)| ((node.first_key.clone(), node.cid.clone()), index))
        .collect();

    let remaining = &new_children[start..];
    let boundaries: Vec<_> = remaining
        .iter()
        .map(|child| is_hash_boundary_config(config, &child.first_key, child.cid.as_bytes()))
        .collect();
    let ranges = chunk_ranges_from_hash_boundaries(config, &boundaries);
    let mut summaries = old_nodes[..affected_node.min(old_nodes.len())].to_vec();
    let mut built = Vec::with_capacity(ranges.len());
    let mut reused = summaries.len();
    let mut synchronized = false;
    for range in ranges {
        let global_end = start + *range.end();
        let mut node = new_node(config, false, level);
        for child in &remaining[range] {
            node.keys.push(child.first_key.clone());
            node.vals.push(child.cid.0.to_vec());
        }
        let node = finish_node(node);
        let matched = old_lookup
            .get(&(node.summary.first_key.clone(), node.summary.cid.clone()))
            .copied();
        summaries.push(node.summary.clone());
        built.push(node);
        if global_end + 1 >= changed_end {
            if let Some(index) = matched {
                summaries.extend_from_slice(&old_nodes[index + 1..]);
                reused += old_nodes.len().saturating_sub(index + 1);
                synchronized = true;
                break;
            }
        }
    }
    Ok(LevelSplice {
        summaries,
        built,
        reused,
        rebuilt_right_edge: !synchronized && !old_nodes.is_empty(),
    })
}

struct MergedStream<'a, S: Store> {
    prolly: &'a Prolly<S>,
    leaves: &'a [NodeSummary],
    leaf_index: usize,
    leaf: Option<Node>,
    entry_index: usize,
    mutations: Vec<Mutation>,
    mutation_index: usize,
    stats: &'a mut CanonicalSpliceStats,
}

impl<'a, S> MergedStream<'a, S>
where
    S: Store,
    S::Error: Send + Sync,
{
    fn new(
        prolly: &'a Prolly<S>,
        leaves: &'a [NodeSummary],
        leaf_index: usize,
        mutations: Vec<Mutation>,
        stats: &'a mut CanonicalSpliceStats,
    ) -> Result<Self, Error> {
        let mut stream = Self {
            prolly,
            leaves,
            leaf_index,
            leaf: None,
            entry_index: 0,
            mutations,
            mutation_index: 0,
            stats,
        };
        stream.ensure_old_entry()?;
        Ok(stream)
    }

    fn mutations_consumed(&self) -> bool {
        self.mutation_index == self.mutations.len()
    }

    fn next(&mut self) -> NextLeafEntry {
        loop {
            self.ensure_old_entry()?;
            let old = self.leaf.as_ref().and_then(|leaf| {
                leaf.keys
                    .get(self.entry_index)
                    .zip(leaf.vals.get(self.entry_index))
            });
            let mutation = self.mutations.get(self.mutation_index).cloned();
            match (old, mutation.as_ref()) {
                (None, None) => return Ok(None),
                (Some((key, value)), None) => {
                    let result = (key.clone(), value.clone());
                    self.advance_old();
                    return Ok(Some(result));
                }
                (None, Some(mutation)) => {
                    self.mutation_index += 1;
                    if let Mutation::Upsert { key, val } = mutation {
                        return Ok(Some((key.clone(), val.clone())));
                    }
                }
                (Some((old_key, old_value)), Some(mutation)) => {
                    match old_key.as_slice().cmp(mutation.key()) {
                        std::cmp::Ordering::Less => {
                            let result = (old_key.clone(), old_value.clone());
                            self.advance_old();
                            return Ok(Some(result));
                        }
                        std::cmp::Ordering::Equal => {
                            self.advance_old();
                            self.mutation_index += 1;
                            if let Mutation::Upsert { key, val } = mutation {
                                return Ok(Some((key.clone(), val.clone())));
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            self.mutation_index += 1;
                            if let Mutation::Upsert { key, val } = mutation {
                                return Ok(Some((key.clone(), val.clone())));
                            }
                        }
                    }
                }
            }
        }
    }

    fn ensure_old_entry(&mut self) -> Result<(), Error> {
        while self
            .leaf
            .as_ref()
            .map_or(true, |leaf| self.entry_index >= leaf.len())
            && self.leaf_index < self.leaves.len()
        {
            let node = self.prolly.load(&self.leaves[self.leaf_index].cid)?;
            self.stats.nodes_read += 1;
            validate_node(&node)?;
            if !node.leaf {
                return Err(Error::InvalidNode);
            }
            self.leaf = Some(node);
            self.entry_index = 0;
            self.leaf_index += 1;
        }
        if self
            .leaf
            .as_ref()
            .is_some_and(|leaf| self.entry_index >= leaf.len())
            && self.leaf_index >= self.leaves.len()
        {
            self.leaf = None;
        }
        Ok(())
    }

    fn advance_old(&mut self) {
        self.entry_index += 1;
        self.stats.entries_scanned += 1;
    }
}

fn new_node(config: &Config, leaf: bool, level: u8) -> Node {
    Node::builder()
        .leaf(leaf)
        .level(level)
        .min_chunk_size(config.min_chunk_size)
        .max_chunk_size(config.max_chunk_size)
        .chunking_factor(config.chunking_factor)
        .hash_seed(config.hash_seed)
        .encoding(config.encoding.clone())
        .build()
}

fn finish_node(node: Node) -> BuiltNode {
    let first_key = node.keys.first().cloned().unwrap_or_default();
    let bytes = node.to_bytes();
    BuiltNode {
        summary: NodeSummary {
            cid: Cid::from_bytes(&bytes),
            first_key,
        },
        bytes,
    }
}

fn persist_nodes<S: Store>(
    store: &S,
    nodes: &[BuiltNode],
    old_cids: &HashSet<Cid>,
    stats: &mut CanonicalSpliceStats,
) -> Result<(), Error>
where
    S::Error: Send + Sync,
{
    stats.nodes_rebuilt += nodes.len();
    if nodes.is_empty() {
        return Ok(());
    }
    let keys: Vec<_> = nodes
        .iter()
        .map(|node| node.summary.cid.as_bytes())
        .collect();
    let existing = store
        .batch_get_ordered_unique(&keys)
        .map_err(|error| Error::Store(Box::new(error)))?;
    if existing.len() != nodes.len() {
        return Err(Error::InvalidNode);
    }
    let mut missing = Vec::new();
    for (node, value) in nodes.iter().zip(existing) {
        if let Some(bytes) = value {
            if Cid::from_bytes(&bytes) != node.summary.cid || bytes != node.bytes {
                return Err(Error::CidMismatch {
                    expected: node.summary.cid.clone(),
                    actual: Cid::from_bytes(&bytes),
                });
            }
            stats.nodes_reused += 1;
        } else {
            missing.push((node.summary.cid.as_bytes(), node.bytes.as_slice()));
        }
        if old_cids.contains(&node.summary.cid) && value_is_missing(&missing, &node.summary.cid) {
            return Err(Error::NotFound(node.summary.cid.clone()));
        }
    }
    store
        .batch_put(&missing)
        .map_err(|error| Error::Store(Box::new(error)))?;
    stats.nodes_written += missing.len();
    Ok(())
}

fn value_is_missing(missing: &[(&[u8], &[u8])], cid: &Cid) -> bool {
    missing.iter().any(|(key, _)| *key == cid.as_bytes())
}

fn validate_node(node: &Node) -> Result<(), Error> {
    if node.keys.len() != node.vals.len()
        || node.keys.windows(2).any(|pair| pair[0] > pair[1])
        || node.min_chunk_size == 0
        || node.max_chunk_size < node.min_chunk_size
    {
        return Err(Error::InvalidNode);
    }
    Ok(())
}

fn decode_child_cid(value: &[u8]) -> Result<Cid, Error> {
    Ok(Cid(value.try_into().map_err(|_| Error::InvalidNode)?))
}
