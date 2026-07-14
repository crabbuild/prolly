use super::super::cid::Cid;
use super::super::error::Error;
use super::super::store::Store;
use super::builder::{build_hierarchy_at_level, IndexedRecord};
use super::distance::score;
use super::node::{ProximityEntry, ProximityNode};
use super::{ProximityConfig, ProximityMutationStats};
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Clone, Debug)]
pub(crate) struct LogicalEdit {
    pub(crate) key: Vec<u8>,
    pub(crate) old: Option<Vec<f32>>,
    pub(crate) new: Option<Vec<f32>>,
    pub(crate) level: u8,
}

pub(crate) struct LocalMutation {
    pub(crate) root: Cid,
    pub(crate) nodes: Vec<(Cid, Vec<u8>)>,
    pub(crate) stats: ProximityMutationStats,
}

pub(crate) fn mutate_hierarchy<S: Store>(
    store: &S,
    root: &Cid,
    config: &ProximityConfig,
    edits: &[LogicalEdit],
) -> Result<LocalMutation, Error> {
    let mut context = Context {
        store,
        config,
        pending: HashMap::new(),
        stats: ProximityMutationStats::default(),
    };
    let (node, _) = context.load_node(root)?;
    let (root, _) = context.visit(root, &node, edits)?;
    let mut nodes: Vec<_> = context.pending.into_iter().collect();
    nodes.sort_by(|(left, _), (right, _)| left.as_bytes().cmp(right.as_bytes()));
    Ok(LocalMutation {
        root,
        nodes,
        stats: context.stats,
    })
}

struct Context<'a, S> {
    store: &'a S,
    config: &'a ProximityConfig,
    pending: HashMap<Cid, Vec<u8>>,
    stats: ProximityMutationStats,
}

impl<S: Store> Context<'_, S> {
    fn visit(
        &mut self,
        old_cid: &Cid,
        node: &ProximityNode,
        edits: &[LogicalEdit],
    ) -> Result<(Cid, u64), Error> {
        if node.level == 0 {
            let mut entries: BTreeMap<Vec<u8>, Vec<f32>> = node
                .entries
                .iter()
                .map(|entry| (entry.key.clone(), entry.vector.clone()))
                .collect();
            for edit in edits {
                if edit.old.is_some() {
                    entries.remove(&edit.key);
                }
                if let Some(vector) = &edit.new {
                    entries.insert(edit.key.clone(), vector.clone());
                }
            }
            let replacement = ProximityNode {
                level: 0,
                subtree_count: entries.len() as u64,
                entries: entries
                    .into_iter()
                    .map(|(key, vector)| ProximityEntry {
                        key,
                        vector,
                        child: None,
                    })
                    .collect(),
            };
            return self.finish_node(old_cid, replacement);
        }

        let mut grouped: BTreeMap<usize, BTreeMap<Vec<u8>, LogicalEdit>> = BTreeMap::new();
        let mut must_rebuild = HashSet::new();
        for edit in edits {
            if let Some(vector) = &edit.old {
                let index = self.closest(&node.entries, vector)?;
                merge_side(&mut grouped, index, edit, true);
                if edit.level == node.level - 1 {
                    must_rebuild.insert(index);
                }
            }
            if let Some(vector) = &edit.new {
                let index = self.closest(&node.entries, vector)?;
                merge_side(&mut grouped, index, edit, false);
                if edit.level == node.level - 1 {
                    must_rebuild.insert(index);
                }
            }
        }

        let mut entries = Vec::with_capacity(node.entries.len());
        let mut subtree_count = 0u64;
        for (index, entry) in node.entries.iter().enumerate() {
            let old_child = entry
                .child
                .as_ref()
                .ok_or_else(|| Error::InvalidProximityObject {
                    kind: "node",
                    reason: "internal entry has no child".to_owned(),
                })?;
            let Some(child_edits) = grouped.remove(&index) else {
                entries.push(entry.clone());
                let (child, _) = self.load_node(old_child)?;
                subtree_count =
                    subtree_count
                        .checked_add(child.subtree_count)
                        .ok_or_else(|| Error::InvalidProximityObject {
                            kind: "mutation",
                            reason: "subtree count overflow".to_owned(),
                        })?;
                self.stats.nodes_reused += 1;
                continue;
            };
            let child_edits: Vec<_> = child_edits.into_values().collect();
            let (old_child_node, _) = self.load_node(old_child)?;
            let (new_child, child_count) = if must_rebuild.contains(&index) {
                let mut records = BTreeMap::new();
                self.collect_leaf_records(old_child, &mut records)?;
                for edit in &child_edits {
                    if edit.old.is_some() {
                        records.remove(&edit.key);
                    }
                    if let Some(vector) = &edit.new {
                        records.insert(edit.key.clone(), vector.clone());
                    }
                }
                let indexed: Vec<_> = records
                    .into_iter()
                    .map(|(key, vector)| IndexedRecord { key, vector })
                    .collect();
                self.stats.records_rebuilt += indexed.len();
                let built =
                    build_hierarchy_at_level(&indexed, self.config, Some(old_child_node.level))?;
                self.stats.distance_evaluations += built.distance_evaluations;
                let count = indexed.len() as u64;
                for (cid, bytes) in built.nodes {
                    self.pending.insert(cid, bytes);
                }
                (built.root, count)
            } else {
                self.visit(old_child, &old_child_node, &child_edits)?
            };
            subtree_count = subtree_count.checked_add(child_count).ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "mutation",
                    reason: "subtree count overflow".to_owned(),
                }
            })?;
            entries.push(ProximityEntry {
                key: entry.key.clone(),
                vector: entry.vector.clone(),
                child: Some(new_child),
            });
        }
        let replacement = ProximityNode {
            level: node.level,
            subtree_count,
            entries,
        };
        self.finish_node(old_cid, replacement)
    }

    fn finish_node(&mut self, old_cid: &Cid, node: ProximityNode) -> Result<(Cid, u64), Error> {
        let bytes = node.encode()?;
        if bytes.len() > self.config.overflow.max_page_bytes as usize {
            return Err(Error::ProximityNodeTooLarge {
                level: node.level,
                entries: node.entries.len(),
                encoded_bytes: bytes.len(),
                limit: self.config.overflow.max_page_bytes as usize,
            });
        }
        let cid = Cid::from_bytes(&bytes);
        if cid == *old_cid {
            self.stats.nodes_reused += 1;
        } else {
            self.pending.insert(cid.clone(), bytes);
        }
        Ok((cid, node.subtree_count))
    }

    fn closest(&mut self, entries: &[ProximityEntry], vector: &[f32]) -> Result<usize, Error> {
        let mut best: Option<(usize, f64)> = None;
        for (index, entry) in entries.iter().enumerate() {
            self.stats.distance_evaluations += 1;
            let distance = score(self.config.metric, vector, &entry.vector);
            if best.map_or(true, |(best_index, best_distance)| {
                distance
                    .total_cmp(&best_distance)
                    .then_with(|| entry.key.cmp(&entries[best_index].key))
                    .is_lt()
            }) {
                best = Some((index, distance));
            }
        }
        best.map(|(index, _)| index)
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "mutation",
                reason: "cannot route through an empty internal node".to_owned(),
            })
    }

    fn collect_leaf_records(
        &mut self,
        cid: &Cid,
        records: &mut BTreeMap<Vec<u8>, Vec<f32>>,
    ) -> Result<(), Error> {
        let (node, _) = self.load_node(cid)?;
        if node.level == 0 {
            for entry in node.entries {
                if records.insert(entry.key, entry.vector).is_some() {
                    return Err(Error::InvalidProximityObject {
                        kind: "mutation",
                        reason: "duplicate leaf identity while rebuilding cluster".to_owned(),
                    });
                }
            }
            return Ok(());
        }
        for child in node.entries.into_iter().filter_map(|entry| entry.child) {
            self.collect_leaf_records(&child, records)?;
        }
        Ok(())
    }

    fn load_node(&mut self, cid: &Cid) -> Result<(ProximityNode, usize), Error> {
        if let Some(bytes) = self.pending.get(cid) {
            return Ok((
                ProximityNode::decode(bytes, self.config.dimensions)?,
                bytes.len(),
            ));
        }
        let bytes = self
            .store
            .get(cid.as_bytes())
            .map_err(|error| Error::Store(Box::new(error)))?
            .ok_or_else(|| Error::NotFound(cid.clone()))?;
        let actual = Cid::from_bytes(&bytes);
        if actual != *cid {
            return Err(Error::CidMismatch {
                expected: cid.clone(),
                actual,
            });
        }
        if bytes.len() > self.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "node exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        self.stats.nodes_read += 1;
        Ok((
            ProximityNode::decode(&bytes, self.config.dimensions)?,
            bytes.len(),
        ))
    }
}

fn merge_side(
    grouped: &mut BTreeMap<usize, BTreeMap<Vec<u8>, LogicalEdit>>,
    index: usize,
    edit: &LogicalEdit,
    old: bool,
) {
    let routed = grouped
        .entry(index)
        .or_default()
        .entry(edit.key.clone())
        .or_insert_with(|| LogicalEdit {
            key: edit.key.clone(),
            old: None,
            new: None,
            level: edit.level,
        });
    if old {
        routed.old = edit.old.clone();
    } else {
        routed.new = edit.new.clone();
    }
}
