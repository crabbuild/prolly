use super::super::cid::Cid;
use super::super::error::Error;
use super::super::store::Store;
use super::builder::{build_hierarchy_at_level, IndexedRecord};
use super::distance::score;
use super::storage::overflow::{persist_logical_node, summarize};
use super::storage::vector::ExternalVector;
use super::storage::{PhysicalNodeKind, ProximityEntry, ProximityNode, VectorRef};
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
                .map(|entry| Ok((entry.key.clone(), entry.vector.inline()?.to_vec())))
                .collect::<Result<_, Error>>()?;
            for edit in edits {
                if edit.old.is_some() {
                    entries.remove(&edit.key);
                }
                if let Some(vector) = &edit.new {
                    entries.insert(edit.key.clone(), vector.clone());
                }
            }
            let replacement = ProximityNode {
                kind: PhysicalNodeKind::Leaf,
                level: 0,
                subtree_count: entries.len() as u64,
                quantizer: None,
                entries: entries
                    .into_iter()
                    .map(|(key, vector)| ProximityEntry::inline_leaf(key, vector))
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
                subtree_count = subtree_count
                    .checked_add(entry.child_count)
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
            let representative = entry.vector.inline()?.to_vec();
            let summary =
                self.summarize_child(new_child, old_child_node.level, &entry.key, &representative)?;
            debug_assert_eq!(summary.count, child_count);
            entries.push(summary.into_entry());
        }
        let replacement = ProximityNode {
            kind: PhysicalNodeKind::Route,
            level: node.level,
            subtree_count,
            quantizer: None,
            entries,
        };
        self.finish_node(old_cid, replacement)
    }

    fn finish_node(&mut self, old_cid: &Cid, node: ProximityNode) -> Result<(Cid, u64), Error> {
        let count = node.subtree_count;
        let cid = if node.entries.is_empty() {
            let bytes = node.encode()?;
            let cid = Cid::from_bytes(&bytes);
            self.pending.insert(cid.clone(), bytes);
            cid
        } else {
            persist_logical_node(
                node.kind,
                node.level,
                node.entries,
                self.config,
                &mut self.pending,
            )?
            .cid
        };
        if cid == *old_cid {
            self.stats.nodes_reused += 1;
        }
        Ok((cid, count))
    }

    fn summarize_child(
        &mut self,
        cid: Cid,
        level: u8,
        representative_key: &[u8],
        representative_vector: &[f32],
    ) -> Result<super::storage::overflow::NodeSummary, Error> {
        let mut entries = Vec::new();
        self.collect_logical_entries(&cid, level, &mut entries)?;
        summarize(cid, &entries, representative_key, representative_vector)
    }

    fn collect_logical_entries(
        &mut self,
        cid: &Cid,
        level: u8,
        entries: &mut Vec<ProximityEntry>,
    ) -> Result<(), Error> {
        let (node, _) = self.load_node(cid)?;
        if node.kind == PhysicalNodeKind::OverflowDirectory {
            for child in node.entries.into_iter().filter_map(|entry| entry.child) {
                self.collect_logical_entries(&child, level, entries)?;
            }
        } else if node.level == level {
            entries.extend(node.entries);
        } else {
            return Err(Error::InvalidProximityObject {
                kind: "mutation",
                reason: "overflow child has an unexpected logical level".to_owned(),
            });
        }
        Ok(())
    }

    fn closest(&mut self, entries: &[ProximityEntry], vector: &[f32]) -> Result<usize, Error> {
        let mut best: Option<(usize, f64)> = None;
        for (index, entry) in entries.iter().enumerate() {
            self.stats.distance_evaluations += 1;
            let distance = score(self.config.metric, vector, entry.vector.inline()?);
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
                if records
                    .insert(entry.key, entry.vector.into_inline()?)
                    .is_some()
                {
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
        let (bytes, stored) = if let Some(bytes) = self.pending.get(cid) {
            (bytes.clone(), false)
        } else {
            (
                self.store
                    .get(cid.as_bytes())
                    .map_err(|error| Error::Store(Box::new(error)))?
                    .ok_or_else(|| Error::NotFound(cid.clone()))?,
                true,
            )
        };
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
        if stored {
            self.stats.nodes_read += 1;
        }
        let mut node = ProximityNode::decode(&bytes, self.config.dimensions)?;
        for entry in &mut node.entries {
            let VectorRef::External(vector_cid) = &entry.vector else {
                continue;
            };
            let vector_bytes = if let Some(bytes) = self.pending.get(vector_cid) {
                bytes.clone()
            } else {
                self.store
                    .get(vector_cid.as_bytes())
                    .map_err(|error| Error::Store(Box::new(error)))?
                    .ok_or_else(|| Error::NotFound(vector_cid.clone()))?
            };
            let actual = Cid::from_bytes(&vector_bytes);
            if actual != *vector_cid {
                return Err(Error::CidMismatch {
                    expected: vector_cid.clone(),
                    actual,
                });
            }
            let external = ExternalVector::decode(&vector_bytes)?;
            if external.vector.len() != self.config.dimensions as usize {
                return Err(Error::InvalidProximityObject {
                    kind: "vector",
                    reason: "external vector dimension mismatch".to_owned(),
                });
            }
            entry.vector = VectorRef::Inline(external.vector);
        }
        Ok((node, bytes.len()))
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
