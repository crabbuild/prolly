use super::super::builder::BatchBuilder;
use super::super::cid::Cid;
use super::super::config::Config;
use super::super::error::Error;
use super::super::store::Store;
use super::super::Prolly;
use super::builder::{build_hierarchy, IndexedRecord};
use super::cache::{ContentCache, DEFAULT_PROXIMITY_CACHE_NODES};
use super::distance::{prepare_vector, score};
use super::mutation::{mutate_hierarchy, LogicalEdit};
use super::storage::{Descriptor, ProximityNode, StoredRecord};
use super::vector::promotion_level;
use super::{
    ExactProximityRecord, Neighbor, ProximityConfig, ProximityMutation, ProximityMutationStats,
    ProximityRecord, ProximitySearchStats, ProximityTree, ProximityVerification, SearchOptions,
    SearchResult,
};
use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

/// Immutable exact-key directory plus deterministic ANN hierarchy.
pub struct ProximityMap<S: Store> {
    store: S,
    directory: Prolly<S>,
    tree: ProximityTree,
    node_cache: Mutex<ContentCache<ProximityNode>>,
}

impl<S> ProximityMap<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Build a canonical proximity map from logical records.
    pub fn build(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
    ) -> Result<Self, Error> {
        config.validate()?;
        let mut records: Vec<_> = records.into_iter().collect();
        records.sort_by(|left, right| left.key.cmp(&right.key));
        for pair in records.windows(2) {
            if pair[0].key == pair[1].key {
                return Err(Error::DuplicateProximityKey {
                    key: pair[0].key.clone(),
                });
            }
        }

        let directory_config = Config::default();
        let mut directory_builder = BatchBuilder::new(store.clone(), directory_config.clone());
        let mut indexed = Vec::with_capacity(records.len());
        for record in records {
            let stored = StoredRecord::new(
                &record.vector,
                record.value,
                config.metric,
                config.dimensions,
            )?;
            indexed.push(IndexedRecord {
                key: record.key.clone(),
                vector: stored.vector.clone(),
            });
            directory_builder.add(record.key, stored.encode());
        }
        let directory_tree = directory_builder.build()?;
        let hierarchy = build_hierarchy(&indexed, &config)?;
        put_missing_nodes(&store, &hierarchy.nodes)?;

        let descriptor = Descriptor {
            config: config.clone(),
            count: indexed.len() as u64,
            directory: directory_tree.clone(),
            proximity_root: hierarchy.root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        store
            .put(descriptor_cid.as_bytes(), &descriptor_bytes)
            .map_err(|error| Error::Store(Box::new(error)))?;

        Ok(Self {
            directory: Prolly::new(store.clone(), directory_config),
            store,
            tree: ProximityTree {
                directory: directory_tree,
                proximity_root: hierarchy.root,
                descriptor: descriptor_cid,
                count: indexed.len() as u64,
                config,
            },
            node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
        })
    }

    /// Load and locally validate a persisted proximity descriptor.
    pub fn load(store: S, descriptor_cid: Cid) -> Result<Self, Error> {
        let descriptor_bytes = load_content(&store, &descriptor_cid)?;
        let descriptor = Descriptor::decode(&descriptor_bytes)?;
        let root_bytes = load_content(&store, &descriptor.proximity_root)?;
        if root_bytes.len() > descriptor.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "root exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let root = ProximityNode::decode(&root_bytes, descriptor.config.dimensions)?;
        if root.subtree_count != descriptor.count {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "record count disagrees with proximity root".to_owned(),
            });
        }
        let directory_config = descriptor.directory.config.clone();
        Ok(Self {
            directory: Prolly::new(store.clone(), directory_config),
            store,
            tree: ProximityTree {
                directory: descriptor.directory,
                proximity_root: descriptor.proximity_root,
                descriptor: descriptor_cid,
                count: descriptor.count,
                config: descriptor.config,
            },
            node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
        })
    }

    /// The immutable roots and shape configuration committed by this map.
    pub fn tree(&self) -> &ProximityTree {
        &self.tree
    }

    /// Exact key lookup through the authoritative ordered directory.
    pub fn get(&self, key: &[u8]) -> Result<Option<ExactProximityRecord>, Error> {
        self.directory
            .get(&self.tree.directory, key)?
            .map(|bytes| {
                let record = StoredRecord::decode(&bytes, self.tree.config.dimensions)?;
                Ok((record.vector, record.value))
            })
            .transpose()
    }

    /// Exact key membership through the authoritative ordered directory.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get(key)?.is_some())
    }

    /// Canonical full rebuild after applying a sorted-unique logical mutation batch.
    pub fn rebuild_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<Self, Error> {
        let mutations = validate_mutations(mutations)?;
        let mut records = self.collect_records()?;
        apply_mutations(&mut records, &mutations, &self.tree.config)?;
        Self::build(
            self.store.clone(),
            self.tree.config.clone(),
            records.into_values(),
        )
    }

    /// Copy-on-write mutation with exact clean-rebuild CID equivalence.
    pub fn mutate_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<(Self, ProximityMutationStats), Error> {
        let mutations = validate_mutations(mutations)?;
        if mutations.is_empty() {
            return Ok((
                Self::load(self.store.clone(), self.tree.descriptor.clone())?,
                Default::default(),
            ));
        }
        let old_records = self.collect_records()?;
        let mut records = old_records.clone();
        apply_mutations(&mut records, &mutations, &self.tree.config)?;
        let logical_edits: Vec<_> = mutations
            .iter()
            .filter_map(|mutation| {
                let old = old_records
                    .get(&mutation.key)
                    .map(|record| record.vector.clone());
                let new = records
                    .get(&mutation.key)
                    .map(|record| record.vector.clone());
                (old != new).then(|| LogicalEdit {
                    key: mutation.key.clone(),
                    old,
                    new,
                    level: promotion_level(
                        &mutation.key,
                        self.tree.config.hierarchy.log_chunk_size,
                        self.tree.config.hierarchy.level_hash_seed,
                    ),
                })
            })
            .collect();
        let directory_tree = build_directory(&self.store, &self.tree.directory.config, &records)?;

        if logical_edits.is_empty() {
            let descriptor = Descriptor {
                config: self.tree.config.clone(),
                count: self.tree.count,
                directory: directory_tree.clone(),
                proximity_root: self.tree.proximity_root.clone(),
            };
            let bytes = descriptor.encode();
            let descriptor_cid = Cid::from_bytes(&bytes);
            self.store
                .put(descriptor_cid.as_bytes(), &bytes)
                .map_err(|error| Error::Store(Box::new(error)))?;
            let map = Self {
                directory: Prolly::new(self.store.clone(), directory_tree.config.clone()),
                store: self.store.clone(),
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root: self.tree.proximity_root.clone(),
                    descriptor: descriptor_cid,
                    count: self.tree.count,
                    config: self.tree.config.clone(),
                },
                node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
            };
            return Ok((
                map,
                ProximityMutationStats {
                    nodes_reused: 1,
                    ..Default::default()
                },
            ));
        }

        let (old_root, _) = self.load_node(&self.tree.proximity_root)?;
        let max_edit_level = logical_edits
            .iter()
            .map(|edit| edit.level)
            .max()
            .unwrap_or(0);
        let (proximity_root, nodes, mut stats) =
            if old_root.entries.is_empty() || max_edit_level >= old_root.level {
                let indexed: Vec<_> = records
                    .values()
                    .map(|record| IndexedRecord {
                        key: record.key.clone(),
                        vector: record.vector.clone(),
                    })
                    .collect();
                let built = build_hierarchy(&indexed, &self.tree.config)?;
                let stats = ProximityMutationStats {
                    records_rebuilt: indexed.len(),
                    distance_evaluations: built.distance_evaluations,
                    full_proximity_rebuild: true,
                    ..Default::default()
                };
                (built.root, built.nodes, stats)
            } else {
                let local = mutate_hierarchy(
                    &self.store,
                    &self.tree.proximity_root,
                    &self.tree.config,
                    &logical_edits,
                )?;
                (local.root, local.nodes, local.stats)
            };
        let pending_count = nodes.len();
        let nodes_written = put_missing_nodes(&self.store, &nodes)?;
        stats.nodes_written = nodes_written;
        stats.nodes_reused += pending_count.saturating_sub(nodes_written);

        let descriptor = Descriptor {
            config: self.tree.config.clone(),
            count: records.len() as u64,
            directory: directory_tree.clone(),
            proximity_root: proximity_root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        self.store
            .put(descriptor_cid.as_bytes(), &descriptor_bytes)
            .map_err(|error| Error::Store(Box::new(error)))?;
        Ok((
            Self {
                directory: Prolly::new(self.store.clone(), directory_tree.config.clone()),
                store: self.store.clone(),
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root,
                    descriptor: descriptor_cid,
                    count: records.len() as u64,
                    config: self.tree.config.clone(),
                },
                node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
            },
            stats,
        ))
    }

    /// Approximate nearest-neighbor search with independent result and beam widths.
    pub fn search(&self, query: &[f32], options: SearchOptions) -> Result<SearchResult, Error> {
        options.validate()?;
        let query = prepare_vector(self.tree.config.metric, query, self.tree.config.dimensions)?;
        let mut stats = ProximitySearchStats::default();
        let (root, root_bytes) = self.load_node(&self.tree.proximity_root)?;
        stats.nodes_read = 1;
        stats.bytes_read = root_bytes;
        stats.levels_visited = 1;

        let mut current = score_entries(
            &root,
            &query,
            &BTreeMap::new(),
            options.beam_width,
            &options,
            self.tree.config.metric,
            &mut stats,
        )?;
        let mut level = root.level;

        while level > 0 && !current.is_empty() && !stats.budget_exhausted {
            let mut seen = HashSet::new();
            let mut child_cids = Vec::new();
            for candidate in &current {
                if let Some(cid) = &candidate.child {
                    if seen.insert(cid.clone()) {
                        child_cids.push(cid.clone());
                    }
                }
            }
            if let Some(max_nodes) = options.max_nodes {
                let available = max_nodes.saturating_sub(stats.nodes_read);
                if child_cids.len() > available {
                    child_cids.truncate(available);
                    stats.budget_exhausted = true;
                }
            }
            if child_cids.is_empty() {
                break;
            }
            let child_nodes = self.load_nodes_ordered(&child_cids)?;
            let known: BTreeMap<Vec<u8>, f64> = current
                .iter()
                .map(|candidate| (candidate.key.clone(), candidate.distance))
                .collect();
            let mut next_by_key = BTreeMap::<Vec<u8>, Candidate>::new();
            for (node, bytes_read) in child_nodes {
                if node.level + 1 != level {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "child level does not descend by one".to_owned(),
                    });
                }
                stats.nodes_read += 1;
                stats.bytes_read += bytes_read;
                for candidate in score_entries(
                    &node,
                    &query,
                    &known,
                    usize::MAX,
                    &options,
                    self.tree.config.metric,
                    &mut stats,
                )? {
                    next_by_key
                        .entry(candidate.key.clone())
                        .and_modify(|existing| {
                            if candidate_order(&candidate, existing).is_lt() {
                                *existing = candidate.clone();
                            }
                        })
                        .or_insert(candidate);
                    if stats.budget_exhausted {
                        break;
                    }
                }
                if stats.budget_exhausted {
                    break;
                }
            }
            current = next_by_key.into_values().collect();
            current.sort_by(candidate_order);
            current.truncate(options.beam_width);
            level -= 1;
            stats.levels_visited += 1;
        }

        if level != 0 {
            return Ok(SearchResult {
                neighbors: Vec::new(),
                stats,
            });
        }
        current.sort_by(candidate_order);
        current.truncate(options.k);
        let keys: Vec<_> = current
            .iter()
            .map(|candidate| candidate.key.clone())
            .collect();
        let values = self.directory.get_many(&self.tree.directory, &keys)?;
        let mut neighbors = Vec::with_capacity(current.len());
        for (candidate, stored) in current.into_iter().zip(values) {
            let bytes = stored.ok_or_else(|| Error::InvalidProximityObject {
                kind: "node",
                reason: "leaf key is absent from exact directory".to_owned(),
            })?;
            let record = StoredRecord::decode(&bytes, self.tree.config.dimensions)?;
            if record.vector != candidate.vector {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "leaf vector disagrees with exact directory".to_owned(),
                });
            }
            neighbors.push(Neighbor {
                key: candidate.key,
                value: record.value,
                distance: candidate.distance as f32,
            });
        }
        Ok(SearchResult { neighbors, stats })
    }

    /// Traverse and validate the descriptor, directory, hierarchy, and routing invariants.
    pub fn verify(&self) -> Result<ProximityVerification, Error> {
        let records = self.collect_records()?;
        let root_bytes = load_content(&self.store, &self.tree.proximity_root)?;
        let root = ProximityNode::decode(&root_bytes, self.tree.config.dimensions)?;
        let mut state = VerificationState {
            records: &records,
            seen_nodes: HashSet::new(),
            seen_leaf_keys: HashSet::new(),
            summary: ProximityVerification {
                record_count: self.tree.count,
                maximum_level: root.level,
                ..Default::default()
            },
        };
        let count = self.verify_node(
            &self.tree.proximity_root,
            Some(root.level),
            None,
            &mut state,
        )?;
        if count != self.tree.count || records.len() as u64 != self.tree.count {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "logical counts disagree".to_owned(),
            });
        }
        if state.seen_leaf_keys.len() != records.len()
            || records
                .keys()
                .any(|key| !state.seen_leaf_keys.contains(key))
        {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "leaf identities do not match the exact directory".to_owned(),
            });
        }
        Ok(state.summary)
    }

    fn load_node(&self, cid: &Cid) -> Result<(Arc<ProximityNode>, usize), Error> {
        if let Some((node, _)) = self
            .node_cache
            .lock()
            .map_err(|_| Error::InvalidProximityObject {
                kind: "cache",
                reason: "node cache lock poisoned".to_owned(),
            })?
            .get(cid)
        {
            return Ok((node, 0));
        }
        let bytes = load_content(&self.store, cid)?;
        if bytes.len() > self.tree.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "node exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let len = bytes.len();
        let node = Arc::new(ProximityNode::decode(&bytes, self.tree.config.dimensions)?);
        self.node_cache
            .lock()
            .map_err(|_| Error::InvalidProximityObject {
                kind: "cache",
                reason: "node cache lock poisoned".to_owned(),
            })?
            .insert(cid.clone(), node.clone(), len);
        Ok((node, len))
    }

    fn load_nodes_ordered(&self, cids: &[Cid]) -> Result<Vec<(Arc<ProximityNode>, usize)>, Error> {
        let mut results: Vec<Option<(Arc<ProximityNode>, usize)>> = vec![None; cids.len()];
        let mut misses = Vec::new();
        {
            let mut cache = self
                .node_cache
                .lock()
                .map_err(|_| Error::InvalidProximityObject {
                    kind: "cache",
                    reason: "node cache lock poisoned".to_owned(),
                })?;
            for (index, cid) in cids.iter().enumerate() {
                if let Some((node, _)) = cache.get(cid) {
                    results[index] = Some((node, 0));
                } else {
                    misses.push((index, cid.clone()));
                }
            }
        }
        let keys: Vec<_> = misses.iter().map(|(_, cid)| cid.as_bytes()).collect();
        let values = self
            .store
            .batch_get_ordered_unique(&keys)
            .map_err(|error| Error::Store(Box::new(error)))?;
        for ((index, cid), value) in misses.into_iter().zip(values) {
            let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
            let actual = Cid::from_bytes(&bytes);
            if actual != cid {
                return Err(Error::CidMismatch {
                    expected: cid,
                    actual,
                });
            }
            if bytes.len() > self.tree.config.overflow.max_page_bytes as usize {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "node exceeds descriptor max_node_bytes".to_owned(),
                });
            }
            let len = bytes.len();
            let node = Arc::new(ProximityNode::decode(&bytes, self.tree.config.dimensions)?);
            self.node_cache
                .lock()
                .map_err(|_| Error::InvalidProximityObject {
                    kind: "cache",
                    reason: "node cache lock poisoned".to_owned(),
                })?
                .insert(cid, node.clone(), len);
            results[index] = Some((node, len));
        }
        results
            .into_iter()
            .map(|result| {
                result.ok_or_else(|| Error::InvalidProximityObject {
                    kind: "cache",
                    reason: "missing ordered cache result".to_owned(),
                })
            })
            .collect()
    }

    fn collect_records(&self) -> Result<BTreeMap<Vec<u8>, ProximityRecord>, Error> {
        let mut records = BTreeMap::new();
        for entry in self.directory.range(&self.tree.directory, &[], None)? {
            let (key, bytes) = entry?;
            let stored = StoredRecord::decode(&bytes, self.tree.config.dimensions)?;
            records.insert(
                key.clone(),
                ProximityRecord {
                    key,
                    vector: stored.vector,
                    value: stored.value,
                },
            );
        }
        Ok(records)
    }

    fn verify_node(
        &self,
        cid: &Cid,
        expected_level: Option<u8>,
        parent: Option<(
            &super::storage::ProximityEntry,
            &[super::storage::ProximityEntry],
        )>,
        state: &mut VerificationState<'_>,
    ) -> Result<u64, Error> {
        if !state.seen_nodes.insert(cid.clone()) {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "cycle or repeated child ownership".to_owned(),
            });
        }
        let bytes = load_content(&self.store, cid)?;
        if bytes.len() > self.tree.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "node exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let node = ProximityNode::decode(&bytes, self.tree.config.dimensions)?;
        if expected_level != Some(node.level) {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "unexpected logical level".to_owned(),
            });
        }
        state.summary.proximity_node_count += 1;
        state.summary.maximum_node_bytes = state.summary.maximum_node_bytes.max(bytes.len());

        if let Some((selected, candidates)) = parent {
            for entry in &node.entries {
                let selected_distance = score(
                    self.tree.config.metric,
                    entry.vector.inline()?,
                    selected.vector.inline()?,
                );
                for candidate in candidates {
                    state.summary.distance_checks += 1;
                    let candidate_distance = score(
                        self.tree.config.metric,
                        entry.vector.inline()?,
                        candidate.vector.inline()?,
                    );
                    let candidate_is_better = candidate_distance
                        .total_cmp(&selected_distance)
                        .then_with(|| candidate.key.cmp(&selected.key))
                        .is_lt();
                    if candidate_is_better {
                        return Err(Error::InvalidProximityObject {
                            kind: "node",
                            reason: "nearest-representative invariant violated".to_owned(),
                        });
                    }
                }
            }
        }

        for entry in &node.entries {
            if super::vector::promotion_level(
                &entry.key,
                self.tree.config.hierarchy.log_chunk_size,
                self.tree.config.hierarchy.level_hash_seed,
            ) < node.level
            {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "entry appears above its deterministic promotion level".to_owned(),
                });
            }
        }

        let computed = if node.level == 0 {
            for entry in &node.entries {
                if !state.seen_leaf_keys.insert(entry.key.clone()) {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "duplicate leaf identity".to_owned(),
                    });
                }
                let record =
                    state
                        .records
                        .get(&entry.key)
                        .ok_or_else(|| Error::InvalidProximityObject {
                            kind: "node",
                            reason: "leaf key is absent from exact directory".to_owned(),
                        })?;
                if record.vector.as_slice() != entry.vector.inline()? {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "leaf vector disagrees with exact directory".to_owned(),
                    });
                }
            }
            node.entries.len() as u64
        } else {
            let mut count = 0u64;
            for entry in &node.entries {
                let child = entry
                    .child
                    .as_ref()
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "node",
                        reason: "internal entry has no child".to_owned(),
                    })?;
                count = count
                    .checked_add(self.verify_node(
                        child,
                        Some(node.level - 1),
                        Some((entry, &node.entries)),
                        state,
                    )?)
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "node",
                        reason: "subtree count overflow".to_owned(),
                    })?;
            }
            count
        };
        if computed != node.subtree_count {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "subtree count mismatch".to_owned(),
            });
        }
        Ok(computed)
    }
}

struct VerificationState<'a> {
    records: &'a BTreeMap<Vec<u8>, ProximityRecord>,
    seen_nodes: HashSet<Cid>,
    seen_leaf_keys: HashSet<Vec<u8>>,
    summary: ProximityVerification,
}

#[derive(Clone)]
struct Candidate {
    key: Vec<u8>,
    vector: Vec<f32>,
    child: Option<Cid>,
    distance: f64,
}

fn candidate_order(left: &Candidate, right: &Candidate) -> std::cmp::Ordering {
    left.distance
        .total_cmp(&right.distance)
        .then_with(|| left.key.cmp(&right.key))
}

fn score_entries(
    node: &ProximityNode,
    query: &[f32],
    known: &BTreeMap<Vec<u8>, f64>,
    limit: usize,
    options: &SearchOptions,
    metric: super::DistanceMetric,
    stats: &mut ProximitySearchStats,
) -> Result<Vec<Candidate>, Error> {
    let mut candidates = Vec::with_capacity(node.entries.len().min(limit));
    for entry in &node.entries {
        let distance = if let Some(distance) = known.get(&entry.key) {
            *distance
        } else {
            if options
                .max_distance_evaluations
                .is_some_and(|max| stats.distance_evaluations >= max)
            {
                stats.budget_exhausted = true;
                break;
            }
            stats.distance_evaluations += 1;
            score(metric, query, entry.vector.inline()?)
        };
        candidates.push(Candidate {
            key: entry.key.clone(),
            vector: entry.vector.inline()?.to_vec(),
            child: entry.child.clone(),
            distance,
        });
    }
    candidates.sort_by(candidate_order);
    candidates.truncate(limit);
    Ok(candidates)
}

fn load_content<S: Store>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error> {
    let bytes = store
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
    Ok(bytes)
}

fn put_missing_nodes<S: Store>(store: &S, nodes: &[(Cid, Vec<u8>)]) -> Result<usize, Error> {
    let keys: Vec<_> = nodes.iter().map(|(cid, _)| cid.as_bytes()).collect();
    let existing = store
        .batch_get_ordered_unique(&keys)
        .map_err(|error| Error::Store(Box::new(error)))?;
    for ((expected, _), value) in nodes.iter().zip(&existing) {
        if let Some(bytes) = value {
            let actual = Cid::from_bytes(bytes);
            if actual != *expected {
                return Err(Error::CidMismatch {
                    expected: expected.clone(),
                    actual,
                });
            }
        }
    }
    let missing: Vec<_> = nodes
        .iter()
        .zip(existing)
        .filter_map(|((cid, bytes), value)| {
            value
                .is_none()
                .then_some((cid.as_bytes(), bytes.as_slice()))
        })
        .collect();
    store
        .batch_put(&missing)
        .map_err(|error| Error::Store(Box::new(error)))?;
    Ok(missing.len())
}

fn validate_mutations(
    mutations: impl IntoIterator<Item = ProximityMutation>,
) -> Result<Vec<ProximityMutation>, Error> {
    let mut mutations: Vec<_> = mutations.into_iter().collect();
    mutations.sort_by(|left, right| left.key.cmp(&right.key));
    for pair in mutations.windows(2) {
        if pair[0].key == pair[1].key {
            return Err(Error::DuplicateProximityKey {
                key: pair[0].key.clone(),
            });
        }
    }
    Ok(mutations)
}

fn apply_mutations(
    records: &mut BTreeMap<Vec<u8>, ProximityRecord>,
    mutations: &[ProximityMutation],
    config: &ProximityConfig,
) -> Result<(), Error> {
    for mutation in mutations {
        match &mutation.value {
            Some((vector, value)) => {
                records.insert(
                    mutation.key.clone(),
                    ProximityRecord {
                        key: mutation.key.clone(),
                        vector: prepare_vector(config.metric, vector, config.dimensions)?,
                        value: value.clone(),
                    },
                );
            }
            None => {
                records.remove(&mutation.key);
            }
        }
    }
    Ok(())
}

fn build_directory<S>(
    store: &S,
    config: &Config,
    records: &BTreeMap<Vec<u8>, ProximityRecord>,
) -> Result<super::super::tree::Tree, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for record in records.values() {
        builder.add(
            record.key.clone(),
            StoredRecord {
                vector: record.vector.clone(),
                value: record.value.clone(),
            }
            .encode(),
        );
    }
    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::store::MemStore;

    fn config() -> ProximityConfig {
        let mut config = ProximityConfig::new(1);
        config.hierarchy.log_chunk_size = 1;
        config.hierarchy.level_hash_seed = 7;
        config.overflow.max_page_bytes = 256 * 1024;
        config
    }

    fn two_representative_map() -> (Arc<MemStore>, ProximityMap<Arc<MemStore>>) {
        let keys: Vec<_> = (0..10_000)
            .map(|index| format!("candidate-{index}").into_bytes())
            .filter(|key| promotion_level(key, 1, 7) == 1)
            .take(2)
            .collect();
        assert_eq!(keys.len(), 2);
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            config(),
            keys.into_iter()
                .enumerate()
                .map(|(index, key)| ProximityRecord {
                    key,
                    vector: vec![index as f32],
                    value: Vec::new(),
                }),
        )
        .unwrap();
        (store, map)
    }

    fn publish_root_descriptor(
        store: &Arc<MemStore>,
        map: &ProximityMap<Arc<MemStore>>,
        root: ProximityNode,
    ) -> Cid {
        let root_bytes = root.encode().unwrap();
        let root_cid = Cid::from_bytes(&root_bytes);
        store.put(root_cid.as_bytes(), &root_bytes).unwrap();

        let descriptor_bytes = store.get(map.tree.descriptor.as_bytes()).unwrap().unwrap();
        let mut descriptor = Descriptor::decode(&descriptor_bytes).unwrap();
        descriptor.proximity_root = root_cid;
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        store
            .put(descriptor_cid.as_bytes(), &descriptor_bytes)
            .unwrap();
        descriptor_cid
    }

    fn publish_replacement_root(
        store: &Arc<MemStore>,
        map: &ProximityMap<Arc<MemStore>>,
        root: ProximityNode,
    ) -> ProximityMap<Arc<MemStore>> {
        let descriptor_cid = publish_root_descriptor(store, map, root);
        ProximityMap::load(store.clone(), descriptor_cid).unwrap()
    }

    #[test]
    fn verify_rejects_a_leaf_vector_that_disagrees_with_the_exact_directory() {
        let store = Arc::new(MemStore::new());
        let mut leaf_config = config();
        leaf_config.hierarchy.log_chunk_size = 63;
        let map = ProximityMap::build(
            store.clone(),
            leaf_config,
            [ProximityRecord {
                key: b"key".to_vec(),
                vector: vec![1.0],
                value: Vec::new(),
            }],
        )
        .unwrap();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.entries[0].vector = super::super::storage::VectorRef::Inline(vec![2.0]);
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "leaf vector disagrees with exact directory"
        ));
    }

    #[test]
    fn verify_rejects_repeated_child_ownership() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        assert_eq!(root.level, 1);
        assert_eq!(root.entries.len(), 2);
        root.entries[1].child = root.entries[0].child.clone();
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "cycle or repeated child ownership"
        ));
    }

    #[test]
    fn verify_rejects_an_invalid_child_level() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.entries[0].child = Some(map.tree.proximity_root.clone());
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "unexpected logical level"
        ));
    }

    #[test]
    fn verify_rejects_a_representative_below_its_node_level() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let replacement = (0..10_000)
            .map(|index| format!("!invalid-{index}").into_bytes())
            .find(|key| promotion_level(key, 1, 7) == 0 && key < &root.entries[1].key)
            .unwrap();
        root.entries[0].key = replacement.clone();
        root.entries[0].min_key = replacement;
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "entry appears above its deterministic promotion level"
        ));
    }

    #[test]
    fn verify_rejects_a_non_nearest_parent_route() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let first = root.entries[0].child.clone();
        root.entries[0].child = root.entries[1].child.clone();
        root.entries[1].child = first;
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "nearest-representative invariant violated"
        ));
    }

    #[test]
    fn load_rejects_a_root_subtree_count_that_disagrees_with_the_descriptor() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.subtree_count += 1;
        root.entries[0].child_count += 1;
        let descriptor = publish_root_descriptor(&store, &map, root);

        assert!(matches!(
            ProximityMap::load(store, descriptor),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "record count disagrees with proximity root"
        ));
    }
}
