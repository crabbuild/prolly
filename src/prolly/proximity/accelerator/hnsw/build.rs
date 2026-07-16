use super::storage::{graph_config, GraphNode};
use super::{HnswBuildLimits, HnswBuildStats, HnswConfig};
use crate::prolly::builder::BatchBuilder;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::score;
use crate::prolly::proximity::storage::StoredRecord;
use crate::prolly::proximity::vector::promotion_level;
use crate::prolly::proximity::ProximityMap;
use crate::prolly::store::Store;
use crate::prolly::tree::Tree;
use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashSet};

pub(super) struct BuiltGraph {
    pub tree: Tree,
    pub entry_point: Vec<u8>,
    pub maximum_level: u8,
    pub stats: HnswBuildStats,
}

#[derive(Clone)]
struct BuildNode {
    key: Vec<u8>,
    vector: Vec<f32>,
    level: u8,
    neighbors: Vec<Vec<usize>>,
}

struct BuildInput {
    key: Vec<u8>,
    vector: Vec<f32>,
}

#[derive(Clone, Debug)]
struct Ranked {
    distance: f64,
    id: usize,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.id == other.id
    }
}

impl Eq for Ranked {}

impl PartialOrd for Ranked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Ranked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.id.cmp(&other.id))
    }
}

struct BuildWork<'a> {
    metric: crate::prolly::proximity::DistanceMetric,
    evaluations: usize,
    limits: &'a HnswBuildLimits,
}

impl BuildWork<'_> {
    fn distance(&mut self, left: &[f32], right: &[f32]) -> Result<f64, Error> {
        let actual = self.evaluations.checked_add(1).ok_or_else(|| {
            Error::ProximityResourceLimitExceeded {
                resource: "distance_evaluations",
                limit: usize::MAX,
                actual: usize::MAX,
            }
        })?;
        enforce_limit(
            "distance_evaluations",
            self.limits.max_distance_evaluations,
            actual,
        )?;
        self.evaluations = actual;
        Ok(score(self.metric, left, right))
    }
}

pub(super) fn build_graph<S>(
    map: &ProximityMap<S>,
    config: &HnswConfig,
    limits: &HnswBuildLimits,
    store: S,
) -> Result<BuiltGraph, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let mut records = Vec::<BuildInput>::new();
    let mut maximum_transient_value_bytes = 0usize;
    for entry in map
        .directory_manager()
        .range(&map.tree().directory, &[], None)?
    {
        let (key, bytes) = entry?;
        let actual = records.len().checked_add(1).ok_or_else(owned_overflow)?;
        enforce_limit("records", limits.max_records, actual)?;
        let record = StoredRecord::decode(&bytes, map.tree().config.dimensions)?;
        maximum_transient_value_bytes = maximum_transient_value_bytes.max(record.value.len());
        records.push(BuildInput {
            key,
            vector: record.vector,
        });
    }
    if records.is_empty() {
        return Err(Error::InvalidProximityConfig {
            reason: "HNSW requires at least one source record".to_owned(),
        });
    }
    let owned_bytes = conservative_owned_bytes(&records, config, maximum_transient_value_bytes)?;
    enforce_limit("owned_bytes", limits.max_owned_bytes, owned_bytes)?;

    // worker_threads is deliberately excluded from graph decisions. Phase one
    // executes insertion serially so every configured count produces identical bytes.
    let _worker_threads = limits.worker_threads;
    let mut nodes = Vec::<BuildNode>::with_capacity(records.len());
    let mut entry_point = 0usize;
    let mut maximum_level = 0u8;
    let mut work = BuildWork {
        metric: map.tree().config.metric,
        evaluations: 0,
        limits,
    };
    let maximum_connections = usize::from(config.max_connections);
    let ef_construction = usize::try_from(config.ef_construction).unwrap_or(usize::MAX);

    for record in records {
        let level = promotion_level(&record.key, config.level_bits, config.seed).min(64);
        let mut node = BuildNode {
            key: record.key,
            vector: record.vector,
            level,
            neighbors: vec![Vec::new(); usize::from(level) + 1],
        };

        if nodes.is_empty() {
            maximum_level = level;
            nodes.push(node);
            continue;
        }

        let mut current = entry_point;
        if maximum_level > level {
            for layer in ((level + 1)..=maximum_level).rev() {
                current =
                    greedy_closest(&nodes, &node.vector, current, usize::from(layer), &mut work)?;
            }
        }

        let top_insert_layer = level.min(maximum_level);
        for layer in (0..=top_insert_layer).rev() {
            let candidates = search_layer(
                &nodes,
                &node.vector,
                &[current],
                usize::from(layer),
                ef_construction,
                &mut work,
            )?;
            if let Some(best) = candidates.first() {
                current = best.id;
            }
            // The standard diversified heuristic only needs a bounded nearest
            // working set to choose M outgoing edges. Keeping 2M candidates
            // avoids turning large ef_construction values back into quadratic
            // pairwise diversification work.
            let selection_count = maximum_connections.saturating_mul(2).min(candidates.len());
            let mut selected = select_diversified(
                &nodes,
                &node.vector,
                &candidates[..selection_count],
                maximum_connections,
                &mut work,
            )?;
            selected.sort();
            node.neighbors[usize::from(layer)] = selected;
        }

        let inserted = nodes.len();
        let reverse_edges = node.neighbors.clone();
        nodes.push(node);
        for (layer, neighbors) in reverse_edges.into_iter().enumerate() {
            for neighbor in neighbors {
                prune_reverse_edge(
                    &mut nodes,
                    neighbor,
                    inserted,
                    layer,
                    maximum_connections,
                    &mut work,
                )?;
            }
        }
        if level > maximum_level {
            entry_point = inserted;
            maximum_level = level;
        }
    }

    let mut builder = BatchBuilder::new(store, graph_config());
    let mut directed_edges = 0usize;
    let mut encoded_graph_bytes = 0usize;
    for node in &nodes {
        directed_edges =
            directed_edges.saturating_add(node.neighbors.iter().map(Vec::len).sum::<usize>());
        let graph = GraphNode {
            level: node.level,
            routing_vector_encoding: config.routing_vector_encoding,
            routing_vector: node.vector.clone(),
            neighbors: node
                .neighbors
                .iter()
                .map(|layer| {
                    let mut keys = layer
                        .iter()
                        .map(|id| nodes[*id].key.clone())
                        .collect::<Vec<_>>();
                    keys.sort();
                    keys
                })
                .collect(),
        };
        let bytes = graph.encode()?;
        encoded_graph_bytes = encoded_graph_bytes
            .checked_add(node.key.len())
            .and_then(|total| total.checked_add(bytes.len()))
            .ok_or_else(|| Error::ProximityResourceLimitExceeded {
                resource: "encoded_graph_bytes",
                limit: usize::MAX,
                actual: usize::MAX,
            })?;
        enforce_limit(
            "encoded_graph_bytes",
            limits.max_encoded_graph_bytes,
            encoded_graph_bytes,
        )?;
        builder.add(node.key.clone(), bytes);
    }
    let tree = builder.build()?;
    Ok(BuiltGraph {
        tree,
        entry_point: nodes[entry_point].key.clone(),
        maximum_level,
        stats: HnswBuildStats {
            records: map.tree().count as usize,
            distance_evaluations: work.evaluations,
            directed_edges,
            maximum_level,
            owned_bytes,
            encoded_graph_bytes,
        },
    })
}

fn greedy_closest(
    nodes: &[BuildNode],
    query: &[f32],
    mut current: usize,
    layer: usize,
    work: &mut BuildWork<'_>,
) -> Result<usize, Error> {
    let mut current_distance = work.distance(query, &nodes[current].vector)?;
    loop {
        let mut best = Ranked {
            distance: current_distance,
            id: current,
        };
        for neighbor in &nodes[current].neighbors[layer] {
            let candidate = Ranked {
                distance: work.distance(query, &nodes[*neighbor].vector)?,
                id: *neighbor,
            };
            if candidate < best {
                best = candidate;
            }
        }
        if best.id == current {
            return Ok(current);
        }
        current_distance = best.distance;
        current = best.id;
    }
}

fn search_layer(
    nodes: &[BuildNode],
    query: &[f32],
    entry_points: &[usize],
    layer: usize,
    ef: usize,
    work: &mut BuildWork<'_>,
) -> Result<Vec<Ranked>, Error> {
    let mut visited = HashSet::new();
    let mut candidates = BinaryHeap::<Reverse<Ranked>>::new();
    let mut closest = BinaryHeap::<Ranked>::new();
    for id in entry_points {
        if visited.insert(*id) {
            let ranked = Ranked {
                distance: work.distance(query, &nodes[*id].vector)?,
                id: *id,
            };
            candidates.push(Reverse(ranked.clone()));
            closest.push(ranked);
        }
    }
    while let Some(Reverse(candidate)) = candidates.pop() {
        if closest.len() >= ef && closest.peek().is_some_and(|worst| candidate > *worst) {
            break;
        }
        for neighbor in &nodes[candidate.id].neighbors[layer] {
            if !visited.insert(*neighbor) {
                continue;
            }
            let ranked = Ranked {
                distance: work.distance(query, &nodes[*neighbor].vector)?,
                id: *neighbor,
            };
            if closest.len() < ef || closest.peek().is_some_and(|worst| ranked < *worst) {
                candidates.push(Reverse(ranked.clone()));
                closest.push(ranked);
                if closest.len() > ef {
                    closest.pop();
                }
            }
        }
    }
    let mut result = closest.into_vec();
    result.sort();
    Ok(result)
}

fn select_diversified(
    nodes: &[BuildNode],
    _query: &[f32],
    candidates: &[Ranked],
    maximum: usize,
    work: &mut BuildWork<'_>,
) -> Result<Vec<usize>, Error> {
    let mut selected = Vec::<usize>::with_capacity(maximum.min(candidates.len()));
    for candidate in candidates {
        if selected.len() == maximum {
            break;
        }
        let candidate_vector = &nodes[candidate.id].vector;
        let mut diverse = true;
        for selected_id in &selected {
            if work.distance(candidate_vector, &nodes[*selected_id].vector)? < candidate.distance {
                diverse = false;
                break;
            }
        }
        if diverse {
            selected.push(candidate.id);
        }
    }
    if selected.len() < maximum {
        let selected_set: HashSet<_> = selected.iter().cloned().collect();
        selected.extend(
            candidates
                .iter()
                .filter(|candidate| !selected_set.contains(&candidate.id))
                .take(maximum - selected.len())
                .map(|candidate| candidate.id),
        );
    }
    Ok(selected)
}

fn prune_reverse_edge(
    nodes: &mut [BuildNode],
    owner: usize,
    inserted: usize,
    layer: usize,
    maximum: usize,
    work: &mut BuildWork<'_>,
) -> Result<(), Error> {
    let owner_vector = nodes[owner].vector.clone();
    let mut ids = nodes[owner].neighbors[layer].clone();
    ids.push(inserted);
    ids.sort();
    ids.dedup();
    let mut ranked = Vec::with_capacity(ids.len());
    for id in ids {
        ranked.push(Ranked {
            distance: work.distance(&owner_vector, &nodes[id].vector)?,
            id,
        });
    }
    ranked.sort();
    let mut selected = select_diversified(nodes, &owner_vector, &ranked, maximum, work)?;
    selected.sort();
    nodes[owner].neighbors[layer] = selected;
    Ok(())
}

fn conservative_owned_bytes(
    records: &[BuildInput],
    config: &HnswConfig,
    maximum_transient_value_bytes: usize,
) -> Result<usize, Error> {
    let per_edge = std::mem::size_of::<usize>();
    let scratch = usize::try_from(config.ef_construction)
        .unwrap_or(usize::MAX)
        .checked_mul(
            std::mem::size_of::<Ranked>()
                .checked_mul(2)
                .and_then(|value| value.checked_add(std::mem::size_of::<usize>() * 2))
                .ok_or_else(owned_overflow)?,
        )
        .ok_or_else(owned_overflow)?;
    let mut total = std::mem::size_of::<Vec<BuildNode>>()
        .checked_add(scratch)
        .and_then(|value| value.checked_add(maximum_transient_value_bytes))
        .ok_or_else(owned_overflow)?;
    for record in records {
        let level = promotion_level(&record.key, config.level_bits, config.seed).min(64);
        let layers = usize::from(level) + 1;
        let record_bytes = record
            .key
            .len()
            .checked_mul(2)
            .and_then(|value| value.checked_add(record.vector.len().checked_mul(4)?))
            .and_then(|value| value.checked_add(std::mem::size_of::<BuildNode>()))
            .and_then(|value| {
                value.checked_add(layers.checked_mul(std::mem::size_of::<Vec<usize>>())?)
            })
            .and_then(|value| {
                value.checked_add(
                    layers
                        .checked_mul(usize::from(config.max_connections))?
                        .checked_mul(per_edge)?,
                )
            })
            .ok_or_else(owned_overflow)?;
        total = total.checked_add(record_bytes).ok_or_else(owned_overflow)?;
    }
    Ok(total)
}

fn owned_overflow() -> Error {
    Error::ProximityResourceLimitExceeded {
        resource: "owned_bytes",
        limit: usize::MAX,
        actual: usize::MAX,
    }
}

fn enforce_limit(resource: &'static str, limit: Option<usize>, actual: usize) -> Result<(), Error> {
    if let Some(limit) = limit {
        if actual > limit {
            return Err(Error::ProximityResourceLimitExceeded {
                resource,
                limit,
                actual,
            });
        }
    }
    Ok(())
}
