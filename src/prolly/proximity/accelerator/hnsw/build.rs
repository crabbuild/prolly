use super::storage::{graph_config, GraphNode};
use super::{HnswBuildStats, HnswConfig};
use crate::prolly::builder::BatchBuilder;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::score;
use crate::prolly::proximity::vector::promotion_level;
use crate::prolly::proximity::ProximityMap;
use crate::prolly::store::Store;
use crate::prolly::tree::Tree;
use std::collections::BTreeMap;

pub(super) struct BuiltGraph {
    pub tree: Tree,
    pub entry_point: Vec<u8>,
    pub maximum_level: u8,
    pub stats: HnswBuildStats,
}

#[derive(Clone)]
struct BuildNode {
    vector: Vec<f32>,
    graph: GraphNode,
}

pub(super) fn build_graph<S>(
    map: &ProximityMap<S>,
    config: &HnswConfig,
    store: S,
) -> Result<BuiltGraph, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let records = map.collect_records()?;
    if records.is_empty() {
        return Err(Error::InvalidProximityConfig {
            reason: "HNSW requires at least one source record".to_owned(),
        });
    }
    let mut nodes = BTreeMap::<Vec<u8>, BuildNode>::new();
    let mut entry_point = Vec::new();
    let mut maximum_level = 0u8;
    let mut evaluations = 0usize;
    let maximum_connections = usize::from(config.max_connections);

    for record in records.into_values() {
        let level = promotion_level(&record.key, config.level_bits, config.seed).min(64);
        let mut graph = GraphNode {
            level,
            neighbors: vec![Vec::new(); usize::from(level) + 1],
        };
        for layer in 0..=level {
            let mut candidates = Vec::new();
            for (key, node) in &nodes {
                if node.graph.level < layer {
                    continue;
                }
                evaluations += 1;
                candidates.push((
                    score(map.tree().config.metric, &record.vector, &node.vector),
                    key.clone(),
                ));
            }
            candidates.sort_by(|left, right| {
                left.0
                    .total_cmp(&right.0)
                    .then_with(|| left.1.cmp(&right.1))
            });
            graph.neighbors[layer as usize] = candidates
                .into_iter()
                .take(maximum_connections)
                .map(|(_, key)| key)
                .collect();
            graph.neighbors[layer as usize].sort();
        }
        let key = record.key.clone();
        let selected = graph.neighbors.clone();
        nodes.insert(
            key.clone(),
            BuildNode {
                vector: record.vector,
                graph,
            },
        );
        for (layer, neighbors) in selected.into_iter().enumerate() {
            for neighbor in neighbors {
                let owner_vector = nodes
                    .get(&neighbor)
                    .expect("selected existing HNSW node")
                    .vector
                    .clone();
                let mut candidates = nodes[&neighbor].graph.neighbors[layer].clone();
                candidates.push(key.clone());
                candidates.sort();
                candidates.dedup();
                let mut ranked = Vec::with_capacity(candidates.len());
                for candidate in candidates {
                    evaluations += 1;
                    ranked.push((
                        score(
                            map.tree().config.metric,
                            &owner_vector,
                            &nodes[&candidate].vector,
                        ),
                        candidate,
                    ));
                }
                ranked.sort_by(|left, right| {
                    left.0
                        .total_cmp(&right.0)
                        .then_with(|| left.1.cmp(&right.1))
                });
                let mut pruned: Vec<_> = ranked
                    .into_iter()
                    .take(maximum_connections)
                    .map(|(_, key)| key)
                    .collect();
                pruned.sort();
                nodes
                    .get_mut(&neighbor)
                    .expect("selected existing HNSW node")
                    .graph
                    .neighbors[layer] = pruned;
            }
        }
        if entry_point.is_empty() || level > maximum_level {
            entry_point = key;
            maximum_level = level;
        }
    }

    let mut builder = BatchBuilder::new(store, graph_config());
    let mut directed_edges = 0usize;
    for (key, node) in nodes {
        directed_edges += node.graph.neighbors.iter().map(Vec::len).sum::<usize>();
        builder.add(key, node.graph.encode()?);
    }
    let tree = builder.build()?;
    Ok(BuiltGraph {
        tree,
        entry_point,
        maximum_level,
        stats: HnswBuildStats {
            records: map.tree().count as usize,
            distance_evaluations: evaluations,
            directed_edges,
            maximum_level,
        },
    })
}
