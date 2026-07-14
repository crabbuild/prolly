use super::storage::GraphNode;
use super::HnswIndex;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::search::PreparedFilter;
use crate::prolly::proximity::{
    Neighbor, ProximityMap, ProximitySearchStats, SearchCompletion, SearchRequest, SearchResult,
};
use crate::prolly::store::Store;
use std::collections::{BTreeMap, HashSet};

pub(super) fn search<S>(
    index: &HnswIndex<S>,
    map: &ProximityMap<S>,
    request: SearchRequest<'_>,
) -> Result<SearchResult, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    request.validate()?;
    let query = prepare_vector(index.metric, request.query, index.dimensions)?;
    let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
    let mut stats = ProximitySearchStats::default();
    let mut completion = SearchCompletion::ApproximatePolicySatisfied;
    let mut score_cache = BTreeMap::<Vec<u8>, f64>::new();

    let mut current = index.entry_point.clone();
    let mut current_score = score_key(map, &current, &query, request.kernel, &mut stats)?;
    score_cache.insert(current.clone(), current_score);
    for layer in (1..=index.maximum_level).rev() {
        loop {
            let Some(node) = load_node(index, &current, &request, &mut stats, &mut completion)?
            else {
                return finish(map, &filter, &score_cache, request.k, stats, completion);
            };
            let Some(neighbors) = node.neighbors.get(layer as usize) else {
                break;
            };
            let mut best = (current_score, current.clone());
            for neighbor in neighbors {
                if distance_budget_exhausted(&request, &stats) {
                    completion = SearchCompletion::BudgetExhausted;
                    return finish(map, &filter, &score_cache, request.k, stats, completion);
                }
                let score = score_key(map, neighbor, &query, request.kernel, &mut stats)?;
                score_cache.insert(neighbor.clone(), score);
                if score
                    .total_cmp(&best.0)
                    .then_with(|| neighbor.cmp(&best.1))
                    .is_lt()
                {
                    best = (score, neighbor.clone());
                }
            }
            if best.1 == current {
                break;
            }
            current_score = best.0;
            current = best.1;
        }
    }

    let expansion_limit = usize::try_from(index.config.ef_search)
        .unwrap_or(usize::MAX)
        .max(
            request
                .k
                .saturating_mul(index.config.overfetch_multiplier as usize),
        );
    let mut frontier = vec![(current_score, current.clone())];
    let mut visited = HashSet::from([current]);
    let mut expanded = 0usize;
    while !frontier.is_empty() && expanded < expansion_limit {
        frontier.sort_by(|left, right| {
            left.0
                .total_cmp(&right.0)
                .then_with(|| left.1.cmp(&right.1))
        });
        let (_, key) = frontier.remove(0);
        let Some(node) = load_node(index, &key, &request, &mut stats, &mut completion)? else {
            break;
        };
        expanded += 1;
        for neighbor in &node.neighbors[0] {
            if !visited.insert(neighbor.clone()) {
                continue;
            }
            if request
                .budget
                .max_frontier_entries
                .is_some_and(|maximum| frontier.len() >= maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            if distance_budget_exhausted(&request, &stats) {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let score = score_key(map, neighbor, &query, request.kernel, &mut stats)?;
            score_cache.insert(neighbor.clone(), score);
            frontier.push((score, neighbor.clone()));
            stats.frontier_peak = stats.frontier_peak.max(frontier.len());
        }
        if completion == SearchCompletion::BudgetExhausted {
            break;
        }
    }
    finish(map, &filter, &score_cache, request.k, stats, completion)
}

fn load_node<S>(
    index: &HnswIndex<S>,
    key: &[u8],
    request: &SearchRequest<'_>,
    stats: &mut ProximitySearchStats,
    completion: &mut SearchCompletion,
) -> Result<Option<GraphNode>, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    if request
        .budget
        .max_nodes
        .is_some_and(|maximum| stats.nodes_read >= maximum)
    {
        *completion = SearchCompletion::BudgetExhausted;
        return Ok(None);
    }
    let bytes =
        index
            .graph
            .get(&index.graph_tree, key)?
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "graph neighbor key is absent".to_owned(),
            })?;
    if request
        .budget
        .max_committed_bytes
        .is_some_and(|maximum| stats.committed_bytes.saturating_add(bytes.len()) > maximum)
    {
        *completion = SearchCompletion::BudgetExhausted;
        return Ok(None);
    }
    stats.nodes_read += 1;
    stats.bytes_read += bytes.len();
    stats.committed_bytes += bytes.len();
    let node = GraphNode::decode(&bytes)?;
    if node.level > index.maximum_level
        || node
            .neighbors
            .iter()
            .any(|layer| layer.len() > usize::from(index.config.max_connections))
        || node
            .neighbors
            .iter()
            .flatten()
            .any(|neighbor| neighbor.as_slice() == key)
    {
        return Err(Error::InvalidProximityObject {
            kind: "HNSW",
            reason: "graph node violates manifest level, degree, or self-edge constraints"
                .to_owned(),
        });
    }
    Ok(Some(node))
}

fn score_key<S>(
    map: &ProximityMap<S>,
    key: &[u8],
    query: &[f32],
    kernel: crate::prolly::proximity::QueryKernel,
    stats: &mut ProximitySearchStats,
) -> Result<f64, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let (vector, _) = map.get(key)?.ok_or_else(|| Error::InvalidProximityObject {
        kind: "HNSW",
        reason: "graph key is absent from authoritative directory".to_owned(),
    })?;
    stats.distance_evaluations += 1;
    Ok(query_score(
        kernel,
        map.tree().config.metric,
        query,
        &vector,
    ))
}

fn finish<S>(
    map: &ProximityMap<S>,
    filter: &PreparedFilter,
    scores: &BTreeMap<Vec<u8>, f64>,
    k: usize,
    mut stats: ProximitySearchStats,
    completion: SearchCompletion,
) -> Result<SearchResult, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let mut candidates: Vec<_> = scores
        .iter()
        .filter(|(key, _)| filter.contains(key))
        .map(|(key, score)| (key.clone(), *score))
        .collect();
    candidates.sort_by(|left, right| {
        left.1
            .total_cmp(&right.1)
            .then_with(|| left.0.cmp(&right.0))
    });
    stats.reranked_candidates = candidates.len();
    let mut neighbors = Vec::with_capacity(k.min(candidates.len()));
    for (key, distance) in candidates.into_iter().take(k) {
        let (_, value) = map
            .get(&key)?
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "result key is absent from authoritative directory".to_owned(),
            })?;
        neighbors.push(Neighbor {
            key,
            value,
            distance,
        });
    }
    Ok(SearchResult {
        neighbors,
        stats,
        completion,
    })
}

fn distance_budget_exhausted(request: &SearchRequest<'_>, stats: &ProximitySearchStats) -> bool {
    request
        .budget
        .max_distance_evaluations
        .is_some_and(|maximum| stats.distance_evaluations >= maximum)
}
