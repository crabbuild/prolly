use super::storage::GraphNode;
use super::HnswIndex;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::search::PreparedFilter;
use crate::prolly::proximity::search::{EligibilityCardinality, SearchPlan, SearchPlanSummary};
use crate::prolly::proximity::{
    Neighbor, ProximityMap, ProximitySearchStats, SearchCompletion, SearchRequest, SearchResult,
};
use crate::prolly::store::Store;
use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BinaryHeap, HashSet};

#[derive(Clone, Debug)]
struct Ranked {
    distance: f64,
    key: Vec<u8>,
}

impl PartialEq for Ranked {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.key == other.key
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
            .then_with(|| self.key.cmp(&other.key))
    }
}

pub(super) fn search<S>(
    index: &HnswIndex<S>,
    map: &ProximityMap<S>,
    request: SearchRequest<'_>,
) -> Result<SearchResult, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
    let ef_search = request
        .options
        .hnsw
        .ef_search
        .unwrap_or(index.config.ef_search);
    let base = (ef_search as usize).max(
        request
            .k
            .saturating_mul(index.config.overfetch_multiplier as usize),
    );
    let cardinality = filter.cardinality(map.tree().count);
    let expansion_target = match cardinality {
        EligibilityCardinality::Known(0) => 0,
        EligibilityCardinality::Known(eligible) => {
            (((base as u128)
                .saturating_mul(map.tree().count as u128)
                .saturating_add(eligible as u128 - 1)
                / eligible as u128)
                .min(map.tree().count as u128)) as usize
        }
        EligibilityCardinality::Unknown => base.min(map.tree().count as usize),
    };
    let eligible_limit = match cardinality {
        EligibilityCardinality::Known(count) => count as usize,
        EligibilityCardinality::Unknown => map.tree().count as usize,
    };
    let rerank_target = request
        .k
        .saturating_mul(index.config.overfetch_multiplier as usize)
        .max(request.k)
        .min(eligible_limit);
    let plan = SearchPlan::Hnsw {
        ef_search,
        expansion_target,
        rerank_target,
    };
    search_planned(index, map, request, &plan)
}

pub(crate) fn search_planned<S>(
    index: &HnswIndex<S>,
    map: &ProximityMap<S>,
    request: SearchRequest<'_>,
    plan: &SearchPlan,
) -> Result<SearchResult, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    search_planned_with_exclusion(index, map, &map.tree().descriptor, request, plan, |_| {
        Ok(false)
    })
}

pub(crate) fn search_planned_with_exclusion<S, F>(
    index: &HnswIndex<S>,
    map: &ProximityMap<S>,
    expected_source: &crate::prolly::cid::Cid,
    request: SearchRequest<'_>,
    plan: &SearchPlan,
    mut excluded: F,
) -> Result<SearchResult, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
    F: FnMut(&[u8]) -> Result<bool, Error>,
{
    let SearchPlan::Hnsw {
        ef_search,
        expansion_target: traversal_target,
        rerank_target,
    } = plan
    else {
        return Err(Error::InvalidProximitySearch {
            reason: "HNSW executor requires an HNSW search plan".to_owned(),
        });
    };
    request.validate()?;
    if &index.source != expected_source
        || index.dimensions != map.tree().config.dimensions
        || index.metric != map.tree().config.metric
    {
        return Err(Error::InvalidProximitySearch {
            reason: "HNSW is bound to a different source descriptor".to_owned(),
        });
    }
    let query = prepare_vector(index.metric, request.query, index.dimensions)?;
    let filter = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
    let mut state = SearchState {
        index,
        request: &request,
        stats: ProximitySearchStats::default(),
        completion: SearchCompletion::ApproximatePolicySatisfied,
        loaded: BTreeMap::new(),
        plan: plan.summary(),
    };

    let mut current = index.entry_point.clone();
    let entry = match state.node(&current)? {
        Some(node) => node,
        None => return Ok(state.finish_without_candidates()),
    };
    let mut current_distance = match state.distance(&query, &entry.routing_vector) {
        Some(distance) => distance,
        None => return Ok(state.finish_without_candidates()),
    };
    for layer in (1..=index.maximum_level).rev() {
        loop {
            let Some(node) = state.node(&current)? else {
                return Ok(state.finish_without_candidates());
            };
            let mut best = Ranked {
                distance: current_distance,
                key: current.clone(),
            };
            for neighbor in &node.neighbors[usize::from(layer)] {
                let Some(neighbor_node) = state.node(neighbor)? else {
                    return Ok(state.finish_without_candidates());
                };
                let Some(distance) = state.distance(&query, &neighbor_node.routing_vector) else {
                    return Ok(state.finish_without_candidates());
                };
                let candidate = Ranked {
                    distance,
                    key: neighbor.clone(),
                };
                if candidate < best {
                    best = candidate;
                }
            }
            if best.key == current {
                break;
            }
            current_distance = best.distance;
            current = best.key;
        }
    }

    let traversal_target = *traversal_target;
    let rerank_target = *rerank_target;
    let _ef_search = *ef_search;
    let first = Ranked {
        distance: current_distance,
        key: current.clone(),
    };
    let mut frontier = BinaryHeap::from([Reverse(first.clone())]);
    let mut traversal_closest = BinaryHeap::from([first.clone()]);
    let mut eligible = BinaryHeap::<Ranked>::new();
    if filter.contains(&current) && !excluded(&current)? {
        eligible.push(first);
    }
    let mut visited = HashSet::from([current]);
    let mut expanded = 0usize;
    state.stats.frontier_peak = 1;

    while let Some(Reverse(candidate)) = frontier.pop() {
        if expanded >= traversal_target
            && eligible.len() >= request.k
            && traversal_closest
                .peek()
                .is_some_and(|worst| candidate > *worst)
        {
            break;
        }
        let Some(node) = state.node(&candidate.key)? else {
            break;
        };
        expanded = expanded.saturating_add(1);
        for neighbor in &node.neighbors[0] {
            if !visited.insert(neighbor.clone()) {
                continue;
            }
            if !state.frontier_allows(
                frontier
                    .len()
                    .saturating_add(traversal_closest.len())
                    .saturating_add(eligible.len())
                    .saturating_add(1),
            ) {
                break;
            }
            let Some(neighbor_node) = state.node(neighbor)? else {
                break;
            };
            let Some(distance) = state.distance(&query, &neighbor_node.routing_vector) else {
                break;
            };
            let ranked = Ranked {
                distance,
                key: neighbor.clone(),
            };
            let competitive = traversal_closest.len() < traversal_target
                || traversal_closest
                    .peek()
                    .is_some_and(|worst| ranked < *worst)
                || eligible.len() < request.k;
            if competitive {
                frontier.push(Reverse(ranked.clone()));
                traversal_closest.push(ranked.clone());
                if traversal_closest.len() > traversal_target {
                    traversal_closest.pop();
                }
            }
            if filter.contains(neighbor) && !excluded(neighbor)? {
                eligible.push(ranked);
                if eligible.len() > rerank_target {
                    eligible.pop();
                }
            }
            state.stats.frontier_peak = state.stats.frontier_peak.max(frontier.len());
        }
        if state.completion == SearchCompletion::BudgetExhausted {
            break;
        }
    }

    let mut candidates = eligible.into_vec();
    candidates.sort();
    let mut neighbors = Vec::with_capacity(request.k.min(candidates.len()));
    for candidate in candidates {
        if neighbors.len() == request.k {
            break;
        }
        if state.distance_exhausted() {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        }
        if request
            .budget
            .max_nodes
            .is_some_and(|limit| state.stats.nodes_read >= limit)
        {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let Some((record, bytes)) = map.get_stored(&candidate.key)? else {
            return Err(Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "result key is absent from authoritative directory".to_owned(),
            });
        };
        if request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| state.stats.committed_bytes.saturating_add(bytes) > limit)
        {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        }
        state.stats.nodes_read += 1;
        state.stats.bytes_read = state.stats.bytes_read.saturating_add(bytes);
        state.stats.committed_bytes = state.stats.committed_bytes.saturating_add(bytes);
        let Some(distance) = state.distance(&query, &record.vector) else {
            break;
        };
        state.stats.reranked_candidates += 1;
        neighbors.push(Neighbor {
            key: candidate.key,
            value: record.value,
            distance,
        });
    }
    neighbors.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.key.cmp(&right.key))
    });
    neighbors.truncate(request.k);
    Ok(SearchResult {
        neighbors,
        stats: state.stats,
        completion: state.completion,
        plan: state.plan,
    })
}

struct SearchState<'a, S: Store> {
    index: &'a HnswIndex<S>,
    request: &'a SearchRequest<'a>,
    stats: ProximitySearchStats,
    completion: SearchCompletion,
    loaded: BTreeMap<Vec<u8>, GraphNode>,
    plan: SearchPlanSummary,
}

impl<S> SearchState<'_, S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    fn node(&mut self, key: &[u8]) -> Result<Option<GraphNode>, Error> {
        if let Some(node) = self.loaded.get(key) {
            return Ok(Some(node.clone()));
        }
        if self
            .request
            .budget
            .max_nodes
            .is_some_and(|maximum| self.stats.nodes_read >= maximum)
        {
            self.completion = SearchCompletion::BudgetExhausted;
            return Ok(None);
        }
        let bytes = self
            .index
            .graph
            .get(&self.index.graph_tree, key)?
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "graph neighbor key is absent".to_owned(),
            })?;
        if self
            .request
            .budget
            .max_committed_bytes
            .is_some_and(|maximum| self.stats.committed_bytes.saturating_add(bytes.len()) > maximum)
        {
            self.completion = SearchCompletion::BudgetExhausted;
            return Ok(None);
        }
        let node = GraphNode::decode(&bytes)?;
        if node.level > self.index.maximum_level
            || node.routing_vector_encoding != self.index.config.routing_vector_encoding
            || node.routing_vector.len() != self.index.dimensions as usize
            || node
                .neighbors
                .iter()
                .any(|layer| layer.len() > usize::from(self.index.config.max_connections))
            || node
                .neighbors
                .iter()
                .flatten()
                .any(|neighbor| neighbor.as_slice() == key)
        {
            return Err(Error::InvalidProximityObject {
                kind: "HNSW",
                reason:
                    "graph node violates manifest vector, level, degree, or self-edge constraints"
                        .to_owned(),
            });
        }
        self.stats.nodes_read += 1;
        self.stats.bytes_read += bytes.len();
        self.stats.committed_bytes += bytes.len();
        self.loaded.insert(key.to_vec(), node.clone());
        Ok(Some(node))
    }

    fn distance(&mut self, query: &[f32], vector: &[f32]) -> Option<f64> {
        if self.distance_exhausted() {
            self.completion = SearchCompletion::BudgetExhausted;
            return None;
        }
        self.stats.distance_evaluations += 1;
        Some(query_score(
            self.request.kernel,
            self.index.metric,
            query,
            vector,
        ))
    }

    fn distance_exhausted(&self) -> bool {
        self.request
            .budget
            .max_distance_evaluations
            .is_some_and(|maximum| self.stats.distance_evaluations >= maximum)
    }

    fn frontier_allows(&mut self, entries: usize) -> bool {
        if self
            .request
            .budget
            .max_frontier_entries
            .is_some_and(|maximum| entries > maximum)
        {
            self.completion = SearchCompletion::BudgetExhausted;
            false
        } else {
            true
        }
    }

    fn finish_without_candidates(self) -> SearchResult {
        SearchResult {
            neighbors: Vec::new(),
            stats: self.stats,
            completion: self.completion,
            plan: self.plan,
        }
    }
}
