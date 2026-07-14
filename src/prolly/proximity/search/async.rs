use super::{
    adaptive_should_stop, engine::insert_top_k, AdaptiveContext, FrontierEntry, PreparedFilter,
    SearchCandidate, SearchRequest,
};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, score};
use crate::prolly::proximity::storage::vector::ExternalVector;
use crate::prolly::proximity::storage::{
    Descriptor, PhysicalNodeKind, ProximityNode, StoredRecord, VectorRef,
};
use crate::prolly::proximity::{
    DistanceMetric, Neighbor, ProximitySearchStats, ProximityTree, SearchCompletion, SearchPolicy,
    SearchResult,
};
use crate::prolly::store::AsyncStore;
use crate::prolly::AsyncProlly;
use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsyncIoConfig {
    pub max_in_flight_reads: usize,
    pub prefetch_window: usize,
    pub max_buffered_bytes: usize,
}

impl Default for AsyncIoConfig {
    fn default() -> Self {
        Self {
            max_in_flight_reads: 8,
            prefetch_window: 16,
            max_buffered_bytes: 8 * 1024 * 1024,
        }
    }
}

impl AsyncIoConfig {
    fn validate(&self) -> Result<(), Error> {
        if self.max_in_flight_reads == 0
            || self.prefetch_window == 0
            || self.max_buffered_bytes == 0
        {
            return Err(Error::InvalidProximitySearch {
                reason: "async I/O limits must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default)]
pub struct CancellationToken(Arc<AtomicBool>);

impl CancellationToken {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Clone, Debug, Default)]
pub struct AsyncSearchControl {
    pub io: AsyncIoConfig,
    pub cancellation: Option<CancellationToken>,
    pub deadline: Option<Instant>,
}

pub struct AsyncProximityMap<S: AsyncStore> {
    store: S,
    directory: AsyncProlly<S>,
    tree: ProximityTree,
}

impl<S> AsyncProximityMap<S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    pub async fn load(store: S, descriptor_cid: Cid) -> Result<Self, Error> {
        let descriptor_bytes = load_content(&store, &descriptor_cid).await?;
        let descriptor = Descriptor::decode(&descriptor_bytes)?;
        let root_bytes = load_content(&store, &descriptor.proximity_root).await?;
        let root = ProximityNode::decode(&root_bytes, descriptor.config.dimensions)?;
        if root.subtree_count != descriptor.count {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "record count disagrees with proximity root".to_owned(),
            });
        }
        let directory = AsyncProlly::new(store.clone(), descriptor.directory.config.clone());
        Ok(Self {
            store,
            directory,
            tree: ProximityTree {
                directory: descriptor.directory,
                proximity_root: descriptor.proximity_root,
                descriptor: descriptor_cid,
                count: descriptor.count,
                config: descriptor.config,
            },
        })
    }

    pub fn tree(&self) -> &ProximityTree {
        &self.tree
    }

    pub async fn search(
        &self,
        request: SearchRequest<'_>,
        control: AsyncSearchControl,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        control.io.validate()?;
        let filter = PreparedFilter::new(request.filter.clone(), &self.tree.directory)?;
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        let mut stats = ProximitySearchStats::default();
        let mut frontier = BinaryHeap::new();
        frontier.push(FrontierEntry {
            bound: 0.0,
            score: 0.0,
            key: Vec::new(),
            cid: self.tree.proximity_root.clone(),
            expected_level: None,
        });
        let mut candidates = Vec::<SearchCandidate>::new();
        let mut score_cache = BTreeMap::<Vec<u8>, f64>::new();
        let mut visited = HashSet::new();
        let mut levels = HashSet::new();
        let mut buffered = HashMap::<Cid, Vec<u8>>::new();
        let mut buffered_bytes = 0usize;
        let mut last_fanout = 0usize;
        let mut completion = SearchCompletion::Exact;

        while let Some(next) = frontier.peek() {
            if let Some(stopped) = stop_reason(&control) {
                completion = stopped;
                break;
            }
            if self.tree.config.metric == DistanceMetric::L2Squared
                && candidates.len() == request.k
                && next.bound > candidates.last().expect("full top-k").score
            {
                break;
            }
            if let SearchPolicy::Adaptive(quality) = request.policy {
                if candidates.last().is_some_and(|worst| {
                    let overlapping = frontier
                        .iter()
                        .filter(|entry| entry.bound <= worst.score)
                        .count();
                    adaptive_should_stop(
                        quality,
                        AdaptiveContext {
                            results: candidates.len(),
                            k: request.k,
                            frontier_bound: next.bound,
                            worst_score: worst.score,
                            overlapping_clusters: overlapping,
                            logical_level: next.expected_level.unwrap_or(u8::MAX),
                            last_fanout,
                            cluster_count: frontier.len(),
                        },
                    )
                }) {
                    completion = SearchCompletion::ApproximatePolicySatisfied;
                    break;
                }
            }
            if request
                .budget
                .max_nodes
                .is_some_and(|maximum| stats.nodes_read >= maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }

            prefetch(
                &self.store,
                &frontier,
                &control,
                &mut buffered,
                &mut buffered_bytes,
                &mut stats,
            )
            .await?;
            if let Some(stopped) = stop_reason(&control) {
                completion = stopped;
                break;
            }
            let next = frontier.pop().expect("peeked frontier");
            if !visited.insert(next.cid.clone()) {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "cycle or repeated child ownership".to_owned(),
                });
            }
            let bytes = match buffered.remove(&next.cid) {
                Some(bytes) => {
                    buffered_bytes -= bytes.len();
                    bytes
                }
                None => {
                    let bytes = load_content(&self.store, &next.cid).await?;
                    stats.physical_bytes_read += bytes.len();
                    bytes
                }
            };
            if let Some(stopped) = stop_reason(&control) {
                completion = stopped;
                break;
            }
            let mut node = ProximityNode::decode(&bytes, self.tree.config.dimensions)?;
            let mut committed = bytes.len();
            for entry in &mut node.entries {
                let VectorRef::External(cid) = &entry.vector else {
                    continue;
                };
                let bytes = load_content(&self.store, cid).await?;
                if let Some(stopped) = stop_reason(&control) {
                    completion = stopped;
                    break;
                }
                stats.physical_bytes_read += bytes.len();
                committed += bytes.len();
                let external = ExternalVector::decode(&bytes)?;
                if external.vector.len() != self.tree.config.dimensions as usize {
                    return Err(Error::InvalidProximityObject {
                        kind: "vector",
                        reason: "external vector dimension mismatch".to_owned(),
                    });
                }
                entry.vector = VectorRef::Inline(external.vector);
            }
            if completion != SearchCompletion::Exact {
                break;
            }
            if next
                .expected_level
                .is_some_and(|expected| node.level != expected)
            {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "child has an unexpected logical level".to_owned(),
                });
            }
            if request
                .budget
                .max_committed_bytes
                .is_some_and(|maximum| stats.committed_bytes.saturating_add(committed) > maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            stats.nodes_read += 1;
            stats.bytes_read += committed;
            stats.committed_bytes += committed;
            levels.insert(node.level);
            stats.levels_visited = levels.len();
            last_fanout = node.entries.len();

            for entry in &node.entries {
                if let Some(stopped) = stop_reason(&control) {
                    completion = stopped;
                    break;
                }
                if node.kind.has_children(node.level) {
                    if !filter.intersects(&entry.min_key, &entry.max_key) {
                        continue;
                    }
                    let child = entry.child.clone().expect("validated internal child");
                    let representative_score = match score_cache.get(&entry.key) {
                        Some(score) => *score,
                        None => {
                            if distance_budget_exhausted(&request, &stats) {
                                completion = SearchCompletion::BudgetExhausted;
                                break;
                            }
                            stats.distance_evaluations += 1;
                            let value =
                                score(self.tree.config.metric, &query, entry.vector.inline()?);
                            score_cache.insert(entry.key.clone(), value);
                            value
                        }
                    };
                    let bound = if self.tree.config.metric == DistanceMetric::L2Squared {
                        crate::prolly::proximity::distance::canonical::l2_lower_bound_down(
                            representative_score,
                            entry.covering_radius,
                        )
                    } else {
                        representative_score
                    };
                    if request
                        .budget
                        .max_frontier_entries
                        .is_some_and(|maximum| frontier.len() >= maximum)
                    {
                        completion = SearchCompletion::BudgetExhausted;
                        break;
                    }
                    frontier.push(FrontierEntry {
                        bound,
                        score: representative_score,
                        key: entry.key.clone(),
                        cid: child,
                        expected_level: Some(if node.kind == PhysicalNodeKind::OverflowDirectory {
                            node.level
                        } else {
                            node.level - 1
                        }),
                    });
                    stats.frontier_peak = stats.frontier_peak.max(frontier.len());
                } else if filter.contains(&entry.key) {
                    let leaf_score = match score_cache.get(&entry.key) {
                        Some(score) => *score,
                        None => {
                            if distance_budget_exhausted(&request, &stats) {
                                completion = SearchCompletion::BudgetExhausted;
                                break;
                            }
                            stats.distance_evaluations += 1;
                            let value =
                                score(self.tree.config.metric, &query, entry.vector.inline()?);
                            score_cache.insert(entry.key.clone(), value);
                            value
                        }
                    };
                    insert_top_k(
                        &mut candidates,
                        SearchCandidate {
                            key: entry.key.clone(),
                            vector: entry.vector.inline()?.to_vec(),
                            score: leaf_score,
                        },
                        request.k,
                    );
                }
            }
            if completion != SearchCompletion::Exact {
                break;
            }
        }

        let mut neighbors = Vec::with_capacity(candidates.len());
        for candidate in candidates {
            let bytes = self
                .directory
                .get(&self.tree.directory, &candidate.key)
                .await?
                .ok_or_else(|| Error::InvalidProximityObject {
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
                distance: candidate.score,
            });
        }
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
        })
    }
}

async fn prefetch<S>(
    store: &S,
    frontier: &BinaryHeap<FrontierEntry>,
    control: &AsyncSearchControl,
    buffered: &mut HashMap<Cid, Vec<u8>>,
    buffered_bytes: &mut usize,
    stats: &mut ProximitySearchStats,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut ordered = frontier.clone();
    let mut cids = Vec::new();
    let limit = control
        .io
        .prefetch_window
        .min(control.io.max_in_flight_reads);
    while cids.len() < limit {
        let Some(entry) = ordered.pop() else { break };
        if !buffered.contains_key(&entry.cid) {
            cids.push(entry.cid);
        }
    }
    if cids.is_empty() {
        return Ok(());
    }
    let keys: Vec<_> = cids.iter().map(Cid::as_bytes).collect();
    let values = store
        .batch_get_ordered_unique(&keys)
        .await
        .map_err(|error| Error::Store(Box::new(error)))?;
    for (cid, value) in cids.into_iter().zip(values) {
        let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
        let actual = Cid::from_bytes(&bytes);
        if actual != cid {
            return Err(Error::CidMismatch {
                expected: cid,
                actual,
            });
        }
        stats.physical_bytes_read += bytes.len();
        if buffered_bytes.saturating_add(bytes.len()) <= control.io.max_buffered_bytes {
            *buffered_bytes += bytes.len();
            buffered.insert(cid, bytes);
        }
    }
    Ok(())
}

fn stop_reason(control: &AsyncSearchControl) -> Option<SearchCompletion> {
    if control
        .cancellation
        .as_ref()
        .is_some_and(CancellationToken::is_cancelled)
    {
        Some(SearchCompletion::Cancelled)
    } else if control
        .deadline
        .is_some_and(|deadline| Instant::now() >= deadline)
    {
        Some(SearchCompletion::DeadlineExceeded)
    } else {
        None
    }
}

fn distance_budget_exhausted(request: &SearchRequest<'_>, stats: &ProximitySearchStats) -> bool {
    request
        .budget
        .max_distance_evaluations
        .is_some_and(|maximum| stats.distance_evaluations >= maximum)
}

async fn load_content<S>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let bytes = store
        .get(cid.as_bytes())
        .await
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
