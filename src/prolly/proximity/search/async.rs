use super::{
    adaptive_should_stop, engine::insert_top_k, insert_reranked_top_k, retained_candidate_bytes,
    retained_search_candidate_bytes, AdaptiveContext, FrontierEntry, PreparedFilter,
    RerankCandidate, SearchCandidate, SearchRequest,
};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::accelerator::hnsw::storage::GraphNode;
use crate::prolly::proximity::accelerator::pq::{build_lookup, score_code, validate_code};
use crate::prolly::proximity::accelerator::{
    AsyncAcceleratorSet, AsyncCompositeAccelerator, AsyncCompositeBase, AsyncHnswIndex,
    AsyncProductQuantizer,
};
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::storage::quantized::ScalarQuantized;
use crate::prolly::proximity::storage::vector::ExternalVector;
use crate::prolly::proximity::storage::{Descriptor, PhysicalNodeKind, ProximityNode, VectorRef};
use crate::prolly::proximity::{
    DistanceMetric, Neighbor, ProximitySearchStats, ProximityTree, SearchBackend, SearchCompletion,
    SearchPolicy, SearchResult,
};
use crate::prolly::store::AsyncStore;
use crate::prolly::AsyncProlly;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsyncIoConfig {
    pub max_in_flight_reads: usize,
    pub prefetch_window: usize,
    pub max_buffered_bytes: usize,
}

impl<S> AsyncProximityMap<super::SearchIo<S>>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    /// Execute native async search with physical I/O derived from the shared
    /// runtime rather than from logical object consumption.
    pub async fn search_with_runtime(
        &self,
        request: SearchRequest<'_>,
        control: AsyncSearchControl,
    ) -> Result<SearchResult, Error> {
        let before = self.store.physical_bytes_read();
        let mut result = self.search(request, control).await?;
        result.stats.physical_bytes_read = self.store.physical_bytes_read().saturating_sub(before);
        Ok(result)
    }

    /// Plan and execute native, eligible-exact, PQ, or HNSW search through an
    /// async-only store using the same logical planner as synchronous search.
    pub async fn search_with_accelerators(
        &self,
        accelerators: &AsyncAcceleratorSet,
        request: SearchRequest<'_>,
        control: AsyncSearchControl,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        control.io.validate()?;
        let before = self.store.physical_bytes_read();
        let eligibility = PreparedFilter::new(request.filter.clone(), &self.tree.directory)?;
        let plan = super::plan_search_capabilities(
            &self.tree,
            accelerators.hnsw().map(AsyncHnswIndex::config),
            accelerators.pq().map(AsyncProductQuantizer::config),
            accelerators
                .composite()
                .map(|composite| super::planner::CompositePlanInput {
                    base_kind: composite.base_kind(),
                    hnsw: composite.hnsw().map(AsyncHnswIndex::config),
                    pq: composite.pq().map(AsyncProductQuantizer::config),
                    base_count: composite.base_count,
                    delta_count: composite.delta_count,
                    shadow_count: composite.shadow_count,
                    config: &composite.config,
                }),
            &request,
            &eligibility,
        )?;
        let mut result = match &plan {
            super::SearchPlan::Native => {
                let mut native = request;
                native.options.backend = SearchBackend::Native;
                self.search(native, control).await
            }
            super::SearchPlan::EligibleExact { .. } => {
                self.search_eligible_exact_async(request, &eligibility, &plan, &control)
                    .await
            }
            super::SearchPlan::ProductQuantized { .. } => {
                search_pq_async(
                    &self.store,
                    &self.directory,
                    &self.tree,
                    accelerators.pq().expect("planner validated PQ"),
                    request,
                    &eligibility,
                    &plan,
                    &control,
                    None,
                )
                .await
            }
            super::SearchPlan::Hnsw { .. } => {
                search_hnsw_async(
                    &self.store,
                    &self.directory,
                    &self.tree,
                    accelerators.hnsw().expect("planner validated HNSW"),
                    request,
                    &eligibility,
                    &plan,
                    &control,
                    None,
                )
                .await
            }
            super::SearchPlan::Composite { .. } => {
                search_composite_async(
                    &self.store,
                    &self.directory,
                    &self.tree,
                    accelerators
                        .composite()
                        .expect("planner validated composite"),
                    request,
                    &eligibility,
                    &plan,
                    &control,
                )
                .await
            }
        }?;
        result.stats.physical_bytes_read = self.store.physical_bytes_read().saturating_sub(before);
        Ok(result)
    }

    async fn search_eligible_exact_async(
        &self,
        request: SearchRequest<'_>,
        eligibility: &PreparedFilter<'_>,
        plan: &super::SearchPlan,
        control: &AsyncSearchControl,
    ) -> Result<SearchResult, Error> {
        let super::SearchPlan::EligibleExact {
            key_count,
            source_bound,
        } = plan
        else {
            unreachable!("called only for eligible-exact plan")
        };
        let Some((keys, prepared_source_bound)) = eligibility.sorted_keys() else {
            return Err(invalid_search(
                "eligible-exact plan requires sorted eligible keys",
            ));
        };
        if *key_count != keys.len() as u64 || *source_bound != prepared_source_bound {
            return Err(invalid_search(
                "eligible-exact plan disagrees with prepared eligibility",
            ));
        }
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        let mut stats = ProximitySearchStats::default();
        let limit = request
            .budget
            .max_frontier_entries
            .unwrap_or(request.k)
            .min(request.k);
        let mut completion = if limit < request.k.min(keys.len()) {
            SearchCompletion::BudgetExhausted
        } else {
            SearchCompletion::Exact
        };
        let mut ranked = Vec::<RerankCandidate>::with_capacity(limit);
        let mut vector_scratch = vec![0.0f32; self.tree.config.dimensions as usize];
        let mut directory = self.directory.read(&self.tree.directory).await?;
        for key in keys {
            if let Some(stopped) = stop_reason(control) {
                completion = stopped;
                break;
            }
            if budget_stops_record(&request, &stats, 0) {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let Some(handle) = directory.get_handle(key).await? else {
                if *source_bound {
                    return Err(Error::InvalidProximityObject {
                        kind: "eligible keys",
                        reason: "source-bound key is absent".to_owned(),
                    });
                }
                continue;
            };
            let bytes = handle.value()?.len();
            if request
                .budget
                .max_committed_bytes
                .is_some_and(|maximum| stats.committed_bytes.saturating_add(bytes) > maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            };
            let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
                handle.value()?,
                self.tree.config.dimensions,
            )?;
            crate::prolly::proximity::ProximityVectorRef::from_encoded(record.vector)
                .copy_to_slice(&mut vector_scratch)?;
            let distance = query_score(
                request.kernel,
                self.tree.config.metric,
                &query,
                &vector_scratch,
            );
            insert_reranked_top_k(
                &mut ranked,
                RerankCandidate::new(handle, key, distance)?,
                limit,
            );
            stats.nodes_read += 1;
            stats.bytes_read += bytes;
            stats.committed_bytes += bytes;
            stats.distance_evaluations += 1;
            stats.candidate_handles_peak = stats.candidate_handles_peak.max(ranked.len());
            stats.candidate_retained_bytes_peak = stats
                .candidate_retained_bytes_peak
                .max(retained_candidate_bytes(&ranked));
        }
        stats.reranked_candidates = stats.distance_evaluations;
        let neighbors = ranked
            .into_iter()
            .map(|candidate| candidate.into_neighbor(self.tree.config.dimensions))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: plan.summary(),
        })
    }
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
        if matches!(
            request.options.backend,
            SearchBackend::ProductQuantized | SearchBackend::Hnsw
        ) {
            return Err(Error::InvalidProximitySearch {
                reason: "requested backend requires a validated accelerator sidecar".to_owned(),
            });
        }
        let filter = PreparedFilter::new(request.filter.clone(), &self.tree.directory)?;
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        let use_scalar_quantization =
            crate::prolly::proximity::accelerator::sq8::enabled(&self.tree.config, request.policy);
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
            if !use_scalar_quantization
                && self.tree.config.metric == DistanceMetric::L2Squared
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
            let node = Arc::new(node);
            let quantizer = if use_scalar_quantization && node.kind.has_children(node.level) {
                let config = self
                    .tree
                    .config
                    .scalar_quantization
                    .as_ref()
                    .expect("checked scalar quantization configuration");
                let cid = node
                    .quantizer
                    .as_ref()
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "quantizer",
                        reason: "configured node has no scalar quantizer".to_owned(),
                    })?;
                let bytes = load_content(&self.store, cid).await?;
                stats.physical_bytes_read += bytes.len();
                committed += bytes.len();
                let quantizer = ScalarQuantized::decode(&bytes)?;
                if quantizer.dimensions != self.tree.config.dimensions
                    || quantizer.group_size != config.group_size
                {
                    return Err(Error::InvalidProximityObject {
                        kind: "quantizer",
                        reason: "quantizer configuration disagrees with descriptor".to_owned(),
                    });
                }
                if quantizer.entry_count != node.entries.len() as u64 {
                    return Err(Error::InvalidProximityObject {
                        kind: "quantizer",
                        reason: "quantizer entry count disagrees with node".to_owned(),
                    });
                }
                Some(quantizer)
            } else {
                None
            };
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

            for (entry_index, entry) in node.entries.iter().enumerate() {
                if let Some(stopped) = stop_reason(&control) {
                    completion = stopped;
                    break;
                }
                if node.kind.has_children(node.level) {
                    if !filter.intersects(&entry.min_key, &entry.max_key) {
                        continue;
                    }
                    let child = entry.child.clone().expect("validated internal child");
                    let representative_score = if let Some(quantizer) = &quantizer {
                        if distance_budget_exhausted(&request, &stats) {
                            completion = SearchCompletion::BudgetExhausted;
                            break;
                        }
                        stats.quantized_distance_evaluations += 1;
                        quantizer.approximate_score(self.tree.config.metric, &query, entry_index)?
                    } else {
                        match score_cache.get(&entry.key) {
                            Some(score) => *score,
                            None => {
                                if distance_budget_exhausted(&request, &stats) {
                                    completion = SearchCompletion::BudgetExhausted;
                                    break;
                                }
                                stats.distance_evaluations += 1;
                                let value = query_score(
                                    request.kernel,
                                    self.tree.config.metric,
                                    &query,
                                    entry.vector.inline()?,
                                );
                                score_cache.insert(entry.key.clone(), value);
                                value
                            }
                        }
                    };
                    let bound = if quantizer.is_none()
                        && self.tree.config.metric == DistanceMetric::L2Squared
                    {
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
                            let value = query_score(
                                request.kernel,
                                self.tree.config.metric,
                                &query,
                                entry.vector.inline()?,
                            );
                            score_cache.insert(entry.key.clone(), value);
                            value
                        }
                    };
                    insert_top_k(
                        &mut candidates,
                        SearchCandidate::new(node.clone(), entry_index, leaf_score),
                        request.k,
                    );
                }
            }
            if completion != SearchCompletion::Exact {
                break;
            }
        }

        if use_scalar_quantization {
            stats.reranked_candidates = candidates.len();
        }
        stats.candidate_handles_peak = stats.candidate_handles_peak.max(candidates.len());
        stats.candidate_retained_bytes_peak = stats
            .candidate_retained_bytes_peak
            .max(retained_search_candidate_bytes(&candidates));
        let mut neighbors = Vec::with_capacity(candidates.len());
        let mut directory_session = self.directory.read(&self.tree.directory).await?;
        for candidate in candidates {
            // This is the final owned result key; no borrowed candidate slice
            // crosses the directory await.
            let key = candidate.key()?.to_vec();
            let handle = directory_session.get_handle(&key).await?.ok_or_else(|| {
                Error::InvalidProximityObject {
                    kind: "node",
                    reason: "leaf key is absent from exact directory".to_owned(),
                }
            })?;
            let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
                handle.value()?,
                self.tree.config.dimensions,
            )?;
            if !super::super::map::encoded_vector_matches(record.vector, candidate.vector()?) {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "leaf vector disagrees with exact directory".to_owned(),
                });
            }
            neighbors.push(Neighbor {
                key,
                value: record.value.to_vec(),
                distance: candidate.score,
            });
        }
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: super::SearchPlan::Native.summary(),
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
        .is_some_and(|maximum| {
            stats
                .distance_evaluations
                .saturating_add(stats.quantized_distance_evaluations)
                >= maximum
        })
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

fn invalid_search(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}

fn budget_stops_record(
    request: &SearchRequest<'_>,
    stats: &ProximitySearchStats,
    bytes: usize,
) -> bool {
    request
        .budget
        .max_nodes
        .is_some_and(|limit| stats.nodes_read >= limit)
        || request
            .budget
            .max_distance_evaluations
            .is_some_and(|limit| {
                stats
                    .distance_evaluations
                    .saturating_add(stats.quantized_distance_evaluations)
                    >= limit
            })
        || request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes) > limit)
}

#[derive(Clone, Debug)]
struct AsyncRanked {
    distance: f64,
    key: Vec<u8>,
}

impl PartialEq for AsyncRanked {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.key == other.key
    }
}
impl Eq for AsyncRanked {}
impl PartialOrd for AsyncRanked {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for AsyncRanked {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.key.cmp(&other.key))
    }
}

#[allow(clippy::too_many_arguments)]
async fn search_pq_async<S>(
    store: &super::SearchIo<S>,
    directory: &AsyncProlly<super::SearchIo<S>>,
    tree: &ProximityTree,
    index: &AsyncProductQuantizer,
    request: SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    plan: &super::SearchPlan,
    control: &AsyncSearchControl,
    excluded: Option<&BTreeSet<Vec<u8>>>,
) -> Result<SearchResult, Error>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    let super::SearchPlan::ProductQuantized {
        rerank_target,
        direct_lookup,
    } = plan
    else {
        return Err(invalid_search("PQ executor requires a PQ plan"));
    };
    let query = prepare_vector(index.metric, request.query, index.dimensions)?;
    let lookup = build_lookup(&query, index.metric, &index.codebooks);
    let code_store =
        store.for_kind(crate::prolly::content_graph::ContentObjectKind::ProductQuantization);
    let codes = AsyncProlly::new(code_store, index.code_tree.config.clone());
    let mut stats = ProximitySearchStats::default();
    let mut approximate = BinaryHeap::<AsyncRanked>::new();
    let mut completion = SearchCompletion::ApproximatePolicySatisfied;

    if *direct_lookup {
        let Some((keys, source_bound)) = eligibility.sorted_keys() else {
            return Err(invalid_search(
                "PQ direct lookup requires sorted eligible keys",
            ));
        };
        for key in keys {
            if excluded.is_some_and(|excluded| excluded.contains(key)) {
                continue;
            }
            if let Some(stopped) = stop_reason(control) {
                completion = stopped;
                break;
            }
            let Some(code) = codes.get(&index.code_tree, key).await? else {
                if source_bound {
                    return Err(Error::InvalidProximityObject {
                        kind: "product quantizer",
                        reason: "source-bound key has no PQ code".to_owned(),
                    });
                }
                continue;
            };
            if !admit_async_code(
                key.clone(),
                code,
                &lookup,
                index,
                *rerank_target,
                &request,
                &mut stats,
                &mut approximate,
            )? {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
        }
    } else {
        let mut range = codes.range(&index.code_tree, &[], None).await?;
        while let Some(entry) = range.next().await {
            if let Some(stopped) = stop_reason(control) {
                completion = stopped;
                break;
            }
            let (key, code) = entry?;
            if excluded.is_some_and(|excluded| excluded.contains(&key)) {
                continue;
            }
            if eligibility.contains(&key)
                && !admit_async_code(
                    key,
                    code,
                    &lookup,
                    index,
                    *rerank_target,
                    &request,
                    &mut stats,
                    &mut approximate,
                )?
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
        }
    }
    let mut approximate = approximate.into_vec();
    approximate.sort();
    let mut reranked = Vec::<RerankCandidate>::with_capacity(approximate.len());
    let mut vector_scratch = vec![0.0f32; tree.config.dimensions as usize];
    let mut directory_session = directory.read(&tree.directory).await?;
    for candidate in approximate {
        if let Some(stopped) = stop_reason(control) {
            completion = stopped;
            break;
        }
        if budget_stops_record(&request, &stats, 0) {
            completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let Some(handle) = directory_session.get_handle(&candidate.key).await? else {
            return Err(Error::InvalidProximityObject {
                kind: "product quantizer",
                reason: "PQ code key is absent from authoritative directory".to_owned(),
            });
        };
        let bytes = handle.value()?.len();
        if request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes) > limit)
        {
            completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
            handle.value()?,
            tree.config.dimensions,
        )?;
        crate::prolly::proximity::ProximityVectorRef::from_encoded(record.vector)
            .copy_to_slice(&mut vector_scratch)?;
        let distance = query_score(request.kernel, index.metric, &query, &vector_scratch);
        stats.nodes_read += 1;
        stats.bytes_read += bytes;
        stats.committed_bytes += bytes;
        stats.distance_evaluations += 1;
        reranked.push(RerankCandidate::new(handle, &candidate.key, distance)?);
    }
    stats.reranked_candidates = reranked.len();
    stats.candidate_handles_peak = reranked.len();
    stats.candidate_retained_bytes_peak = retained_candidate_bytes(&reranked);
    reranked.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.key().cmp(right.key()))
    });
    let neighbors = reranked
        .into_iter()
        .take(request.k)
        .map(|candidate| candidate.into_neighbor(tree.config.dimensions))
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(SearchResult {
        neighbors,
        stats,
        completion,
        plan: plan.summary(),
    })
}

#[allow(clippy::too_many_arguments)]
fn admit_async_code(
    key: Vec<u8>,
    code: Vec<u8>,
    lookup: &[Vec<f64>],
    index: &AsyncProductQuantizer,
    target: usize,
    request: &SearchRequest<'_>,
    stats: &mut ProximitySearchStats,
    approximate: &mut BinaryHeap<AsyncRanked>,
) -> Result<bool, Error> {
    if budget_stops_record(request, stats, code.len())
        || request
            .budget
            .max_frontier_entries
            .is_some_and(|limit| approximate.len().saturating_add(1) > limit)
    {
        return Ok(false);
    }
    validate_code(&code, &index.codebooks)?;
    stats.nodes_read += 1;
    stats.bytes_read += code.len();
    stats.committed_bytes += code.len();
    stats.quantized_distance_evaluations += 1;
    approximate.push(AsyncRanked {
        distance: score_code(index.metric, lookup, &code),
        key,
    });
    if approximate.len() > target {
        approximate.pop();
    }
    stats.frontier_peak = stats.frontier_peak.max(approximate.len());
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
async fn search_composite_async<S>(
    store: &super::SearchIo<S>,
    directory: &AsyncProlly<super::SearchIo<S>>,
    tree: &ProximityTree,
    composite: &AsyncCompositeAccelerator,
    request: SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    plan: &super::SearchPlan,
    control: &AsyncSearchControl,
) -> Result<SearchResult, Error>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    let super::SearchPlan::Composite {
        base,
        delta_records,
        shadow_records,
        merge_target,
    } = plan
    else {
        return Err(invalid_search(
            "async composite executor requires a composite plan",
        ));
    };
    if composite.current_source != tree.descriptor
        || composite.delta_count as usize != *delta_records
        || composite.shadow_count as usize != *shadow_records
    {
        return Err(Error::InvalidProximityObject {
            kind: "composite accelerator",
            reason: "async plan or source disagrees with manifest".to_owned(),
        });
    }
    let ordered_store =
        store.for_kind(crate::prolly::content_graph::ContentObjectKind::OrderedNode);
    let shadow_manager =
        AsyncProlly::new(ordered_store.clone(), composite.shadow_tree.config.clone());
    let delta_manager = AsyncProlly::new(ordered_store, composite.delta_tree.config.clone());
    let mut stats = ProximitySearchStats::default();
    let mut shadow = BTreeSet::new();
    let mut shadow_range = shadow_manager
        .range(&composite.shadow_tree, &[], None)
        .await?;
    while let Some(entry) = shadow_range.next().await {
        if let Some(stopped) = stop_reason(control) {
            return Ok(SearchResult {
                neighbors: Vec::new(),
                stats,
                completion: stopped,
                plan: plan.summary(),
            });
        }
        let (key, value) = entry?;
        if !value.is_empty() || !shadow.insert(key.clone()) {
            return Err(Error::InvalidProximityObject {
                kind: "composite shadow",
                reason: "shadow tree contains a value or duplicate key".to_owned(),
            });
        }
        if request
            .budget
            .max_nodes
            .is_some_and(|limit| stats.nodes_read.saturating_add(1) > limit)
            || request
                .budget
                .max_committed_bytes
                .is_some_and(|limit| stats.committed_bytes.saturating_add(key.len()) > limit)
        {
            return Ok(SearchResult {
                neighbors: Vec::new(),
                stats,
                completion: SearchCompletion::BudgetExhausted,
                plan: plan.summary(),
            });
        }
        stats.nodes_read += 1;
        stats.bytes_read += key.len();
        stats.committed_bytes += key.len();
    }
    if shadow.len() != *shadow_records {
        return Err(Error::InvalidProximityObject {
            kind: "composite shadow",
            reason: "shadow cardinality disagrees with manifest".to_owned(),
        });
    }
    if async_budget_exhausted(&request, &stats) {
        return Ok(SearchResult {
            neighbors: Vec::new(),
            stats,
            completion: SearchCompletion::BudgetExhausted,
            plan: plan.summary(),
        });
    }
    let mut base_request = request.clone();
    base_request.budget.max_nodes = base_request
        .budget
        .max_nodes
        .map(|limit| limit - stats.nodes_read);
    base_request.budget.max_committed_bytes = base_request
        .budget
        .max_committed_bytes
        .map(|limit| limit - stats.committed_bytes);
    base_request.budget.max_distance_evaluations = base_request
        .budget
        .max_distance_evaluations
        .map(|limit| limit - stats.distance_evaluations - stats.quantized_distance_evaluations);
    let mut base_result = match &composite.base {
        AsyncCompositeBase::Hnsw(index) => {
            search_hnsw_async(
                store,
                directory,
                tree,
                index,
                base_request,
                eligibility,
                base,
                control,
                Some(&shadow),
            )
            .await
        }
        AsyncCompositeBase::ProductQuantized(index) => {
            search_pq_async(
                store,
                directory,
                tree,
                index,
                base_request,
                eligibility,
                base,
                control,
                Some(&shadow),
            )
            .await
        }
    }?;
    add_async_stats(&mut stats, &base_result.stats);
    let mut completion = base_result.completion;
    let query = prepare_vector(tree.config.metric, request.query, tree.config.dimensions)?;
    enum CompositeValue {
        Owned { value: Vec<u8>, distance: f64 },
        Retained(RerankCandidate),
    }
    impl CompositeValue {
        fn distance(&self) -> f64 {
            match self {
                Self::Owned { distance, .. } => *distance,
                Self::Retained(candidate) => candidate.distance,
            }
        }
    }
    let mut merged = BTreeMap::<Vec<u8>, CompositeValue>::new();
    for neighbor in base_result.neighbors.drain(..) {
        if merged
            .insert(
                neighbor.key,
                CompositeValue::Owned {
                    value: neighbor.value,
                    distance: neighbor.distance,
                },
            )
            .is_some()
        {
            return Err(Error::InvalidProximityObject {
                kind: "composite base",
                reason: "base executor returned a duplicate key".to_owned(),
            });
        }
    }
    let mut delta_seen = 0usize;
    let mut delta_range = delta_manager
        .range(&composite.delta_tree, &[], None)
        .await?;
    let mut directory_session = directory.read(&tree.directory).await?;
    let mut vector_scratch = vec![0.0f32; tree.config.dimensions as usize];
    let mut retained_backings = HashSet::new();
    let mut retained_bytes = 0usize;
    while let Some(entry) = delta_range.next().await {
        if let Some(stopped) = stop_reason(control) {
            completion = stopped;
            break;
        }
        let (key, bytes) = entry?;
        delta_seen += 1;
        if request
            .budget
            .max_nodes
            .is_some_and(|limit| stats.nodes_read.saturating_add(1) > limit)
            || request
                .budget
                .max_committed_bytes
                .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes.len()) > limit)
        {
            completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let distance = {
            let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
                &bytes,
                tree.config.dimensions,
            )?;
            crate::prolly::proximity::ProximityVectorRef::from_encoded(record.vector)
                .copy_to_slice(&mut vector_scratch)?;
            query_score(request.kernel, tree.config.metric, &query, &vector_scratch)
        };
        stats.nodes_read += 1;
        stats.bytes_read += bytes.len();
        stats.committed_bytes += bytes.len();
        if !eligibility.contains(&key) {
            continue;
        }
        if request
            .budget
            .max_nodes
            .is_some_and(|limit| stats.nodes_read.saturating_add(1) > limit)
            || request
                .budget
                .max_distance_evaluations
                .is_some_and(|limit| {
                    stats
                        .distance_evaluations
                        .saturating_add(stats.quantized_distance_evaluations)
                        .saturating_add(1)
                        > limit
                })
        {
            completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let Some(handle) = directory_session.get_handle(&key).await? else {
            return Err(Error::InvalidProximityObject {
                kind: "composite delta",
                reason: "delta key is absent from current source".to_owned(),
            });
        };
        let authoritative_bytes = handle.value()?.len();
        if request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(authoritative_bytes) > limit)
        {
            completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let authoritative = crate::prolly::proximity::storage::StoredRecordRef::decode(
            handle.value()?,
            tree.config.dimensions,
        )?;
        let delta = crate::prolly::proximity::storage::StoredRecordRef::decode(
            &bytes,
            tree.config.dimensions,
        )?;
        if !super::super::map::encoded_vectors_equal(authoritative.vector, delta.vector) {
            return Err(Error::InvalidProximityObject {
                kind: "composite delta",
                reason: "delta vector disagrees with current source".to_owned(),
            });
        }
        stats.nodes_read += 1;
        stats.bytes_read += authoritative_bytes;
        stats.committed_bytes += authoritative_bytes;
        stats.distance_evaluations += 1;
        stats.reranked_candidates += 1;
        let candidate = RerankCandidate::new(handle, &key, distance)?;
        if retained_backings.insert(candidate.backing_id()) {
            retained_bytes = retained_bytes.saturating_add(candidate.retained_bytes());
        }
        if merged
            .insert(key, CompositeValue::Retained(candidate))
            .is_some()
        {
            return Err(Error::InvalidProximityObject {
                kind: "composite accelerator",
                reason: "delta key was not shadowed from base".to_owned(),
            });
        }
        stats.candidate_handles_peak = stats.candidate_handles_peak.max(retained_backings.len());
        stats.candidate_retained_bytes_peak =
            stats.candidate_retained_bytes_peak.max(retained_bytes);
    }
    if completion != SearchCompletion::BudgetExhausted
        && completion != SearchCompletion::Cancelled
        && completion != SearchCompletion::DeadlineExceeded
        && delta_seen != *delta_records
    {
        return Err(Error::InvalidProximityObject {
            kind: "composite delta",
            reason: "delta cardinality disagrees with manifest".to_owned(),
        });
    }
    let mut candidates = merged.into_iter().collect::<Vec<_>>();
    candidates.sort_by(|(left_key, left), (right_key, right)| {
        left.distance()
            .total_cmp(&right.distance())
            .then_with(|| left_key.cmp(right_key))
    });
    candidates.truncate((*merge_target).min(request.k));
    let neighbors = candidates
        .into_iter()
        .map(|(key, candidate)| match candidate {
            CompositeValue::Owned { value, distance } => Ok(Neighbor {
                key,
                value,
                distance,
            }),
            CompositeValue::Retained(candidate) => {
                let record = candidate.record(tree.config.dimensions)?;
                Ok(Neighbor {
                    key,
                    value: record.value.to_vec(),
                    distance: candidate.distance,
                })
            }
        })
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(SearchResult {
        neighbors,
        stats,
        completion,
        plan: plan.summary(),
    })
}

fn async_budget_exhausted(request: &SearchRequest<'_>, stats: &ProximitySearchStats) -> bool {
    request
        .budget
        .max_nodes
        .is_some_and(|limit| stats.nodes_read >= limit)
        || request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes >= limit)
        || request
            .budget
            .max_distance_evaluations
            .is_some_and(|limit| {
                stats
                    .distance_evaluations
                    .saturating_add(stats.quantized_distance_evaluations)
                    >= limit
            })
}

fn add_async_stats(total: &mut ProximitySearchStats, added: &ProximitySearchStats) {
    total.levels_visited = total.levels_visited.saturating_add(added.levels_visited);
    total.nodes_read = total.nodes_read.saturating_add(added.nodes_read);
    total.bytes_read = total.bytes_read.saturating_add(added.bytes_read);
    total.committed_bytes = total.committed_bytes.saturating_add(added.committed_bytes);
    total.distance_evaluations = total
        .distance_evaluations
        .saturating_add(added.distance_evaluations);
    total.quantized_distance_evaluations = total
        .quantized_distance_evaluations
        .saturating_add(added.quantized_distance_evaluations);
    total.reranked_candidates = total
        .reranked_candidates
        .saturating_add(added.reranked_candidates);
    total.frontier_peak = total.frontier_peak.max(added.frontier_peak);
    total.candidate_handles_peak = total
        .candidate_handles_peak
        .max(added.candidate_handles_peak);
    total.candidate_retained_bytes_peak = total
        .candidate_retained_bytes_peak
        .max(added.candidate_retained_bytes_peak);
}

#[allow(clippy::too_many_arguments)]
async fn search_hnsw_async<S>(
    store: &super::SearchIo<S>,
    directory: &AsyncProlly<super::SearchIo<S>>,
    tree: &ProximityTree,
    index: &AsyncHnswIndex,
    request: SearchRequest<'_>,
    eligibility: &PreparedFilter<'_>,
    plan: &super::SearchPlan,
    control: &AsyncSearchControl,
    excluded: Option<&BTreeSet<Vec<u8>>>,
) -> Result<SearchResult, Error>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    let super::SearchPlan::Hnsw {
        expansion_target,
        rerank_target,
        ..
    } = plan
    else {
        return Err(invalid_search("HNSW executor requires an HNSW plan"));
    };
    let query = prepare_vector(index.metric, request.query, index.dimensions)?;
    let graph_store = store.for_kind(crate::prolly::content_graph::ContentObjectKind::HnswPage);
    let graph = AsyncProlly::new(graph_store, index.graph_tree.config.clone());
    let mut state = AsyncHnswState {
        graph,
        index,
        request: &request,
        stats: ProximitySearchStats::default(),
        completion: SearchCompletion::ApproximatePolicySatisfied,
        loaded: BTreeMap::new(),
    };
    let mut current = index.entry_point.clone();
    let Some(entry) = state.node(&current).await? else {
        return Ok(state.empty(plan));
    };
    let Some(mut current_distance) = state.distance(&query, &entry.routing_vector) else {
        return Ok(state.empty(plan));
    };
    for layer in (1..=index.maximum_level).rev() {
        loop {
            if let Some(stopped) = stop_reason(control) {
                state.completion = stopped;
                return Ok(state.empty(plan));
            }
            let Some(node) = state.node(&current).await? else {
                return Ok(state.empty(plan));
            };
            let mut best = AsyncRanked {
                distance: current_distance,
                key: current.clone(),
            };
            for neighbor in &node.neighbors[usize::from(layer)] {
                let Some(neighbor_node) = state.node(neighbor).await? else {
                    return Ok(state.empty(plan));
                };
                let Some(distance) = state.distance(&query, &neighbor_node.routing_vector) else {
                    return Ok(state.empty(plan));
                };
                let candidate = AsyncRanked {
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
    let first = AsyncRanked {
        distance: current_distance,
        key: current.clone(),
    };
    let mut frontier = BinaryHeap::from([Reverse(first.clone())]);
    let mut closest = BinaryHeap::from([first.clone()]);
    let mut eligible = BinaryHeap::<AsyncRanked>::new();
    if eligibility.contains(&current)
        && !excluded.is_some_and(|excluded| excluded.contains(&current))
    {
        eligible.push(first);
    }
    let mut visited = HashSet::from([current]);
    let mut expanded = 0usize;
    state.stats.frontier_peak = 1;
    while let Some(Reverse(candidate)) = frontier.pop() {
        if let Some(stopped) = stop_reason(control) {
            state.completion = stopped;
            break;
        }
        if expanded >= *expansion_target
            && eligible.len() >= request.k
            && closest.peek().is_some_and(|worst| candidate > *worst)
        {
            break;
        }
        let Some(node) = state.node(&candidate.key).await? else {
            break;
        };
        expanded += 1;
        for neighbor in &node.neighbors[0] {
            if !visited.insert(neighbor.clone()) {
                continue;
            }
            let entries = frontier
                .len()
                .saturating_add(closest.len())
                .saturating_add(eligible.len())
                .saturating_add(1);
            if request
                .budget
                .max_frontier_entries
                .is_some_and(|limit| entries > limit)
            {
                state.completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let Some(neighbor_node) = state.node(neighbor).await? else {
                break;
            };
            let Some(distance) = state.distance(&query, &neighbor_node.routing_vector) else {
                break;
            };
            let ranked = AsyncRanked {
                distance,
                key: neighbor.clone(),
            };
            if closest.len() < *expansion_target
                || closest.peek().is_some_and(|worst| ranked < *worst)
                || eligible.len() < request.k
            {
                frontier.push(Reverse(ranked.clone()));
                closest.push(ranked.clone());
                if closest.len() > *expansion_target {
                    closest.pop();
                }
            }
            if eligibility.contains(neighbor)
                && !excluded.is_some_and(|excluded| excluded.contains(neighbor))
            {
                eligible.push(ranked);
                if eligible.len() > *rerank_target {
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
    let mut reranked = Vec::<RerankCandidate>::with_capacity(candidates.len());
    let mut vector_scratch = vec![0.0f32; tree.config.dimensions as usize];
    let mut directory_session = directory.read(&tree.directory).await?;
    for candidate in candidates {
        if let Some(stopped) = stop_reason(control) {
            state.completion = stopped;
            break;
        }
        if budget_stops_record(&request, &state.stats, 0) {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let Some(handle) = directory_session.get_handle(&candidate.key).await? else {
            return Err(Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "result key is absent from authoritative directory".to_owned(),
            });
        };
        let bytes = handle.value()?.len();
        if request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| state.stats.committed_bytes.saturating_add(bytes) > limit)
        {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let record = crate::prolly::proximity::storage::StoredRecordRef::decode(
            handle.value()?,
            tree.config.dimensions,
        )?;
        crate::prolly::proximity::ProximityVectorRef::from_encoded(record.vector)
            .copy_to_slice(&mut vector_scratch)?;
        let Some(distance) = state.distance(&query, &vector_scratch) else {
            state.completion = SearchCompletion::BudgetExhausted;
            break;
        };
        state.stats.nodes_read += 1;
        state.stats.bytes_read += bytes;
        state.stats.committed_bytes += bytes;
        state.stats.reranked_candidates += 1;
        reranked.push(RerankCandidate::new(handle, &candidate.key, distance)?);
    }
    state.stats.candidate_handles_peak = reranked.len();
    state.stats.candidate_retained_bytes_peak = retained_candidate_bytes(&reranked);
    reranked.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.key().cmp(right.key()))
    });
    let neighbors = reranked
        .into_iter()
        .take(request.k)
        .map(|candidate| candidate.into_neighbor(tree.config.dimensions))
        .collect::<Result<Vec<_>, Error>>()?;
    Ok(SearchResult {
        neighbors,
        stats: state.stats,
        completion: state.completion,
        plan: plan.summary(),
    })
}

struct AsyncHnswState<'a, S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    graph: AsyncProlly<super::SearchIo<S>>,
    index: &'a AsyncHnswIndex,
    request: &'a SearchRequest<'a>,
    stats: ProximitySearchStats,
    completion: SearchCompletion,
    loaded: BTreeMap<Vec<u8>, GraphNode>,
}

impl<S> AsyncHnswState<'_, S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    async fn node(&mut self, key: &[u8]) -> Result<Option<GraphNode>, Error> {
        if let Some(node) = self.loaded.get(key) {
            return Ok(Some(node.clone()));
        }
        if self
            .request
            .budget
            .max_nodes
            .is_some_and(|limit| self.stats.nodes_read >= limit)
        {
            self.completion = SearchCompletion::BudgetExhausted;
            return Ok(None);
        }
        let bytes = self
            .graph
            .get(&self.index.graph_tree, key)
            .await?
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "HNSW",
                reason: "graph neighbor key is absent".to_owned(),
            })?;
        if self
            .request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| self.stats.committed_bytes.saturating_add(bytes.len()) > limit)
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
                reason: "graph node violates manifest constraints".to_owned(),
            });
        }
        self.stats.nodes_read += 1;
        self.stats.bytes_read += bytes.len();
        self.stats.committed_bytes += bytes.len();
        self.loaded.insert(key.to_vec(), node.clone());
        Ok(Some(node))
    }

    fn distance(&mut self, query: &[f32], vector: &[f32]) -> Option<f64> {
        if self
            .request
            .budget
            .max_distance_evaluations
            .is_some_and(|limit| self.stats.distance_evaluations >= limit)
        {
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

    fn empty(&self, plan: &super::SearchPlan) -> SearchResult {
        SearchResult {
            neighbors: Vec::new(),
            stats: self.stats.clone(),
            completion: self.completion,
            plan: plan.summary(),
        }
    }
}
