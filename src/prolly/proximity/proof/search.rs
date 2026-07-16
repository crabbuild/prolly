use super::{invalid, ProximityStructuralProof};
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentObject, TypedContentRoot,
};
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::search::PreparedFilter;
use crate::prolly::proximity::{
    AcceleratorCatalog, AcceleratorSet, CompositeAccelerator, DistanceMetric, HnswIndex,
    ProductQuantizer, ProximityFilter, ProximityMap, QueryKernel, SearchBackend, SearchBudget,
    SearchCompletion, SearchIo, SearchOptions, SearchPlan, SearchPlanSummary, SearchPolicy,
    SearchRequest, SearchResult, SearchRuntime, SEARCH_PLAN_FORMAT_VERSION,
};
use crate::prolly::store::{MemStore, Store};
use crate::prolly::tree::Tree;
use std::collections::HashSet;
use std::sync::Arc;

/// Owned, replayable form of every public proximity filter.
#[derive(Clone, Debug, PartialEq)]
pub enum ProximityProofFilter {
    All,
    KeyRange {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
    },
    Prefix(Vec<u8>),
    EligibleKeys(Vec<Vec<u8>>),
    SecondaryEligible {
        keys: Vec<Vec<u8>>,
        source_directory: Tree,
    },
}

impl ProximityProofFilter {
    fn capture(filter: &ProximityFilter<'_>) -> Self {
        match filter {
            ProximityFilter::All => Self::All,
            ProximityFilter::KeyRange { start, end } => Self::KeyRange {
                start: start.map(<[u8]>::to_vec),
                end: end.map(<[u8]>::to_vec),
            },
            ProximityFilter::Prefix(prefix) => Self::Prefix(prefix.to_vec()),
            ProximityFilter::EligibleKeys(keys) => Self::EligibleKeys(keys.to_vec()),
            ProximityFilter::SecondaryEligible {
                keys,
                source_directory,
            } => Self::SecondaryEligible {
                keys: keys.to_vec(),
                source_directory: (*source_directory).clone(),
            },
        }
    }

    fn borrowed(&self) -> ProximityFilter<'_> {
        match self {
            Self::All => ProximityFilter::All,
            Self::KeyRange { start, end } => ProximityFilter::KeyRange {
                start: start.as_deref(),
                end: end.as_deref(),
            },
            Self::Prefix(prefix) => ProximityFilter::Prefix(prefix),
            Self::EligibleKeys(keys) => ProximityFilter::EligibleKeys(keys),
            Self::SecondaryEligible {
                keys,
                source_directory,
            } => ProximityFilter::SecondaryEligible {
                keys,
                source_directory,
            },
        }
    }

    fn contains(&self, key: &[u8]) -> bool {
        match self {
            Self::All => true,
            Self::KeyRange { start, end } => {
                start.as_ref().map_or(true, |start| key >= start.as_slice())
                    && end.as_ref().map_or(true, |end| key < end.as_slice())
            }
            Self::Prefix(prefix) => key.starts_with(prefix),
            Self::EligibleKeys(keys) | Self::SecondaryEligible { keys, .. } => keys
                .binary_search_by(|candidate| candidate.as_slice().cmp(key))
                .is_ok(),
        }
    }
}

/// Owned deterministic search request committed by a proof.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximitySearchRequest {
    pub query: Vec<f32>,
    pub k: usize,
    pub policy: SearchPolicy,
    pub budget: SearchBudget,
    pub filter: ProximityProofFilter,
    pub kernel: QueryKernel,
    pub options: SearchOptions,
}

impl ProximitySearchRequest {
    fn capture(request: &SearchRequest<'_>) -> Self {
        Self {
            query: request.query.to_vec(),
            k: request.k,
            policy: request.policy,
            budget: request.budget.clone(),
            filter: ProximityProofFilter::capture(&request.filter),
            kernel: request.kernel,
            options: request.options.clone(),
        }
    }

    fn borrowed(&self) -> SearchRequest<'_> {
        SearchRequest {
            query: &self.query,
            k: self.k,
            policy: self.policy,
            budget: self.budget.clone(),
            filter: self.filter.borrowed(),
            kernel: self.kernel,
            options: self.options.clone(),
        }
    }
}

/// Scope of the claim established by replay.
#[derive(Clone, Debug, PartialEq)]
pub enum ProximitySearchClaim {
    /// Full authenticated graph plus exact native replay establishes optimality.
    ExactL2Optimal { terminal_lower_bound: f64 },
    /// Reproducible execution and result membership without global optimality.
    HonestExecution,
}

/// Deterministic, tamper-evident replay transcript summary.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProximitySearchEvent {
    RequestCommitted(Cid),
    FrontierPushed { cid: Cid, bound_bits: u64 },
    FrontierPopped { cid: Cid, bound_bits: u64 },
    VisitedObject(Cid),
    CandidateScored { key: Vec<u8>, distance_bits: u64 },
    AuthenticatedObject { kind: ContentObjectKind, cid: Cid },
    Candidate { key: Vec<u8>, distance_bits: u64 },
    Completed(SearchCompletion),
}

/// Self-contained proof for native, PQ, or HNSW search replay.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximitySearchProof {
    pub format_version: u8,
    pub source: ProximityStructuralProof,
    pub accelerator_root: Option<TypedContentRoot>,
    pub accelerator_objects: Vec<TypedContentObject>,
    pub request: ProximitySearchRequest,
    pub request_commitment: Cid,
    pub result: SearchResult,
    pub plan: SearchPlan,
    pub events: Vec<ProximitySearchEvent>,
    pub claim: ProximitySearchClaim,
}

/// Verified result and appropriately scoped claim.
#[derive(Clone, Debug, PartialEq)]
pub struct ProximitySearchVerification {
    pub result: SearchResult,
    pub claim: ProximitySearchClaim,
    pub replayed_events: usize,
}

impl ProximitySearchProof {
    /// Verify against a source descriptor CID already trusted by the caller.
    pub fn verify_for_source(
        &self,
        expected_descriptor: &Cid,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchVerification, Error> {
        if &self.source.descriptor != expected_descriptor {
            return Err(invalid(
                "search proof targets an unexpected source descriptor",
            ));
        }
        self.verify(limits)
    }

    /// Authenticate all supplied objects and deterministically replay the
    /// requested native or accelerator execution in an isolated store.
    pub fn verify(
        &self,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchVerification, Error> {
        if request_commitment(&self.request, &self.plan) != self.request_commitment {
            return Err(invalid("search request commitment mismatch"));
        }
        let (map, _) = self.source.verified_map(limits)?;
        map.verify()?;
        let store = map.store_clone();
        verify_accelerator_evidence(
            &store,
            self.accelerator_root.as_ref(),
            &self.accelerator_objects,
            limits,
        )?;
        if self.format_version != SEARCH_PLAN_FORMAT_VERSION
            || self.plan.summary() != self.result.plan
        {
            return Err(invalid(
                "unsupported proof version or search plan summary mismatch",
            ));
        }
        let mut request = self.request.borrowed();
        let mut native_trace = Vec::new();
        let replayed = match (&self.accelerator_root, &self.plan) {
            (None, SearchPlan::Native) => {
                request.options.backend = SearchBackend::Native;
                map.search_with_trace(request, Some(&mut native_trace))?
            }
            (Some(root), SearchPlan::ProductQuantized { .. })
                if root.kind == ContentObjectKind::ProductQuantization =>
            {
                ProductQuantizer::load(store.clone(), root.cid.clone())?
                    .search_planned(&map, request, &self.plan)?
            }
            (Some(root), SearchPlan::Hnsw { .. })
                if root.kind == ContentObjectKind::HnswManifest =>
            {
                let index = HnswIndex::load(store, root.cid.clone())?;
                crate::prolly::proximity::accelerator::hnsw::search::search_planned(
                    &index, &map, request, &self.plan,
                )?
            }
            (Some(root), SearchPlan::Composite { .. })
                if root.kind == ContentObjectKind::CompositeAccelerator =>
            {
                let composite = CompositeAccelerator::load(store.clone(), root.cid.clone())?;
                let search_io = SearchIo::new(store.clone(), Arc::new(SearchRuntime::default()));
                let bound_map = ProximityMap::load(
                    search_io.for_kind_with_dimensions(
                        ContentObjectKind::ProximityNode,
                        map.tree().config.dimensions,
                    ),
                    map.tree().descriptor.clone(),
                )?;
                let eligibility =
                    PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
                map.search_composite(
                    &composite,
                    &search_io,
                    &bound_map,
                    request,
                    &eligibility,
                    &self.plan,
                )?
            }
            (Some(root), plan) if root.kind == ContentObjectKind::AcceleratorCatalog => {
                let catalog =
                    AcceleratorCatalog::load(store.clone(), root.cid.clone(), map.tree())?;
                replay_catalog_plan(&map, store, catalog.accelerators(), request, plan)?
            }
            _ => return Err(invalid("search backend and accelerator evidence disagree")),
        };
        if replayed != self.result {
            return Err(invalid("search replay result or completion mismatch"));
        }
        let claim = claim_for(
            &map,
            &self.request,
            &replayed,
            self.accelerator_root.is_some(),
        )?;
        if claim != self.claim {
            return Err(invalid("search proof overstates or changes its claim"));
        }
        let events = if self.accelerator_root.is_none() {
            native_events(&self.request_commitment, native_trace)
        } else {
            events_for(
                &self.request_commitment,
                &self.source.objects,
                &self.accelerator_objects,
                &replayed,
            )
        };
        if events != self.events {
            return Err(invalid("search transcript event mismatch"));
        }
        Ok(ProximitySearchVerification {
            result: replayed,
            claim,
            replayed_events: events.len(),
        })
    }
}

impl<S> ProximityMap<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove deterministic native execution. Auto is allowed when it resolves
    /// to the native map; explicit accelerator requests use their sidecars.
    pub fn prove_search(
        &self,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        if matches!(
            request.options.backend,
            SearchBackend::ProductQuantized | SearchBackend::Hnsw | SearchBackend::Composite
        ) {
            return Err(invalid(
                "explicit accelerator search proofs must be produced by that sidecar",
            ));
        }
        let mut trace = Vec::new();
        let result = self.search_with_trace(request.clone(), Some(&mut trace))?;
        build_proof(self, request, result, None, Vec::new(), Some(trace), limits)
    }
}

impl<S> ProductQuantizer<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove source-bound product-quantized execution and full reranking.
    pub fn prove_search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        let result = self.search(map, request.clone())?;
        let root = TypedContentRoot::new(
            ContentObjectKind::ProductQuantization,
            self.manifest_cid().clone(),
        );
        let objects =
            walk_content_graph(&map.store_clone(), std::slice::from_ref(&root), limits)?.objects;
        build_proof(map, request, result, Some(root), objects, None, limits)
    }
}

impl<S> HnswIndex<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove source-bound HNSW execution and authoritative value resolution.
    pub fn prove_search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        let result = self.search(map, request.clone())?;
        let root =
            TypedContentRoot::new(ContentObjectKind::HnswManifest, self.manifest_cid().clone());
        let objects =
            walk_content_graph(&map.store_clone(), std::slice::from_ref(&root), limits)?.objects;
        build_proof(map, request, result, Some(root), objects, None, limits)
    }
}

impl<S> CompositeAccelerator<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove a source-bound base-plus-delta execution and its committed child plan.
    pub fn prove_search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        let composite = CompositeAccelerator::load(map.store_clone(), self.manifest_cid().clone())?;
        let accelerators = AcceleratorSet::empty().with_composite(map.tree(), composite)?;
        let search_io = SearchIo::new(map.store_clone(), Arc::new(SearchRuntime::default()));
        let mut result = map.search_with(&accelerators, &search_io, request.clone())?;
        // Physical reads depend on verifier cache warmth and transport. Proofs
        // commit only to deterministic logical execution statistics.
        result.stats.physical_bytes_read = 0;
        let root = TypedContentRoot::new(
            ContentObjectKind::CompositeAccelerator,
            self.manifest_cid().clone(),
        );
        let objects =
            walk_content_graph(&map.store_clone(), std::slice::from_ref(&root), limits)?.objects;
        build_proof(map, request, result, Some(root), objects, None, limits)
    }
}

impl<S> AcceleratorCatalog<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Prove execution against one pinned accelerator-catalog snapshot and its exact closure.
    pub fn prove_search(
        &self,
        map: &ProximityMap<S>,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        let search_io = SearchIo::new(map.store_clone(), Arc::new(SearchRuntime::default()));
        let mut result = map.search_with(self.accelerators(), &search_io, request.clone())?;
        if !matches!(
            result.plan.backend,
            SearchBackend::Hnsw | SearchBackend::ProductQuantized | SearchBackend::Composite
        ) {
            return Err(invalid(
                "catalog proof requires an accelerator-backed committed plan",
            ));
        }
        result.stats.physical_bytes_read = 0;
        let root = self.typed_root();
        let objects =
            walk_content_graph(&map.store_clone(), std::slice::from_ref(&root), limits)?.objects;
        build_proof(map, request, result, Some(root), objects, None, limits)
    }
}

fn replay_catalog_plan(
    map: &ProximityMap<Arc<MemStore>>,
    store: Arc<MemStore>,
    accelerators: &AcceleratorSet<Arc<MemStore>>,
    request: SearchRequest<'_>,
    plan: &SearchPlan,
) -> Result<SearchResult, Error> {
    match plan {
        SearchPlan::Hnsw { .. } => {
            let index = accelerators
                .hnsw()
                .ok_or_else(|| invalid("catalog has no committed HNSW accelerator"))?;
            crate::prolly::proximity::accelerator::hnsw::search::search_planned(
                index, map, request, plan,
            )
        }
        SearchPlan::ProductQuantized { .. } => accelerators
            .pq()
            .ok_or_else(|| invalid("catalog has no committed PQ accelerator"))?
            .search_planned(map, request, plan),
        SearchPlan::Composite { .. } => {
            let composite = accelerators
                .composite()
                .ok_or_else(|| invalid("catalog has no committed composite accelerator"))?;
            let search_io = SearchIo::new(store, Arc::new(SearchRuntime::default()));
            let bound_map = ProximityMap::load(
                search_io.for_kind_with_dimensions(
                    ContentObjectKind::ProximityNode,
                    map.tree().config.dimensions,
                ),
                map.tree().descriptor.clone(),
            )?;
            let eligibility = PreparedFilter::new(request.filter.clone(), &map.tree().directory)?;
            map.search_composite(
                composite,
                &search_io,
                &bound_map,
                request,
                &eligibility,
                plan,
            )
        }
        SearchPlan::Native | SearchPlan::EligibleExact { .. } => Err(invalid(
            "catalog proof contains a non-accelerator committed plan",
        )),
    }
}

fn build_proof<S>(
    map: &ProximityMap<S>,
    request: SearchRequest<'_>,
    result: SearchResult,
    accelerator_root: Option<TypedContentRoot>,
    accelerator_objects: Vec<TypedContentObject>,
    native_trace: Option<Vec<ProximitySearchEvent>>,
    limits: &ContentGraphLimits,
) -> Result<ProximitySearchProof, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let source = map.prove_structure(limits)?;
    let plan = plan_from_summary(&result.plan, &request.filter)?;
    let request = ProximitySearchRequest::capture(&request);
    let request_commitment = request_commitment(&request, &plan);
    let claim = claim_for(map, &request, &result, accelerator_root.is_some())?;
    let events = match native_trace {
        Some(trace) => native_events(&request_commitment, trace),
        None => events_for(
            &request_commitment,
            &source.objects,
            &accelerator_objects,
            &result,
        ),
    };
    Ok(ProximitySearchProof {
        format_version: SEARCH_PLAN_FORMAT_VERSION,
        source,
        accelerator_root,
        accelerator_objects,
        request,
        request_commitment,
        result,
        plan,
        events,
        claim,
    })
}

fn verify_accelerator_evidence(
    store: &Arc<MemStore>,
    root: Option<&TypedContentRoot>,
    objects: &[TypedContentObject],
    limits: &ContentGraphLimits,
) -> Result<(), Error> {
    let Some(root) = root else {
        if objects.is_empty() {
            return Ok(());
        }
        return Err(invalid("accelerator objects supplied without a typed root"));
    };
    let mut supplied_cids = HashSet::new();
    let mut supplied_shape = HashSet::new();
    for object in objects {
        if Cid::from_bytes(&object.bytes) != object.root.cid
            || !supplied_cids.insert(object.root.cid.clone())
            || !supplied_shape.insert((object.root.clone(), object.depth))
        {
            return Err(invalid("duplicate or CID-invalid accelerator proof object"));
        }
        Store::put(store, object.root.cid.as_bytes(), &object.bytes)
            .map_err(|error| Error::Store(Box::new(error)))?;
    }
    let walked = walk_content_graph(store, std::slice::from_ref(root), limits)?;
    let reached = walked
        .objects
        .into_iter()
        .map(|object| (object.root, object.depth))
        .collect::<HashSet<_>>();
    if supplied_shape != reached {
        return Err(invalid("accelerator proof is not its exact typed closure"));
    }
    Ok(())
}

fn claim_for<S>(
    map: &ProximityMap<S>,
    request: &ProximitySearchRequest,
    result: &SearchResult,
    accelerated: bool,
) -> Result<ProximitySearchClaim, Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    if !accelerated
        && request.policy == SearchPolicy::Exact
        && result.completion == SearchCompletion::Exact
        && map.tree().config.metric == DistanceMetric::L2Squared
    {
        let query = prepare_vector(
            map.tree().config.metric,
            &request.query,
            map.tree().config.dimensions,
        )?;
        let selected = result
            .neighbors
            .iter()
            .map(|neighbor| neighbor.key.as_slice())
            .collect::<HashSet<_>>();
        let mut terminal_lower_bound = f64::INFINITY;
        for record in map.collect_records()?.into_values() {
            if request.filter.contains(&record.key) && !selected.contains(record.key.as_slice()) {
                terminal_lower_bound = terminal_lower_bound.min(query_score(
                    request.kernel,
                    map.tree().config.metric,
                    &query,
                    &record.vector,
                ));
            }
        }
        Ok(ProximitySearchClaim::ExactL2Optimal {
            terminal_lower_bound,
        })
    } else {
        Ok(ProximitySearchClaim::HonestExecution)
    }
}

fn events_for(
    commitment: &Cid,
    source: &[TypedContentObject],
    accelerator: &[TypedContentObject],
    result: &SearchResult,
) -> Vec<ProximitySearchEvent> {
    let mut events = vec![ProximitySearchEvent::RequestCommitted(commitment.clone())];
    events.extend(
        source
            .iter()
            .map(|object| ProximitySearchEvent::AuthenticatedObject {
                kind: object.root.kind,
                cid: object.root.cid.clone(),
            }),
    );
    events.extend(
        accelerator
            .iter()
            .map(|object| ProximitySearchEvent::AuthenticatedObject {
                kind: object.root.kind,
                cid: object.root.cid.clone(),
            }),
    );
    events.extend(
        result
            .neighbors
            .iter()
            .map(|neighbor| ProximitySearchEvent::Candidate {
                key: neighbor.key.clone(),
                distance_bits: neighbor.distance.to_bits(),
            }),
    );
    events.push(ProximitySearchEvent::Completed(result.completion));
    events
}

fn native_events(commitment: &Cid, trace: Vec<ProximitySearchEvent>) -> Vec<ProximitySearchEvent> {
    let mut events = Vec::with_capacity(trace.len() + 1);
    events.push(ProximitySearchEvent::RequestCommitted(commitment.clone()));
    events.extend(trace);
    events
}

fn request_commitment(request: &ProximitySearchRequest, plan: &SearchPlan) -> Cid {
    let mut bytes = b"PSRQ\x03".to_vec();
    put_len(request.query.len(), &mut bytes);
    for component in &request.query {
        bytes.extend_from_slice(&component.to_bits().to_le_bytes());
    }
    put_usize(request.k, &mut bytes);
    bytes.push(match request.policy {
        SearchPolicy::Exact => 0,
        SearchPolicy::FixedBudget => 1,
        SearchPolicy::Adaptive(crate::prolly::proximity::AdaptiveQuality::Fast) => 2,
        SearchPolicy::Adaptive(crate::prolly::proximity::AdaptiveQuality::Balanced) => 3,
        SearchPolicy::Adaptive(crate::prolly::proximity::AdaptiveQuality::HighRecall) => 4,
    });
    for limit in [
        request.budget.max_nodes,
        request.budget.max_committed_bytes,
        request.budget.max_distance_evaluations,
        request.budget.max_frontier_entries,
    ] {
        put_optional_usize(limit, &mut bytes);
    }
    encode_filter(&request.filter, &mut bytes);
    bytes.push(match request.options.backend {
        SearchBackend::Native => 0,
        SearchBackend::ProductQuantized => 1,
        SearchBackend::Hnsw => 2,
        SearchBackend::Auto => 3,
        SearchBackend::Composite => 4,
    });
    bytes.push(match request.kernel {
        QueryKernel::ScalarDeterministic => 0,
        QueryKernel::SimdDeterministic => 1,
        QueryKernel::AutoDeterministic => 2,
    });
    bytes.push(u8::from(
        request.options.planner.allow_exact_for_approximate,
    ));
    put_usize(
        request.options.planner.eligible_exact_max_records,
        &mut bytes,
    );
    bytes.extend_from_slice(
        &request
            .options
            .planner
            .eligible_exact_ratio_ppm
            .to_le_bytes(),
    );
    bytes.push(match request.options.planner.approximate_preference {
        crate::prolly::proximity::ApproximatePreference::HnswFirst => 0,
        crate::prolly::proximity::ApproximatePreference::ProductQuantizedFirst => 1,
    });
    put_optional_usize(
        request.options.hnsw.ef_search.map(|value| value as usize),
        &mut bytes,
    );
    put_optional_usize(
        request.options.pq.rerank_multiplier.map(usize::from),
        &mut bytes,
    );
    encode_plan(plan, &mut bytes);
    Cid::from_bytes(&bytes)
}

fn encode_plan(plan: &SearchPlan, bytes: &mut Vec<u8>) {
    match plan {
        SearchPlan::Native => bytes.push(0),
        SearchPlan::EligibleExact {
            key_count,
            source_bound,
        } => {
            bytes.push(1);
            bytes.extend_from_slice(&key_count.to_le_bytes());
            bytes.push(u8::from(*source_bound));
        }
        SearchPlan::ProductQuantized {
            rerank_target,
            direct_lookup,
        } => {
            bytes.push(2);
            put_usize(*rerank_target, bytes);
            bytes.push(u8::from(*direct_lookup));
        }
        SearchPlan::Hnsw {
            ef_search,
            expansion_target,
            rerank_target,
        } => {
            bytes.push(3);
            bytes.extend_from_slice(&ef_search.to_le_bytes());
            put_usize(*expansion_target, bytes);
            put_usize(*rerank_target, bytes);
        }
        SearchPlan::Composite {
            base,
            delta_records,
            shadow_records,
            merge_target,
        } => {
            bytes.push(4);
            encode_plan(base, bytes);
            put_usize(*delta_records, bytes);
            put_usize(*shadow_records, bytes);
            put_usize(*merge_target, bytes);
        }
    }
}

fn plan_from_summary(
    summary: &SearchPlanSummary,
    filter: &ProximityFilter<'_>,
) -> Result<SearchPlan, Error> {
    if summary.format_version != SEARCH_PLAN_FORMAT_VERSION {
        return Err(invalid("unsupported search plan summary version"));
    }
    if let Some(key_count) = summary.eligible_exact_records {
        return Ok(SearchPlan::EligibleExact {
            key_count,
            source_bound: matches!(filter, ProximityFilter::SecondaryEligible { .. }),
        });
    }
    match summary.backend {
        SearchBackend::Native | SearchBackend::Auto => Ok(SearchPlan::Native),
        SearchBackend::ProductQuantized => Ok(SearchPlan::ProductQuantized {
            rerank_target: summary
                .rerank_target
                .ok_or_else(|| invalid("PQ plan summary has no rerank target"))?,
            direct_lookup: summary.direct_lookup,
        }),
        SearchBackend::Hnsw => Ok(SearchPlan::Hnsw {
            ef_search: summary
                .hnsw_ef_search
                .ok_or_else(|| invalid("HNSW plan summary has no ef_search"))?,
            expansion_target: summary
                .expansion_target
                .ok_or_else(|| invalid("HNSW plan summary has no expansion target"))?,
            rerank_target: summary
                .rerank_target
                .ok_or_else(|| invalid("HNSW plan summary has no rerank target"))?,
        }),
        SearchBackend::Composite => Ok(SearchPlan::Composite {
            base: Box::new(plan_from_summary(
                summary
                    .composite_base
                    .as_deref()
                    .ok_or_else(|| invalid("composite plan summary has no base plan"))?,
                filter,
            )?),
            delta_records: summary
                .delta_records
                .ok_or_else(|| invalid("composite plan summary has no delta count"))?,
            shadow_records: summary
                .shadow_records
                .ok_or_else(|| invalid("composite plan summary has no shadow count"))?,
            merge_target: summary
                .rerank_target
                .ok_or_else(|| invalid("composite plan summary has no merge target"))?,
        }),
    }
}

fn encode_filter(filter: &ProximityProofFilter, bytes: &mut Vec<u8>) {
    match filter {
        ProximityProofFilter::All => bytes.push(0),
        ProximityProofFilter::KeyRange { start, end } => {
            bytes.push(1);
            put_optional_bytes(start.as_deref(), bytes);
            put_optional_bytes(end.as_deref(), bytes);
        }
        ProximityProofFilter::Prefix(prefix) => {
            bytes.push(2);
            put_bytes(prefix, bytes);
        }
        ProximityProofFilter::EligibleKeys(keys) => {
            bytes.push(3);
            put_keys(keys, bytes);
        }
        ProximityProofFilter::SecondaryEligible {
            keys,
            source_directory,
        } => {
            bytes.push(4);
            put_keys(keys, bytes);
            match &source_directory.root {
                Some(root) => {
                    bytes.push(1);
                    bytes.extend_from_slice(root.as_bytes());
                }
                None => bytes.push(0),
            }
            let config = &source_directory.config;
            let format = config
                .format
                .canonical_bytes()
                .expect("directory tree format must be valid");
            put_bytes(&format, bytes);
        }
    }
}

fn put_keys(keys: &[Vec<u8>], output: &mut Vec<u8>) {
    put_len(keys.len(), output);
    for key in keys {
        put_bytes(key, output);
    }
}

fn put_optional_bytes(value: Option<&[u8]>, output: &mut Vec<u8>) {
    match value {
        Some(value) => {
            output.push(1);
            put_bytes(value, output);
        }
        None => output.push(0),
    }
}

fn put_bytes(value: &[u8], output: &mut Vec<u8>) {
    put_len(value.len(), output);
    output.extend_from_slice(value);
}

fn put_optional_usize(value: Option<usize>, output: &mut Vec<u8>) {
    match value {
        Some(value) => {
            output.push(1);
            put_usize(value, output);
        }
        None => output.push(0),
    }
}

fn put_len(value: usize, output: &mut Vec<u8>) {
    put_usize(value, output);
}

fn put_usize(value: usize, output: &mut Vec<u8>) {
    output.extend_from_slice(&(value as u128).to_le_bytes());
}
