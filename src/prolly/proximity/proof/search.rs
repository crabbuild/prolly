use super::{invalid, ProximityStructuralProof};
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentObject, TypedContentRoot,
};
use crate::prolly::encoding::Encoding;
use crate::prolly::error::Error;
use crate::prolly::proximity::distance::{prepare_vector, query_score};
use crate::prolly::proximity::{
    DistanceMetric, HnswIndex, ProductQuantizer, ProximityFilter, ProximityMap, QueryKernel,
    SearchBackend, SearchBudget, SearchCompletion, SearchPolicy, SearchRequest, SearchResult,
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
                start.as_ref().map_or(true, |start| key >= start)
                    && end.as_ref().map_or(true, |end| key < end)
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
    pub backend: SearchBackend,
    pub kernel: QueryKernel,
}

impl ProximitySearchRequest {
    fn capture(request: &SearchRequest<'_>) -> Self {
        Self {
            query: request.query.to_vec(),
            k: request.k,
            policy: request.policy,
            budget: request.budget.clone(),
            filter: ProximityProofFilter::capture(&request.filter),
            backend: request.backend,
            kernel: request.kernel,
        }
    }

    fn borrowed(&self) -> SearchRequest<'_> {
        SearchRequest {
            query: &self.query,
            k: self.k,
            policy: self.policy,
            budget: self.budget.clone(),
            filter: self.filter.borrowed(),
            backend: self.backend,
            kernel: self.kernel,
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
    pub source: ProximityStructuralProof,
    pub accelerator_root: Option<TypedContentRoot>,
    pub accelerator_objects: Vec<TypedContentObject>,
    pub request: ProximitySearchRequest,
    pub request_commitment: Cid,
    pub result: SearchResult,
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
        if request_commitment(&self.request) != self.request_commitment {
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
        let request = self.request.borrowed();
        let mut native_trace = Vec::new();
        let replayed = match (&self.accelerator_root, self.request.backend) {
            (None, SearchBackend::Native | SearchBackend::Auto) => {
                map.search_with_trace(request, Some(&mut native_trace))?
            }
            (Some(root), SearchBackend::ProductQuantized | SearchBackend::Auto)
                if root.kind == ContentObjectKind::ProductQuantization =>
            {
                ProductQuantizer::load(store.clone(), root.cid.clone())?.search(&map, request)?
            }
            (Some(root), SearchBackend::Hnsw | SearchBackend::Auto)
                if root.kind == ContentObjectKind::HnswManifest =>
            {
                HnswIndex::load(store, root.cid.clone())?.search(&map, request)?
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
            request.backend,
            SearchBackend::ProductQuantized | SearchBackend::Hnsw
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
    let request = ProximitySearchRequest::capture(&request);
    let request_commitment = request_commitment(&request);
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
        source,
        accelerator_root,
        accelerator_objects,
        request,
        request_commitment,
        result,
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

fn request_commitment(request: &ProximitySearchRequest) -> Cid {
    let mut bytes = b"PSRQ\x02".to_vec();
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
    bytes.push(match request.backend {
        SearchBackend::Native => 0,
        SearchBackend::ProductQuantized => 1,
        SearchBackend::Hnsw => 2,
        SearchBackend::Auto => 3,
    });
    bytes.push(match request.kernel {
        QueryKernel::ScalarDeterministic => 0,
        QueryKernel::SimdDeterministic => 1,
        QueryKernel::AutoDeterministic => 2,
    });
    Cid::from_bytes(&bytes)
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
            put_usize(config.min_chunk_size, bytes);
            put_usize(config.max_chunk_size, bytes);
            bytes.extend_from_slice(&config.chunking_factor.to_le_bytes());
            bytes.extend_from_slice(&config.hash_seed.to_le_bytes());
            match &config.encoding {
                Encoding::Raw => bytes.push(0),
                Encoding::Cbor => bytes.push(1),
                Encoding::Json => bytes.push(2),
                Encoding::Custom(name) => {
                    bytes.push(3);
                    put_bytes(name.as_bytes(), bytes);
                }
            }
            put_optional_usize(config.node_cache_max_nodes, bytes);
            put_optional_usize(config.node_cache_max_bytes, bytes);
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
