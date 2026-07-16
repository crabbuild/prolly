use prolly::{
    AcceleratorCatalog, AcceleratorSet, BuildParallelism, CompositeAccelerator,
    CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits, CompositeBuildOutcome,
    ContentGraphLimits, HnswConfig, HnswIndex, MemStore, ProductQuantizationConfig,
    ProductQuantizer, ProximityConfig, ProximityFilter, ProximityMap, ProximityMutation,
    ProximityRecord, ProximitySearchClaim, QueryKernel, ScalarQuantizationConfig, SearchBackend,
    SearchPolicy, SearchRequest,
};
use std::sync::Arc;

fn fixture() -> (Arc<MemStore>, ProximityMap<Arc<MemStore>>) {
    let store = Arc::new(MemStore::new());
    let mut config = ProximityConfig::new(8);
    config.hierarchy.log_chunk_size = 3;
    config.vector_storage.inline_threshold_bytes = 4;
    config.overflow.min_page_bytes = 128;
    config.overflow.target_page_bytes = 256;
    config.overflow.max_page_bytes = 1024;
    config.scalar_quantization = Some(ScalarQuantizationConfig { group_size: 4 });
    let map = ProximityMap::build(
        store.clone(),
        config,
        (0usize..96).map(|index| ProximityRecord {
            key: format!("proof-{index:04}").into_bytes(),
            vector: (0..8)
                .map(|dimension| ((index * 17 + dimension * 29) % 127) as f32 / 11.0)
                .collect(),
            value: format!("value-{index}").into_bytes(),
        }),
    )
    .unwrap();
    (store, map)
}

#[test]
fn membership_proves_presence_absence_vector_value_and_descriptor_binding() {
    let (_, map) = fixture();
    let present = map.prove_membership(b"proof-0017").unwrap();
    let verified = present.verify().unwrap();
    assert_eq!(verified.key, b"proof-0017");
    assert_eq!(verified.record.unwrap().1, b"value-17");

    let absent = map.prove_membership(b"proof-0017/missing").unwrap();
    assert!(absent.verify().unwrap().record.is_none());

    let mut descriptor_tamper = present.clone();
    descriptor_tamper.descriptor_bytes[0] ^= 1;
    assert!(descriptor_tamper.verify().is_err());
    let mut value_tamper = present.clone();
    value_tamper.record_bytes.as_mut().unwrap().push(0);
    assert!(value_tamper.verify().is_err());
    let mut path_tamper = present;
    path_tamper.directory_proof.path.pop();
    assert!(path_tamper.verify().is_err());
}

#[test]
fn structural_proof_authenticates_reference_paths_summaries_radii_and_vectors() {
    let (_, map) = fixture();
    let limits = ContentGraphLimits::default();
    let proof = map.prove_structure(&limits).unwrap();
    let verified = proof.verify(&limits).unwrap();
    assert_eq!(verified.summary.record_count, 96);
    assert!(verified.summary.proximity_node_count > 1);
    assert!(verified.summary.external_vector_count > 0);
    assert!(verified.summary.scalar_quantizer_count > 0);

    let mut missing_reference = proof.clone();
    missing_reference.objects.remove(0);
    assert!(missing_reference.verify(&limits).is_err());

    let mut typed_path_tamper = proof.clone();
    typed_path_tamper.objects[0].depth += 1;
    assert!(typed_path_tamper.verify(&limits).is_err());

    let mut radius_or_summary_tamper = proof.clone();
    let proximity = radius_or_summary_tamper
        .objects
        .iter_mut()
        .find(|object| object.bytes.starts_with(b"PRXN"))
        .unwrap();
    let last = proximity.bytes.len() - 1;
    proximity.bytes[last] ^= 1;
    assert!(radius_or_summary_tamper.verify(&limits).is_err());
}

#[test]
fn native_search_proof_replays_filter_quantized_execution_and_exact_l2_claims() {
    let (_, map) = fixture();
    let limits = ContentGraphLimits::default();
    let query = [1.0f32; 8];
    let exact = map
        .prove_search(SearchRequest::exact(&query, 6), &limits)
        .unwrap();
    assert!(matches!(
        exact.claim,
        ProximitySearchClaim::ExactL2Optimal { .. }
    ));
    if let ProximitySearchClaim::ExactL2Optimal {
        terminal_lower_bound,
    } = &exact.claim
    {
        assert!(*terminal_lower_bound >= exact.result.neighbors.last().unwrap().distance);
    }
    assert_eq!(exact.verify(&limits).unwrap().result, exact.result);

    let eligible = vec![
        b"proof-0001".to_vec(),
        b"proof-0017".to_vec(),
        b"proof-0064".to_vec(),
    ];
    let mut request = SearchRequest::exact(&query, 2);
    request.policy = SearchPolicy::FixedBudget;
    request.filter = ProximityFilter::EligibleKeys(&eligible);
    request.kernel = QueryKernel::SimdDeterministic;
    let quantized = map.prove_search(request, &limits).unwrap();
    assert_eq!(quantized.claim, ProximitySearchClaim::HonestExecution);
    assert!(quantized.result.stats.quantized_distance_evaluations > 0);
    quantized.verify(&limits).unwrap();

    let mut filter_tamper = quantized.clone();
    if let prolly::ProximityProofFilter::EligibleKeys(keys) = &mut filter_tamper.request.filter {
        keys.pop();
    }
    assert!(filter_tamper.verify(&limits).is_err());
    let mut query_tamper = quantized.clone();
    query_tamper.request.query[0] += 1.0;
    assert!(query_tamper.verify(&limits).is_err());
    let mut transcript_tamper = quantized;
    transcript_tamper.events.pop();
    assert!(transcript_tamper.verify(&limits).is_err());
    let mut terminal_tamper = exact;
    terminal_tamper.claim = ProximitySearchClaim::HonestExecution;
    assert!(terminal_tamper.verify(&limits).is_err());
}

#[test]
fn pq_and_hnsw_proofs_authenticate_sidecars_and_only_claim_honest_execution() {
    let (store, map) = fixture();
    let limits = ContentGraphLimits::default();
    let (pq, _) = ProductQuantizer::build(
        &map,
        ProductQuantizationConfig {
            subquantizers: 4,
            centroids_per_subquantizer: 8,
            training_iterations: 3,
            rerank_multiplier: 4,
            seed: 11,
            max_training_vectors: 65_536,
        },
        BuildParallelism::new(2).unwrap(),
    )
    .unwrap();
    let (hnsw, _) = HnswIndex::build(
        &map,
        HnswConfig {
            max_connections: 8,
            ef_construction: 32,
            ef_search: 32,
            level_bits: 4,
            overfetch_multiplier: 4,
            seed: 13,
            routing_vector_encoding: prolly::HnswRoutingVectorEncoding::FullF32,
        },
    )
    .unwrap();
    let query = [3.0f32; 8];

    let mut pq_request = SearchRequest::exact(&query, 5);
    pq_request.policy = SearchPolicy::FixedBudget;
    pq_request.options.backend = SearchBackend::ProductQuantized;
    let pq_proof = pq.prove_search(&map, pq_request, &limits).unwrap();
    assert_eq!(pq_proof.claim, ProximitySearchClaim::HonestExecution);
    pq_proof.verify(&limits).unwrap();

    let mut hnsw_request = SearchRequest::exact(&query, 5);
    hnsw_request.policy = SearchPolicy::FixedBudget;
    hnsw_request.options.backend = SearchBackend::Hnsw;
    let hnsw_proof = hnsw.prove_search(&map, hnsw_request, &limits).unwrap();
    assert_eq!(hnsw_proof.claim, ProximitySearchClaim::HonestExecution);
    hnsw_proof.verify(&limits).unwrap();

    let set = AcceleratorSet::try_new(map.tree(), Some(hnsw), Some(pq)).unwrap();
    let catalog = AcceleratorCatalog::build(store, map.tree(), set).unwrap();
    for backend in [SearchBackend::Hnsw, SearchBackend::ProductQuantized] {
        let mut request = SearchRequest::exact(&query, 5);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = backend;
        let proof = catalog.prove_search(&map, request, &limits).unwrap();
        assert_eq!(proof.result.plan.backend, backend);
        proof.verify(&limits).unwrap();
    }

    let mut graph_tamper = hnsw_proof;
    let object = graph_tamper.accelerator_objects.last_mut().unwrap();
    object.bytes[0] ^= 1;
    assert!(graph_tamper.verify(&limits).is_err());
}

#[test]
fn composite_proof_authenticates_base_delta_shadow_and_replays_committed_plan() {
    let (store, base) = fixture();
    let (current, _) = base
        .mutate_batch([
            ProximityMutation {
                key: b"proof-0001".to_vec(),
                value: None,
            },
            ProximityMutation {
                key: b"proof-0017".to_vec(),
                value: Some((vec![99.0; 8], b"updated".to_vec())),
            },
            ProximityMutation {
                key: b"proof-0096".to_vec(),
                value: Some((vec![2.0; 8], b"inserted".to_vec())),
            },
        ])
        .unwrap();
    let (base_hnsw, _) = HnswIndex::build(&base, HnswConfig::default()).unwrap();
    let composite = match CompositeAccelerator::build(
        &base,
        &current,
        CompositeBase::Hnsw(base_hnsw),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, .. } => accelerator,
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("unexpected rebuild: {reasons:?}")
        }
    };
    let query = [2.0f32; 8];
    let mut request = SearchRequest::exact(&query, 6);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let limits = ContentGraphLimits::default();
    let proof = composite.prove_search(&current, request, &limits).unwrap();
    assert_eq!(proof.result.plan.backend, SearchBackend::Composite);
    assert_eq!(proof.claim, ProximitySearchClaim::HonestExecution);
    assert_eq!(proof.verify(&limits).unwrap().result, proof.result);

    let loaded =
        CompositeAccelerator::load(store.clone(), composite.manifest_cid().clone()).unwrap();
    let set = AcceleratorSet::empty()
        .with_composite(current.tree(), loaded)
        .unwrap();
    let catalog = AcceleratorCatalog::build(store.clone(), current.tree(), set).unwrap();
    let mut catalog_request = SearchRequest::exact(&query, 6);
    catalog_request.policy = SearchPolicy::FixedBudget;
    catalog_request.options.backend = SearchBackend::Composite;
    let catalog_proof = catalog
        .prove_search(&current, catalog_request, &limits)
        .unwrap();
    assert_eq!(
        catalog_proof.accelerator_root.as_ref().unwrap().kind,
        prolly::ContentObjectKind::AcceleratorCatalog
    );
    assert_eq!(
        catalog_proof.verify(&limits).unwrap().result,
        catalog_proof.result
    );

    let mut missing_delta_or_shadow = proof.clone();
    let position = missing_delta_or_shadow
        .accelerator_objects
        .iter()
        .position(|object| object.root.kind == prolly::ContentObjectKind::OrderedNode)
        .unwrap();
    missing_delta_or_shadow.accelerator_objects.remove(position);
    assert!(missing_delta_or_shadow.verify(&limits).is_err());

    let mut plan_tamper = proof;
    if let prolly::SearchPlan::Composite { delta_records, .. } = &mut plan_tamper.plan {
        *delta_records += 1;
    }
    assert!(plan_tamper.verify(&limits).is_err());
    drop(store);
}
