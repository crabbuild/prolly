use prolly::{
    walk_content_graph, AcceleratorCatalog, AcceleratorSet, BatchOp, BuildParallelism,
    ContentGraphLimits, ContentObjectKind, DistanceMetric, MemStore, NodePublication,
    ProximityConfig, ProximityFilter, ProximityMap, ProximityRecord, SearchBackend, SearchBudget,
    SearchCompletion, SearchPolicy, SearchRequest, Store, TurboQuantizationBuildLimits,
    TurboQuantizationConfig, TurboQuantizer, TypedContentRoot,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

const NO_FAULT: usize = usize::MAX;

#[derive(Default)]
struct FaultControl {
    reads: AtomicUsize,
    publications: AtomicUsize,
    fail_read_at: AtomicUsize,
    fail_publication_at: AtomicUsize,
}

impl FaultControl {
    fn reset(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.publications.store(0, Ordering::SeqCst);
        self.fail_read_at.store(NO_FAULT, Ordering::SeqCst);
        self.fail_publication_at.store(NO_FAULT, Ordering::SeqCst);
    }

    fn fail_read(&self, operation: usize) {
        self.reset();
        self.fail_read_at.store(operation, Ordering::SeqCst);
    }

    fn fail_publication(&self, operation: usize) {
        self.reset();
        self.fail_publication_at.store(operation, Ordering::SeqCst);
    }

    fn before_read(&self) -> io::Result<()> {
        let operation = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if operation == self.fail_read_at.load(Ordering::SeqCst) {
            Err(io::Error::other("injected TurboQuant read failure"))
        } else {
            Ok(())
        }
    }

    fn before_publication(&self) -> io::Result<()> {
        let operation = self.publications.fetch_add(1, Ordering::SeqCst) + 1;
        if operation == self.fail_publication_at.load(Ordering::SeqCst) {
            Err(io::Error::other("injected TurboQuant publication failure"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
struct FaultStore {
    inner: Arc<MemStore>,
    control: Arc<FaultControl>,
}

impl FaultStore {
    fn new() -> Self {
        let control = Arc::new(FaultControl::default());
        control.reset();
        Self {
            inner: Arc::new(MemStore::new()),
            control,
        }
    }
}

impl Store for FaultStore {
    type Error = io::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.control.before_read()?;
        Store::get(&*self.inner, key).map_err(|error| io::Error::other(error.to_string()))
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&*self.inner, key, value).map_err(|error| io::Error::other(error.to_string()))
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&*self.inner, key).map_err(|error| io::Error::other(error.to_string()))
    }

    fn batch(&self, operations: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(&*self.inner, operations).map_err(|error| io::Error::other(error.to_string()))
    }

    fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.control.before_publication()?;
        Store::publish_nodes(&*self.inner, publication)
            .map_err(|error| io::Error::other(error.to_string()))
    }
}

fn records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|record| ProximityRecord {
            key: format!("vector-{record:04}").into_bytes(),
            vector: (0..dimensions)
                .map(|dimension| {
                    let base = ((record * 31 + dimension * 17) % 257) as f32 / 19.0;
                    if record == 0 {
                        0.0
                    } else {
                        base - 6.0
                    }
                })
                .collect(),
            value: record.to_le_bytes().to_vec(),
        })
        .collect()
}

fn forced_turboquant_request(query: &[f32], k: usize) -> SearchRequest<'_> {
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    request
}

fn assert_every_cold_read_fails<F>(control: &FaultControl, operation: F) -> usize
where
    F: Fn() -> Result<(), prolly::Error>,
{
    control.reset();
    operation().expect("unfaulted operation must succeed");
    let reads = control.reads.load(Ordering::SeqCst);
    assert!(
        reads > 0,
        "operation must cross at least one store read boundary"
    );
    for fail_at in 1..=reads {
        control.fail_read(fail_at);
        let error = operation().expect_err("injected read must fail closed");
        assert!(
            matches!(error, prolly::Error::Store(_)),
            "read {fail_at}/{reads} returned the wrong error: {error:?}",
        );
        assert!(
            control.reads.load(Ordering::SeqCst) >= fail_at,
            "fault boundary {fail_at}/{reads} was not reached",
        );
    }
    control.reset();
    reads
}

#[test]
fn turboquant_is_canonical_bounded_verified_and_exhaustively_reranked() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let store = Arc::new(MemStore::new());
        let mut map_config = ProximityConfig::new(128);
        map_config.metric = metric;
        let mut source = records(193, 128);
        if metric == DistanceMetric::Cosine {
            source[0].vector[0] = 1.0;
        }
        let map = ProximityMap::build(store.clone(), map_config, source).unwrap();
        let config = TurboQuantizationConfig {
            bit_width: 4,
            rerank_multiplier: 256,
            seed: 0x5eed,
        };
        let (serial, serial_stats) =
            TurboQuantizer::build(&map, config.clone(), BuildParallelism::serial()).unwrap();
        assert_eq!(serial_stats.encoded_vectors, 193);
        assert_eq!(
            serial_stats.zero_vectors,
            usize::from(metric != DistanceMetric::Cosine),
        );
        assert_eq!(serial_stats.encoded_output_bytes, 193 * (8 + 64));
        assert_eq!(serial.verify(&map).unwrap().quality, serial.quality());

        for threads in [2, 4] {
            let (parallel, stats) = TurboQuantizer::build(
                &map,
                config.clone(),
                BuildParallelism::new(threads).unwrap(),
            )
            .unwrap();
            assert_eq!(parallel.manifest_cid(), serial.manifest_cid());
            assert_eq!(parallel.quality(), serial.quality());
            assert_eq!(stats, serial_stats);
        }

        assert!(matches!(
            TurboQuantizer::build_with_limits(
                &map,
                config.clone(),
                BuildParallelism::serial(),
                TurboQuantizationBuildLimits {
                    max_encoded_output_bytes: Some(1),
                    ..Default::default()
                },
            ),
            Err(prolly::Error::ProximityResourceLimitExceeded { .. })
        ));

        let loaded = TurboQuantizer::load(store.clone(), serial.manifest_cid().clone()).unwrap();
        let query: Vec<_> = (0..128).map(|index| index as f32 / 23.0 - 2.0).collect();
        let mut exact_request = SearchRequest::exact(&query, 17);
        exact_request.filter = ProximityFilter::Prefix(b"vector-0");
        let exact = map.search(exact_request).unwrap();

        let mut accelerated_request = SearchRequest::exact(&query, 17);
        accelerated_request.policy = SearchPolicy::FixedBudget;
        accelerated_request.options.backend = SearchBackend::TurboQuantized;
        accelerated_request.filter = ProximityFilter::Prefix(b"vector-0");
        let accelerated = loaded.search(&map, accelerated_request).unwrap();
        assert_eq!(accelerated.neighbors, exact.neighbors);
        assert_eq!(accelerated.plan.backend, SearchBackend::TurboQuantized);
        assert_eq!(accelerated.stats.reranked_candidates, 193);
        assert!(accelerated.stats.quantized_distance_evaluations > 0);
    }
}

#[test]
fn turboquant_all_supported_codes_and_representative_dimensions_rerank_exactly() {
    for dimensions in [8usize, 24, 200] {
        for bit_width in [2, 3, 4] {
            for metric in [
                DistanceMetric::L2Squared,
                DistanceMetric::Cosine,
                DistanceMetric::InnerProduct,
            ] {
                let store = Arc::new(MemStore::new());
                let mut map_config = ProximityConfig::new(dimensions as u32);
                map_config.metric = metric;
                let mut source = records(33, dimensions);
                if metric == DistanceMetric::Cosine {
                    source[0].vector[0] = 1.0;
                }
                let map = ProximityMap::build(store, map_config, source).unwrap();
                let (index, stats) = TurboQuantizer::build(
                    &map,
                    TurboQuantizationConfig {
                        bit_width,
                        rerank_multiplier: 64,
                        seed: 0x5eed,
                    },
                    BuildParallelism::new(2).unwrap(),
                )
                .unwrap();
                assert_eq!(
                    stats.encoded_output_bytes,
                    33 * (8 + (dimensions * bit_width as usize).div_ceil(8)),
                );
                let query: Vec<_> = (0..dimensions)
                    .map(|coordinate| coordinate as f32 / 29.0 - 1.0)
                    .collect();
                let exact = map.search(SearchRequest::exact(&query, 7)).unwrap();
                let accelerated = index
                    .search(&map, forced_turboquant_request(&query, 7))
                    .unwrap();
                assert_eq!(
                    accelerated.neighbors, exact.neighbors,
                    "dimensions={dimensions}, bits={bit_width}, metric={metric:?}",
                );
                assert_eq!(
                    accelerated.completion,
                    SearchCompletion::ApproximatePolicySatisfied,
                );
                assert_eq!(accelerated.stats.reranked_candidates, 33);
            }
        }
    }

    let store = Arc::new(MemStore::new());
    let source = records(65, 128);
    let forward =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), source.clone()).unwrap();
    let reverse =
        ProximityMap::build(store, ProximityConfig::new(128), source.into_iter().rev()).unwrap();
    assert_eq!(forward.tree().descriptor, reverse.tree().descriptor);
    let config = TurboQuantizationConfig {
        bit_width: 3,
        rerank_multiplier: 16,
        seed: u64::MAX,
    };
    let (forward_index, forward_stats) =
        TurboQuantizer::build(&forward, config.clone(), BuildParallelism::serial()).unwrap();
    let (reverse_index, reverse_stats) =
        TurboQuantizer::build(&reverse, config, BuildParallelism::new(4).unwrap()).unwrap();
    assert_eq!(forward_index.manifest_cid(), reverse_index.manifest_cid());
    assert_eq!(forward_stats, reverse_stats);
}

#[test]
fn turboquant_filters_lookup_modes_cache_and_budgets_are_deterministic() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(193, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let manifest = index.manifest_cid().clone();
    let query: Vec<_> = (0..128)
        .map(|coordinate| coordinate as f32 / 31.0 - 1.0)
        .collect();
    let eligible: Vec<_> = (0..100)
        .map(|record| format!("vector-{record:04}").into_bytes())
        .collect();

    let mut prefix_request = forced_turboquant_request(&query, 17);
    prefix_request.filter = ProximityFilter::Prefix(b"vector-00");
    let prefix = index.search(&map, prefix_request).unwrap();
    assert!(!prefix.plan.direct_lookup);

    let mut eligible_request = forced_turboquant_request(&query, 17);
    eligible_request.filter = ProximityFilter::EligibleKeys(&eligible);
    let direct = index.search(&map, eligible_request).unwrap();
    assert!(direct.plan.direct_lookup);
    assert_eq!(direct.neighbors, prefix.neighbors);

    let mut secondary_request = forced_turboquant_request(&query, 17);
    secondary_request.filter = ProximityFilter::SecondaryEligible {
        keys: &eligible,
        source_directory: &map.tree().directory,
    };
    let secondary = index.search(&map, secondary_request).unwrap();
    assert_eq!(secondary.neighbors, direct.neighbors);

    let start = b"vector-0020".as_slice();
    let end = b"vector-0040".as_slice();
    let mut range_request = forced_turboquant_request(&query, 17);
    range_request.filter = ProximityFilter::KeyRange {
        start: Some(start),
        end: Some(end),
    };
    let range = index.search(&map, range_request).unwrap();
    let mut exact_range_request = SearchRequest::exact(&query, 17);
    exact_range_request.filter = ProximityFilter::KeyRange {
        start: Some(start),
        end: Some(end),
    };
    assert_eq!(
        range.neighbors,
        map.search(exact_range_request).unwrap().neighbors,
    );

    let cold_index = TurboQuantizer::load(store.clone(), manifest.clone()).unwrap();
    map.clear_content_cache().unwrap();
    let cold = cold_index
        .search(&map, forced_turboquant_request(&query, 17))
        .unwrap();
    let warm = cold_index
        .search(&map, forced_turboquant_request(&query, 17))
        .unwrap();
    assert_eq!(cold.neighbors, warm.neighbors);
    assert_eq!(cold.plan, warm.plan);
    assert_eq!(cold.completion, warm.completion);
    let mut cold_logical = cold.stats;
    let mut warm_logical = warm.stats;
    cold_logical.physical_bytes_read = 0;
    warm_logical.physical_bytes_read = 0;
    assert_eq!(cold_logical, warm_logical);

    let budgeted = |index: &TurboQuantizer<Arc<MemStore>>| {
        let mut request = forced_turboquant_request(&query, 17);
        request.budget = SearchBudget {
            max_distance_evaluations: Some(75),
            ..SearchBudget::default()
        };
        index.search(&map, request).unwrap()
    };
    let first = budgeted(&TurboQuantizer::load(store.clone(), manifest.clone()).unwrap());
    let second = budgeted(&TurboQuantizer::load(store, manifest).unwrap());
    assert_eq!(first, second);
    assert_eq!(first.completion, SearchCompletion::BudgetExhausted);
    assert_eq!(first.stats.quantized_distance_evaluations, 75);
    assert_eq!(first.stats.reranked_candidates, 0);
}

#[test]
fn turboquant_catalog_and_typed_graph_include_complete_closure() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(33, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let manifest = index.manifest_cid().clone();
    let set = AcceleratorSet::empty()
        .with_turboquant(map.tree(), index)
        .unwrap();
    let catalog = AcceleratorCatalog::build(store.clone(), map.tree(), set).unwrap();
    let reopened =
        AcceleratorCatalog::load(store.clone(), catalog.manifest_cid().clone(), map.tree())
            .unwrap();
    assert_eq!(reopened.entries().len(), 1);
    let walk = walk_content_graph(
        &store,
        &[TypedContentRoot::new(
            ContentObjectKind::TurboQuantization,
            manifest,
        )],
        &ContentGraphLimits::default(),
    )
    .unwrap();
    assert_eq!(
        walk.objects_by_kind
            .get(&ContentObjectKind::TurboQuantization),
        Some(&1)
    );
    assert!(
        walk.objects_by_kind
            .get(&ContentObjectKind::OrderedNode)
            .copied()
            .unwrap_or_default()
            > 0
    );
}

#[test]
fn turboquant_missing_or_corrupt_content_fails_closed() {
    use prolly::{Cid, Store};

    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(33, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let manifest = index.manifest_cid().clone();
    let manifest_bytes = store.get(manifest.as_bytes()).unwrap().unwrap();

    for (offset, replacement) in [(4usize, 0xff), (5, 1)] {
        let mut corrupt = manifest_bytes.clone();
        corrupt[offset] = replacement;
        let cid = Cid::from_bytes(&corrupt);
        store.put(cid.as_bytes(), &corrupt).unwrap();
        assert!(TurboQuantizer::load(store.clone(), cid).is_err());
    }
    let mut trailing = manifest_bytes;
    trailing.push(0);
    let trailing_cid = Cid::from_bytes(&trailing);
    store.put(trailing_cid.as_bytes(), &trailing).unwrap();
    assert!(TurboQuantizer::load(store.clone(), trailing_cid).is_err());

    let walk = walk_content_graph(
        &store,
        &[TypedContentRoot::new(
            ContentObjectKind::TurboQuantization,
            manifest,
        )],
        &ContentGraphLimits::default(),
    )
    .unwrap();
    let code_root = walk
        .objects
        .iter()
        .find(|object| object.root.kind == ContentObjectKind::OrderedNode)
        .unwrap()
        .root
        .cid
        .clone();
    store.delete(code_root.as_bytes()).unwrap();
    assert!(index.verify(&map).is_err());
}

#[test]
fn turboquant_fails_closed_at_every_publication_boundary() {
    let successful_store = FaultStore::new();
    let successful_map = ProximityMap::build(
        successful_store.clone(),
        ProximityConfig::new(128),
        records(193, 128),
    )
    .unwrap();
    successful_store.control.reset();
    let (successful, _) = TurboQuantizer::build(
        &successful_map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let expected_manifest = successful.manifest_cid().clone();
    let publications = successful_store.control.publications.load(Ordering::SeqCst);
    assert!(
        publications >= 2,
        "code tree and manifest publish separately"
    );
    eprintln!("TurboQuant publication boundaries exercised: {publications}");

    for fail_at in 1..=publications {
        let store = FaultStore::new();
        let map = ProximityMap::build(store.clone(), ProximityConfig::new(128), records(193, 128))
            .unwrap();
        store.control.fail_publication(fail_at);
        let error = match TurboQuantizer::build(
            &map,
            TurboQuantizationConfig::default(),
            BuildParallelism::serial(),
        ) {
            Ok(_) => panic!("injected publication must fail the build"),
            Err(error) => error,
        };
        assert!(matches!(error, prolly::Error::Store(_)));
        assert_eq!(
            store.control.publications.load(Ordering::SeqCst),
            fail_at,
            "build continued after publication failure {fail_at}/{publications}",
        );
        assert!(
            Store::get(&*store.inner, expected_manifest.as_bytes())
                .unwrap()
                .is_none(),
            "failed build published its manifest at boundary {fail_at}/{publications}",
        );
    }
}

#[test]
fn turboquant_fails_closed_at_every_manifest_tree_proof_and_rerank_read() {
    let store = FaultStore::new();
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(193, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig {
            rerank_multiplier: 8,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let descriptor = map.tree().descriptor.clone();
    let manifest = index.manifest_cid().clone();
    let query: Vec<_> = (0..128)
        .map(|coordinate| coordinate as f32 / 31.0 - 1.0)
        .collect();

    let verify_reads = assert_every_cold_read_fails(&store.control, || {
        let map = ProximityMap::load(store.clone(), descriptor.clone())?;
        let index = TurboQuantizer::load(store.clone(), manifest.clone())?;
        index.verify(&map).map(|_| ())
    });
    let search_reads = assert_every_cold_read_fails(&store.control, || {
        let map = ProximityMap::load(store.clone(), descriptor.clone())?;
        let index = TurboQuantizer::load(store.clone(), manifest.clone())?;
        index
            .search(&map, forced_turboquant_request(&query, 7))
            .map(|_| ())
    });
    let proof_reads = assert_every_cold_read_fails(&store.control, || {
        let map = ProximityMap::load(store.clone(), descriptor.clone())?;
        let index = TurboQuantizer::load(store.clone(), manifest.clone())?;
        index
            .prove_search(
                &map,
                forced_turboquant_request(&query, 7),
                &ContentGraphLimits::default(),
            )
            .map(|_| ())
    });

    eprintln!(
        "TurboQuant cold-read boundaries exercised: verify={verify_reads}, search={search_reads}, proof={proof_reads}"
    );
    assert!(verify_reads > search_reads / 2);
    assert!(proof_reads >= search_reads);
}

#[test]
fn turboquant_rejects_unsupported_dimensions_and_stale_sources() {
    let store = Arc::new(MemStore::new());
    let unsupported =
        ProximityMap::build(store.clone(), ProximityConfig::new(7), records(17, 7)).unwrap();
    assert!(TurboQuantizer::build(
        &unsupported,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .is_err());

    let map = ProximityMap::build(store.clone(), ProximityConfig::new(8), records(17, 8)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let changed = ProximityMap::build(store, ProximityConfig::new(8), records(18, 8)).unwrap();
    let query = vec![1.0; 8];
    let mut request = SearchRequest::exact(&query, 1);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    assert!(index.search(&changed, request).is_err());
}

#[test]
fn turboquant_remains_explicit_only_until_auto_qualification() {
    use prolly::{ApproximatePreference, SearchIo, SearchRuntime};

    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(65, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig::default(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let accelerators = AcceleratorSet::empty()
        .with_turboquant(map.tree(), index)
        .unwrap();
    let query = vec![0.25; 128];
    let mut request = SearchRequest::exact(&query, 7);
    request.policy = SearchPolicy::FixedBudget;
    request.options.planner.approximate_preference = ApproximatePreference::TurboQuantizedFirst;
    let result = map
        .search_with(
            &accelerators,
            &SearchIo::new(store, Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(result.plan.backend, SearchBackend::Native);
}

#[test]
fn turboquant_proof_replays_the_committed_plan_and_closure() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(65, 128)).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig {
            rerank_multiplier: 128,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let query = vec![0.25; 128];
    let mut request = SearchRequest::exact(&query, 7);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    let proof = index
        .prove_search(&map, request, &ContentGraphLimits::default())
        .unwrap();
    let verified = proof.verify(&ContentGraphLimits::default()).unwrap();
    assert_eq!(verified.result.plan.backend, SearchBackend::TurboQuantized);

    let mut incomplete = proof;
    incomplete.accelerator_objects.pop();
    assert!(incomplete.verify(&ContentGraphLimits::default()).is_err());
}

#[test]
fn turboquant_composite_shadows_base_and_merges_exact_delta() {
    use prolly::{
        CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits,
        CompositeBuildOutcome, ProximityMutation, SearchIo, SearchRuntime,
    };

    let store = Arc::new(MemStore::new());
    let base_map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(97, 128)).unwrap();
    let (base, _) = TurboQuantizer::build(
        &base_map,
        TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let replacement = vec![0.01; 128];
    let (current, _) = base_map
        .mutate_batch([
            ProximityMutation {
                key: b"vector-0001".to_vec(),
                value: Some((replacement, b"updated".to_vec())),
            },
            ProximityMutation {
                key: b"vector-0002".to_vec(),
                value: None,
            },
        ])
        .unwrap();
    let composite = match CompositeAccelerator::build(
        &base_map,
        &current,
        CompositeBase::TurboQuantized(base),
        CompositeAcceleratorConfig {
            max_delta_records: 10,
            max_shadow_records: 10,
            max_delta_ratio_ppm: 1_000_000,
            max_shadow_ratio_ppm: 1_000_000,
            base_overfetch_multiplier: 2,
        },
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, .. } => *accelerator,
        CompositeBuildOutcome::FullRebuildRequired { .. } => panic!("unexpected rebuild"),
    };
    let set = AcceleratorSet::empty()
        .with_composite(current.tree(), composite)
        .unwrap();
    let query = vec![0.0; 128];
    let exact = current.search(SearchRequest::exact(&query, 12)).unwrap();
    let mut request = SearchRequest::exact(&query, 12);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let accelerated = current
        .search_with(
            &set,
            &SearchIo::new(store, Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(accelerated.neighbors, exact.neighbors);
}

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_build_load_search_verify_and_cancel_match_sync() {
    use prolly::{
        AsyncProximityMap, AsyncSearchControl, AsyncTurboQuantizer, CancellationToken,
        SearchCompletion, SyncStoreAsAsync,
    };
    use std::future::Future;
    use std::task::{Context, Poll};

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    block_on(async {
        let store = Arc::new(MemStore::new());
        let async_store = SyncStoreAsAsync::new(store.clone());
        let asynchronous = AsyncProximityMap::build(
            async_store.clone(),
            ProximityConfig::new(128),
            records(129, 128),
        )
        .await
        .unwrap();
        let config = TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        };
        let (index, _) =
            AsyncTurboQuantizer::build(&asynchronous, config, BuildParallelism::new(2).unwrap())
                .await
                .unwrap();
        index
            .verify(&asynchronous, &ContentGraphLimits::default())
            .await
            .unwrap();
        let loaded = AsyncTurboQuantizer::load(&async_store, index.manifest_cid().clone())
            .await
            .unwrap();
        let query: Vec<_> = (0..128).map(|index| index as f32 / 29.0 - 1.0).collect();
        let sync = ProximityMap::load(store.clone(), asynchronous.tree().descriptor.clone())
            .unwrap()
            .search(SearchRequest::exact(&query, 13))
            .unwrap();
        let mut request = SearchRequest::exact(&query, 13);
        request.policy = SearchPolicy::FixedBudget;
        request.options.backend = SearchBackend::TurboQuantized;
        let result = loaded
            .search(
                &asynchronous,
                request.clone(),
                AsyncSearchControl::default(),
            )
            .await
            .unwrap();
        assert_eq!(result.neighbors, sync.neighbors);
        assert_eq!(result.plan.backend, SearchBackend::TurboQuantized);

        let cancellation = CancellationToken::default();
        cancellation.cancel();
        let cancelled = loaded
            .search(
                &asynchronous,
                request,
                AsyncSearchControl {
                    cancellation: Some(cancellation),
                    ..AsyncSearchControl::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(cancelled.completion, SearchCompletion::Cancelled);
        assert!(cancelled.neighbors.is_empty());
    });
}
