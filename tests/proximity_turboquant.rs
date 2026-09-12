use prolly::{
    copy_content_graph, plan_content_gc, sweep_content_gc, walk_content_graph, AcceleratorCatalog,
    AcceleratorSet, BatchOp, BuildParallelism, ContentGraphLimits, ContentObjectKind,
    DistanceMetric, MemStore, NodePublication, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityRecord, SearchBackend, SearchBudget, SearchCompletion, SearchPolicy, SearchRequest,
    Store, TurboQuantizationBuildLimits, TurboQuantizationConfig, TurboQuantizer, TypedContentRoot,
};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct PublicationBoundAsyncStore {
    inner: Arc<MemStore>,
    publications: Arc<AtomicUsize>,
    maximum_batch: Arc<AtomicUsize>,
}

#[cfg(feature = "async-store")]
impl PublicationBoundAsyncStore {
    fn new(inner: Arc<MemStore>) -> Self {
        Self {
            inner,
            publications: Arc::new(AtomicUsize::new(0)),
            maximum_batch: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[cfg(feature = "async-store")]
impl prolly::AsyncStore for PublicationBoundAsyncStore {
    type Error = prolly::MemStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        Store::get(&self.inner, key)
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&self.inner, key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&self.inner, key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(&self.inner, ops)
    }

    async fn publish_nodes(&self, publication: NodePublication<'_>) -> Result<(), Self::Error> {
        self.publications.fetch_add(1, Ordering::SeqCst);
        self.maximum_batch
            .fetch_max(publication.entries().len(), Ordering::SeqCst);
        Store::publish_nodes(&self.inner, publication)
    }
}

#[cfg(feature = "async-store")]
#[derive(Clone)]
struct CancellingReadAsyncStore {
    inner: Arc<MemStore>,
    reads: Arc<AtomicUsize>,
    cancel_after: Arc<AtomicUsize>,
    cancellation: prolly::CancellationToken,
}

#[cfg(feature = "async-store")]
impl CancellingReadAsyncStore {
    fn new(inner: Arc<MemStore>, cancellation: prolly::CancellationToken) -> Self {
        Self {
            inner,
            reads: Arc::new(AtomicUsize::new(0)),
            cancel_after: Arc::new(AtomicUsize::new(usize::MAX)),
            cancellation,
        }
    }

    fn arm(&self, read: usize) {
        assert!(read > 0);
        self.reads.store(0, Ordering::SeqCst);
        self.cancel_after.store(read, Ordering::SeqCst);
    }

    fn reset_reads(&self) {
        self.reads.store(0, Ordering::SeqCst);
        self.cancel_after.store(usize::MAX, Ordering::SeqCst);
    }

    fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    fn after_read(&self) {
        let read = self.reads.fetch_add(1, Ordering::SeqCst) + 1;
        if read == self.cancel_after.load(Ordering::SeqCst) {
            self.cancellation.cancel();
        }
    }
}

#[cfg(feature = "async-store")]
impl prolly::AsyncStore for CancellingReadAsyncStore {
    type Error = prolly::MemStoreError;

    async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let result = Store::get(&self.inner, key);
        self.after_read();
        result
    }

    async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        Store::put(&self.inner, key, value)
    }

    async fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        Store::delete(&self.inner, key)
    }

    async fn batch(&self, ops: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        Store::batch(&self.inner, ops)
    }
}

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
fn turboquant_every_build_limit_fails_typed_before_manifest_publication() {
    let count = 33usize;
    let dimensions = 128usize;
    let source = records(count, dimensions);
    let config = TurboQuantizationConfig {
        bit_width: 4,
        rerank_multiplier: 8,
        seed: 41,
    };
    let oracle_store = Arc::new(MemStore::new());
    let oracle_map = ProximityMap::build(
        oracle_store,
        ProximityConfig::new(dimensions as u32),
        source.clone(),
    )
    .unwrap();
    let (oracle, _) =
        TurboQuantizer::build(&oracle_map, config.clone(), BuildParallelism::serial()).unwrap();
    let manifest = oracle.manifest_cid().clone();
    let cases = [
        (
            "TurboQuant records",
            1,
            TurboQuantizationBuildLimits {
                max_records: Some(count - 1),
                ..Default::default()
            },
        ),
        (
            "TurboQuant input bytes",
            1,
            TurboQuantizationBuildLimits {
                max_input_bytes: Some(1),
                ..Default::default()
            },
        ),
        (
            "TurboQuant temporary bytes",
            1,
            TurboQuantizationBuildLimits {
                max_temporary_bytes: Some(1),
                ..Default::default()
            },
        ),
        (
            "TurboQuant transform operations",
            1,
            TurboQuantizationBuildLimits {
                max_transform_operations: Some(1),
                ..Default::default()
            },
        ),
        (
            "TurboQuant encoded output bytes",
            1,
            TurboQuantizationBuildLimits {
                max_encoded_output_bytes: Some(1),
                ..Default::default()
            },
        ),
        (
            "TurboQuant worker threads",
            2,
            TurboQuantizationBuildLimits {
                max_worker_threads: Some(1),
                ..Default::default()
            },
        ),
    ];

    for (resource, workers, limits) in &cases {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            ProximityConfig::new(dimensions as u32),
            source.clone(),
        )
        .unwrap();
        let error = match TurboQuantizer::build_with_limits(
            &map,
            config.clone(),
            BuildParallelism::new(*workers).unwrap(),
            limits.clone(),
        ) {
            Err(error) => error,
            Ok(_) => panic!("configured TurboQuant limit must fail"),
        };
        let prolly::Error::ProximityResourceLimitExceeded {
            resource: actual_resource,
            limit,
            actual,
        } = error
        else {
            panic!("unexpected limit error: {error:?}");
        };
        assert_eq!(actual_resource, *resource);
        assert!(actual > limit);
        assert!(Store::get(&store, manifest.as_bytes()).unwrap().is_none());
    }

    #[cfg(feature = "async-store")]
    {
        use prolly::{AsyncProximityMap, AsyncTurboQuantizer, SyncStoreAsAsync};
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

        for (resource, workers, limits) in cases {
            let store = Arc::new(MemStore::new());
            block_on(async {
                let map = AsyncProximityMap::build(
                    SyncStoreAsAsync::new(store.clone()),
                    ProximityConfig::new(dimensions as u32),
                    source.clone(),
                )
                .await
                .unwrap();
                let error = match AsyncTurboQuantizer::build_with_limits(
                    &map,
                    config.clone(),
                    BuildParallelism::new(workers).unwrap(),
                    limits,
                    2,
                    &ContentGraphLimits::default(),
                )
                .await
                {
                    Err(error) => error,
                    Ok(_) => panic!("configured async TurboQuant limit must fail"),
                };
                let prolly::Error::ProximityResourceLimitExceeded {
                    resource: actual_resource,
                    limit,
                    actual,
                } = error
                else {
                    panic!("unexpected async limit error: {error:?}");
                };
                assert_eq!(actual_resource, resource);
                assert!(actual > limit);
                assert!(Store::get(&store, manifest.as_bytes()).unwrap().is_none());
            });
        }
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
    let mut corrupt_catalog = Store::get(&store, catalog.manifest_cid().as_bytes())
        .unwrap()
        .unwrap();
    let fingerprint = catalog.entries()[0].configuration_fingerprint.as_bytes();
    let fingerprint_offset = corrupt_catalog
        .windows(fingerprint.len())
        .position(|window| window == fingerprint)
        .expect("catalog contains the TurboQuant configuration fingerprint");
    corrupt_catalog[fingerprint_offset + fingerprint.len() - 1] ^= 1;
    let corrupt_catalog_cid = prolly::Cid::from_bytes(&corrupt_catalog);
    Store::put(&store, corrupt_catalog_cid.as_bytes(), &corrupt_catalog).unwrap();
    match AcceleratorCatalog::load(store.clone(), corrupt_catalog_cid, map.tree()) {
        Err(prolly::Error::InvalidProximityObject { kind, reason }) => {
            assert_eq!(kind, "accelerator catalog");
            assert_eq!(reason, "catalog TurboQuant fingerprint mismatch");
        }
        _ => panic!("mutated TurboQuant catalog fingerprint must fail closed"),
    }
    let other_map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(34, 128)).unwrap();
    match AcceleratorCatalog::load(
        store.clone(),
        catalog.manifest_cid().clone(),
        other_map.tree(),
    ) {
        Err(prolly::Error::InvalidProximityObject { kind, reason }) => {
            assert_eq!(kind, "accelerator catalog");
            assert_eq!(reason, "catalog is bound to a different source snapshot");
        }
        _ => panic!("TurboQuant catalog must reject another source snapshot"),
    }
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
    use prolly::{Cid, ProximitySearchClaim, SearchPlan};

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

    let assert_rejected = |candidate: &prolly::ProximitySearchProof| {
        assert!(candidate.verify(&ContentGraphLimits::default()).is_err());
    };

    let mut fuzz_state = 0xa076_1d64_78bd_642fu64;
    for _ in 0..64 {
        fuzz_state ^= fuzz_state << 13;
        fuzz_state ^= fuzz_state >> 7;
        fuzz_state ^= fuzz_state << 17;
        let mut candidate = proof.clone();
        let event = fuzz_state as usize % candidate.events.len();
        candidate.events.remove(event);
        assert_rejected(&candidate);
    }

    let mut version = proof.clone();
    version.format_version = version.format_version.saturating_sub(1);
    version.request.query[0] += 1.0;
    version.accelerator_objects.pop();
    match version.verify(&ContentGraphLimits::default()) {
        Err(prolly::Error::UnsupportedProximityVersion { found, required }) => {
            assert_eq!(found, version.format_version);
            assert_eq!(required, proof.format_version);
        }
        other => panic!("old TurboQuant proof version must fail explicitly: {other:?}"),
    }

    let mut source = proof.clone();
    source.source.descriptor = Cid::from_bytes(b"tampered source");
    assert_rejected(&source);

    let mut root = proof.clone();
    root.accelerator_root.as_mut().unwrap().kind = ContentObjectKind::ProductQuantization;
    assert_rejected(&root);

    let mut object = proof.clone();
    object.accelerator_objects[0].bytes[0] ^= 1;
    assert_rejected(&object);

    let mut request = proof.clone();
    request.request.query[0] += 1.0;
    assert_rejected(&request);

    let mut commitment = proof.clone();
    commitment.request_commitment = Cid::from_bytes(b"tampered request commitment");
    assert_rejected(&commitment);

    let mut result = proof.clone();
    result.result.neighbors[0].distance += 1.0;
    assert_rejected(&result);

    let mut plan = proof.clone();
    match &mut plan.plan {
        SearchPlan::TurboQuantized { rerank_target, .. } => *rerank_target += 1,
        other => panic!("unexpected TurboQuant proof plan: {other:?}"),
    }
    assert_rejected(&plan);

    let mut transcript = proof.clone();
    transcript.events.pop();
    assert_rejected(&transcript);

    let mut claim = proof.clone();
    claim.claim = ProximitySearchClaim::ExactL2Optimal {
        terminal_lower_bound: 0.0,
    };
    assert_rejected(&claim);

    let mut incomplete = proof;
    incomplete.accelerator_objects.pop();
    assert_rejected(&incomplete);
}

#[test]
fn turboquant_proofs_cover_every_metric_filter_and_budget_completion() {
    let limits = ContentGraphLimits::default();
    let eligible: Vec<_> = (10..40)
        .map(|record| format!("vector-{record:04}").into_bytes())
        .collect();
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let store = Arc::new(MemStore::new());
        let mut source = records(65, 128);
        if metric == DistanceMetric::Cosine {
            source[0].vector[0] = 1.0;
        }
        let mut config = ProximityConfig::new(128);
        config.metric = metric;
        let map = ProximityMap::build(store, config, source).unwrap();
        let (index, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig {
                rerank_multiplier: 128,
                ..TurboQuantizationConfig::default()
            },
            BuildParallelism::new(2).unwrap(),
        )
        .unwrap();
        let query: Vec<_> = (0..128)
            .map(|coordinate| coordinate as f32 / 41.0 - 1.0)
            .collect();

        for filter_case in 0..5 {
            for budgeted in [false, true] {
                let mut request = forced_turboquant_request(&query, 5);
                request.filter = match filter_case {
                    0 => ProximityFilter::All,
                    1 => ProximityFilter::Prefix(b"vector-0"),
                    2 => ProximityFilter::KeyRange {
                        start: Some(b"vector-0010"),
                        end: Some(b"vector-0050"),
                    },
                    3 => ProximityFilter::EligibleKeys(&eligible),
                    4 => ProximityFilter::SecondaryEligible {
                        keys: &eligible,
                        source_directory: &map.tree().directory,
                    },
                    _ => unreachable!(),
                };
                if budgeted {
                    request.budget.max_distance_evaluations = Some(3);
                }
                let proof = index.prove_search(&map, request, &limits).unwrap();
                let verified = proof.verify(&limits).unwrap();
                assert_eq!(verified.result, proof.result);
                assert_eq!(verified.result.plan.backend, SearchBackend::TurboQuantized);
                assert_eq!(
                    verified.result.completion,
                    if budgeted {
                        SearchCompletion::BudgetExhausted
                    } else {
                        SearchCompletion::ApproximatePolicySatisfied
                    },
                    "metric={metric:?}, filter_case={filter_case}, budgeted={budgeted}",
                );
            }
        }
    }
}

#[test]
fn turboquant_copy_reopen_search_and_gc_preserve_only_the_live_closure() {
    let store = Arc::new(MemStore::new());
    let map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(65, 128)).unwrap();
    let config = TurboQuantizationConfig {
        rerank_multiplier: 128,
        seed: 0x5eed,
        ..TurboQuantizationConfig::default()
    };
    let (index, _) =
        TurboQuantizer::build(&map, config.clone(), BuildParallelism::new(2).unwrap()).unwrap();
    let manifest = index.manifest_cid().clone();
    let root = TypedContentRoot::new(ContentObjectKind::TurboQuantization, manifest.clone());
    let limits = ContentGraphLimits::default();
    let live = walk_content_graph(&store, std::slice::from_ref(&root), &limits).unwrap();

    let destination = Arc::new(MemStore::new());
    let copied = copy_content_graph(&store, &destination, root.clone(), &limits).unwrap();
    assert_eq!(copied.copied_objects, copied.required_objects);
    let copied_map =
        ProximityMap::load(destination.clone(), map.tree().descriptor.clone()).unwrap();
    let copied_index = TurboQuantizer::load(destination, manifest.clone()).unwrap();
    copied_index.verify(&copied_map).unwrap();
    let query: Vec<_> = (0..128)
        .map(|coordinate| coordinate as f32 / 37.0 - 1.0)
        .collect();
    let exact = copied_map.search(SearchRequest::exact(&query, 11)).unwrap();
    let accelerated = copied_index
        .search(&copied_map, forced_turboquant_request(&query, 11))
        .unwrap();
    assert_eq!(accelerated.neighbors, exact.neighbors);

    let orphan_map =
        ProximityMap::build(store.clone(), ProximityConfig::new(128), records(66, 128)).unwrap();
    let (orphan, _) =
        TurboQuantizer::build(&orphan_map, config, BuildParallelism::serial()).unwrap();
    let orphan_manifest = orphan.manifest_cid().clone();
    let orphan_root = TypedContentRoot::new(
        ContentObjectKind::TurboQuantization,
        orphan_manifest.clone(),
    );
    let orphan_walk = walk_content_graph(&store, &[orphan_root], &limits).unwrap();
    let mut candidates: Vec<_> = live
        .objects
        .iter()
        .chain(&orphan_walk.objects)
        .map(|object| object.root.cid.clone())
        .collect();
    candidates.sort();
    candidates.dedup();
    let plan = plan_content_gc(&store, std::slice::from_ref(&root), &candidates, &limits).unwrap();
    assert!(plan.reclaimable_cids.contains(&orphan_manifest));
    assert!(!plan.reclaimable_cids.contains(&manifest));
    let swept =
        sweep_content_gc(&store, std::slice::from_ref(&root), &candidates, &limits).unwrap();
    assert_eq!(swept.deleted_objects, plan.reclaimable_cids.len());
    assert!(Store::get(&store, orphan_manifest.as_bytes())
        .unwrap()
        .is_none());

    let reopened_map = ProximityMap::load(store.clone(), map.tree().descriptor.clone()).unwrap();
    let reopened = TurboQuantizer::load(store, manifest).unwrap();
    reopened.verify(&reopened_map).unwrap();
    assert_eq!(
        reopened
            .search(&reopened_map, forced_turboquant_request(&query, 11))
            .unwrap()
            .neighbors,
        exact.neighbors,
    );
}

#[test]
fn turboquant_composite_value_only_and_full_rebuild_preserve_configuration() {
    use prolly::{
        CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits,
        CompositeBuildOrRebuildOutcome, CompositeBuildOutcome, CompositeRebuildOptions,
        ProximityMutation, SearchIo, SearchRuntime,
    };

    let store = Arc::new(MemStore::new());
    let source = records(65, 128);
    let original_vector = source[7].vector.clone();
    let base_map = ProximityMap::build(store.clone(), ProximityConfig::new(128), source).unwrap();
    let config = TurboQuantizationConfig {
        bit_width: 3,
        rerank_multiplier: 128,
        seed: 0xfeed_beef,
    };
    let (base, _) =
        TurboQuantizer::build(&base_map, config.clone(), BuildParallelism::serial()).unwrap();
    let (value_changed, _) = base_map
        .mutate_batch([ProximityMutation {
            key: b"vector-0007".to_vec(),
            value: Some((original_vector.clone(), b"value-only".to_vec())),
        }])
        .unwrap();
    let composite = match CompositeAccelerator::build(
        &base_map,
        &value_changed,
        CompositeBase::TurboQuantized(base),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            assert_eq!(stats.value_only_records, 1);
            assert_eq!(stats.delta_records, 0);
            assert_eq!(stats.shadow_records, 0);
            accelerator
        }
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("value-only change unexpectedly required rebuild: {reasons:?}")
        }
    };
    let set = AcceleratorSet::empty()
        .with_composite(value_changed.tree(), *composite)
        .unwrap();
    let mut request = forced_turboquant_request(&original_vector, 1);
    request.options.backend = SearchBackend::Composite;
    let result = value_changed
        .search_with(
            &set,
            &SearchIo::new(store.clone(), Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    assert_eq!(result.neighbors[0].key, b"vector-0007");
    assert_eq!(result.neighbors[0].value, b"value-only");

    let (changed, _) = base_map
        .mutate_batch([ProximityMutation {
            key: b"vector-0008".to_vec(),
            value: Some((vec![0.0; 128], b"vector-change".to_vec())),
        }])
        .unwrap();
    let (base, _) =
        TurboQuantizer::build(&base_map, config.clone(), BuildParallelism::serial()).unwrap();
    let rebuilt = CompositeAccelerator::build_or_rebuild(
        &base_map,
        &changed,
        CompositeBase::TurboQuantized(base),
        CompositeAcceleratorConfig {
            max_delta_records: 0,
            ..CompositeAcceleratorConfig::default()
        },
        CompositeBuildLimits::default(),
        CompositeRebuildOptions {
            turboquant_parallelism: BuildParallelism::new(4).unwrap(),
            ..CompositeRebuildOptions::default()
        },
    )
    .unwrap();
    let (rebuilt, rebuild_stats) = match rebuilt {
        CompositeBuildOrRebuildOutcome::TurboQuantizedRebuilt {
            accelerator,
            reasons,
            rebuild_stats,
            ..
        } => {
            assert!(!reasons.is_empty());
            (accelerator, rebuild_stats)
        }
        _ => panic!("TurboQuant threshold crossing did not rebuild TurboQuant"),
    };
    assert_eq!(rebuilt.config(), &config);
    assert_eq!(rebuilt.source_descriptor(), &changed.tree().descriptor);
    rebuilt.verify(&changed).unwrap();
    let (expected, expected_stats) =
        TurboQuantizer::build(&changed, config, BuildParallelism::serial()).unwrap();
    assert_eq!(rebuilt.manifest_cid(), expected.manifest_cid());
    assert_eq!(rebuild_stats, expected_stats);
}

#[test]
fn turboquant_extreme_vectors_long_keys_and_equal_scores_are_exact_when_exhaustive() {
    for metric in [DistanceMetric::L2Squared, DistanceMetric::InnerProduct] {
        let store = Arc::new(MemStore::new());
        let source = (0usize..17).map(|index| {
            let mut key = vec![b'x'; 8 * 1024];
            key.extend_from_slice(&(index as u64).to_be_bytes());
            let vector = if index == 0 {
                vec![f32::MAX; 8]
            } else if index == 1 {
                vec![-f32::MAX; 8]
            } else {
                (0..8)
                    .map(|coordinate| {
                        if (index + coordinate).is_multiple_of(3) {
                            f32::MIN_POSITIVE
                        } else if (index + coordinate).is_multiple_of(2) {
                            f32::MAX / 4.0
                        } else {
                            -f32::MAX / 4.0
                        }
                    })
                    .collect()
            };
            ProximityRecord {
                key,
                vector,
                value: index.to_le_bytes().to_vec(),
            }
        });
        let mut map_config = ProximityConfig::new(8);
        map_config.metric = metric;
        let map = ProximityMap::build(store, map_config, source).unwrap();
        let (index, _) = TurboQuantizer::build(
            &map,
            TurboQuantizationConfig {
                rerank_multiplier: 32,
                ..TurboQuantizationConfig::default()
            },
            BuildParallelism::new(2).unwrap(),
        )
        .unwrap();
        let query: Vec<_> = (0..8)
            .map(|coordinate| {
                if coordinate % 2 == 0 {
                    f32::MAX / 4.0
                } else {
                    -f32::MAX / 4.0
                }
            })
            .collect();
        let exact = map.search(SearchRequest::exact(&query, 17)).unwrap();
        let accelerated = index
            .search(&map, forced_turboquant_request(&query, 17))
            .unwrap();
        assert_eq!(accelerated.neighbors, exact.neighbors, "metric={metric:?}");
    }

    let store = Arc::new(MemStore::new());
    let equal = (0usize..33).map(|index| ProximityRecord {
        key: format!("equal-{index:04}").into_bytes(),
        vector: vec![1.0; 24],
        value: index.to_le_bytes().to_vec(),
    });
    let map = ProximityMap::build(store, ProximityConfig::new(24), equal).unwrap();
    let (index, _) = TurboQuantizer::build(
        &map,
        TurboQuantizationConfig {
            bit_width: 2,
            rerank_multiplier: 64,
            seed: u64::MAX,
        },
        BuildParallelism::new(4).unwrap(),
    )
    .unwrap();
    let exact = map.search(SearchRequest::exact(&[1.0; 24], 33)).unwrap();
    let accelerated = index
        .search(&map, forced_turboquant_request(&[1.0; 24], 33))
        .unwrap();
    assert_eq!(accelerated.neighbors, exact.neighbors);
    assert!(accelerated
        .neighbors
        .windows(2)
        .all(|pair| pair[0].key < pair[1].key));
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
    use std::time::Instant;

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

        let expired = loaded
            .search(
                &asynchronous,
                request.clone(),
                AsyncSearchControl {
                    deadline: Some(Instant::now()),
                    ..AsyncSearchControl::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(expired.completion, SearchCompletion::DeadlineExceeded);
        assert!(expired.neighbors.is_empty());
        assert_eq!(expired.stats.nodes_read, 0);
        assert_eq!(expired.stats.physical_bytes_read, 0);
        assert_eq!(expired.stats.committed_bytes, 0);
        assert_eq!(expired.stats.quantized_distance_evaluations, 0);
        assert_eq!(expired.stats.distance_evaluations, 0);

        assert!(asynchronous
            .prove_search(request.clone(), &ContentGraphLimits::default())
            .await
            .is_err());

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

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_cancellation_covers_every_store_read_boundary() {
    use prolly::{
        AsyncProximityMap, AsyncSearchControl, AsyncTurboQuantizer, CancellationToken,
        SearchCompletion,
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

    let backing = Arc::new(MemStore::new());
    let source = ProximityMap::build(
        backing.clone(),
        ProximityConfig::new(128),
        records(129, 128),
    )
    .unwrap();
    let (index, _) = TurboQuantizer::build(
        &source,
        TurboQuantizationConfig {
            rerank_multiplier: 256,
            ..TurboQuantizationConfig::default()
        },
        BuildParallelism::serial(),
    )
    .unwrap();
    let descriptor = source.tree().descriptor.clone();
    let manifest = index.manifest_cid().clone();
    let query: Vec<_> = (0..128).map(|index| index as f32 / 29.0 - 1.0).collect();
    let eligible: Vec<Vec<u8>> = (0..13)
        .map(|record| format!("vector-{record:04}").into_bytes())
        .collect();

    for direct_lookup in [false, true] {
        let make_request = || {
            let mut request = forced_turboquant_request(&query, 13);
            if direct_lookup {
                request.filter = ProximityFilter::EligibleKeys(&eligible);
            }
            request
        };
        let baseline_reads = block_on(async {
            let cancellation = CancellationToken::default();
            let store = CancellingReadAsyncStore::new(backing.clone(), cancellation.clone());
            let map = AsyncProximityMap::load(store.clone(), descriptor.clone())
                .await
                .unwrap();
            let index = AsyncTurboQuantizer::load(&store, manifest.clone())
                .await
                .unwrap();
            store.reset_reads();
            let result = index
                .search(
                    &map,
                    make_request(),
                    AsyncSearchControl {
                        cancellation: Some(cancellation),
                        ..AsyncSearchControl::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(
                result.completion,
                SearchCompletion::ApproximatePolicySatisfied
            );
            assert_eq!(result.plan.direct_lookup, direct_lookup);
            store.reads()
        });
        assert!(baseline_reads > 0);

        for cancel_after in 1..=baseline_reads {
            block_on(async {
                let cancellation = CancellationToken::default();
                let store = CancellingReadAsyncStore::new(backing.clone(), cancellation.clone());
                let map = AsyncProximityMap::load(store.clone(), descriptor.clone())
                    .await
                    .unwrap();
                let index = AsyncTurboQuantizer::load(&store, manifest.clone())
                    .await
                    .unwrap();
                store.arm(cancel_after);
                let result = index
                    .search(
                        &map,
                        make_request(),
                        AsyncSearchControl {
                            cancellation: Some(cancellation),
                            ..AsyncSearchControl::default()
                        },
                    )
                    .await
                    .unwrap();
                assert_eq!(
                    result.completion,
                    SearchCompletion::Cancelled,
                    "direct_lookup={direct_lookup}: cancellation after physical read {cancel_after} of \
                     {baseline_reads} was missed"
                );
                assert!(result.neighbors.is_empty());
                assert!(store.reads() <= baseline_reads);
            });
        }

        block_on(async {
            let cancellation = CancellationToken::default();
            cancellation.cancel();
            let store = CancellingReadAsyncStore::new(backing.clone(), cancellation.clone());
            let map = AsyncProximityMap::load(store.clone(), descriptor.clone())
                .await
                .unwrap();
            let index = AsyncTurboQuantizer::load(&store, manifest.clone())
                .await
                .unwrap();
            store.reset_reads();
            let result = index
                .search(
                    &map,
                    make_request(),
                    AsyncSearchControl {
                        cancellation: Some(cancellation),
                        ..AsyncSearchControl::default()
                    },
                )
                .await
                .unwrap();
            assert_eq!(result.completion, SearchCompletion::Cancelled);
            assert!(result.neighbors.is_empty());
            assert_eq!(store.reads(), 0, "pre-cancelled search performed I/O");
        });
    }
}

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_build_streams_canonical_codes_in_bounded_publications() {
    use prolly::{
        AsyncAcceleratorBuildOptions, AsyncAcceleratorCatalog, AsyncProximityMap,
        AsyncTurboQuantizer, AsyncTurboQuantizerBuild, CatalogAcceleratorKind, SyncStoreAsAsync,
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

    let source = records(1_025, 128);
    let config = TurboQuantizationConfig {
        bit_width: 3,
        rerank_multiplier: 16,
        seed: 73,
    };
    let sync_store = Arc::new(MemStore::new());
    let sync_map =
        ProximityMap::build(sync_store, ProximityConfig::new(128), source.clone()).unwrap();
    let (sync_index, sync_stats) =
        TurboQuantizer::build(&sync_map, config.clone(), BuildParallelism::new(3).unwrap())
            .unwrap();

    block_on(async {
        let backing = Arc::new(MemStore::new());
        let initial = AsyncProximityMap::build(
            SyncStoreAsAsync::new(backing.clone()),
            ProximityConfig::new(128),
            source,
        )
        .await
        .unwrap();
        assert_eq!(initial.tree().descriptor, sync_map.tree().descriptor);

        let bounded = PublicationBoundAsyncStore::new(backing);
        let map = AsyncProximityMap::load(bounded.clone(), initial.tree().descriptor.clone())
            .await
            .unwrap();
        let (async_index, async_stats) = AsyncTurboQuantizer::build_with_limits(
            &map,
            config,
            BuildParallelism::new(3).unwrap(),
            TurboQuantizationBuildLimits::default(),
            1,
            &ContentGraphLimits::default(),
        )
        .await
        .unwrap();

        assert_eq!(async_index.manifest_cid(), sync_index.manifest_cid());
        assert_eq!(async_stats, sync_stats);
        assert!(bounded.publications.load(Ordering::SeqCst) > 1);
        assert_eq!(bounded.maximum_batch.load(Ordering::SeqCst), 1);
        assert_eq!(
            async_index
                .verify(&map, &ContentGraphLimits::default())
                .await
                .unwrap(),
            sync_index.verify(&sync_map).unwrap()
        );

        bounded.publications.store(0, Ordering::SeqCst);
        bounded.maximum_batch.store(0, Ordering::SeqCst);
        let (catalog, catalog_stats) = AsyncAcceleratorCatalog::build(
            &map,
            AsyncAcceleratorBuildOptions {
                turboquant: Some(AsyncTurboQuantizerBuild {
                    config: sync_index.config().clone(),
                    parallelism: BuildParallelism::new(3).unwrap(),
                    limits: TurboQuantizationBuildLimits::default(),
                }),
                publication_batch_items: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(catalog.entries().len(), 1);
        assert_eq!(
            catalog.entries()[0].kind,
            CatalogAcceleratorKind::TurboQuantized
        );
        assert_eq!(catalog.entries()[0].manifest, *sync_index.manifest_cid());
        assert_eq!(catalog_stats.turboquant, Some(sync_stats));
        assert!(catalog_stats.objects_published > 1);
        assert!(catalog_stats.bytes_published > 0);
        assert!(bounded.publications.load(Ordering::SeqCst) > 1);
        assert!(bounded.maximum_batch.load(Ordering::SeqCst) <= 2);
    });
}

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_composite_streams_the_structural_delta_canonically() {
    use prolly::{
        walk_content_graph_async, AsyncAcceleratorSet, AsyncCompositeAccelerator,
        AsyncCompositeBuildOptions, AsyncCompositeBuildOutcome, AsyncProximityMap,
        AsyncSearchControl, AsyncTurboQuantizer, CompositeAccelerator, CompositeAcceleratorConfig,
        CompositeBase, CompositeBuildLimits, CompositeBuildOutcome, ProximityMutation, SearchIo,
        SearchRuntime,
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

    let source = records(129, 128);
    let replacement_vector = vec![2.5; 128];
    let inserted_vector = vec![-1.25; 128];
    let mutations = vec![
        ProximityMutation {
            key: b"vector-0001".to_vec(),
            value: None,
        },
        ProximityMutation {
            key: b"vector-0021".to_vec(),
            value: Some((replacement_vector.clone(), b"vector-updated".to_vec())),
        },
        ProximityMutation {
            key: b"vector-0030".to_vec(),
            value: Some((source[30].vector.clone(), b"value-only".to_vec())),
        },
        ProximityMutation {
            key: b"vector-9999".to_vec(),
            value: Some((inserted_vector.clone(), b"inserted".to_vec())),
        },
    ];
    let quantizer_config = TurboQuantizationConfig {
        rerank_multiplier: 256,
        seed: 97,
        ..TurboQuantizationConfig::default()
    };

    let sync_store = Arc::new(MemStore::new());
    let sync_base = ProximityMap::build(
        sync_store.clone(),
        ProximityConfig::new(128),
        source.clone(),
    )
    .unwrap();
    let (sync_current, _) = sync_base.mutate_batch(mutations.clone()).unwrap();
    let (sync_quantizer, _) = TurboQuantizer::build(
        &sync_base,
        quantizer_config.clone(),
        BuildParallelism::new(3).unwrap(),
    )
    .unwrap();
    let (sync_composite, sync_stats) = match CompositeAccelerator::build(
        &sync_base,
        &sync_current,
        CompositeBase::TurboQuantized(sync_quantizer),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => (accelerator, stats),
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("small TurboQuant delta unexpectedly required rebuild: {reasons:?}")
        }
    };
    let sync_manifest = sync_composite.manifest_cid().clone();
    let sync_set = AcceleratorSet::empty()
        .with_composite(sync_current.tree(), *sync_composite)
        .unwrap();
    let mut sync_request = SearchRequest::exact(&inserted_vector, 11);
    sync_request.policy = SearchPolicy::FixedBudget;
    sync_request.options.backend = SearchBackend::Composite;
    let expected = sync_current
        .search_with(
            &sync_set,
            &SearchIo::new(sync_store, Arc::new(SearchRuntime::default())),
            sync_request.clone(),
        )
        .unwrap();

    block_on(async {
        let backing = Arc::new(MemStore::new());
        let store = PublicationBoundAsyncStore::new(backing);
        let async_base = AsyncProximityMap::build(store.clone(), ProximityConfig::new(128), source)
            .await
            .unwrap();
        let (async_quantizer, _) = AsyncTurboQuantizer::build(
            &async_base,
            quantizer_config,
            BuildParallelism::new(3).unwrap(),
        )
        .await
        .unwrap();
        let (async_current, _) = async_base.mutate_batch(mutations).await.unwrap();
        assert_eq!(async_base.tree(), sync_base.tree());
        assert_eq!(async_current.tree(), sync_current.tree());

        let publications_before_validation = store.publications.load(Ordering::SeqCst);
        let invalid_limits = AsyncCompositeAccelerator::build_from_turboquant(
            &async_base,
            &async_current,
            &async_quantizer,
            AsyncCompositeBuildOptions {
                turboquant_limits: TurboQuantizationBuildLimits {
                    max_records: Some(0),
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await;
        match invalid_limits {
            Err(prolly::Error::InvalidProximityConfig { reason }) => {
                assert!(reason.contains("TurboQuant max_records"));
            }
            _ => panic!("invalid co-resident TurboQuant limits must fail before publication"),
        }
        assert_eq!(
            store.publications.load(Ordering::SeqCst),
            publications_before_validation
        );

        store.publications.store(0, Ordering::SeqCst);
        store.maximum_batch.store(0, Ordering::SeqCst);
        let outcome = AsyncCompositeAccelerator::build_from_turboquant(
            &async_base,
            &async_current,
            &async_quantizer,
            AsyncCompositeBuildOptions {
                publication_batch_items: 1,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let AsyncCompositeBuildOutcome::Composite {
            accelerator,
            stats,
            objects_published,
            bytes_published,
        } = outcome
        else {
            panic!("small async TurboQuant delta unexpectedly required rebuild")
        };
        assert_eq!(accelerator.manifest_cid(), &sync_manifest);
        assert_eq!(stats, sync_stats);
        assert_eq!(bytes_published, stats.encoded_output_bytes);
        assert!(objects_published >= 3);
        assert_eq!(store.publications.load(Ordering::SeqCst), objects_published);
        assert!(objects_published < 129);
        assert_eq!(store.maximum_batch.load(Ordering::SeqCst), 1);

        let root = TypedContentRoot::new(
            ContentObjectKind::CompositeAccelerator,
            accelerator.manifest_cid().clone(),
        );
        walk_content_graph_async(&store, &[root], &ContentGraphLimits::default())
            .await
            .unwrap();

        let serving = AsyncProximityMap::load_with_runtime(
            store,
            async_current.tree().descriptor.clone(),
            Arc::new(SearchRuntime::default()),
        )
        .await
        .unwrap();
        let async_set = AsyncAcceleratorSet::empty()
            .with_composite(serving.tree(), *accelerator)
            .unwrap();
        let actual = serving
            .search_with_accelerators(&async_set, sync_request, AsyncSearchControl::default())
            .await
            .unwrap();
        assert_eq!(actual.plan, expected.plan);
        assert_eq!(actual.neighbors, expected.neighbors);
        assert_eq!(actual.completion, expected.completion);
        assert_eq!(actual.stats.nodes_read, expected.stats.nodes_read);
        assert_eq!(actual.stats.committed_bytes, expected.stats.committed_bytes);
        assert_eq!(
            actual.stats.distance_evaluations,
            expected.stats.distance_evaluations
        );
        assert_eq!(
            actual.stats.quantized_distance_evaluations,
            expected.stats.quantized_distance_evaluations
        );
    });
}

#[cfg(feature = "async-store")]
#[test]
fn turboquant_async_composite_preserves_cross_store_complete_closure() {
    use prolly::{
        walk_content_graph_async, AsyncCompositeAccelerator, AsyncCompositeBuildOptions,
        AsyncCompositeBuildOutcome, AsyncProximityMap, AsyncTurboQuantizer, CompositeAccelerator,
        CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits, CompositeBuildOutcome,
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

    let base_records = records(33, 128);
    let mut current_records = base_records.clone();
    current_records[3].vector = vec![3.25; 128];
    current_records.push(ProximityRecord {
        key: b"vector-9999".to_vec(),
        vector: vec![-2.0; 128],
        value: b"cross-store".to_vec(),
    });
    let quantizer_config = TurboQuantizationConfig {
        seed: 131,
        ..TurboQuantizationConfig::default()
    };

    let expected_store = Arc::new(MemStore::new());
    let expected_base = ProximityMap::build(
        expected_store.clone(),
        ProximityConfig::new(128),
        base_records.clone(),
    )
    .unwrap();
    let expected_current = ProximityMap::build(
        expected_store,
        ProximityConfig::new(128),
        current_records.clone(),
    )
    .unwrap();
    let (expected_quantizer, _) = TurboQuantizer::build(
        &expected_base,
        quantizer_config.clone(),
        BuildParallelism::serial(),
    )
    .unwrap();
    let (expected_manifest, expected_stats) = match CompositeAccelerator::build(
        &expected_base,
        &expected_current,
        CompositeBase::TurboQuantized(expected_quantizer),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            (accelerator.manifest_cid().clone(), stats)
        }
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("small cross-store delta unexpectedly required rebuild: {reasons:?}")
        }
    };

    block_on(async {
        let base_store = PublicationBoundAsyncStore::new(Arc::new(MemStore::new()));
        let current_store = PublicationBoundAsyncStore::new(Arc::new(MemStore::new()));
        let base =
            AsyncProximityMap::build(base_store.clone(), ProximityConfig::new(128), base_records)
                .await
                .unwrap();
        let current = AsyncProximityMap::build(
            current_store.clone(),
            ProximityConfig::new(128),
            current_records,
        )
        .await
        .unwrap();
        let (quantizer, _) =
            AsyncTurboQuantizer::build(&base, quantizer_config, BuildParallelism::serial())
                .await
                .unwrap();
        assert_eq!(base.tree(), expected_base.tree());
        assert_eq!(current.tree(), expected_current.tree());

        current_store.publications.store(0, Ordering::SeqCst);
        current_store.maximum_batch.store(0, Ordering::SeqCst);
        let outcome = AsyncCompositeAccelerator::build_from_turboquant(
            &base,
            &current,
            &quantizer,
            AsyncCompositeBuildOptions {
                publication_batch_items: 2,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        let AsyncCompositeBuildOutcome::Composite {
            accelerator,
            stats,
            objects_published,
            bytes_published,
        } = outcome
        else {
            panic!("small cross-store delta unexpectedly required rebuild")
        };
        assert_eq!(accelerator.manifest_cid(), &expected_manifest);
        assert_eq!(stats, expected_stats);
        assert!(objects_published > 0);
        assert!(bytes_published >= stats.encoded_output_bytes);
        assert!(current_store.publications.load(Ordering::SeqCst) > 0);
        assert!(current_store.maximum_batch.load(Ordering::SeqCst) <= 2);

        AsyncTurboQuantizer::load(&current_store, quantizer.manifest_cid().clone())
            .await
            .unwrap();
        let root = TypedContentRoot::new(
            ContentObjectKind::CompositeAccelerator,
            accelerator.manifest_cid().clone(),
        );
        let walk =
            walk_content_graph_async(&current_store, &[root], &ContentGraphLimits::default())
                .await
                .unwrap();
        assert!(walk.objects.len() >= objects_published);
    });
}
