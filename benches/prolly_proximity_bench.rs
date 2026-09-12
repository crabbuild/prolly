use prolly::{
    copy_content_graph, plan_content_gc, AcceleratorSet, AdaptiveQuality, BuildParallelism, Cid,
    CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase, CompositeBuildLimits,
    CompositeBuildOutcome, ContentGraphLimits, ContentObjectKind, DistanceMetric, FileNodeStore,
    HnswConfig, HnswIndex, MemStore, ProductQuantizationConfig, ProductQuantizer, ProximityConfig,
    ProximityFilter, ProximityMap, ProximityMutation, ProximityRecord, QueryKernel,
    ScalarQuantizationConfig, SearchBackend, SearchCompletion, SearchIo, SearchPolicy,
    SearchRequest, SearchRuntime, TurboQuantizationConfig, TurboQuantizer, TypedContentRoot,
};
#[cfg(feature = "async-store")]
use prolly::{
    AsyncAcceleratorSet, AsyncProductQuantizer, AsyncProximityMap, AsyncSearchControl,
    AsyncTurboQuantizer, SyncStoreAsAsync,
};
use std::collections::HashSet;
#[cfg(feature = "async-store")]
use std::future::Future;
use std::hint::black_box;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
#[cfg(feature = "async-store")]
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

fn main() {
    let records = env_usize("PROLLY_PROXIMITY_BENCH_RECORDS").unwrap_or(1_000);
    assert!(records > 0, "benchmark record count must be positive");
    let dimensions =
        env_list("PROLLY_PROXIMITY_BENCH_DIMENSIONS").unwrap_or_else(|| vec![8, 128, 768, 1_536]);
    assert!(
        dimensions.iter().all(|dimension| *dimension > 0),
        "benchmark dimensions must be positive"
    );
    let threads = env_list("PROLLY_PROXIMITY_BENCH_THREADS").unwrap_or_else(|| vec![1, 2, 4]);
    let scale_only = env_bool("PROLLY_PROXIMITY_BENCH_SCALE_ONLY");
    let quantizers_only = env_bool("PROLLY_PROXIMITY_BENCH_QUANTIZERS_ONLY");
    let store_kind = BenchStoreKind::from_env();
    let durable_root = match store_kind {
        BenchStoreKind::Memory => {
            assert!(
                std::env::var_os("PROLLY_PROXIMITY_BENCH_STORE_PATH").is_none(),
                "store path is only valid for the file benchmark store"
            );
            None
        }
        BenchStoreKind::File => Some(DurableRunRoot::create(
            std::env::var_os("PROLLY_PROXIMITY_BENCH_STORE_PATH")
                .map(PathBuf::from)
                .expect("file benchmark store requires PROLLY_PROXIMITY_BENCH_STORE_PATH"),
        )),
    };
    assert!(
        !(scale_only && quantizers_only),
        "scale-only and quantizers-only benchmark profiles are mutually exclusive"
    );
    assert_unique_positive_workers(&threads);
    let search_repeats = env_usize("PROLLY_PROXIMITY_BENCH_SEARCH_REPEATS").unwrap_or(1);
    assert!(
        search_repeats > 0,
        "benchmark search repetitions must be positive"
    );
    let reset_search_cache = env_bool("PROLLY_PROXIMITY_BENCH_RESET_SEARCH_CACHE");
    let async_quantizers = env_bool("PROLLY_PROXIMITY_BENCH_ASYNC_QUANTIZERS");
    #[cfg(not(feature = "async-store"))]
    assert!(
        !async_quantizers,
        "async quantizer benchmarks require the async-store feature"
    );
    let simd_first = env_bool("PROLLY_PROXIMITY_BENCH_SIMD_FIRST");
    let metric = env_metric("PROLLY_PROXIMITY_BENCH_METRIC").unwrap_or(DistanceMetric::L2Squared);
    let k = env_usize("PROLLY_PROXIMITY_BENCH_K").unwrap_or(10);
    assert!(k > 0, "benchmark k must be positive");
    let eligibility_ppm = env_usize("PROLLY_PROXIMITY_BENCH_ELIGIBILITY_PPM").unwrap_or(1_000_000);
    assert!(
        (1..=1_000_000).contains(&eligibility_ppm),
        "benchmark eligibility PPM must be between 1 and 1000000"
    );
    let turboquant_config = TurboQuantizationConfig {
        bit_width: env_usize("PROLLY_PROXIMITY_BENCH_TURBOQUANT_BITS")
            .map(|value| {
                u8::try_from(value).expect("TurboQuant benchmark bit width must fit in u8")
            })
            .unwrap_or(4),
        rerank_multiplier: env_usize("PROLLY_PROXIMITY_BENCH_RERANK_MULTIPLIER")
            .map(|value| {
                u32::try_from(value)
                    .expect("TurboQuant benchmark rerank multiplier must fit in u32")
            })
            .unwrap_or(8),
        seed: 0,
    };
    if quantizers_only {
        assert!(
            records >= 16,
            "quantizers-only benchmark requires at least 16 records"
        );
        assert!(
            dimensions
                .iter()
                .all(|dimension| *dimension >= 8 && dimension.is_multiple_of(8)),
            "quantizers-only benchmark requires supported TurboQuant dimensions"
        );
    }
    println!("prolly proximity benchmark");
    println!("schema_version=2");
    println!("revision={}", command_output("git", &["rev-parse", "HEAD"]));
    println!(
        "compiler={}",
        command_output("rustc", &["--version", "--verbose"])
    );
    println!("target_arch={}", std::env::consts::ARCH);
    println!("target_os={}", std::env::consts::OS);
    println!("machine={}", command_output("hostname", &[]));
    println!("store={}", store_kind.label());
    if let Some(root) = &durable_root {
        println!("store_path={}", root.path().display());
    }
    println!("seed={}", turboquant_config.seed);
    println!("records={records}");
    println!(
        "profile={}",
        if quantizers_only {
            "quantizers"
        } else if scale_only {
            "scale"
        } else {
            "complete"
        }
    );
    println!("search_repeats={search_repeats}");
    println!("metric={metric:?}");
    println!("k={k}");
    println!("eligibility_ppm={eligibility_ppm}");
    println!("turboquant_bits={}", turboquant_config.bit_width);
    println!("rerank_multiplier={}", turboquant_config.rerank_multiplier);
    println!(
        "search_cache={}",
        if reset_search_cache { "reset" } else { "warm" }
    );
    println!(
        "search_order={}",
        if simd_first {
            "simd-first"
        } else {
            "scalar-first"
        }
    );
    println!("async_quantizers={async_quantizers}");
    println!("operation,dimensions,threads,micros,metric_a,metric_b");
    let settings = BenchSettings {
        threads: &threads,
        scale_only,
        quantizers_only,
        search_repeats,
        reset_search_cache,
        async_quantizers,
        simd_first,
        metric,
        requested_k: k,
        eligibility_ppm,
        turboquant_config: &turboquant_config,
    };
    match durable_root {
        None => {
            let mut make_store = || Arc::new(MemStore::new());
            for dimension in dimensions {
                bench_case(records, dimension, &settings, &mut make_store);
            }
        }
        Some(root) => {
            let mut store_id = 0usize;
            let mut make_store = || {
                store_id += 1;
                Arc::new(
                    FileNodeStore::open(root.path().join(format!("store-{store_id:06}"))).unwrap(),
                )
            };
            for dimension in dimensions {
                bench_case(records, dimension, &settings, &mut make_store);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum BenchStoreKind {
    Memory,
    File,
}

impl BenchStoreKind {
    fn from_env() -> Self {
        match std::env::var("PROLLY_PROXIMITY_BENCH_STORE")
            .unwrap_or_else(|_| "memory".to_owned())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "memory" => Self::Memory,
            "file" => Self::File,
            _ => panic!("PROLLY_PROXIMITY_BENCH_STORE must be memory or file"),
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::Memory => "memory",
            Self::File => "file",
        }
    }
}

struct DurableRunRoot {
    path: PathBuf,
}

impl DurableRunRoot {
    fn create(parent: PathBuf) -> Self {
        assert!(
            parent.is_dir(),
            "file benchmark store path must be an existing directory"
        );
        let parent = parent
            .canonicalize()
            .expect("file benchmark store path must be canonicalizable");
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock must be after the Unix epoch")
            .as_nanos();
        let path = parent.join(format!("prolly-turboquant-{}-{nonce}", std::process::id()));
        std::fs::create_dir(&path).expect("create unique file benchmark run directory");
        Self { path }
    }

    fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for DurableRunRoot {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.path) {
            eprintln!(
                "warning: failed to remove benchmark run directory {}: {error}",
                self.path.display()
            );
        }
    }
}

#[derive(Clone, Copy)]
struct BenchSettings<'a> {
    threads: &'a [usize],
    scale_only: bool,
    quantizers_only: bool,
    search_repeats: usize,
    reset_search_cache: bool,
    async_quantizers: bool,
    simd_first: bool,
    metric: DistanceMetric,
    requested_k: usize,
    eligibility_ppm: usize,
    turboquant_config: &'a TurboQuantizationConfig,
}

fn bench_case<S, F>(
    count: usize,
    dimensions: usize,
    settings: &BenchSettings<'_>,
    make_store: &mut F,
) where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
    F: FnMut() -> S,
{
    let BenchSettings {
        threads,
        scale_only,
        quantizers_only,
        search_repeats,
        reset_search_cache,
        async_quantizers,
        simd_first,
        metric,
        requested_k,
        eligibility_ppm,
        turboquant_config,
    } = *settings;
    let records = make_records(count, dimensions);
    if !quantizers_only {
        for &workers in threads {
            let store = make_store();
            let config = config(dimensions, metric);
            let started = Instant::now();
            let (_, stats) = ProximityMap::build_with_parallelism(
                store,
                config,
                black_box(records.clone()),
                BuildParallelism::new(workers).unwrap(),
            )
            .unwrap();
            row(
                "build",
                dimensions,
                workers,
                started.elapsed(),
                stats.distance_evaluations,
                stats.proximity_objects_written,
            );
        }
    }

    let store = make_store();
    let started = Instant::now();
    let map =
        ProximityMap::build(store.clone(), config(dimensions, metric), records.clone()).unwrap();
    if quantizers_only {
        row("source_build", dimensions, 0, started.elapsed(), count, 0);
    }
    let source_walk = prolly::walk_content_graph(
        &store,
        &[TypedContentRoot::proximity_descriptor(
            map.tree().descriptor.clone(),
        )],
        &ContentGraphLimits::default(),
    )
    .unwrap();
    row(
        "source_closure_bytes",
        dimensions,
        0,
        Duration::ZERO,
        source_walk.total_bytes,
        source_walk.objects.len(),
    );
    let query = make_vector(count / 3, dimensions);
    let eligible_count = count
        .saturating_mul(eligibility_ppm)
        .div_ceil(1_000_000)
        .max(1)
        .min(count.max(1));
    let eligible_keys: Vec<_> = records
        .iter()
        .take(eligible_count)
        .map(|record| record.key.clone())
        .collect();
    let k = requested_k.min(eligible_count);

    if quantizers_only {
        bench_accelerators(
            &map,
            store,
            AcceleratorBenchCase {
                records: &records,
                query: &query,
                k,
                dimensions,
                workers: threads,
                turboquant_config,
            },
            SearchBenchOptions {
                repeats: search_repeats,
                reset_cache: reset_search_cache,
                async_quantizers,
                eligible_keys: &eligible_keys,
                eligibility_ppm,
                eligible_count,
            },
            false,
        );
        return;
    }

    let search_specs = if simd_first {
        [
            (
                "search_exact_simd",
                SearchPolicy::Exact,
                QueryKernel::SimdDeterministic,
            ),
            (
                "search_exact_scalar",
                SearchPolicy::Exact,
                QueryKernel::ScalarDeterministic,
            ),
            (
                "search_adaptive_sq8",
                SearchPolicy::Adaptive(AdaptiveQuality::Balanced),
                QueryKernel::AutoDeterministic,
            ),
        ]
    } else {
        [
            (
                "search_exact_scalar",
                SearchPolicy::Exact,
                QueryKernel::ScalarDeterministic,
            ),
            (
                "search_exact_simd",
                SearchPolicy::Exact,
                QueryKernel::SimdDeterministic,
            ),
            (
                "search_adaptive_sq8",
                SearchPolicy::Adaptive(AdaptiveQuality::Balanced),
                QueryKernel::AutoDeterministic,
            ),
        ]
    };
    for (name, policy, kernel) in search_specs {
        let mut request = SearchRequest::exact(&query, k);
        request.policy = policy;
        request.kernel = kernel;
        request.filter = benchmark_filter(&eligible_keys, eligibility_ppm);
        let started = Instant::now();
        let mut result = None;
        for _ in 0..search_repeats {
            if reset_search_cache {
                map.clear_content_cache().unwrap();
            }
            result = Some(map.search(request.clone()).unwrap());
        }
        let result = result.expect("search repeats is positive");
        row(
            name,
            dimensions,
            0,
            started.elapsed().div_f64(search_repeats as f64),
            result.stats.nodes_read,
            result.stats.distance_evaluations + result.stats.quantized_distance_evaluations,
        );
        if name == "search_exact_scalar" && !scale_only {
            let recall = recall_at_k(&records[..eligible_count], &query, &result, k, metric);
            println!("recall_exact,{dimensions},0,0,{:.6},0", recall);
        }
    }

    let key = format!("record-{:08}", count / 2).into_bytes();
    let started = Instant::now();
    let (_, stats) = map
        .mutate_batch([ProximityMutation {
            key,
            value: Some((make_vector(count + 1, dimensions), b"updated".to_vec())),
        }])
        .unwrap();
    row(
        "localized_mutation",
        dimensions,
        0,
        started.elapsed(),
        stats.nodes_written,
        stats.nodes_reused,
    );

    if scale_only {
        return;
    }

    let limits = ContentGraphLimits::default();
    let root = TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone());
    let replica = make_store();
    let started = Instant::now();
    let copied = copy_content_graph(&store, &replica, root.clone(), &limits).unwrap();
    row(
        "content_graph_copy",
        dimensions,
        0,
        started.elapsed(),
        copied.copied_objects,
        copied.copied_bytes,
    );
    let walk = prolly::walk_content_graph(&store, std::slice::from_ref(&root), &limits).unwrap();
    let candidates: Vec<_> = walk
        .objects
        .iter()
        .map(|object| object.root.cid.clone())
        .collect();
    let started = Instant::now();
    let gc = plan_content_gc(&store, &[root], &candidates, &limits).unwrap();
    row(
        "content_graph_gc_plan",
        dimensions,
        0,
        started.elapsed(),
        gc.live_objects,
        gc.reclaimable_cids.len(),
    );

    let started = Instant::now();
    let proof = map
        .prove_search(SearchRequest::exact(&query, k), &limits)
        .unwrap();
    let generated = started.elapsed();
    let started = Instant::now();
    let verified = proof.verify(&limits).unwrap();
    row(
        "search_proof_generate",
        dimensions,
        0,
        generated,
        proof.events.len(),
        proof.source.objects.len(),
    );
    row(
        "search_proof_verify",
        dimensions,
        0,
        started.elapsed(),
        verified.replayed_events,
        verified.result.neighbors.len(),
    );

    if count >= 16 {
        bench_accelerators(
            &map,
            store.clone(),
            AcceleratorBenchCase {
                records: &records,
                query: &query,
                k,
                dimensions,
                workers: threads,
                turboquant_config,
            },
            SearchBenchOptions {
                repeats: search_repeats,
                reset_cache: reset_search_cache,
                async_quantizers,
                eligible_keys: &eligible_keys,
                eligibility_ppm,
                eligible_count,
            },
            true,
        );
    }
    #[cfg(feature = "async-store")]
    bench_async(&map, store, &query, k, dimensions);
}

#[derive(Clone, Copy)]
struct AcceleratorBenchCase<'a> {
    records: &'a [ProximityRecord],
    query: &'a [f32],
    k: usize,
    dimensions: usize,
    workers: &'a [usize],
    turboquant_config: &'a TurboQuantizationConfig,
}

#[derive(Clone, Copy)]
struct SearchBenchOptions<'a> {
    repeats: usize,
    reset_cache: bool,
    async_quantizers: bool,
    eligible_keys: &'a [Vec<u8>],
    eligibility_ppm: usize,
    eligible_count: usize,
}

fn bench_accelerators<S>(
    map: &ProximityMap<S>,
    store: S,
    case: AcceleratorBenchCase<'_>,
    options: SearchBenchOptions<'_>,
    include_non_quantized: bool,
) where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let AcceleratorBenchCase {
        records,
        query,
        k,
        dimensions,
        workers,
        turboquant_config,
    } = case;
    if dimensions >= 8 && dimensions.is_multiple_of(8) {
        let mut selected = None;
        let mut canonical = None;
        for &worker_count in workers {
            let started = Instant::now();
            let (candidate, stats) = TurboQuantizer::build(
                map,
                turboquant_config.clone(),
                BuildParallelism::new(worker_count).unwrap(),
            )
            .unwrap();
            row(
                "turboquant_build",
                dimensions,
                worker_count,
                started.elapsed(),
                stats.transformed_components,
                stats.encoded_output_bytes,
            );
            row(
                "turboquant_build_resources",
                dimensions,
                worker_count,
                Duration::ZERO,
                stats.peak_temporary_bytes,
                stats.butterfly_operations,
            );
            if let Some((manifest, canonical_stats)) = &canonical {
                assert_eq!(
                    candidate.manifest_cid(),
                    manifest,
                    "TurboQuant manifest changed with worker count"
                );
                assert_eq!(
                    &stats, canonical_stats,
                    "TurboQuant logical build statistics changed with worker count"
                );
            } else {
                canonical = Some((candidate.manifest_cid().clone(), stats.clone()));
                selected = Some(candidate);
            }
        }
        let (turboquant, stats) = selected
            .zip(canonical.map(|(_, stats)| stats))
            .expect("validated worker list is non-empty");
        let turboquant_manifest = turboquant.manifest_cid().clone();
        let sidecar = derived_closure_size(
            &store,
            map.tree().descriptor.clone(),
            TypedContentRoot::new(
                ContentObjectKind::TurboQuantization,
                turboquant.manifest_cid().clone(),
            ),
        );
        row(
            "turboquant_sidecar_bytes",
            dimensions,
            0,
            Duration::ZERO,
            sidecar.total_bytes,
            stats.encoded_output_bytes,
        );
        row(
            "turboquant_manifest_code_bytes",
            dimensions,
            0,
            Duration::ZERO,
            sidecar.manifest_bytes,
            sidecar.code_tree_bytes,
        );
        let quality = turboquant.quality();
        let accelerators = AcceleratorSet::empty()
            .with_turboquant(map.tree(), turboquant)
            .unwrap();
        for (name, kernel) in [
            ("turboquant_search_scalar", QueryKernel::ScalarDeterministic),
            ("turboquant_search_simd", QueryKernel::SimdDeterministic),
            ("turboquant_search_auto", QueryKernel::AutoDeterministic),
        ] {
            let mut request = SearchRequest::exact(query, k);
            request.policy = SearchPolicy::FixedBudget;
            request.kernel = kernel;
            request.options.backend = SearchBackend::TurboQuantized;
            request.filter = benchmark_filter(options.eligible_keys, options.eligibility_ppm);
            let mut result = None;
            let mut samples = Vec::with_capacity(options.repeats);
            let warm_io = SearchIo::new(store.clone(), Arc::new(SearchRuntime::default()));
            if !options.reset_cache {
                map.search_with(&accelerators, &warm_io, request.clone())
                    .unwrap();
            }
            for _ in 0..options.repeats {
                let cold_io = if options.reset_cache {
                    map.clear_content_cache().unwrap();
                    Some(SearchIo::new(
                        store.clone(),
                        Arc::new(SearchRuntime::default()),
                    ))
                } else {
                    None
                };
                let started = Instant::now();
                result = Some(
                    map.search_with(
                        &accelerators,
                        cold_io.as_ref().unwrap_or(&warm_io),
                        request.clone(),
                    )
                    .unwrap(),
                );
                samples.push(started.elapsed());
            }
            let result = result.expect("search repeats is positive");
            let latency = LatencySummary::from_samples(samples);
            row(
                name,
                dimensions,
                0,
                latency.median,
                result.stats.quantized_distance_evaluations,
                result.stats.distance_evaluations,
            );
            row(
                &format!("{name}_p95"),
                dimensions,
                0,
                latency.p95,
                result.stats.bytes_read,
                result.stats.physical_bytes_read,
            );
            row(
                &format!("{name}_p99"),
                dimensions,
                0,
                latency.p99,
                result.stats.candidate_handles_peak,
                result.stats.candidate_retained_bytes_peak,
            );
            row(
                &format!("{name}_work"),
                dimensions,
                0,
                Duration::ZERO,
                result.stats.frontier_peak,
                completion_id(result.completion),
            );
            if kernel == QueryKernel::ScalarDeterministic {
                println!(
                    "turboquant_recall,{dimensions},0,0,{:.6},{}",
                    recall_at_k(
                        &records[..options.eligible_count],
                        query,
                        &result,
                        k,
                        map.tree().config.metric,
                    ),
                    quality.mean_squared_error,
                );
            }
        }
        #[cfg(feature = "async-store")]
        if options.async_quantizers {
            bench_async_turboquant(
                map,
                store.clone(),
                turboquant_manifest,
                AsyncQuantizerBenchCase {
                    records,
                    query,
                    k,
                    dimensions,
                    options,
                },
            );
        }
    }

    let pq_config = ProductQuantizationConfig {
        subquantizers: (dimensions as u32).min(8),
        centroids_per_subquantizer: 16,
        training_iterations: 4,
        rerank_multiplier: turboquant_config.rerank_multiplier,
        seed: 17,
        max_training_vectors: 65_536,
    };
    let mut selected = None;
    let mut canonical = None;
    for &worker_count in workers {
        let started = Instant::now();
        let (candidate, stats) = ProductQuantizer::build(
            map,
            pq_config.clone(),
            BuildParallelism::new(worker_count).unwrap(),
        )
        .unwrap();
        row(
            "pq_build",
            dimensions,
            worker_count,
            started.elapsed(),
            stats.training_distance_evaluations,
            stats.encoded_vectors,
        );
        if let Some((manifest, canonical_stats)) = &canonical {
            assert_eq!(
                candidate.manifest_cid(),
                manifest,
                "PQ manifest changed with worker count"
            );
            assert_eq!(
                &stats, canonical_stats,
                "PQ logical build statistics changed with worker count"
            );
        } else {
            canonical = Some((candidate.manifest_cid().clone(), stats.clone()));
            selected = Some(candidate);
        }
    }
    let (pq, stats) = selected
        .zip(canonical.map(|(_, stats)| stats))
        .expect("validated worker list is non-empty");
    let pq_manifest = pq.manifest_cid().clone();
    let sidecar = derived_closure_size(
        &store,
        map.tree().descriptor.clone(),
        TypedContentRoot::new(
            ContentObjectKind::ProductQuantization,
            pq.manifest_cid().clone(),
        ),
    );
    row(
        "pq_sidecar_bytes",
        dimensions,
        0,
        Duration::ZERO,
        sidecar.total_bytes,
        stats.encoded_vectors,
    );
    row(
        "pq_manifest_code_bytes",
        dimensions,
        0,
        Duration::ZERO,
        sidecar.manifest_bytes,
        sidecar.code_tree_bytes,
    );
    let accelerators = AcceleratorSet::empty().with_pq(map.tree(), pq).unwrap();
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::ProductQuantized;
    request.filter = benchmark_filter(options.eligible_keys, options.eligibility_ppm);
    let mut result = None;
    let mut samples = Vec::with_capacity(options.repeats);
    let warm_io = SearchIo::new(store.clone(), Arc::new(SearchRuntime::default()));
    if !options.reset_cache {
        map.search_with(&accelerators, &warm_io, request.clone())
            .unwrap();
    }
    for _ in 0..options.repeats {
        let cold_io = if options.reset_cache {
            map.clear_content_cache().unwrap();
            Some(SearchIo::new(
                store.clone(),
                Arc::new(SearchRuntime::default()),
            ))
        } else {
            None
        };
        let started = Instant::now();
        result = Some(
            map.search_with(
                &accelerators,
                cold_io.as_ref().unwrap_or(&warm_io),
                request.clone(),
            )
            .unwrap(),
        );
        samples.push(started.elapsed());
    }
    let result = result.expect("search repeats is positive");
    let latency = LatencySummary::from_samples(samples);
    row(
        "pq_search",
        dimensions,
        0,
        latency.median,
        result.stats.quantized_distance_evaluations,
        result.stats.distance_evaluations,
    );
    row(
        "pq_search_p95",
        dimensions,
        0,
        latency.p95,
        result.stats.bytes_read,
        result.stats.physical_bytes_read,
    );
    row(
        "pq_search_p99",
        dimensions,
        0,
        latency.p99,
        result.stats.candidate_handles_peak,
        result.stats.candidate_retained_bytes_peak,
    );
    row(
        "pq_search_work",
        dimensions,
        0,
        Duration::ZERO,
        result.stats.frontier_peak,
        completion_id(result.completion),
    );
    println!(
        "pq_recall,{dimensions},0,0,{:.6},0",
        recall_at_k(
            &records[..options.eligible_count],
            query,
            &result,
            k,
            map.tree().config.metric,
        ),
    );
    #[cfg(feature = "async-store")]
    if options.async_quantizers {
        bench_async_pq(
            map,
            store.clone(),
            pq_manifest,
            AsyncQuantizerBenchCase {
                records,
                query,
                k,
                dimensions,
                options,
            },
        );
    }

    if !include_non_quantized {
        return;
    }

    let started = Instant::now();
    let (hnsw, stats) = HnswIndex::build(map, HnswConfig::default()).unwrap();
    row(
        "hnsw_build",
        dimensions,
        0,
        started.elapsed(),
        stats.distance_evaluations,
        stats.directed_edges,
    );
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Hnsw;
    let started = Instant::now();
    let result = hnsw.search(map, request).unwrap();
    row(
        "hnsw_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.nodes_read,
        result.stats.distance_evaluations,
    );

    let changed_key = format!("record-{:08}", map.tree().count / 2).into_bytes();
    let (current, _) = map
        .mutate_batch([ProximityMutation {
            key: changed_key,
            value: Some((query.to_vec(), b"composite-update".to_vec())),
        }])
        .unwrap();
    let started = Instant::now();
    let (composite, build_stats) = match CompositeAccelerator::build(
        map,
        &current,
        CompositeBase::Hnsw(hnsw),
        CompositeAcceleratorConfig::default(),
        CompositeBuildLimits::default(),
    )
    .unwrap()
    {
        CompositeBuildOutcome::Composite { accelerator, stats } => (accelerator, stats),
        CompositeBuildOutcome::FullRebuildRequired { reasons, .. } => {
            panic!("benchmark delta unexpectedly requires rebuild: {reasons:?}")
        }
    };
    row(
        "composite_build",
        dimensions,
        0,
        started.elapsed(),
        build_stats.diff_entries,
        build_stats.encoded_output_bytes,
    );
    let accelerators = AcceleratorSet::empty()
        .with_composite(current.tree(), *composite)
        .unwrap();
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::Composite;
    let started = Instant::now();
    let result = current
        .search_with(
            &accelerators,
            &SearchIo::new(store, Arc::new(SearchRuntime::default())),
            request,
        )
        .unwrap();
    row(
        "composite_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.nodes_read,
        result.stats.distance_evaluations + result.stats.quantized_distance_evaluations,
    );
}

#[cfg(feature = "async-store")]
#[derive(Clone, Copy)]
struct AsyncQuantizerBenchCase<'a> {
    records: &'a [ProximityRecord],
    query: &'a [f32],
    k: usize,
    dimensions: usize,
    options: SearchBenchOptions<'a>,
}

#[cfg(feature = "async-store")]
fn bench_async_turboquant<S>(
    map: &ProximityMap<S>,
    store: S,
    manifest: Cid,
    case: AsyncQuantizerBenchCase<'_>,
) where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let AsyncQuantizerBenchCase {
        records,
        query,
        k,
        dimensions,
        options,
    } = case;
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.kernel = QueryKernel::ScalarDeterministic;
    request.options.backend = SearchBackend::TurboQuantized;
    request.filter = benchmark_filter(options.eligible_keys, options.eligibility_ppm);
    let descriptor = map.tree().descriptor.clone();

    let (result, latency) = if options.reset_cache {
        let mut result = None;
        let mut samples = Vec::with_capacity(options.repeats);
        for _ in 0..options.repeats {
            map.clear_content_cache().unwrap();
            let io = SearchIo::new(
                SyncStoreAsAsync::new(store.clone()),
                Arc::new(SearchRuntime::default()),
            );
            let before = io.physical_bytes_read();
            let started = Instant::now();
            let (async_map, turboquant) = block_on(async {
                let async_map =
                    AsyncProximityMap::load_with_search_io(io.clone(), descriptor.clone()).await?;
                let turboquant = AsyncTurboQuantizer::load(&io, manifest.clone()).await?;
                Ok::<_, prolly::Error>((async_map, turboquant))
            })
            .unwrap();
            let accelerators = AsyncAcceleratorSet::empty()
                .with_turboquant(async_map.tree(), turboquant)
                .unwrap();
            let mut sample = block_on(async_map.search_with_accelerators(
                &accelerators,
                request.clone(),
                AsyncSearchControl::default(),
            ))
            .unwrap();
            sample.stats.physical_bytes_read = io.physical_bytes_read().saturating_sub(before);
            samples.push(started.elapsed());
            result = Some(sample);
        }
        (
            result.expect("search repeats is positive"),
            LatencySummary::from_samples(samples),
        )
    } else {
        let io = SearchIo::new(
            SyncStoreAsAsync::new(store),
            Arc::new(SearchRuntime::default()),
        );
        let (async_map, turboquant) = block_on(async {
            let async_map =
                AsyncProximityMap::load_with_search_io(io.clone(), descriptor.clone()).await?;
            let turboquant = AsyncTurboQuantizer::load(&io, manifest).await?;
            Ok::<_, prolly::Error>((async_map, turboquant))
        })
        .unwrap();
        let accelerators = AsyncAcceleratorSet::empty()
            .with_turboquant(async_map.tree(), turboquant)
            .unwrap();
        block_on(async_map.search_with_accelerators(
            &accelerators,
            request.clone(),
            AsyncSearchControl::default(),
        ))
        .unwrap();
        let mut result = None;
        let mut samples = Vec::with_capacity(options.repeats);
        for _ in 0..options.repeats {
            let started = Instant::now();
            result = Some(
                block_on(async_map.search_with_accelerators(
                    &accelerators,
                    request.clone(),
                    AsyncSearchControl::default(),
                ))
                .unwrap(),
            );
            samples.push(started.elapsed());
        }
        (
            result.expect("search repeats is positive"),
            LatencySummary::from_samples(samples),
        )
    };

    emit_quantized_search_rows("turboquant_search_async", dimensions, &result, latency);
    println!(
        "turboquant_recall_async,{dimensions},0,0,{:.6},0",
        recall_at_k(
            &records[..options.eligible_count],
            query,
            &result,
            k,
            map.tree().config.metric,
        ),
    );
}

#[cfg(feature = "async-store")]
fn bench_async_pq<S>(
    map: &ProximityMap<S>,
    store: S,
    manifest: Cid,
    case: AsyncQuantizerBenchCase<'_>,
) where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let AsyncQuantizerBenchCase {
        records,
        query,
        k,
        dimensions,
        options,
    } = case;
    let mut request = SearchRequest::exact(query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::ProductQuantized;
    request.filter = benchmark_filter(options.eligible_keys, options.eligibility_ppm);
    let descriptor = map.tree().descriptor.clone();

    let (result, latency) = if options.reset_cache {
        let mut result = None;
        let mut samples = Vec::with_capacity(options.repeats);
        for _ in 0..options.repeats {
            map.clear_content_cache().unwrap();
            let io = SearchIo::new(
                SyncStoreAsAsync::new(store.clone()),
                Arc::new(SearchRuntime::default()),
            );
            let before = io.physical_bytes_read();
            let started = Instant::now();
            let (async_map, pq) = block_on(async {
                let async_map =
                    AsyncProximityMap::load_with_search_io(io.clone(), descriptor.clone()).await?;
                let pq = AsyncProductQuantizer::load(&io, manifest.clone()).await?;
                Ok::<_, prolly::Error>((async_map, pq))
            })
            .unwrap();
            let accelerators = AsyncAcceleratorSet::empty()
                .with_pq(async_map.tree(), pq)
                .unwrap();
            let mut sample = block_on(async_map.search_with_accelerators(
                &accelerators,
                request.clone(),
                AsyncSearchControl::default(),
            ))
            .unwrap();
            sample.stats.physical_bytes_read = io.physical_bytes_read().saturating_sub(before);
            samples.push(started.elapsed());
            result = Some(sample);
        }
        (
            result.expect("search repeats is positive"),
            LatencySummary::from_samples(samples),
        )
    } else {
        let io = SearchIo::new(
            SyncStoreAsAsync::new(store),
            Arc::new(SearchRuntime::default()),
        );
        let (async_map, pq) = block_on(async {
            let async_map =
                AsyncProximityMap::load_with_search_io(io.clone(), descriptor.clone()).await?;
            let pq = AsyncProductQuantizer::load(&io, manifest).await?;
            Ok::<_, prolly::Error>((async_map, pq))
        })
        .unwrap();
        let accelerators = AsyncAcceleratorSet::empty()
            .with_pq(async_map.tree(), pq)
            .unwrap();
        block_on(async_map.search_with_accelerators(
            &accelerators,
            request.clone(),
            AsyncSearchControl::default(),
        ))
        .unwrap();
        let mut result = None;
        let mut samples = Vec::with_capacity(options.repeats);
        for _ in 0..options.repeats {
            let started = Instant::now();
            result = Some(
                block_on(async_map.search_with_accelerators(
                    &accelerators,
                    request.clone(),
                    AsyncSearchControl::default(),
                ))
                .unwrap(),
            );
            samples.push(started.elapsed());
        }
        (
            result.expect("search repeats is positive"),
            LatencySummary::from_samples(samples),
        )
    };

    emit_quantized_search_rows("pq_search_async", dimensions, &result, latency);
    println!(
        "pq_recall_async,{dimensions},0,0,{:.6},0",
        recall_at_k(
            &records[..options.eligible_count],
            query,
            &result,
            k,
            map.tree().config.metric,
        ),
    );
}

#[cfg(feature = "async-store")]
fn emit_quantized_search_rows(
    name: &str,
    dimensions: usize,
    result: &prolly::SearchResult,
    latency: LatencySummary,
) {
    row(
        name,
        dimensions,
        0,
        latency.median,
        result.stats.quantized_distance_evaluations,
        result.stats.distance_evaluations,
    );
    row(
        &format!("{name}_p95"),
        dimensions,
        0,
        latency.p95,
        result.stats.bytes_read,
        result.stats.physical_bytes_read,
    );
    row(
        &format!("{name}_p99"),
        dimensions,
        0,
        latency.p99,
        result.stats.candidate_handles_peak,
        result.stats.candidate_retained_bytes_peak,
    );
    row(
        &format!("{name}_work"),
        dimensions,
        0,
        Duration::ZERO,
        result.stats.frontier_peak,
        completion_id(result.completion),
    );
}

#[cfg(feature = "async-store")]
fn bench_async<S>(map: &ProximityMap<S>, store: S, query: &[f32], k: usize, dimensions: usize)
where
    S: prolly::Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    let async_map = block_on(AsyncProximityMap::load(
        SyncStoreAsAsync::new(store),
        map.tree().descriptor.clone(),
    ))
    .unwrap();
    let started = Instant::now();
    let result = block_on(async_map.search(
        SearchRequest::exact(query, k),
        AsyncSearchControl::default(),
    ))
    .unwrap();
    row(
        "async_search",
        dimensions,
        0,
        started.elapsed(),
        result.stats.nodes_read,
        result.stats.distance_evaluations,
    );
}

#[cfg(feature = "async-store")]
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

fn config(dimensions: usize, metric: DistanceMetric) -> ProximityConfig {
    let mut config = ProximityConfig::new(dimensions as u32);
    config.metric = metric;
    config.hierarchy.level_hash_seed = 42;
    config.overflow.min_page_bytes = 4 * 1024;
    config.overflow.target_page_bytes = 16 * 1024;
    config.overflow.max_page_bytes = 64 * 1024;
    config.vector_storage.inline_threshold_bytes = 4 * 1024;
    config.scalar_quantization = Some(ScalarQuantizationConfig {
        group_size: (dimensions as u32).min(32),
    });
    config
}

#[derive(Clone, Copy)]
struct LatencySummary {
    median: Duration,
    p95: Duration,
    p99: Duration,
}

impl LatencySummary {
    fn from_samples(mut samples: Vec<Duration>) -> Self {
        assert!(!samples.is_empty(), "latency samples must not be empty");
        samples.sort_unstable();
        Self {
            median: nearest_rank(&samples, 50),
            p95: nearest_rank(&samples, 95),
            p99: nearest_rank(&samples, 99),
        }
    }
}

struct DerivedClosureSize {
    total_bytes: usize,
    manifest_bytes: usize,
    code_tree_bytes: usize,
}

fn derived_closure_size<S>(
    store: &S,
    source_descriptor: Cid,
    root: TypedContentRoot,
) -> DerivedClosureSize
where
    S: prolly::Store,
{
    let limits = ContentGraphLimits::default();
    let source = prolly::walk_content_graph(
        store,
        &[TypedContentRoot::proximity_descriptor(source_descriptor)],
        &limits,
    )
    .unwrap();
    let source_cids: HashSet<_> = source
        .objects
        .into_iter()
        .map(|object| object.root.cid)
        .collect();
    let manifest_cid = root.cid.clone();
    let closure = prolly::walk_content_graph(store, &[root], &limits).unwrap();
    let mut total_bytes = 0usize;
    let mut manifest_bytes = 0usize;
    for object in closure.objects {
        if source_cids.contains(&object.root.cid) {
            continue;
        }
        total_bytes = total_bytes.saturating_add(object.bytes.len());
        if object.root.cid == manifest_cid {
            manifest_bytes = object.bytes.len();
        }
    }
    assert!(
        manifest_bytes > 0,
        "derived manifest must be in its closure"
    );
    DerivedClosureSize {
        total_bytes,
        manifest_bytes,
        code_tree_bytes: total_bytes.saturating_sub(manifest_bytes),
    }
}

fn nearest_rank(samples: &[Duration], percentile: usize) -> Duration {
    let rank = samples
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .max(1);
    samples[rank - 1]
}

fn row(
    operation: &str,
    dimensions: usize,
    threads: usize,
    duration: Duration,
    metric_a: usize,
    metric_b: usize,
) {
    println!(
        "{operation},{dimensions},{threads},{:.3},{metric_a},{metric_b}",
        duration.as_secs_f64() * 1_000_000.0
    );
}

fn command_output(program: &str, arguments: &[&str]) -> String {
    Command::new(program)
        .args(arguments)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map(|output| {
            String::from_utf8_lossy(&output.stdout)
                .trim()
                .replace('\n', " | ")
        })
        .filter(|output| !output.is_empty())
        .unwrap_or_else(|| "unavailable".to_owned())
}

fn completion_id(completion: SearchCompletion) -> usize {
    match completion {
        SearchCompletion::Exact => 0,
        SearchCompletion::ApproximatePolicySatisfied => 1,
        SearchCompletion::BudgetExhausted => 2,
        SearchCompletion::Cancelled => 3,
        SearchCompletion::DeadlineExceeded => 4,
    }
}

fn make_records(count: usize, dimensions: usize) -> Vec<ProximityRecord> {
    (0..count)
        .map(|index| ProximityRecord {
            key: format!("record-{index:08}").into_bytes(),
            vector: make_vector(index, dimensions),
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn make_vector(index: usize, dimensions: usize) -> Vec<f32> {
    (0..dimensions)
        .map(|component| {
            let mixed = index
                .wrapping_mul(1_000_003)
                .wrapping_add(component.wrapping_mul(97_409));
            ((mixed % 20_003) as f32 - 10_001.0) / 1_000.0
        })
        .collect()
}

fn recall_at_k(
    records: &[ProximityRecord],
    query: &[f32],
    result: &prolly::SearchResult,
    k: usize,
    metric: DistanceMetric,
) -> f64 {
    let mut scored: Vec<_> = records
        .iter()
        .map(|record| {
            let dot = record
                .vector
                .iter()
                .zip(query)
                .map(|(&left, &right)| f64::from(left) * f64::from(right))
                .sum::<f64>();
            let distance = match metric {
                DistanceMetric::L2Squared => record
                    .vector
                    .iter()
                    .zip(query)
                    .map(|(&left, &right)| {
                        let delta = f64::from(left) - f64::from(right);
                        delta * delta
                    })
                    .sum(),
                DistanceMetric::InnerProduct => -dot,
                DistanceMetric::Cosine => {
                    let left_norm = record
                        .vector
                        .iter()
                        .map(|value| f64::from(*value).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    let right_norm = query
                        .iter()
                        .map(|value| f64::from(*value).powi(2))
                        .sum::<f64>()
                        .sqrt();
                    1.0 - dot / (left_norm * right_norm)
                }
            };
            (distance, record.key.clone())
        })
        .collect();
    scored.sort_by(|left, right| {
        left.0
            .total_cmp(&right.0)
            .then_with(|| left.1.cmp(&right.1))
    });
    let exact: HashSet<_> = scored.into_iter().take(k).map(|(_, key)| key).collect();
    result
        .neighbors
        .iter()
        .filter(|neighbor| exact.contains(&neighbor.key))
        .count() as f64
        / k.max(1) as f64
}

fn benchmark_filter<'a>(keys: &'a [Vec<u8>], eligibility_ppm: usize) -> ProximityFilter<'a> {
    if eligibility_ppm == 1_000_000 {
        ProximityFilter::Prefix(b"record-")
    } else {
        ProximityFilter::EligibleKeys(keys)
    }
}

fn env_metric(name: &str) -> Option<DistanceMetric> {
    let value = std::env::var(name).ok()?;
    Some(match value.trim().to_ascii_lowercase().as_str() {
        "l2" | "l2_squared" => DistanceMetric::L2Squared,
        "cosine" => DistanceMetric::Cosine,
        "inner_product" | "ip" => DistanceMetric::InnerProduct,
        _ => panic!("{name} has an unsupported metric value"),
    })
}

fn env_usize(name: &str) -> Option<usize> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .parse()
            .unwrap_or_else(|_| panic!("{name} must be an unsigned integer")),
    )
}

fn env_bool(name: &str) -> bool {
    std::env::var(name).is_ok_and(|value| {
        matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes"
        )
    })
}

fn env_list(name: &str) -> Option<Vec<usize>> {
    let value = std::env::var(name).ok()?;
    Some(
        value
            .split(',')
            .map(|item| {
                item.trim()
                    .parse()
                    .unwrap_or_else(|_| panic!("{name} must be a comma-separated integer list"))
            })
            .collect(),
    )
}

fn assert_unique_positive_workers(workers: &[usize]) {
    assert!(
        !workers.is_empty(),
        "benchmark worker list must not be empty"
    );
    let mut unique = HashSet::with_capacity(workers.len());
    for worker in workers {
        assert!(*worker > 0, "benchmark worker counts must be positive");
        assert!(
            unique.insert(*worker),
            "benchmark worker counts must be unique"
        );
    }
}
