use std::collections::BTreeMap;
use std::hint::black_box;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

use prolly::{
    append_batch, chunking, BatchApplyStats, BatchBuilder, BoundaryDetector, BoundaryRule,
    ChunkingSpec, Cid, Config, MemStore, Mutation, Node, NodeLayoutSpec, ParallelConfig, Prolly,
    ProllyMetricsSnapshot, Resolution, Resolver, SortedBatchBuilder, Store, Tree,
};

const DEFAULT_SCALE: usize = 10_000;
const PARALLEL_BENCH_SEED: u64 = 0xC4A0_11E1_5EED_2026;

#[derive(Clone, Copy, Debug)]
enum ParallelWorkload {
    Append,
    Random,
    Clustered,
    ValueOnly,
    InsertOnly,
    DeleteOnly,
    Mixed,
}

impl ParallelWorkload {
    const ALL: [Self; 7] = [
        Self::Append,
        Self::Random,
        Self::Clustered,
        Self::ValueOnly,
        Self::InsertOnly,
        Self::DeleteOnly,
        Self::Mixed,
    ];

    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
            Self::ValueOnly => "value-only",
            Self::InsertOnly => "insert-only",
            Self::DeleteOnly => "delete-only",
            Self::Mixed => "mixed-60-20-20",
        }
    }

    fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|workload| workload.name() == value)
    }
}

#[derive(Clone, Debug)]
struct ParallelObservation {
    elapsed_ns: u128,
    peak_rss_bytes: u64,
    nodes_read: u64,
    nodes_written: u64,
    bytes_read: u64,
    bytes_written: u64,
    batch_get_calls: u64,
    batch_put_calls: u64,
    effective_workers: usize,
    parallel_tasks: usize,
    structural_islands: usize,
    coalesced_islands: usize,
    root: String,
}

fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    let explicit_mode = args.first().map(String::as_str);
    let environment_mode = std::env::var("PROLLY_BENCH_ONLY").ok();
    let mode = explicit_mode.or(environment_mode.as_deref());
    if matches!(mode, Some("parallel-scaling" | "parallel-callers")) {
        let base_entries = benchmark_sizes(
            args.get(1),
            "PROLLY_BASE_ENTRIES",
            &[100_000, 1_000_000, 10_000_000],
        );
        let mutations = benchmark_sizes(
            args.get(2),
            "PROLLY_MUTATIONS",
            &[1_000, 10_000, 100_000, 1_000_000],
        );
        match mode {
            Some("parallel-scaling") => bench_parallel_scaling(&base_entries, &mutations),
            Some("parallel-callers") => bench_parallel_callers(&base_entries, &mutations),
            _ => unreachable!(),
        }
        return;
    }

    let scale = std::env::var("PROLLY_BENCH_SCALE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SCALE)
        .max(1_000);

    println!("prolly benchmark scale={scale}");
    println!("name,total_ms,iterations,items,ns_per_item,median_ns,p95_ns,p99_ns");

    match std::env::var("PROLLY_BENCH_ONLY").as_deref() {
        Ok("batch-builder") => {
            bench_batch_builder(scale);
            return;
        }
        Ok("incremental-insert") => {
            bench_incremental_insert(scale / 5);
            return;
        }
        Ok("point-reads") => {
            bench_point_get(scale);
            return;
        }
        Ok("point-updates") => {
            bench_point_updates(scale);
            return;
        }
        Ok("point-deletes") => {
            bench_point_deletes(scale);
            return;
        }
        Ok("range-scans") => {
            bench_range_scan(scale);
            bench_range_scan_window(scale);
            return;
        }
        Ok("diff-suite") => {
            bench_diff_identical(scale);
            bench_diff_sparse(scale);
            bench_diff_append_suffix(scale);
            bench_diff_empty_to_full(scale);
            bench_diff_full_rewrite(scale);
            bench_stream_diff_sparse(scale);
            bench_stream_diff_append_suffix(scale);
            return;
        }
        Ok("append-chain") => {
            bench_append_batch_direct(scale);
            bench_append_batch_chain(scale);
            bench_append_batch_chain_cold(scale);
            return;
        }
        Ok("merge-conflict") => {
            bench_merge_conflict_resolved(scale);
            return;
        }
        Ok("merge-conflict-cold") => {
            bench_merge_conflict_resolved_cold(scale);
            return;
        }
        Ok("stream-append") => {
            bench_stream_diff_append_suffix(scale);
            return;
        }
        Ok("async-engine-recovery") => {
            bench_range_scan(scale);
            bench_stream_diff_append_suffix(scale);
            bench_range_diff_window(scale);
            bench_merge_sparse(scale);
            bench_merge_conflict_resolved(scale);
            bench_merge_conflict_resolved_cold(scale);
            return;
        }
        Ok("async-engine-protected") => {
            bench_incremental_insert(scale / 5);
            bench_batch_builder(scale);
            bench_point_get(scale);
            bench_point_updates(scale);
            bench_point_deletes(scale);
            bench_batch_mutations(scale);
            bench_batch_mutations_mixed(scale);
            bench_batch_mutations_append(scale);
            bench_parallel_batch_mutations(scale);
            return;
        }
        Ok("batch-regressions") => {
            bench_batch_mutations(scale);
            bench_batch_mutations_mixed(scale);
            bench_parallel_batch_mutations(scale);
            return;
        }
        Ok("point-reads-deletes") => {
            bench_point_get(scale);
            bench_point_deletes(scale);
            return;
        }
        Ok("zero-copy-reads") => {
            bench_point_get(scale);
            bench_range_scan(scale);
            bench_range_scan_window(scale);
            return;
        }
        Ok("chunking-cutover") => {
            bench_chunking_cutover(scale);
            return;
        }
        Ok("boundary-hot-path") => {
            bench_boundary_hot_path();
            return;
        }
        Ok("validated-node-decode") => {
            bench_validated_node_decode();
            return;
        }
        Ok(_) | Err(_) => {}
    }

    bench_incremental_insert(scale / 5);
    bench_batch_builder(scale);
    bench_point_get(scale);
    bench_point_updates(scale);
    bench_point_deletes(scale);
    bench_range_scan(scale);
    bench_range_scan_window(scale);
    bench_batch_mutations(scale);
    bench_batch_mutations_mixed(scale);
    bench_batch_mutations_append(scale);
    bench_append_batch_direct(scale);
    bench_append_batch_chain(scale);
    bench_append_batch_chain_cold(scale);
    bench_parallel_batch_mutations(scale);
    bench_diff_identical(scale);
    bench_diff_sparse(scale);
    bench_diff_append_suffix(scale);
    bench_diff_empty_to_full(scale);
    bench_diff_full_rewrite(scale);
    bench_stream_diff_sparse(scale);
    bench_stream_diff_append_suffix(scale);
    bench_range_diff_window(scale);
    bench_merge_sparse(scale);
    bench_merge_conflict_resolved(scale);
}

fn bench_parallel_scaling(base_sizes: &[usize], mutation_sizes: &[usize]) {
    let workers = parse_usize_list("PROLLY_WORKERS", &[1, 2, 4, 8, 12, 16, 0]);
    let workloads = parallel_workloads();
    let runs = balanced_benchmark_runs(benchmark_runs(), workers.len());
    print_parallel_csv_header();

    for &base_entries in base_sizes {
        let entries = data_set(base_entries);
        let config = bench_config();
        let store = Arc::new(MemStore::new());
        let base = build_tree_on_store(store.clone(), &config, &entries);

        for &mutations_len in mutation_sizes {
            for &workload in &workloads {
                let mutations = parallel_mutations(workload, base_entries, mutations_len, 0);
                let reference = apply_parallel_once(store.clone(), &config, &base, &mutations, 1);
                let fresh = fresh_canonical_root(&config, &entries, &mutations);
                assert_eq!(
                    reference.root,
                    tree_root_hex(&fresh),
                    "fresh canonical root mismatch for {} base={} mutations={}",
                    workload.name(),
                    base_entries,
                    mutations_len
                );

                let mut observations_by_worker = workers
                    .iter()
                    .map(|_| Vec::with_capacity(runs))
                    .collect::<Vec<_>>();
                for run in 0..runs {
                    for offset in 0..workers.len() {
                        let worker_index = (run + offset) % workers.len();
                        let requested_workers = workers[worker_index];
                        let observation = apply_parallel_once(
                            store.clone(),
                            &config,
                            &base,
                            &mutations,
                            requested_workers,
                        );
                        assert_eq!(
                            observation.root,
                            reference.root,
                            "worker root mismatch for {} base={} mutations={} workers={}",
                            workload.name(),
                            base_entries,
                            mutations_len,
                            requested_workers
                        );
                        observations_by_worker[worker_index].push(observation);
                    }
                }

                for (worker_index, &requested_workers) in workers.iter().enumerate() {
                    let observations = &observations_by_worker[worker_index];
                    let latencies = observations
                        .iter()
                        .map(|observation| observation.elapsed_ns)
                        .collect::<Vec<_>>();
                    let percentiles = latency_percentiles(&latencies);
                    for (run, observation) in observations.iter().enumerate() {
                        print_parallel_csv_row(
                            workload.name(),
                            base_entries,
                            mutations_len,
                            requested_workers,
                            1,
                            run + 1,
                            observation,
                            percentiles,
                            mutations_len,
                        );
                    }
                }
            }
        }
    }
}

fn bench_parallel_callers(base_sizes: &[usize], mutation_sizes: &[usize]) {
    let workers = parse_usize_list("PROLLY_WORKERS", &[1, 2, 4, 8, 12, 16, 0]);
    let callers = parse_usize_list("PROLLY_CALLERS", &[2, 4, 8]);
    let workloads = parallel_workloads();
    let runs = balanced_benchmark_runs(benchmark_runs(), workers.len());
    print_parallel_csv_header();

    for &base_entries in base_sizes {
        let entries = data_set(base_entries);
        let config = bench_config();
        let store = Arc::new(MemStore::new());
        let base = build_tree_on_store(store.clone(), &config, &entries);

        for &mutations_len in mutation_sizes {
            for &workload in &workloads {
                let max_callers = callers.iter().copied().max().unwrap_or(0);
                let all_caller_mutations = (0..max_callers)
                    .map(|caller| {
                        Arc::new(parallel_mutations(
                            workload,
                            base_entries,
                            mutations_len,
                            caller + 1,
                        ))
                    })
                    .collect::<Vec<_>>();
                let all_reference_roots = all_caller_mutations
                    .iter()
                    .map(|mutations| {
                        let reference =
                            apply_parallel_once(store.clone(), &config, &base, mutations, 1);
                        let fresh = fresh_canonical_root(&config, &entries, mutations);
                        assert_eq!(
                            reference.root,
                            tree_root_hex(&fresh),
                            "fresh caller root mismatch for {}",
                            workload.name(),
                        );
                        reference.root
                    })
                    .collect::<Vec<_>>();

                for &caller_count in &callers {
                    let caller_mutations = &all_caller_mutations[..caller_count];
                    let reference_roots = &all_reference_roots[..caller_count];

                    let mut observations_by_worker = workers
                        .iter()
                        .map(|_| Vec::with_capacity(runs))
                        .collect::<Vec<_>>();
                    let mut latencies_by_worker = workers
                        .iter()
                        .map(|_| Vec::with_capacity(runs * caller_count))
                        .collect::<Vec<_>>();
                    for run in 0..runs {
                        for offset in 0..workers.len() {
                            let worker_index = (run + offset) % workers.len();
                            let requested_workers = workers[worker_index];
                            let barrier = Arc::new(Barrier::new(caller_count + 1));
                            let (total_elapsed, observations) = thread::scope(|scope| {
                                let mut handles = Vec::with_capacity(caller_count);
                                for mutations in caller_mutations {
                                    let store = store.clone();
                                    let config = config.clone();
                                    let base = base.clone();
                                    let barrier = barrier.clone();
                                    let mutations = mutations.clone();
                                    handles.push(scope.spawn(move || {
                                        let manager = Prolly::new(store, config);
                                        let mutations = (*mutations).clone();
                                        barrier.wait();
                                        let start = Instant::now();
                                        let result = manager
                                            .parallel_batch_with_stats(
                                                &base,
                                                mutations,
                                                &ParallelConfig::new(requested_workers, 1),
                                            )
                                            .unwrap();
                                        let elapsed = start.elapsed();
                                        parallel_observation(
                                            elapsed,
                                            result.stats,
                                            manager.metrics(),
                                            &result.tree,
                                        )
                                    }));
                                }

                                let start = Instant::now();
                                barrier.wait();
                                let observations = handles
                                    .into_iter()
                                    .map(|handle| handle.join().unwrap())
                                    .collect::<Vec<_>>();
                                (start.elapsed(), observations)
                            });

                            for (caller, observation) in observations.iter().enumerate() {
                                assert_eq!(
                                    observation.root,
                                    reference_roots[caller],
                                    "concurrent root mismatch for {} callers={} workers={} caller={}",
                                    workload.name(),
                                    caller_count,
                                    requested_workers,
                                    caller
                                );
                                latencies_by_worker[worker_index].push(observation.elapsed_ns);
                            }
                            observations_by_worker[worker_index].push(
                                aggregate_parallel_observations(total_elapsed, &observations),
                            );
                        }
                    }

                    for (worker_index, &requested_workers) in workers.iter().enumerate() {
                        let percentiles = latency_percentiles(&latencies_by_worker[worker_index]);
                        let run_observations = &observations_by_worker[worker_index];
                        for (run, observation) in run_observations.iter().enumerate() {
                            print_parallel_csv_row(
                                workload.name(),
                                base_entries,
                                mutations_len,
                                requested_workers,
                                caller_count,
                                run + 1,
                                observation,
                                percentiles,
                                mutations_len.saturating_mul(caller_count),
                            );
                        }
                    }
                }
            }
        }
    }
}

fn apply_parallel_once(
    store: Arc<MemStore>,
    config: &Config,
    base: &Tree,
    mutations: &[Mutation],
    requested_workers: usize,
) -> ParallelObservation {
    let manager = Prolly::new(store, config.clone());
    let start = Instant::now();
    let result = manager
        .parallel_batch_with_stats(
            base,
            mutations.to_vec(),
            &ParallelConfig::new(requested_workers, 1),
        )
        .unwrap();
    parallel_observation(
        start.elapsed(),
        result.stats,
        manager.metrics(),
        &result.tree,
    )
}

fn parallel_observation(
    elapsed: Duration,
    stats: BatchApplyStats,
    metrics: ProllyMetricsSnapshot,
    tree: &Tree,
) -> ParallelObservation {
    ParallelObservation {
        elapsed_ns: elapsed.as_nanos(),
        peak_rss_bytes: peak_rss_bytes(),
        nodes_read: metrics.nodes_read,
        nodes_written: metrics.nodes_written,
        bytes_read: metrics.bytes_read,
        bytes_written: metrics.bytes_written,
        batch_get_calls: metrics.store_batch_get_calls,
        batch_put_calls: metrics.store_batch_put_calls,
        effective_workers: stats.parallel_width,
        parallel_tasks: stats.parallel_tasks,
        structural_islands: stats.structural_islands,
        coalesced_islands: stats.coalesced_islands,
        root: tree_root_hex(tree),
    }
}

fn aggregate_parallel_observations(
    total_elapsed: Duration,
    observations: &[ParallelObservation],
) -> ParallelObservation {
    ParallelObservation {
        elapsed_ns: total_elapsed.as_nanos(),
        peak_rss_bytes: observations
            .iter()
            .map(|observation| observation.peak_rss_bytes)
            .max()
            .unwrap_or_default(),
        nodes_read: observations.iter().map(|value| value.nodes_read).sum(),
        nodes_written: observations.iter().map(|value| value.nodes_written).sum(),
        bytes_read: observations.iter().map(|value| value.bytes_read).sum(),
        bytes_written: observations.iter().map(|value| value.bytes_written).sum(),
        batch_get_calls: observations.iter().map(|value| value.batch_get_calls).sum(),
        batch_put_calls: observations.iter().map(|value| value.batch_put_calls).sum(),
        effective_workers: observations
            .iter()
            .map(|value| value.effective_workers)
            .max()
            .unwrap_or(1),
        parallel_tasks: observations.iter().map(|value| value.parallel_tasks).sum(),
        structural_islands: observations
            .iter()
            .map(|value| value.structural_islands)
            .sum(),
        coalesced_islands: observations
            .iter()
            .map(|value| value.coalesced_islands)
            .sum(),
        root: observations
            .iter()
            .map(|value| value.root.as_str())
            .collect::<Vec<_>>()
            .join("|"),
    }
}

fn fresh_canonical_root(
    config: &Config,
    base_entries: &[(Vec<u8>, Vec<u8>)],
    mutations: &[Mutation],
) -> Tree {
    let mut expected = base_entries.iter().cloned().collect::<BTreeMap<_, _>>();
    for mutation in mutations {
        match mutation {
            Mutation::Upsert { key, val } => {
                expected.insert(key.clone(), val.clone());
            }
            Mutation::Delete { key } => {
                expected.remove(key);
            }
        }
    }
    let store = Arc::new(MemStore::new());
    let mut builder = SortedBatchBuilder::new(store, config.clone());
    for (key, value) in expected {
        builder.add(key, value).unwrap();
    }
    builder.build().unwrap()
}

fn parallel_mutations(
    workload: ParallelWorkload,
    base_entries: usize,
    mutation_count: usize,
    lane: usize,
) -> Vec<Mutation> {
    let seed = PARALLEL_BENCH_SEED ^ (lane as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let (offset, step) = permutation_parameters(base_entries, seed);
    let existing_index = |index: usize| {
        if base_entries == 0 {
            0
        } else {
            (offset + index.wrapping_mul(step)) % base_entries
        }
    };
    let clustered_span = base_entries
        .min(mutation_count.max((base_entries / 100).max(1)))
        .max(1);
    let clustered_start = base_entries.saturating_sub(clustered_span) / 2;

    (0..mutation_count)
        .map(|index| {
            let existing = existing_index(index);
            // Keep updates no larger than the base fixture values so the
            // key-stable workload measures parallel rewrite work rather than
            // intentionally triggering the hard-byte-cap fallback.
            let value = || format!("{:016x}", splitmix64(seed ^ index as u64)).into_bytes();
            let insert = || Mutation::Upsert {
                key: format!("key-{existing:08}-insert-{lane:04}-{index:012}").into_bytes(),
                val: value(),
            };
            match workload {
                ParallelWorkload::Append => Mutation::Upsert {
                    key: key_for_index(
                        base_entries
                            .saturating_add(lane.saturating_mul(mutation_count))
                            .saturating_add(index),
                    ),
                    val: value(),
                },
                ParallelWorkload::Random => match index % 4 {
                    0 | 1 => Mutation::Upsert {
                        key: key_for_index(existing),
                        val: value(),
                    },
                    2 => Mutation::Delete {
                        key: key_for_index(existing),
                    },
                    _ => insert(),
                },
                ParallelWorkload::Clustered => {
                    let clustered = clustered_start + index % clustered_span;
                    match index % 5 {
                        0 => Mutation::Delete {
                            key: key_for_index(clustered),
                        },
                        1 => Mutation::Upsert {
                            key: format!("key-{clustered:08}-cluster-{lane:04}-{index:012}")
                                .into_bytes(),
                            val: value(),
                        },
                        _ => Mutation::Upsert {
                            key: key_for_index(clustered),
                            val: value(),
                        },
                    }
                }
                ParallelWorkload::ValueOnly => Mutation::Upsert {
                    key: key_for_index(existing),
                    val: value(),
                },
                ParallelWorkload::InsertOnly => insert(),
                ParallelWorkload::DeleteOnly => Mutation::Delete {
                    key: key_for_index(existing),
                },
                ParallelWorkload::Mixed => match index % 5 {
                    0 => Mutation::Delete {
                        key: key_for_index(existing),
                    },
                    1 => insert(),
                    _ => Mutation::Upsert {
                        key: key_for_index(existing),
                        val: value(),
                    },
                },
            }
        })
        .collect()
}

fn permutation_parameters(modulus: usize, seed: u64) -> (usize, usize) {
    if modulus <= 1 {
        return (0, 1);
    }
    let offset = (splitmix64(seed) as usize) % modulus;
    let mut step = ((splitmix64(seed ^ 0xA076_1D64_78BD_642F) as usize) % modulus).max(1);
    while greatest_common_divisor(step, modulus) != 1 {
        step = step.wrapping_add(1) % modulus;
        if step == 0 {
            step = 1;
        }
    }
    (offset, step)
}

fn greatest_common_divisor(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

fn benchmark_sizes(
    positional: Option<&String>,
    environment: &str,
    defaults: &[usize],
) -> Vec<usize> {
    if let Some(value) = positional {
        return value
            .parse::<usize>()
            .ok()
            .filter(|value| *value > 0)
            .into_iter()
            .collect::<Vec<_>>();
    }
    parse_usize_list(environment, defaults)
}

fn parse_usize_list(environment: &str, defaults: &[usize]) -> Vec<usize> {
    std::env::var(environment)
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| part.trim().parse::<usize>().ok())
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| defaults.to_vec())
}

fn parallel_workloads() -> Vec<ParallelWorkload> {
    std::env::var("PROLLY_WORKLOADS")
        .ok()
        .map(|value| {
            value
                .split(',')
                .filter_map(|part| ParallelWorkload::parse(part.trim()))
                .collect::<Vec<_>>()
        })
        .filter(|values| !values.is_empty())
        .unwrap_or_else(|| ParallelWorkload::ALL.to_vec())
}

fn benchmark_runs() -> usize {
    std::env::var("PROLLY_BENCH_RUNS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(5)
        .max(1)
}

fn balanced_benchmark_runs(requested: usize, widths: usize) -> usize {
    requested
        .div_ceil(widths.max(1))
        .saturating_mul(widths.max(1))
}

fn latency_percentiles(samples: &[u128]) -> (u128, u128, u128) {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let percentile = |percent: usize| {
        let rank = (percent * sorted.len()).div_ceil(100).max(1);
        sorted[rank - 1]
    };
    (percentile(50), percentile(95), percentile(99))
}

fn print_parallel_csv_header() {
    println!(
        "workload,base_entries,mutations,workers,effective_workers,callers,run,elapsed_ns,ops_per_sec,p50_ns,p95_ns,p99_ns,peak_rss_bytes,nodes_read,nodes_written,bytes_read,bytes_written,batch_get_calls,batch_put_calls,parallel_tasks,structural_islands,coalesced_islands,root"
    );
}

#[allow(clippy::too_many_arguments)]
fn print_parallel_csv_row(
    workload: &str,
    base_entries: usize,
    mutations: usize,
    requested_workers: usize,
    callers: usize,
    run: usize,
    observation: &ParallelObservation,
    percentiles: (u128, u128, u128),
    total_operations: usize,
) {
    let operations_per_second =
        total_operations as f64 * 1_000_000_000.0 / observation.elapsed_ns.max(1) as f64;
    println!(
        "{workload},{base_entries},{mutations},{requested_workers},{},{callers},{run},{},{operations_per_second:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        observation.effective_workers,
        observation.elapsed_ns,
        percentiles.0,
        percentiles.1,
        percentiles.2,
        observation.peak_rss_bytes,
        observation.nodes_read,
        observation.nodes_written,
        observation.bytes_read,
        observation.bytes_written,
        observation.batch_get_calls,
        observation.batch_put_calls,
        observation.parallel_tasks,
        observation.structural_islands,
        observation.coalesced_islands,
        observation.root,
    );
}

fn tree_root_hex(tree: &Tree) -> String {
    tree.root
        .as_ref()
        .map(|root| {
            root.as_bytes()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        })
        .unwrap_or_else(|| "empty".to_owned())
}

#[cfg(unix)]
fn peak_rss_bytes() -> u64 {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    let status = unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) };
    if status != 0 {
        return 0;
    }
    let resident = unsafe { usage.assume_init() }.ru_maxrss.max(0) as u64;
    if cfg!(any(target_os = "macos", target_os = "ios")) {
        resident
    } else {
        resident.saturating_mul(1024)
    }
}

#[cfg(not(unix))]
fn peak_rss_bytes() -> u64 {
    0
}

fn bench_boundary_hot_path() {
    let entries = std::env::var("PROLLY_BOUNDARY_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(2_000_000)
        .max(10_000);
    measure_boundary_detector(
        "boundary_entry_count_key_hash",
        chunking::entry_count_key_hash(),
        entries,
    );
    measure_boundary_detector(
        "boundary_logical_bytes_key_weibull",
        chunking::logical_bytes_key_weibull(),
        entries,
    );
    measure_boundary_detector(
        "boundary_logical_bytes_rolling_hash",
        chunking::logical_bytes_rolling_hash(),
        entries,
    );
}

fn measure_boundary_detector(name: &str, spec: ChunkingSpec, entries: usize) {
    measure(name, 5, entries, || {
        let mut detector =
            BoundaryDetector::new(spec.clone(), 0).expect("built-in policy is valid");
        let value = [11_u8; 16];
        let mut boundaries = 0usize;
        for index in 0..entries {
            let key = (index as u64).to_be_bytes();
            boundaries += usize::from(
                detector
                    .observe(black_box(&key), black_box(&value), 24)
                    .expect("fixed entry is valid"),
            );
        }
        black_box(boundaries);
    });
}

fn bench_chunking_cutover(items: usize) {
    let data = data_set(items);
    let config = bench_config();

    measure("cutover_sorted_build", 20, items, || {
        let store = Arc::new(MemStore::new());
        let mut builder = SortedBatchBuilder::new(store, config.clone());
        for (key, value) in &data {
            builder.add(key.clone(), value.clone()).unwrap();
        }
        black_box(builder.build().unwrap().root);
    });

    measure("cutover_unsorted_build", 20, items, || {
        let store = Arc::new(MemStore::new());
        let mut builder = BatchBuilder::new(store, config.clone());
        for (key, value) in data.iter().rev() {
            builder.add(key.clone(), value.clone());
        }
        black_box(builder.build().unwrap().root);
    });

    let store = Arc::new(MemStore::new());
    let base = build_tree_on_store(store.clone(), &config, &data);
    let manager = Prolly::new(store, config);
    for append_count in [1, 64, 4_096] {
        let mutations = append_mutations(items, append_count, "cutover-append");
        measure(
            &format!("cutover_append_{append_count}"),
            20,
            append_count,
            || {
                black_box(
                    manager
                        .append_batch(&base, black_box(mutations.clone()))
                        .unwrap()
                        .root,
                );
            },
        );
    }

    let middle = items / 2;
    let update = vec![Mutation::Upsert {
        key: key_for_index(middle),
        val: b"cutover-middle-update".to_vec(),
    }];
    measure("cutover_middle_update", 20, 1, || {
        black_box(
            manager
                .batch(&base, black_box(update.clone()))
                .unwrap()
                .root,
        );
    });

    let insert = vec![Mutation::Upsert {
        key: format!("key-{middle:08}-insert").into_bytes(),
        val: b"cutover-middle-insert".to_vec(),
    }];
    measure("cutover_middle_insert", 20, 1, || {
        black_box(
            manager
                .batch(&base, black_box(insert.clone()))
                .unwrap()
                .root,
        );
    });

    let delete = vec![Mutation::Delete {
        key: key_for_index(middle),
    }];
    measure("cutover_middle_delete", 20, 1, || {
        black_box(
            manager
                .batch(&base, black_box(delete.clone()))
                .unwrap()
                .root,
        );
    });
}

fn bench_incremental_insert(items: usize) {
    let items = std::env::var("PROLLY_BENCH_INCREMENTAL_ITEMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(items);
    let data = data_set(items);
    let config = bench_config();

    measure("incremental_insert_mem", 5, items, || {
        let store = MemStore::new();
        let prolly = Prolly::new(store, config.clone());
        let mut tree = prolly.create();
        for (key, val) in &data {
            tree = prolly
                .put(&tree, black_box(key.clone()), black_box(val.clone()))
                .unwrap();
        }
        black_box(tree.root);
    });
}

fn bench_batch_builder(items: usize) {
    let data = data_set(items);
    let config = bench_config();

    measure("batch_builder_mem", 10, items, || {
        let store = Arc::new(MemStore::new());
        let mut builder = BatchBuilder::new(store, config.clone());
        for (key, val) in data.iter().rev() {
            builder.add(black_box(key.clone()), black_box(val.clone()));
        }
        let tree = builder.build().unwrap();
        black_box(tree.root);
    });
}

fn bench_point_get(items: usize) {
    let data = data_set(items);
    let (_, prolly, tree) = build_tree(&data);
    let gets = std::env::var("PROLLY_BENCH_GETS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(items * 20);

    if std::env::var_os("PROLLY_BENCH_DIAG").is_some() {
        println!(
            "point_get_tree_stats={}",
            prolly.collect_stats(&tree).unwrap()
        );
    }

    measure("point_get_mem", 10, gets, || {
        for i in 0..gets {
            let key = &data[i % data.len()].0;
            black_box(prolly.get(&tree, black_box(key)).unwrap());
        }
    });

    let mut session = prolly.read(&tree).unwrap();
    measure("point_get_borrowed_mem", 10, gets, || {
        for i in 0..gets {
            let key = &data[i % data.len()].0;
            black_box(
                session
                    .get_with(black_box(key), |value| black_box(value.len()))
                    .unwrap(),
            );
        }
    });
}

fn bench_point_updates(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let count = mutation_count(items);
    let updates = point_updates(items, count, "point-update");

    measure("point_updates_mem", 20, count, || {
        let mut tree = base.clone();
        for (key, val) in &updates {
            tree = prolly
                .put(&tree, black_box(key.clone()), black_box(val.clone()))
                .unwrap();
        }
        black_box(tree.root);
    });
}

fn bench_point_deletes(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let count = mutation_count(items);
    let deletes = point_delete_keys(items, count);

    measure("point_deletes_mem", 20, count, || {
        let mut tree = base.clone();
        for key in &deletes {
            tree = prolly.delete(&tree, black_box(key)).unwrap();
        }
        black_box(tree.root);
    });
}

fn bench_range_scan(items: usize) {
    let data = data_set(items);
    let (_, prolly, tree) = build_tree(&data);

    measure("range_scan_mem", 25, items, || {
        let count = prolly
            .range(&tree, &[], None)
            .unwrap()
            .map(|entry| entry.unwrap())
            .inspect(|entry| {
                black_box(entry);
            })
            .count();
        black_box(count);
    });

    let mut session = prolly.read(&tree).unwrap();
    measure("range_scan_borrowed_mem", 25, items, || {
        let count = session
            .scan_range(&[], None, |entry| {
                black_box(entry.key());
                black_box(entry.value());
            })
            .unwrap();
        black_box(count);
    });
}

fn bench_range_scan_window(items: usize) {
    let data = data_set(items);
    let (_, prolly, tree) = build_tree(&data);
    let (start_idx, end_idx) = window_bounds(items);
    let start = key_for_index(start_idx);
    let end = key_for_index(end_idx);
    let window_items = end_idx - start_idx;

    measure("range_scan_window_mem", 50, window_items, || {
        let count = prolly
            .range(
                &tree,
                black_box(start.as_slice()),
                Some(black_box(end.as_slice())),
            )
            .unwrap()
            .map(|entry| entry.unwrap())
            .inspect(|entry| {
                black_box(entry);
            })
            .count();
        black_box(count);
    });

    let mut session = prolly.read(&tree).unwrap();
    measure("range_scan_window_borrowed_mem", 50, window_items, || {
        let count = session
            .scan_range(
                black_box(start.as_slice()),
                Some(black_box(end.as_slice())),
                |entry| {
                    black_box(entry.key());
                    black_box(entry.value());
                },
            )
            .unwrap();
        black_box(count);
    });
}

fn bench_batch_mutations(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = update_mutations(items, mutation_count, "updated");

    measure("batch_mutations_mem", 20, mutation_count, || {
        let tree = prolly.batch(&base, black_box(mutations.clone())).unwrap();
        black_box(tree.root);
    });
}

fn bench_batch_mutations_mixed(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = mixed_mutations(items, mutation_count, "mixed");

    if std::env::var_os("PROLLY_BENCH_DIAG").is_some() {
        let (_, stats) = prolly
            .batch_with_write_stats(&base, mutations.clone())
            .unwrap();
        println!("mixed_batch_stats={stats:?}");
    }

    measure("batch_mutations_mixed_mem", 20, mutation_count, || {
        let tree = prolly.batch(&base, black_box(mutations.clone())).unwrap();
        black_box(tree.root);
    });
}

fn bench_batch_mutations_append(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = append_mutations(items, mutation_count, "append");

    measure("batch_mutations_append_mem", 20, mutation_count, || {
        let tree = prolly.batch(&base, black_box(mutations.clone())).unwrap();
        black_box(tree.root);
    });
}

fn bench_append_batch_direct(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = append_mutations(items, mutation_count, "append-direct");

    measure("append_batch_direct_mem", 20, mutation_count, || {
        let tree = append_batch(&prolly, &base, black_box(mutations.clone())).unwrap();
        black_box(tree.root);
    });
}

fn bench_append_batch_chain(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let batch_size = append_chain_batch_size(items);
    let rounds = append_chain_rounds();
    let batches = append_chain_mutation_batches(items, batch_size, rounds, "append-chain");

    measure("append_batch_chain_mem", 20, batch_size * rounds, || {
        let mut tree = base.clone();
        for batch in &batches {
            tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
        }
        black_box(tree.root);
    });
}

fn bench_append_batch_chain_cold(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let batch_size = append_chain_batch_size(items);
    let rounds = append_chain_rounds();
    let batches = append_chain_mutation_batches(items, batch_size, rounds, "append-chain-cold");

    measure(
        "append_batch_chain_cold_mem",
        20,
        batch_size * rounds,
        || {
            let mut tree = base.clone();
            for batch in &batches {
                prolly.clear_cache();
                tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
            }
            black_box(tree.root);
        },
    );
}

fn bench_validated_node_decode() {
    let entries = std::env::var("PROLLY_DECODE_ENTRIES")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(64)
        .max(1);
    let node = Node::builder()
        .keys(
            (0..entries)
                .map(|index| format!("key/{index:016}").into_bytes())
                .collect(),
        )
        .vals(
            (0..entries)
                .map(|index| format!("value/{index:032}").into_bytes())
                .collect(),
        )
        .leaf(true)
        .build();
    node.validate().expect("decode benchmark fixture is valid");
    let format = node.format.clone();
    let bytes = node.to_bytes();
    let expected = Cid::from_bytes(&bytes);
    let iterations = 20_000;

    measure("node_cid_sha256", iterations, 1, || {
        black_box(Cid::from_bytes(black_box(&bytes)));
    });
    measure("node_decode_structural", iterations, 1, || {
        black_box(Node::from_bytes_with_format(black_box(&bytes), &format).unwrap());
    });
    measure("node_validate_and_decode", iterations, 1, || {
        let actual = Cid::from_bytes(black_box(&bytes));
        assert_eq!(actual, expected);
        black_box(Node::from_bytes_with_format(black_box(&bytes), &format).unwrap());
    });
}

fn bench_parallel_batch_mutations(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = mixed_mutations(items, mutation_count, "parallel");
    let config = ParallelConfig::new(0, 1);

    measure("parallel_batch_mutations_mem", 20, mutation_count, || {
        let tree = prolly
            .parallel_batch(&base, black_box(mutations.clone()), &config)
            .unwrap();
        black_box(tree.root);
    });
}

fn bench_diff_identical(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);

    measure("diff_identical_mem", 1_000, 1, || {
        let diffs = prolly.diff(&base, &base).unwrap();
        black_box(diffs);
    });
}

fn bench_diff_sparse(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let other = sparse_changed_tree(&prolly, &base, items);

    measure("diff_sparse_mem", 20, items, || {
        let diffs = prolly.diff(&base, &other).unwrap();
        black_box(diffs);
    });
}

fn bench_diff_append_suffix(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let count = mutation_count(items);
    let other = append_batch(
        &prolly,
        &base,
        append_mutations(items, count, "diff-append"),
    )
    .unwrap();

    measure("diff_append_suffix_mem", 20, count, || {
        let diffs = prolly.diff(&base, &other).unwrap();
        black_box(diffs);
    });
}

fn bench_diff_empty_to_full(items: usize) {
    let data = data_set(items);
    let (_, prolly, other) = build_tree(&data);
    let empty = prolly.create();

    measure("diff_empty_to_full_mem", 10, items, || {
        let diffs = prolly.diff(&empty, &other).unwrap();
        black_box(diffs.len());
    });
}

fn bench_diff_full_rewrite(items: usize) {
    let data = data_set(items);
    let (store, prolly, base) = build_tree(&data);
    let config = bench_config();
    let rewritten = data
        .iter()
        .enumerate()
        .map(|(i, (key, _))| (key.clone(), format!("rewritten-{i:08}").into_bytes()))
        .collect::<Vec<_>>();
    let other = build_tree_on_store(store, &config, &rewritten);

    measure("diff_full_rewrite_mem", 10, items, || {
        let diffs = prolly.diff(&base, &other).unwrap();
        black_box(diffs);
    });
}

fn bench_stream_diff_sparse(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let other = sparse_changed_tree(&prolly, &base, items);

    measure("stream_diff_sparse_mem", 20, items, || {
        let diffs = prolly
            .stream_diff(&base, &other)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        black_box(diffs);
    });
}

fn bench_stream_diff_append_suffix(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let count = mutation_count(items);
    let other = append_batch(
        &prolly,
        &base,
        append_mutations(items, count, "stream-diff-append"),
    )
    .unwrap();

    if std::env::var_os("PROLLY_BENCH_DIAG").is_some() {
        let page = prolly
            .structural_diff_page(&base, &other, None, count + 1)
            .unwrap();
        println!("stream_append_stats={:?}", page.stats);
    }

    measure("stream_diff_append_suffix_mem", 20, count, || {
        let diffs = prolly
            .stream_diff(&base, &other)
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        black_box(diffs);
    });
}

fn bench_range_diff_window(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let (start_idx, end_idx) = window_bounds(items);
    let other = range_changed_tree(&prolly, &base, start_idx, end_idx);
    let start = key_for_index(start_idx);
    let end = key_for_index(end_idx);
    let window_items = end_idx - start_idx;

    measure("range_diff_window_mem", 30, window_items, || {
        let diffs = prolly
            .range_diff(&base, &other, start.as_slice(), Some(end.as_slice()))
            .unwrap();
        black_box(diffs.len());
    });

    measure("range_diff_window_scan_mem", 30, window_items, || {
        let diff_count = range_diff_count(
            &prolly,
            &base,
            &other,
            start.as_slice(),
            Some(end.as_slice()),
        );
        black_box(diff_count);
    });
}

fn bench_merge_sparse(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mut left = base.clone();
    let mut right = base.clone();

    for i in (0..items).step_by((items / 100).max(1)) {
        left = prolly
            .put(
                &left,
                format!("left-{i:08}").into_bytes(),
                format!("left-value-{i:08}").into_bytes(),
            )
            .unwrap();
        right = prolly
            .put(
                &right,
                format!("right-{i:08}").into_bytes(),
                format!("right-value-{i:08}").into_bytes(),
            )
            .unwrap();
    }

    measure("merge_sparse_mem", 10, items, || {
        let merged = prolly.merge(&base, &left, &right, None).unwrap();
        black_box(merged.root);
    });
}

fn bench_merge_conflict_resolved(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mut left = base.clone();
    let mut right = base.clone();
    let conflict_step = (items / 100).max(1);
    let mut conflicts = 0;

    for i in (0..items).step_by(conflict_step) {
        let key = key_for_index(i);
        left = prolly
            .put(
                &left,
                key.clone(),
                format!("left-conflict-{i:08}").into_bytes(),
            )
            .unwrap();
        right = prolly
            .put(&right, key, format!("right-conflict-{i:08}").into_bytes())
            .unwrap();
        conflicts += 1;
    }

    let iterations = std::env::var("PROLLY_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(10);
    measure("merge_conflict_resolved_mem", iterations, conflicts, || {
        let resolver: Resolver =
            Box::new(|conflict| Resolution::value(conflict.right.clone().expect("right value")));
        let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();
        black_box(merged.root);
    });
}

fn bench_merge_conflict_resolved_cold(items: usize) {
    let data = data_set(items);
    let (store, prolly, base) = build_tree(&data);
    let mut left = base.clone();
    let mut right = base.clone();
    let conflict_step = (items / 100).max(1);
    let mut conflicts = 0;

    for i in (0..items).step_by(conflict_step) {
        let key = key_for_index(i);
        left = prolly
            .put(
                &left,
                key.clone(),
                format!("left-conflict-{i:08}").into_bytes(),
            )
            .unwrap();
        right = prolly
            .put(&right, key, format!("right-conflict-{i:08}").into_bytes())
            .unwrap();
        conflicts += 1;
    }

    let reopened = Prolly::new(store, bench_config());
    measure("merge_conflict_resolved_cold_mem", 10, conflicts, || {
        let resolver: Resolver =
            Box::new(|conflict| Resolution::value(conflict.right.clone().expect("right value")));
        let merged = reopened
            .merge(&base, &left, &right, Some(resolver))
            .unwrap();
        black_box(merged.root);
    });
}

fn measure<F>(name: &str, iterations: usize, items: usize, mut f: F)
where
    F: FnMut(),
{
    let iterations = std::env::var("PROLLY_BENCH_ITERATIONS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(iterations);
    f();

    let mut total = Duration::ZERO;
    let mut samples = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        let elapsed = start.elapsed();
        total += elapsed;
        samples.push(elapsed.as_nanos() / items.max(1) as u128);
    }

    samples.sort_unstable();
    let percentile = |percent: usize| {
        let rank = (percent * samples.len()).div_ceil(100).max(1);
        samples[rank - 1]
    };

    let total_ns = total.as_nanos();
    let total_items = iterations as u128 * items as u128;
    let ns_per_item = total_ns.checked_div(total_items).unwrap_or_default();

    println!(
        "{name},{:.3},{iterations},{items},{ns_per_item},{},{},{}",
        total.as_secs_f64() * 1_000.0,
        percentile(50),
        percentile(95),
        percentile(99)
    );
}

fn build_tree(entries: &[(Vec<u8>, Vec<u8>)]) -> (Arc<MemStore>, Prolly<Arc<MemStore>>, Tree) {
    let store = Arc::new(MemStore::new());
    let config = bench_config();
    let tree = build_tree_on_store(store.clone(), &config, entries);
    let prolly = Prolly::new(store.clone(), config);
    (store, prolly, tree)
}

fn build_tree_on_store(
    store: Arc<MemStore>,
    config: &Config,
    entries: &[(Vec<u8>, Vec<u8>)],
) -> Tree {
    let mut builder = BatchBuilder::new(store.clone(), config.clone());
    for (key, val) in entries {
        builder.add(key.clone(), val.clone());
    }
    builder.build().unwrap()
}

fn sparse_changed_tree<S: Store>(prolly: &Prolly<S>, base: &Tree, items: usize) -> Tree {
    let mut other = base.clone();
    for i in (0..items).step_by((items / 100).max(1)) {
        other = prolly
            .put(
                &other,
                key_for_index(i),
                format!("changed-{i:08}").into_bytes(),
            )
            .unwrap();
    }
    other = prolly
        .put(&other, b"key-new-sparse".to_vec(), b"new".to_vec())
        .unwrap();
    other
}

fn range_changed_tree<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    start_idx: usize,
    end_idx: usize,
) -> Tree {
    let mut other = base.clone();
    for (offset, i) in (start_idx..end_idx).enumerate() {
        let key = key_for_index(i);
        if offset % 7 == 0 {
            other = prolly.delete(&other, &key).unwrap();
        } else if offset % 3 == 0 {
            other = prolly
                .put(&other, key, format!("range-changed-{i:08}").into_bytes())
                .unwrap();
        }
    }

    let inserts = ((end_idx - start_idx) / 20).max(1);
    for offset in 0..inserts {
        let i = start_idx + offset * 20;
        other = prolly
            .put(
                &other,
                format!("key-{i:08}-extra").into_bytes(),
                format!("range-added-{i:08}").into_bytes(),
            )
            .unwrap();
    }

    other
}

fn range_diff_count<S: Store>(
    prolly: &Prolly<S>,
    base: &Tree,
    other: &Tree,
    start: &[u8],
    end: Option<&[u8]>,
) -> usize {
    let base_entries = prolly
        .range(base, start, end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let other_entries = prolly
        .range(other, start, end)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    entry_diff_count(&base_entries, &other_entries)
}

fn entry_diff_count(base: &[(Vec<u8>, Vec<u8>)], other: &[(Vec<u8>, Vec<u8>)]) -> usize {
    let mut base_idx = 0;
    let mut other_idx = 0;
    let mut diffs = 0;

    while base_idx < base.len() && other_idx < other.len() {
        let (base_key, base_val) = &base[base_idx];
        let (other_key, other_val) = &other[other_idx];

        match base_key.cmp(other_key) {
            std::cmp::Ordering::Less => {
                diffs += 1;
                base_idx += 1;
            }
            std::cmp::Ordering::Greater => {
                diffs += 1;
                other_idx += 1;
            }
            std::cmp::Ordering::Equal => {
                if base_val != other_val {
                    diffs += 1;
                }
                base_idx += 1;
                other_idx += 1;
            }
        }
    }

    diffs + base.len() - base_idx + other.len() - other_idx
}

fn update_mutations(items: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| {
            let key_idx = i * 7 % items;
            Mutation::Upsert {
                key: key_for_index(key_idx),
                val: format!("{label}-{i:08}").into_bytes(),
            }
        })
        .collect()
}

fn mixed_mutations(items: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| match i % 5 {
            0 => Mutation::Delete {
                key: key_for_index(i * 11 % items),
            },
            1 | 2 => Mutation::Upsert {
                key: key_for_index(i * 7 % items),
                val: format!("{label}-update-{i:08}").into_bytes(),
            },
            _ => Mutation::Upsert {
                key: format!("key-new-{label}-{i:08}").into_bytes(),
                val: format!("{label}-insert-{i:08}").into_bytes(),
            },
        })
        .collect()
}

fn append_mutations(items: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(items + i),
            val: format!("{label}-{i:08}").into_bytes(),
        })
        .collect()
}

fn append_chain_batch_size(items: usize) -> usize {
    (items / 100).clamp(25, 200)
}

fn append_chain_rounds() -> usize {
    10
}

fn append_chain_mutation_batches(
    start: usize,
    batch_size: usize,
    rounds: usize,
    label: &str,
) -> Vec<Vec<Mutation>> {
    (0..rounds)
        .map(|round| {
            append_mutations(
                start + round * batch_size,
                batch_size,
                &format!("{label}-{round:02}"),
            )
        })
        .collect()
}

fn point_updates(items: usize, count: usize, label: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..count)
        .map(|i| {
            let key_idx = (items / 4 + i * 7) % items;
            (
                key_for_index(key_idx),
                format!("{label}-{i:08}").into_bytes(),
            )
        })
        .collect()
}

fn point_delete_keys(items: usize, count: usize) -> Vec<Vec<u8>> {
    (0..count)
        .map(|i| {
            let key_idx = (items / 4 + i * 7) % items;
            key_for_index(key_idx)
        })
        .collect()
}

fn mutation_count(items: usize) -> usize {
    std::env::var("PROLLY_BENCH_MUTATIONS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| (items / 10).max(100))
        .min(items)
}

fn window_bounds(items: usize) -> (usize, usize) {
    let start = items / 3;
    let len = std::env::var("PROLLY_BENCH_WINDOW_ITEMS")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|value| *value > 0)
        .unwrap_or_else(|| (items / 10).max(100))
        .min(items - start);
    (start, start + len)
}

fn data_set(items: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..items)
        .map(|i| {
            (
                key_for_index(i),
                format!("value-{i:08}-payload").into_bytes(),
            )
        })
        .collect()
}

fn key_for_index(i: usize) -> Vec<u8> {
    format!("key-{i:08}").into_bytes()
}

fn bench_config() -> Config {
    let policy = std::env::var("PROLLY_BENCH_POLICY").unwrap_or_default();
    let key_value = matches!(policy.as_str(), "legacy-equivalent" | "key-value-plain");
    let prefix = !matches!(policy.as_str(), "key-value-plain" | "key-only-plain");
    let mut chunking = if key_value {
        chunking::entry_count_key_value_hash()
    } else {
        chunking::entry_count_key_hash()
    };
    chunking.min = 16;
    chunking.target = 64;
    chunking.max = 128;
    chunking.hash_seed = 0xC0DA;
    chunking.rule = BoundaryRule::HashThreshold { factor: 64 };
    Config::builder()
        .chunking(chunking)
        .node_layout(if prefix {
            NodeLayoutSpec::PrefixCompressed
        } else {
            NodeLayoutSpec::Plain
        })
        .build()
}
