use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use prolly::{
    BatchApplyStats, BatchOp, BatchWriter, BatchWriterConfig, Config, ManifestStore,
    ManifestStoreScan, ManifestUpdate, Mutation, NamedRootManifest, Prolly, RootManifest,
    SlateDbStore, SlateDbStoreConfig, Store, Tree, TreeStats,
};
use slatedb::config::{CompressionCodec, Settings};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

const DEFAULT_STAGES: &str = "10000";
const DEFAULT_BATCH_SIZE: usize = 10_000;
const DEFAULT_OPS_PER_CYCLE: usize = 1_000;
const DEFAULT_CYCLES_PER_STAGE: usize = 1;
const DEFAULT_VALUE_BYTES: usize = 96;
const DEFAULT_MAX_SECONDS: u64 = 900;
const DEFAULT_MAX_OBJECT_GB: u64 = 70;
const DEFAULT_STATS_MAX_RECORDS: usize = 1_000_000;
const DEFAULT_OBJECT_SAMPLE_CYCLES: usize = 1;
const DEFAULT_MIN_CHUNK_SIZE: usize = 64;
const DEFAULT_MAX_CHUNK_SIZE: usize = 512;
const DEFAULT_CHUNKING_FACTOR: usize = 256;

#[derive(Debug, Default, Clone, Copy)]
struct ObjectStats {
    count: usize,
    bytes: u64,
}

#[derive(Debug, Default, Clone, Copy)]
struct StoreWriteStats {
    batches: usize,
    entries: usize,
    bytes: usize,
}

#[derive(Debug, Default, Clone, Copy)]
struct StoreReadStats {
    get_calls: usize,
    get_bytes: usize,
    batch_get_calls: usize,
    batch_get_keys: usize,
    batch_get_bytes: usize,
    batch_get_ordered_calls: usize,
    batch_get_ordered_keys: usize,
    batch_get_ordered_bytes: usize,
    hint_get_calls: usize,
    hint_get_bytes: usize,
}

struct CountingStore<S> {
    inner: S,
    write_batches: AtomicUsize,
    write_entries: AtomicUsize,
    write_bytes: AtomicUsize,
    get_calls: AtomicUsize,
    get_bytes: AtomicUsize,
    batch_get_calls: AtomicUsize,
    batch_get_keys: AtomicUsize,
    batch_get_bytes: AtomicUsize,
    batch_get_ordered_calls: AtomicUsize,
    batch_get_ordered_keys: AtomicUsize,
    batch_get_ordered_bytes: AtomicUsize,
    hint_get_calls: AtomicUsize,
    hint_get_bytes: AtomicUsize,
}

impl<S> CountingStore<S> {
    fn new(inner: S) -> Self {
        Self {
            inner,
            write_batches: AtomicUsize::new(0),
            write_entries: AtomicUsize::new(0),
            write_bytes: AtomicUsize::new(0),
            get_calls: AtomicUsize::new(0),
            get_bytes: AtomicUsize::new(0),
            batch_get_calls: AtomicUsize::new(0),
            batch_get_keys: AtomicUsize::new(0),
            batch_get_bytes: AtomicUsize::new(0),
            batch_get_ordered_calls: AtomicUsize::new(0),
            batch_get_ordered_keys: AtomicUsize::new(0),
            batch_get_ordered_bytes: AtomicUsize::new(0),
            hint_get_calls: AtomicUsize::new(0),
            hint_get_bytes: AtomicUsize::new(0),
        }
    }

    fn reset_stats(&self) {
        self.write_batches.store(0, Ordering::Relaxed);
        self.write_entries.store(0, Ordering::Relaxed);
        self.write_bytes.store(0, Ordering::Relaxed);
        self.get_calls.store(0, Ordering::Relaxed);
        self.get_bytes.store(0, Ordering::Relaxed);
        self.batch_get_calls.store(0, Ordering::Relaxed);
        self.batch_get_keys.store(0, Ordering::Relaxed);
        self.batch_get_bytes.store(0, Ordering::Relaxed);
        self.batch_get_ordered_calls.store(0, Ordering::Relaxed);
        self.batch_get_ordered_keys.store(0, Ordering::Relaxed);
        self.batch_get_ordered_bytes.store(0, Ordering::Relaxed);
        self.hint_get_calls.store(0, Ordering::Relaxed);
        self.hint_get_bytes.store(0, Ordering::Relaxed);
    }

    fn write_stats(&self) -> StoreWriteStats {
        StoreWriteStats {
            batches: self.write_batches.load(Ordering::Relaxed),
            entries: self.write_entries.load(Ordering::Relaxed),
            bytes: self.write_bytes.load(Ordering::Relaxed),
        }
    }

    fn read_stats(&self) -> StoreReadStats {
        StoreReadStats {
            get_calls: self.get_calls.load(Ordering::Relaxed),
            get_bytes: self.get_bytes.load(Ordering::Relaxed),
            batch_get_calls: self.batch_get_calls.load(Ordering::Relaxed),
            batch_get_keys: self.batch_get_keys.load(Ordering::Relaxed),
            batch_get_bytes: self.batch_get_bytes.load(Ordering::Relaxed),
            batch_get_ordered_calls: self.batch_get_ordered_calls.load(Ordering::Relaxed),
            batch_get_ordered_keys: self.batch_get_ordered_keys.load(Ordering::Relaxed),
            batch_get_ordered_bytes: self.batch_get_ordered_bytes.load(Ordering::Relaxed),
            hint_get_calls: self.hint_get_calls.load(Ordering::Relaxed),
            hint_get_bytes: self.hint_get_bytes.load(Ordering::Relaxed),
        }
    }

    fn count_write(&self, entries: usize, bytes: usize) {
        self.write_batches.fetch_add(1, Ordering::Relaxed);
        self.write_entries.fetch_add(entries, Ordering::Relaxed);
        self.write_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl<S: Store> Store for CountingStore<S> {
    type Error = S::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let value = self.inner.get(key)?;
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.get_bytes
            .fetch_add(value.as_ref().map_or(0, Vec::len), Ordering::Relaxed);
        Ok(value)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.count_write(1, value.len());
        self.inner.put(key, value)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key)
    }

    fn batch(&self, ops: &[BatchOp]) -> Result<(), Self::Error> {
        let mut entries = 0usize;
        let mut bytes = 0usize;
        for op in ops {
            if let BatchOp::Upsert { value, .. } = op {
                entries += 1;
                bytes += value.len();
            }
        }
        self.count_write(entries, bytes);
        self.inner.batch(ops)
    }

    fn batch_get(&self, keys: &[&[u8]]) -> Result<HashMap<Vec<u8>, Vec<u8>>, Self::Error> {
        let values = self.inner.batch_get(keys)?;
        self.batch_get_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_get_keys.fetch_add(keys.len(), Ordering::Relaxed);
        self.batch_get_bytes.fetch_add(
            values.values().map(Vec::len).sum::<usize>(),
            Ordering::Relaxed,
        );
        Ok(values)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self.inner.batch_get_ordered(keys)?;
        self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_get_ordered_keys
            .fetch_add(keys.len(), Ordering::Relaxed);
        self.batch_get_ordered_bytes.fetch_add(
            values.iter().flatten().map(Vec::len).sum::<usize>(),
            Ordering::Relaxed,
        );
        Ok(values)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self.inner.batch_get_ordered_unique(keys)?;
        self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_get_ordered_keys
            .fetch_add(keys.len(), Ordering::Relaxed);
        self.batch_get_ordered_bytes.fetch_add(
            values.iter().flatten().map(Vec::len).sum::<usize>(),
            Ordering::Relaxed,
        );
        Ok(values)
    }

    fn prefers_batch_reads(&self) -> bool {
        self.inner.prefers_batch_reads()
    }

    fn batch_put(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
        self.count_write(
            entries.len(),
            entries.iter().map(|(_, value)| value.len()).sum(),
        );
        self.inner.batch_put(entries)
    }

    fn supports_hints(&self) -> bool {
        self.inner.supports_hints()
    }

    fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let value = self.inner.get_hint(namespace, key)?;
        self.hint_get_calls.fetch_add(1, Ordering::Relaxed);
        self.hint_get_bytes
            .fetch_add(value.as_ref().map_or(0, Vec::len), Ordering::Relaxed);
        Ok(value)
    }

    fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        self.count_write(1, value.len());
        self.inner.put_hint(namespace, key, value)
    }

    fn batch_put_with_hint(
        &self,
        entries: &[(&[u8], &[u8])],
        namespace: &[u8],
        key: &[u8],
        value: &[u8],
    ) -> Result<(), Self::Error> {
        self.count_write(
            entries.len() + 1,
            entries.iter().map(|(_, value)| value.len()).sum::<usize>() + value.len(),
        );
        self.inner
            .batch_put_with_hint(entries, namespace, key, value)
    }
}

impl<S: ManifestStore> ManifestStore for CountingStore<S> {
    type Error = S::Error;

    fn get_root(&self, name: &[u8]) -> Result<Option<RootManifest>, Self::Error> {
        self.inner.get_root(name)
    }

    fn put_root(&self, name: &[u8], manifest: &RootManifest) -> Result<(), Self::Error> {
        self.inner.put_root(name, manifest)?;
        self.count_write(1, 0);
        Ok(())
    }

    fn delete_root(&self, name: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete_root(name)
    }

    fn compare_and_swap_root(
        &self,
        name: &[u8],
        expected: Option<&RootManifest>,
        new: Option<&RootManifest>,
    ) -> Result<ManifestUpdate, Self::Error> {
        let update = self.inner.compare_and_swap_root(name, expected, new)?;
        if update.is_applied() {
            self.count_write(usize::from(new.is_some()), 0);
        }
        Ok(update)
    }
}

impl<S: ManifestStoreScan> ManifestStoreScan for CountingStore<S> {
    fn list_roots(&self) -> Result<Vec<NamedRootManifest>, Self::Error> {
        self.inner.list_roots()
    }
}

fn main() {
    let workload = WorkloadConfig::from_env();
    let object_store = build_slatedb_object_store();
    remove_slatedb_prefix(object_store.clone(), &workload.path);

    println!("slatedb production workload bench");
    println!("path={}", workload.path);
    println!("endpoint={}", workload.endpoint);
    println!("bucket={}", workload.bucket);
    println!("stages={:?}", workload.stages);
    println!("batch_size={}", workload.batch_size);
    println!("ops_per_cycle={}", workload.ops_per_cycle);
    println!("cycles_per_stage={}", workload.cycles_per_stage);
    println!("soak_seconds={}", workload.soak_duration.as_secs());
    println!("max_soak_cycles={:?}", workload.max_soak_cycles);
    println!("object_sample_cycles={}", workload.object_sample_cycles);
    println!("value_bytes={}", workload.value_bytes);
    println!("max_seconds={}", workload.max_duration.as_secs());
    println!("max_object_bytes={}", workload.max_object_bytes);
    println!("stats_max_records={}", workload.stats_max_records);
    print_slatedb_settings(&workload.store_config);
    println!(
        "row,stage,target_records,total_records,operation,items,total_ms,items_per_sec,result_count,object_count,object_bytes,bytes_per_record,tree_nodes,tree_leaves,tree_internal_nodes,tree_height,tree_kv_pairs,tree_bytes,avg_node_bytes,avg_entries_per_node,avg_leaf_fill,store_get_calls,store_get_bytes,store_batch_get_calls,store_batch_get_keys,store_batch_get_bytes,store_batch_get_ordered_calls,store_batch_get_ordered_keys,store_batch_get_ordered_bytes,store_hint_get_calls,store_hint_get_bytes,store_write_batches,store_write_entries,store_write_bytes,batch_input_mutations,batch_effective_mutations,batch_affected_leaves,batch_changed_leaves,batch_written_nodes,batch_written_bytes,batch_append_fast_path,batch_batched_route,batch_cache_written_nodes,verified,status"
    );

    let store = Arc::new(CountingStore::new(open_store(
        &workload.path,
        object_store.clone(),
        workload.store_config.clone(),
    )));
    let prolly = Prolly::new(store.clone(), workload.tree_config.clone());
    let writer = BatchWriter::with_config(
        BatchWriterConfig::new().with_cache_written_nodes(workload.cache_written_nodes),
    );
    let mut tree = prolly.create();
    let mut total_records = 0usize;
    let mut deleted = HashSet::new();
    let bench_start = Instant::now();
    let mut last_object_stats = ObjectStats::default();

    for (stage_idx, target) in workload.stages.iter().copied().enumerate() {
        if target <= total_records {
            continue;
        }

        let stage_start_records = total_records;
        let stage_start = Instant::now();
        let mut status = "ok";
        let mut batch_stats = BatchApplyStats::default();
        store.reset_stats();

        while total_records < target {
            let count = (target - total_records).min(workload.batch_size);
            let mutations = append_mutations(total_records, count, workload.value_bytes);
            let result = writer
                .apply_batch_with_stats(&prolly, &tree, mutations)
                .unwrap();
            merge_batch_stats(&mut batch_stats, &result.stats);
            tree = result.tree;
            total_records += count;

            if bench_start.elapsed() >= workload.max_duration {
                status = "hit-max-seconds";
                break;
            }
        }

        last_object_stats = object_stats(object_store.clone(), &workload.path);
        if last_object_stats.bytes >= workload.max_object_bytes {
            status = "hit-max-object-bytes";
        }

        let tree_stats =
            maybe_tree_stats(&prolly, &tree, total_records, workload.stats_max_records);
        print_row(PrintRow {
            row: "stage",
            stage: stage_idx,
            target_records: target,
            total_records,
            operation: "ingest_append_batches".to_string(),
            items: total_records - stage_start_records,
            elapsed: stage_start.elapsed(),
            result_count: total_records,
            object_stats: last_object_stats,
            tree_stats,
            read_stats: store.read_stats(),
            write_stats: store.write_stats(),
            batch_stats,
            verified: total_records >= target || status != "ok",
            status,
        });

        measure_publish_roots(
            &prolly,
            &store,
            &tree,
            stage_idx,
            target,
            total_records,
            last_object_stats,
        );

        for cycle in 0..workload.cycles_per_stage {
            if status != "ok" {
                break;
            }
            let cycle_object_stats = if cycle % workload.object_sample_cycles == 0 {
                last_object_stats = object_stats(object_store.clone(), &workload.path);
                last_object_stats
            } else {
                last_object_stats
            };
            run_cycle(RunCycle {
                prolly: &prolly,
                store: &store,
                writer: &writer,
                tree: &mut tree,
                deleted: &mut deleted,
                total_records: &mut total_records,
                workload: &workload,
                object_stats: cycle_object_stats,
                stage: stage_idx,
                target,
                cycle,
            });

            if bench_start.elapsed() >= workload.max_duration {
                status = "hit-max-seconds";
            }
        }

        last_object_stats = object_stats(object_store.clone(), &workload.path);
        if last_object_stats.bytes >= workload.max_object_bytes {
            status = "hit-max-object-bytes";
        }
        if status != "ok" {
            break;
        }
    }

    if !workload.soak_duration.is_zero() && total_records > 0 {
        let soak_start = Instant::now();
        let soak_stage = workload.stages.len();
        let soak_target = *workload.stages.last().unwrap_or(&total_records);
        let mut soak_cycles = 0usize;
        let mut status = "ok";

        while soak_start.elapsed() < workload.soak_duration {
            if bench_start.elapsed() >= workload.max_duration {
                status = "hit-max-seconds";
                break;
            }
            if let Some(max_cycles) = workload.max_soak_cycles {
                if soak_cycles >= max_cycles {
                    status = "hit-max-soak-cycles";
                    break;
                }
            }

            let cycle_object_stats = if soak_cycles % workload.object_sample_cycles == 0 {
                last_object_stats = object_stats(object_store.clone(), &workload.path);
                last_object_stats
            } else {
                last_object_stats
            };
            if cycle_object_stats.bytes >= workload.max_object_bytes {
                status = "hit-max-object-bytes";
                break;
            }

            run_cycle(RunCycle {
                prolly: &prolly,
                store: &store,
                writer: &writer,
                tree: &mut tree,
                deleted: &mut deleted,
                total_records: &mut total_records,
                workload: &workload,
                object_stats: cycle_object_stats,
                stage: soak_stage,
                target: soak_target,
                cycle: soak_cycles,
            });
            soak_cycles += 1;
        }

        last_object_stats = object_stats(object_store.clone(), &workload.path);
        print_simple_row(
            "soak",
            soak_stage,
            soak_target,
            total_records,
            "soak_summary",
            soak_cycles,
            last_object_stats,
            status != "failed",
            status,
        );
    }

    drop(prolly);
    drop(store);

    let verified = verify_reopen_named_root(
        &workload.path,
        object_store.clone(),
        workload.store_config.clone(),
        b"main",
        total_records,
        &deleted,
    );
    print_simple_row(
        "final",
        workload.stages.len(),
        *workload.stages.last().unwrap_or(&0),
        total_records,
        "reopen_main_sample_get",
        3,
        last_object_stats,
        verified,
        if verified { "ok" } else { "failed" },
    );

    if !workload.keep_db {
        remove_slatedb_prefix(object_store, &workload.path);
    }
}

struct RunCycle<'a, S: Store> {
    prolly: &'a Prolly<Arc<CountingStore<S>>>,
    store: &'a CountingStore<S>,
    writer: &'a BatchWriter,
    tree: &'a mut Tree,
    deleted: &'a mut HashSet<usize>,
    total_records: &'a mut usize,
    workload: &'a WorkloadConfig,
    object_stats: ObjectStats,
    stage: usize,
    target: usize,
    cycle: usize,
}

fn run_cycle<S>(args: RunCycle<'_, S>)
where
    S: Store + ManifestStore,
{
    let RunCycle {
        prolly,
        store,
        writer,
        tree,
        deleted,
        total_records,
        workload,
        object_stats,
        stage,
        target,
        cycle,
    } = args;

    if *total_records == 0 {
        return;
    }

    let ops = workload.ops_per_cycle.min(*total_records).max(1);
    let cycle_key = stage.saturating_mul(1_000_000).saturating_add(cycle);

    measure_store_row(
        prolly,
        store,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_point_get_hot"),
        ops,
        object_stats,
        || {
            let hot_window = (*total_records).min(ops.saturating_mul(16).max(1));
            let mut ok = 0usize;
            for i in 0..ops {
                let idx = i.wrapping_mul(97) % hot_window;
                let value = prolly.get(tree, &key_for_index(idx)).unwrap();
                if value_is_expected(idx, &value, deleted) {
                    ok += 1;
                }
            }
            (ok, ok == ops)
        },
    );

    let random_indices = random_indices_excluding(
        0,
        *total_records,
        ops,
        seed(b"cycle-random-read", cycle_key),
        &HashSet::new(),
    );
    let random_keys = keys_for_indices(&random_indices);
    measure_store_row(
        prolly,
        store,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_point_get_random_many"),
        random_indices.len(),
        object_stats,
        || {
            prolly.clear_cache();
            let values = prolly.get_many(tree, &random_keys).unwrap();
            let ok = values
                .iter()
                .zip(&random_indices)
                .filter(|(value, idx)| value_is_expected(**idx, value, deleted))
                .count();
            (ok, ok == random_indices.len())
        },
    );

    let range_len = ops
        .min(workload.range_scan_records.max(1))
        .min(*total_records);
    let range_start = if *total_records > range_len {
        mix64(seed(b"range", cycle_key)) as usize % (*total_records - range_len)
    } else {
        0
    };
    let range_end = range_start + range_len;
    measure_store_row(
        prolly,
        store,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_range_scan_page"),
        range_len,
        object_stats,
        || {
            let count = prolly
                .range(
                    tree,
                    &key_for_index(range_start),
                    Some(&key_for_index(range_end)),
                )
                .unwrap()
                .try_fold(0usize, |count, entry| entry.map(|_| count + 1))
                .unwrap();
            let deleted_in_range = deleted
                .iter()
                .filter(|idx| **idx >= range_start && **idx < range_end)
                .count();
            (count, count + deleted_in_range == range_len)
        },
    );

    let mut excluded = deleted.clone();
    let update_indices = random_indices_excluding(
        0,
        *total_records,
        ops,
        seed(b"cycle-update", cycle_key),
        &excluded,
    );
    excluded.extend(update_indices.iter().copied());
    let cycle_base = tree.clone();
    let update_result = measure_batch_row(
        prolly,
        store,
        writer,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_batch_update_random"),
        update_indices.len(),
        object_stats,
        tree,
        update_mutations_for_indices(
            &update_indices,
            "cycle-update",
            cycle_key,
            workload.value_bytes,
        ),
    );
    *tree = update_result;
    let verified = verify_labeled_values(
        prolly,
        tree,
        &update_indices,
        "cycle-update",
        cycle_key,
        workload.value_bytes,
    );
    print_verify_row(
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_verify_update_random"),
        update_indices.len(),
        object_stats,
        verified,
    );

    let delete_count = (ops / 10).max(1).min(*total_records);
    let delete_indices = random_indices_excluding(
        0,
        *total_records,
        delete_count,
        seed(b"cycle-delete", cycle_key),
        &excluded,
    );
    let delete_result = measure_batch_row(
        prolly,
        store,
        writer,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_batch_delete_cold"),
        delete_indices.len(),
        object_stats,
        tree,
        delete_mutations_for_indices(&delete_indices),
    );
    *tree = delete_result;
    deleted.extend(delete_indices.iter().copied());
    let verified = delete_indices
        .iter()
        .all(|idx| prolly.get(tree, &key_for_index(*idx)).unwrap().is_none());
    print_verify_row(
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_verify_delete_cold"),
        delete_indices.len(),
        object_stats,
        verified,
    );

    let append_count = ops;
    let append_start = *total_records;
    let append_result = measure_batch_row(
        prolly,
        store,
        writer,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_append_suffix"),
        append_count,
        object_stats,
        tree,
        append_mutations(append_start, append_count, workload.value_bytes),
    );
    *tree = append_result;
    *total_records += append_count;
    let verified = sample_indices(append_count).into_iter().all(|i| {
        let idx = append_start + i;
        prolly.get(tree, &key_for_index(idx)).unwrap().as_deref()
            == Some(value_for_index(idx, workload.value_bytes).as_slice())
    });
    print_verify_row(
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_verify_append_suffix"),
        append_count,
        object_stats,
        verified,
    );

    measure_store_row(
        prolly,
        store,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_stream_diff_cycle"),
        update_indices.len() + delete_indices.len() + append_count,
        object_stats,
        || {
            prolly.clear_cache();
            let diffs = prolly
                .stream_diff(&cycle_base, tree)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (
                count,
                count == update_indices.len() + delete_indices.len() + append_count,
            )
        },
    );

    let branch_base = tree.clone();
    let left_indices = random_indices_excluding(
        0,
        *total_records,
        ops,
        seed(b"cycle-left", cycle_key),
        deleted,
    );
    let mut right_excluded = deleted.clone();
    right_excluded.extend(left_indices.iter().copied());
    let right_indices = random_indices_excluding(
        0,
        *total_records,
        ops,
        seed(b"cycle-right", cycle_key),
        &right_excluded,
    );
    let left = prolly
        .batch(
            &branch_base,
            update_mutations_for_indices(
                &left_indices,
                "cycle-left",
                cycle_key,
                workload.value_bytes,
            ),
        )
        .unwrap();
    let right = prolly
        .batch(
            &branch_base,
            update_mutations_for_indices(
                &right_indices,
                "cycle-right",
                cycle_key,
                workload.value_bytes,
            ),
        )
        .unwrap();
    let merged = measure_tree_row(
        prolly,
        store,
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_merge_disjoint"),
        left_indices.len() + right_indices.len(),
        object_stats,
        || prolly.merge(&branch_base, &left, &right, None).unwrap(),
    );
    let verified = verify_labeled_values(
        prolly,
        &merged,
        &left_indices,
        "cycle-left",
        cycle_key,
        workload.value_bytes,
    ) && verify_labeled_values(
        prolly,
        &merged,
        &right_indices,
        "cycle-right",
        cycle_key,
        workload.value_bytes,
    );
    *tree = merged;
    print_verify_row(
        stage,
        target,
        *total_records,
        format!("cycle_{cycle}_verify_merge_disjoint"),
        left_indices.len() + right_indices.len(),
        object_stats,
        verified,
    );

    measure_publish_roots(
        prolly,
        store,
        tree,
        stage,
        target,
        *total_records,
        object_stats,
    );
}

fn measure_batch_row<S>(
    prolly: &Prolly<Arc<CountingStore<S>>>,
    store: &CountingStore<S>,
    writer: &BatchWriter,
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: String,
    items: usize,
    object_stats: ObjectStats,
    base: &Tree,
    mutations: Vec<Mutation>,
) -> Tree
where
    S: Store,
{
    store.reset_stats();
    let start = Instant::now();
    let result = writer
        .apply_batch_with_stats(prolly, base, mutations)
        .unwrap();
    print_row(PrintRow {
        row: "op",
        stage,
        target_records,
        total_records,
        operation,
        items,
        elapsed: start.elapsed(),
        result_count: items,
        object_stats,
        tree_stats: None,
        read_stats: store.read_stats(),
        write_stats: store.write_stats(),
        batch_stats: result.stats,
        verified: true,
        status: "ok",
    });
    result.tree
}

fn measure_tree_row<S, F>(
    _prolly: &Prolly<Arc<CountingStore<S>>>,
    store: &CountingStore<S>,
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: String,
    items: usize,
    object_stats: ObjectStats,
    f: F,
) -> Tree
where
    S: Store,
    F: FnOnce() -> Tree,
{
    store.reset_stats();
    let start = Instant::now();
    let tree = f();
    print_row(PrintRow {
        row: "op",
        stage,
        target_records,
        total_records,
        operation,
        items,
        elapsed: start.elapsed(),
        result_count: items,
        object_stats,
        tree_stats: None,
        read_stats: store.read_stats(),
        write_stats: store.write_stats(),
        batch_stats: BatchApplyStats::default(),
        verified: true,
        status: "ok",
    });
    tree
}

fn measure_store_row<S, F>(
    _prolly: &Prolly<Arc<CountingStore<S>>>,
    store: &CountingStore<S>,
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: String,
    items: usize,
    object_stats: ObjectStats,
    f: F,
) where
    S: Store,
    F: FnOnce() -> (usize, bool),
{
    store.reset_stats();
    let start = Instant::now();
    let (result_count, verified) = f();
    print_row(PrintRow {
        row: "op",
        stage,
        target_records,
        total_records,
        operation,
        items,
        elapsed: start.elapsed(),
        result_count,
        object_stats,
        tree_stats: None,
        read_stats: store.read_stats(),
        write_stats: store.write_stats(),
        batch_stats: BatchApplyStats::default(),
        verified,
        status: if verified { "ok" } else { "failed" },
    });
}

fn measure_publish_roots<S>(
    prolly: &Prolly<Arc<CountingStore<S>>>,
    store: &CountingStore<S>,
    tree: &Tree,
    stage: usize,
    target_records: usize,
    total_records: usize,
    object_stats: ObjectStats,
) where
    S: Store + ManifestStore,
{
    store.reset_stats();
    let start = Instant::now();
    let main_ok = prolly.publish_named_root(b"main", tree).is_ok();
    let stage_name = format!("stage/{stage:04}/target/{target_records:020}");
    let stage_ok = prolly
        .publish_named_root(stage_name.as_bytes(), tree)
        .is_ok();
    print_row(PrintRow {
        row: "manifest",
        stage,
        target_records,
        total_records,
        operation: "publish_named_roots".to_string(),
        items: 2,
        elapsed: start.elapsed(),
        result_count: usize::from(main_ok) + usize::from(stage_ok),
        object_stats,
        tree_stats: None,
        read_stats: store.read_stats(),
        write_stats: store.write_stats(),
        batch_stats: BatchApplyStats::default(),
        verified: main_ok && stage_ok,
        status: if main_ok && stage_ok { "ok" } else { "failed" },
    });
}

fn print_verify_row(
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: String,
    items: usize,
    object_stats: ObjectStats,
    verified: bool,
) {
    print_row(PrintRow {
        row: "verify",
        stage,
        target_records,
        total_records,
        operation,
        items,
        elapsed: Duration::ZERO,
        result_count: 0,
        object_stats,
        tree_stats: None,
        read_stats: StoreReadStats::default(),
        write_stats: StoreWriteStats::default(),
        batch_stats: BatchApplyStats::default(),
        verified,
        status: if verified { "ok" } else { "failed" },
    });
}

fn print_simple_row(
    row: &str,
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: &str,
    items: usize,
    object_stats: ObjectStats,
    verified: bool,
    status: &str,
) {
    print_row(PrintRow {
        row,
        stage,
        target_records,
        total_records,
        operation: operation.to_string(),
        items,
        elapsed: Duration::ZERO,
        result_count: usize::from(verified),
        object_stats,
        tree_stats: None,
        read_stats: StoreReadStats::default(),
        write_stats: StoreWriteStats::default(),
        batch_stats: BatchApplyStats::default(),
        verified,
        status,
    });
}

struct PrintRow<'a> {
    row: &'a str,
    stage: usize,
    target_records: usize,
    total_records: usize,
    operation: String,
    items: usize,
    elapsed: Duration,
    result_count: usize,
    object_stats: ObjectStats,
    tree_stats: Option<TreeStats>,
    read_stats: StoreReadStats,
    write_stats: StoreWriteStats,
    batch_stats: BatchApplyStats,
    verified: bool,
    status: &'a str,
}

fn print_row(row: PrintRow<'_>) {
    let total_ms = row.elapsed.as_secs_f64() * 1_000.0;
    let items_per_sec = if total_ms > 0.0 {
        row.items as f64 / (total_ms / 1_000.0)
    } else {
        0.0
    };
    let bytes_per_record = if row.total_records == 0 {
        0.0
    } else {
        row.object_stats.bytes as f64 / row.total_records as f64
    };
    let tree = row.tree_stats.unwrap_or_default();

    println!(
        "{},{},{},{},{},{},{total_ms:.3},{items_per_sec:.0},{},{},{},{bytes_per_record:.2},{},{},{},{},{},{},{:.2},{:.2},{:.4},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
        row.row,
        row.stage,
        row.target_records,
        row.total_records,
        row.operation,
        row.items,
        row.result_count,
        row.object_stats.count,
        row.object_stats.bytes,
        tree.num_nodes,
        tree.num_leaves,
        tree.num_internal_nodes,
        tree.tree_height,
        tree.total_key_value_pairs,
        tree.total_tree_size_bytes,
        tree.avg_node_size_bytes,
        tree.avg_entries_per_node,
        tree.avg_leaf_fill_factor,
        row.read_stats.get_calls,
        row.read_stats.get_bytes,
        row.read_stats.batch_get_calls,
        row.read_stats.batch_get_keys,
        row.read_stats.batch_get_bytes,
        row.read_stats.batch_get_ordered_calls,
        row.read_stats.batch_get_ordered_keys,
        row.read_stats.batch_get_ordered_bytes,
        row.read_stats.hint_get_calls,
        row.read_stats.hint_get_bytes,
        row.write_stats.batches,
        row.write_stats.entries,
        row.write_stats.bytes,
        row.batch_stats.input_mutations,
        row.batch_stats.effective_mutations,
        row.batch_stats.affected_leaves,
        row.batch_stats.changed_leaves,
        row.batch_stats.written_nodes,
        row.batch_stats.written_bytes,
        row.batch_stats.used_append_fast_path,
        row.batch_stats.used_batched_route,
        row.batch_stats.cache_written_nodes,
        row.verified,
        row.status
    );
}

fn maybe_tree_stats<S>(
    prolly: &Prolly<Arc<CountingStore<S>>>,
    tree: &Tree,
    total_records: usize,
    stats_max_records: usize,
) -> Option<TreeStats>
where
    S: Store,
{
    if total_records > stats_max_records {
        return None;
    }
    prolly.collect_stats(tree).ok()
}

fn merge_batch_stats(total: &mut BatchApplyStats, next: &BatchApplyStats) {
    total.input_mutations += next.input_mutations;
    total.effective_mutations += next.effective_mutations;
    total.affected_leaves += next.affected_leaves;
    total.changed_leaves += next.changed_leaves;
    total.sparse_leaf_applies += next.sparse_leaf_applies;
    total.written_nodes += next.written_nodes;
    total.written_bytes += next.written_bytes;
    total.preprocess_input_sorted |= next.preprocess_input_sorted;
    total.used_append_fast_path |= next.used_append_fast_path;
    total.used_batched_route |= next.used_batched_route;
    total.used_coalesced_rebuild |= next.used_coalesced_rebuild;
    total.used_deferred_rebalancing |= next.used_deferred_rebalancing;
    total.used_bottom_up_rebuild |= next.used_bottom_up_rebuild;
    total.cache_written_nodes |= next.cache_written_nodes;
}

#[derive(Clone)]
struct WorkloadConfig {
    stages: Vec<usize>,
    batch_size: usize,
    ops_per_cycle: usize,
    cycles_per_stage: usize,
    soak_duration: Duration,
    max_soak_cycles: Option<usize>,
    object_sample_cycles: usize,
    range_scan_records: usize,
    value_bytes: usize,
    max_duration: Duration,
    max_object_bytes: u64,
    stats_max_records: usize,
    keep_db: bool,
    cache_written_nodes: bool,
    path: String,
    endpoint: String,
    bucket: String,
    tree_config: Config,
    store_config: SlateDbStoreConfig,
}

impl WorkloadConfig {
    fn from_env() -> Self {
        let stages = parse_stages(
            &std::env::var("PROLLY_SLATEDB_WORKLOAD_STAGES")
                .unwrap_or_else(|_| DEFAULT_STAGES.to_string()),
        );
        let batch_size = env_count("PROLLY_SLATEDB_WORKLOAD_BATCH").unwrap_or(DEFAULT_BATCH_SIZE);
        let ops_per_cycle =
            env_count("PROLLY_SLATEDB_WORKLOAD_OPS").unwrap_or(DEFAULT_OPS_PER_CYCLE);
        let cycles_per_stage =
            env_count("PROLLY_SLATEDB_WORKLOAD_CYCLES").unwrap_or(DEFAULT_CYCLES_PER_STAGE);
        let soak_duration = Duration::from_secs(
            env_u64("PROLLY_SLATEDB_WORKLOAD_SOAK_SECONDS").unwrap_or_default(),
        );
        let max_soak_cycles = env_count("PROLLY_SLATEDB_WORKLOAD_MAX_SOAK_CYCLES");
        let object_sample_cycles = env_count("PROLLY_SLATEDB_WORKLOAD_OBJECT_SAMPLE_CYCLES")
            .unwrap_or(DEFAULT_OBJECT_SAMPLE_CYCLES)
            .max(1);
        let range_scan_records =
            env_count("PROLLY_SLATEDB_WORKLOAD_RANGE").unwrap_or(ops_per_cycle);
        let value_bytes =
            env_count("PROLLY_SLATEDB_WORKLOAD_VALUE_BYTES").unwrap_or(DEFAULT_VALUE_BYTES);
        let max_duration = Duration::from_secs(
            env_u64("PROLLY_SLATEDB_WORKLOAD_MAX_SECONDS").unwrap_or(DEFAULT_MAX_SECONDS),
        );
        let max_object_bytes = env_u64("PROLLY_SLATEDB_WORKLOAD_MAX_OBJECT_GB")
            .unwrap_or(DEFAULT_MAX_OBJECT_GB)
            * 1024
            * 1024
            * 1024;
        let stats_max_records = env_count("PROLLY_SLATEDB_WORKLOAD_STATS_MAX_RECORDS")
            .unwrap_or(DEFAULT_STATS_MAX_RECORDS);
        let keep_db = env_bool("PROLLY_SLATEDB_WORKLOAD_KEEP_DB").unwrap_or(false);
        let cache_written_nodes =
            env_bool("PROLLY_SLATEDB_WORKLOAD_CACHE_WRITTEN_NODES").unwrap_or(true);
        let endpoint = slatedb_endpoint();
        let bucket = slatedb_bucket();

        Self {
            stages,
            batch_size,
            ops_per_cycle,
            cycles_per_stage,
            soak_duration,
            max_soak_cycles,
            object_sample_cycles,
            range_scan_records,
            value_bytes,
            max_duration,
            max_object_bytes,
            stats_max_records,
            keep_db,
            cache_written_nodes,
            path: db_path(),
            endpoint,
            bucket,
            tree_config: bench_config(),
            store_config: slatedb_store_config(),
        }
    }
}

fn open_store(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: SlateDbStoreConfig,
) -> SlateDbStore {
    SlateDbStore::open_with_config(path.to_string(), object_store, config).unwrap()
}

fn slatedb_store_config() -> SlateDbStoreConfig {
    let mut settings = Settings::default();
    if let Some(ms) = env_u64("PROLLY_SLATEDB_FLUSH_INTERVAL_MS") {
        settings.flush_interval = if ms == 0 {
            None
        } else {
            Some(Duration::from_millis(ms))
        };
    }
    if let Some(mb) = env_count("PROLLY_SLATEDB_L0_SST_MB") {
        settings.l0_sst_size_bytes = mb.saturating_mul(1024 * 1024);
    }
    if let Some(flushes) = env_u64("PROLLY_SLATEDB_MAX_WAL_FLUSHES_BEFORE_L0") {
        settings.max_wal_flushes_before_l0_flush = flushes;
    }
    if let Some(max_ssts) = env_count("PROLLY_SLATEDB_L0_MAX_SSTS") {
        settings.l0_max_ssts = max_ssts;
    }
    if let Some(max_ssts) = env_count("PROLLY_SLATEDB_L0_MAX_SSTS_PER_KEY") {
        settings.l0_max_ssts_per_key = max_ssts;
    }
    if let Some(parallelism) = env_count("PROLLY_SLATEDB_L0_FLUSH_PARALLELISM") {
        settings.l0_flush_parallelism = parallelism.max(1);
    }
    if let Some(mb) = env_count("PROLLY_SLATEDB_MAX_UNFLUSHED_MB") {
        settings.max_unflushed_bytes = mb.saturating_mul(1024 * 1024);
    }
    if let Some(keys) = env_u32("PROLLY_SLATEDB_MIN_FILTER_KEYS") {
        settings.min_filter_keys = keys;
    }
    if let Ok(codec) = std::env::var("PROLLY_SLATEDB_COMPRESSION") {
        let codec = codec.trim().to_ascii_lowercase();
        settings.compression_codec = if codec.is_empty() || codec == "none" || codec == "off" {
            None
        } else {
            Some(CompressionCodec::from_str(&codec).unwrap())
        };
    }
    if let Some(compactions) = env_count("PROLLY_SLATEDB_COMPACTION_CONCURRENCY") {
        if let Some(options) = settings.compactor_options.as_mut() {
            options.max_concurrent_compactions = compactions.max(1);
            if let Some(worker) = options.worker.as_mut() {
                worker.max_concurrent_compactions = compactions.max(1);
            }
        }
    }
    if let Some(subcompactions) = env_count("PROLLY_SLATEDB_COMPACTION_SUBCOMPACTIONS") {
        if let Some(options) = settings.compactor_options.as_mut() {
            if let Some(worker) = options.worker.as_mut() {
                worker.max_subcompactions = subcompactions.max(1);
            }
        }
    }

    SlateDbStoreConfig {
        settings,
        flush_after_write: env_bool("PROLLY_SLATEDB_FLUSH_AFTER_WRITE").unwrap_or(false),
        close_on_drop: true,
        read_parallelism: env_count("PROLLY_SLATEDB_READ_PARALLELISM").unwrap_or(128),
    }
}

fn print_slatedb_settings(config: &SlateDbStoreConfig) {
    let settings = &config.settings;
    println!("flush_after_write={}", config.flush_after_write);
    println!("read_parallelism={}", config.read_parallelism);
    println!("slatedb_flush_interval={:?}", settings.flush_interval);
    println!("slatedb_l0_sst_size_bytes={}", settings.l0_sst_size_bytes);
    println!(
        "slatedb_max_wal_flushes_before_l0={}",
        settings.max_wal_flushes_before_l0_flush
    );
    println!("slatedb_l0_max_ssts={}", settings.l0_max_ssts);
    println!(
        "slatedb_l0_max_ssts_per_key={}",
        settings.l0_max_ssts_per_key
    );
    println!(
        "slatedb_l0_flush_parallelism={}",
        settings.l0_flush_parallelism
    );
    println!(
        "slatedb_max_unflushed_bytes={}",
        settings.max_unflushed_bytes
    );
    println!("slatedb_compression={:?}", settings.compression_codec);
    if let Some(options) = settings.compactor_options.as_ref() {
        println!(
            "slatedb_compaction_concurrency={}",
            options.max_concurrent_compactions
        );
        if let Some(worker) = options.worker.as_ref() {
            println!(
                "slatedb_worker_subcompactions={}",
                worker.max_subcompactions
            );
        }
    }
}

fn build_slatedb_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(
        AmazonS3Builder::new()
            .with_endpoint(slatedb_endpoint().trim_end_matches('/'))
            .with_bucket_name(slatedb_bucket().trim())
            .with_region(slatedb_region().trim())
            .with_access_key_id(slatedb_access_key_id())
            .with_secret_access_key(slatedb_secret_access_key())
            .with_allow_http(env_bool("PROLLY_SLATEDB_ALLOW_HTTP").unwrap_or(true))
            .with_virtual_hosted_style_request(false)
            .build()
            .unwrap(),
    )
}

fn db_path() -> String {
    if let Ok(path) = std::env::var("PROLLY_SLATEDB_WORKLOAD_PATH") {
        return path.trim().trim_matches('/').to_string();
    }

    let prefix = std::env::var("PROLLY_SLATEDB_PATH_PREFIX")
        .unwrap_or_else(|_| "crabdb/prolly-workload".to_string());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}/slatedb-workload-{}-{nanos}",
        prefix.trim().trim_matches('/'),
        std::process::id()
    )
}

fn slatedb_endpoint() -> String {
    std::env::var("PROLLY_SLATEDB_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string())
}

fn slatedb_bucket() -> String {
    std::env::var("PROLLY_SLATEDB_BUCKET").unwrap_or_else(|_| "prolly".to_string())
}

fn slatedb_region() -> String {
    std::env::var("PROLLY_SLATEDB_REGION").unwrap_or_else(|_| "us-east-1".to_string())
}

fn slatedb_access_key_id() -> String {
    std::env::var("PROLLY_SLATEDB_ACCESS_KEY_ID").unwrap_or_else(|_| "crab".to_string())
}

fn slatedb_secret_access_key() -> String {
    std::env::var("PROLLY_SLATEDB_SECRET_ACCESS_KEY").unwrap_or_else(|_| "crab".to_string())
}

fn bench_config() -> Config {
    let min_chunk_size = env_count("PROLLY_SLATEDB_WORKLOAD_MIN_CHUNK_SIZE")
        .unwrap_or(DEFAULT_MIN_CHUNK_SIZE)
        .max(1);
    let max_chunk_size = env_count("PROLLY_SLATEDB_WORKLOAD_MAX_CHUNK_SIZE")
        .unwrap_or(DEFAULT_MAX_CHUNK_SIZE)
        .max(min_chunk_size);
    let chunking_factor = env_count("PROLLY_SLATEDB_WORKLOAD_CHUNKING_FACTOR")
        .unwrap_or(DEFAULT_CHUNKING_FACTOR)
        .clamp(1, u32::MAX as usize) as u32;

    Config::builder()
        .min_chunk_size(min_chunk_size)
        .max_chunk_size(max_chunk_size)
        .chunking_factor(chunking_factor)
        .hash_seed(0xC0DA)
        .build()
}

fn append_mutations(start: usize, count: usize, value_bytes: usize) -> Vec<Mutation> {
    (start..start + count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: value_for_index(i, value_bytes),
        })
        .collect()
}

fn update_mutations_for_indices(
    indices: &[usize],
    label: &str,
    cycle: usize,
    value_bytes: usize,
) -> Vec<Mutation> {
    indices
        .iter()
        .enumerate()
        .map(|(i, &idx)| Mutation::Upsert {
            key: key_for_index(idx),
            val: labeled_value(label, cycle, i, value_bytes),
        })
        .collect()
}

fn delete_mutations_for_indices(indices: &[usize]) -> Vec<Mutation> {
    indices
        .iter()
        .map(|&idx| Mutation::Delete {
            key: key_for_index(idx),
        })
        .collect()
}

fn key_for_index(i: usize) -> Vec<u8> {
    format!("record/{i:020}").into_bytes()
}

fn value_for_index(i: usize, value_bytes: usize) -> Vec<u8> {
    padded_value(format!("value/{i:020}/"), value_bytes, i as u64)
}

fn labeled_value(label: &str, cycle: usize, i: usize, value_bytes: usize) -> Vec<u8> {
    padded_value(
        format!("{label}/{cycle:08}/{i:020}/"),
        value_bytes,
        (cycle as u64) ^ (i as u64),
    )
}

fn padded_value(prefix: String, value_bytes: usize, seed: u64) -> Vec<u8> {
    if value_bytes == 0 {
        return Vec::new();
    }

    let mut value = prefix.into_bytes();
    let mut state = seed;
    while value.len() < value_bytes {
        state = mix64(state.wrapping_add(0x9e37_79b9_7f4a_7c15));
        value.extend_from_slice(format!("{state:016x}").as_bytes());
    }
    value.truncate(value_bytes);
    value
}

fn value_is_expected(idx: usize, value: &Option<Vec<u8>>, deleted: &HashSet<usize>) -> bool {
    if deleted.contains(&idx) {
        value.is_none()
    } else {
        value.is_some()
    }
}

fn verify_labeled_values<S>(
    prolly: &Prolly<Arc<CountingStore<S>>>,
    tree: &Tree,
    indices: &[usize],
    label: &str,
    cycle: usize,
    value_bytes: usize,
) -> bool
where
    S: Store,
{
    sample_indices(indices.len()).into_iter().all(|i| {
        prolly
            .get(tree, &key_for_index(indices[i]))
            .unwrap()
            .as_deref()
            == Some(labeled_value(label, cycle, i, value_bytes).as_slice())
    })
}

fn verify_reopen_named_root(
    path: &str,
    object_store: Arc<dyn ObjectStore>,
    config: SlateDbStoreConfig,
    name: &[u8],
    total_records: usize,
    deleted: &HashSet<usize>,
) -> bool {
    let Ok(store) = SlateDbStore::open_with_config(path.to_string(), object_store, config) else {
        return false;
    };
    let prolly = Prolly::new(store, Config::default());
    let Ok(Some(tree)) = prolly.load_named_root(name) else {
        return false;
    };

    if total_records == 0 {
        return tree.root.is_none();
    }

    sample_indices(total_records).into_iter().all(|idx| {
        let value = prolly.get(&tree, &key_for_index(idx)).unwrap();
        value_is_expected(idx, &value, deleted)
    })
}

fn sample_indices(len: usize) -> Vec<usize> {
    if len == 0 {
        return Vec::new();
    }
    let mut indices = vec![0, len / 2, len - 1];
    indices.sort_unstable();
    indices.dedup();
    indices
}

fn keys_for_indices(indices: &[usize]) -> Vec<Vec<u8>> {
    indices.iter().map(|&idx| key_for_index(idx)).collect()
}

fn random_indices_excluding(
    start: usize,
    end: usize,
    count: usize,
    salt: u64,
    excluded: &HashSet<usize>,
) -> Vec<usize> {
    let len = end.saturating_sub(start);
    if len == 0 || count == 0 {
        return Vec::new();
    }

    let max_count = len.saturating_sub(excluded.len()).min(count);
    let mut state = salt;
    let mut seen = HashSet::with_capacity(max_count);
    let mut indices = Vec::with_capacity(max_count);

    while indices.len() < max_count && seen.len() < len {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let idx = start + (mix64(state) as usize % len);
        if excluded.contains(&idx) || !seen.insert(idx) {
            continue;
        }
        indices.push(idx);
    }

    if indices.len() < max_count {
        for idx in start..end {
            if !excluded.contains(&idx) && seen.insert(idx) {
                indices.push(idx);
                if indices.len() == max_count {
                    break;
                }
            }
        }
    }

    indices
}

fn seed(label: &[u8], cycle: usize) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325 ^ cycle as u64;
    for byte in label {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn parse_stages(raw: &str) -> Vec<usize> {
    let mut stages = raw
        .split(',')
        .filter_map(|part| parse_count(part.trim()))
        .collect::<Vec<_>>();
    stages.sort_unstable();
    stages.dedup();
    stages
}

fn env_count(name: &str) -> Option<usize> {
    parse_count(&std::env::var(name).ok()?)
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.replace('_', "").parse().ok()
}

fn env_u32(name: &str) -> Option<u32> {
    std::env::var(name).ok()?.replace('_', "").parse().ok()
}

fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn parse_count(raw: &str) -> Option<usize> {
    let raw = raw.trim();
    if raw.is_empty() {
        return None;
    }

    let (number, multiplier) = match raw.as_bytes().last().copied() {
        Some(b'k' | b'K') => (&raw[..raw.len() - 1], 1_000u128),
        Some(b'm' | b'M') => (&raw[..raw.len() - 1], 1_000_000u128),
        Some(b'b' | b'B' | b'g' | b'G') => (&raw[..raw.len() - 1], 1_000_000_000u128),
        Some(b't' | b'T') => (&raw[..raw.len() - 1], 1_000_000_000_000u128),
        _ => (raw, 1u128),
    };
    let parsed = number.replace('_', "").parse::<u128>().ok()?;
    parsed
        .checked_mul(multiplier)
        .and_then(|value| usize::try_from(value).ok())
}

fn object_stats(object_store: Arc<dyn ObjectStore>, path: &str) -> ObjectStats {
    let runtime = runtime();
    let prefix = ObjectPath::from(path);
    runtime.block_on(async move {
        let mut stats = ObjectStats::default();
        let mut list = object_store.list(Some(&prefix));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            stats.count += 1;
            stats.bytes += meta.size;
        }
        stats
    })
}

fn remove_slatedb_prefix(object_store: Arc<dyn ObjectStore>, path: &str) {
    let runtime = runtime();
    let prefix = ObjectPath::from(path);
    runtime.block_on(async move {
        let mut locations = Vec::new();
        let mut list = object_store.list(Some(&prefix));
        while let Some(meta) = list.next().await.transpose().unwrap() {
            locations.push(meta.location);
        }
        drop(list);
        for location in locations {
            let _ = object_store.delete(&location).await;
        }
    });
}

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .thread_name("prolly-slatedb-workload")
        .enable_all()
        .build()
        .unwrap()
}
