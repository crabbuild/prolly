use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::StreamExt;
use prolly::{
    append_batch, BatchApplyResult, BatchApplyStats, BatchOp, BatchWriter, BatchWriterConfig,
    Config, Error, Mutation, Prolly, Resolution, Resolver, SlateDbStore, Store, Tree,
};
use slatedb::object_store::aws::AmazonS3Builder;
use slatedb::object_store::path::Path as ObjectPath;
use slatedb::object_store::{ObjectStore, ObjectStoreExt};

const DEFAULT_RECORDS: usize = 1_000_000;
const DEFAULT_CHANGES: usize = 10_000;
const DEFAULT_BUILD_BATCH: usize = 50_000;
const DEFAULT_MIN_CHUNK_SIZE: usize = 64;
const DEFAULT_MAX_CHUNK_SIZE: usize = 512;
const DEFAULT_CHUNKING_FACTOR: usize = 256;

#[derive(Debug, Default)]
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

struct WriteCountingStore<S> {
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

impl<S> WriteCountingStore<S> {
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

    fn count_get(&self, bytes: usize) {
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        self.get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn count_batch_get(&self, keys: usize, bytes: usize) {
        self.batch_get_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_get_keys.fetch_add(keys, Ordering::Relaxed);
        self.batch_get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }

    fn count_batch_get_ordered(&self, keys: usize, bytes: usize) {
        self.batch_get_ordered_calls.fetch_add(1, Ordering::Relaxed);
        self.batch_get_ordered_keys
            .fetch_add(keys, Ordering::Relaxed);
        self.batch_get_ordered_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }

    fn count_hint_get(&self, bytes: usize) {
        self.hint_get_calls.fetch_add(1, Ordering::Relaxed);
        self.hint_get_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
}

impl<S: Store> Store for WriteCountingStore<S> {
    type Error = S::Error;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        let value = self.inner.get(key)?;
        self.count_get(value.as_ref().map_or(0, Vec::len));
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
        self.count_batch_get(keys.len(), values.values().map(Vec::len).sum());
        Ok(values)
    }

    fn batch_get_ordered(&self, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self.inner.batch_get_ordered(keys)?;
        self.count_batch_get_ordered(keys.len(), values.iter().flatten().map(Vec::len).sum());
        Ok(values)
    }

    fn batch_get_ordered_unique(
        &self,
        keys: &[&[u8]],
    ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
        let values = self.inner.batch_get_ordered_unique(keys)?;
        self.count_batch_get_ordered(keys.len(), values.iter().flatten().map(Vec::len).sum());
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
        self.count_hint_get(value.as_ref().map_or(0, Vec::len));
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

fn main() {
    let records = env_usize("PROLLY_SLATEDB_OPS_RECORDS").unwrap_or(DEFAULT_RECORDS);
    let changes = env_usize("PROLLY_SLATEDB_OPS_CHANGES")
        .unwrap_or(DEFAULT_CHANGES)
        .min((records / 4).max(1));
    let build_batch = env_usize("PROLLY_SLATEDB_OPS_BUILD_BATCH").unwrap_or(DEFAULT_BUILD_BATCH);
    let path = db_path();
    let object_store = build_slatedb_object_store();

    remove_slatedb_prefix(object_store.clone(), &path);

    println!("slatedb operation suite bench");
    println!("path={path}");
    println!("endpoint={}", slatedb_endpoint());
    println!("bucket={}", slatedb_bucket());
    println!("records={records}");
    println!("changes={changes}");
    let config = bench_config();
    println!("build_batch={build_batch}");
    println!("min_chunk_size={}", config.min_chunk_size);
    println!("max_chunk_size={}", config.max_chunk_size);
    println!("chunking_factor={}", config.chunking_factor);
    println!(
        "operation,base_records,items,total_ms,items_per_sec,result_count,store_get_calls,store_get_bytes,store_batch_get_calls,store_batch_get_keys,store_batch_get_bytes,store_batch_get_ordered_calls,store_batch_get_ordered_keys,store_batch_get_ordered_bytes,store_hint_get_calls,store_hint_get_bytes,store_write_batches,store_write_entries,store_write_bytes,batch_input_mutations,batch_effective_mutations,batch_preprocess_input_sorted,batch_affected_leaves,batch_changed_leaves,batch_sparse_leaf_applies,batch_written_nodes,batch_written_bytes,batch_used_append_fast_path,batch_used_batched_route,batch_used_coalesced_rebuild,batch_used_deferred_rebalancing,batch_used_bottom_up_rebuild,batch_cache_written_nodes,verified,status"
    );

    let store = Arc::new(WriteCountingStore::new(open_store(
        &path,
        object_store.clone(),
    )));
    let prolly = Prolly::new(store.clone(), config.clone());
    let base = build_base(&prolly, &store, records, build_batch);
    let stats = object_stats(object_store.clone(), &path);
    println!(
        "object_stats_after_build,count={},bytes={}",
        stats.count, stats.bytes
    );

    measure_store_row(&store, "sample_get", records, 3, || {
        let verified = verify_base_samples(&prolly, &base, records);
        (3, verified)
    });

    measure_store_row(&store, "point_get_hot", records, changes, || {
        let mut found = 0usize;
        for i in 0..changes {
            let idx = i * 97 % records;
            if prolly.get(&base, &key_for_index(idx)).unwrap().as_deref()
                == Some(value_for_index(idx).as_slice())
            {
                found += 1;
            }
        }
        (found, found == changes)
    });

    let random_read_indices = random_indices(records, changes, seed(b"read"));
    let random_read_keys = keys_for_indices(&random_read_indices);
    measure_store_row(&store, "point_get_random", records, changes, || {
        let found = random_read_indices
            .iter()
            .filter(|&&idx| {
                prolly.get(&base, &key_for_index(idx)).unwrap().as_deref()
                    == Some(value_for_index(idx).as_slice())
            })
            .count();
        (found, found == changes)
    });
    measure_store_row(&store, "point_get_random_many", records, changes, || {
        prolly.clear_cache();
        let values = prolly.get_many(&base, &random_read_keys).unwrap();
        let found = count_base_values_for_indices(&values, &random_read_indices);
        (found, found == changes)
    });

    let clustered_read_indices = clustered_indices(records, changes, 8, seed(b"cluster-read"));
    measure_store_row(&store, "point_get_clustered", records, changes, || {
        let found = clustered_read_indices
            .iter()
            .filter(|&&idx| {
                prolly.get(&base, &key_for_index(idx)).unwrap().as_deref()
                    == Some(value_for_index(idx).as_slice())
            })
            .count();
        (found, found == changes)
    });

    measure_store_row(&store, "range_scan_window", records, changes, || {
        let start_idx = records / 3;
        let end_idx = start_idx + changes;
        let count = prolly
            .range(
                &base,
                &key_for_index(start_idx),
                Some(&key_for_index(end_idx)),
            )
            .unwrap()
            .try_fold(0usize, |count, entry| entry.map(|_| count + 1))
            .unwrap();
        (count, count == changes)
    });

    measure_store_row(&store, "range_scan_full", records, records, || {
        let count = prolly
            .range(&base, &[], None)
            .unwrap()
            .try_fold(0usize, |count, entry| entry.map(|_| count + 1))
            .unwrap();
        (count, count == records)
    });

    let batch_writer = BatchWriter::new();

    let update_ops = update_mutations(records, changes, "spread-update");
    let batch_updated =
        measure_batch_tree_row(&store, "batch_update_spread", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, update_ops.clone())
                .unwrap()
        });
    let verified = verify_updates(&prolly, &batch_updated, changes, "spread-update");
    print_verify_row("verify_batch_update_spread", records, changes, verified);

    let random_update_indices = random_indices(records, changes, seed(b"update"));
    let random_update_keys = keys_for_indices(&random_update_indices);
    let random_update_ops = update_mutations_for_indices(&random_update_indices, "random-update");
    let random_updated =
        measure_batch_tree_row(&store, "batch_update_random", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, random_update_ops.clone())
                .unwrap()
        });
    measure_store_row(
        &store,
        "point_get_random_updated_default",
        records,
        changes,
        || {
            let mut found = 0usize;
            for (i, &idx) in random_update_indices.iter().enumerate() {
                let expected = format!("random-update-{i:012}");
                if prolly
                    .get(&random_updated, &key_for_index(idx))
                    .unwrap()
                    .as_deref()
                    == Some(expected.as_bytes())
                {
                    found += 1;
                }
            }
            (found, found == changes)
        },
    );
    measure_store_row(
        &store,
        "point_get_random_updated_many_default",
        records,
        changes,
        || {
            prolly.clear_cache();
            let values = prolly
                .get_many(&random_updated, &random_update_keys)
                .unwrap();
            let found =
                count_labeled_values_for_indices(&values, &random_update_indices, "random-update");
            (found, found == changes)
        },
    );
    let verified = verify_updates_for_indices(
        &prolly,
        &random_updated,
        &random_update_indices,
        "random-update",
    );
    print_verify_row("verify_batch_update_random", records, changes, verified);

    let cache_written_writer =
        BatchWriter::with_config(BatchWriterConfig::new().with_cache_written_nodes(true));
    let random_updated_cached = measure_batch_tree_row(
        &store,
        "batch_update_random_cache_written_nodes",
        records,
        changes,
        || {
            cache_written_writer
                .apply_batch_with_stats(&prolly, &base, random_update_ops.clone())
                .unwrap()
        },
    );
    measure_store_row(
        &store,
        "stream_diff_random_update_cache_written_nodes",
        records,
        changes,
        || {
            let diffs = prolly
                .stream_diff(&base, &random_updated_cached)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );
    measure_store_row(
        &store,
        "point_get_random_updated_cache_written_nodes",
        records,
        changes,
        || {
            let mut found = 0usize;
            for (i, &idx) in random_update_indices.iter().enumerate() {
                let expected = format!("random-update-{i:012}");
                if prolly
                    .get(&random_updated_cached, &key_for_index(idx))
                    .unwrap()
                    .as_deref()
                    == Some(expected.as_bytes())
                {
                    found += 1;
                }
            }
            (found, found == changes)
        },
    );
    measure_store_row(
        &store,
        "point_get_random_updated_many_cache_written_nodes",
        records,
        changes,
        || {
            let values = prolly
                .get_many(&random_updated_cached, &random_update_keys)
                .unwrap();
            let found =
                count_labeled_values_for_indices(&values, &random_update_indices, "random-update");
            (found, found == changes)
        },
    );
    let verified = verify_updates_for_indices(
        &prolly,
        &random_updated_cached,
        &random_update_indices,
        "random-update",
    );
    print_verify_row(
        "verify_batch_update_random_cache_written_nodes",
        records,
        changes,
        verified,
    );

    let clustered_update_indices = clustered_indices(records, changes, 8, seed(b"cluster-update"));
    let clustered_update_ops =
        update_mutations_for_indices(&clustered_update_indices, "cluster-update");
    let clustered_updated =
        measure_batch_tree_row(&store, "batch_update_clustered", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, clustered_update_ops.clone())
                .unwrap()
        });
    let verified = verify_updates_for_indices(
        &prolly,
        &clustered_updated,
        &clustered_update_indices,
        "cluster-update",
    );
    print_verify_row("verify_batch_update_clustered", records, changes, verified);

    let delete_ops = delete_mutations(records, changes);
    let batch_deleted =
        measure_batch_tree_row(&store, "batch_delete_spread", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, delete_ops.clone())
                .unwrap()
        });
    let verified = verify_deletes(&prolly, &batch_deleted, changes);
    print_verify_row("verify_batch_delete_spread", records, changes, verified);

    let random_delete_indices = random_indices(records, changes, seed(b"delete"));
    let random_delete_ops = delete_mutations_for_indices(&random_delete_indices);
    let random_deleted =
        measure_batch_tree_row(&store, "batch_delete_random", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, random_delete_ops.clone())
                .unwrap()
        });
    let verified = verify_deletes_for_indices(&prolly, &random_deleted, &random_delete_indices);
    print_verify_row("verify_batch_delete_random", records, changes, verified);

    let clustered_delete_indices = clustered_indices(records, changes, 8, seed(b"cluster-delete"));
    let clustered_delete_ops = delete_mutations_for_indices(&clustered_delete_indices);
    let clustered_deleted =
        measure_batch_tree_row(&store, "batch_delete_clustered", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, clustered_delete_ops.clone())
                .unwrap()
        });
    let verified =
        verify_deletes_for_indices(&prolly, &clustered_deleted, &clustered_delete_indices);
    print_verify_row("verify_batch_delete_clustered", records, changes, verified);

    let mixed_ops = mixed_mutations(records, changes, "mixed");
    let mixed = measure_batch_tree_row(&store, "batch_mixed_spread", records, changes, || {
        batch_writer
            .apply_batch_with_stats(&prolly, &base, mixed_ops.clone())
            .unwrap()
    });
    let verified = verify_mixed(&prolly, &mixed, records, changes, "mixed");
    print_verify_row("verify_batch_mixed_spread", records, changes, verified);

    let random_mixed_indices = random_indices(records, changes, seed(b"mixed"));
    let random_mixed_ops = mixed_mutations_for_indices(&random_mixed_indices, "random-mixed");
    let random_mixed =
        measure_batch_tree_row(&store, "batch_mixed_random", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, random_mixed_ops.clone())
                .unwrap()
        });
    let verified = verify_mixed_for_indices(
        &prolly,
        &random_mixed,
        &random_mixed_indices,
        "random-mixed",
    );
    print_verify_row("verify_batch_mixed_random", records, changes, verified);

    let clustered_mixed_indices = clustered_indices(records, changes, 8, seed(b"cluster-mixed"));
    let clustered_mixed_ops =
        mixed_mutations_for_indices(&clustered_mixed_indices, "cluster-mixed");
    let clustered_mixed =
        measure_batch_tree_row(&store, "batch_mixed_clustered", records, changes, || {
            batch_writer
                .apply_batch_with_stats(&prolly, &base, clustered_mixed_ops.clone())
                .unwrap()
        });
    let verified = verify_mixed_for_indices(
        &prolly,
        &clustered_mixed,
        &clustered_mixed_indices,
        "cluster-mixed",
    );
    print_verify_row("verify_batch_mixed_clustered", records, changes, verified);

    let append_ops = append_mutations(records, changes, "append");
    let appended = measure_batch_tree_row(&store, "append_suffix", records, changes, || {
        batch_writer
            .apply_batch_with_stats(&prolly, &base, append_ops.clone())
            .unwrap()
    });
    let verified = verify_appends(&prolly, &appended, records, changes, "append");
    print_verify_row("verify_append_suffix", records, changes, verified);

    measure_store_row(&store, "diff_identical", records, 1, || {
        let diffs = prolly.diff(&base, &base).unwrap();
        let count = diffs.len();
        (count, count == 0)
    });

    measure_store_row(&store, "diff_sparse_update", records, changes, || {
        let diffs = prolly.diff(&base, &batch_updated).unwrap();
        let count = diffs.len();
        (count, count == changes)
    });

    measure_store_row(&store, "diff_random_update", records, changes, || {
        let diffs = prolly.diff(&base, &random_updated).unwrap();
        let count = diffs.len();
        (count, count == changes)
    });

    measure_store_row(&store, "diff_clustered_update", records, changes, || {
        let diffs = prolly.diff(&base, &clustered_updated).unwrap();
        let count = diffs.len();
        (count, count == changes)
    });

    measure_store_row(
        &store,
        "stream_diff_sparse_update",
        records,
        changes,
        || {
            let diffs = prolly
                .stream_diff(&base, &batch_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    measure_store_row(
        &store,
        "stream_diff_random_update",
        records,
        changes,
        || {
            let diffs = prolly
                .stream_diff(&base, &random_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    measure_store_row(
        &store,
        "stream_diff_clustered_update",
        records,
        changes,
        || {
            let diffs = prolly
                .stream_diff(&base, &clustered_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    measure_store_row(&store, "diff_append_suffix", records, changes, || {
        let diffs = prolly.diff(&base, &appended).unwrap();
        let count = diffs.len();
        (count, count == changes)
    });

    let start_idx = records / 3;
    let end_idx = start_idx + changes;
    let range_changed = prolly
        .batch(
            &base,
            range_changed_mutations(start_idx, end_idx, "range-change"),
        )
        .unwrap();
    measure_store_row(&store, "range_diff_window", records, changes, || {
        let diffs = prolly
            .range_diff(
                &base,
                &range_changed,
                &key_for_index(start_idx),
                Some(&key_for_index(end_idx)),
            )
            .unwrap();
        let count = diffs.len();
        (count, count > 0)
    });

    let cluster_window_start = records / 2;
    let cluster_window_len = changes.min(records - cluster_window_start);
    let cluster_window_end = cluster_window_start + cluster_window_len;
    let cluster_window_indices: Vec<_> = (cluster_window_start..cluster_window_end).collect();
    let cluster_window_changed = prolly
        .batch(
            &base,
            update_mutations_for_indices(&cluster_window_indices, "cluster-window"),
        )
        .unwrap();
    measure_store_row(
        &store,
        "range_diff_clustered_window",
        records,
        cluster_window_len,
        || {
            let diffs = prolly
                .range_diff(
                    &base,
                    &cluster_window_changed,
                    &key_for_index(cluster_window_start),
                    Some(&key_for_index(cluster_window_end)),
                )
                .unwrap();
            let count = diffs.len();
            (count, count == cluster_window_len)
        },
    );

    measure_store_row(&store, "diff_empty_to_full", records, records, || {
        let empty = prolly.create();
        let diffs = prolly.diff(&empty, &base).unwrap();
        let count = diffs.len();
        (count, count == records)
    });

    let left_indices = spread_indices_in_range(0, records / 2, changes, 7);
    let right_indices = spread_indices_in_range(records / 2, records, changes, 7);
    let left = prolly
        .batch(&base, update_mutations_for_indices(&left_indices, "left"))
        .unwrap();
    let right = prolly
        .batch(&base, update_mutations_for_indices(&right_indices, "right"))
        .unwrap();
    let merged = measure_tree_row(
        &store,
        "merge_spread_disjoint",
        records,
        changes * 2,
        || prolly.merge(&base, &left, &right, None).unwrap(),
    );
    let verified = verify_updates_for_indices(&prolly, &merged, &left_indices, "left")
        && verify_updates_for_indices(&prolly, &merged, &right_indices, "right");
    print_verify_row(
        "verify_merge_spread_disjoint",
        records,
        changes * 2,
        verified,
    );

    let random_left_indices = random_indices_in_range(0, records / 2, changes, seed(b"left"));
    let random_right_indices =
        random_indices_in_range(records / 2, records, changes, seed(b"right"));
    let random_left = prolly
        .batch(
            &base,
            update_mutations_for_indices(&random_left_indices, "random-left"),
        )
        .unwrap();
    let random_right = prolly
        .batch(
            &base,
            update_mutations_for_indices(&random_right_indices, "random-right"),
        )
        .unwrap();
    let random_merged = measure_tree_row(
        &store,
        "merge_random_disjoint",
        records,
        changes * 2,
        || {
            prolly
                .merge(&base, &random_left, &random_right, None)
                .unwrap()
        },
    );
    let verified =
        verify_updates_for_indices(&prolly, &random_merged, &random_left_indices, "random-left")
            && verify_updates_for_indices(
                &prolly,
                &random_merged,
                &random_right_indices,
                "random-right",
            );
    print_verify_row(
        "verify_merge_random_disjoint",
        records,
        changes * 2,
        verified,
    );

    let clustered_left_indices =
        clustered_indices_in_range(0, records / 3, changes, 4, seed(b"cluster-left"));
    let clustered_right_indices =
        clustered_indices_in_range(records * 2 / 3, records, changes, 4, seed(b"cluster-right"));
    let clustered_left = prolly
        .batch(
            &base,
            update_mutations_for_indices(&clustered_left_indices, "cluster-left"),
        )
        .unwrap();
    let clustered_right = prolly
        .batch(
            &base,
            update_mutations_for_indices(&clustered_right_indices, "cluster-right"),
        )
        .unwrap();
    let clustered_merged = measure_tree_row(
        &store,
        "merge_clustered_disjoint",
        records,
        changes * 2,
        || {
            prolly
                .merge(&base, &clustered_left, &clustered_right, None)
                .unwrap()
        },
    );
    let verified = verify_updates_for_indices(
        &prolly,
        &clustered_merged,
        &clustered_left_indices,
        "cluster-left",
    ) && verify_updates_for_indices(
        &prolly,
        &clustered_merged,
        &clustered_right_indices,
        "cluster-right",
    );
    print_verify_row(
        "verify_merge_clustered_disjoint",
        records,
        changes * 2,
        verified,
    );

    let conflict_count = changes.min(128);
    let conflict_left = prolly
        .batch(&base, conflict_mutations(conflict_count, "left-conflict"))
        .unwrap();
    let conflict_right = prolly
        .batch(&base, conflict_mutations(conflict_count, "right-conflict"))
        .unwrap();
    measure_store_row(
        &store,
        "merge_conflict_detect",
        records,
        conflict_count,
        || {
            let detected = matches!(
                prolly.merge(&base, &conflict_left, &conflict_right, None),
                Err(Error::Conflict(_))
            );
            (1, detected)
        },
    );
    let resolver: Resolver =
        Box::new(|conflict| Resolution::value(conflict.right.clone().expect("right value")));
    let resolved = measure_tree_row(
        &store,
        "merge_conflict_resolved",
        records,
        conflict_count,
        || {
            prolly
                .merge(&base, &conflict_left, &conflict_right, Some(resolver))
                .unwrap()
        },
    );
    let verified = verify_conflict_resolution(&prolly, &resolved, conflict_count);
    print_verify_row(
        "verify_merge_conflict_resolved",
        records,
        conflict_count,
        verified,
    );

    measure_store_row(
        &store,
        "stream_diff_sparse_update_cold",
        records,
        changes,
        || {
            prolly.clear_cache();
            let diffs = prolly
                .stream_diff(&base, &batch_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    measure_store_row(
        &store,
        "stream_diff_random_update_cold",
        records,
        changes,
        || {
            prolly.clear_cache();
            let diffs = prolly
                .stream_diff(&base, &random_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    measure_store_row(
        &store,
        "stream_diff_clustered_update_cold",
        records,
        changes,
        || {
            prolly.clear_cache();
            let diffs = prolly
                .stream_diff(&base, &clustered_updated)
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap();
            let count = diffs.len();
            (count, count == changes)
        },
    );

    let stats = object_stats(object_store.clone(), &path);
    println!(
        "object_stats_after_suite,count={},bytes={}",
        stats.count, stats.bytes
    );

    drop(prolly);
    drop(store);

    measure_row("reopen_final_sample_get", records, 3, || {
        let reopened = Prolly::new(
            Arc::new(open_store(&path, object_store.clone())),
            config.clone(),
        );
        let verified = verify_base_samples(&reopened, &base, records);
        (3, verified)
    });

    remove_slatedb_prefix(object_store, &path);
}

fn build_base<S>(
    prolly: &Prolly<Arc<WriteCountingStore<S>>>,
    store: &WriteCountingStore<S>,
    records: usize,
    batch_size: usize,
) -> Tree
where
    S: Store,
{
    let mut tree = prolly.create();
    store.reset_stats();
    let start = Instant::now();
    let mut written = 0usize;
    while written < records {
        let count = (records - written).min(batch_size);
        tree = append_batch(prolly, &tree, base_mutations(written, count)).unwrap();
        written += count;
    }
    let read_stats = store.read_stats();
    let write_stats = store.write_stats();
    print_row(PrintRow {
        operation: "build_base_append_batches",
        records,
        items: records,
        elapsed: start.elapsed(),
        result_count: written,
        read_stats,
        write_stats,
        batch_stats: BatchApplyStats::default(),
        verified: written == records,
        status: "ok",
    });
    tree
}

fn measure_tree_row<S, F>(
    store: &WriteCountingStore<S>,
    operation: &str,
    records: usize,
    items: usize,
    f: F,
) -> Tree
where
    S: Store,
    F: FnOnce() -> Tree,
{
    store.reset_stats();
    let start = Instant::now();
    let tree = f();
    let read_stats = store.read_stats();
    let write_stats = store.write_stats();
    print_row(PrintRow {
        operation,
        records,
        items,
        elapsed: start.elapsed(),
        result_count: items,
        read_stats,
        write_stats,
        batch_stats: BatchApplyStats::default(),
        verified: true,
        status: "ok",
    });
    tree
}

fn measure_batch_tree_row<S, F>(
    store: &WriteCountingStore<S>,
    operation: &str,
    records: usize,
    items: usize,
    f: F,
) -> Tree
where
    S: Store,
    F: FnOnce() -> BatchApplyResult,
{
    store.reset_stats();
    let start = Instant::now();
    let result = f();
    let read_stats = store.read_stats();
    let write_stats = store.write_stats();
    print_row(PrintRow {
        operation,
        records,
        items,
        elapsed: start.elapsed(),
        result_count: items,
        read_stats,
        write_stats,
        batch_stats: result.stats,
        verified: true,
        status: "ok",
    });
    result.tree
}

fn measure_store_row<S, F>(
    store: &WriteCountingStore<S>,
    operation: &str,
    records: usize,
    items: usize,
    f: F,
) where
    S: Store,
    F: FnOnce() -> (usize, bool),
{
    store.reset_stats();
    let start = Instant::now();
    let (result_count, verified) = f();
    let read_stats = store.read_stats();
    let write_stats = store.write_stats();
    print_row(PrintRow {
        operation,
        records,
        items,
        elapsed: start.elapsed(),
        result_count,
        read_stats,
        write_stats,
        batch_stats: BatchApplyStats::default(),
        verified,
        status: "ok",
    });
}

fn measure_row<F>(operation: &str, records: usize, items: usize, f: F)
where
    F: FnOnce() -> (usize, bool),
{
    let start = Instant::now();
    let (result_count, verified) = f();
    print_row(PrintRow {
        operation,
        records,
        items,
        elapsed: start.elapsed(),
        result_count,
        read_stats: StoreReadStats::default(),
        write_stats: StoreWriteStats::default(),
        batch_stats: BatchApplyStats::default(),
        verified,
        status: "ok",
    });
}

fn print_verify_row(operation: &str, records: usize, items: usize, verified: bool) {
    print_row(PrintRow {
        operation,
        records,
        items,
        elapsed: Duration::ZERO,
        result_count: 0,
        read_stats: StoreReadStats::default(),
        write_stats: StoreWriteStats::default(),
        batch_stats: BatchApplyStats::default(),
        verified,
        status: "ok",
    });
}

struct PrintRow<'a> {
    operation: &'a str,
    records: usize,
    items: usize,
    elapsed: Duration,
    result_count: usize,
    read_stats: StoreReadStats,
    write_stats: StoreWriteStats,
    batch_stats: BatchApplyStats,
    verified: bool,
    status: &'a str,
}

fn print_row(row: PrintRow<'_>) {
    let PrintRow {
        operation,
        records,
        items,
        elapsed,
        result_count,
        read_stats,
        write_stats,
        batch_stats,
        verified,
        status,
    } = row;
    let total_ms = elapsed.as_secs_f64() * 1_000.0;
    let items_per_sec = if total_ms > 0.0 {
        items as f64 / (total_ms / 1_000.0)
    } else {
        0.0
    };
    println!(
        "{operation},{records},{items},{total_ms:.3},{items_per_sec:.0},{result_count},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{verified},{status}",
        read_stats.get_calls,
        read_stats.get_bytes,
        read_stats.batch_get_calls,
        read_stats.batch_get_keys,
        read_stats.batch_get_bytes,
        read_stats.batch_get_ordered_calls,
        read_stats.batch_get_ordered_keys,
        read_stats.batch_get_ordered_bytes,
        read_stats.hint_get_calls,
        read_stats.hint_get_bytes,
        write_stats.batches,
        write_stats.entries,
        write_stats.bytes,
        batch_stats.input_mutations,
        batch_stats.effective_mutations,
        batch_stats.preprocess_input_sorted,
        batch_stats.affected_leaves,
        batch_stats.changed_leaves,
        batch_stats.sparse_leaf_applies,
        batch_stats.written_nodes,
        batch_stats.written_bytes,
        batch_stats.used_append_fast_path,
        batch_stats.used_batched_route,
        batch_stats.used_coalesced_rebuild,
        batch_stats.used_deferred_rebalancing,
        batch_stats.used_bottom_up_rebuild,
        batch_stats.cache_written_nodes
    );
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}

fn db_path() -> String {
    if let Ok(path) = std::env::var("PROLLY_SLATEDB_OPS_PATH") {
        return path.trim().trim_matches('/').to_string();
    }

    let prefix = std::env::var("PROLLY_SLATEDB_PATH_PREFIX")
        .unwrap_or_else(|_| "crabdb/prolly-bench".to_string());
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!(
        "{}/slatedb-ops-{}-{nanos}",
        prefix.trim().trim_matches('/'),
        std::process::id()
    )
}

fn open_store(path: &str, object_store: Arc<dyn ObjectStore>) -> SlateDbStore {
    SlateDbStore::open(path.to_string(), object_store).unwrap()
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

fn env_bool(name: &str) -> Option<bool> {
    match std::env::var(name).ok()?.to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Some(true),
        "false" | "0" | "no" | "off" => Some(false),
        _ => None,
    }
}

fn slatedb_endpoint() -> String {
    std::env::var("PROLLY_SLATEDB_ENDPOINT").unwrap_or_else(|_| "http://localhost:9000".to_string())
}

fn slatedb_bucket() -> String {
    std::env::var("PROLLY_SLATEDB_BUCKET").unwrap_or_else(|_| "crab".to_string())
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

fn base_mutations(start: usize, count: usize) -> Vec<Mutation> {
    (start..start + count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: value_for_index(i),
        })
        .collect()
}

fn update_mutations(records: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i * 7 % records),
            val: format!("{label}-{i:012}").into_bytes(),
        })
        .collect()
}

fn update_mutations_for_indices(indices: &[usize], label: &str) -> Vec<Mutation> {
    indices
        .iter()
        .enumerate()
        .map(|(i, &idx)| Mutation::Upsert {
            key: key_for_index(idx),
            val: format!("{label}-{i:012}").into_bytes(),
        })
        .collect()
}

fn delete_mutations(records: usize, count: usize) -> Vec<Mutation> {
    (0..count)
        .map(|i| Mutation::Delete {
            key: key_for_index(i * 7 % records),
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

fn mixed_mutations(records: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| match i % 5 {
            0 => Mutation::Delete {
                key: key_for_index(i * 11 % records),
            },
            1 | 2 => Mutation::Upsert {
                key: key_for_index(i * 7 % records),
                val: format!("{label}-update-{i:012}").into_bytes(),
            },
            _ => Mutation::Upsert {
                key: format!("key-new-{label}-{i:012}").into_bytes(),
                val: format!("{label}-insert-{i:012}").into_bytes(),
            },
        })
        .collect()
}

fn mixed_mutations_for_indices(indices: &[usize], label: &str) -> Vec<Mutation> {
    indices
        .iter()
        .enumerate()
        .map(|(i, &idx)| match i % 5 {
            0 => Mutation::Delete {
                key: key_for_index(idx),
            },
            1 | 2 => Mutation::Upsert {
                key: key_for_index(idx),
                val: format!("{label}-update-{i:012}").into_bytes(),
            },
            _ => Mutation::Upsert {
                key: format!("key-new-{label}-{i:012}").into_bytes(),
                val: format!("{label}-insert-{i:012}").into_bytes(),
            },
        })
        .collect()
}

fn append_mutations(records: usize, count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(records + i),
            val: format!("{label}-{i:012}").into_bytes(),
        })
        .collect()
}

fn conflict_mutations(count: usize, label: &str) -> Vec<Mutation> {
    (0..count)
        .map(|i| Mutation::Upsert {
            key: key_for_index(i),
            val: format!("{label}-{i:012}").into_bytes(),
        })
        .collect()
}

fn range_changed_mutations(start_idx: usize, end_idx: usize, label: &str) -> Vec<Mutation> {
    (start_idx..end_idx)
        .enumerate()
        .filter_map(|(offset, i)| {
            let key = key_for_index(i);
            if offset % 7 == 0 {
                Some(Mutation::Delete { key })
            } else if offset % 3 == 0 {
                Some(Mutation::Upsert {
                    key,
                    val: format!("{label}-{i:012}").into_bytes(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn verify_base_samples<S: prolly::Store>(prolly: &Prolly<S>, tree: &Tree, records: usize) -> bool {
    sample_indices(records).into_iter().all(|idx| {
        prolly.get(tree, &key_for_index(idx)).unwrap().as_deref()
            == Some(value_for_index(idx).as_slice())
    })
}

fn verify_updates<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    count: usize,
    label: &str,
) -> bool {
    sample_indices(count).into_iter().all(|i| {
        prolly.get(tree, &key_for_index(i * 7)).unwrap().as_deref()
            == Some(format!("{label}-{i:012}").as_bytes())
    })
}

fn verify_updates_for_indices<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    indices: &[usize],
    label: &str,
) -> bool {
    sample_indices(indices.len()).into_iter().all(|i| {
        prolly
            .get(tree, &key_for_index(indices[i]))
            .unwrap()
            .as_deref()
            == Some(format!("{label}-{i:012}").as_bytes())
    })
}

fn keys_for_indices(indices: &[usize]) -> Vec<Vec<u8>> {
    indices.iter().map(|&idx| key_for_index(idx)).collect()
}

fn count_base_values_for_indices(values: &[Option<Vec<u8>>], indices: &[usize]) -> usize {
    values
        .iter()
        .zip(indices)
        .filter(|(value, idx)| value.as_deref() == Some(value_for_index(**idx).as_slice()))
        .count()
}

fn count_labeled_values_for_indices(
    values: &[Option<Vec<u8>>],
    indices: &[usize],
    label: &str,
) -> usize {
    let mut found = 0usize;
    for (i, value) in values.iter().take(indices.len()).enumerate() {
        let expected = format!("{label}-{i:012}");
        if value.as_deref() == Some(expected.as_bytes()) {
            found += 1;
        }
    }
    found
}

fn verify_deletes<S: prolly::Store>(prolly: &Prolly<S>, tree: &Tree, count: usize) -> bool {
    sample_indices(count)
        .into_iter()
        .all(|i| prolly.get(tree, &key_for_index(i * 7)).unwrap().is_none())
}

fn verify_deletes_for_indices<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    indices: &[usize],
) -> bool {
    sample_indices(indices.len()).into_iter().all(|i| {
        prolly
            .get(tree, &key_for_index(indices[i]))
            .unwrap()
            .is_none()
    })
}

fn verify_mixed<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    records: usize,
    count: usize,
    label: &str,
) -> bool {
    let insert_idx = (0..count).find(|i| i % 5 >= 3).unwrap();
    let update_idx = (0..count).find(|i| i % 5 == 1).unwrap();
    let delete_idx = (0..count).find(|i| i % 5 == 0).unwrap();
    prolly
        .get(tree, format!("key-new-{label}-{insert_idx:012}").as_bytes())
        .unwrap()
        .as_deref()
        == Some(format!("{label}-insert-{insert_idx:012}").as_bytes())
        && prolly
            .get(tree, &key_for_index(update_idx * 7 % records))
            .unwrap()
            .as_deref()
            == Some(format!("{label}-update-{update_idx:012}").as_bytes())
        && prolly
            .get(tree, &key_for_index(delete_idx * 11 % records))
            .unwrap()
            .is_none()
}

fn verify_mixed_for_indices<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    indices: &[usize],
    label: &str,
) -> bool {
    let insert_idx = (0..indices.len()).find(|i| i % 5 >= 3).unwrap();
    let update_idx = (0..indices.len()).find(|i| i % 5 == 1).unwrap();
    let delete_idx = (0..indices.len()).find(|i| i % 5 == 0).unwrap();
    prolly
        .get(tree, format!("key-new-{label}-{insert_idx:012}").as_bytes())
        .unwrap()
        .as_deref()
        == Some(format!("{label}-insert-{insert_idx:012}").as_bytes())
        && prolly
            .get(tree, &key_for_index(indices[update_idx]))
            .unwrap()
            .as_deref()
            == Some(format!("{label}-update-{update_idx:012}").as_bytes())
        && prolly
            .get(tree, &key_for_index(indices[delete_idx]))
            .unwrap()
            .is_none()
}

fn verify_appends<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    records: usize,
    count: usize,
    label: &str,
) -> bool {
    sample_indices(count).into_iter().all(|i| {
        prolly
            .get(tree, &key_for_index(records + i))
            .unwrap()
            .as_deref()
            == Some(format!("{label}-{i:012}").as_bytes())
    })
}

fn verify_conflict_resolution<S: prolly::Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    count: usize,
) -> bool {
    sample_indices(count).into_iter().all(|i| {
        prolly.get(tree, &key_for_index(i)).unwrap().as_deref()
            == Some(format!("right-conflict-{i:012}").as_bytes())
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

fn seed(label: &[u8]) -> u64 {
    let mut value = 0xcbf2_9ce4_8422_2325;
    for byte in label {
        value ^= u64::from(*byte);
        value = value.wrapping_mul(0x0000_0100_0000_01b3);
    }
    value
}

fn random_indices(records: usize, count: usize, salt: u64) -> Vec<usize> {
    random_indices_in_range(0, records, count, salt)
}

fn random_indices_in_range(start: usize, end: usize, count: usize, salt: u64) -> Vec<usize> {
    let len = end.saturating_sub(start);
    if len == 0 || count == 0 {
        return Vec::new();
    }

    let count = count.min(len);
    let mut state = salt;
    let mut seen = HashSet::with_capacity(count);
    let mut indices = Vec::with_capacity(count);
    while indices.len() < count {
        state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let idx = start + (mix64(state) as usize % len);
        if seen.insert(idx) {
            indices.push(idx);
        }
    }
    indices
}

fn spread_indices_in_range(start: usize, end: usize, count: usize, stride: usize) -> Vec<usize> {
    let len = end.saturating_sub(start);
    if len == 0 || count == 0 {
        return Vec::new();
    }

    let count = count.min(len);
    let stride = stride.max(1);
    let mut seen = HashSet::with_capacity(count);
    let mut indices = Vec::with_capacity(count);
    for i in 0..len {
        let idx = start + (i * stride % len);
        if seen.insert(idx) {
            indices.push(idx);
            if indices.len() == count {
                return indices;
            }
        }
    }
    for i in 0..len {
        let idx = start + i;
        if seen.insert(idx) {
            indices.push(idx);
            if indices.len() == count {
                break;
            }
        }
    }
    indices
}

fn clustered_indices(records: usize, count: usize, clusters: usize, salt: u64) -> Vec<usize> {
    clustered_indices_in_range(0, records, count, clusters, salt)
}

fn clustered_indices_in_range(
    start: usize,
    end: usize,
    count: usize,
    clusters: usize,
    salt: u64,
) -> Vec<usize> {
    let len = end.saturating_sub(start);
    if len == 0 || count == 0 {
        return Vec::new();
    }

    let count = count.min(len);
    let clusters = clusters.max(1).min(count);
    let per_cluster = count.div_ceil(clusters);
    let span = (per_cluster * 2).min(len).max(1);
    let mut seen = HashSet::with_capacity(count);
    let mut indices = Vec::with_capacity(count);

    for cluster in 0..clusters {
        let max_anchor = len.saturating_sub(span);
        let anchor = if max_anchor == 0 {
            0
        } else {
            mix64(salt.wrapping_add(cluster as u64)) as usize % (max_anchor + 1)
        };
        for offset in 0..span {
            let idx = start + anchor + offset;
            if seen.insert(idx) {
                indices.push(idx);
                if indices.len() == count {
                    return indices;
                }
            }
        }
    }

    let mut fill = random_indices_in_range(start, end, count, salt ^ 0xa5a5_a5a5_a5a5_a5a5);
    fill.retain(|idx| seen.insert(*idx));
    indices.extend(fill.into_iter().take(count - indices.len()));

    if indices.len() < count {
        for idx in start..end {
            if seen.insert(idx) {
                indices.push(idx);
                if indices.len() == count {
                    break;
                }
            }
        }
    }

    indices
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn key_for_index(i: usize) -> Vec<u8> {
    format!("key-{i:012}").into_bytes()
}

fn value_for_index(i: usize) -> Vec<u8> {
    format!("value-{i:012}-payload").into_bytes()
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
        .thread_name("prolly-slatedb-ops-bench")
        .enable_all()
        .build()
        .unwrap()
}

fn bench_config() -> Config {
    let min_chunk_size = env_usize("PROLLY_SLATEDB_OPS_MIN_CHUNK_SIZE")
        .unwrap_or(DEFAULT_MIN_CHUNK_SIZE)
        .max(1);
    let max_chunk_size = env_usize("PROLLY_SLATEDB_OPS_MAX_CHUNK_SIZE")
        .unwrap_or(DEFAULT_MAX_CHUNK_SIZE)
        .max(min_chunk_size);
    let chunking_factor = env_usize("PROLLY_SLATEDB_OPS_CHUNKING_FACTOR")
        .unwrap_or(DEFAULT_CHUNKING_FACTOR)
        .clamp(1, u32::MAX as usize) as u32;

    Config::builder()
        .min_chunk_size(min_chunk_size)
        .max_chunk_size(max_chunk_size)
        .chunking_factor(chunking_factor)
        .hash_seed(0xC0DA)
        .build()
}
