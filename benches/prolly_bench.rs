use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly::{
    append_batch, BatchBuilder, BatchWriter, BatchWriterConfig, Config, MemStore, Mutation,
    ParallelConfig, Prolly, Resolution, Resolver, Store, Tree,
};

const DEFAULT_SCALE: usize = 10_000;

fn main() {
    let scale = std::env::var("PROLLY_BENCH_SCALE")
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(DEFAULT_SCALE)
        .max(1_000);

    println!("prolly benchmark scale={scale}");
    println!("name,total_ms,iterations,items,ns_per_item");

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
    bench_batch_mutations_no_prefetch(scale);
    bench_batch_mutations_bottom_up(scale);
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

    #[cfg(feature = "sqlite")]
    {
        bench_sqlite_roundtrip(scale / 5);
        bench_sqlite_disk_incremental_persist(scale / 10);
        bench_sqlite_disk_point_updates(scale / 5);
        bench_sqlite_disk_point_deletes(scale / 5);
        bench_sqlite_disk_batch_builder_persist(scale / 2);
        bench_sqlite_disk_reopen_get(scale / 5);
        bench_sqlite_disk_batch_mutations(scale / 5);
        bench_sqlite_disk_append_batch_persist(scale / 5);
        bench_sqlite_disk_append_batch_chain_persist(scale / 5);
        bench_sqlite_disk_append_batch_chain_new_manager_persist(scale / 5);
        bench_sqlite_disk_diff_empty_to_full(scale / 5);
    }
}

fn bench_incremental_insert(items: usize) {
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
    let gets = items * 20;

    measure("point_get_mem", 10, gets, || {
        for i in 0..gets {
            let key = &data[i % data.len()].0;
            black_box(prolly.get(&tree, black_box(key)).unwrap());
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

fn bench_batch_mutations_no_prefetch(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = update_mutations(items, mutation_count, "no-prefetch");
    let writer = BatchWriter::with_config(BatchWriterConfig::new().with_prefetch(false));

    measure(
        "batch_mutations_no_prefetch_mem",
        20,
        mutation_count,
        || {
            let tree = writer
                .apply_batch(&prolly, &base, black_box(mutations.clone()))
                .unwrap();
            black_box(tree.root);
        },
    );
}

fn bench_batch_mutations_bottom_up(items: usize) {
    let data = data_set(items);
    let (_, prolly, base) = build_tree(&data);
    let mutation_count = mutation_count(items);
    let mutations = update_mutations(items, mutation_count, "bottom-up");
    let writer = BatchWriter::with_config(BatchWriterConfig::new().with_bottom_up_rebuild(true));

    measure("batch_mutations_bottom_up_mem", 20, mutation_count, || {
        let tree = writer
            .apply_batch(&prolly, &base, black_box(mutations.clone()))
            .unwrap();
        black_box(tree.root);
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

    measure("merge_conflict_resolved_mem", 10, conflicts, || {
        let resolver: Resolver =
            Box::new(|conflict| Resolution::value(conflict.right.clone().expect("right value")));
        let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();
        black_box(merged.root);
    });
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_roundtrip(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();

    measure("sqlite_insert_get_in_memory", 5, items * 2, || {
        let store = SqliteStore::open_in_memory().unwrap();
        let prolly = Prolly::new(store, config.clone());
        let mut tree = prolly.create();
        for (key, val) in &data {
            tree = prolly
                .put(&tree, black_box(key.clone()), black_box(val.clone()))
                .unwrap();
        }
        for (key, _) in &data {
            black_box(prolly.get(&tree, black_box(key)).unwrap());
        }
    });
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_incremental_persist(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();

    measure("sqlite_disk_incremental_persist", 3, items, || {
        let path = temp_db_path("incremental");
        remove_sqlite_files(&path);
        {
            let store = SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap();
            let prolly = Prolly::new(store, config.clone());
            let mut tree = prolly.create();
            for (key, val) in &data {
                tree = prolly
                    .put(&tree, black_box(key.clone()), black_box(val.clone()))
                    .unwrap();
            }
            black_box(tree.root);
        }
        remove_sqlite_files(&path);
    });
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_point_updates(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("point-updates");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store, config);
        let count = mutation_count(items);
        let updates = point_updates(items, count, "sqlite-point-update");

        measure("sqlite_disk_point_updates", 5, count, || {
            let mut tree = base.clone();
            for (key, val) in &updates {
                tree = prolly
                    .put(&tree, black_box(key.clone()), black_box(val.clone()))
                    .unwrap();
            }
            black_box(tree.root);
        });
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_point_deletes(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("point-deletes");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store, config);
        let count = mutation_count(items);
        let deletes = point_delete_keys(items, count);

        measure("sqlite_disk_point_deletes", 5, count, || {
            let mut tree = base.clone();
            for key in &deletes {
                tree = prolly.delete(&tree, black_box(key)).unwrap();
            }
            black_box(tree.root);
        });
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_batch_builder_persist(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();

    measure("sqlite_disk_batch_builder_persist", 5, items, || {
        let path = temp_db_path("batch-builder");
        remove_sqlite_files(&path);
        {
            let store =
                Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
            let mut builder = BatchBuilder::new(store, config.clone());
            for (key, val) in data.iter().rev() {
                builder.add(black_box(key.clone()), black_box(val.clone()));
            }
            let tree = builder.build().unwrap();
            black_box(tree.root);
        }
        remove_sqlite_files(&path);
    });
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_reopen_get(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("reopen-get");
    remove_sqlite_files(&path);

    let tree = {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store, config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        builder.build().unwrap()
    };

    let gets = items * 10;
    measure("sqlite_disk_reopen_get", 5, gets, || {
        let store = SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap();
        let prolly = Prolly::new(store, config.clone());
        for i in 0..gets {
            let key = &data[i % data.len()].0;
            black_box(prolly.get(&tree, black_box(key)).unwrap());
        }
    });

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_batch_mutations(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("batch-mutations");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store, config);
        let count = mutation_count(items);
        let mutations = mixed_mutations(items, count, "sqlite-disk");

        measure("sqlite_disk_batch_mutations", 5, count, || {
            let tree = prolly.batch(&base, black_box(mutations.clone())).unwrap();
            black_box(tree.root);
        });
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_append_batch_persist(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("append-batch");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store, config);
        let count = mutation_count(items);
        let mutations = append_mutations(items, count, "sqlite-append");

        measure("sqlite_disk_append_batch_persist", 5, count, || {
            let tree = append_batch(&prolly, &base, black_box(mutations.clone())).unwrap();
            black_box(tree.root);
        });
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_append_batch_chain_persist(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("append-batch-chain");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let prolly = Prolly::new(store, config);
        let batch_size = append_chain_batch_size(items);
        let rounds = append_chain_rounds();
        let batches =
            append_chain_mutation_batches(items, batch_size, rounds, "sqlite-append-chain");

        measure(
            "sqlite_disk_append_batch_chain_persist",
            5,
            batch_size * rounds,
            || {
                let mut tree = base.clone();
                for batch in &batches {
                    tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
                }
                black_box(tree.root);
            },
        );
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_append_batch_chain_new_manager_persist(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("append-batch-chain-new-manager");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let base = builder.build().unwrap();
        let batch_size = append_chain_batch_size(items);
        let rounds = append_chain_rounds();
        let batches = append_chain_mutation_batches(
            items,
            batch_size,
            rounds,
            "sqlite-append-chain-new-manager",
        );

        measure(
            "sqlite_disk_append_batch_chain_new_manager_persist",
            5,
            batch_size * rounds,
            || {
                let mut tree = base.clone();
                for batch in &batches {
                    let prolly = Prolly::new(store.clone(), config.clone());
                    tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
                }
                black_box(tree.root);
            },
        );
    }

    remove_sqlite_files(&path);
}

#[cfg(feature = "sqlite")]
fn bench_sqlite_disk_diff_empty_to_full(items: usize) {
    use prolly::SqliteStore;

    let data = data_set(items);
    let config = bench_config();
    let path = temp_db_path("diff-empty-to-full");
    remove_sqlite_files(&path);

    {
        let store =
            Arc::new(SqliteStore::open_with_config(&path, durable_sqlite_config()).unwrap());
        let mut builder = BatchBuilder::new(store.clone(), config.clone());
        for (key, val) in &data {
            builder.add(key.clone(), val.clone());
        }
        let other = builder.build().unwrap();

        measure("sqlite_disk_diff_empty_to_full", 5, items, || {
            let prolly = Prolly::new(store.clone(), config.clone());
            let empty = prolly.create();
            let diffs = prolly.diff(&empty, &other).unwrap();
            black_box(diffs.len());
        });
    }

    remove_sqlite_files(&path);
}

fn measure<F>(name: &str, iterations: usize, items: usize, mut f: F)
where
    F: FnMut(),
{
    f();

    let mut total = Duration::ZERO;
    for _ in 0..iterations {
        let start = Instant::now();
        f();
        total += start.elapsed();
    }

    let total_ns = total.as_nanos();
    let total_items = iterations as u128 * items as u128;
    let ns_per_item = if total_items == 0 {
        0
    } else {
        total_ns / total_items
    };

    println!(
        "{name},{:.3},{iterations},{items},{ns_per_item}",
        total.as_secs_f64() * 1_000.0
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
    (items / 10).max(100)
}

fn window_bounds(items: usize) -> (usize, usize) {
    let start = items / 3;
    let len = (items / 10).max(100).min(items - start);
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
    Config::builder()
        .min_chunk_size(16)
        .max_chunk_size(128)
        .chunking_factor(64)
        .hash_seed(0xC0DA)
        .build()
}

#[cfg(feature = "sqlite")]
fn durable_sqlite_config() -> prolly::SqliteStoreConfig {
    prolly::SqliteStoreConfig {
        busy_timeout_ms: 5_000,
        enable_wal: true,
        synchronous_normal: false,
    }
}

#[cfg(feature = "sqlite")]
fn temp_db_path(label: &str) -> std::path::PathBuf {
    use std::time::{SystemTime, UNIX_EPOCH};

    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "crabdb-prolly-bench-{label}-{}-{nanos}.db",
        std::process::id()
    ))
}

#[cfg(feature = "sqlite")]
fn remove_sqlite_files(path: &std::path::Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("db-wal"));
    let _ = std::fs::remove_file(path.with_extension("db-shm"));
}
