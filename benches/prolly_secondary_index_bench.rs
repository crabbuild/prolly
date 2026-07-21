use prolly::{
    Config, IndexProjection, MemStore, Prolly, SecondaryIndex, SecondaryIndexEntry,
    SecondaryIndexRegistry,
};
use std::sync::Arc;
use std::time::Instant;

fn main() {
    let scale = env_usize("PROLLY_INDEX_BENCH_SCALE", 10_000).max(100);
    let batch = env_usize("PROLLY_INDEX_BENCH_BATCH", 256).clamp(1, scale);
    println!("operation,scale,batch,total_ms,items_per_sec,verified");

    let engine = Arc::new(Prolly::new(Arc::new(MemStore::new()), Config::default()));
    let raw = engine.versioned_map(b"users");
    let start = Instant::now();
    raw.edit(|edit| {
        for id in 0..scale {
            edit.put(key(id), value(id, "name"));
        }
    })
    .unwrap();
    row(
        "source_build",
        scale,
        batch,
        start,
        raw.get(&key(scale - 1)).unwrap().is_some(),
    );

    let indexed = engine.indexed_map(b"users", registry()).unwrap();
    let start = Instant::now();
    indexed.ensure_index(b"keys").unwrap();
    indexed.ensure_index(b"include").unwrap();
    indexed.ensure_index(b"all").unwrap();
    row(
        "index_build_all_projections",
        scale,
        batch,
        start,
        indexed.health().unwrap().active_indexes.len() == 3,
    );

    let plain_engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let plain = plain_engine.versioned_map(b"plain");
    plain
        .edit(|edit| {
            for id in 0..scale {
                edit.put(key(id), value(id, "name"));
            }
        })
        .unwrap();
    let start = Instant::now();
    plain
        .edit(|edit| {
            for id in 0..batch {
                edit.put(key(id), value(id, "plain-update"));
            }
        })
        .unwrap();
    row("non_indexed_update", scale, batch, start, true);

    let start = Instant::now();
    indexed
        .edit(|edit| {
            for id in 0..batch {
                edit.put(key(id), value(id, "indexed-update"));
            }
        })
        .unwrap();
    row("indexed_update", scale, batch, start, true);

    let start = Instant::now();
    indexed
        .edit(|edit| {
            for id in 0..batch {
                // Status is unchanged; Include/All values change while KeysOnly is stable.
                edit.put(key(id), value(id, "projection-only"));
            }
        })
        .unwrap();
    let metrics = indexed.metrics();
    row(
        "projection_only_update",
        scale,
        batch,
        start,
        metrics.unchanged_emissions_skipped > 0,
    );

    let large = vec![b'x'; 32 * 1024];
    let start = Instant::now();
    indexed
        .put(
            key(0),
            [b"status-00|".as_slice(), large.as_slice()].concat(),
        )
        .unwrap();
    row("all_projection_amplification", scale, 1, start, true);

    let snapshot = indexed.snapshot().unwrap();
    let keys = snapshot.index(b"keys").unwrap();
    let start = Instant::now();
    let exact = keys.exact(b"status-01").unwrap();
    row("query_exact", scale, exact.len(), start, !exact.is_empty());
    let start = Instant::now();
    let prefix = keys.prefix(b"status-0").unwrap();
    row(
        "query_prefix",
        scale,
        prefix.len(),
        start,
        !prefix.is_empty(),
    );
    let start = Instant::now();
    let range = keys.range(b"status-01", Some(b"status-05")).unwrap();
    row("query_range", scale, range.len(), start, !range.is_empty());
    let start = Instant::now();
    let records = keys.records(b"status-01").unwrap();
    row(
        "query_records_batch",
        scale,
        records.len(),
        start,
        !records.is_empty(),
    );

    let start = Instant::now();
    let verification_snapshot = indexed.snapshot().unwrap();
    let verifications = indexed
        .verify_all(&verification_snapshot.id().source_version)
        .unwrap();
    let verified = verifications
        .iter()
        .all(prolly::IndexVerification::is_valid);
    if !verified {
        eprintln!("verification mismatch: {verifications:#?}");
    }
    row("verify", scale, 3, start, verified);

    let start = Instant::now();
    let bundle = indexed.export_current().unwrap();
    row(
        "export",
        scale,
        bundle.node_count(),
        start,
        bundle.verify().unwrap().valid,
    );
    let replica_engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let replica = replica_engine.indexed_map(b"users", registry()).unwrap();
    let start = Instant::now();
    replica.import_current(&bundle, None).unwrap();
    row(
        "import",
        scale,
        bundle.node_count(),
        start,
        replica.snapshot().is_ok(),
    );

    let start = Instant::now();
    std::thread::scope(|scope| {
        for id in [scale + 1, scale + 2] {
            let engine = engine.clone();
            scope.spawn(move || {
                engine
                    .indexed_map(b"users", registry())
                    .unwrap()
                    .put(key(id), value(id, "concurrent"))
                    .unwrap();
            });
        }
    });
    row(
        "two_writer_retry",
        scale,
        2,
        start,
        indexed.get(&key(scale + 1)).unwrap().is_some()
            && indexed.get(&key(scale + 2)).unwrap().is_some(),
    );
}

fn registry() -> SecondaryIndexRegistry {
    let keys = SecondaryIndex::non_unique("keys", 1, "bench.keys/v1", |_, value| {
        Ok(vec![status(value)])
    })
    .unwrap();
    let include = SecondaryIndex::builder("include", 1, "bench.include/v1")
        .projection(IndexProjection::Include)
        .extract(|_, value| Ok(vec![SecondaryIndexEntry::included(status(value), value)]))
        .unwrap();
    let all = SecondaryIndex::builder("all", 1, "bench.all/v1")
        .projection(IndexProjection::All)
        .extract_terms(|_, value| Ok(vec![status(value)]))
        .unwrap();
    SecondaryIndexRegistry::new()
        .register(keys)
        .unwrap()
        .register(include)
        .unwrap()
        .register(all)
        .unwrap()
}

fn key(id: usize) -> Vec<u8> {
    format!("user-{id:08}").into_bytes()
}

fn value(id: usize, suffix: &str) -> Vec<u8> {
    format!("status-{:02}|{suffix}-{id}", id % 10).into_bytes()
}

fn status(value: &[u8]) -> Vec<u8> {
    value
        .split(|byte| *byte == b'|')
        .next()
        .unwrap_or_default()
        .to_vec()
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn row(operation: &str, scale: usize, batch: usize, start: Instant, verified: bool) {
    let elapsed = start.elapsed();
    let seconds = elapsed.as_secs_f64().max(f64::EPSILON);
    println!(
        "{operation},{scale},{batch},{:.3},{:.0},{verified}",
        elapsed.as_secs_f64() * 1000.0,
        batch as f64 / seconds
    );
    assert!(verified, "benchmark verification failed for {operation}");
}
