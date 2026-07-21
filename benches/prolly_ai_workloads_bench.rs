use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly::{
    append_batch, Config, KeyBuilder, MemStore, Mutation, Prolly, Resolution, Resolver, Store, Tree,
};

const DEFAULT_SCALE: usize = 10_000;
const DEFAULT_BATCH: usize = 256;

fn main() {
    let scale = env_usize("PROLLY_AI_BENCH_SCALE")
        .unwrap_or(DEFAULT_SCALE)
        .max(128);
    let batch_size = env_usize("PROLLY_AI_BENCH_BATCH")
        .unwrap_or(DEFAULT_BATCH)
        .clamp(16, scale);
    let changes = env_usize("PROLLY_AI_BENCH_CHANGES")
        .unwrap_or((scale / 10).max(64))
        .min(scale / 2)
        .max(1);

    println!("prolly AI/local-first workload bench");
    println!("scale={scale}");
    println!("batch_size={batch_size}");
    println!("changes={changes}");
    println!("workload,operation,records,changes,total_ms,items_per_sec,extra,verified,status");

    bench_conversation_event_appends(scale, batch_size);
    bench_document_chunk_ingestion(scale, batch_size);
    bench_metadata_updates_many_prefixes(scale, changes);
    bench_branch_merge_agent_memory(scale, changes);
    bench_sync_missing_node_exchange(scale, changes);
}

fn bench_conversation_event_appends(events: usize, batch_size: usize) {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, bench_config());
    let batches = conversation_event_batches(events, batch_size);
    let mut tree = prolly.create();

    let start = Instant::now();
    for batch in &batches {
        tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
    }
    let elapsed = start.elapsed();

    let verified = prolly
        .get(&tree, &conversation_event_key(events - 1))
        .unwrap()
        .is_some();
    print_row(BenchRow {
        workload: "conversation",
        operation: "event_appends",
        records: events,
        changes: events,
        elapsed,
        extra: batches.len(),
        verified,
        status: "ok",
    });
}

fn bench_document_chunk_ingestion(chunks: usize, batch_size: usize) {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, bench_config());
    let batches = document_chunk_batches(chunks, batch_size);
    let mut tree = prolly.create();

    let start = Instant::now();
    for batch in &batches {
        tree = append_batch(&prolly, &tree, black_box(batch.clone())).unwrap();
    }
    let elapsed = start.elapsed();

    let sample = chunks / 2;
    let verified = prolly
        .get(&tree, &document_chunk_key(sample / 32, sample % 32))
        .unwrap()
        .is_some();
    print_row(BenchRow {
        workload: "rag",
        operation: "document_chunk_ingestion",
        records: chunks,
        changes: chunks,
        elapsed,
        extra: batches.len(),
        verified,
        status: "ok",
    });
}

fn bench_metadata_updates_many_prefixes(records: usize, changes: usize) {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, bench_config());
    let base = append_batch(&prolly, &prolly.create(), metadata_base(records)).unwrap();
    let updates = metadata_updates(records, changes);

    let start = Instant::now();
    let updated = prolly.batch(&base, black_box(updates)).unwrap();
    let update_elapsed = start.elapsed();
    let update_verified = verify_metadata_updates(&prolly, &updated, records, changes);
    print_row(BenchRow {
        workload: "metadata",
        operation: "updates_many_prefixes",
        records,
        changes,
        elapsed: update_elapsed,
        extra: 0,
        verified: update_verified,
        status: "ok",
    });

    let start = Instant::now();
    let diff_count = prolly.diff(&base, &updated).unwrap().len();
    print_row(BenchRow {
        workload: "metadata",
        operation: "diff_after_updates",
        records,
        changes,
        elapsed: start.elapsed(),
        extra: diff_count,
        verified: diff_count == changes,
        status: "ok",
    });
}

fn bench_branch_merge_agent_memory(records: usize, changes: usize) {
    let store = Arc::new(MemStore::new());
    let prolly = Prolly::new(store, bench_config());
    let base = append_batch(&prolly, &prolly.create(), memory_base(records)).unwrap();
    let conflicts = changes.min(128);
    let left = prolly
        .batch(&base, memory_left_updates(changes, conflicts))
        .unwrap();
    let right = prolly
        .batch(&base, memory_right_updates(changes, conflicts))
        .unwrap();
    let resolver: Resolver = Box::new(|conflict| {
        let mut value = conflict.left.clone().unwrap_or_default();
        value.extend_from_slice(b"\n--- agent-right ---\n");
        value.extend(conflict.right.clone().unwrap_or_default());
        Resolution::value(value)
    });

    let start = Instant::now();
    let merged = prolly.merge(&base, &left, &right, Some(resolver)).unwrap();
    let elapsed = start.elapsed();

    let verified = verify_memory_merge(&prolly, &merged, changes, conflicts);
    print_row(BenchRow {
        workload: "agent_memory",
        operation: "branch_merge_with_resolver",
        records,
        changes: changes + conflicts,
        elapsed,
        extra: conflicts,
        verified,
        status: "ok",
    });
}

fn bench_sync_missing_node_exchange(records: usize, changes: usize) {
    let source_store = Arc::new(MemStore::new());
    let destination_store = Arc::new(MemStore::new());
    let source = Prolly::new(source_store, bench_config());
    let destination = Prolly::new(destination_store.clone(), bench_config());

    let base_records = records.saturating_sub(changes).max(1);
    let base = append_batch(&source, &source.create(), sync_base(base_records)).unwrap();
    source
        .copy_missing_nodes(&base, &destination_store)
        .unwrap();

    let updated = append_batch(&source, &base, sync_append(base_records, changes.max(1))).unwrap();

    let start = Instant::now();
    let plan = source
        .plan_missing_nodes(&updated, &destination_store)
        .unwrap();
    let plan_elapsed = start.elapsed();
    let plan_verified = plan.missing_nodes > 0 && plan.missing_nodes < plan.required_nodes;
    print_row(BenchRow {
        workload: "sync",
        operation: "missing_node_plan",
        records,
        changes,
        elapsed: plan_elapsed,
        extra: plan.missing_nodes,
        verified: plan_verified,
        status: "ok",
    });

    let start = Instant::now();
    let copied = source
        .copy_missing_nodes(&updated, &destination_store)
        .unwrap();
    let copy_elapsed = start.elapsed();
    let copied_sample = destination
        .get(&updated, &sync_key(base_records + changes - 1))
        .unwrap()
        .is_some();
    print_row(BenchRow {
        workload: "sync",
        operation: "missing_node_copy",
        records,
        changes,
        elapsed: copy_elapsed,
        extra: copied.copied_nodes,
        verified: copied.copied_nodes == plan.missing_nodes && copied_sample,
        status: "ok",
    });
}

struct BenchRow<'a> {
    workload: &'a str,
    operation: &'a str,
    records: usize,
    changes: usize,
    elapsed: Duration,
    extra: usize,
    verified: bool,
    status: &'a str,
}

fn print_row(row: BenchRow<'_>) {
    let total_ms = row.elapsed.as_secs_f64() * 1_000.0;
    let items_per_sec = if total_ms > 0.0 {
        row.changes as f64 / (total_ms / 1_000.0)
    } else {
        0.0
    };
    let BenchRow {
        workload,
        operation,
        records,
        changes,
        extra,
        verified,
        status,
        ..
    } = row;
    println!(
        "{workload},{operation},{records},{changes},{total_ms:.3},{items_per_sec:.0},{extra},{verified},{status}"
    );
}

fn conversation_event_batches(events: usize, batch_size: usize) -> Vec<Vec<Mutation>> {
    (0..events)
        .step_by(batch_size)
        .map(|start| {
            let end = (start + batch_size).min(events);
            (start..end)
                .map(|idx| Mutation::Upsert {
                    key: conversation_event_key(idx),
                    val: conversation_event_value(idx),
                })
                .collect()
        })
        .collect()
}

fn document_chunk_batches(chunks: usize, batch_size: usize) -> Vec<Vec<Mutation>> {
    (0..chunks)
        .step_by(batch_size)
        .map(|start| {
            let end = (start + batch_size).min(chunks);
            (start..end)
                .map(|idx| Mutation::Upsert {
                    key: document_chunk_key(idx / 32, idx % 32),
                    val: document_chunk_value(idx),
                })
                .collect()
        })
        .collect()
}

fn metadata_base(records: usize) -> Vec<Mutation> {
    (0..records)
        .map(|idx| Mutation::Upsert {
            key: metadata_key(idx),
            val: format!("{{\"version\":0,\"owner\":\"agent-{:04}\"}}", idx % 128).into_bytes(),
        })
        .collect()
}

fn metadata_updates(records: usize, changes: usize) -> Vec<Mutation> {
    (0..changes)
        .map(|idx| {
            let key_idx = (idx * 97) % records;
            Mutation::Upsert {
                key: metadata_key(key_idx),
                val: format!("{{\"version\":1,\"owner\":\"agent-{:04}\"}}", idx % 128).into_bytes(),
            }
        })
        .collect()
}

fn memory_base(records: usize) -> Vec<Mutation> {
    (0..records)
        .map(|idx| Mutation::Upsert {
            key: memory_key(idx),
            val: format!("base-memory-fact-{idx:08}").into_bytes(),
        })
        .collect()
}

fn memory_left_updates(changes: usize, conflicts: usize) -> Vec<Mutation> {
    (0..changes)
        .map(|idx| Mutation::Upsert {
            key: memory_key(idx),
            val: if idx < conflicts {
                format!("left-conflict-memory-{idx:08}").into_bytes()
            } else {
                format!("left-memory-{idx:08}").into_bytes()
            },
        })
        .collect()
}

fn memory_right_updates(changes: usize, conflicts: usize) -> Vec<Mutation> {
    let conflict_updates = (0..conflicts).map(|idx| Mutation::Upsert {
        key: memory_key(idx),
        val: format!("right-conflict-memory-{idx:08}").into_bytes(),
    });
    let disjoint_updates = (0..changes).map(|idx| Mutation::Upsert {
        key: memory_key(changes + idx),
        val: format!("right-memory-{idx:08}").into_bytes(),
    });
    conflict_updates.chain(disjoint_updates).collect()
}

fn sync_base(records: usize) -> Vec<Mutation> {
    (0..records)
        .map(|idx| Mutation::Upsert {
            key: sync_key(idx),
            val: format!("sync-base-{idx:08}").into_bytes(),
        })
        .collect()
}

fn sync_append(start: usize, changes: usize) -> Vec<Mutation> {
    (start..start + changes)
        .map(|idx| Mutation::Upsert {
            key: sync_key(idx),
            val: format!("sync-append-{idx:08}").into_bytes(),
        })
        .collect()
}

fn verify_metadata_updates<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    records: usize,
    changes: usize,
) -> bool {
    sample_indices(changes).into_iter().all(|idx| {
        let key_idx = (idx * 97) % records;
        prolly.get(tree, &metadata_key(key_idx)).unwrap().is_some()
    })
}

fn verify_memory_merge<S: Store>(
    prolly: &Prolly<S>,
    tree: &Tree,
    changes: usize,
    conflicts: usize,
) -> bool {
    let conflict_ok = if conflicts == 0 {
        true
    } else {
        prolly
            .get(tree, &memory_key(0))
            .unwrap()
            .is_some_and(|value| {
                value
                    .windows(b"agent-right".len())
                    .any(|w| w == b"agent-right")
            })
    };
    let right_ok = prolly
        .get(tree, &memory_key(changes))
        .unwrap()
        .is_some_and(|value| value == b"right-memory-00000000");
    conflict_ok && right_ok
}

fn conversation_event_key(idx: usize) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("tenant")
        .push_str("t1")
        .push_str("conversation")
        .push_str("c000001")
        .push_str("event")
        .push_u64(idx as u64)
        .finish()
}

fn conversation_event_value(idx: usize) -> Vec<u8> {
    let kind = match idx % 5 {
        0 => "message",
        1 => "tool_call",
        2 => "tool_result",
        3 => "memory_write",
        _ => "checkpoint",
    };
    format!(
        "{{\"seq\":{idx},\"kind\":\"{kind}\",\"tokens\":{}}}",
        idx % 4096
    )
    .into_bytes()
}

fn document_chunk_key(doc_idx: usize, chunk_idx: usize) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("tenant")
        .push_str("t1")
        .push_str("doc")
        .push_u64(doc_idx as u64)
        .push_str("chunk")
        .push_u64(chunk_idx as u64)
        .finish()
}

fn document_chunk_value(idx: usize) -> Vec<u8> {
    format!(
        "{{\"chunk\":{idx},\"blob\":\"blob-{idx:08}\",\"embedding\":\"vec-{idx:08}\",\"parser\":\"markdown-v2\"}}"
    )
    .into_bytes()
}

fn metadata_key(idx: usize) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("tenant")
        .push_u64((idx % 128) as u64)
        .push_str("workspace")
        .push_u64((idx % 512) as u64)
        .push_str("doc")
        .push_u64(idx as u64)
        .push_str("metadata")
        .finish()
}

fn memory_key(idx: usize) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("agent")
        .push_str("a1")
        .push_str("memory")
        .push_str("fact")
        .push_u64(idx as u64)
        .finish()
}

fn sync_key(idx: usize) -> Vec<u8> {
    KeyBuilder::new()
        .push_str("sync")
        .push_str("object")
        .push_u64(idx as u64)
        .finish()
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

fn bench_config() -> Config {
    Config::builder()
        .min_chunk_size(32)
        .max_chunk_size(256)
        .chunking_factor(128)
        .hash_seed(0xA11C)
        .build()
}

fn env_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.parse().ok()
}
