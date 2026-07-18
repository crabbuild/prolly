#[path = "prolly_version_support/mod.rs"]
mod support;

use std::hint::black_box;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use prolly::{Config, Diff, MemStore, Mutation, Prolly, Resolution, Tree};
use support::{
    Args, FNV_OFFSET, base_mutations, branch_mutations, change_count, conflicting_mutations,
    digest_diffs, digest_entry, digest_patch, range_bounds, validate_unique_keys, workload_digest,
};

const CSV_HEADER: &str = "implementation,revision,contract_version,records,density,locality,operation,relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_digest,result_count,base_count,target_count,conflict_count,validated";

struct Measurement<'a> {
    operation: &'a str,
    relationship: &'a str,
    operations: usize,
    elapsed_ns: u128,
    workload_digest: u64,
    result_digest: u64,
    result_count: usize,
    base_count: usize,
    target_count: usize,
    conflict_count: usize,
}

fn main() {
    let args = support::parse_common_args();
    let revision = std::env::var("BENCH_REVISION").unwrap_or_else(|_| "unknown".into());
    let measurements = run(&args);
    println!("{CSV_HEADER}");
    for measurement in measurements {
        emit(&revision, &args, &measurement);
    }
}

fn run(args: &Args) -> Vec<Measurement<'static>> {
    let manager = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let base = build(&manager, None, base_mutations(args.records));
    let base_summary = tree_summary(&manager, &base);
    assert_eq!(base_summary.0, args.records);

    let edits = change_count(args.records, args.density);
    let left_mutations = branch_mutations(args.records, args.density, args.locality, 0, 1);
    validate_unique_keys(&left_mutations);
    let left = build(&manager, Some(&base), left_mutations.clone());
    let left_summary = tree_summary(&manager, &left);
    let (range_start, range_end) =
        range_bounds(args.records, args.density, args.locality, &left_mutations);
    let compare_workload = workload_digest(args.records, "compare", &[&left_mutations]);
    let expected_diffs = manager
        .diff(&base, &left)
        .expect("validation diff succeeds");
    assert_eq!(expected_diffs.len(), edits);
    let expected_diff_digest = digest_diffs(&expected_diffs);

    let mut rows = Vec::new();
    let started = Instant::now();
    let diffs = manager
        .diff(black_box(&base), black_box(&left))
        .expect("diff succeeds");
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(digest_diffs(&diffs), expected_diff_digest);
    rows.push(Measurement {
        operation: "full_diff",
        relationship: "compare",
        operations: diffs.len().max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: expected_diff_digest,
        result_count: diffs.len(),
        base_count: base_summary.0,
        target_count: left_summary.0,
        conflict_count: 0,
    });

    let expected_range: Vec<Diff> = expected_diffs
        .iter()
        .filter(|diff| diff.key() >= range_start.as_slice() && diff.key() < range_end.as_slice())
        .cloned()
        .collect();
    let expected_range_digest = digest_diffs(&expected_range);
    let started = Instant::now();
    let range_diffs = manager
        .range_diff(
            black_box(&base),
            black_box(&left),
            black_box(&range_start),
            Some(black_box(&range_end)),
        )
        .expect("range diff succeeds");
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(digest_diffs(&range_diffs), expected_range_digest);
    rows.push(Measurement {
        operation: "range_diff",
        relationship: "compare",
        operations: range_diffs.len().max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: expected_range_digest,
        result_count: range_diffs.len(),
        base_count: base_summary.0,
        target_count: left_summary.0,
        conflict_count: 0,
    });

    let started = Instant::now();
    let patch = manager
        .diff_patch(black_box(&base), black_box(&left))
        .expect("patch generation succeeds");
    let elapsed = started.elapsed().as_nanos();
    let patch_digest = digest_patch(&patch.edits);
    assert_eq!(patch_digest, expected_diff_digest);
    rows.push(Measurement {
        operation: "patch_generate",
        relationship: "compare",
        operations: patch.edits.len().max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: patch_digest,
        result_count: patch.edits.len(),
        base_count: base_summary.0,
        target_count: left_summary.0,
        conflict_count: 0,
    });

    let started = Instant::now();
    let patched = manager
        .apply_patch(black_box(&base), black_box(&patch))
        .expect("patch application succeeds");
    let elapsed = started.elapsed().as_nanos();
    let patched_summary = tree_summary(&manager, &patched);
    assert_eq!(patched_summary, left_summary);
    assert!(
        manager
            .diff(&patched, &left)
            .expect("patch validation diff succeeds")
            .is_empty()
    );
    rows.push(Measurement {
        operation: "patch_apply",
        relationship: "compare",
        operations: patch.edits.len().max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: patched_summary.1,
        result_count: patched_summary.0,
        base_count: base_summary.0,
        target_count: left_summary.0,
        conflict_count: 0,
    });

    if args.density == 0 {
        rows.push(measure_merge(
            &manager,
            &base,
            &base,
            &base,
            "noop",
            compare_workload,
            0,
            1,
        ));
        return rows;
    }

    let right_mutations = branch_mutations(args.records, args.density, args.locality, edits, 2);
    validate_unique_keys(&right_mutations);
    let right_disjoint = build(&manager, Some(&base), right_mutations.clone());
    rows.push(measure_merge(
        &manager,
        &base,
        &left,
        &right_disjoint,
        "disjoint",
        workload_digest(
            args.records,
            "disjoint",
            &[&left_mutations, &right_mutations],
        ),
        0,
        edits * 2,
    ));

    rows.push(measure_merge(
        &manager,
        &base,
        &left,
        &left,
        "convergent",
        workload_digest(
            args.records,
            "convergent",
            &[&left_mutations, &left_mutations],
        ),
        0,
        edits,
    ));

    let conflict_mutations = conflicting_mutations(&left_mutations);
    let right_conflict = build(&manager, Some(&base), conflict_mutations.clone());
    rows.push(measure_merge(
        &manager,
        &base,
        &left,
        &right_conflict,
        "conflict",
        workload_digest(
            args.records,
            "conflict",
            &[&left_mutations, &conflict_mutations],
        ),
        edits,
        edits,
    ));
    rows
}

fn build(manager: &Prolly<Arc<MemStore>>, base: Option<&Tree>, mutations: Vec<Mutation>) -> Tree {
    let base = base.cloned().unwrap_or_else(|| manager.create());
    manager
        .batch(&base, mutations)
        .expect("benchmark tree construction succeeds")
}

fn tree_summary(manager: &Prolly<Arc<MemStore>>, tree: &Tree) -> (usize, u64) {
    let mut count = 0usize;
    let mut digest = FNV_OFFSET;
    manager
        .read(tree)
        .expect("read session opens")
        .scan_range(&[], None, |entry| {
            digest = digest_entry(digest, entry.key(), entry.value());
            count += 1;
        })
        .expect("tree scan succeeds");
    (count, support::digest_u64(digest, count as u64))
}

fn measure_merge(
    manager: &Prolly<Arc<MemStore>>,
    base: &Tree,
    left: &Tree,
    right: &Tree,
    relationship: &'static str,
    workload: u64,
    expected_conflicts: usize,
    operation_count: usize,
) -> Measurement<'static> {
    let resolver_calls = Arc::new(AtomicUsize::new(0));
    let resolver = if expected_conflicts == 0 {
        None
    } else {
        let resolver_calls = Arc::clone(&resolver_calls);
        Some(Box::new(move |conflict: &prolly::Conflict| {
            resolver_calls.fetch_add(1, Ordering::Relaxed);
            match &conflict.left {
                Some(value) => Resolution::Value(value.clone()),
                None => Resolution::Delete,
            }
        }) as prolly::Resolver)
    };
    let started = Instant::now();
    let merged = manager
        .merge(black_box(base), black_box(left), black_box(right), resolver)
        .expect("merge succeeds");
    let elapsed = started.elapsed().as_nanos();
    let resolver_call_count = resolver_calls.load(Ordering::Relaxed);
    if expected_conflicts == 0 {
        assert_eq!(resolver_call_count, 0);
    } else {
        assert!(resolver_call_count >= expected_conflicts);
    }
    let summary = tree_summary(manager, &merged);
    if relationship == "convergent" || relationship == "conflict" {
        assert_eq!(summary, tree_summary(manager, left));
    }
    Measurement {
        operation: if relationship == "noop" {
            "merge_noop"
        } else if relationship == "disjoint" {
            "merge_disjoint"
        } else if relationship == "convergent" {
            "merge_convergent"
        } else {
            "merge_conflict"
        },
        relationship,
        operations: operation_count.max(1),
        elapsed_ns: elapsed,
        workload_digest: workload,
        result_digest: summary.1,
        result_count: summary.0,
        base_count: tree_summary(manager, base).0,
        target_count: summary.0,
        conflict_count: expected_conflicts,
    }
}

fn emit(revision: &str, args: &Args, row: &Measurement<'_>) {
    let operations = row.operations.max(1);
    let ns_per_op = row.elapsed_ns as f64 / operations as f64;
    let ops_per_sec = if row.elapsed_ns == 0 {
        0.0
    } else {
        operations as f64 * 1_000_000_000.0 / row.elapsed_ns as f64
    };
    println!(
        "rust,{revision},{},{},{},{},{},{},{},{},{ns_per_op:.3},{ops_per_sec:.3},{:016x},{:016x},{},{},{},{},true",
        support::CONTRACT_VERSION,
        args.records,
        args.density,
        args.locality.name(),
        row.operation,
        row.relationship,
        operations,
        row.elapsed_ns,
        row.workload_digest,
        row.result_digest,
        row.result_count,
        row.base_count,
        row.target_count,
        row.conflict_count,
    );
}
