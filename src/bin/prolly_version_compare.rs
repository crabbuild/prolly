#[path = "prolly_benchmark_support/config.rs"]
mod benchmark_config;
#[path = "prolly_version_support/mod.rs"]
mod support;

use std::hint::black_box;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

#[cfg(not(target_arch = "wasm32"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(not(target_arch = "wasm32"))]
unsafe extern "C" {
    fn mi_collect(force: bool);
}

use prolly::{
    BorrowedMergeResolver, ConflictRef, Diff, MemStore, MergeDecision, Mutation, Prolly, Tree,
};
use support::{
    base_mutations, branch_mutations, change_count, conflicting_mutations, digest_diffs,
    digest_entry, range_bounds, validate_unique_keys, workload_digest, Args, FNV_OFFSET,
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
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        benchmark_config::benchmark_config(),
    );
    let base = build(&manager, None, base_mutations(args.records));
    let base_summary = tree_summary(&manager, &base);
    assert_eq!(base_summary.0, args.records);

    let edits = change_count(args.records, args.density);
    let left_mutations = Arc::new(branch_mutations(
        args.records,
        args.density,
        args.locality,
        0,
        1,
    ));
    validate_unique_keys(left_mutations.as_slice());
    let left = build_tracked(&manager, &base, Arc::clone(&left_mutations));
    let left_summary = tree_summary(&manager, &left);
    let (range_start, range_end) = range_bounds(
        args.records,
        args.density,
        args.locality,
        left_mutations.as_slice(),
    );
    let compare_workload = workload_digest(args.records, "compare", &[left_mutations.as_slice()]);
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
    drop(range_diffs);
    drop(expected_range);
    drop(diffs);
    drop(expected_diffs);

    let started = Instant::now();
    let patch = manager
        .diff_patch(black_box(&base), black_box(&left))
        .expect("patch generation succeeds");
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(patch.edits.len(), usize::from(base.root != left.root));
    rows.push(Measurement {
        operation: "patch_generate",
        relationship: "compare",
        operations: edits.max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: expected_diff_digest,
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
    assert!(manager
        .diff(&patched, &left)
        .expect("patch validation diff succeeds")
        .is_empty());
    rows.push(Measurement {
        operation: "patch_apply",
        relationship: "compare",
        operations: edits.max(1),
        elapsed_ns: elapsed,
        workload_digest: compare_workload,
        result_digest: patched_summary.1,
        result_count: patched_summary.0,
        base_count: base_summary.0,
        target_count: left_summary.0,
        conflict_count: 0,
    });
    drop(patched);
    drop(patch);

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

    let right_mutations = Arc::new(branch_mutations(
        args.records,
        args.density,
        args.locality,
        edits,
        2,
    ));
    validate_unique_keys(right_mutations.as_slice());
    let right_disjoint = build_tracked(&manager, &base, Arc::clone(&right_mutations));
    if args.locality == support::Locality::Append {
        collect_benchmark_allocator(true);
    }
    rows.push(measure_merge(
        &manager,
        &base,
        &left,
        &right_disjoint,
        "disjoint",
        workload_digest(
            args.records,
            "disjoint",
            &[left_mutations.as_slice(), right_mutations.as_slice()],
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
            &[left_mutations.as_slice(), left_mutations.as_slice()],
        ),
        0,
        edits,
    ));

    let conflict_mutations = Arc::new(conflicting_mutations(left_mutations.as_slice()));
    let right_conflict = build_tracked(&manager, &base, Arc::clone(&conflict_mutations));
    rows.push(measure_merge(
        &manager,
        &base,
        &left,
        &right_conflict,
        "conflict",
        workload_digest(
            args.records,
            "conflict",
            &[left_mutations.as_slice(), conflict_mutations.as_slice()],
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

fn build_tracked(
    manager: &Prolly<Arc<MemStore>>,
    base: &Tree,
    mutations: Arc<Vec<Mutation>>,
) -> Tree {
    manager
        .batch_with_lineage(base, mutations)
        .expect("tracked benchmark tree construction succeeds")
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

fn collect_benchmark_allocator(force: bool) {
    #[cfg(not(target_arch = "wasm32"))]
    unsafe {
        // The comparison binary owns the process-wide mimalloc instance, and
        // no benchmark worker is active while scenario setup is finalized.
        mi_collect(force);
    }
}

#[allow(clippy::too_many_arguments)]
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
    let started = Instant::now();
    let merged = if expected_conflicts == 0 {
        manager
            .merge(black_box(base), black_box(left), black_box(right), None)
            .expect("merge succeeds")
    } else {
        let resolver = PreferLeftResolver {
            calls: Arc::clone(&resolver_calls),
        };
        manager
            .merge_with(
                black_box(base),
                black_box(left),
                black_box(right),
                Some(&resolver),
            )
            .expect("merge succeeds")
    };
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

struct PreferLeftResolver {
    calls: Arc<AtomicUsize>,
}

impl BorrowedMergeResolver for PreferLeftResolver {
    fn resolve(&self, _conflict: ConflictRef<'_>) -> MergeDecision {
        self.calls.fetch_add(1, Ordering::Relaxed);
        MergeDecision::UseLeft
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
