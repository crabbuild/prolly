#[path = "prolly_benchmark_support/config.rs"]
mod benchmark_config;
#[path = "prolly_version_support/mod.rs"]
mod support;

use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

use prolly::{MapVersion, MemStore, Mutation, Prolly};
use support::{
    base_mutations, branch_mutations, digest_bytes, digest_entry, digest_u64, key_for_id,
    value_for, Locality, FNV_OFFSET,
};

const CSV_HEADER: &str = "implementation,revision,contract_version,records,density,locality,operation,relationship,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_digest,result_count,base_count,target_count,conflict_count,validated";
const HISTORY_DEPTH: usize = 100;
const HEAD_RESOLUTIONS: usize = 10_000;
const SNAPSHOT_RESOLUTIONS: usize = 2_000;
const HISTORICAL_READS: usize = 100_000;
const LIST_REPETITIONS: usize = 100;
const ROLLBACK_REPETITIONS: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Scenario {
    Publish,
    Read,
    Rollback,
    Prune,
}

impl Scenario {
    fn parse(value: &str) -> Self {
        match value {
            "publish" => Self::Publish,
            "read" => Self::Read,
            "rollback" => Self::Rollback,
            "prune" => Self::Prune,
            _ => panic!("invalid lifecycle scenario {value:?}"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Publish => "publish",
            Self::Read => "read",
            Self::Rollback => "rollback",
            Self::Prune => "prune",
        }
    }
}

struct Args {
    records: usize,
    scenario: Scenario,
    density: usize,
    locality: Locality,
}

struct Measurement<'a> {
    operation: &'a str,
    operations: usize,
    elapsed_ns: u128,
    workload_digest: u64,
    result_digest: u64,
    result_count: usize,
    base_count: usize,
    target_count: usize,
}

fn main() {
    let args = parse_args();
    let revision = env::var("BENCH_REVISION").unwrap_or_else(|_| "unknown".into());
    let rows = match args.scenario {
        Scenario::Publish => run_publish(&args),
        Scenario::Read => run_read(&args),
        Scenario::Rollback => run_rollback(&args),
        Scenario::Prune => run_prune(&args),
    };
    println!("{CSV_HEADER}");
    for row in rows {
        emit(&revision, &args, &row);
    }
}

fn parse_args() -> Args {
    let mut records = None;
    let mut scenario = None;
    let mut density = 0usize;
    let mut locality = Locality::None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--records" => records = Some(value.parse().expect("records must be an integer")),
            "--scenario" => scenario = Some(Scenario::parse(&value)),
            "--density" => density = value.parse().expect("density must be an integer"),
            "--locality" => locality = Locality::parse(&value),
            _ => panic!("unknown argument {flag}"),
        }
    }
    let result = Args {
        records: records.expect("--records is required"),
        scenario: scenario.expect("--scenario is required"),
        density,
        locality,
    };
    assert!(result.records >= 1_000 && result.records % 1_000 == 0);
    if result.scenario == Scenario::Publish {
        assert!(matches!(result.density, 1 | 30));
        assert!(result.locality != Locality::None);
    } else {
        assert_eq!(result.density, 0);
        assert_eq!(result.locality, Locality::None);
    }
    result
}

fn run_publish(args: &Args) -> Vec<Measurement<'static>> {
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        benchmark_config::benchmark_config(),
    );
    let map = manager.versioned_map(b"version-lifecycle-publish");
    let base = map
        .apply_at_millis(base_mutations(args.records), 1)
        .expect("base version publishes");
    let mutations = branch_mutations(args.records, args.density, args.locality, 0, 1);
    let workload = support::workload_digest(args.records, "publish", &[&mutations]);
    let started = Instant::now();
    let published = if args.locality == Locality::Append {
        map.append(black_box(mutations.clone()))
    } else {
        map.apply(black_box(mutations.clone()))
    }
    .expect("version publishes");
    let elapsed = started.elapsed().as_nanos();
    let base_summary = tree_summary(&manager, &base);
    let target_summary = tree_summary(&manager, &published);
    assert_eq!(
        map.head_id().expect("head resolves"),
        Some(published.id.clone())
    );
    vec![Measurement {
        operation: "version_publish",
        operations: mutations.len().max(1),
        elapsed_ns: elapsed,
        workload_digest: workload,
        result_digest: target_summary.1,
        result_count: target_summary.0,
        base_count: base_summary.0,
        target_count: target_summary.0,
    }]
}

fn run_read(args: &Args) -> Vec<Measurement<'static>> {
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        benchmark_config::benchmark_config(),
    );
    let map = manager.versioned_map(b"version-lifecycle-read");
    let (versions, history_digest) = build_history(&map, args.records);
    let head_id = versions.last().expect("history has head").id.clone();
    let base_count = args.records;
    let mut rows = Vec::new();

    let started = Instant::now();
    let mut head_digest = FNV_OFFSET;
    for _ in 0..HEAD_RESOLUTIONS {
        let observed = map
            .head_id()
            .expect("head resolution succeeds")
            .expect("head exists");
        head_digest = digest_bytes(head_digest, observed.as_cid().as_bytes());
    }
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(map.head_id().expect("head validates"), Some(head_id));
    rows.push(Measurement {
        operation: "head_resolve",
        operations: HEAD_RESOLUTIONS,
        elapsed_ns: elapsed,
        workload_digest: history_digest,
        result_digest: digest_u64(head_digest, HEAD_RESOLUTIONS as u64),
        result_count: HEAD_RESOLUTIONS,
        base_count,
        target_count: base_count,
    });

    let started = Instant::now();
    let mut snapshot_digest = FNV_OFFSET;
    for index in 0..SNAPSHOT_RESOLUTIONS {
        let version = &versions[index % versions.len()];
        let snapshot = map
            .snapshot_at(&version.id)
            .expect("snapshot resolution succeeds")
            .expect("snapshot exists");
        snapshot_digest = digest_bytes(snapshot_digest, snapshot.id().as_cid().as_bytes());
    }
    let elapsed = started.elapsed().as_nanos();
    rows.push(Measurement {
        operation: "snapshot_resolve",
        operations: SNAPSHOT_RESOLUTIONS,
        elapsed_ns: elapsed,
        workload_digest: history_digest,
        result_digest: digest_u64(snapshot_digest, SNAPSHOT_RESOLUTIONS as u64),
        result_count: SNAPSHOT_RESOLUTIONS,
        base_count,
        target_count: base_count,
    });

    let snapshots: Vec<_> = versions
        .iter()
        .map(|version| {
            map.snapshot_at(&version.id)
                .expect("historical snapshot resolves")
                .expect("historical snapshot exists")
        })
        .collect();
    let expected_read_digest = historical_read_digest(&snapshots, args.records);
    let started = Instant::now();
    let read_digest = historical_read_digest(&snapshots, args.records);
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(read_digest, expected_read_digest);
    rows.push(Measurement {
        operation: "historical_point_read",
        operations: HISTORICAL_READS,
        elapsed_ns: elapsed,
        workload_digest: history_digest,
        result_digest: read_digest,
        result_count: HISTORICAL_READS,
        base_count,
        target_count: base_count,
    });

    let sample_indexes = [0, versions.len() / 2, versions.len() - 1];
    let expected_scan = historical_scan_digest(&snapshots, &sample_indexes);
    let started = Instant::now();
    let scan = historical_scan_digest(&snapshots, &sample_indexes);
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(scan, expected_scan);
    rows.push(Measurement {
        operation: "historical_range_scan",
        operations: scan.0.max(1),
        elapsed_ns: elapsed,
        workload_digest: history_digest,
        result_digest: scan.1,
        result_count: scan.0,
        base_count,
        target_count: base_count,
    });

    let started = Instant::now();
    let mut listed = 0usize;
    let mut list_digest = FNV_OFFSET;
    for _ in 0..LIST_REPETITIONS {
        let catalog = map.versions().expect("version listing succeeds");
        listed += catalog.len();
        for version in catalog {
            list_digest = digest_bytes(list_digest, version.id.as_cid().as_bytes());
        }
    }
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(listed, HISTORY_DEPTH * LIST_REPETITIONS);
    rows.push(Measurement {
        operation: "version_list",
        operations: listed,
        elapsed_ns: elapsed,
        workload_digest: history_digest,
        result_digest: digest_u64(list_digest, listed as u64),
        result_count: listed,
        base_count,
        target_count: base_count,
    });
    rows
}

fn run_rollback(args: &Args) -> Vec<Measurement<'static>> {
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        benchmark_config::benchmark_config(),
    );
    let map = manager.versioned_map(b"version-lifecycle-rollback");
    let (versions, workload) = build_history(&map, args.records);
    let first = &versions[HISTORY_DEPTH / 4].id;
    let second = &versions[HISTORY_DEPTH * 3 / 4].id;
    let started = Instant::now();
    let mut result_digest = FNV_OFFSET;
    for index in 0..ROLLBACK_REPETITIONS {
        let target = if index % 2 == 0 { first } else { second };
        let rolled = map
            .rollback_to(black_box(target))
            .expect("rollback succeeds");
        result_digest = digest_bytes(result_digest, rolled.id.as_cid().as_bytes());
    }
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(map.head_id().expect("head validates"), Some(second.clone()));
    vec![Measurement {
        operation: "rollback",
        operations: ROLLBACK_REPETITIONS,
        elapsed_ns: elapsed,
        workload_digest: workload,
        result_digest: digest_u64(result_digest, ROLLBACK_REPETITIONS as u64),
        result_count: ROLLBACK_REPETITIONS,
        base_count: args.records,
        target_count: args.records,
    }]
}

fn run_prune(args: &Args) -> Vec<Measurement<'static>> {
    let manager = Prolly::new(
        Arc::new(MemStore::new()),
        benchmark_config::benchmark_config(),
    );
    let map = manager.versioned_map(b"version-lifecycle-prune");
    let (versions, workload) = build_history(&map, args.records);
    let old_head = versions[HISTORY_DEPTH / 4].id.clone();
    map.rollback_to(&old_head)
        .expect("pre-prune rollback succeeds");
    let started = Instant::now();
    let result = map.prune_versions(black_box(10)).expect("pruning succeeds");
    let elapsed = started.elapsed().as_nanos();
    assert_eq!(result.removed.len(), 89);
    assert_eq!(result.retained.len(), 11);
    let catalog = map.versions().expect("pruned catalog lists");
    assert_eq!(catalog.len(), 11);
    assert!(catalog
        .iter()
        .any(|version| version.id == old_head && version.is_head));
    vec![Measurement {
        operation: "retention_prune",
        operations: HISTORY_DEPTH,
        elapsed_ns: elapsed,
        workload_digest: workload,
        result_digest: digest_versions(&catalog),
        result_count: catalog.len(),
        base_count: HISTORY_DEPTH,
        target_count: 11,
    }]
}

fn build_history<'a>(
    map: &prolly::VersionedMap<'a, Arc<MemStore>>,
    records: usize,
) -> (Vec<MapVersion>, u64) {
    let mut versions = Vec::with_capacity(HISTORY_DEPTH);
    let base = base_mutations(records);
    let mut workload_digest =
        support::workload_digest(records, "lifecycle-history-v1", &[base.as_slice()]);
    versions.push(
        map.apply_at_millis(base, 1)
            .expect("base history version publishes"),
    );
    for index in 1..HISTORY_DEPTH {
        let position = index % records;
        let key = key_for_id(position * 2);
        let val = value_for((position * 2) as u64, 10 + index as u64);
        workload_digest = digest_u64(workload_digest, 1);
        workload_digest = digest_bytes(workload_digest, &[1]);
        workload_digest = digest_entry(workload_digest, &key, &val);
        versions.push(
            map.apply_at_millis(vec![Mutation::Upsert { key, val }], index as u64 + 1)
                .expect("history version publishes"),
        );
    }
    assert_eq!(map.versions().expect("history lists").len(), HISTORY_DEPTH);
    (versions, digest_u64(workload_digest, HISTORY_DEPTH as u64))
}

fn historical_read_digest<S: prolly::Store>(
    snapshots: &[prolly::MapSnapshot<'_, S>],
    records: usize,
) -> u64 {
    let mut digest = FNV_OFFSET;
    for index in 0..HISTORICAL_READS {
        let snapshot = &snapshots[index % snapshots.len()];
        let key = if index % 2 == 0 {
            key_for_id((index % records) * 2)
        } else {
            key_for_id(records * 4 + index)
        };
        digest = digest_bytes(digest, &key);
        match snapshot
            .get(black_box(&key))
            .expect("historical read succeeds")
        {
            Some(value) => {
                digest = digest_bytes(digest, &[1]);
                digest = digest_bytes(digest, &value);
            }
            None => digest = digest_bytes(digest, &[0]),
        }
    }
    digest_u64(digest, HISTORICAL_READS as u64)
}

fn historical_scan_digest<S: prolly::Store>(
    snapshots: &[prolly::MapSnapshot<'_, S>],
    indexes: &[usize],
) -> (usize, u64) {
    let mut count = 0usize;
    let mut digest = FNV_OFFSET;
    for index in indexes {
        for entry in snapshots[*index]
            .range(&[], None)
            .expect("historical range opens")
        {
            let (key, value) = entry.expect("historical range entry reads");
            digest = digest_entry(digest, &key, &value);
            count += 1;
        }
    }
    (count, digest_u64(digest, count as u64))
}

fn tree_summary(manager: &Prolly<Arc<MemStore>>, version: &MapVersion) -> (usize, u64) {
    let mut count = 0usize;
    let mut digest = FNV_OFFSET;
    manager
        .read(&version.tree)
        .expect("version read session opens")
        .scan_range(&[], None, |entry| {
            digest = digest_entry(digest, entry.key(), entry.value());
            count += 1;
        })
        .expect("version tree scans");
    (count, digest_u64(digest, count as u64))
}

fn digest_versions(versions: &[MapVersion]) -> u64 {
    let mut digest = FNV_OFFSET;
    for version in versions {
        digest = digest_bytes(digest, version.id.as_cid().as_bytes());
    }
    digest_u64(digest, versions.len() as u64)
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
        "rust-lifecycle,{revision},{},{},{},{},{},{},{},{},{ns_per_op:.3},{ops_per_sec:.3},{:016x},{:016x},{},{},{},0,true",
        support::CONTRACT_VERSION,
        args.records,
        args.density,
        args.locality.name(),
        row.operation,
        args.scenario.name(),
        operations,
        row.elapsed_ns,
        row.workload_digest,
        row.result_digest,
        row.result_count,
        row.base_count,
        row.target_count,
    );
}
