use std::env;
use std::hint::black_box;
use std::sync::Arc;
use std::time::{Duration, Instant};

use prolly::{Config, MemStore, Mutation, Prolly, Tree};

const CLUSTER_SIZE: usize = 1_000;
const CONTRACT_VERSION: &str = "prolly-compare-v1";
const DEFAULT_POINT_READS: usize = 100_000;
const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Fresh,
    Mutation,
}

impl Phase {
    fn parse(value: &str) -> Self {
        match value {
            "fresh" => Self::Fresh,
            "mutation" => Self::Mutation,
            _ => panic!("invalid phase {value:?}; expected fresh or mutation"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Fresh => "fresh",
            Self::Mutation => "mutation",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Workload {
    Append,
    Random,
    Clustered,
}

impl Workload {
    fn parse(value: &str) -> Self {
        match value {
            "append" => Self::Append,
            "random" => Self::Random,
            "clustered" => Self::Clustered,
            _ => panic!("invalid workload {value:?}; expected append, random, or clustered"),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
        }
    }
}

struct Args {
    records: usize,
    phase: Phase,
    workload: Workload,
}

fn main() {
    let args = parse_args();
    assert!(
        args.records >= CLUSTER_SIZE && args.records % CLUSTER_SIZE == 0,
        "records must be a positive multiple of {CLUSTER_SIZE}"
    );

    let revision = env::var("BENCH_REVISION").unwrap_or_else(|_| "unknown".to_string());
    let result = run_scenario(&args);

    println!("{}", csv_header());
    emit(
        &revision,
        &args,
        "write",
        result.write_operations,
        result.write_elapsed,
        result.digest,
        result.result_count,
    );
    emit(
        &revision,
        &args,
        "point_read",
        result.read_operations,
        result.read_elapsed,
        result.digest,
        result.result_count,
    );
    emit(
        &revision,
        &args,
        "range_scan",
        result.scan_operations,
        result.scan_elapsed,
        result.digest,
        result.result_count,
    );
}

struct ScenarioResult {
    write_operations: usize,
    write_elapsed: Duration,
    read_operations: usize,
    read_elapsed: Duration,
    scan_operations: usize,
    scan_elapsed: Duration,
    digest: u64,
    result_count: usize,
}

fn run_scenario(args: &Args) -> ScenarioResult {
    let store = Arc::new(MemStore::new());
    let manager = Prolly::new(store, Config::default());
    let (tree, write_operations, write_elapsed, digest, result_count) = match args.phase {
        Phase::Fresh => {
            let (tree, elapsed, digest) = build_fresh(&manager, args.records, args.workload);
            (tree, args.records, elapsed, digest, args.records)
        }
        Phase::Mutation => {
            let (base, _, _) = build_fresh(&manager, args.records, Workload::Append);
            let writes = args.records * 30 / 100;
            let (tree, elapsed, digest) =
                apply_mutations(&manager, base, args.records, writes, args.workload);
            let inserts = match args.workload {
                Workload::Append => writes,
                Workload::Random | Workload::Clustered => writes - writes / 2,
            };
            (tree, writes, elapsed, digest, args.records + inserts)
        }
    };

    let mut validation_reader = manager.read(&tree).expect("validation reader opens");
    let mut previous: Option<Vec<u8>> = None;
    let actual_count = validation_reader
        .scan_range(&[], None, |entry| {
            if let Some(previous) = previous.as_ref() {
                assert!(
                    previous.as_slice() < entry.key(),
                    "range keys are not strictly sorted"
                );
            }
            previous = Some(entry.key().to_vec());
        })
        .expect("count range succeeds") as usize;
    assert_eq!(
        actual_count, result_count,
        "post-write cardinality mismatch"
    );

    let point_reads = env::var("PROLLY_COMPARE_POINT_READS")
        .ok()
        .map(|value| {
            value
                .parse::<usize>()
                .expect("PROLLY_COMPARE_POINT_READS must be an integer")
        })
        .unwrap_or(DEFAULT_POINT_READS);
    let targets = read_targets(
        args.phase,
        args.workload,
        args.records,
        write_operations,
        point_reads,
    );
    for (key, expected) in &targets {
        assert_eq!(
            validation_reader
                .get_with(key, |value| value == expected.as_slice())
                .expect("warm point read succeeds"),
            Some(true),
            "warm point-read value mismatch for {:?}",
            String::from_utf8_lossy(key)
        );
    }

    let mut read_session = manager.read(&tree).expect("point read session opens");
    let read_started = Instant::now();
    let mut observed_bytes = 0usize;
    for (key, expected) in &targets {
        let found = read_session
            .get_with(black_box(key), |value| {
                assert_eq!(value, expected);
                observed_bytes = observed_bytes.wrapping_add(value.len());
                black_box(value);
            })
            .expect("point read succeeds")
            .is_some();
        assert!(found, "point-read key exists");
    }
    let read_elapsed = read_started.elapsed();
    black_box(observed_bytes);

    let scan_started = Instant::now();
    let mut scan_count = 0usize;
    let mut scanned_bytes = 0usize;
    let mut scan_session = manager.read(&tree).expect("range scan session opens");
    scan_session
        .scan_range(&[], None, |entry| {
            scanned_bytes = scanned_bytes
                .wrapping_add(entry.key().len())
                .wrapping_add(entry.value().len());
            scan_count += 1;
        })
        .expect("range scan succeeds");
    let scan_elapsed = scan_started.elapsed();
    assert_eq!(scan_count, result_count, "range scan cardinality mismatch");
    black_box(scanned_bytes);

    ScenarioResult {
        write_operations,
        write_elapsed,
        read_operations: targets.len(),
        read_elapsed,
        scan_operations: scan_count,
        scan_elapsed,
        digest,
        result_count,
    }
}

fn build_fresh(
    manager: &Prolly<Arc<MemStore>>,
    records: usize,
    workload: Workload,
) -> (Tree, Duration, u64) {
    let mut tree = manager.create();
    let mut elapsed = Duration::ZERO;
    let mut digest = FNV_OFFSET;
    let mut batch = Vec::with_capacity(records);

    for index in 0..records {
        let id = fresh_id(workload, index, records);
        let key = key_for_position(id * 2);
        let value = value_for_position(id * 2, 0);
        digest = digest_operation(digest, &key, &value);
        batch.push(Mutation::Upsert { key, val: value });
        if index + 1 == records {
            let started = Instant::now();
            tree = manager
                .batch(&tree, black_box(std::mem::take(&mut batch)))
                .expect("fresh write batch succeeds");
            elapsed += started.elapsed();
        }
    }
    (tree, elapsed, digest)
}

fn apply_mutations(
    manager: &Prolly<Arc<MemStore>>,
    mut tree: Tree,
    records: usize,
    writes: usize,
    workload: Workload,
) -> (Tree, Duration, u64) {
    let mut elapsed = Duration::ZERO;
    let mut digest = FNV_OFFSET;
    let mut batch = Vec::with_capacity(writes);

    for index in 0..writes {
        let position = mutation_position(workload, index, records, writes);
        let key = key_for_position(position);
        let value = value_for_position(position, 1);
        digest = digest_operation(digest, &key, &value);
        batch.push(Mutation::Upsert { key, val: value });
        if index + 1 == writes {
            let started = Instant::now();
            tree = if workload == Workload::Append {
                manager
                    .append_batch(&tree, black_box(std::mem::take(&mut batch)))
                    .expect("append batch succeeds")
            } else {
                manager
                    .batch(&tree, black_box(std::mem::take(&mut batch)))
                    .expect("mutation batch succeeds")
            };
            elapsed += started.elapsed();
        }
    }
    (tree, elapsed, digest)
}

fn fresh_id(workload: Workload, index: usize, records: usize) -> usize {
    match workload {
        Workload::Append => index,
        Workload::Random => permute(index, records, RANDOM_SEED ^ records as u64),
        Workload::Clustered => {
            let blocks = records / CLUSTER_SIZE;
            let block = index / CLUSTER_SIZE;
            let offset = index % CLUSTER_SIZE;
            permute(block, blocks, RANDOM_SEED ^ 0xc1a5_7e2d) * CLUSTER_SIZE + offset
        }
    }
}

fn mutation_position(workload: Workload, index: usize, records: usize, writes: usize) -> usize {
    match workload {
        Workload::Append => records * 2 + index,
        Workload::Random => {
            let ordinal = index / 2;
            if index % 2 == 0 {
                permute(ordinal, records, RANDOM_SEED ^ 0xa11c_e001) * 2
            } else {
                permute(ordinal, records, RANDOM_SEED ^ 0x1a5e_2701) * 2 + 1
            }
        }
        Workload::Clustered => {
            let updates = writes / 2;
            let inserts = writes - updates;
            let width = updates.max(inserts);
            let start = (records - width) / 2;
            let ordinal = index / 2;
            if index % 2 == 0 {
                (start + ordinal) * 2
            } else {
                (start + ordinal) * 2 + 1
            }
        }
    }
}

fn read_targets(
    phase: Phase,
    workload: Workload,
    records: usize,
    writes: usize,
    point_reads: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let count = point_reads.min(match phase {
        Phase::Fresh => records,
        Phase::Mutation => records + writes,
    });
    let mut targets = Vec::with_capacity(count);
    for index in 0..count {
        let (position, generation) = match phase {
            Phase::Fresh => {
                let id = permute(index % records, records, RANDOM_SEED ^ 0x5ead_0001);
                (id * 2, 0)
            }
            Phase::Mutation => mutation_read_target(workload, index, records, writes),
        };
        targets.push((
            key_for_position(position),
            value_for_position(position, generation),
        ));
    }
    targets
}

fn mutation_read_target(
    workload: Workload,
    index: usize,
    records: usize,
    writes: usize,
) -> (usize, u64) {
    match workload {
        Workload::Append => {
            if index % 2 == 0 {
                let id = (index / 2) % records;
                (id * 2, 0)
            } else {
                let id = (index / 2) % writes;
                (records * 2 + id, 1)
            }
        }
        Workload::Random | Workload::Clustered => {
            let updates = writes / 2;
            let inserts = writes - updates;
            match index % 3 {
                0 => {
                    let op = 2 * ((index / 3) % updates);
                    (mutation_position(workload, op, records, writes), 1)
                }
                1 => {
                    let op = 2 * ((index / 3) % inserts) + 1;
                    (mutation_position(workload, op, records, writes), 1)
                }
                _ => {
                    let unchanged_ordinal = (index / 3) % (records - updates);
                    let id = match workload {
                        Workload::Random => permute(
                            updates + unchanged_ordinal,
                            records,
                            RANDOM_SEED ^ 0xa11c_e001,
                        ),
                        Workload::Clustered => {
                            let width = updates.max(inserts);
                            let start = (records - width) / 2;
                            unchanged_ordinal % start
                        }
                        Workload::Append => unreachable!(),
                    };
                    (id * 2, 0)
                }
            }
        }
    }
}

fn key_for_position(position: usize) -> Vec<u8> {
    format!("key-{position:020}").into_bytes()
}

fn value_for_position(position: usize, generation: u64) -> Vec<u8> {
    let mut state = mix64(position as u64 ^ generation.wrapping_mul(0x9e37_79b9_7f4a_7c15));
    let len = (state % 100 + 1) as usize;
    let mut value = Vec::with_capacity(len);
    for index in 0..len {
        state = mix64(state.wrapping_add(index as u64).wrapping_add(0x9e37_79b9));
        value.push(state as u8);
    }
    value
}

fn permute(index: usize, count: usize, seed: u64) -> usize {
    if count <= 1 {
        return 0;
    }
    let mut multiplier = (mix64(seed) as usize % count) | 1;
    while gcd(multiplier, count) != 1 {
        multiplier = (multiplier + 2) % count;
        if multiplier == 0 {
            multiplier = 1;
        }
    }
    let offset = mix64(seed ^ 0xd1b5_4a32_d192_ed03) as usize % count;
    (multiplier * index + offset) % count
}

fn gcd(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        let remainder = left % right;
        left = right;
        right = remainder;
    }
    left
}

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn digest_operation(mut digest: u64, key: &[u8], value: &[u8]) -> u64 {
    digest = digest_bytes(digest, &(key.len() as u32).to_be_bytes());
    digest = digest_bytes(digest, key);
    digest = digest_bytes(digest, &(value.len() as u32).to_be_bytes());
    digest_bytes(digest, value)
}

fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in bytes {
        digest ^= u64::from(*byte);
        digest = digest.wrapping_mul(FNV_PRIME);
    }
    digest
}

#[cfg(test)]
fn workload_digest(phase: Phase, workload: Workload, records: usize) -> u64 {
    let operations = match phase {
        Phase::Fresh => records,
        Phase::Mutation => records * 30 / 100,
    };
    let mut digest = FNV_OFFSET;
    for index in 0..operations {
        let (position, generation) = match phase {
            Phase::Fresh => (fresh_id(workload, index, records) * 2, 0),
            Phase::Mutation => (mutation_position(workload, index, records, operations), 1),
        };
        let key = key_for_position(position);
        let value = value_for_position(position, generation);
        digest = digest_operation(digest, &key, &value);
    }
    digest
}

fn csv_header() -> &'static str {
    "implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated"
}

fn emit(
    revision: &str,
    args: &Args,
    operation: &str,
    operations: usize,
    elapsed: Duration,
    digest: u64,
    result_count: usize,
) {
    let elapsed_ns = elapsed.as_nanos();
    let ns_per_op = elapsed_ns as f64 / operations.max(1) as f64;
    let ops_per_sec = operations as f64 * 1_000_000_000.0 / elapsed_ns.max(1) as f64;
    println!(
        "rust,{revision},{CONTRACT_VERSION},{},{},{},{operation},{operations},{elapsed_ns},{ns_per_op:.3},{ops_per_sec:.3},{digest:016x},{result_count},true",
        args.records,
        args.phase.name(),
        args.workload.name(),
    );
}

fn parse_args() -> Args {
    let mut records = None;
    let mut phase = None;
    let mut workload = None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--records" => records = Some(value.parse().expect("records must be an integer")),
            "--phase" => phase = Some(Phase::parse(&value)),
            "--workload" => workload = Some(Workload::parse(&value)),
            _ => panic!("unknown argument {flag}"),
        }
    }
    Args {
        records: records.expect("--records is required"),
        phase: phase.expect("--phase is required"),
        workload: workload.expect("--workload is required"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn permutation_is_unique_for_requested_scales() {
        for count in [10_000, 50_000, 1_000_000] {
            let values = (0..count)
                .map(|index| permute(index, count, RANDOM_SEED))
                .collect::<BTreeSet<_>>();
            assert_eq!(values.len(), count);
            assert_eq!(values.first(), Some(&0));
            assert_eq!(values.last(), Some(&(count - 1)));
        }
    }

    #[test]
    fn values_are_deterministic_and_within_requested_size() {
        let first = value_for_position(42, 0);
        assert_eq!(first, value_for_position(42, 0));
        assert_ne!(first, value_for_position(42, 1));
        assert!((1..=100).contains(&first.len()));
    }

    #[test]
    fn mutation_mix_has_equal_updates_and_inserts() {
        let records = 10_000;
        let writes = records * 30 / 100;
        for workload in [Workload::Random, Workload::Clustered] {
            let positions = (0..writes)
                .map(|index| mutation_position(workload, index, records, writes))
                .collect::<Vec<_>>();
            assert_eq!(
                positions
                    .iter()
                    .filter(|position| **position % 2 == 0)
                    .count(),
                1_500,
                "{workload:?} update count"
            );
            assert_eq!(
                positions
                    .iter()
                    .filter(|position| **position % 2 == 1)
                    .count(),
                1_500,
                "{workload:?} insert count"
            );
            assert_eq!(
                positions.iter().collect::<BTreeSet<_>>().len(),
                writes,
                "{workload:?} mutation positions must be unique"
            );
        }
    }

    #[test]
    fn workload_contract_has_stable_digests() {
        let cases = [
            (Phase::Fresh, Workload::Append, 0x51f5_5fcd_5918_7cbf),
            (Phase::Fresh, Workload::Random, 0x0041_97dd_790a_1245),
            (Phase::Fresh, Workload::Clustered, 0x86e3_8047_f6ae_04b3),
            (Phase::Mutation, Workload::Append, 0x2ef1_df79_e122_6620),
            (Phase::Mutation, Workload::Random, 0x3bc7_e45e_f276_a1c5),
            (Phase::Mutation, Workload::Clustered, 0x5cae_d8db_d305_6277),
        ];
        for (phase, workload, expected) in cases {
            assert_eq!(
                workload_digest(phase, workload, 10_000),
                expected,
                "{phase:?}/{workload:?}"
            );
        }
    }

    #[test]
    fn csv_schema_includes_contract_version() {
        assert_eq!(CONTRACT_VERSION, "prolly-compare-v1");
        assert_eq!(
            csv_header(),
            "implementation,revision,contract_version,records,phase,workload,operation,operations,elapsed_ns,ns_per_op,ops_per_sec,workload_digest,result_count,validated"
        );
    }
}
