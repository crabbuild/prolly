#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::PathBuf;

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const READ_OPERATIONS: usize = 1_000_000;

#[derive(Clone, Debug, PartialEq)]
pub struct CsvRow {
    pub version: String,
    pub profile: String,
    pub records: usize,
    pub run: usize,
    pub workload: String,
    pub operations: usize,
    pub total_ns: u128,
    pub ns_per_op: f64,
    pub ops_per_sec: f64,
    pub nodes_read: u64,
    pub nodes_written: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_evictions: u64,
    pub result_entries: usize,
    pub num_nodes: usize,
    pub num_leaves: usize,
    pub num_internal: usize,
    pub height: usize,
    pub tree_bytes: usize,
    pub db_bytes_before: u64,
    pub db_bytes_after: u64,
    pub wal_bytes_after: u64,
    pub shm_bytes_after: u64,
    pub fixture_bytes_after: u64,
    pub sqlite_node_count: u64,
    pub sqlite_node_payload_bytes: u64,
    pub validated: bool,
    pub status: String,
}

impl CsvRow {
    pub fn header() -> &'static str {
        "version,profile,records,run,workload,operations,total_ns,ns_per_op,ops_per_sec,nodes_read,nodes_written,bytes_read,bytes_written,cache_hits,cache_misses,cache_evictions,result_entries,num_nodes,num_leaves,num_internal,height,tree_bytes,db_bytes_before,db_bytes_after,wal_bytes_after,shm_bytes_after,fixture_bytes_after,sqlite_node_count,sqlite_node_payload_bytes,validated,status"
    }

    pub fn to_csv(&self) -> String {
        format!(
            "{},{},{},{},{},{},{},{:.3},{:.3},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
            self.version,
            self.profile,
            self.records,
            self.run,
            self.workload,
            self.operations,
            self.total_ns,
            self.ns_per_op,
            self.ops_per_sec,
            self.nodes_read,
            self.nodes_written,
            self.bytes_read,
            self.bytes_written,
            self.cache_hits,
            self.cache_misses,
            self.cache_evictions,
            self.result_entries,
            self.num_nodes,
            self.num_leaves,
            self.num_internal,
            self.height,
            self.tree_bytes,
            self.db_bytes_before,
            self.db_bytes_after,
            self.wal_bytes_after,
            self.shm_bytes_after,
            self.fixture_bytes_after,
            self.sqlite_node_count,
            self.sqlite_node_payload_bytes,
            self.validated,
            self.status.replace(',', ";")
        )
    }

    pub fn example() -> Self {
        Self {
            version: "current".to_string(),
            profile: "full".to_string(),
            records: 1_000,
            run: 1,
            workload: "sorted_stream_build".to_string(),
            operations: 1_000,
            total_ns: 1_000_000,
            ns_per_op: 1_000.0,
            ops_per_sec: 1_000_000.0,
            nodes_read: 0,
            nodes_written: 1,
            bytes_read: 0,
            bytes_written: 1_024,
            cache_hits: 0,
            cache_misses: 0,
            cache_evictions: 0,
            result_entries: 1_000,
            num_nodes: 1,
            num_leaves: 1,
            num_internal: 0,
            height: 1,
            tree_bytes: 1_024,
            db_bytes_before: 0,
            db_bytes_after: 4_096,
            wal_bytes_after: 0,
            shm_bytes_after: 0,
            fixture_bytes_after: 4_096,
            sqlite_node_count: 1,
            sqlite_node_payload_bytes: 1_024,
            validated: true,
            status: "ok".to_string(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityProfile {
    Full,
    Normal,
}

impl DurabilityProfile {
    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "full" => Ok(Self::Full),
            "normal" => Ok(Self::Normal),
            other => Err(format!("unknown SQLite durability profile: {other}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Full => "full",
            Self::Normal => "normal",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Workload {
    SortedStreamBuild,
    ShuffledBatchBuild,
    RandomReadsColdManager,
    RandomReadsWarmManager,
    ClusteredReadsColdManager,
    ClusteredReadsWarmManager,
    RightEdgeReadsColdManager,
    RightEdgeReadsWarmManager,
    AppendBatchUpserts,
    RandomBatchUpdates,
    ClusteredBatchUpdates,
    RandomBatchDeletes,
    ClusteredBatchDeletes,
    IdenticalDiff,
    AppendSparseDiff,
    RandomSparseDiff,
    ClusteredSparseDiff,
    RandomDeleteDiff,
    ClusteredDeleteDiff,
    AppendDisjointSparseMerge,
    RandomDisjointSparseMerge,
    ClusteredDisjointSparseMerge,
    RandomConflictResolvedMerge,
    ClusteredConflictResolvedMerge,
}

impl Workload {
    pub const ALL_NAMES: &'static [&'static str] = &[
        "sorted_stream_build",
        "shuffled_batch_build",
        "random_reads_cold_manager",
        "random_reads_warm_manager",
        "clustered_reads_cold_manager",
        "clustered_reads_warm_manager",
        "right_edge_reads_cold_manager",
        "right_edge_reads_warm_manager",
        "append_batch_upserts",
        "random_batch_updates",
        "clustered_batch_updates",
        "random_batch_deletes",
        "clustered_batch_deletes",
        "identical_diff",
        "append_sparse_diff",
        "random_sparse_diff",
        "clustered_sparse_diff",
        "random_delete_diff",
        "clustered_delete_diff",
        "append_disjoint_sparse_merge",
        "random_disjoint_sparse_merge",
        "clustered_disjoint_sparse_merge",
        "random_conflict_resolved_merge",
        "clustered_conflict_resolved_merge",
    ];

    pub fn parse(value: &str) -> Result<Self, String> {
        match value {
            "sorted_stream_build" => Ok(Self::SortedStreamBuild),
            "shuffled_batch_build" => Ok(Self::ShuffledBatchBuild),
            "random_reads_cold_manager" => Ok(Self::RandomReadsColdManager),
            "random_reads_warm_manager" => Ok(Self::RandomReadsWarmManager),
            "clustered_reads_cold_manager" => Ok(Self::ClusteredReadsColdManager),
            "clustered_reads_warm_manager" => Ok(Self::ClusteredReadsWarmManager),
            "right_edge_reads_cold_manager" => Ok(Self::RightEdgeReadsColdManager),
            "right_edge_reads_warm_manager" => Ok(Self::RightEdgeReadsWarmManager),
            "append_batch_upserts" => Ok(Self::AppendBatchUpserts),
            "random_batch_updates" => Ok(Self::RandomBatchUpdates),
            "clustered_batch_updates" => Ok(Self::ClusteredBatchUpdates),
            "random_batch_deletes" => Ok(Self::RandomBatchDeletes),
            "clustered_batch_deletes" => Ok(Self::ClusteredBatchDeletes),
            "identical_diff" => Ok(Self::IdenticalDiff),
            "append_sparse_diff" => Ok(Self::AppendSparseDiff),
            "random_sparse_diff" => Ok(Self::RandomSparseDiff),
            "clustered_sparse_diff" => Ok(Self::ClusteredSparseDiff),
            "random_delete_diff" => Ok(Self::RandomDeleteDiff),
            "clustered_delete_diff" => Ok(Self::ClusteredDeleteDiff),
            "append_disjoint_sparse_merge" => Ok(Self::AppendDisjointSparseMerge),
            "random_disjoint_sparse_merge" => Ok(Self::RandomDisjointSparseMerge),
            "clustered_disjoint_sparse_merge" => Ok(Self::ClusteredDisjointSparseMerge),
            "random_conflict_resolved_merge" => Ok(Self::RandomConflictResolvedMerge),
            "clustered_conflict_resolved_merge" => Ok(Self::ClusteredConflictResolvedMerge),
            other => Err(format!("unknown SQLite workload: {other}")),
        }
    }

    pub fn as_str(self) -> &'static str {
        Self::ALL_NAMES[self as usize]
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BenchArgs {
    pub workload: Workload,
    pub records: usize,
    pub profile: DurabilityProfile,
    pub version: String,
    pub run: usize,
    pub db_path: PathBuf,
}

impl BenchArgs {
    pub fn from_env() -> Result<Self, String> {
        let required = |name: &str| {
            std::env::var(name).map_err(|_| format!("missing required environment variable {name}"))
        };
        let workload = Workload::parse(&required("PROLLY_SQLITE_WORKLOAD")?)?;
        let records = required("PROLLY_SQLITE_RECORDS")?
            .parse::<usize>()
            .map_err(|err| format!("invalid PROLLY_SQLITE_RECORDS: {err}"))?;
        if records == 0 {
            return Err("PROLLY_SQLITE_RECORDS must be positive".to_string());
        }
        let profile = DurabilityProfile::parse(&required("PROLLY_SQLITE_PROFILE")?)?;
        let version = required("PROLLY_SQLITE_VERSION")?;
        let run = required("PROLLY_SQLITE_RUN")?
            .parse::<usize>()
            .map_err(|err| format!("invalid PROLLY_SQLITE_RUN: {err}"))?;
        let db_path = PathBuf::from(required("PROLLY_SQLITE_DB")?);
        Ok(Self {
            workload,
            records,
            profile,
            version,
            run,
            db_path,
        })
    }
}

pub fn sample_count(records: usize) -> usize {
    records.min((records / 100).max(100)).min(10_000)
}

pub fn merge_count(records: usize) -> usize {
    (sample_count(records) / 2).max(50).min(records)
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("value-{id:020}-{generation:02}-payload").into_bytes()
}

pub fn random_indexes(records: usize, count: usize, seed: u64) -> Vec<usize> {
    let wanted = count.min(records);
    let mut state = seed;
    let mut indexes = BTreeSet::new();
    while indexes.len() < wanted {
        indexes.insert((next_random(&mut state) as usize) % records);
    }
    indexes.into_iter().collect()
}

pub fn clustered_indexes(records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    let start = records.saturating_sub(wanted) / 2;
    (start..start + wanted).collect()
}

pub fn right_edge_indexes(records: usize, count: usize) -> Vec<usize> {
    let wanted = count.min(records);
    (records - wanted..records).collect()
}

pub fn mutation_indexes(workload: Workload, records: usize, count: usize) -> Vec<usize> {
    match workload {
        Workload::AppendBatchUpserts => (records..records + count).collect(),
        Workload::RandomBatchUpdates | Workload::RandomBatchDeletes => {
            random_indexes(records, count, RANDOM_SEED)
        }
        Workload::ClusteredBatchUpdates | Workload::ClusteredBatchDeletes => {
            clustered_indexes(records, count)
        }
        _ => Vec::new(),
    }
}

pub fn expected_result_entries(workload: Workload, records: usize, count: usize) -> usize {
    match workload {
        Workload::AppendBatchUpserts => records.saturating_add(count),
        Workload::RandomBatchDeletes | Workload::ClusteredBatchDeletes => {
            records.saturating_sub(count.min(records))
        }
        _ => records,
    }
}

pub fn merge_branch_indexes(
    workload: Workload,
    records: usize,
    count: usize,
) -> (Vec<usize>, Vec<usize>) {
    match workload {
        Workload::AppendDisjointSparseMerge => (
            (records..records + count).collect(),
            (records + count..records + count.saturating_mul(2)).collect(),
        ),
        Workload::RandomDisjointSparseMerge => {
            let combined = random_indexes(records, count.saturating_mul(2), RANDOM_SEED);
            let split = combined.len() / 2;
            (combined[..split].to_vec(), combined[split..].to_vec())
        }
        Workload::ClusteredDisjointSparseMerge => {
            let combined = clustered_indexes(records, count.saturating_mul(2));
            let split = combined.len() / 2;
            (combined[..split].to_vec(), combined[split..].to_vec())
        }
        Workload::RandomConflictResolvedMerge => {
            let indexes = random_indexes(records, count, RANDOM_SEED);
            (indexes.clone(), indexes)
        }
        Workload::ClusteredConflictResolvedMerge => {
            let indexes = clustered_indexes(records, count);
            (indexes.clone(), indexes)
        }
        _ => (Vec::new(), Vec::new()),
    }
}

pub fn expected_merge_entries(workload: Workload, records: usize, count: usize) -> usize {
    match workload {
        Workload::AppendDisjointSparseMerge => records.saturating_add(count.saturating_mul(2)),
        _ => records,
    }
}

pub fn shuffled_ids(records: usize, seed: u64) -> Vec<usize> {
    let mut ids = (0..records).collect::<Vec<_>>();
    let mut state = seed;
    for index in (1..ids.len()).rev() {
        let replacement = (next_random(&mut state) as usize) % (index + 1);
        ids.swap(index, replacement);
    }
    ids
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}
