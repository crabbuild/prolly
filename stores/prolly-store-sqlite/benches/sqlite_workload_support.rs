#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::PathBuf;

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const READ_OPERATIONS: usize = 1_000_000;

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
