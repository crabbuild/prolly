use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::FromStr;

use prolly::Mutation;
use serde::Serialize;

pub const RANDOM_SEED: u64 = 0x243f_6a88_85a3_08d3;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Adapter {
    MemorySync,
    FileSync,
    SqliteSync,
    RocksdbSync,
    SlatedbSync,
    PgliteSync,
    TursoAsync,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Api {
    Put,
    Batch,
    Build,
    Diff,
    Merge,
    Reopen,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Pattern {
    Append,
    Random,
    Clustered,
}

#[derive(Clone, Debug)]
pub struct RunConfig {
    pub output: PathBuf,
    pub records: usize,
    pub changes: usize,
    pub runs: usize,
    pub adapters: Vec<Adapter>,
    pub apis: Vec<Api>,
    pub patterns: Vec<Pattern>,
    pub revision: String,
}

#[derive(Clone, Copy, Debug)]
pub struct CellSpec {
    pub adapter: Adapter,
    pub records: usize,
    pub changes: usize,
    pub run: usize,
    pub api: Api,
    pub pattern: Pattern,
}

#[derive(Debug, Serialize)]
pub struct ResultRow {
    pub revision: String,
    pub adapter: Adapter,
    pub records: usize,
    pub changes: usize,
    pub api: Api,
    pub pattern: Pattern,
    pub run: usize,
    pub total_ns: u128,
    pub operations_per_sec: f64,
    pub p50_ns: u128,
    pub p95_ns: u128,
    pub root: String,
    pub node_count: usize,
    pub byte_count: usize,
    pub value_valid: bool,
    pub count_valid: bool,
    pub root_valid: bool,
    pub reopen_valid: bool,
}

impl Adapter {
    pub const ALL: [Self; 7] = [
        Self::MemorySync,
        Self::FileSync,
        Self::SqliteSync,
        Self::RocksdbSync,
        Self::SlatedbSync,
        Self::PgliteSync,
        Self::TursoAsync,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::MemorySync => "memory-sync",
            Self::FileSync => "file-sync",
            Self::SqliteSync => "sqlite-sync",
            Self::RocksdbSync => "rocksdb-sync",
            Self::SlatedbSync => "slatedb-sync",
            Self::PgliteSync => "pglite-sync",
            Self::TursoAsync => "turso-async",
        }
    }
}

impl Api {
    pub const ALL: [Self; 6] = [
        Self::Put,
        Self::Batch,
        Self::Build,
        Self::Diff,
        Self::Merge,
        Self::Reopen,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Put => "put",
            Self::Batch => "batch",
            Self::Build => "build",
            Self::Diff => "diff",
            Self::Merge => "merge",
            Self::Reopen => "reopen",
        }
    }
}

impl Pattern {
    pub const ALL: [Self; 3] = [Self::Append, Self::Random, Self::Clustered];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
        }
    }
}

impl FromStr for Adapter {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "memory-sync" => Ok(Self::MemorySync),
            "file-sync" => Ok(Self::FileSync),
            "sqlite-sync" => Ok(Self::SqliteSync),
            "rocksdb-sync" => Ok(Self::RocksdbSync),
            "slatedb-sync" => Ok(Self::SlatedbSync),
            "pglite-sync" => Ok(Self::PgliteSync),
            "turso-async" => Ok(Self::TursoAsync),
            _ => Err(format!("unknown adapter: {value}")),
        }
    }
}

impl FromStr for Api {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "put" => Ok(Self::Put),
            "batch" => Ok(Self::Batch),
            "build" => Ok(Self::Build),
            "diff" => Ok(Self::Diff),
            "merge" => Ok(Self::Merge),
            "reopen" => Ok(Self::Reopen),
            _ => Err(format!("unknown API: {value}")),
        }
    }
}

impl FromStr for Pattern {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "append" => Ok(Self::Append),
            "random" => Ok(Self::Random),
            "clustered" => Ok(Self::Clustered),
            _ => Err(format!("unknown pattern: {value}")),
        }
    }
}

impl RunConfig {
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self, String> {
        let values = args.into_iter().collect::<Vec<_>>();
        let mut output = None;
        let mut records = None;
        let mut changes = None;
        let mut runs = None;
        let mut adapters = None;
        let mut apis = None;
        let mut patterns = None;
        let mut revision = None;
        let mut index = 1;
        while index < values.len() {
            let flag = values[index].as_str();
            let value = values
                .get(index + 1)
                .ok_or_else(|| format!("{flag} requires a value"))?;
            match flag {
                "--output" => output = Some(PathBuf::from(value)),
                "--records" => records = Some(parse_positive(value, flag)?),
                "--changes" => changes = Some(parse_positive(value, flag)?),
                "--runs" => runs = Some(parse_positive(value, flag)?),
                "--adapters" => adapters = Some(parse_list(value)?),
                "--apis" => apis = Some(parse_list(value)?),
                "--patterns" => patterns = Some(parse_list(value)?),
                "--revision" => revision = Some(value.clone()),
                _ => return Err(format!("unknown option: {flag}\n{}", usage())),
            }
            index += 2;
        }
        let config = Self {
            output: output.ok_or_else(|| usage().to_string())?,
            records: records.ok_or_else(|| usage().to_string())?,
            changes: changes.ok_or_else(|| usage().to_string())?,
            runs: runs.ok_or_else(|| usage().to_string())?,
            adapters: adapters.unwrap_or_else(|| Adapter::ALL.to_vec()),
            apis: apis.unwrap_or_else(|| Api::ALL.to_vec()),
            patterns: patterns.unwrap_or_else(|| Pattern::ALL.to_vec()),
            revision: revision.unwrap_or_else(|| "unknown".to_string()),
        };
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> Result<(), String> {
        if self.changes.saturating_mul(2) > self.records {
            return Err("changes must leave two disjoint in-range merge branches".to_string());
        }
        if self.revision.is_empty() {
            return Err("revision must not be empty".to_string());
        }
        ensure_unique(&self.adapters, "adapters")?;
        ensure_unique(&self.apis, "APIs")?;
        ensure_unique(&self.patterns, "patterns")
    }
}

pub fn usage() -> &'static str {
    "usage: prolly-local-store-publication-bench --output PATH --records N --changes N --runs N --adapters CSV --apis CSV --patterns CSV --revision TEXT"
}

fn parse_positive(value: &str, flag: &str) -> Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {flag} value {value}: {error}"))?;
    if parsed == 0 {
        Err(format!("{flag} must be positive"))
    } else {
        Ok(parsed)
    }
}

fn parse_list<T: FromStr<Err = String>>(value: &str) -> Result<Vec<T>, String> {
    if value.is_empty() {
        return Err("list must not be empty".to_string());
    }
    value.split(',').map(str::parse).collect()
}

fn ensure_unique<T: Copy + Ord>(values: &[T], name: &str) -> Result<(), String> {
    if values.is_empty() {
        return Err(format!("{name} must not be empty"));
    }
    if values.iter().copied().collect::<BTreeSet<_>>().len() != values.len() {
        return Err(format!("{name} contains a duplicate"));
    }
    Ok(())
}

pub fn key(index: usize) -> Vec<u8> {
    format!("key-{index:016x}").into_bytes()
}

pub fn value(index: usize, generation: u8) -> Vec<u8> {
    format!("value-{generation:02x}-{index:016x}").into_bytes()
}

pub fn base_entries(records: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    (0..records)
        .map(|index| (key(index), value(index, 0)))
        .collect()
}

pub fn mutation_ids(pattern: Pattern, records: usize, changes: usize) -> Vec<usize> {
    match pattern {
        Pattern::Append => (records..records + changes).collect(),
        Pattern::Clustered => {
            let start = records.saturating_sub(changes) / 2;
            (start..start + changes).collect()
        }
        Pattern::Random => random_ids(records, changes, RANDOM_SEED),
    }
}

pub fn merge_ids(pattern: Pattern, records: usize, changes: usize) -> (Vec<usize>, Vec<usize>) {
    match pattern {
        Pattern::Append => (
            (records..records + changes).collect(),
            (records + changes..records + changes * 2).collect(),
        ),
        Pattern::Clustered => {
            let start = records.saturating_sub(changes * 2) / 2;
            (
                (start..start + changes).collect(),
                (start + changes..start + changes * 2).collect(),
            )
        }
        Pattern::Random => {
            let all = random_ids(records, changes * 2, RANDOM_SEED ^ 0x9e37_79b9);
            (all[..changes].to_vec(), all[changes..].to_vec())
        }
    }
}

pub fn mutations(ids: &[usize], generation: u8) -> Vec<Mutation> {
    ids.iter()
        .map(|index| Mutation::Upsert {
            key: key(*index),
            val: value(*index, generation),
        })
        .collect()
}

pub fn build_entries(pattern: Pattern, records: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut ids = (0..records).collect::<Vec<_>>();
    match pattern {
        Pattern::Append => {}
        Pattern::Clustered => {
            let cluster = 64usize;
            for chunk in ids.chunks_mut(cluster) {
                chunk.reverse();
            }
        }
        Pattern::Random => shuffle(&mut ids, RANDOM_SEED),
    }
    ids.into_iter()
        .map(|index| (key(index), value(index, 4)))
        .collect()
}

pub fn expected_records(spec: &CellSpec) -> usize {
    match spec.api {
        Api::Build => spec.records,
        Api::Merge if spec.pattern == Pattern::Append => spec.records + spec.changes * 2,
        Api::Put | Api::Batch | Api::Diff if spec.pattern == Pattern::Append => {
            spec.records + spec.changes
        }
        _ => spec.records,
    }
}

fn random_ids(records: usize, count: usize, seed: u64) -> Vec<usize> {
    let mut ids = (0..records).collect::<Vec<_>>();
    shuffle(&mut ids, seed);
    ids.truncate(count);
    ids
}

fn shuffle(values: &mut [usize], mut state: u64) {
    for index in (1..values.len()).rev() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        values.swap(index, state as usize % (index + 1));
    }
}

pub fn percentile(samples: &[u128], quantile: usize) -> u128 {
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let index = (sorted.len() * quantile).div_ceil(100).saturating_sub(1);
    sorted[index]
}

pub fn root_hex(tree: &prolly::Tree) -> String {
    tree.root
        .as_ref()
        .map(|cid| {
            let mut output = String::with_capacity(cid.as_bytes().len() * 2);
            for byte in cid.as_bytes() {
                use std::fmt::Write;
                write!(output, "{byte:02x}").expect("write to String");
            }
            output
        })
        .unwrap_or_else(|| "empty".to_string())
}
