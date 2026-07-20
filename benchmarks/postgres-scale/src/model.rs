use std::collections::BTreeSet;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Pattern {
    Append,
    Random,
    Clustered,
}

impl Pattern {
    pub const ALL: [Self; 3] = [Self::Append, Self::Random, Self::Clustered];
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

impl Pattern {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Build,
    Put,
    Batch,
    GetCold,
    GetWarm,
    Query,
    Scan,
    FullScan,
    Diff,
    Merge,
}

impl Operation {
    pub const ALL: [Self; 9] = [
        Self::Put,
        Self::Batch,
        Self::GetCold,
        Self::GetWarm,
        Self::Query,
        Self::Scan,
        Self::FullScan,
        Self::Diff,
        Self::Merge,
    ];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Build => "build",
            Self::Put => "put",
            Self::Batch => "batch",
            Self::GetCold => "get_cold",
            Self::GetWarm => "get_warm",
            Self::Query => "query",
            Self::Scan => "scan",
            Self::FullScan => "full_scan",
            Self::Diff => "diff",
            Self::Merge => "merge",
        }
    }
}

impl FromStr for Operation {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "build" => Ok(Self::Build),
            "put" => Ok(Self::Put),
            "batch" => Ok(Self::Batch),
            "get_cold" => Ok(Self::GetCold),
            "get_warm" => Ok(Self::GetWarm),
            "query" => Ok(Self::Query),
            "scan" => Ok(Self::Scan),
            "full_scan" => Ok(Self::FullScan),
            "diff" => Ok(Self::Diff),
            "merge" => Ok(Self::Merge),
            _ => Err(format!("unknown operation: {value}")),
        }
    }
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn value(id: usize, generation: u8) -> Vec<u8> {
    format!("val-{id:020}-{generation:02}").into_bytes()
}

pub fn change_count(records: usize) -> usize {
    records.min((records / 100).max(100)).min(10_000)
}

pub fn pattern_ids(records: usize, count: usize, pattern: Pattern, salt: u64) -> Vec<usize> {
    match pattern {
        Pattern::Append => (records..records.saturating_add(count)).collect(),
        Pattern::Clustered => {
            let count = count.min(records);
            let start = records.saturating_sub(count) / 2;
            (start..start + count).collect()
        }
        Pattern::Random => random_ids(records, count, salt),
    }
}

pub fn merge_ids(records: usize, count: usize, pattern: Pattern) -> (Vec<usize>, Vec<usize>) {
    let branch_count = count / 2;
    match pattern {
        Pattern::Append => (
            (records..records.saturating_add(branch_count)).collect(),
            (records.saturating_add(branch_count)..records.saturating_add(count)).collect(),
        ),
        Pattern::Random => {
            let ids = random_ids(records, count, 0x006d_6572_6765);
            let mut left = Vec::with_capacity(branch_count);
            let mut right = Vec::with_capacity(branch_count);
            for (index, id) in ids.into_iter().enumerate() {
                if index % 2 == 0 {
                    left.push(id);
                } else {
                    right.push(id);
                }
            }
            (left, right)
        }
        Pattern::Clustered => {
            let ids = pattern_ids(records, count, pattern, 0);
            (ids[..branch_count].to_vec(), ids[branch_count..].to_vec())
        }
    }
}

fn random_ids(records: usize, count: usize, salt: u64) -> Vec<usize> {
    let count = count.min(records);
    let mut state = RANDOM_SEED ^ (records as u64).rotate_left(29) ^ salt.rotate_left(11);
    let mut ids = BTreeSet::new();
    while ids.len() < count {
        ids.insert((next_random(&mut state) as usize) % records);
    }
    ids.into_iter().collect()
}

fn next_random(state: &mut u64) -> u64 {
    *state ^= *state << 13;
    *state ^= *state >> 7;
    *state ^= *state << 17;
    *state
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    #[test]
    fn fixed_width_keys_sort_numerically() {
        assert_eq!(key(0).len(), 24);
        assert_eq!(key(9_999_999).len(), 24);
        assert_eq!(value(0, 0).len(), 27);
        assert!(key(9) < key(10));
        assert!(key(999_999) < key(1_000_000));
    }

    #[test]
    fn requested_sizes_use_ten_thousand_changes() {
        assert_eq!(change_count(1_000_000), 10_000);
        assert_eq!(change_count(10_000_000), 10_000);
    }

    #[test]
    fn append_ids_start_at_right_edge() {
        assert_eq!(
            pattern_ids(1_000, 4, Pattern::Append, 0),
            vec![1_000, 1_001, 1_002, 1_003]
        );
    }

    #[test]
    fn clustered_ids_are_centered_and_contiguous() {
        assert_eq!(
            pattern_ids(1_000, 4, Pattern::Clustered, 0),
            vec![498, 499, 500, 501]
        );
    }

    #[test]
    fn random_ids_are_unique_stable_and_in_range() {
        let first = pattern_ids(10_000, 1_000, Pattern::Random, 7);
        let second = pattern_ids(10_000, 1_000, Pattern::Random, 7);
        assert_eq!(first, second);
        assert!(first.windows(2).all(|pair| pair[0] < pair[1]));
        assert!(first.iter().all(|id| *id < 10_000));
        assert_eq!(first.iter().copied().collect::<BTreeSet<_>>().len(), 1_000);
    }

    #[test]
    fn merge_total_is_split_evenly_into_disjoint_branches() {
        for pattern in Pattern::ALL {
            let (left, right) = merge_ids(100_000, 1_000, pattern);
            assert_eq!(left.len(), 500);
            assert_eq!(right.len(), 500);
            assert!(left.iter().all(|id| !right.contains(id)));
        }
    }

    #[test]
    fn random_merge_branches_are_interleaved_across_the_keyspace() {
        let (left, right) = merge_ids(100_000, 1_000, Pattern::Random);
        let left = left.into_iter().collect::<BTreeSet<_>>();
        let right = right.into_iter().collect::<BTreeSet<_>>();
        let combined = left.union(&right).copied().collect::<Vec<_>>();
        let transitions = combined
            .windows(2)
            .filter(|pair| left.contains(&pair[0]) != left.contains(&pair[1]))
            .count();
        assert!(transitions > 900, "only {transitions} branch transitions");
    }
}
