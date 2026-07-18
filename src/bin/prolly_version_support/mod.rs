use std::collections::BTreeSet;
use std::env;

use prolly::{Diff, LogicalPatch, Mutation, StructuralEdit};

pub const CONTRACT_VERSION: &str = "prolly-version-compare-v1";
pub const RANDOM_SEED: u64 = 0x6a09_e667_f3bc_c909;
pub const FNV_OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
const CLUSTER_SIZE: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Locality {
    None,
    Append,
    Random,
    Clustered,
}

impl Locality {
    pub fn parse(value: &str) -> Self {
        match value {
            "none" => Self::None,
            "append" => Self::Append,
            "random" => Self::Random,
            "clustered" => Self::Clustered,
            _ => panic!("invalid locality {value:?}"),
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Append => "append",
            Self::Random => "random",
            Self::Clustered => "clustered",
        }
    }
}

#[derive(Debug)]
pub struct Args {
    pub records: usize,
    pub density: usize,
    pub locality: Locality,
}

pub fn parse_common_args() -> Args {
    let mut records = None;
    let mut density = None;
    let mut locality = None;
    let mut args = env::args().skip(1);
    while let Some(flag) = args.next() {
        let value = args
            .next()
            .unwrap_or_else(|| panic!("missing value for {flag}"));
        match flag.as_str() {
            "--records" => records = Some(value.parse().expect("records must be an integer")),
            "--density" => density = Some(value.parse().expect("density must be an integer")),
            "--locality" => locality = Some(Locality::parse(&value)),
            _ => panic!("unknown argument {flag}"),
        }
    }
    let result = Args {
        records: records.expect("--records is required"),
        density: density.expect("--density is required"),
        locality: locality.expect("--locality is required"),
    };
    assert!(result.records >= CLUSTER_SIZE);
    assert!(matches!(result.density, 0 | 1 | 30));
    assert_eq!(result.density == 0, result.locality == Locality::None);
    result
}

pub fn base_mutations(records: usize) -> Vec<Mutation> {
    (0..records)
        .map(|position| Mutation::Upsert {
            key: key_for_id(position * 2),
            val: value_for(position as u64, 0),
        })
        .collect()
}

pub fn change_count(records: usize, density: usize) -> usize {
    records * density / 100
}

pub fn branch_mutations(
    records: usize,
    density: usize,
    locality: Locality,
    disjoint_ordinal_offset: usize,
    generation: u64,
) -> Vec<Mutation> {
    let count = change_count(records, density);
    if count == 0 {
        return Vec::new();
    }
    if locality == Locality::Append {
        return (0..count)
            .map(|ordinal| {
                let append_ordinal = ordinal + disjoint_ordinal_offset;
                let id = records * 2 + append_ordinal * 2;
                Mutation::Upsert {
                    key: key_for_id(id),
                    val: value_for(id as u64, generation),
                }
            })
            .collect();
    }

    let updates = count * 40 / 100;
    let inserts = count * 30 / 100;
    let mut mutations = Vec::with_capacity(count);
    for ordinal in 0..count {
        let position = selected_position(ordinal + disjoint_ordinal_offset, records, locality);
        if ordinal < updates {
            let id = position * 2;
            mutations.push(Mutation::Upsert {
                key: key_for_id(id),
                val: value_for(id as u64, generation),
            });
        } else if ordinal < updates + inserts {
            let id = position * 2 + 1;
            mutations.push(Mutation::Upsert {
                key: key_for_id(id),
                val: value_for(id as u64, generation),
            });
        } else {
            mutations.push(Mutation::Delete {
                key: key_for_id(position * 2),
            });
        }
    }
    mutations.sort_by(|left, right| left.key().cmp(right.key()));
    assert!(
        mutations
            .windows(2)
            .all(|pair| pair[0].key() < pair[1].key())
    );
    mutations
}

pub fn conflicting_mutations(left: &[Mutation]) -> Vec<Mutation> {
    left.iter()
        .map(|mutation| match mutation {
            Mutation::Upsert { key, .. } => Mutation::Upsert {
                key: key.clone(),
                val: value_for_key(key, 3),
            },
            Mutation::Delete { key } => Mutation::Upsert {
                key: key.clone(),
                val: value_for_key(key, 3),
            },
        })
        .collect()
}

pub fn range_bounds(
    records: usize,
    density: usize,
    locality: Locality,
    left: &[Mutation],
) -> (Vec<u8>, Vec<u8>) {
    if density == 0 {
        return (key_for_id(0), key_for_id((records / 10).max(1) * 2));
    }
    let inserts = if locality == Locality::Append {
        change_count(records, density)
    } else {
        change_count(records, density) * 30 / 100
    };
    let union = records + inserts;
    let width_ids = (union / 10).max(1) * 2;
    if locality == Locality::Append {
        let end_id = records * 2 + change_count(records, density) * 2 + 1;
        return (
            key_for_id(end_id.saturating_sub(width_ids)),
            key_for_id(end_id),
        );
    }
    let first = left
        .iter()
        .map(|mutation| parse_key_id(mutation.key()))
        .min()
        .expect("non-zero workload has a mutation");
    let max_id = records * 2 + 2;
    let start = first.min(max_id.saturating_sub(width_ids));
    (
        key_for_id(start),
        key_for_id((start + width_ids).min(max_id)),
    )
}

pub fn workload_digest(base_count: usize, relationship: &str, mutations: &[&[Mutation]]) -> u64 {
    let mut digest = digest_bytes(FNV_OFFSET, CONTRACT_VERSION.as_bytes());
    digest = digest_u64(digest, base_count as u64);
    digest = digest_bytes(digest, relationship.as_bytes());
    for group in mutations {
        digest = digest_u64(digest, group.len() as u64);
        for mutation in *group {
            match mutation {
                Mutation::Upsert { key, val } => {
                    digest = digest_bytes(digest, &[1]);
                    digest = digest_bytes(digest, key);
                    digest = digest_bytes(digest, val);
                }
                Mutation::Delete { key } => {
                    digest = digest_bytes(digest, &[2]);
                    digest = digest_bytes(digest, key);
                }
            }
        }
    }
    digest
}

pub fn digest_diffs(diffs: &[Diff]) -> u64 {
    let mut digest = FNV_OFFSET;
    for diff in diffs {
        match diff {
            Diff::Added { key, val } => {
                digest = digest_bytes(digest, &[1]);
                digest = digest_bytes(digest, key);
                digest = digest_bytes(digest, val);
            }
            Diff::Removed { key, val } => {
                digest = digest_bytes(digest, &[2]);
                digest = digest_bytes(digest, key);
                digest = digest_bytes(digest, val);
            }
            Diff::Changed { key, old, new } => {
                digest = digest_bytes(digest, &[3]);
                digest = digest_bytes(digest, key);
                digest = digest_bytes(digest, old);
                digest = digest_bytes(digest, new);
            }
        }
    }
    digest_u64(digest, diffs.len() as u64)
}

pub fn digest_patch(edits: &[StructuralEdit]) -> u64 {
    let mut digest = FNV_OFFSET;
    for edit in edits {
        let point = match edit {
            StructuralEdit::Point(point) => point,
            StructuralEdit::Subtree { .. } => {
                panic!("logical benchmark patch contains subtree edit")
            }
        };
        match point {
            LogicalPatch::Upsert { key, old, new } => {
                digest = digest_bytes(digest, &[if old.is_some() { 3 } else { 1 }]);
                digest = digest_bytes(digest, key);
                if let Some(old) = old {
                    digest = digest_bytes(digest, old);
                }
                digest = digest_bytes(digest, new);
            }
            LogicalPatch::Delete { key, old } => {
                digest = digest_bytes(digest, &[2]);
                digest = digest_bytes(digest, key);
                digest = digest_bytes(digest, old);
            }
        }
    }
    digest_u64(digest, edits.len() as u64)
}

pub fn digest_entry(mut digest: u64, key: &[u8], value: &[u8]) -> u64 {
    digest = digest_bytes(digest, key);
    digest_bytes(digest, value)
}

pub fn digest_u64(digest: u64, value: u64) -> u64 {
    digest_bytes(digest, &value.to_le_bytes())
}

pub fn digest_bytes(mut digest: u64, bytes: &[u8]) -> u64 {
    for byte in (bytes.len() as u64).to_le_bytes().iter().chain(bytes) {
        digest ^= u64::from(*byte);
        digest = digest.wrapping_mul(FNV_PRIME);
    }
    digest
}

pub fn validate_unique_keys(mutations: &[Mutation]) {
    let unique: BTreeSet<&[u8]> = mutations.iter().map(Mutation::key).collect();
    assert_eq!(
        unique.len(),
        mutations.len(),
        "mutation keys must be unique"
    );
}

fn selected_position(ordinal: usize, records: usize, locality: Locality) -> usize {
    match locality {
        Locality::Random => permute(ordinal, records, RANDOM_SEED ^ records as u64),
        Locality::Clustered => {
            let blocks = records.div_ceil(CLUSTER_SIZE);
            let logical_block = ordinal / CLUSTER_SIZE;
            let offset = ordinal % CLUSTER_SIZE;
            let block = permute(logical_block, blocks, RANDOM_SEED ^ 0xc1a5_7e2d);
            (block * CLUSTER_SIZE + offset) % records
        }
        _ => panic!("selected positions require random or clustered locality"),
    }
}

fn permute(index: usize, count: usize, seed: u64) -> usize {
    if count <= 1 {
        return 0;
    }
    let mut step = (mix64(seed) as usize | 1) % count;
    if step == 0 {
        step = 1;
    }
    while gcd(step, count) != 1 {
        step = (step + 2) % count;
        if step == 0 {
            step = 1;
        }
    }
    let offset = mix64(seed ^ 0x9e37_79b9_7f4a_7c15) as usize % count;
    ((index % count).wrapping_mul(step).wrapping_add(offset)) % count
}

fn gcd(mut left: usize, mut right: usize) -> usize {
    while right != 0 {
        (left, right) = (right, left % right);
    }
    left
}

pub fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

pub fn key_for_id(id: usize) -> Vec<u8> {
    format!("k{id:020}").into_bytes()
}

fn parse_key_id(key: &[u8]) -> usize {
    std::str::from_utf8(&key[1..])
        .expect("benchmark key is utf8")
        .parse()
        .expect("benchmark key has numeric suffix")
}

pub fn value_for(position: u64, generation: u64) -> Vec<u8> {
    let seed = mix64(position ^ generation.wrapping_mul(0xd1b5_4a32_d192_ed03) ^ RANDOM_SEED);
    let len = 16 + (seed as usize % 84);
    let mut value = Vec::with_capacity(len);
    let mut state = seed;
    while value.len() < len {
        state = mix64(state.wrapping_add(0x9e37_79b9_7f4a_7c15));
        value.extend_from_slice(&state.to_le_bytes());
    }
    value.truncate(len);
    value
}

fn value_for_key(key: &[u8], generation: u64) -> Vec<u8> {
    let seed = digest_bytes(FNV_OFFSET, key);
    value_for(seed, generation)
}
