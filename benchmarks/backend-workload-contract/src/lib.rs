mod digest;
mod oracle;

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

pub use digest::{digest_diffs, digest_entries, digest_mutations, Digest, DigestBuilder};
pub use oracle::{apply_mutations, logical_diff, DiffRecord, State};

pub const CONTRACT_VERSION: &str = "backend-workload-v1";
pub const DEFAULT_SEED: u64 = 0x6a09_e667_f3bc_c909;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkloadSpec {
    pub records: usize,
    pub value_bytes: usize,
    pub changes: usize,
    pub samples: usize,
    pub concurrency: usize,
    pub seed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MutationRecord {
    Upsert { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl MutationRecord {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Upsert { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MergeBranches {
    pub left: Vec<MutationRecord>,
    pub right: Vec<MutationRecord>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExpectedTree {
    Base,
    Batch,
    DiffTarget,
    Merged,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExpectedOutcomes {
    pub base_count: usize,
    pub batch_count: usize,
    pub diff_target_count: usize,
    pub merged_count: usize,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Workload {
    pub contract_version: String,
    pub spec: WorkloadSpec,
    pub query_ids: Vec<usize>,
    pub batch_mutations: Vec<MutationRecord>,
    pub diff_mutations: Vec<MutationRecord>,
    pub merge: MergeBranches,
    pub expected: ExpectedOutcomes,
    pub workload_digest: Digest,
    batch_generations: BTreeMap<usize, u64>,
    diff_generations: BTreeMap<usize, u64>,
    merged_generations: BTreeMap<usize, u64>,
}

impl Workload {
    pub fn generate(spec: WorkloadSpec) -> Result<Self, String> {
        validate_spec(spec)?;
        let query_ids = deterministic_ids(spec.records, spec.samples, spec.seed ^ 17);
        let batch_ids = deterministic_ids(spec.records, spec.changes, spec.seed ^ 29);
        let diff_ids = deterministic_ids(spec.records, spec.changes, spec.seed ^ 37);
        let merge_ids = deterministic_ids(spec.records, spec.changes, spec.seed ^ 53);

        let batch_mutations = mutations(&batch_ids, 1, spec);
        let diff_mutations = mutations(&diff_ids, 2, spec);
        let batch_generations = batch_ids.iter().map(|id| (*id, 1)).collect();
        let diff_generations = diff_ids.iter().map(|id| (*id, 2)).collect();
        let mut left = Vec::with_capacity(spec.changes / 2);
        let mut right = Vec::with_capacity(spec.changes / 2);
        let mut merged_generations = BTreeMap::new();
        for (position, id) in merge_ids.into_iter().enumerate() {
            let generation = if position % 2 == 0 { 3 } else { 4 };
            let mutation = upsert(id, generation, spec);
            merged_generations.insert(id, generation);
            if position % 2 == 0 {
                left.push(mutation);
            } else {
                right.push(mutation);
            }
        }
        let merge = MergeBranches { left, right };
        let expected = ExpectedOutcomes {
            base_count: spec.records,
            batch_count: spec.records,
            diff_target_count: spec.records,
            merged_count: spec.records,
        };
        let workload_digest =
            workload_digest(spec, &query_ids, &batch_mutations, &diff_mutations, &merge);
        Ok(Self {
            contract_version: CONTRACT_VERSION.to_string(),
            spec,
            query_ids,
            batch_mutations,
            diff_mutations,
            merge,
            expected,
            workload_digest,
            batch_generations,
            diff_generations,
            merged_generations,
        })
    }

    pub fn base_entry(&self, id: usize) -> Option<(Vec<u8>, Vec<u8>)> {
        (id < self.spec.records).then(|| (key(id), value(id, 0, self.spec)))
    }

    pub fn base_entries(&self) -> impl ExactSizeIterator<Item = (Vec<u8>, Vec<u8>)> + '_ {
        (0..self.spec.records).map(|id| (key(id), value(id, 0, self.spec)))
    }

    pub fn query_keys(&self) -> Vec<Vec<u8>> {
        self.query_ids.iter().copied().map(key).collect()
    }

    pub fn expected_diff_records(&self) -> Vec<DiffRecord> {
        self.diff_mutations
            .iter()
            .map(|mutation| match mutation {
                MutationRecord::Upsert { key, value: after } => {
                    let id = key_id(key).expect("generated workload keys are valid");
                    DiffRecord {
                        key: key.clone(),
                        before: Some(value(id, 0, self.spec)),
                        after: Some(after.clone()),
                    }
                }
                MutationRecord::Delete { key } => {
                    let id = key_id(key).expect("generated workload keys are valid");
                    DiffRecord {
                        key: key.clone(),
                        before: Some(value(id, 0, self.spec)),
                        after: None,
                    }
                }
            })
            .collect()
    }

    pub fn expected_value(&self, tree: ExpectedTree, id: usize) -> Option<Vec<u8>> {
        if id >= self.spec.records {
            return None;
        }
        let generation = match tree {
            ExpectedTree::Base => 0,
            ExpectedTree::Batch => self.batch_generations.get(&id).copied().unwrap_or(0),
            ExpectedTree::DiffTarget => self.diff_generations.get(&id).copied().unwrap_or(0),
            ExpectedTree::Merged => self.merged_generations.get(&id).copied().unwrap_or(0),
        };
        Some(value(id, generation, self.spec))
    }

    pub fn materialize(&self, tree: ExpectedTree) -> State {
        (0..self.spec.records)
            .map(|id| {
                (
                    key(id),
                    self.expected_value(tree, id)
                        .expect("in-range workload id has a value"),
                )
            })
            .collect()
    }
}

pub fn key(id: usize) -> Vec<u8> {
    format!("key-{id:020}").into_bytes()
}

pub fn key_id(key: &[u8]) -> Result<usize, String> {
    std::str::from_utf8(key)
        .map_err(|error| format!("key is not UTF-8: {error}"))?
        .strip_prefix("key-")
        .ok_or_else(|| "key does not use the workload prefix".to_string())?
        .parse()
        .map_err(|error| format!("key identifier is invalid: {error}"))
}

pub fn value(id: usize, generation: u64, spec: WorkloadSpec) -> Vec<u8> {
    let mut output = Vec::with_capacity(spec.value_bytes);
    let mut state = spec.seed
        ^ (id as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15)
        ^ generation.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    while output.len() < spec.value_bytes {
        state = splitmix64(state);
        output.extend_from_slice(&state.to_le_bytes());
    }
    output.truncate(spec.value_bytes);
    output
}

fn validate_spec(spec: WorkloadSpec) -> Result<(), String> {
    if spec.records == 0
        || spec.value_bytes == 0
        || spec.changes == 0
        || spec.samples == 0
        || spec.concurrency == 0
    {
        return Err("workload dimensions must be positive".to_string());
    }
    if spec.changes > spec.records {
        return Err("changes cannot exceed records".to_string());
    }
    if spec.changes % 2 != 0 {
        return Err("changes must be even".to_string());
    }
    if spec.samples > spec.records {
        return Err("samples cannot exceed records".to_string());
    }
    Ok(())
}

fn deterministic_ids(records: usize, count: usize, salt: u64) -> Vec<usize> {
    let mut state = salt ^ (records as u64).rotate_left(29);
    let mut ids = BTreeSet::new();
    while ids.len() < count {
        state = splitmix64(state);
        ids.insert((state as usize) % records);
    }
    ids.into_iter().collect()
}

fn mutations(ids: &[usize], generation: u64, spec: WorkloadSpec) -> Vec<MutationRecord> {
    ids.iter()
        .copied()
        .map(|id| upsert(id, generation, spec))
        .collect()
}

fn upsert(id: usize, generation: u64, spec: WorkloadSpec) -> MutationRecord {
    MutationRecord::Upsert {
        key: key(id),
        value: value(id, generation, spec),
    }
}

fn workload_digest(
    spec: WorkloadSpec,
    query_ids: &[usize],
    batch: &[MutationRecord],
    diff: &[MutationRecord],
    merge: &MergeBranches,
) -> Digest {
    let mut digest = DigestBuilder::new(b"backend-workload");
    digest.field(CONTRACT_VERSION.as_bytes());
    digest.field(&(spec.records as u64).to_le_bytes());
    digest.field(&(spec.value_bytes as u64).to_le_bytes());
    digest.field(&(spec.changes as u64).to_le_bytes());
    digest.field(&(spec.samples as u64).to_le_bytes());
    digest.field(&(spec.concurrency as u64).to_le_bytes());
    digest.field(&spec.seed.to_le_bytes());
    for id in query_ids {
        digest.field(&(*id as u64).to_le_bytes());
    }
    digest.field(digest_mutations(batch).as_bytes());
    digest.field(digest_mutations(diff).as_bytes());
    digest.field(digest_mutations(&merge.left).as_bytes());
    digest.field(digest_mutations(&merge.right).as_bytes());
    digest.finish()
}

fn splitmix64(mut state: u64) -> u64 {
    state = state.wrapping_add(0x9e37_79b9_7f4a_7c15);
    let mut value = state;
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    fn small_spec() -> WorkloadSpec {
        WorkloadSpec {
            records: 100,
            value_bytes: 27,
            changes: 10,
            samples: 8,
            concurrency: 4,
            seed: DEFAULT_SEED,
        }
    }

    #[test]
    fn golden_workload_has_stable_shape() {
        let workload = Workload::generate(small_spec()).unwrap();

        assert_eq!(workload.contract_version, CONTRACT_VERSION);
        assert_eq!(
            workload.base_entry(0).unwrap().0,
            b"key-00000000000000000000"
        );
        assert_eq!(workload.base_entry(0).unwrap().1.len(), 27);
        assert_eq!(workload.query_ids.len(), 8);
        assert_eq!(workload.batch_mutations.len(), 10);
        assert_eq!(workload.merge.left.len(), 5);
        assert_eq!(workload.merge.right.len(), 5);
        assert_eq!(workload.query_ids, [4, 13, 41, 69, 75, 83, 88, 97]);
        assert_eq!(
            workload.workload_digest.to_hex(),
            "b0db9f176a6c29f512b9209de204fe800de4d122561f668569e23728b539a71a"
        );
        assert_eq!(
            workload.base_entry(0).unwrap().1,
            [
                0x92, 0x75, 0x09, 0x2b, 0x2a, 0xc6, 0xcf, 0x63, 0xba, 0x6e, 0x6a, 0x12, 0xd7, 0xb1,
                0xdc, 0x96, 0x54, 0x70, 0x3a, 0x5c, 0xca, 0x6e, 0x04, 0x25, 0xdc, 0xab, 0xd5,
            ]
        );

        let left = workload
            .merge
            .left
            .iter()
            .map(MutationRecord::key)
            .collect::<BTreeSet<_>>();
        let right = workload
            .merge
            .right
            .iter()
            .map(MutationRecord::key)
            .collect::<BTreeSet<_>>();
        assert!(left.is_disjoint(&right));
    }

    #[test]
    fn generation_is_byte_identical_and_input_sensitive() {
        let first = Workload::generate(small_spec()).unwrap();
        let second = Workload::generate(small_spec()).unwrap();
        assert_eq!(first, second);

        let mut changed = small_spec();
        changed.seed ^= 1;
        let third = Workload::generate(changed).unwrap();
        assert_ne!(first.workload_digest, third.workload_digest);
        assert_eq!(
            apply_mutations(
                &third.materialize(ExpectedTree::Base),
                &third.batch_mutations
            ),
            third.materialize(ExpectedTree::Batch)
        );
    }

    #[test]
    fn oracle_states_match_mutation_application() {
        let workload = Workload::generate(small_spec()).unwrap();
        let base = workload.materialize(ExpectedTree::Base);
        assert_eq!(
            apply_mutations(&base, &workload.batch_mutations),
            workload.materialize(ExpectedTree::Batch)
        );
        assert_eq!(
            apply_mutations(&base, &workload.diff_mutations),
            workload.materialize(ExpectedTree::DiffTarget)
        );
        let left = apply_mutations(&base, &workload.merge.left);
        assert_eq!(
            apply_mutations(&left, &workload.merge.right),
            workload.materialize(ExpectedTree::Merged)
        );
    }

    #[test]
    fn invalid_dimensions_fail_closed() {
        let mut spec = small_spec();
        spec.changes = 11;
        assert!(Workload::generate(spec)
            .unwrap_err()
            .contains("changes must be even"));

        let mut spec = small_spec();
        spec.samples = 101;
        assert!(Workload::generate(spec)
            .unwrap_err()
            .contains("samples cannot exceed records"));
    }
}
