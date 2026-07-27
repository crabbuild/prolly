use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::MutationRecord;

pub type State = BTreeMap<Vec<u8>, Vec<u8>>;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DiffRecord {
    pub key: Vec<u8>,
    pub before: Option<Vec<u8>>,
    pub after: Option<Vec<u8>>,
}

pub fn apply_mutations(base: &State, mutations: &[MutationRecord]) -> State {
    let mut state = base.clone();
    for mutation in mutations {
        match mutation {
            MutationRecord::Upsert { key, value } => {
                state.insert(key.clone(), value.clone());
            }
            MutationRecord::Delete { key } => {
                state.remove(key);
            }
        }
    }
    state
}

pub fn logical_diff(base: &State, target: &State) -> Vec<DiffRecord> {
    let mut before = base.iter().peekable();
    let mut after = target.iter().peekable();
    let mut diffs = Vec::new();
    loop {
        match (before.peek(), after.peek()) {
            (Some((left_key, left_value)), Some((right_key, right_value))) => {
                match left_key.cmp(right_key) {
                    std::cmp::Ordering::Less => {
                        diffs.push(removed(left_key, left_value));
                        before.next();
                    }
                    std::cmp::Ordering::Greater => {
                        diffs.push(added(right_key, right_value));
                        after.next();
                    }
                    std::cmp::Ordering::Equal => {
                        if left_value != right_value {
                            diffs.push(changed(left_key, left_value, right_value));
                        }
                        before.next();
                        after.next();
                    }
                }
            }
            (Some((key, value)), None) => {
                diffs.push(removed(key, value));
                before.next();
            }
            (None, Some((key, value))) => {
                diffs.push(added(key, value));
                after.next();
            }
            (None, None) => break,
        }
    }
    diffs
}

fn added(key: &&Vec<u8>, value: &&Vec<u8>) -> DiffRecord {
    DiffRecord {
        key: (*key).clone(),
        before: None,
        after: Some((*value).clone()),
    }
}

fn removed(key: &&Vec<u8>, value: &&Vec<u8>) -> DiffRecord {
    DiffRecord {
        key: (*key).clone(),
        before: Some((*value).clone()),
        after: None,
    }
}

fn changed(key: &&Vec<u8>, before: &&Vec<u8>, after: &&Vec<u8>) -> DiffRecord {
    DiffRecord {
        key: (*key).clone(),
        before: Some((*before).clone()),
        after: Some((*after).clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logical_diff_preserves_key_order_and_presence() {
        let base = BTreeMap::from([
            (b"a".to_vec(), b"1".to_vec()),
            (b"b".to_vec(), b"2".to_vec()),
        ]);
        let target = BTreeMap::from([
            (b"b".to_vec(), b"3".to_vec()),
            (b"c".to_vec(), b"4".to_vec()),
        ]);
        assert_eq!(
            logical_diff(&base, &target),
            vec![
                DiffRecord {
                    key: b"a".to_vec(),
                    before: Some(b"1".to_vec()),
                    after: None,
                },
                DiffRecord {
                    key: b"b".to_vec(),
                    before: Some(b"2".to_vec()),
                    after: Some(b"3".to_vec()),
                },
                DiffRecord {
                    key: b"c".to_vec(),
                    before: None,
                    after: Some(b"4".to_vec()),
                },
            ]
        );
    }
}
