use crate::prolly::cid::Cid;
use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub(crate) struct FrontierEntry {
    pub(crate) bound: f64,
    pub(crate) score: f64,
    pub(crate) key: Vec<u8>,
    pub(crate) cid: Cid,
    pub(crate) expected_level: Option<u8>,
}

impl PartialEq for FrontierEntry {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for FrontierEntry {}

impl PartialOrd for FrontierEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for FrontierEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse the canonical tuple so BinaryHeap pops the global minimum.
        other
            .bound
            .total_cmp(&self.bound)
            .then_with(|| other.score.total_cmp(&self.score))
            .then_with(|| other.key.cmp(&self.key))
            .then_with(|| other.cid.as_bytes().cmp(self.cid.as_bytes()))
    }
}

#[derive(Clone, Debug)]
pub(crate) struct SearchCandidate {
    pub(crate) key: Vec<u8>,
    pub(crate) vector: Vec<f32>,
    pub(crate) score: f64,
}

pub(crate) fn insert_top_k(
    candidates: &mut Vec<SearchCandidate>,
    candidate: SearchCandidate,
    k: usize,
) {
    candidates.push(candidate);
    candidates.sort_by(|left, right| {
        left.score
            .total_cmp(&right.score)
            .then_with(|| left.key.cmp(&right.key))
    });
    candidates.truncate(k);
}
