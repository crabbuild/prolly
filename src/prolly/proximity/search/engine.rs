use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::storage::StoredRecordRef;
use crate::prolly::proximity::storage::{ProximityEntry, ProximityNode};
use crate::prolly::proximity::Neighbor;
use crate::prolly::read::ReadValueHandle;
use std::cmp::Ordering;
use std::sync::Arc;

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
    node: Arc<ProximityNode>,
    entry_index: usize,
    pub(crate) score: f64,
}

/// An authoritatively scored candidate that retains its packed directory leaf.
///
/// The application value stays borrowed until the final top-k result boundary.
/// This also prevents cache eviction from invalidating the record while sorting
/// the exact shortlist.
#[derive(Clone, Debug)]
pub(crate) struct RerankCandidate {
    record: ReadValueHandle,
    pub(crate) distance: f64,
}

impl RerankCandidate {
    pub(crate) fn new(
        record: ReadValueHandle,
        expected_key: &[u8],
        distance: f64,
    ) -> Result<Self, Error> {
        // Callers decode and score this exact handle immediately before
        // construction. Re-decoding here would scan every vector twice.
        if record.key()? != expected_key {
            return Err(Error::InvalidProximityObject {
                kind: "candidate",
                reason: "retained directory key disagrees with candidate key".to_owned(),
            });
        }
        Ok(Self { record, distance })
    }

    #[inline]
    pub(crate) fn key(&self) -> &[u8] {
        self.record
            .key()
            .expect("rerank candidate was validated at construction")
    }

    #[inline]
    pub(crate) fn record(&self, dimensions: u32) -> Result<StoredRecordRef<'_>, Error> {
        StoredRecordRef::decode(self.record.value()?, dimensions)
    }

    #[inline]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.record.retained_bytes()
    }

    #[inline]
    pub(crate) fn backing_id(&self) -> usize {
        self.record.backing_id()
    }

    pub(crate) fn into_neighbor(self, dimensions: u32) -> Result<Neighbor, Error> {
        let key = self.key().to_vec();
        let value = self.record(dimensions)?.value.to_vec();
        Ok(Neighbor {
            key,
            value,
            distance: self.distance,
        })
    }
}

impl SearchCandidate {
    pub(crate) fn new(node: Arc<ProximityNode>, entry_index: usize, score: f64) -> Self {
        Self {
            node,
            entry_index,
            score,
        }
    }

    pub(crate) fn entry(&self) -> Result<&ProximityEntry, Error> {
        self.node
            .entries
            .get(self.entry_index)
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "candidate",
                reason: "retained entry index is outside its node".to_owned(),
            })
    }

    pub(crate) fn key(&self) -> Result<&[u8], Error> {
        Ok(&self.entry()?.key)
    }

    pub(crate) fn vector(&self) -> Result<&[f32], Error> {
        self.entry()?.vector.inline()
    }

    #[inline]
    pub(crate) fn retained_bytes(&self) -> usize {
        self.node.retained_bytes()
    }

    #[inline]
    pub(crate) fn backing_id(&self) -> usize {
        Arc::as_ptr(&self.node) as usize
    }
}

pub(crate) fn insert_top_k(
    candidates: &mut Vec<SearchCandidate>,
    candidate: SearchCandidate,
    k: usize,
) {
    let position = candidates.partition_point(|current| {
        current
            .score
            .total_cmp(&candidate.score)
            .then_with(|| {
                current
                    .key()
                    .expect("candidate handle was validated at construction")
                    .cmp(
                        candidate
                            .key()
                            .expect("candidate handle was validated at construction"),
                    )
            })
            .is_le()
    });
    if position < k {
        candidates.insert(position, candidate);
        candidates.truncate(k);
    }
}

pub(crate) fn insert_reranked_top_k(
    candidates: &mut Vec<RerankCandidate>,
    candidate: RerankCandidate,
    k: usize,
) {
    let position = candidates.partition_point(|current| {
        current
            .distance
            .total_cmp(&candidate.distance)
            .then_with(|| current.key().cmp(candidate.key()))
            .is_le()
    });
    if position < k {
        candidates.insert(position, candidate);
        candidates.truncate(k);
    }
}

pub(crate) fn retained_candidate_bytes(candidates: &[RerankCandidate]) -> usize {
    let mut unique = std::collections::HashSet::with_capacity(candidates.len());
    candidates
        .iter()
        .filter(|candidate| unique.insert(candidate.backing_id()))
        .map(RerankCandidate::retained_bytes)
        .sum()
}

pub(crate) fn retained_search_candidate_bytes(candidates: &[SearchCandidate]) -> usize {
    let mut unique = std::collections::HashSet::with_capacity(candidates.len());
    candidates
        .iter()
        .filter(|candidate| unique.insert(candidate.backing_id()))
        .map(SearchCandidate::retained_bytes)
        .sum()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::proximity::storage::{PhysicalNodeKind, ProximityEntry, StoredRecord};
    use crate::prolly::{Config, Prolly};
    use crate::{DistanceMetric, MemStore};

    fn node(keys: &[&[u8]]) -> Arc<ProximityNode> {
        Arc::new(ProximityNode {
            kind: PhysicalNodeKind::Leaf,
            level: 0,
            subtree_count: keys.len() as u64,
            quantizer: None,
            entries: keys
                .iter()
                .map(|key| ProximityEntry::inline_leaf(key.to_vec(), vec![1.0, 2.0]))
                .collect(),
        })
    }

    #[test]
    fn retained_candidates_are_ordered_bounded_and_keep_their_node_alive() {
        let node = node(&[b"c", b"a", b"b"]);
        let mut candidates = Vec::new();
        insert_top_k(
            &mut candidates,
            SearchCandidate::new(node.clone(), 0, 2.0),
            2,
        );
        insert_top_k(
            &mut candidates,
            SearchCandidate::new(node.clone(), 1, 1.0),
            2,
        );
        insert_top_k(
            &mut candidates,
            SearchCandidate::new(node.clone(), 2, 1.0),
            2,
        );

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].key().unwrap(), b"a");
        assert_eq!(candidates[1].key().unwrap(), b"b");
        assert_eq!(Arc::strong_count(&node), 3);
        drop(node);
        assert_eq!(candidates[0].vector().unwrap(), &[1.0, 2.0]);
    }

    #[test]
    fn retained_candidate_rejects_an_invalid_entry_index() {
        let candidate = SearchCandidate::new(node(&[b"a"]), 1, 0.0);
        assert!(matches!(
            candidate.entry(),
            Err(Error::InvalidProximityObject { .. })
        ));
    }

    #[test]
    fn authoritative_candidate_survives_cache_clear_and_copies_only_final_value() {
        let prolly = Prolly::new(MemStore::new(), Config::default());
        let record = StoredRecord::new(
            &[1.0, 2.0],
            b"application-value".to_vec(),
            DistanceMetric::L2Squared,
            2,
        )
        .unwrap();
        let tree = prolly
            .put(&prolly.create(), b"key".to_vec(), record.encode())
            .unwrap();
        let mut session = prolly.read(&tree).unwrap();
        let handle = session.get_handle(b"key").unwrap().unwrap();
        StoredRecordRef::decode(handle.value().unwrap(), 2).unwrap();
        let candidate = RerankCandidate::new(handle, b"key", 3.5).unwrap();
        assert!(candidate.retained_bytes() > 0);

        prolly.clear_cache();
        drop(session);
        let neighbor = candidate.into_neighbor(2).unwrap();
        assert_eq!(neighbor.key, b"key");
        assert_eq!(neighbor.value, b"application-value");
        assert_eq!(neighbor.distance, 3.5);
    }
}
