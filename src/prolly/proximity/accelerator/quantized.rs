//! Behavior shared by ordered-code quantizers.
//!
//! Code validation and approximate math remain accelerator-specific. This
//! module freezes candidate admission, logical budget accounting, and
//! authoritative reranking so PQ and TurboQuant cannot drift independently.

use crate::prolly::error::Error;
use crate::prolly::proximity::search::{retained_candidate_bytes, RerankCandidate};
use crate::prolly::proximity::storage::StoredRecordRef;
use crate::prolly::proximity::{
    Neighbor, ProximityMap, ProximitySearchStats, SearchCompletion, SearchRequest,
};
use crate::prolly::store::Store;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

#[derive(Clone, Debug)]
pub(crate) struct QuantizedRanked {
    pub(crate) distance: f64,
    pub(crate) key: Vec<u8>,
}

impl PartialEq for QuantizedRanked {
    fn eq(&self, other: &Self) -> bool {
        self.distance.to_bits() == other.distance.to_bits() && self.key == other.key
    }
}

impl Eq for QuantizedRanked {}

impl PartialOrd for QuantizedRanked {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for QuantizedRanked {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance
            .total_cmp(&other.distance)
            .then_with(|| self.key.cmp(&other.key))
    }
}

pub(crate) fn admit_quantized<F>(
    key: Vec<u8>,
    code: &[u8],
    target: usize,
    request: &SearchRequest<'_>,
    stats: &mut ProximitySearchStats,
    approximate: &mut BinaryHeap<QuantizedRanked>,
    score: F,
) -> Result<bool, Error>
where
    F: FnOnce(&[u8]) -> Result<f64, Error>,
{
    if request
        .budget
        .max_nodes
        .is_some_and(|limit| stats.nodes_read >= limit)
        || request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(code.len()) > limit)
        || distance_budget_exhausted(request, stats)
        || request
            .budget
            .max_frontier_entries
            .is_some_and(|limit| approximate.len().saturating_add(1) > limit)
    {
        return Ok(false);
    }

    let distance = score(code)?;
    stats.nodes_read += 1;
    stats.bytes_read = stats.bytes_read.saturating_add(code.len());
    stats.committed_bytes = stats.committed_bytes.saturating_add(code.len());
    stats.quantized_distance_evaluations += 1;
    let candidate = QuantizedRanked { distance, key };
    if approximate.len() < target {
        approximate.push(candidate);
    } else if target != 0 && approximate.peek().is_some_and(|worst| candidate < *worst) {
        *approximate
            .peek_mut()
            .expect("non-empty quantized candidate heap") = candidate;
    }
    stats.frontier_peak = stats.frontier_peak.max(approximate.len());
    Ok(true)
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn rerank_authoritative<S: Store + Clone + Send + Sync>(
    map: &ProximityMap<S>,
    request: &SearchRequest<'_>,
    query: &[f32],
    dimensions: u32,
    approximate: BinaryHeap<QuantizedRanked>,
    stats: &mut ProximitySearchStats,
    completion: &mut SearchCompletion,
    missing_code_message: &'static str,
) -> Result<Vec<Neighbor>, Error> {
    let mut approximate = approximate.into_vec();
    approximate.sort();
    let mut reranked = Vec::<RerankCandidate>::with_capacity(approximate.len());
    let mut directory = map.directory_manager().read(&map.tree().directory)?;

    for candidate in approximate {
        if distance_budget_exhausted(request, stats)
            || request
                .budget
                .max_nodes
                .is_some_and(|limit| stats.nodes_read >= limit)
        {
            *completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let Some(handle) = directory.get_handle(&candidate.key)? else {
            return Err(Error::InvalidProximityObject {
                kind: "quantized accelerator",
                reason: missing_code_message.to_owned(),
            });
        };
        let bytes = handle.value()?.len();
        if request
            .budget
            .max_committed_bytes
            .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes) > limit)
        {
            *completion = SearchCompletion::BudgetExhausted;
            break;
        }
        let record = StoredRecordRef::decode(handle.value()?, dimensions)?;
        let distance = record
            .vector
            .score(request.kernel, map.tree().config.metric, query);
        stats.nodes_read += 1;
        stats.bytes_read = stats.bytes_read.saturating_add(bytes);
        stats.committed_bytes = stats.committed_bytes.saturating_add(bytes);
        stats.distance_evaluations += 1;
        reranked.push(RerankCandidate::new(handle, &candidate.key, distance)?);
    }

    stats.reranked_candidates = reranked.len();
    stats.candidate_handles_peak = reranked.len();
    stats.candidate_retained_bytes_peak = retained_candidate_bytes(&reranked);
    reranked.sort_by(|left, right| {
        left.distance
            .total_cmp(&right.distance)
            .then_with(|| left.key().cmp(right.key()))
    });
    reranked
        .into_iter()
        .take(request.k)
        .map(|candidate| candidate.into_neighbor(dimensions))
        .collect()
}

pub(crate) fn distance_budget_exhausted(
    request: &SearchRequest<'_>,
    stats: &ProximitySearchStats,
) -> bool {
    request
        .budget
        .max_distance_evaluations
        .is_some_and(|maximum| {
            stats
                .distance_evaluations
                .saturating_add(stats.quantized_distance_evaluations)
                >= maximum
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn candidate_admission_is_total_ordered_and_bounded() {
        let mut stats = ProximitySearchStats::default();
        let mut heap = BinaryHeap::new();
        let request = SearchRequest::exact(&[0.0], 1);
        for (key, score) in [
            (b"c".to_vec(), 1.0),
            (b"a".to_vec(), 1.0),
            (b"b".to_vec(), 0.5),
            (b"d".to_vec(), 2.0),
        ] {
            assert!(
                admit_quantized(key, &[0], 2, &request, &mut stats, &mut heap, |_| Ok(score),)
                    .unwrap()
            );
        }
        let mut retained = heap.into_vec();
        retained.sort();
        assert_eq!(retained.len(), 2);
        assert_eq!(retained[0].key, b"b");
        assert_eq!(retained[1].key, b"a");
        assert_eq!(stats.frontier_peak, 2);

        let mut empty = BinaryHeap::new();
        let mut empty_stats = ProximitySearchStats::default();
        assert!(admit_quantized(
            b"ignored".to_vec(),
            &[0],
            0,
            &request,
            &mut empty_stats,
            &mut empty,
            |_| Ok(0.0),
        )
        .unwrap());
        assert!(empty.is_empty());
        assert_eq!(empty_stats.frontier_peak, 0);
        assert_eq!(empty_stats.quantized_distance_evaluations, 1);
    }
}
