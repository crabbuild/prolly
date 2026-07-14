use prolly::{
    AdaptiveQuality, DistanceMetric, MemStore, ProximityConfig, ProximityFilter, ProximityMap,
    ProximityRecord, SearchBackend, SearchBudget, SearchCompletion, SearchPolicy, SearchRequest,
};
use std::sync::Arc;

fn records() -> Vec<ProximityRecord> {
    (0usize..256)
        .map(|index| ProximityRecord {
            key: format!("group-{}/item-{index:04}", index % 4).into_bytes(),
            vector: vec![
                index as f32 / 7.0 + 1.0,
                (index % 17) as f32 + 1.0,
                (index % 5) as f32 + 1.0,
            ],
            value: index.to_le_bytes().to_vec(),
        })
        .collect()
}

fn map(metric: DistanceMetric) -> (ProximityMap<Arc<MemStore>>, Vec<ProximityRecord>) {
    let mut config = ProximityConfig::new(3);
    config.metric = metric;
    config.hierarchy.log_chunk_size = 2;
    config.hierarchy.level_hash_seed = 41;
    let records = records();
    let map = ProximityMap::build(Arc::new(MemStore::new()), config, records.clone()).unwrap();
    (map, records)
}

fn expected_l2(
    records: &[ProximityRecord],
    query: &[f32],
    include: impl Fn(&[u8]) -> bool,
    k: usize,
) -> Vec<Vec<u8>> {
    let mut records: Vec<_> = records
        .iter()
        .filter(|record| include(&record.key))
        .collect();
    records.sort_by(|left, right| {
        let score = |record: &ProximityRecord| {
            record
                .vector
                .iter()
                .zip(query)
                .map(|(&left, &right)| {
                    let delta = f64::from(left) - f64::from(right);
                    delta * delta
                })
                .sum::<f64>()
        };
        score(left)
            .total_cmp(&score(right))
            .then_with(|| left.key.cmp(&right.key))
    });
    records
        .into_iter()
        .take(k)
        .map(|record| record.key.clone())
        .collect()
}

fn keys(result: &prolly::SearchResult) -> Vec<Vec<u8>> {
    result
        .neighbors
        .iter()
        .map(|neighbor| neighbor.key.clone())
        .collect()
}

fn exact_request<'a>(query: &'a [f32], k: usize, filter: ProximityFilter<'a>) -> SearchRequest<'a> {
    SearchRequest {
        query,
        k,
        policy: SearchPolicy::Exact,
        budget: SearchBudget::default(),
        filter,
        backend: SearchBackend::Native,
        kernel: prolly::QueryKernel::AutoDeterministic,
    }
}

#[test]
fn exact_l2_matches_brute_force_for_all_structural_filters() {
    let (map, records) = map(DistanceMetric::L2Squared);
    let query = [18.25, 3.5, 2.0];

    let all = map
        .search(exact_request(&query, 9, ProximityFilter::All))
        .unwrap();
    assert_eq!(keys(&all), expected_l2(&records, &query, |_| true, 9));
    assert_eq!(all.completion, SearchCompletion::Exact);

    let start = b"group-1/item-0050".as_slice();
    let end = b"group-3/item-0200".as_slice();
    let range = map
        .search(exact_request(
            &query,
            9,
            ProximityFilter::KeyRange {
                start: Some(start),
                end: Some(end),
            },
        ))
        .unwrap();
    assert_eq!(
        keys(&range),
        expected_l2(&records, &query, |key| key >= start && key < end, 9)
    );

    let prefix = b"group-2/".as_slice();
    let prefixed = map
        .search(exact_request(&query, 9, ProximityFilter::Prefix(prefix)))
        .unwrap();
    assert_eq!(
        keys(&prefixed),
        expected_l2(&records, &query, |key| key.starts_with(prefix), 9)
    );

    let mut eligible: Vec<_> = records
        .iter()
        .filter(|record| record.key.ends_with(b"0") || record.key.ends_with(b"7"))
        .map(|record| record.key.clone())
        .collect();
    eligible.sort();
    let eligible_result = map
        .search(exact_request(
            &query,
            9,
            ProximityFilter::EligibleKeys(&eligible),
        ))
        .unwrap();
    assert_eq!(
        keys(&eligible_result),
        expected_l2(
            &records,
            &query,
            |key| eligible
                .binary_search_by(|candidate| candidate.as_slice().cmp(key))
                .is_ok(),
            9
        )
    );
}

#[test]
fn filters_reject_unsorted_keys_and_stale_secondary_snapshots() {
    let (map, _) = map(DistanceMetric::L2Squared);
    let query = [0.0; 3];
    let unsorted = vec![b"z".to_vec(), b"a".to_vec()];
    assert!(map
        .search(exact_request(
            &query,
            1,
            ProximityFilter::EligibleKeys(&unsorted),
        ))
        .is_err());

    let stale = ProximityMap::build(
        Arc::new(MemStore::new()),
        ProximityConfig::new(3),
        [ProximityRecord {
            key: b"stale".to_vec(),
            vector: vec![1.0; 3],
            value: Vec::new(),
        }],
    )
    .unwrap();
    let eligible = vec![b"group-0/item-0000".to_vec()];
    assert!(map
        .search(exact_request(
            &query,
            1,
            ProximityFilter::SecondaryEligible {
                keys: &eligible,
                source_directory: &stale.tree().directory,
            },
        ))
        .is_err());
}

#[test]
fn hard_budgets_return_deterministic_honest_partial_results() {
    let (map, _) = map(DistanceMetric::L2Squared);
    let query = [18.25, 3.5, 2.0];
    let request = SearchRequest {
        query: &query,
        k: 5,
        policy: SearchPolicy::FixedBudget,
        budget: SearchBudget {
            max_nodes: Some(3),
            ..SearchBudget::default()
        },
        filter: ProximityFilter::All,
        backend: SearchBackend::Native,
        kernel: prolly::QueryKernel::AutoDeterministic,
    };
    let first = map.search(request.clone()).unwrap();
    let second = map.search(request).unwrap();
    assert_eq!(first.completion, SearchCompletion::BudgetExhausted);
    assert_eq!(first.neighbors, second.neighbors);
    assert_eq!(first.stats, second.stats);
    assert_eq!(first.stats.nodes_read, 3);
}

#[test]
fn exact_cosine_and_inner_product_exhaust_every_eligible_leaf() {
    let query = [2.0, 3.0, 5.0];
    for metric in [DistanceMetric::Cosine, DistanceMetric::InnerProduct] {
        let (map, records) = map(metric);
        let prefix = b"group-1/";
        let result = map
            .search(exact_request(&query, 11, ProximityFilter::Prefix(prefix)))
            .unwrap();
        let mut expected: Vec<_> = records
            .iter()
            .filter(|record| record.key.starts_with(prefix))
            .map(|record| {
                let (stored, _) = map.get(&record.key).unwrap().unwrap();
                let dot = stored
                    .iter()
                    .zip(&query)
                    .map(|(&left, &right)| f64::from(left) * f64::from(right))
                    .sum::<f64>();
                let score = -dot;
                (record.key.clone(), score)
            })
            .collect();
        expected.sort_by(|left, right| {
            left.1
                .total_cmp(&right.1)
                .then_with(|| left.0.cmp(&right.0))
        });
        assert_eq!(
            keys(&result),
            expected
                .into_iter()
                .take(11)
                .map(|(key, _)| key)
                .collect::<Vec<_>>(),
            "metric {metric:?}"
        );
        assert_eq!(result.completion, SearchCompletion::Exact);
    }
}

#[test]
fn adaptive_profiles_have_fixed_seed_deterministic_completion() {
    let (map, _) = map(DistanceMetric::InnerProduct);
    let query = [2.0, 3.0, 5.0];
    let request = SearchRequest {
        query: &query,
        k: 5,
        policy: SearchPolicy::Adaptive(AdaptiveQuality::Fast),
        budget: SearchBudget::default(),
        filter: ProximityFilter::All,
        backend: SearchBackend::Native,
        kernel: prolly::QueryKernel::AutoDeterministic,
    };
    let first = map.search(request.clone()).unwrap();
    let second = map.search(request).unwrap();
    assert_eq!(
        first.completion,
        SearchCompletion::ApproximatePolicySatisfied
    );
    assert_eq!(first.neighbors, second.neighbors);
    assert_eq!(first.stats, second.stats);
    assert!(first.stats.nodes_read >= 8);
}
