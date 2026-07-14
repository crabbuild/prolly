use prolly::{
    DistanceMetric, Error, MemStore, ProximityConfig, ProximityMap, ProximityRecord, SearchOptions,
};
use std::sync::Arc;

fn build(metric: DistanceMetric, records: &[(&[u8], &[f32])]) -> ProximityMap<Arc<MemStore>> {
    let mut config = ProximityConfig::new(2);
    config.metric = metric;
    ProximityMap::build(
        Arc::new(MemStore::new()),
        config,
        records.iter().map(|(key, vector)| ProximityRecord {
            key: key.to_vec(),
            vector: vector.to_vec(),
            value: key.to_vec(),
        }),
    )
    .unwrap()
}

#[test]
fn metric_scores_and_key_ties_are_canonical() {
    let l2 = build(
        DistanceMetric::L2Squared,
        &[(b"b", &[4.0, 6.0]), (b"a", &[-2.0, -2.0])],
    );
    let result = l2.search(&[1.0, 2.0], SearchOptions::new(2)).unwrap();
    assert_eq!(result.neighbors[0].key, b"a");
    assert_eq!(result.neighbors[0].distance.to_bits(), 25.0f32.to_bits());
    assert_eq!(result.neighbors[1].distance.to_bits(), 25.0f32.to_bits());
    assert_eq!(result.neighbors[1].key, b"b");

    let cosine = build(
        DistanceMetric::Cosine,
        &[(b"x", &[1.0, 0.0]), (b"y", &[0.0, 1.0])],
    );
    let result = cosine.search(&[3.0, 4.0], SearchOptions::new(2)).unwrap();
    assert_eq!(result.neighbors[0].key, b"y");
    assert_eq!(
        result.neighbors[0].distance.to_bits(),
        0.199_999_99f32.to_bits()
    );

    let inner = build(
        DistanceMetric::InnerProduct,
        &[(b"positive", &[2.0, 1.0]), (b"negative", &[-1.0, 0.0])],
    );
    let result = inner.search(&[1.0, 2.0], SearchOptions::new(2)).unwrap();
    assert_eq!(result.neighbors[0].key, b"positive");
    assert_eq!(result.neighbors[0].distance.to_bits(), (-4.0f32).to_bits());
}

#[test]
fn cosine_preparation_is_persisted_and_rejects_zero_vectors() {
    let cosine = build(DistanceMetric::Cosine, &[(b"unit", &[3.0, 4.0])]);
    let (stored, _) = cosine.get(b"unit").unwrap().unwrap();
    assert_eq!(stored[0].to_bits(), 0.6f32.to_bits());
    assert_eq!(stored[1].to_bits(), 0.8f32.to_bits());

    let mut config = ProximityConfig::new(2);
    config.metric = DistanceMetric::Cosine;
    let error = ProximityMap::build(
        Arc::new(MemStore::new()),
        config,
        [ProximityRecord {
            key: b"zero".to_vec(),
            vector: vec![0.0, -0.0],
            value: Vec::new(),
        }],
    )
    .err()
    .expect("zero cosine vectors must be rejected");
    assert!(matches!(error, Error::ZeroCosineVector));

    let error = cosine
        .search(&[0.0, -0.0], SearchOptions::new(1))
        .expect_err("zero cosine queries must be rejected");
    assert!(matches!(error, Error::ZeroCosineVector));
}

#[test]
fn all_metrics_normalize_signed_zero_and_reject_non_finite_components() {
    for metric in [
        DistanceMetric::L2Squared,
        DistanceMetric::Cosine,
        DistanceMetric::InnerProduct,
    ] {
        let vector: &[f32] = if metric == DistanceMetric::Cosine {
            &[-0.0, 1.0]
        } else {
            &[-0.0, 0.0]
        };
        let map = build(metric, &[(b"zero", vector)]);
        assert_eq!(map.get(b"zero").unwrap().unwrap().0[0].to_bits(), 0);

        let mut config = ProximityConfig::new(2);
        config.metric = metric;
        let result = ProximityMap::build(
            Arc::new(MemStore::new()),
            config,
            [ProximityRecord {
                key: b"bad".to_vec(),
                vector: vec![f32::NAN, 1.0],
                value: Vec::new(),
            }],
        );
        assert!(matches!(result, Err(Error::InvalidProximityVector { .. })));
    }
}
