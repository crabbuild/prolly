#![no_main]

use libfuzzer_sys::fuzz_target;
use prolly::{
    walk_content_graph, BuildParallelism, ContentGraphLimits, ContentObjectKind, DistanceMetric,
    MemStore, ProximityConfig, ProximityMap, ProximityRecord, QueryKernel, SearchBackend,
    SearchPolicy, SearchRequest, Store, TurboQuantizationConfig, TurboQuantizer, TypedContentRoot,
};
use std::sync::Arc;

const DIMENSIONS: [usize; 5] = [8, 16, 24, 32, 64];

fn byte(input: &[u8], index: usize) -> u8 {
    input[index % input.len()]
}

fn component(input: &[u8], index: usize, first: bool) -> f32 {
    let value = f32::from(byte(input, index) as i8) / 16.0;
    if first {
        value + 16.0
    } else {
        value
    }
}

fuzz_target!(|input: &[u8]| {
    if input.is_empty() {
        return;
    }

    let dimensions = DIMENSIONS[usize::from(byte(input, 0)) % DIMENSIONS.len()];
    let count = 1 + usize::from(byte(input, 1)) % 8;
    let metric = match byte(input, 2) % 3 {
        0 => DistanceMetric::L2Squared,
        1 => DistanceMetric::Cosine,
        _ => DistanceMetric::InnerProduct,
    };
    let bit_width = 2 + byte(input, 3) % 3;
    let mut seed_bytes = [0u8; 8];
    for (index, output) in seed_bytes.iter_mut().enumerate() {
        *output = byte(input, 4 + index);
    }
    let seed = u64::from_le_bytes(seed_bytes);

    let records = (0..count)
        .map(|record| ProximityRecord {
            key: (record as u64).to_be_bytes().to_vec(),
            vector: (0..dimensions)
                .map(|dimension| {
                    component(input, 12 + record * dimensions + dimension, dimension == 0)
                })
                .collect(),
            value: vec![byte(input, 12 + count * dimensions + record)],
        })
        .collect::<Vec<_>>();

    let store = Arc::new(MemStore::new());
    let mut map_config = ProximityConfig::new(dimensions as u32);
    map_config.metric = metric;
    let map = ProximityMap::build(store.clone(), map_config, records)
        .expect("bounded generated source is valid");
    let config = TurboQuantizationConfig {
        bit_width,
        seed,
        rerank_multiplier: u32::from(1 + byte(input, 11) % 16),
    };
    let (index, _) = TurboQuantizer::build(&map, config, BuildParallelism::serial())
        .expect("bounded generated accelerator is valid");
    let verification = index.verify(&map).expect("fresh accelerator verifies");
    assert_eq!(verification.encoded_vectors, count as u64);

    let manifest = index.manifest_cid().clone();
    let reopened =
        TurboQuantizer::load(store.clone(), manifest.clone()).expect("fresh accelerator reopens");
    let query = (0..dimensions)
        .map(|dimension| component(input, 20 + count * dimensions + dimension, dimension == 0))
        .collect::<Vec<_>>();
    let k = 1 + usize::from(byte(input, 19)) % count;
    let mut request = SearchRequest::exact(&query, k);
    request.policy = SearchPolicy::FixedBudget;
    request.options.backend = SearchBackend::TurboQuantized;
    request.kernel = match byte(input, 18) % 3 {
        0 => QueryKernel::ScalarDeterministic,
        1 => QueryKernel::SimdDeterministic,
        _ => QueryKernel::AutoDeterministic,
    };
    let result = reopened
        .search(&map, request)
        .expect("fresh accelerator searches");
    assert_eq!(result.neighbors.len(), k);

    let limits = ContentGraphLimits {
        max_objects: 4_096,
        max_depth: 64,
        max_bytes: 16 * 1024 * 1024,
        max_references_per_object: 4_096,
    };
    let root = TypedContentRoot::new(ContentObjectKind::TurboQuantization, manifest.clone());
    let walk = walk_content_graph(&store, &[root], &limits).expect("fresh closure walks");
    let ordered = walk
        .objects
        .iter()
        .filter(|object| object.root.kind == ContentObjectKind::OrderedNode)
        .collect::<Vec<_>>();
    let selected = ordered[usize::from(byte(input, 17)) % ordered.len()];
    let mut corrupted = selected.bytes.clone();
    let offset = usize::from(byte(input, 16)) % corrupted.len();
    corrupted[offset] ^= 1;
    Store::put(&store, selected.root.cid.as_bytes(), &corrupted)
        .expect("in-memory corruption injection");

    let reopened_map = ProximityMap::load(store.clone(), map.tree().descriptor.clone());
    let reopened_index = TurboQuantizer::load(store.clone(), manifest);
    if let (Ok(reopened_map), Ok(reopened_index)) = (reopened_map, reopened_index) {
        assert!(reopened_index.verify(&reopened_map).is_err());
    }
});
