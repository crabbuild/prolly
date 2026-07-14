use prolly::{
    BatchOp, Cid, DistanceMetric, Error, MemStore, ProximityConfig, ProximityMap, ProximityRecord,
    Store,
};
use std::fmt;
use std::sync::Arc;

#[derive(Clone)]
struct RejectDescriptorStore {
    inner: Arc<MemStore>,
}

#[derive(Debug)]
struct RejectedDescriptor;

impl fmt::Display for RejectedDescriptor {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("descriptor write rejected")
    }
}

impl std::error::Error for RejectedDescriptor {}

impl Store for RejectDescriptorStore {
    type Error = RejectedDescriptor;

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
        self.inner.get(key).map_err(|_| RejectedDescriptor)
    }

    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
        if value.starts_with(b"PRXI") {
            return Err(RejectedDescriptor);
        }
        self.inner.put(key, value).map_err(|_| RejectedDescriptor)
    }

    fn delete(&self, key: &[u8]) -> Result<(), Self::Error> {
        self.inner.delete(key).map_err(|_| RejectedDescriptor)
    }

    fn batch(&self, operations: &[BatchOp<'_>]) -> Result<(), Self::Error> {
        self.inner.batch(operations).map_err(|_| RejectedDescriptor)
    }
}

fn config() -> ProximityConfig {
    let mut config = ProximityConfig::new(3);
    config.metric = DistanceMetric::L2Squared;
    config.hierarchy.log_chunk_size = 2;
    config.hierarchy.level_hash_seed = 19;
    config.overflow.max_page_bytes = 256 * 1024;
    config
}

fn records() -> Vec<ProximityRecord> {
    (0..48)
        .map(|index| ProximityRecord {
            key: format!("record-{index:03}").into_bytes(),
            vector: vec![index as f32, (index % 5) as f32, (index % 11) as f32],
            value: vec![index as u8; index % 9],
        })
        .collect()
}

#[test]
fn proximity_roots_are_independent_of_bulk_input_order() {
    let forward_store = Arc::new(MemStore::new());
    let reverse_store = Arc::new(MemStore::new());
    let forward = ProximityMap::build(forward_store, config(), records()).unwrap();
    let mut reversed = records();
    reversed.reverse();
    let reverse = ProximityMap::build(reverse_store, config(), reversed).unwrap();

    assert_eq!(forward.tree(), reverse.tree());
}

#[test]
fn proximity_load_rejects_v1_and_trailing_bytes() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store.clone(), config(), records()).unwrap();
    let bytes = store
        .get(map.tree().descriptor.as_bytes())
        .unwrap()
        .unwrap();

    let mut bad_version = bytes.clone();
    bad_version[4] = 1;
    let bad_version_cid = Cid::from_bytes(&bad_version);
    store.put(bad_version_cid.as_bytes(), &bad_version).unwrap();
    assert!(matches!(
        ProximityMap::load(store.clone(), bad_version_cid),
        Err(Error::UnsupportedProximityVersion {
            found: 1,
            required: 2
        })
    ));

    let mut trailing = bytes;
    trailing.push(0);
    let trailing_cid = Cid::from_bytes(&trailing);
    store.put(trailing_cid.as_bytes(), &trailing).unwrap();
    assert!(matches!(
        ProximityMap::load(store, trailing_cid),
        Err(Error::InvalidProximityObject { .. })
    ));
}

#[test]
fn proximity_build_rejects_duplicate_keys_non_finite_vectors_and_large_nodes() {
    let duplicate = ProximityRecord {
        key: b"same".to_vec(),
        vector: vec![1.0, 2.0, 3.0],
        value: Vec::new(),
    };
    assert!(matches!(
        ProximityMap::build(
            Arc::new(MemStore::new()),
            config(),
            [duplicate.clone(), duplicate]
        ),
        Err(Error::DuplicateProximityKey { .. })
    ));

    assert!(matches!(
        ProximityMap::build(
            Arc::new(MemStore::new()),
            config(),
            [ProximityRecord {
                key: b"nan".to_vec(),
                vector: vec![f32::NAN, 0.0, 0.0],
                value: Vec::new(),
            }]
        ),
        Err(Error::InvalidProximityVector { .. })
    ));

    let mut tiny = config();
    tiny.overflow.min_page_bytes = 64;
    tiny.overflow.target_page_bytes = 64;
    tiny.overflow.max_page_bytes = 64;
    tiny.vector_storage.inline_threshold_bytes = 64;
    assert!(matches!(
        ProximityMap::build(
            Arc::new(MemStore::new()),
            tiny,
            [ProximityRecord {
                key: vec![b'x'; 128],
                vector: vec![0.0, 0.0, 0.0],
                value: Vec::new(),
            }]
        ),
        Err(Error::ProximityNodeTooLarge { .. })
    ));
}

#[test]
fn checked_in_empty_proximity_fixture_matches_canonical_bytes() {
    let store = Arc::new(MemStore::new());
    let map = ProximityMap::build(store.clone(), config(), []).unwrap();
    let descriptor = store
        .get(map.tree().descriptor.as_bytes())
        .unwrap()
        .unwrap();
    let root = store
        .get(map.tree().proximity_root.as_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        descriptor[4], 2,
        "descriptor must use the required proximity format"
    );
    assert_eq!(root[4], 2, "node must use the required proximity format");
    let hex = |bytes: &[u8]| {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    };
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../conformance/proximity-fixtures.json")).unwrap();
    let empty = &fixture["empty"];
    assert_eq!(
        hex(map.tree().descriptor.as_bytes()),
        empty["descriptor_cid"]
    );
    assert_eq!(hex(&descriptor), empty["descriptor_bytes"]);
    assert_eq!(hex(map.tree().proximity_root.as_bytes()), empty["root_cid"]);
    assert_eq!(hex(&root), empty["root_bytes"]);
}

#[test]
fn proximity_build_rejects_corrupt_existing_content_at_an_expected_cid() {
    let source_store = Arc::new(MemStore::new());
    let source = ProximityMap::build(source_store, config(), records()).unwrap();
    let corrupt_store = Arc::new(MemStore::new());
    corrupt_store
        .put(source.tree().proximity_root.as_bytes(), b"corrupt")
        .unwrap();

    assert!(matches!(
        ProximityMap::build(corrupt_store, config(), records()),
        Err(Error::CidMismatch { .. })
    ));
}

#[test]
fn proximity_build_publishes_the_descriptor_only_after_descendants() {
    let reference_store = Arc::new(MemStore::new());
    let reference = ProximityMap::build(reference_store, config(), records()).unwrap();
    let inner = Arc::new(MemStore::new());
    let store = RejectDescriptorStore {
        inner: inner.clone(),
    };

    assert!(matches!(
        ProximityMap::build(store, config(), records()),
        Err(Error::Store(_))
    ));
    assert!(inner
        .get(reference.tree().proximity_root.as_bytes())
        .unwrap()
        .is_some());
    assert!(inner
        .get(reference.tree().descriptor.as_bytes())
        .unwrap()
        .is_none());
}
