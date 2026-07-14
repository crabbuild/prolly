pub(crate) mod codec;
pub(crate) mod descriptor;
pub(crate) mod node;
pub(crate) mod overflow;
#[allow(dead_code)] // Persisted local quantizers are wired into construction in the SQ8 slice.
pub(crate) mod quantized;
pub(crate) mod record;
pub(crate) mod vector;

pub(crate) use descriptor::Descriptor;
pub(crate) use node::{PhysicalNodeKind, ProximityEntry, ProximityNode, VectorRef};
pub(crate) use record::StoredRecord;

#[allow(dead_code)] // Consumed by the typed graph walker and replication slices.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ReferenceKind {
    OrderedNode,
    ProximityNode,
    ExternalVector,
    ScalarQuantizer,
}

#[allow(dead_code)] // Consumed by the typed graph walker and replication slices.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TypedReference {
    pub(crate) kind: ReferenceKind,
    pub(crate) cid: crate::prolly::cid::Cid,
}

#[cfg(test)]
mod fixture_tests {
    use super::node::PhysicalNodeKind;
    use super::quantized::ScalarQuantized;
    use super::vector::ExternalVector;
    use super::*;
    use crate::prolly::cid::Cid;
    use crate::prolly::proximity::{DistanceMetric, ProximityConfig, ProximityMap};
    use crate::prolly::store::{MemStore, Store};
    use std::sync::Arc;

    fn hex(bytes: &[u8]) -> String {
        bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>()
    }

    fn assert_object(fixture: &serde_json::Value, name: &str, bytes: &[u8]) {
        let object = &fixture["objects"][name];
        assert_eq!(hex(Cid::from_bytes(bytes).as_bytes()), object["cid"]);
        assert_eq!(hex(bytes), object["bytes"]);
    }

    fn decode_hex(value: &str) -> Vec<u8> {
        value
            .as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let digit = |byte: u8| match byte {
                    b'0'..=b'9' => byte - b'0',
                    b'a'..=b'f' => byte - b'a' + 10,
                    _ => panic!("fixture contains non-hex byte"),
                };
                (digit(pair[0]) << 4) | digit(pair[1])
            })
            .collect()
    }

    #[test]
    fn every_v2_object_matches_the_frozen_fixture() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/proximity-fixtures.v2.json"
        ))
        .unwrap();
        let record = StoredRecord::new(
            &[-0.0, 2.5],
            b"value".to_vec(),
            DistanceMetric::L2Squared,
            2,
        )
        .unwrap();
        assert_object(&fixture, "record", &record.encode());
        let leaf = ProximityNode {
            kind: PhysicalNodeKind::Leaf,
            level: 0,
            subtree_count: 2,
            quantizer: None,
            entries: vec![
                ProximityEntry::inline_leaf(b"a".to_vec(), vec![0.0, 1.0]),
                ProximityEntry::inline_leaf(b"b".to_vec(), vec![1.0, 0.0]),
            ],
        };
        let leaf_bytes = leaf.encode().unwrap();
        assert_object(&fixture, "leaf", &leaf_bytes);
        let route = ProximityNode {
            kind: PhysicalNodeKind::Route,
            level: 1,
            subtree_count: 2,
            quantizer: None,
            entries: vec![ProximityEntry {
                key: b"a".to_vec(),
                vector: VectorRef::Inline(vec![0.0, 1.0]),
                child: Some(Cid::from_bytes(&leaf_bytes)),
                child_count: 2,
                covering_radius: 1.0,
                min_key: b"a".to_vec(),
                max_key: b"b".to_vec(),
            }],
        };
        assert_object(&fixture, "route", &route.encode().unwrap());
        let vector = ExternalVector {
            vector: vec![0.0, 2.5],
            norm: Some(2.5),
        };
        let vector_bytes = vector.encode().unwrap();
        assert_object(&fixture, "vector", &vector_bytes);
        assert_eq!(ExternalVector::decode(&vector_bytes).unwrap(), vector);
        let sq8 = ScalarQuantized {
            dimensions: 2,
            group_size: 2,
            entry_count: 2,
            scales: vec![0.5],
            max_error: 0.25,
            values: vec![0, 5, 2, 0],
        };
        let sq8_bytes = sq8.encode().unwrap();
        assert_object(&fixture, "sq8", &sq8_bytes);
        assert_eq!(ScalarQuantized::decode(&sq8_bytes).unwrap(), sq8);
        let store = Arc::new(MemStore::new());
        let mut config = ProximityConfig::new(3);
        config.hierarchy.log_chunk_size = 2;
        config.hierarchy.level_hash_seed = 19;
        config.overflow.max_page_bytes = 256 * 1024;
        let map = ProximityMap::build(store.clone(), config, []).unwrap();
        let descriptor = store
            .get(map.tree().descriptor.as_bytes())
            .unwrap()
            .unwrap();
        let root = store
            .get(map.tree().proximity_root.as_bytes())
            .unwrap()
            .unwrap();
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
    fn legacy_v1_objects_are_rejected_with_the_typed_version_error() {
        let legacy: serde_json::Value = serde_json::from_str(include_str!(
            "../../../../conformance/legacy/proximity-fixtures.v1.json"
        ))
        .unwrap();
        let record = decode_hex(legacy["objects"]["record"]["bytes"].as_str().unwrap());
        let node = decode_hex(legacy["objects"]["leaf"]["bytes"].as_str().unwrap());
        let descriptor = decode_hex(legacy["empty"]["descriptor_bytes"].as_str().unwrap());
        for error in [
            StoredRecord::decode(&record, 2).unwrap_err(),
            ProximityNode::decode(&node, 2).unwrap_err(),
            Descriptor::decode(&descriptor).unwrap_err(),
        ] {
            assert!(matches!(
                error,
                crate::prolly::error::Error::UnsupportedProximityVersion {
                    found: 1,
                    required: 2
                }
            ));
        }
    }
}
