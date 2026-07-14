use super::super::cid::Cid;
use super::super::error::Error;
use super::codec::{put_varint, Reader};
use super::vector::{decode_components, encode_components};

const MAGIC: &[u8; 4] = b"PRXN";
const VERSION: u8 = 1;
const LEAF_FLAG: u8 = 1;

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProximityEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) vector: Vec<f32>,
    pub(crate) child: Option<Cid>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProximityNode {
    pub(crate) level: u8,
    pub(crate) subtree_count: u64,
    pub(crate) entries: Vec<ProximityEntry>,
}

impl ProximityNode {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        let leaf = self.level == 0;
        let mut previous: Option<&[u8]> = None;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        bytes.push(if leaf { LEAF_FLAG } else { 0 });
        bytes.push(self.level);
        put_varint(self.subtree_count, &mut bytes);
        put_varint(self.entries.len() as u64, &mut bytes);
        for entry in &self.entries {
            if previous.is_some_and(|key| key >= entry.key.as_slice()) {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "entry keys must be strictly ascending".to_owned(),
                });
            }
            if leaf != entry.child.is_none() {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "leaf/child mismatch".to_owned(),
                });
            }
            put_varint(entry.key.len() as u64, &mut bytes);
            bytes.extend_from_slice(&entry.key);
            encode_components(&entry.vector, &mut bytes);
            if let Some(child) = &entry.child {
                bytes.extend_from_slice(child.as_bytes());
            }
            previous = Some(&entry.key);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], dimensions: u32) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "node");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "unsupported version".to_owned(),
            });
        }
        let flags = reader.u8()?;
        if flags & !LEAF_FLAG != 0 {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "unknown flags".to_owned(),
            });
        }
        let level = reader.u8()?;
        let leaf = flags & LEAF_FLAG != 0;
        if leaf != (level == 0) {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "leaf flag disagrees with level".to_owned(),
            });
        }
        let subtree_count = reader.varint()?;
        let entry_count = reader.usize()?;
        let component_bytes = usize::try_from(dimensions)
            .ok()
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "node",
                reason: "vector length overflow".to_owned(),
            })?;
        let mut entries = Vec::with_capacity(entry_count.min(1_000_000));
        for _ in 0..entry_count {
            let key_len = reader.usize()?;
            let key = reader.take(key_len)?.to_vec();
            if entries
                .last()
                .is_some_and(|entry: &ProximityEntry| entry.key >= key)
            {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "entry keys are not strictly ascending".to_owned(),
                });
            }
            let vector = decode_components(reader.take(component_bytes)?, dimensions)?;
            let child = if leaf {
                None
            } else {
                let raw: [u8; 32] =
                    reader
                        .take(32)?
                        .try_into()
                        .map_err(|_| Error::InvalidProximityObject {
                            kind: "node",
                            reason: "invalid child CID".to_owned(),
                        })?;
                Some(Cid(raw))
            };
            entries.push(ProximityEntry { key, vector, child });
        }
        reader.finish()?;
        Ok(Self {
            level,
            subtree_count,
            entries,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex(bytes: &[u8]) -> String {
        bytes.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    #[test]
    fn leaf_node_round_trip_is_strict() {
        let node = ProximityNode {
            level: 0,
            subtree_count: 2,
            entries: vec![
                ProximityEntry {
                    key: b"a".to_vec(),
                    vector: vec![0.0, 1.0],
                    child: None,
                },
                ProximityEntry {
                    key: b"b".to_vec(),
                    vector: vec![1.0, 0.0],
                    child: None,
                },
            ],
        };
        let bytes = node.encode().unwrap();
        assert_eq!(ProximityNode::decode(&bytes, 2).unwrap(), node);
    }

    #[test]
    fn node_encoder_rejects_duplicate_or_unsorted_keys() {
        let node = ProximityNode {
            level: 0,
            subtree_count: 2,
            entries: vec![
                ProximityEntry {
                    key: b"b".to_vec(),
                    vector: vec![0.0],
                    child: None,
                },
                ProximityEntry {
                    key: b"a".to_vec(),
                    vector: vec![1.0],
                    child: None,
                },
            ],
        };
        assert!(node.encode().is_err());
    }

    #[test]
    fn leaf_and_internal_nodes_match_checked_in_golden_objects() {
        let leaf = ProximityNode {
            level: 0,
            subtree_count: 2,
            entries: vec![
                ProximityEntry {
                    key: b"a".to_vec(),
                    vector: vec![0.0, 1.0],
                    child: None,
                },
                ProximityEntry {
                    key: b"b".to_vec(),
                    vector: vec![1.0, 0.0],
                    child: None,
                },
            ],
        };
        let leaf_bytes = leaf.encode().unwrap();
        let leaf_cid = Cid::from_bytes(&leaf_bytes);
        let internal = ProximityNode {
            level: 1,
            subtree_count: 2,
            entries: vec![ProximityEntry {
                key: b"a".to_vec(),
                vector: vec![0.0, 1.0],
                child: Some(leaf_cid.clone()),
            }],
        };
        let internal_bytes = internal.encode().unwrap();
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../../../conformance/proximity-fixtures.v1.json"
        ))
        .unwrap();

        for (name, bytes) in [("leaf", leaf_bytes), ("internal", internal_bytes)] {
            let object = &fixture["objects"][name];
            assert_eq!(hex(&bytes), object["bytes"]);
            assert_eq!(hex(Cid::from_bytes(&bytes).as_bytes()), object["cid"]);
            assert_eq!(
                ProximityNode::decode(&bytes, 2).unwrap().encode().unwrap(),
                bytes
            );
        }
    }
}
