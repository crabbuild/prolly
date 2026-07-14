use super::codec::{
    put_bytes, put_cid, put_f64, put_varint, Reader, FORMAT_VERSION, MAX_KEY_BYTES,
    MAX_OBJECT_ENTRIES,
};
use super::{ReferenceKind, TypedReference};
use crate::prolly::cid::Cid;
use crate::prolly::error::Error;
use crate::prolly::proximity::vector::{decode_components, encode_components};

const MAGIC: &[u8; 4] = b"PRXN";
const HAS_QUANTIZER: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum PhysicalNodeKind {
    Leaf = 1,
    Route = 2,
    OverflowPage = 3,
    OverflowDirectory = 4,
}

impl PhysicalNodeKind {
    fn decode(value: u8, reader: &Reader<'_>) -> Result<Self, Error> {
        match value {
            1 => Ok(Self::Leaf),
            2 => Ok(Self::Route),
            3 => Ok(Self::OverflowPage),
            4 => Ok(Self::OverflowDirectory),
            _ => Err(reader.invalid("unknown physical node kind")),
        }
    }

    pub(crate) fn has_children(self, level: u8) -> bool {
        match self {
            Self::Leaf => false,
            Self::Route | Self::OverflowDirectory => true,
            Self::OverflowPage => level > 0,
        }
    }

    pub(crate) fn is_logical_leaf(self, level: u8) -> bool {
        matches!(self, Self::Leaf) || (matches!(self, Self::OverflowPage) && level == 0)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum VectorRef {
    Inline(Vec<f32>),
    External(Cid),
}

impl VectorRef {
    pub(crate) fn inline(&self) -> Result<&[f32], Error> {
        match self {
            Self::Inline(vector) => Ok(vector),
            Self::External(_) => Err(invalid("external vector has not been resolved")),
        }
    }

    pub(crate) fn into_inline(self) -> Result<Vec<f32>, Error> {
        match self {
            Self::Inline(vector) => Ok(vector),
            Self::External(_) => Err(invalid("external vector has not been resolved")),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProximityEntry {
    pub(crate) key: Vec<u8>,
    pub(crate) vector: VectorRef,
    pub(crate) child: Option<Cid>,
    pub(crate) child_count: u64,
    pub(crate) covering_radius: f64,
    pub(crate) min_key: Vec<u8>,
    pub(crate) max_key: Vec<u8>,
}

impl ProximityEntry {
    pub(crate) fn inline_leaf(key: Vec<u8>, vector: Vec<f32>) -> Self {
        Self {
            min_key: key.clone(),
            max_key: key.clone(),
            key,
            vector: VectorRef::Inline(vector),
            child: None,
            child_count: 1,
            covering_radius: 0.0,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ProximityNode {
    pub(crate) kind: PhysicalNodeKind,
    pub(crate) level: u8,
    pub(crate) subtree_count: u64,
    pub(crate) quantizer: Option<Cid>,
    pub(crate) entries: Vec<ProximityEntry>,
}

impl ProximityNode {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate(None)?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(FORMAT_VERSION);
        bytes.push(self.kind as u8);
        bytes.push(if self.quantizer.is_some() {
            HAS_QUANTIZER
        } else {
            0
        });
        bytes.push(self.level);
        put_varint(self.subtree_count, &mut bytes);
        put_varint(self.entries.len() as u64, &mut bytes);
        if let Some(quantizer) = &self.quantizer {
            put_cid(quantizer, &mut bytes);
        }
        for entry in &self.entries {
            put_bytes(&entry.key, &mut bytes);
            match &entry.vector {
                VectorRef::Inline(vector) => {
                    bytes.push(0);
                    encode_components(vector, &mut bytes);
                }
                VectorRef::External(cid) => {
                    bytes.push(1);
                    put_cid(cid, &mut bytes);
                }
            }
            if let Some(child) = &entry.child {
                put_cid(child, &mut bytes);
            }
            put_varint(entry.child_count, &mut bytes);
            put_f64(entry.covering_radius, &mut bytes)?;
            put_bytes(&entry.min_key, &mut bytes);
            put_bytes(&entry.max_key, &mut bytes);
        }
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8], dimensions: u32) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "node");
        reader.exact(MAGIC)?;
        reader.version()?;
        let kind = PhysicalNodeKind::decode(reader.u8()?, &reader)?;
        let flags = reader.u8()?;
        if flags & !HAS_QUANTIZER != 0 {
            return Err(reader.invalid("unknown flags"));
        }
        let level = reader.u8()?;
        let subtree_count = reader.varint()?;
        let entry_count = reader.bounded_usize(MAX_OBJECT_ENTRIES)?;
        let quantizer = if flags & HAS_QUANTIZER != 0 {
            Some(reader.cid()?)
        } else {
            None
        };
        if entry_count > reader.remaining() / 12 {
            return Err(reader.invalid("entry count is impossible for object length"));
        }
        let component_bytes = usize::try_from(dimensions)
            .ok()
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| reader.invalid("vector length overflow"))?;
        let mut entries = Vec::with_capacity(entry_count);
        for _ in 0..entry_count {
            let key = reader.bytes(MAX_KEY_BYTES)?;
            let vector = match reader.u8()? {
                0 => VectorRef::Inline(decode_components(
                    reader.take(component_bytes)?,
                    dimensions,
                )?),
                1 => VectorRef::External(reader.cid()?),
                _ => return Err(reader.invalid("invalid vector reference tag")),
            };
            let child = if kind.has_children(level) {
                Some(reader.cid()?)
            } else {
                None
            };
            entries.push(ProximityEntry {
                key,
                vector,
                child,
                child_count: reader.varint()?,
                covering_radius: reader.f64()?,
                min_key: reader.bytes(MAX_KEY_BYTES)?,
                max_key: reader.bytes(MAX_KEY_BYTES)?,
            });
        }
        reader.finish()?;
        let node = Self {
            kind,
            level,
            subtree_count,
            quantizer,
            entries,
        };
        node.validate(Some(dimensions))?;
        Ok(node)
    }

    #[allow(dead_code)] // Used by typed graph traversal in the integration slice.
    pub(crate) fn references(bytes: &[u8], dimensions: u32) -> Result<Vec<TypedReference>, Error> {
        let node = Self::decode(bytes, dimensions)?;
        let mut references = Vec::with_capacity(
            node.entries.len().saturating_mul(2) + usize::from(node.quantizer.is_some()),
        );
        if let Some(cid) = node.quantizer {
            references.push(TypedReference {
                kind: ReferenceKind::ScalarQuantizer,
                cid,
            });
        }
        for entry in node.entries {
            if let VectorRef::External(cid) = entry.vector {
                references.push(TypedReference {
                    kind: ReferenceKind::ExternalVector,
                    cid,
                });
            }
            if let Some(cid) = entry.child {
                references.push(TypedReference {
                    kind: ReferenceKind::ProximityNode,
                    cid,
                });
            }
        }
        Ok(references)
    }

    fn validate(&self, dimensions: Option<u32>) -> Result<(), Error> {
        if (self.kind == PhysicalNodeKind::Leaf && self.level != 0)
            || (self.kind == PhysicalNodeKind::Route && self.level == 0)
            || (self.kind == PhysicalNodeKind::OverflowDirectory && self.entries.is_empty())
        {
            return Err(invalid("logical level disagrees with physical node kind"));
        }
        let mut previous: Option<&[u8]> = None;
        let mut count = 0u64;
        for entry in &self.entries {
            if previous.is_some_and(|key| key >= entry.key.as_slice()) {
                return Err(invalid("entry keys must be strictly ascending"));
            }
            let leaf = self.kind.is_logical_leaf(self.level);
            if self.kind.has_children(self.level) == entry.child.is_none() {
                return Err(invalid("physical kind/child mismatch"));
            }
            if let VectorRef::Inline(vector) = &entry.vector {
                if vector
                    .iter()
                    .any(|component| !component.is_finite() || component.to_bits() == 0x8000_0000)
                {
                    return Err(invalid("inline vector is non-canonical"));
                }
                if dimensions.is_some_and(|dimensions| vector.len() != dimensions as usize) {
                    return Err(invalid("inline vector dimension mismatch"));
                }
            }
            if entry.child_count == 0 {
                return Err(invalid("child logical count must be non-zero"));
            }
            if !entry.covering_radius.is_finite()
                || entry.covering_radius < 0.0
                || entry.covering_radius.to_bits() == 0x8000_0000_0000_0000
            {
                return Err(invalid("covering radius is non-canonical"));
            }
            if entry.min_key > entry.key
                || entry.key > entry.max_key
                || entry.min_key > entry.max_key
            {
                return Err(invalid("invalid subtree key bounds"));
            }
            if leaf
                && (entry.child_count != 1
                    || entry.covering_radius != 0.0
                    || entry.min_key != entry.key
                    || entry.max_key != entry.key)
            {
                return Err(invalid("leaf summary is not canonical"));
            }
            count = count
                .checked_add(entry.child_count)
                .ok_or_else(|| invalid("subtree count overflow"))?;
            previous = Some(&entry.key);
        }
        if count != self.subtree_count {
            return Err(invalid("subtree count does not equal entry summaries"));
        }
        Ok(())
    }
}

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "node",
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn leaf_and_route_round_trip_with_canonical_summaries() {
        let leaf = ProximityNode {
            kind: PhysicalNodeKind::Leaf,
            level: 0,
            subtree_count: 1,
            quantizer: None,
            entries: vec![ProximityEntry::inline_leaf(b"a".to_vec(), vec![0.0, 1.0])],
        };
        let leaf_bytes = leaf.encode().unwrap();
        assert_eq!(&leaf_bytes[..6], b"PRXN\x02\x01");
        assert_eq!(ProximityNode::decode(&leaf_bytes, 2).unwrap(), leaf);

        let route = ProximityNode {
            kind: PhysicalNodeKind::Route,
            level: 1,
            subtree_count: 1,
            quantizer: Some(Cid::from_bytes(b"quantizer")),
            entries: vec![ProximityEntry {
                key: b"a".to_vec(),
                vector: VectorRef::Inline(vec![0.0, 1.0]),
                child: Some(Cid::from_bytes(&leaf_bytes)),
                child_count: 1,
                covering_radius: 0.0,
                min_key: b"a".to_vec(),
                max_key: b"a".to_vec(),
            }],
        };
        let route_bytes = route.encode().unwrap();
        assert_eq!(ProximityNode::decode(&route_bytes, 2).unwrap(), route);
        assert_eq!(ProximityNode::references(&route_bytes, 2).unwrap().len(), 2);
    }

    #[test]
    fn node_rejects_bad_flags_counts_bounds_radii_and_ordering() {
        let mut node = ProximityNode {
            kind: PhysicalNodeKind::Leaf,
            level: 0,
            subtree_count: 2,
            quantizer: None,
            entries: vec![
                ProximityEntry::inline_leaf(b"a".to_vec(), vec![0.0]),
                ProximityEntry::inline_leaf(b"b".to_vec(), vec![1.0]),
            ],
        };
        let mut bad_flags = node.encode().unwrap();
        bad_flags[6] = 0x80;
        assert!(ProximityNode::decode(&bad_flags, 1).is_err());

        node.subtree_count = 1;
        assert!(node.encode().is_err());
        node.subtree_count = 2;
        node.entries[0].min_key = b"z".to_vec();
        assert!(node.encode().is_err());
        node.entries[0].min_key = b"a".to_vec();
        node.entries[0].covering_radius = f64::NAN;
        assert!(node.encode().is_err());
        node.entries[0].covering_radius = 0.0;
        node.entries.swap(0, 1);
        assert!(node.encode().is_err());
    }
}
