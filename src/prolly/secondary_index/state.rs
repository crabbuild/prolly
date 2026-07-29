use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use super::super::cid::Cid;
use super::super::error::{Error, Mutation};
use super::super::key::{decode_segments, KeyBuilder};
use super::super::store::Store;
use super::super::tree::Tree;
use super::super::Prolly;
use super::definition::{IndexProjection, SecondaryIndex};

/// The only secondary-index collection format accepted by this release.
///
/// Incompatible changes replace this value through a hard cutover. The engine
/// never keeps multiple format readers alive.
pub const INDEXED_COLLECTION_FORMAT: u32 = 1;

const ROOT_PREFIX: &[u8] = b"\0prolly/indexed-collection/";
const ROOT_SUFFIX: &[u8] = b"/state";
const MAX_STATE_VALUE_BYTES: usize = 16 * 1024 * 1024;

const META_FORMAT: &[u8] = b"format";
const META_SOURCE: &[u8] = b"source-map-id";
const META_POLICY: &[u8] = b"collection-policy";
const HEAD: &[u8] = b"head";
const SNAPSHOTS: &[u8] = b"snapshots";
const DESCRIPTORS: &[u8] = b"descriptors";
const ACTIVE: &[u8] = b"active";
const RETIRED: &[u8] = b"retired";
const PINS: &[u8] = b"pins";

fn key(segments: &[&[u8]]) -> Vec<u8> {
    segments
        .iter()
        .fold(KeyBuilder::new(), |builder, segment| {
            builder.push_segment(segment)
        })
        .finish()
}

fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, Error> {
    let bytes = serde_cbor::to_vec(value).map_err(|error| Error::Serialize(error.to_string()))?;
    if bytes.len() > MAX_STATE_VALUE_BYTES {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "indexed_collection_state_value_bytes",
            limit: MAX_STATE_VALUE_BYTES,
            actual: bytes.len(),
        });
    }
    Ok(bytes)
}

fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, Error> {
    if bytes.len() > MAX_STATE_VALUE_BYTES {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "indexed_collection_state_value_bytes",
            limit: MAX_STATE_VALUE_BYTES,
            actual: bytes.len(),
        });
    }
    let mut decoder = serde_cbor::Deserializer::from_slice(bytes);
    let value =
        T::deserialize(&mut decoder).map_err(|error| Error::Deserialize(error.to_string()))?;
    decoder
        .end()
        .map_err(|error| Error::Deserialize(error.to_string()))?;
    Ok(value)
}

fn cid(bytes: &[u8], field: &str) -> Result<Cid, Error> {
    let value: [u8; 32] = bytes.try_into().map_err(|_| {
        Error::Deserialize(format!(
            "{field} must contain exactly 32 bytes, got {}",
            bytes.len()
        ))
    })?;
    Ok(Cid(value))
}

fn hex(bytes: &[u8]) -> Vec<u8> {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = Vec::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[(byte >> 4) as usize]);
        encoded.push(DIGITS[(byte & 0x0f) as usize]);
    }
    encoded
}

/// Canonical mutable root name for a managed indexed collection.
pub fn indexed_collection_root_name(source_map_id: &[u8]) -> Result<Vec<u8>, Error> {
    if source_map_id.is_empty() {
        return Err(Error::InvalidIndexDefinition {
            reason: "indexed source map ID must not be empty".to_string(),
        });
    }
    let mut name =
        Vec::with_capacity(ROOT_PREFIX.len() + source_map_id.len() * 2 + ROOT_SUFFIX.len());
    name.extend_from_slice(ROOT_PREFIX);
    name.extend_from_slice(&hex(source_map_id));
    name.extend_from_slice(ROOT_SUFFIX);
    Ok(name)
}

/// Semantic collection limits that participate in persisted state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionIndexPolicy {
    pub max_active_indexes: usize,
}

impl Default for CollectionIndexPolicy {
    fn default() -> Self {
        Self {
            max_active_indexes: 32,
        }
    }
}

impl CollectionIndexPolicy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_active_indexes == 0 {
            return Err(Error::InvalidIndexDefinition {
                reason: "max_active_indexes must be greater than zero".to_string(),
            });
        }
        Ok(())
    }
}

/// Per-record semantic limits committed by an index descriptor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexSemanticLimits {
    pub max_term_bytes: usize,
    pub max_projection_bytes: usize,
    pub max_all_value_bytes: usize,
    pub max_terms_per_record: usize,
    pub max_projected_bytes_per_record: usize,
}

/// Canonical persisted definition for one extractor generation.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDescriptor {
    pub source_map_id: Vec<u8>,
    pub name: Vec<u8>,
    pub generation: u64,
    pub extractor_id: String,
    pub projection: IndexProjection,
    pub limits: IndexSemanticLimits,
    pub physical_layout: u32,
    pub fingerprint: Cid,
}

impl IndexDescriptor {
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode(self)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let descriptor: Self = decode(bytes)?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn from_runtime(source_map_id: &[u8], index: &SecondaryIndex) -> Result<Self, Error> {
        let mut descriptor = Self {
            source_map_id: source_map_id.to_vec(),
            name: index.name().to_vec(),
            generation: index.generation(),
            extractor_id: index.extractor_id().to_string(),
            projection: index.projection(),
            limits: IndexSemanticLimits {
                max_term_bytes: index.limits().max_term_bytes,
                max_projection_bytes: index.limits().max_projection_bytes,
                max_all_value_bytes: index.limits().max_all_value_bytes,
                max_terms_per_record: index.limits().max_terms_per_record,
                max_projected_bytes_per_record: index.limits().max_projected_bytes_per_record,
            },
            physical_layout: super::storage::INDEX_PHYSICAL_LAYOUT_VERSION,
            fingerprint: Cid([0; 32]),
        };
        descriptor.fingerprint = descriptor.canonical_fingerprint()?;
        descriptor.validate()?;
        Ok(descriptor)
    }

    pub fn canonical_fingerprint(&self) -> Result<Cid, Error> {
        #[derive(Serialize)]
        struct Fingerprint<'a> {
            source_map_id: &'a [u8],
            name: &'a [u8],
            generation: u64,
            extractor_id: &'a str,
            projection: IndexProjection,
            limits: &'a IndexSemanticLimits,
            physical_layout: u32,
        }
        Ok(Cid::from_bytes(&encode(&Fingerprint {
            source_map_id: &self.source_map_id,
            name: &self.name,
            generation: self.generation,
            extractor_id: &self.extractor_id,
            projection: self.projection,
            limits: &self.limits,
            physical_layout: self.physical_layout,
        })?))
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.source_map_id.is_empty()
            || self.name.is_empty()
            || self.generation == 0
            || self.extractor_id.is_empty()
            || self.physical_layout == 0
        {
            return Err(Error::InvalidIndexDefinition {
                reason: "persisted index descriptor has invalid required fields".to_string(),
            });
        }
        let expected = self.canonical_fingerprint()?;
        if expected != self.fingerprint {
            return Err(Error::IndexDefinitionMismatch {
                name: self.name.clone(),
                persisted: self.fingerprint.clone(),
                runtime: expected,
            });
        }
        Ok(())
    }
}

/// Stable identifier of a canonical indexed snapshot record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct IndexedSnapshotId(pub Cid);

impl IndexedSnapshotId {
    pub fn as_cid(&self) -> &Cid {
        &self.0
    }
}

/// Immutable source tree selected by one indexed snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceSnapshotRef {
    pub tree: Tree,
    pub entry_count: u64,
}

/// Immutable physical index selected by one indexed snapshot.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexSnapshotRef {
    pub name: Vec<u8>,
    pub descriptor_fingerprint: Cid,
    pub tree: Tree,
    pub entry_count: u64,
}

/// Canonical record that binds one source tree to exact physical indexes.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexedSnapshotRecord {
    pub source_map_id: Vec<u8>,
    pub parent: Option<IndexedSnapshotId>,
    pub source: SourceSnapshotRef,
    pub indexes: Vec<IndexSnapshotRef>,
}

impl IndexedSnapshotRecord {
    pub fn id(&self) -> Result<IndexedSnapshotId, Error> {
        self.validate()?;
        Ok(IndexedSnapshotId(Cid::from_bytes(&encode(self)?)))
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.source_map_id.is_empty() {
            return Err(Error::InvalidVersionedMap(
                "indexed snapshot has an empty source map ID".to_string(),
            ));
        }
        let mut previous: Option<&[u8]> = None;
        for index in &self.indexes {
            if index.name.is_empty() {
                return Err(Error::InvalidVersionedMap(
                    "indexed snapshot has an empty index name".to_string(),
                ));
            }
            if previous.is_some_and(|previous| previous >= index.name.as_slice()) {
                return Err(Error::InvalidVersionedMap(
                    "indexed snapshot indexes are not strictly ordered".to_string(),
                ));
            }
            previous = Some(&index.name);
        }
        Ok(())
    }
}

/// Explicit retention pin stored in collection state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPin {
    pub snapshot: IndexedSnapshotId,
}

/// Complete authoritative state for one managed indexed collection.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedCollectionState {
    pub source_map_id: Vec<u8>,
    pub policy: CollectionIndexPolicy,
    pub head: IndexedSnapshotId,
    pub snapshots: BTreeMap<IndexedSnapshotId, IndexedSnapshotRecord>,
    pub descriptors: BTreeMap<(Vec<u8>, Cid), IndexDescriptor>,
    pub active: BTreeMap<Vec<u8>, Cid>,
    pub retired: BTreeSet<(Vec<u8>, Cid)>,
    pub pins: BTreeMap<Vec<u8>, SnapshotPin>,
}

impl IndexedCollectionState {
    pub fn new(
        source_map_id: Vec<u8>,
        policy: CollectionIndexPolicy,
        head: IndexedSnapshotRecord,
    ) -> Result<Self, Error> {
        policy.validate()?;
        if head.source_map_id != source_map_id {
            return Err(Error::InvalidVersionedMap(
                "initial indexed snapshot belongs to another source map".to_string(),
            ));
        }
        let head_id = head.id()?;
        let mut snapshots = BTreeMap::new();
        snapshots.insert(head_id.clone(), head);
        let state = Self {
            source_map_id,
            policy,
            head: head_id,
            snapshots,
            descriptors: BTreeMap::new(),
            active: BTreeMap::new(),
            retired: BTreeSet::new(),
            pins: BTreeMap::new(),
        };
        state.validate_closure()?;
        Ok(state)
    }

    pub fn head_snapshot(&self) -> Result<&IndexedSnapshotRecord, Error> {
        self.snapshots.get(&self.head).ok_or_else(|| {
            Error::InvalidVersionedMap(
                "indexed collection head is absent from retained snapshots".to_string(),
            )
        })
    }

    pub fn validate_closure(&self) -> Result<(), Error> {
        if self.source_map_id.is_empty() {
            return Err(Error::InvalidVersionedMap(
                "indexed collection has an empty source map ID".to_string(),
            ));
        }
        self.policy.validate()?;
        if self.active.len() > self.policy.max_active_indexes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "active_indexes",
                limit: self.policy.max_active_indexes,
                actual: self.active.len(),
            });
        }
        let head = self.head_snapshot()?;
        if head.source_map_id != self.source_map_id {
            return Err(Error::InvalidVersionedMap(
                "indexed collection head belongs to another source map".to_string(),
            ));
        }
        for (id, snapshot) in &self.snapshots {
            snapshot.validate()?;
            if snapshot.source_map_id != self.source_map_id || snapshot.id()? != *id {
                return Err(Error::InvalidVersionedMap(
                    "indexed snapshot identity or ownership mismatch".to_string(),
                ));
            }
            for index in &snapshot.indexes {
                let descriptor = self
                    .descriptors
                    .get(&(index.name.clone(), index.descriptor_fingerprint.clone()))
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(
                            "indexed snapshot references a missing descriptor".to_string(),
                        )
                    })?;
                descriptor.validate()?;
                if descriptor.source_map_id != self.source_map_id
                    || descriptor.name != index.name
                    || descriptor.fingerprint != index.descriptor_fingerprint
                {
                    return Err(Error::InvalidVersionedMap(
                        "indexed snapshot descriptor ownership mismatch".to_string(),
                    ));
                }
            }
        }
        for (name, fingerprint) in &self.active {
            if !self
                .descriptors
                .contains_key(&(name.clone(), fingerprint.clone()))
            {
                return Err(Error::InvalidVersionedMap(
                    "active index references a missing descriptor".to_string(),
                ));
            }
            let selected = head.indexes.iter().find(|index| &index.name == name);
            if selected.map(|index| &index.descriptor_fingerprint) != Some(fingerprint) {
                return Err(Error::InvalidVersionedMap(
                    "active index does not match the head snapshot".to_string(),
                ));
            }
        }
        for pin in self.pins.values() {
            if !self.snapshots.contains_key(&pin.snapshot) {
                return Err(Error::InvalidVersionedMap(
                    "snapshot pin references an absent snapshot".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub fn to_tree<S: Store>(&self, prolly: &Prolly<S>) -> Result<Tree, Error> {
        self.validate_closure()?;
        let mut mutations = vec![
            Mutation::Upsert {
                key: key(&[b"meta", META_FORMAT]),
                val: INDEXED_COLLECTION_FORMAT.to_be_bytes().to_vec(),
            },
            Mutation::Upsert {
                key: key(&[b"meta", META_SOURCE]),
                val: self.source_map_id.clone(),
            },
            Mutation::Upsert {
                key: key(&[b"meta", META_POLICY]),
                val: encode(&self.policy)?,
            },
            Mutation::Upsert {
                key: key(&[HEAD]),
                val: self.head.0.as_bytes().to_vec(),
            },
        ];
        for (id, snapshot) in &self.snapshots {
            mutations.push(Mutation::Upsert {
                key: key(&[SNAPSHOTS, id.0.as_bytes()]),
                val: encode(snapshot)?,
            });
        }
        for ((name, fingerprint), descriptor) in &self.descriptors {
            mutations.push(Mutation::Upsert {
                key: key(&[DESCRIPTORS, name, fingerprint.as_bytes()]),
                val: encode(descriptor)?,
            });
        }
        for (name, fingerprint) in &self.active {
            mutations.push(Mutation::Upsert {
                key: key(&[ACTIVE, name]),
                val: fingerprint.as_bytes().to_vec(),
            });
        }
        for (name, fingerprint) in &self.retired {
            mutations.push(Mutation::Upsert {
                key: key(&[RETIRED, name, fingerprint.as_bytes()]),
                val: Vec::new(),
            });
        }
        for (pin_id, pin) in &self.pins {
            mutations.push(Mutation::Upsert {
                key: key(&[PINS, pin_id]),
                val: encode(pin)?,
            });
        }
        prolly.batch(&prolly.create(), mutations)
    }

    pub fn from_tree<S: Store>(prolly: &Prolly<S>, tree: &Tree) -> Result<Self, Error> {
        let mut source_map_id = None;
        let mut policy = None;
        let mut head = None;
        let mut snapshots = BTreeMap::new();
        let mut descriptors = BTreeMap::new();
        let mut active = BTreeMap::new();
        let mut retired = BTreeSet::new();
        let mut pins = BTreeMap::new();
        let mut saw_format = false;

        for entry in prolly.range(tree, b"", None)? {
            let (entry_key, value) = entry?;
            let segments = decode_segments(&entry_key)
                .map_err(|error| Error::Deserialize(error.to_string()))?;
            match segments.as_slice() {
                [kind, field] if kind.as_slice() == b"meta" && field == META_FORMAT => {
                    if saw_format || value.as_slice() != INDEXED_COLLECTION_FORMAT.to_be_bytes() {
                        return Err(Error::Deserialize(
                            "unsupported or duplicate indexed collection format".to_string(),
                        ));
                    }
                    saw_format = true;
                }
                [kind, field] if kind.as_slice() == b"meta" && field == META_SOURCE => {
                    if source_map_id.replace(value).is_some() {
                        return Err(Error::Deserialize(
                            "duplicate indexed collection source ID".to_string(),
                        ));
                    }
                }
                [kind, field] if kind.as_slice() == b"meta" && field == META_POLICY => {
                    if policy.replace(decode(&value)?).is_some() {
                        return Err(Error::Deserialize(
                            "duplicate indexed collection policy".to_string(),
                        ));
                    }
                }
                [kind] if kind.as_slice() == HEAD => {
                    if head
                        .replace(IndexedSnapshotId(cid(&value, "indexed snapshot ID")?))
                        .is_some()
                    {
                        return Err(Error::Deserialize(
                            "duplicate indexed collection head".to_string(),
                        ));
                    }
                }
                [kind, id] if kind.as_slice() == SNAPSHOTS => {
                    let id = IndexedSnapshotId(cid(id, "snapshot key")?);
                    if snapshots.insert(id, decode(&value)?).is_some() {
                        return Err(Error::Deserialize("duplicate indexed snapshot".to_string()));
                    }
                }
                [kind, name, fingerprint] if kind.as_slice() == DESCRIPTORS => {
                    let fingerprint = cid(fingerprint, "descriptor key")?;
                    if descriptors
                        .insert((name.clone(), fingerprint), decode(&value)?)
                        .is_some()
                    {
                        return Err(Error::Deserialize(
                            "duplicate indexed descriptor".to_string(),
                        ));
                    }
                }
                [kind, name] if kind.as_slice() == ACTIVE => {
                    if active
                        .insert(name.clone(), cid(&value, "active descriptor")?)
                        .is_some()
                    {
                        return Err(Error::Deserialize("duplicate active index".to_string()));
                    }
                }
                [kind, name, fingerprint] if kind.as_slice() == RETIRED => {
                    if !value.is_empty() {
                        return Err(Error::Deserialize(
                            "retired descriptor marker must be empty".to_string(),
                        ));
                    }
                    if !retired.insert((name.clone(), cid(fingerprint, "retired descriptor")?)) {
                        return Err(Error::Deserialize(
                            "duplicate retired descriptor".to_string(),
                        ));
                    }
                }
                [kind, pin_id] if kind.as_slice() == PINS => {
                    if pins.insert(pin_id.clone(), decode(&value)?).is_some() {
                        return Err(Error::Deserialize("duplicate snapshot pin".to_string()));
                    }
                }
                _ => {
                    return Err(Error::Deserialize(
                        "unknown indexed collection state key".to_string(),
                    ));
                }
            }
        }

        if !saw_format {
            return Err(Error::Deserialize(
                "indexed collection state is missing format".to_string(),
            ));
        }
        let state = Self {
            source_map_id: source_map_id.ok_or_else(|| {
                Error::Deserialize("indexed collection state is missing source ID".to_string())
            })?,
            policy: policy.ok_or_else(|| {
                Error::Deserialize("indexed collection state is missing policy".to_string())
            })?,
            head: head.ok_or_else(|| {
                Error::Deserialize("indexed collection state is missing head".to_string())
            })?,
            snapshots,
            descriptors,
            active,
            retired,
            pins,
        };
        state.validate_closure()?;
        Ok(state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Config, MemStore};

    fn empty_state() -> IndexedCollectionState {
        IndexedCollectionState::new(
            b"users".to_vec(),
            CollectionIndexPolicy::default(),
            IndexedSnapshotRecord {
                source_map_id: b"users".to_vec(),
                parent: None,
                source: SourceSnapshotRef {
                    tree: Tree::default(),
                    entry_count: 0,
                },
                indexes: Vec::new(),
            },
        )
        .unwrap()
    }

    #[test]
    fn state_round_trips_through_canonical_tree() {
        let prolly = Prolly::new(MemStore::new(), Config::default());
        let state = empty_state();
        let tree = state.to_tree(&prolly).unwrap();
        assert_eq!(
            IndexedCollectionState::from_tree(&prolly, &tree).unwrap(),
            state
        );
    }

    #[test]
    fn root_name_is_canonical_and_rejects_empty_ids() {
        assert_eq!(
            indexed_collection_root_name(b"\x00a").unwrap(),
            b"\0prolly/indexed-collection/0061/state"
        );
        assert!(indexed_collection_root_name(b"").is_err());
    }

    #[test]
    fn state_rejects_missing_head_descriptor() {
        let mut state = empty_state();
        let snapshot = state.snapshots.get_mut(&state.head).unwrap();
        snapshot.indexes.push(IndexSnapshotRef {
            name: b"team".to_vec(),
            descriptor_fingerprint: Cid::from_bytes(b"missing"),
            tree: Tree::default(),
            entry_count: 0,
        });
        assert!(state.validate_closure().is_err());
    }
}
