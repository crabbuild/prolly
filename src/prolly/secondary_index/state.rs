use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

use super::super::cid::Cid;
use super::super::error::{Error, Mutation};
use super::super::key::{decode_segments, KeyBuilder};
use super::super::store::{AsyncStore, Store};
use super::super::tree::Tree;
use super::super::{AsyncProlly, Prolly};
use super::definition::{IndexProjection, SecondaryIndex};

/// The only secondary-index collection format accepted by this release.
///
/// Incompatible changes replace this value through a hard cutover. The engine
/// never keeps multiple format readers alive.
pub const INDEXED_COLLECTION_FORMAT: u32 = 1;

/// Canonical immutable record format used to resolve one indexed snapshot
/// without loading the mutable collection coordinator's retained history.
pub const INDEXED_SNAPSHOT_MANIFEST_FORMAT: u32 = 1;

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

/// Decode a canonical indexed-collection state-root name. Ordinary named roots
/// return `None`; malformed names inside the reserved namespace fail closed.
pub fn indexed_collection_source_map_id(name: &[u8]) -> Result<Option<Vec<u8>>, Error> {
    if !name.starts_with(ROOT_PREFIX) {
        return Ok(None);
    }
    let encoded = name
        .strip_prefix(ROOT_PREFIX)
        .and_then(|rest| rest.strip_suffix(ROOT_SUFFIX));
    let Some(encoded) = encoded else {
        return Err(Error::InvalidVersionedMap(
            "malformed indexed collection root name".to_string(),
        ));
    };
    if encoded.is_empty() || encoded.len() % 2 != 0 {
        return Err(Error::InvalidVersionedMap(
            "indexed collection root contains malformed source-map hex".to_string(),
        ));
    }
    let mut source_map_id = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_chunks::<2>().0 {
        let high = decode_hex_nibble(pair[0]).ok_or_else(|| {
            Error::InvalidVersionedMap(
                "indexed collection root contains malformed source-map hex".to_string(),
            )
        })?;
        let low = decode_hex_nibble(pair[1]).ok_or_else(|| {
            Error::InvalidVersionedMap(
                "indexed collection root contains malformed source-map hex".to_string(),
            )
        })?;
        source_map_id.push((high << 4) | low);
    }
    Ok(Some(source_map_id))
}

fn decode_hex_nibble(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        _ => None,
    }
}

/// Semantic collection limits that participate in persisted state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CollectionIndexPolicy {
    pub max_active_indexes: usize,
    pub max_retained_snapshots: usize,
    pub max_descriptors: usize,
    pub max_durable_pins: usize,
}

impl Default for CollectionIndexPolicy {
    fn default() -> Self {
        Self {
            max_active_indexes: 32,
            max_retained_snapshots: 1_024,
            max_descriptors: 4_096,
            max_durable_pins: 1_024,
        }
    }
}

impl CollectionIndexPolicy {
    pub fn validate(&self) -> Result<(), Error> {
        if self.max_active_indexes == 0
            || self.max_retained_snapshots == 0
            || self.max_descriptors == 0
            || self.max_durable_pins == 0
        {
            return Err(Error::InvalidIndexDefinition {
                reason: "all collection index policy limits must be greater than zero".to_string(),
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

/// Self-contained immutable locator for one source/index snapshot.
///
/// The manifest contains exactly the descriptors referenced by `record` and
/// can therefore be stored under an application-owned version key. It is
/// deliberately independent of the mutable indexed-collection state so an
/// application can keep that coordinator bounded while retaining deep,
/// content-addressed history elsewhere.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct IndexedSnapshotManifest {
    pub format_version: u32,
    pub snapshot_id: IndexedSnapshotId,
    pub record: IndexedSnapshotRecord,
    pub descriptors: Vec<IndexDescriptor>,
}

impl IndexedSnapshotManifest {
    /// Construct the exact manifest for the current state head.
    pub fn from_state_head(state: &IndexedCollectionState) -> Result<Self, Error> {
        state.validate_closure()?;
        let record = state.head_snapshot()?.clone();
        let mut descriptors = record
            .indexes
            .iter()
            .map(|index| {
                state
                    .descriptors
                    .get(&(index.name.clone(), index.descriptor_fingerprint.clone()))
                    .cloned()
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(
                            "indexed snapshot manifest descriptor is absent".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        descriptors.sort_by(|left, right| {
            (&left.name, left.fingerprint.as_bytes())
                .cmp(&(&right.name, right.fingerprint.as_bytes()))
        });
        let manifest = Self {
            format_version: INDEXED_SNAPSHOT_MANIFEST_FORMAT,
            snapshot_id: state.head.clone(),
            record,
            descriptors,
        };
        manifest.validate()?;
        Ok(manifest)
    }

    /// Validate identity, ownership, ordering, and exact descriptor closure.
    pub fn validate(&self) -> Result<(), Error> {
        if self.format_version != INDEXED_SNAPSHOT_MANIFEST_FORMAT {
            return Err(Error::IndexFormatUnsupported);
        }
        self.record.validate()?;
        if self.record.id()? != self.snapshot_id {
            return Err(Error::InvalidVersionedMap(
                "indexed snapshot manifest identity mismatch".to_string(),
            ));
        }
        let expected = self
            .record
            .indexes
            .iter()
            .map(|index| (index.name.as_slice(), &index.descriptor_fingerprint))
            .collect::<Vec<_>>();
        let mut actual = Vec::with_capacity(self.descriptors.len());
        let mut previous: Option<(&[u8], &[u8])> = None;
        for descriptor in &self.descriptors {
            descriptor.validate()?;
            if descriptor.source_map_id != self.record.source_map_id {
                return Err(Error::InvalidVersionedMap(
                    "indexed snapshot manifest descriptor ownership mismatch".to_string(),
                ));
            }
            let identity = (
                descriptor.name.as_slice(),
                descriptor.fingerprint.as_bytes(),
            );
            if previous.is_some_and(|previous| previous >= identity) {
                return Err(Error::InvalidVersionedMap(
                    "indexed snapshot manifest descriptors are not strictly ordered".to_string(),
                ));
            }
            previous = Some(identity);
            actual.push((descriptor.name.as_slice(), &descriptor.fingerprint));
        }
        if actual != expected {
            return Err(Error::InvalidVersionedMap(
                "indexed snapshot manifest descriptor closure mismatch".to_string(),
            ));
        }
        Ok(())
    }

    /// Encode a canonical durable manifest.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        encode(self)
    }

    /// Decode and require the exact canonical byte representation.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        let manifest: Self = decode(bytes)?;
        manifest.validate()?;
        if manifest.to_bytes()? != bytes {
            return Err(Error::Deserialize(
                "indexed snapshot manifest encoding is not canonical".to_string(),
            ));
        }
        Ok(manifest)
    }
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
        if self.snapshots.len() > self.policy.max_retained_snapshots {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "retained_snapshots",
                limit: self.policy.max_retained_snapshots,
                actual: self.snapshots.len(),
            });
        }
        if self.descriptors.len() > self.policy.max_descriptors {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "index_descriptors",
                limit: self.policy.max_descriptors,
                actual: self.descriptors.len(),
            });
        }
        if self.pins.len() > self.policy.max_durable_pins {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "durable_snapshot_pins",
                limit: self.policy.max_durable_pins,
                actual: self.pins.len(),
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

    /// Bound canonical state before publication while preserving the head and
    /// every explicitly pinned snapshot.
    pub(crate) fn enforce_policy_limits(&mut self) -> Result<(), Error> {
        self.policy.validate()?;
        // Do not discard non-head branches merely because a caller restored
        // an older snapshot. They remain addressable history until the
        // configured bound is actually exceeded or explicit retention runs.
        if self.snapshots.len() <= self.policy.max_retained_snapshots {
            return self.validate_closure();
        }
        let mut keep = self
            .pins
            .values()
            .map(|pin| pin.snapshot.clone())
            .collect::<BTreeSet<_>>();
        keep.insert(self.head.clone());
        if keep.len() > self.policy.max_retained_snapshots {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "pinned_retained_snapshots",
                limit: self.policy.max_retained_snapshots,
                actual: keep.len(),
            });
        }
        let mut cursor = Some(self.head.clone());
        while keep.len() < self.policy.max_retained_snapshots {
            let Some(id) = cursor.take() else {
                break;
            };
            let Some(snapshot) = self.snapshots.get(&id) else {
                break;
            };
            cursor = snapshot.parent.clone();
            if let Some(parent) = &cursor {
                keep.insert(parent.clone());
            }
        }
        self.snapshots.retain(|id, _| keep.contains(id));

        let referenced = self
            .snapshots
            .values()
            .flat_map(|snapshot| {
                snapshot
                    .indexes
                    .iter()
                    .map(|index| (index.name.clone(), index.descriptor_fingerprint.clone()))
            })
            .chain(
                self.active
                    .iter()
                    .map(|(name, fingerprint)| (name.clone(), fingerprint.clone())),
            )
            .collect::<BTreeSet<_>>();
        self.descriptors
            .retain(|identity, _| referenced.contains(identity));
        self.retired
            .retain(|identity| referenced.contains(identity));
        self.validate_closure()
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

    pub(crate) fn to_tree_from<S: Store>(
        &self,
        prolly: &Prolly<S>,
        previous: &Self,
        previous_tree: &Tree,
    ) -> Result<Tree, Error> {
        self.validate_closure()?;
        previous.validate_closure()?;
        let mut mutations = Vec::new();
        if self.source_map_id != previous.source_map_id {
            mutations.push(Mutation::Upsert {
                key: key(&[b"meta", META_SOURCE]),
                val: self.source_map_id.clone(),
            });
        }
        if self.policy != previous.policy {
            mutations.push(Mutation::Upsert {
                key: key(&[b"meta", META_POLICY]),
                val: encode(&self.policy)?,
            });
        }
        if self.head != previous.head {
            mutations.push(Mutation::Upsert {
                key: key(&[HEAD]),
                val: self.head.0.as_bytes().to_vec(),
            });
        }
        for (id, snapshot) in &self.snapshots {
            if previous.snapshots.get(id) != Some(snapshot) {
                mutations.push(Mutation::Upsert {
                    key: key(&[SNAPSHOTS, id.0.as_bytes()]),
                    val: encode(snapshot)?,
                });
            }
        }
        for id in previous.snapshots.keys() {
            if !self.snapshots.contains_key(id) {
                mutations.push(Mutation::Delete {
                    key: key(&[SNAPSHOTS, id.0.as_bytes()]),
                });
            }
        }
        for (identity @ (name, fingerprint), descriptor) in &self.descriptors {
            if previous.descriptors.get(identity) != Some(descriptor) {
                mutations.push(Mutation::Upsert {
                    key: key(&[DESCRIPTORS, name, fingerprint.as_bytes()]),
                    val: encode(descriptor)?,
                });
            }
        }
        for (name, fingerprint) in previous.descriptors.keys() {
            if !self
                .descriptors
                .contains_key(&(name.clone(), fingerprint.clone()))
            {
                mutations.push(Mutation::Delete {
                    key: key(&[DESCRIPTORS, name, fingerprint.as_bytes()]),
                });
            }
        }
        for (name, fingerprint) in &self.active {
            if previous.active.get(name) != Some(fingerprint) {
                mutations.push(Mutation::Upsert {
                    key: key(&[ACTIVE, name]),
                    val: fingerprint.as_bytes().to_vec(),
                });
            }
        }
        for name in previous.active.keys() {
            if !self.active.contains_key(name) {
                mutations.push(Mutation::Delete {
                    key: key(&[ACTIVE, name]),
                });
            }
        }
        for (name, fingerprint) in &self.retired {
            if !previous
                .retired
                .contains(&(name.clone(), fingerprint.clone()))
            {
                mutations.push(Mutation::Upsert {
                    key: key(&[RETIRED, name, fingerprint.as_bytes()]),
                    val: Vec::new(),
                });
            }
        }
        for (name, fingerprint) in &previous.retired {
            if !self.retired.contains(&(name.clone(), fingerprint.clone())) {
                mutations.push(Mutation::Delete {
                    key: key(&[RETIRED, name, fingerprint.as_bytes()]),
                });
            }
        }
        for (pin_id, pin) in &self.pins {
            if previous.pins.get(pin_id) != Some(pin) {
                mutations.push(Mutation::Upsert {
                    key: key(&[PINS, pin_id]),
                    val: encode(pin)?,
                });
            }
        }
        for pin_id in previous.pins.keys() {
            if !self.pins.contains_key(pin_id) {
                mutations.push(Mutation::Delete {
                    key: key(&[PINS, pin_id]),
                });
            }
        }
        prolly.batch(previous_tree, mutations)
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

impl IndexedCollectionState {
    /// Encode canonical collection state through the native async engine.
    pub async fn to_async_tree<S>(&self, prolly: &AsyncProlly<S>) -> Result<Tree, Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
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
        prolly.batch(&prolly.create(), mutations).await
    }

    /// Apply a canonical state delta through the native async engine.
    pub(crate) async fn to_async_tree_from<S>(
        &self,
        prolly: &AsyncProlly<S>,
        previous: &Self,
        previous_tree: &Tree,
    ) -> Result<Tree, Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        self.validate_closure()?;
        previous.validate_closure()?;
        let mut mutations = Vec::new();
        if self.source_map_id != previous.source_map_id {
            mutations.push(Mutation::Upsert {
                key: key(&[b"meta", META_SOURCE]),
                val: self.source_map_id.clone(),
            });
        }
        if self.policy != previous.policy {
            mutations.push(Mutation::Upsert {
                key: key(&[b"meta", META_POLICY]),
                val: encode(&self.policy)?,
            });
        }
        if self.head != previous.head {
            mutations.push(Mutation::Upsert {
                key: key(&[HEAD]),
                val: self.head.0.as_bytes().to_vec(),
            });
        }
        for (id, snapshot) in &self.snapshots {
            if previous.snapshots.get(id) != Some(snapshot) {
                mutations.push(Mutation::Upsert {
                    key: key(&[SNAPSHOTS, id.0.as_bytes()]),
                    val: encode(snapshot)?,
                });
            }
        }
        for id in previous.snapshots.keys() {
            if !self.snapshots.contains_key(id) {
                mutations.push(Mutation::Delete {
                    key: key(&[SNAPSHOTS, id.0.as_bytes()]),
                });
            }
        }
        for (identity @ (name, fingerprint), descriptor) in &self.descriptors {
            if previous.descriptors.get(identity) != Some(descriptor) {
                mutations.push(Mutation::Upsert {
                    key: key(&[DESCRIPTORS, name, fingerprint.as_bytes()]),
                    val: encode(descriptor)?,
                });
            }
        }
        for (name, fingerprint) in previous.descriptors.keys() {
            if !self
                .descriptors
                .contains_key(&(name.clone(), fingerprint.clone()))
            {
                mutations.push(Mutation::Delete {
                    key: key(&[DESCRIPTORS, name, fingerprint.as_bytes()]),
                });
            }
        }
        for (name, fingerprint) in &self.active {
            if previous.active.get(name) != Some(fingerprint) {
                mutations.push(Mutation::Upsert {
                    key: key(&[ACTIVE, name]),
                    val: fingerprint.as_bytes().to_vec(),
                });
            }
        }
        for name in previous.active.keys() {
            if !self.active.contains_key(name) {
                mutations.push(Mutation::Delete {
                    key: key(&[ACTIVE, name]),
                });
            }
        }
        for (name, fingerprint) in &self.retired {
            if !previous
                .retired
                .contains(&(name.clone(), fingerprint.clone()))
            {
                mutations.push(Mutation::Upsert {
                    key: key(&[RETIRED, name, fingerprint.as_bytes()]),
                    val: Vec::new(),
                });
            }
        }
        for (name, fingerprint) in &previous.retired {
            if !self.retired.contains(&(name.clone(), fingerprint.clone())) {
                mutations.push(Mutation::Delete {
                    key: key(&[RETIRED, name, fingerprint.as_bytes()]),
                });
            }
        }
        for (pin_id, pin) in &self.pins {
            if previous.pins.get(pin_id) != Some(pin) {
                mutations.push(Mutation::Upsert {
                    key: key(&[PINS, pin_id]),
                    val: encode(pin)?,
                });
            }
        }
        for pin_id in previous.pins.keys() {
            if !self.pins.contains_key(pin_id) {
                mutations.push(Mutation::Delete {
                    key: key(&[PINS, pin_id]),
                });
            }
        }
        prolly.batch(previous_tree, mutations).await
    }

    /// Decode canonical collection state through the native async engine.
    pub async fn from_async_tree<S>(prolly: &AsyncProlly<S>, tree: &Tree) -> Result<Self, Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        let mut source_map_id = None;
        let mut policy = None;
        let mut head = None;
        let mut snapshots = BTreeMap::new();
        let mut descriptors = BTreeMap::new();
        let mut active = BTreeMap::new();
        let mut retired = BTreeSet::new();
        let mut pins = BTreeMap::new();
        let mut saw_format = false;
        let mut entries = prolly.range(tree, b"", None).await?;

        while let Some(entry) = entries.next().await {
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
    fn state_delta_publication_matches_full_canonical_rebuild() {
        let prolly = Prolly::new(MemStore::new(), Config::default());
        let previous = empty_state();
        let previous_tree = previous.to_tree(&prolly).unwrap();
        let mut candidate = previous.clone();
        let source = prolly
            .put(
                &candidate.head_snapshot().unwrap().source.tree,
                b"u1".to_vec(),
                b"active".to_vec(),
            )
            .unwrap();
        let snapshot = IndexedSnapshotRecord {
            source_map_id: b"users".to_vec(),
            parent: Some(candidate.head.clone()),
            source: SourceSnapshotRef {
                tree: source,
                entry_count: 1,
            },
            indexes: Vec::new(),
        };
        let id = snapshot.id().unwrap();
        candidate.snapshots.insert(id.clone(), snapshot);
        candidate.head = id;
        let delta = candidate
            .to_tree_from(&prolly, &previous, &previous_tree)
            .unwrap();
        assert_eq!(delta, candidate.to_tree(&prolly).unwrap());
    }

    #[test]
    fn root_name_is_canonical_and_rejects_empty_ids() {
        assert_eq!(
            indexed_collection_root_name(b"\x00a").unwrap(),
            b"\0prolly/indexed-collection/0061/state"
        );
        assert!(indexed_collection_root_name(b"").is_err());
        assert_eq!(
            indexed_collection_source_map_id(b"\0prolly/indexed-collection/0061/state").unwrap(),
            Some(b"\0a".to_vec())
        );
        assert_eq!(
            indexed_collection_source_map_id(b"maps/versioned/example").unwrap(),
            None
        );
        assert!(indexed_collection_source_map_id(b"\0prolly/indexed-collection/0A/state").is_err());
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

    #[test]
    fn snapshot_manifest_is_canonical_and_self_contained() {
        let mut state = empty_state();
        let definition =
            SecondaryIndex::non_unique("team", 1, "team-v1", |_, _| Ok(vec![b"blue".to_vec()]))
                .unwrap();
        let descriptor = IndexDescriptor::from_runtime(b"users", &definition).unwrap();
        let mut record = state.head_snapshot().unwrap().clone();
        record.indexes.push(IndexSnapshotRef {
            name: descriptor.name.clone(),
            descriptor_fingerprint: descriptor.fingerprint.clone(),
            tree: Tree::default(),
            entry_count: 0,
        });
        let id = record.id().unwrap();
        state.snapshots.clear();
        state.snapshots.insert(id.clone(), record);
        state.head = id;
        state
            .active
            .insert(descriptor.name.clone(), descriptor.fingerprint.clone());
        state.descriptors.insert(
            (descriptor.name.clone(), descriptor.fingerprint.clone()),
            descriptor,
        );

        let manifest = IndexedSnapshotManifest::from_state_head(&state).unwrap();
        let bytes = manifest.to_bytes().unwrap();
        assert_eq!(
            IndexedSnapshotManifest::from_bytes(&bytes).unwrap(),
            manifest
        );
        assert!(IndexedSnapshotManifest::from_bytes(&[bytes, vec![0]].concat()).is_err());

        let mut tampered = manifest.clone();
        tampered.snapshot_id = IndexedSnapshotId(Cid::from_bytes(b"tampered"));
        assert!(tampered.validate().is_err());

        let mut tampered = manifest;
        tampered.descriptors.clear();
        assert!(tampered.validate().is_err());
    }
}
