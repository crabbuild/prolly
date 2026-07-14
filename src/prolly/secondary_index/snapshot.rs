use super::super::cid::Cid;
use super::super::error::Error;
use super::super::manifest::ManifestStore;
use super::super::range::ReverseCursor;
use super::super::store::Store;
use super::super::transaction::TransactionalStore;
use super::super::tree::Tree;
use super::super::versioned_map::{MapSnapshot, MapVersionId, VersionedMap};
use super::super::Prolly;
use super::coordinator::{validate_catalog_format, IndexedMap};
use super::definition::IndexProjection;
use super::storage::{
    catalog_checkpoint_key, catalog_current_key, catalog_descriptor_key, catalog_map_id,
    decode_physical_index_key, term_bounds_exact, term_bounds_prefix, term_bounds_range,
    IndexCheckpoint, IndexValue, IndexedHeadRecord, SecondaryIndexDescriptor, TermBounds,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const CURSOR_MAGIC: &[u8; 8] = b"PSICUR01";
const CURSOR_VERSION: u32 = 1;

/// Reproducible identity of one catalog-selected indexed snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct IndexedSnapshotId {
    pub source_version: MapVersionId,
    pub catalog_version: MapVersionId,
}

/// One decoded physical secondary-index match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexMatch {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

/// Direction captured by a resumable secondary-index cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LogicalBounds {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Range(Vec<u8>, Option<Vec<u8>>),
}

/// Snapshot- and query-bound cursor for resumable secondary-index scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCursor {
    source_version: MapVersionId,
    catalog_version: MapVersionId,
    index_name: Vec<u8>,
    index_version: MapVersionId,
    definition_fingerprint: Cid,
    direction: SecondaryIndexDirection,
    bounds: LogicalBounds,
    raw_key: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct CursorWire(
    MapVersionId,
    MapVersionId,
    Vec<u8>,
    MapVersionId,
    Cid,
    SecondaryIndexDirection,
    LogicalBounds,
    Option<Vec<u8>>,
);

impl SecondaryIndexCursor {
    /// Encode this cursor into a stable, versioned binary envelope.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let payload = serde_cbor::to_vec(&CursorWire(
            self.source_version.clone(),
            self.catalog_version.clone(),
            self.index_name.clone(),
            self.index_version.clone(),
            self.definition_fingerprint.clone(),
            self.direction,
            self.bounds.clone(),
            self.raw_key.clone(),
        ))
        .map_err(|error| Error::Serialize(error.to_string()))?;
        let mut bytes = Vec::with_capacity(12 + payload.len());
        bytes.extend_from_slice(CURSOR_MAGIC);
        bytes.extend_from_slice(&CURSOR_VERSION.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// Decode a canonical secondary-index cursor envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 12 || &bytes[..8] != CURSOR_MAGIC {
            return Err(Error::Deserialize(
                "invalid secondary-index cursor envelope".to_string(),
            ));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed cursor header"));
        if version != CURSOR_VERSION {
            return Err(Error::Deserialize(format!(
                "unsupported secondary-index cursor version {version}"
            )));
        }
        let CursorWire(
            source_version,
            catalog_version,
            index_name,
            index_version,
            definition_fingerprint,
            direction,
            bounds,
            raw_key,
        ) = serde_cbor::from_slice(&bytes[12..])
            .map_err(|error| Error::Deserialize(error.to_string()))?;
        if index_name.is_empty() {
            return Err(Error::Deserialize(
                "secondary-index cursor has an empty index name".to_string(),
            ));
        }
        Ok(Self {
            source_version,
            catalog_version,
            index_name,
            index_version,
            definition_fingerprint,
            direction,
            bounds,
            raw_key,
        })
    }

    pub fn direction(&self) -> SecondaryIndexDirection {
        self.direction
    }

    pub fn snapshot_id(&self) -> IndexedSnapshotId {
        IndexedSnapshotId {
            source_version: self.source_version.clone(),
            catalog_version: self.catalog_version.clone(),
        }
    }

    pub fn index_version(&self) -> &MapVersionId {
        &self.index_version
    }

    pub fn definition_fingerprint(&self) -> &Cid {
        &self.definition_fingerprint
    }
}

/// One bounded secondary-index query page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecondaryIndexPage {
    pub matches: Vec<SecondaryIndexMatch>,
    pub next_cursor: Option<SecondaryIndexCursor>,
}

/// Immutable catalog-selected source and index view.
pub struct IndexedSnapshot<'a, S: Store> {
    id: IndexedSnapshotId,
    catalog: MapSnapshot<'a, S>,
    source: MapSnapshot<'a, S>,
    indexes: BTreeMap<Vec<u8>, SecondaryIndexSnapshot<'a, S>>,
}

impl<'a, S: Store> IndexedSnapshot<'a, S> {
    pub fn id(&self) -> &IndexedSnapshotId {
        &self.id
    }

    pub fn source(&self) -> &MapSnapshot<'a, S> {
        &self.source
    }

    pub fn catalog(&self) -> &MapSnapshot<'a, S> {
        &self.catalog
    }

    pub fn index(&self, name: impl AsRef<[u8]>) -> Result<&SecondaryIndexSnapshot<'a, S>, Error> {
        self.indexes
            .get(name.as_ref())
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.as_ref().to_vec(),
                source_version: self.id.source_version.clone(),
            })
    }

    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &SecondaryIndexSnapshot<'a, S>> {
        self.indexes.values()
    }
}

/// Query handle for one exact hidden-index checkpoint.
pub struct SecondaryIndexSnapshot<'a, S: Store> {
    prolly: &'a Prolly<S>,
    snapshot_id: IndexedSnapshotId,
    descriptor: SecondaryIndexDescriptor,
    checkpoint: IndexCheckpoint,
    source_tree: Tree,
    index: MapSnapshot<'a, S>,
    max_projection_bytes: usize,
}

impl<'a, S: Store> SecondaryIndexSnapshot<'a, S> {
    pub fn name(&self) -> &[u8] {
        &self.descriptor.name
    }

    pub fn descriptor(&self) -> &SecondaryIndexDescriptor {
        &self.descriptor
    }

    pub fn checkpoint(&self) -> &IndexCheckpoint {
        &self.checkpoint
    }

    pub fn exact(&self, term: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect(LogicalBounds::Exact(term.to_vec()))
    }

    pub fn prefix(&self, prefix: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect(LogicalBounds::Prefix(prefix.to_vec()))
    }

    pub fn range(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
    ) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect(LogicalBounds::Range(
            start_term.to_vec(),
            end_term.map(ToOwned::to_owned),
        ))
    }

    pub fn primary_keys(&self, term: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .exact(term)?
            .into_iter()
            .map(|matched| matched.primary_key)
            .collect())
    }

    pub fn projected(&self, term: &[u8]) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>, Error> {
        Ok(self
            .exact(term)?
            .into_iter()
            .map(|matched| (matched.primary_key, matched.projection))
            .collect())
    }

    /// Resolve matching primary keys with one ordered batched source read.
    pub fn records(&self, term: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>, Error> {
        let matches = self.exact(term)?;
        let keys: Vec<&[u8]> = matches
            .iter()
            .map(|matched| matched.primary_key.as_slice())
            .collect();
        let values = self.prolly.get_many(&self.source_tree, &keys)?;
        matches
            .into_iter()
            .zip(values)
            .map(|(matched, value)| match value {
                Some(value) => Ok((matched.primary_key, value)),
                None => Err(Error::IndexCheckpointMismatch {
                    name: self.descriptor.name.clone(),
                    source_version: self.snapshot_id.source_version.clone(),
                    reason: format!(
                        "index references missing source primary key {:?}",
                        matched.primary_key
                    ),
                }),
            })
            .collect()
    }

    pub fn exact_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
        )
    }

    pub fn exact_reverse_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
        )
    }

    pub fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
        )
    }

    pub fn prefix_reverse_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
        )
    }

    pub fn range_page(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
        )
    }

    pub fn range_reverse_page(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
        )
    }

    fn collect(&self, logical: LogicalBounds) -> Result<Vec<SecondaryIndexMatch>, Error> {
        let bounds = physical_bounds(&logical)?;
        self.prolly
            .range(self.index.tree(), &bounds.start, bounds.end.as_deref())?
            .map(|entry| {
                let (key, value) = entry?;
                self.decode_match(&key, &value)
            })
            .collect()
    }

    fn page(
        &self,
        logical: LogicalBounds,
        direction: SecondaryIndexDirection,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        if let Some(cursor) = cursor {
            self.validate_cursor(cursor, &logical, direction)?;
        }
        if limit == 0 {
            let next_cursor = cursor.cloned().or_else(|| {
                Some(SecondaryIndexCursor {
                    source_version: self.snapshot_id.source_version.clone(),
                    catalog_version: self.snapshot_id.catalog_version.clone(),
                    index_name: self.descriptor.name.clone(),
                    index_version: self.checkpoint.index_version.clone(),
                    definition_fingerprint: self.descriptor.fingerprint.clone(),
                    direction,
                    bounds: logical,
                    raw_key: None,
                })
            });
            return Ok(SecondaryIndexPage {
                matches: Vec::new(),
                next_cursor,
            });
        }
        let bounds = physical_bounds(&logical)?;
        let page_limit = limit.saturating_add(1);
        let mut raw_entries = match direction {
            SecondaryIndexDirection::Forward => {
                let mut iter = match cursor.and_then(|cursor| cursor.raw_key.as_deref()) {
                    Some(after) => {
                        self.prolly
                            .range_after(self.index.tree(), after, bounds.end.as_deref())?
                    }
                    None => self.prolly.range(
                        self.index.tree(),
                        &bounds.start,
                        bounds.end.as_deref(),
                    )?,
                };
                let mut entries = Vec::with_capacity(page_limit);
                for _ in 0..page_limit {
                    let Some(entry) = iter.next() else { break };
                    entries.push(entry?);
                }
                entries
            }
            SecondaryIndexDirection::Reverse => {
                let raw = cursor
                    .and_then(|cursor| cursor.raw_key.clone())
                    .map(ReverseCursor::before_key)
                    .unwrap_or_else(ReverseCursor::end);
                self.prolly
                    .reverse_range_page(
                        self.index.tree(),
                        &raw,
                        &bounds.start,
                        bounds.end.as_deref(),
                        page_limit,
                    )?
                    .entries
            }
        };
        let has_more = raw_entries.len() > limit;
        raw_entries.truncate(limit);
        let raw_key = has_more
            .then(|| raw_entries.last().map(|(key, _)| key.clone()))
            .flatten();
        let matches = raw_entries
            .into_iter()
            .map(|(key, value)| self.decode_match(&key, &value))
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = raw_key.map(|raw_key| SecondaryIndexCursor {
            source_version: self.snapshot_id.source_version.clone(),
            catalog_version: self.snapshot_id.catalog_version.clone(),
            index_name: self.descriptor.name.clone(),
            index_version: self.checkpoint.index_version.clone(),
            definition_fingerprint: self.descriptor.fingerprint.clone(),
            direction,
            bounds: logical,
            raw_key: Some(raw_key),
        });
        Ok(SecondaryIndexPage {
            matches,
            next_cursor,
        })
    }

    fn validate_cursor(
        &self,
        cursor: &SecondaryIndexCursor,
        bounds: &LogicalBounds,
        direction: SecondaryIndexDirection,
    ) -> Result<(), Error> {
        let valid = cursor.source_version == self.snapshot_id.source_version
            && cursor.catalog_version == self.snapshot_id.catalog_version
            && cursor.index_name == self.descriptor.name
            && cursor.index_version == self.checkpoint.index_version
            && cursor.definition_fingerprint == self.descriptor.fingerprint
            && cursor.direction == direction
            && &cursor.bounds == bounds;
        if valid {
            Ok(())
        } else {
            Err(Error::IndexCursorVersionMismatch {
                expected: format!(
                    "source={}, catalog={}, index={}, direction={direction:?}, bounds={bounds:?}",
                    self.snapshot_id.source_version,
                    self.snapshot_id.catalog_version,
                    self.checkpoint.index_version
                ),
                actual: format!(
                    "source={}, catalog={}, index={}, direction={:?}, bounds={:?}",
                    cursor.source_version,
                    cursor.catalog_version,
                    cursor.index_version,
                    cursor.direction,
                    cursor.bounds
                ),
            })
        }
    }

    fn decode_match(&self, key: &[u8], value: &[u8]) -> Result<SecondaryIndexMatch, Error> {
        let decoded = decode_physical_index_key(key)?;
        let stored = IndexValue::from_bytes(value, self.max_projection_bytes)?;
        let projection = match (self.descriptor.projection, stored) {
            (IndexProjection::KeysOnly, IndexValue::KeysOnly) => None,
            (IndexProjection::Include, IndexValue::Included(bytes))
            | (IndexProjection::All, IndexValue::FullSource(bytes)) => Some(bytes),
            _ => {
                return Err(Error::IndexCheckpointMismatch {
                    name: self.descriptor.name.clone(),
                    source_version: self.snapshot_id.source_version.clone(),
                    reason: "stored projection value does not match its descriptor".to_string(),
                })
            }
        };
        Ok(SecondaryIndexMatch {
            term: decoded.term,
            primary_key: decoded.primary_key,
            projection,
        })
    }
}

fn physical_bounds(bounds: &LogicalBounds) -> Result<TermBounds, Error> {
    match bounds {
        LogicalBounds::Exact(term) => Ok(term_bounds_exact(term)),
        LogicalBounds::Prefix(prefix) => Ok(term_bounds_prefix(prefix)),
        LogicalBounds::Range(start, end) => term_bounds_range(start, end.as_deref()),
    }
}

impl<'a, S> IndexedMap<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Pin the current catalog first, then reopen every selected immutable root.
    pub fn snapshot(&self) -> Result<IndexedSnapshot<'a, S>, Error> {
        let catalog_map = VersionedMap::new(self.prolly, catalog_map_id(&self.source_map_id));
        let catalog = catalog_map.snapshot()?.ok_or_else(|| {
            Error::InvalidVersionedMap("secondary-index catalog is absent".to_string())
        })?;
        let current = load_current(&catalog)?;
        self.resolve_snapshot(catalog, &current.source_version)
    }

    /// Reopen exact checkpoints for a retained source using current generations.
    pub fn snapshot_at(
        &self,
        source_version: &MapVersionId,
    ) -> Result<IndexedSnapshot<'a, S>, Error> {
        let catalog_map = VersionedMap::new(self.prolly, catalog_map_id(&self.source_map_id));
        let catalog = catalog_map.snapshot()?.ok_or_else(|| {
            Error::InvalidVersionedMap("secondary-index catalog is absent".to_string())
        })?;
        self.resolve_snapshot(catalog, source_version)
    }

    /// Reopen the exact retained catalog/source pair represented by `id`.
    pub fn snapshot_by_id(&self, id: &IndexedSnapshotId) -> Result<IndexedSnapshot<'a, S>, Error> {
        let catalog = VersionedMap::new(self.prolly, catalog_map_id(&self.source_map_id))
            .snapshot_at(&id.catalog_version)?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(format!(
                    "indexed catalog version {} is unavailable",
                    id.catalog_version
                ))
            })?;
        self.resolve_snapshot(catalog, &id.source_version)
    }

    fn resolve_snapshot(
        &self,
        catalog: MapSnapshot<'a, S>,
        source_version: &MapVersionId,
    ) -> Result<IndexedSnapshot<'a, S>, Error> {
        validate_catalog_format(&catalog)?;
        let current = load_current(&catalog)?;
        let source = VersionedMap::new(self.prolly, &self.source_map_id)
            .snapshot_at(source_version)?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(format!(
                    "indexed source version {source_version} is unavailable"
                ))
            })?;
        let snapshot_id = IndexedSnapshotId {
            source_version: source_version.clone(),
            catalog_version: catalog.id().clone(),
        };
        let mut indexes = BTreeMap::new();
        for active in current.indexes {
            let checkpoint = if active.source_version == *source_version {
                active
            } else {
                let key =
                    catalog_checkpoint_key(source_version, &active.index_name, active.generation);
                let bytes = catalog
                    .get(&key)?
                    .ok_or_else(|| Error::IndexUnavailableAtVersion {
                        name: active.index_name.clone(),
                        source_version: source_version.clone(),
                    })?;
                IndexCheckpoint::from_bytes(&bytes)?
            };
            if checkpoint.source_map_id != self.source_map_id
                || checkpoint.source_version != *source_version
            {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: source_version.clone(),
                    reason: "checkpoint source ownership does not match snapshot".to_string(),
                });
            }
            let descriptor_bytes = catalog
                .get(&catalog_descriptor_key(
                    &checkpoint.index_name,
                    checkpoint.generation,
                ))?
                .ok_or_else(|| Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: source_version.clone(),
                    reason: "checkpoint descriptor is missing".to_string(),
                })?;
            let descriptor = SecondaryIndexDescriptor::from_bytes(&descriptor_bytes)?;
            if descriptor.fingerprint != checkpoint.definition_fingerprint {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: source_version.clone(),
                    reason: "checkpoint fingerprint does not match descriptor".to_string(),
                });
            }
            let runtime = self.registry.get(&checkpoint.index_name).ok_or_else(|| {
                Error::IndexRuntimeDefinitionMissing {
                    name: checkpoint.index_name.clone(),
                    generation: checkpoint.generation,
                }
            })?;
            let runtime_descriptor =
                SecondaryIndexDescriptor::from_runtime(&self.source_map_id, runtime)?;
            if runtime_descriptor.fingerprint != descriptor.fingerprint {
                return Err(Error::IndexDefinitionMismatch {
                    name: checkpoint.index_name.clone(),
                    persisted: descriptor.fingerprint,
                    runtime: runtime_descriptor.fingerprint,
                });
            }
            let index = VersionedMap::new(self.prolly, &checkpoint.index_map_id)
                .snapshot_at(&checkpoint.index_version)?
                .ok_or_else(|| Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: source_version.clone(),
                    reason: "hidden index version is unavailable".to_string(),
                })?;
            indexes.insert(
                checkpoint.index_name.clone(),
                SecondaryIndexSnapshot {
                    prolly: self.prolly,
                    snapshot_id: snapshot_id.clone(),
                    descriptor,
                    checkpoint,
                    source_tree: source.tree().clone(),
                    index,
                    max_projection_bytes: match runtime.projection() {
                        IndexProjection::KeysOnly => 0,
                        IndexProjection::Include => runtime.limits().max_projection_bytes,
                        IndexProjection::All => runtime.limits().max_all_value_bytes,
                    },
                },
            );
        }
        Ok(IndexedSnapshot {
            id: snapshot_id,
            catalog,
            source,
            indexes,
        })
    }
}

fn load_current<S: Store>(catalog: &MapSnapshot<'_, S>) -> Result<IndexedHeadRecord, Error> {
    validate_catalog_format(catalog)?;
    catalog
        .get(&catalog_current_key())?
        .ok_or_else(|| {
            Error::InvalidVersionedMap(
                "secondary-index catalog is missing current selection".to_string(),
            )
        })
        .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))
}
