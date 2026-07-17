use std::sync::{Arc, Mutex};

use prolly::{
    MapCatalogVerification, MapVersion, MapVersionId, Mutation, VersionPruneResult,
    VersionedMapBackup, VersionedMapUpdate,
};

use crate::{
    BindingEngine, DiffRecord, EntryRecord, KeyProofRecord, MutationRecord, ProllyBindingError,
    ProllyEngine, ProllyReadSession, RangeCursorRecord, RangePageRecord, SnapshotBundleRecord,
    TreeRecord,
};

macro_rules! with_readable_map {
    ($self:expr, $map:ident, $body:block) => {{
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            BindingEngine::File(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            BindingEngine::Host(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
        }
    }};
}

macro_rules! with_writable_map {
    ($self:expr, $map:ident, $body:block) => {{
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            BindingEngine::File(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let $map = engine.versioned_map(&$self.id);
                $body
            }
            BindingEngine::Host(_) => Err(ProllyBindingError::Internal {
                reason: "custom host stores do not expose versioned-map transactions".to_string(),
            }),
        }
    }};
}

/// Portable, owned description of one durable managed-map version.
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MapVersionRecord {
    /// Raw 32-byte content-derived version identifier.
    pub id: Vec<u8>,
    /// Immutable tree handle for this version.
    pub tree: TreeRecord,
    /// Creation timestamp recorded by the version root, when available.
    pub created_at_millis: Option<u64>,
    /// Whether this version was the head when resolved.
    pub is_head: bool,
}

impl From<MapVersion> for MapVersionRecord {
    fn from(version: MapVersion) -> Self {
        Self {
            id: version.id.as_cid().as_bytes().to_vec(),
            tree: version.tree.into(),
            created_at_millis: version.created_at_millis,
            is_head: version.is_head,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum MapUpdateKind {
    Applied,
    Unchanged,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, uniffi::Record)]
pub struct MapUpdateRecord {
    pub kind: MapUpdateKind,
    pub previous: Option<Vec<u8>>,
    pub current: Option<MapVersionRecord>,
}

impl From<VersionedMapUpdate> for MapUpdateRecord {
    fn from(update: VersionedMapUpdate) -> Self {
        match update {
            VersionedMapUpdate::Applied { previous, current } => Self {
                kind: MapUpdateKind::Applied,
                previous: previous.map(|id| id.into_cid().0.to_vec()),
                current: Some(current.into()),
            },
            VersionedMapUpdate::Unchanged { current } => Self {
                kind: MapUpdateKind::Unchanged,
                previous: None,
                current: current.map(Into::into),
            },
            VersionedMapUpdate::Conflict { current } => Self {
                kind: MapUpdateKind::Conflict,
                previous: None,
                current: current.map(Into::into),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct VersionPruneRecord {
    pub retained: Vec<Vec<u8>>,
    pub removed: Vec<Vec<u8>>,
}

impl From<VersionPruneResult> for VersionPruneRecord {
    fn from(result: VersionPruneResult) -> Self {
        let bytes = |id: MapVersionId| id.into_cid().0.to_vec();
        Self {
            retained: result.retained.into_iter().map(bytes).collect(),
            removed: result.removed.into_iter().map(bytes).collect(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MapCatalogVerificationRecord {
    pub head: Vec<u8>,
    pub version_count: u64,
    pub reachable_nodes: u64,
    pub reachable_bytes: u64,
}

impl From<MapCatalogVerification> for MapCatalogVerificationRecord {
    fn from(verification: MapCatalogVerification) -> Self {
        Self {
            head: verification.head.into_cid().0.to_vec(),
            version_count: verification.version_count as u64,
            reachable_nodes: verification.reachable_nodes as u64,
            reachable_bytes: verification.reachable_bytes as u64,
        }
    }
}

fn decode_version_id(bytes: &[u8]) -> Result<MapVersionId, ProllyBindingError> {
    MapVersionId::from_bytes(bytes).map_err(Into::into)
}

/// Application-facing managed map with version history and optimistic updates.
#[derive(uniffi::Object)]
pub struct BindingVersionedMap {
    engine: Arc<ProllyEngine>,
    id: Vec<u8>,
}

#[uniffi::export]
impl BindingVersionedMap {
    #[uniffi::constructor]
    pub fn new(engine: Arc<ProllyEngine>, id: Vec<u8>) -> Result<Self, ProllyBindingError> {
        if id.is_empty() {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "versioned-map id must not be empty".to_string(),
            });
        }
        Ok(Self { engine, id })
    }

    pub fn id(&self) -> Vec<u8> {
        self.id.clone()
    }

    pub fn is_initialized(&self) -> Result<bool, ProllyBindingError> {
        with_readable_map!(self, map, { map.is_initialized().map_err(Into::into) })
    }

    pub fn initialize(&self) -> Result<MapVersionRecord, ProllyBindingError> {
        with_writable_map!(self, map, {
            map.initialize()
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn head(&self) -> Result<Option<MapVersionRecord>, ProllyBindingError> {
        with_readable_map!(self, map, {
            map.head()
                .map(|version| version.map(MapVersionRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn version(&self, id: Vec<u8>) -> Result<Option<MapVersionRecord>, ProllyBindingError> {
        let id = decode_version_id(&id)?;
        with_readable_map!(self, map, {
            map.version(&id)
                .map(|version| version.map(MapVersionRecord::from))
                .map_err(Into::into)
        })
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        with_readable_map!(self, map, { map.get(&key).map_err(Into::into) })
    }

    pub fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, ProllyBindingError> {
        with_readable_map!(self, map, { map.get_many(&keys).map_err(Into::into) })
    }

    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<MapVersionRecord, ProllyBindingError> {
        with_writable_map!(self, map, {
            map.put(key, value)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<MapVersionRecord, ProllyBindingError> {
        with_writable_map!(self, map, {
            map.delete(key)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn apply(
        &self,
        mutations: Vec<MutationRecord>,
    ) -> Result<MapVersionRecord, ProllyBindingError> {
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_writable_map!(self, map, {
            map.apply(mutations)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn apply_at_millis(
        &self,
        mutations: Vec<MutationRecord>,
        timestamp_millis: u64,
    ) -> Result<MapVersionRecord, ProllyBindingError> {
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_writable_map!(self, map, {
            map.apply_at_millis(mutations, timestamp_millis)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn apply_if(
        &self,
        expected: Option<Vec<u8>>,
        mutations: Vec<MutationRecord>,
    ) -> Result<MapUpdateRecord, ProllyBindingError> {
        let expected = expected.as_deref().map(decode_version_id).transpose()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_writable_map!(self, map, {
            map.apply_if(expected.as_ref(), mutations)
                .map(MapUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn apply_if_at_millis(
        &self,
        expected: Option<Vec<u8>>,
        mutations: Vec<MutationRecord>,
        timestamp_millis: u64,
    ) -> Result<MapUpdateRecord, ProllyBindingError> {
        let expected = expected.as_deref().map(decode_version_id).transpose()?;
        let mutations = mutations
            .into_iter()
            .map(Mutation::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        with_writable_map!(self, map, {
            map.apply_if_at_millis(expected.as_ref(), mutations, timestamp_millis)
                .map(MapUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn put_if(
        &self,
        expected: Option<Vec<u8>>,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<MapUpdateRecord, ProllyBindingError> {
        let expected = expected.as_deref().map(decode_version_id).transpose()?;
        with_writable_map!(self, map, {
            map.put_if(expected.as_ref(), key, value)
                .map(MapUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn delete_if(
        &self,
        expected: Option<Vec<u8>>,
        key: Vec<u8>,
    ) -> Result<MapUpdateRecord, ProllyBindingError> {
        let expected = expected.as_deref().map(decode_version_id).transpose()?;
        with_writable_map!(self, map, {
            map.delete_if(expected.as_ref(), key)
                .map(MapUpdateRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn versions(&self) -> Result<Vec<MapVersionRecord>, ProllyBindingError> {
        with_readable_map!(self, map, {
            map.versions()
                .map(|versions| versions.into_iter().map(Into::into).collect())
                .map_err(Into::into)
        })
    }

    pub fn backup(&self) -> Result<Vec<u8>, ProllyBindingError> {
        with_readable_map!(self, map, {
            map.backup()
                .and_then(|backup| backup.to_bytes())
                .map_err(Into::into)
        })
    }

    pub fn restore_backup(&self, bytes: Vec<u8>) -> Result<MapVersionRecord, ProllyBindingError> {
        let backup = VersionedMapBackup::from_bytes(&bytes)?;
        with_writable_map!(self, map, {
            map.restore_backup(&backup)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prune_versions(
        &self,
        keep_latest: u64,
    ) -> Result<VersionPruneRecord, ProllyBindingError> {
        let keep_latest =
            usize::try_from(keep_latest).map_err(|_| ProllyBindingError::InvalidArgument {
                reason: "keep_latest does not fit this platform".to_string(),
            })?;
        with_writable_map!(self, map, {
            map.prune_versions(keep_latest)
                .map(VersionPruneRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn verify_catalog(&self) -> Result<MapCatalogVerificationRecord, ProllyBindingError> {
        with_readable_map!(self, map, {
            map.verify_catalog()
                .map(MapCatalogVerificationRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn compare(
        &self,
        base: Vec<u8>,
        target: Vec<u8>,
    ) -> Result<Arc<BindingMapComparison>, ProllyBindingError> {
        let base = self
            .version(base)?
            .ok_or_else(|| ProllyBindingError::NotFound {
                reason: "base map version is not cataloged".to_string(),
            })?;
        let target = self
            .version(target)?
            .ok_or_else(|| ProllyBindingError::NotFound {
                reason: "target map version is not cataloged".to_string(),
            })?;
        Ok(Arc::new(BindingMapComparison {
            engine: self.engine.clone(),
            base,
            target,
        }))
    }

    pub fn compare_to_head(
        &self,
        base: Vec<u8>,
    ) -> Result<Arc<BindingMapComparison>, ProllyBindingError> {
        let head = self.head()?.ok_or_else(|| ProllyBindingError::NotFound {
            reason: "versioned map has not been initialized".to_string(),
        })?;
        self.compare(base, head.id)
    }

    pub fn rollback_to(&self, id: Vec<u8>) -> Result<MapVersionRecord, ProllyBindingError> {
        let id = decode_version_id(&id)?;
        with_writable_map!(self, map, {
            map.rollback_to(&id)
                .map(MapVersionRecord::from)
                .map_err(Into::into)
        })
    }

    pub fn prepare_merge(
        &self,
        base: Vec<u8>,
        candidate: Vec<u8>,
    ) -> Result<Arc<BindingMapMerge>, ProllyBindingError> {
        let base = self
            .version(base)?
            .ok_or_else(|| ProllyBindingError::NotFound {
                reason: "merge base version is not cataloged".to_string(),
            })?;
        let candidate = self
            .version(candidate)?
            .ok_or_else(|| ProllyBindingError::NotFound {
                reason: "merge candidate version is not cataloged".to_string(),
            })?;
        let head = self.head()?.ok_or_else(|| ProllyBindingError::NotFound {
            reason: "versioned map has not been initialized".to_string(),
        })?;
        Ok(Arc::new(BindingMapMerge {
            map: Arc::new(BindingVersionedMap {
                engine: self.engine.clone(),
                id: self.id.clone(),
            }),
            base,
            head,
            candidate,
        }))
    }

    pub fn subscribe(&self) -> Result<Arc<BindingMapSubscription>, ProllyBindingError> {
        let last_seen = self.head()?.map(|version| version.id);
        Ok(Arc::new(BindingMapSubscription::new(
            self.engine.clone(),
            self.id.clone(),
            last_seen,
        )))
    }

    pub fn subscribe_from(
        &self,
        last_seen: Option<Vec<u8>>,
    ) -> Result<Arc<BindingMapSubscription>, ProllyBindingError> {
        if let Some(id) = last_seen.as_deref() {
            decode_version_id(id)?;
        }
        Ok(Arc::new(BindingMapSubscription::new(
            self.engine.clone(),
            self.id.clone(),
            last_seen,
        )))
    }

    pub fn snapshot(&self) -> Result<Option<Arc<BindingMapSnapshot>>, ProllyBindingError> {
        self.head().map(|version| {
            version.map(|version| Arc::new(BindingMapSnapshot::new(self.engine.clone(), version)))
        })
    }

    pub fn snapshot_at(
        &self,
        id: Vec<u8>,
    ) -> Result<Option<Arc<BindingMapSnapshot>>, ProllyBindingError> {
        self.version(id).map(|version| {
            version.map(|version| Arc::new(BindingMapSnapshot::new(self.engine.clone(), version)))
        })
    }
}

/// Three-way merge pinned to a concrete base, head, and candidate.
#[derive(uniffi::Object)]
pub struct BindingMapMerge {
    map: Arc<BindingVersionedMap>,
    base: MapVersionRecord,
    head: MapVersionRecord,
    candidate: MapVersionRecord,
}

#[uniffi::export]
impl BindingMapMerge {
    pub fn base(&self) -> MapVersionRecord {
        self.base.clone()
    }

    pub fn head(&self) -> MapVersionRecord {
        self.head.clone()
    }

    pub fn candidate(&self) -> MapVersionRecord {
        self.candidate.clone()
    }

    pub fn merge(&self, resolver: Option<String>) -> Result<TreeRecord, ProllyBindingError> {
        self.map.engine.merge(
            self.base.tree.clone(),
            self.head.tree.clone(),
            self.candidate.tree.clone(),
            resolver,
        )
    }

    /// Publish only if the head pinned when this object was created is still current.
    pub fn publish(&self, resolver: Option<String>) -> Result<MapUpdateRecord, ProllyBindingError> {
        let current = self.map.head()?;
        if current.as_ref().map(|version| &version.id) != Some(&self.head.id) {
            return Ok(MapUpdateRecord {
                kind: MapUpdateKind::Conflict,
                previous: None,
                current,
            });
        }
        let base = decode_version_id(&self.base.id)?;
        let candidate = decode_version_id(&self.candidate.id)?;
        with_writable_map!(self.map.as_ref(), map, {
            let merge = map.prepare_merge(&base, &candidate)?;
            let resolver = crate::resolver_from_name(resolver.clone())?;
            merge
                .publish(resolver)
                .map(MapUpdateRecord::from)
                .map_err(Into::into)
        })
    }
}

/// Owned version-pinned comparison. It never re-resolves head.
#[derive(uniffi::Object)]
pub struct BindingMapComparison {
    engine: Arc<ProllyEngine>,
    base: MapVersionRecord,
    target: MapVersionRecord,
}

#[uniffi::export]
impl BindingMapComparison {
    pub fn base(&self) -> MapVersionRecord {
        self.base.clone()
    }

    pub fn target(&self) -> MapVersionRecord {
        self.target.clone()
    }

    pub fn diff(&self) -> Result<Vec<DiffRecord>, ProllyBindingError> {
        self.engine
            .diff(self.base.tree.clone(), self.target.tree.clone())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct MapChangeEventRecord {
    pub previous: Option<Vec<u8>>,
    pub current: MapVersionRecord,
    pub diffs: Vec<DiffRecord>,
}

/// Resumable polling subscription with portable owned state.
#[derive(uniffi::Object)]
pub struct BindingMapSubscription {
    engine: Arc<ProllyEngine>,
    map_id: Vec<u8>,
    last_seen: Mutex<Option<Vec<u8>>>,
}

impl BindingMapSubscription {
    fn new(engine: Arc<ProllyEngine>, map_id: Vec<u8>, last_seen: Option<Vec<u8>>) -> Self {
        Self {
            engine,
            map_id,
            last_seen: Mutex::new(last_seen),
        }
    }

    fn map(&self) -> BindingVersionedMap {
        BindingVersionedMap {
            engine: self.engine.clone(),
            id: self.map_id.clone(),
        }
    }
}

#[uniffi::export]
impl BindingMapSubscription {
    pub fn last_seen(&self) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        self.last_seen
            .lock()
            .map(|last_seen| last_seen.clone())
            .map_err(|_| ProllyBindingError::Internal {
                reason: "versioned-map subscription state is poisoned".to_string(),
            })
    }

    pub fn poll(&self) -> Result<Option<MapChangeEventRecord>, ProllyBindingError> {
        let mut last_seen = self
            .last_seen
            .lock()
            .map_err(|_| ProllyBindingError::Internal {
                reason: "versioned-map subscription state is poisoned".to_string(),
            })?;
        let map = self.map();
        let Some(current) = map.head()? else {
            return Ok(None);
        };
        if last_seen.as_ref() == Some(&current.id) {
            return Ok(None);
        }
        let previous_tree = match last_seen.as_ref() {
            Some(id) => {
                map.version(id.clone())?
                    .ok_or_else(|| ProllyBindingError::NotFound {
                        reason: "subscription resume version was pruned".to_string(),
                    })?
                    .tree
            }
            None => self.engine.create(),
        };
        let diffs = self.engine.diff(previous_tree, current.tree.clone())?;
        let previous = last_seen.replace(current.id.clone());
        Ok(Some(MapChangeEventRecord {
            previous,
            current,
            diffs,
        }))
    }
}

/// Owned immutable snapshot that remains valid while the managed head advances.
#[derive(uniffi::Object)]
pub struct BindingMapSnapshot {
    engine: Arc<ProllyEngine>,
    version: MapVersionRecord,
}

impl BindingMapSnapshot {
    fn new(engine: Arc<ProllyEngine>, version: MapVersionRecord) -> Self {
        Self { engine, version }
    }
}

#[uniffi::export]
impl BindingMapSnapshot {
    pub fn id(&self) -> Vec<u8> {
        self.version.id.clone()
    }

    pub fn tree(&self) -> TreeRecord {
        self.version.tree.clone()
    }

    pub fn version(&self) -> MapVersionRecord {
        self.version.clone()
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        self.engine.get(self.version.tree.clone(), key)
    }

    pub fn get_many(&self, keys: Vec<Vec<u8>>) -> Result<Vec<Option<Vec<u8>>>, ProllyBindingError> {
        self.engine.get_many(self.version.tree.clone(), keys)
    }

    pub fn contains_key(&self, key: Vec<u8>) -> Result<bool, ProllyBindingError> {
        self.get(key).map(|value| value.is_some())
    }

    pub fn first_entry(&self) -> Result<Option<EntryRecord>, ProllyBindingError> {
        self.engine.first_entry(self.version.tree.clone())
    }

    pub fn last_entry(&self) -> Result<Option<EntryRecord>, ProllyBindingError> {
        self.engine.last_entry(self.version.tree.clone())
    }

    pub fn lower_bound(&self, key: Vec<u8>) -> Result<Option<EntryRecord>, ProllyBindingError> {
        self.engine.lower_bound(self.version.tree.clone(), key)
    }

    pub fn upper_bound(&self, key: Vec<u8>) -> Result<Option<EntryRecord>, ProllyBindingError> {
        self.engine.upper_bound(self.version.tree.clone(), key)
    }

    pub fn range(
        &self,
        start: Vec<u8>,
        range_end: Option<Vec<u8>>,
    ) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        self.engine
            .range(self.version.tree.clone(), start, range_end)
    }

    pub fn prefix(&self, prefix: Vec<u8>) -> Result<Vec<EntryRecord>, ProllyBindingError> {
        self.engine.prefix(self.version.tree.clone(), prefix)
    }

    pub fn range_page(
        &self,
        cursor: Option<RangeCursorRecord>,
        range_end: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<RangePageRecord, ProllyBindingError> {
        self.engine
            .range_page(self.version.tree.clone(), cursor, range_end, limit)
    }

    pub fn prefix_page(
        &self,
        prefix: Vec<u8>,
        cursor: Option<RangeCursorRecord>,
        limit: u64,
    ) -> Result<RangePageRecord, ProllyBindingError> {
        self.engine
            .prefix_page(self.version.tree.clone(), prefix, cursor, limit)
    }

    pub fn prove_key(&self, key: Vec<u8>) -> Result<KeyProofRecord, ProllyBindingError> {
        self.engine.prove_key(self.version.tree.clone(), key)
    }

    pub fn export(&self) -> Result<SnapshotBundleRecord, ProllyBindingError> {
        self.engine.export_snapshot(self.version.tree.clone())
    }

    /// Bind this snapshot to a reusable session. Native adapters use the
    /// packed borrowed-read ABI from this session on performance-sensitive paths.
    pub fn read_session(&self) -> Result<Arc<ProllyReadSession>, ProllyBindingError> {
        self.engine.read_session(self.version.tree.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ConfigRecord, MutationKind, MutationRecord, ProllyEngine};

    fn memory_engine() -> Arc<ProllyEngine> {
        Arc::new(ProllyEngine::memory(ConfigRecord::from(prolly::Config::default())).unwrap())
    }

    #[test]
    fn snapshot_stays_pinned_while_head_advances() {
        let engine = memory_engine();
        let map = engine.versioned_map(b"users".to_vec()).unwrap();

        let initial = map.initialize().unwrap();
        let first = map.put(b"alice".to_vec(), b"active".to_vec()).unwrap();
        let snapshot = map.snapshot_at(first.id.clone()).unwrap().unwrap();
        let second = map.put(b"alice".to_vec(), b"disabled".to_vec()).unwrap();

        assert_ne!(initial.id, first.id);
        assert_ne!(first.id, second.id);
        assert_eq!(
            snapshot.get(b"alice".to_vec()).unwrap(),
            Some(b"active".to_vec())
        );
        assert_eq!(
            map.get(b"alice".to_vec()).unwrap(),
            Some(b"disabled".to_vec())
        );
        assert_eq!(snapshot.version().id, first.id);
    }

    #[test]
    fn history_conflict_backup_restore_and_prune_round_trip() {
        let map = BindingVersionedMap::new(memory_engine(), b"users".to_vec()).unwrap();
        let initial = map.initialize().unwrap();
        let first = map
            .apply(vec![MutationRecord {
                kind: MutationKind::Upsert,
                key: b"alice".to_vec(),
                value: Some(b"active".to_vec()),
            }])
            .unwrap();
        let head = map.put(b"bob".to_vec(), b"active".to_vec()).unwrap();

        let conflict = map
            .apply_if(
                Some(first.id.clone()),
                vec![MutationRecord {
                    kind: MutationKind::Delete,
                    key: b"alice".to_vec(),
                    value: None,
                }],
            )
            .unwrap();
        assert_eq!(conflict.kind, MapUpdateKind::Conflict);
        assert_eq!(conflict.current.unwrap().id, head.id);

        assert_eq!(map.versions().unwrap().len(), 3);
        let verification = map.verify_catalog().unwrap();
        assert_eq!(verification.version_count, 3);

        let backup = map.backup().unwrap();
        let restored = BindingVersionedMap::new(memory_engine(), b"users".to_vec()).unwrap();
        assert_eq!(restored.restore_backup(backup).unwrap().id, head.id);
        assert_eq!(
            restored.get(b"alice".to_vec()).unwrap(),
            Some(b"active".to_vec())
        );

        let pruned = map.prune_versions(1).unwrap();
        assert_eq!(pruned.retained, vec![head.id]);
        assert_eq!(pruned.removed.len(), 2);
        assert!(pruned.removed.contains(&initial.id));
        assert!(pruned.removed.contains(&first.id));
    }

    #[test]
    fn comparison_and_subscription_are_version_pinned() {
        let map = BindingVersionedMap::new(memory_engine(), b"users".to_vec()).unwrap();
        let initial = map.initialize().unwrap();
        let first = map.put(b"alice".to_vec(), b"active".to_vec()).unwrap();
        let comparison = map.compare(initial.id.clone(), first.id.clone()).unwrap();
        assert_eq!(comparison.base().id, initial.id);
        assert_eq!(comparison.target().id, first.id);
        assert_eq!(comparison.diff().unwrap().len(), 1);

        let subscription = map.subscribe_from(Some(initial.id.clone())).unwrap();
        let event = subscription.poll().unwrap().unwrap();
        assert_eq!(event.previous, Some(initial.id));
        assert_eq!(event.current.id, first.id);
        assert_eq!(event.diffs.len(), 1);
        assert!(subscription.poll().unwrap().is_none());
    }

    #[test]
    fn snapshot_exposes_paging_and_proofs_without_lifetime_leaks() {
        let map = BindingVersionedMap::new(memory_engine(), b"users".to_vec()).unwrap();
        map.put(b"alice".to_vec(), b"active".to_vec()).unwrap();
        map.put(b"bob".to_vec(), b"active".to_vec()).unwrap();
        let snapshot = map.snapshot().unwrap().unwrap();

        assert!(snapshot.contains_key(b"alice".to_vec()).unwrap());
        assert_eq!(snapshot.first_entry().unwrap().unwrap().key, b"alice");
        assert_eq!(snapshot.range(Vec::new(), None).unwrap().len(), 2);
        assert_eq!(snapshot.range_page(None, None, 1).unwrap().entries.len(), 1);
        assert_eq!(
            snapshot.prove_key(b"missing".to_vec()).unwrap().key,
            b"missing"
        );
        assert!(snapshot.export().unwrap().nodes.len() >= 1);
    }

    #[test]
    fn merge_is_pinned_and_publishes_with_compare_and_swap() {
        let map = BindingVersionedMap::new(memory_engine(), b"users".to_vec()).unwrap();
        map.initialize().unwrap();
        let base = map.put(b"alice".to_vec(), b"base".to_vec()).unwrap();
        let head = map.put(b"alice".to_vec(), b"head".to_vec()).unwrap();
        map.rollback_to(base.id.clone()).unwrap();
        let candidate = map.put(b"alice".to_vec(), b"candidate".to_vec()).unwrap();
        map.rollback_to(head.id.clone()).unwrap();

        let merge = map
            .prepare_merge(base.id.clone(), candidate.id.clone())
            .unwrap();
        assert_eq!(merge.head().id, head.id);
        let preview = merge.merge(Some("prefer_right".to_string())).unwrap();
        assert_eq!(
            map.engine.get(preview, b"alice".to_vec()).unwrap(),
            Some(b"candidate".to_vec())
        );
        assert_eq!(
            merge
                .publish(Some("prefer_right".to_string()))
                .unwrap()
                .kind,
            MapUpdateKind::Applied
        );
        assert_eq!(
            map.get(b"alice".to_vec()).unwrap(),
            Some(b"candidate".to_vec())
        );
    }
}
