use super::super::builder::SortedBatchBuilder;
use super::super::cid::Cid;
use super::super::error::{Error, Mutation};
use super::super::manifest::ManifestStore;
use super::super::store::Store;
use super::super::transaction::{TransactionConflict, TransactionalStore};
use super::super::tree::Tree;
use super::super::versioned_map::{
    IndexMaintenancePermit, MapSnapshot, MapVersion, MapVersionId, VersionedMap, VersionedMapUpdate,
};
use super::super::Prolly;
use super::definition::{IndexProjection, SecondaryIndexRegistry};
use super::storage::{
    catalog_checkpoint_key, catalog_current_key, catalog_descriptor_key, catalog_format_key,
    catalog_map_id, control_record_key, control_root_name, index_map_id, physical_index_key,
    ActiveIndexControl, IndexCheckpoint, IndexControl, IndexValue, IndexedHeadRecord,
    SecondaryIndexDescriptor, SECONDARY_INDEX_FORMAT_VERSION,
};
use std::collections::BTreeMap;

/// Bounded startup-health details for one active index generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActiveIndexHealth {
    pub name: Vec<u8>,
    pub generation: u64,
    pub fingerprint: Cid,
    pub projection: IndexProjection,
    pub index_map_id: Vec<u8>,
    pub index_version: MapVersionId,
}

/// Structurally validated current state of an [`IndexedMap`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexedMapHealth {
    pub source_map_id: Vec<u8>,
    pub source_version: Option<MapVersionId>,
    pub catalog_version: Option<MapVersionId>,
    pub active_indexes: Vec<ActiveIndexHealth>,
    pub supports_transactions: bool,
}

/// Outcome of one idempotent dynamic index registration attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexBuildResult {
    pub source_version: MapVersionId,
    pub index_version: MapVersionId,
    pub catalog_version: MapVersionId,
    pub generation: u64,
    pub entries: usize,
    pub attempts: usize,
    pub activated: bool,
}

/// Strict coordinator for one source map and its active secondary indexes.
pub struct IndexedMap<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    pub(crate) prolly: &'a Prolly<S>,
    pub(crate) source_map_id: Vec<u8>,
    pub(crate) registry: SecondaryIndexRegistry,
}

impl<'a, S> IndexedMap<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    pub(crate) fn open(
        prolly: &'a Prolly<S>,
        source_map_id: impl AsRef<[u8]>,
        registry: SecondaryIndexRegistry,
    ) -> Result<Self, Error> {
        if !prolly.store().supports_transactions() {
            return Err(Error::UnsupportedTransactions {
                store: std::any::type_name::<S>(),
            });
        }
        if source_map_id.as_ref().is_empty() {
            return Err(Error::InvalidIndexDefinition {
                reason: "indexed source map ID must not be empty".to_string(),
            });
        }
        let indexed = Self {
            prolly,
            source_map_id: source_map_id.as_ref().to_vec(),
            registry,
        };
        indexed.validate_state()?;
        Ok(indexed)
    }

    /// Stable source-map identifier.
    pub fn id(&self) -> &[u8] {
        &self.source_map_id
    }

    /// Borrow runtime definitions supplied when this handle was opened.
    pub fn registry(&self) -> &SecondaryIndexRegistry {
        &self.registry
    }

    /// Open the underlying source as a read-only raw map handle.
    pub fn source(&self) -> VersionedMap<'_, S> {
        self.prolly.versioned_map(&self.source_map_id)
    }

    /// Re-run bounded structural startup validation and return exact health state.
    pub fn health(&self) -> Result<IndexedMapHealth, Error> {
        self.validate_state()
    }

    fn validate_state(&self) -> Result<IndexedMapHealth, Error> {
        let source = self.source();
        let source_head = source.head()?;
        let control = self.load_control()?;
        let catalog = self
            .prolly
            .versioned_map(catalog_map_id(&self.source_map_id));
        let catalog_head = catalog.head()?;

        let Some(control) = control else {
            if let Some(catalog_head) = &catalog_head {
                let snapshot = catalog.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
                    Error::InvalidVersionedMap(
                        "catalog head has no matching immutable version root".to_string(),
                    )
                })?;
                validate_catalog_format(&snapshot)?;
                let current = snapshot
                    .get(&catalog_current_key())?
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(
                            "secondary-index catalog is missing current selection".to_string(),
                        )
                    })
                    .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))?;
                if !current.indexes.is_empty() {
                    return Err(Error::InvalidVersionedMap(
                        "catalog has active indexes while the control root is absent".to_string(),
                    ));
                }
                if source_head.as_ref().map(|head| &head.id) != Some(&current.source_version) {
                    return Err(Error::InvalidVersionedMap(
                        "deactivated catalog current source does not match source head".to_string(),
                    ));
                }
            }
            return Ok(IndexedMapHealth {
                source_map_id: self.source_map_id.clone(),
                source_version: source_head.map(|head| head.id),
                catalog_version: catalog_head.map(|head| head.id),
                active_indexes: Vec::new(),
                supports_transactions: true,
            });
        };

        if control.catalog_map_id != catalog_map_id(&self.source_map_id) {
            return Err(Error::InvalidVersionedMap(
                "secondary-index control references the wrong catalog map".to_string(),
            ));
        }
        let catalog_head = catalog_head.ok_or_else(|| {
            Error::InvalidVersionedMap(
                "secondary-index control is active but the catalog is absent".to_string(),
            )
        })?;
        let catalog_snapshot = catalog.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap(
                "catalog head has no matching immutable version root".to_string(),
            )
        })?;
        validate_catalog_format(&catalog_snapshot)?;
        let current_bytes = catalog_snapshot
            .get(&catalog_current_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "secondary-index catalog is missing current selection".to_string(),
                )
            })?;
        let current = IndexedHeadRecord::from_bytes(&current_bytes)?;
        let source_head = source_head.ok_or_else(|| {
            Error::InvalidVersionedMap(
                "secondary-index control is active but the source is uninitialized".to_string(),
            )
        })?;
        if source_head.id != current.source_version
            || source.version(&current.source_version)?.is_none()
        {
            return Err(Error::InvalidVersionedMap(
                "catalog current source version does not match the durable source head".to_string(),
            ));
        }
        if control.active.len() != current.indexes.len() {
            return Err(Error::InvalidVersionedMap(
                "control and catalog active-index counts disagree".to_string(),
            ));
        }

        let mut active_indexes = Vec::with_capacity(current.indexes.len());
        for (control_entry, checkpoint) in control.active.iter().zip(&current.indexes) {
            if control_entry.name != checkpoint.index_name
                || control_entry.fingerprint != checkpoint.definition_fingerprint
                || checkpoint.source_map_id != self.source_map_id
                || checkpoint.source_version != current.source_version
            {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "control, source ownership, or current selection mismatch".to_string(),
                });
            }
            let runtime = self.registry.get(&checkpoint.index_name).ok_or_else(|| {
                Error::IndexRuntimeDefinitionMissing {
                    name: checkpoint.index_name.clone(),
                    generation: checkpoint.generation,
                }
            })?;
            let descriptor_bytes = catalog_snapshot
                .get(&catalog_descriptor_key(
                    &checkpoint.index_name,
                    checkpoint.generation,
                ))?
                .ok_or_else(|| Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "persisted descriptor is missing".to_string(),
                })?;
            let descriptor = SecondaryIndexDescriptor::from_bytes(&descriptor_bytes)?;
            if descriptor.fingerprint != checkpoint.definition_fingerprint {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "descriptor fingerprint does not match checkpoint".to_string(),
                });
            }
            let runtime_descriptor =
                SecondaryIndexDescriptor::from_runtime(&self.source_map_id, runtime)?;
            if runtime_descriptor.fingerprint != descriptor.fingerprint {
                return Err(Error::IndexDefinitionMismatch {
                    name: checkpoint.index_name.clone(),
                    persisted: descriptor.fingerprint,
                    runtime: runtime_descriptor.fingerprint,
                });
            }
            let expected_index_map_id = index_map_id(
                &self.source_map_id,
                &checkpoint.index_name,
                &checkpoint.definition_fingerprint,
            );
            if checkpoint.index_map_id != expected_index_map_id {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "hidden index map ID does not match descriptor ownership".to_string(),
                });
            }
            let index_map = self.prolly.versioned_map(&checkpoint.index_map_id);
            if index_map.version(&checkpoint.index_version)?.is_none() {
                return Err(Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "hidden index immutable version root is missing".to_string(),
                });
            }
            active_indexes.push(ActiveIndexHealth {
                name: checkpoint.index_name.clone(),
                generation: checkpoint.generation,
                fingerprint: checkpoint.definition_fingerprint.clone(),
                projection: descriptor.projection,
                index_map_id: checkpoint.index_map_id.clone(),
                index_version: checkpoint.index_version.clone(),
            });
        }

        Ok(IndexedMapHealth {
            source_map_id: self.source_map_id.clone(),
            source_version: Some(source_head.id),
            catalog_version: Some(catalog_head.id),
            active_indexes,
            supports_transactions: true,
        })
    }

    fn load_control(&self) -> Result<Option<IndexControl>, Error> {
        let Some(tree) = self
            .prolly
            .load_named_root(&control_root_name(&self.source_map_id))?
        else {
            return Ok(None);
        };
        let bytes = self
            .prolly
            .get(&tree, &control_record_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "secondary-index control tree is missing its canonical record".to_string(),
                )
            })?;
        IndexControl::from_bytes(&bytes).map(Some)
    }
}

impl<S> IndexedMap<'_, S>
where
    S: Store + ManifestStore + TransactionalStore + Clone + Send + Sync,
{
    /// Build a runtime definition against a pinned source snapshot and atomically activate it.
    pub fn ensure_index(&self, name: impl AsRef<[u8]>) -> Result<IndexBuildResult, Error> {
        let name = name.as_ref();
        let definition =
            self.registry
                .get(name)
                .cloned()
                .ok_or_else(|| Error::InvalidIndexDefinition {
                    reason: format!("runtime index definition {:?} is not registered", name),
                })?;

        for attempt in 1..=definition.limits().max_build_retries {
            let health = self.health()?;
            if let Some(active) = health
                .active_indexes
                .iter()
                .find(|active| active.name == name)
            {
                if active.generation != definition.generation() {
                    return Err(Error::IndexOperationUnsupported {
                        operation:
                            "ensure_index cannot replace an active generation; use replace_index",
                    });
                }
                return Ok(IndexBuildResult {
                    source_version: health.source_version.ok_or_else(|| {
                        Error::InvalidVersionedMap("active index has no source version".to_string())
                    })?,
                    index_version: active.index_version.clone(),
                    catalog_version: health.catalog_version.ok_or_else(|| {
                        Error::InvalidVersionedMap(
                            "active index has no catalog version".to_string(),
                        )
                    })?,
                    generation: active.generation,
                    entries: self.index_entry_count(active)?,
                    attempts: attempt - 1,
                    activated: false,
                });
            }

            let source_head = self.source().head()?;
            let source_tree = source_head
                .as_ref()
                .map(|head| head.tree.clone())
                .unwrap_or_else(|| self.prolly.create());
            let source_version = MapVersionId::for_tree(&source_tree)?;
            let descriptor =
                SecondaryIndexDescriptor::from_runtime(&self.source_map_id, &definition)?;
            let hidden_map_id = index_map_id(&self.source_map_id, name, &descriptor.fingerprint);
            let hidden_head = self.prolly.versioned_map(&hidden_map_id).head()?;
            let (index_tree, entry_count) = self.build_index_tree(&source_tree, &definition)?;
            let index_version = MapVersionId::for_tree(&index_tree)?;

            let (catalog_head, catalog_tree, mut current_indexes) =
                self.load_catalog_selection()?;
            if current_indexes
                .iter()
                .any(|checkpoint| checkpoint.index_name == name)
            {
                return Err(Error::InvalidVersionedMap(
                    "catalog selection contains an index absent from validated health".to_string(),
                ));
            }
            let checkpoint = IndexCheckpoint {
                source_map_id: self.source_map_id.clone(),
                source_version: source_version.clone(),
                index_name: name.to_vec(),
                generation: definition.generation(),
                definition_fingerprint: descriptor.fingerprint.clone(),
                index_map_id: hidden_map_id.clone(),
                index_version: index_version.clone(),
            };
            current_indexes.push(checkpoint.clone());
            current_indexes.sort_by(|left, right| left.index_name.cmp(&right.index_name));
            let current = IndexedHeadRecord {
                source_version: source_version.clone(),
                indexes: current_indexes.clone(),
            };
            let catalog_candidate = self.prolly.batch(
                &catalog_tree,
                vec![
                    Mutation::Upsert {
                        key: catalog_format_key(),
                        val: SECONDARY_INDEX_FORMAT_VERSION.to_be_bytes().to_vec(),
                    },
                    Mutation::Upsert {
                        key: catalog_descriptor_key(name, definition.generation()),
                        val: descriptor.to_bytes()?,
                    },
                    Mutation::Upsert {
                        key: catalog_checkpoint_key(&source_version, name, definition.generation()),
                        val: checkpoint.to_bytes()?,
                    },
                    Mutation::Upsert {
                        key: catalog_current_key(),
                        val: current.to_bytes()?,
                    },
                ],
            )?;
            let catalog_version = MapVersionId::for_tree(&catalog_candidate)?;
            let next_control = IndexControl {
                source_map_id: self.source_map_id.clone(),
                catalog_map_id: catalog_map_id(&self.source_map_id),
                active: current_indexes
                    .iter()
                    .map(|checkpoint| ActiveIndexControl {
                        name: checkpoint.index_name.clone(),
                        fingerprint: checkpoint.definition_fingerprint.clone(),
                    })
                    .collect(),
            };
            let control_tree = self.prolly.put(
                &self.prolly.create(),
                control_record_key(),
                next_control.to_bytes()?,
            )?;
            let permit_fingerprint = self
                .load_control()?
                .map(|control| control.fingerprint())
                .transpose()?
                .unwrap_or(next_control.fingerprint()?);
            let catalog_id = catalog_map_id(&self.source_map_id);
            let source_expected = source_head.as_ref().map(|head| &head.id);
            let index_expected = hidden_head.as_ref().map(|head| &head.id);
            let catalog_expected = catalog_head.as_ref().map(|head| &head.id);

            let activation = self.prolly.versioned_maps_transaction(|maps| {
                let source_permit = IndexMaintenancePermit::new(
                    self.source_map_id.clone(),
                    permit_fingerprint.clone(),
                );
                let source_update = maps.publish_tree_index_maintenance(
                    &source_permit,
                    source_expected,
                    &source_tree,
                )?;
                let source_published =
                    require_non_conflict(source_update, self.source().head_name())?;

                let index_permit =
                    IndexMaintenancePermit::new(hidden_map_id.clone(), permit_fingerprint.clone());
                let index_update = maps.publish_tree_index_maintenance(
                    &index_permit,
                    index_expected,
                    &index_tree,
                )?;
                let index_published = require_non_conflict(index_update, &hidden_map_id)?;

                let catalog_permit =
                    IndexMaintenancePermit::new(catalog_id.clone(), permit_fingerprint.clone());
                let catalog_update = maps.publish_tree_index_maintenance(
                    &catalog_permit,
                    catalog_expected,
                    &catalog_candidate,
                )?;
                let catalog_published = require_non_conflict(catalog_update, &catalog_id)?;

                maps.raw_transaction()
                    .publish_named_root(&control_root_name(&self.source_map_id), &control_tree)?;
                Ok((source_published, index_published, catalog_published))
            });

            match activation {
                Ok((published_source, published_index, published_catalog)) => {
                    debug_assert_eq!(published_source.id, source_version);
                    debug_assert_eq!(published_index.id, index_version);
                    debug_assert_eq!(published_catalog.id, catalog_version);
                    return Ok(IndexBuildResult {
                        source_version,
                        index_version,
                        catalog_version,
                        generation: definition.generation(),
                        entries: entry_count,
                        attempts: attempt,
                        activated: true,
                    });
                }
                Err(Error::TransactionConflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }

        Err(Error::IndexBuildConflictLimitExceeded {
            name: name.to_vec(),
            attempts: definition.limits().max_build_retries,
        })
    }

    fn build_index_tree(
        &self,
        source_tree: &Tree,
        definition: &super::definition::SecondaryIndex,
    ) -> Result<(Tree, usize), Error> {
        let mut entries = BTreeMap::<Vec<u8>, Vec<u8>>::new();
        let mut temporary_bytes = 0usize;
        for source_entry in self.prolly.range(source_tree, &[], None)? {
            let (primary_key, source_value) = source_entry?;
            for emission in definition.extract(&primary_key, &source_value)? {
                let key = physical_index_key(&emission.term, &primary_key)?;
                let value = match definition.projection() {
                    IndexProjection::KeysOnly => IndexValue::KeysOnly,
                    IndexProjection::Include => IndexValue::Included(
                        emission
                            .projection
                            .expect("Include emissions are validated"),
                    ),
                    IndexProjection::All => IndexValue::FullSource(source_value.clone()),
                }
                .to_bytes()?;
                temporary_bytes = temporary_bytes
                    .saturating_add(key.len())
                    .saturating_add(value.len());
                if temporary_bytes > definition.limits().max_temporary_sort_bytes {
                    return Err(Error::IndexResourceLimitExceeded {
                        resource: "temporary_sort_bytes",
                        limit: definition.limits().max_temporary_sort_bytes,
                        actual: temporary_bytes,
                    });
                }
                if let Some(previous) = entries.insert(key, value.clone()) {
                    if previous != value {
                        return Err(Error::ConflictingIndexProjection {
                            name: definition.name().to_vec(),
                            primary_key: primary_key.clone(),
                            term: emission.term,
                        });
                    }
                }
                if entries.len() > definition.limits().max_verification_entries {
                    return Err(Error::IndexResourceLimitExceeded {
                        resource: "build_entries",
                        limit: definition.limits().max_verification_entries,
                        actual: entries.len(),
                    });
                }
            }
        }
        let entry_count = entries.len();
        let mut builder =
            SortedBatchBuilder::new(self.prolly.store().clone(), self.prolly.config().clone());
        for (key, value) in entries {
            builder.add(key, value)?;
        }
        Ok((builder.build()?, entry_count))
    }

    fn load_catalog_selection(
        &self,
    ) -> Result<(Option<MapVersion>, Tree, Vec<IndexCheckpoint>), Error> {
        let catalog = self
            .prolly
            .versioned_map(catalog_map_id(&self.source_map_id));
        let Some(head) = catalog.head()? else {
            return Ok((None, self.prolly.create(), Vec::new()));
        };
        let snapshot = catalog.snapshot_at(&head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap(
                "catalog head has no matching immutable version root".to_string(),
            )
        })?;
        validate_catalog_format(&snapshot)?;
        let current = snapshot
            .get(&catalog_current_key())?
            .map(|bytes| IndexedHeadRecord::from_bytes(&bytes))
            .transpose()?;
        Ok((
            Some(head.clone()),
            head.tree,
            current.map(|current| current.indexes).unwrap_or_default(),
        ))
    }

    fn index_entry_count(&self, active: &ActiveIndexHealth) -> Result<usize, Error> {
        let map = self.prolly.versioned_map(&active.index_map_id);
        let snapshot = map.snapshot_at(&active.index_version)?.ok_or_else(|| {
            Error::InvalidVersionedMap("active hidden index version is missing".to_string())
        })?;
        snapshot.range(&[], None)?.try_fold(0usize, |count, entry| {
            entry.map(|_| count.saturating_add(1))
        })
    }
}

fn require_non_conflict(
    update: VersionedMapUpdate,
    conflict_name: &[u8],
) -> Result<MapVersion, Error> {
    match update {
        VersionedMapUpdate::Applied { current, .. } => Ok(current),
        VersionedMapUpdate::Unchanged {
            current: Some(current),
        } => Ok(current),
        VersionedMapUpdate::Unchanged { current: None }
        | VersionedMapUpdate::Conflict { current: None }
        | VersionedMapUpdate::Conflict { current: Some(_) } => Err(Error::TransactionConflict(
            TransactionConflict::new(conflict_name.to_vec(), None, None),
        )),
    }
}

fn validate_catalog_format<S>(snapshot: &MapSnapshot<'_, S>) -> Result<(), Error>
where
    S: Store,
{
    let bytes = snapshot.get(&catalog_format_key())?.ok_or_else(|| {
        Error::InvalidVersionedMap("secondary-index catalog is missing format record".to_string())
    })?;
    if bytes != SECONDARY_INDEX_FORMAT_VERSION.to_be_bytes() {
        return Err(Error::InvalidVersionedMap(
            "unsupported secondary-index catalog format".to_string(),
        ));
    }
    Ok(())
}

impl<S> Prolly<S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    /// Open a strict indexed-map coordinator with runtime extractor definitions.
    pub fn indexed_map(
        &self,
        source_map_id: impl AsRef<[u8]>,
        registry: SecondaryIndexRegistry,
    ) -> Result<IndexedMap<'_, S>, Error> {
        IndexedMap::open(self, source_map_id, registry)
    }
}
