use super::super::cid::Cid;
use super::super::error::Error;
use super::super::manifest::ManifestStore;
use super::super::store::Store;
use super::super::transaction::TransactionalStore;
use super::super::versioned_map::{MapSnapshot, MapVersionId, VersionedMap};
use super::super::Prolly;
use super::definition::{IndexProjection, SecondaryIndexRegistry};
use super::storage::{
    catalog_current_key, catalog_descriptor_key, catalog_format_key, catalog_map_id,
    control_record_key, control_root_name, index_map_id, IndexControl, IndexedHeadRecord,
    SecondaryIndexDescriptor, SECONDARY_INDEX_FORMAT_VERSION,
};

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
