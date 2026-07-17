use super::super::builder::SortedBatchBuilder;
use super::super::cid::Cid;
use super::super::error::{Error, Mutation};
use super::super::gc::GcPlan;
use super::super::manifest::{ManifestStore, ManifestStoreScan, NamedRootRetention};
use super::super::store::{NodeStoreScan, Store};
use super::super::transaction::{TransactionConflict, TransactionalStore};
use super::super::tree::Tree;
use super::super::versioned_map::{
    IndexMaintenancePermit, MapSnapshot, MapVersion, MapVersionId, VersionedMap, VersionedMapUpdate,
};
use super::super::Prolly;
use super::definition::{IndexProjection, SecondaryIndex, SecondaryIndexRegistry};
use super::storage::{
    catalog_checkpoint_key, catalog_checkpoints_prefix, catalog_current_key,
    catalog_descriptor_key, catalog_format_key, catalog_map_id, catalog_retired_key,
    control_record_key, control_root_name, index_map_id, physical_index_key, ActiveIndexControl,
    IndexCheckpoint, IndexControl, IndexValue, IndexedHeadRecord, SecondaryIndexDescriptor,
    SECONDARY_INDEX_FORMAT_VERSION,
};
use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

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

/// Full semantic comparison of one rebuilt index and its selected checkpoint.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexVerification {
    pub name: Vec<u8>,
    pub source_version: MapVersionId,
    pub expected_index_version: MapVersionId,
    pub actual_index_version: MapVersionId,
    pub expected_entries: usize,
    pub actual_entries: usize,
    pub semantic_differences: usize,
}

/// Deterministic root-name pruning result for one indexed source namespace.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexedRetentionResult {
    pub retained_source_versions: Vec<MapVersionId>,
    pub removed_source_versions: Vec<MapVersionId>,
    pub retained_index_versions: Vec<MapVersionId>,
    pub removed_index_versions: Vec<MapVersionId>,
    pub removed_catalog_versions: Vec<MapVersionId>,
    pub removed_checkpoint_records: usize,
    pub removed_named_roots: Vec<Vec<u8>>,
}

impl IndexVerification {
    pub fn is_valid(&self) -> bool {
        self.expected_entries == self.actual_entries && self.semantic_differences == 0
    }

    /// Whether the selected tree is structurally identical to a sorted rebuild.
    pub fn is_canonical(&self) -> bool {
        self.expected_index_version == self.actual_index_version
    }
}

/// Complete coordinated publication selected by one catalog version.
#[derive(Clone, Debug, PartialEq)]
pub struct IndexedVersion {
    pub source: MapVersion,
    pub catalog: Option<MapVersion>,
    pub indexes: Vec<IndexCheckpoint>,
}

/// Conditional indexed-map write outcome.
#[derive(Clone, Debug, PartialEq)]
pub enum IndexedMapUpdate {
    Applied {
        previous: Option<MapVersionId>,
        current: IndexedVersion,
    },
    Unchanged {
        current: Option<IndexedVersion>,
    },
    Conflict {
        current: Option<IndexedVersion>,
    },
}

impl IndexedMapUpdate {
    pub fn is_applied(&self) -> bool {
        matches!(self, Self::Applied { .. })
    }

    pub fn is_conflict(&self) -> bool {
        matches!(self, Self::Conflict { .. })
    }
}

/// Last-write-wins mutation collector for [`IndexedMap::edit`].
#[derive(Clone, Debug, Default)]
pub struct IndexedMapEditor {
    mutations: Vec<Mutation>,
}

/// Cumulative logical work counters for one `IndexedMap` handle.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct IndexedMapMetricsSnapshot {
    pub normalized_source_mutations: u64,
    pub records_extracted: u64,
    pub terms_emitted: u64,
    pub projected_bytes: u64,
    pub physical_upserts: u64,
    pub physical_deletes: u64,
    pub unchanged_emissions_skipped: u64,
    pub source_nodes_written: u64,
    pub index_nodes_written: u64,
    pub catalog_nodes_written: u64,
    pub retries: u64,
    pub build_attempts: u64,
    pub verification_outcomes: u64,
    pub retained_roots: u64,
}

#[derive(Default)]
struct IndexedMapMetrics {
    normalized_source_mutations: AtomicU64,
    records_extracted: AtomicU64,
    terms_emitted: AtomicU64,
    projected_bytes: AtomicU64,
    physical_upserts: AtomicU64,
    physical_deletes: AtomicU64,
    unchanged_emissions_skipped: AtomicU64,
    source_nodes_written: AtomicU64,
    index_nodes_written: AtomicU64,
    catalog_nodes_written: AtomicU64,
    retries: AtomicU64,
    build_attempts: AtomicU64,
    verification_outcomes: AtomicU64,
    retained_roots: AtomicU64,
}

#[derive(Default)]
struct OperationMetrics {
    normalized_source_mutations: u64,
    records_extracted: u64,
    terms_emitted: u64,
    projected_bytes: u64,
    physical_upserts: u64,
    physical_deletes: u64,
    unchanged_emissions_skipped: u64,
    changed_indexes: u64,
}

impl IndexedMapEditor {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> &mut Self {
        self.mutations.push(Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        });
        self
    }

    pub fn delete(&mut self, key: impl Into<Vec<u8>>) -> &mut Self {
        self.mutations.push(Mutation::Delete { key: key.into() });
        self
    }

    pub fn push(&mut self, mutation: Mutation) -> &mut Self {
        self.mutations.push(mutation);
        self
    }

    pub fn len(&self) -> usize {
        self.mutations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.mutations.is_empty()
    }
}

/// Strict coordinator for one source map and its active secondary indexes.
pub struct IndexedMap<'a, S>
where
    S: Store + ManifestStore + TransactionalStore,
{
    pub(crate) prolly: &'a Prolly<S>,
    pub(crate) source_map_id: Vec<u8>,
    pub(crate) registry: SecondaryIndexRegistry,
    runtime_overrides: RwLock<BTreeMap<Vec<u8>, SecondaryIndex>>,
    metrics: Arc<IndexedMapMetrics>,
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
            runtime_overrides: RwLock::new(BTreeMap::new()),
            metrics: Arc::new(IndexedMapMetrics::default()),
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

    pub(crate) fn runtime_definition(&self, name: &[u8]) -> Option<SecondaryIndex> {
        self.runtime_overrides
            .read()
            .expect("secondary-index runtime override lock poisoned")
            .get(name)
            .cloned()
            .or_else(|| self.registry.get(name).cloned())
    }

    pub(crate) fn runtime_definition_for_descriptor(
        &self,
        descriptor: &SecondaryIndexDescriptor,
    ) -> Result<Option<SecondaryIndex>, Error> {
        let override_definition = self
            .runtime_overrides
            .read()
            .expect("secondary-index runtime override lock poisoned")
            .get(&descriptor.name)
            .cloned();
        for definition in override_definition
            .into_iter()
            .chain(self.registry.definitions_for_name(&descriptor.name))
        {
            let runtime = SecondaryIndexDescriptor::from_runtime(&self.source_map_id, &definition)?;
            if runtime.fingerprint == descriptor.fingerprint {
                return Ok(Some(definition));
            }
        }
        Ok(None)
    }

    fn runtime_definitions(&self) -> Vec<SecondaryIndex> {
        let overrides = self
            .runtime_overrides
            .read()
            .expect("secondary-index runtime override lock poisoned");
        let mut definitions = self
            .registry
            .iter()
            .filter(|definition| !overrides.contains_key(definition.name()))
            .cloned()
            .collect::<Vec<_>>();
        definitions.extend(overrides.values().cloned());
        definitions
    }

    /// Open the underlying source as a read-only raw map handle.
    pub fn source(&self) -> VersionedMap<'_, S> {
        self.prolly.versioned_map(&self.source_map_id)
    }

    /// Read one source key from the current durable head.
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.source().get(key)
    }

    /// Read one source value without allocating an intermediate owned value.
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl FnOnce(&[u8]) -> R,
    ) -> Result<Option<R>, Error> {
        self.source().get_with(key, read)
    }

    /// Re-run bounded structural startup validation and return exact health state.
    pub fn health(&self) -> Result<IndexedMapHealth, Error> {
        self.validate_state()
    }

    /// Snapshot cumulative logical work counters for this handle.
    pub fn metrics(&self) -> IndexedMapMetricsSnapshot {
        IndexedMapMetricsSnapshot {
            normalized_source_mutations: self
                .metrics
                .normalized_source_mutations
                .load(Ordering::Relaxed),
            records_extracted: self.metrics.records_extracted.load(Ordering::Relaxed),
            terms_emitted: self.metrics.terms_emitted.load(Ordering::Relaxed),
            projected_bytes: self.metrics.projected_bytes.load(Ordering::Relaxed),
            physical_upserts: self.metrics.physical_upserts.load(Ordering::Relaxed),
            physical_deletes: self.metrics.physical_deletes.load(Ordering::Relaxed),
            unchanged_emissions_skipped: self
                .metrics
                .unchanged_emissions_skipped
                .load(Ordering::Relaxed),
            source_nodes_written: self.metrics.source_nodes_written.load(Ordering::Relaxed),
            index_nodes_written: self.metrics.index_nodes_written.load(Ordering::Relaxed),
            catalog_nodes_written: self.metrics.catalog_nodes_written.load(Ordering::Relaxed),
            retries: self.metrics.retries.load(Ordering::Relaxed),
            build_attempts: self.metrics.build_attempts.load(Ordering::Relaxed),
            verification_outcomes: self.metrics.verification_outcomes.load(Ordering::Relaxed),
            retained_roots: self.metrics.retained_roots.load(Ordering::Relaxed),
        }
    }

    fn record_operation_metrics(&self, operation: &OperationMetrics) {
        self.metrics
            .normalized_source_mutations
            .fetch_add(operation.normalized_source_mutations, Ordering::Relaxed);
        self.metrics
            .records_extracted
            .fetch_add(operation.records_extracted, Ordering::Relaxed);
        self.metrics
            .terms_emitted
            .fetch_add(operation.terms_emitted, Ordering::Relaxed);
        self.metrics
            .projected_bytes
            .fetch_add(operation.projected_bytes, Ordering::Relaxed);
        self.metrics
            .physical_upserts
            .fetch_add(operation.physical_upserts, Ordering::Relaxed);
        self.metrics
            .physical_deletes
            .fetch_add(operation.physical_deletes, Ordering::Relaxed);
        self.metrics
            .unchanged_emissions_skipped
            .fetch_add(operation.unchanged_emissions_skipped, Ordering::Relaxed);
        self.metrics
            .source_nodes_written
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .index_nodes_written
            .fetch_add(operation.changed_indexes, Ordering::Relaxed);
        self.metrics
            .catalog_nodes_written
            .fetch_add(1, Ordering::Relaxed);
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
            let runtime = self
                .runtime_definition(&checkpoint.index_name)
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: checkpoint.index_name.clone(),
                    generation: checkpoint.generation,
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
                SecondaryIndexDescriptor::from_runtime(&self.source_map_id, &runtime)?;
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

    pub(crate) fn load_control(&self) -> Result<Option<IndexControl>, Error> {
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
            self.runtime_definition(name)
                .ok_or_else(|| Error::InvalidIndexDefinition {
                    reason: format!("runtime index definition {:?} is not registered", name),
                })?;

        for attempt in 1..=definition.limits().max_build_retries {
            self.metrics.build_attempts.fetch_add(1, Ordering::Relaxed);
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

    /// Rebuild and compare one exact retained source/index checkpoint without publishing roots.
    pub fn verify_index(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
    ) -> Result<IndexVerification, Error> {
        let name = name.as_ref();
        let definition =
            self.runtime_definition(name)
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: name.to_vec(),
                    generation: 0,
                })?;
        let selected = self.snapshot_at(source_version)?;
        let checkpoint = selected.index(name)?.checkpoint().clone();
        let (expected_tree, expected_entries) =
            self.build_index_tree(selected.source().tree(), &definition)?;
        let expected_index_version = MapVersionId::for_tree(&expected_tree)?;
        let actual = self
            .prolly
            .versioned_map(&checkpoint.index_map_id)
            .snapshot_at(&checkpoint.index_version)?
            .ok_or_else(|| Error::IndexCheckpointMismatch {
                name: name.to_vec(),
                source_version: source_version.clone(),
                reason: "checkpointed hidden index version is unavailable".to_string(),
            })?;
        let actual_entries = actual.range(&[], None)?.try_fold(0usize, |count, entry| {
            entry.map(|_| count.saturating_add(1))
        })?;
        let semantic_differences = self.prolly.diff(&expected_tree, actual.tree())?.len();
        let verification = IndexVerification {
            name: name.to_vec(),
            source_version: source_version.clone(),
            expected_index_version,
            actual_index_version: checkpoint.index_version,
            expected_entries,
            actual_entries,
            semantic_differences,
        };
        self.metrics
            .verification_outcomes
            .fetch_add(1, Ordering::Relaxed);
        Ok(verification)
    }

    /// Verify every generation currently selected by the catalog at one source version.
    pub fn verify_all(
        &self,
        source_version: &MapVersionId,
    ) -> Result<Vec<IndexVerification>, Error> {
        let selected = self.snapshot_at(source_version)?;
        let names = selected
            .indexes()
            .map(|index| index.name().to_vec())
            .collect::<Vec<_>>();
        names
            .into_iter()
            .map(|name| self.verify_index(name, source_version))
            .collect()
    }

    /// Rebuild and atomically correct one selected checkpoint when semantic drift exists.
    pub fn repair_index(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
    ) -> Result<IndexVerification, Error> {
        let name = name.as_ref();
        let definition =
            self.runtime_definition(name)
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: name.to_vec(),
                    generation: 0,
                })?;
        let selected = self.snapshot_at(source_version)?;
        let old_checkpoint = selected.index(name)?.checkpoint().clone();
        let (expected_tree, expected_entries) =
            self.build_index_tree(selected.source().tree(), &definition)?;
        let expected_version = MapVersionId::for_tree(&expected_tree)?;
        let actual_snapshot = self
            .prolly
            .versioned_map(&old_checkpoint.index_map_id)
            .snapshot_at(&old_checkpoint.index_version)?
            .ok_or_else(|| Error::IndexCheckpointMismatch {
                name: name.to_vec(),
                source_version: source_version.clone(),
                reason: "checkpointed hidden index version is unavailable".to_string(),
            })?;
        let actual_entries = actual_snapshot
            .range(&[], None)?
            .try_fold(0usize, |count, entry| {
                entry.map(|_| count.saturating_add(1))
            })?;
        let semantic_differences = self
            .prolly
            .diff(&expected_tree, actual_snapshot.tree())?
            .len();
        if expected_version == old_checkpoint.index_version && expected_entries == actual_entries {
            return Ok(IndexVerification {
                name: name.to_vec(),
                source_version: source_version.clone(),
                expected_index_version: expected_version.clone(),
                actual_index_version: expected_version,
                expected_entries,
                actual_entries,
                semantic_differences,
            });
        }

        let catalog_id = catalog_map_id(&self.source_map_id);
        let catalog_map = self.prolly.versioned_map(&catalog_id);
        let catalog_head = catalog_map.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("repair requires a catalog head".to_string())
        })?;
        let catalog = catalog_map.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap("repair catalog immutable root is missing".to_string())
        })?;
        validate_catalog_format(&catalog)?;
        let mut current = catalog
            .get(&catalog_current_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap("repair catalog has no current selection".to_string())
            })
            .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))?;
        let repaired_checkpoint = IndexCheckpoint {
            index_version: expected_version.clone(),
            ..old_checkpoint.clone()
        };
        let mut mutations = vec![Mutation::Upsert {
            key: catalog_checkpoint_key(source_version, name, old_checkpoint.generation),
            val: repaired_checkpoint.to_bytes()?,
        }];
        let repairs_current = current.source_version == *source_version;
        if repairs_current {
            let checkpoint = current
                .indexes
                .iter_mut()
                .find(|checkpoint| checkpoint.index_name == name)
                .ok_or_else(|| Error::IndexUnavailableAtVersion {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                })?;
            if *checkpoint != old_checkpoint {
                return Err(Error::IndexCheckpointMismatch {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                    reason: "catalog current checkpoint changed during repair planning".to_string(),
                });
            }
            *checkpoint = repaired_checkpoint.clone();
            mutations.push(Mutation::Upsert {
                key: catalog_current_key(),
                val: current.to_bytes()?,
            });
        }
        let catalog_candidate = self.prolly.batch(catalog.tree(), mutations)?;
        let control_fingerprint = self
            .load_control()?
            .ok_or_else(|| {
                Error::InvalidVersionedMap("repair requires an active control root".to_string())
            })?
            .fingerprint()?;
        self.prolly.versioned_maps_transaction(|maps| {
            let index_permit = IndexMaintenancePermit::new(
                old_checkpoint.index_map_id.clone(),
                control_fingerprint.clone(),
            );
            if repairs_current {
                let update = maps.publish_tree_index_maintenance(
                    &index_permit,
                    Some(&old_checkpoint.index_version),
                    &expected_tree,
                )?;
                require_non_conflict(update, &old_checkpoint.index_map_id)?;
            } else {
                maps.publish_version_index_maintenance(&index_permit, &expected_tree)?;
            }
            let catalog_permit =
                IndexMaintenancePermit::new(catalog_id.clone(), control_fingerprint.clone());
            let update = maps.publish_tree_index_maintenance(
                &catalog_permit,
                Some(&catalog_head.id),
                &catalog_candidate,
            )?;
            require_non_conflict(update, &catalog_id)?;
            Ok(())
        })?;
        self.metrics
            .verification_outcomes
            .fetch_add(1, Ordering::Relaxed);
        Ok(IndexVerification {
            name: name.to_vec(),
            source_version: source_version.clone(),
            expected_index_version: expected_version.clone(),
            actual_index_version: expected_version,
            expected_entries,
            actual_entries: expected_entries,
            semantic_differences: 0,
        })
    }

    /// Shadow-build and atomically replace one active definition generation.
    pub fn replace_index(
        &self,
        name: impl AsRef<[u8]>,
        new_definition: SecondaryIndex,
    ) -> Result<IndexBuildResult, Error> {
        let name = name.as_ref();
        if new_definition.name() != name {
            return Err(Error::InvalidIndexDefinition {
                reason: "replacement definition name does not match requested index".to_string(),
            });
        }
        let health = self.health()?;
        let old = health
            .active_indexes
            .iter()
            .find(|active| active.name == name)
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.to_vec(),
                source_version: health.source_version.clone().unwrap_or_else(|| {
                    MapVersionId::for_tree(&self.prolly.create()).expect("empty tree version")
                }),
            })?;
        if new_definition.generation() <= old.generation {
            return Err(Error::InvalidIndexDefinition {
                reason: "replacement generation must be strictly greater than active generation"
                    .to_string(),
            });
        }
        let new_descriptor =
            SecondaryIndexDescriptor::from_runtime(&self.source_map_id, &new_definition)?;
        if new_descriptor.fingerprint == old.fingerprint {
            return Err(Error::InvalidIndexDefinition {
                reason: "replacement descriptor fingerprint must differ".to_string(),
            });
        }
        let source = self.source().head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("replacement requires an initialized source".to_string())
        })?;
        let (index_tree, entries) = self.build_index_tree(&source.tree, &new_definition)?;
        let index_version = MapVersionId::for_tree(&index_tree)?;
        let hidden_map_id = index_map_id(&self.source_map_id, name, &new_descriptor.fingerprint);
        let hidden_head = self.prolly.versioned_map(&hidden_map_id).head()?;
        let catalog_id = catalog_map_id(&self.source_map_id);
        let catalog_map = self.prolly.versioned_map(&catalog_id);
        let catalog_head = catalog_map.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("replacement requires a catalog head".to_string())
        })?;
        let catalog = catalog_map.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap("replacement catalog root is missing".to_string())
        })?;
        let mut current = catalog
            .get(&catalog_current_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "replacement catalog has no current selection".to_string(),
                )
            })
            .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))?;
        if current.source_version != source.id {
            return Err(Error::transaction_conflict(TransactionConflict::new(
                self.source().head_name().to_vec(),
                None,
                None,
            )));
        }
        let position = current
            .indexes
            .iter()
            .position(|checkpoint| checkpoint.index_name == name)
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.to_vec(),
                source_version: source.id.clone(),
            })?;
        let old_checkpoint = current.indexes[position].clone();
        let old_descriptor = catalog
            .get(&catalog_descriptor_key(name, old_checkpoint.generation))?
            .ok_or_else(|| Error::IndexCheckpointMismatch {
                name: name.to_vec(),
                source_version: source.id.clone(),
                reason: "active descriptor is missing during replacement".to_string(),
            })?;
        let checkpoint = IndexCheckpoint {
            source_map_id: self.source_map_id.clone(),
            source_version: source.id.clone(),
            index_name: name.to_vec(),
            generation: new_definition.generation(),
            definition_fingerprint: new_descriptor.fingerprint.clone(),
            index_map_id: hidden_map_id.clone(),
            index_version: index_version.clone(),
        };
        current.indexes[position] = checkpoint.clone();
        let catalog_candidate = self.prolly.batch(
            catalog.tree(),
            vec![
                Mutation::Upsert {
                    key: catalog_descriptor_key(name, new_definition.generation()),
                    val: new_descriptor.to_bytes()?,
                },
                Mutation::Upsert {
                    key: catalog_checkpoint_key(&source.id, name, new_definition.generation()),
                    val: checkpoint.to_bytes()?,
                },
                Mutation::Upsert {
                    key: catalog_retired_key(name, old_checkpoint.generation),
                    val: old_descriptor,
                },
                Mutation::Upsert {
                    key: catalog_current_key(),
                    val: current.to_bytes()?,
                },
            ],
        )?;
        let next_control = IndexControl {
            source_map_id: self.source_map_id.clone(),
            catalog_map_id: catalog_id.clone(),
            active: current
                .indexes
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
        let control_fingerprint = self
            .load_control()?
            .ok_or_else(|| {
                Error::InvalidVersionedMap("replacement requires active control".to_string())
            })?
            .fingerprint()?;
        let (published_index, published_catalog) =
            self.prolly.versioned_maps_transaction(|maps| {
                let source_permit = IndexMaintenancePermit::new(
                    self.source_map_id.clone(),
                    control_fingerprint.clone(),
                );
                let source_update = maps.publish_tree_index_maintenance(
                    &source_permit,
                    Some(&source.id),
                    &source.tree,
                )?;
                require_non_conflict(source_update, self.source().head_name())?;
                let index_permit =
                    IndexMaintenancePermit::new(hidden_map_id.clone(), control_fingerprint.clone());
                let index_update = maps.publish_tree_index_maintenance(
                    &index_permit,
                    hidden_head.as_ref().map(|head| &head.id),
                    &index_tree,
                )?;
                let published_index = require_non_conflict(index_update, &hidden_map_id)?;
                let catalog_permit =
                    IndexMaintenancePermit::new(catalog_id.clone(), control_fingerprint.clone());
                let catalog_update = maps.publish_tree_index_maintenance(
                    &catalog_permit,
                    Some(&catalog_head.id),
                    &catalog_candidate,
                )?;
                let published_catalog = require_non_conflict(catalog_update, &catalog_id)?;
                maps.raw_transaction()
                    .publish_named_root(&control_root_name(&self.source_map_id), &control_tree)?;
                Ok((published_index, published_catalog))
            })?;
        self.runtime_overrides
            .write()
            .expect("secondary-index runtime override lock poisoned")
            .insert(name.to_vec(), new_definition.clone());
        Ok(IndexBuildResult {
            source_version: source.id,
            index_version: published_index.id,
            catalog_version: published_catalog.id,
            generation: new_definition.generation(),
            entries,
            attempts: 1,
            activated: true,
        })
    }

    /// Remove one active selection while retaining its descriptors, checkpoints, and roots.
    pub fn deactivate_index(&self, name: impl AsRef<[u8]>) -> Result<IndexedVersion, Error> {
        let name = name.as_ref();
        let health = self.health()?;
        let source = self.source().head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("deactivation requires an initialized source".to_string())
        })?;
        let catalog_id = catalog_map_id(&self.source_map_id);
        let catalog_map = self.prolly.versioned_map(&catalog_id);
        let catalog_head = catalog_map.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("deactivation requires a catalog head".to_string())
        })?;
        let catalog = catalog_map.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap("deactivation catalog root is missing".to_string())
        })?;
        let mut current = catalog
            .get(&catalog_current_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "deactivation catalog has no current selection".to_string(),
                )
            })
            .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))?;
        if current.source_version != source.id || health.source_version.as_ref() != Some(&source.id)
        {
            return Err(Error::transaction_conflict(TransactionConflict::new(
                self.source().head_name().to_vec(),
                None,
                None,
            )));
        }
        let before = current.indexes.len();
        current
            .indexes
            .retain(|checkpoint| checkpoint.index_name != name);
        if current.indexes.len() == before {
            return Err(Error::IndexUnavailableAtVersion {
                name: name.to_vec(),
                source_version: source.id.clone(),
            });
        }
        let catalog_candidate = self.prolly.batch(
            catalog.tree(),
            vec![Mutation::Upsert {
                key: catalog_current_key(),
                val: current.to_bytes()?,
            }],
        )?;
        let control = self.load_control()?.ok_or_else(|| {
            Error::InvalidVersionedMap("deactivation requires active control".to_string())
        })?;
        let control_fingerprint = control.fingerprint()?;
        let next_control_tree = if current.indexes.is_empty() {
            None
        } else {
            let next = IndexControl {
                source_map_id: self.source_map_id.clone(),
                catalog_map_id: catalog_id.clone(),
                active: current
                    .indexes
                    .iter()
                    .map(|checkpoint| ActiveIndexControl {
                        name: checkpoint.index_name.clone(),
                        fingerprint: checkpoint.definition_fingerprint.clone(),
                    })
                    .collect(),
            };
            Some(self.prolly.put(
                &self.prolly.create(),
                control_record_key(),
                next.to_bytes()?,
            )?)
        };
        let (published_source, published_catalog) =
            self.prolly.versioned_maps_transaction(|maps| {
                let source_permit = IndexMaintenancePermit::new(
                    self.source_map_id.clone(),
                    control_fingerprint.clone(),
                );
                let source_update = maps.publish_tree_index_maintenance(
                    &source_permit,
                    Some(&source.id),
                    &source.tree,
                )?;
                let published_source =
                    require_non_conflict(source_update, self.source().head_name())?;
                let catalog_permit =
                    IndexMaintenancePermit::new(catalog_id.clone(), control_fingerprint.clone());
                let catalog_update = maps.publish_tree_index_maintenance(
                    &catalog_permit,
                    Some(&catalog_head.id),
                    &catalog_candidate,
                )?;
                let published_catalog = require_non_conflict(catalog_update, &catalog_id)?;
                match &next_control_tree {
                    Some(tree) => maps
                        .raw_transaction()
                        .publish_named_root(&control_root_name(&self.source_map_id), tree)?,
                    None => maps
                        .raw_transaction()
                        .delete_named_root(&control_root_name(&self.source_map_id))?,
                }
                Ok((published_source, published_catalog))
            })?;
        Ok(IndexedVersion {
            source: published_source,
            catalog: Some(published_catalog),
            indexes: current.indexes,
        })
    }

    /// Apply source mutations while maintaining every active index atomically.
    pub fn apply(&self, mutations: Vec<Mutation>) -> Result<IndexedVersion, Error> {
        let max_retries = self
            .runtime_definitions()
            .iter()
            .map(|definition| definition.limits().max_write_retries)
            .min()
            .unwrap_or(8);
        let mut last_conflict = None;
        for _ in 0..max_retries {
            match self.try_apply_indexed(None, &mutations) {
                Ok(IndexedMapUpdate::Applied { current, .. }) => return Ok(current),
                Ok(IndexedMapUpdate::Unchanged {
                    current: Some(current),
                }) => return Ok(current),
                Ok(IndexedMapUpdate::Conflict { .. }) => {
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                Ok(IndexedMapUpdate::Unchanged { current: None }) => {
                    return Err(Error::InvalidVersionedMap(
                        "empty indexed update has no current source".to_string(),
                    ));
                }
                Err(Error::TransactionConflict(conflict)) => {
                    self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                    last_conflict = Some(*conflict);
                }
                Err(error) => return Err(error),
            }
        }
        Err(Error::transaction_conflict(last_conflict.unwrap_or_else(
            || TransactionConflict::new(self.source().head_name().to_vec(), None, None),
        )))
    }

    /// Conditionally apply source mutations when the expected source version is current.
    pub fn apply_if(
        &self,
        expected: Option<&MapVersionId>,
        mutations: Vec<Mutation>,
    ) -> Result<IndexedMapUpdate, Error> {
        self.try_apply_indexed(Some(expected), &mutations)
    }

    /// Insert or replace one source record and maintain all active indexes.
    pub fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<IndexedVersion, Error> {
        self.apply(vec![Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        }])
    }

    /// Delete one source record and maintain all active indexes.
    pub fn delete(&self, key: impl Into<Vec<u8>>) -> Result<IndexedVersion, Error> {
        self.apply(vec![Mutation::Delete { key: key.into() }])
    }

    /// Collect several source edits and publish one coordinated version.
    pub fn edit(&self, edit: impl FnOnce(&mut IndexedMapEditor)) -> Result<IndexedVersion, Error> {
        let mut editor = IndexedMapEditor::new();
        edit(&mut editor);
        self.apply(editor.mutations)
    }

    fn try_apply_indexed(
        &self,
        expected: Option<Option<&MapVersionId>>,
        mutations: &[Mutation],
    ) -> Result<IndexedMapUpdate, Error> {
        let health = self.health()?;
        if health.active_indexes.is_empty() {
            let update = match expected {
                Some(expected) => self.source().apply_if(expected, mutations.to_vec())?,
                None => {
                    let previous = self.source().head()?.map(|head| head.id);
                    let current = self.source().apply(mutations.to_vec())?;
                    if previous.as_ref() == Some(&current.id) {
                        VersionedMapUpdate::Unchanged {
                            current: Some(current),
                        }
                    } else {
                        VersionedMapUpdate::Applied { previous, current }
                    }
                }
            };
            return Ok(match update {
                VersionedMapUpdate::Applied { previous, current } => IndexedMapUpdate::Applied {
                    previous,
                    current: IndexedVersion {
                        source: current,
                        catalog: None,
                        indexes: Vec::new(),
                    },
                },
                VersionedMapUpdate::Unchanged { current } => IndexedMapUpdate::Unchanged {
                    current: current.map(|source| IndexedVersion {
                        source,
                        catalog: None,
                        indexes: Vec::new(),
                    }),
                },
                VersionedMapUpdate::Conflict { current } => IndexedMapUpdate::Conflict {
                    current: current.map(|source| IndexedVersion {
                        source,
                        catalog: None,
                        indexes: Vec::new(),
                    }),
                },
            });
        }

        let current = self.current_indexed_version()?;
        if let Some(expected) = expected {
            if current.as_ref().map(|version| &version.source.id) != expected {
                return Ok(IndexedMapUpdate::Conflict { current });
            }
        }
        let Some(current) = current else {
            return Err(Error::InvalidVersionedMap(
                "active indexes require a current coordinated version".to_string(),
            ));
        };

        let mut normalized = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        for mutation in mutations {
            match mutation {
                Mutation::Upsert { key, val } => {
                    normalized.insert(key.clone(), Some(val.clone()));
                }
                Mutation::Delete { key } => {
                    normalized.insert(key.clone(), None);
                }
            }
        }
        let mut operation = OperationMetrics {
            normalized_source_mutations: normalized.len() as u64,
            ..OperationMetrics::default()
        };
        if normalized.is_empty() {
            return Ok(IndexedMapUpdate::Unchanged {
                current: Some(current),
            });
        }

        let source_snapshot = self
            .source()
            .snapshot_at(&current.source.id)?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "current indexed source immutable root is missing".to_string(),
                )
            })?;
        let keys = normalized.keys().cloned().collect::<Vec<_>>();
        let old_values = source_snapshot.get_many(&keys)?;
        let source_mutations = normalized
            .iter()
            .map(|(key, value)| match value {
                Some(value) => Mutation::Upsert {
                    key: key.clone(),
                    val: value.clone(),
                },
                None => Mutation::Delete { key: key.clone() },
            })
            .collect::<Vec<_>>();
        let source_candidate = self.prolly.batch(&current.source.tree, source_mutations)?;
        if source_candidate == current.source.tree {
            return Ok(IndexedMapUpdate::Unchanged {
                current: Some(current),
            });
        }
        let source_version = MapVersionId::for_tree(&source_candidate)?;

        let mut planned_indexes = Vec::with_capacity(current.indexes.len());
        let mut new_checkpoints = Vec::with_capacity(current.indexes.len());
        for checkpoint in &current.indexes {
            let definition = self
                .runtime_definition(&checkpoint.index_name)
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: checkpoint.index_name.clone(),
                    generation: checkpoint.generation,
                })?;
            let index_version = self
                .prolly
                .versioned_map(&checkpoint.index_map_id)
                .version(&checkpoint.index_version)?
                .ok_or_else(|| Error::IndexCheckpointMismatch {
                    name: checkpoint.index_name.clone(),
                    source_version: current.source.id.clone(),
                    reason: "current hidden index root is missing".to_string(),
                })?;
            let mut delta = BTreeMap::<Vec<u8>, Mutation>::new();
            let mut projected_bytes = 0usize;
            for ((primary_key, final_value), old_value) in normalized.iter().zip(old_values.iter())
            {
                let old_entries =
                    self.projected_entries(&definition, primary_key, old_value.as_deref())?;
                let new_entries =
                    self.projected_entries(&definition, primary_key, final_value.as_deref())?;
                operation.records_extracted = operation
                    .records_extracted
                    .saturating_add(u64::from(old_value.is_some()))
                    .saturating_add(u64::from(final_value.is_some()));
                operation.terms_emitted = operation
                    .terms_emitted
                    .saturating_add(old_entries.len() as u64)
                    .saturating_add(new_entries.len() as u64);
                operation.unchanged_emissions_skipped =
                    operation.unchanged_emissions_skipped.saturating_add(
                        old_entries
                            .iter()
                            .filter(|(key, value)| new_entries.get(*key) == Some(*value))
                            .count() as u64,
                    );
                for (key, old_projection) in &old_entries {
                    if new_entries.get(key) != Some(old_projection)
                        && !new_entries.contains_key(key)
                    {
                        operation.physical_deletes = operation.physical_deletes.saturating_add(1);
                        delta.insert(key.clone(), Mutation::Delete { key: key.clone() });
                    }
                }
                for (key, new_projection) in &new_entries {
                    if old_entries.get(key) != Some(new_projection) {
                        projected_bytes = projected_bytes.saturating_add(new_projection.len());
                        operation.physical_upserts = operation.physical_upserts.saturating_add(1);
                        operation.projected_bytes = operation
                            .projected_bytes
                            .saturating_add(new_projection.len() as u64);
                        delta.insert(
                            key.clone(),
                            Mutation::Upsert {
                                key: key.clone(),
                                val: new_projection.clone(),
                            },
                        );
                    }
                }
            }
            if delta.len() > definition.limits().max_derived_mutations_per_transaction {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "derived_mutations_per_transaction",
                    limit: definition.limits().max_derived_mutations_per_transaction,
                    actual: delta.len(),
                });
            }
            if projected_bytes > definition.limits().max_projected_bytes_per_transaction {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "projected_bytes_per_transaction",
                    limit: definition.limits().max_projected_bytes_per_transaction,
                    actual: projected_bytes,
                });
            }
            let next_tree = if delta.is_empty() {
                index_version.tree.clone()
            } else {
                operation.changed_indexes = operation.changed_indexes.saturating_add(1);
                self.prolly
                    .batch(&index_version.tree, delta.into_values().collect())?
            };
            let next_version = MapVersionId::for_tree(&next_tree)?;
            let next_checkpoint = IndexCheckpoint {
                source_map_id: self.source_map_id.clone(),
                source_version: source_version.clone(),
                index_name: checkpoint.index_name.clone(),
                generation: checkpoint.generation,
                definition_fingerprint: checkpoint.definition_fingerprint.clone(),
                index_map_id: checkpoint.index_map_id.clone(),
                index_version: next_version,
            };
            planned_indexes.push((checkpoint.clone(), next_tree));
            new_checkpoints.push(next_checkpoint);
        }

        let catalog = current.catalog.as_ref().ok_or_else(|| {
            Error::InvalidVersionedMap("active indexes require a catalog version".to_string())
        })?;
        let mut catalog_mutations = new_checkpoints
            .iter()
            .map(|checkpoint| {
                Ok(Mutation::Upsert {
                    key: catalog_checkpoint_key(
                        &source_version,
                        &checkpoint.index_name,
                        checkpoint.generation,
                    ),
                    val: checkpoint.to_bytes()?,
                })
            })
            .collect::<Result<Vec<_>, Error>>()?;
        catalog_mutations.push(Mutation::Upsert {
            key: catalog_current_key(),
            val: IndexedHeadRecord {
                source_version: source_version.clone(),
                indexes: new_checkpoints.clone(),
            }
            .to_bytes()?,
        });
        let catalog_candidate = self.prolly.batch(&catalog.tree, catalog_mutations)?;
        let permit_fingerprint = self
            .load_control()?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "active catalog selection has no control root".to_string(),
                )
            })?
            .fingerprint()?;
        let catalog_id = catalog_map_id(&self.source_map_id);

        let publication = self.prolly.versioned_maps_transaction(|maps| {
            let source_permit =
                IndexMaintenancePermit::new(self.source_map_id.clone(), permit_fingerprint.clone());
            let source_update = maps.publish_tree_index_maintenance(
                &source_permit,
                Some(&current.source.id),
                &source_candidate,
            )?;
            let source_published = require_non_conflict(source_update, self.source().head_name())?;

            for (checkpoint, next_tree) in &planned_indexes {
                let permit = IndexMaintenancePermit::new(
                    checkpoint.index_map_id.clone(),
                    permit_fingerprint.clone(),
                );
                let update = maps.publish_tree_index_maintenance(
                    &permit,
                    Some(&checkpoint.index_version),
                    next_tree,
                )?;
                require_non_conflict(update, &checkpoint.index_map_id)?;
            }

            let catalog_permit =
                IndexMaintenancePermit::new(catalog_id.clone(), permit_fingerprint.clone());
            let catalog_update = maps.publish_tree_index_maintenance(
                &catalog_permit,
                Some(&catalog.id),
                &catalog_candidate,
            )?;
            let catalog_published = require_non_conflict(catalog_update, &catalog_id)?;
            Ok((source_published, catalog_published))
        });

        match publication {
            Ok((source, catalog)) => {
                self.record_operation_metrics(&operation);
                Ok(IndexedMapUpdate::Applied {
                    previous: Some(current.source.id),
                    current: IndexedVersion {
                        source,
                        catalog: Some(catalog),
                        indexes: new_checkpoints,
                    },
                })
            }
            Err(Error::TransactionConflict(_)) => Ok(IndexedMapUpdate::Conflict {
                current: self.current_indexed_version()?,
            }),
            Err(error) => Err(error),
        }
    }

    fn projected_entries(
        &self,
        definition: &super::definition::SecondaryIndex,
        primary_key: &[u8],
        source_value: Option<&[u8]>,
    ) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, Error> {
        let Some(source_value) = source_value else {
            return Ok(BTreeMap::new());
        };
        definition
            .extract(primary_key, source_value)?
            .into_iter()
            .map(|entry| {
                let key = physical_index_key(&entry.term, primary_key)?;
                let value = match definition.projection() {
                    IndexProjection::KeysOnly => IndexValue::KeysOnly,
                    IndexProjection::Include => IndexValue::Included(
                        entry.projection.expect("Include emissions are validated"),
                    ),
                    IndexProjection::All => IndexValue::FullSource(source_value.to_vec()),
                }
                .to_bytes()?;
                Ok((key, value))
            })
            .collect()
    }

    fn current_indexed_version(&self) -> Result<Option<IndexedVersion>, Error> {
        let source = self.source().head()?;
        let catalog_map = self
            .prolly
            .versioned_map(catalog_map_id(&self.source_map_id));
        let catalog = catalog_map.head()?;
        match (source, catalog) {
            (None, None) => Ok(None),
            (Some(source), None) => Ok(Some(IndexedVersion {
                source,
                catalog: None,
                indexes: Vec::new(),
            })),
            (Some(source), Some(catalog)) => {
                let snapshot = catalog_map.snapshot_at(&catalog.id)?.ok_or_else(|| {
                    Error::InvalidVersionedMap("catalog immutable root is missing".to_string())
                })?;
                let indexes = snapshot
                    .get(&catalog_current_key())?
                    .map(|bytes| IndexedHeadRecord::from_bytes(&bytes))
                    .transpose()?
                    .map(|head| head.indexes)
                    .unwrap_or_default();
                Ok(Some(IndexedVersion {
                    source,
                    catalog: Some(catalog),
                    indexes,
                }))
            }
            (None, Some(_)) => Err(Error::InvalidVersionedMap(
                "catalog exists without a source head".to_string(),
            )),
        }
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

impl<S> IndexedMap<'_, S>
where
    S: Store + ManifestStore + ManifestStoreScan + TransactionalStore + Clone + Send + Sync,
{
    /// Retain the newest source versions and their complete transitive index closure.
    ///
    /// `count == 0` still retains the current source. This removes named roots
    /// and catalog checkpoint records only; content-addressed nodes require a
    /// separate store-global GC plan/sweep.
    pub fn keep_last(&self, count: usize) -> Result<IndexedRetentionResult, Error> {
        let mut last_conflict = None;
        for _ in 0..8 {
            match self.keep_last_once(count) {
                Err(Error::TransactionConflict(conflict)) => last_conflict = Some(*conflict),
                result => return result,
            }
        }
        Err(Error::transaction_conflict(last_conflict.expect(
            "indexed retention retry exhaustion follows a transaction conflict",
        )))
    }

    fn keep_last_once(&self, count: usize) -> Result<IndexedRetentionResult, Error> {
        let source_map = self.source();
        let source_head = source_map.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("indexed retention requires a source head".to_string())
        })?;
        let source_versions = source_map.versions()?;
        let mut retained_source_ids = source_versions
            .iter()
            .take(count)
            .map(|version| version.id.clone())
            .collect::<HashSet<_>>();
        retained_source_ids.insert(source_head.id.clone());
        let mut retained_source_versions = source_versions
            .iter()
            .filter(|version| retained_source_ids.contains(&version.id))
            .map(|version| version.id.clone())
            .collect::<Vec<_>>();
        let mut removed_source_versions = source_versions
            .iter()
            .filter(|version| !retained_source_ids.contains(&version.id))
            .map(|version| version.id.clone())
            .collect::<Vec<_>>();
        sort_version_ids(&mut retained_source_versions);
        sort_version_ids(&mut removed_source_versions);

        let catalog_id = catalog_map_id(&self.source_map_id);
        let catalog_map = self.prolly.versioned_map(&catalog_id);
        let catalog_head = catalog_map.head()?.ok_or_else(|| {
            Error::InvalidVersionedMap("indexed retention requires a catalog head".to_string())
        })?;
        let catalog = catalog_map.snapshot_at(&catalog_head.id)?.ok_or_else(|| {
            Error::InvalidVersionedMap("indexed retention catalog root is missing".to_string())
        })?;
        validate_catalog_format(&catalog)?;
        let current = catalog
            .get(&catalog_current_key())?
            .ok_or_else(|| {
                Error::InvalidVersionedMap(
                    "indexed retention catalog has no current selection".to_string(),
                )
            })
            .and_then(|bytes| IndexedHeadRecord::from_bytes(&bytes))?;
        if current.source_version != source_head.id {
            return Err(Error::transaction_conflict(TransactionConflict::new(
                source_map.head_name().to_vec(),
                None,
                None,
            )));
        }

        let checkpoint_entries = catalog
            .prefix(&catalog_checkpoints_prefix())?
            .map(|entry| {
                let (key, value) = entry?;
                Ok((key, IndexCheckpoint::from_bytes(&value)?))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        let mut catalog_mutations = Vec::new();
        let mut retained_checkpoints = Vec::new();
        for (key, checkpoint) in &checkpoint_entries {
            if retained_source_ids.contains(&checkpoint.source_version) {
                retained_checkpoints.push(checkpoint.clone());
            } else {
                catalog_mutations.push(Mutation::Delete { key: key.clone() });
            }
        }
        for active in &current.indexes {
            if !retained_checkpoints.iter().any(|checkpoint| {
                checkpoint.source_version == active.source_version
                    && checkpoint.index_name == active.index_name
                    && checkpoint.generation == active.generation
            }) {
                return Err(Error::IndexCheckpointMismatch {
                    name: active.index_name.clone(),
                    source_version: current.source_version.clone(),
                    reason: "retention closure is missing a current checkpoint record".to_string(),
                });
            }
        }
        let catalog_candidate = if catalog_mutations.is_empty() {
            catalog.tree().clone()
        } else {
            self.prolly.batch(catalog.tree(), catalog_mutations)?
        };
        let catalog_candidate_id = MapVersionId::for_tree(&catalog_candidate)?;

        let mut referenced_by_map = BTreeMap::<Vec<u8>, HashSet<MapVersionId>>::new();
        let mut retained_index_set = HashSet::new();
        let mut all_index_map_ids = BTreeSet::new();
        for (_, checkpoint) in &checkpoint_entries {
            all_index_map_ids.insert(checkpoint.index_map_id.clone());
        }
        for checkpoint in &retained_checkpoints {
            retained_index_set.insert(checkpoint.index_version.clone());
            referenced_by_map
                .entry(checkpoint.index_map_id.clone())
                .or_default()
                .insert(checkpoint.index_version.clone());
        }
        let active_index_map_ids = current
            .indexes
            .iter()
            .map(|checkpoint| checkpoint.index_map_id.clone())
            .collect::<HashSet<_>>();

        let named_roots = self.prolly.list_named_root_manifests()?;
        let mut roots_to_remove = BTreeSet::<Vec<u8>>::new();
        for id in &removed_source_versions {
            let mut name = source_map.versions_prefix().to_vec();
            name.extend_from_slice(id.as_cid().as_bytes());
            roots_to_remove.insert(name);
        }

        let mut removed_catalog_versions = Vec::new();
        for version in catalog_map.versions()? {
            if version.id != catalog_candidate_id {
                let mut name = catalog_map.versions_prefix().to_vec();
                name.extend_from_slice(version.id.as_cid().as_bytes());
                roots_to_remove.insert(name);
                removed_catalog_versions.push(version.id);
            }
        }
        sort_version_ids(&mut removed_catalog_versions);

        let mut removed_index_versions = Vec::new();
        for map_id in &all_index_map_ids {
            let map = self.prolly.versioned_map(map_id);
            let versions_prefix = map.versions_prefix();
            let referenced = referenced_by_map.get(map_id);
            for root in named_roots
                .iter()
                .filter(|root| root.name.starts_with(versions_prefix))
            {
                let suffix = &root.name[versions_prefix.len()..];
                let bytes: [u8; 32] = suffix.try_into().map_err(|_| {
                    Error::InvalidVersionedMap(
                        "hidden index version root has an invalid name".to_string(),
                    )
                })?;
                let version = MapVersionId::from_cid(Cid(bytes));
                if !referenced.is_some_and(|versions| versions.contains(&version)) {
                    roots_to_remove.insert(root.name.clone());
                    removed_index_versions.push(version);
                }
            }
            if !active_index_map_ids.contains(map_id) {
                roots_to_remove.insert(map.head_name().to_vec());
            }
        }
        removed_index_versions
            .sort_by(|left, right| left.as_cid().as_bytes().cmp(right.as_cid().as_bytes()));
        removed_index_versions.dedup();
        let mut retained_index_versions = retained_index_set.into_iter().collect::<Vec<_>>();
        sort_version_ids(&mut retained_index_versions);

        let control_fingerprint = self
            .load_control()?
            .map(|control| control.fingerprint())
            .transpose()?
            .unwrap_or_else(|| Cid::from_bytes(b"inactive-indexed-retention"));
        let removed_named_roots = roots_to_remove.into_iter().collect::<Vec<_>>();
        self.prolly.versioned_maps_transaction(|maps| {
            let source_permit = IndexMaintenancePermit::new(
                self.source_map_id.clone(),
                control_fingerprint.clone(),
            );
            let source_update = maps.publish_tree_index_maintenance(
                &source_permit,
                Some(&source_head.id),
                &source_head.tree,
            )?;
            require_non_conflict(source_update, source_map.head_name())?;
            let catalog_permit =
                IndexMaintenancePermit::new(catalog_id.clone(), control_fingerprint.clone());
            let catalog_update = maps.publish_tree_index_maintenance(
                &catalog_permit,
                Some(&catalog_head.id),
                &catalog_candidate,
            )?;
            require_non_conflict(catalog_update, &catalog_id)?;
            for name in &removed_named_roots {
                if maps.raw_transaction().load_named_root(name)?.is_some() {
                    maps.raw_transaction().delete_named_root(name)?;
                }
            }
            Ok(())
        })?;
        self.metrics.retained_roots.fetch_add(
            retained_source_versions
                .len()
                .saturating_add(retained_index_versions.len()) as u64,
            Ordering::Relaxed,
        );
        Ok(IndexedRetentionResult {
            retained_source_versions,
            removed_source_versions,
            retained_index_versions,
            removed_index_versions,
            removed_catalog_versions,
            removed_checkpoint_records: checkpoint_entries
                .len()
                .saturating_sub(retained_checkpoints.len()),
            removed_named_roots,
        })
    }
}

impl<S> IndexedMap<'_, S>
where
    S: Store + NodeStoreScan + ManifestStore + ManifestStoreScan + TransactionalStore,
{
    /// Plan node GC from every remaining named root in the shared store.
    pub fn plan_indexed_gc(&self) -> Result<GcPlan, Error> {
        self.prolly
            .plan_store_gc_for_retention(&NamedRootRetention::all())
    }
}

fn sort_version_ids(ids: &mut [MapVersionId]) {
    ids.sort_by(|left, right| left.as_cid().as_bytes().cmp(right.as_cid().as_bytes()));
}

pub(crate) fn require_non_conflict(
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
        | VersionedMapUpdate::Conflict { current: Some(_) } => Err(Error::transaction_conflict(
            TransactionConflict::new(conflict_name.to_vec(), None, None),
        )),
    }
}

pub(crate) fn validate_catalog_format<S>(snapshot: &MapSnapshot<'_, S>) -> Result<(), Error>
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
