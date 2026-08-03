use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use super::super::error::{Error, Mutation};
use super::super::manifest::{AsyncManifestStore, NamedRootUpdate};
use super::super::store::AsyncStore;
use super::super::transaction::{AsyncTransactionalStore, TransactionUpdate};
use super::super::tree::Tree;
use super::super::versioned_map::{MapVersion, MapVersionId};
use super::super::AsyncProlly;
use super::budget::{BudgetCounter, MaintenanceBudget, MutationBudget};
use super::coordinator::{
    ActiveIndexHealth, IndexBuildResult, IndexVerification, IndexedMapEditor, IndexedMapHealth,
    IndexedMapMetricsSnapshot, IndexedMapUpdate, IndexedRetentionResult, IndexedVersion,
};
use super::definition::{IndexProjection, SecondaryIndex, SecondaryIndexRegistry};
use super::publication::AsyncIndexedStore;
use super::state::{
    indexed_collection_root_name, CollectionIndexPolicy, IndexDescriptor, IndexSnapshotRef,
    IndexedCollectionState, IndexedSnapshotRecord, SnapshotPin, SourceSnapshotRef,
};
use super::storage::{physical_index_key, IndexValue};
use super::workspace::IndexBuildWorkspace;

#[derive(Default)]
struct AsyncIndexedMapMetrics {
    normalized_source_mutations: AtomicU64,
    records_extracted: AtomicU64,
    terms_emitted: AtomicU64,
    projected_bytes: AtomicU64,
    physical_upserts: AtomicU64,
    physical_deletes: AtomicU64,
    unchanged_emissions_skipped: AtomicU64,
    retries: AtomicU64,
    build_attempts: AtomicU64,
    verification_outcomes: AtomicU64,
    retained_roots: AtomicU64,
}

pub(crate) struct AsyncLoadedIndexedState {
    pub(crate) tree: Tree,
    pub(crate) state: IndexedCollectionState,
}

/// Native asynchronous coordinator for one canonical source map and its indexes.
///
/// The coordinator uses [`AsyncStore`] for all node I/O and
/// [`AsyncTransactionalStore`] for the sole visibility transition. It never
/// routes remote work through the synchronous IndexedMap facade.
pub struct AsyncIndexedMap<'a, S: AsyncIndexedStore> {
    pub(crate) prolly: &'a AsyncProlly<S>,
    pub(crate) source_map_id: Vec<u8>,
    pub(crate) registry: SecondaryIndexRegistry,
    runtime_overrides: RwLock<BTreeMap<(Vec<u8>, super::super::cid::Cid), SecondaryIndex>>,
    metrics: Arc<AsyncIndexedMapMetrics>,
}

/// Async read view over the current canonical source tree.
pub struct AsyncIndexedSourceView<'map, 'engine, S: AsyncIndexedStore> {
    map: &'map AsyncIndexedMap<'engine, S>,
}

impl<S> AsyncIndexedSourceView<'_, '_, S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Load current source head metadata.
    pub async fn head(&self) -> Result<Option<MapVersion>, Error> {
        let loaded = self.map.load_state().await?;
        Ok(Some(map_version(
            loaded.state.head_snapshot()?.source.tree.clone(),
            true,
        )?))
    }

    /// Read one source value from the current canonical snapshot.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        self.map.get(key).await
    }
}

impl<'a, S> AsyncIndexedMap<'a, S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    pub(crate) async fn open(
        prolly: &'a AsyncProlly<S>,
        source_map_id: impl AsRef<[u8]>,
        registry: SecondaryIndexRegistry,
    ) -> Result<Self, Error> {
        if !AsyncTransactionalStore::supports_transactions(prolly.store()) {
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
            metrics: Arc::new(AsyncIndexedMapMetrics::default()),
        };
        indexed.initialize_or_load().await?;
        Ok(indexed)
    }

    /// Application identifier for this indexed collection.
    pub fn id(&self) -> &[u8] {
        &self.source_map_id
    }

    /// Runtime extractor registry used to interpret persisted descriptors.
    pub fn registry(&self) -> &SecondaryIndexRegistry {
        &self.registry
    }

    /// Open an async view over the canonical source tree.
    pub fn source(&self) -> AsyncIndexedSourceView<'_, 'a, S> {
        AsyncIndexedSourceView { map: self }
    }

    /// Read one source value from the current canonical snapshot.
    pub async fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let loaded = self.load_state().await?;
        self.prolly
            .get(&loaded.state.head_snapshot()?.source.tree, key)
            .await
    }

    /// Inspect the current canonical source/index closure.
    pub async fn health(&self) -> Result<IndexedMapHealth, Error> {
        let loaded = self.load_state().await?;
        loaded.state.validate_closure()?;
        let head = loaded.state.head_snapshot()?;
        let source_version = MapVersionId::for_tree(&head.source.tree)?;
        let state_version = MapVersionId::for_tree(&loaded.tree)?;
        let mut active_indexes = Vec::with_capacity(loaded.state.active.len());
        for (name, fingerprint) in &loaded.state.active {
            let descriptor = loaded
                .state
                .descriptors
                .get(&(name.clone(), fingerprint.clone()))
                .ok_or_else(|| {
                    Error::InvalidVersionedMap("active canonical descriptor is missing".to_string())
                })?;
            let selected = head
                .indexes
                .iter()
                .find(|index| &index.name == name)
                .ok_or_else(|| {
                    Error::InvalidVersionedMap("active canonical index tree is missing".to_string())
                })?;
            active_indexes.push(ActiveIndexHealth {
                name: name.clone(),
                generation: descriptor.generation,
                fingerprint: fingerprint.clone(),
                projection: descriptor.projection,
                index_version: MapVersionId::for_tree(&selected.tree)?,
            });
        }
        Ok(IndexedMapHealth {
            source_map_id: self.source_map_id.clone(),
            source_version: Some(source_version),
            state_version: Some(state_version),
            active_indexes,
            closure_valid: true,
            retained_snapshots: loaded.state.snapshots.len(),
            durable_pins: loaded.state.pins.len(),
        })
    }

    /// Snapshot coordinator work counters.
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
            retries: self.metrics.retries.load(Ordering::Relaxed),
            build_attempts: self.metrics.build_attempts.load(Ordering::Relaxed),
            verification_outcomes: self.metrics.verification_outcomes.load(Ordering::Relaxed),
            retained_roots: self.metrics.retained_roots.load(Ordering::Relaxed),
        }
    }

    pub(crate) fn runtime_definition(&self, name: &[u8]) -> Option<SecondaryIndex> {
        let overrides = self
            .runtime_overrides
            .read()
            .expect("secondary-index runtime override lock poisoned");
        overrides
            .iter()
            .filter(|((candidate, _), _)| candidate.as_slice() == name)
            .map(|(_, definition)| definition)
            .max_by_key(|definition| definition.generation())
            .cloned()
            .or_else(|| self.registry.get(name).cloned())
    }

    pub(crate) fn runtime_definition_for_descriptor(
        &self,
        descriptor: &IndexDescriptor,
    ) -> Result<Option<SecondaryIndex>, Error> {
        if let Some(definition) = self
            .runtime_overrides
            .read()
            .expect("secondary-index runtime override lock poisoned")
            .get(&(descriptor.name.clone(), descriptor.fingerprint.clone()))
            .cloned()
        {
            return Ok(Some(definition));
        }
        for definition in self.registry.definitions_for_name(&descriptor.name) {
            let runtime = IndexDescriptor::from_runtime(&self.source_map_id, &definition)?;
            if runtime.fingerprint == descriptor.fingerprint {
                return Ok(Some(definition));
            }
        }
        Ok(None)
    }

    fn root_name(&self) -> Result<Vec<u8>, Error> {
        indexed_collection_root_name(&self.source_map_id)
    }

    async fn commit_collection_root(
        &self,
        expected: Option<&Tree>,
        candidate: &Tree,
    ) -> Result<bool, Error> {
        let transaction = self.prolly.begin_transaction()?;
        match transaction
            .compare_and_swap_named_root(&self.root_name()?, expected, Some(candidate))
            .await?
        {
            NamedRootUpdate::Conflict { .. } => {
                transaction.rollback();
                Ok(false)
            }
            NamedRootUpdate::Applied => match transaction.commit().await? {
                TransactionUpdate::Applied { .. } => Ok(true),
                TransactionUpdate::Conflict(_) => Ok(false),
            },
        }
    }

    async fn initialize_or_load(&self) -> Result<(), Error> {
        if self
            .prolly
            .load_named_root(&self.root_name()?)
            .await?
            .is_some()
        {
            self.load_state().await?;
            return Ok(());
        }
        if self
            .prolly
            .versioned_map(&self.source_map_id)
            .head()
            .await?
            .is_some()
        {
            return Err(Error::IndexFormatUnsupported);
        }
        let source = self.prolly.create();
        let state = IndexedCollectionState::new(
            self.source_map_id.clone(),
            CollectionIndexPolicy::default(),
            IndexedSnapshotRecord {
                source_map_id: self.source_map_id.clone(),
                parent: None,
                source: SourceSnapshotRef {
                    tree: source.clone(),
                    entry_count: 0,
                },
                indexes: Vec::new(),
            },
        )?;
        let state_tree = state.to_async_tree(self.prolly).await?;
        self.prolly
            .store()
            .confirm_async_indexed_publication(&[&source, &state_tree])
            .await?;
        if self.commit_collection_root(None, &state_tree).await? {
            return Ok(());
        }
        self.load_state().await?;
        Ok(())
    }

    pub(crate) async fn load_state(&self) -> Result<AsyncLoadedIndexedState, Error> {
        let tree = self
            .prolly
            .load_named_root(&self.root_name()?)
            .await?
            .ok_or_else(|| {
                Error::InvalidVersionedMap("indexed collection state root is absent".to_string())
            })?;
        let state = IndexedCollectionState::from_async_tree(self.prolly, &tree).await?;
        if state.source_map_id != self.source_map_id {
            return Err(Error::InvalidVersionedMap(
                "indexed collection state belongs to another source".to_string(),
            ));
        }
        Ok(AsyncLoadedIndexedState { tree, state })
    }

    async fn publish(
        &self,
        loaded: &AsyncLoadedIndexedState,
        candidate: &mut IndexedCollectionState,
    ) -> Result<bool, Error> {
        candidate.enforce_policy_limits()?;
        let candidate_tree = candidate
            .to_async_tree_from(self.prolly, &loaded.state, &loaded.tree)
            .await?;
        let mut referenced = vec![&candidate_tree];
        let head = candidate.head_snapshot()?;
        referenced.push(&head.source.tree);
        referenced.extend(head.indexes.iter().map(|index| &index.tree));
        self.prolly
            .store()
            .confirm_async_indexed_publication(&referenced)
            .await?;
        self.commit_collection_root(Some(&loaded.tree), &candidate_tree)
            .await
    }

    pub(crate) fn current_version(
        &self,
        loaded: &AsyncLoadedIndexedState,
    ) -> Result<IndexedVersion, Error> {
        let head = loaded.state.head_snapshot()?;
        Ok(IndexedVersion {
            source: map_version(head.source.tree.clone(), true)?,
            state: map_version(loaded.tree.clone(), true)?,
            indexes: head.indexes.clone(),
        })
    }

    /// Ensure one registered index exists for the current source snapshot.
    pub async fn ensure_index(&self, name: impl AsRef<[u8]>) -> Result<IndexBuildResult, Error> {
        self.ensure_index_with_budget(name, &MaintenanceBudget::default())
            .await
    }

    /// Ensure one registered index under an explicit maintenance budget.
    pub async fn ensure_index_with_budget(
        &self,
        name: impl AsRef<[u8]>,
        budget: &MaintenanceBudget,
    ) -> Result<IndexBuildResult, Error> {
        budget.validate()?;
        let name = name.as_ref();
        let definition =
            self.runtime_definition(name)
                .ok_or_else(|| Error::InvalidIndexDefinition {
                    reason: "runtime index definition is not registered".to_string(),
                })?;
        let descriptor = IndexDescriptor::from_runtime(&self.source_map_id, &definition)?;
        for attempt in 1..=budget.max_cas_attempts {
            self.metrics.build_attempts.fetch_add(1, Ordering::Relaxed);
            let loaded = self.load_state().await?;
            let head = loaded.state.head_snapshot()?;
            if let Some(active) = loaded.state.active.get(name) {
                if active != &descriptor.fingerprint {
                    return Err(Error::IndexOperationUnsupported {
                        operation: "ensure_index cannot replace an active descriptor",
                    });
                }
                let selected = head
                    .indexes
                    .iter()
                    .find(|index| index.name == name)
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap(
                            "active index is absent from head snapshot".to_string(),
                        )
                    })?;
                return Ok(IndexBuildResult {
                    source_version: MapVersionId::for_tree(&head.source.tree)?,
                    index_version: MapVersionId::for_tree(&selected.tree)?,
                    state_version: MapVersionId::for_tree(&loaded.tree)?,
                    generation: descriptor.generation,
                    entries: usize::try_from(selected.entry_count).unwrap_or(usize::MAX),
                    attempts: attempt - 1,
                    activated: false,
                });
            }
            if loaded.state.active.len() >= loaded.state.policy.max_active_indexes {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "active_indexes",
                    limit: loaded.state.policy.max_active_indexes,
                    actual: loaded.state.active.len().saturating_add(1),
                });
            }
            let (index_tree, entries) = self
                .build_index_tree(&head.source.tree, &definition, budget)
                .await?;
            let mut candidate = loaded.state.clone();
            candidate.descriptors.insert(
                (name.to_vec(), descriptor.fingerprint.clone()),
                descriptor.clone(),
            );
            candidate
                .active
                .insert(name.to_vec(), descriptor.fingerprint.clone());
            let mut snapshot = head.clone();
            snapshot.parent = Some(loaded.state.head.clone());
            snapshot.indexes.push(IndexSnapshotRef {
                name: name.to_vec(),
                descriptor_fingerprint: descriptor.fingerprint.clone(),
                tree: index_tree.clone(),
                entry_count: entries as u64,
            });
            snapshot
                .indexes
                .sort_by(|left, right| left.name.cmp(&right.name));
            let snapshot_id = snapshot.id()?;
            candidate.snapshots.insert(snapshot_id.clone(), snapshot);
            candidate.head = snapshot_id;
            if self.publish(&loaded, &mut candidate).await? {
                let published = self.load_state().await?;
                return Ok(IndexBuildResult {
                    source_version: MapVersionId::for_tree(&head.source.tree)?,
                    index_version: MapVersionId::for_tree(&index_tree)?,
                    state_version: MapVersionId::for_tree(&published.tree)?,
                    generation: descriptor.generation,
                    entries,
                    attempts: attempt,
                    activated: true,
                });
            }
            self.metrics.retries.fetch_add(1, Ordering::Relaxed);
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: name.to_vec(),
            attempts: budget.max_cas_attempts,
        })
    }

    /// Atomically apply source mutations and all active secondary-index deltas.
    pub async fn apply(&self, mutations: Vec<Mutation>) -> Result<IndexedVersion, Error> {
        self.apply_with_budget(mutations, &MutationBudget::default())
            .await
    }

    /// Apply source mutations under an explicit finite budget.
    pub async fn apply_with_budget(
        &self,
        mutations: Vec<Mutation>,
        budget: &MutationBudget,
    ) -> Result<IndexedVersion, Error> {
        match self
            .apply_with_budget_internal(mutations, budget, None)
            .await?
        {
            IndexedMapUpdate::Applied { current, .. }
            | IndexedMapUpdate::Unchanged {
                current: Some(current),
            } => Ok(current),
            IndexedMapUpdate::Unchanged { current: None } => Err(Error::InvalidVersionedMap(
                "initialized indexed map has no current version".to_string(),
            )),
            IndexedMapUpdate::Conflict { .. } => Err(Error::InvalidVersionedMap(
                "unconditional indexed mutation returned a conflict".to_string(),
            )),
        }
    }

    async fn apply_with_budget_internal(
        &self,
        mutations: Vec<Mutation>,
        budget: &MutationBudget,
        expected: Option<Option<&MapVersionId>>,
    ) -> Result<IndexedMapUpdate, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        if mutations.len() > budget.max_input_records {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "mutation_input_records",
                limit: budget.max_input_records,
                actual: mutations.len(),
            });
        }
        let input_bytes = mutations.iter().try_fold(0usize, |total, mutation| {
            let bytes = match mutation {
                Mutation::Upsert { key, val } => key.len().checked_add(val.len()),
                Mutation::Delete { key } => Some(key.len()),
            }
            .ok_or(Error::IndexResourceLimitExceeded {
                resource: "mutation_input_bytes",
                limit: budget.max_input_bytes,
                actual: usize::MAX,
            })?;
            total
                .checked_add(bytes)
                .ok_or(Error::IndexResourceLimitExceeded {
                    resource: "mutation_input_bytes",
                    limit: budget.max_input_bytes,
                    actual: usize::MAX,
                })
        })?;
        if input_bytes > budget.max_input_bytes {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "mutation_input_bytes",
                limit: budget.max_input_bytes,
                actual: input_bytes,
            });
        }
        let normalized = normalize_mutations(&mutations);
        for _ in 0..budget.max_cas_attempts {
            counter.check_elapsed("mutation_elapsed_millis", budget.max_elapsed)?;
            let loaded = self.load_state().await?;
            let head = loaded.state.head_snapshot()?;
            let previous = MapVersionId::for_tree(&head.source.tree)?;
            if let Some(expected) = expected {
                if expected != Some(&previous) {
                    return Ok(IndexedMapUpdate::Conflict {
                        current: Some(self.current_version(&loaded)?),
                    });
                }
            }

            let source_keys = normalized.keys().cloned().collect::<Vec<_>>();
            let old_values = self
                .prolly
                .get_many(&head.source.tree, &source_keys)
                .await?;
            let previous_values = source_keys
                .into_iter()
                .zip(old_values)
                .collect::<BTreeMap<_, _>>();
            let source_mutations = normalized.values().cloned().collect::<Vec<_>>();
            let source_tree = self
                .prolly
                .batch(&head.source.tree, source_mutations)
                .await?;
            if source_tree == head.source.tree {
                return Ok(IndexedMapUpdate::Unchanged {
                    current: Some(self.current_version(&loaded)?),
                });
            }
            let mut source_entry_delta = 0i64;
            for (key, mutation) in &normalized {
                let existed = previous_values.get(key).is_some_and(Option::is_some);
                match (existed, mutation) {
                    (false, Mutation::Upsert { .. }) => source_entry_delta += 1,
                    (true, Mutation::Delete { .. }) => source_entry_delta -= 1,
                    _ => {}
                }
            }

            let mut next_indexes = Vec::with_capacity(head.indexes.len());
            let mut derived_entries = 0usize;
            let mut derived_bytes = 0usize;
            let mut accounted_memory = 0usize;
            let mut records_extracted = 0u64;
            let mut terms_emitted = 0u64;
            let mut projected_bytes = 0u64;
            let mut physical_upserts = 0u64;
            let mut physical_deletes = 0u64;
            let mut unchanged_skipped = 0u64;
            for selected in &head.indexes {
                let descriptor = loaded
                    .state
                    .descriptors
                    .get(&(
                        selected.name.clone(),
                        selected.descriptor_fingerprint.clone(),
                    ))
                    .ok_or_else(|| {
                        Error::InvalidVersionedMap("selected descriptor is absent".to_string())
                    })?;
                let definition = self
                    .runtime_definition_for_descriptor(descriptor)?
                    .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                        name: descriptor.name.clone(),
                        generation: descriptor.generation,
                    })?;
                let mut delta = BTreeMap::new();
                let mut index_entry_delta = 0i64;
                for (key, mutation) in &normalized {
                    let old = previous_values.get(key).cloned().flatten();
                    let new = match mutation {
                        Mutation::Upsert { val, .. } => Some(val.as_slice()),
                        Mutation::Delete { .. } => None,
                    };
                    let old_entries = self.projected_entries(&definition, key, old.as_deref())?;
                    let new_entries = self.projected_entries(&definition, key, new)?;
                    records_extracted = records_extracted
                        .saturating_add(u64::from(old.is_some()))
                        .saturating_add(u64::from(new.is_some()));
                    terms_emitted = terms_emitted
                        .saturating_add(old_entries.len() as u64)
                        .saturating_add(new_entries.len() as u64);
                    projected_bytes = projected_bytes.saturating_add(
                        old_entries
                            .values()
                            .chain(new_entries.values())
                            .map(|value| value.len() as u64)
                            .sum::<u64>(),
                    );
                    for old_key in old_entries.keys() {
                        if !new_entries.contains_key(old_key) {
                            counter.charge(
                                "mutation_derived_entries",
                                &mut derived_entries,
                                1,
                                budget.max_derived_entries,
                            )?;
                            counter.charge(
                                "mutation_derived_bytes",
                                &mut derived_bytes,
                                old_key.len(),
                                budget.max_derived_bytes,
                            )?;
                            counter.charge(
                                "mutation_accounted_memory_bytes",
                                &mut accounted_memory,
                                old_key.len(),
                                budget.max_accounted_memory_bytes,
                            )?;
                            delta.insert(
                                old_key.clone(),
                                Mutation::Delete {
                                    key: old_key.clone(),
                                },
                            );
                            physical_deletes = physical_deletes.saturating_add(1);
                            index_entry_delta -= 1;
                        }
                    }
                    for (new_key, new_value) in new_entries {
                        if old_entries.get(&new_key) != Some(&new_value) {
                            if !old_entries.contains_key(&new_key) {
                                index_entry_delta += 1;
                            }
                            counter.charge(
                                "mutation_derived_entries",
                                &mut derived_entries,
                                1,
                                budget.max_derived_entries,
                            )?;
                            counter.charge(
                                "mutation_derived_bytes",
                                &mut derived_bytes,
                                new_key.len().saturating_add(new_value.len()),
                                budget.max_derived_bytes,
                            )?;
                            counter.charge(
                                "mutation_accounted_memory_bytes",
                                &mut accounted_memory,
                                new_key.len().saturating_add(new_value.len()),
                                budget.max_accounted_memory_bytes,
                            )?;
                            delta.insert(
                                new_key.clone(),
                                Mutation::Upsert {
                                    key: new_key,
                                    val: new_value,
                                },
                            );
                            physical_upserts = physical_upserts.saturating_add(1);
                        } else {
                            unchanged_skipped = unchanged_skipped.saturating_add(1);
                        }
                    }
                }
                let index_tree = self
                    .prolly
                    .batch(&selected.tree, delta.into_values().collect())
                    .await?;
                next_indexes.push(IndexSnapshotRef {
                    name: selected.name.clone(),
                    descriptor_fingerprint: selected.descriptor_fingerprint.clone(),
                    entry_count: apply_entry_count_delta(selected.entry_count, index_entry_delta)?,
                    tree: index_tree,
                });
            }
            let mut candidate = loaded.state.clone();
            let snapshot = IndexedSnapshotRecord {
                source_map_id: self.source_map_id.clone(),
                parent: Some(loaded.state.head.clone()),
                source: SourceSnapshotRef {
                    tree: source_tree,
                    entry_count: apply_entry_count_delta(
                        head.source.entry_count,
                        source_entry_delta,
                    )?,
                },
                indexes: next_indexes,
            };
            let snapshot_id = snapshot.id()?;
            candidate.snapshots.insert(snapshot_id.clone(), snapshot);
            candidate.head = snapshot_id;
            if self.publish(&loaded, &mut candidate).await? {
                let loaded = self.load_state().await?;
                self.metrics
                    .normalized_source_mutations
                    .fetch_add(normalized.len() as u64, Ordering::Relaxed);
                self.metrics
                    .records_extracted
                    .fetch_add(records_extracted, Ordering::Relaxed);
                self.metrics
                    .terms_emitted
                    .fetch_add(terms_emitted, Ordering::Relaxed);
                self.metrics
                    .projected_bytes
                    .fetch_add(projected_bytes, Ordering::Relaxed);
                self.metrics
                    .physical_upserts
                    .fetch_add(physical_upserts, Ordering::Relaxed);
                self.metrics
                    .physical_deletes
                    .fetch_add(physical_deletes, Ordering::Relaxed);
                self.metrics
                    .unchanged_emissions_skipped
                    .fetch_add(unchanged_skipped, Ordering::Relaxed);
                return Ok(IndexedMapUpdate::Applied {
                    previous: Some(previous),
                    current: self.current_version(&loaded)?,
                });
            }
            self.metrics.retries.fetch_add(1, Ordering::Relaxed);
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: b"mutation".to_vec(),
            attempts: budget.max_cas_attempts,
        })
    }

    /// Apply a batch only if `expected` is still the canonical source version.
    pub async fn apply_if(
        &self,
        expected: Option<&MapVersionId>,
        mutations: Vec<Mutation>,
    ) -> Result<IndexedMapUpdate, Error> {
        self.apply_with_budget_internal(mutations, &MutationBudget::default(), Some(expected))
            .await
    }

    /// Put one source value and update every active index atomically.
    pub async fn put(
        &self,
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
    ) -> Result<IndexedVersion, Error> {
        self.apply(vec![Mutation::Upsert {
            key: key.into(),
            val: value.into(),
        }])
        .await
    }

    /// Delete one source value and update every active index atomically.
    pub async fn delete(&self, key: impl Into<Vec<u8>>) -> Result<IndexedVersion, Error> {
        self.apply(vec![Mutation::Delete { key: key.into() }]).await
    }

    /// Collect and atomically apply several source edits.
    pub async fn edit(
        &self,
        edit: impl FnOnce(&mut IndexedMapEditor),
    ) -> Result<IndexedVersion, Error> {
        let mut editor = IndexedMapEditor::new();
        edit(&mut editor);
        self.apply(editor.into_mutations()).await
    }

    /// Verify one retained index against a canonical rebuild.
    pub async fn verify_index(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
    ) -> Result<IndexVerification, Error> {
        self.verify_index_with_budget(name, source_version, &MaintenanceBudget::default())
            .await
    }

    /// Verify one retained index under an explicit maintenance budget.
    pub async fn verify_index_with_budget(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
        budget: &MaintenanceBudget,
    ) -> Result<IndexVerification, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        let name = name.as_ref();
        let loaded = self.load_state().await?;
        let snapshot = find_snapshot(&loaded.state, source_version)?;
        let selected = snapshot
            .indexes
            .iter()
            .find(|index| index.name == name)
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.to_vec(),
                source_version: source_version.clone(),
            })?;
        let descriptor = loaded
            .state
            .descriptors
            .get(&(name.to_vec(), selected.descriptor_fingerprint.clone()))
            .ok_or_else(|| {
                Error::InvalidVersionedMap("verification descriptor is missing".to_string())
            })?;
        let definition = self
            .runtime_definition_for_descriptor(descriptor)?
            .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                name: name.to_vec(),
                generation: descriptor.generation,
            })?;
        let (expected, expected_entries) = self
            .build_index_tree(&snapshot.source.tree, &definition, budget)
            .await?;
        let actual_entries = count_entries_with_budget(
            self.prolly,
            &selected.tree,
            budget.max_derived_entries,
            budget,
            &counter,
        )
        .await?;
        let semantic_differences =
            count_differences_with_budget(self.prolly, &expected, &selected.tree, budget, &counter)
                .await?;
        self.metrics
            .verification_outcomes
            .fetch_add(1, Ordering::Relaxed);
        Ok(IndexVerification {
            name: name.to_vec(),
            source_version: source_version.clone(),
            expected_index_version: MapVersionId::for_tree(&expected)?,
            actual_index_version: MapVersionId::for_tree(&selected.tree)?,
            expected_entries,
            actual_entries,
            semantic_differences,
        })
    }

    /// Verify every index selected by one retained source version.
    pub async fn verify_all(
        &self,
        source_version: &MapVersionId,
    ) -> Result<Vec<IndexVerification>, Error> {
        self.verify_all_with_budget(source_version, &MaintenanceBudget::default())
            .await
    }

    /// Verify every selected index under a partitioned maintenance budget.
    pub async fn verify_all_with_budget(
        &self,
        source_version: &MapVersionId,
        budget: &MaintenanceBudget,
    ) -> Result<Vec<IndexVerification>, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        let loaded = self.load_state().await?;
        let snapshot = find_snapshot(&loaded.state, source_version)?;
        let per_index = partition_verification_budget(budget, snapshot.indexes.len())?;
        let names = snapshot
            .indexes
            .iter()
            .map(|index| index.name.clone())
            .collect::<Vec<_>>();
        let mut verifications = Vec::with_capacity(names.len());
        for name in names {
            counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
            verifications.push(
                self.verify_index_with_budget(&name, source_version, &per_index)
                    .await?,
            );
        }
        counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
        Ok(verifications)
    }

    /// Rebuild and atomically repair one index at the current head.
    pub async fn repair_index(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
    ) -> Result<IndexVerification, Error> {
        self.repair_index_with_budget(name, source_version, &MaintenanceBudget::default())
            .await
    }

    /// Rebuild and repair one index under an explicit maintenance budget.
    pub async fn repair_index_with_budget(
        &self,
        name: impl AsRef<[u8]>,
        source_version: &MapVersionId,
        budget: &MaintenanceBudget,
    ) -> Result<IndexVerification, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        let name = name.as_ref();
        for _ in 0..budget.max_cas_attempts {
            counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
            let loaded = self.load_state().await?;
            let snapshot = find_snapshot(&loaded.state, source_version)?.clone();
            if snapshot.id()? != loaded.state.head {
                return Err(Error::IndexOperationUnsupported {
                    operation: "repair_index currently requires the head snapshot",
                });
            }
            let selected = snapshot
                .indexes
                .iter()
                .find(|index| index.name == name)
                .ok_or_else(|| Error::IndexUnavailableAtVersion {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                })?;
            let descriptor = loaded
                .state
                .descriptors
                .get(&(name.to_vec(), selected.descriptor_fingerprint.clone()))
                .ok_or_else(|| {
                    Error::InvalidVersionedMap("repair descriptor is missing".to_string())
                })?;
            let definition = self
                .runtime_definition_for_descriptor(descriptor)?
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: name.to_vec(),
                    generation: descriptor.generation,
                })?;
            let (rebuilt, entries) = self
                .build_index_tree(&snapshot.source.tree, &definition, budget)
                .await?;
            let expected_version = MapVersionId::for_tree(&rebuilt)?;
            let actual_version = MapVersionId::for_tree(&selected.tree)?;
            let differences = count_differences_with_budget(
                self.prolly,
                &rebuilt,
                &selected.tree,
                budget,
                &counter,
            )
            .await?;
            if differences == 0 && expected_version == actual_version {
                return Ok(IndexVerification {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                    expected_index_version: expected_version,
                    actual_index_version: actual_version,
                    expected_entries: entries,
                    actual_entries: selected.entry_count as usize,
                    semantic_differences: 0,
                });
            }
            let descriptor_fingerprint = descriptor.fingerprint.clone();
            let mut next_snapshot = snapshot;
            let entry = next_snapshot
                .indexes
                .iter_mut()
                .find(|index| index.name == name)
                .expect("selected index exists");
            entry.tree = rebuilt;
            entry.entry_count = entries as u64;
            entry.descriptor_fingerprint = descriptor_fingerprint;
            next_snapshot.parent = Some(loaded.state.head.clone());
            let mut candidate = loaded.state.clone();
            let id = next_snapshot.id()?;
            candidate.snapshots.insert(id.clone(), next_snapshot);
            candidate.head = id;
            if self.publish(&loaded, &mut candidate).await? {
                return Ok(IndexVerification {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                    expected_index_version: expected_version.clone(),
                    actual_index_version: expected_version,
                    expected_entries: entries,
                    actual_entries: entries,
                    semantic_differences: 0,
                });
            }
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: name.to_vec(),
            attempts: budget.max_cas_attempts,
        })
    }

    /// Atomically replace an active index with a higher-generation definition.
    pub async fn replace_index(
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
        let budget = MaintenanceBudget::default();
        let descriptor = IndexDescriptor::from_runtime(&self.source_map_id, &new_definition)?;
        for attempt in 1..=budget.max_cas_attempts {
            let loaded = self.load_state().await?;
            let current_fingerprint =
                loaded
                    .state
                    .active
                    .get(name)
                    .ok_or_else(|| Error::IndexUnavailableAtVersion {
                        name: name.to_vec(),
                        source_version: MapVersionId::for_tree(
                            &loaded.state.head_snapshot().expect("validated").source.tree,
                        )
                        .expect("valid tree"),
                    })?;
            let current_descriptor = loaded
                .state
                .descriptors
                .get(&(name.to_vec(), current_fingerprint.clone()))
                .ok_or_else(|| {
                    Error::InvalidVersionedMap("active descriptor is missing".to_string())
                })?;
            if descriptor.generation <= current_descriptor.generation {
                return Err(Error::InvalidIndexDefinition {
                    reason: "replacement generation must be strictly greater".to_string(),
                });
            }
            let current_descriptor_fingerprint = current_descriptor.fingerprint.clone();
            let head = loaded.state.head_snapshot()?;
            let (tree, entries) = self
                .build_index_tree(&head.source.tree, &new_definition, &budget)
                .await?;
            let mut candidate = loaded.state.clone();
            candidate
                .retired
                .insert((name.to_vec(), current_descriptor_fingerprint));
            candidate.descriptors.insert(
                (name.to_vec(), descriptor.fingerprint.clone()),
                descriptor.clone(),
            );
            candidate
                .active
                .insert(name.to_vec(), descriptor.fingerprint.clone());
            let mut snapshot = head.clone();
            snapshot.parent = Some(loaded.state.head.clone());
            let source_version = MapVersionId::for_tree(&head.source.tree)?;
            let selected = snapshot
                .indexes
                .iter_mut()
                .find(|index| index.name == name)
                .ok_or_else(|| Error::IndexUnavailableAtVersion {
                    name: name.to_vec(),
                    source_version: source_version.clone(),
                })?;
            selected.tree = tree.clone();
            selected.entry_count = entries as u64;
            selected.descriptor_fingerprint = descriptor.fingerprint.clone();
            let id = snapshot.id()?;
            candidate.snapshots.insert(id.clone(), snapshot);
            candidate.head = id;
            if self.publish(&loaded, &mut candidate).await? {
                self.runtime_overrides
                    .write()
                    .expect("secondary-index runtime override lock poisoned")
                    .insert(
                        (name.to_vec(), descriptor.fingerprint.clone()),
                        new_definition.clone(),
                    );
                let published = self.load_state().await?;
                return Ok(IndexBuildResult {
                    source_version,
                    index_version: MapVersionId::for_tree(&tree)?,
                    state_version: MapVersionId::for_tree(&published.tree)?,
                    generation: descriptor.generation,
                    entries,
                    attempts: attempt,
                    activated: true,
                });
            }
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: name.to_vec(),
            attempts: budget.max_cas_attempts,
        })
    }

    /// Deactivate one index without changing the source snapshot.
    pub async fn deactivate_index(&self, name: impl AsRef<[u8]>) -> Result<IndexedVersion, Error> {
        let name = name.as_ref();
        let attempts = MaintenanceBudget::default().max_cas_attempts;
        for _ in 0..attempts {
            let loaded = self.load_state().await?;
            let Some(fingerprint) = loaded.state.active.get(name).cloned() else {
                return self.current_version(&loaded);
            };
            let mut candidate = loaded.state.clone();
            candidate.active.remove(name);
            candidate.retired.insert((name.to_vec(), fingerprint));
            let mut snapshot = loaded.state.head_snapshot()?.clone();
            snapshot.parent = Some(loaded.state.head.clone());
            snapshot.indexes.retain(|index| index.name != name);
            let id = snapshot.id()?;
            candidate.snapshots.insert(id.clone(), snapshot);
            candidate.head = id;
            if self.publish(&loaded, &mut candidate).await? {
                return self.current_version(&self.load_state().await?);
            }
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: name.to_vec(),
            attempts,
        })
    }

    /// Persist a durable retention pin for one retained source version.
    pub async fn retain_snapshot_pin(
        &self,
        pin_id: impl AsRef<[u8]>,
        source_version: &MapVersionId,
    ) -> Result<(), Error> {
        let pin_id = pin_id.as_ref();
        if pin_id.is_empty() || pin_id.len() > 256 {
            return Err(Error::InvalidIndexDefinition {
                reason: "snapshot pin ID must contain 1..=256 bytes".to_string(),
            });
        }
        let attempts = MaintenanceBudget::default().max_cas_attempts;
        for _ in 0..attempts {
            let loaded = self.load_state().await?;
            let snapshot_id = find_snapshot(&loaded.state, source_version)?.id()?;
            if loaded
                .state
                .pins
                .get(pin_id)
                .is_some_and(|pin| pin.snapshot == snapshot_id)
            {
                return Ok(());
            }
            let mut candidate = loaded.state.clone();
            candidate.pins.insert(
                pin_id.to_vec(),
                SnapshotPin {
                    snapshot: snapshot_id,
                },
            );
            if self.publish(&loaded, &mut candidate).await? {
                return Ok(());
            }
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: b"snapshot-pin".to_vec(),
            attempts,
        })
    }

    /// Release one durable async retention pin.
    pub async fn release_snapshot_pin(&self, pin_id: impl AsRef<[u8]>) -> Result<(), Error> {
        let pin_id = pin_id.as_ref();
        let attempts = MaintenanceBudget::default().max_cas_attempts;
        for _ in 0..attempts {
            let loaded = self.load_state().await?;
            if !loaded.state.pins.contains_key(pin_id) {
                return Ok(());
            }
            let mut candidate = loaded.state.clone();
            candidate.pins.remove(pin_id);
            if self.publish(&loaded, &mut candidate).await? {
                return Ok(());
            }
        }
        Err(Error::IndexBuildConflictLimitExceeded {
            name: b"snapshot-unpin".to_vec(),
            attempts,
        })
    }

    /// Retain the newest snapshots plus every durably pinned snapshot.
    pub async fn keep_last(&self, count: usize) -> Result<IndexedRetentionResult, Error> {
        let loaded = self.load_state().await?;
        let mut keep = BTreeSet::new();
        let mut cursor = Some(loaded.state.head.clone());
        for _ in 0..count.max(1) {
            let Some(id) = cursor.take() else {
                break;
            };
            keep.insert(id.clone());
            cursor = loaded
                .state
                .snapshots
                .get(&id)
                .and_then(|snapshot| snapshot.parent.clone());
        }
        keep.extend(loaded.state.pins.values().map(|pin| pin.snapshot.clone()));
        let mut candidate = loaded.state.clone();
        let removed = candidate
            .snapshots
            .keys()
            .filter(|id| !keep.contains(*id))
            .cloned()
            .collect::<Vec<_>>();
        let removed_source_versions = removed
            .iter()
            .filter_map(|id| candidate.snapshots.get(id))
            .map(|snapshot| MapVersionId::for_tree(&snapshot.source.tree))
            .collect::<Result<Vec<_>, _>>()?;
        candidate.snapshots.retain(|id, _| keep.contains(id));
        let referenced = candidate
            .snapshots
            .values()
            .flat_map(|snapshot| {
                snapshot
                    .indexes
                    .iter()
                    .map(|index| (index.name.clone(), index.descriptor_fingerprint.clone()))
            })
            .collect::<BTreeSet<_>>();
        candidate
            .descriptors
            .retain(|identity, _| referenced.contains(identity));
        candidate
            .retired
            .retain(|identity| referenced.contains(identity));
        if !self.publish(&loaded, &mut candidate).await? {
            return Err(Error::IndexBuildConflictLimitExceeded {
                name: b"retention".to_vec(),
                attempts: 1,
            });
        }
        let retained_source_versions = candidate
            .snapshots
            .values()
            .map(|snapshot| MapVersionId::for_tree(&snapshot.source.tree))
            .collect::<Result<Vec<_>, _>>()?;
        self.metrics
            .retained_roots
            .fetch_add(retained_source_versions.len() as u64, Ordering::Relaxed);
        Ok(IndexedRetentionResult {
            retained_source_versions,
            removed_source_versions,
            removed_snapshot_records: removed.len(),
            ..IndexedRetentionResult::default()
        })
    }

    fn projected_entries(
        &self,
        definition: &SecondaryIndex,
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

    async fn build_index_tree(
        &self,
        source_tree: &Tree,
        definition: &SecondaryIndex,
        budget: &MaintenanceBudget,
    ) -> Result<(Tree, usize), Error> {
        let counter = BudgetCounter::new();
        let mut workspace = IndexBuildWorkspace::new(budget)?;
        let mut source_entries = 0usize;
        let mut derived_entries = 0usize;
        let mut entries = self.prolly.range(source_tree, b"", None).await?;
        while let Some(entry) = entries.next().await {
            counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
            let (primary_key, source_value) = entry?;
            counter.charge(
                "maintenance_source_entries",
                &mut source_entries,
                1,
                budget.max_source_entries,
            )?;
            for emission in definition.extract(&primary_key, &source_value)? {
                counter.charge(
                    "maintenance_derived_entries",
                    &mut derived_entries,
                    1,
                    budget.max_derived_entries,
                )?;
                let key = physical_index_key(&emission.term, &primary_key)?;
                let value = match definition.projection() {
                    IndexProjection::KeysOnly => IndexValue::KeysOnly,
                    IndexProjection::Include => IndexValue::Included(
                        emission.projection.expect("validated Include projection"),
                    ),
                    IndexProjection::All => IndexValue::FullSource(source_value.clone()),
                }
                .to_bytes()?;
                workspace.add(key, value)?;
            }
        }
        workspace
            .finish_async(self.prolly.store().clone(), self.prolly.config().clone())
            .await
    }
}

impl<S> AsyncProlly<S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Open a native asynchronous indexed-map coordinator.
    pub async fn indexed_map(
        &self,
        source_map_id: impl AsRef<[u8]>,
        registry: SecondaryIndexRegistry,
    ) -> Result<AsyncIndexedMap<'_, S>, Error> {
        AsyncIndexedMap::open(self, source_map_id, registry).await
    }
}

fn map_version(tree: Tree, is_head: bool) -> Result<MapVersion, Error> {
    Ok(MapVersion {
        id: MapVersionId::for_tree(&tree)?,
        tree,
        created_at_millis: None,
        is_head,
    })
}

fn normalize_mutations(mutations: &[Mutation]) -> BTreeMap<Vec<u8>, Mutation> {
    let mut normalized = BTreeMap::new();
    for mutation in mutations {
        let key = match mutation {
            Mutation::Upsert { key, .. } | Mutation::Delete { key } => key.clone(),
        };
        normalized.insert(key, mutation.clone());
    }
    normalized
}

fn apply_entry_count_delta(current: u64, delta: i64) -> Result<u64, Error> {
    if delta >= 0 {
        current
            .checked_add(delta as u64)
            .ok_or_else(|| Error::InvalidVersionedMap("index entry count overflow".to_string()))
    } else {
        current
            .checked_sub(delta.unsigned_abs())
            .ok_or_else(|| Error::InvalidVersionedMap("index entry count underflow".to_string()))
    }
}

async fn count_entries_with_budget<S>(
    prolly: &AsyncProlly<S>,
    tree: &Tree,
    limit: usize,
    budget: &MaintenanceBudget,
    counter: &BudgetCounter,
) -> Result<usize, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut count = 0usize;
    let outcome = prolly
        .scan_range_until(tree, b"", None, |_| {
            count = count.saturating_add(1);
            if count > limit {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await?;
    counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
    if outcome.break_value.is_some() {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "maintenance_counted_entries",
            limit,
            actual: count,
        });
    }
    Ok(count)
}

async fn count_differences_with_budget<S>(
    prolly: &AsyncProlly<S>,
    expected: &Tree,
    actual: &Tree,
    budget: &MaintenanceBudget,
    counter: &BudgetCounter,
) -> Result<usize, Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    let mut differences = 0usize;
    let outcome = prolly
        .scan_diff_until(expected, actual, |_| {
            differences = differences.saturating_add(1);
            if differences > budget.max_verification_findings {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .await?;
    counter.check_elapsed("maintenance_elapsed_millis", budget.max_elapsed)?;
    if outcome.break_value.is_some() {
        return Err(Error::IndexResourceLimitExceeded {
            resource: "maintenance_verification_findings",
            limit: budget.max_verification_findings,
            actual: differences,
        });
    }
    Ok(differences)
}

fn partition_verification_budget(
    budget: &MaintenanceBudget,
    index_count: usize,
) -> Result<MaintenanceBudget, Error> {
    if index_count == 0 {
        return Ok(budget.clone());
    }
    let partition = |resource: &'static str, value: usize| {
        let per_index = value / index_count;
        if per_index == 0 {
            Err(Error::IndexResourceLimitExceeded {
                resource,
                limit: value,
                actual: index_count,
            })
        } else {
            Ok(per_index)
        }
    };
    Ok(MaintenanceBudget {
        max_source_entries: partition(
            "maintenance.verify_all_source_entries",
            budget.max_source_entries,
        )?,
        max_derived_entries: partition(
            "maintenance.verify_all_derived_entries",
            budget.max_derived_entries,
        )?,
        max_verification_findings: partition(
            "maintenance.verify_all_findings",
            budget.max_verification_findings,
        )?,
        max_accounted_memory_bytes: budget.max_accounted_memory_bytes,
        max_spill_bytes: partition("maintenance.verify_all_spill_bytes", budget.max_spill_bytes)?,
        max_spill_runs: partition("maintenance.verify_all_spill_runs", budget.max_spill_runs)?,
        max_merge_fan_in: budget.max_merge_fan_in,
        max_cas_attempts: budget.max_cas_attempts,
        max_elapsed: budget.max_elapsed,
    })
}

pub(crate) fn find_snapshot<'a>(
    state: &'a IndexedCollectionState,
    source_version: &MapVersionId,
) -> Result<&'a IndexedSnapshotRecord, Error> {
    let mut cursor = Some(&state.head);
    while let Some(id) = cursor {
        let Some(snapshot) = state.snapshots.get(id) else {
            break;
        };
        if MapVersionId::for_tree(&snapshot.source.tree)? == *source_version {
            return Ok(snapshot);
        }
        cursor = snapshot.parent.as_ref();
    }
    for snapshot in state.snapshots.values() {
        if MapVersionId::for_tree(&snapshot.source.tree)? == *source_version {
            return Ok(snapshot);
        }
    }
    Err(Error::InvalidVersionedMap(format!(
        "indexed source version {source_version} is not retained"
    )))
}
