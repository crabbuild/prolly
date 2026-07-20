use super::super::builder::BatchBuilder;
use super::super::cid::Cid;
use super::super::config::Config;
use super::super::error::{Error, Mutation as TreeMutation};
use super::super::read::{OwnedValueLease, ReadSession, ScanOutcome};
use super::super::splice::{splice_with_origin, SpliceStats};
use super::super::store::{NodePublication, PublicationOrigin, Store};
use super::super::Prolly;
use super::builder::{build_hierarchy, build_hierarchy_parallel, IndexedRecord};
use super::cache::{ContentCache, DEFAULT_PROXIMITY_CACHE_NODES};
use super::distance::{prepare_vector, query_score, score};
use super::mutation::{mutate_hierarchy, LogicalEdit};
use super::search::{
    adaptive_should_stop, insert_reranked_top_k, insert_top_k, plan_search,
    retained_candidate_bytes, retained_search_candidate_bytes, AdaptiveContext, FrontierEntry,
    PreparedFilter, RerankCandidate, SearchCandidate, SearchPlan,
};
use super::storage::quantized::ScalarQuantized;
use super::storage::vector::ExternalVector;
use super::storage::{
    Descriptor, PhysicalNodeKind, ProximityNode, StoredRecord, StoredRecordRef, VectorRef,
};
use super::vector::promotion_level;
use super::{
    AcceleratorSet, BuildParallelism, DistanceMetric, ExactProximityRecord, Neighbor,
    ProximityBuildStats, ProximityConfig, ProximityMutation, ProximityMutationStats,
    ProximityRecord, ProximityRecordRef, ProximitySearchStats, ProximityTree, ProximityVectorRef,
    ProximityVerification, SearchBackend, SearchBudget, SearchCompletion, SearchIo, SearchPolicy,
    SearchRequest, SearchResult,
};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, HashSet};
use std::ops::ControlFlow;
use std::sync::{Arc, Mutex};

/// Immutable exact-key directory plus deterministic ANN hierarchy.
pub struct ProximityMap<S: Store> {
    store: S,
    directory: Prolly<S>,
    tree: ProximityTree,
    node_cache: Mutex<ContentCache<ProximityNode>>,
}

/// Reusable exact-read context over one immutable proximity map.
pub struct ProximityReadSession<'map, S: Store> {
    directory: ReadSession<'map, 'map, S>,
    dimensions: u32,
}

impl<S: Store> ProximityReadSession<'_, S> {
    pub fn get_with<R>(
        &mut self,
        key: &[u8],
        read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
    ) -> Result<Option<R>, Error> {
        self.directory
            .get_with(key, |bytes| {
                let stored = StoredRecordRef::decode(bytes, self.dimensions)?;
                Ok(read(ProximityRecordRef {
                    vector: ProximityVectorRef::from_encoded(stored.vector),
                    value: stored.value,
                }))
            })?
            .transpose()
    }

    /// Retain the immutable packed leaf containing one encoded exact record.
    /// Native adapters validate the record before exposing callback-scoped
    /// vector and value views over this lease.
    pub fn get_lease(&mut self, key: &[u8]) -> Result<Option<OwnedValueLease>, Error> {
        let lease = self.directory.get_lease(key)?;
        if let Some(lease) = &lease {
            StoredRecordRef::decode(lease.as_bytes()?, self.dimensions)?;
        }
        Ok(lease)
    }

    pub fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get_with(key, |_| ())?.is_some())
    }

    pub fn scan_records(
        &mut self,
        mut visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_records_until(|key, record| {
                visit(key, record);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_records_until<B>(
        &mut self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_records_range_until(&[], None, visit)
    }

    /// Visit a half-open exact-record range with callback-directed early
    /// termination. This is the seekable primitive used by bounded binding
    /// pages; `start` is inclusive and `end` is exclusive.
    pub fn scan_records_range_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let dimensions = self.dimensions;
        let outcome = self.directory.scan_range_until(start, end, |entry| {
            let stored = match StoredRecordRef::decode(entry.value(), dimensions) {
                Ok(stored) => stored,
                Err(error) => return ControlFlow::Break(Err(error)),
            };
            let record = ProximityRecordRef {
                vector: ProximityVectorRef::from_encoded(stored.vector),
                value: stored.value,
            };
            match visit(entry.key(), record) {
                ControlFlow::Continue(()) => ControlFlow::Continue(()),
                ControlFlow::Break(value) => ControlFlow::Break(Ok(value)),
            }
        })?;
        match outcome.break_value {
            Some(Ok(value)) => Ok(ScanOutcome::stopped(outcome.visited, value)),
            Some(Err(error)) => Err(error),
            None => Ok(ScanOutcome::complete(outcome.visited)),
        }
    }
}

impl<S> ProximityMap<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Build a canonical proximity map from logical records.
    pub fn build(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
    ) -> Result<Self, Error> {
        Self::build_with_parallelism(store, config, records, BuildParallelism::default())
            .map(|(map, _)| map)
    }

    /// Build with an explicit runtime worker limit and canonical logical stats.
    pub fn build_with_parallelism(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
        parallelism: BuildParallelism,
    ) -> Result<(Self, ProximityBuildStats), Error> {
        config.validate()?;
        let mut records: Vec<_> = records.into_iter().collect();
        records.sort_by(|left, right| left.key.cmp(&right.key));
        for pair in records.windows(2) {
            if pair[0].key == pair[1].key {
                return Err(Error::DuplicateProximityKey {
                    key: pair[0].key.clone(),
                });
            }
        }

        let directory_config = Config::default();
        let mut directory_builder = BatchBuilder::new_with_origin(
            store.clone(),
            directory_config.clone(),
            PublicationOrigin::Maintenance,
        );
        let mut indexed = Vec::with_capacity(records.len());
        for record in records {
            let stored = StoredRecord::new(
                &record.vector,
                record.value,
                config.metric,
                config.dimensions,
            )?;
            indexed.push(IndexedRecord {
                key: record.key.clone(),
                vector: stored.vector.clone(),
            });
            directory_builder.add(record.key, stored.encode());
        }
        let directory_tree = directory_builder.build()?;
        let hierarchy = build_hierarchy_parallel(&indexed, &config, parallelism.threads())?;
        let objects_written = put_missing_nodes(&store, &hierarchy.nodes)?;

        let descriptor = Descriptor {
            config: config.clone(),
            count: indexed.len() as u64,
            directory: directory_tree.clone(),
            proximity_root: hierarchy.root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        publish_maintenance_content(&store, &descriptor_cid, &descriptor_bytes)?;

        let stats = ProximityBuildStats {
            distance_evaluations: hierarchy.distance_evaluations,
            proximity_objects: hierarchy.nodes.len(),
            proximity_objects_written: objects_written,
        };
        Ok((
            Self {
                directory: Prolly::new(store.clone(), directory_config),
                store,
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root: hierarchy.root,
                    descriptor: descriptor_cid,
                    count: indexed.len() as u64,
                    config,
                },
                node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
            },
            stats,
        ))
    }

    /// Load and locally validate a persisted proximity descriptor.
    pub fn load(store: S, descriptor_cid: Cid) -> Result<Self, Error> {
        let descriptor_bytes = load_content(&store, &descriptor_cid)?;
        let descriptor = Descriptor::decode(&descriptor_bytes)?;
        let root_bytes = load_content(&store, &descriptor.proximity_root)?;
        if root_bytes.len() > descriptor.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "root exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let root = ProximityNode::decode(&root_bytes, descriptor.config.dimensions)?;
        if root.subtree_count != descriptor.count {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "record count disagrees with proximity root".to_owned(),
            });
        }
        let directory_config = descriptor.directory.config.clone();
        Ok(Self {
            directory: Prolly::new(store.clone(), directory_config),
            store,
            tree: ProximityTree {
                directory: descriptor.directory,
                proximity_root: descriptor.proximity_root,
                descriptor: descriptor_cid,
                count: descriptor.count,
                config: descriptor.config,
            },
            node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
        })
    }

    /// The immutable roots and shape configuration committed by this map.
    pub fn tree(&self) -> &ProximityTree {
        &self.tree
    }

    /// Invalidate process-local decoded proximity objects after an external
    /// content sweep. This does not mutate or remove persisted content.
    pub fn clear_content_cache(&self) -> Result<(), Error> {
        self.node_cache
            .lock()
            .map_err(|_| Error::InvalidProximityObject {
                kind: "cache",
                reason: "node cache lock poisoned".to_owned(),
            })?
            .clear();
        Ok(())
    }

    /// Exact key lookup through the authoritative ordered directory.
    pub fn get(&self, key: &[u8]) -> Result<Option<ExactProximityRecord>, Error> {
        self.get_with(key, |record| record.to_owned())
    }

    /// Open a reusable zero-copy exact-read session.
    pub fn read(&self) -> Result<ProximityReadSession<'_, S>, Error> {
        Ok(ProximityReadSession {
            directory: self.directory.read(&self.tree.directory)?,
            dimensions: self.tree.config.dimensions,
        })
    }

    /// Inspect one exact record without allocating its vector or value.
    pub fn get_with<R>(
        &self,
        key: &[u8],
        read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read()?.get_with(key, read)
    }

    /// Visit every exact record in key order without allocating records.
    pub fn scan_records(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error> {
        self.read()?.scan_records(visit)
    }

    /// Visit exact records with callback-directed early termination.
    pub fn scan_records_until<B>(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read()?.scan_records_until(visit)
    }

    /// Visit a half-open exact-record range without allocating logical
    /// records. Binding adapters use this seekable form for bounded pages.
    pub fn scan_records_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read()?.scan_records_range_until(start, end, visit)
    }

    /// Exact key membership through the authoritative ordered directory.
    pub fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
        self.read()?.contains_key(key)
    }

    /// Canonical full rebuild after applying a sorted-unique logical mutation batch.
    pub fn rebuild_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<Self, Error> {
        let mutations = validate_mutations(mutations)?;
        let mut records = self.collect_records()?;
        apply_mutations(&mut records, &mutations, &self.tree.config)?;
        Self::build(
            self.store.clone(),
            self.tree.config.clone(),
            records.into_values(),
        )
    }

    /// Copy-on-write mutation with exact clean-rebuild CID equivalence.
    pub fn mutate_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<(Self, ProximityMutationStats), Error> {
        let mutations = validate_mutations(mutations)?;
        if mutations.is_empty() {
            return Ok((
                Self::load(self.store.clone(), self.tree.descriptor.clone())?,
                Default::default(),
            ));
        }
        let keys: Vec<_> = mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect();
        let old_values = self.directory.get_many(&self.tree.directory, &keys)?;
        let mut logical_edits = Vec::new();
        let mut directory_mutations = Vec::with_capacity(mutations.len());
        let mut count = self.tree.count;
        for (mutation, old_bytes) in mutations.iter().zip(old_values) {
            let old = old_bytes
                .as_deref()
                .map(|bytes| StoredRecord::decode(bytes, self.tree.config.dimensions))
                .transpose()?;
            let new = mutation
                .value
                .as_ref()
                .map(|(vector, value)| {
                    StoredRecord::new(
                        vector,
                        value.clone(),
                        self.tree.config.metric,
                        self.tree.config.dimensions,
                    )
                })
                .transpose()?;
            match (&old, &new) {
                (None, Some(_)) => {
                    count = count
                        .checked_add(1)
                        .ok_or_else(|| Error::InvalidProximityObject {
                            kind: "mutation",
                            reason: "record count overflow".to_owned(),
                        })?
                }
                (Some(_), None) => count -= 1,
                _ => {}
            }
            let old_vector = old.as_ref().map(|record| record.vector.clone());
            let new_vector = new.as_ref().map(|record| record.vector.clone());
            if old_vector != new_vector {
                logical_edits.push(LogicalEdit {
                    key: mutation.key.clone(),
                    old: old_vector,
                    new: new_vector,
                    level: promotion_level(
                        &mutation.key,
                        self.tree.config.hierarchy.log_chunk_size,
                        self.tree.config.hierarchy.level_hash_seed,
                    ),
                });
            }
            directory_mutations.push(match new {
                Some(record) => TreeMutation::Upsert {
                    key: mutation.key.clone(),
                    val: record.encode(),
                },
                None => TreeMutation::Delete {
                    key: mutation.key.clone(),
                },
            });
        }
        let (directory_tree, directory_stats) = splice_with_origin(
            &self.directory,
            &self.tree.directory,
            directory_mutations,
            PublicationOrigin::Maintenance,
        )?;

        if logical_edits.is_empty() {
            let descriptor = Descriptor {
                config: self.tree.config.clone(),
                count,
                directory: directory_tree.clone(),
                proximity_root: self.tree.proximity_root.clone(),
            };
            let bytes = descriptor.encode();
            let descriptor_cid = Cid::from_bytes(&bytes);
            publish_maintenance_content(&self.store, &descriptor_cid, &bytes)?;
            let map = Self {
                directory: Prolly::new(self.store.clone(), directory_tree.config.clone()),
                store: self.store.clone(),
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root: self.tree.proximity_root.clone(),
                    descriptor: descriptor_cid,
                    count,
                    config: self.tree.config.clone(),
                },
                node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
            };
            return Ok((
                map,
                ProximityMutationStats {
                    nodes_reused: 1,
                    directory_entries_scanned: directory_stats.entries_scanned,
                    directory_nodes_read: directory_stats.nodes_read,
                    directory_nodes_rebuilt: directory_stats.nodes_rebuilt,
                    directory_nodes_written: directory_stats.nodes_written,
                    directory_nodes_reused: directory_stats.nodes_reused,
                    directory_levels_rebuilt: directory_stats.levels_rebuilt,
                    directory_right_edge_rebuilt: directory_stats.right_edge_rebuilt,
                    ..Default::default()
                },
            ));
        }

        let (old_root, _) = self.load_node(&self.tree.proximity_root)?;
        let max_edit_level = logical_edits
            .iter()
            .map(|edit| edit.level)
            .max()
            .unwrap_or(0);
        let (proximity_root, nodes, mut stats) =
            if old_root.entries.is_empty() || max_edit_level >= old_root.level {
                let records = self.collect_records_from(&directory_tree)?;
                let indexed: Vec<_> = records
                    .values()
                    .map(|record| IndexedRecord {
                        key: record.key.clone(),
                        vector: record.vector.clone(),
                    })
                    .collect();
                let built = build_hierarchy(&indexed, &self.tree.config)?;
                let stats = ProximityMutationStats {
                    records_rebuilt: indexed.len(),
                    distance_evaluations: built.distance_evaluations,
                    full_proximity_rebuild: true,
                    ..Default::default()
                };
                (built.root, built.nodes, stats)
            } else {
                let local = mutate_hierarchy(
                    &self.store,
                    &self.tree.proximity_root,
                    &self.tree.config,
                    &logical_edits,
                )?;
                (local.root, local.nodes, local.stats)
            };
        let pending_count = nodes.len();
        let nodes_written = put_missing_nodes(&self.store, &nodes)?;
        stats.nodes_written = nodes_written;
        stats.nodes_reused += pending_count.saturating_sub(nodes_written);
        apply_directory_stats(&mut stats, directory_stats);

        let descriptor = Descriptor {
            config: self.tree.config.clone(),
            count,
            directory: directory_tree.clone(),
            proximity_root: proximity_root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        publish_maintenance_content(&self.store, &descriptor_cid, &descriptor_bytes)?;
        Ok((
            Self {
                directory: Prolly::new(self.store.clone(), directory_tree.config.clone()),
                store: self.store.clone(),
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root,
                    descriptor: descriptor_cid,
                    count,
                    config: self.tree.config.clone(),
                },
                node_cache: Mutex::new(ContentCache::new(DEFAULT_PROXIMITY_CACHE_NODES)),
            },
            stats,
        ))
    }

    /// Deterministic global best-first search over the authoritative hierarchy.
    pub fn search(&self, request: SearchRequest<'_>) -> Result<SearchResult, Error> {
        self.search_with_trace(request, None)
    }

    /// Plan and execute against validated, source-bound derived accelerators.
    /// The `SearchIo` binding supplies the authoritative store for this call.
    pub fn search_with(
        &self,
        accelerators: &AcceleratorSet<S>,
        search_io: &SearchIo<S>,
        request: SearchRequest<'_>,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        let physical_bytes_before = search_io.physical_bytes_read();
        let eligibility = PreparedFilter::new(request.filter.clone(), &self.tree.directory)?;
        let plan = plan_search(&self.tree, accelerators, &request, &eligibility)?;
        search_io.runtime().load(
            search_io,
            super::super::content_graph::ContentObjectKind::ProximityDescriptor,
            &self.tree.descriptor,
            2,
            |bytes| Descriptor::decode(bytes).map(|_| ()),
        )?;
        match &plan {
            SearchPlan::Hnsw { .. } => {
                let index = accelerators.hnsw().expect("planner validated HNSW");
                search_io.runtime().load(
                    search_io,
                    super::super::content_graph::ContentObjectKind::HnswManifest,
                    index.manifest_cid(),
                    2,
                    |bytes| super::accelerator::hnsw::storage::Manifest::decode(bytes).map(|_| ()),
                )?;
            }
            SearchPlan::ProductQuantized { .. } => {
                let index = accelerators.pq().expect("planner validated PQ");
                search_io.runtime().load(
                    search_io,
                    super::super::content_graph::ContentObjectKind::ProductQuantization,
                    index.manifest_cid(),
                    2,
                    |bytes| super::accelerator::pq::Manifest::decode(bytes).map(|_| ()),
                )?;
            }
            SearchPlan::Composite { .. } => {
                let accelerator = accelerators
                    .composite()
                    .expect("planner validated composite");
                search_io.runtime().load(
                    search_io,
                    super::super::content_graph::ContentObjectKind::CompositeAccelerator,
                    accelerator.manifest_cid(),
                    1,
                    |bytes| super::accelerator::composite::Manifest::decode(bytes).map(|_| ()),
                )?;
            }
            SearchPlan::Native | SearchPlan::EligibleExact { .. } => {}
        }
        let bound_map = ProximityMap::load(
            search_io.for_kind_with_dimensions(
                super::super::content_graph::ContentObjectKind::ProximityNode,
                self.tree.config.dimensions,
            ),
            self.tree.descriptor.clone(),
        )?;
        let mut result = match &plan {
            SearchPlan::Native => {
                let mut native = request;
                native.options.backend = SearchBackend::Native;
                bound_map.search(native)
            }
            SearchPlan::EligibleExact {
                key_count,
                source_bound,
            } => bound_map.search_eligible_exact(
                &request,
                &eligibility,
                *key_count,
                *source_bound,
                plan,
            ),
            SearchPlan::Hnsw { .. } => {
                let index = accelerators
                    .hnsw()
                    .ok_or_else(|| Error::InvalidProximitySearch {
                        reason: "planned HNSW accelerator is unavailable".to_owned(),
                    })?;
                let index = index.rebind(
                    search_io.for_kind(super::super::content_graph::ContentObjectKind::HnswPage),
                );
                super::accelerator::hnsw::search::search_planned(&index, &bound_map, request, &plan)
            }
            SearchPlan::ProductQuantized { .. } => {
                let index = accelerators
                    .pq()
                    .ok_or_else(|| Error::InvalidProximitySearch {
                        reason: "planned product-quantized accelerator is unavailable".to_owned(),
                    })?;
                let index =
                    index.rebind(search_io.for_kind(
                        super::super::content_graph::ContentObjectKind::ProductQuantization,
                    ));
                index.search_planned(&bound_map, request, &plan)
            }
            SearchPlan::Composite { .. } => self.search_composite(
                accelerators
                    .composite()
                    .ok_or_else(|| Error::InvalidProximitySearch {
                        reason: "planned composite accelerator is unavailable".to_owned(),
                    })?,
                search_io,
                &bound_map,
                request,
                &eligibility,
                &plan,
            ),
        }?;
        result.stats.physical_bytes_read = search_io
            .physical_bytes_read()
            .saturating_sub(physical_bytes_before);
        Ok(result)
    }

    pub(crate) fn search_composite(
        &self,
        composite: &super::accelerator::composite::CompositeAccelerator<S>,
        search_io: &SearchIo<S>,
        current: &ProximityMap<SearchIo<S>>,
        request: SearchRequest<'_>,
        eligibility: &PreparedFilter<'_>,
        plan: &SearchPlan,
    ) -> Result<SearchResult, Error> {
        let SearchPlan::Composite {
            base,
            delta_records,
            shadow_records,
            merge_target,
        } = plan
        else {
            return Err(Error::InvalidProximitySearch {
                reason: "composite executor requires a composite plan".to_owned(),
            });
        };
        if *delta_records != composite.delta_count as usize
            || *shadow_records != composite.shadow_count as usize
            || composite.current_source != self.tree.descriptor
        {
            return Err(Error::InvalidProximityObject {
                kind: "composite accelerator",
                reason: "plan or source binding disagrees with manifest".to_owned(),
            });
        }
        let ordered_store =
            search_io.for_kind(super::super::content_graph::ContentObjectKind::OrderedNode);
        let shadow_manager =
            Prolly::new(ordered_store.clone(), composite.shadow_tree.config.clone());
        let delta_manager = Prolly::new(ordered_store, composite.delta_tree.config.clone());
        let mut stats = ProximitySearchStats::default();
        let mut shadow = BTreeSet::new();
        for entry in shadow_manager.range(&composite.shadow_tree, &[], None)? {
            let (key, value) = entry?;
            if !value.is_empty() || !shadow.insert(key.clone()) {
                return Err(Error::InvalidProximityObject {
                    kind: "composite shadow",
                    reason: "shadow tree contains a value or duplicate key".to_owned(),
                });
            }
            if request
                .budget
                .max_nodes
                .is_some_and(|limit| stats.nodes_read >= limit)
                || request
                    .budget
                    .max_committed_bytes
                    .is_some_and(|limit| stats.committed_bytes.saturating_add(key.len()) > limit)
            {
                return Ok(SearchResult {
                    neighbors: Vec::new(),
                    stats,
                    completion: SearchCompletion::BudgetExhausted,
                    plan: plan.summary(),
                });
            }
            stats.nodes_read += 1;
            stats.bytes_read += key.len();
            stats.committed_bytes += key.len();
        }
        if shadow.len() != *shadow_records {
            return Err(Error::InvalidProximityObject {
                kind: "composite shadow",
                reason: "shadow tree cardinality disagrees with manifest".to_owned(),
            });
        }
        if search_budget_exhausted(&request.budget, &stats) {
            return Ok(SearchResult {
                neighbors: Vec::new(),
                stats,
                completion: SearchCompletion::BudgetExhausted,
                plan: plan.summary(),
            });
        }
        let mut base_request = request.clone();
        base_request.budget = remaining_budget(&request.budget, &stats);
        let mut base_result = match &composite.base {
            super::accelerator::composite::CompositeBase::Hnsw(index) => {
                let index = index.rebind(
                    search_io.for_kind(super::super::content_graph::ContentObjectKind::HnswPage),
                );
                super::accelerator::hnsw::search::search_planned_with_exclusion(
                    &index,
                    current,
                    &composite.base_source,
                    base_request,
                    base,
                    |key| Ok(shadow.contains(key)),
                )
            }
            super::accelerator::composite::CompositeBase::ProductQuantized(index) => {
                let index =
                    index.rebind(search_io.for_kind(
                        super::super::content_graph::ContentObjectKind::ProductQuantization,
                    ));
                index.search_planned_with_exclusion(
                    current,
                    &composite.base_source,
                    base_request,
                    base,
                    |key| Ok(shadow.contains(key)),
                )
            }
        }?;
        add_search_stats(&mut stats, &base_result.stats);
        let mut completion = base_result.completion;
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        enum CompositeValue {
            Owned { value: Vec<u8>, distance: f64 },
            Retained(RerankCandidate),
        }
        impl CompositeValue {
            fn distance(&self) -> f64 {
                match self {
                    Self::Owned { distance, .. } => *distance,
                    Self::Retained(candidate) => candidate.distance,
                }
            }
        }
        let mut delta_seen = 0usize;
        let mut merged = BTreeMap::<Vec<u8>, CompositeValue>::new();
        for neighbor in base_result.neighbors.drain(..) {
            if merged
                .insert(
                    neighbor.key,
                    CompositeValue::Owned {
                        value: neighbor.value,
                        distance: neighbor.distance,
                    },
                )
                .is_some()
            {
                return Err(Error::InvalidProximityObject {
                    kind: "composite base",
                    reason: "base executor returned a duplicate key".to_owned(),
                });
            }
        }
        let mut vector_scratch = vec![0.0f32; self.tree.config.dimensions as usize];
        let mut current_directory = current
            .directory_manager()
            .read(&current.tree().directory)?;
        let mut retained_backings = HashSet::new();
        let mut retained_bytes = 0usize;
        for entry in delta_manager.range(&composite.delta_tree, &[], None)? {
            let (key, bytes) = entry?;
            delta_seen += 1;
            if request
                .budget
                .max_nodes
                .is_some_and(|limit| stats.nodes_read.saturating_add(1) > limit)
                || request
                    .budget
                    .max_committed_bytes
                    .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes.len()) > limit)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let record = StoredRecordRef::decode(&bytes, self.tree.config.dimensions)?;
            stats.nodes_read += 1;
            stats.bytes_read += bytes.len();
            stats.committed_bytes += bytes.len();
            if !eligibility.contains(&key) {
                continue;
            }
            if request
                .budget
                .max_nodes
                .is_some_and(|limit| stats.nodes_read.saturating_add(1) > limit)
                || request
                    .budget
                    .max_distance_evaluations
                    .is_some_and(|limit| {
                        stats
                            .distance_evaluations
                            .saturating_add(stats.quantized_distance_evaluations)
                            .saturating_add(1)
                            > limit
                    })
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let Some(handle) = current_directory.get_handle(&key)? else {
                return Err(Error::InvalidProximityObject {
                    kind: "composite delta",
                    reason: "delta key is absent from current source".to_owned(),
                });
            };
            let authoritative_bytes = handle.value()?.len();
            if request.budget.max_committed_bytes.is_some_and(|limit| {
                stats.committed_bytes.saturating_add(authoritative_bytes) > limit
            }) {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let authoritative =
                StoredRecordRef::decode(handle.value()?, self.tree.config.dimensions)?;
            if !encoded_vectors_equal(authoritative.vector, record.vector) {
                return Err(Error::InvalidProximityObject {
                    kind: "composite delta",
                    reason: "delta vector disagrees with current source".to_owned(),
                });
            }
            ProximityVectorRef::from_encoded(record.vector).copy_to_slice(&mut vector_scratch)?;
            let distance = query_score(
                request.kernel,
                self.tree.config.metric,
                &query,
                &vector_scratch,
            );
            stats.nodes_read += 1;
            stats.bytes_read += authoritative_bytes;
            stats.committed_bytes += authoritative_bytes;
            stats.distance_evaluations += 1;
            stats.reranked_candidates += 1;
            let candidate = RerankCandidate::new(handle, &key, distance)?;
            if retained_backings.insert(candidate.backing_id()) {
                retained_bytes = retained_bytes.saturating_add(candidate.retained_bytes());
            }
            if merged
                .insert(key, CompositeValue::Retained(candidate))
                .is_some()
            {
                return Err(Error::InvalidProximityObject {
                    kind: "composite accelerator",
                    reason: "delta key was not shadowed from the base".to_owned(),
                });
            }
            stats.candidate_handles_peak =
                stats.candidate_handles_peak.max(retained_backings.len());
            stats.candidate_retained_bytes_peak =
                stats.candidate_retained_bytes_peak.max(retained_bytes);
        }
        if completion != SearchCompletion::BudgetExhausted && delta_seen != *delta_records {
            return Err(Error::InvalidProximityObject {
                kind: "composite delta",
                reason: "delta tree cardinality disagrees with manifest".to_owned(),
            });
        }
        let mut candidates = merged.into_iter().collect::<Vec<_>>();
        candidates.sort_by(|(left_key, left), (right_key, right)| {
            left.distance()
                .total_cmp(&right.distance())
                .then_with(|| left_key.cmp(right_key))
        });
        candidates.truncate((*merge_target).min(request.k));
        let neighbors = candidates
            .into_iter()
            .map(|(key, candidate)| match candidate {
                CompositeValue::Owned { value, distance } => Ok(Neighbor {
                    key,
                    value,
                    distance,
                }),
                CompositeValue::Retained(candidate) => {
                    let record = candidate.record(self.tree.config.dimensions)?;
                    Ok(Neighbor {
                        key,
                        value: record.value.to_vec(),
                        distance: candidate.distance,
                    })
                }
            })
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: plan.summary(),
        })
    }

    fn search_eligible_exact(
        &self,
        request: &SearchRequest<'_>,
        eligibility: &PreparedFilter<'_>,
        key_count: u64,
        source_bound: bool,
        plan: SearchPlan,
    ) -> Result<SearchResult, Error> {
        let Some((keys, prepared_source_bound)) = eligibility.sorted_keys() else {
            return Err(Error::InvalidProximitySearch {
                reason: "eligible-exact plan requires sorted eligible keys".to_owned(),
            });
        };
        if key_count != keys.len() as u64 || source_bound != prepared_source_bound {
            return Err(Error::InvalidProximitySearch {
                reason: "eligible-exact plan disagrees with prepared eligibility".to_owned(),
            });
        }
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        let mut stats = ProximitySearchStats::default();
        let candidate_limit = request
            .budget
            .max_frontier_entries
            .unwrap_or(request.k)
            .min(request.k);
        let mut completion = if candidate_limit < request.k.min(keys.len()) {
            SearchCompletion::BudgetExhausted
        } else {
            SearchCompletion::Exact
        };
        let mut candidates = Vec::<RerankCandidate>::with_capacity(candidate_limit);
        let mut vector_scratch = vec![0.0f32; self.tree.config.dimensions as usize];
        let mut directory = self.directory.read(&self.tree.directory)?;
        for key in keys {
            if request
                .budget
                .max_nodes
                .is_some_and(|limit| stats.nodes_read >= limit)
                || request
                    .budget
                    .max_distance_evaluations
                    .is_some_and(|limit| stats.distance_evaluations >= limit)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            stats.nodes_read += 1;
            let Some(handle) = directory.get_handle(key)? else {
                if source_bound {
                    return Err(Error::InvalidProximityObject {
                        kind: "eligible keys",
                        reason:
                            "source-bound eligible key is absent from the authoritative directory"
                                .to_owned(),
                    });
                }
                continue;
            };
            let bytes = handle.value()?.len();
            if request
                .budget
                .max_committed_bytes
                .is_some_and(|limit| stats.committed_bytes.saturating_add(bytes) > limit)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let record = StoredRecordRef::decode(handle.value()?, self.tree.config.dimensions)?;
            ProximityVectorRef::from_encoded(record.vector).copy_to_slice(&mut vector_scratch)?;
            stats.bytes_read = stats.bytes_read.saturating_add(bytes);
            stats.committed_bytes = stats.committed_bytes.saturating_add(bytes);
            stats.distance_evaluations += 1;
            let distance = query_score(
                request.kernel,
                self.tree.config.metric,
                &query,
                &vector_scratch,
            );
            insert_reranked_top_k(
                &mut candidates,
                RerankCandidate::new(handle, key, distance)?,
                candidate_limit,
            );
            stats.frontier_peak = stats.frontier_peak.max(candidates.len());
            stats.candidate_handles_peak = stats.candidate_handles_peak.max(candidates.len());
            stats.candidate_retained_bytes_peak = stats
                .candidate_retained_bytes_peak
                .max(retained_candidate_bytes(&candidates));
        }
        stats.reranked_candidates = stats.distance_evaluations;
        let neighbors = candidates
            .into_iter()
            .map(|candidate| candidate.into_neighbor(self.tree.config.dimensions))
            .collect::<Result<Vec<_>, Error>>()?;
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: plan.summary(),
        })
    }

    pub(super) fn search_with_trace(
        &self,
        request: SearchRequest<'_>,
        mut trace: Option<&mut Vec<super::proof::ProximitySearchEvent>>,
    ) -> Result<SearchResult, Error> {
        request.validate()?;
        if matches!(
            request.options.backend,
            SearchBackend::ProductQuantized | SearchBackend::Hnsw
        ) {
            return Err(Error::InvalidProximitySearch {
                reason: "requested backend requires a validated accelerator sidecar".to_owned(),
            });
        }
        let filter = PreparedFilter::new(request.filter.clone(), &self.tree.directory)?;
        let query = prepare_vector(
            self.tree.config.metric,
            request.query,
            self.tree.config.dimensions,
        )?;
        let use_scalar_quantization =
            super::accelerator::sq8::enabled(&self.tree.config, request.policy);
        let mut stats = ProximitySearchStats::default();
        let mut frontier = BinaryHeap::new();
        frontier.push(FrontierEntry {
            bound: 0.0,
            score: 0.0,
            key: Vec::new(),
            cid: self.tree.proximity_root.clone(),
            expected_level: None,
        });
        if let Some(trace) = trace.as_deref_mut() {
            trace.push(super::proof::ProximitySearchEvent::FrontierPushed {
                cid: self.tree.proximity_root.clone(),
                bound_bits: 0.0f64.to_bits(),
            });
        }
        let mut candidates = Vec::<SearchCandidate>::new();
        let mut score_cache = BTreeMap::<Vec<u8>, f64>::new();
        let mut visited = HashSet::new();
        let mut levels = HashSet::new();
        let mut last_fanout = 0usize;
        let mut completion = SearchCompletion::Exact;

        while let Some(next) = frontier.peek() {
            if !use_scalar_quantization
                && self.tree.config.metric == DistanceMetric::L2Squared
                && candidates.len() == request.k
                && next.bound > candidates.last().expect("full top-k").score
            {
                break;
            }
            if let SearchPolicy::Adaptive(quality) = request.policy {
                if candidates.last().is_some_and(|worst| {
                    let overlapping = frontier
                        .iter()
                        .filter(|entry| entry.bound <= worst.score)
                        .count();
                    adaptive_should_stop(
                        quality,
                        AdaptiveContext {
                            results: candidates.len(),
                            k: request.k,
                            frontier_bound: next.bound,
                            worst_score: worst.score,
                            overlapping_clusters: overlapping,
                            logical_level: next.expected_level.unwrap_or(u8::MAX),
                            last_fanout,
                            cluster_count: frontier.len(),
                        },
                    )
                }) {
                    completion = SearchCompletion::ApproximatePolicySatisfied;
                    break;
                }
            }
            if request
                .budget
                .max_nodes
                .is_some_and(|maximum| stats.nodes_read >= maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            let next = frontier.pop().expect("peeked frontier");
            if let Some(trace) = trace.as_deref_mut() {
                trace.push(super::proof::ProximitySearchEvent::FrontierPopped {
                    cid: next.cid.clone(),
                    bound_bits: next.bound.to_bits(),
                });
            }
            if !visited.insert(next.cid.clone()) {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "cycle or repeated child ownership".to_owned(),
                });
            }
            let (node, mut bytes) = self.load_node(&next.cid)?;
            if let Some(trace) = trace.as_deref_mut() {
                trace.push(super::proof::ProximitySearchEvent::VisitedObject(
                    next.cid.clone(),
                ));
            }
            let quantizer = if use_scalar_quantization && node.kind.has_children(node.level) {
                let (quantizer, quantizer_bytes) = self.load_scalar_quantizer(&node)?;
                bytes = bytes.saturating_add(quantizer_bytes);
                Some(quantizer)
            } else {
                None
            };
            if next
                .expected_level
                .is_some_and(|expected| node.level != expected)
            {
                return Err(Error::InvalidProximityObject {
                    kind: "node",
                    reason: "child has an unexpected logical level".to_owned(),
                });
            }
            stats.bytes_read = stats.bytes_read.saturating_add(bytes);
            if request
                .budget
                .max_committed_bytes
                .is_some_and(|maximum| stats.committed_bytes.saturating_add(bytes) > maximum)
            {
                completion = SearchCompletion::BudgetExhausted;
                break;
            }
            stats.nodes_read += 1;
            stats.committed_bytes += bytes;
            last_fanout = node.entries.len();
            levels.insert(node.level);
            stats.levels_visited = levels.len();

            for (entry_index, entry) in node.entries.iter().enumerate() {
                if node.kind.has_children(node.level) {
                    if !filter.intersects(&entry.min_key, &entry.max_key) {
                        continue;
                    }
                    let Some(child) = &entry.child else {
                        return Err(Error::InvalidProximityObject {
                            kind: "node",
                            reason: "internal entry has no child".to_owned(),
                        });
                    };
                    let representative_score = if let Some(quantizer) = &quantizer {
                        if distance_budget_exhausted(&request, &stats) {
                            completion = SearchCompletion::BudgetExhausted;
                            break;
                        }
                        stats.quantized_distance_evaluations += 1;
                        quantizer.approximate_score(self.tree.config.metric, &query, entry_index)?
                    } else {
                        match score_cache.get(&entry.key) {
                            Some(score) => *score,
                            None => {
                                if distance_budget_exhausted(&request, &stats) {
                                    completion = SearchCompletion::BudgetExhausted;
                                    break;
                                }
                                stats.distance_evaluations += 1;
                                let value = query_score(
                                    request.kernel,
                                    self.tree.config.metric,
                                    &query,
                                    entry.vector.inline()?,
                                );
                                score_cache.insert(entry.key.clone(), value);
                                value
                            }
                        }
                    };
                    let bound = if quantizer.is_none()
                        && self.tree.config.metric == DistanceMetric::L2Squared
                    {
                        super::distance::canonical::l2_lower_bound_down(
                            representative_score,
                            entry.covering_radius,
                        )
                    } else {
                        representative_score
                    };
                    if request
                        .budget
                        .max_frontier_entries
                        .is_some_and(|maximum| frontier.len() >= maximum)
                    {
                        completion = SearchCompletion::BudgetExhausted;
                        break;
                    }
                    frontier.push(FrontierEntry {
                        bound,
                        score: representative_score,
                        key: entry.key.clone(),
                        cid: child.clone(),
                        expected_level: Some(if node.kind == PhysicalNodeKind::OverflowDirectory {
                            node.level
                        } else {
                            node.level - 1
                        }),
                    });
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.push(super::proof::ProximitySearchEvent::FrontierPushed {
                            cid: child.clone(),
                            bound_bits: bound.to_bits(),
                        });
                    }
                    stats.frontier_peak = stats.frontier_peak.max(frontier.len());
                } else if filter.contains(&entry.key) {
                    let leaf_score = match score_cache.get(&entry.key) {
                        Some(score) => *score,
                        None => {
                            if distance_budget_exhausted(&request, &stats) {
                                completion = SearchCompletion::BudgetExhausted;
                                break;
                            }
                            stats.distance_evaluations += 1;
                            let value = query_score(
                                request.kernel,
                                self.tree.config.metric,
                                &query,
                                entry.vector.inline()?,
                            );
                            score_cache.insert(entry.key.clone(), value);
                            value
                        }
                    };
                    if let Some(trace) = trace.as_deref_mut() {
                        trace.push(super::proof::ProximitySearchEvent::CandidateScored {
                            key: entry.key.clone(),
                            distance_bits: leaf_score.to_bits(),
                        });
                    }
                    insert_top_k(
                        &mut candidates,
                        SearchCandidate::new(node.clone(), entry_index, leaf_score),
                        request.k,
                    );
                }
            }
            if completion == SearchCompletion::BudgetExhausted {
                break;
            }
        }

        stats.candidate_handles_peak = stats.candidate_handles_peak.max(candidates.len());
        stats.candidate_retained_bytes_peak = stats
            .candidate_retained_bytes_peak
            .max(retained_search_candidate_bytes(&candidates));
        let keys = candidates
            .iter()
            .map(SearchCandidate::key)
            .collect::<Result<Vec<_>, Error>>()?;
        if use_scalar_quantization {
            stats.reranked_candidates = candidates.len();
        }
        let mut neighbors = Vec::with_capacity(candidates.len());
        let mut rerank_error = None;
        let mut directory = self.directory.read(&self.tree.directory)?;
        directory.get_many_with(&keys, |position, _, stored| {
            if rerank_error.is_some() {
                return;
            }
            let result = (|| {
                let candidate =
                    candidates
                        .get(position)
                        .ok_or_else(|| Error::InvalidProximityObject {
                            kind: "candidate",
                            reason: "directory multi-get returned an invalid position".to_owned(),
                        })?;
                let bytes = stored.ok_or_else(|| Error::InvalidProximityObject {
                    kind: "node",
                    reason: "leaf key is absent from exact directory".to_owned(),
                })?;
                let record = StoredRecordRef::decode(bytes, self.tree.config.dimensions)?;
                if !encoded_vector_matches(record.vector, candidate.vector()?) {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "leaf vector disagrees with exact directory".to_owned(),
                    });
                }
                neighbors.push(Neighbor {
                    key: candidate.key()?.to_vec(),
                    value: record.value.to_vec(),
                    distance: candidate.score,
                });
                Ok(())
            })();
            if let Err(error) = result {
                rerank_error = Some(error);
            }
        })?;
        if let Some(error) = rerank_error {
            return Err(error);
        }
        if let Some(trace) = trace {
            trace.push(super::proof::ProximitySearchEvent::Completed(completion));
        }
        Ok(SearchResult {
            neighbors,
            stats,
            completion,
            plan: super::search::SearchPlan::Native.summary(),
        })
    }

    /// Traverse and validate the descriptor, directory, hierarchy, and routing invariants.
    pub fn verify(&self) -> Result<ProximityVerification, Error> {
        let records = self.collect_records()?;
        let root_bytes = load_content(&self.store, &self.tree.proximity_root)?;
        let root = ProximityNode::decode(&root_bytes, self.tree.config.dimensions)?;
        let mut state = VerificationState {
            records: &records,
            seen_nodes: HashSet::new(),
            seen_external_vectors: HashSet::new(),
            seen_scalar_quantizers: HashSet::new(),
            seen_leaf_keys: HashSet::new(),
            summary: ProximityVerification {
                record_count: self.tree.count,
                maximum_level: root.level,
                ..Default::default()
            },
        };
        let verified = self.verify_node(
            &self.tree.proximity_root,
            Some(root.level),
            None,
            &mut state,
        )?;
        if verified.count != self.tree.count || records.len() as u64 != self.tree.count {
            return Err(Error::InvalidProximityObject {
                kind: "descriptor",
                reason: "logical counts disagree".to_owned(),
            });
        }
        if state.seen_leaf_keys.len() != records.len()
            || records
                .keys()
                .any(|key| !state.seen_leaf_keys.contains(key))
        {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "leaf identities do not match the exact directory".to_owned(),
            });
        }
        Ok(state.summary)
    }

    fn load_node(&self, cid: &Cid) -> Result<(Arc<ProximityNode>, usize), Error> {
        if let Some((node, bytes)) = self
            .node_cache
            .lock()
            .map_err(|_| Error::InvalidProximityObject {
                kind: "cache",
                reason: "node cache lock poisoned".to_owned(),
            })?
            .get(cid)
        {
            return Ok((node, bytes));
        }
        let bytes = load_content(&self.store, cid)?;
        if bytes.len() > self.tree.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "node exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let len = bytes.len();
        let mut node = ProximityNode::decode(&bytes, self.tree.config.dimensions)?;
        let vector_bytes = self.resolve_external_vectors(&mut node)?;
        let node = Arc::new(node);
        self.node_cache
            .lock()
            .map_err(|_| Error::InvalidProximityObject {
                kind: "cache",
                reason: "node cache lock poisoned".to_owned(),
            })?
            .insert(cid.clone(), node.clone(), len + vector_bytes);
        Ok((node, len + vector_bytes))
    }

    fn resolve_external_vectors(&self, node: &mut ProximityNode) -> Result<usize, Error> {
        let mut bytes_read = 0usize;
        for entry in &mut node.entries {
            let VectorRef::External(cid) = &entry.vector else {
                continue;
            };
            let bytes = load_content(&self.store, cid)?;
            let external = ExternalVector::decode(&bytes)?;
            if external.vector.len() != self.tree.config.dimensions as usize {
                return Err(Error::InvalidProximityObject {
                    kind: "vector",
                    reason: "external vector dimension mismatch".to_owned(),
                });
            }
            bytes_read += bytes.len();
            entry.vector = VectorRef::Inline(external.vector);
        }
        Ok(bytes_read)
    }

    fn load_scalar_quantizer(
        &self,
        node: &ProximityNode,
    ) -> Result<(ScalarQuantized, usize), Error> {
        let config = self
            .tree
            .config
            .scalar_quantization
            .as_ref()
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "quantized search requires descriptor configuration".to_owned(),
            })?;
        let cid = node
            .quantizer
            .as_ref()
            .ok_or_else(|| Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "configured node has no scalar quantizer".to_owned(),
            })?;
        let bytes = load_content(&self.store, cid)?;
        let quantizer = ScalarQuantized::decode(&bytes)?;
        if quantizer.dimensions != self.tree.config.dimensions
            || quantizer.group_size != config.group_size
        {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "quantizer configuration disagrees with descriptor".to_owned(),
            });
        }
        if quantizer.entry_count != node.entries.len() as u64 {
            return Err(Error::InvalidProximityObject {
                kind: "quantizer",
                reason: "quantizer entry count disagrees with node".to_owned(),
            });
        }
        Ok((quantizer, bytes.len()))
    }

    pub(crate) fn collect_records(&self) -> Result<BTreeMap<Vec<u8>, ProximityRecord>, Error> {
        self.collect_records_from(&self.tree.directory)
    }

    pub(crate) fn store_clone(&self) -> S {
        self.store.clone()
    }

    pub(super) fn directory_manager(&self) -> &Prolly<S> {
        &self.directory
    }

    pub(super) fn load_descriptor_bytes(&self) -> Result<Vec<u8>, Error> {
        load_content(&self.store, &self.tree.descriptor)
    }

    fn collect_records_from(
        &self,
        directory: &super::super::tree::Tree,
    ) -> Result<BTreeMap<Vec<u8>, ProximityRecord>, Error> {
        let mut records = BTreeMap::new();
        let mut decode_error = None;
        self.directory
            .scan_range_until(directory, &[], None, |entry| {
                let stored =
                    match StoredRecordRef::decode(entry.value(), self.tree.config.dimensions) {
                        Ok(stored) => stored,
                        Err(error) => {
                            decode_error = Some(error);
                            return ControlFlow::Break(());
                        }
                    };
                let key = entry.key().to_vec();
                records.insert(
                    key.clone(),
                    ProximityRecord {
                        key,
                        vector: ProximityVectorRef::from_encoded(stored.vector).to_vec(),
                        value: stored.value.to_vec(),
                    },
                );
                ControlFlow::Continue(())
            })?;
        if let Some(error) = decode_error {
            return Err(error);
        }
        Ok(records)
    }

    fn verify_node(
        &self,
        cid: &Cid,
        expected_level: Option<u8>,
        parent: Option<(
            &super::storage::ProximityEntry,
            &[super::storage::ProximityEntry],
        )>,
        state: &mut VerificationState<'_>,
    ) -> Result<VerifiedSubtree, Error> {
        if !state.seen_nodes.insert(cid.clone()) {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "cycle or repeated child ownership".to_owned(),
            });
        }
        let bytes = load_content(&self.store, cid)?;
        if bytes.len() > self.tree.config.overflow.max_page_bytes as usize {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "node exceeds descriptor max_node_bytes".to_owned(),
            });
        }
        let mut node = ProximityNode::decode(&bytes, self.tree.config.dimensions)?;
        for entry in &node.entries {
            if let VectorRef::External(vector) = &entry.vector {
                if state.seen_external_vectors.insert(vector.clone()) {
                    state.summary.external_vector_count += 1;
                }
            }
        }
        self.resolve_external_vectors(&mut node)?;
        match (&self.tree.config.scalar_quantization, &node.quantizer) {
            (None, None) => {}
            (Some(config), Some(cid)) => {
                let quantizer_bytes = load_content(&self.store, cid)?;
                let quantizer = ScalarQuantized::decode(&quantizer_bytes)?;
                if quantizer.dimensions != self.tree.config.dimensions
                    || quantizer.group_size != config.group_size
                {
                    return Err(Error::InvalidProximityObject {
                        kind: "quantizer",
                        reason: "quantizer configuration disagrees with descriptor".to_owned(),
                    });
                }
                let vectors = node
                    .entries
                    .iter()
                    .map(|entry| entry.vector.inline())
                    .collect::<Result<Vec<_>, _>>()?;
                quantizer.verify(&vectors)?;
                state.summary.quantized_node_count += 1;
                if state.seen_scalar_quantizers.insert(cid.clone()) {
                    state.summary.scalar_quantizer_count += 1;
                }
            }
            _ => {
                return Err(Error::InvalidProximityObject {
                    kind: "quantizer",
                    reason: "node quantizer presence disagrees with descriptor".to_owned(),
                })
            }
        }
        if expected_level != Some(node.level) {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "unexpected logical level".to_owned(),
            });
        }
        state.summary.proximity_node_count += 1;
        match node.kind {
            PhysicalNodeKind::OverflowPage => state.summary.overflow_page_count += 1,
            PhysicalNodeKind::OverflowDirectory => state.summary.overflow_directory_count += 1,
            PhysicalNodeKind::Leaf | PhysicalNodeKind::Route => {}
        }
        state.summary.maximum_node_bytes = state.summary.maximum_node_bytes.max(bytes.len());

        if node.kind != PhysicalNodeKind::OverflowDirectory {
            if let Some((selected, candidates)) = parent {
                for entry in &node.entries {
                    // A promoted representative is deterministically retained in
                    // its own route even when another representative has an
                    // equal vector and a lexicographically smaller key.
                    if entry.key == selected.key {
                        continue;
                    }
                    let selected_distance = score(
                        self.tree.config.metric,
                        entry.vector.inline()?,
                        selected.vector.inline()?,
                    );
                    for candidate in candidates {
                        state.summary.distance_checks += 1;
                        let candidate_distance = score(
                            self.tree.config.metric,
                            entry.vector.inline()?,
                            candidate.vector.inline()?,
                        );
                        let candidate_is_better = candidate_distance
                            .total_cmp(&selected_distance)
                            .then_with(|| candidate.key.cmp(&selected.key))
                            .is_lt();
                        if candidate_is_better {
                            return Err(Error::InvalidProximityObject {
                                kind: "node",
                                reason: "nearest-representative invariant violated".to_owned(),
                            });
                        }
                    }
                }
            }
        }

        if node.kind != PhysicalNodeKind::OverflowDirectory {
            for entry in &node.entries {
                if super::vector::promotion_level(
                    &entry.key,
                    self.tree.config.hierarchy.log_chunk_size,
                    self.tree.config.hierarchy.level_hash_seed,
                ) < node.level
                {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "entry appears above its deterministic promotion level".to_owned(),
                    });
                }
            }
        }

        let verified = if node.kind.is_logical_leaf(node.level) {
            let mut points = Vec::with_capacity(node.entries.len());
            for entry in &node.entries {
                if !state.seen_leaf_keys.insert(entry.key.clone()) {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "duplicate leaf identity".to_owned(),
                    });
                }
                let record =
                    state
                        .records
                        .get(&entry.key)
                        .ok_or_else(|| Error::InvalidProximityObject {
                            kind: "node",
                            reason: "leaf key is absent from exact directory".to_owned(),
                        })?;
                if record.vector.as_slice() != entry.vector.inline()? {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "leaf vector disagrees with exact directory".to_owned(),
                    });
                }
                points.push((entry.key.clone(), entry.vector.inline()?.to_vec()));
            }
            VerifiedSubtree::from_points(node.entries.len() as u64, points)
        } else {
            let mut count = 0u64;
            let mut points = Vec::new();
            let mut minimum: Option<Vec<u8>> = None;
            let mut maximum: Option<Vec<u8>> = None;
            for entry in &node.entries {
                let child = entry
                    .child
                    .as_ref()
                    .ok_or_else(|| Error::InvalidProximityObject {
                        kind: "node",
                        reason: "internal entry has no child".to_owned(),
                    })?;
                let child_verified = self.verify_node(
                    child,
                    Some(if node.kind == PhysicalNodeKind::OverflowDirectory {
                        node.level
                    } else {
                        node.level - 1
                    }),
                    if node.kind == PhysicalNodeKind::OverflowDirectory {
                        parent
                    } else {
                        Some((entry, &node.entries))
                    },
                    state,
                )?;
                if child_verified.count != entry.child_count {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "child count summary mismatch".to_owned(),
                    });
                }
                if child_verified.minimum.as_deref() != Some(entry.min_key.as_slice())
                    || child_verified.maximum.as_deref() != Some(entry.max_key.as_slice())
                {
                    return Err(Error::InvalidProximityObject {
                        kind: "node",
                        reason: "child key-bound summary mismatch".to_owned(),
                    });
                }
                for (_, vector) in &child_verified.points {
                    let required = super::distance::euclidean_radius_up(
                        score(
                            super::DistanceMetric::L2Squared,
                            entry.vector.inline()?,
                            vector,
                        ),
                        0.0,
                    );
                    if required > entry.covering_radius {
                        return Err(Error::InvalidProximityObject {
                            kind: "node",
                            reason: "covering-radius summary is not conservative".to_owned(),
                        });
                    }
                }
                count = count.checked_add(child_verified.count).ok_or_else(|| {
                    Error::InvalidProximityObject {
                        kind: "node",
                        reason: "subtree count overflow".to_owned(),
                    }
                })?;
                if minimum.as_ref().map_or(true, |key| entry.min_key < *key) {
                    minimum = Some(entry.min_key.clone());
                }
                if maximum.as_ref().map_or(true, |key| entry.max_key > *key) {
                    maximum = Some(entry.max_key.clone());
                }
                points.extend(child_verified.points);
            }
            VerifiedSubtree {
                count,
                minimum,
                maximum,
                points,
            }
        };
        if verified.count != node.subtree_count {
            return Err(Error::InvalidProximityObject {
                kind: "node",
                reason: "subtree count mismatch".to_owned(),
            });
        }
        Ok(verified)
    }
}

struct VerificationState<'a> {
    records: &'a BTreeMap<Vec<u8>, ProximityRecord>,
    seen_nodes: HashSet<Cid>,
    seen_external_vectors: HashSet<Cid>,
    seen_scalar_quantizers: HashSet<Cid>,
    seen_leaf_keys: HashSet<Vec<u8>>,
    summary: ProximityVerification,
}

struct VerifiedSubtree {
    count: u64,
    minimum: Option<Vec<u8>>,
    maximum: Option<Vec<u8>>,
    points: Vec<(Vec<u8>, Vec<f32>)>,
}

impl VerifiedSubtree {
    fn from_points(count: u64, points: Vec<(Vec<u8>, Vec<f32>)>) -> Self {
        let minimum = points.iter().map(|(key, _)| key).min().cloned();
        let maximum = points.iter().map(|(key, _)| key).max().cloned();
        Self {
            count,
            minimum,
            maximum,
            points,
        }
    }
}

fn remaining_budget(budget: &SearchBudget, used: &ProximitySearchStats) -> SearchBudget {
    SearchBudget {
        max_nodes: budget
            .max_nodes
            .map(|limit| limit.saturating_sub(used.nodes_read)),
        max_committed_bytes: budget
            .max_committed_bytes
            .map(|limit| limit.saturating_sub(used.committed_bytes)),
        max_distance_evaluations: budget.max_distance_evaluations.map(|limit| {
            limit.saturating_sub(
                used.distance_evaluations
                    .saturating_add(used.quantized_distance_evaluations),
            )
        }),
        max_frontier_entries: budget.max_frontier_entries,
    }
}

fn search_budget_exhausted(budget: &SearchBudget, used: &ProximitySearchStats) -> bool {
    budget
        .max_nodes
        .is_some_and(|limit| used.nodes_read >= limit)
        || budget
            .max_committed_bytes
            .is_some_and(|limit| used.committed_bytes >= limit)
        || budget.max_distance_evaluations.is_some_and(|limit| {
            used.distance_evaluations
                .saturating_add(used.quantized_distance_evaluations)
                >= limit
        })
}

fn add_search_stats(total: &mut ProximitySearchStats, added: &ProximitySearchStats) {
    total.levels_visited = total.levels_visited.saturating_add(added.levels_visited);
    total.nodes_read = total.nodes_read.saturating_add(added.nodes_read);
    total.bytes_read = total.bytes_read.saturating_add(added.bytes_read);
    total.physical_bytes_read = total
        .physical_bytes_read
        .saturating_add(added.physical_bytes_read);
    total.committed_bytes = total.committed_bytes.saturating_add(added.committed_bytes);
    total.distance_evaluations = total
        .distance_evaluations
        .saturating_add(added.distance_evaluations);
    total.quantized_distance_evaluations = total
        .quantized_distance_evaluations
        .saturating_add(added.quantized_distance_evaluations);
    total.reranked_candidates = total
        .reranked_candidates
        .saturating_add(added.reranked_candidates);
    total.frontier_peak = total.frontier_peak.max(added.frontier_peak);
    total.candidate_handles_peak = total
        .candidate_handles_peak
        .max(added.candidate_handles_peak);
    total.candidate_retained_bytes_peak = total
        .candidate_retained_bytes_peak
        .max(added.candidate_retained_bytes_peak);
}

pub(super) fn encoded_vector_matches(
    encoded: super::storage::EncodedVectorRef<'_>,
    expected: &[f32],
) -> bool {
    encoded.dimensions as usize == expected.len()
        && encoded
            .bytes
            .chunks_exact(4)
            .zip(expected)
            .all(|(bytes, expected)| {
                u32::from_le_bytes(bytes.try_into().expect("validated vector component"))
                    == expected.to_bits()
            })
}

pub(super) fn encoded_vectors_equal(
    left: super::storage::EncodedVectorRef<'_>,
    right: super::storage::EncodedVectorRef<'_>,
) -> bool {
    left.dimensions == right.dimensions && left.bytes == right.bytes
}

fn load_content<S: Store>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error> {
    let bytes = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
        .ok_or_else(|| Error::NotFound(cid.clone()))?;
    let actual = Cid::from_bytes(&bytes);
    if actual != *cid {
        return Err(Error::CidMismatch {
            expected: cid.clone(),
            actual,
        });
    }
    Ok(bytes)
}

fn publish_maintenance_content<S: Store>(store: &S, cid: &Cid, bytes: &[u8]) -> Result<(), Error> {
    let entries = [(cid.as_bytes(), bytes)];
    store
        .publish_nodes(NodePublication::new(
            &entries,
            PublicationOrigin::Maintenance,
        ))
        .map_err(|error| Error::Store(Box::new(error)))
}

fn distance_budget_exhausted(request: &SearchRequest<'_>, stats: &ProximitySearchStats) -> bool {
    request
        .budget
        .max_distance_evaluations
        .is_some_and(|maximum| {
            stats
                .distance_evaluations
                .saturating_add(stats.quantized_distance_evaluations)
                >= maximum
        })
}

fn put_missing_nodes<S: Store>(store: &S, nodes: &[(Cid, Vec<u8>)]) -> Result<usize, Error> {
    let keys: Vec<_> = nodes.iter().map(|(cid, _)| cid.as_bytes()).collect();
    let existing = store
        .batch_get_ordered_unique(&keys)
        .map_err(|error| Error::Store(Box::new(error)))?;
    for ((expected, _), value) in nodes.iter().zip(&existing) {
        if let Some(bytes) = value {
            let actual = Cid::from_bytes(bytes);
            if actual != *expected {
                return Err(Error::CidMismatch {
                    expected: expected.clone(),
                    actual,
                });
            }
        }
    }
    let missing: Vec<_> = nodes
        .iter()
        .zip(existing)
        .filter_map(|((cid, bytes), value)| {
            value
                .is_none()
                .then_some((cid.as_bytes(), bytes.as_slice()))
        })
        .collect();
    if !missing.is_empty() {
        store
            .publish_nodes(NodePublication::new(
                &missing,
                PublicationOrigin::Maintenance,
            ))
            .map_err(|error| Error::Store(Box::new(error)))?;
    }
    Ok(missing.len())
}

fn apply_directory_stats(target: &mut ProximityMutationStats, source: SpliceStats) {
    target.directory_entries_scanned = source.entries_scanned;
    target.directory_nodes_read = source.nodes_read;
    target.directory_nodes_rebuilt = source.nodes_rebuilt;
    target.directory_nodes_written = source.nodes_written;
    target.directory_nodes_reused = source.nodes_reused;
    target.directory_levels_rebuilt = source.levels_rebuilt;
    target.directory_right_edge_rebuilt = source.right_edge_rebuilt;
}

fn validate_mutations(
    mutations: impl IntoIterator<Item = ProximityMutation>,
) -> Result<Vec<ProximityMutation>, Error> {
    let mut mutations: Vec<_> = mutations.into_iter().collect();
    mutations.sort_by(|left, right| left.key.cmp(&right.key));
    for pair in mutations.windows(2) {
        if pair[0].key == pair[1].key {
            return Err(Error::DuplicateProximityKey {
                key: pair[0].key.clone(),
            });
        }
    }
    Ok(mutations)
}

fn apply_mutations(
    records: &mut BTreeMap<Vec<u8>, ProximityRecord>,
    mutations: &[ProximityMutation],
    config: &ProximityConfig,
) -> Result<(), Error> {
    for mutation in mutations {
        match &mutation.value {
            Some((vector, value)) => {
                records.insert(
                    mutation.key.clone(),
                    ProximityRecord {
                        key: mutation.key.clone(),
                        vector: prepare_vector(config.metric, vector, config.dimensions)?,
                        value: value.clone(),
                    },
                );
            }
            None => {
                records.remove(&mutation.key);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prolly::proximity::distance::{query_kernel_calls, reset_query_kernel_calls};
    use crate::prolly::store::MemStore;

    fn config() -> ProximityConfig {
        let mut config = ProximityConfig::new(1);
        config.hierarchy.log_chunk_size = 1;
        config.hierarchy.level_hash_seed = 7;
        config.overflow.max_page_bytes = 256 * 1024;
        config
    }

    fn two_representative_map() -> (Arc<MemStore>, ProximityMap<Arc<MemStore>>) {
        let keys: Vec<_> = (0..10_000)
            .map(|index| format!("candidate-{index}").into_bytes())
            .filter(|key| promotion_level(key, 1, 7) == 1)
            .take(2)
            .collect();
        assert_eq!(keys.len(), 2);
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            config(),
            keys.into_iter()
                .enumerate()
                .map(|(index, key)| ProximityRecord {
                    key,
                    vector: vec![index as f32],
                    value: Vec::new(),
                }),
        )
        .unwrap();
        (store, map)
    }

    #[test]
    fn exact_read_lease_retains_and_validates_the_stored_record() {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store,
            ProximityConfig::new(2),
            [ProximityRecord {
                key: b"key".to_vec(),
                vector: vec![1.0, 2.0],
                value: b"value".to_vec(),
            }],
        )
        .unwrap();
        let mut read = map.read().unwrap();
        let lease = read.get_lease(b"key").unwrap().unwrap();
        let stored = StoredRecordRef::decode(lease.as_bytes().unwrap(), 2).unwrap();
        assert_eq!(
            ProximityVectorRef::from_encoded(stored.vector).to_vec(),
            vec![1.0, 2.0]
        );
        assert_eq!(stored.value, b"value");
        assert!(read.get_lease(b"missing").unwrap().is_none());
    }

    #[test]
    fn construction_and_mutation_never_enter_a_query_kernel() {
        reset_query_kernel_calls();
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store,
            config(),
            (0..64).map(|index| ProximityRecord {
                key: format!("key-{index:03}").into_bytes(),
                vector: vec![index as f32],
                value: Vec::new(),
            }),
        )
        .unwrap();
        let (map, _) = map
            .mutate_batch([ProximityMutation {
                key: b"key-017".to_vec(),
                value: Some((vec![17.25], b"updated".to_vec())),
            }])
            .unwrap();
        assert_eq!(query_kernel_calls(), 0);

        let mut request = SearchRequest::exact(&[17.0], 3);
        request.kernel = super::super::QueryKernel::SimdDeterministic;
        map.search(request).unwrap();
        assert!(query_kernel_calls() > 0);
    }

    fn publish_root_descriptor(
        store: &Arc<MemStore>,
        map: &ProximityMap<Arc<MemStore>>,
        root: ProximityNode,
    ) -> Cid {
        let root_bytes = root.encode().unwrap();
        let root_cid = Cid::from_bytes(&root_bytes);
        store.put(root_cid.as_bytes(), &root_bytes).unwrap();

        let descriptor_bytes = store.get(map.tree.descriptor.as_bytes()).unwrap().unwrap();
        let mut descriptor = Descriptor::decode(&descriptor_bytes).unwrap();
        descriptor.proximity_root = root_cid;
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        store
            .put(descriptor_cid.as_bytes(), &descriptor_bytes)
            .unwrap();
        descriptor_cid
    }

    fn publish_replacement_root(
        store: &Arc<MemStore>,
        map: &ProximityMap<Arc<MemStore>>,
        root: ProximityNode,
    ) -> ProximityMap<Arc<MemStore>> {
        let descriptor_cid = publish_root_descriptor(store, map, root);
        ProximityMap::load(store.clone(), descriptor_cid).unwrap()
    }

    #[test]
    fn verify_rejects_a_leaf_vector_that_disagrees_with_the_exact_directory() {
        let store = Arc::new(MemStore::new());
        let mut leaf_config = config();
        leaf_config.hierarchy.log_chunk_size = 63;
        let map = ProximityMap::build(
            store.clone(),
            leaf_config,
            [ProximityRecord {
                key: b"key".to_vec(),
                vector: vec![1.0],
                value: Vec::new(),
            }],
        )
        .unwrap();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.entries[0].vector = super::super::storage::VectorRef::Inline(vec![2.0]);
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "leaf vector disagrees with exact directory"
        ));
    }

    #[test]
    fn verify_rejects_repeated_child_ownership() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        assert_eq!(root.level, 1);
        assert_eq!(root.entries.len(), 2);
        root.entries[1].child = root.entries[0].child.clone();
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "cycle or repeated child ownership"
        ));
    }

    #[test]
    fn verify_rejects_an_invalid_child_level() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.entries[0].child = Some(map.tree.proximity_root.clone());
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "unexpected logical level"
        ));
    }

    #[test]
    fn verify_rejects_a_representative_below_its_node_level() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let replacement = (0..10_000)
            .map(|index| format!("!invalid-{index}").into_bytes())
            .find(|key| promotion_level(key, 1, 7) == 0 && key < &root.entries[1].key)
            .unwrap();
        root.entries[0].key = replacement.clone();
        root.entries[0].min_key = replacement;
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "entry appears above its deterministic promotion level"
        ));
    }

    #[test]
    fn verify_rejects_a_non_nearest_parent_route() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let first = root.entries[0].child.clone();
        root.entries[0].child = root.entries[1].child.clone();
        root.entries[1].child = first;
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "nearest-representative invariant violated"
        ));
    }

    #[test]
    fn load_rejects_a_root_subtree_count_that_disagrees_with_the_descriptor() {
        let (store, map) = two_representative_map();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        root.subtree_count += 1;
        root.entries[0].child_count += 1;
        let descriptor = publish_root_descriptor(&store, &map, root);

        assert!(matches!(
            ProximityMap::load(store, descriptor),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "record count disagrees with proximity root"
        ));
    }

    #[test]
    fn verify_rejects_a_non_conservative_covering_radius() {
        let store = Arc::new(MemStore::new());
        let map = ProximityMap::build(
            store.clone(),
            config(),
            (0..128).map(|index| ProximityRecord {
                key: format!("radius-{index:04}").into_bytes(),
                vector: vec![index as f32],
                value: Vec::new(),
            }),
        )
        .unwrap();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let entry = root
            .entries
            .iter_mut()
            .find(|entry| entry.covering_radius > 0.0)
            .expect("test hierarchy has a nontrivial cluster");
        entry.covering_radius = 0.0;
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { reason, .. })
                if reason == "covering-radius summary is not conservative"
        ));
    }

    #[test]
    fn verify_rejects_a_scalar_quantizer_that_disagrees_with_its_node() {
        let store = Arc::new(MemStore::new());
        let mut quantized_config = config();
        quantized_config.scalar_quantization =
            Some(super::super::ScalarQuantizationConfig { group_size: 1 });
        let map = ProximityMap::build(
            store.clone(),
            quantized_config,
            (0..64).map(|index| ProximityRecord {
                key: format!("quantized-{index:03}").into_bytes(),
                vector: vec![index as f32],
                value: Vec::new(),
            }),
        )
        .unwrap();
        let bytes = store
            .get(map.tree.proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        let mut root = ProximityNode::decode(&bytes, 1).unwrap();
        let fake_vectors = vec![vec![999.0]; root.entries.len()];
        let fake_refs: Vec<_> = fake_vectors.iter().map(Vec::as_slice).collect();
        let fake = ScalarQuantized::build(&fake_refs, 1, 1).unwrap();
        let fake_bytes = fake.encode().unwrap();
        let fake_cid = Cid::from_bytes(&fake_bytes);
        store.put(fake_cid.as_bytes(), &fake_bytes).unwrap();
        root.quantizer = Some(fake_cid);
        let corrupt = publish_replacement_root(&store, &map, root);

        assert!(matches!(
            corrupt.verify(),
            Err(Error::InvalidProximityObject { kind: "quantizer", reason })
                if reason.contains("disagree")
        ));
    }
}
