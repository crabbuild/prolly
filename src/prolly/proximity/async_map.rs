use super::super::builder::AsyncSortedBatchBuilder;
use super::super::cid::Cid;
use super::super::config::Config;
use super::super::content_graph::{walk_content_graph_async, ContentGraphLimits, TypedContentRoot};
use super::super::error::{Error, Mutation as TreeMutation};
use super::super::read::{AsyncReadSession, OwnedValueLease, ScanOutcome};
use super::super::store::{AsyncStore, NodePublication, PublicationOrigin};
use super::super::write::WriteStats;
use super::super::AsyncProlly;
use super::builder::{build_hierarchy_parallel, IndexedRecord};
use super::distance::score;
use super::map::{apply_mutations, validate_mutations};
use super::mutation::{mutate_hierarchy_async, LogicalEdit};
use super::storage::quantized::ScalarQuantized;
use super::storage::vector::ExternalVector;
use super::storage::{Descriptor, PhysicalNodeKind, ProximityEntry, ProximityNode, StoredRecord};
use super::storage::{StoredRecordRef, VectorRef};
use super::vector::promotion_level;
use super::{
    AsyncProximityMap, BuildParallelism, ExactProximityRecord, ProximityBuildStats,
    ProximityConfig, ProximityMembershipProof, ProximityMutation, ProximityMutationStats,
    ProximityRecord, ProximityRecordRef, ProximitySearchProof, ProximityStructuralProof,
    ProximityTree, ProximityVectorRef, ProximityVerification, SearchBackend, SearchRequest,
};
use futures_util::future::LocalBoxFuture;
use std::collections::{BTreeMap, HashSet};
use std::ops::ControlFlow;

/// Runtime-only limits for canonical async proximity construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AsyncProximityBuildOptions {
    pub parallelism: BuildParallelism,
    pub max_records: Option<usize>,
    pub max_owned_bytes: Option<usize>,
    pub publication_batch_items: usize,
}

impl Default for AsyncProximityBuildOptions {
    fn default() -> Self {
        Self {
            parallelism: BuildParallelism::default(),
            max_records: None,
            max_owned_bytes: None,
            publication_batch_items: 1_024,
        }
    }
}

impl AsyncProximityBuildOptions {
    fn validate(&self) -> Result<(), Error> {
        if self.max_records == Some(0)
            || self.max_owned_bytes == Some(0)
            || self.publication_batch_items == 0
        {
            return Err(Error::InvalidProximityConfig {
                reason: "async build limits must be greater than zero".to_owned(),
            });
        }
        Ok(())
    }
}

/// Reusable exact-read context over one immutable async proximity map.
pub struct AsyncProximityReadSession<'map, S: AsyncStore> {
    directory: AsyncReadSession<'map, 'map, S>,
    dimensions: u32,
}

impl<S> AsyncProximityReadSession<'_, S>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    pub async fn get_with<R>(
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
            })
            .await?
            .transpose()
    }

    pub async fn get_lease(&mut self, key: &[u8]) -> Result<Option<OwnedValueLease>, Error> {
        let lease = self.directory.get_lease(key).await?;
        if let Some(lease) = &lease {
            StoredRecordRef::decode(lease.as_bytes()?, self.dimensions)?;
        }
        Ok(lease)
    }

    pub async fn contains_key(&mut self, key: &[u8]) -> Result<bool, Error> {
        Ok(self.get_with(key, |_| ()).await?.is_some())
    }

    pub async fn scan_records(
        &mut self,
        mut visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_records_until(|key, record| {
                visit(key, record);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_records_until<B>(
        &mut self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_records_range_until(&[], None, visit).await
    }

    pub async fn scan_records_range_until<B>(
        &mut self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let dimensions = self.dimensions;
        let outcome = self
            .directory
            .scan_range_until(start, end, |entry| {
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
            })
            .await?;
        match outcome.break_value {
            Some(Ok(value)) => Ok(ScanOutcome::stopped(outcome.visited, value)),
            Some(Err(error)) => Err(error),
            None => Ok(ScanOutcome::complete(outcome.visited)),
        }
    }
}

impl<S> AsyncProximityMap<S>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    /// Build and publish a canonical proximity map directly through AsyncStore.
    pub async fn build(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
    ) -> Result<Self, Error> {
        Self::build_with_parallelism(store, config, records, BuildParallelism::default())
            .await
            .map(|(map, _)| map)
    }

    /// Build through AsyncStore with an explicit deterministic CPU worker limit.
    pub async fn build_with_parallelism(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
        parallelism: BuildParallelism,
    ) -> Result<(Self, ProximityBuildStats), Error> {
        Self::build_with_options(
            store,
            config,
            records,
            AsyncProximityBuildOptions {
                parallelism,
                ..Default::default()
            },
        )
        .await
    }

    /// Build with explicit memory, record, CPU, and provider batch limits.
    pub async fn build_with_options(
        store: S,
        config: ProximityConfig,
        records: impl IntoIterator<Item = ProximityRecord>,
        options: AsyncProximityBuildOptions,
    ) -> Result<(Self, ProximityBuildStats), Error> {
        config.validate()?;
        options.validate()?;
        let source = records;
        let mut records = Vec::new();
        let mut owned_bytes = 0usize;
        for record in source.into_iter() {
            if let Some(limit) = options.max_records {
                if records.len() >= limit {
                    return Err(async_build_limit("records", limit, records.len() + 1));
                }
            }
            let vector_bytes = record
                .vector
                .len()
                .checked_mul(std::mem::size_of::<f32>())
                .ok_or_else(|| async_build_limit("owned_bytes", usize::MAX, usize::MAX))?;
            owned_bytes = owned_bytes
                .checked_add(record.key.len())
                .and_then(|total| total.checked_add(record.value.len()))
                .and_then(|total| total.checked_add(vector_bytes))
                .and_then(|total| total.checked_add(std::mem::size_of::<ProximityRecord>()))
                .ok_or_else(|| async_build_limit("owned_bytes", usize::MAX, usize::MAX))?;
            if let Some(limit) = options.max_owned_bytes {
                if owned_bytes > limit {
                    return Err(async_build_limit("owned_bytes", limit, owned_bytes));
                }
            }
            records.push(record);
        }
        records.sort_by(|left, right| left.key.cmp(&right.key));
        for pair in records.windows(2) {
            if pair[0].key == pair[1].key {
                return Err(Error::DuplicateProximityKey {
                    key: pair[0].key.clone(),
                });
            }
        }

        let directory_config = Config::default();
        let mut directory_builder = AsyncSortedBatchBuilder::new_with_origin(
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
            directory_builder.add(record.key, stored.encode()).await?;
        }
        let directory_tree = directory_builder.build().await?;
        let hierarchy = build_hierarchy_parallel(&indexed, &config, options.parallelism.threads())?;
        let objects_written =
            put_missing_nodes_bounded(&store, &hierarchy.nodes, options.publication_batch_items)
                .await?;

        let descriptor = Descriptor {
            config: config.clone(),
            count: indexed.len() as u64,
            directory: directory_tree.clone(),
            proximity_root: hierarchy.root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        publish_content(&store, &descriptor_cid, &descriptor_bytes).await?;

        Ok((
            Self {
                directory: AsyncProlly::new(store.clone(), directory_config),
                store,
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root: hierarchy.root,
                    descriptor: descriptor_cid,
                    count: indexed.len() as u64,
                    config,
                },
            },
            ProximityBuildStats {
                distance_evaluations: hierarchy.distance_evaluations,
                proximity_objects: hierarchy.nodes.len(),
                proximity_objects_written: objects_written,
            },
        ))
    }

    /// Clear retained ordered-directory nodes. Search-runtime caches are owned separately.
    pub fn clear_content_cache(&self) {
        self.directory.clear_cache();
    }

    pub async fn read(&self) -> Result<AsyncProximityReadSession<'_, S>, Error> {
        Ok(AsyncProximityReadSession {
            directory: self.directory.read(&self.tree.directory).await?,
            dimensions: self.tree.config.dimensions,
        })
    }

    pub async fn get(&self, key: &[u8]) -> Result<Option<ExactProximityRecord>, Error> {
        self.get_with(key, |record| record.to_owned()).await
    }

    pub async fn get_with<R>(
        &self,
        key: &[u8],
        read: impl for<'record> FnOnce(ProximityRecordRef<'record>) -> R,
    ) -> Result<Option<R>, Error> {
        self.read().await?.get_with(key, read).await
    }

    pub async fn contains_key(&self, key: &[u8]) -> Result<bool, Error> {
        self.read().await?.contains_key(key).await
    }

    pub async fn scan_records(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>),
    ) -> Result<u64, Error> {
        self.read().await?.scan_records(visit).await
    }

    pub async fn scan_records_until<B>(
        &self,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read().await?.scan_records_until(visit).await
    }

    pub async fn scan_records_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'record> FnMut(&[u8], ProximityRecordRef<'record>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.read()
            .await?
            .scan_records_range_until(start, end, visit)
            .await
    }

    /// Apply mutations through a clean canonical rebuild oracle.
    pub async fn rebuild_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<Self, Error> {
        let mutations = validate_mutations(mutations)?;
        let mut records = self.collect_records_from(&self.tree.directory).await?;
        apply_mutations(&mut records, &mutations, &self.tree.config)?;
        Self::build(
            self.store.clone(),
            self.tree.config.clone(),
            records.into_values(),
        )
        .await
    }

    /// Canonically mutate the exact directory and rebuild derived routing only when vectors change.
    pub async fn mutate_batch(
        &self,
        mutations: impl IntoIterator<Item = ProximityMutation>,
    ) -> Result<(Self, ProximityMutationStats), Error> {
        let mutations = validate_mutations(mutations)?;
        if mutations.is_empty() {
            return Ok((
                Self::load(self.store.clone(), self.tree.descriptor.clone()).await?,
                Default::default(),
            ));
        }

        let keys: Vec<_> = mutations
            .iter()
            .map(|mutation| mutation.key.clone())
            .collect();
        let old_values = self.directory.get_many(&self.tree.directory, &keys).await?;
        let mut directory_mutations = Vec::with_capacity(mutations.len());
        let mut count = self.tree.count;
        let mut logical_edits = Vec::new();
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
                (None, Some(_)) => count = count.checked_add(1).ok_or_else(count_overflow)?,
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

        let (directory_tree, directory_stats) = self
            .directory
            .batch_with_write_stats_origin(
                &self.tree.directory,
                directory_mutations,
                PublicationOrigin::Maintenance,
            )
            .await?;
        let mut stats = mutation_stats_from_write(directory_stats);

        let proximity_root = if logical_edits.is_empty() {
            stats.nodes_reused = 1;
            self.tree.proximity_root.clone()
        } else {
            let root_bytes =
                load_bounded_node(&self.store, &self.tree.proximity_root, &self.tree.config)
                    .await?;
            let old_root = ProximityNode::decode(&root_bytes, self.tree.config.dimensions)?;
            let max_edit_level = logical_edits
                .iter()
                .map(|edit| edit.level)
                .max()
                .unwrap_or(0);
            let (root, nodes, proximity_stats) =
                if old_root.entries.is_empty() || max_edit_level >= old_root.level {
                    let records = self.collect_records_from(&directory_tree).await?;
                    let indexed: Vec<_> = records
                        .values()
                        .map(|record| IndexedRecord {
                            key: record.key.clone(),
                            vector: record.vector.clone(),
                        })
                        .collect();
                    let hierarchy = build_hierarchy_parallel(&indexed, &self.tree.config, 1)?;
                    let proximity_stats = ProximityMutationStats {
                        records_rebuilt: indexed.len(),
                        distance_evaluations: hierarchy.distance_evaluations,
                        full_proximity_rebuild: true,
                        ..Default::default()
                    };
                    (hierarchy.root, hierarchy.nodes, proximity_stats)
                } else {
                    let local = mutate_hierarchy_async(
                        &self.store,
                        &self.tree.proximity_root,
                        &self.tree.config,
                        &logical_edits,
                    )
                    .await?;
                    (local.root, local.nodes, local.stats)
                };
            let pending = nodes.len();
            let written = put_missing_nodes(&self.store, &nodes).await?;
            stats.nodes_read = proximity_stats.nodes_read;
            stats.nodes_written = written;
            stats.nodes_reused = proximity_stats
                .nodes_reused
                .saturating_add(pending.saturating_sub(written));
            stats.records_rebuilt = proximity_stats.records_rebuilt;
            stats.distance_evaluations = proximity_stats.distance_evaluations;
            stats.full_proximity_rebuild = proximity_stats.full_proximity_rebuild;
            root
        };

        let descriptor = Descriptor {
            config: self.tree.config.clone(),
            count,
            directory: directory_tree.clone(),
            proximity_root: proximity_root.clone(),
        };
        let descriptor_bytes = descriptor.encode();
        let descriptor_cid = Cid::from_bytes(&descriptor_bytes);
        publish_content(&self.store, &descriptor_cid, &descriptor_bytes).await?;
        Ok((
            Self {
                directory: AsyncProlly::new(self.store.clone(), directory_tree.config.clone()),
                store: self.store.clone(),
                tree: ProximityTree {
                    directory: directory_tree,
                    proximity_root,
                    descriptor: descriptor_cid,
                    count,
                    config: self.tree.config.clone(),
                },
            },
            stats,
        ))
    }

    /// Prove exact presence or absence through the asynchronous directory engine.
    pub async fn prove_membership(&self, key: &[u8]) -> Result<ProximityMembershipProof, Error> {
        let descriptor_bytes = load_content(&self.store, &self.tree.descriptor).await?;
        let directory_proof = self.directory.prove_key(&self.tree.directory, key).await?;
        let record_bytes = directory_proof.verify().value;
        Ok(ProximityMembershipProof {
            descriptor: self.tree.descriptor.clone(),
            descriptor_bytes,
            directory_proof,
            record_bytes,
        })
    }

    /// Capture the authenticated typed closure through AsyncStore.
    pub async fn prove_structure(
        &self,
        limits: &ContentGraphLimits,
    ) -> Result<ProximityStructuralProof, Error> {
        let root = TypedContentRoot::proximity_descriptor(self.tree.descriptor.clone());
        let walk = walk_content_graph_async(&self.store, &[root], limits).await?;
        Ok(ProximityStructuralProof {
            descriptor: self.tree.descriptor.clone(),
            objects: walk.objects,
        })
    }

    /// Prove deterministic native search execution directly from an async store.
    ///
    /// Deadline and cancellation controls are intentionally excluded because
    /// their completion point depends on wall-clock state and cannot be replayed.
    pub async fn prove_search(
        &self,
        request: SearchRequest<'_>,
        limits: &ContentGraphLimits,
    ) -> Result<ProximitySearchProof, Error> {
        if matches!(
            request.options.backend,
            SearchBackend::ProductQuantized | SearchBackend::Hnsw | SearchBackend::Composite
        ) {
            return Err(Error::InvalidProximityObject {
                kind: "proximity proof",
                reason: "explicit accelerator search proofs must be produced by that sidecar"
                    .to_owned(),
            });
        }
        let mut trace = Vec::new();
        let mut result = self
            .search_with_trace(request.clone(), Default::default(), Some(&mut trace))
            .await?;
        // Physical transport reads vary with cache and prefetch state; proofs
        // commit only to the deterministic logical execution statistics.
        result.stats.physical_bytes_read = 0;
        let source = self.prove_structure(limits).await?;
        super::proof::build_native_proof_from_source(source, request, result, trace, limits)
    }

    /// Traverse and validate the async descriptor, directory, hierarchy, and routing invariants.
    pub async fn verify(&self) -> Result<ProximityVerification, Error> {
        let records = self.collect_records_from(&self.tree.directory).await?;
        let root_bytes =
            load_bounded_node(&self.store, &self.tree.proximity_root, &self.tree.config).await?;
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
        let verified = self
            .verify_node(
                &self.tree.proximity_root,
                Some(root.level),
                None,
                &mut state,
            )
            .await?;
        if verified.count != self.tree.count || records.len() as u64 != self.tree.count {
            return Err(invalid("descriptor", "logical counts disagree"));
        }
        if state.seen_leaf_keys.len() != records.len()
            || records
                .keys()
                .any(|key| !state.seen_leaf_keys.contains(key))
        {
            return Err(invalid(
                "node",
                "leaf identities do not match the exact directory",
            ));
        }
        Ok(state.summary)
    }

    pub(crate) async fn collect_records_from(
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
            })
            .await?;
        if let Some(error) = decode_error {
            return Err(error);
        }
        Ok(records)
    }

    pub(crate) async fn collect_records(
        &self,
    ) -> Result<BTreeMap<Vec<u8>, ProximityRecord>, Error> {
        self.collect_records_from(&self.tree.directory).await
    }

    pub(crate) fn store_clone(&self) -> S {
        self.store.clone()
    }

    fn verify_node<'a>(
        &'a self,
        cid: &'a Cid,
        expected_level: Option<u8>,
        parent: Option<(ProximityEntry, Vec<ProximityEntry>)>,
        state: &'a mut VerificationState<'_>,
    ) -> LocalBoxFuture<'a, Result<VerifiedSubtree, Error>> {
        Box::pin(async move {
            if !state.seen_nodes.insert(cid.clone()) {
                return Err(invalid("node", "cycle or repeated child ownership"));
            }
            let bytes = load_bounded_node(&self.store, cid, &self.tree.config).await?;
            let mut node = ProximityNode::decode(&bytes, self.tree.config.dimensions)?;
            for entry in &node.entries {
                if let VectorRef::External(vector) = &entry.vector {
                    if state.seen_external_vectors.insert(vector.clone()) {
                        state.summary.external_vector_count += 1;
                    }
                }
            }
            resolve_external_vectors(&self.store, &mut node, &self.tree.config).await?;
            match (&self.tree.config.scalar_quantization, &node.quantizer) {
                (None, None) => {}
                (Some(config), Some(cid)) => {
                    let quantizer_bytes = load_content(&self.store, cid).await?;
                    let quantizer = ScalarQuantized::decode(&quantizer_bytes)?;
                    if quantizer.dimensions != self.tree.config.dimensions
                        || quantizer.group_size != config.group_size
                    {
                        return Err(invalid(
                            "quantizer",
                            "quantizer configuration disagrees with descriptor",
                        ));
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
                    return Err(invalid(
                        "quantizer",
                        "node quantizer presence disagrees with descriptor",
                    ))
                }
            }
            if expected_level != Some(node.level) {
                return Err(invalid("node", "unexpected logical level"));
            }
            state.summary.proximity_node_count += 1;
            match node.kind {
                PhysicalNodeKind::OverflowPage => state.summary.overflow_page_count += 1,
                PhysicalNodeKind::OverflowDirectory => state.summary.overflow_directory_count += 1,
                PhysicalNodeKind::Leaf | PhysicalNodeKind::Route => {}
            }
            state.summary.maximum_node_bytes = state.summary.maximum_node_bytes.max(bytes.len());

            if node.kind != PhysicalNodeKind::OverflowDirectory {
                if let Some((selected, candidates)) = &parent {
                    for entry in &node.entries {
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
                            if candidate_distance
                                .total_cmp(&selected_distance)
                                .then_with(|| candidate.key.cmp(&selected.key))
                                .is_lt()
                            {
                                return Err(invalid(
                                    "node",
                                    "nearest-representative invariant violated",
                                ));
                            }
                        }
                    }
                }
                for entry in &node.entries {
                    if super::vector::promotion_level(
                        &entry.key,
                        self.tree.config.hierarchy.log_chunk_size,
                        self.tree.config.hierarchy.level_hash_seed,
                    ) < node.level
                    {
                        return Err(invalid(
                            "node",
                            "entry appears above its deterministic promotion level",
                        ));
                    }
                }
            }

            let verified = if node.kind.is_logical_leaf(node.level) {
                let mut points = Vec::with_capacity(node.entries.len());
                for entry in &node.entries {
                    if !state.seen_leaf_keys.insert(entry.key.clone()) {
                        return Err(invalid("node", "duplicate leaf identity"));
                    }
                    let record = state.records.get(&entry.key).ok_or_else(|| {
                        invalid("node", "leaf key is absent from exact directory")
                    })?;
                    if record.vector.as_slice() != entry.vector.inline()? {
                        return Err(invalid(
                            "node",
                            "leaf vector disagrees with exact directory",
                        ));
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
                        .ok_or_else(|| invalid("node", "internal entry has no child"))?;
                    let child_verified = self
                        .verify_node(
                            child,
                            Some(if node.kind == PhysicalNodeKind::OverflowDirectory {
                                node.level
                            } else {
                                node.level - 1
                            }),
                            if node.kind == PhysicalNodeKind::OverflowDirectory {
                                parent.clone()
                            } else {
                                Some((entry.clone(), node.entries.clone()))
                            },
                            state,
                        )
                        .await?;
                    if child_verified.count != entry.child_count {
                        return Err(invalid("node", "child count summary mismatch"));
                    }
                    if child_verified.minimum.as_deref() != Some(entry.min_key.as_slice())
                        || child_verified.maximum.as_deref() != Some(entry.max_key.as_slice())
                    {
                        return Err(invalid("node", "child key-bound summary mismatch"));
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
                            return Err(invalid(
                                "node",
                                "covering-radius summary is not conservative",
                            ));
                        }
                    }
                    count = count
                        .checked_add(child_verified.count)
                        .ok_or_else(count_overflow)?;
                    if minimum.as_ref().is_none_or(|key| entry.min_key < *key) {
                        minimum = Some(entry.min_key.clone());
                    }
                    if maximum.as_ref().is_none_or(|key| entry.max_key > *key) {
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
                return Err(invalid("node", "subtree count mismatch"));
            }
            Ok(verified)
        })
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

async fn resolve_external_vectors<S: AsyncStore>(
    store: &S,
    node: &mut ProximityNode,
    config: &ProximityConfig,
) -> Result<(), Error>
where
    S::Error: Send + Sync,
{
    let mut positions = BTreeMap::<Cid, Vec<usize>>::new();
    for (index, entry) in node.entries.iter().enumerate() {
        if let VectorRef::External(cid) = &entry.vector {
            positions.entry(cid.clone()).or_default().push(index);
        }
    }
    if positions.is_empty() {
        return Ok(());
    }
    let cids: Vec<_> = positions.keys().cloned().collect();
    let keys: Vec<_> = cids.iter().map(Cid::as_bytes).collect();
    let values = store
        .batch_get_ordered_unique(&keys)
        .await
        .map_err(|error| Error::Store(Box::new(error)))?;
    if values.len() != cids.len() {
        return Err(invalid(
            "vector",
            "ordered batch read returned the wrong result count",
        ));
    }
    for ((cid, entry_positions), value) in positions.into_iter().zip(values) {
        let bytes = value.ok_or_else(|| Error::NotFound(cid.clone()))?;
        let actual = Cid::from_bytes(&bytes);
        if actual != cid {
            return Err(Error::CidMismatch {
                expected: cid,
                actual,
            });
        }
        let external = ExternalVector::decode(&bytes)?;
        if external.vector.len() != config.dimensions as usize {
            return Err(invalid("vector", "external vector dimension mismatch"));
        }
        for index in entry_positions {
            node.entries[index].vector = VectorRef::Inline(external.vector.clone());
        }
    }
    Ok(())
}

async fn load_bounded_node<S: AsyncStore>(
    store: &S,
    cid: &Cid,
    config: &ProximityConfig,
) -> Result<Vec<u8>, Error>
where
    S::Error: Send + Sync,
{
    let bytes = load_content(store, cid).await?;
    if bytes.len() > config.overflow.max_page_bytes as usize {
        return Err(invalid("node", "node exceeds descriptor max_node_bytes"));
    }
    Ok(bytes)
}

async fn load_content<S: AsyncStore>(store: &S, cid: &Cid) -> Result<Vec<u8>, Error>
where
    S::Error: Send + Sync,
{
    let bytes = store
        .get(cid.as_bytes())
        .await
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

async fn publish_content<S: AsyncStore>(store: &S, cid: &Cid, bytes: &[u8]) -> Result<(), Error>
where
    S::Error: Send + Sync,
{
    let entries = [(cid.as_bytes(), bytes)];
    store
        .publish_nodes(NodePublication::new(
            &entries,
            PublicationOrigin::Maintenance,
        ))
        .await
        .map_err(|error| Error::Store(Box::new(error)))
}

async fn put_missing_nodes<S: AsyncStore>(
    store: &S,
    nodes: &[(Cid, Vec<u8>)],
) -> Result<usize, Error>
where
    S::Error: Send + Sync,
{
    put_missing_nodes_bounded(store, nodes, 1_024).await
}

async fn put_missing_nodes_bounded<S: AsyncStore>(
    store: &S,
    nodes: &[(Cid, Vec<u8>)],
    batch_items: usize,
) -> Result<usize, Error>
where
    S::Error: Send + Sync,
{
    if batch_items == 0 {
        return Err(Error::InvalidProximityConfig {
            reason: "publication batch size must be greater than zero".to_owned(),
        });
    }
    let mut written = 0usize;
    for chunk in nodes.chunks(batch_items) {
        let keys: Vec<_> = chunk.iter().map(|(cid, _)| cid.as_bytes()).collect();
        let existing = store
            .batch_get_ordered_unique(&keys)
            .await
            .map_err(|error| Error::Store(Box::new(error)))?;
        if existing.len() != chunk.len() {
            return Err(invalid(
                "store",
                "ordered batch read returned the wrong result count",
            ));
        }
        for ((expected, _), value) in chunk.iter().zip(&existing) {
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
        let missing: Vec<_> = chunk
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
                .await
                .map_err(|error| Error::Store(Box::new(error)))?;
            written = written.saturating_add(missing.len());
        }
    }
    Ok(written)
}

fn mutation_stats_from_write(source: WriteStats) -> ProximityMutationStats {
    ProximityMutationStats {
        directory_entries_scanned: saturating_usize(source.entries_streamed),
        directory_nodes_read: saturating_usize(source.nodes_read),
        directory_nodes_rebuilt: saturating_usize(source.nodes_written),
        directory_nodes_written: saturating_usize(source.nodes_written),
        directory_nodes_reused: saturating_usize(source.nodes_reused),
        ..Default::default()
    }
}

fn saturating_usize(value: u64) -> usize {
    usize::try_from(value).unwrap_or(usize::MAX)
}

fn count_overflow() -> Error {
    invalid("mutation", "record count overflow")
}

fn async_build_limit(resource: &'static str, limit: usize, actual: usize) -> Error {
    Error::ProximityResourceLimitExceeded {
        resource,
        limit,
        actual,
    }
}

fn invalid(kind: &'static str, reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind,
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MemStore, ProximityMap, Store, SyncStoreAsAsync};
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Poll};

    fn block_on<F: Future>(future: F) -> F::Output {
        let waker = futures_util::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        let mut future = Box::pin(future);
        loop {
            match future.as_mut().poll(&mut context) {
                Poll::Ready(value) => return value,
                Poll::Pending => std::thread::yield_now(),
            }
        }
    }

    #[test]
    fn async_load_rejects_a_root_above_the_descriptor_byte_limit() {
        let store = Arc::new(MemStore::new());
        let records = (0usize..64).map(|index| ProximityRecord {
            key: format!("wide-key-{index:03}-{}", "x".repeat(24)).into_bytes(),
            vector: vec![index as f32, (index % 7) as f32],
            value: Vec::new(),
        });
        let mut build_config = ProximityConfig::new(2);
        build_config.hierarchy.log_chunk_size = 63;
        let map = ProximityMap::build(store.clone(), build_config, records).unwrap();
        let root_bytes = Store::get(&store, map.tree().proximity_root.as_bytes())
            .unwrap()
            .unwrap();
        assert!(root_bytes.len() > 512);

        let mut invalid_config = map.tree().config.clone();
        invalid_config.overflow.min_page_bytes = 128;
        invalid_config.overflow.target_page_bytes = 256;
        invalid_config.overflow.max_page_bytes = 512;
        invalid_config.vector_storage.inline_threshold_bytes = 256;
        let descriptor = Descriptor {
            config: invalid_config,
            count: map.tree().count,
            directory: map.tree().directory.clone(),
            proximity_root: map.tree().proximity_root.clone(),
        };
        let bytes = descriptor.encode();
        let cid = Cid::from_bytes(&bytes);
        Store::put(&store, cid.as_bytes(), &bytes).unwrap();

        assert!(block_on(AsyncProximityMap::load(SyncStoreAsAsync::new(store), cid)).is_err());
    }
}
