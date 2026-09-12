use super::catalog::{
    AcceleratorCatalogEntry, CatalogAcceleratorKind, Manifest as CatalogManifest,
};
use super::composite::config_fingerprint as composite_fingerprint;
use super::composite::{
    account_delta as account_composite_delta, account_shadow as account_composite_shadow,
    checked_add as checked_add_composite, composite_tree_config, enforce as enforce_composite,
    rebuild_reasons as composite_rebuild_reasons, CompositeAccelerator, CompositeAcceleratorConfig,
    CompositeBase, CompositeBaseKind, CompositeBuildLimits, CompositeBuildOutcome,
    CompositeBuildStats, FullRebuildReason, Manifest as CompositeManifest,
};
use super::hnsw::storage::config_fingerprint as hnsw_fingerprint;
use super::hnsw::storage::{graph_config, GraphNode, Manifest as HnswManifest};
use super::hnsw::{HnswBuildLimits, HnswBuildStats, HnswConfig, HnswIndex};
use super::pq::config_fingerprint as pq_fingerprint;
use super::pq::{
    code_tree_config, Manifest as PqManifest, ProductQuantizationBuildLimits,
    ProductQuantizationBuildStats, ProductQuantizationConfig, ProductQuantizer,
};
use super::turboquant::config_fingerprint as turboquant_fingerprint;
use super::turboquant::{
    codebook as turboquant_codebook, encode_vector_reusing, enforce_resource,
    invalid_object as invalid_turboquant_object, packed_len as turboquant_packed_len,
    resource_limit as turboquant_resource_limit,
    temporary_peak_bytes as turboquant_temporary_peak_bytes, turboquant_code_tree_config,
    validate_code_tree_root as validate_turboquant_code_tree_root, EncodingScratch,
    Manifest as TurboQuantManifest, StructuredRotation, TurboQuantizationBuildLimits,
    TurboQuantizationBuildStats, TurboQuantizationConfig, TurboQuantizer,
};
use super::validate_binding;
use crate::prolly::builder::AsyncSortedBatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    walk_content_graph, walk_content_graph_async, ContentGraphLimits, ContentObjectKind,
    TypedContentRoot,
};
use crate::prolly::error::{Diff, Error};
use crate::prolly::proximity::distance::canonical::sqrt_down;
use crate::prolly::proximity::storage::StoredRecord;
use crate::prolly::proximity::{
    BuildParallelism, DistanceMetric, ProductQuantizationQuality, ProximityMap, ProximityTree,
    TurboQuantizationQuality, TurboQuantizationVerification,
};
use crate::prolly::store::{AsyncStore, MemStore, NodePublication, PublicationOrigin};
use crate::prolly::tree::Tree;
use crate::prolly::AsyncProlly;
use rayon::prelude::*;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct AsyncHnswBuild {
    pub config: HnswConfig,
    pub limits: HnswBuildLimits,
}

#[derive(Clone, Debug)]
pub struct AsyncProductQuantizerBuild {
    pub config: ProductQuantizationConfig,
    pub parallelism: BuildParallelism,
    pub limits: ProductQuantizationBuildLimits,
}

#[derive(Clone, Debug)]
pub struct AsyncTurboQuantizerBuild {
    pub config: TurboQuantizationConfig,
    pub parallelism: BuildParallelism,
    pub limits: TurboQuantizationBuildLimits,
}

/// Async-store publication plan for canonical accelerator sidecars.
#[derive(Clone, Debug)]
pub struct AsyncAcceleratorBuildOptions {
    pub hnsw: Option<AsyncHnswBuild>,
    pub product_quantizer: Option<AsyncProductQuantizerBuild>,
    pub turboquant: Option<AsyncTurboQuantizerBuild>,
    pub publication_batch_items: usize,
    pub graph_limits: ContentGraphLimits,
}

impl Default for AsyncAcceleratorBuildOptions {
    fn default() -> Self {
        Self {
            hnsw: None,
            product_quantizer: None,
            turboquant: None,
            publication_batch_items: 1_024,
            graph_limits: ContentGraphLimits::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AsyncAcceleratorBuildStats {
    pub hnsw: Option<HnswBuildStats>,
    pub product_quantizer: Option<ProductQuantizationBuildStats>,
    pub turboquant: Option<TurboQuantizationBuildStats>,
    pub objects_published: usize,
    pub bytes_published: usize,
}

#[derive(Clone, Debug)]
pub struct AsyncCompositeBuildOptions {
    pub config: CompositeAcceleratorConfig,
    pub limits: CompositeBuildLimits,
    pub hnsw_limits: HnswBuildLimits,
    pub pq_parallelism: BuildParallelism,
    pub pq_limits: ProductQuantizationBuildLimits,
    pub turboquant_parallelism: BuildParallelism,
    pub turboquant_limits: TurboQuantizationBuildLimits,
    pub publication_batch_items: usize,
    pub graph_limits: ContentGraphLimits,
}

impl Default for AsyncCompositeBuildOptions {
    fn default() -> Self {
        Self {
            config: CompositeAcceleratorConfig::default(),
            limits: CompositeBuildLimits::default(),
            hnsw_limits: HnswBuildLimits::default(),
            pq_parallelism: BuildParallelism::serial(),
            pq_limits: ProductQuantizationBuildLimits::default(),
            turboquant_parallelism: BuildParallelism::serial(),
            turboquant_limits: TurboQuantizationBuildLimits::default(),
            publication_batch_items: 1_024,
            graph_limits: ContentGraphLimits::default(),
        }
    }
}

pub enum AsyncCompositeBuildOutcome {
    Composite {
        accelerator: Box<AsyncCompositeAccelerator>,
        stats: CompositeBuildStats,
        objects_published: usize,
        bytes_published: usize,
    },
    FullRebuildRequired {
        reasons: Vec<FullRebuildReason>,
        stats: CompositeBuildStats,
    },
}

/// Validated HNSW metadata for an async-only store.
#[derive(Clone)]
pub struct AsyncHnswIndex {
    pub(crate) manifest: Cid,
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: HnswConfig,
    pub(crate) graph_tree: Tree,
    pub(crate) entry_point: Vec<u8>,
    pub(crate) maximum_level: u8,
    pub(crate) canonical: bool,
}

impl AsyncHnswIndex {
    pub async fn load<S>(store: &S, manifest: Cid) -> Result<Self, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let bytes = load_content(store, &manifest).await?;
        let object = HnswManifest::decode(&bytes)?;
        object.config.validate()?;
        load_content(store, &object.graph_root).await?;
        let graph_tree = Tree {
            root: Some(object.graph_root),
            config: graph_config(),
        };
        let graph = AsyncProlly::new(store.clone(), graph_tree.config.clone());
        let entry = graph
            .get(&graph_tree, &object.entry_point)
            .await?
            .ok_or_else(|| invalid("HNSW entry point is absent from graph"))?;
        if GraphNode::decode(&entry)?.level != object.maximum_level {
            return Err(invalid("HNSW entry-point level disagrees with manifest"));
        }
        Ok(Self {
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            count: object.count,
            config: object.config,
            graph_tree,
            entry_point: object.entry_point,
            maximum_level: object.maximum_level,
            canonical: object.canonical,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }
    pub fn config(&self) -> &HnswConfig {
        &self.config
    }
    pub fn is_canonical(&self) -> bool {
        self.canonical
    }
}

/// Validated PQ metadata for an async-only store.
#[derive(Clone)]
pub struct AsyncProductQuantizer {
    pub(crate) manifest: Cid,
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: ProductQuantizationConfig,
    pub(crate) code_tree: Tree,
    pub(crate) codebooks: Vec<Vec<Vec<f32>>>,
    pub(crate) quality: ProductQuantizationQuality,
}

/// Validated TurboQuant metadata for an async-only store.
#[derive(Clone)]
pub struct AsyncTurboQuantizer {
    pub(crate) manifest: Cid,
    pub(crate) source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) count: u64,
    pub(crate) config: TurboQuantizationConfig,
    pub(crate) code_tree: Tree,
    pub(crate) quality: TurboQuantizationQuality,
    pub(crate) zero_vectors: u64,
}

#[derive(Clone)]
pub(crate) enum AsyncCompositeBase {
    Hnsw(AsyncHnswIndex),
    ProductQuantized(AsyncProductQuantizer),
    TurboQuantized(AsyncTurboQuantizer),
}

/// Validated composite metadata and base sidecar for an async-only store.
#[derive(Clone)]
pub struct AsyncCompositeAccelerator {
    pub(crate) manifest: Cid,
    pub(crate) current_source: Cid,
    pub(crate) base_source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) current_count: u64,
    pub(crate) base_count: u64,
    pub(crate) base: AsyncCompositeBase,
    pub(crate) delta_tree: Tree,
    pub(crate) shadow_tree: Tree,
    pub(crate) delta_count: u64,
    pub(crate) shadow_count: u64,
    pub(crate) config: CompositeAcceleratorConfig,
}

impl AsyncCompositeAccelerator {
    pub async fn build_from_hnsw<S>(
        base_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        current_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        base: &AsyncHnswIndex,
        options: AsyncCompositeBuildOptions,
    ) -> Result<AsyncCompositeBuildOutcome, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let (staging, staged_base, staged_current) =
            stage_source_pair(base_map, current_map).await?;
        let (rebuilt, _) = HnswIndex::build_with_limits(
            &staged_base,
            base.config.clone(),
            options.hnsw_limits.clone(),
        )?;
        if rebuilt.manifest_cid() != base.manifest_cid() {
            return Err(invalid("staged HNSW base is not canonical with async base"));
        }
        publish_composite_outcome(
            current_map,
            staging,
            CompositeAccelerator::build(
                &staged_base,
                &staged_current,
                CompositeBase::Hnsw(rebuilt),
                options.config,
                options.limits,
            )?,
            options.publication_batch_items,
            &options.graph_limits,
        )
        .await
    }

    pub async fn build_from_product_quantizer<S>(
        base_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        current_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        base: &AsyncProductQuantizer,
        options: AsyncCompositeBuildOptions,
    ) -> Result<AsyncCompositeBuildOutcome, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let (staging, staged_base, staged_current) =
            stage_source_pair(base_map, current_map).await?;
        let (rebuilt, _) = ProductQuantizer::build_with_limits(
            &staged_base,
            base.config.clone(),
            options.pq_parallelism,
            options.pq_limits.clone(),
        )?;
        if rebuilt.manifest_cid() != base.manifest_cid() {
            return Err(invalid("staged PQ base is not canonical with async base"));
        }
        publish_composite_outcome(
            current_map,
            staging,
            CompositeAccelerator::build(
                &staged_base,
                &staged_current,
                CompositeBase::ProductQuantized(rebuilt),
                options.config,
                options.limits,
            )?,
            options.publication_batch_items,
            &options.graph_limits,
        )
        .await
    }

    pub async fn build_from_turboquant<S>(
        base_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        current_map: &crate::prolly::proximity::AsyncProximityMap<S>,
        base: &AsyncTurboQuantizer,
        options: AsyncCompositeBuildOptions,
    ) -> Result<AsyncCompositeBuildOutcome, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        options.config.validate()?;
        options.limits.validate()?;
        // Validate every caller-provided TurboQuant option before consulting
        // store residency. Otherwise the same logical composite request could
        // accept malformed limits on the native co-resident path but reject
        // them on the compatibility staging path.
        options.turboquant_limits.validate()?;
        if options.publication_batch_items == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "composite publication batch size must be greater than zero".to_owned(),
            });
        }
        if base_map.tree().config.dimensions != current_map.tree().config.dimensions
            || base_map.tree().config.metric != current_map.tree().config.metric
            || base.source_descriptor() != &base_map.tree().descriptor
        {
            return Err(invalid(
                "composite base/current sources or TurboQuant configuration disagree",
            ));
        }
        let target = current_map.store_clone();
        let base_manifest_present = target
            .get(base.manifest_cid().as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
            .is_some();
        let base_source_present = target
            .get(base_map.tree().descriptor.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
            .is_some();
        if !base_manifest_present || !base_source_present {
            // Preserve the existing cross-store contract. Co-resident
            // snapshots take the structural streaming path below; only a
            // genuinely separate base store pays the compatibility staging
            // cost needed to copy the complete authenticated closure.
            let (staging, staged_base, staged_current) =
                stage_source_pair(base_map, current_map).await?;
            let (rebuilt, _) = TurboQuantizer::build_with_limits(
                &staged_base,
                base.config.clone(),
                options.turboquant_parallelism,
                options.turboquant_limits.clone(),
            )?;
            if rebuilt.manifest_cid() != base.manifest_cid() {
                return Err(invalid(
                    "staged TurboQuant base is not canonical with async base",
                ));
            }
            return publish_composite_outcome(
                current_map,
                staging,
                CompositeAccelerator::build(
                    &staged_base,
                    &staged_current,
                    CompositeBase::TurboQuantized(rebuilt),
                    options.config,
                    options.limits,
                )?,
                options.publication_batch_items,
                &options.graph_limits,
            )
            .await;
        }

        let AsyncCompositeBuildOptions {
            config,
            limits,
            publication_batch_items,
            graph_limits,
            ..
        } = options;
        let persisted_base =
            AsyncTurboQuantizer::load(&target, base.manifest_cid().clone()).await?;
        let persisted_source = crate::prolly::proximity::AsyncProximityMap::load(
            target.clone(),
            base_map.tree().descriptor.clone(),
        )
        .await?;
        if persisted_base.source_descriptor() != base.source_descriptor()
            || persisted_base.config() != base.config()
            || persisted_source.tree() != base_map.tree()
        {
            return Err(invalid(
                "async composite TurboQuant base or source disagrees with persisted content",
            ));
        }

        let tree_config = composite_tree_config();
        let mut delta_builder = AsyncSortedBatchBuilder::new_with_origin_and_batch_size(
            target.clone(),
            tree_config.clone(),
            PublicationOrigin::Maintenance,
            publication_batch_items,
        );
        let mut shadow_builder = AsyncSortedBatchBuilder::new_with_origin_and_batch_size(
            target.clone(),
            tree_config,
            PublicationOrigin::Maintenance,
            publication_batch_items,
        );
        let mut stats = CompositeBuildStats::default();
        let mut changes = current_map
            .directory
            .stream_diff(&base_map.tree().directory, &current_map.tree().directory);
        while let Some(change) = changes.next().await {
            let change = change?;
            stats.diff_entries = checked_add_composite(stats.diff_entries, 1, "diff_entries")?;
            enforce_composite("diff_entries", limits.max_diff_entries, stats.diff_entries)?;
            match change {
                Diff::Added { key, val } => {
                    StoredRecord::decode(&val, current_map.tree().config.dimensions)?;
                    account_composite_delta(&mut stats, &key, &val, &limits)?;
                    stats.inserted_records =
                        checked_add_composite(stats.inserted_records, 1, "inserted_records")?;
                    delta_builder.add(key, val).await?;
                }
                Diff::Removed { key, val } => {
                    StoredRecord::decode(&val, base_map.tree().config.dimensions)?;
                    account_composite_shadow(&mut stats, &key, &limits)?;
                    stats.deleted_records =
                        checked_add_composite(stats.deleted_records, 1, "deleted_records")?;
                    shadow_builder.add(key, Vec::new()).await?;
                }
                Diff::Changed { key, old, new } => {
                    let old_record = StoredRecord::decode(&old, base_map.tree().config.dimensions)?;
                    let new_record =
                        StoredRecord::decode(&new, current_map.tree().config.dimensions)?;
                    if old_record.vector == new_record.vector {
                        stats.value_only_records = checked_add_composite(
                            stats.value_only_records,
                            1,
                            "value_only_records",
                        )?;
                        continue;
                    }
                    account_composite_delta(&mut stats, &key, &new, &limits)?;
                    account_composite_shadow(&mut stats, &key, &limits)?;
                    stats.vector_updated_records = checked_add_composite(
                        stats.vector_updated_records,
                        1,
                        "vector_updated_records",
                    )?;
                    delta_builder.add(key.clone(), new).await?;
                    shadow_builder.add(key, Vec::new()).await?;
                }
            }
        }

        let reasons = composite_rebuild_reasons(
            &config,
            stats.delta_records,
            stats.shadow_records,
            current_map.tree().count,
            base_map.tree().count,
        );
        if !reasons.is_empty() {
            return Ok(AsyncCompositeBuildOutcome::FullRebuildRequired { reasons, stats });
        }

        let delta_tree = delta_builder.build().await?;
        let shadow_tree = shadow_builder.build().await?;
        let roots = [delta_tree.root.as_ref(), shadow_tree.root.as_ref()]
            .into_iter()
            .flatten()
            .cloned()
            .map(|cid| TypedContentRoot::new(ContentObjectKind::OrderedNode, cid))
            .collect::<Vec<_>>();
        let (published_descendants, descendant_bytes) = if roots.is_empty() {
            (0, 0)
        } else {
            let walk = walk_content_graph_async(&target, &roots, &graph_limits).await?;
            (walk.objects.len(), walk.total_bytes)
        };
        stats.encoded_output_bytes = descendant_bytes;
        enforce_composite(
            "distance_evaluations",
            limits.max_distance_evaluations,
            stats.distance_evaluations,
        )?;
        let mut object = CompositeManifest {
            current_source: current_map.tree().descriptor.clone(),
            base_source: base_map.tree().descriptor.clone(),
            dimensions: current_map.tree().config.dimensions,
            metric: current_map.tree().config.metric,
            current_count: current_map.tree().count,
            base_count: base_map.tree().count,
            base_kind: CompositeBaseKind::TurboQuantized,
            base_manifest: base.manifest_cid().clone(),
            base_fingerprint: turboquant_fingerprint(base.config()),
            delta_root: delta_tree.root,
            shadow_root: shadow_tree.root,
            inserted_count: stats.inserted_records as u64,
            updated_count: stats.vector_updated_records as u64,
            deleted_count: stats.deleted_records as u64,
            delta_count: stats.delta_records as u64,
            shadow_count: stats.shadow_records as u64,
            diff_entries: stats.diff_entries as u64,
            value_only_count: stats.value_only_records as u64,
            owned_bytes_peak: stats.owned_bytes_peak as u64,
            encoded_output_bytes: stats.encoded_output_bytes as u64,
            distance_evaluations: stats.distance_evaluations as u64,
            config,
        };
        let manifest_bytes = loop {
            let bytes = object.encode()?;
            let total =
                checked_add_composite(descendant_bytes, bytes.len(), "encoded_output_bytes")?;
            if object.encoded_output_bytes == total as u64 {
                break bytes;
            }
            object.encoded_output_bytes = total as u64;
        };
        stats.encoded_output_bytes = object.encoded_output_bytes as usize;
        enforce_composite(
            "encoded_output_bytes",
            limits.max_encoded_output_bytes,
            stats.encoded_output_bytes,
        )?;
        let manifest = Cid::from_bytes(&manifest_bytes);
        match target
            .get(manifest.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
        {
            Some(bytes) => {
                let actual = Cid::from_bytes(&bytes);
                if actual != manifest {
                    return Err(Error::CidMismatch {
                        expected: manifest,
                        actual,
                    });
                }
            }
            None => {
                let entries = [(manifest.as_bytes(), manifest_bytes.as_slice())];
                target
                    .publish_nodes(NodePublication::new(
                        &entries,
                        PublicationOrigin::Maintenance,
                    ))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
        }
        let loaded = Self::load(&target, manifest).await?;
        Ok(AsyncCompositeBuildOutcome::Composite {
            accelerator: Box::new(loaded),
            stats,
            objects_published: checked_add_composite(
                published_descendants,
                1,
                "published_objects",
            )?,
            bytes_published: object.encoded_output_bytes as usize,
        })
    }

    pub async fn load<S>(store: &S, manifest: Cid) -> Result<Self, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let object = CompositeManifest::decode(&load_content(store, &manifest).await?)?;
        if let Some(root) = &object.delta_root {
            load_content(store, root).await?;
        }
        if let Some(root) = &object.shadow_root {
            load_content(store, root).await?;
        }
        let base = match object.base_kind {
            CompositeBaseKind::Hnsw => {
                let index = AsyncHnswIndex::load(store, object.base_manifest.clone()).await?;
                if index.source != object.base_source
                    || hnsw_fingerprint(&index.config) != object.base_fingerprint
                {
                    return Err(invalid("async composite HNSW base binding mismatch"));
                }
                AsyncCompositeBase::Hnsw(index)
            }
            CompositeBaseKind::ProductQuantized => {
                let index =
                    AsyncProductQuantizer::load(store, object.base_manifest.clone()).await?;
                if index.source != object.base_source
                    || pq_fingerprint(&index.config) != object.base_fingerprint
                {
                    return Err(invalid("async composite PQ base binding mismatch"));
                }
                AsyncCompositeBase::ProductQuantized(index)
            }
            CompositeBaseKind::TurboQuantized => {
                let index = AsyncTurboQuantizer::load(store, object.base_manifest.clone()).await?;
                if index.source != object.base_source
                    || turboquant_fingerprint(&index.config) != object.base_fingerprint
                {
                    return Err(invalid("async composite TurboQuant base binding mismatch"));
                }
                AsyncCompositeBase::TurboQuantized(index)
            }
        };
        let tree_config = composite_tree_config();
        Ok(Self {
            manifest,
            current_source: object.current_source,
            base_source: object.base_source,
            dimensions: object.dimensions,
            metric: object.metric,
            current_count: object.current_count,
            base_count: object.base_count,
            base,
            delta_tree: Tree {
                root: object.delta_root,
                config: tree_config.clone(),
            },
            shadow_tree: Tree {
                root: object.shadow_root,
                config: tree_config,
            },
            delta_count: object.delta_count,
            shadow_count: object.shadow_count,
            config: object.config,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn current_source_descriptor(&self) -> &Cid {
        &self.current_source
    }
    pub fn base_source_descriptor(&self) -> &Cid {
        &self.base_source
    }
    pub fn delta_count(&self) -> u64 {
        self.delta_count
    }
    pub fn shadow_count(&self) -> u64 {
        self.shadow_count
    }
    pub fn config(&self) -> &CompositeAcceleratorConfig {
        &self.config
    }

    pub(crate) fn base_kind(&self) -> CompositeBaseKind {
        match self.base {
            AsyncCompositeBase::Hnsw(_) => CompositeBaseKind::Hnsw,
            AsyncCompositeBase::ProductQuantized(_) => CompositeBaseKind::ProductQuantized,
            AsyncCompositeBase::TurboQuantized(_) => CompositeBaseKind::TurboQuantized,
        }
    }
    pub(crate) fn hnsw(&self) -> Option<&AsyncHnswIndex> {
        match &self.base {
            AsyncCompositeBase::Hnsw(index) => Some(index),
            AsyncCompositeBase::ProductQuantized(_) => None,
            AsyncCompositeBase::TurboQuantized(_) => None,
        }
    }
    pub(crate) fn pq(&self) -> Option<&AsyncProductQuantizer> {
        match &self.base {
            AsyncCompositeBase::ProductQuantized(index) => Some(index),
            AsyncCompositeBase::Hnsw(_) => None,
            AsyncCompositeBase::TurboQuantized(_) => None,
        }
    }
    pub(crate) fn turboquant(&self) -> Option<&AsyncTurboQuantizer> {
        match &self.base {
            AsyncCompositeBase::TurboQuantized(index) => Some(index),
            AsyncCompositeBase::Hnsw(_) | AsyncCompositeBase::ProductQuantized(_) => None,
        }
    }
}

impl AsyncProductQuantizer {
    pub async fn load<S>(store: &S, manifest: Cid) -> Result<Self, Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        let bytes = load_content(store, &manifest).await?;
        let object = PqManifest::decode(&bytes)?;
        object.config.validate(
            object.dimensions,
            usize::from(object.config.centroids_per_subquantizer),
        )?;
        load_content(store, &object.code_root).await?;
        Ok(Self {
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            count: object.count,
            config: object.config,
            code_tree: Tree {
                root: Some(object.code_root),
                config: code_tree_config(),
            },
            codebooks: object.codebooks,
            quality: object.quality,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }
    pub fn config(&self) -> &ProductQuantizationConfig {
        &self.config
    }
    pub fn quality(&self) -> ProductQuantizationQuality {
        self.quality
    }
}

#[derive(Default)]
struct AsyncTurboQuantBuildState {
    encoded_vectors: usize,
    zero_vectors: usize,
    transformed_components: usize,
    butterfly_operations: usize,
    transform_operations: usize,
    quality_sum: f64,
    quality_maximum: f64,
    peak_temporary_bytes: usize,
}

#[allow(clippy::too_many_arguments)]
async fn append_turboquant_batch<S>(
    builder: &mut AsyncSortedBatchBuilder<S>,
    batch: Vec<(Vec<u8>, Vec<f32>)>,
    plan: &StructuredRotation,
    config: &TurboQuantizationConfig,
    worker_threads: usize,
    per_worker_buffers: usize,
    packed_len: usize,
    pool: Option<&rayon::ThreadPool>,
    limits: &TurboQuantizationBuildLimits,
    state: &mut AsyncTurboQuantBuildState,
) -> Result<(), Error>
where
    S: AsyncStore,
    S::Error: Send + Sync,
{
    if batch.is_empty() {
        return Ok(());
    }
    let batch_input_bytes = batch.iter().try_fold(0usize, |total, (key, vector)| {
        total
            .checked_add(key.len())
            .and_then(|value| value.checked_add(vector.len().checked_mul(4)?))
            .ok_or_else(|| {
                turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
            })
    })?;
    let encoded_value_bytes = 8usize.checked_add(packed_len).ok_or_else(|| {
        turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
    })?;
    let physical_peak = turboquant_temporary_peak_bytes(
        plan.owned_bytes(),
        per_worker_buffers,
        worker_threads,
        batch_input_bytes,
        batch.len(),
        encoded_value_bytes,
    )?;
    enforce_resource(
        "TurboQuant temporary bytes",
        limits.max_temporary_bytes,
        physical_peak,
    )?;
    let logical_peak = turboquant_temporary_peak_bytes(
        plan.owned_bytes(),
        per_worker_buffers,
        1,
        batch_input_bytes,
        batch.len(),
        encoded_value_bytes,
    )?;
    state.peak_temporary_bytes = state.peak_temporary_bytes.max(logical_peak);

    let dimensions = plan.dimensions();
    let sqrt_dimensions = sqrt_down(dimensions as f64);
    let codebook = turboquant_codebook(config.bit_width);
    let encoded = if let Some(pool) = pool {
        pool.install(|| {
            batch
                .into_par_iter()
                .map_init(
                    || EncodingScratch::new(dimensions, packed_len),
                    |scratch, (key, vector)| {
                        let encoded = encode_vector_reusing(
                            &vector,
                            plan,
                            codebook,
                            config.bit_width,
                            sqrt_dimensions,
                            scratch,
                        );
                        (key, encoded)
                    },
                )
                .collect::<Vec<_>>()
        })
    } else {
        let mut scratch = EncodingScratch::new(dimensions, packed_len);
        batch
            .into_iter()
            .map(|(key, vector)| {
                let encoded = encode_vector_reusing(
                    &vector,
                    plan,
                    codebook,
                    config.bit_width,
                    sqrt_dimensions,
                    &mut scratch,
                );
                (key, encoded)
            })
            .collect()
    };
    for (key, encoded) in encoded {
        let encoded = encoded?;
        if encoded.zero {
            state.zero_vectors = state.zero_vectors.saturating_add(1);
        } else {
            state.transformed_components = state
                .transformed_components
                .checked_add(dimensions)
                .ok_or_else(|| {
                    turboquant_resource_limit(
                        "TurboQuant transform operations",
                        usize::MAX,
                        usize::MAX,
                    )
                })?;
            state.butterfly_operations = state
                .butterfly_operations
                .checked_add(plan.butterfly_operations_per_vector())
                .ok_or_else(|| {
                    turboquant_resource_limit(
                        "TurboQuant transform operations",
                        usize::MAX,
                        usize::MAX,
                    )
                })?;
            state.transform_operations = state
                .transform_operations
                .checked_add(plan.operations_per_vector())
                .ok_or_else(|| {
                    turboquant_resource_limit(
                        "TurboQuant transform operations",
                        usize::MAX,
                        usize::MAX,
                    )
                })?;
            enforce_resource(
                "TurboQuant transform operations",
                limits.max_transform_operations,
                state.transform_operations,
            )?;
        }
        state.quality_sum += encoded.error;
        state.quality_maximum = state.quality_maximum.max(encoded.error);
        builder.add(key, encoded.bytes).await?;
        state.encoded_vectors += 1;
    }
    Ok(())
}

impl AsyncTurboQuantizer {
    pub async fn load<S>(store: &S, manifest: Cid) -> Result<Self, Error>
    where
        S: AsyncStore,
        S::Error: Send + Sync,
    {
        let bytes = load_content(store, &manifest).await?;
        let object = TurboQuantManifest::decode(&bytes)?;
        object.config.validate(object.dimensions)?;
        let root_bytes = load_content(store, &object.code_root).await?;
        validate_turboquant_code_tree_root(&root_bytes, object.count)?;
        Ok(Self {
            manifest,
            source: object.source,
            dimensions: object.dimensions,
            metric: object.metric,
            count: object.count,
            config: object.config,
            code_tree: Tree {
                root: Some(object.code_root),
                config: turboquant_code_tree_config(),
            },
            quality: object.quality,
            zero_vectors: object.zero_vectors,
        })
    }

    pub async fn build<S>(
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        config: TurboQuantizationConfig,
        parallelism: BuildParallelism,
    ) -> Result<(Self, TurboQuantizationBuildStats), Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        Self::build_with_limits(
            map,
            config,
            parallelism,
            TurboQuantizationBuildLimits::default(),
            1_024,
            &ContentGraphLimits::default(),
        )
        .await
    }

    pub async fn build_with_limits<S>(
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        config: TurboQuantizationConfig,
        parallelism: BuildParallelism,
        limits: TurboQuantizationBuildLimits,
        publication_batch_items: usize,
        graph_limits: &ContentGraphLimits,
    ) -> Result<(Self, TurboQuantizationBuildStats), Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        if publication_batch_items == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "TurboQuant publication batch size must be positive".to_owned(),
            });
        }
        limits.validate()?;
        let dimensions = map.tree().config.dimensions;
        config.validate(dimensions)?;
        let records = usize::try_from(map.tree().count)
            .map_err(|_| turboquant_resource_limit("TurboQuant records", usize::MAX, usize::MAX))?;
        if records == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "TurboQuant requires a non-empty source map".to_owned(),
            });
        }
        enforce_resource("TurboQuant records", limits.max_records, records)?;
        enforce_resource(
            "TurboQuant worker threads",
            limits.max_worker_threads,
            parallelism.threads(),
        )?;

        let dimensions_usize = dimensions as usize;
        let input_bytes = records
            .checked_mul(dimensions_usize)
            .and_then(|value| value.checked_mul(4))
            .ok_or_else(|| {
                turboquant_resource_limit("TurboQuant input bytes", usize::MAX, usize::MAX)
            })?;
        enforce_resource(
            "TurboQuant input bytes",
            limits.max_input_bytes,
            input_bytes,
        )?;
        let packed_len = turboquant_packed_len(dimensions_usize, config.bit_width)?;
        let encoded_output_bytes = records
            .checked_mul(8usize.checked_add(packed_len).ok_or_else(|| {
                turboquant_resource_limit("TurboQuant encoded output bytes", usize::MAX, usize::MAX)
            })?)
            .ok_or_else(|| {
                turboquant_resource_limit("TurboQuant encoded output bytes", usize::MAX, usize::MAX)
            })?;
        enforce_resource(
            "TurboQuant encoded output bytes",
            limits.max_encoded_output_bytes,
            encoded_output_bytes,
        )?;

        let plan = StructuredRotation::derive(dimensions_usize, config.seed)?;
        let per_worker_buffers = dimensions_usize
            .checked_mul(25)
            .and_then(|value| value.checked_add(packed_len))
            .ok_or_else(|| {
                turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
            })?;
        let encoded_value_bytes = 8usize.checked_add(packed_len).ok_or_else(|| {
            turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
        })?;
        let minimum_input_bytes = dimensions_usize.checked_mul(4).ok_or_else(|| {
            turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
        })?;
        let required_temporary_bytes = turboquant_temporary_peak_bytes(
            plan.owned_bytes(),
            per_worker_buffers,
            parallelism.threads(),
            minimum_input_bytes,
            1,
            encoded_value_bytes,
        )?;
        enforce_resource(
            "TurboQuant temporary bytes",
            limits.max_temporary_bytes,
            required_temporary_bytes,
        )?;

        let target = map.store_clone();
        let code_config = turboquant_code_tree_config();
        let mut builder = AsyncSortedBatchBuilder::new_with_origin_and_batch_size(
            target.clone(),
            code_config.clone(),
            PublicationOrigin::Maintenance,
            publication_batch_items,
        );
        let pool = (parallelism.threads() > 1)
            .then(|| {
                rayon::ThreadPoolBuilder::new()
                    .num_threads(parallelism.threads())
                    .build()
            })
            .transpose()
            .map_err(|_| Error::InvalidProximityConfig {
                reason: "cannot create TurboQuant worker pool".to_owned(),
            })?;
        let mut state = AsyncTurboQuantBuildState {
            peak_temporary_bytes: turboquant_temporary_peak_bytes(
                plan.owned_bytes(),
                per_worker_buffers,
                1,
                minimum_input_bytes,
                1,
                encoded_value_bytes,
            )?,
            ..Default::default()
        };
        const ENCODE_BATCH_RECORDS: usize = 128;
        let mut batch = if limits.max_temporary_bytes.is_some() {
            Vec::new()
        } else {
            Vec::with_capacity(ENCODE_BATCH_RECORDS)
        };
        let mut batch_input_bytes = 0usize;
        let mut source = map
            .directory
            .range(&map.tree().directory, &[], None)
            .await?;
        while let Some(entry) = source.next().await {
            let (key, bytes) = entry?;
            let mut next_batch_input_bytes = batch_input_bytes;
            if let Some(limit) = limits.max_temporary_bytes {
                let record_input_bytes =
                    key.len().checked_add(minimum_input_bytes).ok_or_else(|| {
                        turboquant_resource_limit(
                            "TurboQuant temporary bytes",
                            usize::MAX,
                            usize::MAX,
                        )
                    })?;
                let mut projected_input_bytes = batch_input_bytes
                    .checked_add(record_input_bytes)
                    .ok_or_else(|| {
                    turboquant_resource_limit("TurboQuant temporary bytes", usize::MAX, usize::MAX)
                })?;
                let mut projected_peak = turboquant_temporary_peak_bytes(
                    plan.owned_bytes(),
                    per_worker_buffers,
                    parallelism.threads(),
                    projected_input_bytes,
                    batch.len() + 1,
                    encoded_value_bytes,
                )?;
                if projected_peak > limit && !batch.is_empty() {
                    append_turboquant_batch(
                        &mut builder,
                        std::mem::take(&mut batch),
                        &plan,
                        &config,
                        parallelism.threads(),
                        per_worker_buffers,
                        packed_len,
                        pool.as_ref(),
                        &limits,
                        &mut state,
                    )
                    .await?;
                    projected_input_bytes = record_input_bytes;
                    projected_peak = turboquant_temporary_peak_bytes(
                        plan.owned_bytes(),
                        per_worker_buffers,
                        parallelism.threads(),
                        projected_input_bytes,
                        1,
                        encoded_value_bytes,
                    )?;
                }
                enforce_resource("TurboQuant temporary bytes", Some(limit), projected_peak)?;
                next_batch_input_bytes = projected_input_bytes;
            }
            let stored = StoredRecord::decode(&bytes, dimensions)?;
            batch.push((key, stored.vector));
            batch_input_bytes = next_batch_input_bytes;
            if batch.len() == ENCODE_BATCH_RECORDS {
                append_turboquant_batch(
                    &mut builder,
                    std::mem::take(&mut batch),
                    &plan,
                    &config,
                    parallelism.threads(),
                    per_worker_buffers,
                    packed_len,
                    pool.as_ref(),
                    &limits,
                    &mut state,
                )
                .await?;
                batch_input_bytes = 0;
            }
        }
        append_turboquant_batch(
            &mut builder,
            batch,
            &plan,
            &config,
            parallelism.threads(),
            per_worker_buffers,
            packed_len,
            pool.as_ref(),
            &limits,
            &mut state,
        )
        .await?;
        if state.encoded_vectors != records {
            return Err(invalid_turboquant_object(
                "TurboQuant source count changed during async build",
            ));
        }
        let code_tree = builder.build().await?;
        let code_root = code_tree.root.clone().ok_or_else(|| {
            invalid_turboquant_object("TurboQuant requires a non-empty code tree")
        })?;
        let quality = TurboQuantizationQuality {
            mean_squared_error: state.quality_sum / state.encoded_vectors as f64,
            maximum_squared_error: state.quality_maximum,
        };
        let manifest_object = TurboQuantManifest {
            source: map.tree().descriptor.clone(),
            dimensions,
            metric: map.tree().config.metric,
            count: map.tree().count,
            config: config.clone(),
            transform_id: super::turboquant::STRUCTURED_ROTATION_ID,
            codebook_id: super::turboquant::NORMAL_LLOYD_MAX_CODEBOOK_ID,
            code_root,
            quality,
            zero_vectors: state.zero_vectors as u64,
        };
        let manifest_bytes = manifest_object.encode()?;
        let manifest = Cid::from_bytes(&manifest_bytes);
        match target
            .get(manifest.as_bytes())
            .await
            .map_err(|error| Error::Store(Box::new(error)))?
        {
            Some(bytes) => {
                let actual = Cid::from_bytes(&bytes);
                if actual != manifest {
                    return Err(Error::CidMismatch {
                        expected: manifest,
                        actual,
                    });
                }
            }
            None => {
                let entries = [(manifest.as_bytes(), manifest_bytes.as_slice())];
                target
                    .publish_nodes(NodePublication::new(
                        &entries,
                        PublicationOrigin::Maintenance,
                    ))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
        }
        let root = TypedContentRoot::new(ContentObjectKind::TurboQuantization, manifest.clone());
        walk_content_graph_async(&target, &[root], graph_limits).await?;
        Ok((
            Self::load(&target, manifest).await?,
            TurboQuantizationBuildStats {
                encoded_vectors: state.encoded_vectors,
                zero_vectors: state.zero_vectors,
                transformed_components: state.transformed_components,
                butterfly_operations: state.butterfly_operations,
                input_bytes,
                encoded_output_bytes,
                peak_temporary_bytes: state.peak_temporary_bytes,
            },
        ))
    }

    pub async fn search<S>(
        &self,
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        request: crate::prolly::proximity::SearchRequest<'_>,
        control: crate::prolly::proximity::AsyncSearchControl,
    ) -> Result<crate::prolly::proximity::SearchResult, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let runtime_map =
            map.bind_search_runtime(Arc::new(crate::prolly::proximity::SearchRuntime::default()));
        let set = AsyncAcceleratorSet::empty().with_turboquant(runtime_map.tree(), self.clone())?;
        runtime_map
            .search_with_accelerators(&set, request, control)
            .await
    }

    pub async fn verify<S>(
        &self,
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        limits: &ContentGraphLimits,
    ) -> Result<TurboQuantizationVerification, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        if self.source != map.tree().descriptor
            || self.dimensions != map.tree().config.dimensions
            || self.metric != map.tree().config.metric
            || self.count != map.tree().count
        {
            return Err(invalid("async TurboQuant source binding mismatch"));
        }
        crate::prolly::content_graph::walk_content_graph_async(
            &map.store_clone(),
            &[TypedContentRoot::new(
                ContentObjectKind::TurboQuantization,
                self.manifest.clone(),
            )],
            limits,
        )
        .await?;
        let store = map.store_clone();
        let codes = AsyncProlly::new(store, self.code_tree.config.clone());
        let mut source = map
            .directory
            .range(&map.tree().directory, &[], None)
            .await?;
        let mut encoded = codes.range(&self.code_tree, &[], None).await?;
        let plan = StructuredRotation::derive(self.dimensions as usize, self.config.seed)?;
        let packed_len = turboquant_packed_len(self.dimensions as usize, self.config.bit_width)?;
        let mut scratch = EncodingScratch::new(self.dimensions as usize, packed_len);
        let mut count = 0u64;
        let mut zeros = 0u64;
        let mut quality_sum = 0.0;
        let mut quality_maximum = 0.0f64;
        loop {
            match (source.next().await, encoded.next().await) {
                (None, None) => break,
                (Some(source), Some(code)) => {
                    let (source_key, source_bytes) = source?;
                    let (code_key, actual) = code?;
                    if source_key != code_key {
                        return Err(invalid_turboquant_object(
                            "TurboQuant source and code keys do not match",
                        ));
                    }
                    let stored = StoredRecord::decode(&source_bytes, self.dimensions)?;
                    let expected = encode_vector_reusing(
                        &stored.vector,
                        &plan,
                        turboquant_codebook(self.config.bit_width),
                        self.config.bit_width,
                        sqrt_down(f64::from(self.dimensions)),
                        &mut scratch,
                    )?;
                    if actual != expected.bytes {
                        return Err(invalid_turboquant_object(
                            "TurboQuant code disagrees with authoritative source vector",
                        ));
                    }
                    count += 1;
                    zeros += u64::from(expected.zero);
                    quality_sum += expected.error;
                    quality_maximum = quality_maximum.max(expected.error);
                }
                _ => {
                    return Err(invalid_turboquant_object(
                        "TurboQuant source and code counts do not match",
                    ));
                }
            }
        }
        if count != self.count || zeros != self.zero_vectors {
            return Err(invalid_turboquant_object(
                "TurboQuant verified counts disagree with manifest",
            ));
        }
        let quality = TurboQuantizationQuality {
            mean_squared_error: quality_sum / count as f64,
            maximum_squared_error: quality_maximum,
        };
        if quality.mean_squared_error.to_bits() != self.quality.mean_squared_error.to_bits()
            || quality.maximum_squared_error.to_bits()
                != self.quality.maximum_squared_error.to_bits()
        {
            return Err(invalid_turboquant_object(
                "TurboQuant quality measurements disagree with manifest",
            ));
        }
        Ok(TurboQuantizationVerification {
            encoded_vectors: count,
            zero_vectors: zeros,
            quality,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }
    pub fn config(&self) -> &TurboQuantizationConfig {
        &self.config
    }
    pub fn quality(&self) -> TurboQuantizationQuality {
        self.quality
    }
    pub fn zero_vectors(&self) -> u64 {
        self.zero_vectors
    }
}

/// Source-bound async accelerator capabilities available to one logical search.
#[derive(Clone, Default)]
pub struct AsyncAcceleratorSet {
    hnsw: Option<AsyncHnswIndex>,
    pq: Option<AsyncProductQuantizer>,
    turboquant: Option<AsyncTurboQuantizer>,
    composite: Option<AsyncCompositeAccelerator>,
}

impl AsyncAcceleratorSet {
    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_hnsw(
        mut self,
        source: &ProximityTree,
        index: AsyncHnswIndex,
    ) -> Result<Self, Error> {
        if self.hnsw.is_some() {
            return Err(invalid("duplicate HNSW accelerator"));
        }
        validate_binding(
            source,
            &index.source,
            index.dimensions,
            index.metric,
            index.count,
            "HNSW",
        )?;
        self.hnsw = Some(index);
        Ok(self)
    }

    pub fn with_pq(
        mut self,
        source: &ProximityTree,
        index: AsyncProductQuantizer,
    ) -> Result<Self, Error> {
        if self.pq.is_some() {
            return Err(invalid("duplicate product-quantization accelerator"));
        }
        validate_binding(
            source,
            &index.source,
            index.dimensions,
            index.metric,
            index.count,
            "product quantization",
        )?;
        self.pq = Some(index);
        Ok(self)
    }

    pub fn with_composite(
        mut self,
        source: &ProximityTree,
        index: AsyncCompositeAccelerator,
    ) -> Result<Self, Error> {
        if self.composite.is_some() {
            return Err(invalid("duplicate composite accelerator"));
        }
        validate_binding(
            source,
            &index.current_source,
            index.dimensions,
            index.metric,
            index.current_count,
            "composite",
        )?;
        self.composite = Some(index);
        Ok(self)
    }

    pub fn with_turboquant(
        mut self,
        source: &ProximityTree,
        index: AsyncTurboQuantizer,
    ) -> Result<Self, Error> {
        if self.turboquant.is_some() {
            return Err(invalid("duplicate TurboQuant accelerator"));
        }
        validate_binding(
            source,
            &index.source,
            index.dimensions,
            index.metric,
            index.count,
            "TurboQuant",
        )?;
        self.turboquant = Some(index);
        Ok(self)
    }

    pub(crate) fn hnsw(&self) -> Option<&AsyncHnswIndex> {
        self.hnsw.as_ref()
    }
    pub(crate) fn pq(&self) -> Option<&AsyncProductQuantizer> {
        self.pq.as_ref()
    }
    pub(crate) fn turboquant(&self) -> Option<&AsyncTurboQuantizer> {
        self.turboquant.as_ref()
    }
    pub(crate) fn composite(&self) -> Option<&AsyncCompositeAccelerator> {
        self.composite.as_ref()
    }
}

/// Validated accelerator-catalog metadata and sidecars for an async-only store.
#[derive(Clone)]
pub struct AsyncAcceleratorCatalog {
    manifest: Cid,
    source: Cid,
    entries: Vec<AcceleratorCatalogEntry>,
    accelerators: AsyncAcceleratorSet,
}

impl AsyncAcceleratorCatalog {
    /// Publish a canonical catalog for already-validated, source-bound async
    /// accelerators, then reopen it through the async store.
    pub async fn publish<S>(
        store: &S,
        source: &ProximityTree,
        accelerators: AsyncAcceleratorSet,
    ) -> Result<Self, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let mut entries = Vec::new();
        if let Some(index) = accelerators.hnsw() {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::Hnsw,
                configuration_fingerprint: hnsw_fingerprint(index.config()),
                manifest: index.manifest_cid().clone(),
            });
        }
        if let Some(index) = accelerators.pq() {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::ProductQuantized,
                configuration_fingerprint: pq_fingerprint(index.config()),
                manifest: index.manifest_cid().clone(),
            });
        }
        if let Some(index) = accelerators.turboquant() {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::TurboQuantized,
                configuration_fingerprint: turboquant_fingerprint(index.config()),
                manifest: index.manifest_cid().clone(),
            });
        }
        if let Some(index) = accelerators.composite() {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::Composite,
                configuration_fingerprint: composite_fingerprint(index.config()),
                manifest: index.manifest_cid().clone(),
            });
        }
        entries.sort_by_key(|entry| entry.kind);
        if entries.is_empty() {
            return Err(invalid("accelerator catalog must not be empty"));
        }
        let object = CatalogManifest {
            source: source.descriptor.clone(),
            entries,
        };
        let bytes = object.encode()?;
        let manifest = Cid::from_bytes(&bytes);
        let publication = [(manifest.as_bytes(), bytes.as_slice())];
        store
            .publish_nodes(NodePublication::new(
                &publication,
                PublicationOrigin::Maintenance,
            ))
            .await
            .map_err(|error| Error::Store(Box::new(error)))?;
        Self::load(store, manifest, source).await
    }

    /// Construct canonical sidecars from an async-only source and publish
    /// their complete catalog closure in bounded provider batches.
    pub async fn build<S>(
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        options: AsyncAcceleratorBuildOptions,
    ) -> Result<(Self, AsyncAcceleratorBuildStats), Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let AsyncAcceleratorBuildOptions {
            hnsw,
            product_quantizer,
            turboquant,
            publication_batch_items,
            graph_limits,
        } = options;
        if publication_batch_items == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "accelerator publication batch size must be greater than zero".to_owned(),
            });
        }
        if hnsw.is_none() && product_quantizer.is_none() && turboquant.is_none() {
            return Err(invalid("async accelerator build plan is empty"));
        }

        let store = map.store_clone();
        let mut stats = AsyncAcceleratorBuildStats::default();
        let mut accelerators = AsyncAcceleratorSet::empty();

        // HNSW and PQ retain their established canonical staging path. Do not
        // make TurboQuant pay that unbounded corpus-copy cost: its async
        // builder streams directly from the immutable source directory.
        if hnsw.is_some() || product_quantizer.is_some() {
            let records = map.collect_records().await?;
            let staging = Arc::new(MemStore::new());
            let staged_map = ProximityMap::build(
                staging.clone(),
                map.tree().config.clone(),
                records.into_values(),
            )?;
            if staged_map.tree() != map.tree() {
                return Err(invalid(
                    "async accelerator staging did not reproduce the source descriptor",
                ));
            }
            let mut roots = Vec::new();
            let mut hnsw_manifest = None;
            let mut pq_manifest = None;
            if let Some(build) = hnsw {
                let (index, built) =
                    HnswIndex::build_with_limits(&staged_map, build.config, build.limits)?;
                let manifest = index.manifest_cid().clone();
                roots.push(TypedContentRoot::new(
                    ContentObjectKind::HnswManifest,
                    manifest.clone(),
                ));
                hnsw_manifest = Some(manifest);
                stats.hnsw = Some(built);
            }
            if let Some(build) = product_quantizer {
                let (index, built) = ProductQuantizer::build_with_limits(
                    &staged_map,
                    build.config,
                    build.parallelism,
                    build.limits,
                )?;
                let manifest = index.manifest_cid().clone();
                roots.push(TypedContentRoot::new(
                    ContentObjectKind::ProductQuantization,
                    manifest.clone(),
                ));
                pq_manifest = Some(manifest);
                stats.product_quantizer = Some(built);
            }
            let walk = walk_content_graph(&staging, &roots, &graph_limits)?;
            for chunk in walk.objects.chunks(publication_batch_items) {
                let entries = chunk
                    .iter()
                    .map(|object| (object.root.cid.as_bytes(), object.bytes.as_slice()))
                    .collect::<Vec<_>>();
                store
                    .publish_nodes(NodePublication::new(
                        &entries,
                        PublicationOrigin::Maintenance,
                    ))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
            if let Some(manifest) = hnsw_manifest {
                accelerators = accelerators
                    .with_hnsw(map.tree(), AsyncHnswIndex::load(&store, manifest).await?)?;
            }
            if let Some(manifest) = pq_manifest {
                accelerators = accelerators.with_pq(
                    map.tree(),
                    AsyncProductQuantizer::load(&store, manifest).await?,
                )?;
            }
        }

        if let Some(build) = turboquant {
            let (index, built) = AsyncTurboQuantizer::build_with_limits(
                map,
                build.config,
                build.parallelism,
                build.limits,
                publication_batch_items,
                &graph_limits,
            )
            .await?;
            stats.turboquant = Some(built);
            accelerators = accelerators.with_turboquant(map.tree(), index)?;
        }

        let catalog = Self::publish(&store, map.tree(), accelerators).await?;
        let walk = walk_content_graph_async(&store, &[catalog.typed_root()], &graph_limits).await?;
        stats.objects_published = walk.objects.len();
        stats.bytes_published = walk.total_bytes;
        Ok((catalog, stats))
    }

    pub async fn load<S>(store: &S, manifest: Cid, source: &ProximityTree) -> Result<Self, Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        let object = CatalogManifest::decode(&load_content(store, &manifest).await?)?;
        if object.source != source.descriptor {
            return Err(invalid("catalog is bound to a different source snapshot"));
        }
        let mut accelerators = AsyncAcceleratorSet::empty();
        for entry in &object.entries {
            accelerators = match entry.kind {
                CatalogAcceleratorKind::Hnsw => {
                    let index = AsyncHnswIndex::load(store, entry.manifest.clone()).await?;
                    if hnsw_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog HNSW fingerprint mismatch"));
                    }
                    accelerators.with_hnsw(source, index)?
                }
                CatalogAcceleratorKind::ProductQuantized => {
                    let index = AsyncProductQuantizer::load(store, entry.manifest.clone()).await?;
                    if pq_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog PQ fingerprint mismatch"));
                    }
                    accelerators.with_pq(source, index)?
                }
                CatalogAcceleratorKind::Composite => {
                    let index =
                        AsyncCompositeAccelerator::load(store, entry.manifest.clone()).await?;
                    if composite_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog composite fingerprint mismatch"));
                    }
                    accelerators.with_composite(source, index)?
                }
                CatalogAcceleratorKind::TurboQuantized => {
                    let index = AsyncTurboQuantizer::load(store, entry.manifest.clone()).await?;
                    if turboquant_fingerprint(index.config()) != entry.configuration_fingerprint {
                        return Err(invalid("catalog TurboQuant fingerprint mismatch"));
                    }
                    accelerators.with_turboquant(source, index)?
                }
            };
        }
        Ok(Self {
            manifest,
            source: object.source,
            entries: object.entries,
            accelerators,
        })
    }

    pub fn manifest_cid(&self) -> &Cid {
        &self.manifest
    }
    pub fn typed_root(&self) -> TypedContentRoot {
        TypedContentRoot::new(ContentObjectKind::AcceleratorCatalog, self.manifest.clone())
    }
    pub fn source_descriptor(&self) -> &Cid {
        &self.source
    }
    pub fn entries(&self) -> &[AcceleratorCatalogEntry] {
        &self.entries
    }
    pub fn accelerators(&self) -> &AsyncAcceleratorSet {
        &self.accelerators
    }
    pub fn into_accelerators(self) -> AsyncAcceleratorSet {
        self.accelerators
    }
}

async fn stage_source_pair<S>(
    base_map: &crate::prolly::proximity::AsyncProximityMap<S>,
    current_map: &crate::prolly::proximity::AsyncProximityMap<S>,
) -> Result<
    (
        Arc<MemStore>,
        ProximityMap<Arc<MemStore>>,
        ProximityMap<Arc<MemStore>>,
    ),
    Error,
>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    if base_map.tree().config != current_map.tree().config {
        return Err(invalid("composite source configurations disagree"));
    }
    let base_records = base_map.collect_records().await?;
    let current_records = current_map.collect_records().await?;
    let staging = Arc::new(MemStore::new());
    let staged_base = ProximityMap::build(
        staging.clone(),
        base_map.tree().config.clone(),
        base_records.into_values(),
    )?;
    let staged_current = ProximityMap::build(
        staging.clone(),
        current_map.tree().config.clone(),
        current_records.into_values(),
    )?;
    if staged_base.tree() != base_map.tree() || staged_current.tree() != current_map.tree() {
        return Err(invalid(
            "async composite staging did not reproduce source descriptors",
        ));
    }
    Ok((staging, staged_base, staged_current))
}

async fn publish_composite_outcome<S>(
    current_map: &crate::prolly::proximity::AsyncProximityMap<S>,
    staging: Arc<MemStore>,
    outcome: CompositeBuildOutcome<Arc<MemStore>>,
    publication_batch_items: usize,
    graph_limits: &ContentGraphLimits,
) -> Result<AsyncCompositeBuildOutcome, Error>
where
    S: AsyncStore + Clone,
    S::Error: Send + Sync,
{
    match outcome {
        CompositeBuildOutcome::FullRebuildRequired { reasons, stats } => {
            Ok(AsyncCompositeBuildOutcome::FullRebuildRequired { reasons, stats })
        }
        CompositeBuildOutcome::Composite { accelerator, stats } => {
            if publication_batch_items == 0 {
                return Err(Error::InvalidProximityConfig {
                    reason: "composite publication batch size must be greater than zero".to_owned(),
                });
            }
            let manifest = accelerator.manifest_cid().clone();
            let root =
                TypedContentRoot::new(ContentObjectKind::CompositeAccelerator, manifest.clone());
            let walk = walk_content_graph(&staging, &[root], graph_limits)?;
            let store = current_map.store_clone();
            for chunk in walk.objects.chunks(publication_batch_items) {
                let entries = chunk
                    .iter()
                    .map(|object| (object.root.cid.as_bytes(), object.bytes.as_slice()))
                    .collect::<Vec<_>>();
                store
                    .publish_nodes(NodePublication::new(
                        &entries,
                        PublicationOrigin::Maintenance,
                    ))
                    .await
                    .map_err(|error| Error::Store(Box::new(error)))?;
            }
            let loaded = AsyncCompositeAccelerator::load(&store, manifest).await?;
            Ok(AsyncCompositeBuildOutcome::Composite {
                accelerator: Box::new(loaded),
                stats,
                objects_published: walk.objects.len(),
                bytes_published: walk.total_bytes,
            })
        }
    }
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

fn invalid(reason: impl Into<String>) -> Error {
    Error::InvalidProximitySearch {
        reason: reason.into(),
    }
}
