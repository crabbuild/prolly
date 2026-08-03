use super::catalog::{
    AcceleratorCatalogEntry, CatalogAcceleratorKind, Manifest as CatalogManifest,
};
use super::composite::config_fingerprint as composite_fingerprint;
use super::composite::{
    composite_tree_config, CompositeAccelerator, CompositeAcceleratorConfig, CompositeBase,
    CompositeBaseKind, CompositeBuildLimits, CompositeBuildOutcome, CompositeBuildStats,
    FullRebuildReason, Manifest as CompositeManifest,
};
use super::hnsw::storage::config_fingerprint as hnsw_fingerprint;
use super::hnsw::storage::{graph_config, GraphNode, Manifest as HnswManifest};
use super::hnsw::{HnswBuildLimits, HnswBuildStats, HnswConfig, HnswIndex};
use super::pq::config_fingerprint as pq_fingerprint;
use super::pq::{
    code_tree_config, Manifest as PqManifest, ProductQuantizationBuildLimits,
    ProductQuantizationBuildStats, ProductQuantizationConfig, ProductQuantizer,
};
use super::validate_binding;
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{
    walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentRoot,
};
use crate::prolly::error::Error;
use crate::prolly::proximity::{
    AcceleratorCatalog, AcceleratorSet, BuildParallelism, DistanceMetric,
    ProductQuantizationQuality, ProximityMap, ProximityTree,
};
use crate::prolly::store::{AsyncStore, MemStore, NodePublication, PublicationOrigin};
use crate::prolly::tree::Tree;
use crate::prolly::AsyncProlly;
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

/// Async-store publication plan for canonical accelerator sidecars.
#[derive(Clone, Debug)]
pub struct AsyncAcceleratorBuildOptions {
    pub hnsw: Option<AsyncHnswBuild>,
    pub product_quantizer: Option<AsyncProductQuantizerBuild>,
    pub publication_batch_items: usize,
    pub graph_limits: ContentGraphLimits,
}

impl Default for AsyncAcceleratorBuildOptions {
    fn default() -> Self {
        Self {
            hnsw: None,
            product_quantizer: None,
            publication_batch_items: 1_024,
            graph_limits: ContentGraphLimits::default(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AsyncAcceleratorBuildStats {
    pub hnsw: Option<HnswBuildStats>,
    pub product_quantizer: Option<ProductQuantizationBuildStats>,
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
            publication_batch_items: 1_024,
            graph_limits: ContentGraphLimits::default(),
        }
    }
}

pub enum AsyncCompositeBuildOutcome {
    Composite {
        accelerator: AsyncCompositeAccelerator,
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

#[derive(Clone)]
pub(crate) enum AsyncCompositeBase {
    Hnsw(AsyncHnswIndex),
    ProductQuantized(AsyncProductQuantizer),
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
        }
    }
    pub(crate) fn hnsw(&self) -> Option<&AsyncHnswIndex> {
        match &self.base {
            AsyncCompositeBase::Hnsw(index) => Some(index),
            AsyncCompositeBase::ProductQuantized(_) => None,
        }
    }
    pub(crate) fn pq(&self) -> Option<&AsyncProductQuantizer> {
        match &self.base {
            AsyncCompositeBase::ProductQuantized(index) => Some(index),
            AsyncCompositeBase::Hnsw(_) => None,
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

/// Source-bound async accelerator capabilities available to one logical search.
#[derive(Clone, Default)]
pub struct AsyncAcceleratorSet {
    hnsw: Option<AsyncHnswIndex>,
    pq: Option<AsyncProductQuantizer>,
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

    pub(crate) fn hnsw(&self) -> Option<&AsyncHnswIndex> {
        self.hnsw.as_ref()
    }
    pub(crate) fn pq(&self) -> Option<&AsyncProductQuantizer> {
        self.pq.as_ref()
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
        if let Some(index) = accelerators.composite() {
            entries.push(AcceleratorCatalogEntry {
                kind: CatalogAcceleratorKind::Composite,
                configuration_fingerprint: composite_fingerprint(index.config()),
                manifest: index.manifest_cid().clone(),
            });
        }
        entries.sort_by(|left, right| left.kind.cmp(&right.kind));
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

    /// Construct canonical HNSW/PQ sidecars from an async-only source and
    /// publish their complete catalog closure in bounded provider batches.
    pub async fn build<S>(
        map: &crate::prolly::proximity::AsyncProximityMap<S>,
        options: AsyncAcceleratorBuildOptions,
    ) -> Result<(Self, AsyncAcceleratorBuildStats), Error>
    where
        S: AsyncStore + Clone,
        S::Error: Send + Sync,
    {
        if options.publication_batch_items == 0 {
            return Err(Error::InvalidProximityConfig {
                reason: "accelerator publication batch size must be greater than zero".to_owned(),
            });
        }
        if options.hnsw.is_none() && options.product_quantizer.is_none() {
            return Err(invalid("async accelerator build plan is empty"));
        }

        // CPU builders remain runtime-neutral and deterministic. Staging in a
        // memory Store lets async-only applications use those canonical
        // builders without requiring a synchronous remote adapter.
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

        let mut stats = AsyncAcceleratorBuildStats::default();
        let mut set = AcceleratorSet::empty();
        if let Some(build) = options.hnsw {
            let (index, built) =
                HnswIndex::build_with_limits(&staged_map, build.config, build.limits)?;
            stats.hnsw = Some(built);
            set = set.with_hnsw(staged_map.tree(), index)?;
        }
        if let Some(build) = options.product_quantizer {
            let (index, built) = ProductQuantizer::build_with_limits(
                &staged_map,
                build.config,
                build.parallelism,
                build.limits,
            )?;
            stats.product_quantizer = Some(built);
            set = set.with_pq(staged_map.tree(), index)?;
        }
        let catalog = AcceleratorCatalog::build(staging.clone(), staged_map.tree(), set)?;
        let manifest = catalog.manifest_cid().clone();
        let walk = walk_content_graph(&staging, &[catalog.typed_root()], &options.graph_limits)?;
        stats.objects_published = walk.objects.len();
        stats.bytes_published = walk.total_bytes;
        let store = map.store_clone();
        for chunk in walk.objects.chunks(options.publication_batch_items) {
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
        let catalog = Self::load(&store, manifest, map.tree()).await?;
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
                accelerator: loaded,
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
