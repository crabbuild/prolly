use super::catalog::{
    AcceleratorCatalogEntry, CatalogAcceleratorKind, Manifest as CatalogManifest,
};
use super::composite::config_fingerprint as composite_fingerprint;
use super::composite::{
    composite_tree_config, CompositeAcceleratorConfig, CompositeBaseKind,
    Manifest as CompositeManifest,
};
use super::hnsw::storage::config_fingerprint as hnsw_fingerprint;
use super::hnsw::storage::{graph_config, GraphNode, Manifest as HnswManifest};
use super::hnsw::HnswConfig;
use super::pq::config_fingerprint as pq_fingerprint;
use super::pq::{code_tree_config, Manifest as PqManifest, ProductQuantizationConfig};
use super::validate_binding;
use crate::prolly::cid::Cid;
use crate::prolly::content_graph::{ContentObjectKind, TypedContentRoot};
use crate::prolly::error::Error;
use crate::prolly::proximity::{DistanceMetric, ProductQuantizationQuality, ProximityTree};
use crate::prolly::store::AsyncStore;
use crate::prolly::tree::Tree;
use crate::prolly::AsyncProlly;

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
