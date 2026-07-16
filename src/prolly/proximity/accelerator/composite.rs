use super::hnsw::storage::config_fingerprint as hnsw_fingerprint;
use super::pq::config_fingerprint as pq_fingerprint;
use super::{HnswIndex, ProductQuantizer};
use crate::prolly::builder::SortedBatchBuilder;
use crate::prolly::cid::Cid;
use crate::prolly::config::Config;
use crate::prolly::content_graph::{
    walk_content_graph, ContentGraphLimits, ContentObjectKind, TypedContentRoot,
};
use crate::prolly::encoding::Encoding;
use crate::prolly::error::{Diff, Error};
use crate::prolly::proximity::storage::codec::{put_cid, put_varint, Reader, MAX_OBJECT_ENTRIES};
use crate::prolly::proximity::storage::StoredRecord;
use crate::prolly::proximity::{
    BuildParallelism, DistanceMetric, HnswBuildLimits, HnswBuildStats,
    ProductQuantizationBuildLimits, ProductQuantizationBuildStats, ProximityMap, ProximityTree,
};
use crate::prolly::store::Store;
use crate::prolly::tree::Tree;

const MAGIC: &[u8; 4] = b"PCOM";
const VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum CompositeBaseKind {
    Hnsw,
    ProductQuantized,
}

impl CompositeBaseKind {
    pub(crate) const fn id(self) -> u8 {
        match self {
            Self::Hnsw => 1,
            Self::ProductQuantized => 2,
        }
    }

    fn from_id(id: u8) -> Result<Self, Error> {
        match id {
            1 => Ok(Self::Hnsw),
            2 => Ok(Self::ProductQuantized),
            _ => Err(invalid_object("unknown composite base accelerator kind")),
        }
    }
}

pub enum CompositeBase<S: Store> {
    Hnsw(HnswIndex<S>),
    ProductQuantized(ProductQuantizer<S>),
}

impl<S> CompositeBase<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    pub fn kind(&self) -> CompositeBaseKind {
        match self {
            Self::Hnsw(_) => CompositeBaseKind::Hnsw,
            Self::ProductQuantized(_) => CompositeBaseKind::ProductQuantized,
        }
    }

    pub fn manifest_cid(&self) -> &Cid {
        match self {
            Self::Hnsw(index) => index.manifest_cid(),
            Self::ProductQuantized(index) => index.manifest_cid(),
        }
    }

    fn source_descriptor(&self) -> &Cid {
        match self {
            Self::Hnsw(index) => index.source_descriptor(),
            Self::ProductQuantized(index) => index.source_descriptor(),
        }
    }

    fn config_fingerprint(&self) -> Cid {
        match self {
            Self::Hnsw(index) => hnsw_fingerprint(index.config()),
            Self::ProductQuantized(index) => pq_fingerprint(index.config()),
        }
    }

    pub(crate) fn hnsw(&self) -> Option<&HnswIndex<S>> {
        match self {
            Self::Hnsw(index) => Some(index),
            Self::ProductQuantized(_) => None,
        }
    }

    pub(crate) fn pq(&self) -> Option<&ProductQuantizer<S>> {
        match self {
            Self::ProductQuantized(index) => Some(index),
            Self::Hnsw(_) => None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompositeAcceleratorConfig {
    pub max_delta_records: usize,
    pub max_shadow_records: usize,
    pub max_delta_ratio_ppm: u32,
    pub max_shadow_ratio_ppm: u32,
    pub base_overfetch_multiplier: u32,
}

impl Default for CompositeAcceleratorConfig {
    fn default() -> Self {
        Self {
            max_delta_records: 4_096,
            max_shadow_records: 8_192,
            max_delta_ratio_ppm: 100_000,
            max_shadow_ratio_ppm: 200_000,
            base_overfetch_multiplier: 2,
        }
    }
}

impl CompositeAcceleratorConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.max_delta_ratio_ppm > 1_000_000
            || self.max_shadow_ratio_ppm > 1_000_000
            || self.base_overfetch_multiplier == 0
        {
            return Err(Error::InvalidProximityConfig {
                reason:
                    "composite overfetch must be positive and ratios must not exceed 1,000,000 ppm"
                        .to_owned(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompositeBuildLimits {
    pub max_diff_entries: Option<usize>,
    pub max_owned_bytes: Option<usize>,
    pub max_encoded_output_bytes: Option<usize>,
    pub max_distance_evaluations: Option<usize>,
}

impl CompositeBuildLimits {
    fn validate(&self) -> Result<(), Error> {
        for (resource, value) in [
            ("diff_entries", self.max_diff_entries),
            ("owned_bytes", self.max_owned_bytes),
            ("encoded_output_bytes", self.max_encoded_output_bytes),
        ] {
            if value == Some(0) {
                return Err(Error::InvalidProximityConfig {
                    reason: format!("composite {resource} limit must be positive"),
                });
            }
        }
        // Composite diffing compares canonical vectors directly and performs
        // no distance work. A zero distance-work ceiling is therefore valid.
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CompositeBuildStats {
    pub diff_entries: usize,
    pub inserted_records: usize,
    pub vector_updated_records: usize,
    pub value_only_records: usize,
    pub deleted_records: usize,
    pub delta_records: usize,
    pub shadow_records: usize,
    pub owned_bytes_peak: usize,
    pub encoded_output_bytes: usize,
    pub distance_evaluations: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FullRebuildReason {
    DeltaRecords { actual: usize, maximum: usize },
    ShadowRecords { actual: usize, maximum: usize },
    DeltaRatio { actual_ppm: u32, maximum_ppm: u32 },
    ShadowRatio { actual_ppm: u32, maximum_ppm: u32 },
}

pub enum CompositeBuildOutcome<S: Store> {
    Composite {
        accelerator: Box<CompositeAccelerator<S>>,
        stats: CompositeBuildStats,
    },
    FullRebuildRequired {
        reasons: Vec<FullRebuildReason>,
        stats: CompositeBuildStats,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompositeRebuildOptions {
    pub hnsw_limits: HnswBuildLimits,
    pub pq_parallelism: BuildParallelism,
    pub pq_limits: ProductQuantizationBuildLimits,
}

impl Default for CompositeRebuildOptions {
    fn default() -> Self {
        Self {
            hnsw_limits: HnswBuildLimits::default(),
            pq_parallelism: BuildParallelism::serial(),
            pq_limits: ProductQuantizationBuildLimits::default(),
        }
    }
}

pub enum CompositeBuildOrRebuildOutcome<S: Store> {
    Composite {
        accelerator: Box<CompositeAccelerator<S>>,
        stats: CompositeBuildStats,
    },
    NoAcceleratorRequired {
        reasons: Vec<FullRebuildReason>,
        composite_stats: CompositeBuildStats,
    },
    HnswRebuilt {
        accelerator: Box<HnswIndex<S>>,
        reasons: Vec<FullRebuildReason>,
        composite_stats: CompositeBuildStats,
        rebuild_stats: HnswBuildStats,
    },
    ProductQuantizedRebuilt {
        accelerator: Box<ProductQuantizer<S>>,
        reasons: Vec<FullRebuildReason>,
        composite_stats: CompositeBuildStats,
        rebuild_stats: ProductQuantizationBuildStats,
    },
}

pub struct CompositeAccelerator<S: Store> {
    pub(crate) manifest: Cid,
    pub(crate) current_source: Cid,
    pub(crate) base_source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) current_count: u64,
    pub(crate) base_count: u64,
    pub(crate) base: CompositeBase<S>,
    pub(crate) delta_tree: Tree,
    pub(crate) shadow_tree: Tree,
    pub(crate) delta_count: u64,
    pub(crate) shadow_count: u64,
    pub(crate) config: CompositeAcceleratorConfig,
    pub(crate) build_stats: CompositeBuildStats,
}

impl<S> CompositeAccelerator<S>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    /// Build a bounded composite, or synchronously replace it with a full
    /// current-source accelerator using the base accelerator's configuration.
    pub fn build_or_rebuild(
        base_map: &ProximityMap<S>,
        current_map: &ProximityMap<S>,
        base: CompositeBase<S>,
        config: CompositeAcceleratorConfig,
        limits: CompositeBuildLimits,
        rebuild: CompositeRebuildOptions,
    ) -> Result<CompositeBuildOrRebuildOutcome<S>, Error> {
        enum RebuildConfig {
            Hnsw(super::hnsw::HnswConfig),
            ProductQuantized(super::pq::ProductQuantizationConfig),
        }
        let rebuild_config = match &base {
            CompositeBase::Hnsw(index) => RebuildConfig::Hnsw(index.config().clone()),
            CompositeBase::ProductQuantized(index) => {
                RebuildConfig::ProductQuantized(index.config().clone())
            }
        };
        match Self::build(base_map, current_map, base, config, limits)? {
            CompositeBuildOutcome::Composite { accelerator, stats } => {
                Ok(CompositeBuildOrRebuildOutcome::Composite { accelerator, stats })
            }
            CompositeBuildOutcome::FullRebuildRequired { reasons, stats } => match rebuild_config {
                _ if current_map.tree().count == 0 => {
                    Ok(CompositeBuildOrRebuildOutcome::NoAcceleratorRequired {
                        reasons,
                        composite_stats: stats,
                    })
                }
                RebuildConfig::Hnsw(config) => {
                    let (accelerator, rebuild_stats) =
                        HnswIndex::build_with_limits(current_map, config, rebuild.hnsw_limits)?;
                    Ok(CompositeBuildOrRebuildOutcome::HnswRebuilt {
                        accelerator: Box::new(accelerator),
                        reasons,
                        composite_stats: stats,
                        rebuild_stats,
                    })
                }
                RebuildConfig::ProductQuantized(config) => {
                    let (accelerator, rebuild_stats) = ProductQuantizer::build_with_limits(
                        current_map,
                        config,
                        rebuild.pq_parallelism,
                        rebuild.pq_limits,
                    )?;
                    Ok(CompositeBuildOrRebuildOutcome::ProductQuantizedRebuilt {
                        accelerator: Box::new(accelerator),
                        reasons,
                        composite_stats: stats,
                        rebuild_stats,
                    })
                }
            },
        }
    }

    pub fn build(
        base_map: &ProximityMap<S>,
        current_map: &ProximityMap<S>,
        base: CompositeBase<S>,
        config: CompositeAcceleratorConfig,
        limits: CompositeBuildLimits,
    ) -> Result<CompositeBuildOutcome<S>, Error> {
        config.validate()?;
        limits.validate()?;
        validate_source_pair(base_map.tree(), current_map.tree(), &base)?;
        let store = current_map.store_clone();
        let tree_config = composite_tree_config();
        let mut delta_builder = SortedBatchBuilder::new(store.clone(), tree_config.clone());
        let mut shadow_builder = SortedBatchBuilder::new(store.clone(), tree_config.clone());
        let mut stats = CompositeBuildStats::default();

        for change in current_map
            .directory_manager()
            .stream_diff(&base_map.tree().directory, &current_map.tree().directory)?
        {
            let change = change?;
            stats.diff_entries = checked_add(stats.diff_entries, 1, "diff_entries")?;
            enforce("diff_entries", limits.max_diff_entries, stats.diff_entries)?;
            match change {
                Diff::Added { key, val } => {
                    StoredRecord::decode(&val, current_map.tree().config.dimensions)?;
                    account_delta(&mut stats, &key, &val, &limits)?;
                    stats.inserted_records += 1;
                    delta_builder.add(key, val)?;
                }
                Diff::Removed { key, val } => {
                    StoredRecord::decode(&val, base_map.tree().config.dimensions)?;
                    account_shadow(&mut stats, &key, &limits)?;
                    stats.deleted_records += 1;
                    shadow_builder.add(key, Vec::new())?;
                }
                Diff::Changed { key, old, new } => {
                    let old_record = StoredRecord::decode(&old, base_map.tree().config.dimensions)?;
                    let new_record =
                        StoredRecord::decode(&new, current_map.tree().config.dimensions)?;
                    if old_record.vector == new_record.vector {
                        stats.value_only_records += 1;
                        continue;
                    }
                    account_delta(&mut stats, &key, &new, &limits)?;
                    account_shadow(&mut stats, &key, &limits)?;
                    stats.vector_updated_records += 1;
                    delta_builder.add(key.clone(), new)?;
                    shadow_builder.add(key, Vec::new())?;
                }
            }
        }

        let reasons = rebuild_reasons(
            &config,
            stats.delta_records,
            stats.shadow_records,
            current_map.tree().count,
            base_map.tree().count,
        );
        if !reasons.is_empty() {
            return Ok(CompositeBuildOutcome::FullRebuildRequired { reasons, stats });
        }

        let delta_tree = delta_builder.build()?;
        let shadow_tree = shadow_builder.build()?;
        let roots = [delta_tree.root.as_ref(), shadow_tree.root.as_ref()]
            .into_iter()
            .flatten()
            .cloned()
            .map(|cid| TypedContentRoot::new(ContentObjectKind::OrderedNode, cid))
            .collect::<Vec<_>>();
        stats.encoded_output_bytes = if roots.is_empty() {
            0
        } else {
            walk_content_graph(&store, &roots, &ContentGraphLimits::default())?.total_bytes
        };
        let object = Manifest {
            current_source: current_map.tree().descriptor.clone(),
            base_source: base_map.tree().descriptor.clone(),
            dimensions: current_map.tree().config.dimensions,
            metric: current_map.tree().config.metric,
            current_count: current_map.tree().count,
            base_count: base_map.tree().count,
            base_kind: base.kind(),
            base_manifest: base.manifest_cid().clone(),
            base_fingerprint: base.config_fingerprint(),
            delta_root: delta_tree.root.clone(),
            shadow_root: shadow_tree.root.clone(),
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
            config: config.clone(),
        };
        enforce(
            "distance_evaluations",
            limits.max_distance_evaluations,
            stats.distance_evaluations,
        )?;
        let base_output_bytes = stats.encoded_output_bytes;
        let mut object = object;
        let bytes = loop {
            let bytes = object.encode()?;
            let total = checked_add(base_output_bytes, bytes.len(), "encoded_output_bytes")?;
            if object.encoded_output_bytes == total as u64 {
                break bytes;
            }
            object.encoded_output_bytes = total as u64;
        };
        stats.encoded_output_bytes = object.encoded_output_bytes as usize;
        enforce(
            "encoded_output_bytes",
            limits.max_encoded_output_bytes,
            stats.encoded_output_bytes,
        )?;
        let manifest = Cid::from_bytes(&bytes);
        put_content(&store, &manifest, &bytes)?;
        Ok(CompositeBuildOutcome::Composite {
            accelerator: Box::new(Self::from_manifest(manifest, object, base)),
            stats,
        })
    }

    pub fn load(store: S, manifest: Cid) -> Result<Self, Error> {
        let bytes = load_content(&store, &manifest)?;
        let object = Manifest::decode(&bytes)?;
        let base = match object.base_kind {
            CompositeBaseKind::Hnsw => CompositeBase::Hnsw(HnswIndex::load(
                store.clone(),
                object.base_manifest.clone(),
            )?),
            CompositeBaseKind::ProductQuantized => CompositeBase::ProductQuantized(
                ProductQuantizer::load(store.clone(), object.base_manifest.clone())?,
            ),
        };
        validate_loaded_base(&object, &base)?;
        if let Some(root) = &object.delta_root {
            load_content(&store, root)?;
        }
        if let Some(root) = &object.shadow_root {
            load_content(&store, root)?;
        }
        Ok(Self::from_manifest(manifest, object, base))
    }

    fn from_manifest(manifest: Cid, object: Manifest, base: CompositeBase<S>) -> Self {
        Self {
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
                config: composite_tree_config(),
            },
            shadow_tree: Tree {
                root: object.shadow_root,
                config: composite_tree_config(),
            },
            delta_count: object.delta_count,
            shadow_count: object.shadow_count,
            config: object.config,
            build_stats: CompositeBuildStats {
                diff_entries: object.diff_entries as usize,
                inserted_records: object.inserted_count as usize,
                vector_updated_records: object.updated_count as usize,
                value_only_records: object.value_only_count as usize,
                deleted_records: object.deleted_count as usize,
                delta_records: object.delta_count as usize,
                shadow_records: object.shadow_count as usize,
                owned_bytes_peak: object.owned_bytes_peak as usize,
                encoded_output_bytes: object.encoded_output_bytes as usize,
                distance_evaluations: object.distance_evaluations as usize,
            },
        }
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
    pub fn base_kind(&self) -> CompositeBaseKind {
        self.base.kind()
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
    pub fn build_stats(&self) -> &CompositeBuildStats {
        &self.build_stats
    }
}

#[derive(Clone)]
pub(crate) struct Manifest {
    pub(crate) current_source: Cid,
    pub(crate) base_source: Cid,
    pub(crate) dimensions: u32,
    pub(crate) metric: DistanceMetric,
    pub(crate) current_count: u64,
    pub(crate) base_count: u64,
    pub(crate) base_kind: CompositeBaseKind,
    pub(crate) base_manifest: Cid,
    pub(crate) base_fingerprint: Cid,
    pub(crate) delta_root: Option<Cid>,
    pub(crate) shadow_root: Option<Cid>,
    pub(crate) inserted_count: u64,
    pub(crate) updated_count: u64,
    pub(crate) deleted_count: u64,
    pub(crate) delta_count: u64,
    pub(crate) shadow_count: u64,
    pub(crate) diff_entries: u64,
    pub(crate) value_only_count: u64,
    pub(crate) owned_bytes_peak: u64,
    pub(crate) encoded_output_bytes: u64,
    pub(crate) distance_evaluations: u64,
    pub(crate) config: CompositeAcceleratorConfig,
}

impl Manifest {
    pub(crate) fn encode(&self) -> Result<Vec<u8>, Error> {
        self.validate()?;
        let mut bytes = Vec::new();
        bytes.extend_from_slice(MAGIC);
        bytes.push(VERSION);
        put_cid(&self.current_source, &mut bytes);
        put_cid(&self.base_source, &mut bytes);
        put_varint(u64::from(self.dimensions), &mut bytes);
        bytes.push(self.metric.id());
        put_varint(self.current_count, &mut bytes);
        put_varint(self.base_count, &mut bytes);
        bytes.push(self.base_kind.id());
        put_cid(&self.base_manifest, &mut bytes);
        put_cid(&self.base_fingerprint, &mut bytes);
        put_optional_cid(&self.delta_root, &mut bytes);
        put_optional_cid(&self.shadow_root, &mut bytes);
        for count in [
            self.inserted_count,
            self.updated_count,
            self.deleted_count,
            self.delta_count,
            self.shadow_count,
            self.diff_entries,
            self.value_only_count,
            self.owned_bytes_peak,
            self.encoded_output_bytes,
            self.distance_evaluations,
        ] {
            put_varint(count, &mut bytes);
        }
        encode_config(&self.config, &mut bytes);
        put_cid(&config_fingerprint(&self.config), &mut bytes);
        Ok(bytes)
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, Error> {
        let mut reader = Reader::new(bytes, "composite accelerator");
        reader.exact(MAGIC)?;
        if reader.u8()? != VERSION {
            return Err(reader.invalid("unsupported composite version"));
        }
        let current_source = reader.cid()?;
        let base_source = reader.cid()?;
        let dimensions =
            u32::try_from(reader.varint()?).map_err(|_| reader.invalid("dimensions exceed u32"))?;
        let metric = DistanceMetric::from_id(reader.u8()?)?;
        let current_count = reader.varint()?;
        let base_count = reader.varint()?;
        let base_kind = CompositeBaseKind::from_id(reader.u8()?)?;
        let base_manifest = reader.cid()?;
        let base_fingerprint = reader.cid()?;
        let delta_root = read_optional_cid(&mut reader)?;
        let shadow_root = read_optional_cid(&mut reader)?;
        let inserted_count = reader.varint()?;
        let updated_count = reader.varint()?;
        let deleted_count = reader.varint()?;
        let delta_count = reader.varint()?;
        let shadow_count = reader.varint()?;
        let diff_entries = reader.varint()?;
        let value_only_count = reader.varint()?;
        let owned_bytes_peak = reader.varint()?;
        let encoded_output_bytes = reader.varint()?;
        let distance_evaluations = reader.varint()?;
        let config = decode_config(&mut reader)?;
        if reader.cid()? != config_fingerprint(&config) {
            return Err(reader.invalid("composite configuration fingerprint mismatch"));
        }
        reader.finish()?;
        let object = Self {
            current_source,
            base_source,
            dimensions,
            metric,
            current_count,
            base_count,
            base_kind,
            base_manifest,
            base_fingerprint,
            delta_root,
            shadow_root,
            inserted_count,
            updated_count,
            deleted_count,
            delta_count,
            shadow_count,
            diff_entries,
            value_only_count,
            owned_bytes_peak,
            encoded_output_bytes,
            distance_evaluations,
            config,
        };
        object.validate()?;
        Ok(object)
    }

    fn validate(&self) -> Result<(), Error> {
        self.config.validate()?;
        if self.dimensions == 0
            || [
                self.current_count,
                self.base_count,
                self.inserted_count,
                self.updated_count,
                self.deleted_count,
                self.delta_count,
                self.shadow_count,
                self.diff_entries,
                self.value_only_count,
                self.owned_bytes_peak,
                self.encoded_output_bytes,
                self.distance_evaluations,
            ]
            .into_iter()
            .any(|value| usize::try_from(value).is_err())
            || self.delta_count != self.inserted_count.saturating_add(self.updated_count)
            || self.shadow_count != self.deleted_count.saturating_add(self.updated_count)
            || (self.delta_count == 0) != self.delta_root.is_none()
            || (self.shadow_count == 0) != self.shadow_root.is_none()
            || self.diff_entries
                != self
                    .inserted_count
                    .saturating_add(self.updated_count)
                    .saturating_add(self.value_only_count)
                    .saturating_add(self.deleted_count)
            || self.distance_evaluations != 0
            || !rebuild_reasons(
                &self.config,
                self.delta_count as usize,
                self.shadow_count as usize,
                self.current_count,
                self.base_count,
            )
            .is_empty()
        {
            return Err(invalid_object(
                "invalid composite counts, roots, or thresholds",
            ));
        }
        Ok(())
    }
}

fn validate_source_pair<S>(
    base: &ProximityTree,
    current: &ProximityTree,
    accelerator: &CompositeBase<S>,
) -> Result<(), Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    if base.config.dimensions != current.config.dimensions
        || base.config.metric != current.config.metric
        || accelerator.source_descriptor() != &base.descriptor
    {
        return Err(Error::InvalidProximitySearch {
            reason: "composite base/current sources or accelerator configuration disagree"
                .to_owned(),
        });
    }
    Ok(())
}

fn validate_loaded_base<S>(manifest: &Manifest, base: &CompositeBase<S>) -> Result<(), Error>
where
    S: Store + Clone + Send + Sync,
    S::Error: Send + Sync,
{
    if base.source_descriptor() != &manifest.base_source
        || base.manifest_cid() != &manifest.base_manifest
        || base.kind() != manifest.base_kind
        || base.config_fingerprint() != manifest.base_fingerprint
    {
        return Err(invalid_object("composite base manifest binding mismatch"));
    }
    Ok(())
}

fn account_delta(
    stats: &mut CompositeBuildStats,
    key: &[u8],
    value: &[u8],
    limits: &CompositeBuildLimits,
) -> Result<(), Error> {
    stats.delta_records = checked_add(stats.delta_records, 1, "delta_records")?;
    account_bytes(stats, key.len().saturating_add(value.len()), limits)
}

fn account_shadow(
    stats: &mut CompositeBuildStats,
    key: &[u8],
    limits: &CompositeBuildLimits,
) -> Result<(), Error> {
    stats.shadow_records = checked_add(stats.shadow_records, 1, "shadow_records")?;
    account_bytes(stats, key.len(), limits)
}

fn account_bytes(
    stats: &mut CompositeBuildStats,
    bytes: usize,
    limits: &CompositeBuildLimits,
) -> Result<(), Error> {
    stats.owned_bytes_peak = checked_add(stats.owned_bytes_peak, bytes, "owned_bytes")?;
    enforce(
        "owned_bytes",
        limits.max_owned_bytes,
        stats.owned_bytes_peak,
    )
}

fn rebuild_reasons(
    config: &CompositeAcceleratorConfig,
    delta: usize,
    shadow: usize,
    current_count: u64,
    base_count: u64,
) -> Vec<FullRebuildReason> {
    let mut reasons = Vec::new();
    if delta > config.max_delta_records {
        reasons.push(FullRebuildReason::DeltaRecords {
            actual: delta,
            maximum: config.max_delta_records,
        });
    }
    if shadow > config.max_shadow_records {
        reasons.push(FullRebuildReason::ShadowRecords {
            actual: shadow,
            maximum: config.max_shadow_records,
        });
    }
    let delta_ppm = ratio_ppm(delta as u64, current_count);
    if delta_ppm > config.max_delta_ratio_ppm {
        reasons.push(FullRebuildReason::DeltaRatio {
            actual_ppm: delta_ppm,
            maximum_ppm: config.max_delta_ratio_ppm,
        });
    }
    let shadow_ppm = ratio_ppm(shadow as u64, base_count);
    if shadow_ppm > config.max_shadow_ratio_ppm {
        reasons.push(FullRebuildReason::ShadowRatio {
            actual_ppm: shadow_ppm,
            maximum_ppm: config.max_shadow_ratio_ppm,
        });
    }
    reasons
}

fn ratio_ppm(numerator: u64, denominator: u64) -> u32 {
    if numerator == 0 {
        return 0;
    }
    if denominator == 0 {
        return 1_000_000;
    }
    let value = (u128::from(numerator) * 1_000_000)
        .div_ceil(u128::from(denominator))
        .min(1_000_000);
    value as u32
}

fn checked_add(value: usize, increment: usize, resource: &'static str) -> Result<usize, Error> {
    value
        .checked_add(increment)
        .ok_or(Error::ProximityResourceLimitExceeded {
            resource,
            limit: usize::MAX,
            actual: usize::MAX,
        })
}

fn enforce(resource: &'static str, limit: Option<usize>, actual: usize) -> Result<(), Error> {
    if let Some(limit) = limit {
        if actual > limit {
            return Err(Error::ProximityResourceLimitExceeded {
                resource,
                limit,
                actual,
            });
        }
    }
    Ok(())
}

fn put_optional_cid(cid: &Option<Cid>, bytes: &mut Vec<u8>) {
    match cid {
        Some(cid) => {
            bytes.push(1);
            put_cid(cid, bytes);
        }
        None => bytes.push(0),
    }
}

fn read_optional_cid(reader: &mut Reader<'_>) -> Result<Option<Cid>, Error> {
    match reader.u8()? {
        0 => Ok(None),
        1 => Ok(Some(reader.cid()?)),
        _ => Err(reader.invalid("invalid optional CID tag")),
    }
}

fn encode_config(config: &CompositeAcceleratorConfig, bytes: &mut Vec<u8>) {
    put_varint(config.max_delta_records as u64, bytes);
    put_varint(config.max_shadow_records as u64, bytes);
    put_varint(u64::from(config.max_delta_ratio_ppm), bytes);
    put_varint(u64::from(config.max_shadow_ratio_ppm), bytes);
    put_varint(u64::from(config.base_overfetch_multiplier), bytes);
}

fn decode_config(reader: &mut Reader<'_>) -> Result<CompositeAcceleratorConfig, Error> {
    Ok(CompositeAcceleratorConfig {
        max_delta_records: reader.bounded_usize(MAX_OBJECT_ENTRIES)?,
        max_shadow_records: reader.bounded_usize(MAX_OBJECT_ENTRIES)?,
        max_delta_ratio_ppm: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("delta ratio exceeds u32"))?,
        max_shadow_ratio_ppm: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("shadow ratio exceeds u32"))?,
        base_overfetch_multiplier: u32::try_from(reader.varint()?)
            .map_err(|_| reader.invalid("overfetch exceeds u32"))?,
    })
}

pub(crate) fn config_fingerprint(config: &CompositeAcceleratorConfig) -> Cid {
    let mut bytes = Vec::new();
    encode_config(config, &mut bytes);
    Cid::from_bytes(&bytes)
}

pub(crate) fn composite_tree_config() -> Config {
    Config::builder()
        .min_chunk_size(4)
        .max_chunk_size(1024 * 1024)
        .chunking_factor(128)
        .hash_seed(0)
        .encoding(Encoding::Raw)
        .build()
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

fn put_content<S: Store>(store: &S, cid: &Cid, bytes: &[u8]) -> Result<(), Error> {
    if let Some(existing) = store
        .get(cid.as_bytes())
        .map_err(|error| Error::Store(Box::new(error)))?
    {
        let actual = Cid::from_bytes(&existing);
        if actual != *cid {
            return Err(Error::CidMismatch {
                expected: cid.clone(),
                actual,
            });
        }
        return Ok(());
    }
    store
        .put(cid.as_bytes(), bytes)
        .map_err(|error| Error::Store(Box::new(error)))
}

fn invalid_object(reason: impl Into<String>) -> Error {
    Error::InvalidProximityObject {
        kind: "composite accelerator",
        reason: reason.into(),
    }
}
