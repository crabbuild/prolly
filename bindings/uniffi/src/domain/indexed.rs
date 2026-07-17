use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use prolly::{
    ActiveIndexHealth, IndexProjection, IndexVerification, IndexedMapHealth,
    IndexedMapMetricsSnapshot, IndexedMapUpdate, IndexedRetentionResult, IndexedSnapshotBundle,
    IndexedSnapshotId, IndexedVersion, SecondaryIndex, SecondaryIndexCursor, SecondaryIndexEntry,
    SecondaryIndexError, SecondaryIndexLimits, SecondaryIndexMatch, SecondaryIndexPage,
    SecondaryIndexRegistry,
};

use crate::{BindingEngine, GcPlanRecord, MutationRecord, ProllyBindingError, ProllyEngine};

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum IndexProjectionRecord {
    KeysOnly,
    Include,
    All,
}

impl From<IndexProjectionRecord> for IndexProjection {
    fn from(value: IndexProjectionRecord) -> Self {
        match value {
            IndexProjectionRecord::KeysOnly => Self::KeysOnly,
            IndexProjectionRecord::Include => Self::Include,
            IndexProjectionRecord::All => Self::All,
        }
    }
}

impl From<IndexProjection> for IndexProjectionRecord {
    fn from(value: IndexProjection) -> Self {
        match value {
            IndexProjection::KeysOnly => Self::KeysOnly,
            IndexProjection::Include => Self::Include,
            IndexProjection::All => Self::All,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexEntryRecord {
    pub term: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct SecondaryIndexLimitsRecord {
    pub max_term_bytes: u64,
    pub max_projection_bytes: u64,
    pub max_all_value_bytes: u64,
    pub max_terms_per_record: u64,
    pub max_projected_bytes_per_record: u64,
    pub max_derived_mutations_per_transaction: u64,
    pub max_projected_bytes_per_transaction: u64,
    pub max_indexes: u64,
    pub build_page_size: u64,
    pub max_temporary_sort_bytes: u64,
    pub max_bundle_nodes: u64,
    pub max_bundle_bytes: u64,
    pub max_verification_entries: u64,
    pub max_write_retries: u64,
    pub max_build_retries: u64,
}

impl From<SecondaryIndexLimits> for SecondaryIndexLimitsRecord {
    fn from(value: SecondaryIndexLimits) -> Self {
        Self {
            max_term_bytes: value.max_term_bytes as u64,
            max_projection_bytes: value.max_projection_bytes as u64,
            max_all_value_bytes: value.max_all_value_bytes as u64,
            max_terms_per_record: value.max_terms_per_record as u64,
            max_projected_bytes_per_record: value.max_projected_bytes_per_record as u64,
            max_derived_mutations_per_transaction: value.max_derived_mutations_per_transaction
                as u64,
            max_projected_bytes_per_transaction: value.max_projected_bytes_per_transaction as u64,
            max_indexes: value.max_indexes as u64,
            build_page_size: value.build_page_size as u64,
            max_temporary_sort_bytes: value.max_temporary_sort_bytes as u64,
            max_bundle_nodes: value.max_bundle_nodes as u64,
            max_bundle_bytes: value.max_bundle_bytes as u64,
            max_verification_entries: value.max_verification_entries as u64,
            max_write_retries: value.max_write_retries as u64,
            max_build_retries: value.max_build_retries as u64,
        }
    }
}

#[uniffi::export]
pub fn default_secondary_index_limits() -> SecondaryIndexLimitsRecord {
    SecondaryIndexLimits::default().into()
}

fn limits_from_record(
    value: SecondaryIndexLimitsRecord,
) -> Result<SecondaryIndexLimits, ProllyBindingError> {
    let usize_field = |value: u64, name: &str| {
        usize::try_from(value).map_err(|_| ProllyBindingError::InvalidArgument {
            reason: format!("{name} does not fit this platform"),
        })
    };
    Ok(SecondaryIndexLimits {
        max_term_bytes: usize_field(value.max_term_bytes, "max_term_bytes")?,
        max_projection_bytes: usize_field(value.max_projection_bytes, "max_projection_bytes")?,
        max_all_value_bytes: usize_field(value.max_all_value_bytes, "max_all_value_bytes")?,
        max_terms_per_record: usize_field(value.max_terms_per_record, "max_terms_per_record")?,
        max_projected_bytes_per_record: usize_field(
            value.max_projected_bytes_per_record,
            "max_projected_bytes_per_record",
        )?,
        max_derived_mutations_per_transaction: usize_field(
            value.max_derived_mutations_per_transaction,
            "max_derived_mutations_per_transaction",
        )?,
        max_projected_bytes_per_transaction: usize_field(
            value.max_projected_bytes_per_transaction,
            "max_projected_bytes_per_transaction",
        )?,
        max_indexes: usize_field(value.max_indexes, "max_indexes")?,
        build_page_size: usize_field(value.build_page_size, "build_page_size")?,
        max_temporary_sort_bytes: usize_field(
            value.max_temporary_sort_bytes,
            "max_temporary_sort_bytes",
        )?,
        max_bundle_nodes: usize_field(value.max_bundle_nodes, "max_bundle_nodes")?,
        max_bundle_bytes: usize_field(value.max_bundle_bytes, "max_bundle_bytes")?,
        max_verification_entries: usize_field(
            value.max_verification_entries,
            "max_verification_entries",
        )?,
        max_write_retries: usize_field(value.max_write_retries, "max_write_retries")?,
        max_build_retries: usize_field(value.max_build_retries, "max_build_retries")?,
    })
}

#[uniffi::export(with_foreign)]
pub trait SecondaryIndexExtractorCallback: Send + Sync {
    fn extract(
        &self,
        primary_key: Vec<u8>,
        source_value: Vec<u8>,
    ) -> Result<Vec<IndexEntryRecord>, ProllyBindingError>;
}

fn secondary_index_from_callback(
    name: Vec<u8>,
    generation: u64,
    extractor_id: String,
    projection: IndexProjectionRecord,
    limits: Option<SecondaryIndexLimitsRecord>,
    extractor: Arc<dyn SecondaryIndexExtractorCallback>,
) -> Result<SecondaryIndex, ProllyBindingError> {
    let limits = limits
        .map(limits_from_record)
        .transpose()?
        .unwrap_or_default();
    Ok(SecondaryIndex::builder(name, generation, extractor_id)
        .projection(projection.into())
        .limits(limits)
        .extract(move |primary_key, source_value| {
            extractor
                .extract(primary_key.to_vec(), source_value.to_vec())
                .map(|entries| {
                    entries
                        .into_iter()
                        .map(|entry| SecondaryIndexEntry {
                            term: entry.term,
                            projection: entry.projection,
                        })
                        .collect()
                })
                .map_err(|error| SecondaryIndexError::new(error.to_string()))
        })?)
}

#[derive(uniffi::Object)]
pub struct BindingIndexRegistry {
    inner: Mutex<SecondaryIndexRegistry>,
}

impl BindingIndexRegistry {
    pub(crate) fn snapshot(&self) -> Result<SecondaryIndexRegistry, ProllyBindingError> {
        self.inner
            .lock()
            .map(|registry| registry.clone())
            .map_err(|_| ProllyBindingError::Internal {
                reason: "secondary-index registry is poisoned".to_string(),
            })
    }
}

impl Default for BindingIndexRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[uniffi::export]
impl BindingIndexRegistry {
    #[uniffi::constructor]
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(SecondaryIndexRegistry::new()),
        }
    }

    pub fn register(
        &self,
        name: Vec<u8>,
        generation: u64,
        extractor_id: String,
        projection: IndexProjectionRecord,
        limits: Option<SecondaryIndexLimitsRecord>,
        extractor: Arc<dyn SecondaryIndexExtractorCallback>,
    ) -> Result<(), ProllyBindingError> {
        let definition = secondary_index_from_callback(
            name,
            generation,
            extractor_id,
            projection,
            limits,
            extractor,
        )?;
        let mut registry = self
            .inner
            .lock()
            .map_err(|_| ProllyBindingError::Internal {
                reason: "secondary-index registry is poisoned".to_string(),
            })?;
        *registry = registry.clone().register(definition)?;
        Ok(())
    }

    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> Result<u64, ProllyBindingError> {
        self.inner
            .lock()
            .map(|registry| registry.len() as u64)
            .map_err(|_| ProllyBindingError::Internal {
                reason: "secondary-index registry is poisoned".to_string(),
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedSnapshotIdRecord {
    pub source_version: Vec<u8>,
    pub catalog_version: Vec<u8>,
}

impl From<IndexedSnapshotId> for IndexedSnapshotIdRecord {
    fn from(value: IndexedSnapshotId) -> Self {
        Self {
            source_version: value.source_version.into_cid().0.to_vec(),
            catalog_version: value.catalog_version.into_cid().0.to_vec(),
        }
    }
}

fn snapshot_id_from_record(
    value: &IndexedSnapshotIdRecord,
) -> Result<IndexedSnapshotId, ProllyBindingError> {
    Ok(IndexedSnapshotId {
        source_version: prolly::MapVersionId::from_bytes(&value.source_version)?,
        catalog_version: prolly::MapVersionId::from_bytes(&value.catalog_version)?,
    })
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedVersionRecord {
    pub source_version: Vec<u8>,
    pub catalog_version: Option<Vec<u8>>,
    pub index_count: u64,
}

impl From<IndexedVersion> for IndexedVersionRecord {
    fn from(value: IndexedVersion) -> Self {
        Self {
            source_version: value.source.id.into_cid().0.to_vec(),
            catalog_version: value
                .catalog
                .map(|version| version.id.into_cid().0.to_vec()),
            index_count: value.indexes.len() as u64,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, uniffi::Enum)]
pub enum IndexedUpdateKind {
    Applied,
    Unchanged,
    Conflict,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedUpdateRecord {
    pub kind: IndexedUpdateKind,
    pub previous_source_version: Option<Vec<u8>>,
    pub current: Option<IndexedVersionRecord>,
}

impl From<IndexedMapUpdate> for IndexedUpdateRecord {
    fn from(value: IndexedMapUpdate) -> Self {
        match value {
            IndexedMapUpdate::Applied { previous, current } => Self {
                kind: IndexedUpdateKind::Applied,
                previous_source_version: previous.map(|version| version.into_cid().0.to_vec()),
                current: Some(current.into()),
            },
            IndexedMapUpdate::Unchanged { current } => Self {
                kind: IndexedUpdateKind::Unchanged,
                previous_source_version: None,
                current: current.map(Into::into),
            },
            IndexedMapUpdate::Conflict { current } => Self {
                kind: IndexedUpdateKind::Conflict,
                previous_source_version: None,
                current: current.map(Into::into),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexBuildResultRecord {
    pub source_version: Vec<u8>,
    pub index_version: Vec<u8>,
    pub catalog_version: Vec<u8>,
    pub generation: u64,
    pub entries: u64,
    pub attempts: u64,
    pub activated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexMatchRecord {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

impl From<SecondaryIndexMatch> for IndexMatchRecord {
    fn from(value: SecondaryIndexMatch) -> Self {
        Self {
            term: value.term,
            primary_key: value.primary_key,
            projection: value.projection,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexPageRecord {
    pub matches: Vec<IndexMatchRecord>,
    pub next_cursor: Option<Vec<u8>>,
}

fn index_page_record(value: SecondaryIndexPage) -> Result<IndexPageRecord, ProllyBindingError> {
    Ok(IndexPageRecord {
        matches: value.matches.into_iter().map(Into::into).collect(),
        next_cursor: value
            .next_cursor
            .map(|cursor| cursor.to_bytes())
            .transpose()?,
    })
}

fn index_cursor(
    bytes: Option<Vec<u8>>,
) -> Result<Option<SecondaryIndexCursor>, ProllyBindingError> {
    bytes
        .map(|bytes| SecondaryIndexCursor::from_bytes(&bytes))
        .transpose()
        .map_err(Into::into)
}

fn page_limit(limit: u64) -> Result<usize, ProllyBindingError> {
    usize::try_from(limit).map_err(|_| ProllyBindingError::InvalidArgument {
        reason: "page limit does not fit this platform".to_string(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedSourceRecord {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub projection: Option<Vec<u8>>,
    pub source_value: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct ActiveIndexHealthRecord {
    pub name: Vec<u8>,
    pub generation: u64,
    pub fingerprint: Vec<u8>,
    pub projection: IndexProjectionRecord,
    pub index_map_id: Vec<u8>,
    pub index_version: Vec<u8>,
}

impl From<ActiveIndexHealth> for ActiveIndexHealthRecord {
    fn from(value: ActiveIndexHealth) -> Self {
        Self {
            name: value.name,
            generation: value.generation,
            fingerprint: value.fingerprint.as_bytes().to_vec(),
            projection: value.projection.into(),
            index_map_id: value.index_map_id,
            index_version: value.index_version.into_cid().0.to_vec(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedMapHealthRecord {
    pub source_map_id: Vec<u8>,
    pub source_version: Option<Vec<u8>>,
    pub catalog_version: Option<Vec<u8>>,
    pub active_indexes: Vec<ActiveIndexHealthRecord>,
    pub supports_transactions: bool,
}

impl From<IndexedMapHealth> for IndexedMapHealthRecord {
    fn from(value: IndexedMapHealth) -> Self {
        Self {
            source_map_id: value.source_map_id,
            source_version: value
                .source_version
                .map(|version| version.into_cid().0.to_vec()),
            catalog_version: value
                .catalog_version
                .map(|version| version.into_cid().0.to_vec()),
            active_indexes: value.active_indexes.into_iter().map(Into::into).collect(),
            supports_transactions: value.supports_transactions,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexVerificationRecord {
    pub name: Vec<u8>,
    pub source_version: Vec<u8>,
    pub expected_index_version: Vec<u8>,
    pub actual_index_version: Vec<u8>,
    pub expected_entries: u64,
    pub actual_entries: u64,
    pub semantic_differences: u64,
    pub valid: bool,
    pub canonical: bool,
}

impl From<IndexVerification> for IndexVerificationRecord {
    fn from(value: IndexVerification) -> Self {
        let valid = value.is_valid();
        let canonical = value.is_canonical();
        Self {
            name: value.name,
            source_version: value.source_version.into_cid().0.to_vec(),
            expected_index_version: value.expected_index_version.into_cid().0.to_vec(),
            actual_index_version: value.actual_index_version.into_cid().0.to_vec(),
            expected_entries: value.expected_entries as u64,
            actual_entries: value.actual_entries as u64,
            semantic_differences: value.semantic_differences as u64,
            valid,
            canonical,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, uniffi::Record)]
pub struct IndexedMapMetricsRecord {
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

impl From<IndexedMapMetricsSnapshot> for IndexedMapMetricsRecord {
    fn from(value: IndexedMapMetricsSnapshot) -> Self {
        Self {
            normalized_source_mutations: value.normalized_source_mutations,
            records_extracted: value.records_extracted,
            terms_emitted: value.terms_emitted,
            projected_bytes: value.projected_bytes,
            physical_upserts: value.physical_upserts,
            physical_deletes: value.physical_deletes,
            unchanged_emissions_skipped: value.unchanged_emissions_skipped,
            source_nodes_written: value.source_nodes_written,
            index_nodes_written: value.index_nodes_written,
            catalog_nodes_written: value.catalog_nodes_written,
            retries: value.retries,
            build_attempts: value.build_attempts,
            verification_outcomes: value.verification_outcomes,
            retained_roots: value.retained_roots,
        }
    }
}

impl IndexedMapMetricsRecord {
    fn add(&mut self, value: IndexedMapMetricsSnapshot) {
        self.normalized_source_mutations += value.normalized_source_mutations;
        self.records_extracted += value.records_extracted;
        self.terms_emitted += value.terms_emitted;
        self.projected_bytes += value.projected_bytes;
        self.physical_upserts += value.physical_upserts;
        self.physical_deletes += value.physical_deletes;
        self.unchanged_emissions_skipped += value.unchanged_emissions_skipped;
        self.source_nodes_written += value.source_nodes_written;
        self.index_nodes_written += value.index_nodes_written;
        self.catalog_nodes_written += value.catalog_nodes_written;
        self.retries += value.retries;
        self.build_attempts += value.build_attempts;
        self.verification_outcomes += value.verification_outcomes;
        self.retained_roots += value.retained_roots;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct IndexedRetentionRecord {
    pub retained_source_versions: Vec<Vec<u8>>,
    pub removed_source_versions: Vec<Vec<u8>>,
    pub retained_index_versions: Vec<Vec<u8>>,
    pub removed_index_versions: Vec<Vec<u8>>,
    pub removed_catalog_versions: Vec<Vec<u8>>,
    pub removed_checkpoint_records: u64,
    pub removed_named_roots: Vec<Vec<u8>>,
}

impl From<IndexedRetentionResult> for IndexedRetentionRecord {
    fn from(value: IndexedRetentionResult) -> Self {
        let versions = |items: Vec<prolly::MapVersionId>| {
            items
                .into_iter()
                .map(|version| version.into_cid().0.to_vec())
                .collect()
        };
        Self {
            retained_source_versions: versions(value.retained_source_versions),
            removed_source_versions: versions(value.removed_source_versions),
            retained_index_versions: versions(value.retained_index_versions),
            removed_index_versions: versions(value.removed_index_versions),
            removed_catalog_versions: versions(value.removed_catalog_versions),
            removed_checkpoint_records: value.removed_checkpoint_records as u64,
            removed_named_roots: value.removed_named_roots,
        }
    }
}

macro_rules! with_indexed_map {
    ($self:expr, $map:ident, $body:block) => {{
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let $map = engine.indexed_map(&$self.id, $self.registry_snapshot()?)?;
                let result = $body;
                $self.capture_metrics($map.metrics())?;
                result
            }
            BindingEngine::File(engine) => {
                let $map = engine.indexed_map(&$self.id, $self.registry_snapshot()?)?;
                let result = $body;
                $self.capture_metrics($map.metrics())?;
                result
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let $map = engine.indexed_map(&$self.id, $self.registry_snapshot()?)?;
                let result = $body;
                $self.capture_metrics($map.metrics())?;
                result
            }
            BindingEngine::Host(_) => Err(ProllyBindingError::Internal {
                reason: "custom host stores do not expose indexed-map transactions".to_string(),
            }),
        }
    }};
}

#[derive(uniffi::Object)]
pub struct BindingIndexedMap {
    engine: Arc<ProllyEngine>,
    id: Vec<u8>,
    registry: Mutex<SecondaryIndexRegistry>,
    metrics: Mutex<IndexedMapMetricsRecord>,
}

impl BindingIndexedMap {
    fn registry_snapshot(&self) -> Result<SecondaryIndexRegistry, ProllyBindingError> {
        self.registry
            .lock()
            .map(|registry| registry.clone())
            .map_err(|_| ProllyBindingError::Internal {
                reason: "indexed-map registry is poisoned".to_string(),
            })
    }

    fn replace_registry_definition(
        &self,
        _name: &[u8],
        replacement: SecondaryIndex,
    ) -> Result<(), ProllyBindingError> {
        let mut registry = self
            .registry
            .lock()
            .map_err(|_| ProllyBindingError::Internal {
                reason: "indexed-map registry is poisoned".to_string(),
            })?;
        *registry = registry.clone().replace(replacement)?;
        Ok(())
    }

    fn capture_metrics(
        &self,
        metrics: IndexedMapMetricsSnapshot,
    ) -> Result<(), ProllyBindingError> {
        self.metrics
            .lock()
            .map_err(|_| ProllyBindingError::Internal {
                reason: "indexed-map metrics are poisoned".to_string(),
            })?
            .add(metrics);
        Ok(())
    }
}

#[uniffi::export]
impl BindingIndexedMap {
    #[uniffi::constructor]
    pub fn new(
        engine: Arc<ProllyEngine>,
        id: Vec<u8>,
        registry: Arc<BindingIndexRegistry>,
    ) -> Result<Self, ProllyBindingError> {
        if id.is_empty() {
            return Err(ProllyBindingError::InvalidArgument {
                reason: "indexed-map id must not be empty".to_string(),
            });
        }
        Ok(Self {
            engine,
            id,
            registry: Mutex::new(registry.snapshot()?),
            metrics: Mutex::new(IndexedMapMetricsRecord::default()),
        })
    }

    pub fn id(&self) -> Vec<u8> {
        self.id.clone()
    }

    pub fn ensure_index(
        &self,
        name: Vec<u8>,
    ) -> Result<IndexBuildResultRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            let result = map.ensure_index(name)?;
            Ok(IndexBuildResultRecord {
                source_version: result.source_version.into_cid().0.to_vec(),
                index_version: result.index_version.into_cid().0.to_vec(),
                catalog_version: result.catalog_version.into_cid().0.to_vec(),
                generation: result.generation,
                entries: result.entries as u64,
                attempts: result.attempts as u64,
                activated: result.activated,
            })
        })
    }

    pub fn put(
        &self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> Result<IndexedVersionRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            map.put(key, value).map(Into::into).map_err(Into::into)
        })
    }

    pub fn apply(
        &self,
        mutations: Vec<MutationRecord>,
    ) -> Result<IndexedVersionRecord, ProllyBindingError> {
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        with_indexed_map!(self, map, {
            map.apply(mutations).map(Into::into).map_err(Into::into)
        })
    }

    pub fn apply_if(
        &self,
        expected_source: Option<Vec<u8>>,
        mutations: Vec<MutationRecord>,
    ) -> Result<IndexedUpdateRecord, ProllyBindingError> {
        let expected_source = expected_source
            .map(|version| prolly::MapVersionId::from_bytes(&version))
            .transpose()?;
        let mutations = mutations
            .into_iter()
            .map(TryInto::try_into)
            .collect::<Result<Vec<_>, _>>()?;
        with_indexed_map!(self, map, {
            map.apply_if(expected_source.as_ref(), mutations)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn delete(&self, key: Vec<u8>) -> Result<IndexedVersionRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            map.delete(key).map(Into::into).map_err(Into::into)
        })
    }

    pub fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>, ProllyBindingError> {
        with_indexed_map!(self, map, { map.get(&key).map_err(Into::into) })
    }

    pub fn health(&self) -> Result<IndexedMapHealthRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            map.health().map(Into::into).map_err(Into::into)
        })
    }

    pub fn metrics(&self) -> Result<IndexedMapMetricsRecord, ProllyBindingError> {
        self.metrics
            .lock()
            .map(|metrics| metrics.clone())
            .map_err(|_| ProllyBindingError::Internal {
                reason: "indexed-map metrics are poisoned".to_string(),
            })
    }

    pub fn verify_index(
        &self,
        name: Vec<u8>,
        source_version: Vec<u8>,
    ) -> Result<IndexVerificationRecord, ProllyBindingError> {
        let source_version = prolly::MapVersionId::from_bytes(&source_version)?;
        with_indexed_map!(self, map, {
            map.verify_index(name, &source_version)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn verify_all(
        &self,
        source_version: Vec<u8>,
    ) -> Result<Vec<IndexVerificationRecord>, ProllyBindingError> {
        let source_version = prolly::MapVersionId::from_bytes(&source_version)?;
        with_indexed_map!(self, map, {
            map.verify_all(&source_version)
                .map(|items| items.into_iter().map(Into::into).collect())
                .map_err(Into::into)
        })
    }

    pub fn repair_index(
        &self,
        name: Vec<u8>,
        source_version: Vec<u8>,
    ) -> Result<IndexVerificationRecord, ProllyBindingError> {
        let source_version = prolly::MapVersionId::from_bytes(&source_version)?;
        with_indexed_map!(self, map, {
            map.repair_index(name, &source_version)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn replace_index(
        &self,
        name: Vec<u8>,
        generation: u64,
        extractor_id: String,
        projection: IndexProjectionRecord,
        limits: Option<SecondaryIndexLimitsRecord>,
        extractor: Arc<dyn SecondaryIndexExtractorCallback>,
    ) -> Result<IndexBuildResultRecord, ProllyBindingError> {
        let definition = secondary_index_from_callback(
            name.clone(),
            generation,
            extractor_id,
            projection,
            limits,
            extractor,
        )?;
        let retained_definition = definition.clone();
        let replacement_name = name.clone();
        with_indexed_map!(self, map, {
            let result = map.replace_index(name, definition)?;
            self.replace_registry_definition(&replacement_name, retained_definition)?;
            Ok(IndexBuildResultRecord {
                source_version: result.source_version.into_cid().0.to_vec(),
                index_version: result.index_version.into_cid().0.to_vec(),
                catalog_version: result.catalog_version.into_cid().0.to_vec(),
                generation: result.generation,
                entries: result.entries as u64,
                attempts: result.attempts as u64,
                activated: result.activated,
            })
        })
    }

    pub fn deactivate_index(
        &self,
        name: Vec<u8>,
    ) -> Result<IndexedVersionRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            map.deactivate_index(name)
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn snapshot(&self) -> Result<Arc<BindingIndexedSnapshot>, ProllyBindingError> {
        with_indexed_map!(self, map, {
            let snapshot = map.snapshot()?;
            Ok(Arc::new(BindingIndexedSnapshot {
                engine: self.engine.clone(),
                id: self.id.clone(),
                registry: self.registry_snapshot()?,
                snapshot_id: snapshot.id().clone().into(),
            }))
        })
    }

    pub fn snapshot_at(
        &self,
        source_version: Vec<u8>,
    ) -> Result<Arc<BindingIndexedSnapshot>, ProllyBindingError> {
        let source_version = prolly::MapVersionId::from_bytes(&source_version)?;
        with_indexed_map!(self, map, {
            let snapshot = map.snapshot_at(&source_version)?;
            Ok(Arc::new(BindingIndexedSnapshot {
                engine: self.engine.clone(),
                id: self.id.clone(),
                registry: self.registry_snapshot()?,
                snapshot_id: snapshot.id().clone().into(),
            }))
        })
    }

    pub fn snapshot_by_id(
        &self,
        snapshot_id: IndexedSnapshotIdRecord,
    ) -> Result<Arc<BindingIndexedSnapshot>, ProllyBindingError> {
        let id = snapshot_id_from_record(&snapshot_id)?;
        with_indexed_map!(self, map, {
            map.snapshot_by_id(&id)?;
            Ok(Arc::new(BindingIndexedSnapshot {
                engine: self.engine.clone(),
                id: self.id.clone(),
                registry: self.registry_snapshot()?,
                snapshot_id,
            }))
        })
    }

    pub fn export_current(&self) -> Result<Vec<u8>, ProllyBindingError> {
        with_indexed_map!(self, map, { Ok(map.export_current()?.to_bytes()?) })
    }

    pub fn import_current(
        &self,
        bundle: Vec<u8>,
        expected_source: Option<Vec<u8>>,
    ) -> Result<IndexedVersionRecord, ProllyBindingError> {
        let bundle = IndexedSnapshotBundle::from_bytes(&bundle)?;
        let expected_source = expected_source
            .map(|version| prolly::MapVersionId::from_bytes(&version))
            .transpose()?;
        with_indexed_map!(self, map, {
            map.import_current(&bundle, expected_source.as_ref())
                .map(Into::into)
                .map_err(Into::into)
        })
    }

    pub fn keep_last(&self, count: u64) -> Result<IndexedRetentionRecord, ProllyBindingError> {
        let count = page_limit(count)?;
        with_indexed_map!(self, map, {
            map.keep_last(count).map(Into::into).map_err(Into::into)
        })
    }

    pub fn plan_gc(&self) -> Result<GcPlanRecord, ProllyBindingError> {
        with_indexed_map!(self, map, {
            GcPlanRecord::try_from(map.plan_indexed_gc()?)
        })
    }
}

#[derive(uniffi::Object)]
pub struct BindingIndexedSnapshot {
    engine: Arc<ProllyEngine>,
    id: Vec<u8>,
    registry: SecondaryIndexRegistry,
    snapshot_id: IndexedSnapshotIdRecord,
}

#[uniffi::export]
impl BindingIndexedSnapshot {
    pub fn id(&self) -> IndexedSnapshotIdRecord {
        self.snapshot_id.clone()
    }

    pub fn index(
        &self,
        name: Vec<u8>,
    ) -> Result<Arc<BindingSecondaryIndexSnapshot>, ProllyBindingError> {
        let snapshot = Arc::new(BindingSecondaryIndexSnapshot {
            engine: self.engine.clone(),
            id: self.id.clone(),
            registry: self.registry.clone(),
            snapshot_id: self.snapshot_id.clone(),
            name,
            fast_handle: AtomicU64::new(0),
        });
        let handle = crate::fast_abi::register_index_snapshot(&snapshot);
        snapshot.fast_handle.store(handle, Ordering::Release);
        Ok(snapshot)
    }
}

macro_rules! with_secondary_snapshot {
    ($self:expr, $index:ident, $body:block) => {{
        let snapshot_id = snapshot_id_from_record(&$self.snapshot_id)?;
        match &$self.engine.inner {
            BindingEngine::Memory(engine) => {
                let map = engine.indexed_map(&$self.id, $self.registry.clone())?;
                let snapshot = map.snapshot_by_id(&snapshot_id)?;
                let $index = snapshot.index(&$self.name)?;
                $body
            }
            BindingEngine::File(engine) => {
                let map = engine.indexed_map(&$self.id, $self.registry.clone())?;
                let snapshot = map.snapshot_by_id(&snapshot_id)?;
                let $index = snapshot.index(&$self.name)?;
                $body
            }
            #[cfg(feature = "sqlite")]
            BindingEngine::Sqlite(engine) => {
                let map = engine.indexed_map(&$self.id, $self.registry.clone())?;
                let snapshot = map.snapshot_by_id(&snapshot_id)?;
                let $index = snapshot.index(&$self.name)?;
                $body
            }
            BindingEngine::Host(_) => Err(ProllyBindingError::Internal {
                reason: "custom host stores do not expose indexed-map snapshots".to_string(),
            }),
        }
    }};
}

#[derive(uniffi::Object)]
pub struct BindingSecondaryIndexSnapshot {
    engine: Arc<ProllyEngine>,
    id: Vec<u8>,
    registry: SecondaryIndexRegistry,
    snapshot_id: IndexedSnapshotIdRecord,
    name: Vec<u8>,
    fast_handle: AtomicU64,
}

#[uniffi::export]
impl BindingSecondaryIndexSnapshot {
    pub fn name(&self) -> Vec<u8> {
        self.name.clone()
    }

    pub fn fast_handle(&self) -> u64 {
        self.fast_handle.load(Ordering::Acquire)
    }

    pub fn exact(&self, term: Vec<u8>) -> Result<Vec<IndexMatchRecord>, ProllyBindingError> {
        with_secondary_snapshot!(self, index, {
            index
                .exact(&term)
                .map(|matches| matches.into_iter().map(Into::into).collect())
                .map_err(Into::into)
        })
    }

    pub fn prefix(&self, prefix: Vec<u8>) -> Result<Vec<IndexMatchRecord>, ProllyBindingError> {
        with_secondary_snapshot!(self, index, {
            index
                .prefix(&prefix)
                .map(|matches| matches.into_iter().map(Into::into).collect())
                .map_err(Into::into)
        })
    }

    pub fn range(
        &self,
        start: Vec<u8>,
        range_end: Option<Vec<u8>>,
    ) -> Result<Vec<IndexMatchRecord>, ProllyBindingError> {
        with_secondary_snapshot!(self, index, {
            index
                .range(&start, range_end.as_deref())
                .map(|matches| matches.into_iter().map(Into::into).collect())
                .map_err(Into::into)
        })
    }

    pub fn records(&self, term: Vec<u8>) -> Result<Vec<IndexedSourceRecord>, ProllyBindingError> {
        with_secondary_snapshot!(self, index, {
            let mut records = Vec::new();
            index.scan_records(&term, |record| {
                records.push(IndexedSourceRecord {
                    term: record.term.to_vec(),
                    primary_key: record.primary_key.to_vec(),
                    projection: record.projection.map(<[u8]>::to_vec),
                    source_value: record.source_value.to_vec(),
                });
            })?;
            Ok(records)
        })
    }

    pub fn exact_page(
        &self,
        term: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.exact_page(&term, cursor.as_ref(), limit)?)
        })
    }

    pub fn exact_reverse_page(
        &self,
        term: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.exact_reverse_page(&term, cursor.as_ref(), limit)?)
        })
    }

    pub fn prefix_page(
        &self,
        prefix: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.prefix_page(&prefix, cursor.as_ref(), limit)?)
        })
    }

    pub fn prefix_reverse_page(
        &self,
        prefix: Vec<u8>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.prefix_reverse_page(&prefix, cursor.as_ref(), limit)?)
        })
    }

    pub fn range_page(
        &self,
        start: Vec<u8>,
        range_end: Option<Vec<u8>>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.range_page(
                &start,
                range_end.as_deref(),
                cursor.as_ref(),
                limit,
            )?)
        })
    }

    pub fn range_reverse_page(
        &self,
        start: Vec<u8>,
        range_end: Option<Vec<u8>>,
        cursor: Option<Vec<u8>>,
        limit: u64,
    ) -> Result<IndexPageRecord, ProllyBindingError> {
        let cursor = index_cursor(cursor)?;
        let limit = page_limit(limit)?;
        with_secondary_snapshot!(self, index, {
            index_page_record(index.range_reverse_page(
                &start,
                range_end.as_deref(),
                cursor.as_ref(),
                limit,
            )?)
        })
    }
}

impl Drop for BindingSecondaryIndexSnapshot {
    fn drop(&mut self) {
        crate::fast_abi::unregister_index_snapshot(self.fast_handle.load(Ordering::Relaxed));
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::{ConfigRecord, ProllyEngine};

    struct TeamExtractor;

    impl SecondaryIndexExtractorCallback for TeamExtractor {
        fn extract(
            &self,
            _primary_key: Vec<u8>,
            _source_value: Vec<u8>,
        ) -> Result<Vec<IndexEntryRecord>, ProllyBindingError> {
            Ok(vec![IndexEntryRecord {
                term: b"red".to_vec(),
                projection: Some(b"Ada".to_vec()),
            }])
        }
    }

    #[test]
    fn portable_indexed_fixture_is_valid_json() {
        let fixture = include_str!("../../../../conformance/binding-indexed-fixtures.v1.json");
        let parsed: serde_json::Value = serde_json::from_str(fixture).unwrap();
        assert_eq!(parsed["schema_version"], 1);
        assert_eq!(parsed["packed_page"]["magic"], "PRPG");
        assert_eq!(parsed["packed_page"]["version"], 2);
        assert_eq!(parsed["packed_page"]["kind"], 5);
    }

    #[test]
    fn indexed_snapshot_queries_and_joins_source_inside_rust() {
        let engine =
            Arc::new(ProllyEngine::memory(ConfigRecord::from(prolly::Config::default())).unwrap());
        let registry = Arc::new(BindingIndexRegistry::new());
        registry
            .register(
                b"by_team".to_vec(),
                1,
                "tests.users.by-team/v1".to_string(),
                IndexProjectionRecord::Include,
                None,
                Arc::new(TeamExtractor),
            )
            .unwrap();
        let map = engine.indexed_map(b"users".to_vec(), registry).unwrap();
        map.ensure_index(b"by_team".to_vec()).unwrap();
        map.put(b"u1".to_vec(), br#"{"team":"red","name":"Ada"}"#.to_vec())
            .unwrap();

        let snapshot = map.snapshot().unwrap();
        let historical_id = snapshot.id();
        let index = snapshot.index(b"by_team".to_vec()).unwrap();
        let matches = index.exact(b"red".to_vec()).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].primary_key, b"u1");
        assert_eq!(matches[0].projection, Some(b"Ada".to_vec()));
        let records = index.records(b"red".to_vec()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].primary_key, b"u1");
        assert_eq!(
            records[0].source_value,
            br#"{"team":"red","name":"Ada"}"#.to_vec()
        );
        let page = index.exact_page(b"red".to_vec(), None, 1).unwrap();
        assert_eq!(page.matches.len(), 1);
        assert!(page.next_cursor.is_none());

        let health = map.health().unwrap();
        assert_eq!(health.active_indexes.len(), 1);
        assert!(
            map.verify_index(b"by_team".to_vec(), health.source_version.unwrap())
                .unwrap()
                .valid
        );
        let metrics = map.metrics().unwrap();
        assert!(metrics.records_extracted >= 1);
        map.delete(b"u1".to_vec()).unwrap();
        assert!(map.get(b"u1".to_vec()).unwrap().is_none());
        let historical = map.snapshot_by_id(historical_id.clone()).unwrap();
        assert_eq!(
            historical
                .index(b"by_team".to_vec())
                .unwrap()
                .exact(b"red".to_vec())
                .unwrap()
                .len(),
            1
        );
        let replacement = map
            .replace_index(
                b"by_team".to_vec(),
                2,
                "tests.users.by-team/v2".to_string(),
                IndexProjectionRecord::Include,
                None,
                Arc::new(TeamExtractor),
            )
            .unwrap();
        assert_eq!(replacement.generation, 2);
        assert_eq!(map.health().unwrap().active_indexes[0].generation, 2);
        assert_eq!(
            map.snapshot_by_id(historical_id)
                .unwrap()
                .index(b"by_team".to_vec())
                .unwrap()
                .exact(b"red".to_vec())
                .unwrap()
                .len(),
            1
        );
        let bundle = map.export_current().unwrap();
        assert!(!bundle.is_empty());
        assert!(!map
            .keep_last(1)
            .unwrap()
            .retained_source_versions
            .is_empty());
        assert!(map.plan_gc().unwrap().reachability.live_nodes > 0);

        let stale = map.health().unwrap().source_version.unwrap();
        map.put(b"u2".to_vec(), b"next".to_vec()).unwrap();
        let conditional = map
            .apply_if(Some(stale), vec![crate::delete_mutation(b"u2".to_vec())])
            .unwrap();
        assert_eq!(conditional.kind, IndexedUpdateKind::Conflict);
    }
}
