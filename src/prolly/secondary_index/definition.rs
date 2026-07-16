use serde::{Deserialize, Serialize};
use std::collections::{btree_map::Entry, BTreeMap};
use std::fmt;
use std::sync::Arc;

use super::super::error::Error;

/// Bytes stored beside a matching primary key in the physical index tree.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum IndexProjection {
    /// Store only the physical `(term, primary_key)` key.
    #[default]
    KeysOnly,
    /// Store extractor-supplied deterministic projection bytes.
    Include,
    /// Store the complete raw source value.
    All,
}

/// One logical index emission from one source record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexEntry {
    /// Logical term used for exact, prefix, and range lookup.
    pub term: Vec<u8>,
    /// Application projection bytes. Present only for [`IndexProjection::Include`].
    pub projection: Option<Vec<u8>>,
}

/// Callback-scoped index emission used by allocation-reusing extractors.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SecondaryIndexEntryRef<'a> {
    pub term: &'a [u8],
    pub projection: Option<&'a [u8]>,
}

/// Extractor that emits borrowed terms/projections into a synchronous sink.
pub trait StreamingSecondaryIndexExtractor: Send + Sync {
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
        emit: &mut dyn FnMut(SecondaryIndexEntryRef<'_>) -> Result<(), SecondaryIndexError>,
    ) -> Result<(), SecondaryIndexError>;
}

impl SecondaryIndexEntry {
    /// Emit a term without application projection bytes.
    pub fn term(term: impl AsRef<[u8]>) -> Self {
        Self {
            term: term.as_ref().to_vec(),
            projection: None,
        }
    }

    /// Emit a term and deterministic application projection bytes.
    pub fn included(term: impl AsRef<[u8]>, projection: impl AsRef<[u8]>) -> Self {
        Self {
            term: term.as_ref().to_vec(),
            projection: Some(projection.as_ref().to_vec()),
        }
    }
}

/// Error returned by an application index extractor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexError {
    reason: String,
}

impl SecondaryIndexError {
    /// Create an extractor failure with a stable application-facing reason.
    pub fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
        }
    }

    /// Borrow the failure reason.
    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl fmt::Display for SecondaryIndexError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.reason)
    }
}

impl std::error::Error for SecondaryIndexError {}

/// Deterministic runtime callback that derives zero or more index emissions.
pub trait SecondaryIndexExtractor: Send + Sync + 'static {
    /// Derive emissions for one source record.
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError>;
}

impl<F> SecondaryIndexExtractor for F
where
    F: Fn(&[u8], &[u8]) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError>
        + Send
        + Sync
        + 'static,
{
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError> {
        self(primary_key, source_value)
    }
}

struct TermsExtractor<F>(F);

impl<F> SecondaryIndexExtractor for TermsExtractor<F>
where
    F: Fn(&[u8], &[u8]) -> Result<Vec<Vec<u8>>, SecondaryIndexError> + Send + Sync + 'static,
{
    fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError> {
        (self.0)(primary_key, source_value)
            .map(|terms| terms.into_iter().map(SecondaryIndexEntry::term).collect())
    }
}

/// Resource bounds applied before index-derived buffers are published.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexLimits {
    pub max_term_bytes: usize,
    pub max_projection_bytes: usize,
    pub max_all_value_bytes: usize,
    pub max_terms_per_record: usize,
    pub max_projected_bytes_per_record: usize,
    pub max_derived_mutations_per_transaction: usize,
    pub max_projected_bytes_per_transaction: usize,
    pub max_indexes: usize,
    pub build_page_size: usize,
    pub max_temporary_sort_bytes: usize,
    pub max_bundle_nodes: usize,
    pub max_bundle_bytes: usize,
    pub max_verification_entries: usize,
    pub max_write_retries: usize,
    pub max_build_retries: usize,
}

impl Default for SecondaryIndexLimits {
    fn default() -> Self {
        Self {
            max_term_bytes: 4 * 1024,
            max_projection_bytes: 64 * 1024,
            max_all_value_bytes: 1024 * 1024,
            max_terms_per_record: 1024,
            max_projected_bytes_per_record: 1024 * 1024,
            max_derived_mutations_per_transaction: 100_000,
            max_projected_bytes_per_transaction: 64 * 1024 * 1024,
            max_indexes: 32,
            build_page_size: 4096,
            max_temporary_sort_bytes: 256 * 1024 * 1024,
            max_bundle_nodes: 1_000_000,
            max_bundle_bytes: 1024 * 1024 * 1024,
            max_verification_entries: 10_000_000,
            max_write_retries: 8,
            max_build_retries: 8,
        }
    }
}

impl SecondaryIndexLimits {
    fn validate(&self) -> Result<(), Error> {
        let values = [
            ("max_term_bytes", self.max_term_bytes),
            ("max_projection_bytes", self.max_projection_bytes),
            ("max_all_value_bytes", self.max_all_value_bytes),
            ("max_terms_per_record", self.max_terms_per_record),
            (
                "max_projected_bytes_per_record",
                self.max_projected_bytes_per_record,
            ),
            (
                "max_derived_mutations_per_transaction",
                self.max_derived_mutations_per_transaction,
            ),
            (
                "max_projected_bytes_per_transaction",
                self.max_projected_bytes_per_transaction,
            ),
            ("max_indexes", self.max_indexes),
            ("build_page_size", self.build_page_size),
            ("max_temporary_sort_bytes", self.max_temporary_sort_bytes),
            ("max_bundle_nodes", self.max_bundle_nodes),
            ("max_bundle_bytes", self.max_bundle_bytes),
            ("max_verification_entries", self.max_verification_entries),
            ("max_write_retries", self.max_write_retries),
            ("max_build_retries", self.max_build_retries),
        ];
        if let Some((field, _)) = values.into_iter().find(|(_, value)| *value == 0) {
            return Err(Error::InvalidIndexDefinition {
                reason: format!("{field} must be greater than zero"),
            });
        }
        Ok(())
    }
}

/// One immutable runtime index definition.
#[derive(Clone)]
pub struct SecondaryIndex {
    name: Vec<u8>,
    generation: u64,
    extractor_id: String,
    projection: IndexProjection,
    limits: SecondaryIndexLimits,
    extractor: Arc<dyn SecondaryIndexExtractor>,
}

impl fmt::Debug for SecondaryIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SecondaryIndex")
            .field("name", &self.name)
            .field("generation", &self.generation)
            .field("extractor_id", &self.extractor_id)
            .field("projection", &self.projection)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}

impl SecondaryIndex {
    /// Define a non-unique keys-only index from a zero-or-more term callback.
    pub fn non_unique<N, I, F>(
        name: N,
        generation: u64,
        extractor_id: I,
        extractor: F,
    ) -> Result<Self, Error>
    where
        N: AsRef<[u8]>,
        I: Into<String>,
        F: Fn(&[u8], &[u8]) -> Result<Vec<Vec<u8>>, SecondaryIndexError> + Send + Sync + 'static,
    {
        Self::builder(name, generation, extractor_id)
            .projection(IndexProjection::KeysOnly)
            .extract_terms(extractor)
    }

    /// Start a general runtime definition builder.
    pub fn builder<N, I>(name: N, generation: u64, extractor_id: I) -> SecondaryIndexBuilder
    where
        N: AsRef<[u8]>,
        I: Into<String>,
    {
        SecondaryIndexBuilder {
            name: name.as_ref().to_vec(),
            generation,
            extractor_id: extractor_id.into(),
            projection: IndexProjection::KeysOnly,
            limits: SecondaryIndexLimits::default(),
        }
    }

    /// Stable arbitrary-byte index name.
    pub fn name(&self) -> &[u8] {
        &self.name
    }
    /// Monotonically increasing semantic definition generation.
    pub fn generation(&self) -> u64 {
        self.generation
    }
    /// Application-controlled identity for deterministic extractor semantics.
    pub fn extractor_id(&self) -> &str {
        &self.extractor_id
    }
    /// Projection mode for physical index values.
    pub fn projection(&self) -> IndexProjection {
        self.projection
    }
    /// Resource bounds attached to this definition.
    pub fn limits(&self) -> &SecondaryIndexLimits {
        &self.limits
    }

    /// Run and validate the deterministic extractor for one record.
    pub fn extract(
        &self,
        primary_key: &[u8],
        source_value: &[u8],
    ) -> Result<Vec<SecondaryIndexEntry>, Error> {
        if self.projection == IndexProjection::All
            && source_value.len() > self.limits.max_all_value_bytes
        {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "all_source_value_bytes",
                limit: self.limits.max_all_value_bytes,
                actual: source_value.len(),
            });
        }
        let entries = self
            .extractor
            .extract(primary_key, source_value)
            .map_err(|error| Error::IndexExtractionFailed {
                name: self.name.clone(),
                primary_key: primary_key.to_vec(),
                reason: error.to_string(),
            })?;
        self.validate_entries(primary_key, entries)
    }

    fn validate_entries(
        &self,
        primary_key: &[u8],
        entries: Vec<SecondaryIndexEntry>,
    ) -> Result<Vec<SecondaryIndexEntry>, Error> {
        if entries.len() > self.limits.max_terms_per_record {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "terms_per_record",
                limit: self.limits.max_terms_per_record,
                actual: entries.len(),
            });
        }
        let mut canonical = BTreeMap::<Vec<u8>, Option<Vec<u8>>>::new();
        let mut projected_bytes = 0usize;
        for entry in entries {
            if entry.term.len() > self.limits.max_term_bytes {
                return Err(Error::IndexResourceLimitExceeded {
                    resource: "term_bytes",
                    limit: self.limits.max_term_bytes,
                    actual: entry.term.len(),
                });
            }
            let projection_matches = match self.projection {
                IndexProjection::KeysOnly | IndexProjection::All => entry.projection.is_none(),
                IndexProjection::Include => entry.projection.is_some(),
            };
            if !projection_matches {
                return Err(Error::IndexProjectionMismatch {
                    name: self.name.clone(),
                    mode: self.projection,
                    primary_key: primary_key.to_vec(),
                });
            }
            if let Some(projection) = &entry.projection {
                if projection.len() > self.limits.max_projection_bytes {
                    return Err(Error::IndexResourceLimitExceeded {
                        resource: "projection_bytes",
                        limit: self.limits.max_projection_bytes,
                        actual: projection.len(),
                    });
                }
                projected_bytes = projected_bytes.saturating_add(projection.len());
            }
            match canonical.entry(entry.term) {
                Entry::Vacant(slot) => {
                    slot.insert(entry.projection);
                }
                Entry::Occupied(slot) if slot.get() == &entry.projection => {}
                Entry::Occupied(slot) => {
                    return Err(Error::ConflictingIndexProjection {
                        name: self.name.clone(),
                        primary_key: primary_key.to_vec(),
                        term: slot.key().clone(),
                    });
                }
            }
        }
        if projected_bytes > self.limits.max_projected_bytes_per_record {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "projected_bytes_per_record",
                limit: self.limits.max_projected_bytes_per_record,
                actual: projected_bytes,
            });
        }
        Ok(canonical
            .into_iter()
            .map(|(term, projection)| SecondaryIndexEntry { term, projection })
            .collect())
    }
}

/// Builder for a general projection-aware runtime definition.
#[derive(Clone, Debug)]
pub struct SecondaryIndexBuilder {
    name: Vec<u8>,
    generation: u64,
    extractor_id: String,
    projection: IndexProjection,
    limits: SecondaryIndexLimits,
}

impl SecondaryIndexBuilder {
    /// Select the persisted projection mode.
    pub fn projection(mut self, projection: IndexProjection) -> Self {
        self.projection = projection;
        self
    }
    /// Override all default resource bounds.
    pub fn limits(mut self, limits: SecondaryIndexLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Finish with a projection-aware entry callback.
    pub fn extract<F>(self, extractor: F) -> Result<SecondaryIndex, Error>
    where
        F: Fn(&[u8], &[u8]) -> Result<Vec<SecondaryIndexEntry>, SecondaryIndexError>
            + Send
            + Sync
            + 'static,
    {
        self.finish(Arc::new(extractor))
    }

    /// Finish with a term-only callback for `KeysOnly` or `All` projection.
    pub fn extract_terms<F>(self, extractor: F) -> Result<SecondaryIndex, Error>
    where
        F: Fn(&[u8], &[u8]) -> Result<Vec<Vec<u8>>, SecondaryIndexError> + Send + Sync + 'static,
    {
        if self.projection == IndexProjection::Include {
            return Err(Error::InvalidIndexDefinition {
                reason: "Include projection requires an entry extractor".to_string(),
            });
        }
        self.finish(Arc::new(TermsExtractor(extractor)))
    }

    fn finish(self, extractor: Arc<dyn SecondaryIndexExtractor>) -> Result<SecondaryIndex, Error> {
        if self.name.is_empty() {
            return Err(Error::InvalidIndexDefinition {
                reason: "index name must not be empty".to_string(),
            });
        }
        if self.generation == 0 {
            return Err(Error::InvalidIndexDefinition {
                reason: "index generation must be greater than zero".to_string(),
            });
        }
        if self.extractor_id.is_empty() {
            return Err(Error::InvalidIndexDefinition {
                reason: "extractor ID must not be empty".to_string(),
            });
        }
        self.limits.validate()?;
        Ok(SecondaryIndex {
            name: self.name,
            generation: self.generation,
            extractor_id: self.extractor_id,
            projection: self.projection,
            limits: self.limits,
            extractor,
        })
    }
}

/// Deterministically ordered runtime definitions supplied when opening an indexed map.
#[derive(Clone, Debug, Default)]
pub struct SecondaryIndexRegistry {
    indexes: BTreeMap<Vec<u8>, SecondaryIndex>,
}

impl SecondaryIndexRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register one definition, rejecting duplicate logical names.
    pub fn register(mut self, index: SecondaryIndex) -> Result<Self, Error> {
        if self.indexes.contains_key(index.name()) {
            return Err(Error::InvalidIndexDefinition {
                reason: format!("duplicate index name {:?}", index.name()),
            });
        }
        self.indexes.insert(index.name.clone(), index);
        Ok(self)
    }

    /// Borrow a definition by its exact raw name.
    pub fn get(&self, name: &[u8]) -> Option<&SecondaryIndex> {
        self.indexes.get(name)
    }
    /// Iterate definitions in raw-name order.
    pub fn iter(&self) -> impl ExactSizeIterator<Item = &SecondaryIndex> {
        self.indexes.values()
    }
    /// Number of definitions in the registry.
    pub fn len(&self) -> usize {
        self.indexes.len()
    }
    /// Whether the registry contains no definitions.
    pub fn is_empty(&self) -> bool {
        self.indexes.is_empty()
    }
}
