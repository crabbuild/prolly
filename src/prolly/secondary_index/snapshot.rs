use super::super::cid::Cid;
use super::super::error::Error;
use super::super::read::{EntryRef, ScanOutcome};
use super::super::store::Store;
use super::super::tree::Tree;
use super::super::versioned_map::{MapSnapshot, MapVersionId};
use super::super::Prolly;
use super::budget::{BudgetCounter, Deadline, QueryBudget};
use super::coordinator::IndexedMap;
use super::definition::IndexProjection;
use super::publication::IndexedStore;
use super::state::{IndexDescriptor, IndexedSnapshotId, IndexedSnapshotRecord};
use super::storage::{
    decode_physical_index_key, decode_physical_index_key_ref, term_bounds_exact,
    term_bounds_prefix, term_bounds_range, IndexValue, IndexValueRef, TermBounds,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ops::ControlFlow;

const CURSOR_MAGIC: &[u8; 8] = b"PSICUR01";
const CURSOR_VERSION: u32 = 1;
#[derive(Clone)]
struct SnapshotContext {
    snapshot: IndexedSnapshotId,
    source_version: MapVersionId,
    state_version: MapVersionId,
}

/// One decoded physical secondary-index match.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexMatch {
    pub term: Vec<u8>,
    pub primary_key: Vec<u8>,
    pub projection: Option<Vec<u8>>,
}

/// Callback-scoped decoded secondary-index match.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SecondaryIndexMatchRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
    pub projection: Option<&'a [u8]>,
}

impl SecondaryIndexMatchRef<'_> {
    pub fn to_owned(self) -> SecondaryIndexMatch {
        SecondaryIndexMatch {
            term: self.term.to_vec(),
            primary_key: self.primary_key.to_vec(),
            projection: self.projection.map(<[u8]>::to_vec),
        }
    }
}

/// Callback-scoped index match joined to its pinned source record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IndexedSourceRecordRef<'a> {
    pub term: &'a [u8],
    pub primary_key: &'a [u8],
    pub projection: Option<&'a [u8]>,
    pub source_value: &'a [u8],
}

impl IndexedSourceRecordRef<'_> {
    pub fn to_owned(self) -> IndexedSourceRecord {
        (self.primary_key.to_vec(), self.source_value.to_vec())
    }
}

/// One primary key and its optional projected bytes.
pub type ProjectedIndexEntry = (Vec<u8>, Option<Vec<u8>>);

/// One primary key and full source value resolved from the pinned snapshot.
pub type IndexedSourceRecord = (Vec<u8>, Vec<u8>);

/// Direction captured by a resumable secondary-index cursor.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SecondaryIndexDirection {
    Forward,
    Reverse,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum LogicalBounds {
    Exact(Vec<u8>),
    Prefix(Vec<u8>),
    Range(Vec<u8>, Option<Vec<u8>>),
}

/// Snapshot- and query-bound cursor for resumable secondary-index scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SecondaryIndexCursor {
    snapshot: IndexedSnapshotId,
    source_version: MapVersionId,
    state_version: MapVersionId,
    index_name: Vec<u8>,
    index_version: MapVersionId,
    definition_fingerprint: Cid,
    direction: SecondaryIndexDirection,
    bounds: LogicalBounds,
    raw_key: Option<Vec<u8>>,
}

#[derive(Serialize, Deserialize)]
struct CursorWire(
    IndexedSnapshotId,
    MapVersionId,
    MapVersionId,
    Vec<u8>,
    MapVersionId,
    Cid,
    SecondaryIndexDirection,
    LogicalBounds,
    Option<Vec<u8>>,
);

impl SecondaryIndexCursor {
    /// Encode this cursor into a stable, versioned binary envelope.
    pub fn to_bytes(&self) -> Result<Vec<u8>, Error> {
        let payload = serde_cbor::to_vec(&CursorWire(
            self.snapshot.clone(),
            self.source_version.clone(),
            self.state_version.clone(),
            self.index_name.clone(),
            self.index_version.clone(),
            self.definition_fingerprint.clone(),
            self.direction,
            self.bounds.clone(),
            self.raw_key.clone(),
        ))
        .map_err(|error| Error::Serialize(error.to_string()))?;
        let mut bytes = Vec::with_capacity(12 + payload.len());
        bytes.extend_from_slice(CURSOR_MAGIC);
        bytes.extend_from_slice(&CURSOR_VERSION.to_be_bytes());
        bytes.extend_from_slice(&payload);
        Ok(bytes)
    }

    /// Decode a canonical secondary-index cursor envelope.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Error> {
        if bytes.len() < 12 || &bytes[..8] != CURSOR_MAGIC {
            return Err(Error::Deserialize(
                "invalid secondary-index cursor envelope".to_string(),
            ));
        }
        let version = u32::from_be_bytes(bytes[8..12].try_into().expect("fixed cursor header"));
        if version != CURSOR_VERSION {
            return Err(Error::Deserialize(format!(
                "unsupported secondary-index cursor version {version}"
            )));
        }
        let CursorWire(
            snapshot,
            source_version,
            state_version,
            index_name,
            index_version,
            definition_fingerprint,
            direction,
            bounds,
            raw_key,
        ) = serde_cbor::from_slice(&bytes[12..])
            .map_err(|error| Error::Deserialize(error.to_string()))?;
        if index_name.is_empty() {
            return Err(Error::Deserialize(
                "secondary-index cursor has an empty index name".to_string(),
            ));
        }
        Ok(Self {
            snapshot,
            source_version,
            state_version,
            index_name,
            index_version,
            definition_fingerprint,
            direction,
            bounds,
            raw_key,
        })
    }

    pub fn direction(&self) -> SecondaryIndexDirection {
        self.direction
    }

    pub fn snapshot_id(&self) -> IndexedSnapshotId {
        self.snapshot.clone()
    }

    pub fn index_version(&self) -> &MapVersionId {
        &self.index_version
    }

    pub fn definition_fingerprint(&self) -> &Cid {
        &self.definition_fingerprint
    }
}

/// One bounded secondary-index query page.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SecondaryIndexPage {
    pub matches: Vec<SecondaryIndexMatch>,
    pub next_cursor: Option<SecondaryIndexCursor>,
}

/// Immutable collection-state-selected source and index view.
pub struct IndexedSnapshot<'a, S: Store> {
    id: IndexedSnapshotId,
    state: MapSnapshot<'a, S>,
    source: MapSnapshot<'a, S>,
    indexes: BTreeMap<Vec<u8>, SecondaryIndexSnapshot<'a, S>>,
}

impl<'a, S: Store> IndexedSnapshot<'a, S> {
    pub fn id(&self) -> &IndexedSnapshotId {
        &self.id
    }

    pub fn source_version(&self) -> &MapVersionId {
        self.source.id()
    }

    pub fn state_version(&self) -> &MapVersionId {
        self.state.id()
    }

    pub fn source(&self) -> &MapSnapshot<'a, S> {
        &self.source
    }

    pub fn state(&self) -> &MapSnapshot<'a, S> {
        &self.state
    }

    pub fn index(&self, name: impl AsRef<[u8]>) -> Result<&SecondaryIndexSnapshot<'a, S>, Error> {
        self.indexes
            .get(name.as_ref())
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.as_ref().to_vec(),
                source_version: self.source.id().clone(),
            })
    }

    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &SecondaryIndexSnapshot<'a, S>> {
        self.indexes.values()
    }
}

/// Query handle for one exact secondary-index snapshot.
pub struct SecondaryIndexSnapshot<'a, S: Store> {
    prolly: &'a Prolly<S>,
    snapshot_id: SnapshotContext,
    descriptor: IndexDescriptor,
    selected: super::state::IndexSnapshotRef,
    source_tree: Tree,
    index: MapSnapshot<'a, S>,
    max_projection_bytes: usize,
}

/// A finite-budget query session over one immutable secondary-index snapshot.
pub struct SecondaryIndexQuery<'query, 'engine, S: Store> {
    index: &'query SecondaryIndexSnapshot<'engine, S>,
    budget: QueryBudget,
}

impl<'query, 'engine, S: Store> SecondaryIndexQuery<'query, 'engine, S> {
    pub fn exact_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn range_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn exact_reverse_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn prefix_reverse_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn range_reverse_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index.page(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &self.budget,
        )
    }

    pub fn records(&self, term: &[u8]) -> Result<Vec<IndexedSourceRecord>, Error> {
        self.index.records_with_budget(term, &self.budget)
    }
}

impl<'a, S: Store> SecondaryIndexSnapshot<'a, S> {
    pub fn query(&self, budget: QueryBudget) -> Result<SecondaryIndexQuery<'_, 'a, S>, Error> {
        budget.validate()?;
        Ok(SecondaryIndexQuery {
            index: self,
            budget,
        })
    }

    pub fn name(&self) -> &[u8] {
        &self.descriptor.name
    }

    pub fn descriptor(&self) -> &IndexDescriptor {
        &self.descriptor
    }

    pub fn snapshot_ref(&self) -> &super::state::IndexSnapshotRef {
        &self.selected
    }

    pub fn tree(&self) -> &Tree {
        self.index.tree()
    }

    pub fn exact(&self, term: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(self.exact_page(
            term,
            None,
            QueryBudget::default().max_returned_entries,
        )?)
    }

    pub fn prefix(&self, prefix: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(self.prefix_page(
            prefix,
            None,
            QueryBudget::default().max_returned_entries,
        )?)
    }

    pub fn range(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
    ) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(self.range_page(
            start_term,
            end_term,
            None,
            QueryBudget::default().max_returned_entries,
        )?)
    }

    pub fn scan_exact(
        &self,
        term: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_exact_until(term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_exact_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            visit,
        )
    }

    pub fn scan_prefix(
        &self,
        prefix: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_until(prefix, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_prefix_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Forward,
            visit,
        )
    }

    pub fn scan_range(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_until(start_term, end_term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_range_until<B>(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            visit,
        )
    }

    pub fn scan_exact_reverse(
        &self,
        term: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_exact_reverse_until(term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_exact_reverse_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Reverse,
            visit,
        )
    }

    pub fn scan_prefix_reverse(
        &self,
        prefix: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_reverse_until(prefix, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_prefix_reverse_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Reverse,
            visit,
        )
    }

    pub fn scan_range_reverse(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_reverse_until(start_term, end_term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_range_reverse_until<B>(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            visit,
        )
    }

    pub fn primary_keys(&self, term: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .exact(term)?
            .into_iter()
            .map(|matched| matched.primary_key)
            .collect())
    }

    pub fn projected(&self, term: &[u8]) -> Result<Vec<ProjectedIndexEntry>, Error> {
        Ok(self
            .exact(term)?
            .into_iter()
            .map(|matched| (matched.primary_key, matched.projection))
            .collect())
    }

    /// Resolve matching primary keys with one ordered batched source read.
    pub fn records(&self, term: &[u8]) -> Result<Vec<IndexedSourceRecord>, Error> {
        self.records_with_budget(term, &QueryBudget::default())
    }

    fn records_with_budget(
        &self,
        term: &[u8],
        budget: &QueryBudget,
    ) -> Result<Vec<IndexedSourceRecord>, Error> {
        let mut records = Vec::new();
        self.scan_records_until_with_budget(term, budget, |record| {
            records.push(record.to_owned());
            ControlFlow::<()>::Continue(())
        })?;
        Ok(records)
    }

    pub fn scan_records(
        &self,
        term: &[u8],
        mut visit: impl for<'row> FnMut(IndexedSourceRecordRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_records_until(term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })?
            .visited)
    }

    pub fn scan_records_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(IndexedSourceRecordRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_records_until_with_budget(term, &QueryBudget::default(), visit)
    }

    fn scan_records_until_with_budget<B>(
        &self,
        term: &[u8],
        budget: &QueryBudget,
        mut visit: impl for<'row> FnMut(IndexedSourceRecordRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        let mut source = self.prolly.read(&self.source_tree)?;
        let mut delivered = 0u64;
        let mut source_fetches = 0usize;
        let mut returned_bytes = 0usize;
        let mut accounted_memory = 0usize;
        let mut terminal = None;
        self.scan_matches_until_with_budget(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            budget,
            |matched| {
                if terminal.is_some() {
                    return ControlFlow::Break(());
                }
                if source_fetches == budget.max_source_fetches {
                    terminal = Some(Err(Error::IndexResourceLimitExceeded {
                        resource: "query_source_fetches",
                        limit: budget.max_source_fetches,
                        actual: source_fetches.saturating_add(1),
                    }));
                    return ControlFlow::Break(());
                }
                source_fetches += 1;
                let result = source.get_with(matched.primary_key, |source_value| {
                    let retained = matched
                        .primary_key
                        .len()
                        .checked_add(source_value.len())
                        .ok_or(Error::IndexResourceLimitExceeded {
                            resource: "query_returned_bytes",
                            limit: budget.max_returned_bytes,
                            actual: usize::MAX,
                        })?;
                    counter.charge(
                        "query_returned_bytes",
                        &mut returned_bytes,
                        retained,
                        budget.max_returned_bytes,
                    )?;
                    counter.charge(
                        "query_accounted_memory_bytes",
                        &mut accounted_memory,
                        retained,
                        budget.max_accounted_memory_bytes,
                    )?;
                    counter.check_elapsed("query_elapsed_millis", budget.max_elapsed)?;
                    delivered = delivered.saturating_add(1);
                    Ok(visit(IndexedSourceRecordRef {
                        term: matched.term,
                        primary_key: matched.primary_key,
                        projection: matched.projection,
                        source_value,
                    }))
                });
                match result {
                    Ok(Some(Ok(ControlFlow::Continue(())))) => ControlFlow::Continue(()),
                    Ok(Some(Ok(ControlFlow::Break(value)))) => {
                        terminal = Some(Ok(value));
                        ControlFlow::Break(())
                    }
                    Ok(Some(Err(error))) | Err(error) => {
                        terminal = Some(Err(error));
                        ControlFlow::Break(())
                    }
                    Ok(None) => {
                        terminal = Some(Err(Error::IndexSnapshotMismatch {
                            name: self.descriptor.name.clone(),
                            source_version: self.snapshot_id.source_version.clone(),
                            reason: format!(
                                "index references missing source primary key {:?}",
                                matched.primary_key
                            ),
                        }));
                        ControlFlow::Break(())
                    }
                }
            },
        )?;
        match terminal {
            Some(Ok(value)) => Ok(ScanOutcome::stopped(delivered, value)),
            Some(Err(error)) => Err(error),
            None => Ok(ScanOutcome::complete(delivered)),
        }
    }

    pub fn exact_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    pub fn exact_reverse_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    pub fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    pub fn prefix_reverse_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    pub fn range_page(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    pub fn range_reverse_page(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start_term.to_vec(), end_term.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &QueryBudget::default(),
        )
    }

    fn scan_matches_until<B>(
        &self,
        logical: LogicalBounds,
        direction: SecondaryIndexDirection,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let budget = QueryBudget::default();
        self.scan_matches_until_with_budget(logical, direction, &budget, visit)
    }

    fn scan_matches_until_with_budget<B>(
        &self,
        logical: LogicalBounds,
        direction: SecondaryIndexDirection,
        budget: &QueryBudget,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        budget.validate()?;
        let started = Deadline::new();
        let mut scanned = 0usize;
        let mut returned = 0usize;
        let mut returned_bytes = 0usize;
        let bounds = physical_bounds(&logical)?;
        let mut term_scratch = Vec::new();
        let mut primary_key_scratch = Vec::new();
        let mut handle = |entry: EntryRef<'_>| {
            scanned = scanned.saturating_add(1);
            returned = returned.saturating_add(1);
            returned_bytes = returned_bytes
                .saturating_add(entry.key().len())
                .saturating_add(entry.value().len());
            if scanned > budget.max_scanned_entries
                || returned > budget.max_returned_entries
                || returned_bytes > budget.max_returned_bytes
                || returned_bytes > budget.max_accounted_memory_bytes
                || started.exceeded(budget.max_elapsed)
            {
                return ControlFlow::Break(Err(Error::IndexResourceLimitExceeded {
                    resource: "query_scan_budget",
                    limit: budget.max_scanned_entries.min(budget.max_returned_entries),
                    actual: scanned.max(returned),
                }));
            }
            let matched = match self.decode_match_ref(
                entry.key(),
                entry.value(),
                &mut term_scratch,
                &mut primary_key_scratch,
            ) {
                Ok(matched) => matched,
                Err(error) => return ControlFlow::Break(Err(error)),
            };
            match visit(matched) {
                ControlFlow::Continue(()) => ControlFlow::Continue(()),
                ControlFlow::Break(value) => ControlFlow::Break(Ok(value)),
            }
        };
        let outcome = match direction {
            SecondaryIndexDirection::Forward => self.prolly.scan_range_until(
                self.index.tree(),
                &bounds.start,
                bounds.end.as_deref(),
                &mut handle,
            )?,
            SecondaryIndexDirection::Reverse => self.prolly.scan_range_reverse_until(
                self.index.tree(),
                &bounds.start,
                bounds.end.as_deref(),
                &mut handle,
            )?,
        };
        match outcome.break_value {
            Some(Ok(value)) => Ok(ScanOutcome::stopped(outcome.visited, value)),
            Some(Err(error)) => Err(error),
            None => Ok(ScanOutcome::complete(outcome.visited)),
        }
    }

    fn page(
        &self,
        logical: LogicalBounds,
        direction: SecondaryIndexDirection,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
        budget: &QueryBudget,
    ) -> Result<SecondaryIndexPage, Error> {
        budget.validate()?;
        let counter = BudgetCounter::new();
        let max_page_entries = budget.max_page_entries.min(budget.max_returned_entries);
        if limit > max_page_entries {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "query_page_entries",
                limit: max_page_entries,
                actual: limit,
            });
        }
        if let Some(cursor) = cursor {
            self.validate_cursor(cursor, &logical, direction)?;
        }
        if limit == 0 {
            let next_cursor = cursor.cloned().or_else(|| {
                Some(SecondaryIndexCursor {
                    snapshot: self.snapshot_id.snapshot.clone(),
                    source_version: self.snapshot_id.source_version.clone(),
                    state_version: self.snapshot_id.state_version.clone(),
                    index_name: self.descriptor.name.clone(),
                    index_version: MapVersionId::for_tree(&self.selected.tree)
                        .expect("validated index tree"),
                    definition_fingerprint: self.descriptor.fingerprint.clone(),
                    direction,
                    bounds: logical,
                    raw_key: None,
                })
            });
            return Ok(SecondaryIndexPage {
                matches: Vec::new(),
                next_cursor,
            });
        }
        let bounds = physical_bounds(&logical)?;
        let mut matches = Vec::with_capacity(limit);
        let mut returned_bytes = 0usize;
        let mut accounted_memory = 0usize;
        let mut scanned = 0usize;
        let mut has_more = false;
        let mut raw_key = None;
        let after = cursor.and_then(|cursor| cursor.raw_key.as_deref());
        let mut handle = |entry: EntryRef<'_>| {
            if direction == SecondaryIndexDirection::Forward
                && after.is_some_and(|after| entry.key() <= after)
            {
                return ControlFlow::Continue(());
            }
            scanned = scanned.saturating_add(1);
            if scanned > budget.max_scanned_entries {
                return ControlFlow::Break(Err(Error::IndexResourceLimitExceeded {
                    resource: "query_scanned_entries",
                    limit: budget.max_scanned_entries,
                    actual: scanned,
                }));
            }
            if matches.len() == limit {
                has_more = true;
                return ControlFlow::Break(Ok(()));
            }
            let matched = match self.decode_match(entry.key(), entry.value()) {
                Ok(matched) => matched,
                Err(error) => return ControlFlow::Break(Err(error)),
            };
            let retained = matched
                .term
                .len()
                .checked_add(matched.primary_key.len())
                .and_then(|bytes| {
                    bytes.checked_add(matched.projection.as_ref().map_or(0, Vec::len))
                })
                .ok_or(Error::IndexResourceLimitExceeded {
                    resource: "query_returned_bytes",
                    limit: budget.max_returned_bytes,
                    actual: usize::MAX,
                });
            let retained = match retained {
                Ok(retained) => retained,
                Err(error) => return ControlFlow::Break(Err(error)),
            };
            if let Err(error) = counter
                .charge(
                    "query_returned_bytes",
                    &mut returned_bytes,
                    retained,
                    budget.max_returned_bytes,
                )
                .and_then(|_| {
                    counter.charge(
                        "query_accounted_memory_bytes",
                        &mut accounted_memory,
                        retained,
                        budget.max_accounted_memory_bytes,
                    )
                })
                .and_then(|_| counter.check_elapsed("query_elapsed_millis", budget.max_elapsed))
            {
                return ControlFlow::Break(Err(error));
            }
            raw_key = Some(entry.key().to_vec());
            matches.push(matched);
            ControlFlow::Continue(())
        };
        let outcome = match direction {
            SecondaryIndexDirection::Forward => {
                let start = after.unwrap_or(&bounds.start);
                self.prolly.scan_range_until(
                    self.index.tree(),
                    start,
                    bounds.end.as_deref(),
                    &mut handle,
                )?
            }
            SecondaryIndexDirection::Reverse => {
                let end = after.or(bounds.end.as_deref());
                self.prolly.scan_range_reverse_until(
                    self.index.tree(),
                    &bounds.start,
                    end,
                    &mut handle,
                )?
            }
        };
        if let Some(Err(error)) = outcome.break_value {
            return Err(error);
        }
        let next_cursor = has_more.then(|| SecondaryIndexCursor {
            snapshot: self.snapshot_id.snapshot.clone(),
            source_version: self.snapshot_id.source_version.clone(),
            state_version: self.snapshot_id.state_version.clone(),
            index_name: self.descriptor.name.clone(),
            index_version: MapVersionId::for_tree(&self.selected.tree)
                .expect("validated index tree"),
            definition_fingerprint: self.descriptor.fingerprint.clone(),
            direction,
            bounds: logical,
            raw_key,
        });
        Ok(SecondaryIndexPage {
            matches,
            next_cursor,
        })
    }

    fn validate_cursor(
        &self,
        cursor: &SecondaryIndexCursor,
        bounds: &LogicalBounds,
        direction: SecondaryIndexDirection,
    ) -> Result<(), Error> {
        let valid = cursor.snapshot == self.snapshot_id.snapshot
            && cursor.source_version == self.snapshot_id.source_version
            && cursor.state_version == self.snapshot_id.state_version
            && cursor.index_name == self.descriptor.name
            && cursor.index_version
                == MapVersionId::for_tree(&self.selected.tree).expect("validated index tree")
            && cursor.definition_fingerprint == self.descriptor.fingerprint
            && cursor.direction == direction
            && &cursor.bounds == bounds;
        let physical_key_valid = match cursor.raw_key.as_deref() {
            None => true,
            Some(raw_key) => {
                let physical = physical_bounds(bounds)?;
                raw_key >= physical.start.as_slice()
                    && physical.end.as_deref().map_or(true, |end| raw_key < end)
                    && decode_physical_index_key(raw_key).is_ok()
            }
        };
        if !valid || !physical_key_valid {
            Err(Error::IndexCursorVersionMismatch {
                expected: format!(
                    "source={}, state={}, index={}, direction={direction:?}, bounds={bounds:?}",
                    self.snapshot_id.source_version,
                    self.snapshot_id.state_version,
                    MapVersionId::for_tree(&self.selected.tree).expect("validated index tree")
                ),
                actual: format!(
                    "source={}, state={}, index={}, direction={:?}, bounds={:?}",
                    cursor.source_version,
                    cursor.state_version,
                    cursor.index_version,
                    cursor.direction,
                    cursor.bounds
                ),
            })
        } else {
            Ok(())
        }
    }

    fn collect_page(&self, page: SecondaryIndexPage) -> Result<Vec<SecondaryIndexMatch>, Error> {
        if page.next_cursor.is_some() {
            let limit = QueryBudget::default().max_returned_entries;
            return Err(Error::IndexResourceLimitExceeded {
                resource: "query_returned_entries",
                limit,
                actual: limit.saturating_add(1),
            });
        }
        Ok(page.matches)
    }

    fn decode_match(&self, key: &[u8], value: &[u8]) -> Result<SecondaryIndexMatch, Error> {
        let decoded = decode_physical_index_key(key)?;
        let stored = IndexValue::from_bytes(value, self.max_projection_bytes)?;
        let projection = match (self.descriptor.projection, stored) {
            (IndexProjection::KeysOnly, IndexValue::KeysOnly) => None,
            (IndexProjection::Include, IndexValue::Included(bytes))
            | (IndexProjection::All, IndexValue::FullSource(bytes)) => Some(bytes),
            _ => {
                return Err(Error::IndexSnapshotMismatch {
                    name: self.descriptor.name.clone(),
                    source_version: self.snapshot_id.source_version.clone(),
                    reason: "stored projection value does not match its descriptor".to_string(),
                })
            }
        };
        Ok(SecondaryIndexMatch {
            term: decoded.term,
            primary_key: decoded.primary_key,
            projection,
        })
    }

    fn decode_match_ref<'row>(
        &self,
        key: &'row [u8],
        value: &'row [u8],
        term_scratch: &'row mut Vec<u8>,
        primary_key_scratch: &'row mut Vec<u8>,
    ) -> Result<SecondaryIndexMatchRef<'row>, Error> {
        let decoded = decode_physical_index_key_ref(key, term_scratch, primary_key_scratch)?;
        let stored = IndexValueRef::decode(value, self.max_projection_bytes)?;
        let projection = match (self.descriptor.projection, stored) {
            (IndexProjection::KeysOnly, IndexValueRef::KeysOnly) => None,
            (IndexProjection::Include, IndexValueRef::Included(bytes))
            | (IndexProjection::All, IndexValueRef::FullSource(bytes)) => Some(bytes),
            _ => {
                return Err(Error::IndexSnapshotMismatch {
                    name: self.descriptor.name.clone(),
                    source_version: self.snapshot_id.source_version.clone(),
                    reason: "stored projection value does not match its descriptor".to_string(),
                })
            }
        };
        Ok(SecondaryIndexMatchRef {
            term: decoded.term,
            primary_key: decoded.primary_key,
            projection,
        })
    }
}

fn physical_bounds(bounds: &LogicalBounds) -> Result<TermBounds, Error> {
    match bounds {
        LogicalBounds::Exact(term) => Ok(term_bounds_exact(term)),
        LogicalBounds::Prefix(prefix) => Ok(term_bounds_prefix(prefix)),
        LogicalBounds::Range(start, end) => term_bounds_range(start, end.as_deref()),
    }
}

impl<'a, S> IndexedMap<'a, S>
where
    S: IndexedStore,
{
    /// Pin one canonical collection-state root and all immutable trees it names.
    pub fn snapshot(&self) -> Result<IndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state()?;
        let record = loaded.state.head_snapshot()?.clone();
        let record_id = loaded.state.head.clone();
        self.resolve_snapshot(loaded.tree, loaded.state, record_id, record)
    }

    /// Reopen the retained canonical snapshot containing this source tree.
    pub fn snapshot_at(
        &self,
        source_version: &MapVersionId,
    ) -> Result<IndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state()?;
        let (record_id, record) = retained_snapshot_for_source(&loaded.state, source_version)?;
        self.resolve_snapshot(loaded.tree, loaded.state, record_id, record)
    }

    /// Reopen the exact retained content-addressed snapshot represented by `id`.
    pub fn snapshot_by_id(&self, id: &IndexedSnapshotId) -> Result<IndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state()?;
        let record = loaded.state.snapshots.get(id).cloned().ok_or_else(|| {
            Error::InvalidVersionedMap(format!(
                "indexed snapshot {:?} is not retained",
                id.as_cid()
            ))
        })?;
        self.resolve_snapshot(loaded.tree, loaded.state, id.clone(), record)
    }

    fn resolve_snapshot(
        &self,
        state_tree: Tree,
        state: super::state::IndexedCollectionState,
        record_id: IndexedSnapshotId,
        record: IndexedSnapshotRecord,
    ) -> Result<IndexedSnapshot<'a, S>, Error> {
        let source_version = MapVersionId::for_tree(&record.source.tree)?;
        let source = MapSnapshot::from_tree(self.prolly, record.source.tree.clone(), true)?;
        let state_snapshot = MapSnapshot::from_tree(self.prolly, state_tree, true)?;
        let snapshot_id = SnapshotContext {
            snapshot: record_id,
            source_version: source_version.clone(),
            state_version: state_snapshot.id().clone(),
        };
        let mut indexes = BTreeMap::new();
        for selected in record.indexes {
            let descriptor = state
                .descriptors
                .get(&(
                    selected.name.clone(),
                    selected.descriptor_fingerprint.clone(),
                ))
                .cloned()
                .ok_or_else(|| Error::IndexSnapshotMismatch {
                    name: selected.name.clone(),
                    source_version: source_version.clone(),
                    reason: "canonical descriptor is missing".to_string(),
                })?;
            let runtime = self
                .runtime_definition_for_descriptor(&descriptor)?
                .ok_or_else(|| Error::IndexRuntimeDefinitionMissing {
                    name: descriptor.name.clone(),
                    generation: descriptor.generation,
                })?;
            let runtime_descriptor = IndexDescriptor::from_runtime(&self.source_map_id, &runtime)?;
            if runtime_descriptor.fingerprint != descriptor.fingerprint {
                return Err(Error::IndexDefinitionMismatch {
                    name: descriptor.name.clone(),
                    persisted: descriptor.fingerprint.clone(),
                    runtime: runtime_descriptor.fingerprint,
                });
            }
            let index = MapSnapshot::from_tree(self.prolly, selected.tree.clone(), true)?;
            indexes.insert(
                selected.name.clone(),
                SecondaryIndexSnapshot {
                    prolly: self.prolly,
                    snapshot_id: snapshot_id.clone(),
                    descriptor,
                    selected,
                    source_tree: source.tree().clone(),
                    index,
                    max_projection_bytes: match runtime.projection() {
                        IndexProjection::KeysOnly => 0,
                        IndexProjection::Include => runtime.limits().max_projection_bytes,
                        IndexProjection::All => runtime.limits().max_all_value_bytes,
                    },
                },
            );
        }
        Ok(IndexedSnapshot {
            id: snapshot_id.snapshot.clone(),
            state: state_snapshot,
            source,
            indexes,
        })
    }
}

fn retained_snapshot_for_source(
    state: &super::state::IndexedCollectionState,
    source_version: &MapVersionId,
) -> Result<(IndexedSnapshotId, IndexedSnapshotRecord), Error> {
    let mut cursor = Some(state.head.clone());
    while let Some(id) = cursor {
        let snapshot = state.snapshots.get(&id).ok_or_else(|| {
            Error::InvalidVersionedMap("snapshot ancestry is incomplete".to_string())
        })?;
        if MapVersionId::for_tree(&snapshot.source.tree)? == *source_version {
            return Ok((id, snapshot.clone()));
        }
        cursor = snapshot.parent.clone();
    }
    Err(Error::InvalidVersionedMap(format!(
        "indexed source version {source_version} is not retained"
    )))
}
