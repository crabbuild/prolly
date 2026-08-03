use std::collections::BTreeMap;
use std::ops::ControlFlow;

use super::super::error::Error;
use super::super::manifest::AsyncManifestStore;
use super::super::read::{EntryRef, ScanOutcome};
use super::super::store::AsyncStore;
use super::super::tree::Tree;
use super::super::versioned_map::MapVersionId;
use super::super::AsyncProlly;
use super::async_coordinator::{find_snapshot, AsyncIndexedMap};
use super::budget::{BudgetCounter, Deadline, QueryBudget};
use super::definition::IndexProjection;
use super::publication::AsyncIndexedStore;
use super::snapshot::{
    physical_bounds, IndexedSourceRecord, LogicalBounds, ProjectedIndexEntry, SecondaryIndexCursor,
    SecondaryIndexDirection, SecondaryIndexMatch, SecondaryIndexMatchRef, SecondaryIndexPage,
    SnapshotContext,
};
use super::state::{IndexDescriptor, IndexedSnapshotId, IndexedSnapshotRecord};
use super::storage::{decode_physical_index_key, IndexValue};

/// Immutable async view pinned to one canonical collection-state root.
pub struct AsyncIndexedSnapshot<'a, S: AsyncIndexedStore> {
    id: IndexedSnapshotId,
    state_tree: Tree,
    state_version: MapVersionId,
    source_tree: Tree,
    source_version: MapVersionId,
    indexes: BTreeMap<Vec<u8>, AsyncSecondaryIndexSnapshot<'a, S>>,
}

impl<'a, S: AsyncIndexedStore> AsyncIndexedSnapshot<'a, S> {
    /// Content-addressed canonical snapshot identifier.
    pub fn id(&self) -> &IndexedSnapshotId {
        &self.id
    }

    /// Pinned source version.
    pub fn source_version(&self) -> &MapVersionId {
        &self.source_version
    }

    /// Pinned collection-state version.
    pub fn state_version(&self) -> &MapVersionId {
        &self.state_version
    }

    /// Pinned source tree.
    pub fn source_tree(&self) -> &Tree {
        &self.source_tree
    }

    /// Pinned canonical state tree.
    pub fn state_tree(&self) -> &Tree {
        &self.state_tree
    }

    /// Select one active index from this exact snapshot.
    pub fn index(
        &self,
        name: impl AsRef<[u8]>,
    ) -> Result<&AsyncSecondaryIndexSnapshot<'a, S>, Error> {
        self.indexes
            .get(name.as_ref())
            .ok_or_else(|| Error::IndexUnavailableAtVersion {
                name: name.as_ref().to_vec(),
                source_version: self.source_version.clone(),
            })
    }

    /// Iterate all indexes selected by this exact snapshot.
    pub fn indexes(&self) -> impl ExactSizeIterator<Item = &AsyncSecondaryIndexSnapshot<'a, S>> {
        self.indexes.values()
    }
}

/// One immutable secondary-index tree selected by an async snapshot.
pub struct AsyncSecondaryIndexSnapshot<'a, S: AsyncIndexedStore> {
    prolly: &'a AsyncProlly<S>,
    snapshot_id: SnapshotContext,
    descriptor: IndexDescriptor,
    selected: super::state::IndexSnapshotRef,
    source_tree: Tree,
    index_tree: Tree,
    max_projection_bytes: usize,
}

/// Finite-budget async query session over one immutable index tree.
pub struct AsyncSecondaryIndexQuery<'query, 'engine, S: AsyncIndexedStore> {
    index: &'query AsyncSecondaryIndexSnapshot<'engine, S>,
    budget: QueryBudget,
}

impl<'query, 'engine, S> AsyncSecondaryIndexQuery<'query, 'engine, S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    pub async fn exact_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Exact(term.to_vec()),
                SecondaryIndexDirection::Forward,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn prefix_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Prefix(prefix.to_vec()),
                SecondaryIndexDirection::Forward,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn range_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
                SecondaryIndexDirection::Forward,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn exact_reverse_page(
        &self,
        term: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Exact(term.to_vec()),
                SecondaryIndexDirection::Reverse,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn prefix_reverse_page(
        &self,
        prefix: &[u8],
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Prefix(prefix.to_vec()),
                SecondaryIndexDirection::Reverse,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn range_reverse_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.index
            .page(
                LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
                SecondaryIndexDirection::Reverse,
                cursor,
                limit,
                &self.budget,
            )
            .await
    }

    pub async fn records(&self, term: &[u8]) -> Result<Vec<IndexedSourceRecord>, Error> {
        self.index.records_with_budget(term, &self.budget).await
    }
}

impl<'a, S> AsyncSecondaryIndexSnapshot<'a, S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Create a finite-budget query session.
    pub fn query(&self, budget: QueryBudget) -> Result<AsyncSecondaryIndexQuery<'_, 'a, S>, Error> {
        budget.validate()?;
        Ok(AsyncSecondaryIndexQuery {
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
        &self.index_tree
    }

    pub async fn exact(&self, term: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(
            self.exact_page(term, None, QueryBudget::default().max_returned_entries)
                .await?,
        )
    }

    pub async fn prefix(&self, prefix: &[u8]) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(
            self.prefix_page(prefix, None, QueryBudget::default().max_returned_entries)
                .await?,
        )
    }

    pub async fn range(
        &self,
        start_term: &[u8],
        end_term: Option<&[u8]>,
    ) -> Result<Vec<SecondaryIndexMatch>, Error> {
        self.collect_page(
            self.range_page(
                start_term,
                end_term,
                None,
                QueryBudget::default().max_returned_entries,
            )
            .await?,
        )
    }

    pub async fn primary_keys(&self, term: &[u8]) -> Result<Vec<Vec<u8>>, Error> {
        Ok(self
            .exact(term)
            .await?
            .into_iter()
            .map(|matched| matched.primary_key)
            .collect())
    }

    pub async fn projected(&self, term: &[u8]) -> Result<Vec<ProjectedIndexEntry>, Error> {
        Ok(self
            .exact(term)
            .await?
            .into_iter()
            .map(|matched| (matched.primary_key, matched.projection))
            .collect())
    }

    /// Resolve matching primary keys with one native ordered async batch read.
    pub async fn records(&self, term: &[u8]) -> Result<Vec<IndexedSourceRecord>, Error> {
        self.records_with_budget(term, &QueryBudget::default())
            .await
    }

    async fn records_with_budget(
        &self,
        term: &[u8],
        budget: &QueryBudget,
    ) -> Result<Vec<IndexedSourceRecord>, Error> {
        budget.validate()?;
        let matches = self
            .page(
                LogicalBounds::Exact(term.to_vec()),
                SecondaryIndexDirection::Forward,
                None,
                budget.max_returned_entries,
                budget,
            )
            .await?;
        let matches = self.collect_page(matches)?;
        if matches.len() > budget.max_source_fetches {
            return Err(Error::IndexResourceLimitExceeded {
                resource: "query_source_fetches",
                limit: budget.max_source_fetches,
                actual: matches.len(),
            });
        }
        let keys = matches
            .iter()
            .map(|matched| matched.primary_key.as_slice())
            .collect::<Vec<_>>();
        let values = self.prolly.get_many(&self.source_tree, &keys).await?;
        let counter = BudgetCounter::new();
        let mut returned_bytes = 0usize;
        let mut accounted_memory = 0usize;
        let mut records = Vec::with_capacity(matches.len());
        for (matched, value) in matches.into_iter().zip(values) {
            let value = value.ok_or_else(|| Error::IndexSnapshotMismatch {
                name: self.descriptor.name.clone(),
                source_version: self.snapshot_id.source_version.clone(),
                reason: format!(
                    "index references missing source primary key {:?}",
                    matched.primary_key
                ),
            })?;
            let retained = matched.primary_key.len().checked_add(value.len()).ok_or(
                Error::IndexResourceLimitExceeded {
                    resource: "query_returned_bytes",
                    limit: budget.max_returned_bytes,
                    actual: usize::MAX,
                },
            )?;
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
            records.push((matched.primary_key, value));
        }
        Ok(records)
    }

    pub async fn scan_exact(
        &self,
        term: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_exact_until(term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_exact_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Forward,
            visit,
        )
        .await
    }

    pub async fn scan_prefix(
        &self,
        prefix: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_until(prefix, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_prefix_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Forward,
            visit,
        )
        .await
    }

    pub async fn scan_range(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_until(start, end, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_range_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            visit,
        )
        .await
    }

    pub async fn scan_exact_reverse(
        &self,
        term: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_exact_reverse_until(term, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_exact_reverse_until<B>(
        &self,
        term: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Exact(term.to_vec()),
            SecondaryIndexDirection::Reverse,
            visit,
        )
        .await
    }

    pub async fn scan_prefix_reverse(
        &self,
        prefix: &[u8],
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_prefix_reverse_until(prefix, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_prefix_reverse_until<B>(
        &self,
        prefix: &[u8],
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Prefix(prefix.to_vec()),
            SecondaryIndexDirection::Reverse,
            visit,
        )
        .await
    }

    pub async fn scan_range_reverse(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>),
    ) -> Result<u64, Error> {
        Ok(self
            .scan_range_reverse_until(start, end, |row| {
                visit(row);
                ControlFlow::<()>::Continue(())
            })
            .await?
            .visited)
    }

    pub async fn scan_range_reverse_until<B>(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        self.scan_matches_until(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            visit,
        )
        .await
    }

    async fn scan_matches_until<B>(
        &self,
        logical: LogicalBounds,
        direction: SecondaryIndexDirection,
        mut visit: impl for<'row> FnMut(SecondaryIndexMatchRef<'row>) -> ControlFlow<B>,
    ) -> Result<ScanOutcome<B>, Error> {
        let budget = QueryBudget::default();
        budget.validate()?;
        let started = Deadline::new();
        let mut scanned = 0usize;
        let mut returned = 0usize;
        let mut returned_bytes = 0usize;
        let bounds = physical_bounds(&logical)?;
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
            match self.decode_match(entry.key(), entry.value()) {
                Ok(matched) => match visit(SecondaryIndexMatchRef {
                    term: &matched.term,
                    primary_key: &matched.primary_key,
                    projection: matched.projection.as_deref(),
                }) {
                    ControlFlow::Continue(()) => ControlFlow::Continue(()),
                    ControlFlow::Break(value) => ControlFlow::Break(Ok(value)),
                },
                Err(error) => ControlFlow::Break(Err(error)),
            }
        };
        let outcome = match direction {
            SecondaryIndexDirection::Reverse => {
                self.prolly
                    .scan_range_reverse_until(
                        &self.index_tree,
                        &bounds.start,
                        bounds.end.as_deref(),
                        &mut handle,
                    )
                    .await?
            }
            SecondaryIndexDirection::Forward => {
                self.prolly
                    .scan_range_until(
                        &self.index_tree,
                        &bounds.start,
                        bounds.end.as_deref(),
                        &mut handle,
                    )
                    .await?
            }
        };
        match outcome.break_value {
            Some(Ok(value)) => Ok(ScanOutcome::stopped(outcome.visited, value)),
            Some(Err(error)) => Err(error),
            None => Ok(ScanOutcome::complete(outcome.visited)),
        }
    }

    pub async fn exact_page(
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
        .await
    }

    pub async fn exact_reverse_page(
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
        .await
    }

    pub async fn prefix_page(
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
        .await
    }

    pub async fn prefix_reverse_page(
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
        .await
    }

    pub async fn range_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Forward,
            cursor,
            limit,
            &QueryBudget::default(),
        )
        .await
    }

    pub async fn range_reverse_page(
        &self,
        start: &[u8],
        end: Option<&[u8]>,
        cursor: Option<&SecondaryIndexCursor>,
        limit: usize,
    ) -> Result<SecondaryIndexPage, Error> {
        self.page(
            LogicalBounds::Range(start.to_vec(), end.map(ToOwned::to_owned)),
            SecondaryIndexDirection::Reverse,
            cursor,
            limit,
            &QueryBudget::default(),
        )
        .await
    }

    async fn page(
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
        let outcome: ScanOutcome<Result<(), Error>> = match direction {
            SecondaryIndexDirection::Forward => {
                let start = after.unwrap_or(&bounds.start);
                self.prolly
                    .scan_range_until(&self.index_tree, start, bounds.end.as_deref(), &mut handle)
                    .await?
            }
            SecondaryIndexDirection::Reverse => {
                let end = after.or(bounds.end.as_deref());
                self.prolly
                    .scan_range_reverse_until(&self.index_tree, &bounds.start, end, &mut handle)
                    .await?
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
        let index_version =
            MapVersionId::for_tree(&self.selected.tree).expect("validated index tree");
        let valid = cursor.snapshot == self.snapshot_id.snapshot
            && cursor.source_version == self.snapshot_id.source_version
            && cursor.state_version == self.snapshot_id.state_version
            && cursor.index_name == self.descriptor.name
            && cursor.index_version == index_version
            && cursor.definition_fingerprint == self.descriptor.fingerprint
            && cursor.direction == direction
            && &cursor.bounds == bounds;
        let physical_key_valid = match cursor.raw_key.as_deref() {
            None => true,
            Some(raw_key) => {
                let physical = physical_bounds(bounds)?;
                raw_key >= physical.start.as_slice()
                    && physical.end.as_deref().is_none_or(|end| raw_key < end)
                    && decode_physical_index_key(raw_key).is_ok()
            }
        };
        if valid && physical_key_valid {
            return Ok(());
        }
        Err(Error::IndexCursorVersionMismatch {
            expected: format!(
                "source={}, state={}, index={}, direction={direction:?}, bounds={bounds:?}",
                self.snapshot_id.source_version, self.snapshot_id.state_version, index_version
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
}

impl<'a, S> AsyncIndexedMap<'a, S>
where
    S: AsyncIndexedStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    /// Pin the current canonical collection state and every tree it names.
    pub async fn snapshot(&self) -> Result<AsyncIndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state().await?;
        let record = loaded.state.head_snapshot()?.clone();
        let record_id = loaded.state.head.clone();
        self.resolve_snapshot(loaded.tree, loaded.state, record_id, record)
    }

    /// Reopen the retained snapshot containing `source_version`.
    pub async fn snapshot_at(
        &self,
        source_version: &MapVersionId,
    ) -> Result<AsyncIndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state().await?;
        let record = find_snapshot(&loaded.state, source_version)?.clone();
        let record_id = record.id()?;
        self.resolve_snapshot(loaded.tree, loaded.state, record_id, record)
    }

    /// Reopen one exact retained content-addressed snapshot.
    pub async fn snapshot_by_id(
        &self,
        id: &IndexedSnapshotId,
    ) -> Result<AsyncIndexedSnapshot<'a, S>, Error> {
        let loaded = self.load_state().await?;
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
    ) -> Result<AsyncIndexedSnapshot<'a, S>, Error> {
        let source_version = MapVersionId::for_tree(&record.source.tree)?;
        let state_version = MapVersionId::for_tree(&state_tree)?;
        let snapshot_id = SnapshotContext {
            snapshot: record_id,
            source_version: source_version.clone(),
            state_version: state_version.clone(),
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
            indexes.insert(
                selected.name.clone(),
                AsyncSecondaryIndexSnapshot {
                    prolly: self.prolly,
                    snapshot_id: snapshot_id.clone(),
                    descriptor,
                    source_tree: record.source.tree.clone(),
                    index_tree: selected.tree.clone(),
                    max_projection_bytes: match runtime.projection() {
                        IndexProjection::KeysOnly => 0,
                        IndexProjection::Include => runtime.limits().max_projection_bytes,
                        IndexProjection::All => runtime.limits().max_all_value_bytes,
                    },
                    selected,
                },
            );
        }
        Ok(AsyncIndexedSnapshot {
            id: snapshot_id.snapshot,
            state_tree,
            state_version,
            source_tree: record.source.tree,
            source_version,
            indexes,
        })
    }
}
