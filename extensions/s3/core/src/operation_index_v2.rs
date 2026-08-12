use std::{collections::BTreeMap, sync::Arc};

use serde::{Deserialize, Serialize};

use crate::{
    decode_canonical, encode_canonical, CompareExchange, CompareExchangeOutcome, Error, ErrorCode,
    GetRequest, IdempotencyRetentionV2, ImmutablePut, IndexedOperationV2,
    JournalIndexRebuildChunkIdV2, JournalIndexRebuildChunkV2, MutableControlStore, ObjectPath,
    ObjectPlane, OperationId, OperationIndexHeadV2, OperationIndexSegmentIdV2,
    OperationIndexSegmentRefV2, OperationIndexSegmentV2, PublicationEventIdV2, RefGeneration,
    RepositoryId, Result, RetryAdvice, ShardedBranchPublisherV2, StorageToken,
    DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
};

pub const DEFAULT_OPERATION_INDEX_LEAF_ENTRIES: usize = 4_096;
pub const DEFAULT_OPERATION_INDEX_MERGE_FANOUT: usize = 8;
pub const DEFAULT_OPERATION_INDEX_MAX_UNINDEXED_EVENTS: usize = 4_096;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationIndexAdvanceReportV2 {
    pub checkpoint: PublicationEventIdV2,
    pub checkpoint_generation: RefGeneration,
    pub indexed_events: usize,
    pub segments_written: usize,
    pub initialized: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexRebuildCursorV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub job: OperationId,
    pub snapshot: PublicationEventIdV2,
    pub snapshot_generation: RefGeneration,
    pub next_chunk: Option<JournalIndexRebuildChunkIdV2>,
    pub levels: Vec<Vec<OperationIndexSegmentRefV2>>,
    pub indexed_events: u64,
    pub segments_written: u64,
    pub baseline_checkpoint: Option<PublicationEventIdV2>,
    pub baseline_generation: Option<u64>,
    pub complete: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperationIndexRebuildStepV2 {
    pub cursor: OperationIndexRebuildCursorV2,
    pub indexed_events: usize,
    pub segments_written: usize,
    pub complete: bool,
}

#[derive(Clone)]
pub struct SegmentedOperationIndexV2<P: ObjectPlane> {
    plane: Arc<P>,
    controls: MutableControlStore<P>,
    prefix: String,
    repository: RepositoryId,
    retention: IdempotencyRetentionV2,
    leaf_entries: usize,
    merge_fanout: usize,
    max_unindexed_events: usize,
}

struct LoadedHeadV2 {
    value: OperationIndexHeadV2,
    token: StorageToken,
}

impl<P: ObjectPlane> SegmentedOperationIndexV2<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        retention: IdempotencyRetentionV2,
    ) -> Result<Self> {
        Self::new_with_limits(
            plane,
            prefix,
            repository,
            retention,
            DEFAULT_OPERATION_INDEX_LEAF_ENTRIES,
            DEFAULT_OPERATION_INDEX_MERGE_FANOUT,
            DEFAULT_OPERATION_INDEX_MAX_UNINDEXED_EVENTS,
            DEFAULT_MUTABLE_CONTROL_VERSIONS_TO_RETAIN,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_limits(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        retention: IdempotencyRetentionV2,
        leaf_entries: usize,
        merge_fanout: usize,
        max_unindexed_events: usize,
        control_versions_to_retain: usize,
    ) -> Result<Self> {
        retention.validate()?;
        if !(1..=65_536).contains(&leaf_entries)
            || !(2..=32).contains(&merge_fanout)
            || max_unindexed_events < leaf_entries
            || max_unindexed_events > 1_000_000
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 operation-index segment limits are invalid",
            ));
        }
        let prefix = prefix.into();
        let controls =
            MutableControlStore::new(plane.clone(), prefix.clone(), control_versions_to_retain)?;
        Ok(Self {
            plane,
            controls,
            prefix,
            repository,
            retention,
            leaf_entries,
            merge_fanout,
            max_unindexed_events,
        })
    }

    /// Advance the advisory branch-local index to one stable journal head.
    /// The first call intentionally starts at the current event; production
    /// repositories should initialize the index with branch creation.
    pub async fn advance(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        now_millis: u64,
    ) -> Result<OperationIndexAdvanceReportV2> {
        let current = publisher.load(branch).await?;
        if now_millis < current.value.updated_at_millis {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index clock predates the current branch ref",
            ));
        }
        let loaded = self.load_head(branch).await?;
        if loaded.is_none() && current.value.generation.0 != 0 {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "operation index must be initialized at branch creation or rebuilt resumably",
            ));
        }
        if loaded
            .as_ref()
            .is_some_and(|head| now_millis < head.value.updated_at_millis)
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index clock predates its durable head",
            ));
        }
        if loaded
            .as_ref()
            .is_some_and(|head| head.value.checkpoint == current.value.publication)
        {
            return Ok(OperationIndexAdvanceReportV2 {
                checkpoint: current.value.publication,
                checkpoint_generation: current.value.generation,
                indexed_events: 0,
                segments_written: 0,
                initialized: false,
            });
        }

        let (events, initialized) = if let Some(head) = loaded.as_ref() {
            if head.value.checkpoint_generation.0 >= current.value.generation.0 {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "operation-index checkpoint is not an ancestor of the branch head",
                ));
            }
            (
                self.collect_unindexed_events(
                    publisher,
                    branch,
                    head.value.checkpoint,
                    current.value.publication,
                )
                .await?,
                false,
            )
        } else {
            (
                vec![
                    publisher
                        .load_publication(current.value.publication)
                        .await?,
                ],
                true,
            )
        };

        let mut levels = loaded
            .as_ref()
            .map(|head| head.value.levels.clone())
            .unwrap_or_default();
        let entries = events
            .iter()
            .filter(|event| {
                self.retention.contains(
                    current.value.generation,
                    now_millis,
                    event.generation,
                    event.created_at_millis,
                )
            })
            .map(|event| {
                Ok(IndexedOperationV2 {
                    operation: event.operation,
                    publication: event.id()?,
                    target: event.new_target,
                    generation: event.generation,
                    created_at_millis: event.created_at_millis,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut segments_written = 0;
        for chunk in entries.chunks(self.leaf_entries) {
            if let Some(reference) = self
                .store_segment(
                    branch,
                    0,
                    chunk.to_vec(),
                    current.value.generation,
                    now_millis,
                )
                .await?
            {
                segments_written += 1;
                self.push_segment(
                    branch,
                    &mut levels,
                    reference,
                    current.value.generation,
                    now_millis,
                    &mut segments_written,
                )
                .await?;
            }
        }
        self.prune_catalog(&mut levels, current.value.generation, now_millis);
        let head = OperationIndexHeadV2 {
            repository: self.repository,
            branch: branch.to_string(),
            checkpoint: current.value.publication,
            checkpoint_generation: current.value.generation,
            retention: self.retention,
            levels,
            generation: loaded.as_ref().map_or(Ok(0), |head| {
                head.value.generation.checked_add(1).ok_or_else(|| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "operation-index head generation overflow",
                    )
                })
            })?,
            updated_at_millis: now_millis,
        };
        self.validate_head(&head)?;
        let bytes = encode_canonical(&head)?;
        let path = self.head_path(branch)?;
        let outcome = self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: loaded.map(|head| head.token),
                bytes: bytes.clone(),
            })
            .await;
        match outcome {
            Ok(CompareExchangeOutcome::Applied(_)) => {}
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => {}
            Ok(CompareExchangeOutcome::Conflict(_)) => {
                return Err(Error::new(
                    ErrorCode::RefConflict,
                    "operation-index head advanced concurrently",
                )
                .retry(RetryAdvice::ReloadHead));
            }
            Err(error) => {
                if self
                    .plane
                    .load_mutable(&path)
                    .await?
                    .is_none_or(|current| current.bytes != bytes)
                {
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("operation-index publication outcome is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation));
                }
            }
        }
        Ok(OperationIndexAdvanceReportV2 {
            checkpoint: head.checkpoint,
            checkpoint_generation: head.checkpoint_generation,
            indexed_events: events.len(),
            segments_written,
            initialized,
        })
    }

    pub async fn start_rebuild(
        &self,
        branch: &str,
        job: OperationId,
        snapshot: PublicationEventIdV2,
        snapshot_generation: RefGeneration,
        oldest_chunk: JournalIndexRebuildChunkIdV2,
    ) -> Result<OperationIndexRebuildCursorV2> {
        crate::repository::validate_branch(branch)?;
        if job.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index rebuild requires a non-nil job ID",
            ));
        }
        let baseline = self.load_head(branch).await?;
        Ok(OperationIndexRebuildCursorV2 {
            repository: self.repository,
            branch: branch.to_string(),
            job,
            snapshot,
            snapshot_generation,
            next_chunk: Some(oldest_chunk),
            levels: Vec::new(),
            indexed_events: 0,
            segments_written: 0,
            baseline_checkpoint: baseline.as_ref().map(|head| head.value.checkpoint),
            baseline_generation: baseline.as_ref().map(|head| head.value.generation),
            complete: false,
        })
    }

    pub async fn advance_rebuild(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        cursor: &OperationIndexRebuildCursorV2,
        chunk: &JournalIndexRebuildChunkV2,
        expected_chunk: JournalIndexRebuildChunkIdV2,
        max_events: usize,
        now_millis: u64,
    ) -> Result<OperationIndexRebuildStepV2> {
        self.validate_rebuild_cursor(cursor)?;
        chunk.validate(self.repository, &cursor.branch)?;
        if chunk.job != cursor.job
            || chunk.id()? != expected_chunk
            || cursor.next_chunk != Some(expected_chunk)
        {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "operation-index rebuild received another job's journal chunk",
            ));
        }
        if !(1..=1_000).contains(&max_events) || chunk.events.len() > max_events {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "operation-index rebuild page is outside the 1 to 1,000 event bound",
            ));
        }
        let entries = chunk
            .events
            .iter()
            .rev()
            .filter(|event| {
                self.retention.contains(
                    cursor.snapshot_generation,
                    now_millis,
                    event.generation,
                    event.created_at_millis,
                )
            })
            .map(|event| {
                Ok(IndexedOperationV2 {
                    operation: event.operation,
                    publication: event.id()?,
                    target: event.new_target,
                    generation: event.generation,
                    created_at_millis: event.created_at_millis,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut next = cursor.clone();
        let mut segments_written = 0usize;
        for leaf in entries.chunks(self.leaf_entries) {
            if let Some(reference) = self
                .store_segment(
                    &cursor.branch,
                    0,
                    leaf.to_vec(),
                    cursor.snapshot_generation,
                    now_millis,
                )
                .await?
            {
                segments_written += 1;
                self.push_segment(
                    &cursor.branch,
                    &mut next.levels,
                    reference,
                    cursor.snapshot_generation,
                    now_millis,
                    &mut segments_written,
                )
                .await?;
            }
        }
        self.prune_catalog(&mut next.levels, cursor.snapshot_generation, now_millis);
        next.indexed_events = next
            .indexed_events
            .checked_add(u64::try_from(entries.len()).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "operation rebuild event count overflow",
                )
            })?)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "operation rebuild event count overflow",
                )
            })?;
        next.segments_written = next
            .segments_written
            .checked_add(u64::try_from(segments_written).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "operation rebuild segment count overflow",
                )
            })?)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "operation rebuild segment count overflow",
                )
            })?;
        next.next_chunk = chunk.newer;
        if next.next_chunk.is_none() {
            self.publish_rebuild(publisher, &next, now_millis).await?;
            next.complete = true;
        }
        Ok(OperationIndexRebuildStepV2 {
            cursor: next.clone(),
            indexed_events: entries.len(),
            segments_written,
            complete: next.complete,
        })
    }

    pub async fn lookup(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        operation: OperationId,
        now_millis: u64,
    ) -> Result<Option<IndexedOperationV2>> {
        if operation.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index lookup requires a non-nil operation ID",
            ));
        }
        let current = publisher.load(branch).await?;
        if now_millis < current.value.updated_at_millis {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "operation-index lookup clock predates the current branch ref",
            ));
        }
        if current.value.operation == operation
            && self.retention.contains(
                current.value.generation,
                now_millis,
                current.value.generation,
                current.value.updated_at_millis,
            )
        {
            return Ok(Some(IndexedOperationV2 {
                operation,
                publication: current.value.publication,
                target: current.value.target,
                generation: current.value.generation,
                created_at_millis: current.value.updated_at_millis,
            }));
        }
        let Some(head) = self.load_head(branch).await? else {
            return Ok(None);
        };
        self.validate_head(&head.value)?;
        if head.value.checkpoint != current.value.publication {
            let events = self
                .collect_unindexed_events(
                    publisher,
                    branch,
                    head.value.checkpoint,
                    current.value.publication,
                )
                .await?;
            if let Some(event) = events.into_iter().find(|event| {
                event.operation == operation
                    && self.retention.contains(
                        current.value.generation,
                        now_millis,
                        event.generation,
                        event.created_at_millis,
                    )
            }) {
                return Ok(Some(IndexedOperationV2 {
                    operation,
                    publication: event.id()?,
                    target: event.new_target,
                    generation: event.generation,
                    created_at_millis: event.created_at_millis,
                }));
            }
        }
        for level in &head.value.levels {
            for reference in level.iter().rev() {
                if !self.segment_may_overlap_retention(
                    reference,
                    current.value.generation,
                    now_millis,
                ) {
                    continue;
                }
                let segment = self.load_segment(branch, reference).await?;
                if let Ok(index) = segment
                    .entries
                    .binary_search_by_key(&operation, |entry| entry.operation)
                {
                    let entry = segment.entries[index].clone();
                    return Ok(self
                        .retention
                        .contains(
                            current.value.generation,
                            now_millis,
                            entry.generation,
                            entry.created_at_millis,
                        )
                        .then_some(entry));
                }
            }
        }
        Ok(None)
    }

    async fn collect_unindexed_events(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        branch: &str,
        checkpoint: PublicationEventIdV2,
        current: PublicationEventIdV2,
    ) -> Result<Vec<crate::PublicationEventV2>> {
        let mut cursor = publisher.open_journal(branch).await?;
        if cursor.snapshot_head != current {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "branch advanced while opening the operation-index journal snapshot",
            )
            .retry(RetryAdvice::ReloadHead));
        }
        let mut events = Vec::new();
        loop {
            let page = publisher
                .read_journal_page(&cursor, self.leaf_entries.min(1_000))
                .await?;
            let mut found = false;
            for entry in page.entries {
                if entry.id == checkpoint {
                    found = true;
                    break;
                }
                if events.len() == self.max_unindexed_events {
                    return Err(Error::new(
                        ErrorCode::HistoryLimitExceeded,
                        "operation-index lag exceeds its bounded catch-up window; run resumable rebuild",
                    ));
                }
                events.push(entry.event);
            }
            if found {
                return Ok(events);
            }
            let Some(next) = page.continuation else {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "operation-index checkpoint is not in the branch publication journal",
                ));
            };
            if next.next == Some(checkpoint) {
                return Ok(events);
            }
            cursor = next;
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn push_segment(
        &self,
        branch: &str,
        levels: &mut Vec<Vec<OperationIndexSegmentRefV2>>,
        reference: OperationIndexSegmentRefV2,
        current_generation: RefGeneration,
        now_millis: u64,
        segments_written: &mut usize,
    ) -> Result<()> {
        let mut pending = Some(reference);
        while let Some(reference) = pending.take() {
            let level = usize::from(reference.level);
            if levels.len() <= level {
                levels.resize_with(level + 1, Vec::new);
            }
            levels[level].push(reference);
            if levels[level].len() < self.merge_fanout {
                break;
            }
            let merging = std::mem::take(&mut levels[level]);
            let at_top_level = level + 1 >= self.max_levels();
            let next_level = if at_top_level {
                u8::try_from(level)
            } else {
                u8::try_from(level + 1)
            }
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "operation-index level exceeds u8"))?;
            let mut entries = Vec::new();
            for reference in merging {
                entries.extend(self.load_segment(branch, &reference).await?.entries);
            }
            pending = self
                .store_segment(branch, next_level, entries, current_generation, now_millis)
                .await?;
            if pending.is_some() {
                *segments_written += 1;
            }
            if at_top_level {
                if let Some(reference) = pending.take() {
                    levels[level].push(reference);
                }
                break;
            }
        }
        Ok(())
    }

    async fn store_segment(
        &self,
        branch: &str,
        level: u8,
        entries: Vec<IndexedOperationV2>,
        current_generation: RefGeneration,
        now_millis: u64,
    ) -> Result<Option<OperationIndexSegmentRefV2>> {
        let mut unique = BTreeMap::new();
        for entry in entries {
            if !self.retention.contains(
                current_generation,
                now_millis,
                entry.generation,
                entry.created_at_millis,
            ) {
                continue;
            }
            match unique.entry(entry.operation) {
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(entry);
                }
                std::collections::btree_map::Entry::Occupied(slot)
                    if slot.get().publication == entry.publication => {}
                std::collections::btree_map::Entry::Occupied(_) => {
                    return Err(Error::new(
                        ErrorCode::IdempotencyConflict,
                        "one operation ID published different v2 journal events",
                    ));
                }
            }
        }
        if unique.is_empty() {
            return Ok(None);
        }
        let segment = OperationIndexSegmentV2 {
            repository: self.repository,
            branch: branch.to_string(),
            level,
            entries: unique.into_values().collect(),
        };
        segment.validate()?;
        let id = segment.id()?;
        let bytes = encode_canonical(&segment)?;
        self.plane
            .put_immutable(ImmutablePut {
                path: self.segment_path(branch, id)?,
                expected_sha256: crate::codec::sha256(&bytes),
                bytes,
            })
            .await?;
        let min_generation = segment
            .entries
            .iter()
            .min_by_key(|entry| entry.generation.0)
            .map(|entry| entry.generation)
            .expect("nonempty segment");
        let max_generation = segment
            .entries
            .iter()
            .max_by_key(|entry| entry.generation.0)
            .map(|entry| entry.generation)
            .expect("nonempty segment");
        let min_created_at_millis = segment
            .entries
            .iter()
            .map(|entry| entry.created_at_millis)
            .min()
            .expect("nonempty segment");
        let max_created_at_millis = segment
            .entries
            .iter()
            .map(|entry| entry.created_at_millis)
            .max()
            .expect("nonempty segment");
        Ok(Some(OperationIndexSegmentRefV2 {
            id,
            level,
            min_generation,
            max_generation,
            min_created_at_millis,
            max_created_at_millis,
            entries: u32::try_from(segment.entries.len()).map_err(|_| {
                Error::new(
                    ErrorCode::InvalidLimit,
                    "operation-index segment exceeds u32 entries",
                )
            })?,
        }))
    }

    async fn load_segment(
        &self,
        branch: &str,
        reference: &OperationIndexSegmentRefV2,
    ) -> Result<OperationIndexSegmentV2> {
        let stored = self
            .plane
            .get(GetRequest {
                path: self.segment_path(branch, reference.id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::MissingClosure,
                    "operation-index segment is missing",
                )
            })?;
        let segment: OperationIndexSegmentV2 = decode_canonical(&stored.bytes)?;
        segment.validate()?;
        let min_generation = segment.entries.iter().map(|entry| entry.generation.0).min();
        let max_generation = segment.entries.iter().map(|entry| entry.generation.0).max();
        let min_created_at_millis = segment
            .entries
            .iter()
            .map(|entry| entry.created_at_millis)
            .min();
        let max_created_at_millis = segment
            .entries
            .iter()
            .map(|entry| entry.created_at_millis)
            .max();
        if segment.id()? != reference.id
            || segment.repository != self.repository
            || segment.branch != branch
            || segment.level != reference.level
            || segment.entries.len() != reference.entries as usize
            || min_generation != Some(reference.min_generation.0)
            || max_generation != Some(reference.max_generation.0)
            || min_created_at_millis != Some(reference.min_created_at_millis)
            || max_created_at_millis != Some(reference.max_created_at_millis)
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "operation-index segment does not match its catalog reference",
            ));
        }
        Ok(segment)
    }

    fn validate_rebuild_cursor(&self, cursor: &OperationIndexRebuildCursorV2) -> Result<()> {
        crate::repository::validate_branch(&cursor.branch)?;
        if cursor.repository != self.repository
            || cursor.job.is_nil()
            || cursor.baseline_checkpoint.is_some() != cursor.baseline_generation.is_some()
            || cursor.complete != cursor.next_chunk.is_none()
            || cursor.levels.len() > self.max_levels()
            || cursor.levels.iter().enumerate().any(|(level, segments)| {
                segments.len() >= self.merge_fanout
                    || segments
                        .iter()
                        .any(|segment| usize::from(segment.level) != level || segment.entries == 0)
            })
        {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "operation-index rebuild cursor is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    async fn publish_rebuild(
        &self,
        publisher: &ShardedBranchPublisherV2<P>,
        cursor: &OperationIndexRebuildCursorV2,
        now_millis: u64,
    ) -> Result<()> {
        let snapshot = publisher.load_publication(cursor.snapshot).await?;
        if snapshot.repository != self.repository
            || snapshot.branch != cursor.branch
            || snapshot.generation != cursor.snapshot_generation
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "operation-index rebuild cursor does not match its journal snapshot",
            ));
        }
        let loaded = self.load_head(&cursor.branch).await?;
        let baseline_matches = match (
            loaded.as_ref(),
            cursor.baseline_checkpoint,
            cursor.baseline_generation,
        ) {
            (None, None, None) => true,
            (Some(head), Some(checkpoint), Some(generation)) => {
                head.value.checkpoint == checkpoint && head.value.generation == generation
            }
            _ => false,
        };
        if !baseline_matches {
            return Err(Error::new(
                ErrorCode::RefConflict,
                "operation index changed while its resumable rebuild was running",
            )
            .retry(RetryAdvice::ReloadHead));
        }
        let generation = loaded.as_ref().map_or(Ok(0), |head| {
            head.value.generation.checked_add(1).ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "operation-index rebuild head generation overflow",
                )
            })
        })?;
        let head = OperationIndexHeadV2 {
            repository: self.repository,
            branch: cursor.branch.clone(),
            checkpoint: cursor.snapshot,
            checkpoint_generation: cursor.snapshot_generation,
            retention: self.retention,
            levels: cursor.levels.clone(),
            generation,
            updated_at_millis: now_millis,
        };
        self.validate_head(&head)?;
        let bytes = encode_canonical(&head)?;
        let path = self.head_path(&cursor.branch)?;
        match self
            .controls
            .compare_exchange(CompareExchange {
                path: path.clone(),
                expected: loaded.map(|head| head.token),
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(CompareExchangeOutcome::Applied(_)) => Ok(()),
            Ok(CompareExchangeOutcome::Conflict(Some(current))) if current.bytes == bytes => Ok(()),
            Ok(CompareExchangeOutcome::Conflict(_)) => Err(Error::new(
                ErrorCode::RefConflict,
                "operation-index rebuild head publication conflicted",
            )
            .retry(RetryAdvice::ReloadHead)),
            Err(error) => {
                if self
                    .plane
                    .load_mutable(&path)
                    .await?
                    .is_some_and(|current| current.bytes == bytes)
                {
                    Ok(())
                } else {
                    Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        format!("operation-index rebuild publication is unknown: {error}"),
                    )
                    .retry(RetryAdvice::ReconcileOperation))
                }
            }
        }
    }

    async fn load_head(&self, branch: &str) -> Result<Option<LoadedHeadV2>> {
        let Some(stored) = self.plane.load_mutable(&self.head_path(branch)?).await? else {
            return Ok(None);
        };
        let value: OperationIndexHeadV2 = decode_canonical(&stored.bytes)?;
        self.validate_head(&value)?;
        if value.branch != branch {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "operation-index head is stored under another branch",
            ));
        }
        Ok(Some(LoadedHeadV2 {
            value,
            token: stored.metadata.token,
        }))
    }

    fn validate_head(&self, head: &OperationIndexHeadV2) -> Result<()> {
        crate::repository::validate_branch(&head.branch)?;
        let valid = head.repository == self.repository
            && head.retention == self.retention
            && head.levels.len() <= self.max_levels()
            && head.levels.iter().enumerate().all(|(level, segments)| {
                segments.len() < self.merge_fanout
                    && segments
                        .iter()
                        .all(|segment| usize::from(segment.level) == level && segment.entries > 0)
            });
        if !valid {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "operation-index head is malformed or uses incompatible limits",
            ));
        }
        Ok(())
    }

    fn prune_catalog(
        &self,
        levels: &mut [Vec<OperationIndexSegmentRefV2>],
        current_generation: RefGeneration,
        now_millis: u64,
    ) {
        for level in levels {
            level.retain(|reference| {
                self.segment_may_overlap_retention(reference, current_generation, now_millis)
            });
        }
    }

    fn segment_may_overlap_retention(
        &self,
        reference: &OperationIndexSegmentRefV2,
        current_generation: RefGeneration,
        now_millis: u64,
    ) -> bool {
        current_generation
            .0
            .saturating_sub(reference.max_generation.0)
            <= self.retention.max_generations
            && now_millis.saturating_sub(reference.max_created_at_millis)
                <= self.retention.max_age_millis
    }

    fn max_levels(&self) -> usize {
        let mut capacity = 0_u128;
        let mut level_capacity = self.leaf_entries as u128;
        let target = u128::from(self.retention.max_generations).saturating_add(1);
        let per_level_segments = (self.merge_fanout - 1) as u128;
        let mut levels = 0;
        while capacity < target {
            capacity = capacity.saturating_add(level_capacity.saturating_mul(per_level_segments));
            level_capacity = level_capacity.saturating_mul(self.merge_fanout as u128);
            levels += 1;
        }
        levels
    }

    fn head_path(&self, branch: &str) -> Result<ObjectPath> {
        crate::repository::validate_branch(branch)?;
        ObjectPath::new(format!(
            "{}/operation-index/v2/heads/{}.cbor",
            self.prefix,
            hex::encode(branch.as_bytes())
        ))
    }

    fn segment_path(&self, branch: &str, id: OperationIndexSegmentIdV2) -> Result<ObjectPath> {
        crate::repository::validate_branch(branch)?;
        let encoded = hex::encode(id.as_bytes());
        ObjectPath::new(format!(
            "{}/operation-index/v2/segments/{}/sha256/{}/{}/{}",
            self.prefix,
            hex::encode(branch.as_bytes()),
            &encoded[..2],
            &encoded[2..4],
            encoded
        ))
    }
}
