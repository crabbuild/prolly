use std::{collections::BTreeMap, sync::Arc};

use crate::{
    decode_canonical, encode_canonical, BatchId, CommitSessionCheckpoint,
    CommitSessionCleanupReport, DeleteOutcome, Error, ErrorCode, GetRequest, ImmutablePut,
    ImmutablePutOutcome, ListRequest, ObjectPath, ObjectPlane, PhysicalVersion, RepositoryId,
    Result,
};

/// Immutable checkpoint store for resumable repository ingestion.
///
/// Checkpoints are append-only and sequence-addressed. Resume lists only one
/// batch prefix; cleanup pages the staging namespace and deletes exact expired
/// physical versions. No repository-wide mutable staging head is required.
#[derive(Clone)]
pub struct CommitSessionStore<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: RepositoryId,
    max_mutations: usize,
}

impl<P: ObjectPlane> CommitSessionStore<P> {
    pub fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        max_mutations: usize,
    ) -> Result<Self> {
        if max_mutations == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-session store requires a positive mutation limit",
            ));
        }
        Ok(Self {
            plane,
            prefix: prefix.into(),
            repository,
            max_mutations,
        })
    }

    pub async fn save(&self, checkpoint: &CommitSessionCheckpoint) -> Result<()> {
        checkpoint.validate(self.repository, self.max_mutations)?;
        let bytes = encode_canonical(checkpoint)?;
        let path = self.checkpoint_path(checkpoint.session.id, checkpoint.sequence)?;
        let expected_sha256 = crate::codec::sha256(&bytes);
        match self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                bytes: bytes.clone(),
                expected_sha256,
            })
            .await
        {
            Ok(ImmutablePutOutcome::Created(_) | ImmutablePutOutcome::AlreadyPresent(_)) => Ok(()),
            Err(original) => {
                let stored = self
                    .plane
                    .get(GetRequest {
                        path,
                        range: None,
                        physical_version: None,
                    })
                    .await;
                match stored {
                    Ok(Some(stored)) if stored.bytes == bytes => Ok(()),
                    _ => Err(original),
                }
            }
        }
    }

    pub async fn latest(&self, batch: BatchId) -> Result<Option<CommitSessionCheckpoint>> {
        let prefix = self.batch_prefix(batch);
        let mut continuation = None;
        let mut paths = BTreeMap::new();
        let mut scanned = 0_usize;
        loop {
            let page = self
                .plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            for entry in page.entries {
                scanned = scanned.checked_add(1).ok_or_else(|| {
                    Error::new(ErrorCode::InvalidLimit, "checkpoint scan overflow")
                })?;
                if scanned > self.max_mutations.saturating_add(2) {
                    return Err(Error::new(
                        ErrorCode::InvalidLimit,
                        "commit session has more checkpoints than its mutation limit",
                    ));
                }
                let sequence = parse_checkpoint_sequence(&prefix, &entry.path)?;
                if paths.insert(sequence, entry.path).is_some() {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "commit session has duplicate checkpoint sequences",
                    ));
                }
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        if paths.is_empty() {
            return Ok(None);
        }

        let mut aggregate = BTreeMap::<Vec<u8>, crate::StagedMutation>::new();
        let mut latest = None;
        let checkpoint_count = paths.len();
        for (index, (sequence, path)) in paths.into_iter().enumerate() {
            if sequence != index as u64 {
                return Err(Error::new(
                    ErrorCode::MissingClosure,
                    "commit-session checkpoint sequence has a gap",
                ));
            }
            let stored = self
                .plane
                .get(GetRequest {
                    path,
                    range: None,
                    physical_version: None,
                })
                .await?
                .ok_or_else(|| {
                    Error::new(
                        ErrorCode::OutcomeUnknown,
                        "commit-session checkpoint disappeared during resume",
                    )
                })?;
            let checkpoint: CommitSessionCheckpoint = decode_canonical(&stored.bytes)?;
            checkpoint.validate(self.repository, self.max_mutations)?;
            if checkpoint.session.id != batch || checkpoint.sequence != sequence {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "checkpoint path and embedded identity disagree",
                ));
            }
            if let Some(previous) = latest.as_ref() {
                if !same_session_lineage(previous, &checkpoint) {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "commit-session checkpoint lineage changed",
                    ));
                }
                if previous.state == crate::CommitSessionState::Aborted {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        "commit session continues after an aborted checkpoint",
                    ));
                }
            }
            if checkpoint.state == crate::CommitSessionState::Aborted
                && index + 1 != checkpoint_count
            {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "aborted commit-session checkpoint is not terminal",
                ));
            }
            for mutation in &checkpoint.mutations {
                aggregate.insert(mutation.key().to_vec(), mutation.clone());
            }
            if aggregate.len() as u64 != checkpoint.total_mutations {
                return Err(Error::new(
                    ErrorCode::MissingClosure,
                    "commit-session checkpoint total does not match its windows",
                ));
            }
            latest = Some(checkpoint);
        }
        let mut latest = latest.expect("nonempty checkpoint paths produced a checkpoint");
        latest.mutations = aggregate.into_values().collect();
        Ok(Some(latest))
    }

    pub async fn cleanup_expired_page(
        &self,
        now_millis: u64,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<CommitSessionCleanupReport> {
        if !(1..=1_000).contains(&limit) {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-session cleanup page must contain 1 to 1,000 versions",
            ));
        }
        let page = self
            .plane
            .list(ListRequest {
                prefix: self.staging_prefix(),
                continuation,
                limit,
                include_versions: true,
            })
            .await?;
        let mut report = CommitSessionCleanupReport {
            continuation: page.continuation,
            ..CommitSessionCleanupReport::default()
        };
        for entry in page.entries {
            report.scanned += 1;
            if entry.metadata.delete_marker {
                report.retained += 1;
                continue;
            }
            let physical_version = entry.metadata.token.version_id.clone().map_or_else(
                || PhysicalVersion::Unversioned {
                    token: Some(entry.metadata.token.clone()),
                },
                |version_id| PhysicalVersion::Versioned { version_id },
            );
            let Some(stored) = self
                .plane
                .get(GetRequest {
                    path: entry.path.clone(),
                    range: None,
                    physical_version: Some(physical_version.clone()),
                })
                .await?
            else {
                report.already_missing += 1;
                continue;
            };
            let checkpoint: CommitSessionCheckpoint = decode_canonical(&stored.bytes)?;
            checkpoint.validate(self.repository, self.max_mutations)?;
            if checkpoint.session.expires_at_millis >= now_millis {
                report.retained += 1;
                continue;
            }
            match self
                .plane
                .delete_exact(&entry.path, physical_version)
                .await?
            {
                DeleteOutcome::Deleted => report.deleted += 1,
                DeleteOutcome::NotFound => report.already_missing += 1,
                DeleteOutcome::TokenMismatch => {
                    return Err(Error::new(
                        ErrorCode::OutcomeUnknown,
                        "checkpoint changed during exact cleanup",
                    ))
                }
            }
        }
        Ok(report)
    }

    fn staging_prefix(&self) -> String {
        format!(
            "{}/staging/{}/",
            self.prefix,
            hex::encode(self.repository.as_bytes())
        )
    }

    fn batch_prefix(&self, batch: BatchId) -> String {
        format!("{}{batch}/checkpoints/", self.staging_prefix())
    }

    fn checkpoint_path(&self, batch: BatchId, sequence: u64) -> Result<ObjectPath> {
        ObjectPath::new(format!("{}{sequence:020}.cbor", self.batch_prefix(batch)))
    }
}

fn same_session_lineage(
    previous: &CommitSessionCheckpoint,
    next: &CommitSessionCheckpoint,
) -> bool {
    let previous = &previous.session;
    let next = &next.session;
    previous.id == next.id
        && previous.branch == next.branch
        && previous.base_commit == next.base_commit
        && previous.identity.repository == next.identity.repository
        && previous.identity.operation == next.identity.operation
        && previous.identity.authority.writer_id == next.identity.authority.writer_id
        && previous.message == next.message
        && previous.created_at_millis == next.created_at_millis
        && previous.expires_at_millis == next.expires_at_millis
}

fn parse_checkpoint_sequence(prefix: &str, path: &ObjectPath) -> Result<u64> {
    let suffix = path
        .as_str()
        .strip_prefix(prefix)
        .and_then(|value| value.strip_suffix(".cbor"))
        .ok_or_else(|| {
            Error::new(
                ErrorCode::CorruptContent,
                "checkpoint listing returned a path outside the batch namespace",
            )
        })?;
    if suffix.len() != 20 || !suffix.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(Error::new(
            ErrorCode::CorruptContent,
            "checkpoint path has a non-canonical sequence",
        ));
    }
    suffix.parse().map_err(|_| {
        Error::new(
            ErrorCode::CorruptContent,
            "checkpoint sequence is out of range",
        )
    })
}
