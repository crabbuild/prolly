use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::{
    codec::sha256, decode_canonical, encode_canonical, Error, ErrorCode, ImmutablePut,
    ImmutablePutOutcome, ListRequest, ObjectPath, ObjectPlane, OperationId, RepositoryId, Result,
};

/// One complete immutable physical object expected to be created by an ingest
/// window. The journal contains identity only; it never contains payload bytes
/// or transfer-manager state.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhysicalObjectIntent {
    pub(crate) path: ObjectPath,
    pub(crate) size: u64,
    pub(crate) checksum_sha256: [u8; 32],
}

/// Immutable, batch-addressed creation intent for physical objects.
///
/// A record is written once before the corresponding payload uploads begin.
/// Replaying the same operation and identity list is idempotent, while a
/// process crash after the record and before an upload leaves only a harmless
/// missing-object intent for GC to skip.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhysicalObjectIntentBatch {
    pub(crate) repository: RepositoryId,
    pub(crate) operation: OperationId,
    pub(crate) created_at_millis: u64,
    pub(crate) objects: Vec<PhysicalObjectIntent>,
}

/// Provider identity captured after a payload batch has completed. This keeps
/// GC off the per-object HEAD path for the normal completion case while the
/// pre-upload intent remains the recovery source for a crash in between.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhysicalObjectCompletion {
    pub(crate) path: ObjectPath,
    pub(crate) size: u64,
    pub(crate) checksum_sha256: [u8; 32],
    pub(crate) provider_version_id: Option<String>,
    pub(crate) provider_etag: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PhysicalObjectCompletionBatch {
    pub(crate) repository: RepositoryId,
    pub(crate) operation: OperationId,
    pub(crate) completed_at_millis: u64,
    pub(crate) objects: Vec<PhysicalObjectCompletion>,
}

fn validate_identity_paths(
    repository: RepositoryId,
    payload_prefix: &str,
    operation: OperationId,
    object_count: usize,
    max_objects: usize,
    objects: impl IntoIterator<Item = (ObjectPath, [u8; 32])>,
) -> Result<()> {
    if repository.as_bytes() == &[0; 32]
        || operation.is_nil()
        || object_count == 0
        || object_count > max_objects
    {
        return Err(Error::new(
            ErrorCode::CorruptContent,
            "physical-object journal batch is malformed",
        ));
    }
    let mut previous: Option<ObjectPath> = None;
    for (path, checksum_sha256) in objects {
        let encoded = hex::encode(checksum_sha256);
        let expected_path = format!(
            "{payload_prefix}/sha256/{}/{}/{}",
            &encoded[..2],
            &encoded[2..4],
            encoded
        );
        if checksum_sha256 == [0; 32]
            || path.as_str() != expected_path
            || previous.as_ref().is_some_and(|previous| previous >= &path)
        {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "physical-object journal paths are not canonical",
            ));
        }
        previous = Some(path);
    }
    Ok(())
}

impl PhysicalObjectIntentBatch {
    fn validate(
        &self,
        repository: RepositoryId,
        payload_prefix: &str,
        max_objects: usize,
    ) -> Result<()> {
        validate_identity_paths(
            repository,
            payload_prefix,
            self.operation,
            self.objects.len(),
            max_objects,
            self.objects
                .iter()
                .map(|object| (object.path.clone(), object.checksum_sha256)),
        )?;
        if self.repository != repository {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "physical-object journal repository identity changed",
            ));
        }
        Ok(())
    }
}

impl PhysicalObjectCompletionBatch {
    fn validate(
        &self,
        repository: RepositoryId,
        payload_prefix: &str,
        max_objects: usize,
    ) -> Result<()> {
        validate_identity_paths(
            repository,
            payload_prefix,
            self.operation,
            self.objects.len(),
            max_objects,
            self.objects
                .iter()
                .map(|object| (object.path.clone(), object.checksum_sha256)),
        )?;
        if self.repository != repository
            || self
                .objects
                .iter()
                .any(|object| object.provider_etag.is_empty())
        {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "physical-object completion batch is malformed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone)]
pub(crate) struct PhysicalObjectJournal<P: ObjectPlane> {
    plane: Arc<P>,
    prefix: String,
    repository: RepositoryId,
    max_objects: usize,
}

impl<P: ObjectPlane> PhysicalObjectJournal<P> {
    pub(crate) fn new(
        plane: Arc<P>,
        prefix: impl Into<String>,
        repository: RepositoryId,
        max_objects: usize,
    ) -> Result<Self> {
        if max_objects == 0 {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "physical-object journal requires a positive object limit",
            ));
        }
        Ok(Self {
            plane,
            prefix: prefix.into(),
            repository,
            max_objects,
        })
    }

    pub(crate) async fn record(
        &self,
        operation: OperationId,
        created_at_millis: u64,
        mut objects: Vec<PhysicalObjectIntent>,
    ) -> Result<()> {
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        objects.dedup_by(|left, right| left.path == right.path);
        let batch = PhysicalObjectIntentBatch {
            repository: self.repository,
            operation,
            created_at_millis,
            objects,
        };
        batch.validate(self.repository, &self.payload_prefix(), self.max_objects)?;
        let bytes = encode_canonical(&batch)?;
        let path = self.batch_path(operation, &batch.objects)?;
        match self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                expected_sha256: sha256(&bytes),
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(ImmutablePutOutcome::Created(_) | ImmutablePutOutcome::AlreadyPresent(_)) => Ok(()),
            Err(original) => {
                let stored = self
                    .plane
                    .get(crate::GetRequest {
                        path,
                        range: None,
                        physical_version: None,
                    })
                    .await?;
                match stored {
                    Some(stored) if stored.bytes == bytes => Ok(()),
                    Some(stored) => {
                        let existing: PhysicalObjectIntentBatch = decode_canonical(&stored.bytes)?;
                        (existing.repository == batch.repository
                            && existing.operation == batch.operation
                            && existing.objects == batch.objects)
                            .then_some(())
                            .ok_or(original)
                    }
                    _ => Err(original),
                }
            }
        }
    }

    pub(crate) async fn load(&self, path: ObjectPath) -> Result<PhysicalObjectIntentBatch> {
        let stored = self
            .plane
            .get(crate::GetRequest {
                path,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::OutcomeUnknown,
                    "physical-object journal disappeared",
                )
            })?;
        let batch: PhysicalObjectIntentBatch = decode_canonical(&stored.bytes)?;
        batch.validate(self.repository, &self.payload_prefix(), self.max_objects)?;
        Ok(batch)
    }

    pub(crate) async fn record_completion(
        &self,
        operation: OperationId,
        completed_at_millis: u64,
        mut objects: Vec<PhysicalObjectCompletion>,
    ) -> Result<()> {
        objects.sort_by(|left, right| left.path.cmp(&right.path));
        objects.dedup_by(|left, right| left.path == right.path);
        let batch = PhysicalObjectCompletionBatch {
            repository: self.repository,
            operation,
            completed_at_millis,
            objects,
        };
        batch.validate(self.repository, &self.payload_prefix(), self.max_objects)?;
        let bytes = encode_canonical(&batch)?;
        let path = self.completion_path(operation, &batch.objects)?;
        match self
            .plane
            .put_immutable(ImmutablePut {
                path: path.clone(),
                expected_sha256: sha256(&bytes),
                bytes: bytes.clone(),
            })
            .await
        {
            Ok(ImmutablePutOutcome::Created(_) | ImmutablePutOutcome::AlreadyPresent(_)) => Ok(()),
            Err(original) => {
                let stored = self
                    .plane
                    .get(crate::GetRequest {
                        path,
                        range: None,
                        physical_version: None,
                    })
                    .await?;
                match stored {
                    Some(stored) if stored.bytes == bytes => Ok(()),
                    Some(stored) => {
                        let existing: PhysicalObjectCompletionBatch =
                            decode_canonical(&stored.bytes)?;
                        (existing.repository == batch.repository
                            && existing.operation == batch.operation
                            && existing.objects == batch.objects)
                            .then_some(())
                            .ok_or(original)
                    }
                    _ => Err(original),
                }
            }
        }
    }

    pub(crate) async fn load_completion(
        &self,
        path: ObjectPath,
    ) -> Result<PhysicalObjectCompletionBatch> {
        let stored = self
            .plane
            .get(crate::GetRequest {
                path,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::OutcomeUnknown,
                    "physical-object completion disappeared",
                )
            })?;
        let batch: PhysicalObjectCompletionBatch = decode_canonical(&stored.bytes)?;
        batch.validate(self.repository, &self.payload_prefix(), self.max_objects)?;
        Ok(batch)
    }

    pub(crate) fn prefix(&self) -> String {
        format!(
            "{}/administration/physical-object-journal/{}/intents/",
            self.prefix,
            hex::encode(self.repository.as_bytes())
        )
    }

    pub(crate) fn completion_prefix(&self) -> String {
        format!(
            "{}/administration/physical-object-journal/{}/completions/",
            self.prefix,
            hex::encode(self.repository.as_bytes())
        )
    }

    fn payload_prefix(&self) -> String {
        format!(
            "{}/payloads/{}",
            self.prefix,
            hex::encode(self.repository.as_bytes())
        )
    }

    fn batch_path(
        &self,
        operation: OperationId,
        objects: &[PhysicalObjectIntent],
    ) -> Result<ObjectPath> {
        let identity = encode_canonical(&(operation, objects))?;
        ObjectPath::new(format!(
            "{}{}.cbor",
            self.prefix(),
            hex::encode(sha256(&identity))
        ))
    }

    fn completion_path(
        &self,
        operation: OperationId,
        objects: &[PhysicalObjectCompletion],
    ) -> Result<ObjectPath> {
        let objects = objects
            .iter()
            .map(|object| PhysicalObjectIntent {
                path: object.path.clone(),
                size: object.size,
                checksum_sha256: object.checksum_sha256,
            })
            .collect::<Vec<_>>();
        self.completion_path_for_intents(operation, &objects)
    }

    pub(crate) fn completion_path_for_intents(
        &self,
        operation: OperationId,
        objects: &[PhysicalObjectIntent],
    ) -> Result<ObjectPath> {
        let identity = encode_canonical(&(operation, objects))?;
        ObjectPath::new(format!(
            "{}{}.cbor",
            self.completion_prefix(),
            hex::encode(sha256(&identity))
        ))
    }
}

pub(crate) fn journal_list_request(
    prefix: String,
    continuation: Option<String>,
    limit: usize,
) -> ListRequest {
    ListRequest {
        prefix,
        continuation,
        limit,
        include_versions: false,
    }
}
