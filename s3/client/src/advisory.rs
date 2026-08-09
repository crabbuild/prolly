use std::{collections::HashMap, sync::RwLock};

use prolly_s3_core::{CommitId, CommitReceipt, Error, ErrorCode, RepositoryId, Result};
#[cfg(feature = "slatedb-index")]
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct AdvisoryRebuildReport {
    pub removed_entries: usize,
    pub quarantined_entries: usize,
    pub written_heads: usize,
    pub resumed_from_checkpoint: bool,
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct AdvisoryHeadV1 {
    repository: RepositoryId,
    branch: String,
    commit: CommitId,
}

#[async_trait::async_trait]
pub trait AdvisoryIndex: Send + Sync {
    async fn record_commit(&self, repository: RepositoryId, receipt: &CommitReceipt) -> Result<()>;
    async fn branch_head(&self, repository: RepositoryId, branch: &str)
        -> Result<Option<CommitId>>;
    async fn rebuild_heads(
        &self,
        repository: RepositoryId,
        heads: &[(String, CommitId)],
    ) -> Result<AdvisoryRebuildReport>;
}

#[derive(Default)]
pub struct MemoryAdvisoryIndex {
    heads: RwLock<HashMap<(RepositoryId, String), CommitId>>,
}

#[async_trait::async_trait]
impl AdvisoryIndex for MemoryAdvisoryIndex {
    async fn record_commit(&self, repository: RepositoryId, receipt: &CommitReceipt) -> Result<()> {
        self.heads
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "advisory index lock poisoned"))?
            .insert((repository, receipt.branch.clone()), receipt.id);
        Ok(())
    }

    async fn branch_head(
        &self,
        repository: RepositoryId,
        branch: &str,
    ) -> Result<Option<CommitId>> {
        Ok(self
            .heads
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "advisory index lock poisoned"))?
            .get(&(repository, branch.to_string()))
            .copied())
    }

    async fn rebuild_heads(
        &self,
        repository: RepositoryId,
        heads: &[(String, CommitId)],
    ) -> Result<AdvisoryRebuildReport> {
        let mut stored = self.heads.write().map_err(|_| {
            Error::new(ErrorCode::InternalInvariant, "advisory index lock poisoned")
        })?;
        let removed_entries = stored
            .keys()
            .filter(|(candidate, _)| *candidate == repository)
            .count();
        stored.retain(|(candidate, _), _| *candidate != repository);
        for (branch, commit) in heads {
            stored.insert((repository, branch.clone()), *commit);
        }
        Ok(AdvisoryRebuildReport {
            removed_entries,
            quarantined_entries: 0,
            written_heads: heads.len(),
            resumed_from_checkpoint: false,
        })
    }
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone)]
pub struct SlateDbAdvisoryIndex {
    db: slatedb::Db,
    path: String,
}

#[cfg(feature = "slatedb-index")]
impl SlateDbAdvisoryIndex {
    /// Opens an enforced per-repository, per-writer cache namespace.
    pub async fn open_owned(
        object_store: std::sync::Arc<dyn slatedb::object_store::ObjectStore>,
        repository: RepositoryId,
        writer_id: impl Into<String>,
    ) -> Result<Self> {
        let writer_id = writer_id.into();
        validate_writer_id(&writer_id)?;
        let path = format!(
            ".prolly-cache/{repository}/{}",
            hex::encode(writer_id.as_bytes())
        );
        let db = slatedb::Db::open(path.clone(), object_store)
            .await
            .map_err(map_slatedb)?;
        let expected = CacheOwnerV1 {
            schema: 1,
            repository,
            writer_id,
        };
        match db.get(cache_owner_key()).await.map_err(map_slatedb)? {
            Some(bytes) => {
                let owner: CacheOwnerV1 = serde_cbor::from_slice(&bytes).map_err(|error| {
                    Error::new(
                        ErrorCode::PermissionDenied,
                        format!("SlateDB cache owner record is corrupt: {error}"),
                    )
                })?;
                if owner != expected {
                    return Err(Error::new(
                        ErrorCode::PermissionDenied,
                        "SlateDB writable cache path belongs to another repository or writer",
                    ));
                }
            }
            None => {
                db.put(
                    cache_owner_key(),
                    serde_cbor::to_vec(&expected).map_err(|error| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            format!("SlateDB cache owner encoding failed: {error}"),
                        )
                    })?,
                )
                .await
                .map_err(map_slatedb)?;
                db.flush().await.map_err(map_slatedb)?;
            }
        }
        Ok(Self { db, path })
    }

    pub fn path(&self) -> &str {
        &self.path
    }

    /// Exposes the advisory database for diagnostics and lifecycle controls.
    /// Writes through this handle remain non-authoritative by contract.
    pub fn database(&self) -> &slatedb::Db {
        &self.db
    }

    pub async fn flush(&self) -> Result<()> {
        self.db.flush().await.map_err(map_slatedb)
    }

    pub async fn close(&self) -> Result<()> {
        self.db.close().await.map_err(map_slatedb)
    }

    pub async fn quarantine_count(&self, repository: RepositoryId) -> Result<usize> {
        let mut iterator = self
            .db
            .scan_prefix(quarantine_prefix(repository), ..)
            .await
            .map_err(map_slatedb)?;
        let mut count = 0;
        while iterator.next().await.map_err(map_slatedb)?.is_some() {
            count += 1;
        }
        Ok(count)
    }

    async fn quarantine(&self, repository: RepositoryId, branch: &str, bytes: &[u8]) -> Result<()> {
        self.quarantine_bytes(repository, bytes).await?;
        self.db
            .delete(branch_key(repository, branch))
            .await
            .map_err(map_slatedb)?;
        self.db.flush().await.map_err(map_slatedb)
    }

    async fn quarantine_bytes(&self, repository: RepositoryId, bytes: &[u8]) -> Result<()> {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(bytes);
        let mut key = quarantine_prefix(repository);
        key.extend_from_slice(hex::encode(digest).as_bytes());
        self.db.put(key, bytes).await.map_err(map_slatedb)?;
        Ok(())
    }

    async fn begin_rebuild(
        &self,
        repository: RepositoryId,
        heads: &[(String, CommitId)],
    ) -> Result<(CacheRebuildCheckpointV1, bool)> {
        use sha2::{Digest, Sha256};
        let input_digest: [u8; 32] =
            Sha256::digest(serde_cbor::to_vec(&heads).map_err(|error| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    format!("advisory rebuild input encoding failed: {error}"),
                )
            })?)
            .into();
        let previous = self
            .db
            .get(cache_rebuild_key(repository))
            .await
            .map_err(map_slatedb)?
            .map(|bytes| serde_cbor::from_slice::<CacheRebuildCheckpointV1>(&bytes))
            .transpose()
            .map_err(|error| {
                Error::new(
                    ErrorCode::CorruptCommit,
                    format!("advisory rebuild checkpoint is corrupt: {error}"),
                )
            })?;
        let resumed = previous.as_ref().is_some_and(|checkpoint| {
            checkpoint.repository == repository
                && checkpoint.input_digest == input_digest
                && matches!(checkpoint.state, CacheRebuildStateV1::Running)
        });
        let generation = previous
            .as_ref()
            .map_or(0, |checkpoint| checkpoint.generation)
            .checked_add(1)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "advisory rebuild generation overflow",
                )
            })?;
        let checkpoint = CacheRebuildCheckpointV1 {
            schema: 1,
            repository,
            input_digest,
            generation,
            state: CacheRebuildStateV1::Running,
        };
        self.db
            .put(
                cache_rebuild_key(repository),
                serde_cbor::to_vec(&checkpoint).map_err(|error| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        format!("advisory rebuild checkpoint encoding failed: {error}"),
                    )
                })?,
            )
            .await
            .map_err(map_slatedb)?;
        self.db.flush().await.map_err(map_slatedb)?;
        Ok((checkpoint, resumed))
    }

    async fn complete_rebuild(&self, mut checkpoint: CacheRebuildCheckpointV1) -> Result<()> {
        checkpoint.state = CacheRebuildStateV1::Completed;
        self.db
            .put(
                cache_rebuild_key(checkpoint.repository),
                serde_cbor::to_vec(&checkpoint).map_err(|error| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        format!("advisory rebuild checkpoint encoding failed: {error}"),
                    )
                })?,
            )
            .await
            .map_err(map_slatedb)?;
        self.db.flush().await.map_err(map_slatedb)
    }
}

#[cfg(feature = "slatedb-index")]
#[async_trait::async_trait]
impl AdvisoryIndex for SlateDbAdvisoryIndex {
    async fn record_commit(&self, repository: RepositoryId, receipt: &CommitReceipt) -> Result<()> {
        let bytes = serde_cbor::to_vec(&AdvisoryHeadV1 {
            repository,
            branch: receipt.branch.clone(),
            commit: receipt.id,
        })
        .map_err(|error| {
            Error::new(
                ErrorCode::InternalInvariant,
                format!("advisory head encoding failed: {error}"),
            )
        })?;
        self.db
            .put(branch_key(repository, &receipt.branch), bytes)
            .await
            .map_err(map_slatedb)?;
        Ok(())
    }

    async fn branch_head(
        &self,
        repository: RepositoryId,
        branch: &str,
    ) -> Result<Option<CommitId>> {
        let Some(bytes) = self
            .db
            .get(branch_key(repository, branch))
            .await
            .map_err(map_slatedb)?
        else {
            return Ok(None);
        };
        let head: AdvisoryHeadV1 = match serde_cbor::from_slice(&bytes) {
            Ok(head) => head,
            Err(error) => {
                self.quarantine(repository, branch, &bytes).await?;
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    format!("advisory head decode failed and was quarantined: {error}"),
                ));
            }
        };
        if head.repository != repository || head.branch != branch {
            self.quarantine(repository, branch, &bytes).await?;
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "advisory head identity mismatch was quarantined",
            ));
        }
        Ok(Some(head.commit))
    }

    async fn rebuild_heads(
        &self,
        repository: RepositoryId,
        heads: &[(String, CommitId)],
    ) -> Result<AdvisoryRebuildReport> {
        let (checkpoint, resumed_from_checkpoint) = self.begin_rebuild(repository, heads).await?;
        let mut iterator = self
            .db
            .scan_prefix(branch_prefix(repository), ..)
            .await
            .map_err(map_slatedb)?;
        let mut existing = Vec::new();
        while let Some(entry) = iterator.next().await.map_err(map_slatedb)? {
            existing.push((entry.key.to_vec(), entry.value.to_vec()));
        }
        let mut quarantined_entries = 0;
        for (key, value) in &existing {
            if serde_cbor::from_slice::<AdvisoryHeadV1>(value).is_err() {
                self.quarantine_bytes(repository, value).await?;
                quarantined_entries += 1;
            }
            self.db.delete(key).await.map_err(map_slatedb)?;
        }
        for (branch, commit) in heads {
            self.db
                .put(
                    branch_key(repository, branch),
                    serde_cbor::to_vec(&AdvisoryHeadV1 {
                        repository,
                        branch: branch.clone(),
                        commit: *commit,
                    })
                    .map_err(|error| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            format!("advisory head encoding failed: {error}"),
                        )
                    })?,
                )
                .await
                .map_err(map_slatedb)?;
        }
        self.db.flush().await.map_err(map_slatedb)?;
        self.complete_rebuild(checkpoint).await?;
        Ok(AdvisoryRebuildReport {
            removed_entries: existing.len(),
            quarantined_entries,
            written_heads: heads.len(),
            resumed_from_checkpoint,
        })
    }
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CacheOwnerV1 {
    schema: u8,
    repository: RepositoryId,
    writer_id: String,
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum CacheRebuildStateV1 {
    Running,
    Completed,
}

#[cfg(feature = "slatedb-index")]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct CacheRebuildCheckpointV1 {
    schema: u8,
    repository: RepositoryId,
    input_digest: [u8; 32],
    generation: u64,
    state: CacheRebuildStateV1,
}

#[cfg(feature = "slatedb-index")]
fn branch_key(repository: RepositoryId, branch: &str) -> Vec<u8> {
    format!("prolly-s3/{repository}/branch/{branch}").into_bytes()
}

#[cfg(feature = "slatedb-index")]
fn branch_prefix(repository: RepositoryId) -> Vec<u8> {
    format!("prolly-s3/{repository}/branch/").into_bytes()
}

#[cfg(feature = "slatedb-index")]
fn quarantine_prefix(repository: RepositoryId) -> Vec<u8> {
    format!("prolly-s3/{repository}/quarantine/").into_bytes()
}

#[cfg(feature = "slatedb-index")]
fn cache_owner_key() -> &'static [u8] {
    b"prolly-s3/cache-owner/v1"
}

#[cfg(feature = "slatedb-index")]
fn cache_rebuild_key(repository: RepositoryId) -> Vec<u8> {
    format!("prolly-s3/{repository}/maintenance/rebuild-v1").into_bytes()
}

#[cfg(feature = "slatedb-index")]
fn validate_writer_id(writer_id: &str) -> Result<()> {
    if writer_id.is_empty()
        || writer_id.len() > 128
        || writer_id
            .bytes()
            .any(|byte| !(byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')))
    {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "SlateDB writer ID must be 1..=128 ASCII alphanumeric, '-', '_', or '.' bytes",
        ));
    }
    Ok(())
}

#[cfg(feature = "slatedb-index")]
fn map_slatedb(error: slatedb::Error) -> Error {
    Error::new(
        ErrorCode::Transport,
        format!("SlateDB advisory index failed: {error}"),
    )
}

#[cfg(all(test, feature = "slatedb-index"))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn cache_rebuild_resumes_a_durable_running_checkpoint() {
        let store = std::sync::Arc::new(slatedb::object_store::memory::InMemory::new());
        let repository = RepositoryId::from_hash([7; 32]);
        let heads = vec![("main".to_string(), CommitId::from_hash([9; 32]))];
        let first =
            SlateDbAdvisoryIndex::open_owned(store.clone(), repository, "checkpoint-writer")
                .await
                .unwrap();
        let (_, resumed) = first.begin_rebuild(repository, &heads).await.unwrap();
        assert!(!resumed);
        first.close().await.unwrap();
        drop(first);

        let reopened = SlateDbAdvisoryIndex::open_owned(store, repository, "checkpoint-writer")
            .await
            .unwrap();
        let report = reopened.rebuild_heads(repository, &heads).await.unwrap();
        assert!(report.resumed_from_checkpoint);
        assert_eq!(report.written_heads, 1);
        assert_eq!(
            reopened.branch_head(repository, "main").await.unwrap(),
            Some(heads[0].1)
        );
        reopened.close().await.unwrap();
    }
}
