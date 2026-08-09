use std::collections::BTreeMap;

use prolly::{BlobRef, Cid, MapVersionId, Node, SnapshotBundle, SnapshotBundleSummary, ValueRef};
use serde::{Deserialize, Serialize};

use crate::{DatabaseFormatRecord, Error, Result, TableDescription, TableStatus};

const ARCHIVE_MAGIC: &[u8; 5] = b"DDBA\x01";
pub const TABLE_ARCHIVE_FORMAT_VERSION: u32 = 1;

/// Explicit resource envelope for encoding, decoding, verifying, and exporting
/// one logical table archive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TableArchiveLimits {
    pub max_nodes: usize,
    pub max_node_bytes: usize,
    pub max_blobs: usize,
    pub max_blob_bytes: usize,
    pub max_archive_bytes: usize,
}

impl TableArchiveLimits {
    pub const fn new(
        max_nodes: usize,
        max_node_bytes: usize,
        max_blobs: usize,
        max_blob_bytes: usize,
        max_archive_bytes: usize,
    ) -> Self {
        Self {
            max_nodes,
            max_node_bytes,
            max_blobs,
            max_blob_bytes,
            max_archive_bytes,
        }
    }

    pub(crate) fn validate(self) -> Result<Self> {
        for (name, value) in [
            ("max_nodes", self.max_nodes),
            ("max_node_bytes", self.max_node_bytes),
            ("max_blobs", self.max_blobs),
            ("max_blob_bytes", self.max_blob_bytes),
            ("max_archive_bytes", self.max_archive_bytes),
        ] {
            if value == 0 {
                return Err(Error::Validation(format!(
                    "table archive limit {name} must be nonzero"
                )));
            }
        }
        Ok(self)
    }
}

/// One verified content-addressed large logical value included in an archive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableArchiveBlob {
    pub reference: BlobRef,
    pub bytes: Vec<u8>,
}

/// Self-contained, canonical backup of one immutable logical table version.
#[derive(Clone, Debug, PartialEq)]
pub struct TableArchive {
    pub format_version: u32,
    pub source: TableDescription,
    pub version: MapVersionId,
    pub version_created_at_millis: Option<u64>,
    pub database_format: DatabaseFormatRecord,
    pub snapshot: SnapshotBundle,
    pub blobs: Vec<TableArchiveBlob>,
}

/// Verified archive metadata suitable for dry-run import output.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableArchiveSummary {
    pub archive_digest: Cid,
    pub version: MapVersionId,
    pub snapshot: SnapshotBundleSummary,
    pub blob_count: usize,
    pub blob_bytes: usize,
    pub encoded_bytes: usize,
}

#[derive(Serialize, Deserialize)]
struct ArchiveWire {
    version: u32,
    source: TableDescription,
    table_version: MapVersionId,
    version_created_at_millis: Option<u64>,
    database_format: Vec<u8>,
    snapshot: Vec<u8>,
    blobs: Vec<ArchiveBlobWire>,
}

#[derive(Serialize, Deserialize)]
struct ArchiveBlobWire {
    cid: [u8; 32],
    len: u64,
    bytes: Vec<u8>,
}

impl TableArchive {
    /// Verify completeness, exact blob reachability, content identities, and
    /// all caller resource limits without mutating a destination.
    pub fn verify(&self, limits: TableArchiveLimits) -> Result<TableArchiveSummary> {
        let limits = limits.validate()?;
        if self.format_version != TABLE_ARCHIVE_FORMAT_VERSION {
            return Err(Error::CorruptData(format!(
                "unsupported table archive format {}",
                self.format_version
            )));
        }
        self.source.validate()?;
        if self.source.status != TableStatus::Active {
            return Err(Error::CorruptData(
                "table archive source descriptor is not active".into(),
            ));
        }
        let snapshot_verification = self.snapshot.verify()?;
        if !snapshot_verification.valid {
            return Err(Error::CorruptData(
                "table archive snapshot is not self-contained".into(),
            ));
        }
        if MapVersionId::for_tree(&self.snapshot.tree)? != self.version {
            return Err(Error::CorruptData(
                "table archive version does not identify its snapshot tree".into(),
            ));
        }
        if self.snapshot.tree.config.format.digest()? != self.database_format.tree_format_digest {
            return Err(Error::CorruptData(
                "table archive snapshot tree format disagrees with its database format".into(),
            ));
        }
        enforce_limit(
            "nodes",
            limits.max_nodes,
            snapshot_verification.summary.node_count,
        )?;
        enforce_limit(
            "node bytes",
            limits.max_node_bytes,
            snapshot_verification.summary.byte_count,
        )?;

        let expected = referenced_blobs(&self.snapshot)?;
        let mut actual = BTreeMap::<Cid, (&BlobRef, &[u8])>::new();
        let mut blob_bytes = 0usize;
        for blob in &self.blobs {
            blob.reference.validate_bytes(&blob.bytes)?;
            let length = usize::try_from(blob.reference.len).map_err(|_| {
                Error::CorruptData("archive blob length exceeds platform limits".into())
            })?;
            if length != blob.bytes.len() {
                return Err(Error::CorruptData(
                    "archive blob reference length does not match payload".into(),
                ));
            }
            if actual
                .insert(blob.reference.cid.clone(), (&blob.reference, &blob.bytes))
                .is_some()
            {
                return Err(Error::CorruptData(
                    "table archive contains a duplicate blob CID".into(),
                ));
            }
            blob_bytes = blob_bytes.checked_add(blob.bytes.len()).ok_or_else(|| {
                Error::Validation("table archive blob byte count overflow".into())
            })?;
        }
        if expected.len() != actual.len()
            || expected.iter().any(|(cid, len)| {
                actual
                    .get(cid)
                    .is_none_or(|(reference, _)| reference.len != *len)
            })
        {
            return Err(Error::CorruptData(
                "table archive blob set is not exactly the snapshot's reachable blob set".into(),
            ));
        }
        enforce_limit("blobs", limits.max_blobs, actual.len())?;
        enforce_limit("blob bytes", limits.max_blob_bytes, blob_bytes)?;

        let encoded = self.encode_canonical()?;
        enforce_limit("archive bytes", limits.max_archive_bytes, encoded.len())?;
        Ok(TableArchiveSummary {
            archive_digest: Cid::from_bytes(&encoded),
            version: self.version.clone(),
            snapshot: snapshot_verification.summary,
            blob_count: actual.len(),
            blob_bytes,
            encoded_bytes: encoded.len(),
        })
    }

    /// Encode a verified archive deterministically under explicit limits.
    pub fn to_bytes(&self, limits: TableArchiveLimits) -> Result<Vec<u8>> {
        self.verify(limits)?;
        self.encode_canonical()
    }

    /// Decode and fully verify an archive before returning it to the caller.
    pub fn from_bytes(bytes: &[u8], limits: TableArchiveLimits) -> Result<Self> {
        let limits = limits.validate()?;
        enforce_limit("archive bytes", limits.max_archive_bytes, bytes.len())?;
        let payload = bytes.strip_prefix(ARCHIVE_MAGIC).ok_or_else(|| {
            Error::CorruptData("table archive magic/version header is invalid".into())
        })?;
        let wire: ArchiveWire = serde_cbor::from_slice(payload)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        let archive = Self {
            format_version: wire.version,
            source: wire.source,
            version: wire.table_version,
            version_created_at_millis: wire.version_created_at_millis,
            database_format: DatabaseFormatRecord::decode(&wire.database_format)?,
            snapshot: SnapshotBundle::from_bytes(&wire.snapshot)?,
            blobs: wire
                .blobs
                .into_iter()
                .map(|blob| TableArchiveBlob {
                    reference: BlobRef {
                        cid: Cid(blob.cid),
                        len: blob.len,
                    },
                    bytes: blob.bytes,
                })
                .collect(),
        };
        archive.verify(limits)?;
        if archive.encode_canonical()? != bytes {
            return Err(Error::CorruptData(
                "table archive encoding is not canonical".into(),
            ));
        }
        Ok(archive)
    }

    fn encode_canonical(&self) -> Result<Vec<u8>> {
        let mut blobs = self.blobs.clone();
        blobs.sort_by(|left, right| {
            left.reference
                .cid
                .as_bytes()
                .cmp(right.reference.cid.as_bytes())
        });
        let wire = ArchiveWire {
            version: self.format_version,
            source: self.source.clone(),
            table_version: self.version.clone(),
            version_created_at_millis: self.version_created_at_millis,
            database_format: self.database_format.encode(),
            snapshot: self.snapshot.to_bytes()?,
            blobs: blobs
                .into_iter()
                .map(|blob| ArchiveBlobWire {
                    cid: blob.reference.cid.0,
                    len: blob.reference.len,
                    bytes: blob.bytes,
                })
                .collect(),
        };
        let payload = serde_cbor::ser::to_vec_packed(&wire)
            .map_err(|error| Error::Serialization(error.to_string()))?;
        let mut encoded = Vec::with_capacity(ARCHIVE_MAGIC.len() + payload.len());
        encoded.extend_from_slice(ARCHIVE_MAGIC);
        encoded.extend_from_slice(&payload);
        Ok(encoded)
    }
}

pub(crate) fn referenced_blobs(snapshot: &SnapshotBundle) -> Result<BTreeMap<Cid, u64>> {
    let mut references = BTreeMap::new();
    for bundled in &snapshot.nodes {
        let node = Node::from_bytes(&bundled.bytes)?;
        if !node.leaf {
            continue;
        }
        for value in &node.vals {
            if let ValueRef::Blob(reference) = ValueRef::from_stored_bytes(value)? {
                match references.insert(reference.cid.clone(), reference.len) {
                    Some(existing) if existing != reference.len => {
                        return Err(Error::CorruptData(
                            "snapshot contains conflicting lengths for one blob CID".into(),
                        ));
                    }
                    _ => {}
                }
            }
        }
    }
    Ok(references)
}

fn enforce_limit(resource: &str, limit: usize, actual: usize) -> Result<()> {
    if actual > limit {
        Err(Error::Validation(format!(
            "table archive exceeded {resource} limit: limit={limit}, actual={actual}"
        )))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use prolly::{Config, MemStore, Mutation, Prolly};

    use super::*;
    use crate::{KeyAttribute, KeyKind, StoragePublicationMode, TableStatus};

    const LIMITS: TableArchiveLimits =
        TableArchiveLimits::new(100, 1024 * 1024, 10, 1024 * 1024, 2 * 1024 * 1024);

    fn fixture() -> TableArchive {
        let manager = Prolly::new(Arc::new(MemStore::new()), Config::default());
        let blob_bytes = b"large legal record".repeat(100);
        let reference = BlobRef::from_bytes(&blob_bytes);
        let tree = manager
            .batch(
                &manager.create(),
                vec![Mutation::Upsert {
                    key: b"case-42".to_vec(),
                    val: ValueRef::Blob(reference.clone()).to_bytes(),
                }],
            )
            .unwrap();
        let version = MapVersionId::for_tree(&tree).unwrap();
        TableArchive {
            format_version: TABLE_ARCHIVE_FORMAT_VERSION,
            source: TableDescription {
                name: "Evidence".into(),
                id: crate::TableId([7; 32]),
                partition_key: KeyAttribute {
                    name: "case_id".into(),
                    kind: KeyKind::String,
                },
                sort_key: None,
                attribute_definitions: std::collections::BTreeMap::from([(
                    "case_id".into(),
                    KeyKind::String,
                )]),
                secondary_indexes: Vec::new(),
                status: TableStatus::Active,
                created_at_millis: 1_700_000_000_000,
            },
            version,
            version_created_at_millis: Some(1_700_000_000_001),
            database_format: DatabaseFormatRecord::current(
                tree.config.format.digest().unwrap(),
                StoragePublicationMode::AtomicNodesAndRoots,
                64 * 1024,
            ),
            snapshot: manager.export_snapshot(&tree).unwrap(),
            blobs: vec![TableArchiveBlob {
                reference,
                bytes: blob_bytes,
            }],
        }
    }

    #[test]
    fn table_archive_is_canonical_complete_and_resource_bounded() {
        let archive = fixture();
        let summary = archive.verify(LIMITS).unwrap();
        assert_eq!(summary.version, archive.version);
        assert_eq!(summary.blob_count, 1);
        assert_eq!(summary.blob_bytes, archive.blobs[0].bytes.len());

        let bytes = archive.to_bytes(LIMITS).unwrap();
        assert_eq!(summary.encoded_bytes, bytes.len());
        assert_eq!(summary.archive_digest, Cid::from_bytes(&bytes));
        let decoded = TableArchive::from_bytes(&bytes, LIMITS).unwrap();
        assert_eq!(decoded, archive);
        assert_eq!(decoded.to_bytes(LIMITS).unwrap(), bytes);

        let too_small = TableArchiveLimits {
            max_blob_bytes: archive.blobs[0].bytes.len() - 1,
            ..LIMITS
        };
        assert!(matches!(
            archive.verify(too_small),
            Err(Error::Validation(message)) if message.contains("blob bytes limit")
        ));
        let decode_too_small = TableArchiveLimits {
            max_archive_bytes: bytes.len() - 1,
            ..LIMITS
        };
        assert!(TableArchive::from_bytes(&bytes, decode_too_small).is_err());
    }

    #[test]
    fn table_archive_rejects_missing_extra_and_corrupt_blobs() {
        let archive = fixture();

        let mut missing = archive.clone();
        missing.blobs.clear();
        assert!(missing.verify(LIMITS).is_err());

        let mut extra = archive.clone();
        let bytes = b"unreachable".to_vec();
        extra.blobs.push(TableArchiveBlob {
            reference: BlobRef::from_bytes(&bytes),
            bytes,
        });
        assert!(extra.verify(LIMITS).is_err());

        let mut corrupt = archive;
        corrupt.blobs[0].bytes[0] ^= 0xff;
        assert!(corrupt.verify(LIMITS).is_err());
    }
}
