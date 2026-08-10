use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use data_encoding::BASE32_NOPAD;
use prolly::{Cid, Tree, TreeFormat};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{codec::domain_hash, encode_canonical, Error, ErrorCode, ObjectPath, Result};

macro_rules! hash_id {
    ($name:ident, $prefix:literal) => {
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
        pub struct $name(pub [u8; 32]);

        impl $name {
            pub const PREFIX: &'static str = $prefix;

            pub fn from_hash(bytes: [u8; 32]) -> Self {
                Self(bytes)
            }

            pub fn as_bytes(&self) -> &[u8; 32] {
                &self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt::Display::fmt(self, f)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "{}{}",
                    Self::PREFIX,
                    BASE32_NOPAD.encode(&self.0).to_ascii_lowercase()
                )
            }
        }

        impl FromStr for $name {
            type Err = Error;

            fn from_str(value: &str) -> Result<Self> {
                let encoded = value.strip_prefix(Self::PREFIX).ok_or_else(|| {
                    Error::new(ErrorCode::InvalidRevision, "invalid identifier prefix")
                })?;
                if encoded.bytes().any(|byte| byte.is_ascii_uppercase()) {
                    return Err(Error::new(
                        ErrorCode::InvalidRevision,
                        "identifier must use lowercase base32",
                    ));
                }
                let decoded = BASE32_NOPAD
                    .decode(encoded.to_ascii_uppercase().as_bytes())
                    .map_err(|_| Error::new(ErrorCode::InvalidRevision, "invalid base32 ID"))?;
                let bytes: [u8; 32] = decoded.try_into().map_err(|_| {
                    Error::new(ErrorCode::InvalidRevision, "invalid identifier length")
                })?;
                let parsed = Self(bytes);
                if parsed.to_string() != value {
                    return Err(Error::new(
                        ErrorCode::InvalidRevision,
                        "noncanonical identifier",
                    ));
                }
                Ok(parsed)
            }
        }
    };
}

hash_id!(RepositoryId, "pr1_");
hash_id!(CommitId, "pbc1_");
hash_id!(ObjectVersionId, "pov1_");
hash_id!(DeltaId, "pdl1_");
hash_id!(ReflogEntryId, "prl1_");
hash_id!(ContentManifestRef, "pcm1_");
hash_id!(TreeFormatDigest, "ptf1_");
hash_id!(ProviderProfileId, "ppf1_");
hash_id!(ProtectionSegmentId, "pps1_");
hash_id!(GcPlanId, "pgc1_");
hash_id!(MultipartCatalogSnapshotId, "pmc1_");

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct UploadId(pub Uuid);

impl UploadId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for UploadId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for UploadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pu1_{}", self.0.simple())
    }
}
impl FromStr for UploadId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("pu1_")
            .ok_or_else(|| Error::new(ErrorCode::InvalidRequest, "invalid upload ID prefix"))?;
        Ok(Self(Uuid::parse_str(value).map_err(|_| {
            Error::new(ErrorCode::InvalidRequest, "invalid upload ID")
        })?))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkspaceId(pub Uuid);
impl WorkspaceId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for WorkspaceId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Debug for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for WorkspaceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pws1_{}", self.0.simple())
    }
}
impl FromStr for WorkspaceId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("pws1_")
            .ok_or_else(|| Error::new(ErrorCode::InvalidRequest, "invalid workspace ID prefix"))?;
        Ok(Self(Uuid::parse_str(value).map_err(|_| {
            Error::new(ErrorCode::InvalidRequest, "invalid workspace ID")
        })?))
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OperationId(pub Uuid);

impl OperationId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for OperationId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

impl fmt::Display for OperationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "op1_{}", self.0.simple())
    }
}

impl FromStr for OperationId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("op1_")
            .ok_or_else(|| Error::new(ErrorCode::InvalidRequest, "invalid operation ID prefix"))?;
        Ok(Self(Uuid::parse_str(value).map_err(|_| {
            Error::new(ErrorCode::InvalidRequest, "invalid operation ID")
        })?))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalLimits {
    pub max_key_bytes: u16,
    pub max_list_page: u16,
    pub max_delete_objects: u16,
    pub max_mutations_per_commit: u16,
    pub max_object_bytes: u64,
    pub content_chunk_bytes: u32,
}

impl Default for CanonicalLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1_024,
            max_list_page: 1_000,
            max_delete_objects: 1_000,
            max_mutations_per_commit: 10_000,
            max_object_bytes: 5 * 1024 * 1024 * 1024 * 1024,
            content_chunk_bytes: 8 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryFormatV1 {
    pub repository_id: RepositoryId,
    pub format_version: u16,
    pub state_tree_format: TreeFormat,
    pub content_index_format: TreeFormat,
    pub canonical_limits: CanonicalLimits,
    pub min_reader_version: u32,
    pub min_writer_version: u32,
    pub created_at_millis: u64,
    /// Appended to the original v1 marker. Missing means the v1 distributed
    /// S3 profile so previously written packed-CBOR maps remain readable.
    #[cfg(not(prolly_s3_legacy_v1_codec))]
    #[serde(
        default = "default_required_capability_profile",
        skip_serializing_if = "is_default_required_capability_profile"
    )]
    pub required_capability_profile: u16,
}

impl RepositoryFormatV1 {
    pub const VERSION: u16 = 1;
    pub const DISTRIBUTED_S3_CAPABILITY_PROFILE: u16 = 1;
    pub const CURRENT_READER_VERSION: u32 = 1;
    pub const CURRENT_WRITER_VERSION: u32 = 1;
}

#[cfg(not(prolly_s3_legacy_v1_codec))]
fn default_required_capability_profile() -> u16 {
    RepositoryFormatV1::DISTRIBUTED_S3_CAPABILITY_PROFILE
}

#[cfg(not(prolly_s3_legacy_v1_codec))]
fn is_default_required_capability_profile(value: &u16) -> bool {
    *value == RepositoryFormatV1::DISTRIBUTED_S3_CAPABILITY_PROFILE
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializationIntentV1 {
    pub repository_id: RepositoryId,
    pub format: RepositoryFormatV1,
    pub operation: OperationId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum BucketClass {
    GeneralPurpose,
    S3Compatible,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalVersioning {
    Unversioned,
    Enabled,
    Suspended,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderCapabilities {
    pub conditional_create: bool,
    pub conditional_update: bool,
    pub strong_get_after_put: bool,
    pub strong_list_after_put: bool,
    pub strong_list_after_delete: bool,
    pub ranged_get: bool,
    pub paged_list: bool,
    pub list_physical_versions: bool,
    pub exact_version_delete: bool,
    pub physical_versioning: PhysicalVersioning,
    pub conflicting_lifecycle_rule: bool,
    pub default_object_lock_retention: bool,
    pub max_object_bytes: u64,
    pub max_single_put_bytes: u64,
}

impl ProviderCapabilities {
    pub fn validate_distributed(&self) -> Result<()> {
        let required = [
            ("conditional_create", self.conditional_create),
            ("conditional_update", self.conditional_update),
            ("strong_get_after_put", self.strong_get_after_put),
            ("strong_list_after_put", self.strong_list_after_put),
            ("strong_list_after_delete", self.strong_list_after_delete),
            ("ranged_get", self.ranged_get),
            ("paged_list", self.paged_list),
            ("list_physical_versions", self.list_physical_versions),
            ("exact_version_delete", self.exact_version_delete),
        ];
        if let Some((name, _)) = required.into_iter().find(|(_, value)| !value) {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                format!("provider is missing required capability {name}"),
            ));
        }
        if self.conflicting_lifecycle_rule {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "provider bucket has a lifecycle rule that may delete repository data",
            ));
        }
        if self.default_object_lock_retention {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "provider bucket has default Object Lock retention",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAttestationBodyV1 {
    pub endpoint_fingerprint: [u8; 32],
    pub bucket_fingerprint: [u8; 32],
    pub bucket_class: BucketClass,
    pub capabilities: ProviderCapabilities,
    pub probe_suite_version: u32,
    pub sdk_version: String,
    pub observed_at_millis: u64,
    pub expires_at_millis: u64,
    pub signer_key_id: String,
}

impl ProviderAttestationBodyV1 {
    pub fn id(&self) -> Result<ProviderProfileId> {
        let bytes = encode_canonical(self)?;
        Ok(ProviderProfileId(domain_hash(
            b"prolly-s3/provider-profile/v1",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAttestationV1 {
    pub id: ProviderProfileId,
    pub body: ProviderAttestationBodyV1,
    pub signature: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtectionSegmentV1 {
    pub operation: OperationId,
    pub previous: Option<ProtectionSegmentId>,
    pub paths: Vec<ObjectPath>,
    pub created_at_millis: u64,
}

impl ProtectionSegmentV1 {
    pub fn id(&self) -> Result<ProtectionSegmentId> {
        let bytes = encode_canonical(self)?;
        Ok(ProtectionSegmentId(domain_hash(
            b"prolly-s3/protection-segment/v1",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublicationLeaseStateV1 {
    Active,
    Completed { commit: CommitId },
    Abandoned,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationLeaseV1 {
    pub operation: OperationId,
    pub writer: String,
    pub generation: u64,
    pub expires_at_millis: u64,
    pub protection_head: Option<ProtectionSegmentId>,
    pub proposal: Option<CommitId>,
    pub state: PublicationLeaseStateV1,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
}

impl ProviderAttestationV1 {
    pub fn validate_id(&self) -> Result<()> {
        if self.id != self.body.id()? {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "provider attestation profile ID does not match its canonical body",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TreeRootV1 {
    pub root: Option<Cid>,
    pub format_digest: TreeFormatDigest,
}

impl TreeRootV1 {
    pub fn from_tree(tree: &Tree) -> Result<Self> {
        Ok(Self {
            root: tree.root.clone(),
            format_digest: tree_format_digest(&tree.config.format)?,
        })
    }
}

pub fn tree_format_digest(format: &TreeFormat) -> Result<TreeFormatDigest> {
    let bytes = encode_canonical(format)?;
    Ok(TreeFormatDigest(domain_hash(
        b"prolly-s3/tree-format/v1",
        &[&bytes],
    )))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketStateV1 {
    pub objects: TreeRootV1,
    pub versions: TreeRootV1,
    pub operations: TreeRootV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentRef {
    Empty,
    Chunks(ContentManifestRef),
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentLayoutV1 {
    CanonicalFixed,
    Composed,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentManifestV1 {
    pub total_len: u64,
    pub chunk_count: u64,
    pub layout: ContentLayoutV1,
    pub chunk_index: TreeRootV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentChunkRef {
    pub cid: Cid,
    pub len: u32,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectHeaders {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    pub content_disposition: Option<String>,
    pub cache_control: Option<String>,
    pub expires_at_millis: Option<u64>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checksums {
    pub md5: Option<[u8; 16]>,
    pub sha256: Option<[u8; 32]>,
    pub algorithm_values: BTreeMap<String, Vec<u8>>,
}

/// Canonical logical ETag predicate evaluated against the exact branch head
/// used as the parent of a put commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum EtagPredicateV1 {
    Any,
    OneOf(BTreeSet<String>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectWriteConditionV1 {
    pub if_match: Option<EtagPredicateV1>,
    pub if_none_match: Option<EtagPredicateV1>,
    pub expected_head: Option<CommitId>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChecksumExpectation {
    pub md5: Option<[u8; 16]>,
    pub sha256: Option<[u8; 32]>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct CommitGeneration(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ObjectVersionOrder {
    pub commit_generation: CommitGeneration,
    pub mutation_ordinal: u32,
}

// Keep the canonical model direct and language-neutral. Boxing only one
// variant would complicate every binding without changing persisted size.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ObjectVersionKindV1 {
    Live {
        content: ContentRef,
        size: u64,
        logical_etag: String,
        headers: ObjectHeaders,
        checksums: Checksums,
        user_metadata: BTreeMap<String, String>,
        tags: BTreeMap<String, String>,
    },
    DeleteMarker,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersionBodyV1 {
    pub order: ObjectVersionOrder,
    pub created_at_millis: u64,
    pub kind: ObjectVersionKindV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersionV1 {
    pub id: ObjectVersionId,
    pub body: ObjectVersionBodyV1,
}

impl ObjectVersionV1 {
    pub fn derive(
        repository: RepositoryId,
        key: &[u8],
        operation: OperationId,
        body: ObjectVersionBodyV1,
    ) -> Result<Self> {
        let body_bytes = encode_canonical(&body)?;
        let id = ObjectVersionId(domain_hash(
            b"prolly-s3/object-version/v1",
            &[
                repository.as_bytes(),
                key,
                operation.as_bytes(),
                &body_bytes,
            ],
        ));
        Ok(Self { id, body })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentObjectV1 {
    pub version: ObjectVersionId,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OperationKind {
    Put,
    Delete,
    Copy,
    MultiDelete,
    CommitSession,
    Merge,
    Restore,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalOperationResult {
    pub kind: OperationKind,
    pub object_versions: Vec<ObjectVersionId>,
    pub changed_keys: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationRecordV1 {
    pub input_digest: [u8; 32],
    pub result: CanonicalOperationResult,
    pub commit_generation: CommitGeneration,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectTransition {
    pub key: Vec<u8>,
    pub previous: Option<ObjectVersionId>,
    pub next: ObjectVersionId,
    pub delete_marker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDeltaV1 {
    pub operation_ids: Vec<OperationId>,
    pub changes: Vec<ObjectTransition>,
}

impl BucketDeltaV1 {
    pub fn id(&self) -> Result<DeltaId> {
        let bytes = encode_canonical(self)?;
        Ok(DeltaId(domain_hash(b"prolly-s3/delta/v1", &[&bytes])))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCommitV1 {
    pub state: BucketStateV1,
    pub parents: Vec<CommitId>,
    pub generation: CommitGeneration,
    pub delta: DeltaId,
    pub author: String,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcFenceV1 {
    pub branches: BTreeMap<String, CommitId>,
    pub tags: BTreeMap<String, CommitId>,
    pub cutoff_millis: u64,
    pub planned_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcCandidateV1 {
    pub path: ObjectPath,
    pub physical_version: crate::PhysicalVersion,
    pub len: u64,
    pub last_modified_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcPlanBodyV1 {
    pub repository: RepositoryId,
    pub fence: GcFenceV1,
    pub candidates: Vec<GcCandidateV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcPlanV1 {
    pub id: GcPlanId,
    pub body: GcPlanBodyV1,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RetentionPinV1 {
    pub name: String,
    pub target: CommitId,
    pub owner: String,
    pub reason: String,
    pub created_at_millis: u64,
    /// Zero means the pin does not expire automatically.
    pub expires_at_millis: u64,
    pub generation: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcMarkRunStateV1 {
    Running,
    Completed,
}

/// Mutable restart checkpoint for GC reachability analysis. Mark state is
/// intentionally recomputed from canonical roots after a crash instead of
/// serializing an unbounded in-memory reachability set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcMarkRunV1 {
    pub id: OperationId,
    pub repository: RepositoryId,
    pub grace_millis: u64,
    pub max_candidates: u64,
    pub planned_at_millis: u64,
    pub generation: u64,
    pub state: GcMarkRunStateV1,
    pub plan: Option<GcPlanId>,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum GcRunStateV1 {
    Running,
    Paused,
    Completed,
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcRunV1 {
    pub plan: GcPlanId,
    pub next_index: usize,
    pub generation: u64,
    pub state: GcRunStateV1,
    pub deleted_versions: usize,
    pub deleted_bytes: u64,
    pub skipped_reachable: usize,
    pub already_missing: usize,
    pub deleted_by_kind: BTreeMap<String, usize>,
    pub deleted_bytes_by_kind: BTreeMap<String, u64>,
    pub updated_at_millis: u64,
    #[serde(default)]
    pub abort_reason: Option<String>,
    #[serde(default)]
    pub delete_rate_limit_per_second: u32,
    #[serde(default)]
    pub last_delete_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SyncRunStateV1 {
    Running,
    Completed,
}

/// Destination-local checkpoint for a reachable-closure transfer. The sorted
/// closure is recomputed from `source_head` on resume, so the checkpoint stays
/// bounded regardless of object count.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SyncRunV1 {
    pub id: OperationId,
    pub repository: RepositoryId,
    pub source_head: CommitId,
    pub source_branch: String,
    pub after_relative_path: Option<String>,
    pub generation: u64,
    pub state: SyncRunStateV1,
    pub copied_objects: u64,
    pub copied_bytes: u64,
    pub already_present: u64,
    pub updated_at_millis: u64,
}

impl GcPlanV1 {
    pub fn derive(body: GcPlanBodyV1) -> Result<Self> {
        let bytes = encode_canonical(&body)?;
        Ok(Self {
            id: GcPlanId(domain_hash(b"prolly-s3/gc-plan/v1", &[&bytes])),
            body,
        })
    }

    pub fn validate_id(&self) -> Result<()> {
        if Self::derive(self.body.clone())?.id != self.id {
            return Err(Error::new(ErrorCode::CorruptCommit, "GC plan ID mismatch"));
        }
        Ok(())
    }
}

impl BucketCommitV1 {
    pub fn id(&self) -> Result<CommitId> {
        let bytes = encode_canonical(self)?;
        Ok(CommitId(domain_hash(b"prolly-s3/commit/v1", &[&bytes])))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefGeneration(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReflogEntryV1 {
    pub branch: String,
    pub old_target: Option<CommitId>,
    pub new_target: CommitId,
    pub operation: OperationId,
    pub actor: String,
    pub message: String,
    pub created_at_millis: u64,
}

impl ReflogEntryV1 {
    pub fn id(&self) -> Result<ReflogEntryId> {
        let bytes = encode_canonical(self)?;
        Ok(ReflogEntryId(domain_hash(
            b"prolly-s3/reflog/v1",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValueV1 {
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub reflog: ReflogEntryId,
    pub writer: String,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagValueV1 {
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub reflog: ReflogEntryId,
    pub writer: String,
    pub created_at_millis: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitReceipt {
    pub id: CommitId,
    pub operation: OperationId,
    pub branch: String,
    pub parents: Vec<CommitId>,
    pub changed_keys: u64,
    pub object_versions: Vec<ObjectVersionId>,
    pub idempotent_replay: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartPartV1 {
    pub part_number: u32,
    pub content: crate::StoredContent,
    pub etag: String,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum MultipartStateV1 {
    Active,
    Completing {
        operation: OperationId,
        request_digest: [u8; 32],
    },
    Completed {
        operation: OperationId,
        request_digest: [u8; 32],
        receipt: CommitReceipt,
    },
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartUploadV1 {
    pub id: UploadId,
    pub branch: String,
    pub key: Vec<u8>,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub parts: BTreeMap<u32, MultipartPartV1>,
    pub generation: u64,
    pub state: MultipartStateV1,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    /// Zero is reserved for uploads written before expiry was introduced and
    /// means "no automatic expiry".
    #[serde(default)]
    pub expires_at_millis: u64,
}

/// Immutable entry captured for stable pagination of active multipart uploads.
/// Upload manifests remain authoritative for lifecycle operations; this entry
/// is only a time-bounded listing projection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartCatalogEntryV1 {
    pub id: UploadId,
    pub key: Vec<u8>,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartCatalogSnapshotBodyV1 {
    pub repository: RepositoryId,
    pub branch: String,
    pub key_prefix: Vec<u8>,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
    pub entries: Vec<MultipartCatalogEntryV1>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MultipartCatalogSnapshotV1 {
    pub id: MultipartCatalogSnapshotId,
    pub body: MultipartCatalogSnapshotBodyV1,
}

impl MultipartCatalogSnapshotV1 {
    pub fn derive(body: MultipartCatalogSnapshotBodyV1) -> Result<Self> {
        let bytes = encode_canonical(&body)?;
        Ok(Self {
            id: MultipartCatalogSnapshotId(domain_hash(
                b"prolly-s3/multipart-catalog-snapshot/v1",
                &[&bytes],
            )),
            body,
        })
    }

    pub fn validate_id(&self) -> Result<()> {
        if Self::derive(self.body.clone())?.id != self.id {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "multipart catalog snapshot ID mismatch",
            ));
        }
        Ok(())
    }
}

// Workspace manifests are durable wire objects; direct fields keep their
// schema symmetric with ordinary mutation inputs.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceMutationV1 {
    Put {
        key: Vec<u8>,
        content: crate::StoredContent,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl WorkspaceMutationV1 {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkspaceStateV1 {
    Active,
    Publishing {
        request_digest: [u8; 32],
    },
    Completed {
        request_digest: [u8; 32],
        receipt: CommitReceipt,
    },
    Aborted,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkspaceManifestV1 {
    pub id: WorkspaceId,
    pub branch: String,
    pub base_commit: CommitId,
    pub operation: OperationId,
    pub message: String,
    pub mutations: BTreeMap<Vec<u8>, WorkspaceMutationV1>,
    pub generation: u64,
    pub state: WorkspaceStateV1,
    pub created_at_millis: u64,
    pub updated_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ObjectData {
    pub key: Vec<u8>,
    pub version: ObjectVersionV1,
    pub bytes: Vec<u8>,
    pub snapshot: CommitId,
}

pub(crate) fn derive_repository_id(operation: OperationId) -> RepositoryId {
    RepositoryId(domain_hash(
        b"prolly-s3/repository/v1",
        &[operation.as_bytes()],
    ))
}

pub(crate) fn derive_content_manifest_id(bytes: &[u8]) -> ContentManifestRef {
    ContentManifestRef(domain_hash(b"prolly-s3/content-manifest/v1", &[bytes]))
}

pub(crate) fn derive_input_digest(parts: &[&[u8]]) -> [u8; 32] {
    domain_hash(b"prolly-s3/operation-input/v1", parts)
}
