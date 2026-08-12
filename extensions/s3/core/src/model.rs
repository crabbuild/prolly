use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    str::FromStr,
};

use data_encoding::BASE32_NOPAD;
use prolly::{Cid, Tree, TreeFormat};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    authority::{AuthorityScopeV2, AuthorityStampV2},
    codec::{domain_hash, sha256},
    decode_canonical, encode_canonical, Error, ErrorCode, ObjectPath, Result,
};

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
hash_id!(CommitIdV2, "pbc2_");
hash_id!(ObjectVersionId, "pov1_");
hash_id!(ObjectVersionIdV2, "pov2_");
hash_id!(ReflogEntryId, "prl1_");
hash_id!(ReflogEntryIdV2, "prl2_");
hash_id!(PublicationEventIdV2, "ppe2_");
hash_id!(OperationIndexSegmentIdV2, "poi2_");
hash_id!(TreeFormatDigest, "ptf1_");
hash_id!(ProviderProfileId, "ppf1_");
hash_id!(GcPlanId, "pgc1_");
hash_id!(NodePackId, "pnp1_");
hash_id!(NodeIndexCheckpointId, "nic1_");

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Uuid);
impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
    pub fn as_bytes(&self) -> &[u8; 16] {
        self.0.as_bytes()
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Debug for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "pb1_{}", self.0.simple())
    }
}
impl FromStr for BatchId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("pb1_")
            .ok_or_else(|| Error::new(ErrorCode::InvalidRequest, "invalid batch ID prefix"))?;
        Ok(Self(Uuid::parse_str(value).map_err(|_| {
            Error::new(ErrorCode::InvalidRequest, "invalid batch ID")
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

    pub const fn nil() -> Self {
        Self(Uuid::nil())
    }

    pub const fn is_nil(self) -> bool {
        self.0.is_nil()
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
}

impl Default for CanonicalLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: 1_024,
            max_list_page: 1_000,
            max_delete_objects: 1_000,
            max_mutations_per_commit: 10_000,
            max_object_bytes: 5 * 1024 * 1024 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryFormatV1 {
    pub repository_id: RepositoryId,
    pub format_version: u16,
    pub state_tree_format: TreeFormat,
    pub canonical_limits: CanonicalLimits,
    pub min_reader_version: u32,
    pub min_writer_version: u32,
    pub created_at_millis: u64,
    pub required_capability_profile: u16,
}

impl RepositoryFormatV1 {
    pub const VERSION: u16 = 1;
    pub const PROLLY_S3_CAPABILITY_PROFILE: u16 = 1;
    pub const PROLLY_S3_PROTOCOL_VERSION: u32 = 1;
    pub const CURRENT_READER_VERSION: u32 = 1;
    pub const CURRENT_WRITER_VERSION: u32 = 1;
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
    pub fn validate_required(&self) -> Result<()> {
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

    pub fn validate_prolly_s3(&self) -> Result<()> {
        self.validate_required()?;
        if self.physical_versioning != PhysicalVersioning::Enabled {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "Prolly S3 repositories require bucket versioning to be enabled",
            ));
        }
        if self.max_single_put_bytes == 0 || self.max_object_bytes == 0 {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "provider did not report usable physical object size limits",
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

/// Provider binding for a logical object version in the prolly-s3
/// profile. The key is deliberately absent: it is always the logical UTF-8
/// key under which this record is stored.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalObjectBindingV1 {
    Live {
        version_id: String,
        provider_etag: String,
        checksum_sha256: [u8; 32],
    },
    DeleteMarker {
        version_id: String,
    },
}

// Keep the canonical wire shape direct. Boxing the live variant would change
// the public persisted model only to reduce its in-memory enum size.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalObjectVersionKindV1 {
    Live {
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
pub struct LogicalObjectVersionBodyV1 {
    pub order: ObjectVersionOrder,
    pub created_at_millis: u64,
    pub kind: LogicalObjectVersionKindV1,
}

/// Prolly S3 object version. Its logical ID excludes the provider
/// binding so a verified clone may preserve logical identity while rebinding
/// to destination-issued S3 VersionIds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersionV1 {
    pub id: ObjectVersionId,
    pub body: LogicalObjectVersionBodyV1,
    pub binding: PhysicalObjectBindingV1,
}

impl ObjectVersionV1 {
    pub fn derive(
        repository: RepositoryId,
        key: &[u8],
        operation: OperationId,
        body: LogicalObjectVersionBodyV1,
        binding: PhysicalObjectBindingV1,
    ) -> Result<Self> {
        validate_physical_object_version(&body, &binding)?;
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
        Ok(Self { id, body, binding })
    }

    pub fn validate(&self) -> Result<()> {
        validate_physical_object_version(&self.body, &self.binding)
    }
}

fn validate_physical_object_version(
    body: &LogicalObjectVersionBodyV1,
    binding: &PhysicalObjectBindingV1,
) -> Result<()> {
    let valid = match (&body.kind, binding) {
        (
            LogicalObjectVersionKindV1::Live { checksums, .. },
            PhysicalObjectBindingV1::Live {
                version_id,
                checksum_sha256,
                ..
            },
        ) => {
            !version_id.is_empty()
                && checksums
                    .sha256
                    .is_some_and(|logical| logical == *checksum_sha256)
        }
        (
            LogicalObjectVersionKindV1::DeleteMarker,
            PhysicalObjectBindingV1::DeleteMarker { version_id },
        ) => !version_id.is_empty(),
        _ => false,
    };
    if !valid {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "physical object version has an invalid logical-to-physical binding",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentObjectV1 {
    /// Complete current version so ordinary reads need only one Prolly-tree
    /// lookup before fetching the exact S3 VersionId.
    pub version: ObjectVersionV1,
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackEntryV1 {
    pub cid: Cid,
    pub offset: u64,
    pub len: u32,
    pub sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePackAttachmentKindV1 {
    BucketDelta,
    Reflog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackAttachmentV1 {
    pub kind: NodePackAttachmentKindV1,
    pub digest: [u8; 32],
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackV1 {
    pub format_digest: TreeFormatDigest,
    /// Sorted strictly by CID.
    pub entries: Vec<NodePackEntryV1>,
    pub attachments: Vec<NodePackAttachmentV1>,
    /// Concatenated canonical node and attachment bytes.
    pub payload: Vec<u8>,
}

const NODE_PACK_MAGIC: &[u8; 8] = b"PLYPACK1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackTocV1 {
    pub format_digest: TreeFormatDigest,
    pub entries: Vec<NodePackEntryV1>,
    pub attachments: Vec<NodePackAttachmentV1>,
    pub payload_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackRefV1 {
    pub id: NodePackId,
    pub object_len: u64,
    pub node_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIndexEntryV1 {
    pub cid: Cid,
    /// Immutable commit object that physically contains the node pack.
    pub container: CommitId,
    pub pack: NodePackId,
    pub absolute_offset: u64,
    pub len: u32,
    pub sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIndexCheckpointV1 {
    pub id: NodeIndexCheckpointId,
    pub repository: RepositoryId,
    pub branch: String,
    pub head: CommitId,
    pub generation: CommitGeneration,
    pub entries: Vec<NodeIndexEntryV1>,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIndexHeadV1 {
    pub checkpoint: NodeIndexCheckpointId,
    pub head: CommitId,
    pub generation: CommitGeneration,
    pub updated_at_millis: u64,
}

/// Mutable head for the scalable node-location index. The root addresses a
/// separate immutable Prolly tree whose keys are node CIDs and whose values are
/// canonical [`NodeIndexEntryV1`] records.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIndexHeadV2 {
    pub repository: RepositoryId,
    pub root: TreeRootV1,
    pub generation: u64,
    /// Opaque provider continuation for the current bounded commit scan.
    pub scan_continuation: Option<String>,
    /// Increments whenever a complete commit namespace scan finishes.
    pub scan_epoch: u64,
    pub indexed_commit_objects: u64,
    pub updated_at_millis: u64,
}

impl NodeIndexHeadV2 {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-index v2 head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

/// Rebuildable catalog entry for scalable ref enumeration. Authoritative ref
/// objects remain the source of truth for reads and compare-and-exchange.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefCatalogEntryV2 {
    Branch {
        target: CommitId,
        generation: RefGeneration,
    },
    Tag {
        target: CommitId,
        generation: RefGeneration,
    },
}

/// Mutable head for the derived ref catalog Prolly tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefCatalogHeadV2 {
    pub repository: RepositoryId,
    pub root: TreeRootV1,
    pub generation: u64,
    /// False scans heads, true scans tags. Completing tags completes an epoch.
    pub scanning_tags: bool,
    pub scan_continuation: Option<String>,
    pub scan_epoch: u64,
    pub indexed_ref_objects: u64,
    pub updated_at_millis: u64,
}

impl RefCatalogHeadV2 {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "ref-catalog v2 head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

/// Rebuildable acceleration record for commit ancestry. `first_parent_jumps[n]`
/// is the ancestor 2^n first-parent edges away when that ancestor was indexed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitGraphEntryV2 {
    pub commit: CommitId,
    pub generation: CommitGeneration,
    pub parents: Vec<CommitId>,
    pub first_parent_jumps: Vec<CommitId>,
}

/// Mutable head for the derived commit-graph Prolly tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitGraphHeadV2 {
    pub repository: RepositoryId,
    pub root: TreeRootV1,
    pub generation: u64,
    pub scan_continuation: Option<String>,
    pub scan_epoch: u64,
    pub indexed_commit_objects: u64,
    pub updated_at_millis: u64,
}

impl CommitGraphHeadV2 {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "commit-graph v2 head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

impl NodeIndexHeadV1 {
    pub fn validate(&self, checkpoint: &NodeIndexCheckpointV1) -> Result<()> {
        if self.checkpoint != checkpoint.id
            || self.head != checkpoint.head
            || self.generation != checkpoint.generation
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-index head does not match its checkpoint",
            ));
        }
        Ok(())
    }
}

impl NodeIndexCheckpointV1 {
    pub fn derive(
        repository: RepositoryId,
        branch: String,
        head: CommitId,
        generation: CommitGeneration,
        entries: Vec<NodeIndexEntryV1>,
        created_at_millis: u64,
    ) -> Result<Self> {
        let body = encode_canonical(&(
            repository,
            &branch,
            head,
            generation,
            &entries,
            created_at_millis,
        ))?;
        Ok(Self {
            id: NodeIndexCheckpointId(domain_hash(b"prolly-s3/node-index-checkpoint/v1", &[&body])),
            repository,
            branch,
            head,
            generation,
            entries,
            created_at_millis,
        })
    }

    pub fn validate(&self) -> Result<()> {
        let expected = Self::derive(
            self.repository,
            self.branch.clone(),
            self.head,
            self.generation,
            self.entries.clone(),
            self.created_at_millis,
        )?;
        if expected.id != self.id
            || self
                .entries
                .windows(2)
                .any(|pair| pair[0].cid >= pair[1].cid)
            || self.entries.iter().any(|entry| {
                entry.len == 0 || entry.cid.as_bytes() != entry.sha256 || entry.absolute_offset < 12
            })
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-index checkpoint is invalid",
            ));
        }
        Ok(())
    }
}

impl NodePackV1 {
    /// Encode a range-readable pack: fixed magic and header length, canonical
    /// CBOR table of contents, then the raw concatenated payload.
    pub fn encode_object(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let header = encode_canonical(&NodePackTocV1 {
            format_digest: self.format_digest,
            entries: self.entries.clone(),
            attachments: self.attachments.clone(),
            payload_len: self.payload.len() as u64,
        })?;
        let header_len = u32::try_from(header.len())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "node-pack header exceeds u32"))?;
        let mut object = Vec::with_capacity(12 + header.len() + self.payload.len());
        object.extend_from_slice(NODE_PACK_MAGIC);
        object.extend_from_slice(&header_len.to_be_bytes());
        object.extend_from_slice(&header);
        object.extend_from_slice(&self.payload);
        Ok(object)
    }

    pub fn decode_object(object: &[u8]) -> Result<Self> {
        if object.len() < 12 || &object[..8] != NODE_PACK_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node pack has an invalid wire header",
            ));
        }
        let header_len =
            u32::from_be_bytes(object[8..12].try_into().expect("fixed range")) as usize;
        let payload_start = 12usize.checked_add(header_len).ok_or_else(|| {
            Error::new(ErrorCode::CorruptNode, "node-pack header length overflow")
        })?;
        if payload_start > object.len() {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-pack header is truncated",
            ));
        }
        let header: NodePackTocV1 = decode_canonical(&object[12..payload_start])?;
        let payload = object[payload_start..].to_vec();
        if payload.len() as u64 != header.payload_len {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-pack payload length mismatch",
            ));
        }
        let pack = Self {
            format_digest: header.format_digest,
            entries: header.entries,
            attachments: header.attachments,
            payload,
        };
        pack.validate()?;
        Ok(pack)
    }

    pub fn object_payload_offset(object_prefix: &[u8]) -> Result<u64> {
        if object_prefix.len() < 12 || &object_prefix[..8] != NODE_PACK_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node pack has an invalid wire header",
            ));
        }
        Ok(12
            + u64::from(u32::from_be_bytes(
                object_prefix[8..12].try_into().expect("fixed range"),
            )))
    }

    pub fn decode_toc(bytes: &[u8]) -> Result<NodePackTocV1> {
        let toc: NodePackTocV1 = decode_canonical(bytes)?;
        for pair in toc.entries.windows(2) {
            if pair[0].cid >= pair[1].cid {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node-pack table of contents is not CID-sorted",
                ));
            }
        }
        for entry in &toc.entries {
            let end = entry
                .offset
                .checked_add(u64::from(entry.len))
                .ok_or_else(|| {
                    Error::new(ErrorCode::CorruptNode, "node-pack entry range overflow")
                })?;
            if entry.len == 0 || end > toc.payload_len || entry.cid.as_bytes() != entry.sha256 {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node-pack table of contents contains an invalid node range",
                ));
            }
        }
        for attachment in &toc.attachments {
            if attachment
                .offset
                .checked_add(u64::from(attachment.len))
                .is_none_or(|end| end > toc.payload_len)
            {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node-pack attachment range is invalid",
                ));
            }
        }
        validate_node_pack_ranges(&toc.entries, &toc.attachments, toc.payload_len)?;
        Ok(toc)
    }

    pub fn id(&self) -> Result<NodePackId> {
        let bytes = self.encode_object()?;
        Ok(NodePackId(domain_hash(
            b"prolly-s3/node-pack/v1",
            &[&bytes],
        )))
    }

    pub fn reference(&self) -> Result<NodePackRefV1> {
        Ok(NodePackRefV1 {
            id: self.id()?,
            object_len: u64::try_from(self.encode_object()?.len())
                .map_err(|_| Error::new(ErrorCode::InvalidLimit, "node pack length exceeds u64"))?,
            node_count: u32::try_from(self.entries.len()).map_err(|_| {
                Error::new(ErrorCode::InvalidLimit, "node pack contains too many nodes")
            })?,
        })
    }

    pub fn validate(&self) -> Result<()> {
        for pair in self.entries.windows(2) {
            if pair[0].cid >= pair[1].cid {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node pack entries are not strictly CID-sorted",
                ));
            }
        }
        validate_node_pack_ranges(
            &self.entries,
            &self.attachments,
            u64::try_from(self.payload.len())
                .map_err(|_| Error::new(ErrorCode::CorruptNode, "node-pack payload exceeds u64"))?,
        )?;
        for entry in &self.entries {
            let bytes = self.payload_slice(entry.offset, entry.len)?;
            if sha256(bytes) != entry.sha256 || entry.cid.as_bytes() != entry.sha256 {
                return Err(Error::new(
                    ErrorCode::CorruptNode,
                    "node pack entry does not match its CID and checksum",
                ));
            }
        }
        for attachment in &self.attachments {
            let bytes = self.payload_slice(attachment.offset, attachment.len)?;
            if sha256(bytes) != attachment.digest {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "node pack attachment checksum mismatch",
                ));
            }
        }
        Ok(())
    }

    pub fn node(&self, cid: &Cid) -> Result<Option<&[u8]>> {
        let Ok(index) = self.entries.binary_search_by(|entry| entry.cid.cmp(cid)) else {
            return Ok(None);
        };
        let entry = &self.entries[index];
        Ok(Some(self.payload_slice(entry.offset, entry.len)?))
    }

    fn payload_slice(&self, offset: u64, len: u32) -> Result<&[u8]> {
        let start = usize::try_from(offset)
            .map_err(|_| Error::new(ErrorCode::CorruptNode, "node pack offset overflow"))?;
        let end = start
            .checked_add(len as usize)
            .filter(|end| *end <= self.payload.len())
            .ok_or_else(|| {
                Error::new(ErrorCode::CorruptNode, "node pack range is out of bounds")
            })?;
        Ok(&self.payload[start..end])
    }
}

fn validate_node_pack_ranges(
    entries: &[NodePackEntryV1],
    attachments: &[NodePackAttachmentV1],
    payload_len: u64,
) -> Result<()> {
    let mut ranges = Vec::with_capacity(entries.len() + attachments.len());
    for entry in entries {
        if entry.len == 0 {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-pack entries must be nonempty",
            ));
        }
        let end = entry
            .offset
            .checked_add(u64::from(entry.len))
            .filter(|end| *end <= payload_len)
            .ok_or_else(|| Error::new(ErrorCode::CorruptNode, "node-pack node range is invalid"))?;
        ranges.push((entry.offset, end));
    }
    for attachment in attachments {
        let end = attachment
            .offset
            .checked_add(u64::from(attachment.len))
            .filter(|end| *end <= payload_len)
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::CorruptNode,
                    "node-pack attachment range is invalid",
                )
            })?;
        if attachment.len != 0 {
            ranges.push((attachment.offset, end));
        }
    }
    ranges.sort_unstable();
    if ranges.windows(2).any(|pair| pair[0].1 > pair[1].0) {
        return Err(Error::new(
            ErrorCode::CorruptNode,
            "node-pack payload ranges overlap",
        ));
    }
    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCommitV1 {
    pub state: BucketStateV1,
    pub parents: Vec<CommitId>,
    pub generation: CommitGeneration,
    pub delta: BucketDeltaV1,
    pub node_pack: Option<NodePackRefV1>,
    pub writer_fence_generation: u64,
    pub author: String,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl BucketCommitV1 {
    pub fn id(&self) -> Result<CommitId> {
        let bytes = encode_canonical(self)?;
        Ok(CommitId(domain_hash(b"prolly-s3/commit/v1", &[&bytes])))
    }
}

const COMMIT_OBJECT_MAGIC: &[u8; 8] = b"PLYCOM01";
const COMMIT_OBJECT_HEADER_LEN: usize = 20;

/// Physical immutable representation of a commit and the Prolly nodes created
/// by that commit. Keeping both in one object removes a foreground S3 PUT
/// without changing the logical, content-addressed commit identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitObjectV1 {
    pub commit: BucketCommitV1,
    pub node_pack: Option<NodePackV1>,
}

impl CommitObjectV1 {
    pub fn new(commit: BucketCommitV1, node_pack: Option<NodePackV1>) -> Result<Self> {
        let object = Self { commit, node_pack };
        object.validate()?;
        Ok(object)
    }

    pub fn validate(&self) -> Result<()> {
        match (&self.commit.node_pack, &self.node_pack) {
            (None, None) => Ok(()),
            (Some(expected), Some(pack)) if pack.reference()? == *expected => pack.validate(),
            _ => Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit object node pack does not match its logical reference",
            )),
        }
    }

    /// Encode a range-readable object: fixed header, canonical commit bytes,
    /// then the existing range-readable node-pack wire representation.
    pub fn encode_object(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let commit = encode_canonical(&self.commit)?;
        let pack = self
            .node_pack
            .as_ref()
            .map(NodePackV1::encode_object)
            .transpose()?
            .unwrap_or_default();
        let commit_len = u32::try_from(commit.len())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "commit exceeds u32"))?;
        let pack_len = u64::try_from(pack.len())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "node pack exceeds u64"))?;
        let mut encoded = Vec::with_capacity(COMMIT_OBJECT_HEADER_LEN + commit.len() + pack.len());
        encoded.extend_from_slice(COMMIT_OBJECT_MAGIC);
        encoded.extend_from_slice(&commit_len.to_be_bytes());
        encoded.extend_from_slice(&pack_len.to_be_bytes());
        encoded.extend_from_slice(&commit);
        encoded.extend_from_slice(&pack);
        Ok(encoded)
    }

    pub fn decode_object(encoded: &[u8]) -> Result<Self> {
        let (commit_range, pack_range) = Self::ranges(encoded)?;
        let commit = decode_canonical::<BucketCommitV1>(&encoded[commit_range])?;
        let node_pack = if pack_range.is_empty() {
            None
        } else {
            Some(NodePackV1::decode_object(&encoded[pack_range])?)
        };
        Self::new(commit, node_pack)
    }

    /// Absolute offset of the packed-node payload inside the commit object.
    pub fn node_payload_offset(encoded: &[u8]) -> Result<Option<u64>> {
        let (_, pack_range) = Self::ranges(encoded)?;
        if pack_range.is_empty() {
            return Ok(None);
        }
        let relative = NodePackV1::object_payload_offset(&encoded[pack_range.start..])?;
        Ok(Some(pack_range.start as u64 + relative))
    }

    fn ranges(encoded: &[u8]) -> Result<(std::ops::Range<usize>, std::ops::Range<usize>)> {
        if encoded.len() < COMMIT_OBJECT_HEADER_LEN || &encoded[..8] != COMMIT_OBJECT_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit object has an invalid wire header",
            ));
        }
        let commit_len =
            u32::from_be_bytes(encoded[8..12].try_into().expect("fixed range")) as usize;
        let pack_len = usize::try_from(u64::from_be_bytes(
            encoded[12..20].try_into().expect("fixed range"),
        ))
        .map_err(|_| Error::new(ErrorCode::CorruptCommit, "node-pack length exceeds usize"))?;
        let commit_start = COMMIT_OBJECT_HEADER_LEN;
        let commit_end = commit_start
            .checked_add(commit_len)
            .ok_or_else(|| Error::new(ErrorCode::CorruptCommit, "commit length overflow"))?;
        let pack_end = commit_end
            .checked_add(pack_len)
            .filter(|end| *end == encoded.len())
            .ok_or_else(|| Error::new(ErrorCode::CorruptCommit, "commit object length mismatch"))?;
        Ok((commit_start..commit_end, commit_end..pack_end))
    }
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
    pub next_index: u64,
    pub generation: u64,
    pub state: GcRunStateV1,
    pub deleted_versions: u64,
    pub deleted_bytes: u64,
    pub skipped_reachable: u64,
    pub already_missing: u64,
    pub deleted_by_kind: BTreeMap<String, u64>,
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
pub enum GcEpochPhaseV2 {
    DiscoverRoots,
    MarkCommits,
    MarkNodes,
    MarkVersions,
    ScanCandidates,
    Ready,
    Sweeping,
    Completed,
    Aborted,
}

/// Bounded, restartable GC state. The large mark set, work queues, and
/// candidates live in the immutable Prolly tree addressed by `root`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcEpochV2 {
    pub id: OperationId,
    pub repository: RepositoryId,
    pub process_session: OperationId,
    pub writer_fence_generation: u64,
    pub publication_acquisition: u64,
    pub planned_at_millis: u64,
    pub cutoff_millis: u64,
    pub root: TreeRootV1,
    pub phase: GcEpochPhaseV2,
    /// 0=heads, 1=tags, 2=pins, 3=tag reflogs.
    pub root_namespace: u8,
    pub source_continuation: Option<String>,
    pub sweep_after: Option<Vec<u8>>,
    pub generation: u64,
    pub marked_commits: u64,
    pub marked_nodes: u64,
    pub marked_versions: u64,
    pub candidates: u64,
    pub candidate_bytes: u64,
    pub deleted_versions: u64,
    pub deleted_bytes: u64,
    pub skipped_reachable: u64,
    pub already_missing: u64,
    pub updated_at_millis: u64,
    pub abort_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcCommitWorkV2 {
    pub commit: CommitId,
    /// Direct roots need their version tree scanned; ordinary ancestors do not
    /// because logical versions are append-only along descendants.
    pub scan_versions: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GcVersionWorkV2 {
    pub root: TreeRootV1,
    pub after: Option<Vec<u8>>,
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
    pub inline_reflog: ReflogEntryV1,
    pub writer: String,
    pub writer_fence_generation: u64,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExclusiveWriterLeaseV1 {
    pub repository: RepositoryId,
    pub writer_id: String,
    pub generation: u64,
    pub fencing_token: [u8; 32],
    pub expires_at_millis: u64,
    pub updated_at_millis: u64,
}

impl ExclusiveWriterLeaseV1 {
    pub fn validate(&self, repository: RepositoryId) -> Result<()> {
        if self.repository != repository
            || self.writer_id.is_empty()
            || self.generation == 0
            || self.fencing_token == [0; 32]
            || self.expires_at_millis <= self.updated_at_millis
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "exclusive writer lease is malformed",
            ));
        }
        Ok(())
    }
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

/// Durable caller-held handle for provider-managed multipart state. The handle
/// contains no secret credentials; callers must persist the exact value
/// returned by create if they intend to complete after a process restart.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalMultipartSessionV1 {
    pub repository: RepositoryId,
    pub branch: String,
    pub key: Vec<u8>,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub provider_upload_id: String,
    pub operation: OperationId,
    pub writer_fence_generation: u64,
    pub created_at_millis: u64,
    #[serde(default)]
    pub discovered: bool,
}

impl PhysicalMultipartSessionV1 {
    pub fn validate_address(&self, repository: RepositoryId) -> Result<()> {
        crate::repository::validate_branch(&self.branch)?;
        if self.repository != repository
            || self.key.is_empty()
            || self.provider_upload_id.is_empty()
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical multipart session is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }

    pub fn validate(&self, repository: RepositoryId) -> Result<()> {
        self.validate_address(repository)?;
        if self.discovered || self.operation.is_nil() || self.writer_fence_generation == 0 {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical multipart completion requires the original create handle",
            ));
        }
        Ok(())
    }
}

// Prepared physical mutations bind logical versions to provider version IDs.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalPreparedMutationV1 {
    PhysicalPut {
        key: Vec<u8>,
        size: u64,
        logical_etag: String,
        checksums: Checksums,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
        binding: PhysicalObjectBindingV1,
    },
    PhysicalDelete {
        key: Vec<u8>,
        binding: PhysicalObjectBindingV1,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PhysicalBatchMutationV1 {
    Put {
        key: Vec<u8>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        user_metadata: BTreeMap<String, String>,
    },
    Delete {
        key: Vec<u8>,
    },
}

impl PhysicalBatchMutationV1 {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

impl PhysicalPreparedMutationV1 {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::PhysicalPut { key, .. } | Self::PhysicalDelete { key, .. } => key,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalBatchV1 {
    pub id: BatchId,
    pub branch: String,
    pub base_commit: CommitId,
    pub operation: OperationId,
    pub message: String,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReflogEntryV2 {
    pub branch: String,
    pub old_target: Option<CommitIdV2>,
    pub new_target: CommitIdV2,
    pub operation: OperationId,
    pub actor: String,
    pub message: String,
    pub created_at_millis: u64,
}

impl ReflogEntryV2 {
    pub fn id(&self) -> Result<ReflogEntryIdV2> {
        crate::repository::validate_branch(&self.branch)?;
        let bytes = encode_canonical(self)?;
        Ok(ReflogEntryIdV2(domain_hash(
            b"prolly-s3/reflog/v2",
            &[&bytes],
        )))
    }
}

/// Immutable, content-addressed record for one successful branch-ref
/// transition. A ref points at the newest event and events point backward,
/// forming a stable per-branch publication journal without namespace scans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationEventV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub generation: RefGeneration,
    pub previous: Option<PublicationEventIdV2>,
    pub old_target: Option<CommitIdV2>,
    pub new_target: CommitIdV2,
    pub operation: OperationId,
    pub reflog: ReflogEntryIdV2,
    pub authority: AuthorityStampV2,
    pub created_at_millis: u64,
}

impl PublicationEventV2 {
    pub fn id(&self) -> Result<PublicationEventIdV2> {
        self.validate()?;
        let bytes = encode_canonical(self)?;
        Ok(PublicationEventIdV2(domain_hash(
            b"prolly-s3/publication-event/v2",
            &[&bytes],
        )))
    }

    pub fn validate(&self) -> Result<()> {
        crate::repository::validate_branch(&self.branch)?;
        self.authority.validate(
            self.repository,
            &AuthorityScopeV2::Branch {
                name: self.branch.clone(),
            },
        )?;
        let link_shape_is_valid = if self.generation.0 == 0 {
            self.previous.is_none() && self.old_target.is_none()
        } else {
            self.previous.is_some() && self.old_target.is_some()
        };
        if self.operation.is_nil() || !link_shape_is_valid {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 publication event has an invalid journal link",
            ));
        }
        Ok(())
    }

    pub fn matches_ref(&self, reference: &RefValueV2) -> Result<bool> {
        Ok(self.id()? == reference.publication
            && self.repository == reference.authority.repository
            && self.branch == reference.inline_reflog.branch
            && self.generation == reference.generation
            && self.old_target == reference.previous_target
            && self.new_target == reference.target
            && self.operation == reference.operation
            && self.reflog == reference.reflog
            && self.authority == reference.authority
            && self.created_at_millis == reference.updated_at_millis)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRetentionV2 {
    /// An operation remains replayable only while it is within this many ref
    /// generations of the current branch head.
    pub max_generations: u64,
    /// The generation window is additionally capped by wall-clock age.
    pub max_age_millis: u64,
}

impl Default for IdempotencyRetentionV2 {
    fn default() -> Self {
        Self {
            max_generations: 1_000_000,
            max_age_millis: 7 * 24 * 60 * 60 * 1_000,
        }
    }
}

impl IdempotencyRetentionV2 {
    pub fn validate(self) -> Result<()> {
        if self.max_generations == 0
            || self.max_generations > 1_000_000
            || self.max_age_millis < 60_000
            || self.max_age_millis > 365 * 24 * 60 * 60 * 1_000
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "v2 idempotency retention is outside the supported production bounds",
            ));
        }
        Ok(())
    }

    pub fn contains(
        self,
        current_generation: RefGeneration,
        now_millis: u64,
        operation_generation: RefGeneration,
        created_at_millis: u64,
    ) -> bool {
        operation_generation.0 <= current_generation.0
            && created_at_millis <= now_millis
            && current_generation.0.saturating_sub(operation_generation.0) <= self.max_generations
            && now_millis.saturating_sub(created_at_millis) <= self.max_age_millis
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexedOperationV2 {
    pub operation: OperationId,
    pub publication: PublicationEventIdV2,
    pub target: CommitIdV2,
    pub generation: RefGeneration,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexSegmentV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub level: u8,
    /// Strictly sorted by operation ID.
    pub entries: Vec<IndexedOperationV2>,
}

impl OperationIndexSegmentV2 {
    pub fn validate(&self) -> Result<()> {
        crate::repository::validate_branch(&self.branch)?;
        if self.entries.is_empty()
            || self.entries.iter().any(|entry| entry.operation.is_nil())
            || self
                .entries
                .windows(2)
                .any(|pair| pair[0].operation >= pair[1].operation)
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 operation-index segment is empty or not strictly sorted",
            ));
        }
        Ok(())
    }

    pub fn id(&self) -> Result<OperationIndexSegmentIdV2> {
        self.validate()?;
        let bytes = encode_canonical(self)?;
        Ok(OperationIndexSegmentIdV2(domain_hash(
            b"prolly-s3/operation-index-segment/v2",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexSegmentRefV2 {
    pub id: OperationIndexSegmentIdV2,
    pub level: u8,
    pub min_generation: RefGeneration,
    pub max_generation: RefGeneration,
    pub min_created_at_millis: u64,
    pub max_created_at_millis: u64,
    pub entries: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexHeadV2 {
    pub repository: RepositoryId,
    pub branch: String,
    pub checkpoint: PublicationEventIdV2,
    pub checkpoint_generation: RefGeneration,
    pub retention: IdempotencyRetentionV2,
    /// Each level contains fewer than the configured merge fanout segments.
    pub levels: Vec<Vec<OperationIndexSegmentRefV2>>,
    pub generation: u64,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValueV2 {
    pub target: CommitIdV2,
    pub previous_target: Option<CommitIdV2>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub reflog: ReflogEntryIdV2,
    pub publication: PublicationEventIdV2,
    pub inline_reflog: ReflogEntryV2,
    pub authority: AuthorityStampV2,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}

impl RefValueV2 {
    pub fn validate(&self, repository: RepositoryId, branch: &str) -> Result<()> {
        crate::repository::validate_branch(branch)?;
        self.authority.validate(
            repository,
            &AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
        )?;
        if self.operation.is_nil()
            || self.inline_reflog.branch != branch
            || self.inline_reflog.old_target != self.previous_target
            || self.inline_reflog.new_target != self.target
            || self.inline_reflog.operation != self.operation
            || self.inline_reflog.id()? != self.reflog
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 branch ref does not match its authority or inline reflog",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCommitV2 {
    pub state: BucketStateV1,
    pub parents: Vec<CommitIdV2>,
    pub generation: CommitGeneration,
    pub delta: BucketDeltaV1,
    pub node_pack: Option<NodePackRefV1>,
    pub authority: AuthorityStampV2,
    pub author: String,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl BucketCommitV2 {
    pub fn id(&self) -> Result<CommitIdV2> {
        let bytes = encode_canonical(self)?;
        Ok(CommitIdV2(domain_hash(b"prolly-s3/commit/v2", &[&bytes])))
    }

    pub fn validate_authority(&self, repository: RepositoryId, branch: &str) -> Result<()> {
        self.authority.validate(
            repository,
            &AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
        )
    }
}

const COMMIT_OBJECT_V2_MAGIC: &[u8; 8] = b"PLYCOM02";

/// Physical immutable representation of an authority-stamped v2 commit and
/// the Prolly nodes created by it. The v2 magic keeps the wire object
/// unambiguously separate from `CommitObjectV1` while reusing the frozen v1
/// node-pack format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitObjectV2 {
    pub commit: BucketCommitV2,
    pub node_pack: Option<NodePackV1>,
}

impl CommitObjectV2 {
    pub fn new(commit: BucketCommitV2, node_pack: Option<NodePackV1>) -> Result<Self> {
        let object = Self { commit, node_pack };
        object.validate()?;
        Ok(object)
    }

    pub fn validate(&self) -> Result<()> {
        match (&self.commit.node_pack, &self.node_pack) {
            (None, None) => Ok(()),
            (Some(expected), Some(pack)) if pack.reference()? == *expected => pack.validate(),
            _ => Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 commit object node pack does not match its logical reference",
            )),
        }
    }

    pub fn encode_object(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let commit = encode_canonical(&self.commit)?;
        let pack = self
            .node_pack
            .as_ref()
            .map(NodePackV1::encode_object)
            .transpose()?
            .unwrap_or_default();
        let commit_len = u32::try_from(commit.len())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "v2 commit exceeds u32"))?;
        let pack_len = u64::try_from(pack.len())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "v2 node pack exceeds u64"))?;
        let mut encoded = Vec::with_capacity(COMMIT_OBJECT_HEADER_LEN + commit.len() + pack.len());
        encoded.extend_from_slice(COMMIT_OBJECT_V2_MAGIC);
        encoded.extend_from_slice(&commit_len.to_be_bytes());
        encoded.extend_from_slice(&pack_len.to_be_bytes());
        encoded.extend_from_slice(&commit);
        encoded.extend_from_slice(&pack);
        Ok(encoded)
    }

    pub fn decode_object(encoded: &[u8]) -> Result<Self> {
        let (commit_range, pack_range) = Self::ranges(encoded)?;
        let commit = decode_canonical::<BucketCommitV2>(&encoded[commit_range])?;
        let node_pack = if pack_range.is_empty() {
            None
        } else {
            Some(NodePackV1::decode_object(&encoded[pack_range])?)
        };
        Self::new(commit, node_pack)
    }

    pub fn node_payload_offset(encoded: &[u8]) -> Result<Option<u64>> {
        let (_, pack_range) = Self::ranges(encoded)?;
        if pack_range.is_empty() {
            return Ok(None);
        }
        let relative = NodePackV1::object_payload_offset(&encoded[pack_range.start..])?;
        Ok(Some(pack_range.start as u64 + relative))
    }

    fn ranges(encoded: &[u8]) -> Result<(std::ops::Range<usize>, std::ops::Range<usize>)> {
        if encoded.len() < COMMIT_OBJECT_HEADER_LEN || &encoded[..8] != COMMIT_OBJECT_V2_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 commit object has an invalid wire header",
            ));
        }
        let commit_len =
            u32::from_be_bytes(encoded[8..12].try_into().expect("fixed range")) as usize;
        let pack_len = usize::try_from(u64::from_be_bytes(
            encoded[12..20].try_into().expect("fixed range"),
        ))
        .map_err(|_| {
            Error::new(
                ErrorCode::CorruptCommit,
                "v2 node-pack length exceeds usize",
            )
        })?;
        let commit_start = COMMIT_OBJECT_HEADER_LEN;
        let commit_end = commit_start
            .checked_add(commit_len)
            .ok_or_else(|| Error::new(ErrorCode::CorruptCommit, "v2 commit length overflow"))?;
        let pack_end = commit_end
            .checked_add(pack_len)
            .filter(|end| *end == encoded.len())
            .ok_or_else(|| {
                Error::new(ErrorCode::CorruptCommit, "v2 commit object length mismatch")
            })?;
        Ok((commit_start..commit_end, commit_end..pack_end))
    }
}

/// Identity stamped on every provider mutation in protocol v2. The scope is
/// part of idempotency identity, so operation IDs may be safely segmented by
/// writer shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalMutationIdentityV2 {
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub authority: AuthorityStampV2,
}

/// Protocol-v2 payload binding. The physical path is explicit because v2
/// stores content under immutable derived keys instead of accumulating
/// provider versions at the logical user key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalObjectBindingV2 {
    pub path: ObjectPath,
    pub provider_version_id: Option<String>,
    pub provider_etag: String,
    pub checksum_sha256: [u8; 32],
}

impl PhysicalObjectBindingV2 {
    pub fn validate(&self) -> Result<()> {
        if self.provider_etag.is_empty() {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "v2 physical payload binding is malformed",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersionV2 {
    pub id: ObjectVersionIdV2,
    pub body: LogicalObjectVersionBodyV1,
    /// Delete markers are logical-only and carry no physical binding.
    pub binding: Option<PhysicalObjectBindingV2>,
}

impl ObjectVersionV2 {
    pub fn derive(
        repository: RepositoryId,
        key: &[u8],
        operation: OperationId,
        body: LogicalObjectVersionBodyV1,
        binding: Option<PhysicalObjectBindingV2>,
    ) -> Result<Self> {
        validate_physical_object_version_v2(&body, binding.as_ref())?;
        let body_bytes = encode_canonical(&body)?;
        Ok(Self {
            id: ObjectVersionIdV2(domain_hash(
                b"prolly-s3/object-version/v2",
                &[
                    repository.as_bytes(),
                    key,
                    operation.as_bytes(),
                    &body_bytes,
                ],
            )),
            body,
            binding,
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_physical_object_version_v2(&self.body, self.binding.as_ref())
    }
}

fn validate_physical_object_version_v2(
    body: &LogicalObjectVersionBodyV1,
    binding: Option<&PhysicalObjectBindingV2>,
) -> Result<()> {
    let valid = match (&body.kind, binding) {
        (LogicalObjectVersionKindV1::Live { checksums, .. }, Some(binding)) => {
            binding.validate().is_ok()
                && checksums
                    .sha256
                    .is_some_and(|logical| logical == binding.checksum_sha256)
        }
        (LogicalObjectVersionKindV1::DeleteMarker, None) => true,
        _ => false,
    };
    if !valid {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "v2 object version has an invalid immutable payload binding",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderPerKeyVersionLimitV2 {
    Unlimited,
    Finite(u64),
    Unknown,
}

impl ProviderPerKeyVersionLimitV2 {
    /// Immutable payload keys consume one version. Only bounded mutable
    /// controls need per-key headroom; unknown limits fail closed.
    pub fn validate_immutable_payload_profile(self, mutable_control_bound: usize) -> Result<()> {
        let required = u64::try_from(mutable_control_bound)
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "control bound exceeds u64"))?
            .checked_add(2)
            .ok_or_else(|| Error::new(ErrorCode::InvalidLimit, "control headroom overflow"))?;
        match self {
            Self::Unlimited => Ok(()),
            Self::Finite(limit) if limit >= required => Ok(()),
            Self::Finite(limit) => Err(Error::new(
                ErrorCode::ProviderNotQualified,
                format!(
                    "provider per-key version limit {limit} is below required control headroom {required}"
                ),
            )),
            Self::Unknown => Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "provider per-key version limit is unknown",
            )),
        }
    }
}

impl PhysicalMutationIdentityV2 {
    pub fn validate(&self, branch: &str) -> Result<()> {
        if self.operation.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical mutation operation ID is nil",
            ));
        }
        self.authority.validate(
            self.repository,
            &AuthorityScopeV2::Branch {
                name: branch.to_string(),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalMultipartSessionV2 {
    pub identity: PhysicalMutationIdentityV2,
    pub branch: String,
    pub key: Vec<u8>,
    pub headers: ObjectHeaders,
    pub user_metadata: BTreeMap<String, String>,
    pub provider_upload_id: String,
    pub created_at_millis: u64,
    #[serde(default)]
    pub discovered: bool,
}

impl PhysicalMultipartSessionV2 {
    pub fn validate(&self, repository: RepositoryId) -> Result<()> {
        self.identity.validate(&self.branch)?;
        if self.identity.repository != repository
            || self.key.is_empty()
            || self.provider_upload_id.is_empty()
            || self.discovered
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 multipart session is malformed or cannot publish",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PhysicalBatchV2 {
    pub id: BatchId,
    pub branch: String,
    pub base_commit: CommitIdV2,
    pub identity: PhysicalMutationIdentityV2,
    pub message: String,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
}

impl PhysicalBatchV2 {
    pub fn validate(&self, repository: RepositoryId) -> Result<()> {
        self.identity.validate(&self.branch)?;
        if self.identity.repository != repository
            || self.message.trim().is_empty()
            || self.expires_at_millis <= self.created_at_millis
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "v2 physical batch is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }
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

pub(crate) fn derive_input_digest(parts: &[&[u8]]) -> [u8; 32] {
    domain_hash(b"prolly-s3/operation-input/v1", parts)
}
