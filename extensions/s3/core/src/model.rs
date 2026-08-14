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
    authority::{AuthorityScope, AuthorityStamp},
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

hash_id!(RepositoryId, "pr_");
hash_id!(CommitId, "pbc_");
hash_id!(ObjectVersionId, "pov_");
hash_id!(ReflogEntryId, "prl_");
hash_id!(PublicationEventId, "ppe_");
hash_id!(RefCatalogEventId, "pce_");
hash_id!(JournalIndexRebuildChunkId, "jrc_");
hash_id!(OperationIndexSegmentId, "poi_");
hash_id!(TreeFormatDigest, "ptf_");
hash_id!(ProviderProfileId, "ppf_");
hash_id!(NodePackId, "pnp_");

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
        write!(f, "pb_{}", self.0.simple())
    }
}
impl FromStr for BatchId {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("pb_")
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
        write!(f, "op_{}", self.0.simple())
    }
}

impl FromStr for OperationId {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self> {
        let value = value
            .strip_prefix("op_")
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

/// Create-once format marker for a repository.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryFormat {
    pub repository_id: RepositoryId,
    pub state_tree_format: TreeFormat,
    pub canonical_limits: CanonicalLimits,
    pub idempotency_retention: IdempotencyRetention,
    pub provider_per_key_version_limit: ProviderPerKeyVersionLimit,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InitializationIntent {
    pub repository_id: RepositoryId,
    pub format: RepositoryFormat,
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
pub struct ProviderAttestationBody {
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

impl ProviderAttestationBody {
    pub fn id(&self) -> Result<ProviderProfileId> {
        let bytes = encode_canonical(self)?;
        Ok(ProviderProfileId(domain_hash(
            b"prolly-s3/provider-profile",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderAttestation {
    pub id: ProviderProfileId,
    pub body: ProviderAttestationBody,
    pub signature: Vec<u8>,
}

impl ProviderAttestation {
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
pub struct RootManifest {
    pub root: Option<Cid>,
    pub format_digest: TreeFormatDigest,
}

impl RootManifest {
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
        b"prolly-s3/tree-format",
        &[&bytes],
    )))
}

/// Repository logical state.
///
/// Operation idempotency is intentionally absent:  checkpoints the bounded
/// branch publication journal into `SegmentedOperationIndex` instead of
/// retaining an operation tree in every repository snapshot.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketState {
    pub objects: RootManifest,
    pub versions: RootManifest,
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
pub enum EtagPredicate {
    Any,
    OneOf(BTreeSet<String>),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectWriteCondition {
    pub if_match: Option<EtagPredicate>,
    pub if_none_match: Option<EtagPredicate>,
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
// Keep the canonical wire shape direct. Boxing the live variant would change
// the public persisted model only to reduce its in-memory enum size.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogicalObjectVersionKind {
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
pub struct LogicalObjectVersionBody {
    pub order: ObjectVersionOrder,
    pub created_at_millis: u64,
    pub kind: LogicalObjectVersionKind,
}

/// Current logical object state, including its immutable payload binding.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CurrentObject {
    /// Complete current version so a lookup can fetch the immutable payload
    /// without loading the version-history tree.
    pub version: ObjectVersion,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectTransition {
    pub key: Vec<u8>,
    pub previous: Option<ObjectVersionId>,
    pub next: ObjectVersionId,
    pub delete_marker: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketDelta {
    /// Canonical digest used to reject reuse of a publication operation ID
    /// with different logical input. The operation ID itself lives in the
    /// authority-stamped publication event and bounded operation index.
    pub input_digest: [u8; 32],
    pub changes: Vec<ObjectTransition>,
    /// Optional immutable Prolly root for a merge delta that is too large to
    /// embed in one commit envelope. Tree keys are logical object keys and
    /// values are canonical `ObjectTransition` records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub changes_root: Option<RootManifest>,
    /// Exact number of records in `changes_root`. Inline deltas leave this at
    /// zero and derive their count from `changes`.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub change_count: u64,
}

impl BucketDelta {
    pub fn logical_change_count(&self) -> u64 {
        if self.changes_root.is_some() {
            self.change_count
        } else {
            self.changes.len() as u64
        }
    }
}

fn is_zero_u64(value: &u64) -> bool {
    *value == 0
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackEntry {
    pub cid: Cid,
    pub offset: u64,
    pub len: u32,
    pub sha256: [u8; 32],
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodePackAttachmentKind {
    BucketDelta,
    Reflog,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackAttachment {
    pub kind: NodePackAttachmentKind,
    pub digest: [u8; 32],
    pub offset: u64,
    pub len: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePack {
    pub format_digest: TreeFormatDigest,
    /// Sorted strictly by CID.
    pub entries: Vec<NodePackEntry>,
    pub attachments: Vec<NodePackAttachment>,
    /// Concatenated canonical node and attachment bytes.
    pub payload: Vec<u8>,
}

const NODE_PACK_MAGIC: &[u8; 8] = b"PLYPACK1";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackToc {
    pub format_digest: TreeFormatDigest,
    pub entries: Vec<NodePackEntry>,
    pub attachments: Vec<NodePackAttachment>,
    pub payload_len: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodePackRef {
    pub id: NodePackId,
    pub object_len: u64,
    pub node_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeIndexHead {
    pub repository: RepositoryId,
    pub root: RootManifest,
    pub generation: u64,
    /// Opaque provider continuation for the current bounded commit scan.
    pub scan_continuation: Option<String>,
    /// Increments whenever a complete commit namespace scan finishes.
    pub scan_epoch: u64,
    pub indexed_commit_objects: u64,
    pub updated_at_millis: u64,
}

impl NodeIndexHead {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node-index head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

/// Rebuildable catalog entry for scalable ref enumeration. Authoritative ref
/// objects remain the source of truth for reads and compare-and-exchange.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RefCatalogEntry {
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
pub struct RefCatalogHead {
    pub repository: RepositoryId,
    pub root: RootManifest,
    pub generation: u64,
    /// False scans heads, true scans tags. Completing tags completes an epoch.
    pub scanning_tags: bool,
    pub scan_continuation: Option<String>,
    pub scan_epoch: u64,
    pub indexed_ref_objects: u64,
    pub updated_at_millis: u64,
}

impl RefCatalogHead {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "ref-catalog head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

/// Rebuildable acceleration record for commit ancestry. `first_parent_jumps[n]`
/// is the ancestor 2^n first-parent edges away when that ancestor was indexed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitGraphEntry {
    pub commit: CommitId,
    pub generation: CommitGeneration,
    pub parents: Vec<CommitId>,
    pub first_parent_jumps: Vec<CommitId>,
}

/// Mutable head for the derived commit-graph Prolly tree.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitGraphHead {
    pub repository: RepositoryId,
    pub root: RootManifest,
    pub generation: u64,
    pub scan_continuation: Option<String>,
    pub scan_epoch: u64,
    pub indexed_commit_objects: u64,
    pub updated_at_millis: u64,
}

/// Node location discovered from one authority-stamped  commit publication.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalNodeIndexEntry {
    pub cid: Cid,
    pub container: CommitId,
    pub pack: NodePackId,
    pub absolute_offset: u64,
    pub len: u32,
    pub sha256: [u8; 32],
}

/// Branch-local ancestry acceleration derived only from publication events.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalCommitGraphEntry {
    pub commit: CommitId,
    pub generation: CommitGeneration,
    pub parents: Vec<CommitId>,
    pub first_parent_jumps: Vec<CommitId>,
}

/// One atomic checkpoint for all branch-local journal-derived indexes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalDerivedIndexHead {
    pub repository: RepositoryId,
    pub branch: String,
    pub checkpoint: PublicationEventId,
    pub checkpoint_generation: RefGeneration,
    pub target: CommitId,
    pub node_root: RootManifest,
    pub commit_graph_root: RootManifest,
    pub generation: u64,
    pub indexed_publications: u64,
    pub indexed_commits: u64,
    pub updated_at_millis: u64,
}

impl JournalDerivedIndexHead {
    pub fn validate(
        &self,
        repository: RepositoryId,
        branch: &str,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        crate::repository::validate_branch(branch)?;
        if self.repository != repository
            || self.branch != branch
            || self.node_root.format_digest != expected_format
            || self.commit_graph_root.format_digest != expected_format
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "journal-derived index head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

impl CommitGraphHead {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository || self.root.format_digest != expected_format {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "commit-graph head namespace or tree format is invalid",
            ));
        }
        Ok(())
    }
}

impl NodePack {
    pub(crate) const fn object_header_len() -> usize {
        12
    }

    pub(crate) fn toc_len_from_header(header: &[u8]) -> Result<usize> {
        if header.len() != Self::object_header_len() || &header[..8] != NODE_PACK_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "node pack has an invalid wire header",
            ));
        }
        Ok(u32::from_be_bytes(header[8..12].try_into().expect("fixed range")) as usize)
    }

    /// Encode a range-readable pack: fixed magic and header length, canonical
    /// CBOR table of contents, then the raw concatenated payload.
    pub fn encode_object(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let header = encode_canonical(&NodePackToc {
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
        let header: NodePackToc = decode_canonical(&object[12..payload_start])?;
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

    pub fn decode_toc(bytes: &[u8]) -> Result<NodePackToc> {
        let toc: NodePackToc = decode_canonical(bytes)?;
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
        Ok(NodePackId(domain_hash(b"prolly-s3/node-pack", &[&bytes])))
    }

    pub fn reference(&self) -> Result<NodePackRef> {
        Ok(NodePackRef {
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
    entries: &[NodePackEntry],
    attachments: &[NodePackAttachment],
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

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RefGeneration(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReflogEntry {
    pub branch: String,
    pub old_target: Option<CommitId>,
    pub new_target: CommitId,
    pub operation: OperationId,
    pub actor: String,
    pub message: String,
    pub created_at_millis: u64,
}

impl ReflogEntry {
    pub fn id(&self) -> Result<ReflogEntryId> {
        crate::repository::validate_branch(&self.branch)?;
        let bytes = encode_canonical(self)?;
        Ok(ReflogEntryId(domain_hash(b"prolly-s3/reflog", &[&bytes])))
    }
}

/// Immutable, content-addressed record for one successful branch-ref
/// transition. A ref points at the newest event and events point backward,
/// forming a stable per-branch publication journal without namespace scans.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicationEvent {
    pub repository: RepositoryId,
    pub branch: String,
    pub generation: RefGeneration,
    pub previous: Option<PublicationEventId>,
    pub old_target: Option<CommitId>,
    pub new_target: CommitId,
    pub operation: OperationId,
    pub reflog: ReflogEntryId,
    pub authority: AuthorityStamp,
    pub created_at_millis: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum RefKind {
    Branch,
    Tag,
}

/// Immutable lifecycle event consumed by one prefix-sharded ref catalog.
/// Branch publication events remain the authoritative per-branch history;
/// this record provides a common branch/tag discovery stream whose loss can be
/// repaired from authoritative ref objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefCatalogEvent {
    pub repository: RepositoryId,
    pub shard: u8,
    pub previous: Option<RefCatalogEventId>,
    pub kind: RefKind,
    pub name: String,
    pub target: CommitId,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub tombstone: bool,
    pub created_at_millis: u64,
}

impl RefCatalogEvent {
    pub fn validate(&self, repository: RepositoryId, expected_shard: u8) -> Result<()> {
        crate::repository::validate_branch(&self.name)?;
        if self.repository != repository || self.shard != expected_shard || self.operation.is_nil()
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "ref-catalog event is malformed or belongs to another shard",
            ));
        }
        Ok(())
    }

    pub fn id(&self) -> Result<RefCatalogEventId> {
        let bytes = encode_canonical(self)?;
        Ok(RefCatalogEventId(domain_hash(
            b"prolly-s3/ref-catalog-event",
            &[&bytes],
        )))
    }
}

/// Derived ref state stored in a catalog shard. Tombstones are retained so an
/// old or duplicated lifecycle event cannot resurrect a deleted ref.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeRefCatalogEntry {
    pub target: CommitId,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub tombstone: bool,
    pub updated_at_millis: u64,
}

/// Mutable root of one independently updated ref-catalog shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefCatalogShardHead {
    pub repository: RepositoryId,
    pub shard: u8,
    pub latest_event: RefCatalogEventId,
    pub root: RootManifest,
    pub generation: u64,
    pub updated_at_millis: u64,
}

impl RefCatalogShardHead {
    pub fn validate(
        &self,
        repository: RepositoryId,
        expected_shard: u8,
        expected_format: TreeFormatDigest,
    ) -> Result<()> {
        if self.repository != repository
            || self.shard != expected_shard
            || self.root.format_digest != expected_format
        {
            return Err(Error::new(
                ErrorCode::CorruptNode,
                "ref-catalog shard head is malformed or belongs to another shard",
            ));
        }
        Ok(())
    }
}

impl PublicationEvent {
    pub fn id(&self) -> Result<PublicationEventId> {
        self.validate()?;
        let bytes = encode_canonical(self)?;
        Ok(PublicationEventId(domain_hash(
            b"prolly-s3/publication-event",
            &[&bytes],
        )))
    }

    pub fn validate(&self) -> Result<()> {
        crate::repository::validate_branch(&self.branch)?;
        self.authority.validate(
            self.repository,
            &AuthorityScope::Branch {
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
                "publication event has an invalid journal link",
            ));
        }
        Ok(())
    }

    pub fn matches_ref(&self, reference: &RefValue) -> Result<bool> {
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

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JournalIndexRebuildChunk {
    pub repository: RepositoryId,
    pub branch: String,
    pub job: OperationId,
    pub sequence: u64,
    pub newer: Option<JournalIndexRebuildChunkId>,
    pub events: Vec<PublicationEvent>,
}

impl JournalIndexRebuildChunk {
    pub fn id(&self) -> Result<JournalIndexRebuildChunkId> {
        let bytes = encode_canonical(self)?;
        Ok(JournalIndexRebuildChunkId(domain_hash(
            b"prolly-s3/journal-index-rebuild-chunk",
            &[&bytes],
        )))
    }

    pub fn validate(&self, repository: RepositoryId, branch: &str) -> Result<()> {
        crate::repository::validate_branch(branch)?;
        if self.repository != repository
            || self.branch != branch
            || self.job.is_nil()
            || self.events.is_empty()
            || self.events.len() > 1_000
        {
            return Err(Error::new(
                ErrorCode::CorruptContent,
                "journal-index rebuild chunk is malformed or belongs to another job",
            ));
        }
        let mut previous_generation = None;
        for event in &self.events {
            event.validate()?;
            if event.repository != repository
                || event.branch != branch
                || previous_generation.is_some_and(|previous: u64| event.generation.0 >= previous)
            {
                return Err(Error::new(
                    ErrorCode::CorruptContent,
                    "journal-index rebuild chunk events are not newest-to-oldest",
                ));
            }
            previous_generation = Some(event.generation.0);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct IdempotencyRetention {
    /// An operation remains replayable only while it is within this many ref
    /// generations of the current branch head.
    pub max_generations: u64,
    /// The generation window is additionally capped by wall-clock age.
    pub max_age_millis: u64,
}

impl Default for IdempotencyRetention {
    fn default() -> Self {
        Self {
            max_generations: 1_000_000,
            max_age_millis: 7 * 24 * 60 * 60 * 1_000,
        }
    }
}

impl IdempotencyRetention {
    pub fn validate(self) -> Result<()> {
        if self.max_generations == 0
            || self.max_generations > 1_000_000
            || self.max_age_millis < 60_000
            || self.max_age_millis > 365 * 24 * 60 * 60 * 1_000
        {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "idempotency retention is outside the supported production bounds",
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
pub struct IndexedOperation {
    pub operation: OperationId,
    pub publication: PublicationEventId,
    pub target: CommitId,
    pub generation: RefGeneration,
    pub created_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexSegment {
    pub repository: RepositoryId,
    pub branch: String,
    pub level: u8,
    /// Strictly sorted by operation ID.
    pub entries: Vec<IndexedOperation>,
}

impl OperationIndexSegment {
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
                "operation-index segment is empty or not strictly sorted",
            ));
        }
        Ok(())
    }

    pub fn id(&self) -> Result<OperationIndexSegmentId> {
        self.validate()?;
        let bytes = encode_canonical(self)?;
        Ok(OperationIndexSegmentId(domain_hash(
            b"prolly-s3/operation-index-segment",
            &[&bytes],
        )))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexSegmentRef {
    pub id: OperationIndexSegmentId,
    pub level: u8,
    pub min_generation: RefGeneration,
    pub max_generation: RefGeneration,
    pub min_created_at_millis: u64,
    pub max_created_at_millis: u64,
    pub entries: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OperationIndexHead {
    pub repository: RepositoryId,
    pub branch: String,
    pub checkpoint: PublicationEventId,
    pub checkpoint_generation: RefGeneration,
    pub retention: IdempotencyRetention,
    /// Each level contains fewer than the configured merge fanout segments.
    pub levels: Vec<Vec<OperationIndexSegmentRef>>,
    pub generation: u64,
    pub updated_at_millis: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefValue {
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub reflog: ReflogEntryId,
    pub publication: PublicationEventId,
    pub inline_reflog: ReflogEntry,
    pub authority: AuthorityStamp,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}

impl RefValue {
    pub fn validate(&self, repository: RepositoryId, branch: &str) -> Result<()> {
        crate::repository::validate_branch(branch)?;
        self.authority.validate(
            repository,
            &AuthorityScope::Branch {
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
                "branch ref does not match its authority or inline reflog",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TagValue {
    pub target: CommitId,
    pub previous_target: Option<CommitId>,
    pub generation: RefGeneration,
    pub operation: OperationId,
    pub inline_reflog: ReflogEntry,
    pub authority: AuthorityStamp,
    pub updated_at_millis: u64,
    pub tombstone: bool,
}

impl TagValue {
    pub fn validate(&self, repository: RepositoryId, name: &str) -> Result<()> {
        crate::repository::validate_branch(name)?;
        self.authority.validate(
            repository,
            &AuthorityScope::System {
                namespace: "tags".to_string(),
            },
        )?;
        if self.operation.is_nil()
            || self.inline_reflog.branch != name
            || self.inline_reflog.old_target != self.previous_target
            || self.inline_reflog.new_target != self.target
            || self.inline_reflog.operation != self.operation
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "tag ref does not match its authority or inline reflog",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BucketCommit {
    pub state: BucketState,
    pub parents: Vec<CommitId>,
    pub generation: CommitGeneration,
    pub delta: BucketDelta,
    pub node_pack: Option<NodePackRef>,
    pub authority: AuthorityStamp,
    pub author: String,
    pub message: Option<String>,
    pub created_at_millis: u64,
    pub metadata: BTreeMap<String, Vec<u8>>,
}

impl BucketCommit {
    pub fn id(&self) -> Result<CommitId> {
        let bytes = encode_canonical(self)?;
        Ok(CommitId(domain_hash(b"prolly-s3/commit", &[&bytes])))
    }

    pub fn validate_authority(&self, repository: RepositoryId, branch: &str) -> Result<()> {
        self.authority.validate(
            repository,
            &AuthorityScope::Branch {
                name: branch.to_string(),
            },
        )
    }
}

const COMMIT_OBJECT_MAGIC: &[u8; 8] = b"PLYCOM01";
const COMMIT_OBJECT_HEADER_LEN: usize = 20;

/// Physical immutable representation of an authority-stamped  commit and
/// the Prolly nodes created by it. The  magic keeps the wire object
/// unambiguously separate from `CommitObject` while reusing the frozen
/// node-pack format.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CommitObject {
    pub commit: BucketCommit,
    pub node_pack: Option<NodePack>,
}

impl CommitObject {
    pub(crate) fn commit_object_header_len() -> usize {
        COMMIT_OBJECT_HEADER_LEN
    }

    pub(crate) fn commit_len_from_header(header: &[u8]) -> Result<usize> {
        if header.len() != COMMIT_OBJECT_HEADER_LEN || &header[..8] != COMMIT_OBJECT_MAGIC {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit object has an invalid wire header",
            ));
        }
        Ok(u32::from_be_bytes(header[8..12].try_into().expect("fixed range")) as usize)
    }

    pub(crate) fn pack_len_from_header(header: &[u8]) -> Result<u64> {
        Self::commit_len_from_header(header)?;
        Ok(u64::from_be_bytes(
            header[12..20].try_into().expect("fixed range"),
        ))
    }

    pub(crate) fn decode_commit_metadata(encoded: &[u8]) -> Result<BucketCommit> {
        if encoded.len() < COMMIT_OBJECT_HEADER_LEN {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "commit metadata is shorter than its header",
            ));
        }
        let commit_len = Self::commit_len_from_header(&encoded[..COMMIT_OBJECT_HEADER_LEN])?;
        let end = COMMIT_OBJECT_HEADER_LEN
            .checked_add(commit_len)
            .filter(|end| *end == encoded.len())
            .ok_or_else(|| {
                Error::new(ErrorCode::CorruptCommit, "commit metadata length mismatch")
            })?;
        decode_canonical(&encoded[COMMIT_OBJECT_HEADER_LEN..end])
    }

    pub fn new(commit: BucketCommit, node_pack: Option<NodePack>) -> Result<Self> {
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
        }?;
        if let Some(root) = &self.commit.delta.changes_root {
            if !self.commit.delta.changes.is_empty()
                || self.commit.delta.change_count == 0
                || root.root.is_none()
                || root.format_digest != self.commit.state.objects.format_digest
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "external commit delta is malformed or uses another tree format",
                ));
            }
        } else if self.commit.delta.change_count != 0 {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "inline commit delta carries an external change count",
            ));
        }
        Ok(())
    }

    pub fn encode_object(&self) -> Result<Vec<u8>> {
        self.validate()?;
        let commit = encode_canonical(&self.commit)?;
        let pack = self
            .node_pack
            .as_ref()
            .map(NodePack::encode_object)
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
        let commit = decode_canonical::<BucketCommit>(&encoded[commit_range])?;
        let node_pack = if pack_range.is_empty() {
            None
        } else {
            Some(NodePack::decode_object(&encoded[pack_range])?)
        };
        Self::new(commit, node_pack)
    }

    pub fn node_payload_offset(encoded: &[u8]) -> Result<Option<u64>> {
        let (_, pack_range) = Self::ranges(encoded)?;
        if pack_range.is_empty() {
            return Ok(None);
        }
        let relative = NodePack::object_payload_offset(&encoded[pack_range.start..])?;
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

/// Identity stamped on every provider mutation in repository. The scope is
/// part of idempotency identity, so operation IDs may be safely segmented by
/// writer shard.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct MutationIdentity {
    pub repository: RepositoryId,
    pub operation: OperationId,
    pub authority: AuthorityStamp,
}

/// Repository payload binding. The physical path is explicit because
/// stores content under immutable derived keys instead of accumulating
/// provider versions at the logical user key.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadBinding {
    pub path: ObjectPath,
    pub provider_version_id: Option<String>,
    pub provider_etag: String,
    /// SHA-256 of this logical object's bytes.
    pub checksum_sha256: [u8; 32],
    /// SHA-256 of the physical pack. Absent for legacy/direct payloads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_checksum_sha256: Option<[u8; 32]>,
    /// Inclusive logical extent inside the physical pack.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pack_range: Option<(u64, u64)>,
    /// Content-addressed manifest for a bounded-memory chunked payload.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunk_manifest: Option<ChunkManifestBinding>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifestBinding {
    pub checksum_sha256: [u8; 32],
    pub chunk_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadChunk {
    pub path: ObjectPath,
    pub provider_version_id: Option<String>,
    pub provider_etag: String,
    pub size: u64,
    pub checksum_sha256: [u8; 32],
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkManifest {
    pub format_version: u8,
    pub logical_size: u64,
    pub logical_checksum_sha256: [u8; 32],
    pub chunks: Vec<PayloadChunk>,
}

impl ChunkManifest {
    pub const FORMAT_VERSION: u8 = 1;

    pub fn validate(&self) -> Result<()> {
        let total = self.chunks.iter().try_fold(0_u64, |total, chunk| {
            if chunk.size == 0 || chunk.provider_etag.is_empty() || chunk.checksum_sha256 == [0; 32]
            {
                return Err(Error::new(
                    ErrorCode::CorruptCommit,
                    "chunk manifest contains a malformed chunk",
                ));
            }
            total.checked_add(chunk.size).ok_or_else(|| {
                Error::new(ErrorCode::EntityTooLarge, "chunk manifest size overflow")
            })
        })?;
        if self.format_version != Self::FORMAT_VERSION
            || self.chunks.is_empty()
            || total != self.logical_size
            || self.logical_checksum_sha256 == [0; 32]
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "chunk manifest is malformed",
            ));
        }
        Ok(())
    }
}

impl PayloadBinding {
    pub fn validate(&self) -> Result<()> {
        let pack_shape_valid = match (self.pack_checksum_sha256, self.pack_range) {
            (None, None) => true,
            (Some(physical), Some((start, end))) => physical != [0; 32] && start <= end,
            _ => false,
        };
        let chunk_shape_valid = self.chunk_manifest.as_ref().is_none_or(|manifest| {
            manifest.checksum_sha256 != [0; 32]
                && manifest.chunk_count > 0
                && self.pack_checksum_sha256.is_none()
                && self.pack_range.is_none()
        });
        if self.provider_etag.is_empty()
            || self.checksum_sha256 == [0; 32]
            || !pack_shape_valid
            || !chunk_shape_valid
        {
            return Err(Error::new(
                ErrorCode::CorruptCommit,
                "physical payload binding is malformed",
            ));
        }
        Ok(())
    }

    pub fn physical_checksum_sha256(&self) -> [u8; 32] {
        self.chunk_manifest
            .as_ref()
            .map(|manifest| manifest.checksum_sha256)
            .or(self.pack_checksum_sha256)
            .unwrap_or(self.checksum_sha256)
    }

    pub fn is_packed(&self) -> bool {
        self.pack_range.is_some()
    }

    pub fn is_chunked(&self) -> bool {
        self.chunk_manifest.is_some()
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ObjectVersion {
    pub id: ObjectVersionId,
    pub body: LogicalObjectVersionBody,
    /// Delete markers are logical-only and carry no physical binding.
    pub binding: Option<PayloadBinding>,
}

impl ObjectVersion {
    pub fn derive_id(
        repository: RepositoryId,
        key: &[u8],
        operation: OperationId,
        body: &LogicalObjectVersionBody,
    ) -> Result<ObjectVersionId> {
        let body_bytes = encode_canonical(body)?;
        Ok(ObjectVersionId(domain_hash(
            b"prolly-s3/object-version",
            &[
                repository.as_bytes(),
                key,
                operation.as_bytes(),
                &body_bytes,
            ],
        )))
    }

    pub fn derive(
        repository: RepositoryId,
        key: &[u8],
        operation: OperationId,
        body: LogicalObjectVersionBody,
        binding: Option<PayloadBinding>,
    ) -> Result<Self> {
        validate_physical_object_version(&body, binding.as_ref())?;
        Ok(Self {
            id: Self::derive_id(repository, key, operation, &body)?,
            body,
            binding,
        })
    }

    pub fn validate(&self) -> Result<()> {
        validate_physical_object_version(&self.body, self.binding.as_ref())
    }
}

fn validate_physical_object_version(
    body: &LogicalObjectVersionBody,
    binding: Option<&PayloadBinding>,
) -> Result<()> {
    let valid = match (&body.kind, binding) {
        (LogicalObjectVersionKind::Live { checksums, .. }, Some(binding)) => {
            binding.validate().is_ok()
                && checksums
                    .sha256
                    .is_some_and(|logical| logical == binding.checksum_sha256)
        }
        (LogicalObjectVersionKind::DeleteMarker, None) => true,
        _ => false,
    };
    if !valid {
        return Err(Error::new(
            ErrorCode::CorruptCommit,
            "object version has an invalid immutable payload binding",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProviderPerKeyVersionLimit {
    Unlimited,
    Finite(u64),
    Unknown,
}

impl ProviderPerKeyVersionLimit {
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

impl MutationIdentity {
    pub fn validate(&self, branch: &str) -> Result<()> {
        if self.operation.is_nil() {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "physical mutation operation ID is nil",
            ));
        }
        self.authority.validate(
            self.repository,
            &AuthorityScope::Branch {
                name: branch.to_string(),
            },
        )
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitSessionManifest {
    pub id: BatchId,
    pub branch: String,
    pub base_commit: CommitId,
    pub identity: MutationIdentity,
    pub message: String,
    pub created_at_millis: u64,
    pub expires_at_millis: u64,
}

impl CommitSessionManifest {
    pub fn validate(&self, repository: RepositoryId) -> Result<()> {
        self.identity.validate(&self.branch)?;
        if self.identity.repository != repository
            || self.message.trim().is_empty()
            || self.expires_at_millis <= self.created_at_millis
        {
            return Err(Error::new(
                ErrorCode::InvalidRequest,
                "commit session is malformed or belongs to another repository",
            ));
        }
        Ok(())
    }
}

/// One payload-complete logical mutation ready for an atomic repository
/// branch publication. Put payloads use immutable content-addressed bindings;
/// delete markers do not create physical objects.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct StagedPut {
    pub(crate) key: Vec<u8>,
    pub(crate) size: u64,
    pub(crate) logical_etag: String,
    pub(crate) checksums: Checksums,
    pub(crate) headers: ObjectHeaders,
    pub(crate) user_metadata: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub(crate) tags: BTreeMap<String, String>,
    pub(crate) binding: PayloadBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum StagedMutationBody {
    Put(Box<StagedPut>),
    Delete { key: Vec<u8> },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct StagedMutation {
    pub(crate) body: StagedMutationBody,
}

impl StagedMutation {
    pub fn delete(key: Vec<u8>) -> Self {
        Self {
            body: StagedMutationBody::Delete { key },
        }
    }

    pub fn key(&self) -> &[u8] {
        match &self.body {
            StagedMutationBody::Put(staged) => &staged.key,
            StagedMutationBody::Delete { key } => key,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CommitSessionState {
    Open,
    Aborted,
}

/// Canonical, immutable recovery checkpoint for a repository commit
/// session. Checkpoints retain only logical mutation metadata and immutable
/// payload bindings; object bodies are never copied into the manifest.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CommitSessionCheckpoint {
    pub session: CommitSessionManifest,
    pub sequence: u64,
    pub mutations: Vec<StagedMutation>,
    pub state: CommitSessionState,
}

impl CommitSessionCheckpoint {
    pub fn validate(&self, repository: RepositoryId, max_mutations: usize) -> Result<()> {
        self.session.validate(repository)?;
        if self.mutations.len() > max_mutations {
            return Err(Error::new(
                ErrorCode::InvalidLimit,
                "commit-session checkpoint exceeds the mutation limit",
            ));
        }
        let mut previous = None;
        for mutation in &self.mutations {
            let key = mutation.key();
            if key.is_empty() || previous.is_some_and(|prior: &[u8]| prior >= key) {
                return Err(Error::new(
                    ErrorCode::InvalidRequest,
                    "checkpoint mutations must have unique canonical key order",
                ));
            }
            previous = Some(key);
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitSessionCleanupReport {
    pub scanned: usize,
    pub deleted: usize,
    pub already_missing: usize,
    pub retained: usize,
    pub continuation: Option<String>,
}

pub(crate) fn derive_repository_id(operation: OperationId) -> RepositoryId {
    RepositoryId(domain_hash(
        b"prolly-s3/repository",
        &[operation.as_bytes()],
    ))
}

pub(crate) fn derive_input_digest(parts: &[&[u8]]) -> [u8; 32] {
    domain_hash(b"prolly-s3/operation-input", parts)
}
