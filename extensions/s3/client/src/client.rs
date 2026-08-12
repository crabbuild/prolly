use std::{
    collections::{BTreeMap, HashMap, HashSet},
    future::Future,
    pin::Pin,
    str::FromStr,
    sync::{Arc, RwLock},
    task::{Context, Poll},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::{
    operation::{
        abort_multipart_upload::AbortMultipartUploadOutput,
        complete_multipart_upload::CompleteMultipartUploadOutput, copy_object::CopyObjectOutput,
        create_multipart_upload::CreateMultipartUploadOutput, delete_object::DeleteObjectOutput,
        delete_objects::DeleteObjectsOutput, get_object::GetObjectOutput,
        head_object::HeadObjectOutput, list_multipart_uploads::ListMultipartUploadsOutput,
        list_object_versions::ListObjectVersionsOutput, list_objects_v2::ListObjectsV2Output,
        list_parts::ListPartsOutput, put_object::PutObjectOutput, upload_part::UploadPartOutput,
        upload_part_copy::UploadPartCopyOutput,
    },
    primitives::ByteStream,
    types::{
        ChecksumMode, CommonPrefix, CompletedMultipartUpload, CopyObjectResult, CopyPartResult,
        Delete, DeleteMarkerEntry, DeletedObject, MultipartUpload, Object, ObjectVersion, Part,
    },
};
use aws_smithy_types::DateTime;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine as _,
};
use futures_util::stream::BoxStream;
use hmac::{Hmac, Mac};
use http_body::{Body, Frame, SizeHint};
use prolly_s3_core::{
    decode_canonical, encode_canonical, version_cursor_after_key, BatchId, ChecksumExpectation,
    CommitId, CommitReceipt, Error, ErrorCode, EtagPredicateV1, GetRequest, ListRequest,
    LogicalObjectVersionKindV1, ObjectHeaders, ObjectPath, ObjectPlane, ObjectSummary,
    ObjectVersionId, ObjectWriteConditionV1, OperationId, PhysicalBatchV1,
    PhysicalMultipartCompletedPart, PhysicalMultipartPartResult, PhysicalMultipartSessionV1,
    PhysicalVersion, PhysicalVersioning, ProviderAttestationV1, ProviderProfileId, RefValueV1,
    Repository, RepositoryOptions, Result, RetryAdvice, VersionSummary, WriterLeaseMaintenance,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

use crate::{
    ensure_attestation_current, load_valid_attestation, qualify_and_store,
    validate_provider_bucket, AdvisoryIndex, AttestationSigner, AwsS3ObjectPlane, ProviderIdentity,
    ProviderQualificationOptions, S3OperationMetrics,
};

/// An AWS-shaped result plus the immutable repository snapshot that produced it.
#[derive(Debug)]
pub struct Versioned<T> {
    pub output: T,
    pub snapshot: CommitId,
    pub commit: Option<CommitReceipt>,
}

/// Default number of whole-file puts published in one bulk-ingest commit.
pub const DEFAULT_INGEST_FILES_PER_COMMIT: usize = 100;

/// One in-memory whole file for [`Client::ingest_objects`].
#[derive(Clone, Debug)]
pub struct IngestObject {
    pub key: String,
    pub bytes: Vec<u8>,
    pub headers: ObjectHeaders,
    pub metadata: BTreeMap<String, String>,
}

impl IngestObject {
    pub fn new(key: impl Into<String>, bytes: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            bytes: bytes.into(),
            headers: ObjectHeaders::default(),
            metadata: BTreeMap::new(),
        }
    }

    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.headers.content_type = Some(value.into());
        self
    }

    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug, Default)]
pub struct IngestReport {
    pub object_count: usize,
    pub commits: Vec<CommitReceipt>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NodeCachePrewarmReport {
    pub snapshot: CommitId,
    pub object_count: usize,
    pub pages: usize,
}

pub fn supported_input_fields(operation: &str) -> Option<&'static [&'static str]> {
    match operation {
        "put_object" => Some(&[
            "bucket",
            "key",
            "body",
            "cache_control",
            "content_disposition",
            "content_encoding",
            "content_language",
            "content_type",
            "metadata",
            "if_match",
            "if_none_match",
            "content_md5",
            "checksum_sha256",
        ]),
        "get_object" => Some(&[
            "bucket",
            "key",
            "range",
            "version_id",
            "if_match",
            "if_none_match",
            "if_modified_since",
            "if_unmodified_since",
            "checksum_mode",
        ]),
        "head_object" => Some(&[
            "bucket",
            "key",
            "version_id",
            "if_match",
            "if_none_match",
            "if_modified_since",
            "if_unmodified_since",
            "checksum_mode",
        ]),
        "list_objects_v2" => Some(&[
            "bucket",
            "continuation_token",
            "delimiter",
            "max_keys",
            "prefix",
            "start_after",
        ]),
        _ => None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QualifiedClone {
    pub copy: prolly_s3_core::CloneReport,
    pub provider_profile: ProviderProfileId,
    /// Target-side object-plane calls and transferred body bytes, including
    /// target qualification and closure copy. Source-side counters remain on
    /// the source client so callers can aggregate the complete clone cost.
    pub target_s3_metrics: S3OperationMetrics,
}

/// A provider-native physical version of this client's selected branch ref.
/// This is an administrative recovery record, never a logical object VersionId.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalBranchRefVersion {
    pub version_id: String,
    pub target: CommitId,
    pub generation: u64,
    pub operation: OperationId,
    pub writer: String,
    pub updated_at_millis: u64,
    pub physical_last_modified_millis: u64,
    pub tombstone: bool,
}

/// The write discipline applied to one physical repository path family.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PhysicalPathDiscipline {
    /// Content-addressed or otherwise immutable after its first successful write.
    Immutable,
    /// Installed exactly once with conditional create during repository initialization.
    CreateOnce,
    /// Updated through provider-token compare-and-exchange and bounded by the
    /// repository mutable-control version-retention policy.
    MutableCas,
    /// Temporary qualification data that is removed after the probe completes.
    EphemeralProbe,
}

/// One stable family in the physical S3 repository namespace.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PhysicalPathFamily {
    /// Path pattern relative to [`PhysicalRepositoryLayout::repository_prefix`].
    pub relative_pattern: &'static str,
    pub discipline: PhysicalPathDiscipline,
    /// Whether portable clone copies this family to a new provider.
    pub portable_clone: bool,
    /// Whether immutable objects in this family can appear in an exact-version GC plan.
    pub gc_managed: bool,
}

/// An inspectable, side-effect-free description of the client's physical S3 namespace.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PhysicalRepositoryLayout {
    pub bucket: String,
    pub repository_prefix: String,
    pub families: &'static [PhysicalPathFamily],
}

const PHYSICAL_PATH_FAMILIES: &[PhysicalPathFamily] = &[
    PhysicalPathFamily {
        relative_pattern: "format/v1.cbor",
        discipline: PhysicalPathDiscipline::CreateOnce,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "format/initialization.cbor",
        discipline: PhysicalPathDiscipline::CreateOnce,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "providers/<provider-profile-id>.cbor",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "node-index/latest.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "node-index/v2/head.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "ref-catalog/v2/head.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "commit-graph/v2/head.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "node-index/checkpoints/<generation>-<checkpoint-id>.cbor",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "commits/sha256/<2>/<2>/<commit-id>",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: true,
        gc_managed: true,
    },
    PhysicalPathFamily {
        relative_pattern: "commits/v2/sha256/<2>/<2>/<commit-id>",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: true,
        gc_managed: true,
    },
    PhysicalPathFamily {
        relative_pattern: "publications/v2/sha256/<2>/<2>/<publication-id>",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "payloads/v2/<repository-id-hex>/sha256/<2>/<2>/<content-id>",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "refs/{heads,tags}/<name-hex>",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "refs/v2/heads/<name-hex>",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "reflogs/tags/<name-hex>/<entry-id>",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: true,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "writers/lease.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "authority/v2/{branches,system}/<scope-hex>/lease.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "authority/v2/maintenance/gate.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "retention/pins/<name-hex>",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "probes/<operation-id>/{mutable,listed}",
        discipline: PhysicalPathDiscipline::EphemeralProbe,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "gc/mark-runs/<operation-id-hex>.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "gc/plans/<plan-id>.cbor",
        discipline: PhysicalPathDiscipline::Immutable,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "gc/runs/<plan-id>.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
    PhysicalPathFamily {
        relative_pattern: "gc/v2/epochs/<operation-id-hex>/head.cbor",
        discipline: PhysicalPathDiscipline::MutableCas,
        portable_clone: false,
        gc_managed: false,
    },
];

#[derive(Clone, Debug, Default)]
pub struct ReadOptions {
    pub deadline: Option<Instant>,
}

#[derive(Clone, Debug, Default)]
pub struct WriteOptions {
    pub operation_id: Option<OperationId>,
    pub expected_head: Option<CommitId>,
    pub deadline: Option<Instant>,
}

pub trait TokenSigner: Send + Sync {
    fn sign(&self, payload: &[u8]) -> Result<(String, Vec<u8>)>;
    fn verify(&self, key_id: &str, payload: &[u8], signature: &[u8]) -> Result<()>;
    /// Validates that every scheduled-retirement key remains usable for the
    /// maximum cursor lifetime plus deployment clock skew.
    fn validate_lifecycle(
        &self,
        max_token_lifetime: Duration,
        clock_skew: Duration,
        now_millis: u64,
    ) -> Result<()>;
}

#[derive(Clone)]
pub struct HmacTokenKey {
    key_id: String,
    secret: Option<Vec<u8>>,
    retired_at_millis: Option<u64>,
}

impl HmacTokenKey {
    /// A key retained indefinitely for signing or verification.
    pub fn retained(key_id: impl Into<String>, secret: impl Into<Vec<u8>>) -> Self {
        Self {
            key_id: key_id.into(),
            secret: Some(secret.into()),
            retired_at_millis: None,
        }
    }

    /// A verification key scheduled from the instant it stopped signing.
    pub fn retired(
        key_id: impl Into<String>,
        secret: impl Into<Vec<u8>>,
        retired_at_millis: u64,
    ) -> Self {
        Self {
            key_id: key_id.into(),
            secret: Some(secret.into()),
            retired_at_millis: Some(retired_at_millis),
        }
    }

    /// A retirement-ledger tombstone. Configuration validation rejects this
    /// until every cursor that key could have signed is beyond TTL plus skew.
    pub fn removed(key_id: impl Into<String>, retired_at_millis: u64) -> Self {
        Self {
            key_id: key_id.into(),
            secret: None,
            retired_at_millis: Some(retired_at_millis),
        }
    }
}

/// Shared HMAC key ring for restart-safe, multi-process listing cursors.
pub struct HmacTokenSigner {
    active_key: String,
    keys: HashMap<String, HmacTokenKey>,
}

impl HmacTokenSigner {
    pub fn new(
        active_key: impl Into<String>,
        keys: impl IntoIterator<Item = (String, Vec<u8>)>,
    ) -> Result<Self> {
        let active_key = active_key.into();
        Self::managed(
            active_key,
            keys.into_iter()
                .map(|(key_id, secret)| HmacTokenKey::retained(key_id, secret)),
        )
    }

    pub fn managed(
        active_key: impl Into<String>,
        keys: impl IntoIterator<Item = HmacTokenKey>,
    ) -> Result<Self> {
        let active_key = active_key.into();
        let mut collected = HashMap::new();
        for key in keys {
            validate_token_key_id(&key.key_id)?;
            if collected.insert(key.key_id.clone(), key).is_some() {
                return Err(invalid("cursor key ring contains a duplicate key ID"));
            }
        }
        let keys = collected;
        if !keys.contains_key(&active_key) {
            return Err(invalid(
                "active cursor signing key is absent from the key ring",
            ));
        }
        if keys
            .values()
            .filter_map(|key| key.secret.as_ref())
            .any(|key| key.len() < 32)
        {
            return Err(invalid(
                "cursor signing keys must contain at least 32 bytes",
            ));
        }
        if keys
            .get(&active_key)
            .is_some_and(|key| key.secret.is_none() || key.retired_at_millis.is_some())
        {
            return Err(invalid(
                "active cursor signing key must be retained and contain secret material",
            ));
        }
        if keys
            .values()
            .any(|key| key.secret.is_none() && key.retired_at_millis.is_none())
        {
            return Err(invalid(
                "removed cursor keys require a retirement timestamp",
            ));
        }
        Ok(Self { active_key, keys })
    }

    pub fn single(key_id: impl Into<String>, key: impl Into<Vec<u8>>) -> Result<Self> {
        let key_id = key_id.into();
        Self::new(key_id.clone(), [(key_id, key.into())])
    }
}

impl TokenSigner for HmacTokenSigner {
    fn sign(&self, payload: &[u8]) -> Result<(String, Vec<u8>)> {
        let key = self
            .keys
            .get(&self.active_key)
            .and_then(|key| key.secret.as_ref())
            .expect("active key validated");
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|_| invalid("invalid cursor signing key"))?;
        mac.update(payload);
        Ok((
            self.active_key.clone(),
            mac.finalize().into_bytes().to_vec(),
        ))
    }

    fn verify(&self, key_id: &str, payload: &[u8], signature: &[u8]) -> Result<()> {
        let key = self
            .keys
            .get(key_id)
            .and_then(|key| key.secret.as_ref())
            .ok_or_else(|| {
                Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "unknown cursor signing key",
                )
            })?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|_| invalid("invalid cursor verification key"))?;
        mac.update(payload);
        mac.verify_slice(signature).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "cursor signature mismatch",
            )
        })
    }

    fn validate_lifecycle(
        &self,
        max_token_lifetime: Duration,
        clock_skew: Duration,
        now_millis: u64,
    ) -> Result<()> {
        let lifetime = duration_millis(max_token_lifetime, "cursor TTL")?;
        let skew = duration_millis(clock_skew, "cursor clock skew")?;
        for key in self.keys.values() {
            let Some(retired_at) = key.retired_at_millis else {
                continue;
            };
            let required_until = retired_at
                .checked_add(lifetime)
                .and_then(|value| value.checked_add(skew))
                .ok_or_else(|| invalid("cursor key retirement horizon overflow"))?;
            if now_millis <= required_until && key.secret.is_none() {
                return Err(invalid(format!(
                    "cursor verification key {:?} was removed before TTL plus clock skew elapsed",
                    key.key_id
                )));
            }
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize)]
struct ListingCursor {
    version: u8,
    kind: String,
    repository: prolly_s3_core::RepositoryId,
    bucket: String,
    branch: String,
    snapshot: CommitId,
    prefix: String,
    delimiter: Option<String>,
    after: Vec<u8>,
    skip_prefix: Option<Vec<u8>>,
    expires_at_millis: u64,
}

#[derive(Serialize, Deserialize)]
struct SignedCursor {
    key_id: String,
    payload: Vec<u8>,
    signature: Vec<u8>,
}

#[derive(Clone)]
pub struct Client {
    repository: Arc<Repository<AwsS3ObjectPlane>>,
    bucket: String,
    branch: String,
    token_signer: Option<Arc<dyn TokenSigner>>,
    cursor_ttl: Duration,
    advisory_index: Option<Arc<dyn AdvisoryIndex>>,
    repository_prefix: String,
    provider_identity: ProviderIdentity,
    attestation_signer: Arc<dyn AttestationSigner>,
    provider_attestation: Arc<RwLock<ProviderAttestationV1>>,
    physical_multipart_parts: Arc<RwLock<BTreeMap<(String, u32), PhysicalMultipartPartResult>>>,
    physical_multipart_sessions: Arc<RwLock<BTreeMap<String, PhysicalMultipartSessionV1>>>,
    writer_lease_maintenance: Option<Arc<WriterLeaseMaintenance>>,
    node_index_maintenance: Option<Arc<prolly_s3_core::NodeIndexMaintenance>>,
    max_staged_batch_bytes: usize,
}

#[derive(Default)]
pub struct ClientBuilder {
    aws_client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
    repository_prefix: Option<String>,
    default_branch: Option<String>,
    writer: Option<String>,
    writer_lease_duration: Option<Duration>,
    read_only: bool,
    max_parallel_payload_writes: Option<usize>,
    max_cached_commits: Option<usize>,
    max_cached_branches: Option<usize>,
    max_cached_node_pack_bytes: Option<usize>,
    max_cached_node_locations: Option<usize>,
    max_cached_node_bytes: Option<usize>,
    node_cache: Option<Arc<dyn prolly_s3_core::NodeCache>>,
    branch_ref_compaction_interval: Option<u64>,
    branch_ref_versions_to_retain: Option<usize>,
    mutable_control_versions_to_retain: Option<usize>,
    node_index_maintenance_interval: Option<Duration>,
    node_index_maintenance_batch: Option<usize>,
    max_staged_batch_bytes: Option<usize>,
    gc_delete_rate_limit_per_second: Option<u32>,
    token_signer: Option<Arc<dyn TokenSigner>>,
    cursor_ttl: Option<Duration>,
    cursor_clock_skew: Option<Duration>,
    advisory_index: Option<Arc<dyn AdvisoryIndex>>,
    provider_identity: Option<ProviderIdentity>,
    attestation_signer: Option<Arc<dyn AttestationSigner>>,
    provider_attestation: Option<ProviderProfileId>,
    qualification_options: Option<ProviderQualificationOptions>,
}

impl Client {
    pub fn builder() -> ClientBuilder {
        ClientBuilder::default()
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }
    pub fn branch(&self) -> &str {
        &self.branch
    }
    pub fn repository_id(&self) -> prolly_s3_core::RepositoryId {
        self.repository.repository_id()
    }

    /// Returns the physical S3 path contract without issuing any provider request.
    pub fn physical_layout(&self) -> PhysicalRepositoryLayout {
        PhysicalRepositoryLayout {
            bucket: self.bucket.clone(),
            repository_prefix: self.repository_prefix.clone(),
            families: PHYSICAL_PATH_FAMILIES,
        }
    }

    /// Returns object-plane SDK calls and body bytes accumulated by this client.
    /// Smithy-internal HTTP retry attempts require provider or interceptor telemetry.
    pub fn s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().metrics()
    }

    /// Starts a new measurement interval and returns the preceding counters.
    pub fn reset_s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().reset_metrics()
    }

    /// Returns hot-branch publication queue and wait counters.
    pub fn performance_snapshot(&self) -> prolly_s3_core::RepositoryPerformanceSnapshot {
        self.repository.performance_snapshot()
    }

    /// Compact obsolete physical versions of the selected branch-ref object.
    /// Logical commits, history, and object versions are not removed.
    pub async fn compact_branch_ref_versions(
        &self,
    ) -> Result<prolly_s3_core::RefVersionCompactionReport> {
        self.ensure_provider_qualified()?;
        self.repository
            .compact_branch_ref_versions(&self.branch)
            .await
    }

    /// Run one bounded node-index maintenance step immediately. Normal
    /// writable clients also run this work in the background.
    pub async fn advance_node_index(
        &self,
        max_commit_objects: usize,
    ) -> Result<prolly_s3_core::NodeIndexAdvanceReport> {
        self.ensure_provider_qualified()?;
        self.repository
            .advance_node_index_v2(max_commit_objects)
            .await
    }

    /// Run one bounded maintenance page for every rebuildable scale index.
    pub async fn advance_scale_indexes(
        &self,
        max_objects: usize,
    ) -> Result<(
        prolly_s3_core::NodeIndexAdvanceReport,
        prolly_s3_core::RefCatalogAdvanceReport,
        prolly_s3_core::CommitGraphAdvanceReport,
    )> {
        self.ensure_provider_qualified()?;
        let nodes = self.repository.advance_node_index_v2(max_objects).await?;
        let refs = self.repository.advance_ref_catalog_v2(max_objects).await?;
        let graph = self.repository.advance_commit_graph_v2(max_objects).await?;
        Ok((nodes, refs, graph))
    }

    /// Perform an operator-authorized writer handoff after the previous
    /// process and credentials have been independently stopped or revoked.
    /// Open this client read-only and ensure no derived branch/snapshot clients
    /// are alive before calling.
    pub async fn takeover_writer(
        &mut self,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        self.ensure_provider_qualified()?;
        let generation = {
            let repository = Arc::get_mut(&mut self.repository).ok_or_else(|| {
                invalid("writer takeover requires an unshared read-only client handle")
            })?;
            repository
                .takeover_physical_writer(expected_writer, expected_generation, handoff_evidence)
                .await?
        };
        self.writer_lease_maintenance =
            Some(Arc::new(self.repository.start_writer_lease_maintenance()?));
        self.node_index_maintenance = Some(Arc::new(
            self.repository
                .start_node_index_maintenance(Duration::from_secs(60), 1_000)?,
        ));
        Ok(generation)
    }

    pub fn on_branch(&self, branch: impl Into<String>) -> Result<Self> {
        let branch = branch.into();
        validate_branch_name(&branch)?;
        Ok(Self {
            repository: self.repository.clone(),
            bucket: self.bucket.clone(),
            branch,
            token_signer: self.token_signer.clone(),
            cursor_ttl: self.cursor_ttl,
            advisory_index: self.advisory_index.clone(),
            repository_prefix: self.repository_prefix.clone(),
            provider_identity: self.provider_identity.clone(),
            attestation_signer: self.attestation_signer.clone(),
            provider_attestation: self.provider_attestation.clone(),
            physical_multipart_parts: self.physical_multipart_parts.clone(),
            physical_multipart_sessions: self.physical_multipart_sessions.clone(),
            writer_lease_maintenance: self.writer_lease_maintenance.clone(),
            node_index_maintenance: self.node_index_maintenance.clone(),
            max_staged_batch_bytes: self.max_staged_batch_bytes,
        })
    }

    pub async fn head_commit(&self) -> Result<CommitId> {
        self.ensure_provider_qualified()?;
        self.repository.head(&self.branch).await
    }
    pub async fn log(
        &self,
        limit: usize,
    ) -> Result<Vec<(CommitId, prolly_s3_core::BucketCommitV1)>> {
        self.ensure_provider_qualified()?;
        self.repository.log(&self.branch, limit).await
    }
    pub async fn log_page(
        &self,
        start: CommitId,
        after: Option<CommitId>,
        limit: usize,
    ) -> Result<Vec<(CommitId, prolly_s3_core::BucketCommitV1)>> {
        self.ensure_provider_qualified()?;
        self.repository.log_at(start, after, limit).await
    }
    pub async fn log_bounded(
        &self,
        start: CommitId,
        cursor: Option<&prolly_s3_core::HistoryCursor>,
        limit: usize,
        budget: prolly_s3_core::TraversalBudget,
    ) -> Result<prolly_s3_core::CommitPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .log_page_bounded(start, cursor, limit, budget)
            .await
    }
    pub async fn first_parent_ancestor_bounded(
        &self,
        start: CommitId,
        distance: u64,
        cursor: Option<&prolly_s3_core::FirstParentCursor>,
        max_reads: usize,
    ) -> Result<prolly_s3_core::FirstParentPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .first_parent_ancestor_bounded(start, distance, cursor, max_reads)
            .await
    }
    pub async fn create_branch(
        &self,
        name: impl AsRef<str>,
        from: Option<CommitId>,
    ) -> Result<prolly_s3_core::BranchHead> {
        self.ensure_provider_qualified()?;
        let from = match from {
            Some(value) => value,
            None => self.head_commit().await?,
        };
        self.repository.create_branch(name.as_ref(), from).await
    }
    pub async fn delete_branch(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.repository.delete_branch(name.as_ref(), expected).await
    }
    pub async fn list_branches(&self) -> Result<Vec<prolly_s3_core::BranchHead>> {
        self.ensure_provider_qualified()?;
        self.repository.list_branches().await
    }
    pub async fn list_branches_page(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<prolly_s3_core::BranchPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_branches_page(continuation, limit)
            .await
    }
    pub async fn list_branch_catalog_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<prolly_s3_core::CatalogBranchPage> {
        self.ensure_provider_qualified()?;
        self.repository.list_branch_catalog_page(after, limit).await
    }
    pub async fn rebuild_advisory_index(&self) -> Result<crate::AdvisoryRebuildReport> {
        self.ensure_provider_qualified()?;
        let index = self.advisory_index.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::MissingCapability,
                "no advisory index is configured",
            )
        })?;
        let heads = self
            .repository
            .list_branches()
            .await?
            .into_iter()
            .map(|branch| (branch.name, branch.target))
            .collect::<Vec<_>>();
        index.rebuild_heads(self.repository_id(), &heads).await
    }
    pub async fn create_tag(
        &self,
        name: impl AsRef<str>,
        target: CommitId,
    ) -> Result<prolly_s3_core::Tag> {
        self.ensure_provider_qualified()?;
        self.repository.create_tag(name.as_ref(), target).await
    }
    pub async fn list_tags(&self) -> Result<Vec<prolly_s3_core::Tag>> {
        self.ensure_provider_qualified()?;
        self.repository.list_tags().await
    }
    pub async fn list_tags_page(
        &self,
        continuation: Option<String>,
        limit: usize,
    ) -> Result<prolly_s3_core::TagPage> {
        self.ensure_provider_qualified()?;
        self.repository.list_tags_page(continuation, limit).await
    }
    pub async fn list_tag_catalog_page(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<prolly_s3_core::CatalogTagPage> {
        self.ensure_provider_qualified()?;
        self.repository.list_tag_catalog_page(after, limit).await
    }
    pub async fn delete_tag(&self, name: impl AsRef<str>, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.repository.delete_tag(name.as_ref(), expected).await
    }
    pub async fn list_tag_reflog(
        &self,
        tag: impl AsRef<str>,
    ) -> Result<Vec<(prolly_s3_core::ReflogEntryId, prolly_s3_core::ReflogEntryV1)>> {
        self.ensure_provider_qualified()?;
        self.repository.list_tag_reflog(tag.as_ref()).await
    }
    pub async fn recover_tag(
        &self,
        tag: impl AsRef<str>,
        reflog: prolly_s3_core::ReflogEntryId,
        expected_target: CommitId,
        reason: &str,
    ) -> Result<prolly_s3_core::Tag> {
        self.ensure_provider_qualified()?;
        self.repository
            .recover_tag(tag.as_ref(), reflog, expected_target, reason)
            .await
    }
    pub fn begin_commit(&self) -> CommitBuilder {
        CommitBuilder {
            client: self.clone(),
            message: "atomic bucket commit".to_string(),
            expires_after: Duration::from_secs(60 * 60),
        }
    }

    /// Publish whole files in bounded atomic commits instead of creating one
    /// hot-branch CAS and commit envelope per file.
    pub async fn ingest_objects<I>(&self, objects: I) -> Result<IngestReport>
    where
        I: IntoIterator<Item = IngestObject>,
    {
        self.ingest_objects_with_limit(objects, DEFAULT_INGEST_FILES_PER_COMMIT)
            .await
    }

    pub async fn ingest_objects_with_limit<I>(
        &self,
        objects: I,
        max_files_per_commit: usize,
    ) -> Result<IngestReport>
    where
        I: IntoIterator<Item = IngestObject>,
    {
        self.ensure_provider_qualified()?;
        if max_files_per_commit == 0
            || max_files_per_commit
                > self
                    .repository
                    .format()
                    .canonical_limits
                    .max_mutations_per_commit as usize
        {
            return Err(invalid(
                "max_files_per_commit must fit the repository mutation limit",
            ));
        }

        let mut report = IngestReport::default();
        let mut chunk = Vec::with_capacity(max_files_per_commit);
        let mut chunk_bytes = 0usize;
        for object in objects {
            if object.bytes.len() > self.max_staged_batch_bytes {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    "one ingest object exceeds the configured staged-byte limit; use multipart",
                ));
            }
            let next_bytes = chunk_bytes
                .checked_add(object.bytes.len())
                .ok_or_else(|| invalid("ingest byte accounting overflow"))?;
            if !chunk.is_empty()
                && (chunk.len() == max_files_per_commit || next_bytes > self.max_staged_batch_bytes)
            {
                report.commits.push(self.publish_ingest_chunk(chunk).await?);
                chunk = Vec::with_capacity(max_files_per_commit);
                chunk_bytes = 0;
            }
            chunk_bytes = chunk_bytes
                .checked_add(object.bytes.len())
                .ok_or_else(|| invalid("ingest byte accounting overflow"))?;
            report.object_count = report
                .object_count
                .checked_add(1)
                .ok_or_else(|| invalid("ingest object count overflow"))?;
            chunk.push(object);
        }
        if !chunk.is_empty() {
            report.commits.push(self.publish_ingest_chunk(chunk).await?);
        }
        Ok(report)
    }

    async fn publish_ingest_chunk(&self, objects: Vec<IngestObject>) -> Result<CommitReceipt> {
        let batch = self
            .repository
            .begin_physical_batch(&self.branch, "bulk ingest", 60 * 60 * 1_000)
            .await?;
        let mutations = objects
            .into_iter()
            .map(|object| prolly_s3_core::PhysicalBatchMutationV1::Put {
                key: object.key.into_bytes(),
                bytes: object.bytes,
                headers: object.headers,
                user_metadata: object.metadata,
            })
            .collect();
        let receipt = self
            .repository
            .publish_physical_batch(batch, mutations)
            .await?;
        self.record_advisory(&receipt).await;
        Ok(receipt)
    }

    /// Traverse one immutable object snapshot and populate the configured
    /// verified node cache without downloading object payloads.
    pub async fn prewarm_node_cache(
        &self,
        snapshot: CommitId,
        prefix: &[u8],
        page_size: usize,
    ) -> Result<NodeCachePrewarmReport> {
        self.ensure_provider_qualified()?;
        if page_size == 0 {
            return Err(invalid("prewarm page_size must be greater than zero"));
        }
        let mut after = None;
        let mut object_count = 0usize;
        let mut pages = 0usize;
        loop {
            let (objects, truncated) = self
                .repository
                .list_objects_at(snapshot, prefix, after.as_deref(), page_size)
                .await?;
            pages = pages
                .checked_add(1)
                .ok_or_else(|| invalid("prewarm page count overflow"))?;
            object_count = object_count
                .checked_add(objects.len())
                .ok_or_else(|| invalid("prewarm object count overflow"))?;
            after = objects.last().map(|object| object.key.clone());
            if !truncated {
                break;
            }
        }
        Ok(NodeCachePrewarmReport {
            snapshot,
            object_count,
            pages,
        })
    }
    pub async fn at(&self, commit: CommitId) -> Result<Snapshot> {
        self.ensure_provider_qualified()?;
        self.repository.commit(commit).await?;
        Ok(Snapshot {
            client: self.clone(),
            commit,
        })
    }
    pub async fn diff(
        &self,
        from: CommitId,
        to: CommitId,
    ) -> Result<Vec<prolly_s3_core::ObjectDiff>> {
        self.ensure_provider_qualified()?;
        self.repository.diff(from, to).await
    }
    pub async fn diff_page(
        &self,
        from: CommitId,
        to: CommitId,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<prolly_s3_core::ObjectDiff>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository.diff_at(from, to, after, limit).await
    }
    pub async fn diff_bounded(
        &self,
        from: CommitId,
        to: CommitId,
        cursor: Option<&prolly_s3_core::ObjectDiffCursor>,
        limit: usize,
    ) -> Result<prolly_s3_core::ObjectDiffPage> {
        self.ensure_provider_qualified()?;
        self.repository
            .diff_page_bounded(from, to, cursor, limit)
            .await
    }
    pub async fn merge_bases(&self, left: CommitId, right: CommitId) -> Result<Vec<CommitId>> {
        self.ensure_provider_qualified()?;
        self.repository.merge_bases(left, right).await
    }
    pub async fn plan_merge(
        &self,
        source: CommitId,
        selected_base: Option<CommitId>,
        policy: prolly_s3_core::MergePolicy,
    ) -> Result<prolly_s3_core::MergePlan> {
        self.ensure_provider_qualified()?;
        self.repository
            .plan_merge(&self.branch, source, selected_base, policy)
            .await
    }
    pub async fn merge(
        &self,
        source: CommitId,
        selected_base: Option<CommitId>,
        policy: prolly_s3_core::MergePolicy,
        operation: Option<OperationId>,
        message: Option<String>,
    ) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .merge(
                &self.branch,
                source,
                selected_base,
                policy,
                operation,
                message,
            )
            .await
    }
    pub async fn restore(
        &self,
        source: CommitId,
        expected_head: CommitId,
        operation: Option<OperationId>,
        message: Option<String>,
    ) -> Result<CommitReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .restore(&self.branch, source, expected_head, operation, message)
            .await
    }
    pub async fn reset_branch(
        &self,
        to: CommitId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<prolly_s3_core::RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .reset_branch(&self.branch, to, expected_head, reason)
            .await
    }
    pub async fn recover_branch(
        &self,
        reflog: prolly_s3_core::ReflogEntryId,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<prolly_s3_core::RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        self.repository
            .recover_branch(&self.branch, reflog, expected_head, reason)
            .await
    }

    /// Lists hash/codec-validated physical S3 versions of the selected branch ref.
    pub async fn list_physical_branch_ref_versions(&self) -> Result<Vec<PhysicalBranchRefVersion>> {
        self.ensure_provider_qualified()?;
        self.ensure_physical_version_recovery_supported()?;
        let path = self.branch_ref_path()?;
        let mut continuation = None;
        let mut versions = Vec::new();
        loop {
            let page = self
                .repository
                .plane()
                .list(ListRequest {
                    prefix: path.as_str().to_string(),
                    continuation,
                    limit: 1_000,
                    include_versions: true,
                })
                .await?;
            for entry in page.entries {
                if entry.path != path || entry.metadata.delete_marker {
                    continue;
                }
                let version_id = entry.metadata.token.version_id.ok_or_else(|| {
                    Error::new(
                        ErrorCode::MissingCapability,
                        "provider omitted a physical version ID while listing a versioned ref",
                    )
                })?;
                let stored = self
                    .repository
                    .plane()
                    .get(GetRequest {
                        path: path.clone(),
                        range: None,
                        physical_version: Some(PhysicalVersion::Versioned {
                            version_id: version_id.clone(),
                        }),
                    })
                    .await?
                    .ok_or_else(|| {
                        Error::new(
                            ErrorCode::NoSuchVersion,
                            "listed physical branch-ref version is no longer readable",
                        )
                    })?;
                let value: RefValueV1 = decode_canonical(&stored.bytes)?;
                versions.push(PhysicalBranchRefVersion {
                    version_id,
                    target: value.target,
                    generation: value.generation.0,
                    operation: value.operation,
                    writer: value.writer,
                    updated_at_millis: value.updated_at_millis,
                    physical_last_modified_millis: entry.metadata.last_modified_millis,
                    tombstone: value.tombstone,
                });
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
        versions.sort_by(|left, right| {
            right
                .physical_last_modified_millis
                .cmp(&left.physical_last_modified_millis)
                .then_with(|| right.generation.cmp(&left.generation))
                .then_with(|| left.version_id.cmp(&right.version_id))
        });
        Ok(versions)
    }

    /// Restores a physical ref version through the ordinary audited reset path.
    /// The selected target closure is fully fscked before any new ref write.
    pub async fn recover_branch_from_physical_version(
        &self,
        version_id: &str,
        expected_head: CommitId,
        reason: &str,
    ) -> Result<prolly_s3_core::RefMoveReceipt> {
        self.ensure_provider_qualified()?;
        self.ensure_physical_version_recovery_supported()?;
        if version_id.is_empty() {
            return Err(invalid("physical branch-ref version ID must not be empty"));
        }
        let path = self.branch_ref_path()?;
        let stored = self
            .repository
            .plane()
            .get(GetRequest {
                path,
                range: None,
                physical_version: Some(PhysicalVersion::Versioned {
                    version_id: version_id.to_string(),
                }),
            })
            .await?
            .ok_or_else(|| {
                Error::new(ErrorCode::NoSuchVersion, "physical ref version not found")
            })?;
        let candidate: RefValueV1 = decode_canonical(&stored.bytes)?;
        if candidate.tombstone {
            return Err(Error::new(
                ErrorCode::InvalidRevision,
                "physical ref version is a logical tombstone",
            ));
        }
        self.repository.fsck_commit(candidate.target).await?;
        self.repository
            .reset_branch(&self.branch, candidate.target, expected_head, reason)
            .await
    }
    pub async fn list_reflog(
        &self,
    ) -> Result<Vec<(prolly_s3_core::ReflogEntryId, prolly_s3_core::ReflogEntryV1)>> {
        self.ensure_provider_qualified()?;
        self.repository.list_reflog(&self.branch).await
    }
    pub async fn fsck(&self) -> Result<prolly_s3_core::FsckReport> {
        self.ensure_provider_qualified()?;
        self.repository.fsck().await
    }
    pub async fn fsck_commit(&self, head: CommitId) -> Result<prolly_s3_core::FsckReport> {
        self.ensure_provider_qualified()?;
        self.repository.fsck_commit(head).await
    }
    pub async fn repair_missing_from(
        &self,
        source: &Client,
    ) -> Result<prolly_s3_core::RepairReport> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        self.repository
            .repair_missing_from(source.repository.as_ref(), &source.branch)
            .await
    }
    pub async fn clone_to(
        &self,
        target_aws_client: aws_sdk_s3::Client,
        target_bucket: impl Into<String>,
        target_repository_prefix: impl AsRef<str>,
        target_identity: ProviderIdentity,
        qualification: ProviderQualificationOptions,
    ) -> Result<QualifiedClone> {
        self.ensure_provider_qualified()?;
        let target = Arc::new(AwsS3ObjectPlane::new(
            target_aws_client,
            target_bucket.into(),
        ));
        let target_repository_prefix = target_repository_prefix.as_ref();
        let attestation = qualify_and_store(
            target.clone(),
            target_repository_prefix,
            &target_identity,
            self.attestation_signer.as_ref(),
            &qualification,
        )
        .await?;
        let copy = self
            .repository
            .clone_to(target.clone(), target_repository_prefix)
            .await?;
        Ok(QualifiedClone {
            copy,
            provider_profile: attestation.id,
            target_s3_metrics: target.metrics(),
        })
    }
    pub async fn fetch_from(&self, source: &Client) -> Result<prolly_s3_core::SyncReport> {
        self.ensure_provider_qualified()?;
        source.ensure_provider_qualified()?;
        self.repository
            .fetch_from(source.repository.as_ref(), &source.branch)
            .await
    }
    pub async fn push_to(
        &self,
        destination: &Client,
        expected_destination: CommitId,
        reason: &str,
    ) -> Result<prolly_s3_core::SyncReport> {
        self.ensure_provider_qualified()?;
        destination.ensure_provider_qualified()?;
        self.repository
            .push_to(
                destination.repository.as_ref(),
                &self.branch,
                &destination.branch,
                expected_destination,
                reason,
            )
            .await
    }
    pub async fn plan_gc(
        &self,
        grace: Duration,
        max_candidates: usize,
    ) -> Result<prolly_s3_core::GcDryRun> {
        self.ensure_provider_qualified()?;
        let grace_millis = u64::try_from(grace.as_millis())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "GC grace exceeds u64 millis"))?;
        self.repository.plan_gc(grace_millis, max_candidates).await
    }
    /// Starts the scalable GC workflow. Prefer this over `plan_gc` when the
    /// repository can exceed one process's memory.
    pub async fn start_gc_epoch(&self, grace: Duration) -> Result<prolly_s3_core::GcEpochV2> {
        self.ensure_provider_qualified()?;
        let grace_millis = u64::try_from(grace.as_millis())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "GC grace exceeds u64 millis"))?;
        self.repository.start_gc_epoch_v2(grace_millis).await
    }
    pub async fn advance_gc_epoch(
        &self,
        epoch: OperationId,
        max_items: usize,
    ) -> Result<prolly_s3_core::GcEpochStepReport> {
        self.ensure_provider_qualified()?;
        self.repository.advance_gc_epoch_v2(epoch, max_items).await
    }
    pub async fn sweep_gc_epoch(
        &self,
        epoch: OperationId,
        max_candidates: usize,
    ) -> Result<prolly_s3_core::GcEpochStepReport> {
        self.ensure_provider_qualified()?;
        self.repository
            .sweep_gc_epoch_v2(epoch, max_candidates)
            .await
    }
    pub async fn gc_epoch(&self, epoch: OperationId) -> Result<prolly_s3_core::GcEpochV2> {
        self.ensure_provider_qualified()?;
        self.repository.gc_epoch_v2(epoch).await
    }
    pub async fn plan_gc_resumable(
        &self,
        run: Option<OperationId>,
        grace: Duration,
        max_candidates: usize,
    ) -> Result<prolly_s3_core::GcMarkRunV1> {
        self.ensure_provider_qualified()?;
        let grace_millis = u64::try_from(grace.as_millis())
            .map_err(|_| Error::new(ErrorCode::InvalidLimit, "GC grace exceeds u64 millis"))?;
        self.repository
            .plan_gc_checkpointed(run, grace_millis, max_candidates)
            .await
    }
    pub async fn gc_mark_run(&self, run: OperationId) -> Result<prolly_s3_core::GcMarkRunV1> {
        self.ensure_provider_qualified()?;
        self.repository.gc_mark_run(run).await
    }
    pub async fn load_gc_plan(
        &self,
        plan: prolly_s3_core::GcPlanId,
    ) -> Result<prolly_s3_core::GcPlanV1> {
        self.ensure_provider_qualified()?;
        self.repository.load_gc_plan(plan).await
    }
    pub async fn sweep_gc(
        &self,
        plan: prolly_s3_core::GcPlanId,
    ) -> Result<prolly_s3_core::GcSweepReport> {
        self.ensure_provider_qualified()?;
        self.repository.sweep_gc(plan).await
    }
    pub async fn sweep_gc_batch(
        &self,
        plan: prolly_s3_core::GcPlanId,
        max_candidates: usize,
    ) -> Result<prolly_s3_core::GcSweepReport> {
        self.ensure_provider_qualified()?;
        self.repository.sweep_gc_batch(plan, max_candidates).await
    }
    pub async fn gc_run(&self, plan: prolly_s3_core::GcPlanId) -> Result<prolly_s3_core::GcRunV1> {
        self.ensure_provider_qualified()?;
        self.repository.gc_run(plan).await
    }
    pub async fn create_retention_pin(
        &self,
        name: &str,
        target: CommitId,
        owner: &str,
        reason: &str,
        ttl: Option<Duration>,
    ) -> Result<prolly_s3_core::RetentionPinV1> {
        self.ensure_provider_qualified()?;
        let ttl_millis = ttl
            .map(|value| {
                u64::try_from(value.as_millis())
                    .map_err(|_| Error::new(ErrorCode::InvalidLimit, "pin TTL exceeds u64 millis"))
            })
            .transpose()?;
        self.repository
            .create_retention_pin(name, target, owner, reason, ttl_millis)
            .await
    }
    pub async fn abort_gc_run(
        &self,
        plan: prolly_s3_core::GcPlanId,
        expected_generation: u64,
        reason: &str,
    ) -> Result<prolly_s3_core::GcRunV1> {
        self.ensure_provider_qualified()?;
        self.repository
            .abort_gc_run(plan, expected_generation, reason)
            .await
    }
    pub async fn delete_retention_pin(&self, name: &str, expected: CommitId) -> Result<()> {
        self.ensure_provider_qualified()?;
        self.repository.delete_retention_pin(name, expected).await
    }
    pub async fn list_retention_pins(&self) -> Result<Vec<prolly_s3_core::RetentionPinV1>> {
        self.ensure_provider_qualified()?;
        self.repository.list_retention_pins().await
    }

    pub fn provider_profile(&self) -> Result<ProviderProfileId> {
        self.ensure_provider_qualified()?;
        Ok(self
            .provider_attestation
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "attestation lock poisoned"))?
            .id)
    }

    /// Reloads a matching signed attestation without running write probes.
    pub async fn refresh_capabilities(&self) -> Result<ProviderProfileId> {
        let attestation = load_valid_attestation(
            self.repository.plane(),
            &self.repository_prefix,
            &self.provider_identity,
            self.attestation_signer.as_ref(),
            None,
        )
        .await?;
        validate_physical_capabilities(&attestation)?;
        let id = attestation.id;
        *self
            .provider_attestation
            .write()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "attestation lock poisoned"))? =
            attestation;
        Ok(id)
    }

    /// Reconcile an idempotent mutation after a timeout or canceled response.
    pub async fn reconcile_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<CommitReceipt>> {
        self.ensure_provider_qualified()?;
        self.repository
            .lookup_operation(&self.branch, operation)
            .await
    }

    pub async fn execute_put_object(
        &self,
        input: aws_sdk_s3::operation::put_object::PutObjectInput,
        options: WriteOptions,
    ) -> Result<Versioned<PutObjectOutput>> {
        validate_put_input(&input)?;
        validate_write_options(&options)?;
        PutObjectBuilder {
            client: self.clone(),
            bucket: input.bucket,
            key: input.key,
            body: Some(input.body),
            content_type: input.content_type,
            content_encoding: input.content_encoding,
            content_language: input.content_language,
            content_disposition: input.content_disposition,
            cache_control: input.cache_control,
            metadata: input.metadata,
            if_match: input.if_match,
            if_none_match: input.if_none_match,
            content_md5: input.content_md5,
            checksum_sha256: input.checksum_sha256,
            expected_head: options.expected_head,
            operation_id: options.operation_id,
            deadline: options.deadline,
        }
        .send()
        .await
    }

    pub async fn execute_get_object(
        &self,
        input: aws_sdk_s3::operation::get_object::GetObjectInput,
        options: ReadOptions,
    ) -> Result<GetObjectOutput> {
        validate_get_input(&input)?;
        validate_deadline(options.deadline)?;
        Ok(GetObjectBuilder {
            client: self.clone(),
            snapshot: None,
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            range: input.range,
            if_match: input.if_match,
            if_none_match: input.if_none_match,
            if_modified_since: input.if_modified_since,
            if_unmodified_since: input.if_unmodified_since,
            checksum_mode: input.checksum_mode,
            deadline: options.deadline,
        }
        .send()
        .await?
        .output)
    }

    pub async fn execute_head_object(
        &self,
        input: aws_sdk_s3::operation::head_object::HeadObjectInput,
        options: ReadOptions,
    ) -> Result<HeadObjectOutput> {
        validate_head_input(&input)?;
        validate_deadline(options.deadline)?;
        Ok(HeadObjectBuilder {
            client: self.clone(),
            snapshot: None,
            bucket: input.bucket,
            key: input.key,
            version_id: input.version_id,
            if_match: input.if_match,
            if_none_match: input.if_none_match,
            if_modified_since: input.if_modified_since,
            if_unmodified_since: input.if_unmodified_since,
            checksum_mode: input.checksum_mode,
            deadline: options.deadline,
        }
        .send()
        .await?
        .output)
    }

    pub async fn execute_list_objects_v2(
        &self,
        input: aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Input,
        options: ReadOptions,
    ) -> Result<Versioned<ListObjectsV2Output>> {
        validate_list_input(&input)?;
        validate_deadline(options.deadline)?;
        ListObjectsV2Builder {
            client: self.clone(),
            snapshot: None,
            bucket: input.bucket,
            prefix: input.prefix,
            delimiter: input.delimiter,
            max_keys: input.max_keys,
            continuation_token: input.continuation_token,
            start_after: input.start_after,
            deadline: options.deadline,
        }
        .send()
        .await
    }

    pub fn put_object(&self) -> PutObjectBuilder {
        PutObjectBuilder::new(self.clone())
    }
    pub fn get_object(&self) -> GetObjectBuilder {
        GetObjectBuilder::new(self.clone())
    }
    pub fn head_object(&self) -> HeadObjectBuilder {
        HeadObjectBuilder::new(self.clone())
    }
    pub fn delete_object(&self) -> DeleteObjectBuilder {
        DeleteObjectBuilder::new(self.clone())
    }
    pub fn delete_objects(&self) -> DeleteObjectsBuilder {
        DeleteObjectsBuilder::new(self.clone())
    }
    pub fn copy_object(&self) -> CopyObjectBuilder {
        CopyObjectBuilder::new(self.clone())
    }
    pub fn list_objects_v2(&self) -> ListObjectsV2Builder {
        ListObjectsV2Builder::new(self.clone())
    }
    pub fn list_object_versions(&self) -> ListObjectVersionsBuilder {
        ListObjectVersionsBuilder::new(self.clone())
    }
    pub fn create_multipart_upload(&self) -> CreateMultipartUploadBuilder {
        CreateMultipartUploadBuilder::new(self.clone())
    }
    pub fn list_multipart_uploads(&self) -> ListMultipartUploadsBuilder {
        ListMultipartUploadsBuilder::new(self.clone())
    }
    pub fn upload_part(&self) -> UploadPartBuilder {
        UploadPartBuilder::new(self.clone())
    }
    pub fn upload_part_copy(&self) -> UploadPartCopyBuilder {
        UploadPartCopyBuilder::new(self.clone())
    }
    pub fn list_parts(&self) -> ListPartsBuilder {
        ListPartsBuilder::new(self.clone())
    }
    pub fn complete_multipart_upload(&self) -> CompleteMultipartUploadBuilder {
        CompleteMultipartUploadBuilder::new(self.clone())
    }
    pub fn abort_multipart_upload(&self) -> AbortMultipartUploadBuilder {
        AbortMultipartUploadBuilder::new(self.clone())
    }
    fn validate_bucket(&self, bucket: Option<&str>) -> Result<()> {
        self.ensure_provider_qualified()?;
        let bucket = required(bucket, "bucket")?;
        if bucket != self.bucket {
            return Err(invalid(format!(
                "logical bucket {bucket:?} does not match configured bucket {:?}",
                self.bucket
            )));
        }
        Ok(())
    }

    fn ensure_provider_qualified(&self) -> Result<()> {
        let attestation = self
            .provider_attestation
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "attestation lock poisoned"))?;
        ensure_attestation_current(&attestation)?;
        validate_physical_capabilities(&attestation)
    }

    fn ensure_physical_version_recovery_supported(&self) -> Result<()> {
        let attestation = self
            .provider_attestation
            .read()
            .map_err(|_| Error::new(ErrorCode::InternalInvariant, "attestation lock poisoned"))?;
        ensure_attestation_current(&attestation)?;
        if attestation.body.capabilities.physical_versioning != PhysicalVersioning::Enabled {
            return Err(Error::new(
                ErrorCode::MissingCapability,
                "physical branch-ref recovery requires provider bucket versioning",
            ));
        }
        Ok(())
    }

    fn branch_ref_path(&self) -> Result<ObjectPath> {
        ObjectPath::new(format!(
            "{}/refs/heads/{}",
            self.repository_prefix,
            hex::encode(self.branch.as_bytes())
        ))
    }

    fn encode_cursor(&self, cursor: &ListingCursor) -> Result<String> {
        let signer = self.token_signer.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::MissingCapability,
                "paginated listing requires a shared token signer",
            )
        })?;
        let payload = serde_cbor::to_vec(cursor).map_err(|error| {
            Error::new(
                ErrorCode::InternalInvariant,
                format!("cursor encoding failed: {error}"),
            )
        })?;
        let (key_id, signature) = signer.sign(&payload)?;
        let envelope = serde_cbor::to_vec(&SignedCursor {
            key_id,
            payload,
            signature,
        })
        .map_err(|error| {
            Error::new(
                ErrorCode::InternalInvariant,
                format!("signed cursor encoding failed: {error}"),
            )
        })?;
        Ok(URL_SAFE_NO_PAD.encode(envelope))
    }

    fn decode_cursor(&self, token: &str) -> Result<ListingCursor> {
        let signer = self.token_signer.as_ref().ok_or_else(|| {
            Error::new(
                ErrorCode::MissingCapability,
                "continuation token verification requires a shared token signer",
            )
        })?;
        let envelope = URL_SAFE_NO_PAD.decode(token).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "continuation token is not canonical base64url",
            )
        })?;
        let signed: SignedCursor = serde_cbor::from_slice(&envelope).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "continuation token envelope is malformed",
            )
        })?;
        signer.verify(&signed.key_id, &signed.payload, &signed.signature)?;
        let cursor: ListingCursor = serde_cbor::from_slice(&signed.payload).map_err(|_| {
            Error::new(
                ErrorCode::InvalidContinuationToken,
                "continuation token payload is malformed",
            )
        })?;
        if cursor.version != 1 || cursor.expires_at_millis < now_millis_client()? {
            return Err(Error::new(
                ErrorCode::InvalidContinuationToken,
                "continuation token is expired or unsupported",
            ));
        }
        Ok(cursor)
    }

    async fn record_advisory(&self, receipt: &CommitReceipt) {
        if let Some(index) = &self.advisory_index {
            // Publication is already committed; advisory failure cannot reverse success.
            let _ = index.record_commit(self.repository_id(), receipt).await;
        }
    }
}

#[derive(Clone)]
pub struct Snapshot {
    client: Client,
    commit: CommitId,
}
impl Snapshot {
    pub fn commit_id(&self) -> CommitId {
        self.commit
    }
    pub fn get_object(&self) -> GetObjectBuilder {
        let mut builder = GetObjectBuilder::new(self.client.clone());
        builder.snapshot = Some(self.commit);
        builder
    }
    pub fn head_object(&self) -> HeadObjectBuilder {
        let mut builder = HeadObjectBuilder::new(self.client.clone());
        builder.snapshot = Some(self.commit);
        builder
    }
    pub fn list_objects_v2(&self) -> ListObjectsV2Builder {
        let mut builder = ListObjectsV2Builder::new(self.client.clone());
        builder.snapshot = Some(self.commit);
        builder
    }
    pub fn list_object_versions(&self) -> ListObjectVersionsBuilder {
        let mut builder = ListObjectVersionsBuilder::new(self.client.clone());
        builder.snapshot = Some(self.commit);
        builder
    }
}

impl ClientBuilder {
    pub fn aws_client(mut self, client: aws_sdk_s3::Client) -> Self {
        self.aws_client = Some(client);
        self
    }
    pub fn bucket(mut self, bucket: impl Into<String>) -> Self {
        self.bucket = Some(bucket.into());
        self
    }
    pub fn repository_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.repository_prefix = Some(prefix.into());
        self
    }
    pub fn default_branch(mut self, branch: impl Into<String>) -> Self {
        self.default_branch = Some(branch.into());
        self
    }
    pub fn writer(mut self, writer: impl Into<String>) -> Self {
        self.writer = Some(writer.into());
        self
    }
    pub fn writer_lease_duration(mut self, duration: Duration) -> Self {
        self.writer_lease_duration = Some(duration);
        self
    }
    pub fn read_only(mut self, value: bool) -> Self {
        self.read_only = value;
        self
    }
    pub fn max_parallel_payload_writes(mut self, writes: usize) -> Self {
        self.max_parallel_payload_writes = Some(writes);
        self
    }
    pub fn max_cached_commits(mut self, commits: usize) -> Self {
        self.max_cached_commits = Some(commits);
        self
    }
    pub fn max_cached_branches(mut self, branches: usize) -> Self {
        self.max_cached_branches = Some(branches);
        self
    }
    pub fn max_cached_node_pack_bytes(mut self, bytes: usize) -> Self {
        self.max_cached_node_pack_bytes = Some(bytes);
        self
    }
    pub fn max_cached_node_locations(mut self, locations: usize) -> Self {
        self.max_cached_node_locations = Some(locations);
        self
    }
    pub fn max_cached_node_bytes(mut self, bytes: usize) -> Self {
        self.max_cached_node_bytes = Some(bytes);
        self
    }
    pub fn node_cache(mut self, cache: Arc<dyn prolly_s3_core::NodeCache>) -> Self {
        self.node_cache = Some(cache);
        self
    }
    pub fn branch_ref_compaction(mut self, interval: u64, versions_to_retain: usize) -> Self {
        self.branch_ref_compaction_interval = Some(interval);
        self.branch_ref_versions_to_retain = Some(versions_to_retain);
        self
    }
    pub fn mutable_control_version_retention(mut self, versions_to_retain: usize) -> Self {
        self.mutable_control_versions_to_retain = Some(versions_to_retain);
        self
    }
    pub fn node_index_maintenance(mut self, interval: Duration, batch: usize) -> Self {
        self.node_index_maintenance_interval = Some(interval);
        self.node_index_maintenance_batch = Some(batch);
        self
    }
    /// Fail-closed memory bound for all bodies staged in one atomic commit.
    pub fn max_staged_batch_bytes(mut self, bytes: usize) -> Self {
        self.max_staged_batch_bytes = Some(bytes);
        self
    }
    pub fn gc_delete_rate_limit_per_second(mut self, deletes: u32) -> Self {
        self.gc_delete_rate_limit_per_second = Some(deletes);
        self
    }
    pub fn token_signer(mut self, signer: Arc<dyn TokenSigner>) -> Self {
        self.token_signer = Some(signer);
        self
    }
    pub fn cursor_ttl(mut self, ttl: Duration) -> Self {
        self.cursor_ttl = Some(ttl);
        self
    }
    pub fn cursor_clock_skew(mut self, skew: Duration) -> Self {
        self.cursor_clock_skew = Some(skew);
        self
    }
    pub fn advisory_index(mut self, index: Arc<dyn AdvisoryIndex>) -> Self {
        self.advisory_index = Some(index);
        self
    }
    pub fn provider_identity(mut self, identity: ProviderIdentity) -> Self {
        self.provider_identity = Some(identity);
        self
    }
    pub fn attestation_signer(mut self, signer: Arc<dyn AttestationSigner>) -> Self {
        self.attestation_signer = Some(signer);
        self
    }
    pub fn provider_attestation(mut self, id: ProviderProfileId) -> Self {
        self.provider_attestation = Some(id);
        self
    }
    pub fn provider_attestation_validity(mut self, validity: Duration) -> Self {
        self.qualification_options = Some(ProviderQualificationOptions { validity });
        self
    }

    /// Runs the isolated behavioral probe and persists a signed attestation.
    pub async fn qualify_provider(self) -> Result<ProviderAttestationV1> {
        let aws = self
            .aws_client
            .ok_or_else(|| invalid("aws_client is required"))?;
        let bucket = self.bucket.ok_or_else(|| invalid("bucket is required"))?;
        let prefix = self
            .repository_prefix
            .unwrap_or_else(|| RepositoryOptions::default().repository_prefix);
        let identity = self
            .provider_identity
            .ok_or_else(|| invalid("provider_identity is required"))?;
        validate_provider_bucket(&identity, &bucket)?;
        let signer = self
            .attestation_signer
            .ok_or_else(|| invalid("attestation_signer is required"))?;
        qualify_and_store(
            Arc::new(AwsS3ObjectPlane::new(aws, bucket)),
            &prefix,
            &identity,
            signer.as_ref(),
            &self.qualification_options.unwrap_or_default(),
        )
        .await
    }

    pub async fn initialize(self) -> Result<Client> {
        self.finish(true).await
    }
    pub async fn open(self) -> Result<Client> {
        self.finish(false).await
    }

    async fn finish(self, initialize: bool) -> Result<Client> {
        if initialize && self.read_only {
            return Err(invalid(
                "repository initialization requires a writable client",
            ));
        }
        let cursor_ttl = self.cursor_ttl.unwrap_or(Duration::from_secs(15 * 60));
        let cursor_clock_skew = self
            .cursor_clock_skew
            .unwrap_or(Duration::from_secs(5 * 60));
        let max_staged_batch_bytes = self.max_staged_batch_bytes.unwrap_or(256 * 1024 * 1024);
        if max_staged_batch_bytes == 0 {
            return Err(invalid("staged batch byte limit must be greater than zero"));
        }
        if cursor_ttl.is_zero() || cursor_ttl > Duration::from_secs(24 * 60 * 60) {
            return Err(invalid(
                "cursor TTL must be greater than zero and at most 24 hours",
            ));
        }
        if cursor_clock_skew > Duration::from_secs(15 * 60) {
            return Err(invalid("cursor clock skew must be at most 15 minutes"));
        }
        if let Some(signer) = &self.token_signer {
            signer.validate_lifecycle(cursor_ttl, cursor_clock_skew, now_millis_client()?)?;
        }
        let aws = self
            .aws_client
            .ok_or_else(|| invalid("aws_client is required"))?;
        let bucket = self.bucket.ok_or_else(|| invalid("bucket is required"))?;
        if bucket.is_empty() {
            return Err(invalid("bucket must not be empty"));
        }
        let mut options = RepositoryOptions::default();
        if let Some(value) = self.repository_prefix {
            options.repository_prefix = value;
        }
        if let Some(value) = self.default_branch {
            options.default_branch = value;
        }
        if let Some(value) = self.writer {
            options.writer = value;
        }
        if let Some(value) = self.writer_lease_duration {
            options.writer_lease_millis = u64::try_from(value.as_millis())
                .map_err(|_| invalid("writer lease duration exceeds u64 milliseconds"))?;
        }
        options.read_only = self.read_only;
        if let Some(value) = self.max_parallel_payload_writes {
            options.max_parallel_payload_writes = value;
        }
        if let Some(value) = self.max_cached_commits {
            options.max_cached_commits = value;
        }
        if let Some(value) = self.max_cached_branches {
            options.max_cached_branches = value;
        }
        if let Some(value) = self.max_cached_node_pack_bytes {
            options.max_cached_node_pack_bytes = value;
        }
        if let Some(value) = self.max_cached_node_locations {
            options.max_cached_node_locations = value;
        }
        if let Some(value) = self.max_cached_node_bytes {
            options.max_cached_node_bytes = value;
        }
        options.node_cache = self.node_cache;
        if let Some(value) = self.branch_ref_compaction_interval {
            options.branch_ref_compaction_interval = value;
        }
        if let Some(value) = self.branch_ref_versions_to_retain {
            options.branch_ref_versions_to_retain = value;
        }
        if let Some(value) = self.mutable_control_versions_to_retain {
            options.mutable_control_versions_to_retain = value;
        }
        if let Some(value) = self.gc_delete_rate_limit_per_second {
            options.gc_delete_rate_limit_per_second = value;
        }
        let maintain_physical_writer = !options.read_only;
        let branch = options.default_branch.clone();
        let plane = Arc::new(AwsS3ObjectPlane::new(aws, bucket.clone()));
        let provider_identity = self
            .provider_identity
            .ok_or_else(|| invalid("provider_identity is required"))?;
        validate_provider_bucket(&provider_identity, &bucket)?;
        let attestation_signer = self
            .attestation_signer
            .ok_or_else(|| invalid("attestation_signer is required"))?;
        let selected_attestation = self.provider_attestation;
        let qualification_options = self.qualification_options.unwrap_or_default();
        let repository_prefix = options.repository_prefix.clone();
        let (repository, attestation) = if initialize {
            let attestation = match load_valid_attestation(
                plane.clone(),
                &repository_prefix,
                &provider_identity,
                attestation_signer.as_ref(),
                selected_attestation,
            )
            .await
            {
                Ok(value) => value,
                Err(error) if error.code == ErrorCode::ProviderNotQualified => {
                    qualify_and_store(
                        plane.clone(),
                        &repository_prefix,
                        &provider_identity,
                        attestation_signer.as_ref(),
                        &qualification_options,
                    )
                    .await?
                }
                Err(error) => return Err(error),
            };
            validate_physical_capabilities(&attestation)?;
            (Repository::initialize(plane, options).await?, attestation)
        } else {
            let repository = Repository::open(plane.clone(), options).await?;
            let attestation = load_valid_attestation(
                plane,
                &repository_prefix,
                &provider_identity,
                attestation_signer.as_ref(),
                selected_attestation,
            )
            .await?;
            validate_physical_capabilities(&attestation)?;
            (repository, attestation)
        };
        let repository = Arc::new(repository);
        let writer_lease_maintenance = maintain_physical_writer
            .then(|| repository.start_writer_lease_maintenance())
            .transpose()?
            .map(Arc::new);
        let node_index_maintenance = maintain_physical_writer
            .then(|| {
                repository.start_node_index_maintenance(
                    self.node_index_maintenance_interval
                        .unwrap_or(Duration::from_secs(60)),
                    self.node_index_maintenance_batch.unwrap_or(1_000),
                )
            })
            .transpose()?
            .map(Arc::new);
        Ok(Client {
            repository,
            bucket,
            branch,
            token_signer: self.token_signer,
            cursor_ttl,
            advisory_index: self.advisory_index,
            repository_prefix,
            provider_identity,
            attestation_signer,
            provider_attestation: Arc::new(RwLock::new(attestation)),
            physical_multipart_parts: Arc::new(RwLock::new(BTreeMap::new())),
            physical_multipart_sessions: Arc::new(RwLock::new(BTreeMap::new())),
            writer_lease_maintenance,
            node_index_maintenance,
            max_staged_batch_bytes,
        })
    }
}

pub struct CommitBuilder {
    client: Client,
    message: String,
    expires_after: Duration,
}
impl CommitBuilder {
    pub fn message(mut self, value: impl Into<String>) -> Self {
        self.message = value.into();
        self
    }
    pub fn expires_after(mut self, value: Duration) -> Self {
        self.expires_after = value;
        self
    }
    pub async fn start(self) -> Result<CommitSession> {
        self.client.ensure_provider_qualified()?;
        let millis = u64::try_from(self.expires_after.as_millis())
            .map_err(|_| invalid("batch expiry exceeds u64 milliseconds"))?;
        let manifest = self
            .client
            .repository
            .begin_physical_batch(&self.client.branch, self.message, millis)
            .await?;
        Ok(CommitSession {
            client: self.client,
            manifest,
            physical_mutations: BTreeMap::new(),
            staged_body_bytes: 0,
        })
    }
}

pub struct CommitSession {
    client: Client,
    manifest: PhysicalBatchV1,
    physical_mutations: BTreeMap<Vec<u8>, prolly_s3_core::PhysicalBatchMutationV1>,
    staged_body_bytes: usize,
}
impl CommitSession {
    pub fn id(&self) -> BatchId {
        self.manifest.id
    }
    pub fn base_commit(&self) -> CommitId {
        self.manifest.base_commit
    }
    pub fn put_object(&mut self) -> StagedPutObjectBuilder<'_> {
        StagedPutObjectBuilder {
            session: self,
            bucket: None,
            key: None,
            body: None,
            content_type: None,
            metadata: None,
        }
    }
    pub fn delete_object(&mut self) -> StagedDeleteObjectBuilder<'_> {
        StagedDeleteObjectBuilder {
            session: self,
            bucket: None,
            key: None,
        }
    }
    pub async fn publish(self) -> Result<CommitReceipt> {
        self.client.ensure_provider_qualified()?;
        let receipt = self
            .client
            .repository
            .publish_physical_batch(
                self.manifest,
                self.physical_mutations.into_values().collect(),
            )
            .await?;
        self.client.record_advisory(&receipt).await;
        Ok(receipt)
    }
    pub async fn abort(self) -> Result<()> {
        self.client.ensure_provider_qualified()?;
        Ok(())
    }
}

pub struct StagedPutObjectBuilder<'a> {
    session: &'a mut CommitSession,
    bucket: Option<String>,
    key: Option<String>,
    body: Option<ByteStream>,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
}
impl<'a> StagedPutObjectBuilder<'a> {
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn body(mut self, value: ByteStream) -> Self {
        self.body = Some(value);
        self
    }
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value.into());
        self
    }
    pub async fn stage(self) -> Result<()> {
        self.session
            .client
            .validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let key_bytes = key.as_bytes().to_vec();
        let replaced_bytes = match self.session.physical_mutations.get(&key_bytes) {
            Some(prolly_s3_core::PhysicalBatchMutationV1::Put { bytes, .. }) => bytes.len(),
            _ => 0,
        };
        let retained_bytes = self
            .session
            .staged_body_bytes
            .checked_sub(replaced_bytes)
            .ok_or_else(|| invalid("staged batch byte accounting underflow"))?;
        let available = self
            .session
            .client
            .max_staged_batch_bytes
            .checked_sub(retained_bytes)
            .ok_or_else(|| invalid("staged batch already exceeds its byte limit"))?;
        let mut body = self.body.ok_or_else(|| invalid("body is required"))?;
        let mut bytes = Vec::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(|error| {
                Error::new(ErrorCode::Transport, format!("staged body failed: {error}"))
            })?;
            let next_len = bytes
                .len()
                .checked_add(chunk.len())
                .ok_or_else(|| invalid("staged body length overflow"))?;
            if next_len > available {
                return Err(Error::new(
                    ErrorCode::EntityTooLarge,
                    format!(
                        "atomic commit bodies exceed the configured {} byte memory limit",
                        self.session.client.max_staged_batch_bytes
                    ),
                ));
            }
            bytes.extend_from_slice(&chunk);
        }
        self.session.staged_body_bytes = retained_bytes + bytes.len();
        self.session.physical_mutations.insert(
            key_bytes.clone(),
            prolly_s3_core::PhysicalBatchMutationV1::Put {
                key: key_bytes,
                bytes,
                headers: ObjectHeaders {
                    content_type: self.content_type,
                    ..ObjectHeaders::default()
                },
                user_metadata: self.metadata.unwrap_or_default().into_iter().collect(),
            },
        );
        Ok(())
    }
}

pub struct StagedDeleteObjectBuilder<'a> {
    session: &'a mut CommitSession,
    bucket: Option<String>,
    key: Option<String>,
}
impl<'a> StagedDeleteObjectBuilder<'a> {
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub async fn stage(self) -> Result<()> {
        self.session
            .client
            .validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let key_bytes = key.as_bytes().to_vec();
        if let Some(prolly_s3_core::PhysicalBatchMutationV1::Put { bytes, .. }) =
            self.session.physical_mutations.get(&key_bytes)
        {
            self.session.staged_body_bytes = self
                .session
                .staged_body_bytes
                .checked_sub(bytes.len())
                .ok_or_else(|| invalid("staged batch byte accounting underflow"))?;
        }
        self.session.physical_mutations.insert(
            key_bytes.clone(),
            prolly_s3_core::PhysicalBatchMutationV1::Delete { key: key_bytes },
        );
        Ok(())
    }
}

pub struct PutObjectBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    body: Option<ByteStream>,
    content_type: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    content_disposition: Option<String>,
    cache_control: Option<String>,
    metadata: Option<HashMap<String, String>>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    content_md5: Option<String>,
    checksum_sha256: Option<String>,
    expected_head: Option<CommitId>,
    operation_id: Option<OperationId>,
    deadline: Option<Instant>,
}

impl PutObjectBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            body: None,
            content_type: None,
            content_encoding: None,
            content_language: None,
            content_disposition: None,
            cache_control: None,
            metadata: None,
            if_match: None,
            if_none_match: None,
            content_md5: None,
            checksum_sha256: None,
            expected_head: None,
            operation_id: None,
            deadline: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn set_bucket(mut self, v: Option<String>) -> Self {
        self.bucket = v;
        self
    }
    pub fn key(mut self, v: impl Into<String>) -> Self {
        self.key = Some(v.into());
        self
    }
    pub fn set_key(mut self, v: Option<String>) -> Self {
        self.key = v;
        self
    }
    pub fn body(mut self, v: ByteStream) -> Self {
        self.body = Some(v);
        self
    }
    pub fn set_body(mut self, v: Option<ByteStream>) -> Self {
        self.body = v;
        self
    }
    pub fn content_type(mut self, v: impl Into<String>) -> Self {
        self.content_type = Some(v.into());
        self
    }
    pub fn set_content_type(mut self, v: Option<String>) -> Self {
        self.content_type = v;
        self
    }
    pub fn content_encoding(mut self, v: impl Into<String>) -> Self {
        self.content_encoding = Some(v.into());
        self
    }
    pub fn content_language(mut self, v: impl Into<String>) -> Self {
        self.content_language = Some(v.into());
        self
    }
    pub fn content_disposition(mut self, v: impl Into<String>) -> Self {
        self.content_disposition = Some(v.into());
        self
    }
    pub fn cache_control(mut self, v: impl Into<String>) -> Self {
        self.cache_control = Some(v.into());
        self
    }
    pub fn metadata(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(k.into(), v.into());
        self
    }
    pub fn set_metadata(mut self, v: Option<HashMap<String, String>>) -> Self {
        self.metadata = v;
        self
    }
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }
    pub fn set_if_match(mut self, value: Option<String>) -> Self {
        self.if_match = value;
        self
    }
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }
    pub fn set_if_none_match(mut self, value: Option<String>) -> Self {
        self.if_none_match = value;
        self
    }
    pub fn content_md5(mut self, value: impl Into<String>) -> Self {
        self.content_md5 = Some(value.into());
        self
    }
    pub fn checksum_sha256(mut self, value: impl Into<String>) -> Self {
        self.checksum_sha256 = Some(value.into());
        self
    }
    pub fn expected_head(mut self, value: CommitId) -> Self {
        self.expected_head = Some(value);
        self
    }
    pub fn operation_id(mut self, v: OperationId) -> Self {
        self.operation_id = Some(v);
        self
    }
    pub fn deadline(mut self, value: Instant) -> Self {
        self.deadline = Some(value);
        self
    }

    pub async fn send(mut self) -> Result<Versioned<PutObjectOutput>> {
        let operation = self.operation_id.unwrap_or_default();
        self.operation_id = Some(operation);
        let deadline = self.deadline;
        validate_deadline(deadline)?;
        apply_write_deadline(deadline, operation, Box::pin(self.send_inner())).await
    }

    async fn send_inner(self) -> Result<Versioned<PutObjectOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?.as_bytes().to_vec();
        let body = self.body.ok_or_else(|| invalid("body is required"))?;
        let body = futures_util::stream::unfold(body, |mut body| async move {
            body.next().await.map(|item| (item, body))
        });
        let headers = ObjectHeaders {
            content_type: self.content_type,
            content_encoding: self.content_encoding,
            content_language: self.content_language,
            content_disposition: self.content_disposition,
            cache_control: self.cache_control,
            expires_at_millis: None,
        };
        let metadata = self
            .metadata
            .unwrap_or_default()
            .into_iter()
            .collect::<BTreeMap<_, _>>();
        let receipt = self
            .client
            .repository
            .put_stream_checked(
                &self.client.branch,
                key,
                body,
                headers,
                metadata,
                self.operation_id,
                ObjectWriteConditionV1 {
                    if_match: self
                        .if_match
                        .as_deref()
                        .map(parse_etag_predicate)
                        .transpose()?,
                    if_none_match: self
                        .if_none_match
                        .as_deref()
                        .map(parse_etag_predicate)
                        .transpose()?,
                    expected_head: self.expected_head,
                },
                ChecksumExpectation {
                    md5: self
                        .content_md5
                        .as_deref()
                        .map(|value| decode_checksum::<16>(value, "content_md5"))
                        .transpose()?,
                    sha256: self
                        .checksum_sha256
                        .as_deref()
                        .map(|value| decode_checksum::<32>(value, "checksum_sha256"))
                        .transpose()?,
                },
            )
            .await?;
        self.client.record_advisory(&receipt).await;
        let version = receipt.object_versions.first().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "put receipt omitted object version",
            )
        })?;
        let summary = self
            .client
            .repository
            .head_current(
                &self.client.branch,
                required(self.key.as_deref(), "key")?.as_bytes(),
            )
            .await?;
        let (etag, size) = live_etag_size(&summary)?;
        let checksum_sha256 = live_sha256(&summary)?.map(|value| STANDARD.encode(value));
        let output = PutObjectOutput::builder()
            .e_tag(etag)
            .version_id(version.to_string())
            .size(i64_len(size)?)
            .set_checksum_sha256(checksum_sha256)
            .build();
        Ok(Versioned {
            snapshot: receipt.id,
            commit: Some(receipt),
            output,
        })
    }
}

pub struct GetObjectBuilder {
    client: Client,
    snapshot: Option<CommitId>,
    bucket: Option<String>,
    key: Option<String>,
    version_id: Option<String>,
    range: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime>,
    if_unmodified_since: Option<DateTime>,
    checksum_mode: Option<ChecksumMode>,
    deadline: Option<Instant>,
}
impl GetObjectBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            snapshot: None,
            bucket: None,
            key: None,
            version_id: None,
            range: None,
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
            checksum_mode: None,
            deadline: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn set_bucket(mut self, v: Option<String>) -> Self {
        self.bucket = v;
        self
    }
    pub fn key(mut self, v: impl Into<String>) -> Self {
        self.key = Some(v.into());
        self
    }
    pub fn set_key(mut self, v: Option<String>) -> Self {
        self.key = v;
        self
    }
    pub fn version_id(mut self, v: impl Into<String>) -> Self {
        self.version_id = Some(v.into());
        self
    }
    pub fn set_version_id(mut self, v: Option<String>) -> Self {
        self.version_id = v;
        self
    }
    pub fn range(mut self, v: impl Into<String>) -> Self {
        self.range = Some(v.into());
        self
    }
    pub fn set_range(mut self, v: Option<String>) -> Self {
        self.range = v;
        self
    }
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }
    pub fn if_modified_since(mut self, value: DateTime) -> Self {
        self.if_modified_since = Some(value);
        self
    }
    pub fn if_unmodified_since(mut self, value: DateTime) -> Self {
        self.if_unmodified_since = Some(value);
        self
    }
    pub fn checksum_mode(mut self, value: ChecksumMode) -> Self {
        self.checksum_mode = Some(value);
        self
    }
    pub fn deadline(mut self, value: Instant) -> Self {
        self.deadline = Some(value);
        self
    }
    pub async fn send(self) -> Result<Versioned<GetObjectOutput>> {
        let deadline = self.deadline;
        validate_deadline(deadline)?;
        apply_read_deadline(deadline, Box::pin(self.send_inner())).await
    }
    async fn send_inner(self) -> Result<Versioned<GetObjectOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let selected = self
            .version_id
            .as_deref()
            .map(ObjectVersionId::from_str)
            .transpose()?;
        let snapshot = match self.snapshot {
            Some(value) => value,
            None => self.client.head_commit().await?,
        };
        let summary = match selected {
            Some(id) => {
                self.client
                    .repository
                    .head_version_in(snapshot, key.as_bytes(), id)
                    .await?
            }
            None => {
                self.client
                    .repository
                    .head_current_in(snapshot, key.as_bytes())
                    .await?
            }
        };
        validate_read_conditions(
            &summary,
            self.if_match.as_deref(),
            self.if_none_match.as_deref(),
            self.if_modified_since.as_ref(),
            self.if_unmodified_since.as_ref(),
        )?;
        validate_checksum_mode(self.checksum_mode.as_ref())?;
        let (body, response_len, content_range) = match &summary.version.body.kind {
            LogicalObjectVersionKindV1::Live { size, .. } => {
                let selected_range = self
                    .range
                    .as_deref()
                    .map(|spec| parse_range(spec, *size))
                    .transpose()?;
                let response_len = selected_range
                    .map(|(start, end)| end - start + 1)
                    .unwrap_or(*size);
                let content_range =
                    selected_range.map(|(start, end)| format!("bytes {start}-{end}/{size}"));
                let stream = self.client.repository.read_version_stream(
                    key.as_bytes(),
                    summary.version.clone(),
                    selected_range,
                );
                (
                    streaming_body(stream, response_len),
                    response_len,
                    content_range,
                )
            }
            LogicalObjectVersionKindV1::DeleteMarker => {
                if self.range.is_some() {
                    return Err(Error::new(
                        ErrorCode::InvalidRange,
                        "a delete marker has no byte range",
                    ));
                }
                (ByteStream::from_static(b""), 0, None)
            }
        };
        let mut output = GetObjectOutput::builder()
            .body(body)
            .content_length(i64_len(response_len)?)
            .version_id(summary.version.id.to_string())
            .last_modified(datetime(summary.version.body.created_at_millis)?)
            .accept_ranges("bytes")
            .set_content_range(content_range);
        match &summary.version.body.kind {
            LogicalObjectVersionKindV1::Live {
                logical_etag,
                headers,
                checksums,
                user_metadata,
                ..
            } => {
                output = output
                    .e_tag(logical_etag)
                    .set_content_type(headers.content_type.clone())
                    .set_content_encoding(headers.content_encoding.clone())
                    .set_content_language(headers.content_language.clone())
                    .set_content_disposition(headers.content_disposition.clone())
                    .set_cache_control(headers.cache_control.clone())
                    .set_checksum_sha256(
                        self.checksum_mode
                            .as_ref()
                            .and(checksums.sha256)
                            .map(|value| STANDARD.encode(value)),
                    )
                    .set_metadata(Some(user_metadata.clone().into_iter().collect()));
            }
            LogicalObjectVersionKindV1::DeleteMarker => {
                output = output.delete_marker(true);
            }
        }
        Ok(Versioned {
            output: output.build(),
            snapshot,
            commit: None,
        })
    }
}

pub struct HeadObjectBuilder {
    client: Client,
    snapshot: Option<CommitId>,
    bucket: Option<String>,
    key: Option<String>,
    version_id: Option<String>,
    if_match: Option<String>,
    if_none_match: Option<String>,
    if_modified_since: Option<DateTime>,
    if_unmodified_since: Option<DateTime>,
    checksum_mode: Option<ChecksumMode>,
    deadline: Option<Instant>,
}
impl HeadObjectBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            snapshot: None,
            bucket: None,
            key: None,
            version_id: None,
            if_match: None,
            if_none_match: None,
            if_modified_since: None,
            if_unmodified_since: None,
            checksum_mode: None,
            deadline: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn key(mut self, v: impl Into<String>) -> Self {
        self.key = Some(v.into());
        self
    }
    pub fn version_id(mut self, v: impl Into<String>) -> Self {
        self.version_id = Some(v.into());
        self
    }
    pub fn if_match(mut self, value: impl Into<String>) -> Self {
        self.if_match = Some(value.into());
        self
    }
    pub fn if_none_match(mut self, value: impl Into<String>) -> Self {
        self.if_none_match = Some(value.into());
        self
    }
    pub fn if_modified_since(mut self, value: DateTime) -> Self {
        self.if_modified_since = Some(value);
        self
    }
    pub fn if_unmodified_since(mut self, value: DateTime) -> Self {
        self.if_unmodified_since = Some(value);
        self
    }
    pub fn checksum_mode(mut self, value: ChecksumMode) -> Self {
        self.checksum_mode = Some(value);
        self
    }
    pub fn deadline(mut self, value: Instant) -> Self {
        self.deadline = Some(value);
        self
    }
    pub async fn send(self) -> Result<Versioned<HeadObjectOutput>> {
        let deadline = self.deadline;
        validate_deadline(deadline)?;
        apply_read_deadline(deadline, Box::pin(self.send_inner())).await
    }
    async fn send_inner(self) -> Result<Versioned<HeadObjectOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let selected = self
            .version_id
            .as_deref()
            .map(ObjectVersionId::from_str)
            .transpose()?;
        let snapshot = match self.snapshot {
            Some(value) => value,
            None => self.client.head_commit().await?,
        };
        let summary = if let Some(id) = selected {
            self.client
                .repository
                .head_version_in(snapshot, key.as_bytes(), id)
                .await?
        } else {
            self.client
                .repository
                .head_current_in(snapshot, key.as_bytes())
                .await?
        };
        validate_read_conditions(
            &summary,
            self.if_match.as_deref(),
            self.if_none_match.as_deref(),
            self.if_modified_since.as_ref(),
            self.if_unmodified_since.as_ref(),
        )?;
        validate_checksum_mode(self.checksum_mode.as_ref())?;
        let mut output = HeadObjectOutput::builder()
            .version_id(summary.version.id.to_string())
            .last_modified(datetime(summary.version.body.created_at_millis)?)
            .accept_ranges("bytes");
        match &summary.version.body.kind {
            LogicalObjectVersionKindV1::Live {
                size,
                logical_etag,
                headers,
                checksums,
                user_metadata,
                ..
            } => {
                output = output
                    .content_length(i64_len(*size)?)
                    .e_tag(logical_etag)
                    .set_content_type(headers.content_type.clone())
                    .set_content_encoding(headers.content_encoding.clone())
                    .set_content_language(headers.content_language.clone())
                    .set_content_disposition(headers.content_disposition.clone())
                    .set_cache_control(headers.cache_control.clone())
                    .set_checksum_sha256(
                        self.checksum_mode
                            .as_ref()
                            .and(checksums.sha256)
                            .map(|value| STANDARD.encode(value)),
                    )
                    .set_metadata(Some(user_metadata.clone().into_iter().collect()));
            }
            LogicalObjectVersionKindV1::DeleteMarker => {
                output = output.delete_marker(true);
            }
        }
        Ok(Versioned {
            output: output.build(),
            snapshot,
            commit: None,
        })
    }
}

pub struct DeleteObjectBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    version_id: Option<String>,
    operation_id: Option<OperationId>,
}
impl DeleteObjectBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            version_id: None,
            operation_id: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn key(mut self, v: impl Into<String>) -> Self {
        self.key = Some(v.into());
        self
    }
    pub fn version_id(mut self, v: impl Into<String>) -> Self {
        self.version_id = Some(v.into());
        self
    }
    pub fn operation_id(mut self, v: OperationId) -> Self {
        self.operation_id = Some(v);
        self
    }
    pub async fn send(self) -> Result<Versioned<DeleteObjectOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        if self.version_id.is_some() {
            return Err(invalid("deleting a selected historical version is unsupported; DeleteObject always creates a new delete marker"));
        }
        let key = required(self.key.as_deref(), "key")?;
        let receipt = self
            .client
            .repository
            .delete_object(
                &self.client.branch,
                key.as_bytes().to_vec(),
                self.operation_id,
            )
            .await?;
        self.client.record_advisory(&receipt).await;
        let version = receipt.object_versions.first().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "delete receipt omitted object version",
            )
        })?;
        let output = DeleteObjectOutput::builder()
            .delete_marker(true)
            .version_id(version.to_string())
            .build();
        Ok(Versioned {
            snapshot: receipt.id,
            commit: Some(receipt),
            output,
        })
    }
}

pub struct DeleteObjectsBuilder {
    client: Client,
    bucket: Option<String>,
    delete: Option<Delete>,
    operation_id: Option<OperationId>,
}

impl DeleteObjectsBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            delete: None,
            operation_id: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn delete(mut self, value: Delete) -> Self {
        self.delete = Some(value);
        self
    }
    pub fn set_delete(mut self, value: Option<Delete>) -> Self {
        self.delete = value;
        self
    }
    pub fn operation_id(mut self, value: OperationId) -> Self {
        self.operation_id = Some(value);
        self
    }

    pub async fn send(self) -> Result<Versioned<DeleteObjectsOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let delete = self.delete.ok_or_else(|| invalid("delete is required"))?;
        let quiet = delete.quiet().unwrap_or(false);
        let mut logical_keys = Vec::with_capacity(delete.objects().len());
        let mut response_keys = Vec::with_capacity(delete.objects().len());
        for object in delete.objects() {
            if object.version_id().is_some() {
                return Err(invalid(
                    "DeleteObjects cannot remove selected historical versions",
                ));
            }
            let key = required(Some(object.key()), "delete.objects[].key")?;
            logical_keys.push(key.as_bytes().to_vec());
            response_keys.push(key.to_string());
        }
        let receipt = self
            .client
            .repository
            .delete_objects(&self.client.branch, logical_keys, self.operation_id)
            .await?;
        self.client.record_advisory(&receipt).await;
        let deleted = if quiet {
            Vec::new()
        } else {
            response_keys
                .into_iter()
                .zip(receipt.object_versions.iter())
                .map(|(key, version)| {
                    DeletedObject::builder()
                        .key(key)
                        .delete_marker(true)
                        .delete_marker_version_id(version.to_string())
                        .build()
                })
                .collect()
        };
        let output = DeleteObjectsOutput::builder()
            .set_deleted(Some(deleted))
            .build();
        Ok(Versioned {
            output,
            snapshot: receipt.id,
            commit: Some(receipt),
        })
    }
}

pub struct CopyObjectBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    copy_source: Option<String>,
    operation_id: Option<OperationId>,
}

impl CopyObjectBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            copy_source: None,
            operation_id: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn copy_source(mut self, value: impl Into<String>) -> Self {
        self.copy_source = Some(value.into());
        self
    }
    pub fn operation_id(mut self, value: OperationId) -> Self {
        self.operation_id = Some(value);
        self
    }

    pub async fn send(self) -> Result<Versioned<CopyObjectOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let destination = required(self.key.as_deref(), "key")?;
        let (source_bucket, source_key, source_version) =
            parse_copy_source(required(self.copy_source.as_deref(), "copy_source")?)?;
        if source_bucket != self.client.bucket {
            return Err(invalid(
                "copy_source must refer to the configured logical bucket",
            ));
        }
        let receipt = self
            .client
            .repository
            .copy_object(
                &self.client.branch,
                source_key.as_bytes(),
                source_version,
                destination.as_bytes().to_vec(),
                self.operation_id,
            )
            .await?;
        self.client.record_advisory(&receipt).await;
        let version = receipt.object_versions.first().copied().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "copy receipt omitted object version",
            )
        })?;
        let summary = self
            .client
            .repository
            .head_current(&self.client.branch, destination.as_bytes())
            .await?;
        let (etag, _) = live_etag_size(&summary)?;
        let output = CopyObjectOutput::builder()
            .version_id(version.to_string())
            .copy_object_result(
                CopyObjectResult::builder()
                    .e_tag(etag)
                    .last_modified(datetime(summary.version.body.created_at_millis)?)
                    .build(),
            )
            .build();
        Ok(Versioned {
            output,
            snapshot: receipt.id,
            commit: Some(receipt),
        })
    }
}

pub struct CreateMultipartUploadBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    content_type: Option<String>,
    metadata: Option<HashMap<String, String>>,
    operation_id: Option<OperationId>,
}

pub struct ListMultipartUploadsBuilder {
    client: Client,
    bucket: Option<String>,
    prefix: Option<String>,
    key_marker: Option<String>,
    upload_id_marker: Option<String>,
    max_uploads: Option<i32>,
}

impl ListMultipartUploadsBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            prefix: None,
            key_marker: None,
            upload_id_marker: None,
            max_uploads: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn prefix(mut self, value: impl Into<String>) -> Self {
        self.prefix = Some(value.into());
        self
    }
    pub fn key_marker(mut self, value: impl Into<String>) -> Self {
        self.key_marker = Some(value.into());
        self
    }
    pub fn upload_id_marker(mut self, value: impl Into<String>) -> Self {
        self.upload_id_marker = Some(value.into());
        self
    }
    pub fn max_uploads(mut self, value: i32) -> Self {
        self.max_uploads = Some(value);
        self
    }
    pub async fn send(self) -> Result<ListMultipartUploadsOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let prefix = self.prefix.unwrap_or_default();
        let limit = validate_limit(self.max_uploads)?;
        if self.key_marker.is_none() && self.upload_id_marker.is_some() {
            return Err(invalid("upload_id_marker requires key_marker"));
        }
        if limit == 0 {
            return Ok(ListMultipartUploadsOutput::builder()
                .bucket(&self.client.bucket)
                .prefix(prefix)
                .max_uploads(0)
                .is_truncated(false)
                .set_key_marker(self.key_marker)
                .set_upload_id_marker(self.upload_id_marker)
                .set_uploads(Some(Vec::new()))
                .build());
        }
        let page = self
            .client
            .repository
            .plane()
            .list_physical_multipart_uploads(prolly_s3_core::PhysicalMultipartListUploads {
                prefix: prefix.clone(),
                key_marker: self.key_marker.clone(),
                upload_id_marker: self.upload_id_marker.clone(),
                limit,
            })
            .await?;
        let mut uploads = page
            .uploads
            .iter()
            .map(|upload| {
                let key = upload.path.as_str().to_string();
                let handle = encode_physical_multipart_session(&PhysicalMultipartSessionV1 {
                    repository: self.client.repository_id(),
                    branch: self.client.branch.clone(),
                    key: key.as_bytes().to_vec(),
                    headers: ObjectHeaders::default(),
                    user_metadata: BTreeMap::new(),
                    provider_upload_id: upload.upload_id.clone(),
                    operation: OperationId::nil(),
                    writer_fence_generation: 0,
                    created_at_millis: upload.initiated_at_millis,
                    discovered: true,
                })?;
                Ok(MultipartUpload::builder()
                    .key(key)
                    .upload_id(handle)
                    .initiated(datetime(upload.initiated_at_millis)?)
                    .build())
            })
            .collect::<Result<Vec<_>>>()?;
        if page.next_key_marker.is_none() {
            let sessions = self
                .client
                .physical_multipart_sessions
                .read()
                .map_err(|_| {
                    Error::new(
                        ErrorCode::InternalInvariant,
                        "physical multipart session cache lock poisoned",
                    )
                })?;
            for (handle, session) in sessions.iter() {
                let key = std::str::from_utf8(&session.key).map_err(|_| {
                    Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8")
                })?;
                if session.branch == self.client.branch
                    && key.starts_with(&prefix)
                    && !page.uploads.iter().any(|upload| {
                        upload.path.as_str() == key
                            && upload.upload_id == session.provider_upload_id
                    })
                {
                    uploads.push(
                        MultipartUpload::builder()
                            .key(key)
                            .upload_id(handle)
                            .initiated(datetime(session.created_at_millis)?)
                            .build(),
                    );
                }
            }
        }
        uploads.sort_by(|left, right| {
            (
                left.key().unwrap_or_default(),
                left.upload_id().unwrap_or_default(),
            )
                .cmp(&(
                    right.key().unwrap_or_default(),
                    right.upload_id().unwrap_or_default(),
                ))
        });
        uploads.truncate(limit);
        Ok(ListMultipartUploadsOutput::builder()
            .bucket(&self.client.bucket)
            .prefix(prefix)
            .max_uploads(i32::try_from(limit).unwrap_or(i32::MAX))
            .is_truncated(page.next_key_marker.is_some())
            .set_key_marker(self.key_marker)
            .set_upload_id_marker(self.upload_id_marker)
            .set_next_key_marker(page.next_key_marker)
            .set_next_upload_id_marker(page.next_upload_id_marker)
            .set_uploads(Some(uploads))
            .build())
    }
}
impl CreateMultipartUploadBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            content_type: None,
            metadata: None,
            operation_id: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn content_type(mut self, value: impl Into<String>) -> Self {
        self.content_type = Some(value.into());
        self
    }
    pub fn metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value.into());
        self
    }
    pub fn operation_id(mut self, value: OperationId) -> Self {
        self.operation_id = Some(value);
        self
    }
    pub async fn send(self) -> Result<CreateMultipartUploadOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let headers = ObjectHeaders {
            content_type: self.content_type,
            ..ObjectHeaders::default()
        };
        let metadata = self.metadata.unwrap_or_default().into_iter().collect();
        let session = self
            .client
            .repository
            .create_physical_multipart_upload(
                &self.client.branch,
                key.as_bytes().to_vec(),
                headers,
                metadata,
                self.operation_id,
            )
            .await?;
        let upload_id = encode_physical_multipart_session(&session)?;
        self.client
            .physical_multipart_sessions
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart session cache lock poisoned",
                )
            })?
            .insert(upload_id.clone(), session);
        Ok(CreateMultipartUploadOutput::builder()
            .bucket(&self.client.bucket)
            .key(key)
            .upload_id(upload_id)
            .build())
    }
}

pub struct UploadPartCopyBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
    part_number: Option<i32>,
    copy_source: Option<String>,
    copy_source_range: Option<String>,
}

impl UploadPartCopyBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            upload_id: None,
            part_number: None,
            copy_source: None,
            copy_source_range: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn upload_id(mut self, value: impl Into<String>) -> Self {
        self.upload_id = Some(value.into());
        self
    }
    pub fn part_number(mut self, value: i32) -> Self {
        self.part_number = Some(value);
        self
    }
    pub fn copy_source(mut self, value: impl Into<String>) -> Self {
        self.copy_source = Some(value.into());
        self
    }
    pub fn copy_source_range(mut self, value: impl Into<String>) -> Self {
        self.copy_source_range = Some(value.into());
        self
    }
    pub async fn send(self) -> Result<UploadPartCopyOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let destination = required(self.key.as_deref(), "key")?;
        let upload_text = required(self.upload_id.as_deref(), "upload_id")?;
        let part_number = u32::try_from(
            self.part_number
                .ok_or_else(|| invalid("part_number is required"))?,
        )
        .map_err(|_| invalid("part_number must be positive"))?;
        let (source_bucket, source_key, source_version) =
            parse_copy_source(required(self.copy_source.as_deref(), "copy_source")?)?;
        if source_bucket != self.client.bucket {
            return Err(invalid(
                "copy_source must refer to the configured logical bucket",
            ));
        }
        let range = if let Some(spec) = self.copy_source_range.as_deref() {
            let (_, summary) = match source_version {
                Some(version) => {
                    self.client
                        .repository
                        .head_version(&self.client.branch, source_key.as_bytes(), version)
                        .await?
                }
                None => {
                    self.client
                        .repository
                        .head_current_at(&self.client.branch, source_key.as_bytes())
                        .await?
                }
            };
            let (_, size) = live_etag_size(&summary)?;
            Some(parse_range(spec, size)?)
        } else {
            None
        };
        let session = decode_physical_multipart_session(&self.client, upload_text, destination)?;
        let part = self
            .client
            .repository
            .upload_physical_multipart_part_copy(
                &session,
                part_number,
                &self.client.branch,
                source_key.as_bytes(),
                source_version,
                range,
            )
            .await?;
        self.client
            .physical_multipart_parts
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart part cache lock poisoned",
                )
            })?
            .insert((upload_text.to_string(), part_number), part.clone());
        Ok(UploadPartCopyOutput::builder()
            .copy_part_result(
                CopyPartResult::builder()
                    .e_tag(part.etag)
                    .set_checksum_sha256(part.checksum_sha256.map(|value| STANDARD.encode(value)))
                    .build(),
            )
            .build())
    }
}

pub struct UploadPartBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
    part_number: Option<i32>,
    body: Option<ByteStream>,
}
impl UploadPartBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            upload_id: None,
            part_number: None,
            body: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn upload_id(mut self, value: impl Into<String>) -> Self {
        self.upload_id = Some(value.into());
        self
    }
    pub fn part_number(mut self, value: i32) -> Self {
        self.part_number = Some(value);
        self
    }
    pub fn body(mut self, value: ByteStream) -> Self {
        self.body = Some(value);
        self
    }
    pub async fn send(self) -> Result<UploadPartOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let upload_text = required(self.upload_id.as_deref(), "upload_id")?;
        let part_number = u32::try_from(
            self.part_number
                .ok_or_else(|| invalid("part_number is required"))?,
        )
        .map_err(|_| invalid("part_number must be positive"))?;
        let body = self.body.ok_or_else(|| invalid("body is required"))?;
        let session = decode_physical_multipart_session(&self.client, upload_text, key)?;
        let stream = futures_util::stream::unfold(body, |mut body| async move {
            body.next().await.map(|item| (item, body))
        });
        let part = self
            .client
            .repository
            .upload_physical_multipart_part_stream(&session, part_number, stream)
            .await?;
        self.client
            .physical_multipart_parts
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart part cache lock poisoned",
                )
            })?
            .insert((upload_text.to_string(), part_number), part.clone());
        Ok(UploadPartOutput::builder()
            .e_tag(part.etag)
            .set_checksum_sha256(part.checksum_sha256.map(|value| STANDARD.encode(value)))
            .build())
    }
}

pub struct ListPartsBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
    part_number_marker: Option<String>,
    max_parts: Option<i32>,
}
impl ListPartsBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            upload_id: None,
            part_number_marker: None,
            max_parts: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn upload_id(mut self, value: impl Into<String>) -> Self {
        self.upload_id = Some(value.into());
        self
    }
    pub fn part_number_marker(mut self, value: impl Into<String>) -> Self {
        self.part_number_marker = Some(value.into());
        self
    }
    pub fn max_parts(mut self, value: i32) -> Self {
        self.max_parts = Some(value);
        self
    }
    pub async fn send(self) -> Result<ListPartsOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let upload_text = required(self.upload_id.as_deref(), "upload_id")?;
        let session = decode_physical_multipart_session(&self.client, upload_text, key)?;
        let marker = self
            .part_number_marker
            .as_deref()
            .map(str::parse::<u32>)
            .transpose()
            .map_err(|_| invalid("part_number_marker must be an integer"))?
            .unwrap_or(0);
        let limit = validate_limit(self.max_parts)?;
        let page = self
            .client
            .repository
            .plane()
            .list_physical_multipart_parts(prolly_s3_core::PhysicalMultipartListParts {
                path: ObjectPath::new(key)?,
                upload_id: session.provider_upload_id,
                after_part_number: marker,
                limit,
            })
            .await?;
        let mut cache = self.client.physical_multipart_parts.write().map_err(|_| {
            Error::new(
                ErrorCode::InternalInvariant,
                "physical multipart part cache lock poisoned",
            )
        })?;
        for part in &page.parts {
            let key = (upload_text.to_string(), part.part_number);
            let mut merged = part.clone();
            if merged.checksum_sha256.is_none() {
                merged.checksum_sha256 = cache.get(&key).and_then(|part| part.checksum_sha256);
            }
            cache.insert(key, merged);
        }
        drop(cache);
        let parts = page
            .parts
            .into_iter()
            .map(|part| {
                Part::builder()
                    .part_number(i32::try_from(part.part_number).unwrap_or(i32::MAX))
                    .e_tag(part.etag)
                    .set_checksum_sha256(part.checksum_sha256.map(|value| STANDARD.encode(value)))
                    .size(i64_len(part.size).expect("provider part size is valid"))
                    .build()
            })
            .collect();
        Ok(ListPartsOutput::builder()
            .bucket(&self.client.bucket)
            .key(key)
            .upload_id(upload_text)
            .set_part_number_marker(self.part_number_marker)
            .set_next_part_number_marker(page.next_part_number.map(|value| value.to_string()))
            .max_parts(i32::try_from(limit).unwrap_or(i32::MAX))
            .is_truncated(page.next_part_number.is_some())
            .set_parts(Some(parts))
            .build())
    }
}

pub struct CompleteMultipartUploadBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
    multipart_upload: Option<CompletedMultipartUpload>,
    operation_id: Option<OperationId>,
    checksum_sha256: Option<String>,
    checksum_md5: Option<String>,
    expected_size: Option<u64>,
    part_sizes: BTreeMap<u32, u64>,
}
impl CompleteMultipartUploadBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            upload_id: None,
            multipart_upload: None,
            operation_id: None,
            checksum_sha256: None,
            checksum_md5: None,
            expected_size: None,
            part_sizes: BTreeMap::new(),
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn upload_id(mut self, value: impl Into<String>) -> Self {
        self.upload_id = Some(value.into());
        self
    }
    pub fn multipart_upload(mut self, value: CompletedMultipartUpload) -> Self {
        self.multipart_upload = Some(value);
        self
    }
    pub fn operation_id(mut self, value: OperationId) -> Self {
        self.operation_id = Some(value);
        self
    }
    pub fn checksum_sha256(mut self, value: impl Into<String>) -> Self {
        self.checksum_sha256 = Some(value.into());
        self
    }
    pub fn checksum_md5(mut self, value: impl Into<String>) -> Self {
        self.checksum_md5 = Some(value.into());
        self
    }
    pub fn expected_size(mut self, value: u64) -> Self {
        self.expected_size = Some(value);
        self
    }
    pub fn part_size(mut self, part_number: u32, size: u64) -> Self {
        self.part_sizes.insert(part_number, size);
        self
    }
    pub async fn send(self) -> Result<Versioned<CompleteMultipartUploadOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let upload_text = required(self.upload_id.as_deref(), "upload_id")?;
        let completed = self
            .multipart_upload
            .ok_or_else(|| invalid("multipart_upload is required"))?;
        let session = decode_physical_multipart_session(&self.client, upload_text, key)?;
        let checksum_sha256 = decode_checksum::<32>(
            required(self.checksum_sha256.as_deref(), "checksum_sha256")?,
            "checksum_sha256",
        )?;
        let checksum_md5 = decode_checksum::<16>(
            required(self.checksum_md5.as_deref(), "checksum_md5")?,
            "checksum_md5",
        )?;
        let expected_size = self
            .expected_size
            .ok_or_else(|| invalid("expected_size is required for physical multipart"))?;
        let physical_parts = {
            let cached = self.client.physical_multipart_parts.read().map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart part cache lock poisoned",
                )
            })?;
            let mut physical_parts = Vec::with_capacity(completed.parts().len());
            for part in completed.parts() {
                let part_number = u32::try_from(
                    part.part_number()
                        .ok_or_else(|| invalid("completed part_number is required"))?,
                )
                .map_err(|_| invalid("completed part_number must be positive"))?;
                let etag = required(part.e_tag(), "completed e_tag")?.to_string();
                let cached_part = cached.get(&(upload_text.to_string(), part_number));
                let checksum = match part.checksum_sha256() {
                    Some(value) => decode_checksum::<32>(value, "completed checksum_sha256")?,
                    None => cached_part
                        .and_then(|part| part.checksum_sha256)
                        .ok_or_else(|| {
                            invalid("completed checksum_sha256 is required after a process restart")
                        })?,
                };
                let size = self
                    .part_sizes
                    .get(&part_number)
                    .copied()
                    .or_else(|| cached_part.map(|part| part.size))
                    .ok_or_else(|| {
                        invalid("part_size is required after a physical multipart process restart")
                    })?;
                if cached_part.is_some_and(|part| {
                    part.etag != etag || part.checksum_sha256 != Some(checksum) || part.size != size
                }) {
                    return Err(Error::new(
                        ErrorCode::ChecksumMismatch,
                        "completed physical multipart part differs from its upload receipt",
                    ));
                }
                physical_parts.push(PhysicalMultipartCompletedPart {
                    part_number,
                    etag,
                    checksum_sha256: checksum,
                    size,
                });
            }
            physical_parts
        };
        let receipt = self
            .client
            .repository
            .complete_physical_multipart_upload(
                session,
                physical_parts,
                checksum_sha256,
                checksum_md5,
                expected_size,
                self.operation_id,
            )
            .await?;
        self.client
            .physical_multipart_parts
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart part cache lock poisoned",
                )
            })?
            .retain(|(upload, _), _| upload != upload_text);
        self.client
            .physical_multipart_sessions
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart session cache lock poisoned",
                )
            })?
            .remove(upload_text);
        self.client.record_advisory(&receipt).await;
        let summary = self
            .client
            .repository
            .head_current(&self.client.branch, key.as_bytes())
            .await?;
        let (etag, _) = live_etag_size(&summary)?;
        let version = receipt.object_versions.first().ok_or_else(|| {
            Error::new(
                ErrorCode::InternalInvariant,
                "multipart receipt omitted version",
            )
        })?;
        let output = CompleteMultipartUploadOutput::builder()
            .bucket(&self.client.bucket)
            .key(key)
            .e_tag(etag)
            .version_id(version.to_string())
            .build();
        Ok(Versioned {
            output,
            snapshot: receipt.id,
            commit: Some(receipt),
        })
    }
}

pub struct AbortMultipartUploadBuilder {
    client: Client,
    bucket: Option<String>,
    key: Option<String>,
    upload_id: Option<String>,
}
impl AbortMultipartUploadBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            bucket: None,
            key: None,
            upload_id: None,
        }
    }
    pub fn bucket(mut self, value: impl Into<String>) -> Self {
        self.bucket = Some(value.into());
        self
    }
    pub fn key(mut self, value: impl Into<String>) -> Self {
        self.key = Some(value.into());
        self
    }
    pub fn upload_id(mut self, value: impl Into<String>) -> Self {
        self.upload_id = Some(value.into());
        self
    }
    pub async fn send(self) -> Result<AbortMultipartUploadOutput> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let key = required(self.key.as_deref(), "key")?;
        let upload_text = required(self.upload_id.as_deref(), "upload_id")?;
        let session = decode_physical_multipart_session(&self.client, upload_text, key)?;
        self.client
            .repository
            .abort_physical_multipart_upload(&session)
            .await?;
        self.client
            .physical_multipart_parts
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart part cache lock poisoned",
                )
            })?
            .retain(|(upload, _), _| upload != upload_text);
        self.client
            .physical_multipart_sessions
            .write()
            .map_err(|_| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "physical multipart session cache lock poisoned",
                )
            })?
            .remove(upload_text);
        Ok(AbortMultipartUploadOutput::builder().build())
    }
}

pub struct ListObjectsV2Builder {
    client: Client,
    snapshot: Option<CommitId>,
    bucket: Option<String>,
    prefix: Option<String>,
    delimiter: Option<String>,
    max_keys: Option<i32>,
    continuation_token: Option<String>,
    start_after: Option<String>,
    deadline: Option<Instant>,
}
impl ListObjectsV2Builder {
    fn new(client: Client) -> Self {
        Self {
            client,
            snapshot: None,
            bucket: None,
            prefix: None,
            delimiter: None,
            max_keys: None,
            continuation_token: None,
            start_after: None,
            deadline: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn prefix(mut self, v: impl Into<String>) -> Self {
        self.prefix = Some(v.into());
        self
    }
    pub fn delimiter(mut self, v: impl Into<String>) -> Self {
        self.delimiter = Some(v.into());
        self
    }
    pub fn max_keys(mut self, v: i32) -> Self {
        self.max_keys = Some(v);
        self
    }
    pub fn continuation_token(mut self, v: impl Into<String>) -> Self {
        self.continuation_token = Some(v.into());
        self
    }
    pub fn start_after(mut self, v: impl Into<String>) -> Self {
        self.start_after = Some(v.into());
        self
    }
    pub fn deadline(mut self, value: Instant) -> Self {
        self.deadline = Some(value);
        self
    }
    pub async fn send(self) -> Result<Versioned<ListObjectsV2Output>> {
        let deadline = self.deadline;
        validate_deadline(deadline)?;
        apply_read_deadline(deadline, Box::pin(self.send_inner())).await
    }
    async fn send_inner(self) -> Result<Versioned<ListObjectsV2Output>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let prefix = self.prefix.unwrap_or_default();
        let limit = validate_limit(self.max_keys)?;
        if self.continuation_token.is_some() && self.start_after.is_some() {
            return Err(invalid(
                "continuation_token and start_after are mutually exclusive",
            ));
        }
        let (snapshot, mut after, mut skip_prefix) =
            if let Some(token) = self.continuation_token.as_deref() {
                let cursor = self.client.decode_cursor(token)?;
                validate_listing_cursor(
                    &cursor,
                    "objects",
                    &self.client,
                    &prefix,
                    self.delimiter.as_deref(),
                )?;
                if self
                    .snapshot
                    .is_some_and(|snapshot| snapshot != cursor.snapshot)
                {
                    return Err(Error::new(
                        ErrorCode::InvalidContinuationToken,
                        "cursor does not belong to this snapshot",
                    ));
                }
                (cursor.snapshot, Some(cursor.after), cursor.skip_prefix)
            } else {
                (
                    match self.snapshot {
                        Some(value) => value,
                        None => self.client.head_commit().await?,
                    },
                    self.start_after
                        .as_ref()
                        .map(|value| value.as_bytes().to_vec()),
                    None,
                )
            };
        let mut common = HashSet::new();
        let mut objects = Vec::new();
        let mut truncated = false;
        'pages: while objects.len() + common.len() < limit {
            let (summaries, more) = self
                .client
                .repository
                .list_objects_at(snapshot, prefix.as_bytes(), after.as_deref(), 1_000)
                .await?;
            if summaries.is_empty() {
                break;
            }
            let count = summaries.len();
            for (index, summary) in summaries.into_iter().enumerate() {
                after = Some(summary.key.clone());
                if skip_prefix
                    .as_ref()
                    .is_some_and(|group| summary.key.starts_with(group))
                {
                    continue;
                }
                skip_prefix = None;
                let key = utf8_key(&summary.key)?;
                let grouped = self.delimiter.as_deref().and_then(|delimiter| {
                    let suffix = key.strip_prefix(&prefix).unwrap_or(&key);
                    suffix
                        .find(delimiter)
                        .map(|pos| format!("{}{}{}", prefix, &suffix[..pos], delimiter))
                });
                let added = if let Some(group) = grouped.as_ref() {
                    common.insert(group.clone())
                } else {
                    let (etag, size) = live_etag_size(&summary)?;
                    objects.push(
                        Object::builder()
                            .key(key)
                            .e_tag(etag)
                            .size(i64_len(size)?)
                            .last_modified(datetime(summary.version.body.created_at_millis)?)
                            .build(),
                    );
                    true
                };
                if added && objects.len() + common.len() == limit {
                    truncated = more || index + 1 < count;
                    if truncated {
                        skip_prefix = grouped.map(String::into_bytes);
                    }
                    break 'pages;
                }
            }
            if !more {
                break;
            }
        }
        let mut prefixes: Vec<_> = common.into_iter().collect();
        prefixes.sort();
        let next_token = if truncated {
            let after = after.ok_or_else(|| {
                Error::new(
                    ErrorCode::InternalInvariant,
                    "truncated listing has no resume key",
                )
            })?;
            Some(
                self.client.encode_cursor(&ListingCursor {
                    version: 1,
                    kind: "objects".to_string(),
                    repository: self.client.repository_id(),
                    bucket: self.client.bucket.clone(),
                    branch: self.client.branch.clone(),
                    snapshot,
                    prefix: prefix.clone(),
                    delimiter: self.delimiter.clone(),
                    after,
                    skip_prefix,
                    expires_at_millis: now_millis_client()?
                        .checked_add(self.client.cursor_ttl.as_millis() as u64)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::InternalInvariant, "cursor expiry overflow")
                        })?,
                })?,
            )
        } else {
            None
        };
        let key_count = objects.len() + prefixes.len();
        let output = ListObjectsV2Output::builder()
            .name(&self.client.bucket)
            .prefix(&prefix)
            .set_delimiter(self.delimiter)
            .max_keys(i32::try_from(limit).unwrap_or(i32::MAX))
            .key_count(i32::try_from(key_count).unwrap_or(i32::MAX))
            .is_truncated(truncated)
            .set_continuation_token(self.continuation_token)
            .set_next_continuation_token(next_token)
            .set_start_after(self.start_after)
            .set_contents(Some(objects))
            .set_common_prefixes(Some(
                prefixes
                    .into_iter()
                    .map(|p| CommonPrefix::builder().prefix(p).build())
                    .collect(),
            ))
            .build();
        Ok(Versioned {
            output,
            snapshot,
            commit: None,
        })
    }
}

pub struct ListObjectVersionsBuilder {
    client: Client,
    snapshot: Option<CommitId>,
    bucket: Option<String>,
    prefix: Option<String>,
    max_keys: Option<i32>,
    key_marker: Option<String>,
    version_id_marker: Option<String>,
}
impl ListObjectVersionsBuilder {
    fn new(client: Client) -> Self {
        Self {
            client,
            snapshot: None,
            bucket: None,
            prefix: None,
            max_keys: None,
            key_marker: None,
            version_id_marker: None,
        }
    }
    pub fn bucket(mut self, v: impl Into<String>) -> Self {
        self.bucket = Some(v.into());
        self
    }
    pub fn prefix(mut self, v: impl Into<String>) -> Self {
        self.prefix = Some(v.into());
        self
    }
    pub fn max_keys(mut self, v: i32) -> Self {
        self.max_keys = Some(v);
        self
    }
    pub fn key_marker(mut self, v: impl Into<String>) -> Self {
        self.key_marker = Some(v.into());
        self
    }
    pub fn version_id_marker(mut self, v: impl Into<String>) -> Self {
        self.version_id_marker = Some(v.into());
        self
    }
    pub async fn send(self) -> Result<Versioned<ListObjectVersionsOutput>> {
        self.client.validate_bucket(self.bucket.as_deref())?;
        let prefix = self.prefix.unwrap_or_default();
        let limit = validate_limit(self.max_keys)?;
        let (snapshot, after, previous_key) = if let Some(token) = self.version_id_marker.as_deref()
        {
            let request_key = self
                .key_marker
                .as_deref()
                .ok_or_else(|| invalid("version_id_marker requires key_marker"))?;
            let cursor = self.client.decode_cursor(token)?;
            validate_listing_cursor(&cursor, "versions", &self.client, &prefix, None)?;
            if cursor.skip_prefix.as_deref() != Some(request_key.as_bytes()) {
                return Err(Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "version cursor does not match key_marker",
                ));
            }
            if self
                .snapshot
                .is_some_and(|snapshot| snapshot != cursor.snapshot)
            {
                return Err(Error::new(
                    ErrorCode::InvalidContinuationToken,
                    "cursor does not belong to this snapshot",
                ));
            }
            (cursor.snapshot, Some(cursor.after), cursor.skip_prefix)
        } else {
            let after = self
                .key_marker
                .as_deref()
                .map(|key| version_cursor_after_key(key.as_bytes()));
            (
                match self.snapshot {
                    Some(value) => value,
                    None => self.client.head_commit().await?,
                },
                after,
                None,
            )
        };
        let (summaries, truncated) = self
            .client
            .repository
            .list_versions_at(snapshot, prefix.as_bytes(), after.as_deref(), limit)
            .await?;
        let mut latest = HashSet::new();
        if let Some(key) = previous_key {
            latest.insert(utf8_key(&key)?);
        }
        let mut versions = Vec::new();
        let mut markers = Vec::new();
        let mut last_cursor = None;
        let mut last_key = None;
        for VersionSummary {
            key,
            version,
            cursor,
        } in summaries
        {
            last_cursor = Some(cursor);
            last_key = Some(key.clone());
            let key = utf8_key(&key)?;
            let is_latest = latest.insert(key.clone());
            let when = datetime(version.body.created_at_millis)?;
            match version.body.kind {
                LogicalObjectVersionKindV1::Live {
                    size, logical_etag, ..
                } => versions.push(
                    ObjectVersion::builder()
                        .key(key)
                        .version_id(version.id.to_string())
                        .is_latest(is_latest)
                        .last_modified(when)
                        .size(i64_len(size)?)
                        .e_tag(logical_etag)
                        .build(),
                ),
                LogicalObjectVersionKindV1::DeleteMarker => markers.push(
                    DeleteMarkerEntry::builder()
                        .key(key)
                        .version_id(version.id.to_string())
                        .is_latest(is_latest)
                        .last_modified(when)
                        .build(),
                ),
            }
        }
        let next_version_marker = if truncated {
            Some(
                self.client.encode_cursor(&ListingCursor {
                    version: 1,
                    kind: "versions".to_string(),
                    repository: self.client.repository_id(),
                    bucket: self.client.bucket.clone(),
                    branch: self.client.branch.clone(),
                    snapshot,
                    prefix: prefix.clone(),
                    delimiter: None,
                    after: last_cursor.ok_or_else(|| {
                        Error::new(
                            ErrorCode::InternalInvariant,
                            "truncated version listing has no cursor",
                        )
                    })?,
                    skip_prefix: last_key.clone(),
                    expires_at_millis: now_millis_client()?
                        .checked_add(self.client.cursor_ttl.as_millis() as u64)
                        .ok_or_else(|| {
                            Error::new(ErrorCode::InternalInvariant, "cursor expiry overflow")
                        })?,
                })?,
            )
        } else {
            None
        };
        let next_key_marker = if truncated {
            last_key.as_deref().map(utf8_key).transpose()?
        } else {
            None
        };
        let output = ListObjectVersionsOutput::builder()
            .name(&self.client.bucket)
            .prefix(&prefix)
            .max_keys(i32::try_from(limit).unwrap_or(i32::MAX))
            .is_truncated(truncated)
            .set_key_marker(self.key_marker)
            .set_version_id_marker(self.version_id_marker)
            .set_next_key_marker(next_key_marker)
            .set_next_version_id_marker(next_version_marker)
            .set_versions(Some(versions))
            .set_delete_markers(Some(markers))
            .build();
        Ok(Versioned {
            output,
            snapshot,
            commit: None,
        })
    }
}

fn required<'a>(value: Option<&'a str>, field: &str) -> Result<&'a str> {
    value
        .filter(|v| !v.is_empty())
        .ok_or_else(|| invalid(format!("{field} is required")))
}
fn unsupported(field: &str) -> Error {
    Error::new(
        ErrorCode::UnsupportedParameter,
        format!("unsupported S3 parameter: {field}"),
    )
}
fn reject_set(field: &str, set: bool) -> Result<()> {
    if set {
        Err(unsupported(field))
    } else {
        Ok(())
    }
}
fn reject_input_field(operation: &str, field: &str, set: bool) -> Result<()> {
    if supported_input_fields(operation).is_some_and(|fields| fields.contains(&field)) {
        return Err(Error::new(
            ErrorCode::InternalInvariant,
            format!("runtime validator rejects advertised field {operation}.{field}"),
        ));
    }
    reject_set(field, set)
}
fn validate_put_input(input: &aws_sdk_s3::operation::put_object::PutObjectInput) -> Result<()> {
    let reject_set = |field, set| reject_input_field("put_object", field, set);
    reject_set("acl", input.acl.is_some())?;
    reject_set("content_length", input.content_length.is_some())?;
    reject_set("checksum_algorithm", input.checksum_algorithm.is_some())?;
    reject_set("checksum_crc32", input.checksum_crc32.is_some())?;
    reject_set("checksum_crc32_c", input.checksum_crc32_c.is_some())?;
    reject_set("checksum_crc64_nvme", input.checksum_crc64_nvme.is_some())?;
    reject_set("checksum_sha1", input.checksum_sha1.is_some())?;
    reject_set("checksum_sha512", input.checksum_sha512.is_some())?;
    reject_set("checksum_md5", input.checksum_md5.is_some())?;
    reject_set("checksum_xxhash64", input.checksum_xxhash64.is_some())?;
    reject_set("checksum_xxhash3", input.checksum_xxhash3.is_some())?;
    reject_set("checksum_xxhash128", input.checksum_xxhash128.is_some())?;
    reject_set("expires", input.expires.is_some())?;
    reject_set("grant_full_control", input.grant_full_control.is_some())?;
    reject_set("grant_read", input.grant_read.is_some())?;
    reject_set("grant_read_acp", input.grant_read_acp.is_some())?;
    reject_set("grant_write_acp", input.grant_write_acp.is_some())?;
    reject_set("write_offset_bytes", input.write_offset_bytes.is_some())?;
    reject_set(
        "server_side_encryption",
        input.server_side_encryption.is_some(),
    )?;
    reject_set("storage_class", input.storage_class.is_some())?;
    reject_set(
        "website_redirect_location",
        input.website_redirect_location.is_some(),
    )?;
    reject_set(
        "sse_customer_algorithm",
        input.sse_customer_algorithm.is_some(),
    )?;
    reject_set("sse_customer_key", input.sse_customer_key.is_some())?;
    reject_set("sse_customer_key_md5", input.sse_customer_key_md5.is_some())?;
    reject_set("ssekms_key_id", input.ssekms_key_id.is_some())?;
    reject_set(
        "ssekms_encryption_context",
        input.ssekms_encryption_context.is_some(),
    )?;
    reject_set("bucket_key_enabled", input.bucket_key_enabled.is_some())?;
    reject_set("request_payer", input.request_payer.is_some())?;
    reject_set("tagging", input.tagging.is_some())?;
    reject_set("object_lock_mode", input.object_lock_mode.is_some())?;
    reject_set(
        "object_lock_retain_until_date",
        input.object_lock_retain_until_date.is_some(),
    )?;
    reject_set(
        "object_lock_legal_hold_status",
        input.object_lock_legal_hold_status.is_some(),
    )?;
    reject_set(
        "expected_bucket_owner",
        input.expected_bucket_owner.is_some(),
    )?;
    Ok(())
}
fn validate_get_input(input: &aws_sdk_s3::operation::get_object::GetObjectInput) -> Result<()> {
    let reject_set = |field, set| reject_input_field("get_object", field, set);
    reject_set(
        "response_cache_control",
        input.response_cache_control.is_some(),
    )?;
    reject_set(
        "response_content_disposition",
        input.response_content_disposition.is_some(),
    )?;
    reject_set(
        "response_content_encoding",
        input.response_content_encoding.is_some(),
    )?;
    reject_set(
        "response_content_language",
        input.response_content_language.is_some(),
    )?;
    reject_set(
        "response_content_type",
        input.response_content_type.is_some(),
    )?;
    reject_set("response_expires", input.response_expires.is_some())?;
    reject_set(
        "sse_customer_algorithm",
        input.sse_customer_algorithm.is_some(),
    )?;
    reject_set("sse_customer_key", input.sse_customer_key.is_some())?;
    reject_set("request_payer", input.request_payer.is_some())?;
    reject_set("part_number", input.part_number.is_some())?;
    reject_set(
        "expected_bucket_owner",
        input.expected_bucket_owner.is_some(),
    )?;
    Ok(())
}
fn validate_head_input(input: &aws_sdk_s3::operation::head_object::HeadObjectInput) -> Result<()> {
    let reject_set = |field, set| reject_input_field("head_object", field, set);
    reject_set("range", input.range.is_some())?;
    reject_set(
        "response_cache_control",
        input.response_cache_control.is_some(),
    )?;
    reject_set(
        "response_content_disposition",
        input.response_content_disposition.is_some(),
    )?;
    reject_set(
        "response_content_encoding",
        input.response_content_encoding.is_some(),
    )?;
    reject_set(
        "response_content_language",
        input.response_content_language.is_some(),
    )?;
    reject_set(
        "response_content_type",
        input.response_content_type.is_some(),
    )?;
    reject_set("response_expires", input.response_expires.is_some())?;
    reject_set(
        "sse_customer_algorithm",
        input.sse_customer_algorithm.is_some(),
    )?;
    reject_set("sse_customer_key", input.sse_customer_key.is_some())?;
    reject_set("request_payer", input.request_payer.is_some())?;
    reject_set("part_number", input.part_number.is_some())?;
    reject_set(
        "expected_bucket_owner",
        input.expected_bucket_owner.is_some(),
    )?;
    Ok(())
}
fn validate_list_input(
    input: &aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Input,
) -> Result<()> {
    let reject_set = |field, set| reject_input_field("list_objects_v2", field, set);
    reject_set("encoding_type", input.encoding_type.is_some())?;
    reject_set("fetch_owner", input.fetch_owner.is_some())?;
    reject_set("request_payer", input.request_payer.is_some())?;
    reject_set(
        "expected_bucket_owner",
        input.expected_bucket_owner.is_some(),
    )?;
    reject_set(
        "optional_object_attributes",
        input.optional_object_attributes.is_some(),
    )?;
    Ok(())
}
fn validate_deadline(deadline: Option<Instant>) -> Result<()> {
    if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
        return Err(Error::new(
            ErrorCode::Timeout,
            "operation deadline has elapsed",
        ));
    }
    Ok(())
}
fn validate_write_options(options: &WriteOptions) -> Result<()> {
    validate_deadline(options.deadline)?;
    Ok(())
}

async fn apply_read_deadline<F, T>(deadline: Option<Instant>, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let Some(deadline) = deadline else {
        return future.await;
    };
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| {
            Error::new(ErrorCode::Timeout, "operation deadline elapsed").retry(RetryAdvice::Safe)
        })?
}

async fn apply_write_deadline<F, T>(
    deadline: Option<Instant>,
    operation: OperationId,
    future: F,
) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let Some(deadline) = deadline else {
        return future.await;
    };
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), future)
        .await
        .map_err(|_| {
            Error::new(
                ErrorCode::OutcomeUnknown,
                "write deadline elapsed after the operation may have started",
            )
            .retry(RetryAdvice::ReconcileOperation)
            .operation(operation.to_string())
        })?
}
fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InvalidRequest, message)
}

fn validate_physical_capabilities(attestation: &ProviderAttestationV1) -> Result<()> {
    attestation.body.capabilities.validate_prolly_s3()
}
fn validate_branch_name(branch: &str) -> Result<()> {
    if branch.is_empty()
        || branch.contains("..")
        || branch.starts_with('/')
        || branch.ends_with('/')
    {
        Err(invalid("invalid branch name"))
    } else {
        Ok(())
    }
}
fn validate_limit(value: Option<i32>) -> Result<usize> {
    match value.unwrap_or(1_000) {
        v if v < 0 => Err(invalid("max_keys must be nonnegative")),
        v => Ok((v as usize).min(1_000)),
    }
}
fn validate_token_key_id(key_id: &str) -> Result<()> {
    if key_id.is_empty()
        || key_id.len() > 128
        || key_id
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace() || !byte.is_ascii())
    {
        return Err(invalid(
            "cursor key IDs must be 1..=128 printable non-whitespace ASCII bytes",
        ));
    }
    Ok(())
}
fn duration_millis(value: Duration, name: &str) -> Result<u64> {
    u64::try_from(value.as_millis())
        .map_err(|_| invalid(format!("{name} exceeds the supported millisecond range")))
}
fn now_millis_client() -> Result<u64> {
    let value = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        Error::new(
            ErrorCode::InternalInvariant,
            "system clock precedes the Unix epoch",
        )
    })?;
    u64::try_from(value.as_millis()).map_err(|_| {
        Error::new(
            ErrorCode::InternalInvariant,
            "system clock exceeds u64 millis",
        )
    })
}
fn validate_listing_cursor(
    cursor: &ListingCursor,
    kind: &str,
    client: &Client,
    prefix: &str,
    delimiter: Option<&str>,
) -> Result<()> {
    if cursor.kind != kind
        || cursor.repository != client.repository_id()
        || cursor.bucket != client.bucket
        || cursor.branch != client.branch
        || cursor.prefix != prefix
        || cursor.delimiter.as_deref() != delimiter
    {
        return Err(Error::new(
            ErrorCode::InvalidContinuationToken,
            "continuation token does not match this listing request",
        ));
    }
    Ok(())
}
fn parse_copy_source(value: &str) -> Result<(String, String, Option<ObjectVersionId>)> {
    let value = value.trim_start_matches('/');
    let (path, query) = value
        .split_once('?')
        .map_or((value, None), |(path, query)| (path, Some(query)));
    let (bucket, key) = path
        .split_once('/')
        .ok_or_else(|| invalid("copy_source must use bucket/key"))?;
    let key = percent_encoding::percent_decode_str(key)
        .decode_utf8()
        .map_err(|_| invalid("copy_source key is not valid UTF-8"))?
        .into_owned();
    let mut version = None;
    if let Some(query) = query {
        for field in query.split('&') {
            let (name, value) = field.split_once('=').unwrap_or((field, ""));
            if name == "versionId" {
                version = Some(ObjectVersionId::from_str(value)?);
            } else if !name.is_empty() {
                return Err(invalid(format!(
                    "unsupported copy_source query field {name:?}"
                )));
            }
        }
    }
    Ok((bucket.to_string(), key, version))
}
const PHYSICAL_MULTIPART_HANDLE_PREFIX: &str = "nmu1_";

fn encode_physical_multipart_session(session: &PhysicalMultipartSessionV1) -> Result<String> {
    Ok(format!(
        "{PHYSICAL_MULTIPART_HANDLE_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(encode_canonical(session)?)
    ))
}

fn decode_physical_multipart_session(
    client: &Client,
    value: &str,
    key: &str,
) -> Result<PhysicalMultipartSessionV1> {
    let encoded = value
        .strip_prefix(PHYSICAL_MULTIPART_HANDLE_PREFIX)
        .ok_or_else(|| {
            Error::new(
                ErrorCode::NoSuchUpload,
                "physical multipart handle is invalid",
            )
        })?;
    let bytes = URL_SAFE_NO_PAD.decode(encoded).map_err(|_| {
        Error::new(
            ErrorCode::NoSuchUpload,
            "physical multipart handle is not canonical base64url",
        )
    })?;
    let session: PhysicalMultipartSessionV1 = decode_canonical(&bytes).map_err(|_| {
        Error::new(
            ErrorCode::NoSuchUpload,
            "physical multipart handle is malformed",
        )
    })?;
    session.validate_address(client.repository_id())?;
    if session.branch != client.branch || session.key != key.as_bytes() {
        return Err(Error::new(
            ErrorCode::NoSuchUpload,
            "physical multipart upload does not belong to this branch and key",
        ));
    }
    Ok(session)
}
fn utf8_key(key: &[u8]) -> Result<String> {
    String::from_utf8(key.to_vec())
        .map_err(|_| Error::new(ErrorCode::InvalidKey, "logical key is not valid UTF-8"))
}
fn datetime(millis: u64) -> Result<DateTime> {
    Ok(DateTime::from_millis(i64::try_from(millis).map_err(
        |_| Error::new(ErrorCode::InternalInvariant, "timestamp exceeds SDK range"),
    )?))
}
fn i64_len(len: u64) -> Result<i64> {
    i64::try_from(len).map_err(|_| {
        Error::new(
            ErrorCode::InternalInvariant,
            "object length exceeds SDK range",
        )
    })
}
fn live_etag_size(summary: &ObjectSummary) -> Result<(String, u64)> {
    match &summary.version.body.kind {
        LogicalObjectVersionKindV1::Live {
            logical_etag, size, ..
        } => Ok((logical_etag.clone(), *size)),
        LogicalObjectVersionKindV1::DeleteMarker => Err(Error::new(
            ErrorCode::NoSuchKey,
            "current version is a delete marker",
        )),
    }
}
fn live_sha256(summary: &ObjectSummary) -> Result<Option<[u8; 32]>> {
    match &summary.version.body.kind {
        LogicalObjectVersionKindV1::Live { checksums, .. } => Ok(checksums.sha256),
        LogicalObjectVersionKindV1::DeleteMarker => Err(Error::new(
            ErrorCode::NoSuchKey,
            "current version is a delete marker",
        )),
    }
}
fn decode_checksum<const N: usize>(value: &str, field: &str) -> Result<[u8; N]> {
    let decoded = STANDARD
        .decode(value)
        .map_err(|_| invalid(format!("{field} is not valid base64")))?;
    decoded
        .try_into()
        .map_err(|_| invalid(format!("{field} must decode to exactly {N} checksum bytes")))
}
fn parse_etag_predicate(value: &str) -> Result<EtagPredicateV1> {
    let value = value.trim();
    if value == "*" {
        return Ok(EtagPredicateV1::Any);
    }
    let mut tags = std::collections::BTreeSet::new();
    for tag in value.split(',').map(str::trim) {
        if tag.len() < 2
            || !tag.starts_with('"')
            || !tag.ends_with('"')
            || tag[1..tag.len() - 1].contains('"')
            || tag.bytes().any(|byte| byte.is_ascii_control())
        {
            return Err(invalid("ETag predicates must use * or quoted ETags"));
        }
        tags.insert(tag.to_string());
    }
    if tags.is_empty() {
        return Err(invalid("ETag predicate cannot be empty"));
    }
    Ok(EtagPredicateV1::OneOf(tags))
}
fn read_predicate_matches(predicate: &EtagPredicateV1, current: Option<&str>) -> bool {
    match predicate {
        EtagPredicateV1::Any => current.is_some(),
        EtagPredicateV1::OneOf(tags) => current.is_some_and(|etag| tags.contains(etag)),
    }
}
fn validate_read_conditions(
    summary: &ObjectSummary,
    if_match: Option<&str>,
    if_none_match: Option<&str>,
    if_modified_since: Option<&DateTime>,
    if_unmodified_since: Option<&DateTime>,
) -> Result<()> {
    let current_etag = match &summary.version.body.kind {
        LogicalObjectVersionKindV1::Live { logical_etag, .. } => Some(logical_etag.as_str()),
        LogicalObjectVersionKindV1::DeleteMarker => None,
    };
    let modified_seconds =
        i64::try_from(summary.version.body.created_at_millis / 1_000).map_err(|_| {
            Error::new(
                ErrorCode::InternalInvariant,
                "timestamp exceeds i64 seconds",
            )
        })?;
    if let Some(value) = if_match {
        if !read_predicate_matches(&parse_etag_predicate(value)?, current_etag) {
            return Err(Error::new(
                ErrorCode::PreconditionFailed,
                "If-Match did not match the selected logical object",
            ));
        }
    } else if if_unmodified_since.is_some_and(|date| modified_seconds > date.secs()) {
        return Err(Error::new(
            ErrorCode::PreconditionFailed,
            "selected logical object was modified after If-Unmodified-Since",
        ));
    }
    if let Some(value) = if_none_match {
        if read_predicate_matches(&parse_etag_predicate(value)?, current_etag) {
            return Err(Error::new(
                ErrorCode::NotModified,
                "If-None-Match matched the selected logical object",
            ));
        }
    } else if if_modified_since.is_some_and(|date| modified_seconds <= date.secs()) {
        return Err(Error::new(
            ErrorCode::NotModified,
            "selected logical object was not modified after If-Modified-Since",
        ));
    }
    Ok(())
}
fn validate_checksum_mode(mode: Option<&ChecksumMode>) -> Result<()> {
    if mode.is_some_and(|mode| mode.as_str() != "ENABLED") {
        return Err(unsupported("checksum_mode"));
    }
    Ok(())
}
fn parse_range(spec: &str, len: u64) -> Result<(u64, u64)> {
    let value = spec
        .strip_prefix("bytes=")
        .ok_or_else(|| invalid("range must use a single bytes range"))?;
    if value.contains(',') {
        return Err(invalid("multiple byte ranges are unsupported"));
    }
    let (start, end) = value
        .split_once('-')
        .ok_or_else(|| invalid("range must use bytes=start-end"))?;
    if len == 0 {
        return Err(Error::new(
            ErrorCode::InvalidRange,
            "range cannot select an empty object",
        ));
    }
    let (start, end) = if start.is_empty() {
        let suffix: u64 = end.parse().map_err(|_| invalid("invalid suffix range"))?;
        if suffix == 0 {
            return Err(Error::new(
                ErrorCode::InvalidRange,
                "suffix range must be greater than zero",
            ));
        }
        (len.saturating_sub(suffix), len - 1)
    } else {
        let start: u64 = start.parse().map_err(|_| invalid("invalid range start"))?;
        let end = if end.is_empty() {
            len - 1
        } else {
            end.parse::<u64>()
                .map_err(|_| invalid("invalid range end"))?
                .min(len - 1)
        };
        (start, end)
    };
    if start > end || end >= len {
        return Err(Error::new(
            ErrorCode::InvalidRange,
            "range is not satisfiable",
        ));
    }
    Ok((start, end))
}

struct VerifiedBody {
    stream: std::sync::Mutex<BoxStream<'static, Result<bytes::Bytes>>>,
    len: u64,
}

impl Body for VerifiedBody {
    type Data = bytes::Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<std::result::Result<Frame<Self::Data>, Self::Error>>> {
        let mut stream = self.stream.lock().expect("verified body mutex poisoned");
        match stream.as_mut().poll_next(context) {
            Poll::Ready(Some(Ok(bytes))) => Poll::Ready(Some(Ok(Frame::data(bytes)))),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(std::io::Error::other(error)))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.len)
    }
}

fn streaming_body(stream: BoxStream<'static, Result<bytes::Bytes>>, len: u64) -> ByteStream {
    ByteStream::from_body_1_x(VerifiedBody {
        stream: std::sync::Mutex::new(stream),
        len,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn operation_deadlines_preserve_read_and_write_retry_semantics() {
        let read = apply_read_deadline(
            Some(Instant::now() + Duration::from_millis(5)),
            futures_util::future::pending::<Result<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(read.code, ErrorCode::Timeout);
        assert_eq!(read.retry, RetryAdvice::Safe);
        assert!(read.operation_id.is_none());

        let operation = OperationId::new();
        let write = apply_write_deadline(
            Some(Instant::now() + Duration::from_millis(5)),
            operation,
            futures_util::future::pending::<Result<()>>(),
        )
        .await
        .unwrap_err();
        assert_eq!(write.code, ErrorCode::OutcomeUnknown);
        assert_eq!(write.retry, RetryAdvice::ReconcileOperation);
        assert_eq!(
            write.operation_id.as_deref(),
            Some(operation.to_string()).as_deref()
        );
    }

    #[test]
    fn elapsed_deadline_fails_before_an_operation_starts() {
        let error = validate_deadline(Some(Instant::now() - Duration::from_millis(1))).unwrap_err();
        assert_eq!(error.code, ErrorCode::Timeout);
        assert_eq!(error.retry, RetryAdvice::Never);
        assert!(error.operation_id.is_none());
    }

    #[test]
    fn physical_layout_classifies_every_stable_namespace_family() {
        assert!(PHYSICAL_PATH_FAMILIES.len() >= 23);
        for required in [
            "format/v1.cbor",
            "providers/<provider-profile-id>.cbor",
            "node-index/latest.cbor",
            "node-index/v2/head.cbor",
            "ref-catalog/v2/head.cbor",
            "commit-graph/v2/head.cbor",
            "node-index/checkpoints/<generation>-<checkpoint-id>.cbor",
            "commits/sha256/<2>/<2>/<commit-id>",
            "publications/v2/sha256/<2>/<2>/<publication-id>",
            "payloads/v2/<repository-id-hex>/sha256/<2>/<2>/<content-id>",
            "refs/{heads,tags}/<name-hex>",
            "refs/v2/heads/<name-hex>",
            "writers/lease.cbor",
            "authority/v2/{branches,system}/<scope-hex>/lease.cbor",
            "authority/v2/maintenance/gate.cbor",
            "gc/plans/<plan-id>.cbor",
            "gc/runs/<plan-id>.cbor",
            "gc/v2/epochs/<operation-id-hex>/head.cbor",
        ] {
            assert!(
                PHYSICAL_PATH_FAMILIES
                    .iter()
                    .any(|family| family.relative_pattern == required),
                "missing physical path family {required}"
            );
        }
        assert!(PHYSICAL_PATH_FAMILIES
            .iter()
            .filter(|family| family.portable_clone)
            .all(|family| family.discipline != PhysicalPathDiscipline::EphemeralProbe));
        assert!(PHYSICAL_PATH_FAMILIES
            .iter()
            .filter(|family| family.gc_managed)
            .all(|family| family.discipline == PhysicalPathDiscipline::Immutable));
    }

    #[test]
    fn multipart_upload_handle_is_self_contained_across_processes() {
        let session = PhysicalMultipartSessionV1 {
            repository: prolly_s3_core::RepositoryId::from_hash([7; 32]),
            branch: "main".to_string(),
            key: b"large/archive.bin".to_vec(),
            headers: ObjectHeaders::default(),
            user_metadata: BTreeMap::from([("purpose".to_string(), "backup".to_string())]),
            provider_upload_id: "provider-upload-id".to_string(),
            operation: OperationId::new(),
            writer_fence_generation: 3,
            created_at_millis: 123,
            discovered: false,
        };
        let handle = encode_physical_multipart_session(&session).unwrap();
        let bytes = URL_SAFE_NO_PAD
            .decode(
                handle
                    .strip_prefix(PHYSICAL_MULTIPART_HANDLE_PREFIX)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            decode_canonical::<PhysicalMultipartSessionV1>(&bytes).unwrap(),
            session
        );
    }

    #[test]
    fn managed_cursor_key_rotation_enforces_ttl_plus_skew() {
        let now = 10_000_000;
        let ttl = Duration::from_secs(15 * 60);
        let skew = Duration::from_secs(5 * 60);
        let old = HmacTokenSigner::single("old", vec![1; 32]).unwrap();
        let (old_id, old_signature) = old.sign(b"cursor").unwrap();

        let retired_at = now - 60_000;
        let rotated = HmacTokenSigner::managed(
            "new",
            [
                HmacTokenKey::retained("new", vec![2; 32]),
                HmacTokenKey::retired("old", vec![1; 32], retired_at),
            ],
        )
        .unwrap();
        rotated.validate_lifecycle(ttl, skew, now).unwrap();
        rotated.verify(&old_id, b"cursor", &old_signature).unwrap();
        assert_eq!(rotated.sign(b"new-cursor").unwrap().0, "new");

        let removed_too_soon = HmacTokenSigner::managed(
            "new",
            [
                HmacTokenKey::retained("new", vec![2; 32]),
                HmacTokenKey::removed("old", retired_at),
            ],
        )
        .unwrap();
        assert_eq!(
            removed_too_soon
                .validate_lifecycle(ttl, skew, now)
                .unwrap_err()
                .code,
            ErrorCode::InvalidRequest
        );

        let retention_millis = duration_millis(ttl + skew, "retention").unwrap();
        let removed_after_window = HmacTokenSigner::managed(
            "new",
            [
                HmacTokenKey::retained("new", vec![2; 32]),
                HmacTokenKey::removed("old", now - retention_millis - 1),
            ],
        )
        .unwrap();
        removed_after_window
            .validate_lifecycle(ttl, skew, now)
            .unwrap();
        assert_eq!(
            removed_after_window
                .verify(&old_id, b"cursor", &old_signature)
                .unwrap_err()
                .code,
            ErrorCode::InvalidContinuationToken
        );
    }

    #[test]
    fn cursor_key_configuration_rejects_ambiguous_or_unsafe_inventory() {
        assert_eq!(
            HmacTokenSigner::managed(
                "active",
                [
                    HmacTokenKey::retained("active", vec![1; 32]),
                    HmacTokenKey::retained("active", vec![2; 32]),
                ],
            )
            .err()
            .unwrap()
            .code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            HmacTokenSigner::managed("active", [HmacTokenKey::retired("active", vec![1; 32], 1)],)
                .err()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            HmacTokenSigner::single("contains whitespace", vec![1; 32])
                .err()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn byte_ranges_distinguish_malformed_from_unsatisfiable_requests() {
        assert_eq!(parse_range("bytes=1-3", 6).unwrap(), (1, 3));
        for range in ["bytes=999-1000", "bytes=4-2", "bytes=-0"] {
            assert_eq!(
                parse_range(range, 6).unwrap_err().code,
                ErrorCode::InvalidRange
            );
        }
        assert_eq!(
            parse_range("items=1-3", 6).unwrap_err().code,
            ErrorCode::InvalidRequest
        );
    }

    #[tokio::test]
    async fn client_builder_rejects_invalid_cursor_time_policy_before_io() {
        assert_eq!(
            Client::builder()
                .cursor_ttl(Duration::ZERO)
                .open()
                .await
                .err()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            Client::builder()
                .cursor_clock_skew(Duration::from_secs(15 * 60 + 1))
                .open()
                .await
                .err()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
        assert_eq!(
            Client::builder()
                .max_staged_batch_bytes(0)
                .open()
                .await
                .err()
                .unwrap()
                .code,
            ErrorCode::InvalidRequest
        );
    }
}
