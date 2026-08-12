use std::{collections::BTreeMap, sync::Arc, time::Duration};

use prolly_s3_core::{
    BranchIndexAdvanceReportV2, CommitIdV2, CommitReceiptV2, Error, ErrorCode, ObjectDataV2,
    ObjectHeaders, ObjectSummaryV2, ObjectVersionV2, ProviderAttestationV1,
    ProviderPerKeyVersionLimitV2, ProviderProfileId, RepositoryV2, RepositoryV2Options, Result,
    VersionSummaryV2,
};

use crate::{
    ensure_attestation_current, load_valid_attestation, qualify_and_store,
    validate_provider_bucket, AttestationSigner, AwsS3ObjectPlane, ProviderIdentity,
    ProviderQualificationOptions, S3OperationMetrics,
};

/// Application-facing native protocol-v2 client.
///
/// V2 repositories are physically and logically separate from legacy v1
/// repositories. This client never dual-writes v1 state.
#[derive(Clone)]
pub struct ClientV2 {
    repository: Arc<RepositoryV2<AwsS3ObjectPlane>>,
    bucket: String,
    branch: String,
    provider_attestation: ProviderAttestationV1,
}

#[derive(Default)]
pub struct ClientV2Builder {
    aws_client: Option<aws_sdk_s3::Client>,
    bucket: Option<String>,
    repository_prefix: Option<String>,
    default_branch: Option<String>,
    writer: Option<String>,
    authority_lease_duration: Option<Duration>,
    read_only: bool,
    max_cached_node_pack_bytes: Option<usize>,
    max_cached_node_locations: Option<usize>,
    max_cached_node_bytes: Option<usize>,
    node_cache: Option<Arc<dyn prolly_s3_core::NodeCache>>,
    mutable_control_versions_to_retain: Option<usize>,
    provider_identity: Option<ProviderIdentity>,
    attestation_signer: Option<Arc<dyn AttestationSigner>>,
    provider_attestation: Option<ProviderProfileId>,
    qualification_options: Option<ProviderQualificationOptions>,
    provider_per_key_version_limit: Option<ProviderPerKeyVersionLimitV2>,
}

impl ClientV2 {
    pub fn builder() -> ClientV2Builder {
        ClientV2Builder::default()
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

    pub fn for_branch(&self, branch: impl Into<String>) -> Result<Self> {
        let branch = branch.into();
        prolly_s3_core::validate_branch(&branch)?;
        let mut client = self.clone();
        client.branch = branch;
        Ok(client)
    }

    pub async fn put_object(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
    ) -> Result<CommitReceiptV2> {
        self.put_object_with_metadata(key, bytes, ObjectHeaders::default(), BTreeMap::new())
            .await
    }

    pub async fn put_object_with_metadata(
        &self,
        key: impl Into<String>,
        bytes: Vec<u8>,
        headers: ObjectHeaders,
        metadata: BTreeMap<String, String>,
    ) -> Result<CommitReceiptV2> {
        self.ensure_provider_qualified()?;
        self.repository
            .put_object(
                &self.branch,
                key.into().into_bytes(),
                bytes,
                headers,
                metadata,
            )
            .await
    }

    pub async fn get_object(&self, key: impl AsRef<str>) -> Result<Option<ObjectDataV2>> {
        self.ensure_provider_qualified()?;
        self.repository
            .get_object(&self.branch, key.as_ref().as_bytes())
            .await
    }

    pub async fn get_object_at(
        &self,
        snapshot: CommitIdV2,
        key: impl AsRef<str>,
    ) -> Result<Option<ObjectDataV2>> {
        self.ensure_provider_qualified()?;
        self.repository
            .get_object_at(&self.branch, snapshot, key.as_ref().as_bytes())
            .await
    }

    pub async fn delete_object(&self, key: impl Into<String>) -> Result<CommitReceiptV2> {
        self.ensure_provider_qualified()?;
        self.repository
            .delete_object(&self.branch, key.into().into_bytes())
            .await
    }

    pub async fn list_objects(
        &self,
        prefix: impl AsRef<str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(CommitIdV2, Vec<ObjectSummaryV2>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_objects(
                &self.branch,
                prefix.as_ref().as_bytes(),
                after.map(str::as_bytes),
                limit,
            )
            .await
    }

    pub async fn list_objects_at(
        &self,
        snapshot: CommitIdV2,
        prefix: impl AsRef<str>,
        after: Option<&str>,
        limit: usize,
    ) -> Result<(Vec<ObjectSummaryV2>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_objects_at(
                &self.branch,
                snapshot,
                prefix.as_ref().as_bytes(),
                after.map(str::as_bytes),
                limit,
            )
            .await
    }

    pub async fn list_object_versions(
        &self,
        key: impl AsRef<str>,
        limit: usize,
    ) -> Result<(CommitIdV2, Vec<ObjectVersionV2>)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_object_versions(&self.branch, key.as_ref().as_bytes(), limit)
            .await
    }

    pub async fn list_versions_prefix(
        &self,
        prefix: impl AsRef<str>,
        limit: usize,
    ) -> Result<(CommitIdV2, Vec<VersionSummaryV2>)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_versions_prefix(&self.branch, prefix.as_ref().as_bytes(), limit)
            .await
    }

    pub async fn list_versions_at(
        &self,
        snapshot: CommitIdV2,
        prefix: impl AsRef<str>,
        after: Option<&[u8]>,
        limit: usize,
    ) -> Result<(Vec<VersionSummaryV2>, bool)> {
        self.ensure_provider_qualified()?;
        self.repository
            .list_versions_at(
                &self.branch,
                snapshot,
                prefix.as_ref().as_bytes(),
                after,
                limit,
            )
            .await
    }

    pub async fn takeover_branch_writer(
        &self,
        branch: impl AsRef<str>,
        expected_writer: &str,
        expected_generation: u64,
        handoff_evidence: &str,
    ) -> Result<u64> {
        self.ensure_provider_qualified()?;
        self.repository
            .takeover_branch_writer(
                branch.as_ref(),
                expected_writer,
                expected_generation,
                handoff_evidence,
            )
            .await
    }

    pub async fn advance_branch_indexes(&self) -> Result<BranchIndexAdvanceReportV2> {
        self.ensure_provider_qualified()?;
        self.repository.advance_branch_indexes(&self.branch).await
    }

    pub fn s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().metrics()
    }

    pub fn reset_s3_operation_metrics(&self) -> S3OperationMetrics {
        self.repository.plane().reset_metrics()
    }

    fn ensure_provider_qualified(&self) -> Result<()> {
        ensure_attestation_current(&self.provider_attestation)?;
        self.provider_attestation
            .body
            .capabilities
            .validate_prolly_s3()
    }
}

impl ClientV2Builder {
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

    pub fn authority_lease_duration(mut self, duration: Duration) -> Self {
        self.authority_lease_duration = Some(duration);
        self
    }

    pub fn read_only(mut self, read_only: bool) -> Self {
        self.read_only = read_only;
        self
    }

    pub fn node_cache(mut self, cache: Arc<dyn prolly_s3_core::NodeCache>) -> Self {
        self.node_cache = Some(cache);
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

    pub fn mutable_control_version_retention(mut self, versions: usize) -> Self {
        self.mutable_control_versions_to_retain = Some(versions);
        self
    }

    /// Provider-attested maximum number of physical versions for one key.
    /// Unknown limits fail closed for native v2 initialization and open.
    pub fn provider_per_key_version_limit(mut self, limit: ProviderPerKeyVersionLimitV2) -> Self {
        self.provider_per_key_version_limit = Some(limit);
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

    pub async fn initialize(self) -> Result<ClientV2> {
        self.finish(true).await
    }

    pub async fn open(self) -> Result<ClientV2> {
        self.finish(false).await
    }

    async fn finish(self, initialize: bool) -> Result<ClientV2> {
        if initialize && self.read_only {
            return Err(invalid(
                "native v2 initialization requires a writable client",
            ));
        }
        let aws = self
            .aws_client
            .ok_or_else(|| invalid("aws_client is required"))?;
        let bucket = self.bucket.ok_or_else(|| invalid("bucket is required"))?;
        let identity = self
            .provider_identity
            .ok_or_else(|| invalid("provider_identity is required"))?;
        validate_provider_bucket(&identity, &bucket)?;
        let signer = self
            .attestation_signer
            .ok_or_else(|| invalid("attestation_signer is required"))?;
        let provider_per_key_version_limit =
            self.provider_per_key_version_limit.ok_or_else(|| {
                Error::new(
                    ErrorCode::ProviderNotQualified,
                    "native v2 requires an explicit provider per-key version-limit attestation",
                )
            })?;
        let prefix = self
            .repository_prefix
            .unwrap_or_else(|| RepositoryV2Options::default().repository_prefix);
        let plane = Arc::new(AwsS3ObjectPlane::new(aws, bucket.clone()));
        let attestation = if initialize {
            match load_valid_attestation(
                plane.clone(),
                &prefix,
                &identity,
                signer.as_ref(),
                self.provider_attestation,
            )
            .await
            {
                Ok(attestation) => attestation,
                Err(error) if error.code == ErrorCode::ProviderNotQualified => {
                    qualify_and_store(
                        plane.clone(),
                        &prefix,
                        &identity,
                        signer.as_ref(),
                        &self.qualification_options.unwrap_or_default(),
                    )
                    .await?
                }
                Err(error) => return Err(error),
            }
        } else {
            load_valid_attestation(
                plane.clone(),
                &prefix,
                &identity,
                signer.as_ref(),
                self.provider_attestation,
            )
            .await?
        };
        attestation.body.capabilities.validate_prolly_s3()?;

        let mut options = RepositoryV2Options {
            repository_prefix: prefix,
            read_only: self.read_only,
            provider_per_key_version_limit,
            ..RepositoryV2Options::default()
        };
        if let Some(branch) = self.default_branch {
            options.default_branch = branch;
        }
        if let Some(writer) = self.writer {
            options.writer = writer;
        }
        if let Some(duration) = self.authority_lease_duration {
            options.authority_lease_millis = u64::try_from(duration.as_millis())
                .map_err(|_| invalid("authority lease duration exceeds u64 milliseconds"))?;
        }
        if let Some(bytes) = self.max_cached_node_pack_bytes {
            options.max_cached_node_pack_bytes = bytes;
        }
        if let Some(locations) = self.max_cached_node_locations {
            options.max_cached_node_locations = locations;
        }
        if let Some(bytes) = self.max_cached_node_bytes {
            options.max_cached_node_bytes = bytes;
        }
        if let Some(versions) = self.mutable_control_versions_to_retain {
            options.mutable_control_versions_to_retain = versions;
        }
        options.node_cache = self.node_cache;
        let branch = options.default_branch.clone();
        let repository = if initialize {
            RepositoryV2::initialize(plane, options).await?
        } else {
            RepositoryV2::open(plane, options).await?
        };
        Ok(ClientV2 {
            repository: Arc::new(repository),
            bucket,
            branch,
            provider_attestation: attestation,
        })
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InvalidRequest, message)
}
