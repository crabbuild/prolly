use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aws_sdk_s3::types::BucketVersioningStatus;
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use hmac::{Hmac, Mac};
use prolly_s3_core::{
    decode_canonical, encode_canonical, BucketClass, CompareExchange, CompareExchangeOutcome,
    Error, ErrorCode, GetRequest, ImmutablePut, ListRequest, ObjectPath, ObjectPlane,
    PhysicalVersion, PhysicalVersioning, ProviderAttestationBodyV1, ProviderAttestationV1,
    ProviderCapabilities, ProviderProfileId, Result,
};
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::AwsS3ObjectPlane;

const PROBE_SUITE_VERSION: u32 = 1;
const SDK_VERSION: &str = "aws-sdk-s3/1.140.0;prolly-s3-client/0.1.0";
const MAX_CLOCK_SKEW_MILLIS: u64 = 5 * 60 * 1_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProviderIdentity {
    endpoint: String,
    region: String,
    path_style: bool,
    bucket_class: BucketClass,
}

impl ProviderIdentity {
    pub fn s3_compatible(endpoint: impl Into<String>, region: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            region: region.into(),
            path_style: true,
            bucket_class: BucketClass::S3Compatible,
        }
    }

    pub fn aws_region(region: impl Into<String>) -> Self {
        let region = region.into();
        Self {
            endpoint: format!("aws:s3:{region}"),
            region,
            path_style: false,
            bucket_class: BucketClass::GeneralPurpose,
        }
    }

    pub fn path_style(mut self, value: bool) -> Self {
        self.path_style = value;
        self
    }

    pub fn bucket_class(&self) -> BucketClass {
        self.bucket_class
    }

    fn endpoint_fingerprint(&self) -> Result<[u8; 32]> {
        #[derive(Serialize)]
        struct Fingerprint<'a> {
            endpoint: &'a str,
            region: &'a str,
            path_style: bool,
            bucket_class: BucketClass,
        }
        let bytes = encode_canonical(&Fingerprint {
            endpoint: &self.endpoint,
            region: &self.region,
            path_style: self.path_style,
            bucket_class: self.bucket_class,
        })?;
        Ok(domain_digest(b"prolly-s3/provider-endpoint/v1", &[&bytes]))
    }

    fn bucket_fingerprint(&self, bucket: &str) -> Result<[u8; 32]> {
        Ok(domain_digest(
            b"prolly-s3/provider-bucket/v1",
            &[&self.endpoint_fingerprint()?, bucket.as_bytes()],
        ))
    }
}

pub(crate) fn validate_provider_bucket(identity: &ProviderIdentity, bucket: &str) -> Result<()> {
    if bucket.is_empty() {
        return Err(Error::new(
            ErrorCode::InvalidBucket,
            "bucket must not be empty",
        ));
    }
    if identity.bucket_class == BucketClass::GeneralPurpose
        && (bucket.starts_with("arn:")
            || bucket.ends_with("--x-s3")
            || bucket.ends_with("-s3alias")
            || bucket.ends_with("--ol-s3")
            || bucket.ends_with("--op-s3")
            || bucket.ends_with(".mrap"))
    {
        return Err(Error::new(
            ErrorCode::ProviderNotQualified,
            "directory, access-point, Object Lambda, Outposts, and multi-region access-point bucket identifiers are outside the general-purpose S3 profile",
        ));
    }
    Ok(())
}

pub trait AttestationSigner: Send + Sync {
    fn active_key_id(&self) -> &str;
    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>>;
    fn verify(&self, key_id: &str, payload: &[u8], signature: &[u8]) -> Result<()>;
}

pub struct HmacAttestationSigner {
    active_key: String,
    keys: std::collections::HashMap<String, Vec<u8>>,
}

impl HmacAttestationSigner {
    pub fn new(
        active_key: impl Into<String>,
        keys: impl IntoIterator<Item = (String, Vec<u8>)>,
    ) -> Result<Self> {
        let active_key = active_key.into();
        let keys = keys
            .into_iter()
            .collect::<std::collections::HashMap<_, _>>();
        if !keys.contains_key(&active_key) {
            return Err(invalid(
                "active attestation key is absent from the key ring",
            ));
        }
        if keys.values().any(|key| key.len() < 32) {
            return Err(invalid("attestation keys must contain at least 32 bytes"));
        }
        Ok(Self { active_key, keys })
    }

    pub fn single(key_id: impl Into<String>, key: impl Into<Vec<u8>>) -> Result<Self> {
        let key_id = key_id.into();
        Self::new(key_id.clone(), [(key_id, key.into())])
    }
}

impl AttestationSigner for HmacAttestationSigner {
    fn active_key_id(&self) -> &str {
        &self.active_key
    }

    fn sign(&self, payload: &[u8]) -> Result<Vec<u8>> {
        let key = self
            .keys
            .get(&self.active_key)
            .expect("active key validated");
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|_| invalid("invalid attestation signing key"))?;
        mac.update(payload);
        Ok(mac.finalize().into_bytes().to_vec())
    }

    fn verify(&self, key_id: &str, payload: &[u8], signature: &[u8]) -> Result<()> {
        let key = self.keys.get(key_id).ok_or_else(|| {
            Error::new(
                ErrorCode::ProviderNotQualified,
                "unknown provider-attestation signing key",
            )
        })?;
        let mut mac = Hmac::<Sha256>::new_from_slice(key)
            .map_err(|_| invalid("invalid attestation verification key"))?;
        mac.update(payload);
        mac.verify_slice(signature).map_err(|_| {
            Error::new(
                ErrorCode::ProviderNotQualified,
                "provider-attestation signature mismatch",
            )
        })
    }
}

#[derive(Clone, Debug)]
pub struct ProviderQualificationOptions {
    pub validity: Duration,
}

impl Default for ProviderQualificationOptions {
    fn default() -> Self {
        Self {
            validity: Duration::from_secs(24 * 60 * 60),
        }
    }
}

pub(crate) async fn qualify_and_store(
    plane: Arc<AwsS3ObjectPlane>,
    repository_prefix: &str,
    identity: &ProviderIdentity,
    signer: &dyn AttestationSigner,
    options: &ProviderQualificationOptions,
) -> Result<ProviderAttestationV1> {
    if options.validity < Duration::from_secs(60)
        || options.validity > Duration::from_secs(30 * 24 * 60 * 60)
    {
        return Err(invalid(
            "provider attestation validity must be between 1 minute and 30 days",
        ));
    }
    let observed_at_millis = now_millis()?;
    let expires_at_millis = observed_at_millis
        .checked_add(
            u64::try_from(options.validity.as_millis())
                .map_err(|_| invalid("provider attestation validity exceeds millisecond range"))?,
        )
        .ok_or_else(|| invalid("provider attestation expiry overflow"))?;
    let capabilities = probe_provider(plane.clone(), repository_prefix).await?;
    capabilities.validate_prolly_s3()?;
    let body = ProviderAttestationBodyV1 {
        endpoint_fingerprint: identity.endpoint_fingerprint()?,
        bucket_fingerprint: identity.bucket_fingerprint(plane.bucket())?,
        bucket_class: identity.bucket_class(),
        capabilities,
        probe_suite_version: PROBE_SUITE_VERSION,
        sdk_version: SDK_VERSION.to_string(),
        observed_at_millis,
        expires_at_millis,
        signer_key_id: signer.active_key_id().to_string(),
    };
    let id = body.id()?;
    let attestation = ProviderAttestationV1 {
        id,
        signature: signer.sign(id.as_bytes())?,
        body,
    };
    let bytes = encode_canonical(&attestation)?;
    plane
        .put_immutable(ImmutablePut {
            path: attestation_path(repository_prefix, id)?,
            expected_sha256: Sha256::digest(&bytes).into(),
            bytes,
        })
        .await?;
    Ok(attestation)
}

pub(crate) async fn load_valid_attestation(
    plane: Arc<AwsS3ObjectPlane>,
    repository_prefix: &str,
    identity: &ProviderIdentity,
    signer: &dyn AttestationSigner,
    selected: Option<ProviderProfileId>,
) -> Result<ProviderAttestationV1> {
    let expected_endpoint = identity.endpoint_fingerprint()?;
    let expected_bucket = identity.bucket_fingerprint(plane.bucket())?;
    let now = now_millis()?;
    let mut candidates = Vec::new();
    if let Some(id) = selected {
        let object = plane
            .get(GetRequest {
                path: attestation_path(repository_prefix, id)?,
                range: None,
                physical_version: None,
            })
            .await?
            .ok_or_else(|| not_qualified("selected provider attestation does not exist"))?;
        candidates.push(decode_canonical::<ProviderAttestationV1>(&object.bytes)?);
    } else {
        let prefix = attestation_prefix(repository_prefix);
        let mut continuation = None;
        loop {
            let page = plane
                .list(ListRequest {
                    prefix: prefix.clone(),
                    continuation,
                    limit: 1_000,
                    include_versions: false,
                })
                .await?;
            for entry in page.entries {
                let object = plane
                    .get(GetRequest {
                        path: entry.path,
                        range: None,
                        physical_version: None,
                    })
                    .await?
                    .ok_or_else(|| not_qualified("listed attestation disappeared"))?;
                candidates.push(decode_canonical::<ProviderAttestationV1>(&object.bytes)?);
            }
            continuation = page.continuation;
            if continuation.is_none() {
                break;
            }
        }
    }

    let mut valid = Vec::new();
    for candidate in candidates {
        candidate.validate_id()?;
        signer.verify(
            &candidate.body.signer_key_id,
            candidate.id.as_bytes(),
            &candidate.signature,
        )?;
        if candidate.body.endpoint_fingerprint != expected_endpoint
            || candidate.body.bucket_fingerprint != expected_bucket
            || candidate.body.bucket_class != identity.bucket_class()
        {
            if selected.is_some() {
                return Err(not_qualified(
                    "selected attestation belongs to a different endpoint or bucket",
                ));
            }
            continue;
        }
        if candidate.body.probe_suite_version != PROBE_SUITE_VERSION {
            continue;
        }
        if candidate.body.observed_at_millis > now.saturating_add(MAX_CLOCK_SKEW_MILLIS)
            || candidate.body.expires_at_millis <= now
        {
            if selected.is_some() {
                return Err(not_qualified(
                    "selected provider attestation is expired or future-dated",
                ));
            }
            continue;
        }
        candidate.body.capabilities.validate_prolly_s3()?;
        valid.push(candidate);
    }
    valid
        .into_iter()
        .max_by_key(|item| (item.body.observed_at_millis, item.id))
        .ok_or_else(|| not_qualified("no matching nonexpired provider attestation exists"))
}

pub(crate) fn ensure_attestation_current(attestation: &ProviderAttestationV1) -> Result<()> {
    if attestation.body.expires_at_millis <= now_millis()? {
        return Err(not_qualified(
            "provider attestation expired; explicitly refresh or requalify",
        ));
    }
    Ok(())
}

async fn probe_provider(
    plane: Arc<AwsS3ObjectPlane>,
    repository_prefix: &str,
) -> Result<ProviderCapabilities> {
    let probe = format!(
        "{repository_prefix}/probes/{}/",
        prolly_s3_core::OperationId::new()
    );
    let mutable = ObjectPath::new(format!("{probe}mutable"))?;
    let listed = ObjectPath::new(format!("{probe}listed"))?;

    let first = match plane
        .compare_exchange(CompareExchange {
            path: mutable.clone(),
            expected: None,
            bytes: b"probe-first".to_vec(),
        })
        .await?
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        CompareExchangeOutcome::Conflict(_) => {
            return Err(not_qualified("isolated conditional create conflicted"));
        }
    };
    if !matches!(
        plane
            .compare_exchange(CompareExchange {
                path: mutable.clone(),
                expected: None,
                bytes: b"must-not-win".to_vec(),
            })
            .await?,
        CompareExchangeOutcome::Conflict(_)
    ) {
        return Err(not_qualified("provider ignored conditional create"));
    }
    let loaded = plane
        .load_mutable(&mutable)
        .await?
        .ok_or_else(|| not_qualified("created probe object was not immediately readable"))?;
    if loaded.bytes != b"probe-first" {
        return Err(not_qualified("probe read returned different bytes"));
    }
    let ranged = plane
        .get(GetRequest {
            path: mutable.clone(),
            range: Some(1..=4),
            physical_version: None,
        })
        .await?
        .ok_or_else(|| not_qualified("ranged probe read returned no object"))?;
    if ranged.bytes != b"robe" {
        return Err(not_qualified("provider returned an incorrect byte range"));
    }
    let second = match plane
        .compare_exchange(CompareExchange {
            path: mutable.clone(),
            expected: Some(first.token.clone()),
            bytes: b"probe-second".to_vec(),
        })
        .await?
    {
        CompareExchangeOutcome::Applied(metadata) => metadata,
        CompareExchangeOutcome::Conflict(_) => {
            return Err(not_qualified(
                "conditional update rejected its current token",
            ));
        }
    };
    if !matches!(
        plane
            .compare_exchange(CompareExchange {
                path: mutable.clone(),
                expected: Some(first.token.clone()),
                bytes: b"stale-must-not-win".to_vec(),
            })
            .await?,
        CompareExchangeOutcome::Conflict(_)
    ) {
        return Err(not_qualified(
            "provider accepted a stale conditional update",
        ));
    }
    let listed_bytes = b"probe-list".to_vec();
    plane
        .put_immutable(ImmutablePut {
            path: listed.clone(),
            expected_sha256: Sha256::digest(&listed_bytes).into(),
            bytes: listed_bytes,
        })
        .await?;
    let page_one = plane
        .list(ListRequest {
            prefix: probe.clone(),
            continuation: None,
            limit: 1,
            include_versions: false,
        })
        .await?;
    let continuation = page_one
        .continuation
        .clone()
        .ok_or_else(|| not_qualified("provider did not paginate a one-item page"))?;
    let page_two = plane
        .list(ListRequest {
            prefix: probe.clone(),
            continuation: Some(continuation),
            limit: 1,
            include_versions: false,
        })
        .await?;
    if page_one.entries.len() != 1 || page_two.entries.len() != 1 {
        return Err(not_qualified(
            "paged listing omitted or duplicated probe objects",
        ));
    }
    let physical = plane
        .list(ListRequest {
            prefix: probe.clone(),
            continuation: None,
            limit: 100,
            include_versions: true,
        })
        .await?;
    if physical.entries.len() < 2 {
        return Err(not_qualified(
            "physical version listing omitted probe objects",
        ));
    }

    let physical_versioning = bucket_versioning(plane.client(), plane.bucket()).await?;
    if matches!(
        physical_versioning,
        PhysicalVersioning::Enabled | PhysicalVersioning::Suspended
    ) {
        if let Some(version_id) = first.token.version_id.as_ref() {
            plane
                .delete_exact(
                    &mutable,
                    PhysicalVersion::Versioned {
                        version_id: version_id.clone(),
                    },
                )
                .await?;
            let current = plane
                .load_mutable(&mutable)
                .await?
                .ok_or_else(|| not_qualified("exact old-version delete removed current data"))?;
            if current.bytes != b"probe-second" {
                return Err(not_qualified("exact version delete changed current data"));
            }
        }
    }
    delete_metadata_exact(plane.as_ref(), &mutable, &second).await?;
    let listed_metadata = plane
        .head(&listed)
        .await?
        .ok_or_else(|| not_qualified("listed probe object disappeared before cleanup"))?;
    delete_metadata_exact(plane.as_ref(), &listed, &listed_metadata).await?;
    let after_delete = plane
        .list(ListRequest {
            prefix: probe,
            continuation: None,
            limit: 100,
            include_versions: false,
        })
        .await?;
    if !after_delete.entries.is_empty() {
        return Err(not_qualified(
            "current listing remained stale after exact deletes",
        ));
    }

    let conflicting_lifecycle_rule = lifecycle_conflicts(plane.client(), plane.bucket()).await?;
    let default_object_lock_retention =
        object_lock_conflicts(plane.client(), plane.bucket()).await?;
    Ok(ProviderCapabilities {
        conditional_create: true,
        conditional_update: true,
        strong_get_after_put: true,
        strong_list_after_put: true,
        strong_list_after_delete: true,
        ranged_get: true,
        paged_list: true,
        list_physical_versions: true,
        exact_version_delete: true,
        physical_versioning,
        conflicting_lifecycle_rule,
        default_object_lock_retention,
        max_object_bytes: 5 * 1_024 * 1_024 * 1_024 * 1_024,
        max_single_put_bytes: 5 * 1_024 * 1_024 * 1_024,
    })
}

async fn delete_metadata_exact(
    plane: &AwsS3ObjectPlane,
    path: &ObjectPath,
    metadata: &prolly_s3_core::StoredMetadata,
) -> Result<()> {
    let version = match metadata.token.version_id.as_ref() {
        Some(version_id) if version_id != "null" => PhysicalVersion::Versioned {
            version_id: version_id.clone(),
        },
        _ => PhysicalVersion::Unversioned {
            token: Some(metadata.token.clone()),
        },
    };
    plane.delete_exact(path, version).await?;
    Ok(())
}

async fn bucket_versioning(
    client: &aws_sdk_s3::Client,
    bucket: &str,
) -> Result<PhysicalVersioning> {
    let output = client
        .get_bucket_versioning()
        .bucket(bucket)
        .send()
        .await
        .map_err(|error| provider_error("GetBucketVersioning", error.code(), &error))?;
    Ok(match output.status() {
        Some(BucketVersioningStatus::Enabled) => PhysicalVersioning::Enabled,
        Some(BucketVersioningStatus::Suspended) => PhysicalVersioning::Suspended,
        _ => PhysicalVersioning::Unversioned,
    })
}

async fn lifecycle_conflicts(client: &aws_sdk_s3::Client, bucket: &str) -> Result<bool> {
    match client
        .get_bucket_lifecycle_configuration()
        .bucket(bucket)
        .send()
        .await
    {
        Ok(output) => Ok(!output.rules().is_empty()),
        Err(error) if absent_configuration(error.code()) => Ok(false),
        Err(error) => Err(provider_error(
            "GetBucketLifecycleConfiguration",
            error.code(),
            &error,
        )),
    }
}

async fn object_lock_conflicts(client: &aws_sdk_s3::Client, bucket: &str) -> Result<bool> {
    match client
        .get_object_lock_configuration()
        .bucket(bucket)
        .send()
        .await
    {
        Ok(output) => Ok(output
            .object_lock_configuration()
            .and_then(|configuration| configuration.rule())
            .is_some()),
        Err(error) if absent_configuration(error.code()) => Ok(false),
        Err(error) => Err(provider_error(
            "GetObjectLockConfiguration",
            error.code(),
            &error,
        )),
    }
}

fn absent_configuration(code: Option<&str>) -> bool {
    matches!(
        code,
        Some(
            "NoSuchLifecycleConfiguration"
                | "NoSuchObjectLockConfiguration"
                | "ObjectLockConfigurationNotFoundError"
                | "NoSuchConfiguration"
                | "NotFound"
        )
    )
}

fn attestation_prefix(repository_prefix: &str) -> String {
    format!("{repository_prefix}/providers/")
}

fn attestation_path(repository_prefix: &str, id: ProviderProfileId) -> Result<ObjectPath> {
    ObjectPath::new(format!(
        "{}{id}.cbor",
        attestation_prefix(repository_prefix)
    ))
}

fn domain_digest(domain: &[u8], values: &[&[u8]]) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update((domain.len() as u64).to_be_bytes());
    hasher.update(domain);
    for value in values {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value);
    }
    hasher.finalize().into()
}

fn now_millis() -> Result<u64> {
    let elapsed = SystemTime::now().duration_since(UNIX_EPOCH).map_err(|_| {
        Error::new(
            ErrorCode::InternalInvariant,
            "system clock precedes the Unix epoch",
        )
    })?;
    u64::try_from(elapsed.as_millis()).map_err(|_| {
        Error::new(
            ErrorCode::InternalInvariant,
            "system clock exceeds u64 millis",
        )
    })
}

fn provider_error<E: std::fmt::Debug>(operation: &str, code: Option<&str>, error: &E) -> Error {
    Error::new(
        ErrorCode::ProviderNotQualified,
        format!("provider qualification {operation} failed ({code:?}): {error:?}"),
    )
    .provider_metadata(code.map(ToString::to_string), None::<String>)
}

fn not_qualified(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::ProviderNotQualified, message)
}

fn invalid(message: impl Into<String>) -> Error {
    Error::new(ErrorCode::InvalidRequest, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capabilities() -> ProviderCapabilities {
        ProviderCapabilities {
            conditional_create: true,
            conditional_update: true,
            strong_get_after_put: true,
            strong_list_after_put: true,
            strong_list_after_delete: true,
            ranged_get: true,
            paged_list: true,
            list_physical_versions: true,
            exact_version_delete: true,
            physical_versioning: PhysicalVersioning::Unversioned,
            conflicting_lifecycle_rule: false,
            default_object_lock_retention: false,
            max_object_bytes: 1,
            max_single_put_bytes: 1,
        }
    }

    #[test]
    fn expired_and_tampered_attestations_fail_closed() {
        let signer = HmacAttestationSigner::single("test", vec![3_u8; 32]).unwrap();
        let now = now_millis().unwrap();
        let body = ProviderAttestationBodyV1 {
            endpoint_fingerprint: [1; 32],
            bucket_fingerprint: [2; 32],
            bucket_class: BucketClass::S3Compatible,
            capabilities: capabilities(),
            probe_suite_version: PROBE_SUITE_VERSION,
            sdk_version: SDK_VERSION.to_string(),
            observed_at_millis: now.saturating_sub(2),
            expires_at_millis: now.saturating_sub(1),
            signer_key_id: signer.active_key_id().to_string(),
        };
        let id = body.id().unwrap();
        let mut attestation = ProviderAttestationV1 {
            id,
            signature: signer.sign(id.as_bytes()).unwrap(),
            body,
        };
        assert_eq!(
            ensure_attestation_current(&attestation).unwrap_err().code,
            ErrorCode::ProviderNotQualified
        );
        attestation.signature[0] ^= 1;
        assert_eq!(
            signer
                .verify(
                    &attestation.body.signer_key_id,
                    attestation.id.as_bytes(),
                    &attestation.signature,
                )
                .unwrap_err()
                .code,
            ErrorCode::ProviderNotQualified
        );
    }

    #[test]
    fn capability_requirements_reject_unsafe_profiles() {
        let mut missing_cas = capabilities();
        missing_cas.conditional_update = false;
        assert_eq!(
            missing_cas.validate_required().unwrap_err().code,
            ErrorCode::MissingCapability
        );
        let mut lifecycle = capabilities();
        lifecycle.conflicting_lifecycle_rule = true;
        assert_eq!(
            lifecycle.validate_required().unwrap_err().code,
            ErrorCode::ProviderNotQualified
        );
    }

    #[test]
    fn aws_general_purpose_identity_rejects_non_bucket_addressing_profiles() {
        let identity = ProviderIdentity::aws_region("us-west-2");
        for bucket in [
            "arn:aws:s3:us-west-2:123456789012:accesspoint/example",
            "example--usw2-az1--x-s3",
            "example-s3alias",
            "example--ol-s3",
            "example--op-s3",
            "example.mrap",
        ] {
            assert_eq!(
                validate_provider_bucket(&identity, bucket)
                    .unwrap_err()
                    .code,
                ErrorCode::ProviderNotQualified
            );
        }
        validate_provider_bucket(&identity, "ordinary-bucket").unwrap();
        validate_provider_bucket(
            &ProviderIdentity::s3_compatible("http://localhost:9000", "us-east-1"),
            "local_bucket",
        )
        .unwrap();
    }
}
