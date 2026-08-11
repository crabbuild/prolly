use std::{
    collections::BTreeMap,
    ops::RangeInclusive,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

use aws_sdk_s3::{primitives::ByteStream, types::MetadataDirective, Client};
use aws_smithy_types::error::metadata::ProvideErrorMetadata;
use aws_smithy_types::DateTime;
use aws_types::request_id::RequestId;
use base64::{engine::general_purpose::STANDARD, Engine as _};
use md5::Md5;
use prolly_s3_core::{
    Checksums, CompareExchange, CompareExchangeOutcome, DeleteOutcome, Error, ErrorCode,
    GetRequest, ImmutablePut, ImmutablePutOutcome, ListRequest, NativeCopy, NativeDelete,
    NativeObjectBindingV1, NativeObjectWriteResult, NativePut, ObjectPath, ObjectPlane,
    PhysicalListEntry, PhysicalListPage, PhysicalVersion, Result, RetryAdvice, StorageToken,
    StoredMetadata, StoredObject,
};
use sha2::{Digest, Sha256};

/// Object-plane calls issued to the AWS SDK and body bytes handed to or
/// collected from it. SDK-internal HTTP retries are intentionally not counted;
/// use a Smithy interceptor or provider telemetry for wire-attempt accounting.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct S3OperationMetrics {
    pub get_object: u64,
    pub head_object: u64,
    pub put_object: u64,
    pub copy_object: u64,
    pub list_objects_v2: u64,
    pub list_object_versions: u64,
    pub delete_object: u64,
    pub uploaded_body_bytes: u64,
    pub downloaded_body_bytes: u64,
}

impl S3OperationMetrics {
    pub fn total_calls(self) -> u64 {
        self.get_object
            + self.head_object
            + self.put_object
            + self.copy_object
            + self.list_objects_v2
            + self.list_object_versions
            + self.delete_object
    }
}

#[derive(Default)]
struct AtomicS3OperationMetrics {
    get_object: AtomicU64,
    head_object: AtomicU64,
    put_object: AtomicU64,
    copy_object: AtomicU64,
    list_objects_v2: AtomicU64,
    list_object_versions: AtomicU64,
    delete_object: AtomicU64,
    uploaded_body_bytes: AtomicU64,
    downloaded_body_bytes: AtomicU64,
}

impl AtomicS3OperationMetrics {
    fn snapshot(&self) -> S3OperationMetrics {
        S3OperationMetrics {
            get_object: self.get_object.load(Ordering::Relaxed),
            head_object: self.head_object.load(Ordering::Relaxed),
            put_object: self.put_object.load(Ordering::Relaxed),
            copy_object: self.copy_object.load(Ordering::Relaxed),
            list_objects_v2: self.list_objects_v2.load(Ordering::Relaxed),
            list_object_versions: self.list_object_versions.load(Ordering::Relaxed),
            delete_object: self.delete_object.load(Ordering::Relaxed),
            uploaded_body_bytes: self.uploaded_body_bytes.load(Ordering::Relaxed),
            downloaded_body_bytes: self.downloaded_body_bytes.load(Ordering::Relaxed),
        }
    }

    fn reset(&self) -> S3OperationMetrics {
        S3OperationMetrics {
            get_object: self.get_object.swap(0, Ordering::Relaxed),
            head_object: self.head_object.swap(0, Ordering::Relaxed),
            put_object: self.put_object.swap(0, Ordering::Relaxed),
            copy_object: self.copy_object.swap(0, Ordering::Relaxed),
            list_objects_v2: self.list_objects_v2.swap(0, Ordering::Relaxed),
            list_object_versions: self.list_object_versions.swap(0, Ordering::Relaxed),
            delete_object: self.delete_object.swap(0, Ordering::Relaxed),
            uploaded_body_bytes: self.uploaded_body_bytes.swap(0, Ordering::Relaxed),
            downloaded_body_bytes: self.downloaded_body_bytes.swap(0, Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct AwsS3ObjectPlane {
    client: Client,
    bucket: String,
    metrics: Arc<AtomicS3OperationMetrics>,
}

impl AwsS3ObjectPlane {
    pub fn new(client: Client, bucket: impl Into<String>) -> Self {
        Self {
            client,
            bucket: bucket.into(),
            metrics: Arc::default(),
        }
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn metrics(&self) -> S3OperationMetrics {
        self.metrics.snapshot()
    }

    /// Resets all object-plane counters and returns their previous values.
    pub fn reset_metrics(&self) -> S3OperationMetrics {
        self.metrics.reset()
    }

    async fn get_current(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.get(GetRequest {
            path: path.clone(),
            range: None,
            physical_version: None,
        })
        .await
    }
}

#[async_trait::async_trait]
impl ObjectPlane for AwsS3ObjectPlane {
    async fn get(&self, request: GetRequest) -> Result<Option<StoredObject>> {
        self.metrics.get_object.fetch_add(1, Ordering::Relaxed);
        let mut operation = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(request.path.as_str());
        if let Some(range) = request.range.as_ref() {
            operation = operation.range(format_range(range));
        }
        if let Some(PhysicalVersion::Versioned { version_id }) = request.physical_version {
            operation = operation.version_id(version_id);
        }
        let output = match operation.send().await {
            Ok(output) => output,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(map_sdk_error("GetObject", error)),
        };
        let etag = output.e_tag().unwrap_or_default().to_string();
        let version_id = output.version_id().map(ToString::to_string);
        let last_modified_millis = output
            .last_modified()
            .and_then(|value| u64::try_from(value.secs()).ok())
            .and_then(|seconds| seconds.checked_mul(1_000))
            .unwrap_or_default();
        let content_length = output.content_length();
        let delete_marker = output.delete_marker().unwrap_or(false);
        let user_metadata = output
            .metadata()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .collect();
        let collected = output
            .body
            .collect()
            .await
            .map_err(|error| transport_error("GetObject body", error))?;
        let bytes = collected.into_bytes().to_vec();
        self.metrics
            .downloaded_body_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        let digest: [u8; 32] = Sha256::digest(&bytes).into();
        Ok(Some(StoredObject {
            metadata: StoredMetadata {
                token: StorageToken { etag, version_id },
                len: content_length
                    .and_then(|value| u64::try_from(value).ok())
                    .unwrap_or(bytes.len() as u64),
                sha256: digest,
                last_modified_millis,
                delete_marker,
                user_metadata,
            },
            bytes,
        }))
    }

    async fn head(&self, path: &ObjectPath) -> Result<Option<StoredMetadata>> {
        self.metrics.head_object.fetch_add(1, Ordering::Relaxed);
        let output = match self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(path.as_str())
            .send()
            .await
        {
            Ok(output) => output,
            Err(error) if is_not_found(&error) => return Ok(None),
            Err(error) => return Err(map_sdk_error("HeadObject", error)),
        };
        let sha256 = output
            .metadata()
            .and_then(|metadata| metadata.get("prolly-sha256"))
            .and_then(|encoded| hex::decode(encoded).ok())
            .and_then(|bytes| bytes.try_into().ok())
            .unwrap_or([0u8; 32]);
        Ok(Some(StoredMetadata {
            token: StorageToken {
                etag: output.e_tag().unwrap_or_default().to_string(),
                version_id: output.version_id().map(ToString::to_string),
            },
            len: output
                .content_length()
                .and_then(|value| u64::try_from(value).ok())
                .unwrap_or_default(),
            sha256,
            last_modified_millis: output
                .last_modified()
                .and_then(|value| u64::try_from(value.secs()).ok())
                .and_then(|seconds| seconds.checked_mul(1_000))
                .unwrap_or_default(),
            delete_marker: output.delete_marker().unwrap_or(false),
            user_metadata: output
                .metadata()
                .cloned()
                .unwrap_or_default()
                .into_iter()
                .collect(),
        }))
    }

    async fn put_immutable(&self, request: ImmutablePut) -> Result<ImmutablePutOutcome> {
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        self.metrics
            .uploaded_body_bytes
            .fetch_add(request.bytes.len() as u64, Ordering::Relaxed);
        let result = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .if_none_match("*")
            .metadata("prolly-sha256", hex::encode(request.expected_sha256))
            .body(ByteStream::from(request.bytes.clone()))
            .send()
            .await;
        match result {
            Ok(output) => Ok(ImmutablePutOutcome::Created(StoredMetadata {
                token: StorageToken {
                    etag: output.e_tag().unwrap_or_default().to_string(),
                    version_id: output.version_id().map(ToString::to_string),
                },
                len: request.bytes.len() as u64,
                sha256: request.expected_sha256,
                last_modified_millis: 0,
                delete_marker: false,
                user_metadata: BTreeMap::from([(
                    "prolly-sha256".to_string(),
                    hex::encode(request.expected_sha256),
                )]),
            })),
            Err(error) if is_precondition_failed(&error) => {
                let existing = self.get_current(&request.path).await?.ok_or_else(|| {
                    Error::new(
                        ErrorCode::OutcomeUnknown,
                        "immutable create conflicted but the winner is not readable",
                    )
                })?;
                if existing.bytes != request.bytes
                    || existing.metadata.sha256 != request.expected_sha256
                {
                    return Err(Error::new(
                        ErrorCode::CorruptContent,
                        format!("different bytes exist at {}", request.path),
                    ));
                }
                Ok(ImmutablePutOutcome::AlreadyPresent(existing.metadata))
            }
            Err(error) => Err(map_sdk_error("PutObject immutable", error)),
        }
    }

    async fn load_mutable(&self, path: &ObjectPath) -> Result<Option<StoredObject>> {
        self.get_current(path).await
    }

    async fn compare_exchange(&self, request: CompareExchange) -> Result<CompareExchangeOutcome> {
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        let len = request.bytes.len() as u64;
        let sha256: [u8; 32] = Sha256::digest(&request.bytes).into();
        self.metrics
            .uploaded_body_bytes
            .fetch_add(len, Ordering::Relaxed);
        let mut operation = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .metadata("prolly-sha256", hex::encode(Sha256::digest(&request.bytes)))
            .body(ByteStream::from(request.bytes));
        operation = match request.expected.as_ref() {
            Some(expected) => operation.if_match(expected.etag.clone()),
            None => operation.if_none_match("*"),
        };
        match operation.send().await {
            Ok(output) => {
                let etag = output.e_tag().ok_or_else(|| {
                    Error::new(
                        ErrorCode::OutcomeUnknown,
                        "CAS was accepted but the provider omitted its ETag",
                    )
                })?;
                Ok(CompareExchangeOutcome::Applied(StoredMetadata {
                    token: StorageToken {
                        etag: etag.to_string(),
                        version_id: output.version_id().map(ToString::to_string),
                    },
                    len,
                    sha256,
                    last_modified_millis: 0,
                    delete_marker: false,
                    user_metadata: BTreeMap::new(),
                }))
            }
            Err(error) if is_precondition_failed(&error) => Ok(CompareExchangeOutcome::Conflict(
                self.get_current(&request.path).await?,
            )),
            Err(error) => Err(map_sdk_error("PutObject compare-exchange", error)),
        }
    }

    async fn list(&self, request: ListRequest) -> Result<PhysicalListPage> {
        if request.include_versions {
            return self.list_versions(request).await;
        }
        self.metrics.list_objects_v2.fetch_add(1, Ordering::Relaxed);
        let output = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(request.prefix)
            .set_continuation_token(request.continuation)
            .max_keys(i32::try_from(request.limit.min(1_000)).unwrap_or(1_000))
            .send()
            .await
            .map_err(|error| map_sdk_error("ListObjectsV2", error))?;
        let entries = output
            .contents()
            .iter()
            .filter_map(|object| {
                let path = ObjectPath::new(object.key()?).ok()?;
                Some(PhysicalListEntry {
                    path,
                    metadata: StoredMetadata {
                        token: StorageToken {
                            etag: object.e_tag().unwrap_or_default().to_string(),
                            version_id: None,
                        },
                        len: object
                            .size()
                            .and_then(|value| u64::try_from(value).ok())
                            .unwrap_or_default(),
                        sha256: [0; 32],
                        last_modified_millis: object
                            .last_modified()
                            .and_then(|value| u64::try_from(value.secs()).ok())
                            .and_then(|seconds| seconds.checked_mul(1_000))
                            .unwrap_or_default(),
                        delete_marker: false,
                        user_metadata: BTreeMap::new(),
                    },
                })
            })
            .collect();
        Ok(PhysicalListPage {
            entries,
            continuation: output.next_continuation_token().map(ToString::to_string),
        })
    }

    async fn delete_exact(
        &self,
        path: &ObjectPath,
        version: PhysicalVersion,
    ) -> Result<DeleteOutcome> {
        self.metrics.delete_object.fetch_add(1, Ordering::Relaxed);
        let mut operation = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(path.as_str());
        operation = match version {
            PhysicalVersion::Versioned { version_id } => operation.version_id(version_id),
            PhysicalVersion::Unversioned { token: Some(token) } => operation.if_match(token.etag),
            PhysicalVersion::Unversioned { token: None } => operation,
        };
        match operation.send().await {
            Ok(_) => Ok(DeleteOutcome::Deleted),
            Err(error) if is_not_found(&error) => Ok(DeleteOutcome::NotFound),
            Err(error) if is_precondition_failed(&error) => Ok(DeleteOutcome::TokenMismatch),
            Err(error) => Err(map_sdk_error("DeleteObject", error)),
        }
    }

    async fn put_native(&self, request: NativePut) -> Result<NativeObjectWriteResult> {
        self.metrics.put_object.fetch_add(1, Ordering::Relaxed);
        let size = request.bytes.len() as u64;
        self.metrics
            .uploaded_body_bytes
            .fetch_add(size, Ordering::Relaxed);
        let checksum_sha256: [u8; 32] = Sha256::digest(&request.bytes).into();
        let checksum_md5: [u8; 16] = Md5::digest(&request.bytes).into();
        let logical_etag = format!("\"{}\"", hex::encode(checksum_md5));
        let metadata = native_metadata(
            request.user_metadata,
            request.repository.to_string(),
            request.operation.to_string(),
            request.writer_fence_generation,
            checksum_sha256,
        )?;
        let mut operation = self
            .client
            .put_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .set_cache_control(request.headers.cache_control)
            .set_content_disposition(request.headers.content_disposition)
            .set_content_encoding(request.headers.content_encoding)
            .set_content_language(request.headers.content_language)
            .set_content_type(request.headers.content_type)
            .set_expires(
                request
                    .headers
                    .expires_at_millis
                    .and_then(|millis| i64::try_from(millis / 1_000).ok())
                    .map(DateTime::from_secs),
            )
            .set_metadata(Some(metadata))
            .checksum_sha256(STANDARD.encode(checksum_sha256))
            .body(ByteStream::from(request.bytes));
        operation = operation.metadata("prolly-logical-etag", &logical_etag);
        let output = operation
            .send()
            .await
            .map_err(|error| map_sdk_error("PutObject native", error))?;
        let version_id = required_version_id("PutObject", output.version_id())?;
        let provider_etag = output.e_tag().unwrap_or_default().to_string();
        Ok(NativeObjectWriteResult {
            binding: NativeObjectBindingV1::Live {
                version_id,
                provider_etag,
                checksum_sha256,
            },
            size,
            logical_etag,
            checksums: Checksums {
                md5: Some(checksum_md5),
                sha256: Some(checksum_sha256),
                algorithm_values: Default::default(),
            },
        })
    }

    async fn copy_native(&self, request: NativeCopy) -> Result<NativeObjectWriteResult> {
        self.metrics.copy_object.fetch_add(1, Ordering::Relaxed);
        let metadata = native_metadata(
            request.user_metadata,
            request.repository.to_string(),
            request.operation.to_string(),
            request.writer_fence_generation,
            request.checksum_sha256,
        )?;
        let copy_source = format!(
            "{}/{}?versionId={}",
            self.bucket,
            request.source.as_str(),
            request.source_version_id
        );
        let output = self
            .client
            .copy_object()
            .bucket(&self.bucket)
            .key(request.destination.as_str())
            .copy_source(copy_source)
            .metadata_directive(MetadataDirective::Replace)
            .set_cache_control(request.headers.cache_control)
            .set_content_disposition(request.headers.content_disposition)
            .set_content_encoding(request.headers.content_encoding)
            .set_content_language(request.headers.content_language)
            .set_content_type(request.headers.content_type)
            .set_expires(
                request
                    .headers
                    .expires_at_millis
                    .and_then(|millis| i64::try_from(millis / 1_000).ok())
                    .map(DateTime::from_secs),
            )
            .set_metadata(Some(metadata))
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Sha256)
            .send()
            .await
            .map_err(|error| map_sdk_error("CopyObject native", error))?;
        let version_id = required_version_id("CopyObject", output.version_id())?;
        let provider_etag = output
            .copy_object_result()
            .and_then(|result| result.e_tag())
            .unwrap_or_default()
            .to_string();
        Ok(NativeObjectWriteResult {
            binding: NativeObjectBindingV1::Live {
                version_id,
                provider_etag,
                checksum_sha256: request.checksum_sha256,
            },
            size: request.size,
            logical_etag: request.logical_etag,
            checksums: request.checksums,
        })
    }

    async fn delete_native(&self, request: NativeDelete) -> Result<NativeObjectBindingV1> {
        self.metrics.delete_object.fetch_add(1, Ordering::Relaxed);
        let output = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(request.path.as_str())
            .send()
            .await
            .map_err(|error| map_sdk_error("DeleteObject native", error))?;
        if output.delete_marker() != Some(true) {
            return Err(Error::new(
                ErrorCode::ProviderNotQualified,
                "DeleteObject did not create a native delete marker",
            ));
        }
        Ok(NativeObjectBindingV1::DeleteMarker {
            version_id: required_version_id("DeleteObject", output.version_id())?,
        })
    }
}

impl AwsS3ObjectPlane {
    async fn list_versions(&self, request: ListRequest) -> Result<PhysicalListPage> {
        self.metrics
            .list_object_versions
            .fetch_add(1, Ordering::Relaxed);
        let (key_marker, version_marker) = request
            .continuation
            .as_deref()
            .and_then(decode_version_cursor)
            .unwrap_or((None, None));
        let output = self
            .client
            .list_object_versions()
            .bucket(&self.bucket)
            .prefix(request.prefix)
            .set_key_marker(key_marker)
            .set_version_id_marker(version_marker)
            .max_keys(i32::try_from(request.limit.min(1_000)).unwrap_or(1_000))
            .send()
            .await
            .map_err(|error| map_sdk_error("ListObjectVersions", error))?;
        let mut entries = Vec::new();
        for version in output.versions() {
            let Some(key) = version.key() else { continue };
            let Some(version_id) = version.version_id() else {
                continue;
            };
            let Ok(path) = ObjectPath::new(key) else {
                continue;
            };
            entries.push(PhysicalListEntry {
                path,
                metadata: StoredMetadata {
                    token: StorageToken {
                        etag: version.e_tag().unwrap_or_default().to_string(),
                        version_id: Some(version_id.to_string()),
                    },
                    len: version
                        .size()
                        .and_then(|value| u64::try_from(value).ok())
                        .unwrap_or_default(),
                    sha256: [0; 32],
                    last_modified_millis: version
                        .last_modified()
                        .and_then(|value| u64::try_from(value.secs()).ok())
                        .and_then(|seconds| seconds.checked_mul(1_000))
                        .unwrap_or_default(),
                    delete_marker: false,
                    user_metadata: BTreeMap::new(),
                },
            });
        }
        for marker in output.delete_markers() {
            let Some(key) = marker.key() else { continue };
            let Some(version_id) = marker.version_id() else {
                continue;
            };
            let Ok(path) = ObjectPath::new(key) else {
                continue;
            };
            entries.push(PhysicalListEntry {
                path,
                metadata: StoredMetadata {
                    token: StorageToken {
                        etag: String::new(),
                        version_id: Some(version_id.to_string()),
                    },
                    len: 0,
                    sha256: [0; 32],
                    last_modified_millis: marker
                        .last_modified()
                        .and_then(|value| u64::try_from(value.secs()).ok())
                        .and_then(|seconds| seconds.checked_mul(1_000))
                        .unwrap_or_default(),
                    delete_marker: true,
                    user_metadata: BTreeMap::new(),
                },
            });
        }
        let continuation = if output.is_truncated().unwrap_or(false) {
            Some(encode_version_cursor(
                output.next_key_marker(),
                output.next_version_id_marker(),
            ))
        } else {
            None
        };
        Ok(PhysicalListPage {
            entries,
            continuation,
        })
    }
}

fn native_metadata(
    user_metadata: std::collections::BTreeMap<String, String>,
    repository: String,
    operation: String,
    writer_fence_generation: u64,
    checksum_sha256: [u8; 32],
) -> Result<std::collections::HashMap<String, String>> {
    if user_metadata
        .keys()
        .any(|key| key.to_ascii_lowercase().starts_with("prolly-"))
    {
        return Err(Error::new(
            ErrorCode::InvalidRequest,
            "user metadata keys beginning with prolly- are reserved",
        ));
    }
    let mut metadata = user_metadata
        .into_iter()
        .collect::<std::collections::HashMap<_, _>>();
    metadata.insert("prolly-repository-id".to_string(), repository);
    metadata.insert("prolly-operation-id".to_string(), operation);
    metadata.insert(
        "prolly-writer-fence".to_string(),
        writer_fence_generation.to_string(),
    );
    metadata.insert("prolly-sha256".to_string(), hex::encode(checksum_sha256));
    Ok(metadata)
}

fn required_version_id(operation: &str, version_id: Option<&str>) -> Result<String> {
    version_id
        .filter(|value| !value.is_empty() && *value != "null")
        .map(ToString::to_string)
        .ok_or_else(|| {
            Error::new(
                ErrorCode::ProviderNotQualified,
                format!("{operation} succeeded without a native S3 VersionId"),
            )
        })
}

fn format_range(range: &RangeInclusive<u64>) -> String {
    format!("bytes={}-{}", range.start(), range.end())
}

fn encode_version_cursor(key: Option<&str>, version: Option<&str>) -> String {
    let key = key.unwrap_or_default();
    let version = version.unwrap_or_default();
    format!("{}:{key}{version}", key.len())
}

fn decode_version_cursor(value: &str) -> Option<(Option<String>, Option<String>)> {
    let (length, rest) = value.split_once(':')?;
    let length: usize = length.parse().ok()?;
    if rest.len() < length || !rest.is_char_boundary(length) {
        return None;
    }
    let (key, version) = rest.split_at(length);
    Some((
        (!key.is_empty()).then(|| key.to_string()),
        (!version.is_empty()).then(|| version.to_string()),
    ))
}

fn is_not_found<E, R>(error: &aws_smithy_runtime_api::client::result::SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "NoSuchKey" | "NotFound" | "NoSuchVersion"))
}

fn is_precondition_failed<E, R>(
    error: &aws_smithy_runtime_api::client::result::SdkError<E, R>,
) -> bool
where
    E: ProvideErrorMetadata,
{
    error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .is_some_and(|code| matches!(code, "PreconditionFailed" | "ConditionalRequestConflict"))
}

fn map_sdk_error<E, R>(
    operation: &str,
    error: aws_smithy_runtime_api::client::result::SdkError<E, R>,
) -> Error
where
    E: ProvideErrorMetadata + RequestId + std::fmt::Debug,
    R: std::fmt::Debug,
{
    let provider_code = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::code)
        .map(ToString::to_string);
    let provider_message = error
        .as_service_error()
        .and_then(ProvideErrorMetadata::message)
        .map(ToString::to_string);
    let provider_request_id = error
        .as_service_error()
        .and_then(RequestId::request_id)
        .map(ToString::to_string);
    let error_code = match (
        provider_code.as_deref().unwrap_or_default(),
        provider_message.as_deref(),
    ) {
        ("AccessDenied" | "InvalidAccessKeyId" | "SignatureDoesNotMatch", _)
        | ("InvalidRequest", Some("ErrAccessKeyDisabled")) => ErrorCode::PermissionDenied,
        ("SlowDown" | "ServiceUnavailable", _) => ErrorCode::Throttled,
        ("RequestTimeout", _) => ErrorCode::Timeout,
        _ => ErrorCode::Transport,
    };
    Error::new(error_code, format!("{operation} failed: {error:?}"))
        .provider_metadata(provider_code, provider_message)
        .provider_request_id(provider_request_id)
        .retry(if matches!(error_code, ErrorCode::PermissionDenied) {
            RetryAdvice::Never
        } else {
            RetryAdvice::Safe
        })
}

fn transport_error(operation: &str, error: impl std::fmt::Display) -> Error {
    Error::new(ErrorCode::Transport, format!("{operation} failed: {error}"))
        .retry(RetryAdvice::Safe)
}

#[cfg(test)]
mod tests {
    use aws_smithy_runtime_api::client::result::SdkError;
    use aws_smithy_types::error::metadata::{ErrorMetadata, ProvideErrorMetadata};
    use aws_types::request_id::RequestId;

    use super::map_sdk_error;
    use prolly_s3_core::{ErrorCode, RetryAdvice};

    #[derive(Debug)]
    struct ServiceFailure {
        metadata: ErrorMetadata,
    }

    impl ProvideErrorMetadata for ServiceFailure {
        fn meta(&self) -> &ErrorMetadata {
            &self.metadata
        }
    }

    impl RequestId for ServiceFailure {
        fn request_id(&self) -> Option<&str> {
            Some("request-123")
        }
    }

    #[test]
    fn service_errors_preserve_provider_code_message_and_request_id() {
        let sdk_error = SdkError::service_error(
            ServiceFailure {
                metadata: ErrorMetadata::builder()
                    .code("SlowDown")
                    .message("reduce request rate")
                    .build(),
            },
            (),
        );
        let mapped = map_sdk_error("fixture", sdk_error);
        assert_eq!(mapped.code, ErrorCode::Throttled);
        assert_eq!(mapped.retry, RetryAdvice::Safe);
        assert_eq!(mapped.provider_code.as_deref(), Some("SlowDown"));
        assert_eq!(
            mapped.provider_message.as_deref(),
            Some("reduce request rate")
        );
        assert_eq!(mapped.provider_request_id.as_deref(), Some("request-123"));
    }

    #[test]
    fn rustfs_disabled_access_key_is_terminal_permission_denial() {
        let sdk_error = SdkError::service_error(
            ServiceFailure {
                metadata: ErrorMetadata::builder()
                    .code("InvalidRequest")
                    .message("ErrAccessKeyDisabled")
                    .build(),
            },
            (),
        );
        let mapped = map_sdk_error("fixture", sdk_error);
        assert_eq!(mapped.code, ErrorCode::PermissionDenied);
        assert_eq!(mapped.retry, RetryAdvice::Never);
        assert_eq!(mapped.provider_code.as_deref(), Some("InvalidRequest"));
        assert_eq!(
            mapped.provider_message.as_deref(),
            Some("ErrAccessKeyDisabled")
        );
    }
}
