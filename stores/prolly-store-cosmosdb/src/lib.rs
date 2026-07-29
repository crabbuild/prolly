#![doc = include_str!("../README.md")]

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition,
    RemoteRootWrite, RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};

/// Cosmos DB adapter entry point.
pub mod cosmosdb {
    use std::collections::{HashMap, HashSet};
    use std::error::Error as StdError;
    use std::fmt;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, SystemTime};

    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use futures_util::stream::{self, StreamExt, TryStreamExt};
    use hmac::{Hmac, Mac};
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    use reqwest::{Method, StatusCode};
    use serde::{Deserialize, Serialize};
    use sha2::Sha256;

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for Cosmos DB-backed prolly nodes and roots.
    pub type CosmosDbStore = crate::RemoteProllyStore<CosmosDbBackend>;

    /// Cosmos DB REST-backed backend.
    ///
    /// The container must use `/kind` as its partition key. The adapter stores
    /// all documents for one backend instance under a single `kind` partition
    /// value so Cosmos DB transactional batches can atomically commit nodes and
    /// roots together. The logical document family lives in `family`.
    #[derive(Clone)]
    pub struct CosmosDbBackend {
        http: reqwest::Client,
        endpoint: String,
        account_key: Vec<u8>,
        database_id: String,
        container_id: String,
        container_link: String,
        key_prefix: Vec<u8>,
        partition_key: String,
        options: CosmosDbBackendOptions,
        metrics: Arc<CosmosDbMetrics>,
    }

    impl fmt::Debug for CosmosDbBackend {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("CosmosDbBackend")
                .field("endpoint", &self.endpoint)
                .field("database_id", &self.database_id)
                .field("container_id", &self.container_id)
                .field("key_prefix", &self.key_prefix)
                .field("partition_key", &self.partition_key)
                .field("options", &self.options)
                .finish_non_exhaustive()
        }
    }

    /// Performance and retry controls for the Cosmos DB REST backend.
    #[derive(Clone, Debug)]
    pub struct CosmosDbBackendOptions {
        /// Maximum in-flight point reads in a batch.
        pub max_concurrency: usize,
        /// Maximum in-flight reads advertised to prolly traversal paths.
        pub read_parallelism: usize,
        /// Number of retry attempts after the initial request.
        pub max_retries: usize,
        /// Maximum cumulative server-requested retry delay.
        pub max_retry_wait: Duration,
        /// Maximum documents requested per query page.
        pub query_page_size: usize,
        /// Maintain rightmost-path hints for append-heavy maps.
        pub rightmost_path_hints: bool,
    }

    impl Default for CosmosDbBackendOptions {
        fn default() -> Self {
            Self {
                max_concurrency: DEFAULT_MAX_CONCURRENCY,
                read_parallelism: DEFAULT_READ_PARALLELISM,
                max_retries: DEFAULT_MAX_RETRIES,
                max_retry_wait: DEFAULT_MAX_RETRY_WAIT,
                query_page_size: DEFAULT_QUERY_PAGE_SIZE,
                rightmost_path_hints: true,
            }
        }
    }

    impl CosmosDbBackendOptions {
        /// Set maximum in-flight point reads used by native batches.
        pub fn with_max_concurrency(mut self, max_concurrency: usize) -> Self {
            self.max_concurrency = max_concurrency.max(1);
            self
        }

        /// Set maximum traversal read concurrency.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Set the number of retries for 429, 408, and 503 responses.
        pub fn with_max_retries(mut self, max_retries: usize) -> Self {
            self.max_retries = max_retries;
            self
        }

        /// Bound cumulative backoff time for a request.
        pub fn with_max_retry_wait(mut self, max_retry_wait: Duration) -> Self {
            self.max_retry_wait = max_retry_wait;
            self
        }

        /// Set the number of documents requested per query page.
        pub fn with_query_page_size(mut self, query_page_size: usize) -> Self {
            self.query_page_size = query_page_size.max(1);
            self
        }

        /// Enable or disable rightmost-path hint maintenance.
        pub fn with_rightmost_path_hints(mut self, enabled: bool) -> Self {
            self.rightmost_path_hints = enabled;
            self
        }
    }

    #[derive(Default)]
    struct CosmosDbMetrics {
        requests: AtomicU64,
        retries: AtomicU64,
        request_charge_micros: AtomicU64,
    }

    /// A point-in-time snapshot of low-overhead Cosmos DB request metrics.
    #[derive(Clone, Copy, Debug, Default, PartialEq)]
    pub struct CosmosDbMetricsSnapshot {
        /// Total HTTP responses received, including retry attempts.
        pub requests: u64,
        /// Total retry attempts triggered by throttling or transient status codes.
        pub retries: u64,
        /// Sum of `x-ms-request-charge` values observed on responses.
        pub request_charge: f64,
    }

    impl CosmosDbBackend {
        /// Create a backend using Cosmos DB key authentication.
        pub fn with_key(
            endpoint: impl Into<String>,
            account_key: &str,
            database_id: impl Into<String>,
            container_id: impl Into<String>,
        ) -> Result<Self, CosmosDbBackendError> {
            Self::with_http_client_and_options(
                reqwest::Client::new(),
                endpoint,
                account_key,
                database_id,
                container_id,
                CosmosDbBackendOptions::default(),
            )
        }

        /// Create a backend with a caller-provided HTTP client.
        pub fn with_http_client(
            http: reqwest::Client,
            endpoint: impl Into<String>,
            account_key: &str,
            database_id: impl Into<String>,
            container_id: impl Into<String>,
        ) -> Result<Self, CosmosDbBackendError> {
            Self::with_http_client_and_options(
                http,
                endpoint,
                account_key,
                database_id,
                container_id,
                CosmosDbBackendOptions::default(),
            )
        }

        /// Create a backend with a caller-provided client and performance controls.
        pub fn with_http_client_and_options(
            http: reqwest::Client,
            endpoint: impl Into<String>,
            account_key: &str,
            database_id: impl Into<String>,
            container_id: impl Into<String>,
            options: CosmosDbBackendOptions,
        ) -> Result<Self, CosmosDbBackendError> {
            let database_id = database_id.into();
            let container_id = container_id.into();
            let container_link = format!(
                "dbs/{}/colls/{}",
                encode_path_segment(&database_id),
                encode_path_segment(&container_id)
            );

            Ok(Self {
                http,
                endpoint: endpoint.into().trim_end_matches('/').to_string(),
                account_key: BASE64
                    .decode(account_key)
                    .map_err(CosmosDbBackendError::InvalidAccountKey)?,
                database_id,
                container_id,
                container_link,
                key_prefix: DEFAULT_KEY_PREFIX.to_vec(),
                partition_key: DEFAULT_PARTITION_KEY.to_string(),
                options: CosmosDbBackendOptions {
                    max_concurrency: options.max_concurrency.max(1),
                    read_parallelism: options.read_parallelism.max(1),
                    max_retries: options.max_retries,
                    max_retry_wait: options.max_retry_wait,
                    query_page_size: options.query_page_size.max(1),
                    rightmost_path_hints: options.rightmost_path_hints,
                },
                metrics: Arc::new(CosmosDbMetrics::default()),
            })
        }

        /// Return the Cosmos DB account endpoint.
        pub fn endpoint(&self) -> &str {
            &self.endpoint
        }

        /// Return the Cosmos DB database id.
        pub fn database_id(&self) -> &str {
            &self.database_id
        }

        /// Return the Cosmos DB container id.
        pub fn container_id(&self) -> &str {
            &self.container_id
        }

        /// Return the namespace prefix prepended to all logical keys.
        pub fn key_prefix(&self) -> &[u8] {
            &self.key_prefix
        }

        /// Return the `/kind` partition value used by this backend instance.
        pub fn partition_key_value(&self) -> &str {
            &self.partition_key
        }

        /// Set the namespace prefix prepended to all logical keys.
        pub fn with_key_prefix(mut self, key_prefix: impl Into<Vec<u8>>) -> Self {
            self.key_prefix = key_prefix.into();
            self
        }

        /// Set the `/kind` partition value used by this backend instance.
        ///
        /// All nodes, roots, and hints for the backend must share this value for
        /// Cosmos DB transactional batch support.
        pub fn with_partition_key_value(mut self, partition_key: impl Into<String>) -> Self {
            self.partition_key = partition_key.into();
            self
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.options.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Return cumulative request, retry, and request-unit metrics.
        pub fn metrics(&self) -> CosmosDbMetricsSnapshot {
            CosmosDbMetricsSnapshot {
                requests: self.metrics.requests.load(Ordering::Relaxed),
                retries: self.metrics.retries.load(Ordering::Relaxed),
                request_charge: self.metrics.request_charge_micros.load(Ordering::Relaxed) as f64
                    / 1_000_000.0,
            }
        }

        /// Delete every document under this backend's namespace prefix.
        ///
        /// This is primarily intended for isolated integration tests.
        pub async fn clear_namespace(&self) -> Result<(), CosmosDbBackendError> {
            if self.key_prefix.is_empty() {
                return Err(CosmosDbBackendError::InvalidConfiguration(
                    "refusing to clear an empty Cosmos DB key prefix".to_string(),
                ));
            }

            for kind in [NODE_KIND, ROOT_KIND, HINT_KIND] {
                let docs = self.query_kind(kind, &self.key_prefix).await?;
                let mut operations = Vec::with_capacity(docs.len());
                for doc in docs {
                    let logical_key = doc.logical_key()?;
                    if logical_key.starts_with(&self.key_prefix) {
                        let etag = match doc.etag {
                            Some(etag) => etag,
                            None => {
                                let Some(current) = self.read_document(kind, &logical_key).await?
                                else {
                                    continue;
                                };
                                current.etag
                            }
                        };
                        operations.push(CosmosBatchOperation::delete(
                            document_id(&logical_key),
                            self.batch_partition_key(),
                            Some(etag),
                        ));
                    }
                }
                self.execute_upsert_batches(&operations).await?;
            }

            Ok(())
        }

        fn node_key(&self, key: &[u8]) -> Vec<u8> {
            self.family_key(NODE_FAMILY, key)
        }

        fn root_key(&self, name: &[u8]) -> Vec<u8> {
            self.family_key(ROOT_FAMILY, name)
        }

        fn hint_key(&self, namespace: &[u8], key: &[u8]) -> Vec<u8> {
            let mut cosmos_key = self.family_key(HINT_FAMILY, &[]);
            cosmos_key.extend_from_slice(&(namespace.len() as u64).to_be_bytes());
            cosmos_key.extend_from_slice(namespace);
            cosmos_key.extend_from_slice(key);
            cosmos_key
        }

        fn family_key(&self, family: &[u8], suffix: &[u8]) -> Vec<u8> {
            let mut key = Vec::with_capacity(self.key_prefix.len() + family.len() + suffix.len());
            key.extend_from_slice(&self.key_prefix);
            key.extend_from_slice(family);
            key.extend_from_slice(suffix);
            key
        }

        fn family_prefix(&self, family: &[u8]) -> Vec<u8> {
            self.family_key(family, &[])
        }

        fn feed_link(&self) -> String {
            format!("{}/docs", self.container_link)
        }

        fn document_link(&self, id: &str) -> String {
            format!("{}/docs/{}", self.container_link, id)
        }

        fn resource_url(&self, link: &str) -> String {
            format!("{}/{}", self.endpoint, link)
        }

        fn authorized_request(
            &self,
            method: Method,
            resource_type: &'static str,
            resource_link: &str,
            url: String,
        ) -> Result<reqwest::RequestBuilder, CosmosDbBackendError> {
            let date = httpdate::fmt_http_date(SystemTime::now());
            let auth =
                self.authorization_header(method.as_str(), resource_type, resource_link, &date)?;
            Ok(self
                .http
                .request(method, url)
                .header("authorization", auth)
                .header("x-ms-date", date)
                .header("x-ms-version", COSMOS_API_VERSION))
        }

        async fn send_with_retry(
            &self,
            request: reqwest::RequestBuilder,
        ) -> Result<reqwest::Response, CosmosDbBackendError> {
            let template = request.try_clone();
            let mut next = Some(request);
            let mut retry_wait = Duration::ZERO;
            let mut attempt = 0usize;

            loop {
                let response = next
                    .take()
                    .expect("Cosmos DB request is available")
                    .send()
                    .await
                    .map_err(CosmosDbBackendError::Http)?;
                self.record_response_metrics(&response);

                if !is_retryable_status(response.status()) || attempt >= self.options.max_retries {
                    return Ok(response);
                }
                let Some(template) = template
                    .as_ref()
                    .and_then(reqwest::RequestBuilder::try_clone)
                else {
                    return Ok(response);
                };

                let delay = retry_delay(&response, attempt);
                if retry_wait.saturating_add(delay) > self.options.max_retry_wait {
                    return Ok(response);
                }
                retry_wait += delay;
                attempt += 1;
                self.metrics.retries.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(delay).await;
                next = Some(template);
            }
        }

        fn record_response_metrics(&self, response: &reqwest::Response) {
            self.metrics.requests.fetch_add(1, Ordering::Relaxed);
            if let Some(charge) = response
                .headers()
                .get("x-ms-request-charge")
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<f64>().ok())
            {
                self.metrics.request_charge_micros.fetch_add(
                    (charge.max(0.0) * 1_000_000.0).round() as u64,
                    Ordering::Relaxed,
                );
            }
        }

        fn authorization_header(
            &self,
            method: &str,
            resource_type: &'static str,
            resource_link: &str,
            date: &str,
        ) -> Result<String, CosmosDbBackendError> {
            let payload = format!(
                "{}\n{}\n{}\n{}\n\n",
                method.to_ascii_lowercase(),
                resource_type,
                resource_link,
                date.to_ascii_lowercase()
            );
            let mut mac = Hmac::<Sha256>::new_from_slice(&self.account_key)
                .map_err(|err| CosmosDbBackendError::InvalidConfiguration(err.to_string()))?;
            mac.update(payload.as_bytes());
            let signature = BASE64.encode(mac.finalize().into_bytes());
            let token = format!("type=master&ver=1.0&sig={signature}");
            Ok(utf8_percent_encode(&token, NON_ALPHANUMERIC).to_string())
        }

        async fn read_document(
            &self,
            _kind: &'static str,
            logical_key: &[u8],
        ) -> Result<Option<CosmosReadDocument>, CosmosDbBackendError> {
            let id = document_id(logical_key);
            let link = self.document_link(&id);
            let request = self
                .authorized_request(Method::GET, DOCS_RESOURCE, &link, self.resource_url(&link))?
                .header("x-ms-documentdb-partitionkey", self.partition_key_header());
            let response = self.send_with_retry(request).await?;

            if response.status() == StatusCode::NOT_FOUND {
                return Ok(None);
            }
            let response = ensure_status(response).await?;
            let etag = response
                .headers()
                .get("etag")
                .and_then(|value| value.to_str().ok())
                .map(str::to_string)
                .ok_or(CosmosDbBackendError::MissingEtag)?;
            let document = response
                .json::<CosmosProllyDocument>()
                .await
                .map_err(CosmosDbBackendError::Http)?;
            Ok(Some(CosmosReadDocument { document, etag }))
        }

        async fn upsert_document(
            &self,
            kind: &'static str,
            logical_key: &[u8],
            value: &[u8],
        ) -> Result<(), CosmosDbBackendError> {
            let doc = self.document(kind, logical_key, value);
            let link = self.feed_link();
            let request = self
                .authorized_request(
                    Method::POST,
                    DOCS_RESOURCE,
                    &self.container_link,
                    self.resource_url(&link),
                )?
                .header("content-type", "application/json")
                .header("x-ms-documentdb-partitionkey", self.partition_key_header())
                .header("x-ms-documentdb-is-upsert", "True")
                .json(&doc);
            let response = self.send_with_retry(request).await?;
            ensure_status(response).await?;
            Ok(())
        }

        async fn create_document_if_absent(
            &self,
            kind: &'static str,
            logical_key: &[u8],
            value: &[u8],
        ) -> Result<bool, CosmosDbBackendError> {
            let doc = self.document(kind, logical_key, value);
            let link = self.feed_link();
            let request = self
                .authorized_request(
                    Method::POST,
                    DOCS_RESOURCE,
                    &self.container_link,
                    self.resource_url(&link),
                )?
                .header("content-type", "application/json")
                .header("if-none-match", "*")
                .header("x-ms-documentdb-partitionkey", self.partition_key_header())
                .json(&doc);
            let response = self.send_with_retry(request).await?;
            if is_conflict_status(response.status()) {
                return Ok(false);
            }
            ensure_status(response).await?;
            Ok(true)
        }

        async fn replace_document_if_match(
            &self,
            kind: &'static str,
            logical_key: &[u8],
            value: &[u8],
            etag: &str,
        ) -> Result<bool, CosmosDbBackendError> {
            let id = document_id(logical_key);
            let doc = self.document(kind, logical_key, value);
            let link = self.document_link(&id);
            let request = self
                .authorized_request(Method::PUT, DOCS_RESOURCE, &link, self.resource_url(&link))?
                .header("content-type", "application/json")
                .header("if-match", etag)
                .header("x-ms-documentdb-partitionkey", self.partition_key_header())
                .json(&doc);
            let response = self.send_with_retry(request).await?;
            if is_conflict_status(response.status()) {
                return Ok(false);
            }
            ensure_status(response).await?;
            Ok(true)
        }

        async fn delete_document(
            &self,
            _kind: &'static str,
            logical_key: &[u8],
            etag: Option<&str>,
            ignore_missing: bool,
        ) -> Result<bool, CosmosDbBackendError> {
            let id = document_id(logical_key);
            let link = self.document_link(&id);
            let mut request = self
                .authorized_request(
                    Method::DELETE,
                    DOCS_RESOURCE,
                    &link,
                    self.resource_url(&link),
                )?
                .header("x-ms-documentdb-partitionkey", self.partition_key_header());
            if let Some(etag) = etag {
                request = request.header("if-match", etag);
            }

            let response = self.send_with_retry(request).await?;
            if response.status() == StatusCode::NOT_FOUND && ignore_missing {
                return Ok(true);
            }
            if is_conflict_status(response.status()) {
                return Ok(false);
            }
            ensure_status(response).await?;
            Ok(true)
        }

        async fn query_kind(
            &self,
            kind: &'static str,
            logical_prefix: &[u8],
        ) -> Result<Vec<CosmosProllyDocument>, CosmosDbBackendError> {
            let mut documents = Vec::new();
            let mut continuation = None;

            loop {
                let link = self.feed_link();
                let body = serde_json::json!({
                    "query": "SELECT * FROM c WHERE c.kind = @kind AND c.family = @family AND STARTSWITH(c.key, @prefix)",
                    "parameters": [
                        { "name": "@kind", "value": self.partition_key },
                        { "name": "@family", "value": kind },
                        { "name": "@prefix", "value": hex::encode(logical_prefix) }
                    ]
                });
                let mut request = self
                    .authorized_request(
                        Method::POST,
                        DOCS_RESOURCE,
                        &self.container_link,
                        self.resource_url(&link),
                    )?
                    .header("content-type", "application/query+json")
                    .header("x-ms-documentdb-isquery", "True")
                    .header("x-ms-documentdb-partitionkey", self.partition_key_header())
                    .header(
                        "x-ms-max-item-count",
                        self.options.query_page_size.to_string(),
                    )
                    .json(&body);
                if let Some(token) = continuation.as_deref() {
                    request = request.header("x-ms-continuation", token);
                }

                let response = ensure_status(self.send_with_retry(request).await?).await?;
                continuation = response
                    .headers()
                    .get("x-ms-continuation")
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_string);
                let page = response
                    .json::<CosmosFeed>()
                    .await
                    .map_err(CosmosDbBackendError::Http)?;
                documents.extend(page.documents);

                if continuation.is_none() {
                    break;
                }
            }

            Ok(documents)
        }

        fn document(
            &self,
            kind: &'static str,
            logical_key: &[u8],
            value: &[u8],
        ) -> CosmosProllyDocument {
            CosmosProllyDocument::new(&self.partition_key, kind, logical_key, value)
        }

        async fn batch_upsert_operations(
            &self,
            kind: &'static str,
            entries: &[(&[u8], &[u8])],
        ) -> Result<Vec<CosmosBatchOperation>, CosmosDbBackendError> {
            let mut operations = stream::iter(entries.iter().enumerate())
                .map(|(index, (logical_key, value))| async move {
                    let operation = match self.read_document(kind, logical_key).await? {
                        Some(current) if current.document.value_bytes()?.as_slice() == *value => {
                            None
                        }
                        Some(current) => Some(CosmosBatchOperation::replace(
                            document_id(logical_key),
                            self.batch_partition_key(),
                            current.etag,
                            self.document(kind, logical_key, value),
                        )),
                        None => Some(CosmosBatchOperation::upsert(
                            self.batch_partition_key(),
                            self.document(kind, logical_key, value),
                        )),
                    };
                    Ok::<_, CosmosDbBackendError>((index, operation))
                })
                .buffer_unordered(self.options.max_concurrency)
                .try_collect::<Vec<_>>()
                .await?;
            operations.sort_unstable_by_key(|(index, _)| *index);
            Ok(operations
                .into_iter()
                .filter_map(|(_, operation)| operation)
                .collect())
        }

        async fn prepare_node_batch_operation(
            &self,
            key: &[u8],
            value: Option<&[u8]>,
        ) -> Result<Option<CosmosBatchOperation>, CosmosDbBackendError> {
            let logical_key = self.node_key(key);
            let current = self.read_document(NODE_KIND, &logical_key).await?;
            match (value, current) {
                (Some(value), Some(current))
                    if current.document.value_bytes()?.as_slice() == value =>
                {
                    Ok(None)
                }
                (Some(value), Some(current)) => Ok(Some(CosmosBatchOperation::replace(
                    document_id(&logical_key),
                    self.batch_partition_key(),
                    current.etag,
                    self.document(NODE_KIND, &logical_key, value),
                ))),
                (Some(value), None) => Ok(Some(CosmosBatchOperation::upsert(
                    self.batch_partition_key(),
                    self.document(NODE_KIND, &logical_key, value),
                ))),
                (None, Some(current)) => Ok(Some(CosmosBatchOperation::delete(
                    document_id(&logical_key),
                    self.batch_partition_key(),
                    Some(current.etag),
                ))),
                (None, None) => Ok(None),
            }
        }

        fn partition_key_header(&self) -> String {
            partition_key(&self.partition_key)
        }

        fn batch_partition_key(&self) -> String {
            self.partition_key_header()
        }

        async fn execute_transaction_batch(
            &self,
            operations: &[CosmosBatchOperation],
        ) -> Result<Vec<CosmosBatchOperationResponse>, CosmosDbBackendError> {
            validate_transaction_batch(operations)?;
            let link = self.feed_link();
            let request = self
                .authorized_request(
                    Method::POST,
                    DOCS_RESOURCE,
                    &self.container_link,
                    self.resource_url(&link),
                )?
                .header("content-type", "application/json")
                .header("x-ms-documentdb-partitionkey", self.partition_key_header())
                .header("x-ms-cosmos-is-batch-request", "True")
                .header("x-ms-cosmos-batch-atomic", "True")
                .json(operations);

            let response = ensure_status(self.send_with_retry(request).await?).await?;
            response
                .json::<Vec<CosmosBatchOperationResponse>>()
                .await
                .map_err(CosmosDbBackendError::Http)
        }

        async fn execute_checked_batch(
            &self,
            operations: &[CosmosBatchOperation],
        ) -> Result<(), CosmosDbBackendError> {
            if operations.is_empty() {
                return Ok(());
            }
            let responses = self.execute_transaction_batch(operations).await?;
            if responses.len() != operations.len()
                || responses.iter().any(|response| !response.is_success())
            {
                return Err(batch_response_error(&responses));
            }
            Ok(())
        }

        async fn execute_upsert_batches(
            &self,
            operations: &[CosmosBatchOperation],
        ) -> Result<(), CosmosDbBackendError> {
            let mut start = 0;
            while start < operations.len() {
                let mut end = (start + COSMOS_BATCH_OPERATION_LIMIT).min(operations.len());
                loop {
                    match validate_transaction_batch(&operations[start..end]) {
                        Ok(()) => break,
                        Err(CosmosDbBackendError::TransactionPayloadTooLarge { .. })
                            if end > start + 1 =>
                        {
                            end -= 1;
                        }
                        Err(err) => return Err(err),
                    }
                }
                self.execute_checked_batch(&operations[start..end]).await?;
                start = end;
            }
            Ok(())
        }

        async fn push_root_condition_operation(
            &self,
            operations: &mut Vec<CosmosBatchOperation>,
            operation_conditions: &mut Vec<Option<RemoteRootCondition>>,
            condition: &RemoteRootCondition,
        ) -> Result<(), CosmosDbBackendError> {
            let logical_key = self.root_key(&condition.name);
            match condition.expected.as_deref() {
                Some(expected) => {
                    let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? else {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                condition.name.clone(),
                                condition.expected.clone(),
                                None,
                            ),
                        ));
                    };
                    let current_value = current.document.value_bytes()?;
                    if current_value.as_slice() != expected {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                condition.name.clone(),
                                condition.expected.clone(),
                                Some(current_value),
                            ),
                        ));
                    }

                    operations.push(CosmosBatchOperation::read(
                        document_id(&logical_key),
                        self.batch_partition_key(),
                        current.etag,
                    ));
                    operation_conditions.push(Some(condition.clone()));
                }
                None => {
                    let doc = self.document(ROOT_KIND, &logical_key, &[]);
                    operations.push(CosmosBatchOperation::create_if_absent(
                        self.batch_partition_key(),
                        doc,
                    ));
                    operation_conditions.push(Some(condition.clone()));
                    operations.push(CosmosBatchOperation::delete(
                        document_id(&logical_key),
                        self.batch_partition_key(),
                        None,
                    ));
                    operation_conditions.push(Some(condition.clone()));
                }
            }
            Ok(())
        }

        async fn push_root_write_operation(
            &self,
            operations: &mut Vec<CosmosBatchOperation>,
            operation_conditions: &mut Vec<Option<RemoteRootCondition>>,
            write: &RemoteRootWrite,
            condition: Option<&RemoteRootCondition>,
        ) -> Result<(), CosmosDbBackendError> {
            let name = root_write_name(write);
            let logical_key = self.root_key(name);
            match (
                condition.and_then(|condition| condition.expected.as_deref()),
                write,
            ) {
                (Some(expected), RemoteRootWrite::Put { manifest, .. }) => {
                    let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? else {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                name.to_vec(),
                                Some(expected.to_vec()),
                                None,
                            ),
                        ));
                    };
                    let current_value = current.document.value_bytes()?;
                    if current_value.as_slice() != expected {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                name.to_vec(),
                                Some(expected.to_vec()),
                                Some(current_value),
                            ),
                        ));
                    }

                    operations.push(CosmosBatchOperation::replace(
                        document_id(&logical_key),
                        self.batch_partition_key(),
                        current.etag,
                        self.document(ROOT_KIND, &logical_key, manifest),
                    ));
                    operation_conditions.push(condition.cloned());
                }
                (Some(expected), RemoteRootWrite::Delete { .. }) => {
                    let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? else {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                name.to_vec(),
                                Some(expected.to_vec()),
                                None,
                            ),
                        ));
                    };
                    let current_value = current.document.value_bytes()?;
                    if current_value.as_slice() != expected {
                        return Err(CosmosDbBackendError::RootConditionConflict(
                            RemoteTransactionConflict::new(
                                name.to_vec(),
                                Some(expected.to_vec()),
                                Some(current_value),
                            ),
                        ));
                    }

                    operations.push(CosmosBatchOperation::delete(
                        document_id(&logical_key),
                        self.batch_partition_key(),
                        Some(current.etag),
                    ));
                    operation_conditions.push(condition.cloned());
                }
                (None, RemoteRootWrite::Put { manifest, .. }) if condition.is_some() => {
                    operations.push(CosmosBatchOperation::create_if_absent(
                        self.batch_partition_key(),
                        self.document(ROOT_KIND, &logical_key, manifest),
                    ));
                    operation_conditions.push(condition.cloned());
                }
                (None, RemoteRootWrite::Delete { .. }) if condition.is_some() => {
                    self.push_root_condition_operation(
                        operations,
                        operation_conditions,
                        condition.expect("condition checked"),
                    )
                    .await?;
                }
                (None, RemoteRootWrite::Put { manifest, .. }) => {
                    operations.push(CosmosBatchOperation::upsert(
                        self.batch_partition_key(),
                        self.document(ROOT_KIND, &logical_key, manifest),
                    ));
                    operation_conditions.push(None);
                }
                (None, RemoteRootWrite::Delete { .. }) => {
                    if let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? {
                        operations.push(CosmosBatchOperation::delete(
                            document_id(&logical_key),
                            self.batch_partition_key(),
                            Some(current.etag),
                        ));
                        operation_conditions.push(None);
                    }
                }
            }
            Ok(())
        }

        async fn conflict_from_batch_response(
            &self,
            responses: &[CosmosBatchOperationResponse],
            operation_conditions: &[Option<RemoteRootCondition>],
            root_conditions: &[RemoteRootCondition],
        ) -> Result<Option<RemoteTransactionConflict>, CosmosDbBackendError> {
            for (response, condition) in responses.iter().zip(operation_conditions) {
                if response.is_success() {
                    continue;
                }
                let Some(condition) = condition else {
                    continue;
                };
                let current = self.get_root_manifest(&condition.name).await?;
                return Ok(Some(RemoteTransactionConflict::new(
                    condition.name.clone(),
                    condition.expected.clone(),
                    current,
                )));
            }

            for condition in root_conditions {
                let current = self.get_root_manifest(&condition.name).await?;
                if current != condition.expected {
                    return Ok(Some(RemoteTransactionConflict::new(
                        condition.name.clone(),
                        condition.expected.clone(),
                        current,
                    )));
                }
            }

            Ok(None)
        }
    }

    impl RemoteStoreBackend for CosmosDbBackend {
        type Error = CosmosDbBackendError;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_document(NODE_KIND, &self.node_key(key))
                .await?
                .map(|doc| doc.document.value_bytes())
                .transpose()
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            self.upsert_document(NODE_KIND, &self.node_key(key), value)
                .await
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            self.delete_document(NODE_KIND, &self.node_key(key), None, true)
                .await?;
            Ok(())
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            let mut writes = HashMap::<Vec<u8>, (usize, Option<Vec<u8>>)>::with_capacity(ops.len());
            for (index, op) in ops.iter().enumerate() {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        writes.insert(key.to_vec(), (index, Some(value.to_vec())));
                    }
                    RemoteBatchOp::Delete { key } => {
                        writes.insert(key.to_vec(), (index, None));
                    }
                }
            }

            let mut writes = writes.into_iter().collect::<Vec<_>>();
            writes.sort_unstable_by_key(|(_, (index, _))| *index);
            let operations = stream::iter(writes)
                .map(|(key, (_, value))| async move {
                    self.prepare_node_batch_operation(&key, value.as_deref())
                        .await
                })
                .buffered(self.options.max_concurrency)
                .try_collect::<Vec<_>>()
                .await?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();
            self.execute_upsert_batches(&operations).await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let mut values = stream::iter(keys.iter().enumerate())
                .map(|(index, key)| async move { Ok((index, self.get_node(key).await?)) })
                .buffer_unordered(self.options.max_concurrency)
                .try_collect::<Vec<(usize, Option<Vec<u8>>)>>()
                .await?;
            values.sort_unstable_by_key(|(index, _)| *index);
            Ok(values.into_iter().map(|(_, value)| value).collect())
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            let logical_entries = entries
                .iter()
                .map(|(key, value)| {
                    let logical_key = self.node_key(key);
                    (logical_key, value.to_vec())
                })
                .collect::<Vec<_>>();
            let refs = logical_entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            let operations = self.batch_upsert_operations(NODE_KIND, &refs).await?;
            self.execute_upsert_batches(&operations).await
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            let prefix = self.family_prefix(NODE_FAMILY);
            let mut cids = self
                .query_kind(NODE_KIND, &prefix)
                .await?
                .into_iter()
                .map(|doc| doc.logical_key())
                .collect::<Result<Vec<_>, _>>()?
                .into_iter()
                .filter_map(|key| {
                    key.strip_prefix(prefix.as_slice())
                        .filter(|cid| cid.len() == 32)
                        .map(<[u8]>::to_vec)
                })
                .collect::<Vec<_>>();
            cids.sort();
            Ok(cids)
        }

        fn read_parallelism(&self) -> usize {
            self.options.read_parallelism
        }

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn supports_hints(&self) -> bool {
            true
        }

        fn prefers_rightmost_path_hints(&self) -> bool {
            self.options.rightmost_path_hints
        }

        async fn get_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
        ) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_document(HINT_KIND, &self.hint_key(namespace, key))
                .await?
                .map(|doc| doc.document.value_bytes())
                .transpose()
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            self.upsert_document(HINT_KIND, &self.hint_key(namespace, key), value)
                .await
        }

        async fn batch_put_nodes_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            let logical_entries = entries
                .iter()
                .map(|(key, value)| {
                    let logical_key = self.node_key(key);
                    (logical_key, value.to_vec())
                })
                .collect::<Vec<_>>();
            let refs = logical_entries
                .iter()
                .map(|(key, value)| (key.as_slice(), value.as_slice()))
                .collect::<Vec<_>>();
            let mut operations = self.batch_upsert_operations(NODE_KIND, &refs).await?;
            let hint_key = self.hint_key(namespace, key);
            let mut hint_operations = self
                .batch_upsert_operations(HINT_KIND, &[(hint_key.as_slice(), value)])
                .await?;

            if operations.len() < COSMOS_BATCH_OPERATION_LIMIT {
                operations.append(&mut hint_operations);
                self.execute_checked_batch(&operations).await
            } else {
                self.execute_upsert_batches(&operations).await?;
                self.execute_checked_batch(&hint_operations).await
            }
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            self.read_document(ROOT_KIND, &self.root_key(name))
                .await?
                .map(|doc| doc.document.value_bytes())
                .transpose()
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            self.upsert_document(ROOT_KIND, &self.root_key(name), manifest)
                .await
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            self.delete_document(ROOT_KIND, &self.root_key(name), None, true)
                .await?;
            Ok(())
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let logical_key = self.root_key(name);
            match (expected, new) {
                (None, Some(manifest)) => {
                    if self
                        .create_document_if_absent(ROOT_KIND, &logical_key, manifest)
                        .await?
                    {
                        Ok(RemoteManifestUpdate::Applied)
                    } else {
                        Ok(RemoteManifestUpdate::Conflict {
                            current: self.get_root_manifest(name).await?,
                        })
                    }
                }
                (Some(expected), Some(manifest)) => {
                    let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? else {
                        return Ok(RemoteManifestUpdate::Conflict { current: None });
                    };
                    let current_value = current.document.value_bytes()?;
                    if current_value.as_slice() != expected {
                        return Ok(RemoteManifestUpdate::Conflict {
                            current: Some(current_value),
                        });
                    }
                    if self
                        .replace_document_if_match(ROOT_KIND, &logical_key, manifest, &current.etag)
                        .await?
                    {
                        Ok(RemoteManifestUpdate::Applied)
                    } else {
                        Ok(RemoteManifestUpdate::Conflict {
                            current: self.get_root_manifest(name).await?,
                        })
                    }
                }
                (Some(expected), None) => {
                    let Some(current) = self.read_document(ROOT_KIND, &logical_key).await? else {
                        return Ok(RemoteManifestUpdate::Conflict { current: None });
                    };
                    let current_value = current.document.value_bytes()?;
                    if current_value.as_slice() != expected {
                        return Ok(RemoteManifestUpdate::Conflict {
                            current: Some(current_value),
                        });
                    }
                    if self
                        .delete_document(ROOT_KIND, &logical_key, Some(&current.etag), false)
                        .await?
                    {
                        Ok(RemoteManifestUpdate::Applied)
                    } else {
                        Ok(RemoteManifestUpdate::Conflict {
                            current: self.get_root_manifest(name).await?,
                        })
                    }
                }
                (None, None) => {
                    let current = self.get_root_manifest(name).await?;
                    if current.is_none() {
                        Ok(RemoteManifestUpdate::Applied)
                    } else {
                        Ok(RemoteManifestUpdate::Conflict { current })
                    }
                }
            }
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let prefix = self.family_prefix(ROOT_FAMILY);
            let mut roots = self
                .query_kind(ROOT_KIND, &prefix)
                .await?
                .into_iter()
                .filter_map(|doc| {
                    let logical_key = match doc.logical_key() {
                        Ok(key) => key,
                        Err(err) => return Some(Err(err)),
                    };
                    let name = logical_key.strip_prefix(prefix.as_slice())?;
                    let manifest = match doc.value_bytes() {
                        Ok(value) => value,
                        Err(err) => return Some(Err(err)),
                    };
                    Some(Ok(RemoteNamedRoot::new(name.to_vec(), manifest)))
                })
                .collect::<Result<Vec<_>, CosmosDbBackendError>>()?;
            roots.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(roots)
        }

        fn supports_transactions(&self) -> bool {
            true
        }

        async fn commit_transaction(
            &self,
            node_writes: &[RemoteBatchOp<'_>],
            root_conditions: &[RemoteRootCondition],
            root_writes: &[RemoteRootWrite],
        ) -> Result<RemoteTransactionUpdate, Self::Error> {
            let conditions_by_name = root_conditions
                .iter()
                .map(|condition| (condition.name.as_slice(), condition))
                .collect::<HashMap<_, _>>();
            let written_roots = root_writes
                .iter()
                .map(root_write_name)
                .collect::<HashSet<_>>();

            let mut operations = Vec::new();
            let mut operation_conditions = Vec::new();

            for condition in root_conditions {
                if !written_roots.contains(condition.name.as_slice()) {
                    if let Err(err) = self
                        .push_root_condition_operation(
                            &mut operations,
                            &mut operation_conditions,
                            condition,
                        )
                        .await
                    {
                        return match err {
                            CosmosDbBackendError::RootConditionConflict(conflict) => {
                                Ok(RemoteTransactionUpdate::Conflict(conflict))
                            }
                            err => Err(err),
                        };
                    }
                }
            }

            for write in root_writes {
                if let Err(err) = self
                    .push_root_write_operation(
                        &mut operations,
                        &mut operation_conditions,
                        write,
                        conditions_by_name.get(root_write_name(write)).copied(),
                    )
                    .await
                {
                    return match err {
                        CosmosDbBackendError::RootConditionConflict(conflict) => {
                            Ok(RemoteTransactionUpdate::Conflict(conflict))
                        }
                        err => Err(err),
                    };
                }
            }

            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { key, value } => {
                        let logical_key = self.node_key(key);
                        let node_operations = self
                            .batch_upsert_operations(NODE_KIND, &[(logical_key.as_slice(), value)])
                            .await?;
                        operation_conditions
                            .extend(std::iter::repeat(None).take(node_operations.len()));
                        operations.extend(node_operations);
                    }
                    RemoteBatchOp::Delete { key } => {
                        let logical_key = self.node_key(key);
                        if let Some(current) = self.read_document(NODE_KIND, &logical_key).await? {
                            operations.push(CosmosBatchOperation::delete(
                                document_id(&logical_key),
                                self.batch_partition_key(),
                                Some(current.etag),
                            ));
                            operation_conditions.push(None);
                        }
                    }
                }
            }

            if operations.len() > COSMOS_BATCH_OPERATION_LIMIT {
                return Err(CosmosDbBackendError::TransactionTooLarge {
                    operations: operations.len(),
                    limit: COSMOS_BATCH_OPERATION_LIMIT,
                });
            }
            if operations.is_empty() {
                return Ok(RemoteTransactionUpdate::Applied);
            }

            let responses = self.execute_transaction_batch(&operations).await?;
            if responses
                .iter()
                .all(CosmosBatchOperationResponse::is_success)
            {
                return Ok(RemoteTransactionUpdate::Applied);
            }

            if let Some(conflict) = self
                .conflict_from_batch_response(&responses, &operation_conditions, root_conditions)
                .await?
            {
                return Ok(RemoteTransactionUpdate::Conflict(conflict));
            }

            Err(batch_response_error(&responses))
        }
    }

    /// Error returned by the Cosmos DB backend.
    #[derive(Debug)]
    pub enum CosmosDbBackendError {
        /// HTTP request failed.
        Http(reqwest::Error),
        /// Cosmos DB returned an unexpected status code.
        UnexpectedStatus { status: StatusCode, body: String },
        /// Account key was not valid base64.
        InvalidAccountKey(base64::DecodeError),
        /// Stored document key was not valid hex.
        InvalidKeyHex(hex::FromHexError),
        /// Stored document value was not valid base64.
        InvalidValueBase64(base64::DecodeError),
        /// A point read response did not include an ETag.
        MissingEtag,
        /// Backend configuration is unsafe or invalid.
        InvalidConfiguration(String),
        /// The staged transaction exceeds Cosmos DB transactional batch limits.
        TransactionTooLarge { operations: usize, limit: usize },
        /// The serialized transactional batch exceeds Cosmos DB's payload limit.
        TransactionPayloadTooLarge { bytes: usize, limit: usize },
        /// A root condition failed while building a transactional batch.
        RootConditionConflict(RemoteTransactionConflict),
    }

    impl fmt::Display for CosmosDbBackendError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            match self {
                Self::Http(err) => write!(f, "Cosmos DB HTTP error: {err}"),
                Self::UnexpectedStatus { status, body } => {
                    write!(f, "Cosmos DB returned {status}: {body}")
                }
                Self::InvalidAccountKey(err) => write!(f, "invalid Cosmos DB account key: {err}"),
                Self::InvalidKeyHex(err) => write!(f, "invalid Cosmos DB document key: {err}"),
                Self::InvalidValueBase64(err) => {
                    write!(f, "invalid Cosmos DB document value: {err}")
                }
                Self::MissingEtag => f.write_str("Cosmos DB response missing ETag"),
                Self::InvalidConfiguration(message) => f.write_str(message),
                Self::TransactionTooLarge { operations, limit } => write!(
                    f,
                    "Cosmos DB transaction has {operations} operations, exceeding the limit of {limit}"
                ),
                Self::TransactionPayloadTooLarge { bytes, limit } => write!(
                    f,
                    "Cosmos DB transaction is {bytes} bytes, exceeding the limit of {limit} bytes"
                ),
                Self::RootConditionConflict(conflict) => write!(
                    f,
                    "Cosmos DB root condition conflict for {:?}",
                    String::from_utf8_lossy(&conflict.name)
                ),
            }
        }
    }

    impl StdError for CosmosDbBackendError {
        fn source(&self) -> Option<&(dyn StdError + 'static)> {
            match self {
                Self::Http(err) => Some(err),
                Self::InvalidAccountKey(err) => Some(err),
                Self::InvalidKeyHex(err) => Some(err),
                Self::InvalidValueBase64(err) => Some(err),
                _ => None,
            }
        }
    }

    #[derive(Debug, Clone, Serialize, Deserialize)]
    struct CosmosProllyDocument {
        id: String,
        kind: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        family: Option<String>,
        key: String,
        value: String,
        #[serde(default, rename = "_etag", skip_serializing_if = "Option::is_none")]
        etag: Option<String>,
    }

    impl CosmosProllyDocument {
        fn new(
            partition_key: &str,
            family: &'static str,
            logical_key: &[u8],
            value: &[u8],
        ) -> Self {
            Self {
                id: document_id(logical_key),
                kind: partition_key.to_string(),
                family: Some(family.to_string()),
                key: hex::encode(logical_key),
                value: BASE64.encode(value),
                etag: None,
            }
        }

        fn logical_key(&self) -> Result<Vec<u8>, CosmosDbBackendError> {
            hex::decode(&self.key).map_err(CosmosDbBackendError::InvalidKeyHex)
        }

        fn value_bytes(&self) -> Result<Vec<u8>, CosmosDbBackendError> {
            BASE64
                .decode(&self.value)
                .map_err(CosmosDbBackendError::InvalidValueBase64)
        }
    }

    #[derive(Debug, Serialize)]
    struct CosmosBatchOperation {
        #[serde(rename = "operationType")]
        operation_type: &'static str,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        #[serde(rename = "partitionKey")]
        partition_key: String,
        #[serde(rename = "ifMatch")]
        if_match: String,
        #[serde(rename = "ifNoneMatch")]
        if_none_match: String,
        #[serde(rename = "resourceBody", skip_serializing_if = "Option::is_none")]
        resource_body: Option<CosmosProllyDocument>,
    }

    impl CosmosBatchOperation {
        fn create_if_absent(partition_key: String, document: CosmosProllyDocument) -> Self {
            Self {
                operation_type: "Create",
                id: None,
                partition_key,
                if_match: String::new(),
                if_none_match: "*".to_string(),
                resource_body: Some(document),
            }
        }

        fn upsert(partition_key: String, document: CosmosProllyDocument) -> Self {
            Self {
                operation_type: "Upsert",
                id: None,
                partition_key,
                if_match: String::new(),
                if_none_match: String::new(),
                resource_body: Some(document),
            }
        }

        fn replace(
            id: String,
            partition_key: String,
            etag: String,
            document: CosmosProllyDocument,
        ) -> Self {
            Self {
                operation_type: "Replace",
                id: Some(id),
                partition_key,
                if_match: etag,
                if_none_match: String::new(),
                resource_body: Some(document),
            }
        }

        fn read(id: String, partition_key: String, etag: String) -> Self {
            Self {
                operation_type: "Read",
                id: Some(id),
                partition_key,
                if_match: etag,
                if_none_match: String::new(),
                resource_body: None,
            }
        }

        fn delete(id: String, partition_key: String, etag: Option<String>) -> Self {
            Self {
                operation_type: "Delete",
                id: Some(id),
                partition_key,
                if_match: etag.unwrap_or_default(),
                if_none_match: String::new(),
                resource_body: None,
            }
        }
    }

    #[derive(Debug, Deserialize)]
    struct CosmosBatchOperationResponse {
        #[serde(rename = "statusCode")]
        status_code: u16,
    }

    impl CosmosBatchOperationResponse {
        fn is_success(&self) -> bool {
            StatusCode::from_u16(self.status_code).is_ok_and(|status| status.is_success())
        }
    }

    struct CosmosReadDocument {
        document: CosmosProllyDocument,
        etag: String,
    }

    #[derive(Debug, Deserialize)]
    struct CosmosFeed {
        #[serde(rename = "Documents", alias = "documents")]
        documents: Vec<CosmosProllyDocument>,
    }

    async fn ensure_status(
        response: reqwest::Response,
    ) -> Result<reqwest::Response, CosmosDbBackendError> {
        if response.status().is_success() {
            return Ok(response);
        }

        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err(CosmosDbBackendError::UnexpectedStatus { status, body })
    }

    fn is_conflict_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::CONFLICT | StatusCode::PRECONDITION_FAILED | StatusCode::NOT_FOUND
        )
    }

    fn partition_key(value: &str) -> String {
        serde_json::to_string(&[value]).expect("serialize Cosmos DB partition key")
    }

    fn document_id(logical_key: &[u8]) -> String {
        format!("k{}", hex::encode(logical_key))
    }

    fn encode_path_segment(segment: &str) -> String {
        utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string()
    }

    fn root_write_name(write: &RemoteRootWrite) -> &[u8] {
        match write {
            RemoteRootWrite::Put { name, .. } | RemoteRootWrite::Delete { name } => name,
        }
    }

    fn batch_response_error(responses: &[CosmosBatchOperationResponse]) -> CosmosDbBackendError {
        if responses.is_empty() {
            return CosmosDbBackendError::UnexpectedStatus {
                status: StatusCode::INTERNAL_SERVER_ERROR,
                body: "Cosmos DB transactional batch returned no operation responses".to_string(),
            };
        }
        let (index, response) = responses
            .iter()
            .enumerate()
            .find(|(_, response)| !response.is_success())
            .unwrap_or_else(|| {
                (
                    0,
                    responses
                        .first()
                        .expect("transactional batch response is not empty"),
                )
            });
        let status =
            StatusCode::from_u16(response.status_code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        CosmosDbBackendError::UnexpectedStatus {
            status,
            body: format!(
                "Cosmos DB transactional batch operation {index} returned status {}; responses={responses:?}",
                response.status_code,
            ),
        }
    }

    fn validate_transaction_batch(
        operations: &[CosmosBatchOperation],
    ) -> Result<(), CosmosDbBackendError> {
        if operations.len() > COSMOS_BATCH_OPERATION_LIMIT {
            return Err(CosmosDbBackendError::TransactionTooLarge {
                operations: operations.len(),
                limit: COSMOS_BATCH_OPERATION_LIMIT,
            });
        }
        let payload_bytes = serde_json::to_vec(operations)
            .map_err(|err| CosmosDbBackendError::InvalidConfiguration(err.to_string()))?
            .len();
        if payload_bytes > COSMOS_BATCH_PAYLOAD_LIMIT {
            return Err(CosmosDbBackendError::TransactionPayloadTooLarge {
                bytes: payload_bytes,
                limit: COSMOS_BATCH_PAYLOAD_LIMIT,
            });
        }
        Ok(())
    }

    fn is_retryable_status(status: StatusCode) -> bool {
        matches!(
            status,
            StatusCode::TOO_MANY_REQUESTS
                | StatusCode::REQUEST_TIMEOUT
                | StatusCode::SERVICE_UNAVAILABLE
        )
    }

    fn retry_delay(response: &reqwest::Response, attempt: usize) -> Duration {
        if let Some(milliseconds) = response
            .headers()
            .get("x-ms-retry-after-ms")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            return Duration::from_millis(milliseconds);
        }
        if let Some(seconds) = response
            .headers()
            .get("retry-after")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
        {
            return Duration::from_secs(seconds);
        }
        Duration::from_millis(100u64.saturating_mul(1u64 << attempt.min(6)))
    }

    const COSMOS_API_VERSION: &str = "2018-12-31";
    const DOCS_RESOURCE: &str = "docs";

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_PARTITION_KEY: &str = "prolly";
    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DEFAULT_MAX_CONCURRENCY: usize = 16;
    const DEFAULT_MAX_RETRIES: usize = 9;
    const DEFAULT_MAX_RETRY_WAIT: Duration = Duration::from_secs(30);
    const DEFAULT_QUERY_PAGE_SIZE: usize = 500;
    const COSMOS_BATCH_OPERATION_LIMIT: usize = 100;
    const COSMOS_BATCH_PAYLOAD_LIMIT: usize = 2 * 1024 * 1024;

    const NODE_KIND: &str = "node";
    const ROOT_KIND: &str = "root";
    const HINT_KIND: &str = "hint";

    const NODE_FAMILY: &[u8] = b"node:";
    const ROOT_FAMILY: &[u8] = b"root:";
    const HINT_FAMILY: &[u8] = b"hint:";

    /// Default `/kind` partition value used by the adapter.
    pub const DEFAULT_PARTITION: &str = DEFAULT_PARTITION_KEY;
    /// Default logical partition for immutable nodes.
    pub const NODE_PARTITION: &str = DEFAULT_PARTITION_KEY;
    /// Default logical partition for named root manifests.
    pub const ROOT_PARTITION: &str = DEFAULT_PARTITION_KEY;
    /// Default logical partition for hints.
    pub const HINT_PARTITION: &str = DEFAULT_PARTITION_KEY;

    #[cfg(test)]
    mod tests {
        use serde_json::json;

        use super::*;

        #[test]
        fn document_layout_uses_shared_partition_and_family() {
            let document = CosmosProllyDocument::new("tenant-a", NODE_KIND, b"node:abc", b"value");

            assert_eq!(document.kind, "tenant-a");
            assert_eq!(document.family.as_deref(), Some(NODE_KIND));
            assert_eq!(document.key, hex::encode(b"node:abc"));
            assert_eq!(document.value_bytes().unwrap(), b"value");
        }

        #[test]
        fn transactional_batch_operation_uses_cosmos_rest_shape() {
            let document = CosmosProllyDocument::new("prolly", ROOT_KIND, b"root:main", b"root");
            let operation =
                CosmosBatchOperation::create_if_absent(partition_key("prolly"), document);
            let value = serde_json::to_value(operation).unwrap();

            assert_eq!(
                value,
                json!({
                    "operationType": "Create",
                    "partitionKey": "[\"prolly\"]",
                    "ifMatch": "",
                    "ifNoneMatch": "*",
                    "resourceBody": {
                        "id": document_id(b"root:main"),
                        "kind": "prolly",
                        "family": "root",
                        "key": hex::encode(b"root:main"),
                        "value": BASE64.encode(b"root")
                    }
                })
            );
        }

        #[test]
        fn transactional_batch_enforces_operation_and_payload_limits() {
            let small = || {
                CosmosBatchOperation::upsert(
                    partition_key("prolly"),
                    CosmosProllyDocument::new("prolly", NODE_KIND, b"node", b"value"),
                )
            };
            let too_many = (0..=COSMOS_BATCH_OPERATION_LIMIT)
                .map(|_| small())
                .collect::<Vec<_>>();
            assert!(matches!(
                validate_transaction_batch(&too_many),
                Err(CosmosDbBackendError::TransactionTooLarge { .. })
            ));

            let large = vec![CosmosBatchOperation::upsert(
                partition_key("prolly"),
                CosmosProllyDocument::new(
                    "prolly",
                    NODE_KIND,
                    b"large-node",
                    &vec![0; COSMOS_BATCH_PAYLOAD_LIMIT],
                ),
            )];
            assert!(matches!(
                validate_transaction_batch(&large),
                Err(CosmosDbBackendError::TransactionPayloadTooLarge { .. })
            ));
        }

        #[test]
        fn production_defaults_enable_bounded_parallelism_and_retries() {
            let options = CosmosDbBackendOptions::default();
            assert_eq!(options.max_concurrency, 16);
            assert_eq!(options.read_parallelism, 16);
            assert_eq!(options.max_retries, 9);
            assert_eq!(options.max_retry_wait, Duration::from_secs(30));
            assert!(options.rightmost_path_hints);
        }
    }
}

pub use cosmosdb::*;
