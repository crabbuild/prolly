//! Cosmos DB store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// Cosmos DB adapter entry point.
pub mod cosmosdb {
    use std::error::Error as StdError;
    use std::fmt;
    use std::time::SystemTime;

    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
    use reqwest::{Method, StatusCode};
    use serde::{Deserialize, Serialize};
    use sha2::Sha256;

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for Cosmos DB-backed prolly nodes and roots.
    pub type CosmosDbStore = crate::RemoteProllyStore<CosmosDbBackend>;

    /// Cosmos DB REST-backed backend.
    ///
    /// The container must use `/kind` as its partition key. The adapter stores
    /// JSON documents with `id`, `kind`, `key`, and `value` fields; `key` is
    /// hex-encoded logical key bytes and `value` is base64-encoded payload
    /// bytes.
    #[derive(Clone, Debug)]
    pub struct CosmosDbBackend {
        http: reqwest::Client,
        endpoint: String,
        account_key: Vec<u8>,
        database_id: String,
        container_id: String,
        container_link: String,
        key_prefix: Vec<u8>,
        read_parallelism: usize,
    }

    impl CosmosDbBackend {
        /// Create a backend using Cosmos DB key authentication.
        pub fn with_key(
            endpoint: impl Into<String>,
            account_key: &str,
            database_id: impl Into<String>,
            container_id: impl Into<String>,
        ) -> Result<Self, CosmosDbBackendError> {
            Self::with_http_client(
                reqwest::Client::new(),
                endpoint,
                account_key,
                database_id,
                container_id,
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
                read_parallelism: DEFAULT_READ_PARALLELISM,
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

        /// Set the namespace prefix prepended to all logical keys.
        pub fn with_key_prefix(mut self, key_prefix: impl Into<Vec<u8>>) -> Self {
            self.key_prefix = key_prefix.into();
            self
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
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
                let docs = self.query_kind(kind).await?;
                for doc in docs {
                    let logical_key = doc.logical_key()?;
                    if logical_key.starts_with(&self.key_prefix) {
                        self.delete_document(kind, &logical_key, None, true).await?;
                    }
                }
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
            kind: &'static str,
            logical_key: &[u8],
        ) -> Result<Option<CosmosReadDocument>, CosmosDbBackendError> {
            let id = document_id(logical_key);
            let link = self.document_link(&id);
            let response = self
                .authorized_request(Method::GET, DOCS_RESOURCE, &link, self.resource_url(&link))?
                .header("x-ms-documentdb-partitionkey", partition_key(kind))
                .send()
                .await
                .map_err(CosmosDbBackendError::Http)?;

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
            let doc = CosmosProllyDocument::new(kind, logical_key, value);
            let link = self.feed_link();
            let response = self
                .authorized_request(
                    Method::POST,
                    DOCS_RESOURCE,
                    &self.container_link,
                    self.resource_url(&link),
                )?
                .header("content-type", "application/json")
                .header("x-ms-documentdb-partitionkey", partition_key(kind))
                .header("x-ms-documentdb-is-upsert", "True")
                .json(&doc)
                .send()
                .await
                .map_err(CosmosDbBackendError::Http)?;
            ensure_status(response).await?;
            Ok(())
        }

        async fn create_document_if_absent(
            &self,
            kind: &'static str,
            logical_key: &[u8],
            value: &[u8],
        ) -> Result<bool, CosmosDbBackendError> {
            let doc = CosmosProllyDocument::new(kind, logical_key, value);
            let link = self.feed_link();
            let response = self
                .authorized_request(
                    Method::POST,
                    DOCS_RESOURCE,
                    &self.container_link,
                    self.resource_url(&link),
                )?
                .header("content-type", "application/json")
                .header("if-none-match", "*")
                .header("x-ms-documentdb-partitionkey", partition_key(kind))
                .json(&doc)
                .send()
                .await
                .map_err(CosmosDbBackendError::Http)?;
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
            let doc = CosmosProllyDocument::new(kind, logical_key, value);
            let link = self.document_link(&id);
            let response = self
                .authorized_request(Method::PUT, DOCS_RESOURCE, &link, self.resource_url(&link))?
                .header("content-type", "application/json")
                .header("if-match", etag)
                .header("x-ms-documentdb-partitionkey", partition_key(kind))
                .json(&doc)
                .send()
                .await
                .map_err(CosmosDbBackendError::Http)?;
            if is_conflict_status(response.status()) {
                return Ok(false);
            }
            ensure_status(response).await?;
            Ok(true)
        }

        async fn delete_document(
            &self,
            kind: &'static str,
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
                .header("x-ms-documentdb-partitionkey", partition_key(kind));
            if let Some(etag) = etag {
                request = request.header("if-match", etag);
            }

            let response = request.send().await.map_err(CosmosDbBackendError::Http)?;
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
        ) -> Result<Vec<CosmosProllyDocument>, CosmosDbBackendError> {
            let mut documents = Vec::new();
            let mut continuation = None;

            loop {
                let link = self.feed_link();
                let body = serde_json::json!({
                    "query": "SELECT * FROM c WHERE c.kind = @kind",
                    "parameters": [{ "name": "@kind", "value": kind }]
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
                    .header("x-ms-documentdb-partitionkey", partition_key(kind))
                    .header("x-ms-max-item-count", "100")
                    .json(&body);
                if let Some(token) = continuation.as_deref() {
                    request = request.header("x-ms-continuation", token);
                }

                let response =
                    ensure_status(request.send().await.map_err(CosmosDbBackendError::Http)?)
                        .await?;
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
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => self.put_node(key, value).await?,
                    RemoteBatchOp::Delete { key } => self.delete_node(key).await?,
                }
            }
            Ok(())
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let mut values = Vec::with_capacity(keys.len());
            for key in keys {
                values.push(self.get_node(key).await?);
            }
            Ok(values)
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            for (key, value) in entries {
                self.put_node(key, value).await?;
            }
            Ok(())
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            let prefix = self.family_prefix(NODE_FAMILY);
            let mut cids = self
                .query_kind(NODE_KIND)
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
            self.read_parallelism
        }

        fn supports_hints(&self) -> bool {
            true
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
            self.batch_put_nodes(entries).await?;
            self.put_hint(namespace, key, value).await
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
                .query_kind(ROOT_KIND)
                .await?
                .into_iter()
                .filter_map(|doc| {
                    let logical_key = match doc.logical_key() {
                        Ok(key) => key,
                        Err(err) => return Some(Err(err)),
                    };
                    let Some(name) = logical_key.strip_prefix(prefix.as_slice()) else {
                        return None;
                    };
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
        key: String,
        value: String,
    }

    impl CosmosProllyDocument {
        fn new(kind: &'static str, logical_key: &[u8], value: &[u8]) -> Self {
            Self {
                id: document_id(logical_key),
                kind: kind.to_string(),
                key: hex::encode(logical_key),
                value: BASE64.encode(value),
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

    fn partition_key(kind: &'static str) -> String {
        format!(r#"["{kind}"]"#)
    }

    fn document_id(logical_key: &[u8]) -> String {
        format!("k{}", hex::encode(logical_key))
    }

    fn encode_path_segment(segment: &str) -> String {
        utf8_percent_encode(segment, NON_ALPHANUMERIC).to_string()
    }

    const COSMOS_API_VERSION: &str = "2018-12-31";
    const DOCS_RESOURCE: &str = "docs";

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_READ_PARALLELISM: usize = 16;

    const NODE_KIND: &str = "node";
    const ROOT_KIND: &str = "root";
    const HINT_KIND: &str = "hint";

    const NODE_FAMILY: &[u8] = b"node:";
    const ROOT_FAMILY: &[u8] = b"root:";
    const HINT_FAMILY: &[u8] = b"hint:";

    /// Recommended logical partition for immutable nodes.
    pub const NODE_PARTITION: &str = "nodes";
    /// Recommended logical partition for named root manifests.
    pub const ROOT_PARTITION: &str = "roots";
    /// Recommended logical partition for hints.
    pub const HINT_PARTITION: &str = "hints";
}

pub use cosmosdb::*;
