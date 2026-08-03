#![doc = include_str!("../README.md")]

pub use prolly::{
    BlockingRemoteBuildError, BlockingRemoteProllyStore, BlockingRemoteStoreError, RemoteBatchOp,
    RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteRootCondition, RemoteRootWrite,
    RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
};

/// Redis adapter entry point.
pub mod redis {
    use std::{collections::HashSet, ops::Range, sync::LazyLock, time::Duration};

    use redis_client::{ErrorKind, RedisError, Script, Value};

    use crate::{
        RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteRootCondition, RemoteRootWrite,
        RemoteStoreBackend, RemoteTransactionConflict, RemoteTransactionUpdate,
    };

    /// Store adapter for Redis-backed prolly nodes and roots.
    ///
    /// Redis should be treated as a cache or edge store unless persistence and
    /// durability are explicitly configured for the Redis deployment.
    pub type RedisStore = crate::RemoteProllyStore<RedisBackend>;

    /// Synchronous Redis store supporting `Prolly::indexed_map`.
    pub type SyncRedisStore = crate::BlockingRemoteProllyStore<RedisBackend>;

    /// Redis-backed prolly node/root backend.
    #[derive(Clone)]
    pub struct RedisBackend {
        control_connection: redis_client::aio::ConnectionManager,
        bulk_connection: redis_client::aio::ConnectionManager,
        key_prefix: Vec<u8>,
        options: RedisBackendOptions,
    }

    /// Operational limits and connection settings for [`RedisBackend`].
    ///
    /// Bounded multi-key commands avoid oversized request buffers and long
    /// single-threaded Redis stalls. Connections created by
    /// [`RedisBackend::connect_with_options`] also apply the configured
    /// timeouts.
    #[derive(Clone, Debug, PartialEq, Eq)]
    pub struct RedisBackendOptions {
        max_batch_items: usize,
        max_batch_bytes: usize,
        read_parallelism: usize,
        scan_count: usize,
        delete_chunk_size: usize,
        response_timeout: Option<Duration>,
        connection_timeout: Option<Duration>,
        rightmost_path_hints: bool,
    }

    impl Default for RedisBackendOptions {
        fn default() -> Self {
            Self {
                max_batch_items: DEFAULT_MAX_BATCH_ITEMS,
                max_batch_bytes: DEFAULT_MAX_BATCH_BYTES,
                read_parallelism: DEFAULT_READ_PARALLELISM,
                scan_count: DEFAULT_SCAN_COUNT,
                delete_chunk_size: DEFAULT_DELETE_CHUNK_SIZE,
                response_timeout: None,
                connection_timeout: None,
                rightmost_path_hints: true,
            }
        }
    }

    impl RedisBackendOptions {
        /// Limit the number of items encoded in one Redis multi-key command.
        pub fn with_max_batch_items(mut self, max_batch_items: usize) -> Self {
            self.max_batch_items = max_batch_items.max(1);
            self
        }

        /// Limit the approximate key/value payload in one multi-key command.
        pub fn with_max_batch_bytes(mut self, max_batch_bytes: usize) -> Self {
            self.max_batch_bytes = max_batch_bytes.max(1);
            self
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Set the approximate number of keys requested from each `SCAN`.
        pub fn with_scan_count(mut self, scan_count: usize) -> Self {
            self.scan_count = scan_count.max(1);
            self
        }

        /// Set the maximum number of keys passed to each namespace cleanup.
        pub fn with_delete_chunk_size(mut self, delete_chunk_size: usize) -> Self {
            self.delete_chunk_size = delete_chunk_size.max(1);
            self
        }

        /// Set the maximum time to await a Redis response.
        pub fn with_response_timeout(mut self, response_timeout: Duration) -> Self {
            self.response_timeout = Some(response_timeout);
            self
        }

        /// Set the maximum time for each Redis connection attempt.
        pub fn with_connection_timeout(mut self, connection_timeout: Duration) -> Self {
            self.connection_timeout = Some(connection_timeout);
            self
        }

        /// Enable or disable persisted rightmost-path hints for append-heavy writes.
        pub fn with_rightmost_path_hints(mut self, enabled: bool) -> Self {
            self.rightmost_path_hints = enabled;
            self
        }

        /// Maximum items encoded in one Redis multi-key command.
        pub fn max_batch_items(&self) -> usize {
            self.max_batch_items
        }

        /// Approximate maximum payload encoded in one Redis multi-key command.
        pub fn max_batch_bytes(&self) -> usize {
            self.max_batch_bytes
        }

        /// Read parallelism advertised to async prolly traversals.
        pub fn read_parallelism(&self) -> usize {
            self.read_parallelism
        }

        /// Approximate number of keys requested from each `SCAN`.
        pub fn scan_count(&self) -> usize {
            self.scan_count
        }

        /// Maximum keys passed to each namespace cleanup command.
        pub fn delete_chunk_size(&self) -> usize {
            self.delete_chunk_size
        }

        /// Configured Redis response timeout.
        pub fn response_timeout(&self) -> Option<Duration> {
            self.response_timeout
        }

        /// Configured Redis connection timeout.
        pub fn connection_timeout(&self) -> Option<Duration> {
            self.connection_timeout
        }

        /// Whether append-heavy writes maintain rightmost-path hints.
        pub fn rightmost_path_hints(&self) -> bool {
            self.rightmost_path_hints
        }
    }

    impl std::fmt::Debug for RedisBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RedisBackend")
                .field("key_prefix", &self.key_prefix)
                .field("options", &self.options)
                .finish_non_exhaustive()
        }
    }

    impl RedisBackend {
        /// Create a backend from an existing Redis connection manager.
        pub fn new(connection: redis_client::aio::ConnectionManager) -> Self {
            Self::new_with_options(
                connection.clone(),
                connection,
                RedisBackendOptions::default(),
            )
        }

        /// Create a backend with separate control and bulk connection managers.
        ///
        /// Supplying distinct managers prevents large scans and multi-key
        /// commands from queueing latency-sensitive root operations behind
        /// them on the same physical connection.
        pub fn new_with_options(
            control_connection: redis_client::aio::ConnectionManager,
            bulk_connection: redis_client::aio::ConnectionManager,
            options: RedisBackendOptions,
        ) -> Self {
            Self {
                control_connection,
                bulk_connection,
                key_prefix: DEFAULT_KEY_PREFIX.to_vec(),
                options,
            }
        }

        /// Connect to Redis using `redis_url`.
        pub async fn connect(redis_url: &str) -> Result<Self, RedisError> {
            Self::connect_with_options(redis_url, RedisBackendOptions::default()).await
        }

        /// Connect to Redis using `redis_url` and explicit operational options.
        pub async fn connect_with_options(
            redis_url: &str,
            options: RedisBackendOptions,
        ) -> Result<Self, RedisError> {
            let client = redis_client::Client::open(redis_url)?;
            Self::from_client_with_options(client, options).await
        }

        /// Create a backend from an existing Redis client.
        pub async fn from_client(client: redis_client::Client) -> Result<Self, RedisError> {
            Self::from_client_with_options(client, RedisBackendOptions::default()).await
        }

        /// Create a backend from a Redis client and explicit operational options.
        pub async fn from_client_with_options(
            client: redis_client::Client,
            options: RedisBackendOptions,
        ) -> Result<Self, RedisError> {
            let connection_config = connection_manager_config(&options);
            let control_connection = client
                .get_connection_manager_with_config(connection_config.clone())
                .await?;
            let bulk_connection = client
                .get_connection_manager_with_config(connection_config)
                .await?;
            Ok(Self::new_with_options(
                control_connection,
                bulk_connection,
                options,
            ))
        }

        /// Borrow the underlying connection manager.
        pub fn connection(&self) -> &redis_client::aio::ConnectionManager {
            &self.control_connection
        }

        /// Borrow the connection manager used for scans and multi-key commands.
        pub fn bulk_connection(&self) -> &redis_client::aio::ConnectionManager {
            &self.bulk_connection
        }

        /// Return this backend's operational options.
        pub fn options(&self) -> &RedisBackendOptions {
            &self.options
        }

        /// Return the namespace prefix prepended to all Redis keys.
        pub fn key_prefix(&self) -> &[u8] {
            &self.key_prefix
        }

        /// Set the namespace prefix prepended to all Redis keys.
        ///
        /// Use a unique prefix when running tests or sharing a Redis database.
        pub fn with_key_prefix(mut self, key_prefix: impl Into<Vec<u8>>) -> Self {
            self.key_prefix = key_prefix.into();
            self
        }

        /// Set the read parallelism advertised to async prolly traversals.
        pub fn with_read_parallelism(mut self, read_parallelism: usize) -> Self {
            self.options.read_parallelism = read_parallelism.max(1);
            self
        }

        /// Delete every key under this backend's namespace prefix.
        ///
        /// This is primarily intended for isolated integration tests.
        pub async fn clear_namespace(&self) -> Result<(), RedisError> {
            if self.key_prefix.is_empty() {
                return Err(redis_type_error(
                    "refusing to clear an empty Redis key prefix",
                ));
            }

            let mut pattern = self.key_prefix.clone();
            pattern.push(b'*');
            let keys = self.scan_keys(&pattern).await?;
            self.delete_keys(&keys).await
        }

        fn node_key(&self, key: &[u8]) -> Vec<u8> {
            self.family_key(NODE_FAMILY, key)
        }

        fn root_key(&self, name: &[u8]) -> Vec<u8> {
            self.family_key(ROOT_FAMILY, name)
        }

        fn hint_key(&self, namespace: &[u8], key: &[u8]) -> Vec<u8> {
            let mut redis_key = self.family_key(HINT_FAMILY, &[]);
            redis_key.extend_from_slice(&(namespace.len() as u64).to_be_bytes());
            redis_key.extend_from_slice(namespace);
            redis_key.extend_from_slice(key);
            redis_key
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

        fn family_pattern(&self, family: &[u8]) -> Vec<u8> {
            let mut pattern = self.family_prefix(family);
            pattern.push(b'*');
            pattern
        }

        async fn scan_keys(&self, pattern: &[u8]) -> Result<Vec<Vec<u8>>, RedisError> {
            let mut connection = self.bulk_connection.clone();
            let mut cursor = 0_u64;
            let mut keys = Vec::new();

            loop {
                let (next_cursor, batch): (u64, Vec<Vec<u8>>) = redis_client::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(self.options.scan_count)
                    .query_async(&mut connection)
                    .await?;
                keys.extend(batch);
                if next_cursor == 0 {
                    break;
                }
                cursor = next_cursor;
            }

            Ok(keys)
        }

        async fn delete_keys(&self, keys: &[Vec<u8>]) -> Result<(), RedisError> {
            if keys.is_empty() {
                return Ok(());
            }

            let mut connection = self.bulk_connection.clone();
            for chunk in keys.chunks(self.options.delete_chunk_size) {
                let mut command = redis_client::cmd("UNLINK");
                for key in chunk {
                    command.arg(key.as_slice());
                }
                command.query_async::<()>(&mut connection).await?;
            }
            Ok(())
        }

        fn command_ranges<F>(&self, len: usize, item_bytes: F) -> Vec<Range<usize>>
        where
            F: Fn(usize) -> usize,
        {
            bounded_ranges(
                len,
                self.options.max_batch_items,
                self.options.max_batch_bytes,
                item_bytes,
            )
        }

        async fn mget_raw_keys(
            &self,
            keys: &[Vec<u8>],
        ) -> Result<Vec<Option<Vec<u8>>>, RedisError> {
            let ranges = self.command_ranges(keys.len(), |index| keys[index].len());
            let mut values = Vec::with_capacity(keys.len());
            let mut connection = self.bulk_connection.clone();
            for range in ranges {
                let mut command = redis_client::cmd("MGET");
                for key in &keys[range] {
                    command.arg(key.as_slice());
                }
                let mut chunk: Vec<Option<Vec<u8>>> = command.query_async(&mut connection).await?;
                values.append(&mut chunk);
            }
            Ok(values)
        }
    }

    impl RemoteStoreBackend for RedisBackend {
        type Error = RedisError;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut connection = self.bulk_connection.clone();
            redis_client::cmd("GET")
                .arg(self.node_key(key))
                .query_async(&mut connection)
                .await
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.bulk_connection.clone();
            redis_client::cmd("SET")
                .arg(self.node_key(key))
                .arg(value)
                .query_async::<()>(&mut connection)
                .await
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.bulk_connection.clone();
            redis_client::cmd("DEL")
                .arg(self.node_key(key))
                .query_async::<()>(&mut connection)
                .await
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            if ops.is_empty() {
                return Ok(());
            }

            // Redis applies commands in a transaction sequentially. Coalescing
            // duplicate keys to the final operation preserves that behavior
            // while avoiding redundant writes.
            let mut seen = HashSet::with_capacity(ops.len());
            let mut effective = Vec::with_capacity(ops.len());
            for op in ops.iter().rev() {
                let key = match op {
                    RemoteBatchOp::Upsert { key, .. } | RemoteBatchOp::Delete { key } => *key,
                };
                if seen.insert(key) {
                    effective.push(op);
                }
            }
            effective.reverse();

            let upserts = effective
                .iter()
                .filter_map(|op| match op {
                    RemoteBatchOp::Upsert { key, value } => Some((*key, *value)),
                    RemoteBatchOp::Delete { .. } => None,
                })
                .collect::<Vec<_>>();
            let deletes = effective
                .iter()
                .filter_map(|op| match op {
                    RemoteBatchOp::Delete { key } => Some(*key),
                    RemoteBatchOp::Upsert { .. } => None,
                })
                .collect::<Vec<_>>();
            let upsert_ranges = self.command_ranges(upserts.len(), |index| {
                self.key_prefix.len()
                    + NODE_FAMILY.len()
                    + upserts[index].0.len()
                    + upserts[index].1.len()
            });
            let delete_ranges = self.command_ranges(deletes.len(), |index| {
                self.key_prefix.len() + NODE_FAMILY.len() + deletes[index].len()
            });

            let mut connection = self.bulk_connection.clone();
            if upsert_ranges.len() == 1 && delete_ranges.is_empty() {
                let mut command = redis_client::cmd("MSET");
                for (key, value) in &upserts[upsert_ranges[0].clone()] {
                    command.arg(self.node_key(key)).arg(*value);
                }
                return command.query_async::<()>(&mut connection).await;
            }
            if delete_ranges.len() == 1 && upsert_ranges.is_empty() {
                let mut command = redis_client::cmd("DEL");
                for key in &deletes[delete_ranges[0].clone()] {
                    command.arg(self.node_key(key));
                }
                return command.query_async::<()>(&mut connection).await;
            }

            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for range in upsert_ranges {
                let command = pipeline.cmd("MSET");
                for (key, value) in &upserts[range] {
                    command.arg(self.node_key(key)).arg(*value);
                }
                command.ignore();
            }
            for range in delete_ranges {
                let command = pipeline.cmd("DEL");
                for key in &deletes[range] {
                    command.arg(self.node_key(key));
                }
                command.ignore();
            }

            pipeline.query_async::<()>(&mut connection).await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            let redis_keys = keys
                .iter()
                .map(|key| self.node_key(key))
                .collect::<Vec<_>>();
            self.mget_raw_keys(&redis_keys).await
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            if entries.is_empty() {
                return Ok(());
            }

            let ranges = self.command_ranges(entries.len(), |index| {
                self.key_prefix.len()
                    + NODE_FAMILY.len()
                    + entries[index].0.len()
                    + entries[index].1.len()
            });
            let mut connection = self.bulk_connection.clone();
            if ranges.len() == 1 {
                let mut command = redis_client::cmd("MSET");
                for (key, value) in entries {
                    command.arg(self.node_key(key)).arg(*value);
                }
                return command.query_async::<()>(&mut connection).await;
            }

            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for range in ranges {
                let command = pipeline.cmd("MSET");
                for (key, value) in &entries[range] {
                    command.arg(self.node_key(key)).arg(*value);
                }
                command.ignore();
            }
            pipeline.query_async::<()>(&mut connection).await
        }

        async fn list_node_cids(&self) -> Result<Vec<Vec<u8>>, Self::Error> {
            let prefix = self.family_prefix(NODE_FAMILY);
            let pattern = self.family_pattern(NODE_FAMILY);
            let mut cids = self
                .scan_keys(&pattern)
                .await?
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

        fn prefers_batch_reads(&self) -> bool {
            true
        }

        fn read_parallelism(&self) -> usize {
            self.options.read_parallelism
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
            let mut connection = self.control_connection.clone();
            redis_client::cmd("GET")
                .arg(self.hint_key(namespace, key))
                .query_async(&mut connection)
                .await
        }

        async fn put_hint(
            &self,
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            let mut connection = self.control_connection.clone();
            redis_client::cmd("SET")
                .arg(self.hint_key(namespace, key))
                .arg(value)
                .query_async::<()>(&mut connection)
                .await
        }

        async fn batch_put_nodes_with_hint(
            &self,
            entries: &[(&[u8], &[u8])],
            namespace: &[u8],
            key: &[u8],
            value: &[u8],
        ) -> Result<(), Self::Error> {
            let ranges = self.command_ranges(entries.len(), |index| {
                self.key_prefix.len()
                    + NODE_FAMILY.len()
                    + entries[index].0.len()
                    + entries[index].1.len()
            });
            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for range in ranges {
                let command = pipeline.cmd("MSET");
                for (key, value) in &entries[range] {
                    command.arg(self.node_key(key)).arg(*value);
                }
                command.ignore();
            }
            pipeline
                .cmd("SET")
                .arg(self.hint_key(namespace, key))
                .arg(value)
                .ignore();

            let mut connection = self.bulk_connection.clone();
            pipeline.query_async::<()>(&mut connection).await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut connection = self.control_connection.clone();
            redis_client::cmd("GET")
                .arg(self.root_key(name))
                .query_async(&mut connection)
                .await
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.control_connection.clone();
            redis_client::cmd("SET")
                .arg(self.root_key(name))
                .arg(manifest)
                .query_async::<()>(&mut connection)
                .await
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.control_connection.clone();
            redis_client::cmd("DEL")
                .arg(self.root_key(name))
                .query_async::<()>(&mut connection)
                .await
        }

        async fn compare_and_swap_root_manifest(
            &self,
            name: &[u8],
            expected: Option<&[u8]>,
            new: Option<&[u8]>,
        ) -> Result<RemoteManifestUpdate, Self::Error> {
            let mut invocation = ROOT_CAS_SCRIPT.prepare_invoke();
            invocation
                .key(self.root_key(name))
                .arg(if expected.is_some() { b"1" } else { b"0" }.as_slice())
                .arg(expected.unwrap_or_default())
                .arg(if new.is_some() { b"1" } else { b"0" }.as_slice())
                .arg(new.unwrap_or_default());

            let mut connection = self.control_connection.clone();
            let response: Value = invocation.invoke_async(&mut connection).await?;
            parse_root_cas_response(response)
        }

        async fn list_root_manifests(&self) -> Result<Vec<RemoteNamedRoot>, Self::Error> {
            let prefix = self.family_prefix(ROOT_FAMILY);
            let pattern = self.family_pattern(ROOT_FAMILY);
            let mut names = self
                .scan_keys(&pattern)
                .await?
                .into_iter()
                .filter_map(|key| key.strip_prefix(prefix.as_slice()).map(<[u8]>::to_vec))
                .collect::<Vec<_>>();
            names.sort();

            let redis_keys = names
                .iter()
                .map(|name| self.root_key(name))
                .collect::<Vec<_>>();
            let manifests = self.mget_raw_keys(&redis_keys).await?;
            Ok(names
                .into_iter()
                .zip(manifests)
                .filter_map(|(name, manifest)| {
                    manifest.map(|manifest| RemoteNamedRoot::new(name, manifest))
                })
                .collect())
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
            let mut invocation = TRANSACTION_COMMIT_SCRIPT.prepare_invoke();
            for condition in root_conditions {
                invocation.key(self.root_key(&condition.name));
            }
            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { key, .. } | RemoteBatchOp::Delete { key } => {
                        invocation.key(self.node_key(key));
                    }
                }
            }
            for write in root_writes {
                match write {
                    RemoteRootWrite::Put { name, .. } | RemoteRootWrite::Delete { name } => {
                        invocation.key(self.root_key(name));
                    }
                }
            }

            invocation
                .arg(root_conditions.len())
                .arg(node_writes.len())
                .arg(root_writes.len());
            for condition in root_conditions {
                invocation
                    .arg(
                        if condition.expected.is_some() {
                            b"1"
                        } else {
                            b"0"
                        }
                        .as_slice(),
                    )
                    .arg(condition.expected.as_deref().unwrap_or_default());
            }
            for write in node_writes {
                match write {
                    RemoteBatchOp::Upsert { value, .. } => {
                        invocation.arg("upsert").arg(*value);
                    }
                    RemoteBatchOp::Delete { .. } => {
                        invocation.arg("delete");
                    }
                }
            }
            for write in root_writes {
                match write {
                    RemoteRootWrite::Put { manifest, .. } => {
                        invocation.arg("put").arg(manifest);
                    }
                    RemoteRootWrite::Delete { .. } => {
                        invocation.arg("delete");
                    }
                }
            }

            let mut connection = self.control_connection.clone();
            let response: Value = invocation.invoke_async(&mut connection).await?;
            parse_transaction_response(response, root_conditions)
        }
    }

    fn connection_manager_config(
        options: &RedisBackendOptions,
    ) -> redis_client::aio::ConnectionManagerConfig {
        let mut config = redis_client::aio::ConnectionManagerConfig::new();
        if let Some(timeout) = options.response_timeout {
            config = config.set_response_timeout(timeout);
        }
        if let Some(timeout) = options.connection_timeout {
            config = config.set_connection_timeout(timeout);
        }
        config
    }

    fn bounded_ranges<F>(
        len: usize,
        max_items: usize,
        max_bytes: usize,
        item_bytes: F,
    ) -> Vec<Range<usize>>
    where
        F: Fn(usize) -> usize,
    {
        let mut ranges = Vec::new();
        let mut start = 0;
        while start < len {
            let mut end = start;
            let mut bytes = 0usize;
            while end < len && end - start < max_items {
                let next_bytes = item_bytes(end);
                if end > start && bytes.saturating_add(next_bytes) > max_bytes {
                    break;
                }
                bytes = bytes.saturating_add(next_bytes);
                end += 1;
            }
            ranges.push(start..end);
            start = end;
        }
        ranges
    }

    fn parse_root_cas_response(response: Value) -> Result<RemoteManifestUpdate, RedisError> {
        let Value::Array(values) = response else {
            return Err(redis_type_error("root CAS script returned a non-array"));
        };
        let [applied, current] = values
            .try_into()
            .map_err(|_| redis_type_error("root CAS script returned wrong arity"))?;

        if value_to_bool(applied)? {
            return Ok(RemoteManifestUpdate::Applied);
        }

        Ok(RemoteManifestUpdate::Conflict {
            current: value_to_optional_bytes(current)?,
        })
    }

    fn value_to_bool(value: Value) -> Result<bool, RedisError> {
        match value {
            Value::Int(0) => Ok(false),
            Value::Int(1) => Ok(true),
            Value::Boolean(value) => Ok(value),
            other => Err(redis_type_error(format!(
                "root CAS script returned invalid applied flag: {other:?}"
            ))),
        }
    }

    fn value_to_usize(value: Value) -> Result<usize, RedisError> {
        match value {
            Value::Int(value) if value >= 0 => Ok(value as usize),
            other => Err(redis_type_error(format!(
                "transaction script returned invalid conflict index: {other:?}"
            ))),
        }
    }

    fn value_to_optional_bytes(value: Value) -> Result<Option<Vec<u8>>, RedisError> {
        match value {
            Value::Nil => Ok(None),
            Value::Boolean(false) => Ok(None),
            Value::BulkString(bytes) => Ok(Some(bytes)),
            other => Err(redis_type_error(format!(
                "root CAS script returned invalid current manifest: {other:?}"
            ))),
        }
    }

    fn parse_transaction_response(
        response: Value,
        root_conditions: &[RemoteRootCondition],
    ) -> Result<RemoteTransactionUpdate, RedisError> {
        let Value::Array(values) = response else {
            return Err(redis_type_error("transaction script returned a non-array"));
        };
        let [applied, conflict_index, current] = values
            .try_into()
            .map_err(|_| redis_type_error("transaction script returned wrong arity"))?;

        if value_to_bool(applied)? {
            return Ok(RemoteTransactionUpdate::Applied);
        }

        let index = value_to_usize(conflict_index)?;
        if index == 0 || index > root_conditions.len() {
            return Err(redis_type_error(format!(
                "transaction script returned out-of-range conflict index: {index}"
            )));
        }
        let condition = &root_conditions[index - 1];
        Ok(RemoteTransactionUpdate::Conflict(
            RemoteTransactionConflict::new(
                condition.name.clone(),
                condition.expected.clone(),
                value_to_optional_bytes(current)?,
            ),
        ))
    }

    fn redis_type_error(detail: impl Into<String>) -> RedisError {
        (
            ErrorKind::TypeError,
            "unexpected Redis adapter response",
            detail.into(),
        )
            .into()
    }

    const DEFAULT_KEY_PREFIX: &[u8] = b"prolly:";
    const DEFAULT_READ_PARALLELISM: usize = 16;
    const DEFAULT_MAX_BATCH_ITEMS: usize = 1024;
    const DEFAULT_MAX_BATCH_BYTES: usize = 8 * 1024 * 1024;
    const DEFAULT_SCAN_COUNT: usize = 1024;
    const DEFAULT_DELETE_CHUNK_SIZE: usize = 512;

    const NODE_FAMILY: &[u8] = b"node:";
    const ROOT_FAMILY: &[u8] = b"root:";
    const HINT_FAMILY: &[u8] = b"hint:";

    /// Recommended key prefix for immutable node values.
    pub const NODE_KEY_PREFIX: &str = "prolly:node:";
    /// Recommended key prefix for named root manifests.
    pub const ROOT_KEY_PREFIX: &str = "prolly:root:";
    /// Recommended key prefix for hints.
    pub const HINT_KEY_PREFIX: &str = "prolly:hint:";

    static ROOT_CAS_SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new(ROOT_CAS_LUA));
    static TRANSACTION_COMMIT_SCRIPT: LazyLock<Script> =
        LazyLock::new(|| Script::new(TRANSACTION_COMMIT_LUA));

    const ROOT_CAS_LUA: &str = r#"
local current = redis.call('GET', KEYS[1])
local has_expected = ARGV[1]
local expected = ARGV[2]
local has_new = ARGV[3]
local new_value = ARGV[4]

if has_expected == '1' then
  if current == false or current ~= expected then
    return {0, current}
  end
else
  if current ~= false then
    return {0, current}
  end
end

if has_new == '1' then
  redis.call('SET', KEYS[1], new_value)
else
  redis.call('DEL', KEYS[1])
end

return {1, false}
"#;

    const TRANSACTION_COMMIT_LUA: &str = r#"
local condition_count = tonumber(ARGV[1])
local node_write_count = tonumber(ARGV[2])
local root_write_count = tonumber(ARGV[3])
local arg_index = 4

for i = 1, condition_count do
  local current = redis.call('GET', KEYS[i])
  local has_expected = ARGV[arg_index]
  local expected = ARGV[arg_index + 1]
  arg_index = arg_index + 2

  if has_expected == '1' then
    if current == false or current ~= expected then
      return {0, i, current}
    end
  else
    if current ~= false then
      return {0, i, current}
    end
  end
end

local node_key_offset = condition_count
for i = 1, node_write_count do
  local kind = ARGV[arg_index]
  arg_index = arg_index + 1
  local key = KEYS[node_key_offset + i]

  if kind == 'upsert' then
    redis.call('SET', key, ARGV[arg_index])
    arg_index = arg_index + 1
  elseif kind == 'delete' then
    redis.call('DEL', key)
  else
    error('unknown transaction node op: ' .. tostring(kind))
  end
end

local root_key_offset = condition_count + node_write_count
for i = 1, root_write_count do
  local kind = ARGV[arg_index]
  arg_index = arg_index + 1
  local key = KEYS[root_key_offset + i]

  if kind == 'put' then
    redis.call('SET', key, ARGV[arg_index])
    arg_index = arg_index + 1
  elseif kind == 'delete' then
    redis.call('DEL', key)
  else
    error('unknown transaction root op: ' .. tostring(kind))
  end
end

return {1, 0, false}
"#;

    #[cfg(test)]
    mod tests {
        use super::{bounded_ranges, RedisBackendOptions};

        #[test]
        fn default_options_are_bounded_and_enable_append_hints() {
            let options = RedisBackendOptions::default();
            assert_eq!(options.max_batch_items(), 1024);
            assert_eq!(options.max_batch_bytes(), 8 * 1024 * 1024);
            assert_eq!(options.read_parallelism(), 16);
            assert_eq!(options.scan_count(), 1024);
            assert_eq!(options.delete_chunk_size(), 512);
            assert!(options.rightmost_path_hints());
            assert_eq!(options.response_timeout(), None);
            assert_eq!(options.connection_timeout(), None);
        }

        #[test]
        fn bounded_ranges_respect_item_and_byte_limits() {
            let sizes = [2, 3, 7, 1, 1];
            assert_eq!(
                bounded_ranges(sizes.len(), 3, 5, |index| sizes[index]),
                vec![0..2, 2..3, 3..5]
            );
        }

        #[test]
        fn bounded_ranges_make_progress_for_one_oversized_item() {
            assert_eq!(bounded_ranges(2, 10, 1, |_| 100), vec![0..1, 1..2]);
        }

        #[test]
        fn zero_option_limits_are_clamped() {
            let options = RedisBackendOptions::default()
                .with_max_batch_items(0)
                .with_max_batch_bytes(0)
                .with_read_parallelism(0)
                .with_scan_count(0)
                .with_delete_chunk_size(0);
            assert_eq!(options.max_batch_items(), 1);
            assert_eq!(options.max_batch_bytes(), 1);
            assert_eq!(options.read_parallelism(), 1);
            assert_eq!(options.scan_count(), 1);
            assert_eq!(options.delete_chunk_size(), 1);
        }
    }
}

pub use redis::*;
