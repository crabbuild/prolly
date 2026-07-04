//! Redis store adapter for prolly-map.

pub use prolly::{
    RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteProllyStore, RemoteStoreBackend,
};

/// Redis adapter entry point.
pub mod redis {
    use redis_client::{ErrorKind, RedisError, Script, Value};

    use crate::{RemoteBatchOp, RemoteManifestUpdate, RemoteNamedRoot, RemoteStoreBackend};

    /// Store adapter for Redis-backed prolly nodes and roots.
    ///
    /// Redis should be treated as a cache or edge store unless persistence and
    /// durability are explicitly configured for the Redis deployment.
    pub type RedisStore = crate::RemoteProllyStore<RedisBackend>;

    /// Redis-backed prolly node/root backend.
    #[derive(Clone)]
    pub struct RedisBackend {
        connection: redis_client::aio::ConnectionManager,
        key_prefix: Vec<u8>,
        read_parallelism: usize,
    }

    impl std::fmt::Debug for RedisBackend {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("RedisBackend")
                .field("key_prefix", &self.key_prefix)
                .field("read_parallelism", &self.read_parallelism)
                .finish_non_exhaustive()
        }
    }

    impl RedisBackend {
        /// Create a backend from an existing Redis connection manager.
        pub fn new(connection: redis_client::aio::ConnectionManager) -> Self {
            Self {
                connection,
                key_prefix: DEFAULT_KEY_PREFIX.to_vec(),
                read_parallelism: DEFAULT_READ_PARALLELISM,
            }
        }

        /// Connect to Redis using `redis_url`.
        pub async fn connect(redis_url: &str) -> Result<Self, RedisError> {
            let client = redis_client::Client::open(redis_url)?;
            Self::from_client(client).await
        }

        /// Create a backend from an existing Redis client.
        pub async fn from_client(client: redis_client::Client) -> Result<Self, RedisError> {
            Ok(Self::new(client.get_connection_manager().await?))
        }

        /// Borrow the underlying connection manager.
        pub fn connection(&self) -> &redis_client::aio::ConnectionManager {
            &self.connection
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
            self.read_parallelism = read_parallelism.max(1);
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
            let mut connection = self.connection.clone();
            let mut cursor = 0_u64;
            let mut keys = Vec::new();

            loop {
                let (next_cursor, batch): (u64, Vec<Vec<u8>>) = redis_client::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(SCAN_COUNT)
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

            let mut connection = self.connection.clone();
            for chunk in keys.chunks(DELETE_CHUNK_SIZE) {
                let mut command = redis_client::cmd("DEL");
                for key in chunk {
                    command.arg(key.as_slice());
                }
                command.query_async::<()>(&mut connection).await?;
            }
            Ok(())
        }
    }

    impl RemoteStoreBackend for RedisBackend {
        type Error = RedisError;

        async fn get_node(&self, key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut connection = self.connection.clone();
            redis_client::cmd("GET")
                .arg(self.node_key(key))
                .query_async(&mut connection)
                .await
        }

        async fn put_node(&self, key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.connection.clone();
            redis_client::cmd("SET")
                .arg(self.node_key(key))
                .arg(value)
                .query_async::<()>(&mut connection)
                .await
        }

        async fn delete_node(&self, key: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.connection.clone();
            redis_client::cmd("DEL")
                .arg(self.node_key(key))
                .query_async::<()>(&mut connection)
                .await
        }

        async fn batch_nodes(&self, ops: &[RemoteBatchOp<'_>]) -> Result<(), Self::Error> {
            if ops.is_empty() {
                return Ok(());
            }

            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for op in ops {
                match op {
                    RemoteBatchOp::Upsert { key, value } => {
                        pipeline
                            .cmd("SET")
                            .arg(self.node_key(key))
                            .arg(*value)
                            .ignore();
                    }
                    RemoteBatchOp::Delete { key } => {
                        pipeline.cmd("DEL").arg(self.node_key(key)).ignore();
                    }
                }
            }

            let mut connection = self.connection.clone();
            pipeline.query_async::<()>(&mut connection).await
        }

        async fn batch_get_nodes_ordered(
            &self,
            keys: &[&[u8]],
        ) -> Result<Vec<Option<Vec<u8>>>, Self::Error> {
            if keys.is_empty() {
                return Ok(Vec::new());
            }

            let mut command = redis_client::cmd("MGET");
            for key in keys {
                command.arg(self.node_key(key));
            }

            let mut connection = self.connection.clone();
            command.query_async(&mut connection).await
        }

        async fn batch_put_nodes(&self, entries: &[(&[u8], &[u8])]) -> Result<(), Self::Error> {
            if entries.is_empty() {
                return Ok(());
            }

            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for (key, value) in entries {
                pipeline
                    .cmd("SET")
                    .arg(self.node_key(key))
                    .arg(*value)
                    .ignore();
            }

            let mut connection = self.connection.clone();
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
            let mut connection = self.connection.clone();
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
            let mut connection = self.connection.clone();
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
            let mut pipeline = redis_client::pipe();
            pipeline.atomic();
            for (key, value) in entries {
                pipeline
                    .cmd("SET")
                    .arg(self.node_key(key))
                    .arg(*value)
                    .ignore();
            }
            pipeline
                .cmd("SET")
                .arg(self.hint_key(namespace, key))
                .arg(value)
                .ignore();

            let mut connection = self.connection.clone();
            pipeline.query_async::<()>(&mut connection).await
        }

        async fn get_root_manifest(&self, name: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
            let mut connection = self.connection.clone();
            redis_client::cmd("GET")
                .arg(self.root_key(name))
                .query_async(&mut connection)
                .await
        }

        async fn put_root_manifest(&self, name: &[u8], manifest: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.connection.clone();
            redis_client::cmd("SET")
                .arg(self.root_key(name))
                .arg(manifest)
                .query_async::<()>(&mut connection)
                .await
        }

        async fn delete_root_manifest(&self, name: &[u8]) -> Result<(), Self::Error> {
            let mut connection = self.connection.clone();
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
            let script = Script::new(ROOT_CAS_LUA);
            let mut invocation = script.prepare_invoke();
            invocation
                .key(self.root_key(name))
                .arg(if expected.is_some() { b"1" } else { b"0" }.as_slice())
                .arg(expected.unwrap_or_default())
                .arg(if new.is_some() { b"1" } else { b"0" }.as_slice())
                .arg(new.unwrap_or_default());

            let mut connection = self.connection.clone();
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

            let mut roots = Vec::with_capacity(names.len());
            for name in names {
                if let Some(manifest) = self.get_root_manifest(&name).await? {
                    roots.push(RemoteNamedRoot::new(name, manifest));
                }
            }
            Ok(roots)
        }
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
    const SCAN_COUNT: usize = 1024;
    const DELETE_CHUNK_SIZE: usize = 512;

    const NODE_FAMILY: &[u8] = b"node:";
    const ROOT_FAMILY: &[u8] = b"root:";
    const HINT_FAMILY: &[u8] = b"hint:";

    /// Recommended key prefix for immutable node values.
    pub const NODE_KEY_PREFIX: &str = "prolly:node:";
    /// Recommended key prefix for named root manifests.
    pub const ROOT_KEY_PREFIX: &str = "prolly:root:";
    /// Recommended key prefix for hints.
    pub const HINT_KEY_PREFIX: &str = "prolly:hint:";

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
}

pub use redis::*;
