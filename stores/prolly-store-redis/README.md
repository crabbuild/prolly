# prolly-store-redis

Redis-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` for Redis and is intended for async
`prolly-map` use through `RemoteProllyStore` and `AsyncProlly`.

## When To Use It

Use this adapter when you want a low-latency remote Prolly tree store and your
Redis deployment is configured with the durability level your application needs.
It is a good fit for edge caches, collaborative sessions, test environments, and
small to medium shared state where Redis persistence, replication, or managed
service guarantees are acceptable.

Do not treat a default in-memory Redis instance as durable storage. For
production, configure AOF/RDB persistence, replication, backups, eviction
policy, memory limits, and access control explicitly.

## Data Model

The adapter stores three logical families under a configurable binary key prefix:

- `node:` content-addressed Prolly tree nodes keyed by CID bytes.
- `root:` named root manifests for durable branch/checkpoint names.
- `hint:` optional traversal hints, such as the append rightmost-path hint.

Redis operations used by the adapter include `GET`, `SET`, `DEL`, `MGET`,
`SCAN`, and pipelined writes.

## Setup

Run Redis locally:

```bash
docker run --rm -p 56379:6379 redis:7-alpine
```

Or use the Prolly service compose file from the Prolly repo root:

```bash
docker compose -p prolly-store-services -f docker-compose.store-services.yml up -d redis
```

Set the connection URL:

```bash
export PROLLY_STORE_REDIS_URL=redis://127.0.0.1:56379/
```

## Basic Usage

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_redis::RedisBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = RedisBackend::connect("redis://127.0.0.1:56379/")
    .await?
    .with_key_prefix(b"my-app:prolly:".to_vec());
let store = RemoteProllyStore::new(backend);
let prolly = AsyncProlly::new(store, Config::default());

let tree = prolly.create();
let tree = prolly
    .batch(
        &tree,
        vec![
            Mutation::Upsert {
                key: b"user/1".to_vec(),
                val: b"Ada".to_vec(),
            },
            Mutation::Upsert {
                key: b"user/2".to_vec(),
                val: b"Grace".to_vec(),
            },
        ],
    )
    .await?;

prolly.publish_named_root(b"main", &tree).await?;
let loaded = prolly.load_named_root(b"main").await?.expect("main root");
assert_eq!(
    prolly.get(&loaded, b"user/1").await?,
    Some(b"Ada".to_vec())
);
# Ok(())
# }
```

## Diff And Merge

Redis stores all nodes needed by multiple immutable tree versions, so you can
branch, diff, and merge without copying the full map:

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_redis::RedisBackend;
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
# let backend = RedisBackend::connect("redis://127.0.0.1:56379/").await?;
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"user/1".to_vec(), val: b"Ada".to_vec() },
#     Mutation::Upsert { key: b"user/2".to_vec(), val: b"Grace".to_vec() },
# ]).await?;
let left = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"user/1".to_vec(),
            val: b"Ada Lovelace".to_vec(),
        }],
    )
    .await?;
let right = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"user/2".to_vec(),
            val: b"Grace Hopper".to_vec(),
        }],
    )
    .await?;

let diffs = prolly.diff(&base, &left).await?;
assert_eq!(diffs.len(), 1);

let merged = prolly.merge(&base, &left, &right, None).await?;
assert_eq!(
    prolly.get(&merged, b"user/2").await?,
    Some(b"Grace Hopper".to_vec())
);
# Ok(())
# }
```

## Namespacing And Tests

Use `with_key_prefix` whenever a Redis database is shared by tests, services, or
tenants. `clear_namespace` deletes every key under the current prefix and should
be used only for isolated test namespaces:

```rust
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
# let backend = prolly_store_redis::RedisBackend::connect("redis://127.0.0.1:56379/").await?;
let backend = backend.with_key_prefix(b"test-run-123:".to_vec());
backend.clear_namespace().await?;
# Ok(())
# }
```

## Running The Example

From the CrabDB workspace root:

```bash
export PROLLY_STORE_REDIS_URL=redis://127.0.0.1:56379/
cargo run -p prolly-store-redis --example basic_usage
```

The example writes a unique Redis namespace, publishes a named root, reloads it,
runs diff/merge, and resolves a merge conflict.
