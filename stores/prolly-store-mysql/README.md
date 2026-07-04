# prolly-store-mysql

MySQL-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using `sqlx::MySqlPool`. Use it
through `RemoteProllyStore` and `AsyncProlly` when your deployment standardizes
on MySQL and you want durable Prolly nodes, hints, and named roots in SQL.

## When To Use It

Use this adapter for applications that already operate MySQL and want Prolly map
semantics without adding another durable service. It is suitable for
transactional application backends, managed MySQL environments, and systems that
need ordinary SQL backup, restore, and operational tooling.

Prefer PostgreSQL if your workload benefits from stronger bytea ergonomics or
Postgres-specific operational features. Prefer Redis for ephemeral low-latency
state and DynamoDB/Cosmos/Spanner for cloud-native managed scale.

## Data Model

`initialize_schema` creates:

- `prolly_nodes(cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)`
- `prolly_hints(namespace VARBINARY(255), key VARBINARY(255), value LONGBLOB)`
- `prolly_roots(name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)`

Nodes are content-addressed by CID. Named roots store serialized root manifests
and are the stable durable handles for branches, checkpoints, and application
heads.

## Setup

Run MySQL locally:

```bash
docker run --rm \
  -e MYSQL_DATABASE=prolly \
  -e MYSQL_USER=prolly \
  -e MYSQL_PASSWORD=prolly \
  -e MYSQL_ROOT_PASSWORD=prolly \
  -p 53306:3306 \
  mysql:8.0
```

Or use the Prolly service compose file from the Prolly repo root:

```bash
docker compose -p prolly-store-services -f docker-compose.store-services.yml up -d mysql
```

Set the connection URL:

```bash
export PROLLY_STORE_MYSQL_URL=mysql://prolly:prolly@127.0.0.1:53306/prolly
```

Initialize schema during application startup:

```rust
# async fn run() -> Result<(), sqlx::Error> {
let backend = prolly_store_mysql::MySqlBackend::connect(
    "mysql://prolly:prolly@127.0.0.1:53306/prolly",
)
.await?;
backend.initialize_schema().await?;
# Ok(())
# }
```

## Basic Usage

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_mysql::MySqlBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let backend = MySqlBackend::connect("mysql://prolly:prolly@127.0.0.1:53306/prolly").await?;
backend.initialize_schema().await?;

let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
let tree = prolly
    .batch(
        &prolly.create(),
        vec![
            Mutation::Upsert {
                key: b"doc/1".to_vec(),
                val: b"draft".to_vec(),
            },
            Mutation::Upsert {
                key: b"doc/2".to_vec(),
                val: b"published".to_vec(),
            },
        ],
    )
    .await?;

prolly.publish_named_root(b"docs/main", &tree).await?;
let loaded = prolly.load_named_root(b"docs/main").await?.expect("root");
assert_eq!(
    prolly.get(&loaded, b"doc/1").await?,
    Some(b"draft".to_vec())
);
# Ok(())
# }
```

## Branching, Diff, And Merge

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_mysql::MySqlBackend;
# async fn run() -> Result<(), Box<dyn std::error::Error>> {
# let backend = MySqlBackend::connect("mysql://prolly:prolly@127.0.0.1:53306/prolly").await?;
# backend.initialize_schema().await?;
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"doc/1".to_vec(), val: b"draft".to_vec() },
#     Mutation::Upsert { key: b"doc/2".to_vec(), val: b"published".to_vec() },
# ]).await?;
let writer_a = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"doc/1".to_vec(),
            val: b"review".to_vec(),
        }],
    )
    .await?;
let writer_b = prolly
    .batch(
        &base,
        vec![Mutation::Upsert {
            key: b"doc/2".to_vec(),
            val: b"archived".to_vec(),
        }],
    )
    .await?;

let diffs = prolly.diff(&base, &writer_a).await?;
assert_eq!(diffs.len(), 1);

let merged = prolly.merge(&base, &writer_a, &writer_b, None).await?;
assert_eq!(
    prolly.get(&merged, b"doc/2").await?,
    Some(b"archived".to_vec())
);
# Ok(())
# }
```

## Operational Notes

- `initialize_schema` is idempotent.
- MySQL named-root compare-and-swap uses SQL transactions and conditional
  writes.
- MySQL key length limits matter for named roots and hint keys; use compact
  binary or slash-separated names rather than large serialized metadata in the
  name itself.
- Node rows are content-addressed and may be shared by many roots.
- Deleting a named root does not immediately delete unreachable nodes.

## Running The Example

From the CrabDB workspace root:

```bash
export PROLLY_STORE_MYSQL_URL=mysql://prolly:prolly@127.0.0.1:53306/prolly
cargo run -p prolly-store-mysql --example basic_usage
```

The example initializes schema, writes and branches a tree, diffs and merges
branches, resolves a conflict, publishes a named root, and reads it back.
