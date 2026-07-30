# prolly-store-mysql

MySQL-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` using `sqlx::MySqlPool`. Use it
through `RemoteProllyStore` and `AsyncProlly` when your deployment standardizes
on MySQL and you want durable Prolly nodes, hints, and named roots in SQL.

## Installation

```toml
[dependencies]
prolly-map = "0.6.0"
prolly-store-mysql = "0.4.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## When to use it

Use this adapter for applications that already operate MySQL and want Prolly map
semantics without adding another durable service. It is suitable for
transactional application backends, managed MySQL environments, and systems that
need ordinary SQL backup, restore, and operational tooling.

Prefer PostgreSQL if your workload benefits from stronger bytea ergonomics or
Postgres-specific operational features. Prefer Redis for ephemeral low-latency
state and DynamoDB/Cosmos/Spanner for cloud-native managed scale.

## Data model

`initialize_schema` creates:

- `prolly_nodes(cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)`
- `prolly_hints(namespace VARBINARY(255), key VARBINARY(255), value LONGBLOB)`
- `prolly_roots(name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)`
- `prolly_root_locks(name VARBINARY(255) PRIMARY KEY)`

Nodes are content-addressed by CID. Named roots store serialized root manifests
and are the stable durable handles for branches, checkpoints, and application
heads. The lock table contains no application data; it makes both absent and
existing root names safely lockable during concurrent publication.

## Performance tuning

Batch reads, writes, deletes, and transactional publications use bounded
set-based SQL. Ordered reads reconstruct the requested order, including
duplicates and missing CIDs. A multi-chunk public batch uses one transaction and
rolls back completely if any chunk fails.

The adapter defaults to 1,000 items per SQL batch. Configure it independently
from the SQLx connection-pool size:

```rust
use std::num::NonZeroUsize;

use prolly_store_mysql::{MySqlBackend, MySqlBackendOptions};
use sqlx::mysql::MySqlPoolOptions;

async fn tuned_backend(url: &str) -> Result<MySqlBackend, sqlx::Error> {
    let pool = MySqlPoolOptions::new()
        .max_connections(32)
        .connect(url)
        .await?;
    let options = MySqlBackendOptions::new(NonZeroUsize::new(2_000).unwrap());
    Ok(MySqlBackend::new_with_options(pool, options))
}
```

Larger batches reduce round trips but increase statement size and transaction
work. Pool size controls concurrent requests; it does not change the batch
limit. Benchmark both together for the deployment workload instead of assuming
that the largest values are fastest.

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
async fn run() -> Result<(), sqlx::Error> {
    let backend = prolly_store_mysql::MySqlBackend::connect(
        "mysql://prolly:prolly@127.0.0.1:53306/prolly",
    )
    .await?;
    backend.initialize_schema().await?;
    Ok(())
}
```

## Basic usage

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_mysql::MySqlBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
    Ok(())
}
```

## Branching, diff, and merge

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_mysql::MySqlBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let backend = MySqlBackend::connect("mysql://prolly:prolly@127.0.0.1:53306/prolly").await?;
    backend.initialize_schema().await?;
    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let base = prolly.batch(&prolly.create(), vec![
        Mutation::Upsert { key: b"doc/1".to_vec(), val: b"draft".to_vec() },
        Mutation::Upsert { key: b"doc/2".to_vec(), val: b"published".to_vec() },
    ]).await?;
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
    Ok(())
}
```

## Operational notes

- `initialize_schema` is idempotent.
- Strict commits validate named-root preconditions and apply node and root
  writes in one MySQL transaction.
- Root mutations acquire persistent lock identities in lexical order, preventing
  absent-root races and reducing multi-root deadlock risk.
- MySQL key length limits matter for named roots and hint keys; use compact
  binary or slash-separated names rather than large serialized metadata in the
  name itself.
- Node rows are content-addressed and may be shared by many roots.
- Deleting a named root does not immediately delete unreachable nodes.

## Running the example

From the standalone repository root:

```bash
export PROLLY_STORE_MYSQL_URL=mysql://prolly:prolly@127.0.0.1:53306/prolly
cargo run --manifest-path stores/prolly-store-mysql/Cargo.toml --example basic_usage
```

The example initializes schema, writes and branches a tree, diffs and merges
branches, resolves a conflict, publishes a named root, and reads it back.

## Testing

The integration test runs when `PROLLY_STORE_MYSQL_URL` is set and returns
without connecting otherwise:

```bash
export PROLLY_STORE_MYSQL_URL=mysql://prolly:prolly@127.0.0.1:53306/prolly
cargo test --manifest-path stores/prolly-store-mysql/Cargo.toml
```

Run it against a disposable database or schema. The adapter tables are shared by
every client using that database.

The repository also provides reproducible MySQL/PostgreSQL end-to-end and
service comparisons:

```bash
scripts/run_mysql_postgres_comparison.sh
scripts/run_mysql_postgres_service_matrix.sh
```

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for the
async map, transaction, diff, and merge APIs used with this backend.
