# prolly-store-dynamodb

DynamoDB-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` with the AWS SDK for DynamoDB. Use it
through native `AsyncProlly::indexed_map(...).await` with `DynamoDbStore`, or
through the first-class `SyncDynamoDbStore` facade with synchronous
`Prolly::indexed_map`.

## Native asynchronous IndexedMap

```rust,no_run
use prolly::{AsyncProlly, Config, SecondaryIndexRegistry};
use prolly_store_dynamodb::{DynamoDbBackend, DynamoDbStore};

async fn open(backend: DynamoDbBackend) -> Result<(), prolly::Error> {
    let engine = AsyncProlly::new(DynamoDbStore::new(backend), Config::default());
    let _users = engine
        .indexed_map(b"users", SecondaryIndexRegistry::new())
        .await?;
    Ok(())
}
```

Prefer this native path for async services. It preserves task cancellation and
AWS SDK request backpressure without blocking workers.

## Synchronous IndexedMap

```rust,no_run
use prolly::{Config, Prolly, SecondaryIndexRegistry};
use prolly_store_dynamodb::{DynamoDbBackend, SyncDynamoDbStore};

fn open(backend: DynamoDbBackend) -> Result<(), Box<dyn std::error::Error>> {
    let engine = Prolly::new(SyncDynamoDbStore::new(backend)?, Config::default());
    let _users = engine.indexed_map(b"users", SecondaryIndexRegistry::new())?;
    Ok(())
}
```

Use `SyncDynamoDbStore::build` when asynchronous SDK initialization or schema
validation must live on the store-owned runtime.

## Installation

The AWS versions below match the adapter's SDK line:
This dependency graph requires Rust 1.91.1 or newer.

```toml
[dependencies]
prolly-map = "0.7.2"
prolly-store-dynamodb = "0.6.1"
aws-config = { version = "=1.5.18", features = ["behavior-version-latest"] }
aws-sdk-dynamodb = "=1.73.0"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

## When to use it

Use this adapter when you want a managed AWS-native store with on-demand
capacity, simple operational scaling, and low administrative overhead. It is a
reasonable fit for multi-tenant services, sync metadata, durable checkpoints,
and remote map state where DynamoDB request pricing and item-size limits are
acceptable.

Use DynamoDB Local for integration tests and development. Do not use DynamoDB
Local performance numbers as production DynamoDB capacity numbers; the local
Java simulator has very different scaling behavior.

## Table model

The adapter uses a primary table with:

- Partition key: `pk`
- Partition key type: binary
- No sort key
- Payload attribute: `value`

The primary table stores content-addressed nodes and traversal hints under
binary family prefixes. It does not store named roots.

Root enumeration uses a companion registry table, named
`<primary-table>-roots` by default, with:

- Partition key: `pk` (binary namespace)
- Sort key: `sk` (binary root name)

The companion table is the sole canonical store for named root manifests.
`list_root_manifests` returns names and manifests directly from a strongly
consistent query, so its read work is proportional to the number of roots in
the namespace rather than the number of node items in the primary table.

`initialize_schema` creates both tables with on-demand billing if needed.
Override the companion name with `with_root_table_name` when table naming or
IAM policy requires it.

Initialization is safe under concurrent provisioners. After either creating a
table or observing `ResourceInUseException`, the adapter waits for the winning
table to become ACTIVE and validates that final table description. A concurrent
creator cannot make an incompatible primary or roots key schema pass
initialization. Call `validate_initialized_schema` instead when the runtime
identity must remain read-only to the control plane.

Version 0.4 is a hard schema cutover. It does not read or migrate root entries
written by 0.3 or earlier. Export or republish required named roots into the
0.4 root table before switching production traffic.

## Local setup

Run DynamoDB Local:

```bash
docker run --rm -p 8000:8000 amazon/dynamodb-local:latest \
  -jar DynamoDBLocal.jar -sharedDb -inMemory
```

Or use the Prolly service compose file from the Prolly repo root:

```bash
docker compose -p prolly-store-services -f docker-compose.store-services.yml up -d dynamodb
```

Set environment variables:

```bash
export PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000
export PROLLY_STORE_DYNAMODB_TABLE=prolly_store_example
export AWS_REGION=us-west-2
```

## AWS setup

For AWS DynamoDB, omit `PROLLY_STORE_DYNAMODB_ENDPOINT` and provide normal AWS
credentials through the environment, profile, instance role, or workload
identity. You can either let `initialize_schema` create the table or create it
ahead of time:

```bash
aws dynamodb create-table \
  --table-name prolly_store \
  --attribute-definitions AttributeName=pk,AttributeType=B \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST

aws dynamodb create-table \
  --table-name prolly_store-roots \
  --attribute-definitions \
      AttributeName=pk,AttributeType=B \
      AttributeName=sk,AttributeType=B \
  --key-schema \
      AttributeName=pk,KeyType=HASH \
      AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
```

## Basic usage

```rust
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_dynamodb::DynamoDbBackend;

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let config = aws_sdk_dynamodb::config::Builder::new()
        .behavior_version(BehaviorVersion::latest())
        .region(Region::new("us-west-2"))
        .endpoint_url("http://127.0.0.1:8000")
        .credentials_provider(Credentials::new("test", "test", None, None, "local"))
        .build();
    let backend = DynamoDbBackend::new(
        aws_sdk_dynamodb::Client::from_conf(config),
        "prolly_store_example",
    )
    .with_key_prefix(b"my-app:".to_vec())
    .with_root_table_name("prolly_store_example-roots")
    .with_read_parallelism(16)
    .with_batch_get_parallelism(16)
    .with_batch_write_parallelism(16)
    .with_scan_parallelism(8);
    backend.initialize_schema().await?;

    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let tree = prolly
        .batch(
            &prolly.create(),
            vec![Mutation::Upsert {
                key: b"task/1".to_vec(),
                val: b"open".to_vec(),
            }],
        )
        .await?;

    prolly.publish_named_root(b"tasks/main", &tree).await?;
    Ok(())
}
```

## Diff and merge

Branching is immutable. A branch update writes new content-addressed nodes while
unchanged subtrees keep their existing CIDs:

```rust
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_dynamodb::DynamoDbBackend;

async fn run(backend: DynamoDbBackend) -> Result<(), Box<dyn std::error::Error>> {
    let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
    let base = prolly.batch(&prolly.create(), vec![
        Mutation::Upsert { key: b"task/1".to_vec(), val: b"open".to_vec() },
        Mutation::Upsert { key: b"task/2".to_vec(), val: b"open".to_vec() },
    ]).await?;
    let left = prolly
        .batch(
            &base,
            vec![Mutation::Upsert {
                key: b"task/1".to_vec(),
                val: b"in-review".to_vec(),
            }],
        )
        .await?;
    let right = prolly
        .batch(
            &base,
            vec![Mutation::Upsert {
                key: b"task/2".to_vec(),
                val: b"done".to_vec(),
            }],
        )
        .await?;

    let diffs = prolly.diff(&base, &left).await?;
    assert_eq!(diffs.len(), 1);

    let merged = prolly.merge(&base, &left, &right, None).await?;
    assert_eq!(
        prolly.get(&merged, b"task/2").await?,
        Some(b"done".to_vec())
    );
    Ok(())
}
```

## Operational notes

- DynamoDB limits batch writes to 25 items and batch reads to 100 items. The
  adapter chunks large requests, executes a bounded number concurrently, and
  retries unprocessed items with exponential backoff.
- `with_batch_get_parallelism` and `with_batch_write_parallelism` control
  request concurrency. The measured default is 16 for each. Lower these values
  for provisioned tables with limited capacity; tune them while observing
  throttling and consumed capacity.
- `with_read_parallelism` controls the async Prolly traversal fan-out and is
  independent of DynamoDB batch-request concurrency.
- `with_scan_parallelism` controls parallel primary-table scans used by node
  enumeration and namespace cleanup. Root operations never scan the primary
  table.
- Each root is one item in the companion table. Ordinary root updates use one
  conditional write. Strict transactions default to verified immutable-node
  prepublication followed by one conditioned roots-only `TransactWriteItems`;
  conflicts can leave unreachable nodes for retention-aware GC, but can never
  expose a partial tree. `AtomicNodesAndRoots` remains an explicit small-write
  compatibility mode. Inspect `transaction_capabilities()` before opening a
  shared namespace.
- Use `dynamodb_safe_config()` for byte-measured trees whose serialized nodes
  stay below the adapter's tested safety ceiling. Oversized logical values must
  use `DynamoDbBlobStore`, which publishes content-addressed chunks before a
  visibility manifest and verifies the complete content on every read.
- Ambiguous root transactions are retried with a deterministic DynamoDB client
  request token. If reconciliation remains ambiguous, the returned error
  includes that token for operator investigation; it is never treated as an
  ordinary retryable logical failure.
- Use `with_key_prefix` for tenant or test isolation inside a shared table.
- `clear_namespace` scans primary-table items under the prefix and queries the
  matching root registry partition before deleting both. Use it for tests, not
  as a production cleanup primitive.
- Do not run 0.3 and 0.4 writers against the same logical namespace. They use
  different root stores and intentionally do not interoperate.

## Running the example

From the standalone repository root:

```bash
export PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000
export PROLLY_STORE_DYNAMODB_TABLE=prolly_store_example
export AWS_REGION=us-west-2
cargo run --manifest-path stores/prolly-store-dynamodb/Cargo.toml --example basic_usage
cargo run --manifest-path stores/prolly-store-dynamodb/Cargo.toml --example versioned_map
```

The example supports both DynamoDB Local and AWS DynamoDB. With
`PROLLY_STORE_DYNAMODB_ENDPOINT` set, it uses local test credentials.

## Testing

The integration test runs when `PROLLY_STORE_DYNAMODB_TABLE` is set. Point
`PROLLY_STORE_DYNAMODB_ENDPOINT` at DynamoDB Local for a credential-free run, or
omit it to use the normal AWS credential chain:

```bash
cargo test --manifest-path stores/prolly-store-dynamodb/Cargo.toml
```

The test uses a unique binary key prefix and clears only that namespace.

## Performance evaluation

The repository includes a configurable Rust benchmark and Docker runner:

```bash
./scripts/run_dynamodb_scale_benchmark.sh --profile smoke
./scripts/run_dynamodb_scale_benchmark.sh --profile full
```

See `benchmarks/dynamodb-scale/README.md` for large-tree, concurrency, and
before/after comparison options. DynamoDB Local results are suitable for
regression comparisons on the same machine, not production capacity planning.

See the [`prolly-map` API documentation](https://docs.rs/prolly-map) for the
async map, transaction, diff, and merge APIs used with this backend.
