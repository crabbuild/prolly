# prolly-store-dynamodb

DynamoDB-backed remote store adapter for `prolly-map`.

This crate implements `RemoteStoreBackend` with the AWS SDK for DynamoDB. Use it
through `RemoteProllyStore` and `AsyncProlly` for serverless, managed,
content-addressed Prolly tree storage.

## When To Use It

Use this adapter when you want a managed AWS-native store with on-demand
capacity, simple operational scaling, and low administrative overhead. It is a
reasonable fit for multi-tenant services, sync metadata, durable checkpoints,
and remote map state where DynamoDB request pricing and item-size limits are
acceptable.

Use DynamoDB Local for integration tests and development. Do not use DynamoDB
Local performance numbers as production DynamoDB capacity numbers; the local
Java simulator has very different scaling behavior.

## Table Model

The adapter uses one table with:

- Partition key: `pk`
- Partition key type: binary
- No sort key
- Payload attribute: `value`

Logical records are namespaced by binary prefixes:

- `node:` content-addressed Prolly nodes keyed by CID.
- `root:` named root manifests.
- `hint:` traversal hints.

`initialize_schema` creates the table with on-demand billing if it does not
exist. Existing tables must already have the binary `pk` partition key.

## Local Setup

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

## AWS Setup

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
```

## Basic Usage

```rust
use aws_sdk_dynamodb::config::{BehaviorVersion, Credentials, Region};
use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
use prolly_store_dynamodb::DynamoDbBackend;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
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
.with_key_prefix(b"my-app:".to_vec());
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
# Ok(())
# }
```

## Diff And Merge

Branching is immutable. A branch update writes new content-addressed nodes while
unchanged subtrees keep their existing CIDs:

```rust
# use prolly::{AsyncProlly, Config, Mutation, RemoteProllyStore};
# use prolly_store_dynamodb::DynamoDbBackend;
# async fn run(backend: DynamoDbBackend) -> Result<(), Box<dyn std::error::Error>> {
# let prolly = AsyncProlly::new(RemoteProllyStore::new(backend), Config::default());
# let base = prolly.batch(&prolly.create(), vec![
#     Mutation::Upsert { key: b"task/1".to_vec(), val: b"open".to_vec() },
#     Mutation::Upsert { key: b"task/2".to_vec(), val: b"open".to_vec() },
# ]).await?;
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
# Ok(())
# }
```

## Operational Notes

- DynamoDB limits batch writes to 25 items; the adapter chunks writes and
  retries unprocessed items.
- Individual serialized nodes must fit DynamoDB item limits.
- Use `with_key_prefix` for tenant or test isolation inside a shared table.
- `clear_namespace` scans and deletes every item under the prefix. Use it for
  tests, not as a production cleanup primitive.
- Root compare-and-swap uses DynamoDB conditional writes.

## Running The Example

From the CrabDB workspace root:

```bash
export PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000
export PROLLY_STORE_DYNAMODB_TABLE=prolly_store_example
export AWS_REGION=us-west-2
cargo run -p prolly-store-dynamodb --example basic_usage
```

The example supports both DynamoDB Local and AWS DynamoDB. With
`PROLLY_STORE_DYNAMODB_ENDPOINT` set, it uses local test credentials.
