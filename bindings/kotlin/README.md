# Prolly Kotlin Binding

This package contains the Kotlin/JVM UniFFI binding for the Rust
`prolly-bindings` facade.

See `COOKBOOK.md` for Kotlin application patterns covering SQLite-backed
indexes, prefix queries, coroutine wrappers, merge callbacks, large values, and
custom stores.

The generated source lives in
`src/main/kotlin/build/crab/prolly/generated/prolly.kt` and uses package
`build.crab.prolly`.

Compiled native libraries are built by Cargo or release CI and are not checked
in. The generated surface includes:

- `readSession(tree)` for reusable owned point, multi-get, range, diff, and
  conflict reads; close it with `use`;
- ordered boundary, prefix, cursor, range-page, and diff-page helpers;
- bulk-build, append-batch, parallel-batch, and execution-stat APIs;
- merge policies, resolver callbacks, CRDT helpers, and explanation traces;
- named roots, retention policies, node GC, blob stores, and large values;
- portable snapshot bundles and store-independent proof bundles;
- mutation constructors, key encoders, config builders, and `HostStoreCallback`.

Tests cover memory, file, SQLite, SQLite-in-memory, and callback-backed
host-store paths through the generated Kotlin API.
`AsyncProllyEngine` and `AsyncProllyBlobStore` expose suspend wrappers for
create/read/write, range/diff, merge, named-root, stats/debug/cache, hint,
GC/sync, snapshot bundle, large-value, and blob-store methods.

Local smoke test:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
mvn -f bindings/kotlin/pom.xml test
```

The tests call `ProllyNative.useLocalDebugLibrary()` to point UniFFI/JNA at the
local Cargo debug library.

## Source Tree Layout

The Kotlin binding is the canonical JVM surface generated from the UniFFI
facade. Java builds on top of it, so changes here affect both JVM languages.

Important files:

- `src/main/kotlin/build/crab/prolly/generated/prolly.kt` is the generated API.
- `ProllyJavaAdapters.kt` contains JVM-friendly helper adapters.
- `AsyncProllyEngine.kt` and `AsyncProllyBlobStore.kt` provide coroutine wrappers.
- `examples/*.kt` contains standalone cookbook scenarios. Each scenario file
  includes the code it needs instead of delegating to one large scenario module.
- `src/test/kotlin` contains fixture and parity tests for the generated surface.

## Running Examples

Build the native Rust facade:

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

Run one scenario:

```sh
mvn -f bindings/kotlin/pom.xml \
  -Dexec.mainClass=build.crab.prolly.examples.LocalFirstStateKt \
  exec:java
```

Run all scenarios:

```sh
mvn -f bindings/kotlin/pom.xml \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenariosKt \
  exec:java
```

`CookbookScenariosKt` is only a launcher; the runnable scenario code lives in
the individual example files.

## API Style

The Kotlin API uses `ByteArray` for keys and values and generated data classes
for records. Keep domain codecs near the call site or in a small domain module.
Do not rely on Kotlin object identity for byte arrays; use `contentEquals` when
comparing roots, keys, values, or CIDs.

Memory engines are best for tests, examples, and temporary computations. File
and SQLite engines are better for tools and local services that need durable
roots. Always close engines and blob stores with `use { ... }` so native handles
are released even when validation fails.

## Coroutine Wrappers

`AsyncProllyEngine` exposes suspend-friendly methods for codebases built around
coroutines. The wrapper is an integration convenience, not a new consistency
model. Keep merge, CAS, and named-root publication steps explicit so cancellation
and retries remain understandable.

Use structured concurrency around long workflows. Avoid launching detached
coroutines that mutate roots after the caller has lost interest in the result.

## Async Store Providers

Every provider is a separate Maven artifact; `build.crab:prolly-kotlin` has no
database SDK dependency. Adapters borrow their inputs and never close the
application's data source, client, connection, dispatcher, or executor.

| Artifact | Minimal Kotlin construction | Preparation |
| --- | --- | --- |
| `prolly-kotlin-store-sqlite` | `SqliteStore(dataSource, dispatcher)` | `store.initializeSchema()` |
| `prolly-kotlin-store-postgres` | `PostgresStore(dataSource, dispatcher)` | `store.initializeSchema()` |
| `prolly-kotlin-store-mysql` | `MysqlStore(dataSource, dispatcher)` | `store.initializeSchema()` |
| `prolly-kotlin-store-redis` | `RedisStore(connection.async())` | use Lettuce `ByteArrayCodec` |
| `prolly-kotlin-store-dynamodb` | `DynamoDbStore(client, DynamoDbStoreOptions("prolly"))` | `store.initializeTable()` |
| `prolly-kotlin-store-cosmosdb` | `CosmosDbStore(container)` | create with `/kind`, then `store.validateContainer()` |
| `prolly-kotlin-store-spanner` | `SpannerStore(database, dispatcher)` | apply `SPANNER_DDL` |

JDBC calls run on the injected bounded dispatcher with interruptible coroutine
bridging. AWS, Azure, Lettuce, and Google clients use their official SDK
futures; cancellation propagates where those SDKs permit it. Credentials,
connection pools, TLS, retries, dispatchers, and shutdown remain owned by the
caller.

DynamoDB enforces its 100-read, 25-write, and 100-operation transaction limits.
Cosmos DB strict operations share one logical partition and have a 100-operation
limit. Redis used as primary storage needs AOF, an explicit `appendfsync`
policy, monitored persistence health, recovery drills, and backups. Run every
Kotlin and Java provider against local services with
`./scripts/test-node-jvm-stores.sh` from the repository root.

## Merge And Domain Rules

Built-in merge resolvers are useful for simple state classes. Domain values with
timestamps, tombstones, CRDT envelopes, or append-only records should use typed
helpers or callbacks. Callback resolvers should be deterministic and should
avoid network calls, clocks, random values, and mutable process state.

Range-limited and prefix-limited merges are preferable when a workflow owns a
known namespace. They reduce conflict inspection cost and make merge traces much
easier to explain in logs.

## Large Values, Blobs, And Snapshots

Use `ProllyBlobStore` for large documents, file contents, prompt transcripts,
retrieval chunks, and generated artifacts. Choose an inline threshold based on
the smallest value worth keeping in leaves. Snapshot bundles are useful for
moving roots and required nodes between engines, test fixtures, and offline
tools.

Named roots are the retention boundary. Publish the roots that must survive
before running node or blob GC. Retained named-root GC is useful for keeping a
current branch head plus selected checkpoints while reclaiming abandoned work.

## Testing Strategy

Run the module tests while iterating on Kotlin:

```sh
mvn -f bindings/kotlin/pom.xml test
```

Run the parent test suite before changing generated types or shared JVM helper
adapters:

```sh
mvn -f bindings/pom.xml test
```

Prefer memory stores for most tests. Add file, SQLite, host-store, and blob-store
tests only when the behavior depends on that storage path.

## Packaging Notes

The generated Kotlin source expects a native `prolly_bindings` library with the
same exported UniFFI symbols. Release packages should pin the Rust facade version
and document supported platforms. Source-tree development uses `target/debug`;
published packages should use CI-built native artifacts.

## Troubleshooting

- `UnsatisfiedLinkError` means the native library path is wrong or the library is
  for a different platform.
- `NoSuchMethodError` usually means Java or Kotlin is loading an older generated
  artifact. Clean the module and reinstall the current reactor artifact.
- Byte-array equality bugs usually come from using `==` instead of
  `contentEquals`.
- If coroutine examples hang, inspect the caller scope and dispatcher rather than
  the prolly operation first.
