# Node and JVM Async Store Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add production-capable Node/TypeScript and shared Kotlin/Java implementations of the version 1 asynchronous store protocol for SQLite, PostgreSQL, MySQL, Redis, DynamoDB, Cosmos DB, and Spanner, plus the approved Node-only PGlite adapter.

**Architecture:** Keep provider SDKs outside the core bindings. Node uses a napi-rs promise callback bridge into `RemoteStoreBackend`; Kotlin uses the existing UniFFI asynchronous foreign trait and Java exposes `CompletableFuture` facades over the same Kotlin implementations. Each provider is delivered as one vertical slice across Node and JVM, with the physical layout and capabilities copied from the Rust and Go adapters.

**Tech Stack:** Rust 1.81+ and napi-rs 2.16; Node 20+ and TypeScript executed by Node; Kotlin 2.2.21, coroutines 1.10.2, Java 17, Maven 3.9; better-sqlite3 12.11.1, pg 8.22.0, mysql2 3.23.0, redis 6.1.0, AWS SDK 3.1089.0, Azure Cosmos 4.9.3, Cloud Spanner 8.9.0, PGlite 0.5.4; sqlite-jdbc 3.53.2.0, PostgreSQL JDBC 42.7.13, MySQL Connector/J 9.7.0, Lettuce 7.6.0.RELEASE, AWS SDK for Java 2.48.2, Azure Cosmos 4.81.0, and Google Cloud Spanner 6.119.0.

## Global Constraints

- Protocol major is exactly `1`; provider schema version is exactly `1`.
- Node, Kotlin, and Java support SQLite, PostgreSQL, MySQL, Redis, DynamoDB, Cosmos DB, and Spanner.
- PGlite is Node-only; RocksDB and SlateDB remain Rust-only.
- Provider SDK dependencies never enter `@trail/prolly-node`, `build.crab:prolly-kotlin`, or `build.crab:prolly-java`.
- Applications own injected clients and pools; adapters never close borrowed clients.
- Missing bytes and present empty bytes remain distinct in every language.
- Ordered batch reads preserve input length, order, duplicates, and missing positions.
- Root CAS conflicts and transaction conflicts are result values, not exceptions.
- Cancellation has one terminal completion; late callback completion is ignored and releases owned values.
- JDBC and synchronous SQLite work runs on a bounded dispatcher, never a coroutine event-loop thread.
- Every supported manifest cell requires conformance, provider integration, package isolation, physical-layout compatibility, cancellation, and transaction evidence.

---

### Task 1: Node portable protocol and native promise bridge

**Files:**
- Create: `bindings/node/src/remote-store.ts`
- Create: `bindings/node/native/src/remote_store.rs`
- Modify: `bindings/node/native/src/lib.rs`
- Modify: `bindings/node/native/Cargo.toml`
- Modify: `bindings/node/src/native.ts`
- Modify: `bindings/node/src/async.ts`
- Modify: `bindings/node/src/index.ts`
- Test: `bindings/node/test/remote-store.test.ts`
- Test: `bindings/node/native/src/remote_store.rs`

**Interfaces:**
- Produces: `RemoteStore`, `StoreDescriptor`, `StoreCapabilities`, `StoreLimits`, `StoreError`, `OptionalBytes`, `NodeMutation`, `RootCondition`, `RootWrite`, `StoreTransactionResult`, and `RemoteAsyncProllyEngine.open(store, config?, signal?)`.
- Produces native objects `NativeRemoteProllyEngine` and `NativeRemoteProllyTransaction` whose methods return JavaScript promises.

- [ ] **Step 1: Write failing TypeScript protocol tests**

```ts
const descriptor: StoreDescriptor = {
  protocolMajor: 1,
  adapterName: "fake",
  provider: "memory",
  schemaVersion: 1,
  capabilities: {
    nativeBatchReads: true, atomicBatchWrites: true, nodeScan: true,
    hints: true, atomicNodesAndHint: true, rootScan: true,
    rootCompareAndSwap: true, transactions: true, readParallelism: 4,
  },
  limits: {},
};
assert.doesNotThrow(() => validateStoreDescriptor(descriptor));
assert.throws(() => validateStoreDescriptor({...descriptor, protocolMajor: 2}), /protocol major/);
assert.deepEqual(missingBytes(), {present: false, value: new Uint8Array()});
assert.deepEqual(presentBytes(new Uint8Array()), {present: true, value: new Uint8Array()});
```

- [ ] **Step 2: Run the focused test and verify the missing-module failure**

Run: `node --test bindings/node/test/remote-store.test.ts`

Expected: FAIL because `../src/remote-store.ts` does not exist.

- [ ] **Step 3: Implement the public TypeScript protocol**

```ts
export interface RemoteStore {
  descriptor(signal?: AbortSignal): Promise<StoreDescriptor>;
  getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void>;
  batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void>;
  batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]>;
  listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]>;
  getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void>;
  getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes>;
  putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void>;
  deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void>;
  compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult>;
  listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]>;
  commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult>;
}
```

Validate protocol major, non-empty names, schema version, non-zero read parallelism, dependent capabilities, and non-zero present limits exactly as `bindings/go/remote_store.go` does. Clone every incoming and outgoing byte array.

- [ ] **Step 4: Write failing Rust bridge tests**

Add tests proving descriptor validation, ordered batch result-length rejection, node-hash rejection through `RemoteProllyStore`, promise rejection mapping, AbortSignal cancellation, ignored late completion, and zero pending completion tokens after engine drop.

Run: `cargo test --manifest-path bindings/node/native/Cargo.toml remote_store -- --nocapture`

Expected: FAIL because `remote_store` is not declared.

- [ ] **Step 5: Implement the napi-rs bridge**

Use `ThreadsafeFunction` in non-blocking mode to send an owned operation enum to JavaScript. Each operation carries a `tokio::sync::oneshot::Sender`; the JS callback calls the matching `RemoteStore` promise and resolves one typed result. Guard every sender with an atomic `Pending -> Completed | Cancelled` transition. Implement `RemoteStoreBackend` on the Rust adapter and route the engine through `RemoteProllyStore` so CIDs and manifests remain Rust-validated. Add `tokio`, `async-trait`, and `futures` only to the native crate.

- [ ] **Step 6: Verify bridge tests and core dependency isolation**

Run:

```sh
cargo test --manifest-path bindings/node/native/Cargo.toml remote_store -- --nocapture
npm --prefix bindings/node run build:native
node --test bindings/node/test/remote-store.test.ts
npm ls --prefix bindings/node --all
```

Expected: all tests pass, and the core dependency tree contains none of the eight provider SDKs.

- [ ] **Step 7: Commit the Node bridge**

```sh
git add bindings/node/native bindings/node/src bindings/node/test/remote-store.test.ts
git commit -m "feat(node): bridge async remote stores"
```

### Task 2: Kotlin UniFFI bridge, Java futures, and shared conformance kits

**Files:**
- Create: `scripts/regenerate-kotlin-bindings.sh`
- Create: `bindings/kotlin/src/main/kotlin/build/crab/prolly/remote/RemoteStore.kt`
- Create: `bindings/kotlin/src/main/kotlin/build/crab/prolly/remote/RemoteProlly.kt`
- Create: `bindings/java/src/main/java/build/crab/prolly/remote/RemoteProlly.java`
- Create: `bindings/node/storetest/package.json`
- Create: `bindings/node/storetest/src/index.ts`
- Create: `bindings/kotlin/storetest/pom.xml`
- Create: `bindings/kotlin/storetest/src/main/kotlin/build/crab/prolly/storetest/StoreConformance.kt`
- Modify: `bindings/pom.xml`
- Modify: `bindings/kotlin/src/main/kotlin/build/crab/prolly/generated/prolly.kt`
- Test: `bindings/kotlin/src/test/kotlin/build/crab/prolly/RemoteStoreTest.kt`
- Test: `bindings/java/src/test/java/build/crab/prolly/RemoteStoreTest.java`

**Interfaces:**
- Consumes: UniFFI `ForeignRemoteStore` and Rust `AsyncProllyEngine` exported by `bindings/uniffi/src/async_store.rs`.
- Produces: idiomatic Kotlin `RemoteStore` suspending interface, `RemoteProlly.open`, and Java `RemoteProlly.open(...): CompletableFuture<RemoteProlly>`.
- Produces reusable Node and Kotlin conformance runners with identical case names.

- [ ] **Step 1: Write failing Kotlin and Java fake-store tests**

The Kotlin fake implements all 16 operations and records dispatcher thread names. The test must cover present-empty bytes, ordered duplicate reads, CAS create/conflict/delete, transaction rollback, cancellation, and an end-to-end engine put/get. The Java test opens the same Kotlin fake through the future facade and verifies cancellation propagates to the coroutine job.

Run: `mvn -f bindings/pom.xml -pl kotlin,java -am -Dtest=RemoteStoreTest test`

Expected: FAIL because the remote wrapper packages do not exist and generated UniFFI glue lacks the async store exports.

- [ ] **Step 2: Regenerate and deterministically post-process Kotlin glue**

Build `bindings/uniffi`, run the provenance-pinned UniFFI 0.31 Kotlin generator, flatten its redundant output directory, and mechanically rename generated Kotlin classes `AsyncProllyEngine` and `AsyncProllyTransaction` to `RemoteNativeProllyEngine` and `RemoteNativeProllyTransaction`. Keep Rust FFI symbol strings unchanged so Go ABI names remain stable. The script must fail unless every expected declaration and converter is replaced exactly once.

- [ ] **Step 3: Implement Kotlin protocol adapters**

```kotlin
interface RemoteStore {
    suspend fun descriptor(): StoreDescriptor
    suspend fun getNode(cid: ByteArray): OptionalBytes
    suspend fun putNode(cid: ByteArray, value: ByteArray)
    suspend fun deleteNode(cid: ByteArray)
    suspend fun batchNodes(operations: List<NodeMutation>)
    suspend fun batchGetNodesOrdered(cids: List<ByteArray>): List<OptionalBytes>
    suspend fun listNodeCids(): List<ByteArray>
    suspend fun getHint(namespace: ByteArray, key: ByteArray): OptionalBytes
    suspend fun putHint(namespace: ByteArray, key: ByteArray, value: ByteArray)
    suspend fun batchPutNodesWithHint(nodes: List<NodeEntry>, namespace: ByteArray, key: ByteArray, value: ByteArray)
    suspend fun getRootManifest(name: ByteArray): OptionalBytes
    suspend fun putRootManifest(name: ByteArray, manifest: ByteArray)
    suspend fun deleteRootManifest(name: ByteArray)
    suspend fun compareAndSwapRootManifest(name: ByteArray, expected: OptionalBytes, replacement: OptionalBytes): RootCasResult
    suspend fun listRootManifests(): List<NamedStoreRoot>
    suspend fun commitTransaction(nodes: List<NodeMutation>, conditions: List<RootCondition>, roots: List<RootWrite>): StoreTransactionResult
}
```

`ForeignRemoteStoreAdapter` converts provider exceptions to structured `StoreErrorRecord`, copies byte arrays, and uses coroutine cancellation as the single terminal state. `RemoteProlly` wraps only `RemoteNativeProllyEngine`; it does not change the existing local `AsyncProllyEngine` API.

- [ ] **Step 4: Implement the Java facade**

Use `kotlinx-coroutines-jdk8` and a private `CoroutineScope(SupervisorJob() + dispatcher)`. Every public operation returns `future { kotlinDelegate.operation(...) }`; `CompletableFuture.cancel(true)` cancels the child job. `close()` cancels the scope and releases the native object without closing the borrowed provider client.

- [ ] **Step 5: Implement shared conformance runners**

Both runners execute the protocol cases in `conformance/store-protocol-v1/cases.json` and the failure cases in `failure-cases.json`, plus descriptor validation, deterministic scans, capability gates, limit enforcement, cancellation, secret redaction, CAS races, conflict rollback, and engine CID validation. Provider tests call one factory function and may skip only live managed-cloud credentials, never protocol behavior.

- [ ] **Step 6: Verify bridge, fake conformance, and regeneration stability**

Run:

```sh
cargo test --manifest-path bindings/uniffi/Cargo.toml --all-features
./scripts/regenerate-kotlin-bindings.sh
git diff --exit-code -- bindings/kotlin/src/main/kotlin/build/crab/prolly/generated/prolly.kt
mvn -f bindings/pom.xml -pl kotlin,java -am -Dtest=RemoteStoreTest test
```

Expected: all commands succeed and a second regeneration produces no diff.

- [ ] **Step 7: Commit the JVM bridge and conformance kits**

```sh
git add scripts/regenerate-kotlin-bindings.sh bindings/kotlin bindings/java bindings/node/storetest bindings/pom.xml
git commit -m "feat(jvm): bridge async remote stores"
```

### Task 3: SQLite vertical slice

**Files:**
- Create: `bindings/node/stores/sqlite/{package.json,src/index.ts,test/sqlite.test.ts,README.md}`
- Create: `bindings/kotlin/stores/sqlite/{pom.xml,src/main/kotlin/build/crab/prolly/store/sqlite/SqliteStore.kt,src/test/kotlin/build/crab/prolly/store/sqlite/SqliteStoreTest.kt,README.md}`
- Create: `bindings/java/stores/sqlite/{pom.xml,src/main/java/build/crab/prolly/store/sqlite/SqliteStores.java,src/test/java/build/crab/prolly/store/sqlite/SqliteStoresTest.java,README.md}`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

**Interfaces:** Node injects a `better-sqlite3` `Database`; Kotlin injects a JDBC `DataSource`; Java returns the Kotlin adapter through `SqliteStores.from(DataSource, Executor)`.

- [ ] **Step 1: Write failing provider conformance and Rust-layout fixture tests**

Create temporary file databases, initialize schema explicitly, run both shared conformance kits, write a Rust fixture and read it from each language, then write from each language and read through `prolly-store-sqlite`. Verify the adapter does not close an injected database or data source.

- [ ] **Step 2: Run the focused tests and verify missing implementation failures**

Run: `npm --prefix bindings/node/stores/sqlite test` and `mvn -f bindings/pom.xml -pl kotlin/stores/sqlite,java/stores/sqlite -am test`.

- [ ] **Step 3: Implement exact schema and transactions**

Use the three `WITHOUT ROWID` tables from `bindings/go/stores/sqlite/schema.go`. Execute batch nodes, node-plus-hint, CAS, and full commit inside `BEGIN IMMEDIATE` transactions. Lock JVM work to `Dispatchers.IO.limitedParallelism(16)` and Node synchronous calls to a bounded worker owned by the adapter. Descriptor capabilities are all true with read parallelism 16 and no numeric limits.

- [ ] **Step 4: Verify and commit SQLite**

Run both focused suites plus bidirectional Rust fixture tests; then commit `feat(stores): add Node and JVM SQLite adapters`.

### Task 4: PostgreSQL vertical slice

**Files:**
- Create: `bindings/node/stores/postgres/package.json`
- Create: `bindings/node/stores/postgres/src/index.ts`
- Create: `bindings/node/stores/postgres/test/postgres.test.ts`
- Create: `bindings/node/stores/postgres/README.md`
- Create: `bindings/kotlin/stores/postgres/pom.xml`
- Create: `bindings/kotlin/stores/postgres/src/main/kotlin/build/crab/prolly/store/postgres/PostgresStore.kt`
- Create: `bindings/kotlin/stores/postgres/src/test/kotlin/build/crab/prolly/store/postgres/PostgresStoreTest.kt`
- Create: `bindings/kotlin/stores/postgres/README.md`
- Create: `bindings/java/stores/postgres/pom.xml`
- Create: `bindings/java/stores/postgres/src/main/java/build/crab/prolly/store/postgres/PostgresStores.java`
- Create: `bindings/java/stores/postgres/src/test/java/build/crab/prolly/store/postgres/PostgresStoresTest.java`
- Create: `bindings/java/stores/postgres/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

**Interfaces:** Node injects `pg.Pool`; Kotlin injects `DataSource`; Java exposes factory overloads for `DataSource` and bounded `Executor`.

- [ ] **Step 1: Write failing conformance, concurrent CAS, rollback, and bidirectional layout tests**

Use `PROLLY_POSTGRES_URL`; run 32 simultaneous missing-to-value CAS operations and assert exactly one winner. Verify a conflicting transaction writes no nodes.

- [ ] **Step 2: Implement the three-table BYTEA schema**

Use `$1` placeholders in Node and JDBC parameters on JVM. Transactions execute `SELECT manifest FROM prolly_roots WHERE name = ? FOR UPDATE`, compare exact bytes, then apply node/root writes before commit. Ordered batches use one `WHERE cid = ANY(...)` query and restore input order in memory.

- [ ] **Step 3: Verify and commit PostgreSQL**

Run Node/JVM conformance against the container, Rust-write/language-read, language-write/Rust-read, and package dependency audits; commit `feat(stores): add Node and JVM PostgreSQL adapters`.

### Task 5: MySQL vertical slice

**Files:**
- Create: `bindings/node/stores/mysql/{package.json,src/index.ts,test/mysql.test.ts,README.md}`
- Create: `bindings/kotlin/stores/mysql/pom.xml`
- Create: `bindings/kotlin/stores/mysql/src/main/kotlin/build/crab/prolly/store/mysql/MysqlStore.kt`
- Create: `bindings/kotlin/stores/mysql/src/test/kotlin/build/crab/prolly/store/mysql/MysqlStoreTest.kt`
- Create: `bindings/kotlin/stores/mysql/README.md`
- Create: `bindings/java/stores/mysql/pom.xml`
- Create: `bindings/java/stores/mysql/src/main/java/build/crab/prolly/store/mysql/MysqlStores.java`
- Create: `bindings/java/stores/mysql/src/test/java/build/crab/prolly/store/mysql/MysqlStoresTest.java`
- Create: `bindings/java/stores/mysql/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing binary-order, CAS-race, rollback, and layout tests**

Use `PROLLY_MYSQL_URL`; include byte keys containing `0x00`, `0x7f`, `0x80`, and `0xff`, and reject CIDs longer than 32 bytes before the driver call.

- [ ] **Step 2: Implement the exact VARBINARY/LONGBLOB schema**

Use `mysql2/promise` pools in Node and Connector/J `DataSource` on JVM. Use `SELECT ... FOR UPDATE`, `INSERT ... ON DUPLICATE KEY UPDATE`, and one SQL transaction for CAS, node-plus-hint, and strict commit. Descriptor matches Go: all capabilities true, read parallelism 16, no numeric limits.

- [ ] **Step 3: Verify and commit MySQL**

Run conformance, 32-way CAS, rollback, bidirectional fixtures, cancellation, and dependency isolation; commit `feat(stores): add Node and JVM MySQL adapters`.

### Task 6: Redis vertical slice

**Files:**
- Create: `bindings/node/stores/redis/{package.json,src/index.ts,test/redis.test.ts,README.md}`
- Create: `bindings/kotlin/stores/redis/pom.xml`
- Create: `bindings/kotlin/stores/redis/src/main/kotlin/build/crab/prolly/store/redis/RedisStore.kt`
- Create: `bindings/kotlin/stores/redis/src/test/kotlin/build/crab/prolly/store/redis/RedisStoreTest.kt`
- Create: `bindings/kotlin/stores/redis/README.md`
- Create: `bindings/java/stores/redis/pom.xml`
- Create: `bindings/java/stores/redis/src/main/java/build/crab/prolly/store/redis/RedisStores.java`
- Create: `bindings/java/stores/redis/src/test/java/build/crab/prolly/store/redis/RedisStoresTest.java`
- Create: `bindings/java/stores/redis/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing binary-prefix, Lua CAS, transaction, and persistence-documentation tests**

Use `PROLLY_REDIS_URL`; verify binary-safe keys for `node:`, `hint:`, and `root:` families, deterministic `SCAN` output after in-memory sorting, and exactly one winner under 32 concurrent CAS calls.

- [ ] **Step 2: Implement official-client adapters**

Use node-redis binary buffers and Lettuce `ByteArrayCodec`. Lua scripts compare exact root bytes and atomically perform CAS and strict node/root transactions. Batch reads use `MGET` and preserve duplicates. Document that Redis primary-storage use requires AOF with an explicit fsync policy and protected backups.

- [ ] **Step 3: Verify and commit Redis**

Run conformance, Lua rollback, cross-language layout, cancellation, and dependency audits; commit `feat(stores): add Node and JVM Redis adapters`.

### Task 7: DynamoDB vertical slice

**Files:**
- Create: `bindings/node/stores/dynamodb/{package.json,src/index.ts,test/dynamodb.test.ts,README.md}`
- Create: `bindings/kotlin/stores/dynamodb/pom.xml`
- Create: `bindings/kotlin/stores/dynamodb/src/main/kotlin/build/crab/prolly/store/dynamodb/DynamoDbStore.kt`
- Create: `bindings/kotlin/stores/dynamodb/src/test/kotlin/build/crab/prolly/store/dynamodb/DynamoDbStoreTest.kt`
- Create: `bindings/kotlin/stores/dynamodb/README.md`
- Create: `bindings/java/stores/dynamodb/pom.xml`
- Create: `bindings/java/stores/dynamodb/src/main/java/build/crab/prolly/store/dynamodb/DynamoDbStores.java`
- Create: `bindings/java/stores/dynamodb/src/test/java/build/crab/prolly/store/dynamodb/DynamoDbStoresTest.java`
- Create: `bindings/java/stores/dynamodb/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing chunking, unprocessed-read retry, conditional-root, limit, and layout tests**

Use DynamoDB Local from `PROLLY_DYNAMODB_ENDPOINT`. Force more than 100 requested reads and more than 25 writes. Stub one `UnprocessedKeys` response and prove restored order and duplicates. Assert transaction requests above 100 operations fail before any SDK call.

- [ ] **Step 2: Implement the single binary-key table**

Use official AWS SDK clients, binary `pk` and `value`, the same key tags as the Rust/Go adapter, 100-item `BatchGetItem` chunks with bounded retries, 25-item `BatchWriteItem` chunks, conditional root updates, and `TransactWriteItems` for strict commits. Advertise non-atomic logical batch writes and node-plus-hint, with limits 100/25/100.

- [ ] **Step 3: Verify and commit DynamoDB**

Run local conformance, limit/preflight tests, bidirectional layout fixtures, cancellation, and package audits; commit `feat(stores): add Node and JVM DynamoDB adapters`.

### Task 8: Cosmos DB vertical slice

**Files:**
- Create: `bindings/node/stores/cosmosdb/{package.json,src/index.ts,test/cosmosdb.test.ts,README.md}`
- Create: `bindings/kotlin/stores/cosmosdb/pom.xml`
- Create: `bindings/kotlin/stores/cosmosdb/src/main/kotlin/build/crab/prolly/store/cosmosdb/CosmosDbStore.kt`
- Create: `bindings/kotlin/stores/cosmosdb/src/test/kotlin/build/crab/prolly/store/cosmosdb/CosmosDbStoreTest.kt`
- Create: `bindings/kotlin/stores/cosmosdb/README.md`
- Create: `bindings/java/stores/cosmosdb/pom.xml`
- Create: `bindings/java/stores/cosmosdb/src/main/java/build/crab/prolly/store/cosmosdb/CosmosDbStores.java`
- Create: `bindings/java/stores/cosmosdb/src/test/java/build/crab/prolly/store/cosmosdb/CosmosDbStoresTest.java`
- Create: `bindings/java/stores/cosmosdb/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing SDK-contract, ETag CAS, partition, limit, redaction, and optional live tests**

Use official SDK fakes for ordinary tests and `PROLLY_COSMOS_ENDPOINT`, `PROLLY_COSMOS_KEY`, and `PROLLY_COSMOS_DATABASE` when present. Verify the container partition key is exactly `/kind`, all strict transaction items share the one `kind` partition, 101 operations fail locally, and account keys never appear in errors.

- [ ] **Step 2: Implement official Cosmos SDK adapters**

Encode node/root/hint documents exactly as the Go adapter, use base64url IDs, point reads, ETag `IfMatch` for CAS, and transactional batches for strict commits. Descriptor advertises non-native batch reads, non-atomic logical batch writes and node-plus-hint, scans, hints, root CAS, transactions, read parallelism 16, and a 100-operation transaction limit.

- [ ] **Step 3: Verify and commit Cosmos DB**

Run SDK-contract conformance in both stacks, optional live gates when credentials exist, physical-document fixtures, cancellation, redaction, and dependency audits; commit `feat(stores): add Node and JVM Cosmos DB adapters`.

### Task 9: Spanner vertical slice

**Files:**
- Create: `bindings/node/stores/spanner/{package.json,src/index.ts,test/spanner.test.ts,README.md}`
- Create: `bindings/kotlin/stores/spanner/pom.xml`
- Create: `bindings/kotlin/stores/spanner/src/main/kotlin/build/crab/prolly/store/spanner/SpannerStore.kt`
- Create: `bindings/kotlin/stores/spanner/src/test/kotlin/build/crab/prolly/store/spanner/SpannerStoreTest.kt`
- Create: `bindings/kotlin/stores/spanner/README.md`
- Create: `bindings/java/stores/spanner/pom.xml`
- Create: `bindings/java/stores/spanner/src/main/java/build/crab/prolly/store/spanner/SpannerStores.java`
- Create: `bindings/java/stores/spanner/src/test/java/build/crab/prolly/store/spanner/SpannerStoresTest.java`
- Create: `bindings/java/stores/spanner/README.md`
- Modify: `bindings/pom.xml`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing emulator conformance, transaction-race, rollback, DDL, and layout tests**

Use `SPANNER_EMULATOR_HOST`; create an isolated emulator database, apply the three exact DDL statements from `bindings/go/stores/spanner/schema.go`, run 32-way CAS, and verify language/Rust bidirectional reads.

- [ ] **Step 2: Implement official Spanner SDK adapters**

Use `ProllyNodes(Cid, Node)`, `ProllyHints(Namespace, HintKey, Value)`, and `ProllyRoots(Name, Manifest)`. Execute CAS and full commit in read-write transactions. Use mutations for atomic batch writes and node-plus-hint. Descriptor advertises all capabilities except native batch reads, read parallelism 16, and no numeric limits.

- [ ] **Step 3: Verify and commit Spanner**

Run emulator conformance, DDL idempotence, CAS/rollback, layout fixtures, cancellation, and package audits; commit `feat(stores): add Node and JVM Spanner adapters`.

### Task 10: Node-only PGlite vertical slice

**Files:**
- Create: `bindings/node/stores/pglite/{package.json,src/index.ts,test/pglite.test.ts,README.md}`
- Modify: `conformance/store-protocol-v1/compatibility.json`

- [ ] **Step 1: Write failing in-process conformance and PostgreSQL-layout tests**

Open a temporary PGlite data directory, run the Node conformance kit, and verify the same three-table PostgreSQL schema and transaction behavior.

- [ ] **Step 2: Implement the PGlite adapter**

Inject `@electric-sql/pglite` `PGlite`, use PostgreSQL SQL with native transactions, restore ordered batch results in memory, and advertise the same capabilities as PostgreSQL.

- [ ] **Step 3: Verify and commit PGlite**

Run in-process conformance, reopen durability, dependency isolation, and layout tests; commit `feat(node): add PGlite remote store adapter`.

### Task 11: Compatibility verifier, aggregate service runner, documentation, and release audit

**Files:**
- Create: `scripts/test-node-jvm-stores.sh`
- Create: `scripts/verify-store-compatibility.mjs`
- Modify: `conformance/store-protocol-v1/compatibility.json`
- Modify: `bindings/VERIFICATION.md`
- Modify: `bindings/node/README.md`
- Modify: `bindings/kotlin/README.md`
- Modify: `bindings/java/README.md`
- Modify: `docs/language-store-adapters-design.md`
- Modify: `.github/workflows/*` only where an existing binding workflow owns these tests.

- [ ] **Step 1: Write failing manifest completeness and dependency-isolation tests**

The verifier requires supported Node and JVM entries for all seven shared providers, Node PGlite, exact protocol/schema versions, SDK coordinates/versions, capabilities, limits, and executable evidence commands. It rejects provider SDKs in the three core artifacts and rejects unsupported placeholders.

- [ ] **Step 2: Implement deterministic aggregate verification**

The service runner accepts `--services-running`, otherwise starts `docker-compose.store-services.yml`, waits with `scripts/verify-store-services.sh`, runs every Node provider with fresh test execution, runs every Kotlin and Java provider through Maven, and always tears down only services it started. Cosmos live execution remains an explicit credentialed gate; its SDK-contract suite always runs.

- [ ] **Step 3: Update user and operator documentation**

Document injected-client ownership, explicit schema initialization, cancellation, limits, durability, credential loading, Redis persistence, local emulator commands, managed-cloud gates, package coordinates, and one minimal constructor example per provider and language facade.

- [ ] **Step 4: Run the full fresh completion audit**

```sh
cargo test --features async-store --lib
cargo test --manifest-path bindings/uniffi/Cargo.toml --all-features
cargo clippy --all-targets --features async-store -- -D warnings
cargo clippy --manifest-path bindings/uniffi/Cargo.toml --all-targets --all-features -- -D warnings
npm --prefix bindings/node run build:native
npm --prefix bindings/node test
mvn -f bindings/pom.xml test
./scripts/test-go-stores.sh --services-running
./scripts/test-node-jvm-stores.sh --services-running
node scripts/verify-store-compatibility.mjs
git diff --check
git status --short
```

Expected: every command exits 0; all local/emulator provider cells pass; Cosmos SDK-contract tests pass and the live result is reported separately according to credential availability; the worktree contains only intentional tracked changes.

- [ ] **Step 5: Commit the release gate and docs**

```sh
git add scripts conformance bindings docs .github/workflows
git commit -m "test(stores): verify Node and JVM provider matrix"
```
