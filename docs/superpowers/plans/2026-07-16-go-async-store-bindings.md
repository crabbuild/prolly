# Go Async Store Bindings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the Go binding drive the Rust `AsyncProlly` engine through a versioned, context-aware host-store protocol, with separately installable SQLite, PostgreSQL, MySQL, Redis, DynamoDB, Cosmos DB, and Spanner adapters that are physically compatible with their Rust counterparts.

**Architecture:** The core Go module defines owned protocol records and a `RemoteStore` interface whose asynchronous boundary is `context.Context`. A UniFFI foreign-future callback bridge adapts that interface to `RemoteStoreBackend`, and an exported Rust `AsyncProllyEngine` keeps all tree algorithms and validation in Rust. Each provider is a nested Go module with its native driver/SDK, explicit schema initialization, advertised capabilities/limits, and the same physical schema as the existing Rust provider crate.

**Tech Stack:** Rust 1.81+ library code, UniFFI 0.31 async callback/future ABI, Go 1.22 core API, Go 1.24/1.25 provider modules, `modernc.org/sqlite`, `pgx/v5`, `go-sql-driver/mysql`, `go-redis/v9`, AWS SDK for Go v2 DynamoDB, Azure `azcosmos`, and Google Cloud Spanner.

## Global Constraints

- Protocol major is exactly `1`; unknown capabilities are false and absent numeric limits mean “not preflightable,” not unlimited.
- Keep `bindings/go/go.mod` at Go 1.22 and free of database/cloud SDK dependencies.
- Keep every provider in its own module under `bindings/go/stores/<provider>`; provider modules may require the Go floor of their selected SDK.
- Provider constructors accept injected clients/pools/connections; connection-string constructors and schema initialization are explicit provider-package APIs.
- Rust remains authoritative for CIDs, manifests, proofs, diff, merge, sync, garbage collection, and transaction coordination.
- All host I/O is truly asynchronous across the ABI; cancellation propagates from Go contexts through Rust futures to provider calls.
- RocksDB, SlateDB, PGlite, IndexedDB, OPFS, and an HTTP gateway are out of scope for Go.
- No supported cell is complete without protocol conformance, cancellation, lifecycle, strict transaction, physical-schema, and Rust-interoperability tests.
- Preserve unrelated dirty changes in `bindings/go/portable_ffi.go`, `bindings/go/portable_parity_test.go`, and `bindings/go/versioned.go`.

---

## File Map

- `src/prolly/transaction.rs`: add an owned async transaction for FFI-safe lifetimes.
- `bindings/uniffi/src/async_store.rs`: portable records, async foreign callback, validated `RemoteStoreBackend`, async engine, and async transaction exports.
- `bindings/uniffi/src/lib.rs`: register and re-export the async-store binding module.
- `bindings/uniffi/Cargo.toml`: enable `prolly-map/async-store` and add only bridge-runtime dependencies.
- `bindings/go/remote_store.go`: public Go protocol, errors, descriptors, and validation.
- `bindings/go/remote_store_ffi.go`: UniFFI record lowering/lifting and foreign-future callback vtable.
- `bindings/go/remote_store_callbacks.go`: callback-handle registry, goroutine dispatch, cancellation, and panic isolation.
- `bindings/go/async_engine.go`: context-aware Go engine and transaction API over Rust futures.
- `bindings/go/async_engine_test.go`: bridge and engine behavior through a deterministic fake store.
- `bindings/go/storetest/conformance.go`: reusable provider conformance contract.
- `conformance/store-protocol-v1/{protocol.json,cases.json,failure-cases.json}`: language-neutral fixtures.
- `bindings/go/stores/<provider>`: one module, adapter, schema, tests, and README per supported provider.
- `bindings/go/stores/go.work`: local workspace joining the core and seven provider modules without publish-time `replace` directives.
- `conformance/store-protocol-v1/rust-interop`: Rust fixture reader/writer used by bidirectional physical compatibility tests.
- `scripts/test-go-stores.sh`: deterministic core/unit/integration/interop test gate.
- `docs/language-store-adapters-design.md`: Go package names, SDKs, setup, and compatibility table.

---

### Task 1: FFI-safe owned async transactions

**Files:**
- Modify: `src/prolly/transaction.rs`
- Test: `src/prolly/transaction.rs` (`tests` module)

**Interfaces:**
- Consumes: `AsyncProlly<S>`, `AsyncStore`, `AsyncManifestStore`, and `AsyncTransactionalStore`.
- Produces: `OwnedAsyncProllyTransaction<S>` and `AsyncProlly::begin_owned_transaction(&self) -> Result<OwnedAsyncProllyTransaction<S>, Error>` where `S: Clone`.

- [ ] **Step 1: Write the failing owned-lifetime and rollback tests**

```rust
#[test]
fn owned_async_transaction_outlives_manager_borrow() {
    futures_lite::future::block_on(async {
        let store = CloneableAsyncTransactionalStore::default();
        let tx = AsyncProlly::default(store.clone())
            .begin_owned_transaction()
            .unwrap();
        let tree = tx.create();
        let tree = tx.put(&tree, b"a".to_vec(), b"1".to_vec()).await.unwrap();
        tx.publish_named_root(b"main", &tree).await.unwrap();
        assert!(matches!(tx.commit().await.unwrap(), TransactionUpdate::Applied { .. }));
    });
}

#[test]
fn dropping_owned_async_transaction_discards_overlay() {
    futures_lite::future::block_on(async {
        let store = CloneableAsyncTransactionalStore::default();
        let engine = AsyncProlly::default(store.clone());
        let tx = engine.begin_owned_transaction().unwrap();
        let tree = tx.put(&tx.create(), b"a".to_vec(), b"1".to_vec()).await.unwrap();
        tx.publish_named_root(b"main", &tree).await.unwrap();
        drop(tx);
        assert_eq!(engine.load_named_root(b"main").await.unwrap(), None);
    });
}
```

- [ ] **Step 2: Run the focused test and verify red**

Run: `cargo test --features async-store owned_async_transaction --lib`

Expected: compilation fails because `begin_owned_transaction` and `OwnedAsyncProllyTransaction` do not exist.

- [ ] **Step 3: Add the owned overlay and transaction**

```rust
#[cfg(feature = "async-store")]
pub struct OwnedAsyncProllyTransaction<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore,
{
    base_store: S,
    state: Arc<Mutex<TransactionState>>,
    manager: AsyncProlly<OwnedAsyncTransactionOverlayStore<S>>,
    completed: bool,
}

impl<S> AsyncProlly<S>
where
    S: AsyncStore + AsyncManifestStore + AsyncTransactionalStore + Clone,
    <S as AsyncStore>::Error: Send + Sync,
    <S as AsyncManifestStore>::Error: Send + Sync,
{
    pub fn begin_owned_transaction(&self) -> Result<OwnedAsyncProllyTransaction<S>, Error> {
        OwnedAsyncProllyTransaction::new(self)
    }
}
```

Implement the owned overlay by cloning `S` into the overlay and the transaction, using the existing `TransactionState` and exact read-set/write-set semantics. `commit(self)` must call `base_store.commit_transaction(...).await`; `rollback(self)` and `Drop` only discard the overlay.

- [ ] **Step 4: Run the async transaction suite and verify green**

Run: `cargo test --features async-store transaction::tests --lib`

Expected: all transaction tests pass, including applied commit, conflict, explicit rollback, and drop rollback.

- [ ] **Step 5: Commit the isolated core change**

```bash
git add src/prolly/transaction.rs
git commit -m "feat: add owned async prolly transactions"
```

### Task 2: Versioned async foreign-store protocol

**Files:**
- Create: `bindings/uniffi/src/async_store.rs`
- Modify: `bindings/uniffi/src/lib.rs`
- Modify: `bindings/uniffi/Cargo.toml`
- Test: `bindings/uniffi/src/async_store.rs` (`tests` module)

**Interfaces:**
- Consumes: `RemoteStoreBackend`, `RemoteProllyStore`, protocol records from the approved design.
- Produces: `ForeignRemoteStore`, `ForeignRemoteBackend`, `StoreDescriptorRecord`, `StoreCapabilitiesRecord`, `StoreLimitsRecord`, `StoreErrorRecord`, and operation result records.

- [ ] **Step 1: Write failing descriptor and callback tests**

```rust
#[test]
fn descriptor_rejects_wrong_protocol_and_zero_parallelism() {
    assert_eq!(validate_descriptor(descriptor(2, 4)).unwrap_err().code, "invalid_descriptor");
    assert_eq!(validate_descriptor(descriptor(1, 0)).unwrap_err().code, "invalid_descriptor");
}

#[test]
fn foreign_backend_preserves_batch_order_and_structured_error() {
    block_on(async {
        let callback = Arc::new(TestForeignStore::with_batch(vec![Some(b"b".to_vec()), None]));
        let backend = ForeignRemoteBackend::new(callback).await.unwrap();
        assert_eq!(backend.batch_get_nodes_ordered(&[b"b", b"missing"]).await.unwrap(),
                   vec![Some(b"b".to_vec()), None]);
    });
}
```

- [ ] **Step 2: Verify red**

Run: `cargo test --manifest-path bindings/uniffi/Cargo.toml async_store --no-default-features`

Expected: compilation fails because `async_store` records and callback do not exist.

- [ ] **Step 3: Define exact protocol records and async callback**

```rust
#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreDescriptorRecord {
    pub protocol_major: u32,
    pub adapter_name: String,
    pub provider: String,
    pub schema_version: u32,
    pub capabilities: StoreCapabilitiesRecord,
    pub limits: StoreLimitsRecord,
}

#[derive(Clone, Debug, PartialEq, Eq, uniffi::Record)]
pub struct StoreErrorRecord {
    pub code: String,
    pub message: String,
    pub retryable: bool,
    pub provider_code: Option<String>,
}

#[uniffi::export(with_foreign)]
#[async_trait]
pub trait ForeignRemoteStore: Send + Sync {
    async fn descriptor(&self) -> StoreDescriptorResultRecord;
    async fn get_node(&self, cid: Vec<u8>) -> OptionalBytesResultRecord;
    async fn put_node(&self, cid: Vec<u8>, value: Vec<u8>) -> UnitResultRecord;
    async fn delete_node(&self, cid: Vec<u8>) -> UnitResultRecord;
    async fn batch_nodes(&self, ops: Vec<NodeMutationRecord>) -> UnitResultRecord;
    async fn batch_get_nodes_ordered(&self, cids: Vec<Vec<u8>>) -> OptionalBytesListResultRecord;
    async fn list_node_cids(&self) -> BytesListResultRecord;
    async fn get_hint(&self, namespace: Vec<u8>, key: Vec<u8>) -> OptionalBytesResultRecord;
    async fn put_hint(&self, namespace: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> UnitResultRecord;
    async fn batch_put_nodes_with_hint(&self, nodes: Vec<NodeEntryRecord>, namespace: Vec<u8>, key: Vec<u8>, value: Vec<u8>) -> UnitResultRecord;
    async fn get_root_manifest(&self, name: Vec<u8>) -> OptionalBytesResultRecord;
    async fn put_root_manifest(&self, name: Vec<u8>, manifest: Vec<u8>) -> UnitResultRecord;
    async fn delete_root_manifest(&self, name: Vec<u8>) -> UnitResultRecord;
    async fn compare_and_swap_root_manifest(&self, name: Vec<u8>, expected: Option<Vec<u8>>, new: Option<Vec<u8>>) -> RootCasResultRecord;
    async fn list_root_manifests(&self) -> NamedBytesListResultRecord;
    async fn commit_transaction(&self, nodes: Vec<NodeMutationRecord>, conditions: Vec<RootConditionRecord>, roots: Vec<RootWriteRecord>) -> TransactionResultRecord;
}
```

Use `#[uniffi::export(with_foreign)]` async callback support from UniFFI 0.31. Result records always contain either a value or `StoreErrorRecord`; map callback errors to a `ForeignStoreError` implementing `std::error::Error`. Cache the validated descriptor in `ForeignRemoteBackend`; never call `descriptor` per operation.

- [ ] **Step 4: Implement the backend mapping and capability gates**

Map every `RemoteStoreBackend` method to its callback. Return `unsupported` before invoking optional methods when the matching capability is false. Enforce batch limits and exact output length for ordered batch reads. Configure:

```toml
prolly-map = { path = "../..", features = ["async-store"] }
async-trait = "0.1"
```

- [ ] **Step 5: Verify green and all-features compatibility**

Run: `cargo test --manifest-path bindings/uniffi/Cargo.toml async_store --no-default-features && cargo test --manifest-path bindings/uniffi/Cargo.toml --all-features`

Expected: protocol tests and the existing binding tests pass.

- [ ] **Step 6: Commit**

```bash
git add bindings/uniffi/Cargo.toml bindings/uniffi/src/lib.rs bindings/uniffi/src/async_store.rs
git commit -m "feat(bindings): add async foreign store protocol"
```

### Task 3: Export the Rust async engine and transaction objects

**Files:**
- Modify: `bindings/uniffi/src/async_store.rs`
- Test: `bindings/uniffi/src/async_store.rs` (`tests` module)

**Interfaces:**
- Consumes: `ForeignRemoteBackend`, `RemoteProllyStore`, `AsyncProlly`, `OwnedAsyncProllyTransaction`.
- Produces: `AsyncProllyEngine::new`, tree CRUD/range/root/diff/merge/proof/sync/GC methods, and `AsyncProllyTransaction` methods.

- [ ] **Step 1: Write a failing end-to-end fake-store test**

```rust
#[test]
fn async_engine_uses_foreign_store_for_tree_root_and_transaction() {
    block_on(async {
        let callback = Arc::new(TestForeignStore::transactional());
        let engine = AsyncProllyEngine::new(callback.clone(), None).await.unwrap();
        let tree = engine.create();
        let tree = engine.put(tree, b"a".to_vec(), b"1".to_vec()).await.unwrap();
        engine.publish_named_root(b"main".to_vec(), tree.clone()).await.unwrap();
        assert_eq!(engine.get(tree.clone(), b"a".to_vec()).await.unwrap(), Some(b"1".to_vec()));
        let tx = engine.begin_transaction().await.unwrap();
        let tx_tree = tx.put(tree, b"b".to_vec(), b"2".to_vec()).await.unwrap();
        tx.publish_named_root(b"main".to_vec(), tx_tree).await.unwrap();
        assert!(tx.commit().await.unwrap().applied);
    });
}
```

- [ ] **Step 2: Verify red**

Run: `cargo test --manifest-path bindings/uniffi/Cargo.toml async_engine_uses_foreign_store --no-default-features`

Expected: `AsyncProllyEngine` is unresolved.

- [ ] **Step 3: Export the engine surface**

```rust
type ForeignEngine = AsyncProlly<RemoteProllyStore<ForeignRemoteBackend>>;

#[derive(uniffi::Object)]
pub struct AsyncProllyEngine { inner: Arc<ForeignEngine> }

#[uniffi::export]
impl AsyncProllyEngine {
    #[uniffi::constructor]
    pub async fn new(store: Arc<dyn ForeignRemoteStore>, config: Option<ConfigRecord>)
        -> Result<Arc<Self>, ProllyBindingError>;
    pub fn create(&self) -> TreeRecord;
    pub async fn get(&self, tree: TreeRecord, key: Vec<u8>) -> Result<Option<Vec<u8>>, ProllyBindingError>;
    pub async fn put(&self, tree: TreeRecord, key: Vec<u8>, value: Vec<u8>) -> Result<TreeRecord, ProllyBindingError>;
    pub async fn delete(&self, tree: TreeRecord, key: Vec<u8>) -> Result<TreeRecord, ProllyBindingError>;
    pub async fn batch(&self, tree: TreeRecord, mutations: Vec<MutationRecord>) -> Result<TreeRecord, ProllyBindingError>;
    pub async fn range(&self, tree: TreeRecord, start: Vec<u8>, end: Option<Vec<u8>>) -> Result<Vec<EntryRecord>, ProllyBindingError>;
    pub async fn load_named_root(&self, name: Vec<u8>) -> Result<Option<TreeRecord>, ProllyBindingError>;
    pub async fn publish_named_root(&self, name: Vec<u8>, tree: TreeRecord) -> Result<(), ProllyBindingError>;
    pub async fn compare_and_swap_named_root(&self, name: Vec<u8>, expected: Option<TreeRecord>, new: Option<TreeRecord>) -> Result<NamedRootUpdateRecord, ProllyBindingError>;
    pub async fn begin_transaction(&self) -> Result<Arc<AsyncProllyTransaction>, ProllyBindingError>;
}
```

Add the established binding records for range pages, reverse range, diff pages, merge, proofs, missing-node plan/copy, named-root enumeration/deletion, GC plan/sweep, and inspection. Do not duplicate algorithms: each export delegates to the corresponding `AsyncProlly` method. Store an owned transaction behind an async-aware mutex and make commit/rollback terminal and idempotently rejected afterward.

- [ ] **Step 4: Test the full binding surface**

Run: `cargo test --manifest-path bindings/uniffi/Cargo.toml async_store::tests --no-default-features`

Expected: fake-store tests pass for CRUD, ordered batches, range, roots/CAS, transaction conflict, cancellation-safe drop, diff/merge/proofs, sync, and GC.

- [ ] **Step 5: Commit**

```bash
git add bindings/uniffi/src/async_store.rs
git commit -m "feat(bindings): export async prolly engine"
```

### Task 4: Core Go protocol and UniFFI async bridge

**Files:**
- Create: `bindings/go/remote_store.go`
- Create: `bindings/go/remote_store_ffi.go`
- Create: `bindings/go/remote_store_callbacks.go`
- Create: `bindings/go/async_engine.go`
- Create: `bindings/go/async_engine_test.go`

**Interfaces:**
- Consumes: generated C symbols for `ForeignRemoteStore` and Rust futures.
- Produces: `RemoteStore`, `NewAsyncEngine(context.Context, RemoteStore, *Config)`, `AsyncEngine`, and `AsyncTransaction`.

- [ ] **Step 1: Write failing public-contract tests**

```go
func TestValidateStoreDescriptor(t *testing.T) {
    good := storetest.Descriptor("fake", true)
    if err := good.Validate(); err != nil { t.Fatal(err) }
    bad := good; bad.ProtocolMajor = 2
    if got := bad.Validate(); !errors.Is(got, ErrInvalidDescriptor) { t.Fatalf("got %v", got) }
}

func TestAsyncEngineCancellationReachesStore(t *testing.T) {
    store := newBlockingFakeStore()
    engine := mustNewAsyncEngine(t, store)
    ctx, cancel := context.WithCancel(context.Background())
    done := make(chan error, 1)
    go func() { _, err := engine.Get(ctx, Tree{}, []byte("a")); done <- err }()
    <-store.started; cancel()
    if err := <-done; !errors.Is(err, context.Canceled) { t.Fatalf("got %v", err) }
    select { case <-store.canceled: case <-time.After(time.Second): t.Fatal("provider context not canceled") }
}
```

- [ ] **Step 2: Verify red**

Run: `(cd bindings/go && go test ./... -run 'TestValidateStoreDescriptor|TestAsyncEngineCancellationReachesStore')`

Expected: compilation fails because `RemoteStore` and `NewAsyncEngine` are undefined.

- [ ] **Step 3: Add the public Go protocol**

```go
type RemoteStore interface {
    Descriptor(context.Context) (StoreDescriptor, error)
    GetNode(context.Context, []byte) ([]byte, error)
    PutNode(context.Context, []byte, []byte) error
    DeleteNode(context.Context, []byte) error
    BatchNodes(context.Context, []NodeMutation) error
    BatchGetNodesOrdered(context.Context, [][]byte) ([][]byte, error)
    ListNodeCIDs(context.Context) ([][]byte, error)
    GetHint(context.Context, []byte, []byte) ([]byte, error)
    PutHint(context.Context, []byte, []byte, []byte) error
    BatchPutNodesWithHint(context.Context, []NodeEntry, []byte, []byte, []byte) error
    GetRootManifest(context.Context, []byte) ([]byte, error)
    PutRootManifest(context.Context, []byte, []byte) error
    DeleteRootManifest(context.Context, []byte) error
    CompareAndSwapRootManifest(context.Context, []byte, []byte, []byte) (RootCASResult, error)
    ListRootManifests(context.Context) ([]NamedRootManifest, error)
    CommitTransaction(context.Context, []NodeMutation, []RootCondition, []RootWrite) (TransactionResult, error)
}

type StoreError struct {
    Code, Message string
    Retryable bool
    ProviderCode string
    Cause error
}
```

Use `nil` slices for protocol “missing” values and preserve empty-but-present values through explicit optional ABI records. Validate provider name, adapter name, protocol/schema versions, parallelism, and nonzero limits.

- [ ] **Step 4: Implement the UniFFI callback/future ABI**

Register each `RemoteStore` in a locked handle table. Each exported callback copies lowered Rust buffers, creates a child context, fills `ForeignFutureDroppedCallbackStruct` with a cancellation function, starts exactly one goroutine, recovers panics into `StoreError{Code: "panic"}`, lowers the result, and calls the UniFFI completion once. The Rust-future awaiter must poll, react to its continuation, call the cancel symbol when `ctx.Done()` wins, then complete/free exactly once. Put all required C declarations and shims in the new cgo files so the already-modified generated `prolly.go` is untouched.

- [ ] **Step 5: Export context-aware engine methods**

```go
func NewAsyncEngine(ctx context.Context, store RemoteStore, config *Config) (*AsyncEngine, error)
func (e *AsyncEngine) Get(ctx context.Context, tree Tree, key []byte) ([]byte, error)
func (e *AsyncEngine) Put(ctx context.Context, tree Tree, key, value []byte) (Tree, error)
func (e *AsyncEngine) Delete(ctx context.Context, tree Tree, key []byte) (Tree, error)
func (e *AsyncEngine) Batch(ctx context.Context, tree Tree, mutations []Mutation) (Tree, error)
func (e *AsyncEngine) Range(ctx context.Context, tree Tree, start, end []byte) ([]Entry, error)
func (e *AsyncEngine) BeginTransaction(ctx context.Context) (*AsyncTransaction, error)
func (e *AsyncEngine) Close() error
```

Mirror the rest of Task 3’s surface using existing Go record names where possible. Finalizers are leak fallbacks only; explicit `Close` releases Rust handles and makes later calls return `ErrClosed`.

- [ ] **Step 6: Verify race, cancellation, and normal operation**

Run: `cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target --no-default-features && (cd bindings/go && go test -race ./...)`

Expected: all core Go tests pass under the race detector with no callback-handle or Rust-future leaks.

- [ ] **Step 7: Commit only the new bridge files**

```bash
git add bindings/go/remote_store.go bindings/go/remote_store_ffi.go bindings/go/remote_store_callbacks.go bindings/go/async_engine.go bindings/go/async_engine_test.go
git diff --cached --check
git commit -m "feat(go): add async remote store engine"
```

### Task 5: Shared Go conformance suite and provider workspace

**Files:**
- Create: `bindings/go/storetest/conformance.go`
- Create: `bindings/go/storetest/fake.go`
- Create: `bindings/go/storetest/conformance_test.go`
- Create: `bindings/go/stores/go.work`
- Create: `conformance/store-protocol-v1/protocol.json`
- Create: `conformance/store-protocol-v1/cases.json`
- Create: `conformance/store-protocol-v1/failure-cases.json`

**Interfaces:**
- Consumes: `RemoteStore`.
- Produces: `storetest.Run(t, Factory)` and canonical binary fixtures consumed by every provider.

- [ ] **Step 1: Write the failing fake conformance test**

```go
func TestFakeConformance(t *testing.T) {
    storetest.Run(t, func(ctx context.Context, t *testing.T) prolly.RemoteStore {
        return storetest.NewFakeStore(storetest.AllCapabilities())
    })
}
```

- [ ] **Step 2: Verify red**

Run: `(cd bindings/go && go test ./storetest)`

Expected: compilation fails because `Run` is undefined.

- [ ] **Step 3: Implement named conformance cases**

`Run` must contain independently reported subtests for descriptor validation; missing-vs-empty bytes; idempotent put/delete; ordered batch reads; limit chunking; node scan sorting; hint capability; atomic nodes+hint; root put/get/delete/list ordering; root CAS create/update/delete/conflict; transaction apply/conflict/no-partial-write; structured retryable/nonretryable errors; cancellation; concurrent close; and engine CID/manifest validation.

The fixture JSON uses hex for opaque bytes and has explicit `present` booleans:

```json
{"protocol_major":1,"cases":[{"name":"empty-present","present":true,"hex":""},{"name":"missing","present":false,"hex":""}]}
```

- [ ] **Step 4: Verify green**

Run: `(cd bindings/go && go test -race ./storetest)`

Expected: every named conformance subtest passes for the fake adapter.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/storetest bindings/go/stores/go.work conformance/store-protocol-v1
git commit -m "test(go): add remote store conformance suite"
```

### Task 6: SQLite provider vertical slice

**Files:**
- Create: `bindings/go/stores/sqlite/go.mod`
- Create: `bindings/go/stores/sqlite/sqlite.go`
- Create: `bindings/go/stores/sqlite/schema.go`
- Create: `bindings/go/stores/sqlite/sqlite_test.go`
- Create: `bindings/go/stores/sqlite/README.md`

**Interfaces:**
- Consumes: `*sql.DB`, `RemoteStore`, `storetest.Run`.
- Produces: `sqlite.New(*sql.DB, Options)`, `sqlite.Open(string, Options)`, and `(*Store).InitializeSchema(context.Context)`.

- [ ] **Step 1: Write failing in-memory conformance and Rust-schema fixture tests**

```go
func TestSQLiteConformance(t *testing.T) {
    storetest.Run(t, func(ctx context.Context, t *testing.T) prolly.RemoteStore {
        s, err := sqlite.Open("file:"+url.PathEscape(t.Name())+"?mode=memory&cache=shared", sqlite.Options{})
        if err != nil { t.Fatal(err) }
        if err := s.InitializeSchema(ctx); err != nil { t.Fatal(err) }
        t.Cleanup(func() { _ = s.Close() })
        return s
    })
}
```

- [ ] **Step 2: Verify red**

Run: `(cd bindings/go/stores/sqlite && go test ./...)`

Expected: package API is undefined.

- [ ] **Step 3: Implement schema and transactional operations**

Use `modernc.org/sqlite` and the exact Rust tables/column encodings. Use SQL transactions for batch nodes, nodes+hint, root CAS, and strict commit; sort scans with `ORDER BY` on raw BLOB keys. Descriptor provider is `sqlite`, schema version `1`, all scan/hint/CAS/transaction capabilities true, with no claimed provider limits.

- [ ] **Step 4: Verify conformance, race, and Rust-written fixture reads**

Run: `(cd bindings/go/stores/sqlite && go test -race ./...)`

Expected: conformance passes and the adapter reads a database produced by `prolly-store-sqlite` without migration.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/sqlite
git commit -m "feat(go): add SQLite remote store"
```

### Task 7: PostgreSQL provider vertical slice

**Files:**
- Create: `bindings/go/stores/postgres/{go.mod,postgres.go,schema.go,postgres_test.go,README.md}`

**Interfaces:**
- Consumes: `*pgxpool.Pool`.
- Produces: `postgres.New(*pgxpool.Pool, Options)` and `InitializeSchema(context.Context)`.

- [ ] **Step 1: Add a buildable failing conformance test using `PROLLY_POSTGRES_URL`**

```go
func TestPostgresConformance(t *testing.T) {
    dsn := os.Getenv("PROLLY_POSTGRES_URL")
    if dsn == "" { t.Skip("PROLLY_POSTGRES_URL is not set") }
    pool, err := pgxpool.New(context.Background(), dsn); if err != nil { t.Fatal(err) }
    s := postgres.New(pool, postgres.Options{TablePrefix: testPrefix(t)})
    if err := s.InitializeSchema(context.Background()); err != nil { t.Fatal(err) }
    storetest.RunWithStore(t, s)
}
```

- [ ] **Step 2: Run against Compose and verify red**

Run: `docker compose -f docker-compose.store-services.yml up -d postgres && PROLLY_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly?sslmode=disable go test ./bindings/go/stores/postgres/...`

Expected: package API is undefined.

- [ ] **Step 3: Implement with pgx/v5**

Use `BYTEA` keys/values, `INSERT ... ON CONFLICT`, ordered scans, a single `pgx.Tx` for atomic methods, and `SELECT ... FOR UPDATE` for root CAS/strict commit. Match the Rust PostgreSQL provider’s table and manifest bytes exactly. Advertise native batch reads/writes, scans, hints, CAS, transactions, and a configurable positive read parallelism.

- [ ] **Step 4: Verify green and Rust interop fixture**

Run: `PROLLY_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly?sslmode=disable go test -race ./bindings/go/stores/postgres/...`

Expected: conformance and Rust-written/read-back fixture tests pass.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/postgres
git commit -m "feat(go): add PostgreSQL remote store"
```

### Task 8: MySQL provider vertical slice

**Files:**
- Create: `bindings/go/stores/mysql/{go.mod,mysql.go,schema.go,mysql_test.go,README.md}`

**Interfaces:**
- Consumes: `*sql.DB` configured with `go-sql-driver/mysql`.
- Produces: `mysql.New(*sql.DB, Options)` and `InitializeSchema(context.Context)`.

- [ ] **Step 1: Add the environment-gated conformance test**

Use `PROLLY_MYSQL_DSN=prolly:prolly@tcp(127.0.0.1:53306)/prolly?parseTime=true` and `storetest.RunWithStore` as in Task 7.

- [ ] **Step 2: Verify red against Compose**

Run: `docker compose -f docker-compose.store-services.yml up -d mysql && PROLLY_MYSQL_DSN='prolly:prolly@tcp(127.0.0.1:53306)/prolly?parseTime=true' go test ./bindings/go/stores/mysql/...`

Expected: package API is undefined.

- [ ] **Step 3: Implement exact Rust schema and transactions**

Use `VARBINARY`/`LONGBLOB`, `INSERT ... ON DUPLICATE KEY UPDATE`, binary collations, explicit SQL transactions, and `SELECT ... FOR UPDATE` for CAS/commit. Reject identifiers outside `[A-Za-z0-9_]+` before constructing DDL. Advertise the same guarantees as PostgreSQL.

- [ ] **Step 4: Verify green**

Run: `PROLLY_MYSQL_DSN='prolly:prolly@tcp(127.0.0.1:53306)/prolly?parseTime=true' go test -race ./bindings/go/stores/mysql/...`

Expected: conformance and Rust interoperability pass.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/mysql
git commit -m "feat(go): add MySQL remote store"
```

### Task 9: Redis provider vertical slice

**Files:**
- Create: `bindings/go/stores/redis/{go.mod,redis.go,scripts.go,redis_test.go,README.md}`

**Interfaces:**
- Consumes: `redis.UniversalClient` from `go-redis/v9`.
- Produces: `redisstore.New(redis.UniversalClient, Options)`.

- [ ] **Step 1: Add `PROLLY_REDIS_ADDR` conformance plus atomic-conflict test**

The conflict test concurrently commits two transactions with the same expected root and asserts exactly one applies and the losing transaction leaves no staged node keys.

- [ ] **Step 2: Verify red**

Run: `docker compose -f docker-compose.store-services.yml up -d redis && PROLLY_REDIS_ADDR=127.0.0.1:56379 go test ./bindings/go/stores/redis/...`

Expected: package API is undefined.

- [ ] **Step 3: Implement key layout and Lua atomics**

Use exact Rust prefixes `prolly:node:`, `prolly:hint:`, and `prolly:root:` plus an isolated test namespace. Use `MGET`/pipelining for ordered reads and Lua scripts for nodes+hint, root CAS, and strict transaction validation/application. Preserve raw bytes and deterministic scan sorting. Document that production primary-store use requires Redis persistence.

- [ ] **Step 4: Verify green**

Run: `PROLLY_REDIS_ADDR=127.0.0.1:56379 go test -race ./bindings/go/stores/redis/...`

Expected: conformance, atomicity, and Rust interop pass.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/redis
git commit -m "feat(go): add Redis remote store"
```

### Task 10: DynamoDB provider vertical slice

**Files:**
- Create: `bindings/go/stores/dynamodb/{go.mod,dynamodb.go,schema.go,dynamodb_test.go,README.md}`

**Interfaces:**
- Consumes: a narrow interface implemented by `*dynamodb.Client` from AWS SDK Go v2.
- Produces: `dynamodbstore.New(Client, Options)` and `CreateTable(context.Context)`.

- [ ] **Step 1: Add DynamoDB Local conformance and limit tests**

The test uses `PROLLY_DYNAMODB_ENDPOINT`, creates a unique table, verifies 101 reads split into 100+1, 26 writes split into 25+1 only for non-atomic batching, and rejects 101 strict transaction operations before SDK I/O.

- [ ] **Step 2: Verify red**

Run: `docker compose -f docker-compose.store-services.yml up -d dynamodb && PROLLY_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 go test ./bindings/go/stores/dynamodb/...`

Expected: package API is undefined.

- [ ] **Step 3: Implement the official SDK adapter**

Use the exact Rust single-table binary partition/sort-key encoding and document schema version 1. Use `BatchGetItem` (100), `BatchWriteItem` (25), conditional root writes, and `TransactWriteItems` (100) for strict transactions. Retry unprocessed batch items with context-aware bounded exponential backoff. Classify throttling and service-unavailable errors as retryable without retrying conditional conflicts.

- [ ] **Step 4: Verify green**

Run: `PROLLY_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 go test -race ./bindings/go/stores/dynamodb/...`

Expected: conformance, chunking, retry classification, and Rust interop pass.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/dynamodb
git commit -m "feat(go): add DynamoDB remote store"
```

### Task 11: Cosmos DB provider vertical slice

**Files:**
- Create: `bindings/go/stores/cosmosdb/{go.mod,cosmosdb.go,schema.go,cosmosdb_test.go,README.md}`

**Interfaces:**
- Consumes: a narrow interface wrapped around `*azcosmos.ContainerClient`.
- Produces: `cosmosdb.New(*azcosmos.ContainerClient, Options)` and explicit database/container creation helpers.

- [ ] **Step 1: Add mock-SDK contract tests and environment-gated live conformance**

Mock tests assert document fields `{id,kind,family,key,value}`, `/kind` partition keys, ETag propagation for root CAS, 100-operation transactional-batch preflight, base64 payload round trips, and Azure error classification. Live tests require `PROLLY_COSMOS_ENDPOINT`, `PROLLY_COSMOS_KEY`, and `PROLLY_COSMOS_DATABASE`.

- [ ] **Step 2: Verify red**

Run: `(cd bindings/go/stores/cosmosdb && go test ./...)`

Expected: package API is undefined.

- [ ] **Step 3: Implement with `azcosmos`**

Match the Rust document schema: deterministic ID, `kind`, `family`, hex key, base64 value, and `/kind` partitioning. Use transactional batches only when all affected documents share the required partition; otherwise advertise the capability false and return `unsupported` for strict transactions rather than weakening atomicity. Use `If-Match`/`If-None-Match` for root CAS and continuation tokens for scans.

- [ ] **Step 4: Verify unit and optional live tests**

Run: `(cd bindings/go/stores/cosmosdb && go test -race ./...)`

Expected: SDK contract tests always pass; live conformance passes when credentials are set.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/cosmosdb
git commit -m "feat(go): add Cosmos DB remote store"
```

### Task 12: Spanner provider vertical slice

**Files:**
- Create: `bindings/go/stores/spanner/{go.mod,spanner.go,schema.go,spanner_test.go,README.md}`

**Interfaces:**
- Consumes: `*spanner.Client`.
- Produces: `spannerstore.New(*spanner.Client, Options)` and explicit DDL statements/helper.

- [ ] **Step 1: Add emulator conformance and transaction-conflict tests**

Tests require `SPANNER_EMULATOR_HOST`, create a unique database, apply exact DDL, run shared conformance, and assert aborted transactions are classified retryable while root-condition conflicts are normal nonretryable results.

- [ ] **Step 2: Verify red**

Run: `SPANNER_EMULATOR_HOST=127.0.0.1:9010 go test ./bindings/go/stores/spanner/...`

Expected: package API is undefined (or emulator-gated test skips if the emulator is unavailable).

- [ ] **Step 3: Implement official Spanner adapter**

Match Rust tables `ProllyNodes`, `ProllyHints`, and `ProllyRoots` and their BYTES columns. Use read-only transactions for consistent multi-read, read/write transactions for batch, nodes+hint, root CAS, and strict commit, and key-range scans with deterministic ordering. Do not hard-code cloud credentials or create instances.

- [ ] **Step 4: Verify unit and emulator tests**

Run: `(cd bindings/go/stores/spanner && go test -race ./...)`

Expected: SDK contract tests pass; emulator conformance passes when configured.

- [ ] **Step 5: Commit**

```bash
git add bindings/go/stores/spanner
git commit -m "feat(go): add Spanner remote store"
```

### Task 13: Bidirectional Rust interoperability, compatibility manifest, and full gate

**Files:**
- Create: `conformance/store-protocol-v1/rust-interop/Cargo.toml`
- Create: `conformance/store-protocol-v1/rust-interop/src/main.rs`
- Create: `conformance/store-protocol-v1/compatibility.json`
- Create: `bindings/go/internal/verifycompat/main.go`
- Create: `scripts/test-go-stores.sh`
- Modify: `scripts/verify-store-services.sh`
- Modify: `docs/language-store-adapters-design.md`
- Modify: provider READMEs as test findings require

**Interfaces:**
- Consumes: all seven Go providers and all seven Rust provider crates.
- Produces: `rust-interop write|verify <provider>`, machine-readable Go support claims, and one repository verification command.

- [ ] **Step 1: Add a failing manifest verifier**

```sh
go run ./bindings/go/internal/verifycompat \
  conformance/store-protocol-v1/compatibility.json
```

The verifier requires exactly seven Go `supported` cells, package path, SDK module, protocol major 1, schema version 1, capability/limit values, minimum Go version, and evidence commands.

- [ ] **Step 2: Implement bidirectional fixture commands**

`rust-interop write` stores canonical nodes/hints/roots and a transaction through the Rust provider; Go verifies and mutates them. `rust-interop verify` then reads the Go mutations through Rust and validates CIDs/manifests. SQLite, PostgreSQL, MySQL, Redis, and DynamoDB run in CI/local Compose; Cosmos DB and Spanner run SDK-mock gates always and credential/emulator gates when available.

- [ ] **Step 3: Add the compatibility manifest**

```json
{
  "protocol_major": 1,
  "languages": {
    "go": {
      "sqlite": {"status":"supported","module":"build.crab/prolly-go/stores/sqlite","schema_version":1},
      "postgresql": {"status":"supported","module":"build.crab/prolly-go/stores/postgres","schema_version":1},
      "mysql": {"status":"supported","module":"build.crab/prolly-go/stores/mysql","schema_version":1},
      "redis": {"status":"supported","module":"build.crab/prolly-go/stores/redis","schema_version":1},
      "dynamodb": {"status":"supported","module":"build.crab/prolly-go/stores/dynamodb","schema_version":1},
      "cosmosdb": {"status":"supported","module":"build.crab/prolly-go/stores/cosmosdb","schema_version":1},
      "spanner": {"status":"supported","module":"build.crab/prolly-go/stores/spanner","schema_version":1}
    }
  }
}
```

Fill each entry with the exact SDK version and tested capability/limit record; the verifier rejects omissions or undocumented claims.

- [ ] **Step 4: Add the deterministic full verification script**

```bash
#!/usr/bin/env bash
set -euo pipefail
cargo test --features async-store --lib
cargo test --manifest-path bindings/uniffi/Cargo.toml --all-features
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
(cd bindings/go && go test -race ./...)
for module in sqlite postgres mysql redis dynamodb cosmosdb spanner; do
  (cd "bindings/go/stores/$module" && go test -race ./...)
done
```

Extend it to start/stop only the Compose services it owns, run Rust→Go→Rust fixtures, and preserve logs on failure.

- [ ] **Step 5: Run formatting, static checks, and the full gate**

Run:

```bash
cargo fmt --all -- --check
cargo clippy --features async-store --all-targets -- -D warnings
cargo clippy --manifest-path bindings/uniffi/Cargo.toml --all-targets --all-features -- -D warnings
find bindings/go -name '*.go' -type f -print0 | xargs -0 gofmt -w
./scripts/test-go-stores.sh
git diff --check
```

Expected: every mandatory local gate passes; live Cosmos/Spanner results are reported separately and never presented as executed when credentials/emulator are absent.

- [ ] **Step 6: Audit the support claim and commit**

Confirm every advertised capability has an atomicity test, every limit has a boundary test, every provider has lifecycle/cancellation/error-classification tests, and core `go list -deps ./...` contains none of the seven provider SDKs.

```bash
git add conformance/store-protocol-v1 scripts/test-go-stores.sh scripts/verify-store-services.sh docs/language-store-adapters-design.md bindings/go/stores/*/README.md
git diff --cached --check
git commit -m "test(go): verify all remote store providers"
```

---

## Self-review results

- **Spec coverage:** The plan covers the Go row for all seven requested providers, the shared asynchronous protocol, SDK injection, package isolation, descriptor validation, structured errors, cancellation, lifecycle, limits, transactions, conformance, compatibility metadata, and bidirectional Rust physical compatibility. Browser-only stores, RocksDB, SlateDB, and HTTP are intentionally excluded by the approved scope.
- **Placeholder scan:** The plan contains no `TBD`, `TODO`, “implement later,” or unnamed test steps. Provider-specific behavior, environment variables, commands, and expected results are explicit.
- **Type consistency:** `RemoteStore`, descriptor/operation records, `AsyncEngine`, `AsyncTransaction`, `storetest.Run`, and provider constructors retain the same names and ownership model throughout. Optional byte values require presence-aware ABI records even though the public Go API uses `nil` for missing.
- **Execution choice:** Inline execution is selected because this task is already active and repository instructions prohibit spawning subagents unless the user explicitly requests them.
