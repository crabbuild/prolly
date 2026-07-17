# Portable Language-Binding Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Deliver a hard-cutover, application-facing API with versioned-map, indexed-map, proximity-map, session, proof, maintenance, and async parity across Python, Go, Node/TypeScript, Kotlin, Java, Ruby, Swift, and browser-safe WASM while preserving retained-session and packed-page performance.

**Architecture:** The existing prolly-map crate remains authoritative. A modular Rust binding-domain facade owns portable records, stable errors, generation-checked handles, and a versioned packed ABI; each language package is a thin idiomatic adapter over that shared behavior. A rustdoc-JSON inventory and checked-in parity manifest make missing mappings and tests release-blocking.

**Tech Stack:** Rust 1.81 runtime code, pinned nightly rustdoc JSON for API inventory, UniFFI 0.31, C ABI/cgo, Node-API, wasm-bindgen, Python ctypes buffer adapters, JVM JNA/direct buffers, Swift C interop, Ruby FFI, JSON conformance fixtures, Maven, npm, SwiftPM, Go test, unittest, and minitest.

## Global Constraints

- This is a major hard cutover; do not preserve the current public binding API as aliases.
- Persisted node bytes, CIDs, canonical ordering, snapshot formats, proof formats, and Rust engine semantics must not change.
- Every public Rust export and reachable public operation must be classified in the parity manifest.
- Python, Go, Node/TypeScript, Kotlin, Java, Ruby, and Swift receive the complete native portable surface.
- WASM excludes only filesystem, SQLite, OS-thread blocking, and native-thread guarantees, with tested browser-safe replacements or unsupported errors.
- Hot reads in every binding must use retained sessions and bounded packed pages.
- Index filtering and source joins stay in Rust.
- Proximity candidate traversal, filtering, approximate search, exact reranking, proof construction, and tie-breaking stay in Rust.
- Owned values copy once; zero-copy terminology is reserved for callback-scoped or explicitly leased views.
- No foreign pointer survives a synchronous call and no scoped view crosses an await.
- Existing optimized point, multi-get, range, diff, and conflict workloads may not regress by more than 10 percent median latency or peak RSS on the documented host without explicit approval.
- Do not modify or stage the pre-existing untracked workspace artifacts.

---

## File Structure

### Shared contract and facade

- Create bindings/api/parity.json: generated, checked-in inventory plus classification, language symbols, test IDs, and performance tiers.
- Create scripts/binding_api_inventory.py: generate and verify manifest coverage from rustdoc JSON.
- Create bindings/uniffi/src/domain/mod.rs: domain module exports and shared store dispatch.
- Create bindings/uniffi/src/domain/error.rs: stable error code and detail mapping.
- Create bindings/uniffi/src/domain/handle.rs: type-tagged, generation-checked resource registry.
- Create bindings/uniffi/src/domain/page.rs: PRPG page kinds, builders, validation, and leases.
- Create bindings/uniffi/src/domain/versioned.rs: portable versioned-map facade.
- Create bindings/uniffi/src/domain/indexed.rs: portable secondary-index and indexed-map facade.
- Create bindings/uniffi/src/domain/proximity.rs: portable proximity-map facade.
- Modify bindings/uniffi/src/lib.rs: export the hard-cutover domain objects and delegate existing shared behavior.
- Modify bindings/uniffi/src/fast_abi.rs: route hot operations through the domain registry and add page kinds.

### Language adapters

- Create bindings/go/versioned.go, indexed.go, proximity.go, packed_page.go, and async.go.
- Create bindings/python/prolly/api.py, packed.py, versioned.py, indexed.py, and proximity.py.
- Create bindings/node/src/versioned.ts, indexed.ts, proximity.ts, and packed.ts; modify the native crate.
- Create Kotlin handwritten domain adapters under bindings/kotlin/src/main/kotlin/build/crab/prolly/api/.
- Create Java facades under bindings/java/src/main/java/build/crab/prolly/api/.
- Create bindings/ruby/lib/prolly/api.rb and packed_page.rb.
- Create Swift domain adapters under bindings/swift/Sources/ProllyAPI/.
- Create bindings/wasm/src/domain.rs, indexed.rs, proximity.rs, and page.rs.

### Conformance and release

- Create conformance/binding-versioned-fixtures.v1.json.
- Create conformance/binding-indexed-fixtures.v1.json.
- Create conformance/binding-proximity-fixtures.v1.json.
- Create per-language parity, lifecycle, async, and packed-page tests.
- Modify bindings/README.md and bindings/VERIFICATION.md.
- Create bindings/MIGRATION.md.

---

### Task 1: Machine-Checked Public API Contract

**Files:**
- Create: scripts/binding_api_inventory.py
- Create: bindings/api/parity.json
- Create: bindings/api/README.md
- Modify: bindings/VERIFICATION.md
- Test: scripts/tests/test_binding_api_inventory.py

**Interfaces:**
- Consumes: nightly rustdoc JSON at the target directory reported by cargo metadata.
- Produces: command scripts/binding_api_inventory.py generate, command scripts/binding_api_inventory.py check, stable operation IDs, and strict release validation.

- [ ] **Step 1: Write the failing inventory tests**

~~~python
class InventoryTests(unittest.TestCase):
    def test_missing_rust_symbol_fails(self):
        result = check_manifest(
            rust_items={"prolly::VersionedMap", "prolly::VersionedMap::head"},
            manifest={"operations": [{"rust": "prolly::VersionedMap"}]},
            release=False,
        )
        self.assertEqual(result.missing, ("prolly::VersionedMap::head",))

    def test_release_requires_all_language_symbols_and_tests(self):
        result = check_manifest(
            rust_items={"prolly::VersionedMap::head"},
            manifest={"operations": [{
                "rust": "prolly::VersionedMap::head",
                "classification": "portable",
                "status": "planned",
                "languages": {},
                "tests": [],
            }]},
            release=True,
        )
        self.assertFalse(result.ok)
~~~

- [ ] **Step 2: Run the tests and verify the missing module failure**

Run: python3 -m unittest scripts.tests.test_binding_api_inventory -v

Expected: FAIL because scripts.binding_api_inventory does not exist.

- [ ] **Step 3: Implement rustdoc extraction, generation, and checking**

~~~python
LANGUAGES = ("python", "go", "node", "kotlin", "java", "ruby", "swift", "wasm")
CLASSIFICATIONS = ("portable", "idiomatic", "platform-excluded", "rust-language-only")

@dataclass(frozen=True)
class CheckResult:
    missing: tuple[str, ...]
    stale: tuple[str, ...]
    incomplete: tuple[str, ...]

    @property
    def ok(self) -> bool:
        return not (self.missing or self.stale or self.incomplete)

def check_manifest(rust_items: set[str], manifest: dict, release: bool) -> CheckResult:
    operations = {entry["rust"]: entry for entry in manifest["operations"]}
    missing = tuple(sorted(rust_items - operations.keys()))
    stale = tuple(sorted(operations.keys() - rust_items))
    incomplete = []
    for name in sorted(rust_items & operations.keys()):
        entry = operations[name]
        if entry.get("classification") not in CLASSIFICATIONS:
            incomplete.append(name)
            continue
        if release and entry["classification"] in {"portable", "idiomatic"}:
            if entry.get("status") != "implemented":
                incomplete.append(name)
            elif set(entry.get("languages", {})) != set(LANGUAGES):
                incomplete.append(name)
            elif not entry.get("tests"):
                incomplete.append(name)
    return CheckResult(missing, stale, tuple(incomplete))
~~~

The extractor walks crate-local public rustdoc paths, public root re-exports,
struct/enum/trait items, and public associated functions from each item impl.
Generation preserves reviewed manifest entries and adds newly discovered
symbols with status planned. Check prints each missing, stale, or incomplete
operation and exits nonzero.

- [ ] **Step 4: Generate the complete current inventory and verify inventory mode**

Run:

~~~sh
cargo +nightly rustdoc --lib -- -Z unstable-options --output-format json
python3 scripts/binding_api_inventory.py generate
python3 scripts/binding_api_inventory.py check
python3 -m unittest scripts.tests.test_binding_api_inventory -v
~~~

Expected: manifest contains every current public item; inventory check and unit tests PASS. The strict --release check reports planned entries until later tasks mark them implemented.

- [ ] **Step 5: Commit**

~~~sh
git add scripts/binding_api_inventory.py scripts/tests/test_binding_api_inventory.py bindings/api/parity.json bindings/api/README.md bindings/VERIFICATION.md
git commit -m "feat(bindings): add machine-checked parity contract"
~~~

### Task 2: Stable Errors, Handles, and Packed Pages

**Files:**
- Create: bindings/uniffi/src/domain/mod.rs
- Create: bindings/uniffi/src/domain/error.rs
- Create: bindings/uniffi/src/domain/handle.rs
- Create: bindings/uniffi/src/domain/page.rs
- Modify: bindings/uniffi/src/lib.rs
- Modify: bindings/uniffi/src/fast_abi.rs
- Test: unit tests in each new module

**Interfaces:**
- Produces: ErrorCode, BindingError, HandleKind, HandleRegistry, ResourceHandle, PackedPageKind, PackedPageLease, PageLimits, and validated PRPG v2 pages.
- Consumes: existing PRPG v1 entry/get-many pages and ProllyBindingError.

- [ ] **Step 1: Write failing unit tests for stale handles and malformed pages**

~~~rust
#[test]
fn stale_generation_is_rejected_after_slot_reuse() {
    let registry = HandleRegistry::new();
    let first = registry.insert(HandleKind::ReadSession, String::from("one"));
    registry.close(first).unwrap();
    let second = registry.insert(HandleKind::ReadSession, String::from("two"));
    assert_ne!(first.generation(), second.generation());
    assert_eq!(registry.get::<String>(first).unwrap_err().code, ErrorCode::InvalidHandle);
}

#[test]
fn page_validator_rejects_out_of_arena_neighbor_payload() {
    let bytes = malformed_neighbor_page_with_payload_offset(u32::MAX);
    let error = PackedPage::parse(&bytes, PageLimits::default()).unwrap_err();
    assert_eq!(error.code, ErrorCode::MalformedTransport);
}
~~~

- [ ] **Step 2: Verify the tests fail because the domain modules do not exist**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml domain:: --target-dir target

Expected: FAIL with unresolved domain module/type errors.

- [ ] **Step 3: Implement the stable core interfaces**

~~~rust
#[repr(u32)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorCode {
    InvalidArgument = 1,
    InvalidHandle = 2,
    Closed = 3,
    Conflict = 4,
    StaleIndex = 5,
    InvalidProximity = 6,
    Verification = 7,
    Cancelled = 8,
    DeadlineExceeded = 9,
    Unsupported = 10,
    MalformedTransport = 11,
    Callback = 12,
    Internal = 255,
}

#[repr(u16)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PackedPageKind {
    Entry = 1,
    GetMany = 2,
    Diff = 3,
    Conflict = 4,
    IndexMatch = 5,
    JoinedIndexRecord = 6,
    ProximityNeighbor = 7,
}

pub const PAGE_MAGIC: [u8; 4] = *b"PRPG";
pub const PAGE_VERSION: u16 = 2;
pub const PAGE_HEADER_BYTES: usize = 28;
~~~

Handle lookup clones an Arc under the registry lock and releases the lock
before executing engine work or host callbacks. Close is idempotent, generation
checked, and prevents new child work.

- [ ] **Step 4: Run focused tests, facade tests, and ABI regression tests**

Run:

~~~sh
cargo test --manifest-path bindings/uniffi/Cargo.toml domain:: --target-dir target
cargo test --manifest-path bindings/uniffi/Cargo.toml fast_abi --target-dir target
~~~

Expected: PASS with v1 compatibility decode tests and v2 round-trip/fuzz-seed tests.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain bindings/uniffi/src/lib.rs bindings/uniffi/src/fast_abi.rs
git commit -m "feat(bindings): add hardened transport domain"
~~~

### Task 3: Portable Versioned-Map Facade

**Files:**
- Create: bindings/uniffi/src/domain/versioned.rs
- Modify: bindings/uniffi/src/domain/mod.rs
- Modify: bindings/uniffi/src/lib.rs
- Create: conformance/binding-versioned-fixtures.v1.json
- Test: bindings/uniffi/src/domain/versioned.rs

**Interfaces:**
- Consumes: HandleRegistry, BindingError, PackedPageLease, existing store dispatch, prolly::VersionedMap.
- Produces: BindingVersionedMap, BindingMapSnapshot, BindingMapComparison, BindingMapMerge, BindingMapSubscription, BindingVersionedTransaction, version/value records, backup and pruning records.

- [ ] **Step 1: Write failing facade tests for lifecycle, history, conflict, and retained reads**

~~~rust
#[test]
fn versioned_map_round_trip_uses_snapshot_session() {
    let engine = BindingEngine::memory(ConfigRecord::default()).unwrap();
    let map = engine.versioned_map(b"users".to_vec()).unwrap();
    let initial = map.initialize().unwrap();
    let next = map.put(b"alice".to_vec(), b"admin".to_vec()).unwrap();
    assert_ne!(initial.id, next.id);
    let snapshot = map.snapshot().unwrap().unwrap();
    let session = snapshot.read().unwrap();
    assert_eq!(session.get(b"alice".to_vec()).unwrap(), Some(b"admin".to_vec()));
    assert_eq!(map.snapshot_at(initial.id).unwrap().unwrap().get(b"alice".to_vec()).unwrap(), None);
}
~~~

- [ ] **Step 2: Verify the missing facade failure**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml versioned_map_round_trip_uses_snapshot_session --target-dir target

Expected: FAIL because BindingEngine::versioned_map and BindingVersionedMap do not exist.

- [ ] **Step 3: Implement complete portable versioned-map records and methods**

~~~rust
#[derive(Clone, Debug, uniffi::Record)]
pub struct MapVersionRecord {
    pub id: Vec<u8>,
    pub root_cid: Vec<u8>,
    pub parent_ids: Vec<Vec<u8>>,
    pub created_at_millis: i64,
}

#[derive(uniffi::Object)]
pub struct BindingVersionedMap {
    engine: Arc<BindingEngine>,
    id: Vec<u8>,
}

#[uniffi::export]
impl BindingVersionedMap {
    pub fn initialize(&self) -> Result<MapVersionRecord, BindingError>;
    pub fn head(&self) -> Result<Option<MapVersionRecord>, BindingError>;
    pub fn snapshot(&self) -> Result<Option<Arc<BindingMapSnapshot>>, BindingError>;
    pub fn snapshot_at(&self, id: Vec<u8>) -> Result<Option<Arc<BindingMapSnapshot>>, BindingError>;
    pub fn apply(&self, mutations: Vec<MutationRecord>) -> Result<MapVersionRecord, BindingError>;
    pub fn apply_if(&self, expected: Option<Vec<u8>>, mutations: Vec<MutationRecord>) -> Result<MapUpdateRecord, BindingError>;
    pub fn versions(&self) -> Result<Vec<MapVersionRecord>, BindingError>;
    pub fn backup(&self) -> Result<Vec<u8>, BindingError>;
    pub fn restore_backup(&self, bytes: Vec<u8>) -> Result<MapVersionRecord, BindingError>;
    pub fn prune_versions(&self, keep_latest: u64) -> Result<VersionPruneRecord, BindingError>;
}
~~~

Implement every VersionedMap, MapSnapshot, MapComparison, MapMerge,
MapChangeSubscription, backup, catalog verification, GC, blob-GC, and
multi-map transaction operation from the manifest. Borrowed Rust methods map
to scoped visitor methods; generic typed maps remain host codec wrappers.

- [ ] **Step 4: Generate fixtures and run Rust facade coverage**

Run:

~~~sh
cargo test --manifest-path bindings/uniffi/Cargo.toml domain::versioned --target-dir target
cargo test --test versioned_map
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS; versioned family entries have facade symbols and Rust test IDs.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain bindings/uniffi/src/lib.rs conformance/binding-versioned-fixtures.v1.json bindings/api/parity.json
git commit -m "feat(bindings): expose portable versioned maps"
~~~

### Task 4: Portable Indexed-Map Facade

**Files:**
- Create: bindings/uniffi/src/domain/indexed.rs
- Modify: bindings/uniffi/src/domain/mod.rs
- Modify: bindings/uniffi/src/lib.rs
- Create: conformance/binding-indexed-fixtures.v1.json
- Test: bindings/uniffi/src/domain/indexed.rs

**Interfaces:**
- Produces: SecondaryIndexExtractorCallback, index definition/limit/projection records, BindingIndexRegistry, BindingIndexedMap, BindingIndexedSnapshot, BindingSecondaryIndexSnapshot, build/health/verification/retention/bundle records.
- Consumes: prolly::SecondaryIndexRegistry, IndexedMap, indexed snapshot bundle APIs, shared engine and callbacks.

- [ ] **Step 1: Write failing index lifecycle and native-join tests**

~~~rust
#[test]
fn indexed_snapshot_queries_and_joins_source_inside_rust() {
    let (engine, registry) = fixture_engine_and_registry();
    let map = engine.indexed_map(b"users".to_vec(), registry).unwrap();
    map.put(b"u1".to_vec(), br#"{"team":"red","name":"Ada"}"#.to_vec()).unwrap();
    let snapshot = map.snapshot().unwrap();
    let matches = snapshot.index(b"by_team".to_vec()).unwrap().records(b"red".to_vec()).unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].primary_key, b"u1");
    assert_eq!(matches[0].value, br#"{"team":"red","name":"Ada"}"#);
}
~~~

- [ ] **Step 2: Verify the missing indexed facade failure**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml indexed_snapshot_queries_and_joins_source_inside_rust --target-dir target

Expected: FAIL because indexed_map and extractor callback types do not exist.

- [ ] **Step 3: Implement all indexed domain operations**

~~~rust
#[uniffi::export(callback_interface)]
pub trait SecondaryIndexExtractorCallback: Send + Sync {
    fn extract(&self, primary_key: Vec<u8>, value: Vec<u8>) -> Result<Vec<IndexEntryRecord>, BindingError>;
}

#[derive(uniffi::Object)]
pub struct BindingSecondaryIndexSnapshot {
    snapshot: IndexedSnapshotOwner,
    name: Vec<u8>,
}

#[uniffi::export]
impl BindingSecondaryIndexSnapshot {
    pub fn exact(&self, term: Vec<u8>) -> Result<Vec<IndexMatchRecord>, BindingError>;
    pub fn prefix(&self, prefix: Vec<u8>) -> Result<Vec<IndexMatchRecord>, BindingError>;
    pub fn range(&self, start: Vec<u8>, end: Option<Vec<u8>>, direction: IndexDirectionRecord) -> Result<Vec<IndexMatchRecord>, BindingError>;
    pub fn records(&self, term: Vec<u8>) -> Result<Vec<IndexedSourceRecord>, BindingError>;
    pub fn open_query(&self, query: IndexQueryRecord) -> Result<Arc<BindingIndexCursor>, BindingError>;
}
~~~

Implement build/rebuild, verify/verify-all, repair, replacement, deactivation,
health, metrics, apply/apply-if, snapshots by version and ID, all forward and
reverse page forms, retention, GC, catalog controls, and bundle import/export.
Callbacks receive owned input and execute without native locks held.

- [ ] **Step 4: Run indexed core, facade, fixture, and callback tests**

Run:

~~~sh
cargo test --test secondary_index
cargo test --manifest-path bindings/uniffi/Cargo.toml domain::indexed --target-dir target
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS with direct-Rust and facade results matching roots, versions, fingerprints, ordering, health, and bundle bytes.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain bindings/uniffi/src/lib.rs conformance/binding-indexed-fixtures.v1.json bindings/api/parity.json
git commit -m "feat(bindings): expose indexed maps"
~~~

### Task 5: Packed Index Query and Join Transport

**Files:**
- Modify: bindings/uniffi/src/domain/page.rs
- Modify: bindings/uniffi/src/domain/indexed.rs
- Modify: bindings/uniffi/src/fast_abi.rs
- Test: packed index tests in page.rs and fast_abi.rs

**Interfaces:**
- Produces: PRPG IndexMatch and JoinedIndexRecord layouts, index cursor open/next/close ABI, scoped visitors.
- Consumes: BindingIndexCursor and generation-checked registry.

- [ ] **Step 1: Write failing binary-layout and early-stop tests**

~~~rust
#[test]
fn joined_index_page_round_trips_binary_fields() {
    let expected = JoinedIndexRecordRef {
        term: b"red",
        primary_key: b"u1",
        projection: Some(b"Ada"),
        value: b"{\"team\":\"red\"}",
    };
    let page = encode_joined_index_page([expected], true).unwrap();
    assert_eq!(decode_joined_index_page(&page).unwrap().collect::<Vec<_>>(), vec![expected]);
}
~~~

- [ ] **Step 2: Verify the missing page-kind failure**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml joined_index_page_round_trips_binary_fields --target-dir target

Expected: FAIL because index page builders and decoders do not exist.

- [ ] **Step 3: Implement page layouts and ABI functions**

~~~rust
#[no_mangle]
pub extern "C" fn prolly_index_cursor_next_page(
    cursor: u64,
    max_records: u32,
    max_arena_bytes: u64,
) -> FastPageResult;

#[no_mangle]
pub extern "C" fn prolly_index_cursor_close(cursor: u64) -> i32;
~~~

IndexMatch fixed records contain term, primary key, optional projection, and
cursor offsets. Joined records additionally contain source value offsets.
Offset/length validation occurs before any view is exposed.

- [ ] **Step 4: Run ABI, fuzz-seed, and bounded-memory tests**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml index_page --target-dir target

Expected: PASS; counters show one cursor, bounded pages, no host callback while locks are held, and balanced leases.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain/page.rs bindings/uniffi/src/domain/indexed.rs bindings/uniffi/src/fast_abi.rs
git commit -m "feat(bindings): add packed index query transport"
~~~

### Task 6: Portable Proximity-Map Facade

**Files:**
- Create: bindings/uniffi/src/domain/proximity.rs
- Modify: bindings/uniffi/src/domain/mod.rs
- Modify: bindings/uniffi/src/lib.rs
- Create: conformance/binding-proximity-fixtures.v1.json
- Test: bindings/uniffi/src/domain/proximity.rs

**Interfaces:**
- Produces: all proximity config/search/filter/budget/plan/result/proof records, BindingProximityMap, BindingProximityReadSession, accelerator objects and catalog.
- Consumes: prolly proximity maps, HNSW, PQ, composite accelerators, and shared store dispatch.

- [ ] **Step 1: Write failing deterministic search and proof tests**

~~~rust
#[test]
fn proximity_search_reranks_and_ties_by_key() {
    let map = fixture_proximity_map();
    let result = map.search(SearchRequestRecord {
        vector: vec![1.0, 0.0],
        top_k: 2,
        backend: SearchBackendRecord::Auto,
        include_values: true,
        ..SearchRequestRecord::exact()
    }).unwrap();
    assert_eq!(result.neighbors.iter().map(|n| n.key.as_slice()).collect::<Vec<_>>(), vec![b"a", b"b"]);
    assert_eq!(result.completion, SearchCompletionRecord::Exact);
}
~~~

- [ ] **Step 2: Verify the missing proximity facade failure**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml proximity_search_reranks_and_ties_by_key --target-dir target

Expected: FAIL because BindingProximityMap and portable search records do not exist.

- [ ] **Step 3: Implement all proximity records and objects**

~~~rust
#[derive(Clone, Debug, uniffi::Record)]
pub struct SearchRequestRecord {
    pub vector: Vec<f32>,
    pub top_k: u64,
    pub filter: Option<ProximityFilterRecord>,
    pub policy: SearchPolicyRecord,
    pub backend: SearchBackendRecord,
    pub budget: SearchBudgetRecord,
    pub include_values: bool,
    pub include_proof: bool,
}

#[derive(uniffi::Object)]
pub struct BindingProximityMap {
    inner: ProximityMapOwner,
}

#[uniffi::export]
impl BindingProximityMap {
    pub fn build(engine: Arc<BindingEngine>, config: ProximityConfigRecord, records: Vec<ProximityRecord>) -> Result<Arc<Self>, BindingError>;
    pub fn load(engine: Arc<BindingEngine>, descriptor_cid: Vec<u8>) -> Result<Arc<Self>, BindingError>;
    pub fn read(&self) -> Result<Arc<BindingProximityReadSession>, BindingError>;
    pub fn mutate_batch(&self, mutations: Vec<ProximityMutationRecord>) -> Result<ProximityMutationStatsRecord, BindingError>;
    pub fn search(&self, request: SearchRequestRecord) -> Result<SearchResultRecord, BindingError>;
    pub fn verify(&self) -> Result<ProximityVerificationRecord, BindingError>;
}
~~~

Implement scalar/SIMD kernel selection, exact/fixed/adaptive policies, all
filters and budgets, search plans/stats/completion, membership/structural/search
proofs, accelerator catalog/set, HNSW, PQ, composite build/rebuild, cache
control, async control records, and cancellation.

- [ ] **Step 4: Run proximity core and facade suites**

Run:

~~~sh
cargo test --test proximity_map
cargo test --test proximity_hnsw
cargo test --test proximity_quantization
cargo test --manifest-path bindings/uniffi/Cargo.toml domain::proximity --target-dir target
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS with exact roots, descriptor CIDs, neighbor ordering, completion, stats, and proof bytes matching direct Rust.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain bindings/uniffi/src/lib.rs conformance/binding-proximity-fixtures.v1.json bindings/api/parity.json
git commit -m "feat(bindings): expose proximity maps"
~~~

### Task 7: Packed Proximity Neighbor Transport

**Files:**
- Modify: bindings/uniffi/src/domain/page.rs
- Modify: bindings/uniffi/src/domain/proximity.rs
- Modify: bindings/uniffi/src/fast_abi.rs
- Test: proximity page and lifecycle tests

**Interfaces:**
- Produces: packed query-vector request, ProximityNeighbor PRPG pages, search cursor open/next/close, payload opt-in.
- Consumes: retained BindingProximityReadSession and in-Rust SearchResult.

- [ ] **Step 1: Write failing neighbor-page and payload-opt-in tests**

~~~rust
#[test]
fn neighbor_page_omits_payload_when_not_requested() {
    let page = search_fixture_page(false);
    let neighbor = decode_neighbor_page(&page).unwrap().next().unwrap();
    assert_eq!(neighbor.key, b"a");
    assert_eq!(neighbor.distance, 0.25);
    assert_eq!(neighbor.value, None);
}
~~~

- [ ] **Step 2: Verify the missing packed-neighbor failure**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml neighbor_page_omits_payload_when_not_requested --target-dir target

Expected: FAIL because neighbor page encoding does not exist.

- [ ] **Step 3: Implement the packed search ABI**

~~~rust
#[no_mangle]
pub extern "C" fn prolly_proximity_search_open(
    session: u64,
    request_ptr: *const u8,
    request_len: u64,
) -> FastScanOpenResult;

#[no_mangle]
pub extern "C" fn prolly_proximity_search_next_page(
    search: u64,
    max_records: u32,
    max_arena_bytes: u64,
) -> FastPageResult;
~~~

The fixed neighbor record contains key offset/length, f32 distance bits,
optional value offset/length, rank, and proof-event offset/length. Rust owns the
query after open and performs all candidate processing and reranking before
pages are exposed.

- [ ] **Step 4: Run search transport, cancellation, and bounded-memory tests**

Run: cargo test --manifest-path bindings/uniffi/Cargo.toml proximity_page --target-dir target

Expected: PASS; candidate vectors never appear in page arenas, top-K bounds page count, and all cancellation paths balance handles.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/uniffi/src/domain/page.rs bindings/uniffi/src/domain/proximity.rs bindings/uniffi/src/fast_abi.rs
git commit -m "feat(bindings): add packed proximity transport"
~~~

### Task 8: UniFFI Languages — Python, Kotlin, Ruby, and Swift

**Files:**
- Modify: bindings/python/prolly/__init__.py
- Create: bindings/python/prolly/api.py
- Create: bindings/python/prolly/packed.py
- Create: bindings/python/tests/test_portable_parity.py
- Create: bindings/kotlin/src/main/kotlin/build/crab/prolly/api/PortableApi.kt
- Create: bindings/kotlin/src/test/kotlin/build/crab/prolly/PortableParityTest.kt
- Create: bindings/ruby/lib/prolly/api.rb
- Create: bindings/ruby/lib/prolly/packed_page.rb
- Create: bindings/ruby/test/portable_parity_test.rb
- Create: bindings/swift/Sources/ProllyAPI/PortableAPI.swift
- Create: bindings/swift/Tests/ProllyTests/PortableParityTests.swift
- Regenerate: checked-in UniFFI glue for Python, Kotlin, Ruby, and Swift

**Interfaces:**
- Consumes: generated shared facade plus packed C ABI.
- Produces: idiomatic context-managed/AutoCloseable/scoped Swift and Ruby block APIs, typed codec wrappers, async adapters, and scoped views.

- [ ] **Step 1: Add failing shared-fixture tests in all four languages**

~~~python
def test_indexed_and_proximity_hot_reads_use_scoped_pages(self):
    with Engine.memory() as engine:
        indexed = indexed_fixture(engine)
        self.assertEqual([b"u1"], [row.primary_key for row in indexed.snapshot().index(b"by_team").exact(b"red")])
        proximity = proximity_fixture(engine)
        with proximity.read() as session:
            self.assertEqual(b"a", session.search_view([1.0, 0.0], 1, lambda rows: rows[0].key))
~~~

- [ ] **Step 2: Run each focused suite and observe missing public API failures**

Run:

~~~sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" PYTHONPATH=bindings/python python3 -m unittest bindings.python.tests.test_portable_parity -v
mvn -f bindings/kotlin/pom.xml -Dtest=PortableParityTest test
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" BUNDLE_GEMFILE=bindings/ruby/Gemfile bundle exec ruby -Ibindings/ruby/lib bindings/ruby/test/portable_parity_test.rb
DYLD_LIBRARY_PATH="$PWD/target/debug" swift test --package-path bindings/swift --filter PortableParityTests
~~~

Expected: each fails because the hard-cutover adapters do not exist.

- [ ] **Step 3: Regenerate glue and implement idiomatic adapters**

Python uses bytes for owned values, memoryview for scoped page fields, context
managers, async generators, and task cancellation. Kotlin uses ByteArray,
direct ByteBuffer views, Sequence, suspend functions, and use. Ruby uses binary
String, Enumerable, scoped yield views, and Future. Swift uses Data,
withUnsafeBytes-style scoped views, Sequence/AsyncSequence, and Task
cancellation. Typed maps wrap host codecs and never call codecs while native
locks are held.

- [ ] **Step 4: Run full four-language suites and manifest checks**

Run:

~~~sh
python3 -m unittest discover -s bindings/python/tests
mvn -f bindings/kotlin/pom.xml test
BUNDLE_GEMFILE=bindings/ruby/Gemfile bundle exec ruby -Ibindings/ruby/lib bindings/ruby/test/prolly_smoke_test.rb
DYLD_LIBRARY_PATH="$PWD/target/debug" swift test --package-path bindings/swift
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS with conformance, close/use-after-close, async cancellation, callback reentry, and page lifetime coverage.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/python bindings/kotlin bindings/ruby bindings/swift bindings/api/parity.json
git commit -m "feat(bindings): add portable UniFFI language APIs"
~~~

### Task 9: Go Hard-Cutover Adapter

**Files:**
- Create: bindings/go/versioned.go
- Create: bindings/go/indexed.go
- Create: bindings/go/proximity.go
- Create: bindings/go/packed_page.go
- Create: bindings/go/async.go
- Modify: bindings/go/prolly.go
- Test: bindings/go/portable_parity_test.go

**Interfaces:**
- Consumes: C ABI facade and packed pages.
- Produces: Engine, VersionedMap, IndexedMap, ProximityMap, ReadSession, context methods, scoped views, and Close semantics.

- [ ] **Step 1: Write failing Go parity and race tests**

~~~go
func TestIndexedAndProximityParity(t *testing.T) {
    engine := NewMemoryEngine(DefaultConfig())
    t.Cleanup(func() {
        if err := engine.Close(); err != nil {
            t.Errorf("close engine: %v", err)
        }
    })
    indexed := indexedFixture(t, engine)
    rows, err := indexed.Snapshot().Index([]byte("by_team")).Exact([]byte("red"))
    if err != nil {
        t.Fatal(err)
    }
    if !bytes.Equal(rows[0].PrimaryKey, []byte("u1")) {
        t.Fatalf("primary key = %q", rows[0].PrimaryKey)
    }
    proximity := proximityFixture(t, engine)
    result, err := proximity.Read().Search(context.Background(), ExactSearch([]float32{1, 0}, 1))
    if err != nil {
        t.Fatal(err)
    }
    if !bytes.Equal(result.Neighbors[0].Key, []byte("a")) {
        t.Fatalf("neighbor key = %q", result.Neighbors[0].Key)
    }
}
~~~

- [ ] **Step 2: Verify missing Go API failures**

Run: (cd bindings/go && go test ./...)

Expected: FAIL with undefined NewMemoryEngine/IndexedMap/ProximityMap APIs.

- [ ] **Step 3: Implement the hard-cutover API and packed decoders**

~~~go
type PageLease struct {
    handle C.uint64_t
    bytes  unsafe.Pointer
    length uint64
    closed atomic.Bool
}

func (p *PageLease) Close() error
func (s *ReadSession) ScanView(ctx context.Context, bounds Bounds, visit func(EntryView) bool) (ScanOutcome, error)
func (s *IndexSession) QueryView(ctx context.Context, query IndexQuery, visit func(IndexMatchView) bool) (ScanOutcome, error)
func (s *ProximitySession) Search(ctx context.Context, request SearchRequest) (SearchResult, error)
~~~

Use runtime.KeepAlive for borrowed inputs, never retain Go pointers, copy async
inputs before goroutine scheduling, and close native cursors on context
cancellation.

- [ ] **Step 4: Run Go conformance, race, and benchmark smoke**

Run:

~~~sh
(cd bindings/go && go test ./...)
(cd bindings/go && go test -race ./...)
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS with balanced handle counters and current optimized read latency inside the 10 percent gate.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/go bindings/api/parity.json
git commit -m "feat(bindings): cut over Go to portable API"
~~~

### Task 10: Node and TypeScript Hard-Cutover Adapter

**Files:**
- Modify: bindings/node/native/src/lib.rs
- Create: bindings/node/src/versioned.ts
- Create: bindings/node/src/indexed.ts
- Create: bindings/node/src/proximity.ts
- Create: bindings/node/src/packed.ts
- Modify: bindings/node/src/index.ts
- Modify: bindings/node/index.d.ts
- Test: bindings/node/test/portable-parity.test.ts

**Interfaces:**
- Produces: Node-API native domain objects, Buffer/Uint8Array ownership, external leased pages, async iterables, promises, AbortSignal cancellation.

- [ ] **Step 1: Write failing TypeScript parity and abort tests**

~~~typescript
test("indexed and proximity operations stay in native sessions", async () => {
  using engine = Engine.memory();
  const indexed = await indexedFixture(engine);
  expect((await indexed.snapshot().index(bytes("by_team")).exact(bytes("red")))[0].primaryKey).toEqual(bytes("u1"));
  const proximity = await proximityFixture(engine);
  const result = await proximity.read().search({ vector: new Float32Array([1, 0]), topK: 1, policy: "exact" });
  expect(result.neighbors[0].key).toEqual(bytes("a"));
});
~~~

- [ ] **Step 2: Verify missing Node API failures**

Run: npm --prefix bindings/node test -- portable-parity

Expected: FAIL at TypeScript compilation because Engine/IndexedMap/ProximityMap are absent.

- [ ] **Step 3: Implement native objects and TypeScript adapters**

Node-API work items own async inputs, release external buffers exactly once,
use async iterables over bounded pages, map AbortSignal to native cancellation,
and preserve Buffer validity until lease disposal. Native callbacks execute
with no registry/session locks held.

- [ ] **Step 4: Run Node build, tests, and manifest check**

Run:

~~~sh
npm --prefix bindings/node run build:native
npm --prefix bindings/node test
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS including GC/finalizer leak protection, explicit disposal, abort, reentry, and malformed-page tests.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/node bindings/api/parity.json
git commit -m "feat(bindings): cut over Node to portable API"
~~~

### Task 11: Java Facade and Async Cancellation

**Files:**
- Create: bindings/java/src/main/java/build/crab/prolly/api/Engine.java
- Create: bindings/java/src/main/java/build/crab/prolly/api/VersionedMap.java
- Create: bindings/java/src/main/java/build/crab/prolly/api/IndexedMap.java
- Create: bindings/java/src/main/java/build/crab/prolly/api/ProximityMap.java
- Create: bindings/java/src/main/java/build/crab/prolly/api/PackedPage.java
- Test: bindings/java/src/test/java/build/crab/prolly/PortableParityTest.java

**Interfaces:**
- Consumes: shared generated Kotlin/JNA transport.
- Produces: Java records/builders, Iterable cursors, explicit Page objects, AutoCloseable resources, CompletableFuture async operations.

- [ ] **Step 1: Write failing Java parity and cancellation tests**

~~~java
@Test
void indexedAndProximityParity() throws Exception {
    try (Engine engine = Engine.memory()) {
        IndexedMap indexed = Fixtures.indexed(engine);
        assertArrayEquals(bytes("u1"), indexed.snapshot().index(bytes("by_team")).exact(bytes("red")).get(0).primaryKey());
        ProximityMap proximity = Fixtures.proximity(engine);
        assertArrayEquals(bytes("a"), proximity.read().search(SearchRequest.exact(new float[]{1, 0}, 1)).neighbors().get(0).key());
    }
}
~~~

- [ ] **Step 2: Verify missing Java facade failures**

Run: mvn -f bindings/pom.xml -pl java -am -Dtest=PortableParityTest test

Expected: FAIL because build.crab.prolly.api types do not exist.

- [ ] **Step 3: Implement Java ownership, iteration, and futures**

All native resources implement AutoCloseable. Synchronous cursors implement
Iterable without hiding checked native failures; pages expose direct read-only
ByteBuffer views. CompletableFuture cancellation closes temporary native
resources and completes with the stable Cancelled error.

- [ ] **Step 4: Run JVM aggregate tests**

Run:

~~~sh
mvn -f bindings/pom.xml test
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS for Kotlin and Java with direct-buffer lifetime and close races.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/java bindings/api/parity.json
git commit -m "feat(bindings): add portable Java API"
~~~

### Task 12: Browser-Safe WASM Hard Cutover

**Files:**
- Create: bindings/wasm/src/domain.rs
- Create: bindings/wasm/src/indexed.rs
- Create: bindings/wasm/src/proximity.rs
- Create: bindings/wasm/src/page.rs
- Modify: bindings/wasm/src/lib.rs
- Modify: bindings/wasm/src/index.ts
- Test: bindings/wasm/test/portable-parity.test.ts

**Interfaces:**
- Produces: browser memory/store protocols, versioned/indexed/proximity objects, guarded typed-array views, async iterables, AbortSignal support, explicit native-only unsupported errors.

- [ ] **Step 1: Write failing browser parity and exclusion tests**

~~~typescript
test("WASM exposes portable maps and explicit native exclusions", async () => {
  const engine = Engine.memory();
  expect(() => Engine.sqlite("db.sqlite")).toThrowError(/unsupported.*wasm/i);
  const indexed = await indexedFixture(engine);
  expect((await indexed.snapshot().index(bytes("by_team")).exact(bytes("red")))[0].primaryKey).toEqual(bytes("u1"));
  const proximity = await proximityFixture(engine);
  expect((await proximity.read().search({ vector: new Float32Array([1, 0]), topK: 1 })).neighbors[0].key).toEqual(bytes("a"));
});
~~~

- [ ] **Step 2: Verify missing WASM domain failures**

Run: npm --prefix bindings/wasm test -- portable-parity

Expected: FAIL because the domain exports do not exist.

- [ ] **Step 3: Implement browser-safe domain objects and guarded views**

WASM reuses Rust engine semantics directly, copies owned Uint8Array results
once, invalidates scoped views at callback return, prevents views from
surviving memory growth or await, and checks AbortSignal between bounded pages.
Filesystem and SQLite constructors throw stable Unsupported errors.

- [ ] **Step 4: Run WASM build and browser-safe tests**

Run:

~~~sh
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown --target-dir target
npm --prefix bindings/wasm run build:wasm
npm --prefix bindings/wasm test
python3 scripts/binding_api_inventory.py check
~~~

Expected: PASS including memory-growth invalidation, async cancellation, browser store, and explicit exclusion cases.

- [ ] **Step 5: Commit**

~~~sh
git add bindings/wasm bindings/api/parity.json
git commit -m "feat(bindings): cut over WASM to portable API"
~~~

### Task 13: Release Cutover, Documentation, and Full Gates

**Files:**
- Modify: bindings/README.md
- Modify: bindings/VERIFICATION.md
- Create: bindings/MIGRATION.md
- Modify: all binding README.md and COOKBOOK.md files
- Modify: package versions and generated declarations
- Remove: legacy public adapter files and exports superseded by the new API
- Create: scripts/run_binding_parity_gate.sh
- Create: scripts/run_binding_performance_gate.sh

**Interfaces:**
- Consumes: completed manifest and all language test suites.
- Produces: strict release gate, migration mapping, hard-cutover packages, benchmark report.

- [ ] **Step 1: Write failing release-gate tests**

~~~python
def test_release_manifest_has_no_planned_operations(self):
    result = subprocess.run(
        [sys.executable, "scripts/binding_api_inventory.py", "check", "--release"],
        text=True,
        capture_output=True,
    )
    self.assertEqual(result.returncode, 0, result.stdout + result.stderr)
~~~

- [ ] **Step 2: Run the release check and observe remaining mappings**

Run: python3 scripts/binding_api_inventory.py check --release

Expected: FAIL listing every operation whose language symbol, conformance test, documentation, or implemented status is incomplete.

- [ ] **Step 3: Complete the manifest, remove legacy exports, and write migration/docs**

The migration guide maps every removed public constructor and operation to the
new object family, documents explicit close patterns, async cancellation,
owned versus scoped reads, WASM exclusions, and major package versions.
Release scripts reject legacy exported names, generated build artifacts, stale
manifest entries, and missing docs/tests.

- [ ] **Step 4: Run fresh complete correctness and performance verification**

Run:

~~~sh
cargo test --all-features
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" PYTHONPATH=bindings/python python3 -m unittest discover -s bindings/python/tests
(cd bindings/go && go test -race ./...)
npm --prefix bindings/node run build:native
npm --prefix bindings/node test
mvn -f bindings/pom.xml test
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" BUNDLE_GEMFILE=bindings/ruby/Gemfile bundle exec ruby -Ibindings/ruby/lib bindings/ruby/test/prolly_smoke_test.rb
DYLD_LIBRARY_PATH="$PWD/target/debug" swift test --package-path bindings/swift
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown --target-dir target
npm --prefix bindings/wasm run build:wasm
npm --prefix bindings/wasm test
python3 scripts/binding_api_inventory.py check --release
scripts/run_binding_performance_gate.sh
git diff --check
~~~

Expected: all commands exit 0; manifest has no planned/missing/stale entries; lifecycle counters balance; persisted fixtures and CIDs match; performance report stays within approved gates.

- [ ] **Step 5: Commit the coordinated hard cutover**

~~~sh
git add bindings conformance scripts docs/superpowers/plans/2026-07-16-portable-language-binding-parity.md
git commit -m "feat(bindings): complete portable API parity"
~~~

---

## Plan Self-Review Checklist

- Every design acceptance criterion maps to at least one task.
- Versioned-map, indexed-map, and proximity-map families have shared facade,
  packed transport, language adapters, conformance, and performance work.
- All eight languages are covered; WASM exclusions are explicit and tested.
- Every production change is preceded by a failing test and observed failure.
- Exact verification commands and expected outcomes are present.
- The release step cannot pass with planned, unmapped, untested, or stale API
  entries.
