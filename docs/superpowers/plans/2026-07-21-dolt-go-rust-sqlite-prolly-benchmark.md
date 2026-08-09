# Dolt Go vs Rust SQLite Prolly Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a reproducible, parity-checked benchmark comparing Dolt Go and Rust prolly trees over isolated SQLite fixtures for the complete `sqlite-scale-v2` operation matrix.

**Architecture:** A checked-in Go command implements a benchmark-only SQLite `chunks.ChunkStore` and exercises Dolt's standard `tree.NewNodeStore`. Go and Rust expose the same fixture/cell JSON protocol; a shell driver pins Dolt, builds both runners, alternates each process-isolated cell, and a Python summarizer validates and compares only common metrics.

**Tech Stack:** Go 1.26, Dolt prolly/tree/chunks packages, `database/sql`, `github.com/mattn/go-sqlite3` v1.14.7, Rust 1.81+, existing `prolly-sqlite-scale-bench`, Serde JSON, POSIX shell, Python 3 standard library.

## Global Constraints

- Keep the Go SQLite adapter under `benchmarks/dolt-prolly-sqlite-compare/`; do not modify Dolt production packages.
- Pin one Dolt commit per run and record the commit, runner source hash, dependency version, and executable hash.
- Preserve `sqlite-scale-v2`: 24-byte `key-{id:020}` keys, deterministic 100-byte values, seed `0x6a09e667f3bcc909`, 30% automatic changes, and even merge changes split across two disjoint branches.
- Configure both SQLite stores with WAL, `synchronous=NORMAL`, `busy_timeout=5000`, and `temp_store=MEMORY`.
- Use `GOMAXPROCS=1` and `RAYON_NUM_THREADS=1`; never overlap measured processes.
- Clone only closed, checkpointed fixtures. Setup, validation, publication, reopen checks, and statistics stay outside timed intervals.
- Represent unavailable implementation-specific metrics as JSON `null`, never numeric zero.
- Reject missing, duplicate, mismatched, unvalidated, or incomplete result groups; never infer measurements.
- Never overwrite an existing completed output directory or remove paths outside its validated `fixtures/` and `cells/` roots.

## File Structure

- `benchmarks/dolt-prolly-sqlite-compare/model.go`: shared workload enums, configuration, deterministic IDs, logical keys, and values.
- `benchmarks/dolt-prolly-sqlite-compare/model_test.go`: golden contract and invalid-configuration tests.
- `benchmarks/dolt-prolly-sqlite-compare/sqlite_store.go`: Dolt `chunks.ChunkStore`, root transactions, counters, and SQLite configuration.
- `benchmarks/dolt-prolly-sqlite-compare/sqlite_store_test.go`: chunk-store persistence and transaction behavior.
- `benchmarks/dolt-prolly-sqlite-compare/codec.go`: Dolt tuple descriptors, logical tuple encoding/decoding, root loading, and map publication.
- `benchmarks/dolt-prolly-sqlite-compare/fixture.go`: safe fixture layout, cloning, checkpointing, and file sizes.
- `benchmarks/dolt-prolly-sqlite-compare/runner.go`: fixture build and operation-cell execution.
- `benchmarks/dolt-prolly-sqlite-compare/runner_test.go`: real-SQLite matrix, persistence, and validation tests.
- `benchmarks/dolt-prolly-sqlite-compare/protocol.go`: shared fixture/cell JSON schema and single-row encoder.
- `benchmarks/dolt-prolly-sqlite-compare/main.go`: `fixture` and `cell` CLI.
- `benchmarks/dolt-prolly-sqlite-compare/main_test.go`: CLI parsing and output protocol tests.
- `benchmarks/sqlite-scale/src/bin/prolly-sqlite-cell-runner.rs`: Rust adapter over existing fixture and cell functions.
- `benchmarks/sqlite-scale/Cargo.toml`: register the Rust cell runner and add `serde_json`.
- `benchmarks/sqlite-scale/tests/cell_runner.rs`: Rust CLI protocol and real-SQLite smoke tests.
- `scripts/summarize_dolt_sqlite_comparison.py`: manifest/result validation, aggregation, CSV, and Markdown report.
- `scripts/tests/test_summarize_dolt_sqlite_comparison.py`: malformed-input and aggregation tests.
- `scripts/run_dolt_sqlite_comparison.sh`: pinned checkout, staging, builds, alternating execution, metrics, and provenance.
- `scripts/tests/test_run_dolt_sqlite_comparison.py`: driver contract and safety tests with fake binaries.
- `docs/prolly-go-rust-sqlite-benchmark.md`: usage, methodology, output, and limitations.

---

### Task 1: Freeze the Go workload contract

**Files:**
- Create: `benchmarks/dolt-prolly-sqlite-compare/model.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/model_test.go`

**Interfaces:**
- Produces: `Operation`, `Pattern`, `CacheState`, `CellSpec`, `key`, `value`, `mutationIDs`, `readIDs`, `rangeIDs`, `rangeBounds`, `mergeIDs`, and `enumerateCells`.
- Consumes: no Dolt APIs; this layer remains independently testable after staging inside the Dolt module.

- [ ] **Step 1: Write failing golden-vector and validation tests**

```go
func TestLogicalRecordContract(t *testing.T) {
    if got := key(42); len(got) != 24 || string(got) != "key-00000000000000000042" {
        t.Fatalf("key(42) = %q (%d bytes)", got, len(got))
    }
    if len(value(42, 0)) != 100 || bytes.Equal(value(42, 0), value(42, 1)) {
        t.Fatal("values must be 100 bytes and generation-sensitive")
    }
    if got := mutationIDs(patternAppend, 10_000, 3, 0); !slices.Equal(got, []uint64{10_000, 10_001, 10_002}) {
        t.Fatalf("append IDs = %v", got)
    }
    if got := mutationIDs(patternClustered, 10_000, 4, 0); !slices.Equal(got, []uint64{4_998, 4_999, 5_000, 5_001}) {
        t.Fatalf("clustered IDs = %v", got)
    }
}

func TestMergeIDsAreDisjointAndInterleaved(t *testing.T) {
    for _, pattern := range allPatterns {
        left, right, err := mergeIDs(100_000, 1_000, pattern)
        if err != nil || len(left) != 500 || len(right) != 500 {
            t.Fatalf("%s: left=%d right=%d err=%v", pattern, len(left), len(right), err)
        }
        seen := map[uint64]bool{}
        for _, id := range left { seen[id] = true }
        for _, id := range right {
            if seen[id] { t.Fatalf("%s: duplicate merge ID %d", pattern, id) }
        }
    }
    if _, _, err := mergeIDs(100, 11, patternRandom); err == nil {
        t.Fatal("odd merge count must fail")
    }
}

func TestSmokeMatrixHasTwentyFiveCells(t *testing.T) {
    cells, err := enumerateCells(100, 10, 10, allOperations, allPatterns)
    if err != nil { t.Fatal(err) }
    if len(cells) != 25 { t.Fatalf("cells = %d, want 25", len(cells)) }
}
```

- [ ] **Step 2: Stage the tests in Dolt and verify RED**

Run:

```sh
mkdir -p dolt/go/cmd/prolly-sqlite-compare
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && go test ./cmd/prolly-sqlite-compare)
```

Expected: FAIL because `key`, `value`, `mutationIDs`, `mergeIDs`, and the matrix types do not exist.

- [ ] **Step 3: Implement the deterministic model**

```go
const randomSeed uint64 = 0x6a09e667f3bcc909

type Operation string
const (
    operationPut Operation = "put"
    operationBatch Operation = "batch"
    operationGetCold Operation = "get_cold"
    operationGetWarm Operation = "get_warm"
    operationQuery Operation = "query"
    operationScan Operation = "scan"
    operationFullScan Operation = "full_scan"
    operationDiff Operation = "diff"
    operationMerge Operation = "merge"
)

type Pattern string
const (
    patternAppend Pattern = "append"
    patternRandom Pattern = "random"
    patternClustered Pattern = "clustered"
)

type CacheState string
const (
    cacheNA CacheState = "n/a"
    cacheCold CacheState = "cold-manager"
    cacheWarm CacheState = "warm-manager"
)

type CellSpec struct {
    Output string
    Records uint64
    Repetition int
    Operation Operation
    Pattern Pattern
    CacheState CacheState
    Changes uint64
    ReadSamples uint64
    Revision string
}

var allOperations = []Operation{operationPut, operationBatch, operationGetCold, operationGetWarm, operationQuery, operationScan, operationFullScan, operationDiff, operationMerge}
var allPatterns = []Pattern{patternAppend, patternRandom, patternClustered}

func key(id uint64) []byte { return []byte(fmt.Sprintf("key-%020d", id)) }

func value(id uint64, generation byte) []byte {
    out := []byte(fmt.Sprintf("value-%020d-%02d-", id, generation))
    return append(out, bytes.Repeat([]byte{'x'}, 100-len(out))...)
}

func nextRandom(state *uint64) uint64 {
    *state ^= *state << 13
    *state ^= *state >> 7
    *state ^= *state << 17
    return *state
}

func randomIDs(records, count, salt uint64) []uint64 {
    wanted := min(count, records)
    state := randomSeed ^ bits.RotateLeft64(records, 29) ^ bits.RotateLeft64(salt, 11)
    selected := make(map[uint64]struct{}, int(wanted))
    for uint64(len(selected)) < wanted { selected[nextRandom(&state)%records] = struct{}{} }
    ids := make([]uint64, 0, wanted)
    for id := range selected { ids = append(ids, id) }
    slices.Sort(ids)
    return ids
}
```

Implement `clusteredIDs`, `rightEdgeIDs`, `mutationIDs`, `readIDs`, `rangeIDs`, `rangeBounds`, and `mergeIDs` by translating `benchmarks/sqlite-scale/src/model.rs` exactly. `enumerateCells` must emit all three patterns except one `full_scan` cell, assign cold/warm cache states, reject zero or out-of-range counts, and reject odd merge changes.

- [ ] **Step 4: Verify GREEN and format**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && gofmt -w cmd/prolly-sqlite-compare && go test ./cmd/prolly-sqlite-compare)
```

Expected: PASS with three tests.

- [ ] **Step 5: Commit**

```sh
git add benchmarks/dolt-prolly-sqlite-compare/model.go benchmarks/dolt-prolly-sqlite-compare/model_test.go
git commit -m "bench: freeze Dolt SQLite workload contract"
```

---

### Task 2: Implement the SQLite Dolt chunk store

**Files:**
- Create: `benchmarks/dolt-prolly-sqlite-compare/sqlite_store.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/sqlite_store_test.go`

**Interfaces:**
- Consumes: Dolt `chunks.ChunkStore`, `chunks.Chunk`, `hash.Hash`, and `constants.FormatDoltString`.
- Produces: `openSQLiteChunkStore(path string) (*sqliteChunkStore, error)`, `checkpoint(context.Context) error`, `resetMetrics()`, and `snapshotMetrics() storeMetrics`.

- [ ] **Step 1: Write failing persistence and CAS tests**

```go
func TestSQLiteChunkStorePersistsChunksAndRoot(t *testing.T) {
    ctx := context.Background()
    path := filepath.Join(t.TempDir(), "prolly.db")
    store, err := openSQLiteChunkStore(path)
    if err != nil { t.Fatal(err) }
    chunk := chunks.NewChunk([]byte("payload"))
    if err := store.Put(ctx, chunk, func(chunks.Chunk) chunks.InsertAddrsCb {
        return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
    }); err != nil { t.Fatal(err) }
    if ok, err := store.Commit(ctx, chunk.Hash(), hash.Hash{}); err != nil || !ok {
        t.Fatalf("commit: ok=%v err=%v", ok, err)
    }
    if err := store.Close(); err != nil { t.Fatal(err) }

    reopened, err := openSQLiteChunkStore(path)
    if err != nil { t.Fatal(err) }
    defer reopened.Close()
    got, err := reopened.Get(ctx, chunk.Hash())
    if err != nil || !bytes.Equal(got.Data(), chunk.Data()) { t.Fatalf("get: %v %q", err, got.Data()) }
    root, err := reopened.Root(ctx)
    if err != nil || root != chunk.Hash() { t.Fatalf("root=%s err=%v", root, err) }
}

func TestSQLiteChunkStoreRejectsStaleCommitWithoutPersistingPending(t *testing.T) {
    ctx := context.Background()
    store, err := openSQLiteChunkStore(filepath.Join(t.TempDir(), "prolly.db"))
    if err != nil { t.Fatal(err) }
    defer store.Close()
    first := chunks.NewChunk([]byte("first"))
    second := chunks.NewChunk([]byte("second"))
    putNoRefs(t, store, first)
    if ok, err := store.Commit(ctx, first.Hash(), hash.Hash{}); err != nil || !ok { t.Fatal(ok, err) }
    putNoRefs(t, store, second)
    if ok, err := store.Commit(ctx, second.Hash(), hash.Of([]byte("stale"))); err != nil || ok {
        t.Fatalf("stale commit: ok=%v err=%v", ok, err)
    }
    if root, _ := store.Root(ctx); root != first.Hash() { t.Fatalf("root moved to %s", root) }
}
```

Add separate tests for `GetMany`, `HasMany`, pending visibility, deduplication, missing referenced chunks, cancelled contexts, rollback after a forced SQL constraint failure, unsupported ghost persistence, and operations after `Close`.

- [ ] **Step 2: Verify RED**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
mkdir -p target
cp dolt/go/go.mod target/dolt-sqlite-test.mod
cp dolt/go/go.sum target/dolt-sqlite-test.sum
go mod edit -modfile="$(pwd)/target/dolt-sqlite-test.mod" -require=github.com/mattn/go-sqlite3@v1.14.7
(cd dolt/go && go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" ./cmd/prolly-sqlite-compare -run SQLiteChunkStore)
```

Expected: FAIL because `openSQLiteChunkStore` and store methods do not exist.

- [ ] **Step 3: Implement schema, reads, pending writes, and atomic commit**

```go
type storeMetrics struct { ChunkReads, ChunkWrites, BytesRead, BytesWritten uint64 }

type sqliteChunkStore struct {
    db *sql.DB
    path string
    mu sync.RWMutex
    pending map[hash.Hash]chunks.Chunk
    pendingRefs hash.HashSet
    root hash.Hash
    closed bool
    metrics storeMetrics
}

func openSQLiteChunkStore(path string) (*sqliteChunkStore, error) {
    db, err := sql.Open("sqlite3", "file:"+path+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL&_temp_store=MEMORY&_txlock=immediate")
    if err != nil { return nil, err }
    db.SetMaxOpenConns(1)
    schema := `
CREATE TABLE IF NOT EXISTS prolly_chunks (hash BLOB PRIMARY KEY CHECK(length(hash)=20), data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS prolly_roots (name TEXT PRIMARY KEY, hash BLOB NOT NULL CHECK(length(hash)=20));`
    if _, err = db.Exec(schema); err != nil { db.Close(); return nil, err }
    s := &sqliteChunkStore{db: db, path: path, pending: map[hash.Hash]chunks.Chunk{}, pendingRefs: hash.HashSet{}}
    row := db.QueryRow(`SELECT hash FROM prolly_roots WHERE name='chunk_store'`)
    var raw []byte
    if err = row.Scan(&raw); err == nil { s.root = hash.New(raw) } else if !errors.Is(err, sql.ErrNoRows) { db.Close(); return nil, err }
    return s, nil
}

func (s *sqliteChunkStore) Put(ctx context.Context, c chunks.Chunk, getAddrs chunks.InsertAddrsCurry) error {
    if err := ctx.Err(); err != nil { return err }
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.closed { return errors.New("sqlite chunk store is closed") }
    refs := hash.HashSet{}
    exists := func(h hash.Hash) bool { _, ok := s.pending[h]; return ok }
    if err := getAddrs(c)(ctx, refs, exists); err != nil { return err }
    for ref := range refs {
        if _, ok := s.pending[ref]; ok { continue }
        var one int
        if err := s.db.QueryRowContext(ctx, `SELECT 1 FROM prolly_chunks WHERE hash=?`, ref[:]).Scan(&one); err != nil {
            if errors.Is(err, sql.ErrNoRows) { return fmt.Errorf("missing referenced chunk %s", ref) }
            return err
        }
    }
    s.pending[c.Hash()] = c
    s.pendingRefs.InsertAll(refs)
    return nil
}

func (s *sqliteChunkStore) Commit(ctx context.Context, current, last hash.Hash) (bool, error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    if s.closed { return false, errors.New("sqlite chunk store is closed") }
    if last != s.root { return false, nil }
    tx, err := s.db.BeginTx(ctx, nil)
    if err != nil { return false, err }
    defer tx.Rollback()
    var persistedRaw []byte
    err = tx.QueryRowContext(ctx, `SELECT hash FROM prolly_roots WHERE name='chunk_store'`).Scan(&persistedRaw)
    var persisted hash.Hash
    if err == nil { persisted = hash.New(persistedRaw) } else if !errors.Is(err, sql.ErrNoRows) { return false, err }
    if persisted != last {
        s.root = persisted
        return false, nil
    }
    stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO prolly_chunks(hash,data) VALUES(?,?)`)
    if err != nil { return false, err }
    for h, chunk := range s.pending {
        if _, err = stmt.ExecContext(ctx, h[:], chunk.Data()); err != nil { stmt.Close(); return false, err }
    }
    if err = stmt.Close(); err != nil { return false, err }
    if _, err = tx.ExecContext(ctx, `INSERT INTO prolly_roots(name,hash) VALUES('chunk_store',?) ON CONFLICT(name) DO UPDATE SET hash=excluded.hash`, current[:]); err != nil { return false, err }
    if err = tx.Commit(); err != nil { return false, err }
    s.metrics.ChunkWrites += uint64(len(s.pending))
    for _, chunk := range s.pending { s.metrics.BytesWritten += uint64(chunk.Size()) }
    s.pending = map[hash.Hash]chunks.Chunk{}
    s.pendingRefs = hash.HashSet{}
    s.root = current
    return true, nil
}
```

Implement all remaining `chunks.ChunkStore` methods with these exact signatures: `Get`, `GetMany`, `Has`, `HasMany`, `Version`, `AccessMode`, `Rebase`, `Root`, `Stats`, `StatsSummary`, `PersistGhostHashes`, `Close`, and `Teardown`. `Get` returns `chunks.EmptyChunk` for absence; `GetMany` invokes the callback only for found chunks; `Version` returns `constants.FormatDoltString`; `AccessMode` returns shared; `PersistGhostHashes` returns `chunks.ErrUnsupportedOperation`; `Teardown` is a context check followed by `nil`.

Add a configuration assertion that reads `PRAGMA journal_mode`, `PRAGMA synchronous`, `PRAGMA busy_timeout`, and `PRAGMA temp_store` from the opened connection and checks `wal`, `1`, `5000`, and `2`, respectively. The `_txlock=immediate` setting plus the in-transaction root read makes compare-and-swap publication atomic against another SQLite writer instead of trusting the cached root.

- [ ] **Step 4: Verify GREEN and the race detector**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && gofmt -w cmd/prolly-sqlite-compare && go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" -race ./cmd/prolly-sqlite-compare -run SQLiteChunkStore)
```

Expected: PASS with no races.

- [ ] **Step 5: Commit**

```sh
git add benchmarks/dolt-prolly-sqlite-compare/sqlite_store.go benchmarks/dolt-prolly-sqlite-compare/sqlite_store_test.go
git commit -m "bench: add SQLite chunk store for Dolt prolly"
```

---

### Task 3: Add tuple codecs and durable fixture handling

**Files:**
- Create: `benchmarks/dolt-prolly-sqlite-compare/codec.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/fixture.go`
- Modify: `benchmarks/dolt-prolly-sqlite-compare/sqlite_store_test.go`

**Interfaces:**
- Consumes: `sqliteChunkStore`, workload `key`/`value`, Dolt `val.TupleDesc`, `tree.NodeStore`, and `prolly.Map`.
- Produces: `newMapCodec`, `buildMap`, `loadMap`, `commitMap`, `fixtureLayout`, `cloneFixture`, and `sqliteFileBytes`.

- [ ] **Step 1: Write failing map reopen and safe-clone tests**

```go
func TestCommittedMapReopensWithExactLogicalRows(t *testing.T) {
    ctx := context.Background()
    path := filepath.Join(t.TempDir(), "prolly.db")
    store, _ := openSQLiteChunkStore(path)
    codec := newMapCodec(tree.NewNodeStore(store))
    m, err := codec.buildMap(ctx, []uint64{0, 1, 2}, 0)
    if err != nil { t.Fatal(err) }
    if err := commitMap(ctx, store, m, hash.Hash{}); err != nil { t.Fatal(err) }
    store.Close()

    reopened, _ := openSQLiteChunkStore(path)
    defer reopened.Close()
    loaded, codec, err := loadMap(ctx, reopened)
    if err != nil { t.Fatal(err) }
    for id := uint64(0); id < 3; id++ {
        if err := codec.assertValue(ctx, loaded, id, 0); err != nil { t.Fatal(err) }
    }
}

func TestCloneFixtureRejectsSymlinkAndExistingDestination(t *testing.T) {
    root := t.TempDir()
    source := filepath.Join(root, "source")
    destination := filepath.Join(root, "destination")
    if err := os.Mkdir(source, 0o755); err != nil { t.Fatal(err) }
    if err := os.Symlink("outside", filepath.Join(source, "bad")); err != nil { t.Fatal(err) }
    if err := cloneFixture(source, destination); err == nil { t.Fatal("symlink fixture must fail") }
}
```

- [ ] **Step 2: Verify RED**

Run: staged `go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" ./cmd/prolly-sqlite-compare -run 'CommittedMap|CloneFixture'` from `dolt/go`.

Expected: FAIL because codec and fixture functions are undefined.

- [ ] **Step 3: Implement exact tuple encoding and map publication**

```go
type mapCodec struct {
    ns tree.NodeStore
    keyDesc, valueDesc *val.TupleDesc
    keyBuilder, valueBuilder *val.TupleBuilder
}

type FixtureSpec struct {
    Output string
    Records uint64
    Repetition int
    Revision string
}

func newMapCodec(ns tree.NodeStore) *mapCodec {
    kd := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})
    vd := val.NewTupleDescriptor(val.Type{Enc: val.ByteStringEnc, Nullable: false})
    return &mapCodec{ns: ns, keyDesc: kd, valueDesc: vd, keyBuilder: val.NewTupleBuilder(kd, ns), valueBuilder: val.NewTupleBuilder(vd, ns)}
}

func (c *mapCodec) keyTuple(ctx context.Context, id uint64) (val.Tuple, error) {
    c.keyBuilder.PutByteString(0, key(id))
    return c.keyBuilder.Build(ctx, c.ns.Pool())
}

func (c *mapCodec) valueTuple(ctx context.Context, id uint64, generation byte) (val.Tuple, error) {
    c.valueBuilder.PutByteString(0, value(id, generation))
    return c.valueBuilder.Build(ctx, c.ns.Pool())
}

func loadMap(ctx context.Context, store *sqliteChunkStore) (prolly.Map, *mapCodec, error) {
    ns := tree.NewNodeStore(store)
    codec := newMapCodec(ns)
    root, err := store.Root(ctx)
    if err != nil { return prolly.Map{}, nil, err }
    if root.IsEmpty() { return prolly.Map{}, nil, errors.New("fixture root is missing") }
    node, err := ns.Read(ctx, root)
    if err != nil { return prolly.Map{}, nil, err }
    return prolly.NewMap(node, ns, codec.keyDesc, codec.valueDesc), codec, nil
}

func commitMap(ctx context.Context, store *sqliteChunkStore, m prolly.Map, last hash.Hash) error {
    ok, err := store.Commit(ctx, m.HashOf(), last)
    if err != nil { return err }
    if !ok { return fmt.Errorf("stale root: expected %s", last) }
    return nil
}
```

Implement `buildMap` by constructing all alternating key/value tuples before calling `prolly.NewMapFromTuples`. Task 4 starts its timer only around that final call. `assertValue` must decode field zero through `TupleDesc.GetBytes` and compare exact bytes.

```go
func (c *mapCodec) buildMap(ctx context.Context, ids []uint64, generation byte) (prolly.Map, error) {
    tuples := make([]val.Tuple, 0, len(ids)*2)
    for _, id := range ids {
        k, err := c.keyTuple(ctx, id)
        if err != nil { return prolly.Map{}, err }
        v, err := c.valueTuple(ctx, id, generation)
        if err != nil { return prolly.Map{}, err }
        tuples = append(tuples, k, v)
    }
    return prolly.NewMapFromTuples(ctx, c.ns, c.keyDesc, c.valueDesc, tuples...)
}
```

```go
func (c *mapCodec) assertValue(ctx context.Context, m prolly.Map, id uint64, generation byte) error {
    wantedKey, err := c.keyTuple(ctx, id)
    if err != nil { return err }
    var observed []byte
    err = m.Get(ctx, wantedKey, func(_, tuple val.Tuple) error {
        if tuple == nil { return fmt.Errorf("record %d is missing", id) }
        field, ok := c.valueDesc.GetBytes(0, tuple)
        if !ok { return fmt.Errorf("record %d has no value field", id) }
        observed = append(observed[:0], field...)
        return nil
    })
    if err != nil { return err }
    if !bytes.Equal(observed, value(id, generation)) { return fmt.Errorf("record %d has the wrong value", id) }
    return nil
}
```

Implement fixture paths as `OUTPUT/fixtures/RECORDS/run-N/prolly.db` and `OUTPUT/cells/RECORDS/run-N/OPERATION/PATTERN/CACHE/prolly.db`. Recursively clone regular files only, checkpoint with `PRAGMA wal_checkpoint(TRUNCATE)`, reject symlinks and existing destinations, and ensure cleanup targets start beneath the exact generated root.

- [ ] **Step 4: Verify GREEN**

Run: staged `go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" ./cmd/prolly-sqlite-compare -run 'CommittedMap|CloneFixture|SQLiteChunkStore'` from `dolt/go`.

Expected: PASS.

- [ ] **Step 5: Commit**

```sh
git add benchmarks/dolt-prolly-sqlite-compare/codec.go benchmarks/dolt-prolly-sqlite-compare/fixture.go benchmarks/dolt-prolly-sqlite-compare/sqlite_store_test.go
git commit -m "bench: persist and clone Dolt prolly fixtures"
```

---

### Task 4: Implement Dolt fixture and operation runners

**Files:**
- Create: `benchmarks/dolt-prolly-sqlite-compare/protocol.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/runner.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/runner_test.go`

**Interfaces:**
- Consumes: all Task 1–3 interfaces plus Dolt `DiffMaps`, `MergeMaps`, `Map.Get`, `IterKeyRange`, and `IterAll`.
- Produces: `ProtocolRow`, `buildFixture(context.Context, FixtureSpec)`, and `runCell(context.Context, CellSpec)`.

- [ ] **Step 1: Write the failing complete smoke-matrix test**

```go
func TestEverySmokeCellValidatesAndReopens(t *testing.T) {
    ctx := context.Background()
    output := t.TempDir()
    fixture, err := buildFixture(ctx, FixtureSpec{Output: output, Records: 100, Repetition: 1, Revision: "test"})
    if err != nil || !fixture.Validated { t.Fatalf("fixture=%+v err=%v", fixture, err) }
    cells, err := enumerateCells(100, 10, 10, allOperations, allPatterns)
    if err != nil { t.Fatal(err) }
    if len(cells) != 25 { t.Fatalf("cells=%d", len(cells)) }
    for _, cell := range cells {
        cell.Output, cell.Repetition, cell.Revision = output, 1, "test"
        row, err := runCell(ctx, cell)
        if err != nil { t.Fatalf("%s/%s: %v", cell.Operation, cell.Pattern, err) }
        if !row.Validated || row.ExpectedEntries != row.ObservedEntries || row.LogicalOperations != row.ObservedItems {
            t.Fatalf("%s/%s: %+v", cell.Operation, cell.Pattern, row)
        }
    }
}
```

Add targeted tests proving cold gets call `PurgeCaches`, query rows contain `QueryStrategy="repeated_map_get"`, diff validates exact keys, merge validates both generations, and a deliberately corrupted expected value returns a non-nil error without `Validated=true`.

- [ ] **Step 2: Verify RED**

Run: staged `go test ./cmd/prolly-sqlite-compare -run EverySmokeCell -count=1`.

Expected: FAIL because `buildFixture`, `runCell`, and `ProtocolRow` do not exist.

- [ ] **Step 3: Define the common nullable JSON protocol**

```go
type ProtocolRow struct {
    ContractVersion string `json:"contract_version"`
    Kind string `json:"kind"`
    Implementation string `json:"implementation"`
    Revision string `json:"revision"`
    Records uint64 `json:"records"`
    Repetition int `json:"repetition"`
    Operation string `json:"operation"`
    Pattern string `json:"pattern"`
    CacheState string `json:"cache_state"`
    LogicalOperations uint64 `json:"logical_operations"`
    ObservedItems uint64 `json:"observed_items"`
    TotalNS uint64 `json:"total_ns"`
    NSPerOperation float64 `json:"ns_per_operation"`
    OperationsPerSecond float64 `json:"operations_per_second"`
    P50NS *uint64 `json:"p50_ns"`
    P95NS *uint64 `json:"p95_ns"`
    P99NS *uint64 `json:"p99_ns"`
    MaxNS *uint64 `json:"max_ns"`
    ChunkReads *uint64 `json:"chunk_reads"`
    ChunkWrites *uint64 `json:"chunk_writes"`
    BytesRead *uint64 `json:"bytes_read"`
    BytesWritten *uint64 `json:"bytes_written"`
    ResultEntries uint64 `json:"result_entries"`
    DBBytes uint64 `json:"db_bytes"`
    WALBytes uint64 `json:"wal_bytes"`
    SHMBytes uint64 `json:"shm_bytes"`
    TotalDatabaseBytes uint64 `json:"total_database_bytes"`
    ExpectedEntries uint64 `json:"expected_entries"`
    ObservedEntries uint64 `json:"observed_entries"`
    QueryStrategy *string `json:"query_strategy"`
    Validated bool `json:"validated"`
    Error string `json:"error"`
}
```

Give every field an explicit snake-case JSON tag. Use `kind="fixture"`, `operation="build"`, `pattern="n/a"`, and `cache_state="n/a"` for fixture rows. Use pointers only for metrics that can be unavailable.

- [ ] **Step 4: Implement fixture construction and each native operation**

```go
func applyBatch(ctx context.Context, base prolly.Map, codec *mapCodec, ids []uint64, generation byte) (prolly.Map, error) {
    mut := base.Mutate()
    for _, id := range ids {
        keyTuple, err := codec.keyTuple(ctx, id)
        if err != nil { return prolly.Map{}, err }
        valueTuple, err := codec.valueTuple(ctx, id, generation)
        if err != nil { return prolly.Map{}, err }
        if err := mut.Put(ctx, keyTuple, valueTuple); err != nil { return prolly.Map{}, err }
    }
    return mut.Map(ctx)
}

func timedGet(ctx context.Context, m prolly.Map, codec *mapCodec, ids []uint64, cold bool) (uint64, []uint64, error) {
    latencies := make([]uint64, 0, len(ids))
    startAll := time.Now()
    for _, id := range ids {
        if cold { m.NodeStore().PurgeCaches() }
        started := time.Now()
        if err := codec.assertValue(ctx, m, id, 0); err != nil { return 0, nil, err }
        latencies = append(latencies, uint64(time.Since(started).Nanoseconds()))
    }
    return uint64(time.Since(startAll).Nanoseconds()), latencies, nil
}
```

`buildFixture` must prebuild alternating key/value tuples, start the timer immediately before `prolly.NewMapFromTuples`, commit the resulting root, validate count and sampled rows, checkpoint, close, reopen, validate again, and return a fixture row.

`runCell` must clone the closed fixture, load the base map, reset store counters immediately before timing, execute exactly one operation, stop timing after all lazy callbacks/iterations are consumed, validate exact results, commit mutating results, collect SQLite sizes, close/reopen mutating results, and remove only the cell clone after the row has been materialized.

For diff, count and validate callback `tree.Diff` values from `prolly.DiffMaps`. For merge, pass a `tree.CollisionFn` that sets a benchmark-owned `collisionSeen` flag and returns `(tree.Diff{}, false)`; fail validation if the flag becomes true because generated branches are disjoint. For query, loop over `Map.Get` and set `QueryStrategy` to `repeated_map_get`. Compute nearest-rank p50/p95/p99 only for point-get operations.

- [ ] **Step 5: Verify GREEN**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && gofmt -w cmd/prolly-sqlite-compare && GOMAXPROCS=1 go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" ./cmd/prolly-sqlite-compare -run EverySmokeCell -count=1)
```

Expected: PASS with 25 validated cells.

- [ ] **Step 6: Run the complete Go test package**

Run: `(cd dolt/go && go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" -race ./cmd/prolly-sqlite-compare)`.

Expected: PASS.

- [ ] **Step 7: Commit**

```sh
git add benchmarks/dolt-prolly-sqlite-compare/protocol.go benchmarks/dolt-prolly-sqlite-compare/runner.go benchmarks/dolt-prolly-sqlite-compare/runner_test.go
git commit -m "bench: run Dolt prolly SQLite operation matrix"
```

---

### Task 5: Add the Go fixture/cell command

**Files:**
- Create: `benchmarks/dolt-prolly-sqlite-compare/main.go`
- Create: `benchmarks/dolt-prolly-sqlite-compare/main_test.go`

**Interfaces:**
- Consumes: `buildFixture`, `runCell`, `FixtureSpec`, `CellSpec`, and `ProtocolRow`.
- Produces CLI: `prolly-sqlite-compare fixture ...` and `prolly-sqlite-compare cell ...`, exactly one JSON row on stdout.

- [ ] **Step 1: Write failing CLI tests**

```go
func TestParseFixtureCommand(t *testing.T) {
    command, err := parseCommand([]string{"fixture", "--output", "/tmp/out", "--records", "100", "--repetition", "2", "--revision", "abc"})
    if err != nil { t.Fatal(err) }
    if command.Kind != "fixture" || command.Records != 100 || command.Repetition != 2 { t.Fatalf("%+v", command) }
}

func TestParseCellRejectsOddMergeChanges(t *testing.T) {
    _, err := parseCommand([]string{"cell", "--output", "/tmp/out", "--records", "100", "--repetition", "1", "--revision", "abc", "--operation", "merge", "--pattern", "random", "--changes", "11", "--read-samples", "10"})
    if err == nil || !strings.Contains(err.Error(), "even") { t.Fatalf("err=%v", err) }
}
```

- [ ] **Step 2: Verify RED**

Run: staged `go test ./cmd/prolly-sqlite-compare -run Parse`.

Expected: FAIL because `parseCommand` does not exist.

- [ ] **Step 3: Implement strict parsing and one-row JSON output**

```go
func main() {
    command, err := parseCommand(os.Args[1:])
    if err != nil { fmt.Fprintln(os.Stderr, err); os.Exit(2) }
    ctx := context.Background()
    var row ProtocolRow
    if command.Kind == "fixture" { row, err = buildFixture(ctx, command.fixtureSpec()) } else { row, err = runCell(ctx, command.cellSpec()) }
    encoder := json.NewEncoder(os.Stdout)
    encoder.SetEscapeHTML(false)
    if err != nil {
        row.Validated = false
        row.Error = err.Error()
        _ = encoder.Encode(row)
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }
    if encodeErr := encoder.Encode(row); encodeErr != nil { fmt.Fprintln(os.Stderr, encodeErr); os.Exit(1) }
}
```

Use a separate `flag.FlagSet` per subcommand with output discarded. Reject extra arguments, empty output/revision, zero records/repetition/changes/read samples, changes or samples above records, invalid operation/pattern, odd merge changes, and unsupported pattern combinations. Never emit progress text on stdout.

- [ ] **Step 4: Verify GREEN and build**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && gofmt -w cmd/prolly-sqlite-compare && go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" ./cmd/prolly-sqlite-compare && go build -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" -trimpath ./cmd/prolly-sqlite-compare)
```

Expected: tests and build exit 0.

- [ ] **Step 5: Commit**

```sh
git add benchmarks/dolt-prolly-sqlite-compare/main.go benchmarks/dolt-prolly-sqlite-compare/main_test.go
git commit -m "bench: expose Dolt SQLite fixture and cell runner"
```

---

### Task 6: Expose the same cell protocol from Rust

**Files:**
- Modify: `benchmarks/sqlite-scale/Cargo.toml`
- Create: `benchmarks/sqlite-scale/src/bin/prolly-sqlite-cell-runner.rs`
- Create: `benchmarks/sqlite-scale/tests/cell_runner.rs`

**Interfaces:**
- Consumes: existing public `FixtureLayout`, `FixtureSpec`, `CellSpec`, `build_fixture`, `run_cell`, `Operation`, and `Pattern`.
- Produces the identical fixture/cell CLI and JSON field names from Task 5, with implementation `rust` and `null` for unavailable common fields.

- [ ] **Step 1: Write a failing real-SQLite protocol test**

```rust
#[test]
fn fixture_then_cell_emit_validated_protocol_rows() {
    let temp = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_prolly-sqlite-cell-runner");
    let fixture = Command::new(binary).args(["fixture", "--output", temp.path().to_str().unwrap(), "--records", "100", "--repetition", "1", "--revision", "test"]).output().unwrap();
    assert!(fixture.status.success(), "{}", String::from_utf8_lossy(&fixture.stderr));
    let fixture: Value = serde_json::from_slice(&fixture.stdout).unwrap();
    assert_eq!(fixture["kind"], "fixture");
    assert_eq!(fixture["validated"], true);
    let cell = Command::new(binary).args(["cell", "--output", temp.path().to_str().unwrap(), "--records", "100", "--repetition", "1", "--revision", "test", "--operation", "merge", "--pattern", "random", "--changes", "10", "--read-samples", "10"]).output().unwrap();
    assert!(cell.status.success(), "{}", String::from_utf8_lossy(&cell.stderr));
    let cell: Value = serde_json::from_slice(&cell.stdout).unwrap();
    assert_eq!(cell["logical_operations"], 10);
    assert_eq!(cell["validated"], true);
}
```

- [ ] **Step 2: Register the binary and verify RED**

Add `serde_json = "1.0"` and:

```toml
[[bin]]
name = "prolly-sqlite-cell-runner"
path = "src/bin/prolly-sqlite-cell-runner.rs"
```

Run: `cargo test --manifest-path benchmarks/sqlite-scale/Cargo.toml --test cell_runner`.

Expected: FAIL because the binary or its subcommands are not implemented.

- [ ] **Step 3: Implement the Rust adapter without duplicating workload logic**

```rust
#[derive(Serialize)]
struct ProtocolRow {
    contract_version: &'static str,
    kind: &'static str,
    implementation: &'static str,
    revision: String,
    records: usize,
    repetition: usize,
    operation: String,
    pattern: String,
    cache_state: String,
    logical_operations: usize,
    observed_items: usize,
    total_ns: u128,
    ns_per_operation: f64,
    operations_per_second: f64,
    p50_ns: Option<u128>,
    p95_ns: Option<u128>,
    p99_ns: Option<u128>,
    max_ns: Option<u128>,
    chunk_reads: Option<u64>,
    chunk_writes: Option<u64>,
    bytes_read: Option<u64>,
    bytes_written: Option<u64>,
    result_entries: usize,
    db_bytes: u64,
    wal_bytes: u64,
    shm_bytes: u64,
    total_database_bytes: u64,
    expected_entries: usize,
    observed_entries: usize,
    query_strategy: Option<&'static str>,
    validated: bool,
    error: String,
}
```

Parse the same flags as Go. `fixture` calls `build_fixture` using the existing derived `FixtureLayout`. `cell` calls `layout.clone_for`, `run_cell`, converts the returned `RawRow`, then calls `layout.remove_cell`. Map Rust `nodes_read/written` to `chunk_reads/writes`, preserve point percentiles, and use `query_strategy="native_get_many"` only for query. Emit one `serde_json::to_writer(stdout.lock(), &row)` value plus newline.

- [ ] **Step 4: Verify GREEN and the existing harness**

Run:

```sh
cargo fmt --manifest-path benchmarks/sqlite-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/sqlite-scale/Cargo.toml
cargo build --release --manifest-path benchmarks/sqlite-scale/Cargo.toml --bin prolly-sqlite-cell-runner
```

Expected: all tests pass and release build exits 0.

- [ ] **Step 5: Commit**

```sh
git add benchmarks/sqlite-scale/Cargo.toml benchmarks/sqlite-scale/Cargo.lock benchmarks/sqlite-scale/src/bin/prolly-sqlite-cell-runner.rs benchmarks/sqlite-scale/tests/cell_runner.rs
git commit -m "bench: expose Rust SQLite fixture and cell protocol"
```

---

### Task 7: Validate and summarize cross-language results

**Files:**
- Create: `scripts/summarize_dolt_sqlite_comparison.py`
- Create: `scripts/tests/test_summarize_dolt_sqlite_comparison.py`

**Interfaces:**
- Consumes: newline-delimited protocol JSON at `raw-results.jsonl` and process manifest CSV with peak RSS.
- Produces: `results.csv`, `summary.csv`, and `report.md`; exits nonzero on any contract violation.

- [ ] **Step 1: Write failing complete-pair and malformed-input tests**

```python
def test_summarizer_pairs_validated_rows_and_reports_median(tmp_path):
    rows = []
    for repetition, rust_ns, go_ns in [(1, 100, 200), (2, 120, 180), (3, 110, 220)]:
        rows.extend([
            row("rust", repetition, rust_ns),
            row("dolt-go", repetition, go_ns, query_strategy="repeated_map_get"),
        ])
    write_inputs(tmp_path, rows)
    run_summary(tmp_path, expected_runs=3)
    summary = list(csv.DictReader((tmp_path / "summary.csv").open()))
    assert summary[0]["rust_median_ns"] == "110"
    assert summary[0]["dolt_go_median_ns"] == "200"
    assert summary[0]["winner"] == "rust"

def test_summarizer_rejects_missing_unvalidated_and_mismatched_rows(tmp_path):
    for mutation in (drop_go_row, mark_unvalidated, change_operation_count):
        rows = mutation([row("rust", 1, 100), row("dolt-go", 1, 200)])
        write_inputs(tmp_path, rows)
        result = run_summary(tmp_path, expected_runs=1, check=False)
        assert result.returncode != 0
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.tests.test_summarize_dolt_sqlite_comparison -v`.

Expected: FAIL because the summarizer does not exist.

- [ ] **Step 3: Implement strict schema validation and aggregation**

```python
PAIR_FIELDS = ("records", "repetition", "operation", "pattern", "cache_state")
PARITY_FIELDS = ("contract_version", "logical_operations", "expected_entries", "observed_entries", "validated")

def validate_pairs(rows, expected_runs):
    by_key = {}
    for row in rows:
        if row["validated"] is not True:
            raise ValueError(f"unvalidated row: {row}")
        key = tuple(row[name] for name in PAIR_FIELDS)
        impls = by_key.setdefault(key, {})
        if row["implementation"] in impls:
            raise ValueError(f"duplicate row for {key}/{row['implementation']}")
        impls[row["implementation"]] = row
    for key, pair in by_key.items():
        if set(pair) != {"rust", "dolt-go"}:
            raise ValueError(f"incomplete pair: {key}")
        for field in PARITY_FIELDS:
            if pair["rust"][field] != pair["dolt-go"][field]:
                raise ValueError(f"parity mismatch for {key}: {field}")
    return by_key
```

Validate the exact JSON key set and scalar types, finite nonnegative timings, positive logical counts, permitted query strategy per implementation, exact expected repetitions, one build row per implementation/size/repetition, and manifest success/RSS for every JSON row. Aggregate medians, min/max, coefficient of variation, winner, ratio, RSS, and SQLite sizes without rounding before winner selection. Report the query strategy and persisted-format limitation prominently.

- [ ] **Step 4: Verify GREEN**

Run: `python3 -m unittest scripts.tests.test_summarize_dolt_sqlite_comparison -v`.

Expected: all summarizer tests pass.

- [ ] **Step 5: Commit**

```sh
git add scripts/summarize_dolt_sqlite_comparison.py scripts/tests/test_summarize_dolt_sqlite_comparison.py
git commit -m "bench: summarize Dolt and Rust SQLite results"
```

---

### Task 8: Build the pinned alternating comparison driver

**Files:**
- Create: `scripts/run_dolt_sqlite_comparison.sh`
- Create: `scripts/tests/test_run_dolt_sqlite_comparison.py`

**Interfaces:**
- Consumes: checked-in Go source, both fixture/cell CLIs, `/usr/bin/time`, and Task 7 summarizer.
- Produces: a complete result directory with binaries, JSONL, manifest, provenance, checksums, machine metadata, summary, and report.

- [ ] **Step 1: Write failing fake-runner driver tests**

```python
def test_smoke_driver_stages_pinned_source_and_alternates_cells(self):
    with tempfile.TemporaryDirectory() as directory:
        temp = pathlib.Path(directory)
        output = temp / "out"
        log = temp / "order.log"
        rust = make_fake_runner(temp / "rust", "rust", log)
        go = make_fake_runner(temp / "go", "dolt-go", log)
        env = os.environ | {
            "BENCH_PROFILE": "smoke", "BENCH_OUT": str(output),
            "DOLT_SQLITE_SKIP_BUILD": "1", "DOLT_SQLITE_RUST_BIN": str(rust),
            "DOLT_SQLITE_GO_BIN": str(go), "ORDER_LOG": str(log),
        }
        subprocess.run([str(DRIVER)], cwd=ROOT, env=env, check=True)
        calls = log.read_text().splitlines()
        assert calls[0].startswith("rust fixture") or calls[0].startswith("dolt-go fixture")
        assert any("rust cell" in call for call in calls)
        assert any("dolt-go cell" in call for call in calls)
        assert (output / "manifest.txt").is_file()
        assert (output / "raw-results.jsonl").is_file()

def test_driver_refuses_existing_completed_output(self):
    output = self.temp / "out"
    output.mkdir()
    (output / "run-status.txt").write_text("complete\n")
    result = subprocess.run([str(DRIVER)], env=self.env(output), capture_output=True, text=True)
    self.assertEqual(result.returncode, 2)
    self.assertIn("refusing to overwrite", result.stderr)
```

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest scripts.tests.test_run_dolt_sqlite_comparison -v`.

Expected: FAIL because the driver does not exist.

- [ ] **Step 3: Implement pinned checkout, dependency, and builds**

```sh
DOLT_REPO_URL=${DOLT_REPO_URL:-https://github.com/dolthub/dolt.git}
DOLT_CACHE=${DOLT_CACHE:-"$ROOT/target/dolt-sqlite-benchmark"}
SQLITE_DRIVER_VERSION=1.14.7

if [ ! -d "$DOLT_CACHE/.git" ]; then
    [ ! -e "$DOLT_CACHE" ] || { printf 'DOLT_CACHE is not a git checkout: %s\n' "$DOLT_CACHE" >&2; exit 2; }
    mkdir -p "$(dirname "$DOLT_CACHE")"
    git clone --filter=blob:none --no-checkout "$DOLT_REPO_URL" "$DOLT_CACHE"
fi
git -C "$DOLT_CACHE" fetch --prune origin main
if [ -n "${DOLT_REV:-}" ] && ! git -C "$DOLT_CACHE" rev-parse --verify "$DOLT_REV^{commit}" >/dev/null 2>&1; then
    git -C "$DOLT_CACHE" fetch origin "$DOLT_REV"
fi
DOLT_SHA=$(git -C "$DOLT_CACHE" rev-parse "${DOLT_REV:-origin/main}^{commit}")
git -C "$DOLT_CACHE" checkout --detach "$DOLT_SHA"
RUNNER_DEST="$DOLT_CACHE/go/cmd/prolly-sqlite-compare"
if [ -e "$RUNNER_DEST" ]; then rm -rf "$RUNNER_DEST"; fi
mkdir -p "$RUNNER_DEST"
cp "$ROOT"/benchmarks/dolt-prolly-sqlite-compare/*.go "$RUNNER_DEST/"
cp "$DOLT_CACHE/go/go.mod" "$OUT/dolt-benchmark.mod"
cp "$DOLT_CACHE/go/go.sum" "$OUT/dolt-benchmark.sum"
go mod edit -modfile="$OUT/dolt-benchmark.mod" -require="github.com/mattn/go-sqlite3@v$SQLITE_DRIVER_VERSION"
(
    cd "$DOLT_CACHE/go"
    go test -modfile="$OUT/dolt-benchmark.mod" ./cmd/prolly-sqlite-compare
    go build -modfile="$OUT/dolt-benchmark.mod" -trimpath -o "$OUT/bin/dolt-go-prolly-sqlite" ./cmd/prolly-sqlite-compare
)
cargo test --manifest-path "$ROOT/benchmarks/sqlite-scale/Cargo.toml"
cargo build --release --manifest-path "$ROOT/benchmarks/sqlite-scale/Cargo.toml" --bin prolly-sqlite-cell-runner
```

Resolve Cargo's target directory through `cargo metadata`; do not assume `target/release`. Hash the complete Go runner source tree and both copied executables. Record `go version`, `rustc -Vv`, `sqlite3 --version` when available, host, OS, CPU, memory, start/end UTC, matrix dimensions, SQLite settings, and dirty source archives following `scripts/run_sqlite_scale_benchmark.sh`.

- [ ] **Step 4: Implement fixture creation, alternating cells, and result capture**

```sh
run_one() {
    implementation=$1; shift
    binary=$1; shift
    prefix=$1; shift
    set +e
    GOMAXPROCS=1 RAYON_NUM_THREADS=1 "$TIME_BIN" "$TIME_MODE" -o "$prefix.time" "$binary" "$@" >"$prefix.json" 2>"$prefix.stderr"
    status=$?
    set -e
    peak_rss=$("$PYTHON_BIN" "$ROOT/scripts/prolly_process_metrics.py" "$prefix.time") || peak_rss=
    printf '%s,%s,%s,%s,%s,%s\n' "$implementation" "$status" "$peak_rss" "$prefix.json" "$prefix.stderr" "$prefix.time" >>"$OUT/process-manifest.csv"
    if "$PYTHON_BIN" -m json.tool "$prefix.json" >/dev/null 2>&1; then
        cat "$prefix.json" >>"$OUT/raw-results.jsonl"
    fi
    [ "$status" -eq 0 ] && [ -n "$peak_rss" ] || return 1
    "$PYTHON_BIN" -m json.tool "$prefix.json" >/dev/null
}
```

For each repetition and size, run one fixture command per implementation. Then enumerate the exact 25 cells: all three patterns for put, batch, cold get, warm get, query, scan, diff, and merge, plus one append-labeled full scan. Alternate first implementation using a deterministic parity of size, repetition, operation index, and pattern index. Give Rust and Go separate fixture output roots under the comparison output. Abort immediately on any failed process or malformed JSON; leave artifacts and `run-status.txt=failed`.

After all cells, invoke the summarizer with exact expected sizes and repetitions, then atomically write `run-status.txt=complete`. Refuse any preexisting complete status or result files.

- [ ] **Step 5: Verify GREEN**

Run: `python3 -m unittest scripts.tests.test_run_dolt_sqlite_comparison -v`.

Expected: all fake-runner driver and safety tests pass.

- [ ] **Step 6: Run a real one-cell staging check**

Run:

```sh
BENCH_PROFILE=smoke BENCH_OPERATIONS=put BENCH_PATTERNS=append BENCH_OUT="$(mktemp -d)/dolt-sqlite-one-cell" scripts/run_dolt_sqlite_comparison.sh
```

Expected: two validated fixture rows, one validated Rust/Go cell pair, matching parity fields, and generated `summary.csv` and `report.md`.

- [ ] **Step 7: Commit**

```sh
git add scripts/run_dolt_sqlite_comparison.sh scripts/tests/test_run_dolt_sqlite_comparison.py
git commit -m "bench: orchestrate Dolt and Rust SQLite comparison"
```

---

### Task 9: Document usage and run final verification

**Files:**
- Create: `docs/prolly-go-rust-sqlite-benchmark.md`
- Modify: `README.md`
- Modify: `docs/performance.md`

**Interfaces:**
- Consumes: the final CLI, report schema, and verified smoke output.
- Produces: discoverable reproduction instructions and explicit comparison limitations.

- [ ] **Step 1: Write documentation with exact commands and caveats**

````markdown
# Dolt Go vs Rust SQLite prolly benchmark

Run a real-SQLite smoke comparison:

```sh
BENCH_PROFILE=smoke scripts/run_dolt_sqlite_comparison.sh
```

Run the full 1M-record, three-repetition matrix:

```sh
BENCH_PROFILE=full scripts/run_dolt_sqlite_comparison.sh
```

Set `DOLT_REV=<commit>` to reproduce an exact Dolt revision and `BENCH_OUT` to
select a new output directory. The driver refuses to overwrite completed output.
````

Document all operations and patterns, fixture cloning, timing boundaries, WAL settings, one-worker isolation, parity rejection rules, output files, and provenance. State that persisted encodings differ and that Rust query uses native `get_many` while Dolt query uses repeated `Map.Get`, so the query row compares available product APIs rather than identical batching primitives.

- [ ] **Step 2: Verify every targeted suite fresh**

Run:

```sh
cp benchmarks/dolt-prolly-sqlite-compare/*.go dolt/go/cmd/prolly-sqlite-compare/
(cd dolt/go && gofmt -w cmd/prolly-sqlite-compare && go test -modfile="$(pwd)/../../target/dolt-sqlite-test.mod" -race ./cmd/prolly-sqlite-compare)
cargo fmt --manifest-path benchmarks/sqlite-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/sqlite-scale/Cargo.toml
python3 -m unittest scripts.tests.test_summarize_dolt_sqlite_comparison scripts.tests.test_run_dolt_sqlite_comparison -v
shellcheck scripts/run_dolt_sqlite_comparison.sh
git diff --check
```

Expected: all commands exit 0 with no test failures, races, formatting errors, shell warnings, or whitespace errors.

- [ ] **Step 3: Run the complete real-SQLite smoke matrix**

Run:

```sh
BENCH_PROFILE=smoke BENCH_OUT="performance-results/dolt-rust-sqlite-smoke-$(date -u +%Y%m%dT%H%M%SZ)" scripts/run_dolt_sqlite_comparison.sh
```

Expected: 2 validated fixture rows per repetition/size, 50 validated operation rows (25 exact Rust/Go pairs), zero missing or duplicate cells, and generated `results.csv`, `summary.csv`, `report.md`, hashes, machine metadata, and `run-status.txt` containing `complete`.

- [ ] **Step 4: Inspect requirements and provenance**

Check that the report discloses both query strategies and format differences; the manifest records the exact Dolt commit, source hashes, dependency version, binary hashes, toolchains, SQLite settings, sizes, runs, and worker limits; every process-manifest row has exit status zero and positive peak RSS.

- [ ] **Step 5: Commit documentation**

```sh
git add docs/prolly-go-rust-sqlite-benchmark.md README.md docs/performance.md
git commit -m "docs: explain Dolt and Rust SQLite benchmark"
```

- [ ] **Step 6: Record final repository state**

Run:

```sh
git status --short
git log -10 --oneline
```

Expected: only preexisting user-owned untracked artifacts remain; the implementation is represented by focused commits from Tasks 1–9.
