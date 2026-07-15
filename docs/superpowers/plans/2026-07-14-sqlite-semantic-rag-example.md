# SQLite Semantic RAG Example Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a runnable offline RAG retrieval example that persists a 1,536-dimensional cosine ProximityMap in SQLite, reopens it across process runs, and emits ranked citations plus an LLM-ready context block.

**Architecture:** Put the example in the `prolly-store-sqlite` package to avoid a root-package dependency cycle. A checked-in JSON fixture carries dense, normalized document and query embeddings; pure validation/retrieval/rendering functions remain unit-testable, while an end-to-end example test proves named-root publication and SQLite reopen.

**Tech Stack:** Rust 1.81, `prolly-map`, `prolly-store-sqlite`, `serde`, `serde_json`, SQLite WAL, ProximityMap cosine search.

## Global Constraints

- The example must run fully offline and make no embedding or generation API request.
- Every document and query vector has exactly 1,536 finite `f32` components and unit norm within `1e-4`.
- Persist the active descriptor under named root `rag/corpus/main` and reject incompatible persisted metadata.
- Search uses `DistanceMetric::Cosine`, `SearchPolicy::Exact`, `k = 3`, and prefix `tenant/acme/docs/`.
- Do not claim that the synthetic checked-in fixture came from a named hosted provider.
- Do not add PQ, HNSW, corpus refresh, migrations, or concurrent publication.
- Preserve the user's unrelated untracked files and directories.

---

## File structure

- `stores/prolly-store-sqlite/examples/semantic_rag.rs`: fixture types, validation, durable lifecycle, exact retrieval, context rendering, CLI, and focused tests.
- `stores/prolly-store-sqlite/examples/data/semantic_rag_embeddings.json`: model contract, six support chunks, three named offline queries, and dense 1,536-dimensional vectors.
- `stores/prolly-store-sqlite/Cargo.toml`: example-only `serde` and `serde_json` development dependencies.
- `stores/prolly-store-sqlite/README.md`: adapter-local invocation and two-run persistence demonstration.
- `README.md`: discoverability link in the examples/store section.

### Task 1: Fixture contract and validation

**Files:**
- Create: `stores/prolly-store-sqlite/examples/semantic_rag.rs`
- Create: `stores/prolly-store-sqlite/examples/data/semantic_rag_embeddings.json`
- Modify: `stores/prolly-store-sqlite/Cargo.toml`

**Interfaces:**
- Consumes: `serde::Deserialize`, `serde_json::from_str`, and the JSON fixture included with `include_str!`.
- Produces: `Fixture`, `ChunkFixture`, `QueryFixture`, `ChunkMetadata`, `load_fixture() -> AppResult<Fixture>`, `validate_fixture(&Fixture) -> AppResult<()>`, and `embedding_for_query(&Fixture, &str) -> AppResult<&[f32]>`.

- [ ] **Step 1: Add the test target with failing validation tests**

Add `serde` and `serde_json` under `[dev-dependencies]`. Create the example with the data structures, `AppResult<T> = Result<T, Box<dyn Error>>`, three deliberately unimplemented functions, and tests equivalent to:

```rust
#[test]
fn fixture_is_1536_dimensional_finite_normalized_and_unique() {
    let fixture = load_fixture().unwrap();
    validate_fixture(&fixture).unwrap();
    assert_eq!(fixture.dimensions, 1_536);
    assert_eq!(fixture.chunks.len(), 6);
}

#[test]
fn validation_rejects_wrong_dimensions_and_non_unit_vectors() {
    let mut fixture = load_fixture().unwrap();
    fixture.queries[0].embedding.pop();
    assert!(validate_fixture(&fixture).unwrap_err().to_string().contains("1536"));
    fixture = load_fixture().unwrap();
    fixture.queries[0].embedding.fill(0.0);
    assert!(validate_fixture(&fixture).unwrap_err().to_string().contains("unit-normalized"));
}

#[test]
fn unknown_query_lists_supported_names() {
    let fixture = load_fixture().unwrap();
    let error = embedding_for_query(&fixture, "unknown").unwrap_err().to_string();
    assert!(error.contains("password-reset"));
    assert!(error.contains("lost-2fa"));
    assert!(error.contains("rotate-api-key"));
}
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```sh
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --example semantic_rag
```

Expected: compilation succeeds, then at least one test fails because fixture loading or validation is deliberately unimplemented.

- [ ] **Step 3: Add the dense fixture and minimal validation implementation**

Generate and check in full 1,536-element arrays by normalizing and zero-padding these exact semantic seed vectors:

```text
chunk password-reset:    [0.98, 0.10, 0.05, 0.00, 0.12]
chunk password-security: [0.90, 0.18, 0.04, 0.00, 0.20]
chunk account-recovery:  [0.72, 0.62, 0.02, 0.00, 0.15]
chunk mfa-recovery:      [0.12, 0.98, 0.02, 0.00, 0.10]
chunk api-key-rotation:  [0.02, 0.05, 0.99, 0.00, 0.10]
chunk billing-update:    [0.02, 0.02, 0.00, 0.99, 0.10]
query password-reset:    [0.99, 0.08, 0.02, 0.00, 0.10]
query lost-2fa:          [0.10, 0.99, 0.00, 0.00, 0.10]
query rotate-api-key:    [0.02, 0.03, 0.99, 0.00, 0.10]
```

Use fixture identifier `offline-hosted-shape-demo-v1`, corpus version `1`, six stable keys under `tenant/acme/docs/`, and HTTPS example source URLs. Implement validation with duplicate-key/name `HashSet`s, `component.is_finite()`, exact vector length, and squared norm tolerance:

```rust
fn validate_vector(label: &str, vector: &[f32], dimensions: usize) -> AppResult<()> {
    if vector.len() != dimensions {
        return Err(format!("{label} must contain {dimensions} dimensions, found {}", vector.len()).into());
    }
    if vector.iter().any(|component| !component.is_finite()) {
        return Err(format!("{label} contains a non-finite component").into());
    }
    let norm = vector.iter().map(|value| f64::from(*value).powi(2)).sum::<f64>().sqrt();
    if (norm - 1.0).abs() > 1e-4 {
        return Err(format!("{label} must be unit-normalized; norm={norm}").into());
    }
    Ok(())
}
```

- [ ] **Step 4: Run the focused tests and verify GREEN**

Run the Task 1 test command. Expected: all fixture tests pass with zero warnings.

- [ ] **Step 5: Commit the fixture contract**

```sh
git add stores/prolly-store-sqlite/Cargo.toml \
  stores/prolly-store-sqlite/examples/semantic_rag.rs \
  stores/prolly-store-sqlite/examples/data/semantic_rag_embeddings.json
git commit -m "docs(sqlite): add offline RAG fixture validation"
```

### Task 2: Exact semantic retrieval and context rendering

**Files:**
- Modify: `stores/prolly-store-sqlite/examples/semantic_rag.rs`

**Interfaces:**
- Consumes: validated `Fixture`, `ProximityMap<Arc<SqliteStore>>`, `SearchRequest`, and JSON-encoded `ChunkMetadata` values.
- Produces: `build_map(Arc<SqliteStore>, &Fixture)`, `retrieve(&ProximityMap<Arc<SqliteStore>>, &Fixture, &str)`, `RetrievedChunk`, and `render_context(&[RetrievedChunk]) -> String`.

- [ ] **Step 1: Write failing ranking and rendering tests**

Add tests that open an in-memory `SqliteStore`, build the six records, and assert:

```rust
#[test]
fn password_query_returns_ranked_cited_chunks() {
    let fixture = load_fixture().unwrap();
    let store = Arc::new(SqliteStore::open_in_memory().unwrap());
    let map = build_map(store, &fixture).unwrap();
    let hits = retrieve(&map, &fixture, "password-reset").unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(hits[0].metadata.section, "Reset a forgotten password");
    assert!(hits.iter().all(|hit| hit.key.starts_with(b"tenant/acme/docs/")));
}

#[test]
fn context_contains_numbered_sources_and_no_generated_answer() {
    let fixture = load_fixture().unwrap();
    let map = build_map(Arc::new(SqliteStore::open_in_memory().unwrap()), &fixture).unwrap();
    let hits = retrieve(&map, &fixture, "rotate-api-key").unwrap();
    let context = render_context(&hits);
    assert!(context.starts_with("<context>\n"));
    assert!(context.contains("[1] Rotate an API key"));
    assert!(context.contains("Source: https://docs.example.com/security/api-keys"));
    assert!(context.ends_with("</context>"));
}
```

- [ ] **Step 2: Run the ranking tests and verify RED**

Run the focused example test command. Expected: failure because `build_map`, `retrieve`, and `render_context` are absent or deliberately unimplemented.

- [ ] **Step 3: Implement minimal build, search, decode, and render behavior**

Build `ProximityRecord`s by serializing `ChunkMetadata` with `serde_json::to_vec`. Configure cosine distance and external-vector threshold suitable for 1,536 dimensions. Search with:

```rust
let mut request = SearchRequest::exact(query_embedding, 3);
request.policy = SearchPolicy::Exact;
request.filter = ProximityFilter::Prefix(CORPUS_PREFIX);
let result = map.search(request)?;
```

Decode every returned value, preserve the returned key and distance, and render numbered title/section/source/text entries inside one context block. Call `map.verify()` after construction.

- [ ] **Step 4: Run focused tests and verify GREEN**

Run the Task 1 command. Expected: validation, ranking, and rendering tests all pass.

- [ ] **Step 5: Commit retrieval behavior**

```sh
git add stores/prolly-store-sqlite/examples/semantic_rag.rs
git commit -m "docs(sqlite): demonstrate exact semantic retrieval"
```

### Task 3: Durable named-root lifecycle, CLI, and documentation

**Files:**
- Modify: `stores/prolly-store-sqlite/examples/semantic_rag.rs`
- Modify: `stores/prolly-store-sqlite/README.md`
- Modify: `README.md`

**Interfaces:**
- Consumes: Task 2's validated fixture, map builder, retriever, renderer, `SqliteStoreConfig`, and typed content-root APIs.
- Produces: `CorpusState::{Built, Reopened}`, `open_or_build_corpus`, CLI output, and documented two-run commands.

- [ ] **Step 1: Write a failing SQLite reopen test**

Use a unique path under `std::env::temp_dir()`, remove the database plus `-wal`/`-shm` before and after the test, and assert:

```rust
#[test]
fn sqlite_named_root_reopens_the_same_verified_proximity_map() {
    let fixture = load_fixture().unwrap();
    let path = temp_db_path("semantic-rag");
    remove_sqlite_files(&path);
    let descriptor = {
        let store = durable_store(&path).unwrap();
        let (map, state) = open_or_build_corpus(store, &fixture).unwrap();
        assert_eq!(state, CorpusState::Built);
        map.tree().descriptor.clone()
    };
    {
        let store = durable_store(&path).unwrap();
        let (map, state) = open_or_build_corpus(store, &fixture).unwrap();
        assert_eq!(state, CorpusState::Reopened);
        assert_eq!(map.tree().descriptor, descriptor);
        assert_eq!(retrieve(&map, &fixture, "lost-2fa").unwrap().len(), 3);
    }
    remove_sqlite_files(&path);
}
```

- [ ] **Step 2: Run the reopen test and verify RED**

Run the focused example test command. Expected: failure because durable publication/reopen is absent.

- [ ] **Step 3: Implement durable publication and strict reopen validation**

Open `Arc<SqliteStore>` with `busy_timeout_ms: 5_000`, WAL enabled, and `synchronous_normal: false`. On first run, publish:

```rust
ContentRootManifest {
    root: TypedContentRoot::proximity_descriptor(map.tree().descriptor.clone()),
    logical_version: 1,
    created_at_millis: 0,
    metadata: BTreeMap::from([
        (b"corpus-version".to_vec(), b"1".to_vec()),
        (b"embedding-model".to_vec(), fixture.embedding_model.as_bytes().to_vec()),
        (b"dimensions".to_vec(), b"1536".to_vec()),
    ]),
}
```

On reopen, require `ContentObjectKind::ProximityDescriptor`, no root-level PRXN dimension context, logical version `1`, and exact metadata values including `dimensions=1536` before calling `ProximityMap::load` and `verify`.

- [ ] **Step 4: Implement the CLI and human-readable output**

Require exactly `<database-path> <query-name>`. Print whether the corpus was built or reopened, descriptor CID, ranked results with distances and excerpts, then the context block and the sentence `Answer generation omitted: pass this context to your LLM.`

- [ ] **Step 5: Run tests and two real process invocations**

Run:

```sh
rm -f ./target/semantic-rag.sqlite ./target/semantic-rag.sqlite-wal ./target/semantic-rag.sqlite-shm
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --example semantic_rag
cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- ./target/semantic-rag.sqlite password-reset
cargo run --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- ./target/semantic-rag.sqlite lost-2fa
```

Expected: tests pass; first process prints `built`, second prints `reopened`; both return three relevant chunks and a context block.

- [ ] **Step 6: Add discoverability documentation**

Document the exact command in the adapter README, explain that vectors are synthetic precomputed fixtures with hosted-model-shaped dimensions, and link the example from the root README's examples/store section.

- [ ] **Step 7: Run final verification**

```sh
cargo fmt --all -- --check
cargo clippy --manifest-path stores/prolly-store-sqlite/Cargo.toml \
  --example semantic_rag -- -D warnings
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml
cargo test --all-features
git diff --check
```

Expected: every command exits zero with no warnings or failures.

- [ ] **Step 8: Commit the durable example and docs**

```sh
git add stores/prolly-store-sqlite/examples/semantic_rag.rs \
  stores/prolly-store-sqlite/README.md README.md
git commit -m "docs(sqlite): add durable semantic RAG example"
```
