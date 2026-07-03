# Roadmap: make `prolly-map` a portable storage layer

This roadmap is the single source of truth for `prolly-map` planning. It merges the older implementation tracker with the docs roadmap, so planning lives beside the rest of the Prolly documentation.

`prolly-map` already has a strong Rust foundation: immutable tree handles, content-addressed nodes, structural sharing, batch mutation, diff, merge, delete-aware resolvers, conflict-free merge, async store support, named roots, garbage collection, blob offload, conformance tests, examples, and docs. The next phase should turn that foundation into a stable public crate, harden storage operations, and prepare the format for future Python, TypeScript, WebAssembly (WASM), and other language ports.

## How to read this roadmap

This page tracks both shipped work and planned work so contributors can see why an item matters before opening an issue or pull request.

Status labels:

- **Shipped**: implemented in the Rust crate
- **Planned**: high-confidence work for the next release bands
- **Candidate**: useful but not yet committed to a milestone
- **Blocked**: waiting on design, fixtures, or backend choices

Priority labels:

- **P0**: required before a serious public `0.1` release or early adopters
- **P1**: high-value production work after the API settles
- **P2**: broader ecosystem, language-port, or specialized application work

## Current foundation

The Rust crate can already support non-trivial local-first and AI-native storage workflows. Treat this section as the baseline for docs, examples, and compatibility tests.

### Core map engine

Status: **Shipped**

- Immutable ordered byte-key map
- Content-addressed nodes with SHA-256 content identifiers (CIDs)
- Deterministic content-defined chunking
- Structural sharing between snapshots
- Point reads and ordered range scans
- Resumable range pages and cursors
- Single-key `put` and `delete`
- Batch mutation with route planning and coalescing
- Append-heavy right-edge hints
- Bulk builders for initial imports
- Tree stats, stats diffs, metrics, and debug views
- Bounded node cache by node count and serialized bytes
- Cache inspection, pinning, clearing, hit/miss counters, and eviction counters

### Diff, merge, and collaboration

Status: **Shipped**

- Full diff and range diff
- Streaming diff and structural diff cursors
- Three-way merge
- Delete-aware conflict shape with explicit `None` for absence
- Built-in standard resolvers: prefer-left, prefer-right, delete-wins, update-wins
- Custom standard resolvers that return value, delete, or unresolved
- Conflict streaming for user interfaces and agent workflows
- Range-limited merge
- Merge explanation traces
- Conflict-free merge with last-writer-wins, multi-value, and custom strategies
- Merge policy registry by prefix, exact key, or custom matcher
- Tombstone helpers for sync-heavy logical deletes

### Storage layer

Status: **Shipped**

- `Store` trait for synchronous byte stores
- `AsyncStore` trait behind the `async-store` feature
- `AsyncProlly` manager for async reads, writes, range scans, diff, merge, stats, and batch mutation
- Sync-store-as-async adapter
- Tokio blocking adapter behind the `tokio` feature
- `Arc<T>` store support
- In-memory store
- File node store with object-style CID layout
- SQLite store behind `sqlite`
- Optional RocksDB, SlateDB, and PGlite stores
- Ordered batch reads and unique ordered reads
- Store performance hints
- Store conformance tests for sync and async traits
- Optional backend tests for RocksDB, SlateDB, SQLite, and PGlite

### Roots, retention, and sync

Status: **Shipped**

- `RootManifest`, `NamedRoot`, `ManifestStore`, and compare-and-swap results
- High-level load, publish, delete, and compare-and-swap helpers for named roots
- Manifest timestamp metadata
- Named-root retention policies
- Reachability planning for retained roots
- Store-native garbage collection (GC) helpers
- Missing-node planning with `plan_missing_nodes`
- Missing-node copy with byte verification
- Sync patterns for local stores and async stores

### Large values and blob storage

Status: **Shipped**

- `BlobRef`, `ValueRef`, `BlobStore`, and `AsyncBlobStore`
- In-memory blob store
- File blob store
- Sync and Tokio blob adapters
- Large value offload through `put_large_value`
- Inline-vs-blob threshold policy
- Blob reachability and sweep helpers
- Blob GC examples

### API ergonomics

Status: **Shipped**

- Crate package name: `prolly-map`
- Rust library crate name: `prolly`
- Public re-exports from the crate root
- Typed key helpers with prefix ranges, escaped composite segments, integers, timestamps, and debug rendering
- JSON and CBOR helpers
- `ValueCodec` trait
- Versioned value envelope
- Reusable JSON, CBOR, versioned JSON, and versioned CBOR codec objects
- Feature-flag docs for `async-store`, `tokio`, `sqlite`, `rocksdb`, `pglite`, and `slatedb`

### AI-native and local-first examples

Status: **Shipped**

- Basic map operations
- Batch build and stats
- Diff and merge
- Resolver patterns
- Conflict-free custom merge
- Conversation memory
- Agent event logs
- Background compaction
- Deterministic retrieval-augmented generation (RAG) snapshots
- Document chunk indexes
- Vector sidecars
- Provenance-rich values
- Secondary indexes
- Materialized views
- File blob store and blob GC

## Public `0.1` release

Goal: publish a crate that early adopters can use without reading internal modules.

Status: **Planned**  
Priority: **P0**

### Shipped release work

- Package identity is set to `prolly-map`
- Library imports use `prolly`
- README explains package and crate naming
- Examples compile and run
- Doctests pass
- Store conformance tests exist
- Async-store docs exist
- Cookbook, guides, architecture, implementation notes, design spec, and language-port docs exist
- Delete-aware resolver semantics are documented
- Root manifest and GC behavior are documented
- Compatibility policy is documented at a high level

### Remaining release work

- [ ] Review public re-exports and hide accidental internals before publishing
- [ ] Decide which modules are stable API and which are explicitly experimental
- [ ] Add a `CHANGELOG.md` scoped to `prolly-map`
- [ ] Add docs.rs landing-page metadata and badges
- [ ] Run `cargo publish --dry-run -p prolly-map` in release CI
- [ ] Add release checklist to `docs/implementation.md`
- [ ] Confirm every feature combination used by examples compiles in CI
- [ ] Add a short migration note for breaking changes before `0.1`
- [ ] Decide whether root-level planning files under `crates/prolly/` should move into `docs/`

### Acceptance criteria

- `cargo test -p prolly-map --all-targets` passes on default features
- `cargo test -p prolly-map --features async-store` passes
- `cargo test -p prolly-map --features tokio` passes
- `cargo test -p prolly-map --features sqlite` passes
- `cargo test -p prolly-map --doc` passes
- `cargo test -p prolly-map --examples` passes
- Every example listed in the cookbook runs successfully
- `cargo package -p prolly-map --allow-dirty --no-verify` includes `docs/`

## Compatibility and format stability

Goal: make compatibility promises explicit before other languages or durable stores depend on them.

Status: **Planned**  
Priority: **P0**

### Shipped foundation

- Deterministic node bytes drive CIDs
- Legacy CBOR node decoding is covered by tests
- Empty values remain distinct from deletion
- Merge conflicts preserve absence as `None`
- Store conformance tests cover missing reads, duplicate batch reads, ordered reads, hints, and writes
- Value envelopes include schema and version metadata

### Remaining work

- [ ] Create fixture files for node bytes, CIDs, roots, manifests, diffs, merges, and blob refs
- [ ] Add a fixture generator binary or test helper
- [ ] Add cross-version read tests for persisted node and manifest bytes
- [ ] Document which fields affect CIDs and which are local metadata
- [ ] Define a persisted-format migration policy
- [ ] Decide whether node encoding is stable for `0.1` or still explicitly unstable
- [ ] Add compatibility labels to APIs: stable, experimental, internal
- [ ] Add error examples for missing nodes, malformed nodes, and invalid manifests

### Acceptance criteria

- A future Python read-only implementation can pass lookup and range fixtures
- A future writer can prove byte-for-byte node encoding compatibility before claiming structural compatibility
- Docs distinguish API compatibility, logical compatibility, structural compatibility, and wire compatibility

## Production storage hardening

Goal: make embedded and durable stores safe to run in real applications.

Status: **Planned**  
Priority: **P1**

### Shipped foundation

- Memory, file, SQLite, RocksDB, SlateDB, and PGlite stores exist
- Store conformance helpers cover the common store contract
- Named roots support CAS where backend semantics are clear
- GC can plan and sweep from retained roots
- File node store verifies node bytes against requested CIDs
- Missing-node copy verifies transferred bytes

### Remaining work

- [ ] Add backend-specific crash-safety notes
- [ ] Document transaction boundaries for every durable store
- [ ] Add backup and restore recipes for SQLite and file stores
- [ ] Add corruption detection examples for node stores and blob stores
- [ ] Add store health-check APIs or recipes
- [ ] Add recovery guidance for missing nodes
- [ ] Add GC safety guide for multi-writer systems
- [ ] Add import and export tooling for portable snapshots
- [ ] Add store-size and reclaimable-byte reports to inspection tooling
- [ ] Add optional encryption-at-rest guidance for file and object stores

### Acceptance criteria

- Each durable backend documents atomicity, batch behavior, manifest behavior, scan behavior, and backup requirements
- GC docs state exactly which roots must be retained before sweeping
- Operators can dry-run GC and understand what will be removed

## Async and remote storage

Goal: make object stores, peer sync, browser storage, and remote caches first-class deployment targets.

Status: **Planned**  
Priority: **P1**

### Shipped foundation

- `async-store` is optional
- Tokio is optional
- `AsyncStore` supports ordered batch reads
- `read_parallelism` can overlap default async point reads
- `AsyncProlly` covers read, write, range, diff, merge, batch, CRDT merge, stats, and cache operations
- Sync-store and Tokio adapters exist
- Async blob store traits exist
- File node store models object-style CID layout locally

### Remaining work

- [ ] Add an S3/R2/object-store backend
- [ ] Add an HTTP peer backend or sync example
- [ ] Add a browser/WASM store prototype
- [ ] Add async named-root manifest traits if remote roots need async CAS
- [ ] Add cancellation-aware long range scans
- [ ] Add background prefetch for hot internal nodes
- [ ] Add request coalescing for repeated CID reads
- [ ] Add retry and backoff guidance for remote stores
- [ ] Add object-store consistency notes for roots and blobs
- [ ] Add async examples to the cookbook

### Acceptance criteria

- A remote store can overlap child-node reads during traversal
- Object-store node writes are idempotent
- Root publication uses conditional writes or a documented external coordinator
- Browser storage can implement async traits without Tokio or `Send`

## Merge, diff, and collaboration

Goal: make collaboration workflows explainable, resumable, and safe for domain-specific records.

Status: **Planned**  
Priority: **P1**

### Shipped foundation

- Standard merge handles disjoint changes
- Resolvers can return value, delete, or unresolved
- CRDT custom merge returns value or delete
- Conflict streaming supports incremental inspection
- Range-limited merge supports partitioned keyspaces
- Merge policy registry composes key-specific rules
- Merge explanation traces report reuse, fallback, resolver calls, and conflicts
- Structural delete resolutions fall back when rebalancing requires the batch path

### Remaining work

- [ ] Add structured-value resolver examples for common records
- [ ] Add persistent conflict-log recipe
- [ ] Add multi-party merge helper or guide
- [ ] Add policy registry examples by key schema
- [ ] Add benchmark coverage for long branch divergence
- [ ] Add UI-oriented conflict summaries
- [ ] Add merge trace examples to docs
- [ ] Add more property tests for CRDT custom strategies

### Acceptance criteria

- Applications can preview conflicts without allocating the full diff
- Delete/update conflicts are visible to users and resolvers
- Merge traces explain why the engine reused, rewrote, or fell back
- Domain resolvers can be tested without full application state

## AI-native application primitives

Goal: turn the example patterns into reusable storage building blocks without bloating the core map.

Status: **Planned**  
Priority: **P1**

### Shipped foundation

- Conversation memory example
- Agent event log example
- Background compaction example
- Deterministic RAG snapshot example
- Document chunk index example
- Vector sidecar example
- Provenance value example
- Materialized view example
- Cookbook recipes explain how each pattern works

### Remaining work

- [ ] Add memory branch helper patterns
- [ ] Add provenance envelope helper crate or module after APIs settle
- [ ] Add deterministic RAG fixture set
- [ ] Add context snapshot export and import recipes
- [ ] Add agent attempt lifecycle guide
- [ ] Add retention policies for memory and event logs
- [ ] Add audit-log recipe built from root diffs
- [ ] Add replay tests for RAG answers after index updates
- [ ] Add examples that combine event logs, memory roots, and provenance values

### Acceptance criteria

- Users can build an agent memory store from docs alone
- RAG examples show how to reproduce old answers after current indexes change
- Provenance examples capture source, parser, embedding, model, prompt, and parent CIDs
- Compaction examples preserve retained roots before sweeping

## Indexing and query layers

Goal: keep the core tree an ordered map while giving users documented patterns for derived access paths.

Status: **Planned**  
Priority: **P2**

### Shipped foundation

- Secondary-index example
- Materialized-view example
- Diff-based index updates
- Prefix key conventions
- Range scan and range page APIs
- Tree manifests can publish source and view roots

### Remaining work

- [ ] Add a secondary-index helper crate or module
- [ ] Add materialized-view helper patterns
- [ ] Add source/view manifest helper APIs
- [ ] Add drift-check tooling for derived indexes
- [ ] Add multi-index batch update examples
- [ ] Add range query planner examples for common key layouts
- [ ] Add full-text sidecar integration recipe
- [ ] Add schema migration helpers for key layout changes

### Acceptance criteria

- Index updates can be derived from source diffs
- View manifests record source and view snapshots
- Index rebuilds can verify incremental updates
- The core crate does not become a SQL engine

## Observability and developer experience

Goal: make tree behavior visible enough that users can tune, debug, and trust it.

Status: **Planned**  
Priority: **P1**

### Shipped foundation

- `collect_stats`
- `stats_diff`
- debug tree views
- debug tree comparisons
- manager metrics
- CLI inspection tooling
- Benchmarks for core, storage, and AI workloads
- Performance hardening notes

### Remaining work

- [ ] Add richer `prolly-inspect` output for manifests, blobs, and GC plans
- [ ] Add visual tree diff output
- [ ] Add root manifest browser command
- [ ] Add store sync progress reports
- [ ] Add benchmark reports for cookbook workloads
- [ ] Add docs that interpret stats and fill-factor reports
- [ ] Add flamegraph or tracing recipes for large workloads
- [ ] Add CI benchmark smoke checks for regression detection

### Acceptance criteria

- Users can inspect root shape, changed spans, and retained nodes
- Benchmarks map to documented workloads
- Metrics tell users whether stores, caches, or chunking cause bottlenecks

## Language ports

Goal: let Python, TypeScript, WASM, and other implementations interoperate without changing the storage contract.

Status: **Planned**  
Priority: **P2**

### Shipped foundation

- Rust source of truth
- Language-porting guide
- Byte-key ordering spec
- Conflict semantics spec
- Value envelope APIs
- Public examples that can become fixture datasets

### Remaining work

- [ ] Build fixture generator from Rust
- [ ] Publish fixture JSON with hex-encoded keys, values, node bytes, CIDs, and manifests
- [ ] Add Python read-only inspector prototype
- [ ] Add Python lookup and range implementation against fixtures
- [ ] Add Python writer after node encoding fixtures are stable
- [ ] Add TypeScript or WASM browser proof of concept
- [ ] Add shared conformance runner for non-Rust ports
- [ ] Decide whether ports use native code, WebAssembly, or both

### Acceptance criteria

- A read-only port can load Rust-generated roots and pass lookup/range tests
- A writer port can produce matching CIDs before claiming structural compatibility
- Cross-language docs explain which compatibility level each port supports

## Milestone plan

Use these milestones to group issues and pull requests.

### Milestone 1: public `0.1`

Status: **Planned**  
Priority: **P0**

- Finish public API audit
- Move planning docs into `docs/`
- Add changelog
- Confirm packaging metadata
- Run release CI gates
- Publish docs.rs output
- Document compatibility boundaries clearly

### Milestone 2: production storage core

Status: **Planned**  
Priority: **P1**

- Document backend durability and transaction semantics
- Add backup and restore recipes
- Add GC safety guide
- Add corruption and missing-node diagnostics
- Expand inspection tooling for manifests and blobs
- Add import/export tooling

### Milestone 3: async remote storage

Status: **Planned**  
Priority: **P1**

- Prototype object-store backend
- Prototype remote peer sync
- Prototype browser/WASM storage
- Add async manifest strategy if needed
- Add cancellation and retry guidance
- Add async cookbook examples

### Milestone 4: AI-native storage kit

Status: **Planned**  
Priority: **P1**

- Turn event-log, memory, RAG, provenance, and compaction patterns into reusable guides
- Add fixtures for deterministic RAG snapshots
- Add audit-log recipes from root diffs
- Add retention policies for memory and event logs
- Add source/view manifest helpers

### Milestone 5: cross-language compatibility

Status: **Planned**  
Priority: **P2**

- Add fixture generator
- Add node encoding fixtures
- Add manifest and blob fixtures
- Build Python read-only inspector
- Build TypeScript or WASM proof of concept
- Publish compatibility test runner

### Milestone 6: `1.0` readiness

Status: **Candidate**  
Priority: **P2**

- Freeze public Rust API
- Freeze or version persisted formats
- Publish migration tooling for any format changes
- Commit to compatibility levels
- Document supported backend matrix
- Document long-term security and maintenance policy

## Parking lot

These ideas are useful, but they should not block `0.1`.

- Domain-specific resolver packs outside the core crate
- Higher-level query planner
- Full-text helper integration
- Hosted sync service
- Encrypted object-store backend
- Signed root manifests
- Visual UI for tree diffs
- Automatic compaction scheduler
- Schema-aware value migrations
- Multi-party merge coordinator
