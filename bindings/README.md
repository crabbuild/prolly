# Prolly language bindings

This directory contains the Rust binding facade for `prolly-map` and the
language packages built on top of it. Start here when you need to understand
what each binding owns, what the shared conformance suite covers, and which
commands run the local smoke checks.

## Directory guide

Each package wraps the same Rust reference implementation for a different host
language or runtime:

| Directory | Purpose |
| --- | --- |
| `uniffi` | Shared Rust foreign function interface (FFI) facade and UniFFI generator config |
| `python` | Rust-backed UniFFI package, fixture harness, helpers, stores, codecs, and schema guards |
| `node` | Node and TypeScript `Uint8Array` APIs, native Node-API crate in `node/native`, and Promise wrappers |
| `wasm` | Browser-oriented `wasm-bindgen` package over the Rust memory engine |
| `go` | Go `[]byte` API over the UniFFI-exported Rust ABI through cgo, with `context.Context` wrappers |
| `kotlin` | Generated Kotlin/JVM UniFFI sources in package `build.crab.prolly`, with suspend wrappers |
| `java` | Java facade over the Kotlin/JVM UniFFI artifact, with `CompletableFuture` wrappers |
| `ruby` | Generated Ruby UniFFI gem scaffold with binary `String` values and dependency-free async wrappers |
| `swift` | Swift Package Manager module with generated UniFFI Swift sources, fixture checks, and cookbook executables |

## Shared fixtures

The bindings consume `conformance/prolly-fixtures.v1.json`.

Go, Node native, Kotlin/JVM, Java, Ruby, and Swift run the broadest fixture
coverage. Browser WebAssembly (WASM) focuses on the memory-engine paths and
browser-safe helper APIs.

## Fixture coverage by area

The fixture suite covers parity with Rust, host-language callback behavior, and
the portable data formats that bindings need to exchange.

### Encoding and core map behavior

- `CRAB` node decode, encode, and content identifier (CID) parity
- Boundary decisions and key helpers, including prefix bounds
- Segment encoding and decoding, plus composite key construction
- Tree, large-value, parallel config, and encoding helper constructors
- Memory-engine write, get, range, and prefix behavior with Rust root CID parity
- Eager diff parity for the shared diff fixture
- Versioned value byte round trips and schema match or require guards
- Value-ref and blob envelope round trips
- Stored-byte and inline-escape helpers
- Blob-ref byte validation
- Root manifest byte round trips

### Batches, scans, cursors, and diffs

- Batch last-write-wins behavior
- Rust bulk-build and sorted bulk-build root parity
- Append-batch, parallel batch, and execution statistics
- Mutation helper constructors
- `get_many`
- Prefix scans and prefix pages
- Range-after and cursor resumption with cursor helper constructors
- Ordered boundary helpers and cursor windows
- Range pages, reverse pages, and prefix-reverse pages
- Cursor-resumed diffs and diff pages
- Typed structural diff cursors with JSON compatibility
- Paged three-way conflict inspection

### Merge and resolver behavior

- Three-way merge with built-in resolver names
- Merge explanations with typed trace events and JSON trace compatibility
- Host-language custom merge resolver callbacks for Python, Go, Node/TypeScript,
  Kotlin/JVM, Java, Ruby, and Swift
- Range and prefix merge variants where the Rust facade exposes them
- Merge resolution helper constructors
- Typed merge-trace resolver events
- Built-in resolver helper functions
- Host-language custom Conflict-free Replicated Data Type (CRDT) resolver
  callbacks for Python, Go, Node/TypeScript, Kotlin/JVM, Java, and Ruby
- CRDT resolution helper constructors
- Merge policy registries with built-in rule names
- Host-language custom merge-policy callbacks for Python, Go, Node/TypeScript,
  Kotlin/JVM, Java, and Ruby

### Stores, roots, snapshots, and sync

- Callback-backed custom stores for Python, Go, Node/TypeScript, Kotlin/JVM,
  Java, Ruby, and Swift
- Node bytes, ordered reads, optional hints, node scans, named-root manifests,
  compare-and-swap (CAS), store garbage collection (GC) planning, and
  missing-node sync
- Portable snapshot bundle export and import
- Canonical snapshot bundle bytes, digests, summaries, and self-contained
  verification
- Named root publish, load, list, manifest-list, batch-load, and CAS
- Retention policy constructor and selection smoke coverage
- Retained named-root GC planning and sweeping
- Snapshot namespace helpers for branch, tag, checkpoint, and custom roots
- Snapshot publish, load, list, CAS, and delete flows where the binding exposes
  durable named roots
- Browser WASM namespace naming helpers for application-managed snapshot roots

### Browser WASM runtime behavior

- P0/P1 memory-engine coverage
- Prefix scans, prefix pages, ordered boundary helpers, cursor windows, and
  range pages
- Diff pages, range diffs, and structural diff pages with typed cursor resume
- Merge and conflict inspection
- Stats and debug helpers
- Fixture runtime checks when generated `pkg/` artifacts are present

### Async wrappers and durable stores

- Async and context wrapper smoke coverage for Go, Node/TypeScript, Java,
  Kotlin, and Ruby
- Create, read, write, range, diff, merge, named-root, stats, cache, hint, GC,
  sync, large-value, and blob-store flows
- File-store reopen, SQLite reopen, and SQLite-in-memory smoke coverage for Go,
  Node native, Kotlin/JVM, Java, Ruby, and Swift where the package exposes that
  store kind

### Stats, proofs, and verification

- Typed stats and debug records
- Stats and debug JSON and text views
- Cache pin and clear stats
- Engine metrics reset
- Changed-span helper constructors
- Performance hint publication and hydration smoke coverage
- Store-independent proof generation for single-key, shared multi-key, range,
  cursor-page, diff-page, and prefix proofs
- Compact path-node export and import
- Canonical proof bundle bytes
- Proof-bundle introspection and routing summaries
- One-shot proof-bundle verification
- Hash-based Message Authentication Code (HMAC) proof envelopes
- One-shot authenticated proof-bundle verification

### Garbage collection, blobs, and CRDT helpers

- Structural diff pages
- Node reachability, GC plan, and GC sweep
- Store GC no-op checks for live roots
- Retained named-root GC checks
- Missing-node planning and copy sync between memory engines
- Complete snapshot bundle export and import into fresh engines through
  in-memory records and canonical bytes
- Snapshot digest, summary, and verification helpers
- Memory blob stores
- Direct blob put, get, delete, list, and count
- Large-value indirection
- Value-ref inspection
- Blob reachability
- Explicit blob GC and whole blob-store GC
- Blob-ref byte validation
- CRDT merge presets
- Timestamped value envelopes
- Multi-value set helpers
- Tombstone envelopes
- Tombstone upsert mutations
- Tombstone compaction mutations

## Coverage level

This is meaningful P0/P1 coverage plus broader P2-P6 smoke coverage. The smoke
coverage includes host callback paths for custom stores, custom merge
resolvers, custom CRDT resolvers, and custom merge-policy registries.

## More documentation

- `VERIFICATION.md`: release gate and per-language test matrix
- `*/COOKBOOK.md`: runnable application patterns for each idiomatic API surface

Python, Go, Node/TypeScript, Java, Kotlin, Ruby, Swift, and WASM also include
executable cookbook scenarios. These mirror the Rust examples for basic map
operations, diff and merge, file or blob workflows where supported, and a
secondary-index application scenario.

## Local smoke checks

Run these commands from `crates/prolly`. Each group checks one binding surface.

### Rust and UniFFI facade

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target
```

### Python

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python \
  python -c "import prolly.uniffi"
```

### JVM packages

```sh
mvn -f bindings/pom.xml test
```

### Go

```sh
(cd bindings/go && go test ./...)
```

### Node

```sh
npm ci --prefix bindings/node
npm --prefix bindings/node run build:native
npm --prefix bindings/node test
```

### Browser WASM

```sh
cargo check \
  --manifest-path bindings/wasm/Cargo.toml \
  --target wasm32-unknown-unknown \
  --target-dir target
npm --prefix bindings/wasm test
```

### Swift

```sh
DYLD_LIBRARY_PATH="$PWD/target/debug" \
  swift run --package-path bindings/swift prolly-fixture-check
```

### Ruby

```sh
BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle install
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec \
  ruby -Ibindings/ruby/lib \
  bindings/ruby/test/prolly_smoke_test.rb
```
