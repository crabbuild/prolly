# Prolly Language Bindings

This directory contains the shared Rust binding facade and each language package
for the Rust `prolly-map` reference implementation.

- `uniffi`: Rust FFI facade and UniFFI generator config shared by generated
  bindings.
- `python`: Rust-backed UniFFI package plus the temporary fixture harness, key
  helpers, boundary checks, logical diff over loaded trees, GC/sync helpers,
  CRDT/tombstone codecs, memory/file blob stores, large-value helpers, and
  value/blob envelope codecs with versioned-value schema guards and blob-ref
  byte validation.
- `node`: Node/TypeScript-oriented `Uint8Array` APIs plus the Rust-backed
  Node-API crate under `node/native` and broad Promise wrappers in
  `node/src/async.ts`.
- `wasm`: browser-oriented `wasm-bindgen` package over the Rust memory engine.
- `go`: Go `[]byte` API over the UniFFI-exported Rust ABI through cgo, plus
  broad `context.Context` wrappers.
- `kotlin`: generated Kotlin/JVM UniFFI sources using package
  `build.crab.prolly`, plus broad suspend wrappers.
- `java`: Java-friendly facade over the Kotlin/JVM UniFFI artifact, plus
  broad `CompletableFuture` wrappers.
- `ruby`: generated Ruby UniFFI gem scaffold using binary `String` values,
  plus dependency-free `Future`/`AsyncEngine`/`AsyncBlobStore` wrappers.
- `swift`: Swift Package Manager module with generated UniFFI Swift sources,
  runnable fixture checks, and cookbook scenario executables.

Language bindings consume `crates/prolly/conformance/prolly-fixtures.v1.json`.
The Go, Node native, Kotlin/JVM, Java facade, Ruby, and Swift packages now run
fixture coverage for:

- `CRAB` node decode/encode/CID parity;
- boundary decisions and key helpers, including prefix bounds and segment
  encoding/decoding plus composite key construction;
- encoding helpers plus tree, large-value, and parallel config constructors;
- memory-engine write/get/range/prefix with Rust root CID parity;
- eager diff parity for the shared diff fixture;
- versioned value byte round trips and schema match/require guards,
  value-ref/blob envelope round trips plus stored-byte/inline-escape helpers,
  blob-ref byte validation, and root manifest byte round trips.
- batch last-write-wins, Rust bulk-build and sorted bulk-build root parity,
  append-batch, parallel batch, batch/append/parallel batch execution statistics, mutation helper
  constructors, `get_many`, prefix scans/pages, range-after/cursor resumption with cursor helper
  constructors, ordered boundary helpers, cursor windows, range pages, reverse and prefix-reverse pages, cursor-resumed diffs, diff pages, typed structural
  diff cursors with JSON compatibility, and paged three-way conflict inspection;
- three-way merge with built-in resolver names and merge explanations with
  typed trace events plus JSON trace compatibility;
- host-language custom merge resolver callbacks for Python, Go, Node/TypeScript,
  Kotlin/JVM, Java facade, Ruby, and Swift, including range/prefix merge
  variants where the Rust facade exposes them, plus merge resolution helper
  constructors, typed merge-trace resolver events, and built-in resolver helper
  functions;
- host-language custom CRDT resolver callbacks for Python, Go, Node/TypeScript,
  Kotlin/JVM, Java facade, and Ruby, plus CRDT resolution helper constructors;
- merge policy registries with built-in rule names and host-language custom
  policy callbacks for Python, Go, Node/TypeScript, Kotlin/JVM, Java facade,
  and Ruby;
- callback-backed custom stores for Python, Go, Node/TypeScript, Kotlin/JVM,
  Java facade, Ruby, and Swift, covering node bytes, ordered reads, optional
  hints, node scans, named-root manifests, CAS, store GC planning, and
  missing-node sync plus portable snapshot bundle export/import and canonical
  snapshot bundle bytes, digests, summaries, and self-contained verification;
- named root publish/load/list/manifest-list/batch-load/CAS and retention
  policy constructor/selection smoke coverage, retained named-root GC planning/sweeping, plus
  snapshot namespace helpers for branch/tag/checkpoint/custom root naming and snapshot
  publish/load/list/CAS/delete flows where the binding exposes durable named
  roots. Browser WASM exposes the portable namespace naming helpers for
  application-managed snapshot roots;
- browser WASM memory-engine coverage for P0/P1 plus prefix scans/pages, ordered boundary helpers, cursor windows, range/diff pages,
  range diff, structural diff pages with typed cursor resume,
  merge/conflict inspection, stats/debug
  helpers, and fixture runtime checks when generated `pkg/` artifacts are
  present;
- async/context wrapper smoke coverage for Go, Node/TypeScript, Java, Kotlin,
  and Ruby create/read/write, range/diff, merge, named-root, stats/cache,
  hint, GC/sync, large-value, and blob-store flows;
- file-store reopen, SQLite reopen, and SQLite-in-memory smoke coverage for
  Go, Node native, Kotlin/JVM, Java facade, Ruby, and Swift where the package
  exposes that store kind;
- typed stats/debug records, stats/debug JSON and text views, cache pin/clear stats, engine metrics
  reset, changed-span helper constructors, and performance hint
  publication/hydration smoke coverage;
- store-independent single-key, shared multi-key, range, cursor-page,
  diff-page, and prefix proof generation, compact path-node export/import,
  canonical proof bundle bytes, proof-bundle introspection/routing summaries,
  one-shot proof-bundle verification, HMAC-authenticated proof envelopes,
  one-shot authenticated proof-bundle verification, and verification;
- structural diff pages, node reachability/GC plan/sweep, store GC no-op
  checks for live roots, retained named-root GC checks, and missing-node
  planning/copy sync between memory engines, plus complete snapshot bundle
  export/import into fresh engines through in-memory records and canonical
  bytes with digest, summary, and verification helpers;
- memory blob stores, direct blob put/get/delete/list/count, large-value
  indirection, value-ref inspection, blob reachability, explicit blob GC, and
  whole blob-store GC, including blob-ref byte validation.
- CRDT merge presets, timestamped value envelopes, multi-value set helpers,
  tombstone envelopes, tombstone upsert mutations, and tombstone compaction
  mutations.

This is meaningful P0/P1 coverage plus broader P2-P6 smoke coverage, including
host callback paths for custom stores, custom merge resolvers, custom CRDT
resolvers, and custom merge-policy registries.

See `VERIFICATION.md` for the release gate and per-language test matrix. Each
binding package also contains a `COOKBOOK.md` with runnable application patterns
for its idiomatic API surface. Python, Go, Node/TypeScript, Java, Kotlin, Ruby,
Swift, and WASM also include executable cookbook scenarios that mirror the Rust
examples: basic map operations, diff/merge, file/blob workflows where the
binding supports blob stores, and a secondary-index application scenario.

Local smoke checks:

```sh
cargo build -p prolly-bindings
cargo test -p prolly-bindings
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python -c "import prolly.uniffi"
mvn -f crates/prolly/bindings/pom.xml test
(cd crates/prolly/bindings/go && go test ./...)
npm ci --prefix crates/prolly/bindings/node
npm --prefix crates/prolly/bindings/node run build:native
npm --prefix crates/prolly/bindings/node test
cargo check -p prolly-wasm --target wasm32-unknown-unknown
npm --prefix crates/prolly/bindings/wasm test
DYLD_LIBRARY_PATH="$PWD/target/debug" \
  swift run --package-path crates/prolly/bindings/swift prolly-fixture-check
BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle install
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec \
  ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/test/prolly_smoke_test.rb
```
