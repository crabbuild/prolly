# Prolly Binding Verification Matrix

This matrix maps major Rust `prolly-map` API groups to the language binding
tests that exercise them. The goal is to keep every binding on the same
behavioral contract while letting each language expose idiomatic names and
async wrappers.

## Major API Groups

| Group | Behavior verified |
| --- | --- |
| Core tree | `create`, `get`, `get_many`, `put`, `delete`, `batch`, bulk build, sorted bulk build, append batch |
| Range/page | `range`, `range_after`, cursor resumption, range pages, diff pages |
| Wire/helpers | compact `CRAB` nodes, CIDs, config, boundary decisions, key helpers, value/blob envelopes, root manifests |
| Diff/merge | eager diff, range diff, conflict pages, built-in resolvers, merge explanations, range/prefix merge |
| Host callbacks | custom merge resolvers, custom CRDT resolvers, custom merge policies, custom stores |
| Stores/roots | memory, file, SQLite, SQLite in-memory, named roots, snapshot namespaces, CAS, retention |
| Operational | stats JSON, debug text/JSON, cache pin/clear stats, metrics reset, hints |
| Data flows | large values, blob stores, blob GC, node GC, missing-node sync, CRDT helpers, tombstones |
| Async/context | Promise, coroutine, `CompletableFuture`, Ruby `Future`, Swift `async` wrapper follow-ups, and Go `context.Context` wrappers where available |
| Cookbook scenarios | Runnable per-scenario examples mirroring the Rust cookbook: map, bulk build, local-first roots, merge policies, CRDT helpers, memory/log workflows, RAG/chunk/vector/provenance patterns, derived views, blob/filesystem storage, and durable SQLite where supported |

## Binding Coverage

| Binding | Verification files | Command |
| --- | --- | --- |
| Rust UniFFI facade | `bindings/uniffi/src/lib.rs` unit tests | `cargo test -p prolly-bindings` |
| Python | `bindings/python/tests/test_uniffi_binding.py`, `test_fixtures.py` | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" PYTHONPATH=crates/prolly/bindings/python python3 -m unittest discover -s crates/prolly/bindings/python/tests` |
| Go | `bindings/go/prolly_test.go` | `(cd crates/prolly/bindings/go && go test ./...)` |
| Node/TypeScript | `bindings/node/test/*.test.ts` | `npm --prefix crates/prolly/bindings/node run build:native && npm --prefix crates/prolly/bindings/node test` |
| Browser WASM | `bindings/wasm/test/wasm.test.ts` | `cargo check -p prolly-wasm --target wasm32-unknown-unknown && npm --prefix crates/prolly/bindings/wasm test` |
| Kotlin/JVM | `bindings/kotlin/src/test/kotlin/build/crab/prolly/*.kt` | `mvn -f crates/prolly/bindings/kotlin/pom.xml test` |
| Java | `bindings/java/src/test/java/build/crab/prolly/*.java` | `mvn -f crates/prolly/bindings/java/pom.xml test` |
| JVM aggregate | Kotlin and Java modules together | `mvn -f crates/prolly/bindings/pom.xml test` |
| Ruby | `bindings/ruby/test/prolly_smoke_test.rb` | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile BUNDLE_PATH=/tmp/prolly-ruby-bundle bundle exec ruby -Icrates/prolly/bindings/ruby/lib crates/prolly/bindings/ruby/test/prolly_smoke_test.rb` |
| Swift | `bindings/swift/Examples/FixtureCheck`, cookbook executable targets | `DYLD_LIBRARY_PATH="$PWD/target/debug" swift run --package-path crates/prolly/bindings/swift prolly-fixture-check` |

## Runnable Cookbook Scenarios

These example programs mirror the Rust examples with real assertions and short
success output. Each binding keeps separate scenario files. Native bindings
cover the full application set: `batch_build`, `local_first_state`, `resolver`,
`crdt_merge`, `conversation_memory`, `agent_event_log`,
`background_compaction`, `deterministic_rag_snapshot`,
`document_chunk_index`, `vector_sidecar`, `provenance_values`,
`materialized_view`, `filesystem_snapshot`, and `durable_sqlite`, alongside
the existing `basic_map`, `diff_merge`, `file_blob_store`, and
`secondary_index`. Browser WASM keeps the browser-safe subset and replaces
native file/SQLite scenarios with `browser_storage`.

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python3 crates/prolly/bindings/python/examples/cookbook_scenarios.py
(cd crates/prolly/bindings/go && go run ./examples/cookbook_scenarios)
npm --prefix crates/prolly/bindings/node run example:cookbook
mvn -q -f crates/prolly/bindings/kotlin/pom.xml compile \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenariosKt exec:java
mvn -q -f crates/prolly/bindings/pom.xml install -Dmaven.test.skip=true
mvn -q -f crates/prolly/bindings/java/pom.xml compile \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenarios exec:java
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=crates/prolly/bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec ruby -Icrates/prolly/bindings/ruby/lib \
  crates/prolly/bindings/ruby/examples/cookbook_scenarios.rb
npm --prefix crates/prolly/bindings/wasm run build:wasm
npm --prefix crates/prolly/bindings/wasm run example:cookbook
DYLD_LIBRARY_PATH="$PWD/target/debug" \
  swift run --package-path crates/prolly/bindings/swift prolly-cookbook-scenarios
```

## Release Gate

Before publishing a binding release:

1. Build the Rust facade with `cargo build -p prolly-bindings`.
2. If `bindings/uniffi/src/lib.rs` changed, regenerate checked-in UniFFI
   language glue using each package's `PROVENANCE.md` command, then review the
   generated diff.
3. Run the command for every binding listed above.
4. Run `git diff --check`.
5. Confirm no generated local artifacts are checked in accidentally:
   `node_modules`, local `.node` binaries, Maven `target`, Python
   `__pycache__`, Ruby `Gemfile.lock` from local Bundler runs, SwiftPM
   `.build`, and WASM `pkg`.
6. Update the binding cookbook when adding or renaming a user-visible API.
