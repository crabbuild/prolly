# Prolly Binding Verification Matrix

This matrix maps major Rust `prolly-map` API groups to the language binding
tests that exercise them. The goal is to keep every binding on the same
behavioral contract while letting each language expose idiomatic names and
async wrappers.

## Major API Groups

| Group | Behavior verified |
| --- | --- |
| Core tree | `create`, `get`, `get_many`, `put`, `delete`, `batch`, batch stats, bulk build, sorted bulk build, append batch, parallel batch stats |
| Range/page | `range`, `prefix`, `prefix_page`, `range_after`, ordered boundary helpers, cursor resumption, cursor windows, range pages, reverse and prefix-reverse pages, diff pages |
| Wire/helpers | compact `CRAB` nodes, CIDs, config, boundary decisions, key helpers, value/blob envelopes, root manifests |
| Diff/merge | eager diff, range diff, conflict pages, built-in resolvers, merge explanations, range/prefix merge |
| Host callbacks | custom merge resolvers, custom CRDT resolvers, custom merge policies, custom stores |
| Stores/roots | memory, file, SQLite, SQLite in-memory, named roots, snapshot namespaces, CAS, retention |
| Operational | stats JSON, debug text/JSON, cache pin/clear stats, metrics reset, hints |
| Data flows | large values, blob stores, blob GC, node GC, missing-node sync, CRDT helpers, tombstones |
| Async/context | Promise, coroutine, `CompletableFuture`, Ruby `Future`, Swift `async` wrapper follow-ups, and Go `context.Context` wrappers where available |
| Cookbook scenarios | Runnable per-scenario examples mirroring the Rust cookbook: map, bulk build, local-first roots, merge policies, CRDT helpers, memory/log workflows, RAG/chunk/vector/provenance patterns, derived views, blob/filesystem storage, and durable SQLite where supported |

## Binding Coverage

Native Rust feature gates should be checked before regenerating or publishing
language bindings, because the wrappers assume parity with both sync and async
tree behavior.

| Native surface | Verification files | Command |
| --- | --- | --- |
| Rust async store | `tests/async_store.rs`, async doctests | `cargo test --features async-store` |

| Binding | Verification files | Command |
| --- | --- | --- |
| Rust UniFFI facade | `bindings/uniffi/src/lib.rs` unit tests | `cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target` |
| Python | `bindings/python/tests/test_uniffi_binding.py`, `test_fixtures.py` | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" PYTHONPATH=bindings/python python3 -m unittest discover -s bindings/python/tests` |
| Go | `bindings/go/prolly_test.go` | `(cd bindings/go && go test ./...)` |
| Node/TypeScript | `bindings/node/test/*.test.ts` | `npm --prefix bindings/node run build:native && npm --prefix bindings/node test` |
| Browser WASM | `bindings/wasm/test/wasm.test.ts` | `cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown --target-dir target && npm --prefix bindings/wasm test` |
| Kotlin/JVM | `bindings/kotlin/src/test/kotlin/build/crab/prolly/*.kt` | `mvn -f bindings/kotlin/pom.xml test` |
| Java | `bindings/java/src/test/java/build/crab/prolly/*.java` | `mvn -f bindings/pom.xml -pl java -am test` |
| JVM aggregate | Kotlin and Java modules together | `mvn -f bindings/pom.xml test` |
| Ruby | `bindings/ruby/test/prolly_smoke_test.rb` | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" BUNDLE_GEMFILE=bindings/ruby/Gemfile BUNDLE_PATH=/tmp/prolly-ruby-bundle bundle exec ruby -Ibindings/ruby/lib bindings/ruby/test/prolly_smoke_test.rb` |
| Swift | `bindings/swift/Examples/FixtureCheck`, cookbook executable targets | `DYLD_LIBRARY_PATH="$PWD/target/debug" swift run --package-path bindings/swift prolly-fixture-check` |

## Runnable Cookbook Scenarios

These example programs mirror the Rust examples with real assertions and short
success output. Each binding keeps separate scenario files.

Native bindings cover the full application set: `batch_build`,
`local_first_state`, `resolver`, `crdt_merge`, `conversation_memory`,
`agent_event_log`, `background_compaction`, `deterministic_rag_snapshot`,
`document_chunk_index`, `vector_sidecar`, `provenance_values`,
`materialized_view`, `filesystem_snapshot`, and `durable_sqlite`.

They also keep the existing `basic_map`, `diff_merge`, `file_blob_store`, and
`secondary_index` examples. Browser WASM keeps the browser-safe subset and
replaces native file/SQLite scenarios with `browser_storage`.

```sh
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=bindings/python \
  python3 bindings/python/examples/cookbook_scenarios.py
(cd bindings/go && go run ./examples/cookbook_scenarios)
npm --prefix bindings/node run example:cookbook
mvn -q -f bindings/kotlin/pom.xml compile \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenariosKt exec:java
mvn -q -f bindings/pom.xml install -Dmaven.test.skip=true
mvn -q -f bindings/java/pom.xml compile \
  -Dexec.mainClass=build.crab.prolly.examples.CookbookScenarios exec:java
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  BUNDLE_GEMFILE=bindings/ruby/Gemfile \
  BUNDLE_PATH=/tmp/prolly-ruby-bundle \
  bundle exec ruby -Ibindings/ruby/lib \
  bindings/ruby/examples/cookbook_scenarios.rb
npm --prefix bindings/wasm run build:wasm
npm --prefix bindings/wasm run example:cookbook
DYLD_LIBRARY_PATH="$PWD/target/debug" \
  swift run --package-path bindings/swift prolly-cookbook-scenarios
```

## Release Gate

Before publishing a binding release:

1. Build the Rust facade with `cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target`.
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
