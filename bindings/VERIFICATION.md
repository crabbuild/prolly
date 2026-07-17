# Prolly Binding Verification Matrix

## Public API Inventory Gate

The checked-in [API parity contract](api/parity.json) is generated from
rustdoc JSON and includes every public Rust root export and reachable public
associated item.

~~~sh
cargo +nightly rustdoc --lib --features async-store -- -Z unstable-options --output-format json
python3 scripts/binding_api_inventory.py generate
python3 scripts/binding_api_inventory.py check
~~~

The inventory check rejects missing, stale, malformed, and duplicate contract
entries. Before a binding release, run the stricter gate:

~~~sh
python3 scripts/binding_api_inventory.py check --release
~~~

The release gate also requires implemented symbols for all eight languages,
conformance test IDs, and tested reasons for platform exclusions. A passing
inventory-only check does not establish feature parity while entries remain
planned.

This matrix maps major Rust `prolly-map` API groups to the language binding
tests that exercise them. The goal is to keep every binding on the same
behavioral contract while letting each language expose idiomatic names and
async wrappers.

## Major API Groups

| Group | Behavior verified |
| --- | --- |
| Core tree | `create`, `get`, `get_many`, `put`, `delete`, raw-byte half-open `delete_range(start, end)`, write stats, `batch`, batch stats, bulk build, sorted bulk build, append batch, parallel batch stats |
| Range/page | `range`, `prefix`, streaming forward/reverse range and prefix visitors with early stop, `prefix_page`, `range_after`, ordered boundary helpers, cursor resumption, cursor windows, range pages, reverse and prefix-reverse pages, streaming diff/range-diff/conflict visitors, diff pages |
| Wire/helpers | compact `CRAB` nodes, CIDs, config, key helpers, value/blob envelopes, root manifests |
| Diff/merge | eager diff, range diff, conflict pages, built-in resolvers, merge explanations, range/prefix merge |
| Host callbacks | custom merge resolvers, custom CRDT resolvers, custom merge policies, custom stores |
| Stores/roots | memory, file, SQLite, SQLite in-memory, named roots, snapshot namespaces, CAS, retention |
| Transactions | built-in transaction begin/commit/rollback, read-own-writes, named-root conflict detection |
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
| Browser WASM | `bindings/wasm/test/wasm.test.ts` | `cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown --target-dir target && npm --prefix bindings/wasm run build:wasm && npm --prefix bindings/wasm test` |
| Kotlin/JVM | `bindings/kotlin/src/test/kotlin/build/crab/prolly/*.kt` | `mvn -f bindings/kotlin/pom.xml test` |
| Java | `bindings/java/src/test/java/build/crab/prolly/*.java` | `mvn -f bindings/pom.xml -pl java -am test` |
| JVM aggregate | Kotlin and Java modules together | `mvn -f bindings/pom.xml test` |
| Ruby | `bindings/ruby/test/prolly_smoke_test.rb` | `PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" BUNDLE_GEMFILE=bindings/ruby/Gemfile BUNDLE_PATH=/tmp/prolly-ruby-bundle bundle exec ruby -Ibindings/ruby/lib bindings/ruby/test/prolly_smoke_test.rb` |
| Swift | `bindings/swift/Examples/FixtureCheck`, cookbook executable targets | `DYLD_LIBRARY_PATH="$PWD/target/debug" swift run --package-path bindings/swift prolly-fixture-check` |

## Generated Binding Regeneration

When the UniFFI facade changes, build it once, then regenerate the checked-in
glue with the provenance-pinned tools. The Python and Ruby generated files keep
their documented `PROLLY_BINDINGS_LIBRARY` local-load adaptations after the
generator runs.

```sh
cargo build --manifest-path bindings/uniffi/Cargo.toml --target-dir target
(cd bindings/python && VIRTUAL_ENV=/tmp/prolly-uniffi-venv PATH=/tmp/prolly-uniffi-venv/bin:$PATH maturin develop)
VIRTUAL_ENV=/tmp/prolly-uniffi-venv PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
  uniffi-bindgen generate target/debug/libprolly_bindings.dylib --language kotlin \
  --out-dir bindings/kotlin/src/main/kotlin/build/crab/prolly/generated --config bindings/uniffi/uniffi.toml
VIRTUAL_ENV=/tmp/prolly-uniffi-venv PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
  uniffi-bindgen generate target/debug/libprolly_bindings.dylib --language ruby \
  --out-dir bindings/ruby/lib/prolly/generated --config bindings/uniffi/uniffi.toml
VIRTUAL_ENV=/tmp/prolly-uniffi-venv PATH=/tmp/prolly-uniffi-venv/bin:$PATH \
  uniffi-bindgen generate target/debug/libprolly_bindings.dylib --language swift \
  --out-dir bindings/swift/Sources/Prolly --config bindings/uniffi/uniffi.toml
npm --prefix bindings/node install
npm --prefix bindings/node run build:native
npm --prefix bindings/wasm run build:wasm
```

The Kotlin generator may place `prolly.kt` under a redundant
`generated/build/crab/prolly/` directory; retain the checked-in flat
`generated/prolly.kt` location. Move the Swift generator's `prollyFFI.h` into
`bindings/swift/Sources/prollyFFI/include/`, where the Swift package's FFI
target exposes it. Do not check in Node `.node` binaries, WASM `pkg/`, Cargo
locks created only for a binding crate, or other local build artifacts.

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
