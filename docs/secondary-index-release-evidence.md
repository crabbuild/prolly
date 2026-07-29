# Secondary Index Industrial Foundation Release Evidence

Evidence date: 2026-07-29  
Implementation revision: `2e7142bb883686955df35ecf811129bb4e8be0f6`  
Branch: `codex/secondary-index-industrial-foundation`

This document covers the hard-cutover industrial foundation only: atomic
publication, bounded resources, correctness, observability, and release gates.
It does not certify later index semantics or online migration. The cutover has
no compatibility reader and no suffix-named replacement API.

## Release status

The local required matrix is green for the secondary-index surface. Merge and
production release remain blocked until the required GitHub workflow passes on
the exact PR head. The scheduled workflow supplies release-mode stress,
sanitizer, and 100,000-record performance evidence after the branch is pushed.

SQLite is the only production-qualified store. Its profile requires a
file-backed database, full synchronous acknowledgement, foreground
checkpoints, cross-handle root CAS, read-after-write visibility, and explicit
quiescence for GC. `MemStore`, `FileNodeStore`, PGlite, redb, RocksDB, and
SlateDB are verification-profile stores and cannot open a production indexed
coordinator.

## Acceptance evidence

| Requirement | Evidence |
|---|---|
| One visibility root and one CAS | `only_the_canonical_root_controls_visibility`; publication-origin audit; obsolete-layout absence gate |
| Complete old-or-new observations | barrier-controlled two-writer test and injected confirmation/CAS failures |
| Exact production-store contract | SQLite shared production contract plus separate-handle CAS test |
| File store is verification-only | profile unit test and production-open rejection |
| Bounded query memory and cursor integrity | bounded page/cursor tests, maximum-size rejection, query-bound cursor validation |
| Bounded build and spill | spill/canonical-root equivalence and spill-exhaustion atomicity tests |
| Preallocation amplification checks | mutation and transfer budget boundary cases |
| Historical extractor identity | three-generation replacement, retention, and verification case |
| Finite work, memory, retry, and time | typed mutation, query, maintenance, and transfer budgets with finite defaults |
| Stable redacted diagnostics | structured error-code, retry-advice, and sensitive-sentinel tests |
| Measured work | builder/store-fed publication and indexed-operation counters |
| Safe retention and GC | durable-pin tests and global named-root GC regression test |
| Malformed-input resilience | deterministic 10,000-case descriptor/cursor/bundle parser fuzz smoke |
| No old or suffix architecture | `scripts/check-secondary-index-cutover.sh` |

## Commands and outcomes

Core:

```text
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps
cargo run --example secondary_index
```

Outcome: clean formatting and Clippy; 500 library tests, all integration tests,
and 74 documentation tests passed; rustdoc completed with warnings denied; the
example verified three active indexes and a five-node bundle.

Focused release gate:

```text
cargo test --test conformance_fixtures --test gc --test node_publication \
  --test secondary_index --test secondary_index_concurrency \
  --test secondary_index_faults --test secondary_index_fuzz_smoke \
  --test secondary_index_resources
./scripts/check-secondary-index-cutover.sh
```

Outcome: 41 focused tests passed, including deterministic atomicity,
concurrency, GC, bounded-resource, malformed-input, and hard-cutover checks.

Store profiles:

```text
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml indexed_profile_
cargo check --manifest-path stores/prolly-store-pglite/Cargo.toml --all-targets
cargo check --manifest-path stores/prolly-store-redb/Cargo.toml --all-targets
cargo check --manifest-path stores/prolly-store-rocksdb/Cargo.toml --all-targets
cargo check --manifest-path stores/prolly-store-slatedb/Cargo.toml --all-targets
```

Outcome: all commands passed. SQLite passed profile selection, separate-handle
CAS coordination, and the shared production indexed-map contract. Every other
listed adapter compiled with an explicit verification profile.

Portable contracts:

```text
cargo test --manifest-path bindings/uniffi/Cargo.toml --target-dir target
npm --prefix bindings/node test
npm --prefix bindings/wasm test
PYTHONPATH=bindings/python python3 -m unittest bindings/python/tests/test_portable_parity.py
RUBYLIB=bindings/ruby/lib ruby bindings/ruby/test/portable_parity_test.rb
mvn -q -f bindings/pom.xml -pl java -am -Dtest=PortableParityTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
cargo +nightly rustdoc --lib --features async-store -- \
  -Z unstable-options --output-format json
python3 scripts/binding_api_inventory.py check
```

Outcome: UniFFI 77/77, Node 60/60, WASM 34/34, Python 23/23, Ruby
22/22, and targeted JVM portable parity passed. The API inventory matched
3,052 operations. Generated Ruby source also passed `ruby -c`.

Performance smoke, run twice:

```text
PROLLY_INDEX_BENCH_SAMPLES=5 PROLLY_INDEX_BENCH_SCALE=100 \
  PROLLY_INDEX_BENCH_BATCH=16 ./scripts/run-secondary-index-bench.sh \
  target/secondary-index-bench-smoke.csv
```

Every row was semantically verified. Across the two runs, representative p95
latencies were 0.396–0.417 ms for a 16-record indexed update, 0.005–0.007 ms
for an exact query, 0.595–0.690 ms for logical verification, and 1.897–2.510
ms for the barrier-free two-writer retry fixture. These numbers are smoke
provenance, not a cross-machine performance baseline.

## Known limitations and release follow-up

- The local Swift parity run was unavailable because the installed macOS
  command-line tools do not contain XCTest. Generated Swift source remains in
  the required API inventory; Swift runtime parity must run on a configured
  macOS CI worker before a binding release.
- The repository-wide JVM suite has one unrelated existing smoke failure:
  `ProllySmokeTest.customStoreCallbacksDriveEngine` expects a rightmost-path
  hint from a host store that does not opt into rightmost-path hints. The
  secondary-index `PortableParityTest` passes. This PR does not weaken that
  unrelated assertion.
- Sanitizer and extended 100,000-record evidence are intentionally generated
  by `.github/workflows/secondary-index-scheduled.yml`; attach the workflow
  links to the release record before declaring a production release.
- Benchmark artifacts live under `target/` and are CI artifacts rather than
  source-controlled machine-specific baselines.
