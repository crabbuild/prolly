# Secondary Index Industrial Foundation Release Evidence

Evidence date: 2026-07-29  
Implementation revision: `f059c34ecddcffa6ca826152189c7c857ed83cec`
Branch: `codex/secondary-index-industrial-foundation`

This document covers the hard-cutover industrial foundation only: atomic
publication, bounded resources, correctness, observability, and release gates.
It does not certify later index semantics or online migration. The cutover has
no compatibility reader and no suffix-named replacement API.

## Release status

The local required matrix is green for the secondary-index surface. The final
audit materially strengthened boundedness, state locality, diagnostics, and
release automation, but this document does **not** certify production release.
Release remains blocked by the exact-head GitHub workflow and the unresolved
industrial gates listed below. The scheduled workflow now supplies 1,000,000-
and 10,000,000-record release-mode stress evidence.

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
| Bounded query memory and cursor integrity | incremental page lookahead, callback-scoped source joins, forward/reverse page tests, maximum-size rejection, query-bound cursor validation |
| Bounded build and spill | aggregate reader/writer/heap memory partitions, spill/canonical-root equivalence, and spill-exhaustion atomicity tests |
| Preallocation amplification checks | mutation and transfer budget boundary cases |
| Historical extractor identity | three-generation replacement, retention, and verification case |
| Finite work, memory, retry, and time | typed mutation, query, maintenance, and transfer budgets with finite defaults; aggregate `verify_all` partitioning |
| Stable redacted diagnostics | structured error-code, retry-advice, and sensitive-sentinel tests |
| Measured logical work | builder/store-fed publication and indexed-operation counters; operation-local physical I/O remains a release blocker |
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

Outcome: clean formatting and Clippy; 501 library tests, all integration tests,
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

Outcome: 42 focused tests passed, including deterministic atomicity,
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

Outcome: UniFFI 78/78, Node 60/60, WASM 34/34, Python 23/23, Ruby
22/22, and targeted JVM portable parity passed. The API inventory matched
3,067 operations. Generated Ruby source also passed `ruby -c`.

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

## Final-audit implementation delta

- Ordinary indexed mutations now update source and index cardinalities from
  checked local deltas instead of scanning the complete source and every
  physical index.
- Canonical state publication mutates the previous state tree instead of
  rebuilding state from an empty tree. Persisted policy caps active indexes,
  retained snapshots, descriptors, and durable pins.
- Query pages charge scan, returned-byte, source-fetch, elapsed, and retained
  memory limits before retaining data. Reverse pages and query-session record
  joins have the same bounded contract.
- Verification counts and diffs stream under finite budgets. `verify_all`
  partitions cumulative work and spill allowances across its indexes while
  retaining full sequential peak-memory and merge-fan-in allowances.
- Bundle export traverses nodes incrementally, verifies CIDs, and charges
  nodes, bytes, work, memory, and elapsed time. Import uploads bounded chunks.
- Spill runs partition aggregate reader buffers, heap/live-entry memory, and
  writer buffers instead of applying a nominal per-reader allowance.
- Core `Debug` output follows the redacted `Display` contract. UniFFI, Kotlin,
  Python, Ruby, Swift, and WASM expose stable index error and retry metadata.
- The required workflow now includes all-target tests, documentation tests,
  bounded benchmark artifacts, and AddressSanitizer. Scheduled stress covers
  1,000,000 and 10,000,000 records.

## Release blockers and follow-up

- The local Swift parity run was unavailable because the installed macOS
  command-line tools do not contain XCTest. Generated Swift source remains in
  the required API inventory; Swift runtime parity must run on a configured
  macOS CI worker before a binding release.
- The repository-wide JVM suite has one unrelated existing smoke failure:
  `ProllySmokeTest.customStoreCallbacksDriveEngine` expects a rightmost-path
  hint from a host store that does not opt into rightmost-path hints. The
  secondary-index `PortableParityTest` passes. This PR does not weaken that
  unrelated assertion.
- Bundle CBOR decoding still materializes wire vectors before semantic
  verification. Replace it with bounded streaming deserialization that charges
  allocations before ownership; the current preflight and post-decode budgets
  are defense in depth, not a hostile-input memory proof.
- Indexed observability exposes useful logical counters, but it does not yet
  attribute operation-local physical node/byte reads and writes, CAS
  contention, query budget consumption, or spill high-water marks. The
  evidence must not label global manager deltas as exact per-operation cost.
- SQLite is the only eligible production adapter, but the shared suite still
  needs independent-process publication/reopen, forced-termination durability,
  and store-enumeration/GC evidence before industrial certification.
- GC plan/sweep still needs a typed aggregate work, memory, and elapsed budget.
- Benchmark workflows produce artifacts but do not yet enforce a reviewed
  baseline, backend/hardware provenance, or regression classifier.
- Required AddressSanitizer and 1,000,000/10,000,000-record workflow results
  must be attached to the exact PR revision before release.
- Benchmark artifacts live under `target/` and are CI artifacts rather than
  source-controlled machine-specific baselines.
