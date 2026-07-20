# Async-first architecture completion report

**Verified source revision:** `57e8ecfa4ba6348d9ecbdff055d42a84b6ebabf5`

## Architecture result

- `ProllyEngine<S: AsyncStore>` is the only production ordered-tree algorithm
  owner.
- `AsyncProlly<S>` is the native async engine surface.
- `Prolly<S>` owns the same engine over `SyncStoreAsAsync<Arc<S>>` and uses the
  sealed one-poll ready runner.
- Reads, traversal, mutations, builders, diff, merge, proofs, statistics,
  snapshots, copy, GC, transactions, and versioned-map publication route
  through the engine or their async-first service.
- Stored nodes validate requested CID, structure, and input-tree format before
  cache admission.
- The retired full logical-map async mutation rebuild and facade-local
  production algorithms are absent. Test-only differential oracles remain
  non-selectable under `cfg(test)`.

The final ownership classification is
[`../../docs/async-first-api-inventory.md`](../../docs/async-first-api-inventory.md).

## Verified commands

All commands completed successfully on the recorded local machine:

```text
cargo fmt --all -- --check
cargo check --no-default-features
cargo check
cargo check --all-features
cargo check --target wasm32-unknown-unknown --no-default-features
cargo clippy --all-features --all-targets -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
cargo test --no-default-features --quiet
cargo test --all-features --quiet
cargo test --all-features --test canonical_roots --test foundation_root_vectors --test conformance_fixtures --quiet
cargo test --manifest-path stores/prolly-store-sqlite/Cargo.toml --quiet
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --quiet
cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --all-features --quiet
cargo check --manifest-path benchmarks/sqlite-turso-local/Cargo.toml --all-targets
cargo check --manifest-path bindings/wasm/Cargo.toml --target wasm32-unknown-unknown
cargo check --manifest-path bindings/uniffi/Cargo.toml --all-targets
```

The all-feature root suite included 488 unit tests plus every integration-test
binary; the dedicated async-store group passed 74 tests. The canonical root
group passed 14 cases, foundation vectors passed 2, and conformance fixtures
passed 1. SQLite passed 18 adapter tests across its test binaries; Turso passed
10 native local tests without cloud sync and 12 with all adapter features. The
WASM and UniFFI binding crates compiled after the bounded frontier futures were
made to own their CID batches.

## Performance gate

The full local SQLite/Turso matrix completed with 432 validated cells, 36
validated fixtures, 0 skipped cells, and no correctness failures. See
[`../sqlite-turso-local-async-first-final-r2-2026-07-18/findings.md`](../sqlite-turso-local-async-first-final-r2-2026-07-18/findings.md).

Point-update p50 changes only 1.29x for random and 1.33x for clustered between
50K and 2M Turso records. The old O(N) behavior is removed. Integrity checks
were enabled for every measurement.
