# redb Store Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a production-ready `prolly-store-redb` crate that persists prolly nodes, roots, and hints in redb and supports strict atomic transactions.

**Architecture:** Store nodes, roots, and hints in three typed redb tables. Use one redb read transaction per logical read batch and one configured write transaction per mutation, including cross-table root validation and node/root commits.

**Tech Stack:** Rust 2021 edition, Rust 1.89, `prolly-map` 0.5.0, `redb` 4.1.0, and the repository's `prolly-store-test` conformance helpers.

## Global Constraints

- Create the crate at `stores/prolly-store-redb` with package name `prolly-store-redb` and library name `prolly_store_redb`.
- Set only the adapter crate's `rust-version` to `1.89`; do not change the root crate's Rust 1.81 minimum.
- Use exactly `redb = "4.1.0"`.
- Default to a 1 GiB cache and `Durability::Immediate`.
- Keep nodes, roots, and hints in separate typed tables named `prolly_nodes`, `prolly_roots`, and `prolly_hints`.
- Implement `Store`, `ManifestStore`, `ManifestStoreScan`, `NodeStoreScan`, and `TransactionalStore`.
- Preserve all unrelated working-tree changes.

---

## File Structure

- `stores/prolly-store-redb/Cargo.toml`: crate metadata, Rust floor, and dependencies.
- `stores/prolly-store-redb/src/lib.rs`: public configuration, error, table definitions, helpers, and all synchronous store trait implementations. This matches the existing single-file adapter convention.
- `stores/prolly-store-redb/tests/redb_store.rs`: conformance, persistence, hint, configuration, and transaction integration tests.
- `stores/prolly-store-redb/examples/basic_usage.rs`: runnable named-root persistence example.
- `stores/prolly-store-redb/README.md`: installation, API, storage model, transactions, and operations guide.

### Task 1: Crate scaffold and core Store contract

**Files:**
- Create: `stores/prolly-store-redb/Cargo.toml`
- Create: `stores/prolly-store-redb/src/lib.rs`
- Create: `stores/prolly-store-redb/tests/redb_store.rs`
- Create: `stores/prolly-store-redb/README.md`

**Interfaces:**
- Consumes: `prolly::{BatchOp, Store}` and `redb::{Database, Durability, TableDefinition}`.
- Produces: `RedbStoreConfig`, `RedbStoreError`, `RedbStore::open`, `RedbStore::open_with_config`, and `impl Store for RedbStore`.

- [ ] **Step 1: Create dependency metadata and an empty library target**

```toml
[package]
name = "prolly-store-redb"
description = "redb store adapter for prolly-map."
edition = "2021"
rust-version = "1.89"
version = "0.3.0"
license = "MIT OR Apache-2.0"
repository = "https://github.com/crabbuild/prolly"
homepage = "https://github.com/crabbuild/prolly"
documentation = "https://docs.rs/prolly-store-redb"
readme = "README.md"
keywords = ["prolly-tree", "storage", "database", "redb"]
categories = ["database-implementations"]

[lib]
name = "prolly_store_redb"
path = "src/lib.rs"

[dependencies]
prolly = { package = "prolly-map", path = "../..", version = "0.5.0" }
redb = "4.1.0"

[dev-dependencies]
prolly-store-test = { path = "../prolly-store-test" }

[lints.rust]
unsafe_code = "forbid"
```

Create `README.md` with `# prolly-store-redb`. Create `src/lib.rs` with only a crate-level comment so the integration test can compile far enough to demonstrate the missing API:

```rust
//! redb store adapter for prolly-map.
```

- [ ] **Step 2: Write the failing basic-store conformance test**

```rust
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use prolly::Store;
use prolly_store_redb::{Durability, RedbStore, RedbStoreConfig};

#[test]
fn redb_store_satisfies_store_contract() {
    let path = temp_db_path("store-contract");
    let store = RedbStore::open(&path).unwrap();
    prolly_store_test::assert_store_contract(&store);
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[test]
fn redb_store_accepts_configuration() {
    let path = temp_db_path("configured");
    let store = RedbStore::open_with_config(
        &path,
        RedbStoreConfig {
            cache_size_bytes: 8 * 1024 * 1024,
            durability: Durability::None,
        },
    )
    .unwrap();
    store.put(b"configured", b"value").unwrap();
    assert_eq!(store.get(b"configured").unwrap(), Some(b"value".to_vec()));
    drop(store);
    let _ = std::fs::remove_file(path);
}

fn temp_db_path(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "prolly-redb-{label}-{}-{nanos}.redb",
        std::process::id()
    ))
}
```

- [ ] **Step 3: Run the test and verify the RED state**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml redb_store_satisfies_store_contract`

Expected: compilation fails with unresolved imports for `RedbStore`, `RedbStoreConfig`, and `Durability`.

- [ ] **Step 4: Implement configuration, opening, errors, and Store**

Add these definitions to `src/lib.rs`:

```rust
use std::collections::HashMap;
use std::path::Path;

use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};

use prolly::{BatchOp, Store};

pub use redb::Durability;

const NODES: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_nodes");
const ROOTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("prolly_roots");
const HINTS: TableDefinition<(&[u8], &[u8]), &[u8]> = TableDefinition::new("prolly_hints");

#[derive(Debug, Clone, Copy)]
pub struct RedbStoreConfig {
    pub cache_size_bytes: usize,
    pub durability: Durability,
}

impl Default for RedbStoreConfig {
    fn default() -> Self {
        Self {
            cache_size_bytes: 1024 * 1024 * 1024,
            durability: Durability::Immediate,
        }
    }
}

#[derive(Debug)]
pub struct RedbStoreError {
    message: String,
    source: Option<redb::Error>,
}

impl RedbStoreError {
    fn message(message: impl Into<String>) -> Self {
        Self { message: message.into(), source: None }
    }

    fn redb(context: &str, error: impl Into<redb::Error>) -> Self {
        let source = error.into();
        Self {
            message: format!("{context}: {source}"),
            source: Some(source),
        }
    }
}

impl std::fmt::Display for RedbStoreError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "redb error: {}", self.message)
    }
}

impl std::error::Error for RedbStoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_ref().map(|source| source as _)
    }
}

pub struct RedbStore {
    db: Database,
    durability: Durability,
}
```

Implement `open` with `Database::builder().set_cache_size(...).create(path)`, then initialize all three tables in one write transaction. Add a private `begin_write` helper that calls `db.begin_write()`, applies `self.durability` with `set_durability`, and returns the transaction. Both helpers must map every redb error through `RedbStoreError::redb` with operation context.

Implement `Store` as follows:

- `get`: one read transaction, `open_table(NODES)`, `get(key)`, then copy `guard.value()`.
- `put` and `delete`: one configured write transaction, mutate `NODES`, drop the table scope, and commit.
- `batch`: one configured write transaction and one open node table; apply every `BatchOp` and commit once.
- `batch_get_ordered`: one read transaction and one open node table; call `get` for each input key and preserve order and duplicates.
- `batch_get_ordered_unique`: delegate to the same one-transaction helper.
- `batch_get`: use the ordered helper, zip keys with values, and collect only present values into a `HashMap<Vec<u8>, Vec<u8>>`.
- `prefers_batch_reads`: return `true`.
- `batch_put`: insert all entries in one configured transaction and commit once.

- [ ] **Step 5: Run the test and verify the GREEN state**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml redb_store_satisfies_store_contract`

Expected: one selected integration test passes.

- [ ] **Step 6: Commit the core store**

```bash
git add stores/prolly-store-redb/Cargo.toml stores/prolly-store-redb/README.md stores/prolly-store-redb/src/lib.rs stores/prolly-store-redb/tests/redb_store.rs
git commit -m "feat(redb): add core store adapter"
```

### Task 2: Hints, manifests, and deterministic scans

**Files:**
- Modify: `stores/prolly-store-redb/src/lib.rs`
- Modify: `stores/prolly-store-redb/tests/redb_store.rs`

**Interfaces:**
- Consumes: `RedbStore::begin_write`, the `NODES`, `ROOTS`, and `HINTS` definitions, and `RootManifest::{to_bytes, from_bytes}`.
- Produces: hint-aware `Store` methods and implementations of `ManifestStore`, `ManifestStoreScan`, and `NodeStoreScan`.

- [ ] **Step 1: Write failing conformance and hint tests**

Add tests that call:

```rust
#[test]
fn redb_store_satisfies_manifest_store_contract() {
    with_store("manifest-contract", |store| {
        prolly_store_test::assert_manifest_store_contract(store)
    });
}

#[test]
fn redb_store_satisfies_node_store_scan_contract() {
    let path = temp_db_path("scan-contract");
    let store = RedbStore::open(&path).unwrap();
    prolly_store_test::assert_node_store_scan_contract(store);
    let _ = std::fs::remove_file(path);
}

#[test]
fn redb_store_persists_hints() {
    use prolly::Store;
    let path = temp_db_path("hints");
    {
        let store = RedbStore::open(&path).unwrap();
        assert!(store.supports_hints());
        store.put_hint(b"tree", b"rightmost", b"path").unwrap();
    }
    let store = RedbStore::open(&path).unwrap();
    assert_eq!(store.get_hint(b"tree", b"rightmost").unwrap(), Some(b"path".to_vec()));
    drop(store);
    let _ = std::fs::remove_file(path);
}

#[test]
fn redb_store_persists_named_root_across_reopen() {
    use prolly::{Config, Prolly};
    let path = temp_db_path("root-reopen");
    let tree = {
        let prolly = Prolly::new(RedbStore::open(&path).unwrap(), Config::default());
        let tree = prolly
            .put(&prolly.create(), b"project/name".to_vec(), b"CrabDB".to_vec())
            .unwrap();
        prolly.publish_named_root(b"main", &tree).unwrap();
        tree
    };
    let prolly = Prolly::new(RedbStore::open(&path).unwrap(), Config::default());
    let loaded = prolly.load_named_root(b"main").unwrap().unwrap();
    assert_eq!(loaded, tree);
    assert_eq!(prolly.get(&loaded, b"project/name").unwrap(), Some(b"CrabDB".to_vec()));
    drop(prolly);
    let _ = std::fs::remove_file(path);
}
```

Add `with_store` to open an isolated database, run the closure, drop the store, and remove the file.

- [ ] **Step 2: Run the new tests and verify the RED state**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml --test redb_store`

Expected: compilation fails because `RedbStore` does not implement the manifest and scan traits and `supports_hints()` is false.

- [ ] **Step 3: Implement hints and atomic node-plus-hint publication**

Extend `impl Store for RedbStore`:

```rust
fn supports_hints(&self) -> bool { true }

fn get_hint(&self, namespace: &[u8], key: &[u8]) -> Result<Option<Vec<u8>>, Self::Error> {
    let txn = self.db.begin_read().map_err(|e| RedbStoreError::redb("begin hint read", e))?;
    let table = txn.open_table(HINTS).map_err(|e| RedbStoreError::redb("open hints", e))?;
    table.get((namespace, key))
        .map(|value| value.map(|guard| guard.value().to_vec()))
        .map_err(|e| RedbStoreError::redb("read hint", e))
}

fn put_hint(&self, namespace: &[u8], key: &[u8], value: &[u8]) -> Result<(), Self::Error> {
    let txn = self.begin_write("begin hint write")?;
    { txn.open_table(HINTS)?.insert((namespace, key), value)?; }
    txn.commit().map_err(|e| RedbStoreError::redb("commit hint", e))
}
```

Map the `open_table` and `insert` errors explicitly rather than relying on `?`. Implement `batch_put_with_hint` by opening both `NODES` and `HINTS` from the same transaction, inserting all nodes and the hint, dropping both tables, and committing once.

- [ ] **Step 4: Implement manifests and scans**

Add helpers `encode_root_manifest`, `decode_root_manifest`, and `cid_from_key`. Implement:

- `ManifestStore::get_root`, `put_root`, and `delete_root` with the root table.
- `compare_and_swap_root` with one configured write transaction; read and decode inside the transaction, return `ManifestUpdate::Conflict { current }` without commit on mismatch, otherwise insert/remove and commit.
- `ManifestStoreScan::list_roots` by iterating `ROOTS`, decoding each value, collecting `NamedRootManifest`, and sorting by `name`.
- `NodeStoreScan::list_node_cids` by iterating `NODES`, rejecting any key whose length is not 32, constructing `Cid([u8; 32])`, and sorting by CID bytes.

Every iterator item and table operation must map failures to `RedbStoreError` with its operation name.

- [ ] **Step 5: Run all adapter tests and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml`

Expected: the basic store, manifest, scan, and hint tests pass.

- [ ] **Step 6: Commit metadata and scan support**

```bash
git add stores/prolly-store-redb/src/lib.rs stores/prolly-store-redb/tests/redb_store.rs
git commit -m "feat(redb): add manifests hints and scans"
```

### Task 3: Strict atomic transactions

**Files:**
- Modify: `stores/prolly-store-redb/src/lib.rs`
- Modify: `stores/prolly-store-redb/tests/redb_store.rs`

**Interfaces:**
- Consumes: `RedbStore::begin_write`, `NODES`, `ROOTS`, and root manifest helpers.
- Produces: `impl TransactionalStore for RedbStore` with atomic validation and commit.

- [ ] **Step 1: Write the failing indexed-map transaction test**

```rust
#[test]
fn redb_store_supports_strict_indexed_maps() {
    let path = temp_db_path("indexed-map");
    let store = RedbStore::open(&path).unwrap();
    prolly_store_test::assert_indexed_map_contract(store);
    let _ = std::fs::remove_file(path);
}
```

- [ ] **Step 2: Run the test and verify the RED state**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml redb_store_supports_strict_indexed_maps`

Expected: compilation fails because `RedbStore` does not implement `TransactionalStore`.

- [ ] **Step 3: Implement TransactionalStore**

Add the following implementation, with `store_error` mapping the adapter error into the prolly engine error:

```rust
fn store_error(error: RedbStoreError) -> prolly::Error {
    prolly::Error::Store(Box::new(error))
}

impl TransactionalStore for RedbStore {
    fn supports_transactions(&self) -> bool { true }

    fn commit_transaction(
        &self,
        node_writes: &[TransactionNodeWrite],
        root_conditions: &[RootCondition],
        root_writes: &[RootWrite],
    ) -> Result<TransactionUpdate, prolly::Error> {
        let txn = self
            .begin_write("begin strict transaction")
            .map_err(store_error)?;
        {
            let mut nodes = txn
                .open_table(NODES)
                .map_err(|error| store_error(RedbStoreError::redb("open nodes", error)))?;
            let mut roots = txn
                .open_table(ROOTS)
                .map_err(|error| store_error(RedbStoreError::redb("open roots", error)))?;

            for condition in root_conditions {
                let current = roots
                    .get(condition.name.as_slice())
                    .map_err(|error| store_error(RedbStoreError::redb("read root condition", error)))?
                    .map(|guard| decode_root_manifest_bytes(guard.value()))
                    .transpose()
                    .map_err(store_error)?;
                if current != condition.expected {
                    return Ok(TransactionUpdate::Conflict(Box::new(
                        TransactionConflict::new(
                            condition.name.clone(),
                            condition.expected.clone(),
                            current,
                        ),
                    )));
                }
            }

            for write in node_writes {
                match write {
                    TransactionNodeWrite::Upsert { key, value } => nodes
                        .insert(key.as_slice(), value.as_slice())
                        .map_err(|error| store_error(RedbStoreError::redb("write transaction node", error)))?,
                    TransactionNodeWrite::Delete { key } => nodes
                        .remove(key.as_slice())
                        .map_err(|error| store_error(RedbStoreError::redb("delete transaction node", error)))?,
                };
            }

            for write in root_writes {
                match write {
                    RootWrite::Put { name, manifest } => {
                        let bytes = encode_root_manifest(manifest).map_err(store_error)?;
                        roots
                            .insert(name.as_slice(), bytes.as_slice())
                            .map_err(|error| store_error(RedbStoreError::redb("write transaction root", error)))?;
                    }
                    RootWrite::Delete { name } => {
                        roots
                            .remove(name.as_slice())
                            .map_err(|error| store_error(RedbStoreError::redb("delete transaction root", error)))?;
                    }
                }
            }
        }
        txn.commit()
            .map_err(|error| store_error(RedbStoreError::redb("commit strict transaction", error)))?;
        Ok(TransactionUpdate::Applied {
            nodes_written: node_writes.len(),
            roots_written: root_writes.len(),
        })
    }
}
```

Import `RootCondition`, `RootWrite`, `TransactionConflict`, `TransactionNodeWrite`, `TransactionUpdate`, and `TransactionalStore` from `prolly`. The early conflict return drops the uncommitted transaction.

- [ ] **Step 4: Run the transaction test and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml redb_store_supports_strict_indexed_maps`

Expected: the selected test passes.

- [ ] **Step 5: Run all adapter tests**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml`

Expected: all adapter tests pass.

- [ ] **Step 6: Commit strict transactions**

```bash
git add stores/prolly-store-redb/src/lib.rs stores/prolly-store-redb/tests/redb_store.rs
git commit -m "feat(redb): add strict transactions"
```

### Task 4: User documentation and final verification

**Files:**
- Modify: `stores/prolly-store-redb/README.md`
- Create: `stores/prolly-store-redb/examples/basic_usage.rs`

**Interfaces:**
- Consumes: the finalized `RedbStore`, `RedbStoreConfig`, and `Durability` API.
- Produces: a documented and runnable crate with a complete verification record.

- [ ] **Step 1: Write the basic example**

Create an example that opens `./data/app.prolly.redb`, constructs `Prolly::new(store, Config::default())`, writes `project/name = CrabDB`, publishes named root `main`, reloads it, and asserts the value. Return `Result<(), Box<dyn std::error::Error>>` from `main`.

- [ ] **Step 2: Write README documentation**

Document:

- Rust 1.89 and dependency installation.
- The quick-start example.
- `RedbStoreConfig` with 1 GiB/Immediate defaults and an 8 MiB/None example.
- The three-table single-file storage model.
- Atomic batches, CAS, and strict cross-table transactions.
- Hint behavior and the fact that rightmost-path hints are not preferred without measurements.
- Operational guidance to reuse one store instance and back up the complete `.redb` file.
- Test command: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml`.

- [ ] **Step 3: Verify docs and the example compile**

Run: `cargo test --manifest-path stores/prolly-store-redb/Cargo.toml --doc`

Expected: all README-backed documentation tests pass.

Run: `cargo check --manifest-path stores/prolly-store-redb/Cargo.toml --example basic_usage`

Expected: exit 0.

- [ ] **Step 4: Run final fresh verification**

Run these commands without reusing earlier output:

```bash
cargo fmt --manifest-path stores/prolly-store-redb/Cargo.toml -- --check
cargo clippy --manifest-path stores/prolly-store-redb/Cargo.toml --all-targets -- -D warnings
cargo test --manifest-path stores/prolly-store-redb/Cargo.toml
cargo check --manifest-path stores/prolly-store-redb/Cargo.toml --all-targets
git diff --check HEAD~1 -- stores/prolly-store-redb docs/superpowers
```

Expected: every command exits 0, Clippy reports no warnings, and all adapter unit, integration, and documentation tests pass.

- [ ] **Step 5: Commit documentation**

```bash
git add stores/prolly-store-redb/README.md stores/prolly-store-redb/examples/basic_usage.rs
git commit -m "docs(redb): document store adapter"
```
