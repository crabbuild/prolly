# Native Turso Store Adapter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a native async Turso Database store adapter with local persistence by default and explicit cloud push/pull behind an optional `sync` feature.

**Architecture:** Add a standalone `prolly-store-turso` crate whose `TursoBackend` implements `RemoteStoreBackend`; expose `TursoStore` as `RemoteProllyStore<TursoBackend>`. The backend retains local or synced Turso database handles, opens an independent connection per operation, and maps batch/CAS/coordinated writes to native SQL transactions.

**Tech Stack:** Rust 2021, adapter package floor 1.88 (required by Turso 0.7's dependency graph), `prolly-map` async store API, `turso` 0.7 with default features disabled, Tokio tests, tempfile.

## Global Constraints

- The default feature set supports only native local Turso database files.
- Cargo feature `sync` enables `turso/sync` and explicit `push()`/`pull()`; no store operation synchronizes implicitly.
- Do not add a synchronous `Store` implementation or hide an async runtime.
- Reuse `RemoteProllyStore` for manifest codecs and CID verification.
- Use a fresh Turso connection per backend operation and an immediate SQL transaction for root CAS and coordinated commits.
- Preserve all unrelated dirty and unmerged worktree files.

## File Structure

- `stores/prolly-store-turso/Cargo.toml`: standalone package metadata, feature wiring, runtime/test dependencies.
- `stores/prolly-store-turso/src/lib.rs`: public backend API, database-handle abstraction, SQL helpers, schema, and `RemoteStoreBackend` implementation.
- `stores/prolly-store-turso/tests/turso_backend.rs`: local conformance, transaction, reopen, and feature-gated sync behavior tests.
- `stores/prolly-store-turso/examples/basic_usage.rs`: executable local async example with a named root.
- `stores/prolly-store-turso/README.md`: local/sync setup, explicit sync semantics, schema, beta warning, and verification commands.
- `README.md`: link the new adapter from the repository overview.
- `../Cargo.toml`: register the adapter in the enclosing CrabDB workspace used by this checkout.

---

### Task 1: Crate Surface and Local Constructor

**Files:**
- Create: `stores/prolly-store-turso/Cargo.toml`
- Create: `stores/prolly-store-turso/src/lib.rs`
- Create: `stores/prolly-store-turso/tests/turso_backend.rs`
- Modify: `../Cargo.toml`

**Interfaces:**
- Produces: `pub type TursoStore = RemoteProllyStore<TursoBackend>`
- Produces: `pub async fn TursoBackend::open(path: impl AsRef<Path>) -> Result<Self, TursoStoreError>`
- Produces: `pub async fn TursoBackend::from_local_database(database: turso::Database) -> Result<Self, TursoStoreError>`
- Produces: `pub fn TursoBackend::is_synced(&self) -> bool`

- [ ] **Step 1: Create the package manifest and failing public-surface test**

Use `prolly = { package = "prolly-map", path = "../..", version = "0.3.0", features = ["async-store"] }`, `turso = { version = "0.7", default-features = false }`, feature `sync = ["turso/sync"]`, and Tokio/tempfile development dependencies. Add a test that imports `TursoBackend` and `TursoStore`, opens a temporary database, asserts `!backend.is_synced()`, and type-checks `TursoStore::new(backend)`.

- [ ] **Step 2: Run the test and verify RED**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_backend_constructs_store`

Expected: compilation fails because `TursoBackend` and `TursoStore` do not exist.

- [ ] **Step 3: Implement the minimal local constructor**

Define a cloneable internal database enum with a local `turso::Database` variant, a debug-safe `TursoBackend`, and `TursoStoreError` variants `InvalidPath(PathBuf)`, `Turso(turso::Error)`, and, under `sync`, `NotSynced`. `open` rejects paths Turso's string-based builder cannot represent, calls `turso::Builder::new_local(...).build().await`, and delegates to `from_local_database`. Both constructors call an idempotent `initialize_schema` using `SCHEMA_SQL` before returning.

- [ ] **Step 4: Run the focused test and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_backend_constructs_store`

Expected: one passing test.

### Task 2: Node, Hint, Manifest, and Scan Operations

**Files:**
- Modify: `stores/prolly-store-turso/src/lib.rs`
- Modify: `stores/prolly-store-turso/tests/turso_backend.rs`

**Interfaces:**
- Consumes: `TursoBackend` local connection helper.
- Produces: complete non-transactional `RemoteStoreBackend` operations and `SCHEMA_SQL` tables `prolly_nodes`, `prolly_hints`, `prolly_roots`.

- [ ] **Step 1: Add the failing shared backend conformance test**

Open a temporary local database and call `prolly::remote_conformance::assert_remote_backend_contract(&backend).await` from a Tokio test.

- [ ] **Step 2: Run the conformance test and verify RED**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_backend_satisfies_remote_contract`

Expected: compilation fails because `TursoBackend` does not implement `RemoteStoreBackend`.

- [ ] **Step 3: Implement SQL operations**

Implement point node reads/writes/deletes, transactional node batches, ordered batch reads, transactional batch puts, sorted CID scans, hint reads/upserts, atomic node-plus-hint batches, root reads/upserts/deletes, root listing, and root compare-and-swap. Use BLOB parameters, drain every query result before issuing another statement, and use `TransactionBehavior::Immediate` for CAS. Return `RemoteManifestUpdate::Conflict { current }` without applying the requested root update when bytes differ.

- [ ] **Step 4: Run the conformance test and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_backend_satisfies_remote_contract`

Expected: the conformance test passes.

### Task 3: Coordinated Transactions and Reopen Persistence

**Files:**
- Modify: `stores/prolly-store-turso/src/lib.rs`
- Modify: `stores/prolly-store-turso/tests/turso_backend.rs`

**Interfaces:**
- Produces: `supports_transactions() -> true` and atomic `commit_transaction(...)`.
- Produces: durable reuse of the same three-table schema across reopen.

- [ ] **Step 1: Add failing transaction and reopen tests**

Add one test calling `assert_remote_backend_transaction_contract(&backend).await`. Add another that creates a native `AsyncProlly<TursoStore>`, writes and publishes `main`, drops all handles, reopens the same path, loads `main`, and reads the stored key.

- [ ] **Step 2: Run both tests and verify RED**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_backend_satisfies_transaction_contract`

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --test turso_backend local_store_persists_named_root_across_reopen`

Expected: the transaction contract fails because transaction support is not yet advertised or implemented.

- [ ] **Step 3: Implement coordinated commit**

Open a fresh connection, start an immediate transaction, read and compare each root condition, explicitly roll back and return `RemoteTransactionUpdate::Conflict` on the first mismatch, then apply all node writes and root writes and commit. Report `RemoteTransactionUpdate::Applied` only after successful commit.

- [ ] **Step 4: Run all default-feature tests and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml`

Expected: all adapter tests and documentation tests pass.

### Task 4: Optional Explicit Cloud Sync

**Files:**
- Modify: `stores/prolly-store-turso/src/lib.rs`
- Modify: `stores/prolly-store-turso/tests/turso_backend.rs`

**Interfaces:**
- Produces under `sync`: `open_synced(path, remote_url, auth_token)`, `from_synced_database`, `push() -> Result<(), TursoStoreError>`, and `pull() -> Result<bool, TursoStoreError>`.

- [ ] **Step 1: Add failing sync-feature API tests**

Under `#[cfg(feature = "sync")]`, assert a local backend returns `TursoStoreError::NotSynced` from both `push` and `pull`. Add an environment-gated test that reads `TURSO_DATABASE_URL` and `TURSO_AUTH_TOKEN`, creates a unique temp replica, builds a synced backend, publishes a named root, calls `push`, calls `pull`, and reloads the named root.

- [ ] **Step 2: Run the local sync misuse test and verify RED**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --features sync --test turso_backend local_backend_rejects_sync_operations`

Expected: compilation fails because the sync APIs do not exist.

- [ ] **Step 3: Implement sync constructors and explicit methods**

Add a feature-gated synced database variant. `open_synced` calls `turso::sync::Builder::new_remote(path).with_remote_url(remote_url).with_auth_token(auth_token).build().await`; `from_synced_database` initializes schema through an awaited synced connection. `push` and `pull` delegate only for the synced variant and otherwise return `NotSynced`.

- [ ] **Step 4: Run the full sync feature test suite and verify GREEN**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --features sync`

Expected: all offline tests pass; the cloud integration test returns early when credentials are absent.

### Task 5: Example, Documentation, and Full Verification

**Files:**
- Create: `stores/prolly-store-turso/examples/basic_usage.rs`
- Create: `stores/prolly-store-turso/README.md`
- Modify: `stores/prolly-store-turso/Cargo.toml`
- Modify: `README.md`

**Interfaces:**
- Consumes: final `TursoBackend` and `TursoStore` APIs.
- Produces: copyable local usage and sync-feature instructions.

- [ ] **Step 1: Add a runnable local example**

Build an async local store, insert `user/1 = Ada`, publish `main`, reload it, and assert the value. Accept an optional database path argument and default to `target/prolly-turso-example.db`.

- [ ] **Step 2: Write adapter and root documentation**

Document dependency syntax, local usage, `--features sync`, explicit push/pull usage, advanced pre-built database constructors, schema, concurrency/transaction behavior, environment-gated sync verification, and Turso's beta/backup caveat. Link the adapter from the root README's adapter section.

- [ ] **Step 3: Format and inspect the complete diff**

Run: `cargo fmt --manifest-path stores/prolly-store-turso/Cargo.toml -- --check`

Run: `git diff --check -- stores/prolly-store-turso README.md docs/superpowers`

Expected: both commands exit successfully with no diagnostics.

- [ ] **Step 4: Run default-feature verification**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml`

Run: `cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml --all-targets -- -D warnings`

Expected: both commands exit successfully with zero failed tests and zero warnings.

- [ ] **Step 5: Run sync-feature verification**

Run: `cargo test --manifest-path stores/prolly-store-turso/Cargo.toml --features sync`

Run: `cargo clippy --manifest-path stores/prolly-store-turso/Cargo.toml --all-targets --features sync -- -D warnings`

Expected: both commands exit successfully with zero failed tests and zero warnings.

- [ ] **Step 6: Verify compatibility with the core crate**

Run: `cargo test --features async-store`

Expected: the core suite exits successfully with zero failed tests.

- [ ] **Step 7: Audit requirements and worktree scope**

Confirm the adapter has local native storage, opt-in sync, explicit-only network calls, native transactions, conformance and persistence coverage, a runnable example, and complete documentation. Inspect `git status --short` and ensure no unrelated file was modified or resolved.

