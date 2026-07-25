# PostgreSQL service and scale performance implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible, concurrency-first PostgreSQL performance harness and remove the Rust adapter's per-node SQL round trips and global root-writer lock.

**Architecture:** Keep the existing `benchmarks/postgres-scale` executable and add independently selectable service and scale suites. The service path splits configuration, deterministic traces, measurements, workload execution, and orchestration into focused modules; the existing scale path retains compatible inputs and outputs. The adapter uses chunked set-based SQL and per-root transaction advisory locks without changing its schema.

**Tech Stack:** Rust 2021, Tokio, SQLx 0.8, PostgreSQL 16, Serde, TOML 0.8, SHA-2 0.10, hdrhistogram 7.5, Python 3 standard library, Docker Compose.

## Global constraints

- Optimize the Rust `prolly-store-postgres` adapter first; do not modify language-specific adapters.
- Preserve the existing PostgreSQL tables, columns, keys, and stored bytes.
- Preserve `PostgresBackend::new`, `PostgresBackend::connect`, and legacy scale CLI behavior.
- Keep `service` and `scale` raw rows separate; never aggregate duration-based service samples with serial scale samples.
- Use `service` as the default suite and primary regression gate.
- Use closed-loop service workers and include PostgreSQL pool wait in measured latency.
- Default service clients are `1, 8, 32, 64`; default pool sizes are `8, 32`.
- Default service traffic uses 64 tenants, one hot root, and 20% hot-root traffic.
- Default service operations are 45% point read, 15% multi-read, 25% commit, 10% diff, and 5% merge.
- Default service data is 1,000,000 records with 256-byte values; multi-read uses 32 keys and commit uses 16 keys.
- Default warmup is 15 seconds and default measurement is 60 seconds.
- Default adapter SQL batch size is 1,024 items.
- Preserve the existing scale defaults of 1,000,000 and 10,000,000 records with 27-byte values.
- Flush every validated cell durably; rerun an interrupted duration cell in full.
- Treat expected compare-and-swap conflicts as measurements, not harness failures.
- Fail on correctness errors, malformed or duplicate rows, worker panics, incompatible resumes, and strict regression-budget violations.
- Record the command, resolved configuration, revision, environment, raw rows, and comparison method for every performance claim.

---

### Task 1: Add adapter options and set-based ordered reads

**Files:**
- Modify: `stores/prolly-store-postgres/src/lib.rs`
- Modify: `stores/prolly-store-postgres/tests/postgres_backend.rs`
- Modify: `stores/prolly-store-postgres/README.md`

**Interfaces:**
- Produces: `PostgresBackendOptions::new(NonZeroUsize) -> Self`
- Produces: `PostgresBackendOptions::max_batch_items(self) -> usize`
- Produces: `PostgresBackend::new_with_options(PgPool, PostgresBackendOptions) -> Self`
- Produces: `PostgresBackend::connect_with_options(&str, PostgresBackendOptions) -> Result<Self, sqlx::Error>`
- Produces: `PostgresBackend::options(&self) -> PostgresBackendOptions`
- Preserves: `PostgresBackend::new(PgPool) -> Self`
- Preserves: `PostgresBackend::connect(&str) -> Result<Self, sqlx::Error>`

- [ ] **Step 1: Add failing option and multi-chunk read tests**

Add these tests to `stores/prolly-store-postgres/tests/postgres_backend.rs`. Keep them in the existing environment-gated test so the shared database is cleared once.

```rust
use std::num::NonZeroUsize;

use prolly::RemoteStoreBackend;
use prolly_store_postgres::{PostgresBackend, PostgresBackendOptions};

#[test]
fn postgres_backend_options_default_to_1024_items() {
    assert_eq!(PostgresBackendOptions::default().max_batch_items(), 1_024);
}

async fn assert_ordered_reads_are_set_based(
    database_url: &str,
    pool: sqlx::PgPool,
) {
    let options = PostgresBackendOptions::new(NonZeroUsize::new(2).unwrap());
    let backend = PostgresBackend::new_with_options(pool, options);
    backend.put_node(b"a", b"A").await.unwrap();
    backend.put_node(b"b", b"B").await.unwrap();
    backend.put_node(b"c", b"C").await.unwrap();

    let keys: Vec<&[u8]> = vec![b"c", b"missing", b"a", b"c", b"b"];
    assert_eq!(
        backend.batch_get_nodes_ordered(&keys).await.unwrap(),
        vec![
            Some(b"C".to_vec()),
            None,
            Some(b"A".to_vec()),
            Some(b"C".to_vec()),
            Some(b"B".to_vec()),
        ]
    );
    assert!(backend.batch_get_nodes_ordered(&[]).await.unwrap().is_empty());

    let connected = PostgresBackend::connect_with_options(
        database_url,
        PostgresBackendOptions::new(NonZeroUsize::new(3).unwrap()),
    )
    .await
    .unwrap();
    assert_eq!(connected.options().max_batch_items(), 3);
}
```

Call `assert_ordered_reads_are_set_based(&database_url, backend.pool().clone()).await` after the conformance assertion.

- [ ] **Step 2: Run the adapter tests and verify the new API fails to compile**

Run:

```bash
cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml
```

Expected: compilation fails because `PostgresBackendOptions`, `new_with_options`, `connect_with_options`, and `options` do not exist.

- [ ] **Step 3: Implement options and ordered chunked reads**

Add this public option type and store it on `PostgresBackend`:

```rust
use std::num::NonZeroUsize;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PostgresBackendOptions {
    max_batch_items: NonZeroUsize,
}

impl PostgresBackendOptions {
    pub const fn new(max_batch_items: NonZeroUsize) -> Self {
        Self { max_batch_items }
    }

    pub const fn max_batch_items(self) -> usize {
        self.max_batch_items.get()
    }
}

impl Default for PostgresBackendOptions {
    fn default() -> Self {
        Self::new(NonZeroUsize::new(1_024).expect("1024 is nonzero"))
    }
}
```

Make the existing constructors delegate to the option-aware constructors. Implement each read chunk with this query:

```sql
SELECT requested.ord, nodes.node
FROM unnest($1::bytea[]) WITH ORDINALITY AS requested(cid, ord)
LEFT JOIN prolly_nodes AS nodes ON nodes.cid = requested.cid
ORDER BY requested.ord
```

Bind `Vec<Vec<u8>>`, decode `Option<Vec<u8>>`, concatenate chunks in input order, and return immediately for empty input.

- [ ] **Step 4: Run format, unit, and Docker-backed adapter tests**

Run:

```bash
cargo fmt --manifest-path stores/prolly-store-postgres/Cargo.toml -- --check
cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml \
  postgres_backend_satisfies_remote_backend_contract_when_url_is_set -- --nocapture
```

Expected: all commands exit 0 and the integration test preserves duplicates, missing entries, and order across three SQL chunks.

- [ ] **Step 5: Document the compatible constructor and batch-read behavior**

Add a README example that imports `NonZeroUsize`, constructs `PostgresBackendOptions`, and passes it to `connect_with_options`. State that `new` and `connect` use 1,024-item chunks.

- [ ] **Step 6: Commit the adapter read optimization**

```bash
git add stores/prolly-store-postgres
git commit -m "perf(postgres): batch ordered node reads"
```

### Task 2: Implement atomic set-based node writes

**Files:**
- Modify: `stores/prolly-store-postgres/src/lib.rs`
- Modify: `stores/prolly-store-postgres/tests/postgres_backend.rs`

**Interfaces:**
- Consumes: `PostgresBackendOptions::max_batch_items() -> usize`
- Produces internally: `upsert_node_chunks(&mut PgConnection, &[(&[u8], &[u8])], usize) -> Result<(), sqlx::Error>`
- Produces internally: `delete_node_chunks(&mut PgConnection, &[&[u8]], usize) -> Result<(), sqlx::Error>`
- Preserves: `RemoteStoreBackend::batch_nodes`, `batch_put_nodes`, `batch_put_nodes_with_hint`

- [ ] **Step 1: Add failing multi-chunk write and mixed-order tests**

Extend the environment-gated integration test with:

```rust
let options = PostgresBackendOptions::new(NonZeroUsize::new(2).unwrap());
let backend = PostgresBackend::new_with_options(backend.pool().clone(), options);
let entries: Vec<(&[u8], &[u8])> = vec![
    (b"a", b"A1"),
    (b"b", b"B1"),
    (b"c", b"C1"),
    (b"d", b"D1"),
    (b"e", b"E1"),
];
backend.batch_put_nodes(&entries).await.unwrap();

backend
    .batch_nodes(&[
        prolly::RemoteBatchOp::Upsert { key: b"a", value: b"A2" },
        prolly::RemoteBatchOp::Delete { key: b"a" },
        prolly::RemoteBatchOp::Upsert { key: b"a", value: b"A3" },
        prolly::RemoteBatchOp::Upsert { key: b"b", value: b"B2" },
        prolly::RemoteBatchOp::Delete { key: b"b" },
    ])
    .await
    .unwrap();
assert_eq!(backend.get_node(b"a").await.unwrap(), Some(b"A3".to_vec()));
assert_eq!(backend.get_node(b"b").await.unwrap(), None);
```

Install two test triggers. An `AFTER INSERT FOR EACH STATEMENT` trigger increments a row in `prolly_bench.test_statement_counts`; five new entries with chunk size two must increment it exactly three times. A `BEFORE INSERT FOR EACH ROW` trigger raises for CID `fail-third`; execute a three-entry batch with chunk size two, assert the call errors, and assert neither of the first two CIDs exists. Drop both triggers, both functions, and the counter table in a cleanup block.

- [ ] **Step 2: Verify the atomic rollback test fails against sequential chunk commits**

Run:

```bash
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml \
  postgres_backend_satisfies_remote_backend_contract_when_url_is_set -- --nocapture
```

Expected: the statement-count assertion observes five insert statements instead of three. The forced-failure assertion also proves the existing transaction rollback behavior before refactoring.

- [ ] **Step 3: Add private set-based write helpers**

Use these SQL forms inside one caller-owned transaction:

```sql
INSERT INTO prolly_nodes (cid, node)
SELECT input.cid, input.node
FROM unnest($1::bytea[], $2::bytea[]) AS input(cid, node)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node
```

```sql
DELETE FROM prolly_nodes
WHERE cid = ANY($1::bytea[])
```

For `batch_nodes`, reduce repeated keys to their final `Option<Vec<u8>>` in a `BTreeMap`. Execute all final deletes and upserts in the same transaction. For `batch_put_nodes_with_hint`, execute node chunks and the hint upsert before one commit. Empty inputs must not open a transaction unless a hint still needs to be written.

- [ ] **Step 4: Verify atomicity and conformance**

Run:

```bash
cargo fmt --manifest-path stores/prolly-store-postgres/Cargo.toml -- --check
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml -- --nocapture
```

Expected: all tests pass; the trigger failure rolls back entries from earlier chunks.

- [ ] **Step 5: Commit set-based writes**

```bash
git add stores/prolly-store-postgres/src/lib.rs stores/prolly-store-postgres/tests/postgres_backend.rs
git commit -m "perf(postgres): batch node writes"
```

### Task 3: Replace the global root lock with ordered per-root locks

**Files:**
- Modify: `stores/prolly-store-postgres/src/lib.rs`
- Modify: `stores/prolly-store-postgres/tests/postgres_backend.rs`
- Modify: `stores/prolly-store-postgres/README.md`

**Interfaces:**
- Produces internally: `lock_root_names(&mut PgConnection, impl IntoIterator<Item = &[u8]>) -> Result<(), sqlx::Error>`
- Produces internally: `read_root_manifests(&mut PgConnection, &[Vec<u8>]) -> Result<BTreeMap<Vec<u8>, Option<Vec<u8>>>, sqlx::Error>`
- Preserves: `compare_and_swap_root_manifest` and `commit_transaction` result types

- [ ] **Step 1: Add failing concurrency tests**

Add one same-root and one independent-root test:

```rust
let contenders = (0..16)
    .map(|index| {
        let backend = backend.clone();
        tokio::spawn(async move {
            backend
                .compare_and_swap_root_manifest(
                    b"hot/main",
                    None,
                    Some(format!("manifest-{index}").as_bytes()),
                )
                .await
                .unwrap()
        })
    })
    .collect::<Vec<_>>();
let mut applied = 0;
for contender in contenders {
    if matches!(
        contender.await.unwrap(),
        prolly::RemoteManifestUpdate::Applied
    ) {
        applied += 1;
    }
}
assert_eq!(applied, 1);
```

For independent progress, begin a raw SQL transaction, acquire the exact advisory lock for `blocked/main`, start CAS calls for `blocked/main` and `free/main`, and assert `free/main` completes within 500 ms while `blocked/main` remains pending. Roll back the raw transaction and assert the blocked operation then completes.

Add two strict transactions whose condition/write root lists are reversed. Start them together and use a 2-second timeout to prove they return without a deadlock.

- [ ] **Step 2: Run the concurrency tests and observe the table-lock failure**

Run:

```bash
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml -- --nocapture
```

Expected: the independent root assertion times out because the existing table lock blocks unrelated root writers.

- [ ] **Step 3: Implement the shared advisory-lock protocol**

Sort and deduplicate names as owned `Vec<Vec<u8>>`. Acquire each lock with:

```sql
SELECT pg_advisory_xact_lock(
  hashtextextended('prolly-root-v1:' || encode($1, 'hex'), 0)
)
```

Wrap unconditional root put/delete in transactions that acquire the same lock. Replace the table lock in CAS and strict transactions.

- [ ] **Step 4: Batch strict root reads and writes**

Read conditions with:

```sql
SELECT requested.name, roots.manifest
FROM unnest($1::bytea[]) AS requested(name)
LEFT JOIN prolly_roots AS roots ON roots.name = requested.name
```

Apply root puts with paired arrays and `UNNEST`; apply deletes with `ANY($1::bytea[])`. Reuse Task 2's node helpers inside the strict transaction. Compare every expected optional manifest before any writes. On the first mismatch, roll back and return `RemoteTransactionUpdate::Conflict` containing that name, expected manifest, and current manifest.

- [ ] **Step 5: Verify concurrency, rollback, and conformance**

Run:

```bash
cargo fmt --manifest-path stores/prolly-store-postgres/Cargo.toml -- --check
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml -- --nocapture
```

Expected: same-root CAS has one winner, free roots progress while another root is locked, reversed multi-root inputs do not deadlock, and conformance remains green.

- [ ] **Step 6: Document the lock contract**

Update the README operational notes with the exact advisory-lock expression, sorted multi-root acquisition rule, collision behavior, and the requirement that future adapters use the same protocol for compatible concurrent writes.

- [ ] **Step 7: Commit root scalability**

```bash
git add stores/prolly-store-postgres
git commit -m "perf(postgres): isolate named root writers"
```

### Task 4: Add versioned TOML configuration and CLI compatibility

**Files:**
- Create: `benchmarks/postgres-scale/src/config.rs`
- Create: `benchmarks/postgres-scale/workloads/default.toml`
- Create: `benchmarks/postgres-scale/workloads/smoke.toml`
- Modify: `benchmarks/postgres-scale/Cargo.toml`
- Modify: `benchmarks/postgres-scale/src/lib.rs`
- Modify: `benchmarks/postgres-scale/src/cli.rs`

**Interfaces:**
- Produces: `WorkloadConfig::load(&Path) -> Result<Self, String>`
- Produces: `WorkloadConfig::parse(&str) -> Result<Self, String>`
- Produces: `WorkloadConfig::validate(&self) -> Result<(), String>`
- Produces: `WorkloadConfig::canonical_toml(&self) -> Result<String, String>`
- Produces: `WorkloadConfig::configuration_hash(&self) -> Result<String, String>`
- Produces: `CommandConfig { workload, url, output, revision, dirty, suites, baseline, allow_environment_mismatch }`
- Preserves: legacy `--profile`, `--sizes`, `--runs`, `--operations`, `--patterns`, `--changes`, `--read-samples`, and `--min-free-gb`

- [ ] **Step 1: Add failing TOML, validation, hash, and override tests**

Add tests in `config.rs` and `cli.rs`:

```rust
#[test]
fn default_file_matches_service_contract() {
    let config = WorkloadConfig::load(Path::new("workloads/default.toml")).unwrap();
    assert!(config.service.enabled);
    assert!(!config.scale.enabled);
    assert_eq!(config.service.clients, vec![1, 8, 32, 64]);
    assert_eq!(config.service.pool_sizes, vec![8, 32]);
    assert_eq!(config.service.operation_mix.total(), 100);
    assert_eq!(config.service.value_bytes, 256);
    assert_eq!(config.service.adapter_batch_items.get(), 1_024);
}

#[test]
fn canonical_hash_changes_only_when_resolved_workload_changes() {
    let first = WorkloadConfig::load(Path::new("workloads/smoke.toml")).unwrap();
    let second = first.clone();
    assert_eq!(
        first.configuration_hash().unwrap(),
        second.configuration_hash().unwrap()
    );
    let mut changed = second;
    changed.service.clients.push(3);
    assert_ne!(
        first.configuration_hash().unwrap(),
        changed.configuration_hash().unwrap()
    );
}

#[test]
fn rejects_zero_batch_and_invalid_mix() {
    let mut config = WorkloadConfig::load(Path::new("workloads/smoke.toml")).unwrap();
    config.service.operation_mix.commit = 24;
    assert!(config.validate().unwrap_err().contains("total 100"));

    let source = std::fs::read_to_string("workloads/smoke.toml")
        .unwrap()
        .replace("adapter_batch_items = 1024", "adapter_batch_items = 0");
    assert!(WorkloadConfig::parse(&source).unwrap_err().contains("nonzero"));
}
```

Add a CLI test proving `--config workloads/smoke.toml --suite both --clients 2,4 --pool-sizes 2 --sizes 1000` resolves both suites and retains revision/output flags.

- [ ] **Step 2: Run tests and verify missing configuration types**

Run:

```bash
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml config cli
```

Expected: compilation fails because `config.rs` and the new command model do not exist.

- [ ] **Step 3: Add dependencies and typed configuration**

Add:

```toml
hdrhistogram = "7.5"
sha2 = "0.10"
toml = "0.8"
```

Define `WorkloadConfig`, `ServiceConfig`, `ScaleConfig`, `OperationMix`, and `RegressionConfig` with `#[serde(deny_unknown_fields)]`. Represent adapter batch items as `NonZeroUsize`. Implement canonical TOML serialization with `toml::to_string`, then lowercase SHA-256 hex encoding.

The default TOML must encode every default in the design. The smoke TOML uses 1,000 records, 64-byte values, clients `[1, 4]`, pool size `[2]`, 100 ms warmup, 500 ms measurement, four tenants, one hot root, 25% hot share, eight-key multi-read, four-key commit, and both suites.

- [ ] **Step 4: Refactor CLI parsing around `CommandConfig`**

Parse the config path first, load TOML, then apply explicit overrides. Map a legacy `--profile smoke|full` invocation to scale-only behavior unless `--suite` is present. Reject unknown options and invalid resolved configuration.

- [ ] **Step 5: Verify configuration and existing CLI tests**

Run:

```bash
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml config cli
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml full_profile_has_requested_scale_and_repetitions
```

Expected: all tests pass and legacy full scale still resolves to 1,000,000 and 10,000,000 records with three repetitions.

- [ ] **Step 6: Commit configuration**

```bash
git add benchmarks/postgres-scale
git commit -m "feat(bench): configure postgres workloads with TOML"
```

### Task 5: Generate deterministic service traces and matrices

**Files:**
- Create: `benchmarks/postgres-scale/src/service_model.rs`
- Modify: `benchmarks/postgres-scale/src/model.rs`
- Modify: `benchmarks/postgres-scale/src/lib.rs`

**Interfaces:**
- Produces: `ServiceOperation::{PointRead, MultiRead, Commit, Diff, Merge}`
- Produces: `TenantClass::{Independent, Hot}`
- Produces: `ServiceCell { clients: usize, pool_size: u32 }`
- Produces: `TraceItem { sequence, operation, tenant, tenant_class, root_name, key_ids, generation }`
- Produces: `enumerate_service_cells(&ServiceConfig) -> Vec<ServiceCell>`
- Produces: `generate_trace(&ServiceConfig, usize) -> Vec<TraceItem>`
- Produces: `value_sized(id: usize, generation: u64, bytes: usize) -> Vec<u8>`

- [ ] **Step 1: Add failing trace, matrix, and value tests**

```rust
#[test]
fn service_matrix_is_clients_times_pool_sizes() {
    let config = smoke_service_config();
    let cells = enumerate_service_cells(&config);
    assert_eq!(cells.len(), config.clients.len() * config.pool_sizes.len());
    assert!(cells.contains(&ServiceCell { clients: 4, pool_size: 2 }));
}

#[test]
fn trace_is_stable_and_exactly_weighted() {
    let config = smoke_service_config();
    let first = generate_trace(&config, 10_000);
    let second = generate_trace(&config, 10_000);
    assert_eq!(first, second);
    assert_eq!(count(&first, ServiceOperation::PointRead), 4_500);
    assert_eq!(count(&first, ServiceOperation::MultiRead), 1_500);
    assert_eq!(count(&first, ServiceOperation::Commit), 2_500);
    assert_eq!(count(&first, ServiceOperation::Diff), 1_000);
    assert_eq!(count(&first, ServiceOperation::Merge), 500);
    assert_eq!(
        first.iter().filter(|item| item.tenant_class == TenantClass::Hot).count(),
        2_500
    );
}

#[test]
fn sized_values_are_exact_and_deterministic() {
    assert_eq!(value_sized(7, 3, 256).len(), 256);
    assert_eq!(value_sized(7, 3, 256), value_sized(7, 3, 256));
    assert_ne!(value_sized(7, 3, 256), value_sized(7, 4, 256));
}
```

- [ ] **Step 2: Run the model tests and verify they fail**

Run:

```bash
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_model
```

Expected: compilation fails because the service model is absent.

- [ ] **Step 3: Implement the deterministic model**

Use the existing xorshift sequence seeded by the configured seed. Assign operation from `sequence % 100` and cumulative weights, then deterministically shuffle 100-operation blocks so workers do not receive long runs of one operation. Ensure the first block contains every `(ServiceOperation, TenantClass)` pair so short smoke cells exercise all result categories. Select exactly the configured hot share over each 10,000 trace items. Generate fixed tenant root names as `tenant/{tenant:06}/main`, `/left`, and `/right`.

Implement `value_sized` by repeatedly hashing `(id, generation, block_index)` with SHA-256 and truncating to the requested byte length. Make the current `value(id, generation)` delegate to `value_sized(id, generation as u64, 27)`.

- [ ] **Step 4: Verify deterministic behavior**

Run:

```bash
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_model model
```

Expected: all trace counts, hot-root counts, and exact value lengths pass.

- [ ] **Step 5: Commit the workload model**

```bash
git add benchmarks/postgres-scale/src
git commit -m "feat(bench): model postgres service load"
```

### Task 6: Add service measurements and durable rows

**Files:**
- Create: `benchmarks/postgres-scale/src/service_measurement.rs`
- Modify: `benchmarks/postgres-scale/src/lib.rs`
- Modify: `benchmarks/postgres-scale/Cargo.toml`

**Interfaces:**
- Produces: `OperationSample { operation, tenant_class, latency_ns, attempts, conflicts, retries, outcome }`
- Produces: `ServiceAccumulator::record(OperationSample) -> Result<(), String>`
- Produces: `ServiceAccumulator::merge(&mut self, Self) -> Result<(), String>`
- Produces: `ServiceRawRow` with `key()`, `validate()`, and PostgreSQL/physical metrics
- Produces: `ServiceCsvSink::open(&Path) -> Result<Self, String>` and `append(&ServiceRawRow)`

- [ ] **Step 1: Add failing histogram, validation, and CSV tests**

```rust
#[test]
fn accumulator_reports_tail_latency_and_retry_counts() {
    let mut accumulator = ServiceAccumulator::new(Duration::from_secs(10)).unwrap();
    for latency in 1..=1_000_u64 {
        accumulator
            .record(OperationSample::success(
                ServiceOperation::Commit,
                TenantClass::Hot,
                latency * 1_000,
                2,
                1,
            ))
            .unwrap();
    }
    let summary = accumulator.summary(
        ServiceOperation::Commit,
        TenantClass::Hot,
    ).unwrap();
    assert_eq!(summary.completed, 1_000);
    assert_eq!(summary.conflicts, 1_000);
    assert!(summary.p99_ns >= 990_000);
    assert!(summary.p999_ns >= summary.p99_ns);
}

#[test]
fn duration_row_rejects_inconsistent_throughput() {
    let mut row = ServiceRawRow::example();
    row.successful_ops_per_sec = 1.0;
    assert!(row.validate().unwrap_err().contains("throughput"));
}

#[test]
fn service_csv_round_trips_failure_text() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("service-raw.csv");
    let mut row = ServiceRawRow::example();
    row.validated = false;
    row.error = "worker, failed\njoin".to_string();
    ServiceCsvSink::open(&path).unwrap().append(&row).unwrap();
    assert_eq!(read_service_rows(&path).unwrap(), vec![row]);
}
```

- [ ] **Step 2: Run measurement tests and verify missing types**

Run:

```bash
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_measurement
```

Expected: compilation fails because service measurement types do not exist.

- [ ] **Step 3: Implement local HDR accumulators**

Give each worker a local `ServiceAccumulator` containing one HDR histogram per `(ServiceOperation, TenantClass)`. Record values in nanoseconds up to the configured timeout and three significant figures. Merge worker histograms after joining workers.

`ServiceRawRow` must include identity, duration, attempts, completions, throughput, p50/p95/p99/p99.9/max, conflicts, retries, exhausted retries, semantic conflicts, timeouts, SQL errors, validation errors, panic count, Prolly counters, `PgMetrics`, and `PhysicalSize` before/after.

Validate arithmetic, finite values, histogram ordering, conflict bounds, and successful cell state. `sync_data` after every appended row.

- [ ] **Step 4: Run measurement tests**

Run:

```bash
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_measurement
```

Expected: all tests pass, including CSV escaping and p99.9 ordering.

- [ ] **Step 5: Commit service measurements**

```bash
git add benchmarks/postgres-scale
git commit -m "feat(bench): record postgres service latency"
```

### Task 7: Build and execute version-control service operations

**Files:**
- Create: `benchmarks/postgres-scale/src/service_workloads.rs`
- Modify: `benchmarks/postgres-scale/src/postgres.rs`
- Modify: `benchmarks/postgres-scale/src/lib.rs`

**Interfaces:**
- Produces: `ServiceFixture::build(PostgresBackend, &ServiceConfig) -> Result<Self, String>`
- Produces: `ServiceFixture::restore(&self) -> Result<(), String>`
- Produces: `ServiceFixture::validate(&self) -> Result<(), String>`
- Produces: `execute_operation(&ServiceFixture, &TraceItem, &ServiceConfig) -> Result<OperationSample, String>`
- Consumes: `AsyncProlly::get`, `get_many`, `batch`, `diff`, `merge`, `load_named_root`, and `compare_and_swap_named_root`

- [ ] **Step 1: Add a failing Docker-backed operation smoke test**

```rust
#[tokio::test]
#[ignore = "requires PROLLY_STORE_POSTGRES_URL"]
async fn service_fixture_exercises_every_operation() {
    let url = std::env::var("PROLLY_STORE_POSTGRES_URL").unwrap();
    let config = smoke_service_config();
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .unwrap();
    let backend = PostgresBackend::new_with_options(
        pool,
        PostgresBackendOptions::new(config.adapter_batch_items),
    );
    let fixture = ServiceFixture::build(backend, &config).await.unwrap();
    for operation in ServiceOperation::ALL {
        let item = trace_item_for(operation, TenantClass::Independent, &config);
        let sample = execute_operation(&fixture, &item, &config).await.unwrap();
        assert!(sample.latency_ns > 0);
    }
    fixture.validate().await.unwrap();
    fixture.restore().await.unwrap();
    fixture.validate().await.unwrap();
}
```

- [ ] **Step 2: Run the ignored smoke test and verify missing workload code**

Run:

```bash
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml \
  service_fixture_exercises_every_operation -- --ignored --nocapture
```

Expected: compilation fails because `ServiceFixture` and `execute_operation` do not exist.

- [ ] **Step 3: Implement fixture creation and restoration**

Build the configured base with `value_sized`, publish tenant main/left/right roots, retain prepared disjoint branch descendants, validate three base keys, and snapshot all production tables. Store recent versions per tenant in `Arc<tokio::sync::RwLock<VecDeque<Tree>>>`.

Add service-specific snapshot metadata to `prolly_bench`, including configuration hash and root count. `restore` must verify the metadata before copying tables. Rebuild the in-memory catalog from named roots after restoration.

- [ ] **Step 4: Implement point read, multi-read, and commit**

Point and multi-read load the current named root and validate exact deterministic values.

Commit retries this sequence up to `cas_retries`:

```rust
let expected = manager
    .load_named_root(&item.root_name)
    .await
    .map_err(error)?
    .ok_or_else(|| "commit root is missing".to_string())?;
let changed = manager
    .batch(&expected, mutations(item, config))
    .await
    .map_err(error)?;
match manager
    .compare_and_swap_named_root(&item.root_name, Some(&expected), Some(&changed))
    .await
    .map_err(error)?
{
    NamedRootUpdate::Applied => {
        fixture.retain_version(item.tenant, changed).await;
        break;
    }
    NamedRootUpdate::Conflict { .. } => conflicts += 1,
}
```

Measure from before the first load through the final outcome. Return exhausted retries as a measured outcome, not a Rust error.

- [ ] **Step 5: Implement diff and merge**

Diff consumes all differences between two retained versions and checks changed keys against retained metadata.

Merge loads the retained base and prepared branch descendants, merges disjoint changes, and CAS publishes the configured target. Record semantic merge conflicts separately. Validate both branches and one unaffected key after an applied merge.

- [ ] **Step 6: Verify every operation and snapshot restoration**

Run:

```bash
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55432/prolly \
  cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml \
  service_fixture_exercises_every_operation -- --ignored --nocapture
```

Expected: every operation returns a sample, final validation passes, and restored roots match the snapshot.

- [ ] **Step 7: Commit service workloads**

```bash
git add benchmarks/postgres-scale/src
git commit -m "feat(bench): execute postgres version workloads"
```

### Task 8: Orchestrate concurrent cells with cancellation and resume

**Files:**
- Create: `benchmarks/postgres-scale/src/service_harness.rs`
- Create: `benchmarks/postgres-scale/src/runner.rs`
- Modify: `benchmarks/postgres-scale/src/harness.rs`
- Modify: `benchmarks/postgres-scale/src/main.rs`
- Modify: `benchmarks/postgres-scale/src/lib.rs`

**Interfaces:**
- Produces: `run_service_suite(&CommandConfig) -> Result<RunStats, String>`
- Produces: `run_benchmark(CommandConfig) -> Result<RunStats, String>`
- Preserves: `run_matrix(RunConfig) -> Result<RunStats, String>` for scale tests

- [ ] **Step 1: Add failing matrix, resume, and worker-failure tests**

```rust
#[test]
fn service_cell_keys_include_pool_and_concurrency() {
    let first = ServiceCellKey::new("hash", 8, 8);
    let second = ServiceCellKey::new("hash", 8, 32);
    assert_ne!(first, second);
}

#[tokio::test]
async fn interrupted_cell_is_not_resumable() {
    let temp = tempfile::tempdir().unwrap();
    write_failure_record(temp.path(), &ServiceCellKey::new("hash", 4, 2), "panic")
        .unwrap();
    let completed = read_completed_cells(temp.path()).unwrap();
    assert!(completed.is_empty());
}

#[tokio::test]
async fn worker_panic_cancels_the_cell() {
    let result = run_workers_for_test(4, || async {
        panic!("injected worker failure");
    })
    .await;
    assert!(result.unwrap_err().contains("worker panic"));
}
```

- [ ] **Step 2: Run harness tests and verify missing orchestration**

Run:

```bash
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_harness runner
```

Expected: compilation fails because service orchestration is absent.

- [ ] **Step 3: Implement one service cell**

For each cell:

1. Create a `PgPool` with the cell's `max_connections`.
2. Construct `PostgresBackend` with configured batch options.
3. Restore and validate the fixture.
4. Run warmup workers.
5. Restore again, reset PostgreSQL statistics, and capture physical size.
6. Spawn `clients` measured workers in a `JoinSet`.
7. Give each operation `tokio::time::timeout(operation_timeout, ...)`.
8. Stop assigning operations at the deadline and finish in-flight operations.
9. Cancel and join every worker after any panic or unexpected error.
10. Merge worker accumulators, read PostgreSQL and size metrics, validate roots, and append one row per operation/tenant class.

Workers claim trace sequence IDs through `AtomicU64`. Use `Arc<AtomicBool>` for cancellation. Do not hold a Tokio lock across an awaited Prolly call.

- [ ] **Step 4: Implement durable resume and failure records**

Write `run-manifest.txt` before fixture creation. Validate existing rows against revision, dirty state, schema, configuration hash, and unique cell keys. Mark a cell complete only after all required operation/tenant rows validate. Write `failure.txt` with cell identity and error before returning nonzero.

- [ ] **Step 5: Add top-level suite dispatch**

`runner::run_benchmark` writes original/resolved TOML, dispatches service and scale suites in the requested order, copies scale outputs to legacy names for legacy scale-only invocations, and returns combined measured/skipped counts. Update `main.rs` to call it.

- [ ] **Step 6: Verify unit orchestration**

Run:

```bash
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml service_harness runner
```

Expected: panic cancellation and incomplete-cell resume tests pass.

- [ ] **Step 7: Commit service orchestration**

```bash
git add benchmarks/postgres-scale/src
git commit -m "feat(bench): run concurrent postgres cells"
```

### Task 9: Add service summaries and strict regression comparison

**Files:**
- Modify: `scripts/summarize_postgres_scale_benchmark.py`
- Modify: `scripts/tests/test_summarize_postgres_scale_benchmark.py`
- Create: `scripts/tests/fixtures/postgres-service-baseline.csv`

**Interfaces:**
- Produces: `validate_service_rows(rows)`
- Produces: `aggregate_service(rows)`
- Produces: `compare_service(current, baseline, budgets, current_environment, baseline_environment, allow_environment_mismatch=False)`
- Produces: combined `report.md`, `service-summary.csv`, and existing scale summary files
- Preserves: existing scale `validate_rows`, `aggregate`, and legacy command arguments

- [ ] **Step 1: Add failing service aggregation and budget tests**

```python
def test_service_summary_keeps_operation_and_tenant_class_separate(self):
    rows = [
        service_row("point_read", "independent", 8, 8, 1000.0, 2_000_000),
        service_row("point_read", "hot", 8, 8, 900.0, 3_000_000),
    ]
    summary = self.module.aggregate_service(rows)
    self.assertEqual(len(summary), 2)

def test_regression_gate_rejects_throughput_p99_and_statement_regressions(self):
    baseline = [service_row("commit", "hot", 32, 8, 1000.0, 10_000_000, pg_calls=2.0)]
    current = [service_row("commit", "hot", 32, 8, 850.0, 13_000_000, pg_calls=3.0)]
    budgets = {
        "max_throughput_loss_percent": 10.0,
        "max_p99_increase_percent": 20.0,
        "max_pg_statements_per_operation": 2.5,
        "minimum_percentile_samples": 1000,
    }
    environment = {"cpu": "test", "postgres_version": "16.14", "settings_hash": "same"}
    with self.assertRaisesRegex(ValueError, "throughput|p99|statements"):
        self.module.compare_service(
            current,
            baseline,
            budgets,
            environment,
            environment,
        )
```

Add tests for missing cells, environment mismatch, exploratory override, insufficient percentile samples, nonzero unexpected error rate, and LF output.

- [ ] **Step 2: Run Python tests and verify missing service functions**

Run:

```bash
python3 -m unittest scripts.tests.test_summarize_postgres_scale_benchmark -v
```

Expected: tests fail because service validation, aggregation, and comparison do not exist.

- [ ] **Step 3: Implement service validation and aggregation**

Use key fields:

```python
SERVICE_KEY_FIELDS = (
    "config_hash",
    "clients",
    "pool_size",
    "operation",
    "tenant_class",
)
```

Validate arithmetic and histogram ordering. Aggregate only rows with identical keys. Write service summary columns for throughput, p50/p95/p99/p99.9/max, conflicts, retries, errors, PostgreSQL calls per operation, WAL per operation, and physical growth.

- [ ] **Step 4: Implement strict comparisons**

Match every current cell to one baseline cell. Calculate percentage throughput loss and p99 increase. Load machine and PostgreSQL fingerprints from both run manifests. Reject missing cells, budget violations, configuration mismatch, and material environment mismatch. `--allow-environment-mismatch` must mark the report exploratory and skip gating rather than silently passing it.

- [ ] **Step 5: Render the combined report**

Lead with a saturation table by clients and pool size. Follow with per-operation tenant-class latency, contention/conflict rates, PostgreSQL calls and write amplification, regression verdict, scale tables, and interpretation limits.

- [ ] **Step 6: Verify all summarizer tests**

Run:

```bash
python3 -m unittest scripts.tests.test_summarize_postgres_scale_benchmark -v
```

Expected: all legacy scale and new service tests pass.

- [ ] **Step 7: Commit reporting and gates**

```bash
git add scripts/summarize_postgres_scale_benchmark.py \
  scripts/tests/test_summarize_postgres_scale_benchmark.py \
  scripts/tests/fixtures/postgres-service-baseline.csv
git commit -m "feat(bench): gate postgres service regressions"
```

### Task 10: Preserve configurable scale behavior and update the repository runner

**Files:**
- Modify: `benchmarks/postgres-scale/src/workloads.rs`
- Modify: `benchmarks/postgres-scale/src/harness.rs`
- Modify: `scripts/run_postgres_scale_benchmark.sh`
- Modify: `scripts/tests/test_run_postgres_scale_benchmark.py`
- Modify: `docs/postgres-scale-performance.md`
- Modify: `stores/prolly-store-postgres/README.md`

**Interfaces:**
- Consumes: `CommandConfig`, `ScaleConfig`, `value_sized`
- Produces: runner options `--config`, `--suite`, `--baseline`, `--allow-environment-mismatch`
- Preserves: `BENCH_PROFILE`, `BENCH_SIZES`, `BENCH_RUNS`, `BENCH_CHANGES`, `BENCH_READ_SAMPLES`, and existing legacy output files

- [ ] **Step 1: Add failing runner forwarding and scale-value tests**

Extend `scripts/tests/test_run_postgres_scale_benchmark.py` to invoke the shell runner with a fake executable and assert that it forwards:

```python
for expected in (
    "--config",
    "workloads/smoke.toml",
    "--suite",
    "both",
    "--baseline",
    str(baseline),
):
    self.assertIn(expected, arguments)
```

Add a Rust scale test:

```rust
#[test]
fn scale_values_follow_configured_length() {
    assert_eq!(scale_value(7, 1, 27).len(), 27);
    assert_eq!(scale_value(7, 1, 4_096).len(), 4_096);
}
```

- [ ] **Step 2: Run runner and scale tests and verify failures**

Run:

```bash
python3 -m unittest scripts.tests.test_run_postgres_scale_benchmark -v
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml scale_values_follow_configured_length
```

Expected: the runner does not forward the new arguments and scale values remain fixed.

- [ ] **Step 3: Thread scale value size through fixture and cell specs**

Add `value_bytes` to the scale config, fixture, and cell specification. Replace `value(id, generation)` calls in scale workloads with `value_sized(id, generation as u64, value_bytes)`. Keep 27 bytes as the legacy/default value.

- [ ] **Step 4: Update the shell runner**

Support:

```text
--config PATH
--suite service|scale|both
--baseline PATH
--allow-environment-mismatch
```

Capture the original and resolved configuration, adapter batch size, suite list, pool sizes, service matrix, PostgreSQL settings, and binary hash. Invoke the summarizer with both raw files when present. Preserve Docker lifecycle and `BENCH_CLEANUP`.

- [ ] **Step 5: Update documentation**

Document:

- default service run;
- correctness smoke run;
- service-only and scale-only selection;
- external PostgreSQL URL;
- workload TOML fields and CLI overrides;
- baseline recording and strict comparison;
- interpretation of conflicts, tail latency, and pool saturation;
- legacy scale compatibility; and
- the exact Rust adapter batching and advisory-lock contract.

- [ ] **Step 6: Verify runner, scale, and documentation formatting**

Run:

```bash
python3 -m unittest scripts.tests.test_run_postgres_scale_benchmark -v
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml model harness workloads
bash -n scripts/run_postgres_scale_benchmark.sh
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit runner and scale compatibility**

```bash
git add benchmarks/postgres-scale scripts/run_postgres_scale_benchmark.sh \
  scripts/tests/test_run_postgres_scale_benchmark.py \
  docs/postgres-scale-performance.md stores/prolly-store-postgres/README.md
git commit -m "docs(bench): reproduce postgres service performance"
```

### Task 11: Run end-to-end smoke, capture performance evidence, and finalize

**Files:**
- Create: `performance-results/postgres-service-smoke/` generated artifacts
- Modify: `docs/postgres-scale-performance.md`

**Interfaces:**
- Consumes: the completed adapter, harness, runner, and summarizer
- Produces: validated service and scale smoke outputs plus measured before/after interpretation

- [ ] **Step 1: Run the full non-Docker verification**

Run:

```bash
cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml
python3 -m unittest \
  scripts.tests.test_run_postgres_scale_benchmark \
  scripts.tests.test_summarize_postgres_scale_benchmark -v
bash -n scripts/run_postgres_scale_benchmark.sh
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 2: Start the dedicated PostgreSQL service**

Run:

```bash
PROLLY_POSTGRES_BENCH_PORT=55433 \
  docker compose -p prolly-postgres-scale-bench \
  -f benchmarks/postgres-scale/docker-compose.yml up -d postgres
```

Wait until:

```bash
docker inspect --format '{{.State.Health.Status}}' \
  prolly-postgres-scale-bench-postgres-1
```

prints `healthy`.

- [ ] **Step 3: Run adapter integration tests**

Run:

```bash
PROLLY_STORE_POSTGRES_URL=postgres://prolly:prolly@127.0.0.1:55433/prolly \
  cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml -- --nocapture
```

Expected: conformance, bulk rollback, same-root CAS, independent-root progress, and reversed multi-root tests pass.

- [ ] **Step 4: Run the combined smoke workload**

Run:

```bash
scripts/run_postgres_scale_benchmark.sh \
  --config benchmarks/postgres-scale/workloads/smoke.toml \
  --suite both \
  --output performance-results/postgres-service-smoke
```

Expected: `service-raw.csv`, `service-summary.csv`, `scale-raw.csv`, `scale-summary.csv`, legacy scale copies, resolved TOML, manifest, provenance, and `report.md` exist; every required cell validates.

- [ ] **Step 5: Verify SQL round-trip reduction from raw metrics**

Read the recorded pre-change rows in `performance-results/postgres/baseline/raw-results.csv` and the new smoke report. Compare cells with matching operation semantics and assert:

- an ordered batch read of more than one node uses no more than `ceil(nodes / 1,024)` adapter SQL read statements per engine batch;
- a node publication uses no more than `ceil(nodes / 1,024)` upsert statements plus its transaction/hint statements; and
- the old adapter's per-node statement amplification is absent from the matching new cells; and
- independent-root writers complete while hot-root conflicts remain measurable.

Record exact observed calls and rates in `docs/postgres-scale-performance.md`; do not replace observations with estimates.

- [ ] **Step 6: Run a representative concurrency sweep**

Create an untracked local override from the default TOML with 100,000 records, 5-second warmup, 20-second measurement, clients `[1, 8, 32, 64]`, and pool sizes `[8, 32]`. Run:

```bash
scripts/run_postgres_scale_benchmark.sh \
  --config /tmp/prolly-postgres-service-verification.toml \
  --suite service \
  --output performance-results/postgres-service-verification
```

Expected: all eight cells validate with zero unexpected errors and produce a saturation curve. Report observed throughput, p99, conflicts, retries, and PostgreSQL calls without adding universal thresholds.

- [ ] **Step 7: Run the complete final verification again**

Run:

```bash
cargo fmt --manifest-path stores/prolly-store-postgres/Cargo.toml -- --check
cargo fmt --manifest-path benchmarks/postgres-scale/Cargo.toml -- --check
cargo test --manifest-path stores/prolly-store-postgres/Cargo.toml
cargo test --manifest-path benchmarks/postgres-scale/Cargo.toml
python3 -m unittest \
  scripts.tests.test_run_postgres_scale_benchmark \
  scripts.tests.test_summarize_postgres_scale_benchmark -v
bash -n scripts/run_postgres_scale_benchmark.sh
git diff --check
```

Expected: all commands exit 0 after the generated report and documentation update.

- [ ] **Step 8: Commit measured evidence**

```bash
git add docs/postgres-scale-performance.md \
  performance-results/postgres-service-smoke
git commit -m "perf: validate postgres service scalability"
```
