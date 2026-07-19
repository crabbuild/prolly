# Async-First Foundation Verification

Date: 2026-07-18  
Architecture: aarch64 macOS, Apple M2 Max, 32 GiB  
Rust: 1.97.0  
Foundation read-cutover commit: `d8b7feb0a0d89b54de1099f3486bd9dd29abe4dd`

## Result

The foundation correctness gates pass. The synchronous and async-adapted
facades execute the same canonical `ProllyEngine` packed-node read path. The
release sentinel found no read regression: async-adapted median throughput was
20.5% higher for repeated point reads and 3.0% higher for `get_many` in this
run. These small facade-level differences are not treated as a backend result;
both sides use the same local `MemStore` and ready-only adapter.

## Benchmark

Command:

```text
PROLLY_FOUNDATION_RECORDS=100000 \
PROLLY_FOUNDATION_LOOKUPS=1000 \
PROLLY_FOUNDATION_SAMPLES=30 \
cargo bench --bench async_first_foundation_bench
```

The fixture contains 100,000 sorted entries. Both managers share the exact same
in-memory tree. A correctness/warmup `get_many` runs before measurement. Each
sample reads the same deterministic 1,000-key permutation. Reported latency is
per 1,000-item sample; throughput uses the median. Peak RSS is the process-wide
`getrusage` high-water mark and therefore includes the shared fixture and both
facades.

| Facade | API | Median | p95 | Throughput | Peak RSS |
|---|---:|---:|---:|---:|---:|
| sync ready-only | get | 326.334 us | 552.667 us | 3,064,345 items/s | 19.95 MiB |
| async adapted | get | 270.917 us | 308.834 us | 3,691,167 items/s | 20.08 MiB |
| sync ready-only | get_many | 466.667 us | 652.166 us | 2,142,856 items/s | 20.33 MiB |
| async adapted | get_many | 453.125 us | 590.459 us | 2,206,897 items/s | 20.34 MiB |

Allocation counts are unavailable in the current stable toolchain. The test
instead asserts that read-only engine traversal calls `get_shared`, and panics
if it falls back to the owned `get` API. Raw measurements are in `results.csv`;
machine metadata is in `machine.txt`.

## Persisted Root Vectors

The exact 2,048-entry fixture was run at pre-engine commit `4723031` in a
detached temporary worktree and again after the read cutover. All three built-in
node layouts are byte-identical. The values are recorded in `root-vectors.csv`
and enforced by `tests/foundation_root_vectors.rs`.

## Correctness and Quality Gates

Passed before this report:

```text
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --features async-store --test async_store
cargo test --test ready_sync
cargo test --test invariants
cargo test --no-default-features --lib
cargo test --all-features --lib
cargo test --doc
```

Final whole-workspace verification after adding the benchmark, root-vector,
mixed-format reachability, and stricter structural validation tests:

```text
cargo test --no-default-features       PASS
cargo test --all-features              PASS
cargo check --no-default-features --target wasm32-unknown-unknown  PASS
```

## Miri

Intended validation subset:

```text
cargo miri test --lib engine::validation
```

Miri is unavailable in this environment: the stable
`aarch64-apple-darwin` toolchain does not have the `cargo-miri` component. This
is an environmental limitation, not a passed gate.
