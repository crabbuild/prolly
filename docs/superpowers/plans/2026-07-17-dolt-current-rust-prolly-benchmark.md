# Current Dolt Go vs Rust Prolly Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a reproducible native Dolt-Go-versus-Rust prolly benchmark, prove both runners execute the identical July 16 workload contract, and run the complete 10K through 10M matrix against one pinned current Dolt `main` commit.

**Architecture:** Keep the existing Rust runner as the logical-contract authority, add a complete checked-in Go runner that is copied into a clean Dolt checkout, and drive both release binaries as isolated single-worker processes. Normalize timing and peak RSS into one strict CSV, reject incomplete or semantically mismatched pairs, and generate both a current cross-language report and a historical Rust-before/Rust-after report.

**Tech Stack:** Rust/Cargo, Go/Dolt `store/prolly`, POSIX shell, Python 3 standard library (`csv`, `statistics`, `unittest`), Git, `/usr/bin/time`.

## Global Constraints

- Benchmark infrastructure only: do not change Rust prolly production behavior or Dolt source beyond copying the benchmark command into `go/cmd/prolly-compare` in the detached benchmark checkout.
- Preserve the July 16 contract: 10K, 50K, 1M, 5M, and 10M base sizes; fresh and mutation phases; append, random, and clustered workloads; 30% mutation count; deterministic 50/50 update/insert mix; at most 100K reads; full ordered scan.
- Run every size three times, including 5M and 10M. Never estimate, interpolate, or silently retry a missing row into a completed matrix.
- Resolve Dolt `origin/main` once before building. Record the exact commit and permit exact reproduction through `DOLT_REV`.
- Exclude fixture generation and Dolt tuple construction from timed write intervals. Include native mutation buffering, sorting, chunking, hashing, encoding, storage writes, and publication.
- Use `GOMAXPROCS=1` and `RAYON_NUM_THREADS=1`, separate processes, sequential execution, and alternating implementation order.
- A runner may emit rows only after cardinality, exact point values, strict scan ordering, scan count, mutation uniqueness/mix, and workload digest validation pass.
- Treat the six 10K workload digests as contract fixtures:
  - fresh/append `51f55fcd59187cbf`
  - fresh/random `004197dd790a1245`
  - fresh/clustered `86e38047f6ae04b3`
  - mutation/append `2ef1df79e1226620`
  - mutation/random `3bc7e45ef276a1c5`
  - mutation/clustered `5caed8dbd3056277`
- Retain raw stdout, stderr, process timing, exit status, peak RSS, manifest, and binary/source hashes for auditability.

---

### Task 1: Lock the Rust workload contract and normalized schema

**Files:**

- Modify: `src/bin/prolly_compare.rs`
- Test: `src/bin/prolly_compare.rs` (`#[cfg(test)]` module)

- [ ] **Step 1: Add failing tests for all golden workload digests and schema version**

  Add a table-driven test that computes the logical operation stream for every 10K phase/workload pair and asserts the six values in Global Constraints. Add a test that captures the CSV header constructor and requires `contract_version` plus the existing columns. Add mutation fixture assertions for exact count, uniqueness, and 50/50 insert/update split for random and clustered workloads.

  Run:

  ```bash
  cargo test --bin prolly_compare workload_contract -- --nocapture
  cargo test --bin prolly_compare csv_schema -- --nocapture
  ```

  Expected: FAIL because the all-scenario fixture helper and `contract_version` schema field do not exist yet.

- [ ] **Step 2: Implement the smallest contract helpers and schema change**

  Introduce `const CONTRACT_VERSION: &str = "prolly-compare-v1"`. Factor CSV header and row emission into small helpers so tests inspect the exact schema. Reuse the existing generator functions without changing their arithmetic, seeds, key/value bytes, arrival order, or digest algorithm. Emit `contract_version` in each Rust row.

- [ ] **Step 3: Verify the Rust runner contract**

  Run:

  ```bash
  cargo fmt --check
  cargo test --bin prolly_compare -- --nocapture
  cargo run --release --bin prolly_compare -- --records 10000 --phase fresh --workload random
  ```

  Expected: all tests pass; the sample emits three validated rows with contract `prolly-compare-v1` and digest `004197dd790a1245`.

- [ ] **Step 4: Commit the Rust contract lock**

  ```bash
  git add src/bin/prolly_compare.rs
  git commit -m "test: lock prolly comparison workload contract"
  ```

### Task 2: Add the checked-in native Dolt runner test-first

**Files:**

- Create: `benchmarks/dolt-prolly-compare/main_test.go`
- Create: `benchmarks/dolt-prolly-compare/main.go`

- [ ] **Step 1: Write failing Go contract tests before the runner exists**

  In `main_test.go`, add table-driven tests for the six 10K digests, permutation determinism, fixed-width key ordering, value determinism, mutation count/uniqueness, exact random and clustered insert/update mix, and CSV contract version. Tests must call the production runner helpers rather than duplicate their implementation.

  Copy only the failing test into the prepared Dolt module and run:

  ```bash
  mkdir -p /tmp/dolt-prolly-tdd/go/cmd/prolly-compare
  cp benchmarks/dolt-prolly-compare/main_test.go /tmp/dolt-prolly-tdd/go/cmd/prolly-compare/main_test.go
  cd /tmp/dolt-prolly-tdd/go && go test ./cmd/prolly-compare
  ```

  Expected: FAIL to compile because `freshID`, `mutationPosition`, workload digest helpers, and `contractVersion` are undefined. The implementation checkout may be a disposable clone/copy; it must not modify an unrelated user checkout.

- [ ] **Step 2: Implement argument parsing and the byte-identical workload generator**

  In `main.go`, implement `--records`, `--phase`, and `--workload` with strict validation. Port the Rust `mix64`, `gcd`, permutation, clustered order, key, value, read-target, mutation-position, and FNV-1a digest functions using explicit `uint64` wrapping. Keep the public CSV fields and operation names identical to Rust.

- [ ] **Step 3: Implement the native Dolt product path**

  Use current Dolt packages `store/chunks`, `store/pool`, `store/prolly`, `store/prolly/tree`, and `store/val`. Construct one non-null `ByteStringEnc` field for keys and values with `val.TupleBuilder`; use `chunks.TestStorage`, `tree.NewNodeStore`, `prolly.NewMapFromTuples`, `Map.Mutate`, `Put`, and `Map(ctx)` publication. Prebuild logical fixtures and encoded tuples outside the timer.

  For fresh writes, time only insertion into the native mutable map and publication. For mutation writes, build the ascending base map before timing, then time the exact 30% mutation stream and publication. Use append positions above the maximum; alternate update and insert positions for random and clustered mutation.

- [ ] **Step 4: Add correctness gates and measured read paths**

  After write publication, assert the expected cardinality. Create at most 100K deterministic read tuples outside the timer, warm them once with exact value comparison, then time callback point reads while consuming bytes. Iterate `Map.IterAll(ctx)` to `io.EOF`, require strictly increasing logical keys, exact final count, and a non-elidable byte accumulator. Exit nonzero without CSV rows on any failed gate.

- [ ] **Step 5: Run Dolt unit and smoke tests in the pinned module**

  ```bash
  cp benchmarks/dolt-prolly-compare/main.go /tmp/dolt-prolly-tdd/go/cmd/prolly-compare/main.go
  cp benchmarks/dolt-prolly-compare/main_test.go /tmp/dolt-prolly-tdd/go/cmd/prolly-compare/main_test.go
  cd /tmp/dolt-prolly-tdd/go && gofmt -w cmd/prolly-compare/main.go cmd/prolly-compare/main_test.go
  cd /tmp/dolt-prolly-tdd/go && go test ./cmd/prolly-compare
  cd /tmp/dolt-prolly-tdd/go && go run ./cmd/prolly-compare --records 10000 --phase fresh --workload random
  ```

  Expected: tests pass; smoke emits three validated rows matching Rust operations, counts, result counts, contract version, and digest.

- [ ] **Step 6: Copy only formatting-equivalent source back and commit**

  Ensure `gofmt -d benchmarks/dolt-prolly-compare/*.go` is empty, then:

  ```bash
  git add benchmarks/dolt-prolly-compare/main.go benchmarks/dolt-prolly-compare/main_test.go
  git commit -m "bench: add reproducible native Dolt prolly runner"
  ```

### Task 3: Add strict metric parsing and pair validation tests

**Files:**

- Create: `scripts/tests/test_prolly_comparison_metrics.py`
- Create: `scripts/prolly_process_metrics.py`
- Create: `scripts/tests/test_summarize_prolly_comparison.py`
- Modify: `scripts/summarize_prolly_comparison.py`

- [ ] **Step 1: Write failing peak-RSS parser tests**

  Test macOS `/usr/bin/time -l` bytes, GNU `/usr/bin/time -v` KiB, absent metric, malformed metric, and conflicting duplicate metric inputs. Define `parse_peak_rss(text: str) -> int`; absent/malformed data must raise `ValueError`, never return zero.

  Run:

  ```bash
  python3 -m unittest scripts.tests.test_prolly_comparison_metrics -v
  ```

  Expected: FAIL because `scripts.prolly_process_metrics` does not exist.

- [ ] **Step 2: Implement portable process metric parsing**

  Parse the exact macOS and GNU labels, normalize to bytes, and provide a small CLI accepting a timing-output path and printing the integer byte count. Keep it standard-library only.

- [ ] **Step 3: Write failing summarizer tests for completeness and semantic parity**

  Build temporary CSV fixtures with two implementations and three repetitions. Assert:

  - median, minimum, maximum, coefficient of variation, median/max RSS, exact unrounded winner, and speedup;
  - rejection of digest, operations, result count, validation, or contract-version mismatch;
  - rejection of duplicate rows and fewer/more than the configured repetition count;
  - rejection when a required implementation or scenario is absent;
  - exact expected complete-matrix counts (180 processes, 540 rows, 270 pairs);
  - historical Rust deltas only for matching size/phase/workload/operation/contract;
  - successful generation of `summary.csv`, `report.md`, `historical-delta.csv`, and `historical-report.md`.

  Run:

  ```bash
  python3 -m unittest scripts.tests.test_summarize_prolly_comparison -v
  ```

  Expected: FAIL because the existing summarizer lacks strict repetitions, contract/RSS statistics, and historical outputs.

- [ ] **Step 4: Refactor the summarizer around explicit validation**

  Add callable functions `load_rows`, `validate_matrix`, `summarize`, and `compare_history`. Accept CLI options `--expected-runs`, `--expected-sizes`, `--history-summary`, and `--allow-partial` (smoke only). Group pairs by records/phase/workload/operation/repetition and require exactly one Rust and one Dolt row. Compute statistics from integer nanoseconds and RSS bytes; select winners before formatting. Treat the July 16 data as `prolly-compare-v1` only after its known six golden digests validate.

- [ ] **Step 5: Verify all Python tests**

  Run:

  ```bash
  python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
  python3 -m py_compile scripts/prolly_process_metrics.py scripts/summarize_prolly_comparison.py
  ```

  Expected: all tests pass.

- [ ] **Step 6: Commit metric and reporting validation**

  ```bash
  git add scripts/prolly_process_metrics.py scripts/summarize_prolly_comparison.py scripts/tests
  git commit -m "bench: enforce prolly comparison parity and metrics"
  ```

### Task 4: Make the comparison driver self-contained and reproducible

**Files:**

- Create: `scripts/tests/test_run_prolly_comparison.py`
- Modify: `scripts/run_prolly_comparison.sh`
- Modify: `.gitignore` only if generated Dolt caches/binaries are not already ignored

- [ ] **Step 1: Write a failing black-box driver test**

  Create temporary fake `cargo`, `go`, `git`, `time`, Rust runner, and Dolt runner commands. Invoke the driver for a one-size/one-run smoke matrix and assert it:

  - resolves `origin/main` exactly once unless `DOLT_REV` is set;
  - copies the checked-in Go runner before `go test`/build;
  - records resolved Dolt/Rust revisions and source/binary SHA-256 values;
  - sets both single-worker environment variables;
  - alternates implementation order;
  - attaches one normalized peak-RSS value to each of the three process rows;
  - preserves stdout/stderr/timing/exit metadata;
  - fails immediately on nonzero status, malformed CSV, or failed smoke parity.

  Run:

  ```bash
  python3 -m unittest scripts.tests.test_run_prolly_comparison -v
  ```

  Expected: FAIL against the current driver because it assumes an untracked Dolt checkout and does not normalize RSS or current-main provenance.

- [ ] **Step 2: Implement Dolt acquisition and immutable provenance**

  Default `DOLT_REPO_URL` to `https://github.com/dolthub/dolt.git` and `DOLT_CACHE` to a gitignored target cache. Clone if absent, otherwise fetch `origin main`; resolve once into `DOLT_SHA`, or resolve `DOLT_REV`. Detach at that commit, copy `benchmarks/dolt-prolly-compare/main.go` and `main_test.go` into `go/cmd/prolly-compare`, run its unit tests, and build once. Never fetch or switch revisions after scenario execution starts.

- [ ] **Step 3: Implement build, smoke, and measured-process orchestration**

  Release-build Rust once and Go once, copy both binaries into the output directory, hash them, and write the manifest before measurement. Run one untimed 10K fresh/random invocation of each binary, combine its six rows, and call the summarizer with `--allow-partial --expected-runs 1`; abort on mismatch.

  For the measured matrix, default sizes to `10000 50000 1000000 5000000 10000000`, `RUNS=3`, and `LARGE_RUNS=3`. Keep phase/workload loops and deterministic alternating order. Execute sequentially with `/usr/bin/time`, capture raw files, parse peak RSS, validate each runner emitted exactly three CSV data rows, append `repetition` and `peak_rss_bytes`, and stop on any failure without fabricating rows.

- [ ] **Step 4: Generate strict current and historical reports**

  Invoke the summarizer with exact sizes/runs and history path `performance-results/zero-copy-final-rerun-2026-07-16/summary.csv`. Write normalized `results.csv`, `summary.csv`, `report.md`, `historical-delta.csv`, `historical-report.md`, `manifest.txt`, and `machine.txt` under `BENCH_OUT`. Keep copied binaries and per-process raw files locally ignored unless explicitly selected.

- [ ] **Step 5: Pass the driver black-box test and shell validation**

  Run:

  ```bash
  python3 -m unittest scripts.tests.test_run_prolly_comparison -v
  bash -n scripts/run_prolly_comparison.sh
  if command -v shellcheck >/dev/null; then shellcheck scripts/run_prolly_comparison.sh; fi
  ```

  Expected: all available checks pass.

- [ ] **Step 6: Commit the reproducible driver**

  ```bash
  git add scripts/run_prolly_comparison.sh scripts/tests/test_run_prolly_comparison.py .gitignore
  git commit -m "bench: automate pinned Dolt and Rust comparison"
  ```

### Task 5: Run the parity smoke matrix

**Files:**

- Generate locally: `/tmp/prolly-comparison-smoke-2026-07-17/`
- Inspect: smoke `results.csv`, `summary.csv`, `report.md`, `manifest.txt`, and raw process files

- [ ] **Step 1: Resolve current Dolt main and run all 10K scenarios once**

  ```bash
  BENCH_OUT=/tmp/prolly-comparison-smoke-2026-07-17 \
  BENCH_SIZES='10000' \
  BENCH_RUNS=1 \
  BENCH_LARGE_RUNS=1 \
  scripts/run_prolly_comparison.sh
  ```

  Expected: 12 measured processes (2 implementations × 2 phases × 3 workloads), 36 validated rows, and 18 exact pairs. Both implementations use the same six golden digests.

- [ ] **Step 2: Audit smoke correctness and artifacts**

  ```bash
  python3 scripts/summarize_prolly_comparison.py \
    --input /tmp/prolly-comparison-smoke-2026-07-17/results.csv \
    --output-dir /tmp/prolly-comparison-smoke-2026-07-17/verified \
    --expected-runs 1 \
    --expected-sizes 10000 \
    --allow-partial
  wc -l /tmp/prolly-comparison-smoke-2026-07-17/results.csv
  rg -n 'validated=false|estimated|interpolated' /tmp/prolly-comparison-smoke-2026-07-17 || true
  ```

  Expected: 37 CSV lines including header; no failed validation or estimated data; all process timing files contain a valid RSS measurement.

- [ ] **Step 3: Fix only benchmark defects through red-green tests**

  If smoke exposes an error, first add the smallest reproducing test to the relevant Rust, Go, Python, or driver test file; observe failure; fix; rerun that test and the complete smoke. Do not tune production tree code from smoke observations.

### Task 6: Run and monitor the complete performance matrix

**Files:**

- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/results.csv`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/summary.csv`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/report.md`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/historical-delta.csv`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/historical-report.md`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/manifest.txt`
- Generate: `performance-results/dolt-current-rust-canonical-2026-07-17/machine.txt`

- [ ] **Step 1: Start the full matrix with the smoke-pinned Dolt revision**

  Read `DOLT_SHA` from the smoke manifest and run:

  ```bash
  DOLT_REV='<exact-smoke-sha>' \
  BENCH_OUT=performance-results/dolt-current-rust-canonical-2026-07-17 \
  BENCH_SIZES='10000 50000 1000000 5000000 10000000' \
  BENCH_RUNS=3 \
  BENCH_LARGE_RUNS=3 \
  scripts/run_prolly_comparison.sh
  ```

  Run in a persistent PTY/session, poll progress without overlapping other workloads, and report progress at least once per minute while active. A failed scenario stops the run and remains visible; diagnose before any explicit fresh rerun.

- [ ] **Step 2: Verify exact completeness and parity**

  ```bash
  python3 scripts/summarize_prolly_comparison.py \
    --input performance-results/dolt-current-rust-canonical-2026-07-17/results.csv \
    --output-dir performance-results/dolt-current-rust-canonical-2026-07-17 \
    --expected-runs 3 \
    --expected-sizes 10000,50000,1000000,5000000,10000000 \
    --history-summary performance-results/zero-copy-final-rerun-2026-07-16/summary.csv
  python3 - <<'PY'
  import csv
  path = 'performance-results/dolt-current-rust-canonical-2026-07-17/results.csv'
  rows = list(csv.DictReader(open(path, newline='')))
  assert len(rows) == 540, len(rows)
  assert all(row['validated'] == 'true' for row in rows)
  pairs = {(r['records'], r['phase'], r['workload'], r['operation'], r['repetition']) for r in rows}
  assert len(pairs) == 270, len(pairs)
  print('540 rows; 270 exact pairs; all validated')
  PY
  ```

  Expected: strict validation passes with 180 successful scenario processes, 540 rows, and 270 pairs.

- [ ] **Step 3: Inspect variance, regressions, and resource behavior honestly**

  Review every median, min/max, coefficient of variation, and RSS maximum. Distinguish robust wins from noisy differences. Call out every Rust regression versus Dolt and versus the July 16 Rust baseline; do not claim causality for zero-copy or canonical streaming from timing correlation alone.

### Task 7: Final verification, report handoff, and commit

**Files:**

- Modify if needed: `docs/prolly-go-rust-benchmark.md`
- Create: `docs/prolly-rust-go-performance-report-2026-07-17.md`
- Add selected normalized files under `performance-results/dolt-current-rust-canonical-2026-07-17/`

- [ ] **Step 1: Update benchmark documentation with the reproducible command**

  Document current-main resolution, `DOLT_REV` reproduction, checked-in runner location, exact contract, process/RSS semantics, strict failure behavior, and the distinction between cross-product results and historical Rust deltas.

- [ ] **Step 2: Write the evidence-backed final report**

  Summarize hardware/toolchain/revisions, matrix completeness, per-operation and per-workload winners, scale trends, latency and throughput, peak RSS, variance, historical Rust deltas, limitations, and targeted next optimization hypotheses. Link directly to normalized artifacts. Do not omit regressions.

- [ ] **Step 3: Run all final verification commands from a clean benchmark state**

  ```bash
  cargo fmt --check
  cargo test --bin prolly_compare -- --nocapture
  python3 -m unittest discover -s scripts/tests -p 'test_*.py' -v
  python3 -m py_compile scripts/prolly_process_metrics.py scripts/summarize_prolly_comparison.py
  bash -n scripts/run_prolly_comparison.sh
  if command -v shellcheck >/dev/null; then shellcheck scripts/run_prolly_comparison.sh; fi
  git diff --check
  git status --short
  ```

  Expected: every applicable test/check passes; only intentional benchmark, documentation, and selected normalized result files remain.

- [ ] **Step 4: Remove disposable external checkout and preserve reproducible evidence**

  Delete only the explicitly created planning clone `/tmp/dolt-benchmark-plan.qV6pOb` after the recorded SHA and required API findings are preserved in source/manifest. Do not delete the configured benchmark cache or raw output unless explicitly requested.

- [ ] **Step 5: Commit the results and report**

  ```bash
  git add docs/prolly-go-rust-benchmark.md \
    docs/prolly-rust-go-performance-report-2026-07-17.md \
    performance-results/dolt-current-rust-canonical-2026-07-17/results.csv \
    performance-results/dolt-current-rust-canonical-2026-07-17/summary.csv \
    performance-results/dolt-current-rust-canonical-2026-07-17/report.md \
    performance-results/dolt-current-rust-canonical-2026-07-17/historical-delta.csv \
    performance-results/dolt-current-rust-canonical-2026-07-17/historical-report.md \
    performance-results/dolt-current-rust-canonical-2026-07-17/manifest.txt \
    performance-results/dolt-current-rust-canonical-2026-07-17/machine.txt
  git commit -m "bench: report current Dolt versus Rust prolly performance"
  ```

- [ ] **Step 6: Hand off the measured conclusion**

  Report the exact Dolt/Rust commits, completion counts, strongest wins, all regressions, variance warnings, RSS effects, and whether recent Rust changes improved latency relative to July 16. State clearly that this native-product comparison measures implementation defaults and runtime/storage stacks together, not language speed in isolation.
