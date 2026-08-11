# Versioned DynamoDB Multi-Process Soak Qualification

This document defines the sustained concurrency/process-failure gate for the
in-process Rust client. Local results establish correctness regression evidence,
not hosted AWS throughput or latency promises.

## Current harness

`multi_process_soak_preserves_items_versions_and_commits` creates an isolated
namespace and one hot logical table, then:

1. starts independent OS processes sharing no client or process-local lock;
2. assigns every logical write a deterministic durable request token;
3. repeatedly aborts in-process write tasks and replays the exact token;
4. kills one process after it submitted a write but before local
   acknowledgement, then restarts that writer from iteration zero;
5. retries only explicit optimistic transaction-conflict outcomes with bounded
   jitter and the unchanged token;
6. verifies the final item set, immutable version count, ordered commit
   sequences, and commit-ID uniqueness;
7. clears only the test's isolated namespace.

This shared-table workload is also a regression environment for maintenance
pagination: unrelated namespaces can occupy evaluated DynamoDB Scan positions.
Provider cursors are namespace-bound opaque envelopes around that physical
position, so GC continues across unrelated keys without returning or deleting
them. A cursor from another table/prefix is rejected.

The default local shape is four processes and 50 writes per process. Its
required final state is exactly 200 items, 201 table versions (including table
creation), and 201 unique commits with contiguous sequences `1..=201`. Replayed
acknowledged, cancelled, and unacknowledged writes must not add a version or
commit.

Run it against DynamoDB Local with:

```bash
PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 \
PROLLY_DYNAMODB_CLIENT_TEST_TABLE=prolly-versioned-client-test \
PROLLY_DYNAMODB_RUN_SOAK=1 \
cargo test --manifest-path extensions/dynamodb/client/Cargo.toml \
  --test dynamodb_local \
  multi_process_soak_preserves_items_versions_and_commits \
  -- --exact --nocapture
```

`PROLLY_DYNAMODB_SOAK_WORKERS` accepts `2..=16` and
`PROLLY_DYNAMODB_SOAK_ITERATIONS` accepts `8..=2000`. CI must retain the exact
settings, duration, binary hashes, DynamoDB Local digest, and complete output.
A skipped test is not soak evidence.

## Cross-binary rolling qualification

Every published client source archive carries the stable
`rolling_compatibility_probe` example. Build that example independently from
the retained old release and the candidate release, preserve both binaries,
then run:

```bash
python3 scripts/run_dynamodb_rolling_compatibility.py \
  --old-binary /release-evidence/old/rolling_compatibility_probe \
  --new-binary /release-evidence/new/rolling_compatibility_probe \
  --physical-table prolly-rolling-qualification \
  --root-table prolly-rolling-qualification-roots \
  --iterations 50 \
  --output-dir /release-evidence/rolling-old-to-new
```

Add `--endpoint` only for DynamoDB Local. Without it, both binaries use the
caller-owned AWS credential/region environment. The coordinator:

1. hashes both binaries and rejects identical SHA-256 values by default;
2. initializes through the old binary, then opens the same namespace through
   both binaries;
3. compares the exact negotiated `database_format_record_hex` and provider
   transaction limits/mode;
4. runs old and new writers concurrently with disjoint deterministic tokens;
5. makes each binary verify the exact item set, immutable-version cardinality,
   unique contiguous commit log, and one head-pinned scan;
6. makes the new binary read an old-produced immutable version and the old
   binary read a new-produced immutable version;
7. writes a machine-readable report containing binary identities, every
   command, stdout/stderr, duration, capability reports, and cleanup status.

`--allow-identical-binaries` exists only to test the harness. A report with
`identical_binary_diagnostic: true` is never mixed-version evidence. Failures
preserve the namespace for investigation unless `--cleanup-on-failure` was
explicitly selected. Successful runs clean the isolated namespace unless
`--keep-namespace` was selected. Qualification artifacts must be immutable and
must identify the source commit, Cargo archive, binary, endpoint class, and AWS
account/region outside the report when those identities are not inferable from
the probe itself.

## Retry contract exercised by the harness

A hot-table writer can exhaust the core's internal optimistic attempt budget
and receive a transaction-conflict cancellation. That result is known not to
have applied. The harness retries it up to 256 times with bounded jitter and
the identical durable token. Any other error is terminal for the test; the
harness never converts an outcome-unknown or generic storage error into a blind
retry.

Production applications may choose a lower attempt budget or admission control,
but must preserve this classification rule. A retry policy is not a substitute
for the measured contention envelope required by `PERFORMANCE.md`.

## Remaining release gate

The local current-binary baseline does not close the production soak gate. The
release candidate must additionally run:

- old-reader/new-writer and new-reader/old-writer binaries for every advertised
  compatible release pair;
- hosted AWS throttling and account/request-limit pressure;
- SDK timeout and connection-reset schedules over sustained runs;
- SIGKILL/process/container/node loss at randomized operation phases;
- worker lease takeover and checkpoint recovery during foreground writes;
- at least the declared maximum supported writer/process/table-history shape;
- post-run root/node/blob reachability, commit, catalog, index, and archive
  verification.

Every run must have a fixed seed, immutable binary/configuration identity, and
machine-readable report. No production claim may extrapolate from DynamoDB
Local timing.
