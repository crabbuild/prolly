# Versioned DynamoDB Publication Fault Qualification

This document defines the failure boundaries that must be exercised before a
Versioned DynamoDB client release. It covers the in-process Rust request path;
there is no service wrapper whose failure semantics need separate testing.

The required DynamoDB Local job repeats the multi-process process-death soak
three times. This specifically guards the split-observation window between a
transaction-pinned base-map root and the independently named indexed root. A
disagreement caused by an intervening committed writer is retried from a fresh
root set; it must never be reported as stored-data corruption.

## Safety outcomes

Every injected failure must establish exactly one of these outcomes:

| Outcome | Required evidence |
| --- | --- |
| Not applied | No logical root or atomic node becomes visible; exact replay is safe |
| Prepared only | Content-addressed blob/node data may exist, but no logical root references it; GC may reclaim it only after normal reachability and retention checks |
| Applied | All conditioned roots and atomic nodes become visible together and durable commit/idempotency evidence resolves the exact result |
| Outcome unknown | The API exposes an unknown disposition and stable provider token; it never claims not-applied or invites blind replay |

A test is invalid if it proves only that an error was returned. It must inspect
durable roots, nodes/blobs, commit evidence, replay identity, and—where
applicable—the number and token of physical SDK executions.

## Qualified boundary matrix

| Boundary | Fake-store evidence | DynamoDB Local / SDK-boundary evidence | Required result |
| --- | --- | --- | --- |
| Validation before preparation | `batch_write_validation_is_global_canonical_and_write_free` and frozen validation fixtures | Client differential/official-input suites | No write |
| Blob put fails before acceptance | `blob_prepare_failures_never_advance_logical_visibility` | Provider blob conformance | No blob and no head movement |
| Blob put is accepted but response fails | `blob_prepare_failures_never_advance_logical_visibility` | Provider content-address verification | One unreachable blob is permitted; no head movement |
| Immutable node preparation conflicts at root | Core/import and index atomicity tests | `dynamodb_backend_immutable_prepublish_conflict_leaves_only_unreachable_content` | Only unreachable immutable nodes; old root remains |
| Immutable batch preparation is accepted but response is lost | Fake blob/node preparation model | `dynamodb_backend_lost_immutable_prepare_response_cannot_publish_a_root` | Prepared node may exist; root transaction was never submitted |
| Atomic root condition fails | Transaction conflict/reason-order tests | `dynamodb_backend_atomic_conflict_publishes_neither_nodes_nor_new_root` | Neither new root nor atomic node is visible |
| Transaction fails before every execution | `fake_store_publication_faults_preserve_exact_single_write_outcomes` | `dynamodb_backend_reports_unknown_after_every_pre_execution_attempt_fails` | Conservative unknown after three stable-token attempts; observed state remains absent |
| First accepted transaction response is lost | Visible-ambiguity transaction and single-write tests | `dynamodb_backend_reconciles_a_response_lost_after_transaction_acceptance` | Same token is retried and one complete publication is returned |
| Every accepted replay response is lost | Restart/reconciliation fake-store tests | `dynamodb_backend_exhausted_accepted_replays_report_unknown_but_commit_once` | Unknown is returned after three identical tokens; complete state is committed once |
| Task is cancelled after acceptance and response loss | Restart/replay fake-store tests | `dynamodb_backend_task_cancellation_after_acceptance_replays_exactly_once` | Cancelled caller receives no false rollback claim; later identical replay uses the same token and resolves one state |
| First reconciliation read fails | `fake_store_publication_faults_preserve_exact_single_write_outcomes` and `transact_write_reconciles_an_ambiguous_commit_after_process_restart` | Stable provider token tests | First call remains unknown/storage-failed; restart resolves exact durable commit without another transition |
| Retry succeeds after an ambiguous response | `transact_write_reconciles_a_visible_ambiguous_commit_before_returning` | Post-acceptance response-loss test | Exact replay result, same token, one transition |
| Retry budget is exhausted | Commit-failure/transaction conflict tests | Pre-execution and accepted-response exhaustion tests | Typed unknown or definitely-not-applied classification; never generic success |

The fake-store layer controls logical commit, conflict, response, and
reconciliation-read cut points deterministically. The SDK interceptor layer
executes against DynamoDB Local and cuts actual `BatchWriteItem` and
`TransactWriteItems` lifecycle phases. Together they cover preparation,
condition/write, response, cancellation, retry, and reconciliation boundaries.

## Commands

Run provider qualification against an isolated DynamoDB Local table:

```bash
PROLLY_STORE_DYNAMODB_ENDPOINT=http://127.0.0.1:8000 \
PROLLY_STORE_DYNAMODB_TABLE=prolly-provider-fault-test \
cargo test --manifest-path stores/prolly-store-dynamodb/Cargo.toml \
  --test dynamodb_backend -- --test-threads=1
```

Run deterministic logical fault tests and strict lint:

```bash
cargo test --manifest-path extensions/dynamodb/core/Cargo.toml --all-targets
cargo clippy --manifest-path extensions/dynamodb/core/Cargo.toml --all-targets -- -D warnings
cargo clippy --manifest-path stores/prolly-store-dynamodb/Cargo.toml \
  --all-targets -- -D warnings
```

The tests intentionally use isolated non-empty prefixes and clean only their
own namespace. A skipped provider test is not qualification evidence; CI must
set both environment variables and must retain test output proving that all
provider tests executed.

## Remaining production-shaped evidence

DynamoDB Local and Smithy interceptors cannot establish hosted AWS throttling,
regional networking, IAM, SDK transport, or service-side timeout distributions.
The hosted-AWS smoke matrix and multi-process throttling/process-death soak gate
remain mandatory. Those gates extend this matrix; they do not weaken any safety
outcome above.
