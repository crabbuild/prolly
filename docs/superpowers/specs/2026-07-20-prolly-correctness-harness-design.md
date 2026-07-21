# Prolly Correctness Harness Design

## Status

Approved for implementation planning on 2026-07-20.

## Purpose

Build a repository-level correctness harness that gives humans and AI agents one reproducible gate to run before claiming that changes preserve the behavior of Prolly's three core map surfaces:

- `VersionedMap` and `AsyncVersionedMap`;
- `IndexedMap` and its coordinated secondary-index API;
- `ProximityMap` and `AsyncProximityMap`.

The harness must detect logical, canonicalization, publication, persistence, sync/async, corruption-handling, and bounded concurrency regressions. It must turn every discovered failure into a deterministic trace that can be replayed permanently.

Testing cannot mathematically prove that arbitrary software contains no defects. The concrete guarantee provided by this design is that every declared in-scope public operation has an explicit test owner and invariant, generated histories are checked against independent reference models, identified failures become permanent regressions, and newly added public operations cannot silently bypass the harness.

## Scope

### In scope

The harness covers the public operations on the following surfaces and on the stateful public objects returned by those operations.

#### Versioned maps

- `VersionedMap` and `AsyncVersionedMap` construction and identity;
- initialization, head and historical version lookup;
- point, multi-key, borrowed, leased, large-value, range, prefix, reverse, page, cursor, bound, and scan reads;
- snapshots and asynchronous snapshots;
- apply, conditional apply, put, delete, edit, append, sorted rebuild, parallel ingestion, import, rollback, and compare-and-swap publication;
- history, diffs, streaming diffs, structural pages, comparisons, merges, merge policies, CRDT merge, conflict streams, and changed-span hints;
- subscriptions and asynchronous subscriptions;
- typed key/value access and migrations;
- proofs, proof authentication, export, missing-node planning/copying, and snapshot push;
- backup, restore, catalog verification, retention, pruning, garbage-collection integration, cache controls, pinning, and publication hints;
- multi-map transactions and their atomicity;
- companion types used to invoke those operations, including map snapshots, comparisons, merges, editors, subscriptions, typed maps, and transactions.

#### Indexed maps

- `IndexedMap` construction, identity, source access, health, and metrics;
- registry and definition validation needed to operate an indexed map;
- source reads and coordinated indexed writes through apply, conditional apply, put, delete, and edit;
- index activation, ensure/build, replacement, verification, repair, deactivation, and lifecycle fencing;
- coordinated snapshots and historical snapshot lookup;
- exact, prefix, range, forward, reverse, callback, record-resolution, projection, and paginated secondary-index queries;
- cursor encoding, decoding, and snapshot identity checks;
- checkpoint, catalog, bundle export/import, retention, and indexed garbage-collection operations;
- companion types used to invoke those operations, including indexed editors, coordinated snapshots, and secondary-index snapshots.

There is currently no independent public asynchronous `IndexedMap`. Indexed-map operations that execute through asynchronous engine primitives are still checked through shared canonical outputs and sync/async engine equivalence where the public API exposes both paths.

#### Proximity maps

- `ProximityMap` build, parallel build, load, descriptor identity, and configuration validation;
- `AsyncProximityMap` load and asynchronous search;
- point reads, borrowed and leased reads, contains checks, full/ranged callback scans, and read sessions;
- canonical batch rebuild and incremental mutation;
- exact and approximate searches for every canonical distance metric;
- structural filters, explicit eligible keys, secondary-index filters, budgets, adaptive policies, planner selection, completion reporting, and deterministic ordering;
- verification, membership proofs, structural proofs, search proofs, and cache clearing;
- companion map-level request, result, session, and proof operations needed to exercise the preceding behavior.

Independent accelerator catalog, HNSW, product-quantization, and composite-accelerator management APIs are outside this map-focused scope unless they are invoked by a map search path. Their standalone contracts remain owned by their existing focused test suites.

### Out of scope

- Performance thresholds and benchmark regression detection. The harness may record elapsed time but correctness cannot depend on it.
- Language-binding API parity. Existing conformance fixtures remain responsible for portable wire compatibility.
- Credentialed remote-service validation. The harness uses deterministic in-process stores and wrappers.
- Exhaustive schedule exploration or formal verification.
- Public APIs unrelated to the three map surfaces, except where a scoped operation necessarily returns or consumes them.

## Architecture

The harness is a standalone Rust crate at `correctness-harness/` with a path dependency on the repository's `prolly-map` crate. It exercises the same public surface available to downstream users, adds no production dependency, and is run through a repository script.

The crate contains five layers.

### 1. Serializable command traces

A trace records its format version, surface, seed, configuration, initial data, and ordered commands. Commands contain only stable owned test data and never implementation-private types. Traces serialize to readable JSON so a failure can be replayed across machines and retained in source control.

Each executed command produces a transcript entry containing the command index, normalized operation, expected result, actual result, relevant logical identities, publication state, and invariant checks. The reporter must be able to emit a useful failure without debug logging from production code.

### 2. Independent reference models

The models intentionally use simple standard-library data structures and scalar algorithms rather than Prolly nodes, index encoders, mutation helpers, or search planners.

#### Versioned-map model

The versioned-map model stores immutable `BTreeMap<Vec<u8>, Vec<u8>>` states with a model version identifier, parent relation, timestamp, publication order, and retention status. It implements straightforward point reads, bounds, ranges, prefixes, reverse traversal, pagination, logical diffs, three-way merge decisions, subscriptions, backup selection, and retention selection.

Prolly content identifiers are observed values, not generated by the model. Canonical-identity assertions compare independent Prolly construction paths that represent the same model content.

#### Indexed-map model

The indexed-map model owns a source `BTreeMap`, registered extractor definitions, generation state, and coordinated snapshot history. After every successful source mutation it reruns each registered extractor over the complete source model, validates the declared limits and projection mode, and builds sorted logical entries `(term, primary_key, projection)`.

It does not read or reuse the incrementally maintained Prolly index. This clean rebuild is the primary oracle for incremental maintenance, lifecycle transitions, query results, and checkpoint selection.

#### Proximity-map model

The proximity model owns an ordered map from key to `(vector, value)`. Exact search uses scalar implementations of L2, cosine, and inner-product distance or score normalization, applies eligibility filters directly, and sorts by the public distance-then-key contract. Input validation follows the public configuration and vector contracts but does not use production search or storage helpers.

Approximate search is not required to equal the brute-force top-k unless the chosen operation promises exact completion. Approximate results are checked for membership, filter eligibility, recomputed distance, ordering, budget compliance, determinism, and truthful completion metadata.

### 3. Public-API drivers

Separate drivers apply a shared logical trace to synchronous and asynchronous public APIs. Drivers adapt representation and scheduling only; they do not contain oracle logic. When both surfaces promise the same behavior, their normalized transcripts and canonical identities must agree.

Operations that cannot naturally appear in a long-lived state machine, such as constructors, metadata accessors, callbacks, proof authentication, cache controls, codecs on returned cursors, and one-shot import helpers, are exercised by deterministic focused contracts that use the same reporters and invariant library.

### 4. Invariant checkers

Invariant functions consume normalized model and driver observations. They are kept separate from generators and drivers so the harness can test them directly with mutation canaries.

### 5. API coverage inventory

The harness contains a checked inventory row for every public method on every in-scope type. A row names the owning surface, sync/async class, semantic oracle or invariant, deterministic contract case, generated command families, applicable failure families, and compared outputs.

A test parses the relevant Rust source files with `syn`, collects public inherent methods for the explicit in-scope type list, and compares the result with the inventory. It fails for missing methods, stale inventory rows, or duplicate ownership. Signature changes continue to be checked by normal Rust compilation because contract cases and drivers call the real public methods.

## Core Invariants

### Versioned-map invariants

- Every point, bound, multi-key, range, prefix, reverse, callback, and paginated result equals the model result.
- Concatenated pages equal the matching unpaged result without gaps, duplicates, or cursor loops.
- A snapshot remains pinned while the live head changes.
- A successful mutation creates the model state requested by the command; a failed mutation leaves the previous public head observable.
- Conditional updates publish only from the expected head and report the actual competing head on conflict.
- Logical diffs and streamed or paged variants equal the model diff.
- Independent construction histories with equal logical content and equal tree configuration produce equal canonical roots.
- Merge outputs equal the declared resolver or policy, and a failed or conflicting publication leaves the head unchanged.
- Subscriptions report each observable head transition once and resume from their recorded identifier.
- Backup, restore, export, import, push, reopen, and missing-node copy preserve the selected logical versions and identities.
- Proofs verify the requested snapshot and fail after relevant tampering.
- Retention and pruning keep the exact requested closure, including the current head and explicitly retained versions.
- Multi-map transactions expose all requested heads or none of them.

### Indexed-map invariants

- Each maintained index is logically identical to a complete independent rebuild from the selected source snapshot.
- Each source emission has exactly one matching physical logical entry after extractor deduplication, and every index entry resolves to a source record that emits the same term and projection.
- Exact, prefix, range, reverse, projected, record-resolution, callback, and paginated queries equal the rebuilt model in bytewise order.
- Incremental maintenance and deterministic clean rebuild produce equal canonical index roots for the same definitions and source version.
- Source head, index heads, checkpoints, and catalog selection advance atomically or remain unchanged.
- An extractor error, limit violation, stale writer, lifecycle race, injected store failure, or retry exhaustion cannot expose a torn coordinated snapshot.
- A snapshot remains coordinated and pinned while source and index heads move.
- Cursors are bound to direction, definition fingerprint, index version, and coordinated snapshot and reject mismatched reuse.
- Verify detects logical drift; repair restores clean-rebuild equivalence without changing source content.
- Replacement and deactivation preserve retained historical selections and fence unsupported raw writes.
- Bundle export/import, reopen, retention, and garbage-collection planning preserve the selected coordinated closure.

### Proximity-map invariants

- Point, borrowed, leased, contains, callback, and range scan reads equal the key-to-record model.
- Exact searches equal scalar brute force for every canonical metric, filter, k value, tie, and eligible-set shape.
- Returned distances are recomputable from stored vectors and the query within the explicitly documented floating-point tolerance.
- Results are ordered by the public distance-then-key rule with no duplicate or ineligible keys.
- Search budgets are never exceeded, and completion metadata does not claim exhaustive completion when eligible work was skipped.
- Fixed map, query, configuration, policy, and seed produce deterministic normalized results.
- Incremental mutations equal clean rebuilds in records, verification, exact search, and canonical identity wherever the public operation promises canonical construction.
- Failed mutation, cancellation, malformed storage, or invalid input does not expose a partial descriptor or record set.
- Load and reopen preserve descriptor identity and observable records.
- Cache clearing and cold-cache execution preserve results.
- Membership, structural, and search proofs verify the claimed source and reject tampering.

### Cross-cutting invariants

- Overlapping synchronous and asynchronous operations produce equivalent normalized outcomes and canonical identities.
- Replaying a trace from an empty store produces the same normalized transcript.
- Reopen/load produces the same observable state as the live instance.
- Serialization round trips preserve identity and malformed or tampered bytes are rejected.
- Cache and hint state may affect work but never logical outcomes.
- Failed, cancelled, stale, corrupted, or resource-limited operations publish no partial state.

## Input Generation and Shrinking

Generation is deterministic from an explicit 64-bit seed and profile. A trace records all generated commands, so reproduction does not depend on a generator version after the trace has been emitted.

Generators weight the following boundary classes more heavily than uniform random data:

- empty, single-byte, invalid UTF-8, embedded-zero, all-zero, all-`0xff`, long, and shared-prefix keys and values;
- absent, present, first, last, exact-boundary, prefix-boundary, and just-outside query keys;
- duplicate mutations, multiple writes to one key, delete-missing, delete-reinsert, no-op replacement, and order permutations;
- tree sizes around configured chunk split and hierarchy boundaries;
- sparse, empty, duplicate, multi-term, binary-term, projection-only, and resource-limit index emissions;
- zero, negative, fractional, large finite, duplicate, equidistant, orthogonal, and near-collinear vector components;
- `k` and page sizes of zero, one, exact cardinality, cardinality plus one, and values spanning multiple pages;
- stale identifiers, retained and pruned identifiers, retry boundaries, reopen points, and cancellation points.

Only public-valid inputs are used for semantic equivalence commands. Separate validation commands generate invalid dimensions, vector lengths, non-finite vector components where rejected by contract, invalid bounds, malformed cursors, malformed bundles, and invalid configuration values.

Shrinking removes commands, reduces byte strings and vectors, reduces collection sizes, simplifies configurations, and moves numeric values toward boundary representatives. Shrinking must preserve the failure classification and must stop at a complete serializable trace. A minimized trace is printed and can be copied verbatim into `correctness-harness/regressions/`.

## Failure, Corruption, and Crash Testing

The harness provides deterministic wrappers over an in-memory store. A fault schedule selects an operation kind and occurrence count; it never relies on wall-clock timing.

Supported failures include:

- missing nodes;
- valid bytes stored under the wrong content identifier;
- malformed node, manifest, descriptor, checkpoint, catalog, cursor, bundle, and proof bytes;
- valid nodes with a mismatched tree format;
- point-read, point-write, delete, native batch, manifest load, manifest update, and compare-and-swap errors;
- stale compare-and-swap conditions and retry exhaustion;
- asynchronous `Pending`, cancellation, and errors at controlled suspension boundaries;
- failure before publication, during staged node persistence, and at atomic publication;
- simulated process loss followed by reopening only durable store state.

Every fault command records the pre-operation observable roots and model state. After the operation, the harness accepts only the documented error/outcome and verifies that either the prior selection remains visible or the complete new selection is visible. A mixed selection is always a failure.

## Deterministic Concurrency Testing

Bounded concurrency campaigns use barriers, controlled store hooks, and seeded release schedules rather than unconstrained sleeps. They cover:

- independent versioned-map writers;
- stale conditional writers;
- subscriptions racing with publication;
- indexed writers racing with other writers, first activation, replacement, repair, deactivation, and retention;
- proximity snapshot readers racing with publication of independently built replacements where the public API permits it;
- asynchronous search cancellation at controlled I/O boundaries.

The oracle describes the set of valid serial outcomes. A run passes when the observed outcomes linearize to one allowed order, preserve all success results, report conflicts honestly, and leave the final state equal to the corresponding model state. The harness does not require one particular thread order.

## Harness Self-Tests

Mutation canaries prove that invariant checkers detect representative defects. Test-only sabotaged observations or drivers simulate:

- a dropped successful write;
- an inclusive/exclusive range-end error;
- a repeated or skipped page entry;
- an omitted incremental index update;
- a torn source/index/catalog publication;
- an incorrect proximity distance;
- incorrect equal-distance key ordering;
- a false exhaustive-completion claim;
- synchronous/asynchronous result divergence.

Each canary test passes only when the intended invariant rejects the sabotage with the expected failure classification. Canary code is confined to the harness and never linked into production Prolly.

## Execution Profiles

The command-line runner accepts `--profile pr`, `--profile nightly`, or `--profile release`. It also accepts explicit seed, command-count, surface, regression-file, and report-path overrides for reproduction and local diagnosis.

### PR profile

- Run every deterministic API contract case.
- Run every saved regression trace.
- Run every mutation canary.
- Run 16 seeds with 250 commands for each applicable state machine.
- Run a bounded representative set of format, projection, metric, failure, reopen, and concurrency configurations.
- Target approximately one to three minutes on the supported CI runner after compilation.

### Nightly profile

- Include all PR checks.
- Run 128 seeds with 2,000 commands for each applicable state machine.
- Expand fault locations, tree formats, chunking boundaries, projection modes, metrics, reopen points, and controlled concurrency schedules.

### Release profile

- Include all nightly classes.
- Run 256 seeds with 10,000 commands for each applicable state machine.
- Exercise every supported in-scope configuration combination subject to pairwise reduction where a full Cartesian product changes no semantics.
- Run long persistence/reopen and deterministic replay campaigns.

Profile constants are versioned in source and included in reports. Local overrides cannot silently redefine a named profile; the report marks an overridden run as custom.

## Reporting and Regression Workflow

Every run prints a concise human summary and writes a versioned JSON report containing:

- crate and harness versions;
- selected profile and any overrides;
- public API inventory totals and ownership;
- surfaces, configurations, seeds, commands, fault points, and schedules executed;
- passed and failed invariant counts;
- elapsed time as diagnostic metadata;
- the first failure classification, operation index, expected and actual normalized observations;
- the original and minimized trace paths or inline JSON.

The default output path is under `target/correctness-harness/` so ordinary reports do not dirty the worktree. A failure can be promoted by copying its minimized trace into `correctness-harness/regressions/<surface>/<descriptive-name>.json`. Regression files run before generated campaigns in all profiles.

## Repository Integration

The repository provides one entry point:

```sh
scripts/run-correctness-harness.sh pr
```

The script verifies harness formatting and compilation and runs the requested profile. It does not rewrite source files. The harness README documents profile selection, exact seed replay, regression promotion, report interpretation, scope, and the limits of the guarantee.

The root README and contributor-facing guidance identify the PR command as required before a human or AI agent claims correctness for changes affecting an in-scope surface. Existing root tests remain required; the new harness complements rather than replaces focused unit, integration, conformance, and benchmark suites.

## Acceptance Criteria

Implementation is complete when all of the following are true:

1. The standalone harness crate builds on the repository's Rust 1.81 minimum supported version.
2. The API coverage check reports no missing, stale, or duplicate inventory entries for the explicit in-scope type list.
3. Every inventory row names and successfully runs at least one deterministic contract owner.
4. Versioned-map generated histories agree with the independent immutable `BTreeMap` model.
5. Indexed-map generated histories agree with complete independent extractor rebuilds, and canonical incremental index roots equal clean rebuild roots where promised.
6. Proximity exact searches agree with independent scalar brute force, while approximate searches satisfy every stated validity, budget, ordering, determinism, and completion invariant.
7. Overlapping synchronous and asynchronous traces produce equivalent normalized outcomes and canonical identities.
8. Fault, corruption, cancellation, crash/reopen, and bounded concurrency suites preserve publication atomicity and documented failure behavior.
9. Every mutation canary is rejected by its intended invariant.
10. The PR, nightly, and release profiles are reproducible, produce versioned machine-readable reports, and replay saved regressions first.
11. The PR profile passes from the documented single command.
12. Existing Prolly tests, formatting, and strict compilation checks remain green.

## Implementation Constraints

- No production dependency or public production API is added solely for the harness.
- Drivers exercise public APIs; private helpers are not imported into the harness.
- Oracles do not reuse production mutation, index-maintenance, distance, search, pagination, or merge algorithms.
- Randomness is always seedable and reported.
- Fault and concurrency schedules do not depend on sleeps or wall-clock races.
- Generated artifacts default to `target/`; only deliberately promoted regression traces are committed.
- The implementation preserves unrelated worktree changes.
- Development follows test-driven cycles: each framework component begins with a failing harness self-test or public contract test before implementation.
