# Canonical Range Deletion Design

## Goal

Add an atomic half-open range deletion operation that removes every entry whose key satisfies `start <= key < end`. Dense contiguous deletion must avoid reading leaf payloads that are wholly covered by the range while preserving configurable chunking, configurable built-in node layouts, canonical-root convergence, and immutable tree publication.

The focused performance goal is to eliminate the remaining SQLite regression for the existing contiguous clustered-deletion workload at 1M and 10M records under WAL+FULL and WAL+NORMAL durability. The result must be compared with the original revision using the existing alternating-order harness and at least five valid repetitions.

## API

The synchronous Rust API will expose:

```rust
pub fn delete_range(
    &self,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<Tree, Error>;

pub fn delete_range_with_stats(
    &self,
    tree: &Tree,
    start: &[u8],
    end: &[u8],
) -> Result<(Tree, CanonicalWriteStats), Error>;
```

The async API will provide equivalent methods. Maintained language bindings will expose language-idiomatic `deleteRange` and stats-returning equivalents without changing the existing point-mutation wire model.

`start >= end`, an empty tree, or a range containing no keys is a no-op that returns the original root. Bounds are raw byte keys and use the same lexicographic ordering as existing range APIs.

## Optimized Height-2 Algorithm

The first optimized implementation targets the current benchmark tree shape: a height-2 root whose children are level-1 internal nodes and whose grandchildren are leaves.

1. Validate the tree root and persisted format.
2. Locate the root children containing `start` and `end` using separator-floor routing.
3. Load a bounded internal-node window containing one predecessor chunk and enough right-side chunks to prove canonical resynchronization.
4. Flatten their child summaries. Persisted child counts and first-key separators are sufficient to classify leaf subtrees relative to the deletion range.
5. Reuse summaries wholly before the range.
6. Remove summaries for leaves proven wholly inside `[start, end)` without loading those leaf payloads. A leaf is wholly covered only when its first key is at or after `start` and the next leaf's first key is at or before `end`. The global rightmost leaf has no successor separator, so it is loaded and filtered rather than elided.
7. Load and filter only partially covered boundary leaves plus predecessor context required by the configured boundary detector.
8. Replay surviving entries through the existing canonical leaf emitter.
9. Continue through right-side context until an unchanged leaf CID proves leaf-boundary resynchronization, or until the global right edge.
10. Rebuild level-1 chunks from the replacement leaf summaries using the configured chunking and layout.
11. Require an unchanged internal CID at the right edge of a nonterminal window before splicing replacement summaries into the root.
12. Validate root ordering, entry counts, chunk limits, and the hard byte cap.
13. Batch-write only new leaf, internal, and root nodes. Cache small write sets using the existing policy.

The classification in step 6 is safe because range deletion removes every key in the interval. Unlike point-delete batches, it does not need to prove that a supplied list exactly equals the leaf's key set.

## General Fallback

Unsupported shapes, custom node layouts, missing or invalid child counts, insufficient successor context, or failed content-ID resynchronization use a correct streaming fallback.

The fallback iterates the tree in key order, emits entries outside `[start, end)`, and builds the result with the same configured canonical builder. It may be slower, but it must produce the same root as a clean build of all surviving entries. Optimization failures are never correctness failures.

Future work may generalize the localized splice level by level for trees taller than height 2. That generalization is not required for the initial performance gate because the 1M and 10M fixtures are height 2.

## Correctness and Failure Semantics

- The result must equal a clean canonical rebuild for every supported chunking policy and built-in node layout.
- The source tree is immutable.
- Content-addressed descendants may be written before an error, but no partial root is returned or published.
- Store, decoding, format, capacity, and malformed-node errors propagate through existing error types.
- Empty, reversed, and disjoint ranges do not write nodes.
- Deleting the complete keyspace returns an empty tree.
- Existing point delete, batch, append, diff, merge, and transaction semantics do not change.

## Binding Contract

The UniFFI facade and native wrappers will add range-delete methods using byte-array start and end bounds. Python, Go, Node, browser WASM, Kotlin, Java, Ruby, and Swift wrappers will expose the same half-open semantics. Existing generated records and mutation kinds remain unchanged.

Binding verification will follow `bindings/VERIFICATION.md`. An unavailable host SDK or dependency will be reported separately from a project-code failure.

## Tests

The red-green implementation sequence will add tests for:

- empty and reversed ranges;
- empty trees and ranges containing no keys;
- exact leaf and internal boundaries;
- partial first and last leaves;
- complete-tree deletion;
- ranges beginning before the first key or ending after the final key;
- custom-layout and deeper-tree fallbacks;
- configurable chunking and every built-in node layout;
- randomized comparison with a clean canonical rebuild;
- no writes for no-op ranges;
- bounded node reads for dense clustered deletion;
- async parity;
- language-binding smoke coverage.

The focused performance regression test must demonstrate that fully covered interior leaves are not fetched from storage.

## Performance Evaluation

The SQLite comparison will use the existing deterministic 1M and 10M fixtures, WAL+FULL and WAL+NORMAL profiles, alternating current/original process order, and at least five repetitions. The logical workload remains deletion of the same contiguous 10,000-key interval; the enhanced revision will invoke the semantically equivalent range operation.

The report will include latency ranges and medians, node and byte reads, node and byte writes, cache behavior, result counts, tree shape, SQLite fixture size, validation status, and machine/build metadata.

The merge gate is:

- every result validates and matches the expected surviving keyspace;
- no material regression at 1M or 10M against the original revision;
- existing workloads and point-delete tests remain green;
- formatting, Clippy with warnings denied, full Rust tests, and maintained binding suites pass.

If the range operation still regresses materially, the result is not merge-ready and the regression is reported without reclassifying it as noise.
