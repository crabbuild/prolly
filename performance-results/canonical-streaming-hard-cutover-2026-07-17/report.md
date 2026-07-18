# Canonical Streaming Hard Cutover Report

## Decision

Proceed with the hard cutover. Correctness gates pass, the default-policy build
and append/value-update latency gates pass, and the principal scale workloads
are neutral or faster. This is not a claim that every latency percentile
improved: middle-delete p95 regressed 8.4%, and clustered mutation remains 2.4%
slower. Neither is hidden or averaged into a positive headline.

Implementation provenance: `fbafdf1` on branch
`codex/canonical-streaming-cutover`, Apple M2 Max, Darwin arm64, Rust 1.97.0,
optimized Cargo bench profile. See [baseline.md](baseline.md) and
[after.md](after.md) for commands and raw samples.

## What Changed

- One policy-aware `BoundaryDetector` now owns boundary probability and reset
  semantics for all tree levels.
- Integer fixed-point thresholds make Weibull and rolling decisions stable
  across targets and platforms.
- Canonical `LevelEmitter`/`HierarchicalEmitter` paths are shared by sorted
  construction, sync/async mutation, append, and serial parent rebuilds.
- Exact serialized byte caps are enforced in release builds. Non-encoded
  policies use a conservative fast bound and invoke exact sizing only near the
  cap.
- Stats and parallel write surfaces delegate to canonical mutation execution.
- Stateless boundary probes, direct rebalancers, count/key-value substitution
  helpers, and their generated binding surfaces were removed.
- Sorted construction retains only height-bounded hierarchy state plus a
  bounded persistence batch.

## Performance

### Default-policy latency

Build values are the median of five process medians (20 timed samples per
process). Mutation values are paired 100-sample nearest-rank measurements.

| Workload | Metric | Before ns/item | After ns/item | Delta |
| --- | ---: | ---: | ---: | ---: |
| Sorted build | median | 192 | 186 | -3.1% |
| Unsorted build | median | 101 | 102 | +1.0% |
| Append 1 | p95 | 27,208 | 23,125 | -15.0% |
| Append 64 | p95 | 627 | 593 | -5.4% |
| Append 4,096 | p95 | 198 | 197 | -0.5% |
| Middle value update | p95 | 36,125 | 36,500 | +1.0% |
| Middle insert | p95 | 220,417 | 216,583 | -1.7% |
| Middle delete | p95 | 34,875 | 37,791 | **+8.4%** |

Append latency improved after serial parent reconstruction stopped
materializing and cloning parallel key/CID/count arrays. The delete p95 result
is a real negative result and should be a future optimization target; median
delete latency was unchanged at 34,542 ns.

### One-million-record medians

| Workload | Before ns/op | After ns/op | Delta |
| --- | ---: | ---: | ---: |
| Base sorted build | 449.663 | 445.832 | -0.9% |
| Random reads | 4,555.881 | 4,421.126 | -3.0% |
| Clustered reads | 502.696 | 484.112 | -3.7% |
| Append mutations | 219.908 | 201.254 | -8.5% |
| Random mutations | 19,411.679 | 17,913.537 | -7.7% |
| Clustered mutations | 357.738 | 366.188 | **+2.4%** |
| Append diff | 213.808 | 196.396 | -8.1% |
| Random diff | 23,489.088 | 22,205.904 | -5.5% |
| Clustered diff | 400.254 | 388.413 | -3.0% |

Peak RSS fell from 655,556,608 to 641,335,296 bytes (-2.2%). The default-policy
tree shape and bytes did not change. Nodes and bytes read/written were also
identical for every mutation workload, so the timing changes are CPU/allocation
effects rather than altered I/O amplification. The existing scale CSV does not
emit store-call counters separately; that remains a benchmark instrumentation
gap.

### Distribution correctness

| Policy | Target | Mean | Error | Forced max rate | p99 | Max |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Rolling logical bytes | 16,384 | 17,045 | +4.0% | 0.155% | 54,824 | 65,560 |
| Weibull logical bytes | 16,384 | 15,601 | -4.8% | 0% | 33,880 | 44,924 |

The pre-cutover rolling implementation averaged 62,824 bytes, near its maximum
rather than its 16,384-byte target.

## Acceptance Gates

| Gate | Result | Evidence |
| --- | --- | --- |
| Zero canonical-root divergence across policy/layout/API matrix | PASS | Deterministic and randomized public-writer histories equal fresh bulk roots for all four built-ins. |
| Rolling mean within 10% of target | PASS | +4.0% on the deterministic 250k corpus. |
| Rolling forced-maximum rate below 1% | PASS | 0.155%. |
| Append work proportional to right edge plus appended entries | PASS | Right-edge instrumentation tests and unchanged 4-node reads for the 10k append scale workload. |
| Key-stable value updates preserve leaf boundaries | PASS | Direct and batched value-update canonical-root tests across layouts. |
| Sorted metadata memory height/batch bounded | PASS | Active-level/buffer invariant and 2.2% lower measured peak RSS. |
| Sorted/unsorted build median regression no greater than 3% | PASS | -3.1% and +1.0% in focused runs; -0.9% at one-million-record scale. |
| Append/value-update p95 regression no greater than 3% | PASS | Append -15.0%/-5.4%/-0.5%; value update +1.0%, each across 100 timed samples. |

## Honest Assessment

The new mechanism is materially better in correctness and architecture and is
better overall in measured performance. The strongest wins are corrected chunk
distributions, canonical equivalence across every public writer, height-bounded
sorted construction, append latency, and removal of duplicate serialization.

It is not flawless in the literal sense. Clustered mutations are 2.4% slower,
middle-delete p95 is 8.4% slower, store-call counts are not separately emitted
by the scale harness, and the benchmark uses `MemStore` rather than a durable
backend. Those are follow-up performance tasks, not reasons to retain the
incorrect legacy chunkers.
