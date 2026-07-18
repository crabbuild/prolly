# Canonical Streaming Hot-Path Optimization Report

## Decision

Keep all three optimizations. Canonical outputs and public behavior are
unchanged, the rolling detector is materially faster, every protected focused
median improved, and peak RSS fell in the paired scale process. No measured
focused median degradation was introduced.

The implementation consists of:

- a lazily populated 256-byte-value xxHash table and allocation-retaining ring
  window for rolling chunking;
- hierarchy-owned reusable cascade scratch restored on both success and error;
- point-update eligibility classification before temporary batch-adapter
  allocation.

## Focused Performance

Each value is the median of five independent process medians. Lower is better.

| Workload | Before median ns | After median ns | Delta |
| --- | ---: | ---: | ---: |
| Boundary entry-count key hash | 13 | 12 | -7.7% |
| Boundary logical-byte Weibull | 48 | 46 | -4.2% |
| Boundary logical-byte rolling hash | 236 | 90 | **-61.9%** |
| Sorted build | 199 | 197 | -1.0% |
| Unsorted build | 106 | 101 | -4.7% |
| Append 1 | 19,708 | 18,917 | -4.0% |
| Append 64 | 589 | 564 | -4.2% |
| Append 4,096 | 197 | 192 | -2.5% |
| Middle value update | 36,833 | 35,291 | -4.2% |
| Middle insert | 214,834 | 209,750 | -2.4% |
| Middle delete | 36,166 | 34,583 | -4.4% |

The only protected p95 increase was append-1, from 23,709 to 24,167 ns
(+1.9%), inside the 3% gate. All other p95 values were neutral or faster;
middle-delete p95 improved 8.2%, removing the prior hard-cutover hotspot in
this focused run.

The new 90 ns/entry rolling result is effectively at parity with the pinned
Dolt Go rolling measurement of about 89.3 ns/entry. Before this optimization,
Rust required 236 ns/entry in the permanent benchmark (and about 230 ns/entry
in the earlier direct diagnostic). This closes nearly all of the implementation
overhead gap without adopting Dolt's tree-format or mutation architecture.

## Correctness and Resource Impact

- Complete rolling boundary sequences match a retained `VecDeque` reference
  across randomized key/value streams, automatic cuts, and explicit resets.
- Cached hashes match direct xxHash for all 256 byte values under seeds 0, 1,
  and `u64::MAX`.
- Canonical-root, chunk-policy, builder-policy, write-stat, and range-delete
  suites pass.
- The full all-feature/all-target workspace run, 93 doc tests, Clippy with
  warnings denied, formatting, and the 2,876-operation binding inventory pass.
- One-million-record outputs retained identical tree shape, serialized bytes,
  nodes read/written, and bytes read/written.
- Same-session peak RSS fell from 629,866,496 to 611,450,880 bytes (-2.9%).

## Scale Timing Caveat

The required first three one-million-record processes all validated, but their
latencies were not clean acceptance evidence. The shared machine reached a
13.04 load average with 24 unrelated Rust compiler processes plus high-CPU
macOS services. Read and diff controls that these changes do not touch slowed
by similar amounts. A same-session exact-baseline worktree confirmed the
machine effect, and those raw samples remain in `after.md`.

A second stage-isolation comparison ran `c41d4c4` (immediately before the point
route edit) three times, followed by current `36dbedc` three times. Clustered
mutations improved 5.2%, random mutations improved 1.4%, and append mutations
were neutral (-0.1%). Unaffected build/read controls stayed within 1%. This
resolves the earlier apparent +16% clustered-mutation outlier as contention,
not a retained code regression.

## Honest Assessment

This is a strong improvement, not literal flawlessness. Rolling chunking is now
competitive with Dolt Go at the detector level, while Rust retains persisted
policy selection, deterministic integer thresholds, exact canonical roots, and
one streaming emitter architecture. The remaining performance work should be
profile-driven: durable-store latency, explicit allocation counters, and a
clean idle-machine million-record rerun. No further speculative hot-path edits
are justified by the current evidence.
