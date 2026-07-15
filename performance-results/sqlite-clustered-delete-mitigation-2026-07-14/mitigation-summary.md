# Clustered batch-delete mitigation summary

## Outcome

The localized rewrite removes the dominant full-frontier regression while preserving canonical-root equivalence. It does **not** remove every regression against the original revision, so this change does not pass a strict no-regression merge gate yet.

At 10 million records, the prior enhanced path read 177 nodes and about 1.93 MB for a clustered deletion. The localized path reads 48 nodes and 0.52 MB:

| Metric | Before mitigation | After mitigation | Change |
| --- | ---: | ---: | ---: |
| Nodes read | 177 | 48 | -72.9% |
| Bytes read | 1,925,000 | 521,551 | -72.9% |
| FULL latency | about 389 ms | 127.419 ms | about -67% |
| NORMAL latency | about 341 ms | 127.178 ms | about -63% |

The improvement comes from reading and rewriting only a bounded level-1 subtree window, replaying enough predecessor context to re-establish chunk boundaries, and requiring an unchanged content ID at the right edge before splicing the replacement back into the root. If that proof fails, the implementation falls back to the complete canonical writer.

## Comparison with the original revision

Five alternating-order repetitions were run for each size and SQLite durability profile. All 80 build/delete rows validated successfully.

| Records | Profile | Original median | Current median | Delta |
| ---: | --- | ---: | ---: | ---: |
| 1M | WAL+FULL | 65.140 ms | 68.470 ms | +5.1% |
| 1M | WAL+NORMAL | 65.490 ms | 64.572 ms | -1.4% |
| 10M | WAL+FULL | 103.651 ms | 127.419 ms | +22.9% |
| 10M | WAL+NORMAL | 99.124 ms | 127.178 ms | +28.3% |

The remaining 10M difference is consistent with extra cold-tree I/O in the current tree format and chunk distribution: 48 nodes / 521,551 bytes versus 37 nodes / 432,979 bytes in the original. The current path also writes 52,518 bytes versus 19,251 bytes for this exact fixture. These are real residual regressions, not benchmark noise.

## Correctness and scope

- The fast path currently applies to clustered batches containing only deletes on a height-2 tree using a built-in node layout.
- It preserves the configured chunking policy and node layout.
- It requires leaf-boundary and internal-boundary content-ID resynchronization before reuse.
- Unsupported shapes or failed resynchronization use the complete canonical path.
- The regression test compares the resulting root with a clean canonical rebuild.

See [report.md](report.md) for complete ranges, build controls, structural I/O, memory, fixture size, methodology, and machine metadata. Raw observations are in [raw-results.csv](raw-results.csv).
