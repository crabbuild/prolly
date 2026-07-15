# SQLite-backed prolly tree performance evaluation

Lower latency, peak RSS, fixture size, and I/O are better. Deltas are `(current - original) / original`. Medians are shown with full measured ranges.

## Failures and invalid rows

None.

## Material latency regressions

None.

## Memory regressions

None.

## SQLite fixture-size regressions

None.

## Prolly I/O regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 10000000 | clustered_batch_deletes | 125.527ms | 56.348ms | -55.1% | +0.4% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 124.758ms | 54.749ms | -56.1% | +0.6% | +0.4% |

## Material latency gains

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000000 | clustered_batch_deletes | 80.020ms | 26.434ms | -67.0% | -2.4% | +0.4% |
| full | 10000000 | clustered_batch_deletes | 125.527ms | 56.348ms | -55.1% | +0.4% | +0.4% |
| normal | 1000000 | clustered_batch_deletes | 81.107ms | 24.409ms | -69.9% | -10.7% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 124.758ms | 54.749ms | -56.1% | +0.6% | +0.4% |

## Complete latency matrix

### WAL+FULL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 80.020ms (74.705ms–87.581ms) | 26.434ms (21.361ms–138.270ms) | -67.0% | material gain |
| 1000000 | sorted_stream_build | 5 | 838.645ms (835.890ms–890.135ms) | 845.449ms (810.133ms–887.388ms) | +0.8% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 125.527ms (115.640ms–129.097ms) | 56.348ms (50.819ms–63.949ms) | -55.1% | material gain |
| 10000000 | sorted_stream_build | 5 | 10.500s (10.194s–11.227s) | 10.407s (10.138s–10.783s) | -0.9% | noise-sensitive |

### WAL+NORMAL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 81.107ms (73.305ms–85.727ms) | 24.409ms (23.096ms–28.266ms) | -69.9% | material gain |
| 1000000 | sorted_stream_build | 5 | 842.775ms (839.891ms–918.143ms) | 822.222ms (816.171ms–898.913ms) | -2.4% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 124.758ms (120.645ms–128.988ms) | 54.749ms (53.468ms–226.513ms) | -56.1% | material gain |
| 10000000 | sorted_stream_build | 5 | 10.521s (10.287s–11.144s) | 10.357s (10.224s–10.470s) | -1.6% | noise-sensitive |

## Structural, storage, memory, and I/O matrix

| Profile | Records | Workload | RSS O→C | Fixture O→C | Nodes read O→C | Nodes written O→C | Bytes read O→C | Bytes written O→C | Tree bytes O→C | Height O→C | Flags |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| full | 1000000 | clustered_batch_deletes | 189.88MiB→185.34MiB | 44.47MiB→44.67MiB | 39→9 | 4→3 | 459956→95136 | 46083→23911 | 40985344→41164194 | 2→2 | — |
| full | 1000000 | sorted_stream_build | 190.83MiB→176.48MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 10000000 | clustered_batch_deletes | 1.74GiB→1.75GiB | 445.02MiB→446.94MiB | 37→10 | 4→4 | 432979→127828 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| full | 10000000 | sorted_stream_build | 1.74GiB→1.60GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 1000000 | clustered_batch_deletes | 190.36MiB→170.06MiB | 44.47MiB→44.67MiB | 39→9 | 4→3 | 459956→95136 | 46083→23911 | 40985344→41164194 | 2→2 | — |
| normal | 1000000 | sorted_stream_build | 190.55MiB→176.48MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 10000000 | clustered_batch_deletes | 1.74GiB→1.75GiB | 445.02MiB→446.94MiB | 37→10 | 4→4 | 432979→127828 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| normal | 10000000 | sorted_stream_build | 1.74GiB→1.60GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |

## Methodology and limitations

The two revisions use byte-identical benchmark sources, deterministic keys and mutation sets, separate WAL+FULL and WAL+NORMAL profiles, alternating process order, and isolated SQLite fixture clones. Cold-manager means a fresh decoded-node cache; the operating-system page cache is not flushed. Diff and merge branch preparation is outside the timed interval, while process peak RSS includes that preparation. Validation and SQLite integrity checks are required before a row enters the aggregates.

Latency is material at ±3% only when both medians are at least 1 ms and measured ranges do not broadly overlap. Memory requires +5% and +4 MiB; fixture size requires +3% and +1 MiB; prolly I/O flags any +3% median increase.

## Machine and build metadata

```text
timestamp_utc=2026-07-15T07:39:10Z
current_revision=d3d6d4cb67aad611189164a984c98483c4d6c41e
baseline_revision=fa7c219afc7e1ee5769dd85e5223ea5dde9e3074
current_dirty=false
harness_sha256=ab97654ff20f9b5e59b2cc5dc3fa6800ce4193a16f51f345637f4b807f2cab49
current_binary_sha256=753cca03f46d31270707219016000077e61bc4d5d225e5bd722fad1864315075
baseline_binary_sha256=c352061decb510c1ff9da311c2c638c51a5438fa79c80bc000880ddbc0b21e79
rustc=rustc 1.97.0 (2d8144b78 2026-07-07);binary: rustc;commit-hash: 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3;commit-date: 2026-07-07;host: aarch64-apple-darwin;release: 1.97.0;LLVM version: 22.1.6;
cargo=cargo 1.97.0 (c980f4866 2026-06-30)
sqlite_cli=3.51.0 2025-06-12 13:14:41 f0ca7bba1c5e232e5d279fad6338121ab55af0c8c68c84cdfb18ba5114dcaapl (64-bit)
uname=Darwin Haipings-Mac-Studio.local 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun  9 22:28:24 PDT 2026; root:xnu-12377.121.10~1/RELEASE_ARM64_T6020 arm64
cpu_count=12
memory_bytes=34359738368
filesystem=/dev/disk3s5 460Gi 428Gi 9.6Gi 98% 3.8M 100M 4% /System/Volumes/Data
copy_method=clonefile
sizes=1000000 10000000
runs=5
profiles=full normal
workloads=clustered_batch_deletes
```
