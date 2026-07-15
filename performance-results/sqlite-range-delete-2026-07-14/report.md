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
| full | 10000000 | clustered_batch_deletes | 114.713ms | 44.029ms | -61.6% | -7.7% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 97.504ms | 43.067ms | -55.8% | -7.6% | +0.4% |

## Material latency gains

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000000 | clustered_batch_deletes | 66.856ms | 22.361ms | -66.6% | -10.0% | +0.4% |
| full | 1000000 | sorted_stream_build | 778.301ms | 738.674ms | -5.1% | -6.4% | +0.5% |
| normal | 1000000 | clustered_batch_deletes | 66.817ms | 21.535ms | -67.8% | -9.9% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 97.504ms | 43.067ms | -55.8% | -7.6% | +0.4% |

## Complete latency matrix

### WAL+FULL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 66.856ms (64.058ms–76.531ms) | 22.361ms (18.602ms–225.705ms) | -66.6% | material gain |
| 1000000 | sorted_stream_build | 5 | 778.301ms (747.660ms–825.889ms) | 738.674ms (721.425ms–796.848ms) | -5.1% | material gain |
| 10000000 | clustered_batch_deletes | 5 | 114.713ms (98.178ms–697.332ms) | 44.029ms (42.627ms–521.565ms) | -61.6% | noise-sensitive |
| 10000000 | sorted_stream_build | 5 | 9.407s (9.161s–10.503s) | 9.753s (9.028s–10.296s) | +3.7% | noise-sensitive |

### WAL+NORMAL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 66.817ms (62.175ms–68.067ms) | 21.535ms (19.411ms–21.759ms) | -67.8% | material gain |
| 1000000 | sorted_stream_build | 5 | 772.213ms (748.883ms–1.096s) | 741.832ms (717.199ms–1.017s) | -3.9% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 97.504ms (92.013ms–103.797ms) | 43.067ms (42.064ms–48.598ms) | -55.8% | material gain |
| 10000000 | sorted_stream_build | 5 | 9.461s (9.231s–9.471s) | 9.388s (9.086s–13.141s) | -0.8% | noise-sensitive |

## Structural, storage, memory, and I/O matrix

| Profile | Records | Workload | RSS O→C | Fixture O→C | Nodes read O→C | Nodes written O→C | Bytes read O→C | Bytes written O→C | Tree bytes O→C | Height O→C | Flags |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| full | 1000000 | clustered_batch_deletes | 206.17MiB→185.47MiB | 44.47MiB→44.67MiB | 39→9 | 4→3 | 459956→95136 | 46083→23911 | 40985344→41164194 | 2→2 | — |
| full | 1000000 | sorted_stream_build | 201.06MiB→188.22MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 10000000 | clustered_batch_deletes | 1.90GiB→1.75GiB | 445.02MiB→446.94MiB | 37→10 | 4→4 | 432979→127828 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| full | 10000000 | sorted_stream_build | 1.89GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 1000000 | clustered_batch_deletes | 205.62MiB→185.22MiB | 44.47MiB→44.67MiB | 39→9 | 4→3 | 459956→95136 | 46083→23911 | 40985344→41164194 | 2→2 | — |
| normal | 1000000 | sorted_stream_build | 205.89MiB→190.38MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 10000000 | clustered_batch_deletes | 1.90GiB→1.75GiB | 445.02MiB→446.94MiB | 37→10 | 4→4 | 432979→127828 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| normal | 10000000 | sorted_stream_build | 1.89GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |

## Methodology and limitations

The two revisions use byte-identical benchmark sources, deterministic keys and mutation sets, separate WAL+FULL and WAL+NORMAL profiles, alternating process order, and isolated SQLite fixture clones. Cold-manager means a fresh decoded-node cache; the operating-system page cache is not flushed. Diff and merge branch preparation is outside the timed interval, while process peak RSS includes that preparation. Validation and SQLite integrity checks are required before a row enters the aggregates.

Latency is material at ±3% only when both medians are at least 1 ms and measured ranges do not broadly overlap. Memory requires +5% and +4 MiB; fixture size requires +3% and +1 MiB; prolly I/O flags any +3% median increase.

## Machine and build metadata

```text
timestamp_utc=2026-07-15T06:21:27Z
current_revision=e1505427bc3feaf4aa018c68d7507f5aa48371c2
baseline_revision=fa7c219afc7e1ee5769dd85e5223ea5dde9e3074
current_dirty=false
harness_sha256=ab97654ff20f9b5e59b2cc5dc3fa6800ce4193a16f51f345637f4b807f2cab49
current_binary_sha256=ea08d9f7475a8420037d97273d9eec21a9d172c1462be75ed398699172bc20b2
baseline_binary_sha256=46c47483a347273d0b9ce754d95940841ff4ba0b3711f8e7b971b37335db46f5
rustc=rustc 1.97.0 (2d8144b78 2026-07-07);binary: rustc;commit-hash: 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3;commit-date: 2026-07-07;host: aarch64-apple-darwin;release: 1.97.0;LLVM version: 22.1.6;
cargo=cargo 1.97.0 (c980f4866 2026-06-30)
sqlite_cli=3.51.0 2025-06-12 13:14:41 f0ca7bba1c5e232e5d279fad6338121ab55af0c8c68c84cdfb18ba5114dcaapl (64-bit)
uname=Darwin Haipings-Mac-Studio.local 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun  9 22:28:24 PDT 2026; root:xnu-12377.121.10~1/RELEASE_ARM64_T6020 arm64
cpu_count=12
memory_bytes=34359738368
filesystem=/dev/disk3s5 460Gi 421Gi 16Gi 97% 3.7M 168M 2% /System/Volumes/Data
copy_method=clonefile
sizes=1000000 10000000
runs=5
profiles=full normal
workloads=clustered_batch_deletes
```
