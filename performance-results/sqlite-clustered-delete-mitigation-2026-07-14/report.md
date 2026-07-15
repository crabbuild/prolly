# SQLite-backed prolly tree performance evaluation

Lower latency, peak RSS, fixture size, and I/O are better. Deltas are `(current - original) / original`. Medians are shown with full measured ranges.

## Failures and invalid rows

None.

## Material latency regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000000 | clustered_batch_deletes | 65.140ms | 68.470ms | +5.1% | -8.6% | +0.4% |
| full | 10000000 | clustered_batch_deletes | 103.651ms | 127.419ms | +22.9% | -7.6% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 99.124ms | 127.178ms | +28.3% | -7.6% | +0.4% |

## Memory regressions

None.

## SQLite fixture-size regressions

None.

## Prolly I/O regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000000 | clustered_batch_deletes | 65.140ms | 68.470ms | +5.1% | -8.6% | +0.4% |
| full | 10000000 | clustered_batch_deletes | 103.651ms | 127.419ms | +22.9% | -7.6% | +0.4% |
| normal | 1000000 | clustered_batch_deletes | 65.490ms | 64.572ms | -1.4% | -1.5% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 99.124ms | 127.178ms | +28.3% | -7.6% | +0.4% |

## Material latency gains

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 10000000 | sorted_stream_build | 10.109s | 9.495s | -6.1% | -7.4% | +0.4% |
| normal | 1000000 | sorted_stream_build | 800.588ms | 768.998ms | -3.9% | -6.6% | +0.5% |

## Complete latency matrix

### WAL+FULL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 65.140ms (62.559ms–68.502ms) | 68.470ms (64.733ms–71.162ms) | +5.1% | material regression |
| 1000000 | sorted_stream_build | 5 | 803.001ms (774.046ms–831.312ms) | 780.179ms (742.615ms–811.737ms) | -2.8% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 103.651ms (99.000ms–213.568ms) | 127.419ms (119.221ms–136.888ms) | +22.9% | material regression |
| 10000000 | sorted_stream_build | 5 | 10.109s (9.599s–10.227s) | 9.495s (9.309s–9.837s) | -6.1% | material gain |

### WAL+NORMAL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000000 | clustered_batch_deletes | 5 | 65.490ms (62.617ms–68.536ms) | 64.572ms (60.753ms–68.279ms) | -1.4% | noise-sensitive |
| 1000000 | sorted_stream_build | 5 | 800.588ms (768.873ms–843.532ms) | 768.998ms (740.388ms–811.282ms) | -3.9% | material gain |
| 10000000 | clustered_batch_deletes | 5 | 99.124ms (93.931ms–115.317ms) | 127.178ms (117.305ms–135.917ms) | +28.3% | material regression |
| 10000000 | sorted_stream_build | 5 | 9.505s (9.393s–9.896s) | 9.711s (9.284s–9.957s) | +2.2% | noise-sensitive |

## Structural, storage, memory, and I/O matrix

| Profile | Records | Workload | RSS O→C | Fixture O→C | Nodes read O→C | Nodes written O→C | Bytes read O→C | Bytes written O→C | Tree bytes O→C | Height O→C | Flags |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| full | 1000000 | clustered_batch_deletes | 204.55MiB→186.89MiB | 44.47MiB→44.67MiB | 39→49 | 4→3 | 459956→502270 | 46083→23911 | 40985344→41164194 | 2→2 | I/O regression |
| full | 1000000 | sorted_stream_build | 185.69MiB→190.27MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 10000000 | clustered_batch_deletes | 1.89GiB→1.75GiB | 445.02MiB→446.94MiB | 37→48 | 4→4 | 432979→521551 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| full | 10000000 | sorted_stream_build | 1.89GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 1000000 | clustered_batch_deletes | 189.56MiB→186.73MiB | 44.47MiB→44.67MiB | 39→49 | 4→3 | 459956→502270 | 46083→23911 | 40985344→41164194 | 2→2 | I/O regression |
| normal | 1000000 | sorted_stream_build | 203.77MiB→190.23MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 10000000 | clustered_batch_deletes | 1.89GiB→1.75GiB | 445.02MiB→446.94MiB | 37→48 | 4→4 | 432979→521551 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| normal | 10000000 | sorted_stream_build | 1.73GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |

## Methodology and limitations

The two revisions use byte-identical benchmark sources, deterministic keys and mutation sets, separate WAL+FULL and WAL+NORMAL profiles, alternating process order, and isolated SQLite fixture clones. Cold-manager means a fresh decoded-node cache; the operating-system page cache is not flushed. Diff and merge branch preparation is outside the timed interval, while process peak RSS includes that preparation. Validation and SQLite integrity checks are required before a row enters the aggregates.

Latency is material at ±3% only when both medians are at least 1 ms and measured ranges do not broadly overlap. Memory requires +5% and +4 MiB; fixture size requires +3% and +1 MiB; prolly I/O flags any +3% median increase.

## Machine and build metadata

```text
timestamp_utc=2026-07-15T03:14:13Z
current_revision=75c5c9216e60710cbdbf63bb6907e8816acb809f
baseline_revision=fa7c219afc7e1ee5769dd85e5223ea5dde9e3074
current_dirty=true
harness_sha256=6d89b3c8856bd157084a31de80654a68a7eded4f17e2a997e045b4dd1ded7268
current_binary_sha256=33b5153f4d8363795a010ed754ac642cdba395534fbc729bf06096442bd6a4e1
baseline_binary_sha256=b1e2eb17cf8db239d6226691badeb961d356962817c49003ac6f35ca0a05867c
rustc=rustc 1.97.0 (2d8144b78 2026-07-07);binary: rustc;commit-hash: 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3;commit-date: 2026-07-07;host: aarch64-apple-darwin;release: 1.97.0;LLVM version: 22.1.6;
cargo=cargo 1.97.0 (c980f4866 2026-06-30)
sqlite_cli=3.51.0 2025-06-12 13:14:41 f0ca7bba1c5e232e5d279fad6338121ab55af0c8c68c84cdfb18ba5114dcaapl (64-bit)
uname=Darwin Haipings-Mac-Studio.local 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun  9 22:28:24 PDT 2026; root:xnu-12377.121.10~1/RELEASE_ARM64_T6020 arm64
cpu_count=12
memory_bytes=34359738368
filesystem=/dev/disk3s5 460Gi 415Gi 21Gi 96% 3.8M 218M 2% /System/Volumes/Data
copy_method=clonefile
sizes=1000000 10000000
runs=5
profiles=full normal
workloads=clustered_batch_deletes
```
