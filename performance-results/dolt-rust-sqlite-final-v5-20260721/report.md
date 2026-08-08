# Dolt Go vs Rust SQLite prolly comparison

Validated pairs: 78. All workload counts and logical cardinalities matched.

## Aggregate result

- Rust sum of 26 paired medians: 3,669.437 ms
- Dolt Go sum of 26 paired medians: 9,462.152 ms
- Dolt Go / Rust: **2.579x**
- Geometric mean of per-cell Go/Rust ratios: 1.650x
- Summary winners: Rust 17, Dolt Go 9
- Process validation: 156/156 passed
- Fixture database size: Rust 10.31 MiB, Dolt Go 157.79 MiB

| Records | Operation | Pattern | Rust median | Dolt Go median | Go/Rust | Winner |
|---:|---|---|---:|---:|---:|---|
| 1000000 | batch | append | 83966959 ns | 760308000 ns | 9.05x | rust |
| 1000000 | batch | clustered | 208324833 ns | 769656459 ns | 3.69x | rust |
| 1000000 | batch | random | 586699458 ns | 1606000125 ns | 2.74x | rust |
| 1000000 | diff | append | 56973959 ns | 43605542 ns | 0.77x | dolt-go |
| 1000000 | diff | clustered | 72244917 ns | 60944250 ns | 0.84x | dolt-go |
| 1000000 | diff | random | 223797750 ns | 660871666 ns | 2.95x | rust |
| 1000000 | full_scan | append | 558073875 ns | 752239750 ns | 1.35x | rust |
| 1000000 | get_cold | append | 173556084 ns | 376479666 ns | 2.17x | rust |
| 1000000 | get_cold | clustered | 192986541 ns | 371910458 ns | 1.93x | rust |
| 1000000 | get_cold | random | 256881250 ns | 420858750 ns | 1.64x | rust |
| 1000000 | get_warm | append | 2591917 ns | 10661375 ns | 4.11x | rust |
| 1000000 | get_warm | clustered | 2684250 ns | 9783500 ns | 3.64x | rust |
| 1000000 | get_warm | random | 10013042 ns | 16148667 ns | 1.61x | rust |
| 1000000 | merge | append | 37774333 ns | 3143875 ns | 0.08x | dolt-go |
| 1000000 | merge | clustered | 898875 ns | 121395083 ns | 135.05x | rust |
| 1000000 | merge | random | 490636000 ns | 2464184166 ns | 5.02x | rust |
| 1000000 | put | append | 2376083 ns | 1317125 ns | 0.55x | dolt-go |
| 1000000 | put | clustered | 3504916 ns | 1744792 ns | 0.50x | dolt-go |
| 1000000 | put | random | 2721458 ns | 1602667 ns | 0.59x | dolt-go |
| 1000000 | query | append | 9039250 ns | 15940750 ns | 1.76x | rust |
| 1000000 | query | clustered | 8594667 ns | 15356791 ns | 1.79x | rust |
| 1000000 | query | random | 114656625 ns | 116757166 ns | 1.02x | rust |
| 1000000 | scan | append | 13824292 ns | 9925375 ns | 0.72x | dolt-go |
| 1000000 | scan | clustered | 15755750 ns | 9215833 ns | 0.58x | dolt-go |
| 1000000 | scan | random | 16026625 ns | 9516333 ns | 0.59x | dolt-go |
| 1000000 | build | n/a | 524832833 ns | 832583625 ns | 1.59x | rust |

## Interpretation limits

Each implementation uses its native tree encoding and chunking policy; only logical workloads and outcomes are paired.
Rust query rows use native `get_many`; Dolt exposes no map-level multi-get, so its query rows use repeated `Map.Get` calls.
SQLite WAL and `synchronous=NORMAL` match, but runtime caches, allocators, and persisted chunk layouts differ.
