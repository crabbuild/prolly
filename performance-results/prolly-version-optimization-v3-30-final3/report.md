# Native Version-Operation Performance

All figures are medians from process-isolated, single-worker, warm in-memory runs. Setup and validation are outside timed regions. Lower latency is better.

Rust wins: 42; Dolt Go wins: 0; ties: 0.
Winner-direction groups with repetition flips: 1.

## Provenance

- Rust revision: `22cac37b5969+src.48720d426093`
- Dolt Go revision: `6b2372c7d4de+runner.41407de9560c`
- Contract: `prolly-version-compare-v3`
- Sizes: `1000000 10000000`
- Repetitions: `3`
- Storage: in-memory; workers: 1

## Common operations at 10,000,000 records

| Density | Locality | Operation | Rust | Dolt Go | Rust vs Go | Winner | Max CV |
|---:|---|---|---:|---:|---:|---|---:|
| 30% | append | full_diff | 228.360 ms | 656.635 ms | 2.88x | rust | 2.17% |
| 30% | append | merge_conflict | 147.332 ms | 1.643 s | 11.15x | rust | 1.36% |
| 30% | append | merge_convergent | 250.0 ns | 131.417 us | 525.67x | rust | 21.98% |
| 30% | append | merge_disjoint | 218.167 us | 666.291 us | 3.05x | rust | 35.15% |
| 30% | append | patch_apply | 4.250 us | 132.916 us | 31.27x | rust | 2.32% |
| 30% | append | patch_generate | 542.0 ns | 26.375 us | 48.66x | rust | 28.03% |
| 30% | append | range_diff | 178.488 ms | 274.978 ms | 1.54x | rust | 2.53% |
| 30% | clustered | full_diff | 395.458 ms | 1.019 s | 2.58x | rust | 3.34% |
| 30% | clustered | merge_conflict | 323.932 ms | 2.076 s | 6.41x | rust | 33.42% |
| 30% | clustered | merge_convergent | 250.0 ns | 642.667 us | 2570.67x | rust | 21.98% |
| 30% | clustered | merge_disjoint | 553.870 ms | 1.142 s | 2.06x | rust | 2.61% |
| 30% | clustered | patch_apply | 3.875 us | 274.125 us | 70.74x | rust | 7.52% |
| 30% | clustered | patch_generate | 791.0 ns | 38.291 us | 48.41x | rust | 120.28% |
| 30% | clustered | range_diff | 45.373 ms | 99.801 ms | 2.20x | rust | 4.17% |
| 30% | random | full_diff | 450.176 ms | 1.623 s | 3.60x | rust | 6.67% |
| 30% | random | merge_conflict | 584.749 ms | 4.866 s | 8.32x | rust | 24.33% |
| 30% | random | merge_convergent | 334.0 ns | 24.791 ms | 74224.93x | rust | 104.70% |
| 30% | random | merge_disjoint | 3.403 s | 5.376 s | 1.58x | rust | 4.48% |
| 30% | random | patch_apply | 4.333 us | 239.042 us | 55.17x | rust | 6.52% |
| 30% | random | patch_generate | 292.0 ns | 21.708 us | 74.34x | rust | 35.03% |
| 30% | random | range_diff | 49.007 ms | 149.309 ms | 3.05x | rust | 2.43% |

`Rust vs Go` is Dolt Go median latency divided by Rust median latency; values above 1.0 favor Rust.
Both implementations use native structural patches. Rust emits one verified target-root subtree envelope, while Dolt may emit multiple structural patches. Native item counts can differ, while comparison units and result validation use identical logical changes.

## Reproducibility

- Complete expected configured matrices: PASS
- Repetitions per scenario: 3 (matrix complete): PASS
- Cross-language workload and logical-result identity: PASS
- All runner validation flags: PASS
- Median CV across implementation/scenario groups: 4.32%
- p95 CV: 62.03%
- Maximum CV: 120.28%
- Groups above 10% CV: 24
