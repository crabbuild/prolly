# Native Version-Operation Performance

All figures are medians from process-isolated, single-worker, warm in-memory runs. Setup and validation are outside timed regions. Lower latency is better.

Rust wins: 36; Dolt Go wins: 11; ties: 0.
Winner-direction groups with repetition flips: 2.

## Provenance

- Rust revision: `fc2dcae8e5d5+src.4dfe4dc7b90d`
- Dolt Go revision: `6b2372c7d4de+runner.8f69a849a214`
- Contract: `prolly-version-compare-v2`
- Sizes: `1000000`
- Repetitions: `3`
- Storage: in-memory; workers: 1

## Common operations at 1,000,000 records

| Density | Locality | Operation | Rust | Dolt Go | Rust vs Go | Winner | Max CV |
|---:|---|---|---:|---:|---:|---|---:|
| 0% | none | full_diff | 83.0 ns | 1.458 us | 17.57x | rust | 53.18% |
| 0% | none | merge_noop | 292.0 ns | 46.833 us | 160.39x | rust | 28.09% |
| 0% | none | patch_apply | 458.0 ns | 500.0 ns | 1.09x | rust | 16.63% |
| 0% | none | patch_generate | 416.0 ns | 2.583 us | 6.21x | rust | 25.46% |
| 0% | none | range_diff | 750.0 ns | 2.541 us | 3.39x | rust | 17.68% |
| 1% | append | full_diff | 1.603 ms | 1.869 ms | 1.17x | rust | 1.81% |
| 1% | append | merge_conflict | 6.196 ms | 4.965 ms | 0.80x | dolt-go | 5.24% |
| 1% | append | merge_convergent | 334.0 ns | 68.208 us | 204.22x | rust | 23.93% |
| 1% | append | merge_disjoint | 6.840 ms | 412.542 us | 0.06x | dolt-go | 4.06% |
| 1% | append | patch_apply | 3.458 us | 81.334 us | 23.52x | rust | 20.43% |
| 1% | append | patch_generate | 625.0 ns | 9.292 us | 14.87x | rust | 64.37% |
| 1% | append | range_diff | 1.634 ms | 1.849 ms | 1.13x | rust | 2.66% |
| 1% | clustered | full_diff | 906.792 us | 2.761 ms | 3.05x | rust | 7.73% |
| 1% | clustered | merge_conflict | 6.839 ms | 6.300 ms | 0.92x | dolt-go | 2.41% |
| 1% | clustered | merge_convergent | 417.0 ns | 57.625 us | 138.19x | rust | 35.50% |
| 1% | clustered | merge_disjoint | 13.898 ms | 833.542 us | 0.06x | dolt-go | 3.96% |
| 1% | clustered | patch_apply | 2.250 us | 32.542 us | 14.46x | rust | 46.85% |
| 1% | clustered | patch_generate | 417.0 ns | 7.667 us | 18.39x | rust | 33.59% |
| 1% | clustered | range_diff | 207.417 us | 482.709 us | 2.33x | rust | 5.57% |
| 1% | random | full_diff | 18.181 ms | 29.196 ms | 1.61x | rust | 1.87% |
| 1% | random | merge_conflict | 235.923 ms | 78.713 ms | 0.33x | dolt-go | 8.04% |
| 1% | random | merge_convergent | 417.0 ns | 376.584 us | 903.08x | rust | 63.67% |
| 1% | random | merge_disjoint | 593.847 ms | 153.338 ms | 0.26x | dolt-go | 4.34% |
| 1% | random | patch_apply | 5.000 us | 33.083 us | 6.62x | rust | 13.17% |
| 1% | random | patch_generate | 625.0 ns | 16.375 us | 26.20x | rust | 75.71% |
| 1% | random | range_diff | 1.743 ms | 2.327 ms | 1.34x | rust | 3.09% |
| 30% | append | full_diff | 19.577 ms | 60.588 ms | 3.09x | rust | 8.56% |
| 30% | append | merge_conflict | 129.320 ms | 147.111 ms | 1.14x | rust | 0.74% |
| 30% | append | merge_convergent | 250.0 ns | 90.750 us | 363.00x | rust | 43.33% |
| 30% | append | merge_disjoint | 196.077 ms | 515.792 us | 0.00x | dolt-go | 3.36% |
| 30% | append | patch_apply | 3.417 us | 88.875 us | 26.01x | rust | 13.72% |
| 30% | append | patch_generate | 1.542 us | 21.583 us | 14.00x | rust | 119.40% |
| 30% | append | range_diff | 11.331 ms | 27.209 ms | 2.40x | rust | 4.44% |
| 30% | clustered | full_diff | 38.171 ms | 86.548 ms | 2.27x | rust | 1.03% |
| 30% | clustered | merge_conflict | 214.186 ms | 192.307 ms | 0.90x | dolt-go | 3.15% |
| 30% | clustered | merge_convergent | 250.0 ns | 336.000 us | 1344.00x | rust | 64.83% |
| 30% | clustered | merge_disjoint | 344.982 ms | 98.918 ms | 0.29x | dolt-go | 7.29% |
| 30% | clustered | patch_apply | 2.791 us | 32.334 us | 11.59x | rust | 8.46% |
| 30% | clustered | patch_generate | 458.0 ns | 11.625 us | 25.38x | rust | 88.61% |
| 30% | clustered | range_diff | 4.457 ms | 9.272 ms | 2.08x | rust | 2.30% |
| 30% | random | full_diff | 45.025 ms | 134.285 ms | 2.98x | rust | 1.17% |
| 30% | random | merge_conflict | 372.193 ms | 293.747 ms | 0.79x | dolt-go | 0.59% |
| 30% | random | merge_convergent | 292.0 ns | 24.539 ms | 84038.81x | rust | 57.85% |
| 30% | random | merge_disjoint | 730.255 ms | 524.241 ms | 0.72x | dolt-go | 0.39% |
| 30% | random | patch_apply | 6.541 us | 41.000 us | 6.27x | rust | 80.39% |
| 30% | random | patch_generate | 667.0 ns | 13.250 us | 19.87x | rust | 49.20% |
| 30% | random | range_diff | 5.017 ms | 14.714 ms | 2.93x | rust | 13.25% |

`Rust vs Go` is Dolt Go median latency divided by Rust median latency; values above 1.0 favor Rust.
Both implementations use native structural patches. Rust v2 emits one verified target-root subtree envelope, while Dolt may emit multiple structural patches. Native item counts can differ, while comparison units and result validation use identical logical changes.

## Rust lifecycle at 1,000,000 records

| Density | Locality | Operation | Median total | Median normalized | Throughput | CV | Result count |
|---:|---|---|---:|---:|---:|---:|---:|
| 0% | none | head_resolve | 9.886 ms | 988.6 ns | 1011531.5/s | 2.46% | 10000 |
| 0% | none | historical_point_read | 434.613 ms | 4.346 us | 230090.0/s | 0.21% | 100000 |
| 0% | none | historical_range_scan | 522.691 ms | 174.2 ns | 5739525.1/s | 0.47% | 3000000 |
| 0% | none | retention_prune | 212.375 us | 2.124 us | 470865.2/s | 3.22% | 11 |
| 0% | none | rollback | 3.677 ms | 3.677 us | 271930.0/s | 1.66% | 1000 |
| 0% | none | snapshot_resolve | 3.902 ms | 1.951 us | 512497.5/s | 0.28% | 2000 |
| 0% | none | version_list | 9.818 ms | 981.8 ns | 1018533.0/s | 1.53% | 10000 |
| 1% | append | version_publish | 4.577 ms | 457.7 ns | 2184717.9/s | 2.15% | 1010000 |
| 1% | clustered | version_publish | 11.452 ms | 1.145 us | 873225.9/s | 0.37% | 1000000 |
| 1% | random | version_publish | 495.108 ms | 49.511 us | 20197.6/s | 1.27% | 1000000 |
| 30% | append | version_publish | 127.145 ms | 423.8 ns | 2359506.9/s | 0.69% | 1300000 |
| 30% | clustered | version_publish | 261.360 ms | 871.2 ns | 1147844.1/s | 0.26% | 1000000 |
| 30% | random | version_publish | 569.806 ms | 1.899 us | 526495.0/s | 0.65% | 1000000 |

## Reproducibility

- Complete expected common and lifecycle matrices: PASS
- Repetitions per scenario: 3 (matrix complete): PASS
- Cross-language workload and logical-result identity: PASS
- All runner validation flags: PASS
- Median CV across implementation/scenario groups: 3.28%
- p95 CV: 64.83%
- Maximum CV: 119.40%
- Groups above 10% CV: 33
