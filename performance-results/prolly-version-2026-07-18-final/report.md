# Native Version-Operation Performance

All figures are medians from process-isolated, single-worker, warm in-memory runs. Setup and validation are outside timed regions. Lower latency is better.

Rust wins: 101; Dolt Go wins: 134; ties: 0.
Winner-direction groups with repetition flips: 14.

## Provenance

- Rust revision: `677bc05461ac+src.4839a3a46636`
- Dolt Go revision: `6b2372c7d4de+runner.652d11a2d7f2`
- Contract: `prolly-version-compare-v1`
- Sizes: `10000 50000 1000000 5000000 10000000`
- Repetitions: `3`
- Storage: in-memory; workers: 1

## Common operations at 10,000,000 records

| Density | Locality | Operation | Rust | Dolt Go | Rust vs Go | Winner | Max CV |
|---:|---|---|---:|---:|---:|---|---:|
| 0% | none | full_diff | 42.0 ns | 1.917 us | 45.64x | rust | 3.56% |
| 0% | none | merge_noop | 334.0 ns | 50.542 us | 151.32x | rust | 126.49% |
| 0% | none | patch_apply | 1.375 us | 375.0 ns | 0.27x | dolt-go | 9.14% |
| 0% | none | patch_generate | 3.917 us | 4.500 us | 1.15x | rust | 75.09% |
| 0% | none | range_diff | 58.625 us | 3.208 us | 0.05x | dolt-go | 16.92% |
| 1% | append | full_diff | 6.855 ms | 19.133 ms | 2.79x | rust | 3.75% |
| 1% | append | merge_conflict | 105.007 ms | 48.481 ms | 0.46x | dolt-go | 1.06% |
| 1% | append | merge_convergent | 125.0 ns | 72.667 us | 581.34x | rust | 46.65% |
| 1% | append | merge_disjoint | 66.012 ms | 503.250 us | 0.01x | dolt-go | 2.01% |
| 1% | append | patch_apply | 447.947 ms | 120.834 us | 0.00x | dolt-go | 11.24% |
| 1% | append | patch_generate | 7.910 ms | 17.167 us | 0.00x | dolt-go | 20.46% |
| 1% | append | range_diff | 7.344 ms | 19.884 ms | 2.71x | rust | 2.42% |
| 1% | clustered | full_diff | 18.445 ms | 29.521 ms | 1.60x | rust | 28.56% |
| 1% | clustered | merge_conflict | 142.722 ms | 63.983 ms | 0.45x | dolt-go | 1.88% |
| 1% | clustered | merge_convergent | 541.0 ns | 57.500 us | 106.28x | rust | 59.54% |
| 1% | clustered | merge_disjoint | 140.344 ms | 8.835 ms | 0.06x | dolt-go | 11.04% |
| 1% | clustered | patch_apply | 144.506 ms | 392.208 us | 0.00x | dolt-go | 10.11% |
| 1% | clustered | patch_generate | 20.346 ms | 35.250 us | 0.00x | dolt-go | 34.92% |
| 1% | clustered | range_diff | 5.934 ms | 3.283 ms | 0.55x | dolt-go | 3.73% |
| 1% | random | full_diff | 118.949 ms | 257.461 ms | 2.16x | rust | 1.63% |
| 1% | random | merge_conflict | 3.847 s | 483.041 ms | 0.13x | dolt-go | 10.07% |
| 1% | random | merge_convergent | 208.0 ns | 353.083 us | 1697.51x | rust | 53.88% |
| 1% | random | merge_disjoint | 4.539 s | 676.819 ms | 0.15x | dolt-go | 1.48% |
| 1% | random | patch_apply | 3.335 s | 306.834 us | 0.00x | dolt-go | 8.81% |
| 1% | random | patch_generate | 120.763 ms | 47.291 us | 0.00x | dolt-go | 27.22% |
| 1% | random | range_diff | 13.100 ms | 17.382 ms | 1.33x | rust | 4.54% |
| 30% | append | full_diff | 277.251 ms | 633.115 ms | 2.28x | rust | 1.56% |
| 30% | append | merge_conflict | 3.497 s | 1.580 s | 0.45x | dolt-go | 3.80% |
| 30% | append | merge_convergent | 167.0 ns | 132.333 us | 792.41x | rust | 63.80% |
| 30% | append | merge_disjoint | 2.157 s | 714.709 us | 0.00x | dolt-go | 107.00% |
| 30% | append | patch_apply | 13.454 s | 133.458 us | 0.00x | dolt-go | 8.48% |
| 30% | append | patch_generate | 304.116 ms | 46.125 us | 0.00x | dolt-go | 137.18% |
| 30% | append | range_diff | 200.370 ms | 263.550 ms | 1.32x | rust | 10.92% |
| 30% | clustered | full_diff | 432.550 ms | 987.654 ms | 2.28x | rust | 9.30% |
| 30% | clustered | merge_conflict | 4.068 s | 1.998 s | 0.49x | dolt-go | 14.47% |
| 30% | clustered | merge_convergent | 167.0 ns | 590.958 us | 3538.67x | rust | 41.49% |
| 30% | clustered | merge_disjoint | 3.286 s | 1.083 s | 0.33x | dolt-go | 9.23% |
| 30% | clustered | patch_apply | 3.560 s | 268.583 us | 0.00x | dolt-go | 43.63% |
| 30% | clustered | patch_generate | 450.664 ms | 44.667 us | 0.00x | dolt-go | 72.64% |
| 30% | clustered | range_diff | 52.336 ms | 115.350 ms | 2.20x | rust | 9.29% |
| 30% | random | full_diff | 508.457 ms | 1.566 s | 3.08x | rust | 23.50% |
| 30% | random | merge_conflict | 10.964 s | 3.881 s | 0.35x | dolt-go | 16.13% |
| 30% | random | merge_convergent | 375.0 ns | 24.023 ms | 64060.11x | rust | 20.42% |
| 30% | random | merge_disjoint | 9.484 s | 4.954 s | 0.52x | dolt-go | 5.90% |
| 30% | random | patch_apply | 9.077 s | 220.833 us | 0.00x | dolt-go | 13.52% |
| 30% | random | patch_generate | 569.490 ms | 37.584 us | 0.00x | dolt-go | 125.43% |
| 30% | random | range_diff | 62.037 ms | 144.348 ms | 2.33x | rust | 18.16% |

`Rust vs Go` is Dolt Go median latency divided by Rust median latency; values above 1.0 favor Rust.
Native patch representations differ: Rust currently materializes logical point edits while Dolt may emit structural subtree patches. Patch generation reports native patch counts, while comparison units and result validation use logical changes.

## Rust lifecycle at 10,000,000 records

| Density | Locality | Operation | Median total | Median normalized | Throughput | CV | Result count |
|---:|---|---|---:|---:|---:|---:|---:|
| 0% | none | head_resolve | 9.918 ms | 991.8 ns | 1008288.9/s | 0.81% | 10000 |
| 0% | none | historical_point_read | 438.473 ms | 4.385 us | 228064.2/s | 0.35% | 100000 |
| 0% | none | historical_range_scan | 5.295 s | 176.5 ns | 5665375.4/s | 0.18% | 30000000 |
| 0% | none | retention_prune | 232.375 us | 2.324 us | 430338.9/s | 37.19% | 11 |
| 0% | none | rollback | 2.928 ms | 2.928 us | 341568.9/s | 1.31% | 1000 |
| 0% | none | snapshot_resolve | 3.889 ms | 1.944 us | 514271.0/s | 2.40% | 2000 |
| 0% | none | version_list | 10.162 ms | 1.016 us | 984050.1/s | 1.82% | 10000 |
| 1% | append | version_publish | 43.753 ms | 437.5 ns | 2285559.7/s | 1.24% | 10100000 |
| 1% | clustered | version_publish | 121.404 ms | 1.214 us | 823697.8/s | 0.94% | 10000000 |
| 1% | random | version_publish | 4.323 s | 43.230 us | 23131.9/s | 7.07% | 10000000 |
| 30% | append | version_publish | 1.321 s | 440.4 ns | 2270560.1/s | 2.55% | 13000000 |
| 30% | clustered | version_publish | 2.703 s | 901.0 ns | 1109892.8/s | 2.33% | 10000000 |
| 30% | random | version_publish | 5.450 s | 1.817 us | 550450.5/s | 2.04% | 10000000 |

## Reproducibility

- Complete expected common and lifecycle matrices: PASS
- Repetitions per scenario: 3 (matrix complete): PASS
- Cross-language workload and logical-result identity: PASS
- All runner validation flags: PASS
- Median CV across implementation/scenario groups: 4.91%
- p95 CV: 72.64%
- Maximum CV: 137.18%
- Groups above 10% CV: 182
