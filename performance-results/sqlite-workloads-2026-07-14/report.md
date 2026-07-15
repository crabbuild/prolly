# SQLite-backed prolly tree performance evaluation

Lower latency, peak RSS, fixture size, and I/O are better. Deltas are `(current - original) / original`. Medians are shown with full measured ranges.

## Failures and invalid rows

None.

## Material latency regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 10000 | clustered_batch_deletes | 1.612ms | 1.679ms | +4.2% | -5.1% | +1.6% |
| full | 50000 | append_disjoint_sparse_merge | 1.494ms | 1.557ms | +4.2% | -8.0% | +1.0% |
| full | 10000000 | sorted_stream_build | 9.586s | 10.903s | +13.7% | -7.7% | +0.4% |
| normal | 10000 | clustered_batch_deletes | 1.331ms | 1.553ms | +16.7% | -4.3% | +1.6% |
| normal | 50000 | append_batch_upserts | 5.774ms | 6.840ms | +18.5% | -6.7% | +0.5% |
| normal | 50000 | clustered_batch_deletes | 4.699ms | 7.835ms | +66.7% | -7.2% | +0.7% |
| normal | 50000 | clustered_disjoint_sparse_merge | 1.073ms | 1.627ms | +51.7% | -8.1% | +0.7% |
| normal | 100000 | clustered_batch_deletes | 8.431ms | 11.014ms | +30.6% | -8.2% | -0.3% |
| normal | 10000000 | append_disjoint_sparse_merge | 19.795ms | 21.168ms | +6.9% | -7.6% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 102.107ms | 340.643ms | +233.6% | -7.4% | +0.4% |
| normal | 10000000 | clustered_batch_updates | 177.066ms | 199.932ms | +12.9% | -7.7% | +0.4% |
| normal | 10000000 | clustered_conflict_resolved_merge | 19.505ms | 24.344ms | +24.8% | -7.6% | +0.4% |

## Memory regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 10000000 | random_batch_deletes | 10.906s | 8.202s | -24.8% | +9.9% | +4.8% |
| full | 10000000 | shuffled_batch_build | 10.719s | 14.321s | +33.6% | +12.6% | +0.4% |
| normal | 1000000 | shuffled_batch_build | 837.914ms | 842.469ms | +0.5% | +11.0% | +0.5% |
| normal | 10000000 | random_batch_deletes | 6.903s | 7.740s | +12.1% | +8.7% | +4.8% |
| normal | 10000000 | random_delete_diff | 2.462s | 1.677s | -31.9% | +8.6% | +4.8% |

## SQLite fixture-size regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 10000000 | random_batch_deletes | 10.906s | 8.202s | -24.8% | +9.9% | +4.8% |
| full | 10000000 | random_delete_diff | 3.059s | 1.647s | -46.2% | +2.7% | +4.8% |
| normal | 10000000 | random_batch_deletes | 6.903s | 7.740s | +12.1% | +8.7% | +4.8% |
| normal | 10000000 | random_delete_diff | 2.462s | 1.677s | -31.9% | +8.6% | +4.8% |

## Prolly I/O regressions

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000 | clustered_batch_deletes | 499.375µs | 605.750µs | +21.3% | +0.7% | +25.0% |
| full | 1000 | clustered_reads_cold_manager | 624.821ms | 414.735ms | -33.6% | +0.4% | +13.6% |
| full | 1000 | random_batch_deletes | 953.834µs | 906.125µs | -5.0% | +1.4% | +11.5% |
| full | 1000 | random_batch_updates | 1.333ms | 983.083µs | -26.3% | -2.7% | +23.1% |
| full | 1000 | random_conflict_resolved_merge | 750.084µs | 456.875µs | -39.1% | +3.5% | +24.3% |
| full | 1000 | random_delete_diff | 562.208µs | 282.792µs | -49.7% | -0.3% | +8.8% |
| full | 1000 | random_reads_cold_manager | 563.229ms | 423.899ms | -24.7% | +0.0% | +13.6% |
| full | 1000 | random_sparse_diff | 457.541µs | 293.875µs | -35.8% | +0.3% | +17.6% |
| full | 1000 | right_edge_reads_cold_manager | 580.723ms | 424.038ms | -27.0% | +0.8% | +13.6% |
| full | 10000 | clustered_batch_deletes | 1.612ms | 1.679ms | +4.2% | -5.1% | +1.6% |
| full | 10000 | clustered_batch_updates | 1.506ms | 1.219ms | -19.1% | -9.2% | +1.6% |
| full | 10000 | clustered_sparse_diff | 318.125µs | 235.125µs | -26.1% | -6.0% | +1.5% |
| full | 10000 | random_batch_deletes | 7.365ms | 5.773ms | -21.6% | -3.5% | +1.8% |
| full | 10000 | random_conflict_resolved_merge | 5.907ms | 2.998ms | -49.2% | -5.2% | +4.3% |
| full | 10000 | random_delete_diff | 4.133ms | 2.860ms | -30.8% | -5.0% | +1.7% |
| full | 10000 | random_disjoint_sparse_merge | 515.666µs | 644.541µs | +25.0% | -4.3% | +3.9% |
| full | 50000 | append_batch_upserts | 4.848ms | 6.426ms | +32.6% | -5.7% | +0.5% |
| full | 50000 | append_disjoint_sparse_merge | 1.494ms | 1.557ms | +4.2% | -8.0% | +1.0% |
| full | 50000 | append_sparse_diff | 952.208µs | 367.750µs | -61.4% | -6.8% | +0.5% |
| full | 50000 | clustered_batch_deletes | 6.165ms | 6.413ms | +4.0% | -7.4% | +0.7% |
| full | 50000 | clustered_batch_updates | 6.340ms | 6.427ms | +1.4% | -7.9% | +0.5% |
| full | 50000 | clustered_disjoint_sparse_merge | 938.083µs | 1.443ms | +53.8% | -6.9% | +0.7% |
| full | 50000 | random_batch_deletes | 47.013ms | 37.680ms | -19.9% | -17.9% | +1.1% |
| full | 50000 | random_conflict_resolved_merge | 31.190ms | 16.616ms | -46.7% | -7.8% | +2.8% |
| full | 50000 | random_disjoint_sparse_merge | 785.583µs | 716.000µs | -8.9% | -12.6% | +0.3% |
| full | 100000 | clustered_batch_deletes | 8.473ms | 11.071ms | +30.7% | -7.4% | -0.3% |
| full | 100000 | clustered_batch_updates | 13.020ms | 12.518ms | -3.9% | -6.5% | -0.1% |
| full | 100000 | clustered_conflict_resolved_merge | 1.549ms | 1.265ms | -18.3% | -8.0% | +0.0% |
| full | 100000 | clustered_reads_cold_manager | 763.643ms | 454.462ms | -40.5% | -7.2% | +0.0% |
| full | 100000 | clustered_sparse_diff | 1.668ms | 1.185ms | -29.0% | -7.4% | -0.1% |
| full | 100000 | random_batch_deletes | 101.924ms | 91.821ms | -9.9% | -12.9% | +1.2% |
| full | 100000 | random_disjoint_sparse_merge | 603.833µs | 977.417µs | +61.9% | -15.6% | +0.8% |
| full | 1000000 | append_batch_upserts | 65.470ms | 70.196ms | +7.2% | -7.1% | +0.5% |
| full | 1000000 | append_disjoint_sparse_merge | 12.553ms | 11.259ms | -10.3% | -7.1% | +0.5% |
| full | 1000000 | append_sparse_diff | 10.424ms | 9.028ms | -13.4% | -7.0% | +0.5% |
| full | 1000000 | clustered_batch_deletes | 67.208ms | 85.657ms | +27.4% | -8.3% | +0.4% |
| full | 1000000 | clustered_batch_updates | 106.546ms | 111.295ms | +4.5% | -7.9% | +0.6% |
| full | 1000000 | clustered_conflict_resolved_merge | 12.193ms | 11.838ms | -2.9% | -8.1% | +0.4% |
| full | 1000000 | clustered_reads_cold_manager | 888.557ms | 504.710ms | -43.2% | -7.4% | +0.5% |
| full | 1000000 | clustered_sparse_diff | 11.857ms | 11.524ms | -2.8% | -7.4% | +0.6% |
| full | 1000000 | random_batch_deletes | 1.247s | 1.101s | -11.7% | -18.7% | +1.6% |
| full | 1000000 | random_disjoint_sparse_merge | 1.207ms | 1.193ms | -1.1% | -28.3% | +0.8% |
| full | 1000000 | right_edge_reads_cold_manager | 894.323ms | 513.452ms | -42.6% | -7.4% | +0.5% |
| full | 10000000 | append_batch_upserts | 141.078ms | 118.851ms | -15.8% | -7.8% | +0.4% |
| full | 10000000 | append_disjoint_sparse_merge | 24.190ms | 22.348ms | -7.6% | -7.7% | +0.4% |
| full | 10000000 | clustered_batch_deletes | 105.900ms | 389.257ms | +267.6% | -7.3% | +0.4% |
| full | 10000000 | clustered_batch_updates | 211.127ms | 211.638ms | +0.2% | -7.6% | +0.4% |
| full | 10000000 | clustered_conflict_resolved_merge | 19.046ms | 14.638ms | -23.1% | -7.5% | +0.4% |
| full | 10000000 | clustered_disjoint_sparse_merge | 10.777ms | 9.414ms | -12.7% | -7.6% | +0.4% |
| full | 10000000 | clustered_reads_cold_manager | 1.003s | 549.800ms | -45.2% | -7.7% | +0.4% |
| full | 10000000 | clustered_sparse_diff | 20.491ms | 15.044ms | -26.6% | -7.6% | +0.4% |
| full | 10000000 | random_batch_deletes | 10.906s | 8.202s | -24.8% | +9.9% | +4.8% |
| full | 10000000 | right_edge_reads_cold_manager | 1.029s | 586.775ms | -43.0% | -7.7% | +0.4% |
| normal | 1000 | clustered_batch_deletes | 452.833µs | 543.250µs | +20.0% | +2.3% | +25.0% |
| normal | 1000 | clustered_reads_cold_manager | 555.583ms | 414.551ms | -25.4% | -0.4% | +13.6% |
| normal | 1000 | random_batch_deletes | 919.209µs | 881.292µs | -4.1% | +1.7% | +11.5% |
| normal | 1000 | random_batch_updates | 962.208µs | 821.583µs | -14.6% | -1.7% | +23.1% |
| normal | 1000 | random_conflict_resolved_merge | 743.417µs | 441.709µs | -40.6% | +4.8% | +24.3% |
| normal | 1000 | random_delete_diff | 541.167µs | 272.750µs | -49.6% | -0.6% | +8.8% |
| normal | 1000 | random_reads_cold_manager | 549.008ms | 419.965ms | -23.5% | +0.0% | +13.6% |
| normal | 1000 | random_sparse_diff | 441.208µs | 281.084µs | -36.3% | -0.3% | +17.6% |
| normal | 1000 | right_edge_reads_cold_manager | 557.132ms | 407.424ms | -26.9% | +0.8% | +13.6% |
| normal | 10000 | clustered_batch_deletes | 1.331ms | 1.553ms | +16.7% | -4.3% | +1.6% |
| normal | 10000 | clustered_batch_updates | 1.396ms | 1.370ms | -1.9% | -7.8% | +1.6% |
| normal | 10000 | clustered_sparse_diff | 303.708µs | 277.458µs | -8.6% | -6.2% | +1.5% |
| normal | 10000 | random_batch_deletes | 7.090ms | 5.488ms | -22.6% | -4.6% | +1.8% |
| normal | 10000 | random_conflict_resolved_merge | 5.870ms | 2.925ms | -50.2% | -3.7% | +4.3% |
| normal | 10000 | random_delete_diff | 3.987ms | 2.981ms | -25.2% | -1.4% | +1.7% |
| normal | 10000 | random_disjoint_sparse_merge | 497.917µs | 622.625µs | +25.0% | -3.6% | +3.9% |
| normal | 50000 | append_batch_upserts | 5.774ms | 6.840ms | +18.5% | -6.7% | +0.5% |
| normal | 50000 | append_disjoint_sparse_merge | 1.396ms | 1.290ms | -7.6% | -5.9% | +1.0% |
| normal | 50000 | append_sparse_diff | 844.250µs | 572.500µs | -32.2% | -7.0% | +0.5% |
| normal | 50000 | clustered_batch_deletes | 4.699ms | 7.835ms | +66.7% | -7.2% | +0.7% |
| normal | 50000 | clustered_batch_updates | 5.907ms | 6.353ms | +7.6% | -9.4% | +0.5% |
| normal | 50000 | clustered_disjoint_sparse_merge | 1.073ms | 1.627ms | +51.7% | -8.1% | +0.7% |
| normal | 50000 | random_batch_deletes | 42.768ms | 41.098ms | -3.9% | -13.1% | +1.1% |
| normal | 50000 | random_conflict_resolved_merge | 30.714ms | 16.139ms | -47.5% | -7.7% | +2.8% |
| normal | 50000 | random_disjoint_sparse_merge | 777.292µs | 642.792µs | -17.3% | -13.3% | +0.3% |
| normal | 100000 | clustered_batch_deletes | 8.431ms | 11.014ms | +30.6% | -8.2% | -0.3% |
| normal | 100000 | clustered_batch_updates | 13.619ms | 13.308ms | -2.3% | -7.1% | -0.1% |
| normal | 100000 | clustered_conflict_resolved_merge | 1.549ms | 1.286ms | -17.0% | -9.0% | +0.0% |
| normal | 100000 | clustered_reads_cold_manager | 777.821ms | 442.563ms | -43.1% | -6.8% | +0.0% |
| normal | 100000 | clustered_sparse_diff | 1.543ms | 1.248ms | -19.1% | -6.9% | -0.1% |
| normal | 100000 | random_batch_deletes | 104.764ms | 91.157ms | -13.0% | -12.8% | +1.2% |
| normal | 100000 | random_disjoint_sparse_merge | 533.666µs | 865.375µs | +62.2% | -15.4% | +0.8% |
| normal | 1000000 | append_batch_upserts | 65.905ms | 69.882ms | +6.0% | -7.6% | +0.5% |
| normal | 1000000 | append_disjoint_sparse_merge | 11.616ms | 11.307ms | -2.7% | -7.6% | +0.5% |
| normal | 1000000 | append_sparse_diff | 10.770ms | 9.330ms | -13.4% | -7.0% | +0.5% |
| normal | 1000000 | clustered_batch_deletes | 64.427ms | 79.956ms | +24.1% | -8.0% | +0.4% |
| normal | 1000000 | clustered_batch_updates | 106.852ms | 106.982ms | +0.1% | -8.2% | +0.6% |
| normal | 1000000 | clustered_conflict_resolved_merge | 12.579ms | 11.708ms | -6.9% | -7.7% | +0.4% |
| normal | 1000000 | clustered_reads_cold_manager | 953.005ms | 500.867ms | -47.4% | -7.3% | +0.5% |
| normal | 1000000 | clustered_sparse_diff | 12.091ms | 10.173ms | -15.9% | -8.1% | +0.6% |
| normal | 1000000 | random_batch_deletes | 1.234s | 1.012s | -17.9% | -31.1% | +1.6% |
| normal | 1000000 | random_disjoint_sparse_merge | 1.109ms | 1.110ms | +0.1% | -26.3% | +0.8% |
| normal | 1000000 | right_edge_reads_cold_manager | 892.972ms | 499.831ms | -44.0% | -7.4% | +0.5% |
| normal | 10000000 | append_batch_upserts | 117.003ms | 111.154ms | -5.0% | -7.6% | +0.4% |
| normal | 10000000 | append_disjoint_sparse_merge | 19.795ms | 21.168ms | +6.9% | -7.6% | +0.4% |
| normal | 10000000 | clustered_batch_deletes | 102.107ms | 340.643ms | +233.6% | -7.4% | +0.4% |
| normal | 10000000 | clustered_batch_updates | 177.066ms | 199.932ms | +12.9% | -7.7% | +0.4% |
| normal | 10000000 | clustered_conflict_resolved_merge | 19.505ms | 24.344ms | +24.8% | -7.6% | +0.4% |
| normal | 10000000 | clustered_disjoint_sparse_merge | 10.079ms | 7.044ms | -30.1% | -7.7% | +0.4% |
| normal | 10000000 | clustered_reads_cold_manager | 912.561ms | 541.476ms | -40.7% | -7.7% | +0.4% |
| normal | 10000000 | clustered_sparse_diff | 16.449ms | 14.089ms | -14.3% | -7.7% | +0.4% |
| normal | 10000000 | random_batch_deletes | 6.903s | 7.740s | +12.1% | +8.7% | +4.8% |
| normal | 10000000 | right_edge_reads_cold_manager | 927.052ms | 522.722ms | -43.6% | -7.7% | +0.4% |

## Material latency gains

| Profile | Records | Workload | Original | Current | Latency delta | RSS delta | Size delta |
|---|---:|---|---:|---:|---:|---:|---:|
| full | 1000 | clustered_reads_cold_manager | 624.821ms | 414.735ms | -33.6% | +0.4% | +13.6% |
| full | 1000 | clustered_reads_warm_manager | 642.854ms | 425.830ms | -33.8% | +0.0% | +13.6% |
| full | 1000 | random_reads_cold_manager | 563.229ms | 423.899ms | -24.7% | +0.0% | +13.6% |
| full | 1000 | random_reads_warm_manager | 568.258ms | 414.113ms | -27.1% | +0.0% | +13.6% |
| full | 1000 | right_edge_reads_cold_manager | 580.723ms | 424.038ms | -27.0% | +0.8% | +13.6% |
| full | 1000 | right_edge_reads_warm_manager | 569.657ms | 423.593ms | -25.6% | +0.4% | +13.6% |
| full | 10000 | clustered_batch_updates | 1.506ms | 1.219ms | -19.1% | -9.2% | +1.6% |
| full | 10000 | clustered_reads_cold_manager | 629.306ms | 425.808ms | -32.3% | -3.3% | +0.0% |
| full | 10000 | clustered_reads_warm_manager | 631.266ms | 419.731ms | -33.5% | -3.6% | +0.0% |
| full | 10000 | random_batch_updates | 7.392ms | 5.294ms | -28.4% | -2.6% | +0.4% |
| full | 10000 | random_conflict_resolved_merge | 5.907ms | 2.998ms | -49.2% | -5.2% | +4.3% |
| full | 10000 | random_delete_diff | 4.133ms | 2.860ms | -30.8% | -5.0% | +1.7% |
| full | 10000 | random_reads_cold_manager | 627.197ms | 436.090ms | -30.5% | -3.8% | +0.0% |
| full | 10000 | random_reads_warm_manager | 640.001ms | 421.372ms | -34.2% | -3.5% | +0.0% |
| full | 10000 | random_sparse_diff | 4.102ms | 2.358ms | -42.5% | -7.2% | +0.4% |
| full | 10000 | right_edge_reads_cold_manager | 643.028ms | 431.082ms | -33.0% | -4.2% | +0.0% |
| full | 10000 | right_edge_reads_warm_manager | 626.169ms | 430.490ms | -31.3% | -3.4% | +0.0% |
| full | 50000 | clustered_conflict_resolved_merge | 1.504ms | 1.039ms | -31.0% | -9.5% | +0.2% |
| full | 50000 | clustered_reads_cold_manager | 658.946ms | 441.096ms | -33.1% | -7.8% | +0.3% |
| full | 50000 | clustered_reads_warm_manager | 654.890ms | 424.712ms | -35.1% | -7.4% | +0.3% |
| full | 50000 | random_conflict_resolved_merge | 31.190ms | 16.616ms | -46.7% | -7.8% | +2.8% |
| full | 50000 | random_delete_diff | 20.016ms | 15.281ms | -23.7% | -42.3% | +1.1% |
| full | 50000 | random_reads_cold_manager | 701.303ms | 506.719ms | -27.7% | -6.5% | +0.3% |
| full | 50000 | random_reads_warm_manager | 670.778ms | 499.213ms | -25.6% | -6.6% | +0.3% |
| full | 50000 | random_sparse_diff | 21.595ms | 11.861ms | -45.1% | -11.4% | -0.2% |
| full | 50000 | right_edge_reads_cold_manager | 659.815ms | 445.879ms | -32.4% | -7.5% | +0.3% |
| full | 50000 | right_edge_reads_warm_manager | 631.040ms | 428.357ms | -32.1% | -8.2% | +0.3% |
| full | 100000 | append_batch_upserts | 11.439ms | 8.768ms | -23.4% | -7.8% | -0.3% |
| full | 100000 | append_disjoint_sparse_merge | 3.876ms | 2.877ms | -25.8% | -7.8% | -0.2% |
| full | 100000 | clustered_conflict_resolved_merge | 1.549ms | 1.265ms | -18.3% | -8.0% | +0.0% |
| full | 100000 | clustered_reads_cold_manager | 763.643ms | 454.462ms | -40.5% | -7.2% | +0.0% |
| full | 100000 | clustered_reads_warm_manager | 764.397ms | 431.128ms | -43.6% | -7.7% | +0.0% |
| full | 100000 | random_conflict_resolved_merge | 62.799ms | 37.050ms | -41.0% | -7.3% | +1.7% |
| full | 100000 | random_delete_diff | 39.905ms | 36.118ms | -9.5% | -32.5% | +1.2% |
| full | 100000 | random_reads_cold_manager | 863.812ms | 650.065ms | -24.7% | -7.8% | +0.0% |
| full | 100000 | random_reads_warm_manager | 784.849ms | 590.475ms | -24.8% | -8.0% | +0.0% |
| full | 100000 | random_sparse_diff | 42.964ms | 24.533ms | -42.9% | -10.0% | +0.8% |
| full | 100000 | right_edge_reads_cold_manager | 786.679ms | 448.338ms | -43.0% | -7.7% | +0.0% |
| full | 100000 | right_edge_reads_warm_manager | 748.175ms | 432.690ms | -42.2% | -7.2% | +0.0% |
| full | 1000000 | append_sparse_diff | 10.424ms | 9.028ms | -13.4% | -7.0% | +0.5% |
| full | 1000000 | clustered_delete_diff | 215.242ms | 3.567ms | -98.3% | -12.2% | +0.4% |
| full | 1000000 | clustered_reads_cold_manager | 888.557ms | 504.710ms | -43.2% | -7.4% | +0.5% |
| full | 1000000 | clustered_reads_warm_manager | 818.812ms | 452.694ms | -44.7% | -7.3% | +0.5% |
| full | 1000000 | random_batch_deletes | 1.247s | 1.101s | -11.7% | -18.7% | +1.6% |
| full | 1000000 | random_batch_updates | 1.279s | 983.987ms | -23.1% | -23.9% | +0.8% |
| full | 1000000 | random_conflict_resolved_merge | 747.742ms | 438.290ms | -41.4% | -11.5% | +0.6% |
| full | 1000000 | random_delete_diff | 445.296ms | 388.620ms | -12.7% | -29.6% | +1.6% |
| full | 1000000 | random_sparse_diff | 504.699ms | 337.144ms | -33.2% | -19.8% | +0.8% |
| full | 1000000 | right_edge_reads_cold_manager | 894.323ms | 513.452ms | -42.6% | -7.4% | +0.5% |
| full | 1000000 | right_edge_reads_warm_manager | 836.263ms | 430.616ms | -48.5% | -7.3% | +0.5% |
| full | 10000000 | append_disjoint_sparse_merge | 24.190ms | 22.348ms | -7.6% | -7.7% | +0.4% |
| full | 10000000 | clustered_conflict_resolved_merge | 19.046ms | 14.638ms | -23.1% | -7.5% | +0.4% |
| full | 10000000 | clustered_delete_diff | 311.853ms | 4.625ms | -98.5% | -7.4% | +0.4% |
| full | 10000000 | clustered_reads_cold_manager | 1.003s | 549.800ms | -45.2% | -7.7% | +0.4% |
| full | 10000000 | clustered_reads_warm_manager | 867.310ms | 437.999ms | -49.5% | -7.8% | +0.4% |
| full | 10000000 | random_conflict_resolved_merge | 1.652s | 978.619ms | -40.7% | -3.9% | +0.4% |
| full | 10000000 | random_delete_diff | 3.059s | 1.647s | -46.2% | +2.7% | +4.8% |
| full | 10000000 | random_sparse_diff | 1.753s | 1.246s | -28.9% | -7.1% | +0.5% |
| full | 10000000 | right_edge_reads_cold_manager | 1.029s | 586.775ms | -43.0% | -7.7% | +0.4% |
| full | 10000000 | right_edge_reads_warm_manager | 888.048ms | 438.289ms | -50.6% | -7.7% | +0.4% |
| normal | 1000 | clustered_reads_cold_manager | 555.583ms | 414.551ms | -25.4% | -0.4% | +13.6% |
| normal | 1000 | clustered_reads_warm_manager | 558.254ms | 407.896ms | -26.9% | +0.4% | +13.6% |
| normal | 1000 | random_reads_cold_manager | 549.008ms | 419.965ms | -23.5% | +0.0% | +13.6% |
| normal | 1000 | random_reads_warm_manager | 554.520ms | 417.726ms | -24.7% | -0.4% | +13.6% |
| normal | 1000 | right_edge_reads_cold_manager | 557.132ms | 407.424ms | -26.9% | +0.8% | +13.6% |
| normal | 1000 | right_edge_reads_warm_manager | 581.789ms | 413.644ms | -28.9% | +0.8% | +13.6% |
| normal | 10000 | clustered_reads_cold_manager | 647.684ms | 420.803ms | -35.0% | -3.1% | +0.0% |
| normal | 10000 | clustered_reads_warm_manager | 667.847ms | 418.802ms | -37.3% | -3.3% | +0.0% |
| normal | 10000 | random_batch_deletes | 7.090ms | 5.488ms | -22.6% | -4.6% | +1.8% |
| normal | 10000 | random_batch_updates | 7.026ms | 6.092ms | -13.3% | -4.6% | +0.4% |
| normal | 10000 | random_conflict_resolved_merge | 5.870ms | 2.925ms | -50.2% | -3.7% | +4.3% |
| normal | 10000 | random_delete_diff | 3.987ms | 2.981ms | -25.2% | -1.4% | +1.7% |
| normal | 10000 | random_reads_cold_manager | 679.011ms | 424.347ms | -37.5% | -4.3% | +0.0% |
| normal | 10000 | random_reads_warm_manager | 663.062ms | 418.262ms | -36.9% | -4.6% | +0.0% |
| normal | 10000 | random_sparse_diff | 3.982ms | 2.364ms | -40.6% | -4.0% | +0.4% |
| normal | 10000 | right_edge_reads_cold_manager | 701.591ms | 427.725ms | -39.0% | -2.8% | +0.0% |
| normal | 10000 | right_edge_reads_warm_manager | 707.694ms | 422.218ms | -40.3% | -3.3% | +0.0% |
| normal | 10000 | sorted_stream_build | 6.171ms | 5.585ms | -9.5% | -3.7% | +0.0% |
| normal | 50000 | clustered_reads_cold_manager | 670.979ms | 456.401ms | -32.0% | -8.5% | +0.3% |
| normal | 50000 | clustered_reads_warm_manager | 647.956ms | 431.709ms | -33.4% | -7.8% | +0.3% |
| normal | 50000 | random_batch_updates | 43.533ms | 38.893ms | -10.7% | -11.0% | -0.2% |
| normal | 50000 | random_conflict_resolved_merge | 30.714ms | 16.139ms | -47.5% | -7.7% | +2.8% |
| normal | 50000 | random_reads_cold_manager | 715.106ms | 530.909ms | -25.8% | -7.0% | +0.3% |
| normal | 50000 | random_reads_warm_manager | 680.446ms | 505.183ms | -25.8% | -7.0% | +0.3% |
| normal | 50000 | random_sparse_diff | 21.480ms | 11.766ms | -45.2% | -10.8% | -0.2% |
| normal | 50000 | right_edge_reads_cold_manager | 663.088ms | 445.210ms | -32.9% | -8.8% | +0.3% |
| normal | 50000 | right_edge_reads_warm_manager | 649.490ms | 428.267ms | -34.1% | -8.0% | +0.3% |
| normal | 100000 | clustered_conflict_resolved_merge | 1.549ms | 1.286ms | -17.0% | -9.0% | +0.0% |
| normal | 100000 | clustered_reads_cold_manager | 777.821ms | 442.563ms | -43.1% | -6.8% | +0.0% |
| normal | 100000 | clustered_reads_warm_manager | 766.979ms | 424.657ms | -44.6% | -7.3% | +0.0% |
| normal | 100000 | clustered_sparse_diff | 1.543ms | 1.248ms | -19.1% | -6.9% | -0.1% |
| normal | 100000 | random_conflict_resolved_merge | 60.824ms | 33.766ms | -44.5% | -9.3% | +1.7% |
| normal | 100000 | random_delete_diff | 39.634ms | 33.023ms | -16.7% | -36.3% | +1.2% |
| normal | 100000 | random_reads_cold_manager | 842.817ms | 635.139ms | -24.6% | -8.0% | +0.0% |
| normal | 100000 | random_reads_warm_manager | 792.582ms | 592.127ms | -25.3% | -7.6% | +0.0% |
| normal | 100000 | random_sparse_diff | 45.152ms | 25.027ms | -44.6% | -8.9% | +0.8% |
| normal | 100000 | right_edge_reads_cold_manager | 793.318ms | 445.279ms | -43.9% | -7.9% | +0.0% |
| normal | 100000 | right_edge_reads_warm_manager | 766.181ms | 422.037ms | -44.9% | -7.6% | +0.0% |
| normal | 1000000 | clustered_delete_diff | 213.637ms | 3.344ms | -98.4% | -12.1% | +0.4% |
| normal | 1000000 | clustered_disjoint_sparse_merge | 8.470ms | 4.643ms | -45.2% | -7.4% | +0.5% |
| normal | 1000000 | clustered_reads_cold_manager | 953.005ms | 500.867ms | -47.4% | -7.3% | +0.5% |
| normal | 1000000 | clustered_reads_warm_manager | 838.306ms | 445.923ms | -46.8% | -7.4% | +0.5% |
| normal | 1000000 | random_batch_updates | 1.255s | 987.331ms | -21.3% | -19.7% | +0.8% |
| normal | 1000000 | random_conflict_resolved_merge | 714.171ms | 424.281ms | -40.6% | -13.1% | +0.6% |
| normal | 1000000 | random_delete_diff | 431.020ms | 383.043ms | -11.1% | -35.0% | +1.6% |
| normal | 1000000 | random_reads_warm_manager | 1.210s | 1.089s | -9.9% | -9.7% | +0.5% |
| normal | 1000000 | random_sparse_diff | 501.296ms | 327.148ms | -34.7% | -28.7% | +0.8% |
| normal | 1000000 | right_edge_reads_cold_manager | 892.972ms | 499.831ms | -44.0% | -7.4% | +0.5% |
| normal | 1000000 | right_edge_reads_warm_manager | 847.110ms | 441.362ms | -47.9% | -7.4% | +0.5% |
| normal | 1000000 | sorted_stream_build | 788.645ms | 711.353ms | -9.8% | -6.8% | +0.5% |
| normal | 10000000 | clustered_delete_diff | 303.812ms | 4.907ms | -98.4% | -7.4% | +0.4% |
| normal | 10000000 | clustered_disjoint_sparse_merge | 10.079ms | 7.044ms | -30.1% | -7.7% | +0.4% |
| normal | 10000000 | clustered_reads_cold_manager | 912.561ms | 541.476ms | -40.7% | -7.7% | +0.4% |
| normal | 10000000 | clustered_reads_warm_manager | 819.511ms | 423.423ms | -48.3% | -7.7% | +0.4% |
| normal | 10000000 | random_conflict_resolved_merge | 1.655s | 962.904ms | -41.8% | -7.2% | +0.4% |
| normal | 10000000 | random_delete_diff | 2.462s | 1.677s | -31.9% | +8.6% | +4.8% |
| normal | 10000000 | random_sparse_diff | 1.710s | 1.157s | -32.3% | -11.5% | +0.5% |
| normal | 10000000 | right_edge_reads_cold_manager | 927.052ms | 522.722ms | -43.6% | -7.7% | +0.4% |
| normal | 10000000 | right_edge_reads_warm_manager | 875.366ms | 426.869ms | -51.2% | -7.7% | +0.4% |
| normal | 10000000 | shuffled_batch_build | 11.165s | 10.613s | -4.9% | +2.9% | +0.4% |

## Complete latency matrix

### WAL+FULL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000 | append_batch_upserts | 5 | 942.041µs (671.917µs–1.244ms) | 855.333µs (765.083µs–957.667µs) | -9.2% | noise-sensitive |
| 1000 | append_disjoint_sparse_merge | 5 | 651.917µs (609.125µs–770.584µs) | 386.625µs (343.542µs–464.167µs) | -40.7% | noise-sensitive |
| 1000 | append_sparse_diff | 5 | 138.417µs (128.417µs–175.917µs) | 69.041µs (65.042µs–71.667µs) | -50.1% | noise-sensitive |
| 1000 | clustered_batch_deletes | 5 | 499.375µs (454.125µs–817.292µs) | 605.750µs (548.333µs–764.167µs) | +21.3% | noise-sensitive |
| 1000 | clustered_batch_updates | 5 | 671.833µs (590.583µs–1.425ms) | 586.958µs (419.000µs–814.542µs) | -12.6% | noise-sensitive |
| 1000 | clustered_conflict_resolved_merge | 5 | 241.167µs (226.542µs–287.709µs) | 118.750µs (107.459µs–125.791µs) | -50.8% | noise-sensitive |
| 1000 | clustered_delete_diff | 5 | 147.625µs (131.834µs–154.875µs) | 69.459µs (64.417µs–88.583µs) | -52.9% | noise-sensitive |
| 1000 | clustered_disjoint_sparse_merge | 5 | 526.542µs (460.709µs–636.958µs) | 351.083µs (342.542µs–423.084µs) | -33.3% | noise-sensitive |
| 1000 | clustered_reads_cold_manager | 5 | 624.821ms (548.680ms–749.283ms) | 414.735ms (409.683ms–475.300ms) | -33.6% | material gain |
| 1000 | clustered_reads_warm_manager | 5 | 642.854ms (545.891ms–730.173ms) | 425.830ms (408.789ms–484.400ms) | -33.8% | material gain |
| 1000 | clustered_sparse_diff | 5 | 168.917µs (155.416µs–271.083µs) | 77.167µs (73.792µs–90.083µs) | -54.3% | noise-sensitive |
| 1000 | identical_diff | 5 | 1.542µs (1.375µs–4.541µs) | 12.375µs (1.459µs–14.584µs) | +702.5% | noise-sensitive |
| 1000 | random_batch_deletes | 5 | 953.834µs (874.625µs–1.237ms) | 906.125µs (854.791µs–1.191ms) | -5.0% | noise-sensitive |
| 1000 | random_batch_updates | 5 | 1.333ms (1.012ms–1.784ms) | 983.083µs (818.792µs–1.216ms) | -26.3% | noise-sensitive |
| 1000 | random_conflict_resolved_merge | 5 | 750.084µs (706.667µs–1.962ms) | 456.875µs (419.583µs–513.375µs) | -39.1% | noise-sensitive |
| 1000 | random_delete_diff | 5 | 562.208µs (521.875µs–586.292µs) | 282.792µs (266.625µs–296.917µs) | -49.7% | noise-sensitive |
| 1000 | random_disjoint_sparse_merge | 5 | 503.625µs (467.208µs–587.541µs) | 387.792µs (355.375µs–422.709µs) | -23.0% | noise-sensitive |
| 1000 | random_reads_cold_manager | 5 | 563.229ms (551.439ms–634.797ms) | 423.899ms (412.851ms–481.672ms) | -24.7% | material gain |
| 1000 | random_reads_warm_manager | 5 | 568.258ms (548.843ms–712.925ms) | 414.113ms (412.513ms–482.837ms) | -27.1% | material gain |
| 1000 | random_sparse_diff | 5 | 457.541µs (421.042µs–472.750µs) | 293.875µs (278.625µs–317.542µs) | -35.8% | noise-sensitive |
| 1000 | right_edge_reads_cold_manager | 5 | 580.723ms (553.995ms–773.051ms) | 424.038ms (412.353ms–473.511ms) | -27.0% | material gain |
| 1000 | right_edge_reads_warm_manager | 5 | 569.657ms (548.082ms–669.232ms) | 423.593ms (405.369ms–480.857ms) | -25.6% | material gain |
| 1000 | shuffled_batch_build | 5 | 1.229ms (1.155ms–1.499ms) | 1.135ms (1.072ms–1.383ms) | -7.6% | noise-sensitive |
| 1000 | sorted_stream_build | 5 | 1.206ms (1.003ms–2.251ms) | 1.136ms (917.250µs–1.848ms) | -5.8% | noise-sensitive |
| 10000 | append_batch_upserts | 5 | 1.971ms (1.015ms–7.171ms) | 1.572ms (914.792µs–4.190ms) | -20.3% | noise-sensitive |
| 10000 | append_disjoint_sparse_merge | 5 | 734.958µs (666.750µs–1.041ms) | 530.875µs (465.209µs–919.917µs) | -27.8% | noise-sensitive |
| 10000 | append_sparse_diff | 5 | 208.417µs (205.291µs–593.292µs) | 95.125µs (87.291µs–107.833µs) | -54.4% | noise-sensitive |
| 10000 | clustered_batch_deletes | 5 | 1.612ms (1.052ms–14.033ms) | 1.679ms (1.497ms–5.258ms) | +4.2% | material regression |
| 10000 | clustered_batch_updates | 5 | 1.506ms (1.199ms–9.161ms) | 1.219ms (1.135ms–4.735ms) | -19.1% | material gain |
| 10000 | clustered_conflict_resolved_merge | 5 | 356.792µs (353.000µs–2.533ms) | 169.417µs (160.667µs–218.000µs) | -52.5% | noise-sensitive |
| 10000 | clustered_delete_diff | 5 | 3.492ms (3.372ms–9.688ms) | 374.875µs (356.625µs–556.542µs) | -89.3% | noise-sensitive |
| 10000 | clustered_disjoint_sparse_merge | 5 | 744.166µs (701.459µs–1.205ms) | 432.625µs (377.250µs–706.834µs) | -41.9% | noise-sensitive |
| 10000 | clustered_reads_cold_manager | 5 | 629.306ms (609.024ms–703.162ms) | 425.808ms (411.121ms–489.136ms) | -32.3% | material gain |
| 10000 | clustered_reads_warm_manager | 5 | 631.266ms (603.756ms–698.793ms) | 419.731ms (416.837ms–482.056ms) | -33.5% | material gain |
| 10000 | clustered_sparse_diff | 5 | 318.125µs (290.375µs–339.959µs) | 235.125µs (232.875µs–290.959µs) | -26.1% | noise-sensitive |
| 10000 | identical_diff | 5 | 1.459µs (1.334µs–2.250µs) | 1.542µs (1.500µs–1.583µs) | +5.7% | noise-sensitive |
| 10000 | random_batch_deletes | 5 | 7.365ms (6.820ms–15.619ms) | 5.773ms (5.580ms–12.229ms) | -21.6% | noise-sensitive |
| 10000 | random_batch_updates | 5 | 7.392ms (7.098ms–20.444ms) | 5.294ms (4.974ms–10.511ms) | -28.4% | material gain |
| 10000 | random_conflict_resolved_merge | 5 | 5.907ms (5.598ms–6.124ms) | 2.998ms (2.884ms–3.185ms) | -49.2% | material gain |
| 10000 | random_delete_diff | 5 | 4.133ms (3.965ms–4.807ms) | 2.860ms (2.766ms–3.223ms) | -30.8% | material gain |
| 10000 | random_disjoint_sparse_merge | 5 | 515.666µs (466.875µs–661.875µs) | 644.541µs (628.750µs–694.792µs) | +25.0% | noise-sensitive |
| 10000 | random_reads_cold_manager | 5 | 627.197ms (608.903ms–705.726ms) | 436.090ms (416.686ms–481.249ms) | -30.5% | material gain |
| 10000 | random_reads_warm_manager | 5 | 640.001ms (605.633ms–705.526ms) | 421.372ms (409.928ms–488.688ms) | -34.2% | material gain |
| 10000 | random_sparse_diff | 5 | 4.102ms (3.893ms–4.526ms) | 2.358ms (2.186ms–2.410ms) | -42.5% | material gain |
| 10000 | right_edge_reads_cold_manager | 5 | 643.028ms (605.591ms–747.410ms) | 431.082ms (412.547ms–490.175ms) | -33.0% | material gain |
| 10000 | right_edge_reads_warm_manager | 5 | 626.169ms (604.400ms–709.146ms) | 430.490ms (407.605ms–484.163ms) | -31.3% | material gain |
| 10000 | shuffled_batch_build | 5 | 5.965ms (5.724ms–7.427ms) | 5.897ms (5.690ms–8.146ms) | -1.1% | noise-sensitive |
| 10000 | sorted_stream_build | 5 | 6.255ms (5.922ms–7.623ms) | 5.917ms (5.640ms–7.509ms) | -5.4% | noise-sensitive |
| 50000 | append_batch_upserts | 5 | 4.848ms (4.665ms–13.865ms) | 6.426ms (4.752ms–15.563ms) | +32.6% | noise-sensitive |
| 50000 | append_disjoint_sparse_merge | 5 | 1.494ms (1.266ms–2.619ms) | 1.557ms (1.281ms–1.848ms) | +4.2% | material regression |
| 50000 | append_sparse_diff | 5 | 952.208µs (649.416µs–1.520ms) | 367.750µs (340.292µs–993.083µs) | -61.4% | noise-sensitive |
| 50000 | clustered_batch_deletes | 5 | 6.165ms (4.331ms–13.376ms) | 6.413ms (3.990ms–19.074ms) | +4.0% | noise-sensitive |
| 50000 | clustered_batch_updates | 5 | 6.340ms (5.095ms–14.759ms) | 6.427ms (5.458ms–17.281ms) | +1.4% | noise-sensitive |
| 50000 | clustered_conflict_resolved_merge | 5 | 1.504ms (1.023ms–2.529ms) | 1.039ms (916.416µs–1.745ms) | -31.0% | material gain |
| 50000 | clustered_delete_diff | 5 | 21.693ms (20.416ms–33.485ms) | 477.334µs (357.042µs–683.333µs) | -97.8% | noise-sensitive |
| 50000 | clustered_disjoint_sparse_merge | 5 | 938.083µs (885.333µs–2.260ms) | 1.443ms (1.260ms–2.509ms) | +53.8% | noise-sensitive |
| 50000 | clustered_reads_cold_manager | 5 | 658.946ms (636.560ms–757.819ms) | 441.096ms (421.027ms–514.502ms) | -33.1% | material gain |
| 50000 | clustered_reads_warm_manager | 5 | 654.890ms (633.334ms–729.185ms) | 424.712ms (414.688ms–542.547ms) | -35.1% | material gain |
| 50000 | clustered_sparse_diff | 5 | 685.250µs (667.792µs–795.542µs) | 652.750µs (471.583µs–1.458ms) | -4.7% | noise-sensitive |
| 50000 | identical_diff | 5 | 1.541µs (1.250µs–2.041µs) | 1.792µs (1.541µs–2.125µs) | +16.3% | noise-sensitive |
| 50000 | random_batch_deletes | 5 | 47.013ms (42.308ms–70.001ms) | 37.680ms (35.213ms–67.332ms) | -19.9% | noise-sensitive |
| 50000 | random_batch_updates | 5 | 45.357ms (42.754ms–72.226ms) | 37.008ms (32.108ms–70.031ms) | -18.4% | noise-sensitive |
| 50000 | random_conflict_resolved_merge | 5 | 31.190ms (29.532ms–32.644ms) | 16.616ms (15.324ms–17.178ms) | -46.7% | material gain |
| 50000 | random_delete_diff | 5 | 20.016ms (17.847ms–23.335ms) | 15.281ms (14.164ms–15.806ms) | -23.7% | material gain |
| 50000 | random_disjoint_sparse_merge | 5 | 785.583µs (746.625µs–1.306ms) | 716.000µs (670.500µs–804.333µs) | -8.9% | noise-sensitive |
| 50000 | random_reads_cold_manager | 5 | 701.303ms (682.556ms–795.265ms) | 506.719ms (495.760ms–602.485ms) | -27.7% | material gain |
| 50000 | random_reads_warm_manager | 5 | 670.778ms (645.573ms–748.182ms) | 499.213ms (485.501ms–556.523ms) | -25.6% | material gain |
| 50000 | random_sparse_diff | 5 | 21.595ms (21.050ms–23.712ms) | 11.861ms (11.392ms–12.275ms) | -45.1% | material gain |
| 50000 | right_edge_reads_cold_manager | 5 | 659.815ms (637.366ms–763.564ms) | 445.879ms (420.181ms–544.236ms) | -32.4% | material gain |
| 50000 | right_edge_reads_warm_manager | 5 | 631.040ms (626.126ms–735.460ms) | 428.357ms (412.999ms–507.425ms) | -32.1% | material gain |
| 50000 | shuffled_batch_build | 5 | 31.146ms (26.842ms–37.416ms) | 27.177ms (26.451ms–36.628ms) | -12.7% | noise-sensitive |
| 50000 | sorted_stream_build | 5 | 29.235ms (28.517ms–42.869ms) | 27.459ms (26.540ms–37.838ms) | -6.1% | noise-sensitive |
| 100000 | append_batch_upserts | 5 | 11.439ms (10.215ms–24.238ms) | 8.768ms (8.505ms–14.379ms) | -23.4% | material gain |
| 100000 | append_disjoint_sparse_merge | 5 | 3.876ms (3.609ms–6.883ms) | 2.877ms (2.590ms–4.206ms) | -25.8% | material gain |
| 100000 | append_sparse_diff | 5 | 1.087ms (956.125µs–2.112ms) | 700.208µs (445.417µs–957.250µs) | -35.6% | noise-sensitive |
| 100000 | clustered_batch_deletes | 5 | 8.473ms (7.911ms–18.704ms) | 11.071ms (10.344ms–18.643ms) | +30.7% | noise-sensitive |
| 100000 | clustered_batch_updates | 5 | 13.020ms (11.909ms–27.742ms) | 12.518ms (11.562ms–22.855ms) | -3.9% | noise-sensitive |
| 100000 | clustered_conflict_resolved_merge | 5 | 1.549ms (1.520ms–4.002ms) | 1.265ms (1.188ms–1.696ms) | -18.3% | material gain |
| 100000 | clustered_delete_diff | 5 | 31.219ms (30.265ms–61.496ms) | 659.542µs (573.209µs–914.208µs) | -97.9% | noise-sensitive |
| 100000 | clustered_disjoint_sparse_merge | 5 | 2.394ms (2.063ms–4.966ms) | 2.011ms (1.595ms–3.826ms) | -16.0% | noise-sensitive |
| 100000 | clustered_reads_cold_manager | 5 | 763.643ms (747.368ms–987.539ms) | 454.462ms (433.848ms–590.422ms) | -40.5% | material gain |
| 100000 | clustered_reads_warm_manager | 5 | 764.397ms (725.921ms–854.051ms) | 431.128ms (416.496ms–569.624ms) | -43.6% | material gain |
| 100000 | clustered_sparse_diff | 5 | 1.668ms (1.414ms–2.396ms) | 1.185ms (1.052ms–2.349ms) | -29.0% | noise-sensitive |
| 100000 | identical_diff | 5 | 1.458µs (1.292µs–1.875µs) | 1.833µs (1.333µs–14.125µs) | +25.7% | noise-sensitive |
| 100000 | random_batch_deletes | 5 | 101.924ms (97.960ms–168.418ms) | 91.821ms (84.821ms–150.693ms) | -9.9% | noise-sensitive |
| 100000 | random_batch_updates | 5 | 104.212ms (100.313ms–168.740ms) | 86.258ms (77.603ms–155.982ms) | -17.2% | noise-sensitive |
| 100000 | random_conflict_resolved_merge | 5 | 62.799ms (57.993ms–96.803ms) | 37.050ms (31.832ms–55.663ms) | -41.0% | material gain |
| 100000 | random_delete_diff | 5 | 39.905ms (38.146ms–78.501ms) | 36.118ms (30.662ms–60.282ms) | -9.5% | material gain |
| 100000 | random_disjoint_sparse_merge | 5 | 603.833µs (509.625µs–688.333µs) | 977.417µs (908.500µs–1.174ms) | +61.9% | noise-sensitive |
| 100000 | random_reads_cold_manager | 5 | 863.812ms (799.322ms–1.005s) | 650.065ms (610.361ms–751.800ms) | -24.7% | material gain |
| 100000 | random_reads_warm_manager | 5 | 784.849ms (745.517ms–981.045ms) | 590.475ms (566.713ms–672.796ms) | -24.8% | material gain |
| 100000 | random_sparse_diff | 5 | 42.964ms (41.256ms–47.520ms) | 24.533ms (23.085ms–26.457ms) | -42.9% | material gain |
| 100000 | right_edge_reads_cold_manager | 5 | 786.679ms (747.516ms–983.183ms) | 448.338ms (432.885ms–530.691ms) | -43.0% | material gain |
| 100000 | right_edge_reads_warm_manager | 5 | 748.175ms (730.470ms–871.837ms) | 432.690ms (414.443ms–488.560ms) | -42.2% | material gain |
| 100000 | shuffled_batch_build | 5 | 65.675ms (62.584ms–106.734ms) | 66.814ms (61.468ms–133.161ms) | +1.7% | noise-sensitive |
| 100000 | sorted_stream_build | 5 | 66.568ms (62.801ms–99.292ms) | 63.469ms (59.007ms–91.391ms) | -4.7% | noise-sensitive |
| 1000000 | append_batch_upserts | 5 | 65.470ms (57.194ms–139.407ms) | 70.196ms (66.868ms–148.121ms) | +7.2% | noise-sensitive |
| 1000000 | append_disjoint_sparse_merge | 5 | 12.553ms (10.452ms–20.497ms) | 11.259ms (10.271ms–17.157ms) | -10.3% | noise-sensitive |
| 1000000 | append_sparse_diff | 5 | 10.424ms (10.292ms–20.295ms) | 9.028ms (8.544ms–13.980ms) | -13.4% | material gain |
| 1000000 | clustered_batch_deletes | 5 | 67.208ms (64.257ms–128.348ms) | 85.657ms (74.790ms–149.761ms) | +27.4% | noise-sensitive |
| 1000000 | clustered_batch_updates | 5 | 106.546ms (87.617ms–211.633ms) | 111.295ms (102.925ms–227.363ms) | +4.5% | noise-sensitive |
| 1000000 | clustered_conflict_resolved_merge | 5 | 12.193ms (11.077ms–17.741ms) | 11.838ms (11.445ms–17.380ms) | -2.9% | noise-sensitive |
| 1000000 | clustered_delete_diff | 5 | 215.242ms (207.986ms–430.668ms) | 3.567ms (3.426ms–4.005ms) | -98.3% | material gain |
| 1000000 | clustered_disjoint_sparse_merge | 5 | 7.138ms (5.909ms–12.082ms) | 4.901ms (4.522ms–10.549ms) | -31.3% | noise-sensitive |
| 1000000 | clustered_reads_cold_manager | 5 | 888.557ms (846.279ms–1.035s) | 504.710ms (479.899ms–671.183ms) | -43.2% | material gain |
| 1000000 | clustered_reads_warm_manager | 5 | 818.812ms (789.565ms–950.922ms) | 452.694ms (422.124ms–508.065ms) | -44.7% | material gain |
| 1000000 | clustered_sparse_diff | 5 | 11.857ms (11.531ms–18.581ms) | 11.524ms (9.728ms–16.578ms) | -2.8% | noise-sensitive |
| 1000000 | identical_diff | 5 | 1.334µs (1.292µs–2.458µs) | 2.666µs (1.459µs–287.333µs) | +99.9% | noise-sensitive |
| 1000000 | random_batch_deletes | 5 | 1.247s (1.216s–2.417s) | 1.101s (1.026s–1.882s) | -11.7% | material gain |
| 1000000 | random_batch_updates | 5 | 1.279s (1.221s–2.115s) | 983.987ms (938.262ms–1.806s) | -23.1% | material gain |
| 1000000 | random_conflict_resolved_merge | 5 | 747.742ms (696.899ms–811.471ms) | 438.290ms (414.744ms–468.059ms) | -41.4% | material gain |
| 1000000 | random_delete_diff | 5 | 445.296ms (410.433ms–475.698ms) | 388.620ms (381.666ms–438.450ms) | -12.7% | material gain |
| 1000000 | random_disjoint_sparse_merge | 5 | 1.207ms (1.094ms–1.647ms) | 1.193ms (1.154ms–1.979ms) | -1.1% | noise-sensitive |
| 1000000 | random_reads_cold_manager | 5 | 1.738s (1.649s–2.173s) | 1.605s (1.498s–2.389s) | -7.6% | noise-sensitive |
| 1000000 | random_reads_warm_manager | 5 | 1.231s (1.086s–1.409s) | 1.145s (1.075s–1.694s) | -7.0% | noise-sensitive |
| 1000000 | random_sparse_diff | 5 | 504.699ms (490.348ms–547.412ms) | 337.144ms (319.529ms–506.490ms) | -33.2% | material gain |
| 1000000 | right_edge_reads_cold_manager | 5 | 894.323ms (849.110ms–1.020s) | 513.452ms (468.513ms–592.901ms) | -42.6% | material gain |
| 1000000 | right_edge_reads_warm_manager | 5 | 836.263ms (793.333ms–913.586ms) | 430.616ms (417.182ms–478.537ms) | -48.5% | material gain |
| 1000000 | shuffled_batch_build | 5 | 799.056ms (775.320ms–1.141s) | 828.249ms (782.288ms–1.133s) | +3.7% | noise-sensitive |
| 1000000 | sorted_stream_build | 5 | 760.924ms (704.283ms–978.769ms) | 747.604ms (707.247ms–993.366ms) | -1.8% | noise-sensitive |
| 10000000 | append_batch_upserts | 5 | 141.078ms (98.566ms–931.160ms) | 118.851ms (97.944ms–841.634ms) | -15.8% | noise-sensitive |
| 10000000 | append_disjoint_sparse_merge | 5 | 24.190ms (16.865ms–32.451ms) | 22.348ms (20.099ms–41.908ms) | -7.6% | material gain |
| 10000000 | append_sparse_diff | 5 | 13.015ms (11.631ms–26.805ms) | 13.759ms (12.345ms–23.579ms) | +5.7% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 105.900ms (97.743ms–817.934ms) | 389.257ms (343.137ms–910.081ms) | +267.6% | noise-sensitive |
| 10000000 | clustered_batch_updates | 5 | 211.127ms (170.845ms–399.796ms) | 211.638ms (189.155ms–422.669ms) | +0.2% | noise-sensitive |
| 10000000 | clustered_conflict_resolved_merge | 5 | 19.046ms (18.365ms–32.396ms) | 14.638ms (11.021ms–99.137ms) | -23.1% | material gain |
| 10000000 | clustered_delete_diff | 5 | 311.853ms (273.441ms–2.230s) | 4.625ms (4.326ms–5.268ms) | -98.5% | material gain |
| 10000000 | clustered_disjoint_sparse_merge | 5 | 10.777ms (9.620ms–20.100ms) | 9.414ms (7.097ms–24.898ms) | -12.7% | noise-sensitive |
| 10000000 | clustered_reads_cold_manager | 5 | 1.003s (902.110ms–1.105s) | 549.800ms (521.259ms–669.488ms) | -45.2% | material gain |
| 10000000 | clustered_reads_warm_manager | 5 | 867.310ms (815.540ms–966.769ms) | 437.999ms (422.695ms–445.449ms) | -49.5% | material gain |
| 10000000 | clustered_sparse_diff | 5 | 20.491ms (13.719ms–30.696ms) | 15.044ms (13.418ms–26.078ms) | -26.6% | noise-sensitive |
| 10000000 | identical_diff | 5 | 1.625µs (1.375µs–4.208µs) | 281.833µs (244.583µs–295.333µs) | +17243.6% | noise-sensitive |
| 10000000 | random_batch_deletes | 5 | 10.906s (6.320s–15.135s) | 8.202s (7.424s–15.594s) | -24.8% | noise-sensitive |
| 10000000 | random_batch_updates | 5 | 7.263s (6.405s–13.094s) | 6.188s (5.573s–10.983s) | -14.8% | noise-sensitive |
| 10000000 | random_conflict_resolved_merge | 5 | 1.652s (1.574s–4.031s) | 978.619ms (958.829ms–1.074s) | -40.7% | material gain |
| 10000000 | random_delete_diff | 5 | 3.059s (2.392s–3.811s) | 1.647s (1.565s–2.152s) | -46.2% | material gain |
| 10000000 | random_disjoint_sparse_merge | 5 | 1.769ms (1.457ms–2.338ms) | 688.834µs (633.084µs–701.500µs) | -61.1% | noise-sensitive |
| 10000000 | random_reads_cold_manager | 5 | 6.651s (5.083s–11.123s) | 5.266s (4.870s–8.847s) | -20.8% | noise-sensitive |
| 10000000 | random_reads_warm_manager | 5 | 1.517s (1.428s–2.266s) | 1.473s (1.380s–1.980s) | -2.9% | noise-sensitive |
| 10000000 | random_sparse_diff | 5 | 1.753s (1.726s–3.596s) | 1.246s (1.152s–1.425s) | -28.9% | material gain |
| 10000000 | right_edge_reads_cold_manager | 5 | 1.029s (959.171ms–1.463s) | 586.775ms (530.328ms–1.138s) | -43.0% | material gain |
| 10000000 | right_edge_reads_warm_manager | 5 | 888.048ms (861.492ms–1.212s) | 438.289ms (422.279ms–474.088ms) | -50.6% | material gain |
| 10000000 | shuffled_batch_build | 5 | 10.719s (9.856s–14.266s) | 14.321s (9.886s–16.481s) | +33.6% | noise-sensitive |
| 10000000 | sorted_stream_build | 5 | 9.586s (8.775s–11.323s) | 10.903s (8.760s–14.572s) | +13.7% | material regression |

### WAL+NORMAL

| Records | Workload | Runs | Original median (range) | Current median (range) | Delta | Classification |
|---:|---|---:|---:|---:|---:|---|
| 1000 | append_batch_upserts | 5 | 1.061ms (588.083µs–1.464ms) | 769.542µs (442.166µs–1.369ms) | -27.5% | noise-sensitive |
| 1000 | append_disjoint_sparse_merge | 5 | 616.541µs (588.084µs–764.292µs) | 324.250µs (318.583µs–442.041µs) | -47.4% | noise-sensitive |
| 1000 | append_sparse_diff | 5 | 140.542µs (127.667µs–161.583µs) | 64.375µs (63.583µs–81.417µs) | -54.2% | noise-sensitive |
| 1000 | clustered_batch_deletes | 5 | 452.833µs (389.083µs–575.500µs) | 543.250µs (508.375µs–883.959µs) | +20.0% | noise-sensitive |
| 1000 | clustered_batch_updates | 5 | 611.500µs (513.750µs–803.750µs) | 447.750µs (398.833µs–742.000µs) | -26.8% | noise-sensitive |
| 1000 | clustered_conflict_resolved_merge | 5 | 244.042µs (240.625µs–259.250µs) | 107.750µs (104.500µs–120.375µs) | -55.8% | noise-sensitive |
| 1000 | clustered_delete_diff | 5 | 141.333µs (130.333µs–153.250µs) | 66.625µs (59.875µs–78.792µs) | -52.9% | noise-sensitive |
| 1000 | clustered_disjoint_sparse_merge | 5 | 442.083µs (416.959µs–567.583µs) | 308.292µs (285.250µs–408.000µs) | -30.3% | noise-sensitive |
| 1000 | clustered_reads_cold_manager | 5 | 555.583ms (548.702ms–642.392ms) | 414.551ms (404.681ms–481.983ms) | -25.4% | material gain |
| 1000 | clustered_reads_warm_manager | 5 | 558.254ms (549.355ms–646.551ms) | 407.896ms (405.343ms–472.251ms) | -26.9% | material gain |
| 1000 | clustered_sparse_diff | 5 | 156.584µs (148.500µs–166.542µs) | 73.791µs (72.542µs–86.250µs) | -52.9% | noise-sensitive |
| 1000 | identical_diff | 5 | 1.458µs (1.417µs–1.750µs) | 1.458µs (1.375µs–1.750µs) | +0.0% | noise-sensitive |
| 1000 | random_batch_deletes | 5 | 919.209µs (847.625µs–1.276ms) | 881.292µs (809.000µs–1.159ms) | -4.1% | noise-sensitive |
| 1000 | random_batch_updates | 5 | 962.208µs (934.834µs–1.166ms) | 821.583µs (758.833µs–1.140ms) | -14.6% | noise-sensitive |
| 1000 | random_conflict_resolved_merge | 5 | 743.417µs (699.708µs–834.125µs) | 441.709µs (425.750µs–472.500µs) | -40.6% | noise-sensitive |
| 1000 | random_delete_diff | 5 | 541.167µs (520.750µs–593.292µs) | 272.750µs (263.125µs–297.875µs) | -49.6% | noise-sensitive |
| 1000 | random_disjoint_sparse_merge | 5 | 463.959µs (425.500µs–636.167µs) | 305.292µs (293.333µs–393.542µs) | -34.2% | noise-sensitive |
| 1000 | random_reads_cold_manager | 5 | 549.008ms (547.257ms–644.337ms) | 419.965ms (406.554ms–477.659ms) | -23.5% | material gain |
| 1000 | random_reads_warm_manager | 5 | 554.520ms (548.752ms–637.077ms) | 417.726ms (405.313ms–475.004ms) | -24.7% | material gain |
| 1000 | random_sparse_diff | 5 | 441.208µs (420.750µs–496.834µs) | 281.084µs (270.000µs–301.542µs) | -36.3% | noise-sensitive |
| 1000 | right_edge_reads_cold_manager | 5 | 557.132ms (552.671ms–644.129ms) | 407.424ms (405.066ms–473.145ms) | -26.9% | material gain |
| 1000 | right_edge_reads_warm_manager | 5 | 581.789ms (550.736ms–658.498ms) | 413.644ms (409.519ms–483.741ms) | -28.9% | material gain |
| 1000 | shuffled_batch_build | 5 | 1.093ms (990.125µs–1.343ms) | 913.416µs (896.750µs–1.303ms) | -16.4% | noise-sensitive |
| 1000 | sorted_stream_build | 5 | 1.014ms (858.542µs–2.101ms) | 832.875µs (763.584µs–1.063ms) | -17.8% | noise-sensitive |
| 10000 | append_batch_upserts | 5 | 1.831ms (1.157ms–4.002ms) | 1.776ms (816.625µs–5.278ms) | -3.0% | noise-sensitive |
| 10000 | append_disjoint_sparse_merge | 5 | 708.291µs (631.125µs–996.083µs) | 628.292µs (451.208µs–690.375µs) | -11.3% | noise-sensitive |
| 10000 | append_sparse_diff | 5 | 214.208µs (210.959µs–248.000µs) | 89.709µs (88.834µs–261.209µs) | -58.1% | noise-sensitive |
| 10000 | clustered_batch_deletes | 5 | 1.331ms (1.008ms–3.735ms) | 1.553ms (1.487ms–6.494ms) | +16.7% | material regression |
| 10000 | clustered_batch_updates | 5 | 1.396ms (1.038ms–3.453ms) | 1.370ms (960.750µs–5.667ms) | -1.9% | noise-sensitive |
| 10000 | clustered_conflict_resolved_merge | 5 | 354.375µs (346.042µs–414.666µs) | 161.875µs (155.625µs–523.792µs) | -54.3% | noise-sensitive |
| 10000 | clustered_delete_diff | 5 | 3.455ms (3.316ms–3.846ms) | 380.125µs (358.833µs–540.667µs) | -89.0% | noise-sensitive |
| 10000 | clustered_disjoint_sparse_merge | 5 | 748.042µs (652.666µs–1.189ms) | 384.125µs (344.750µs–582.375µs) | -48.6% | noise-sensitive |
| 10000 | clustered_reads_cold_manager | 5 | 647.684ms (612.736ms–820.630ms) | 420.803ms (419.077ms–630.646ms) | -35.0% | material gain |
| 10000 | clustered_reads_warm_manager | 5 | 667.847ms (608.197ms–911.883ms) | 418.802ms (410.011ms–613.766ms) | -37.3% | material gain |
| 10000 | clustered_sparse_diff | 5 | 303.708µs (291.292µs–331.125µs) | 277.458µs (239.167µs–387.375µs) | -8.6% | noise-sensitive |
| 10000 | identical_diff | 5 | 1.667µs (1.334µs–4.417µs) | 1.542µs (1.334µs–3.958µs) | -7.5% | noise-sensitive |
| 10000 | random_batch_deletes | 5 | 7.090ms (6.712ms–19.777ms) | 5.488ms (5.316ms–11.704ms) | -22.6% | material gain |
| 10000 | random_batch_updates | 5 | 7.026ms (6.839ms–18.413ms) | 6.092ms (4.756ms–10.367ms) | -13.3% | material gain |
| 10000 | random_conflict_resolved_merge | 5 | 5.870ms (5.612ms–6.140ms) | 2.925ms (2.863ms–7.390ms) | -50.2% | material gain |
| 10000 | random_delete_diff | 5 | 3.987ms (3.890ms–4.214ms) | 2.981ms (2.725ms–3.157ms) | -25.2% | material gain |
| 10000 | random_disjoint_sparse_merge | 5 | 497.917µs (450.084µs–563.333µs) | 622.625µs (558.417µs–781.500µs) | +25.0% | noise-sensitive |
| 10000 | random_reads_cold_manager | 5 | 679.011ms (607.233ms–824.755ms) | 424.347ms (414.319ms–564.797ms) | -37.5% | material gain |
| 10000 | random_reads_warm_manager | 5 | 663.062ms (602.088ms–834.515ms) | 418.262ms (408.909ms–573.792ms) | -36.9% | material gain |
| 10000 | random_sparse_diff | 5 | 3.982ms (3.869ms–4.158ms) | 2.364ms (2.190ms–5.888ms) | -40.6% | material gain |
| 10000 | right_edge_reads_cold_manager | 5 | 701.591ms (615.923ms–830.861ms) | 427.725ms (411.095ms–540.638ms) | -39.0% | material gain |
| 10000 | right_edge_reads_warm_manager | 5 | 707.694ms (600.945ms–745.543ms) | 422.218ms (417.926ms–558.546ms) | -40.3% | material gain |
| 10000 | shuffled_batch_build | 5 | 6.148ms (5.485ms–7.304ms) | 5.692ms (5.422ms–7.774ms) | -7.4% | noise-sensitive |
| 10000 | sorted_stream_build | 5 | 6.171ms (5.709ms–10.137ms) | 5.585ms (5.324ms–7.257ms) | -9.5% | material gain |
| 50000 | append_batch_upserts | 5 | 5.774ms (3.988ms–12.894ms) | 6.840ms (5.340ms–45.783ms) | +18.5% | material regression |
| 50000 | append_disjoint_sparse_merge | 5 | 1.396ms (1.000ms–2.271ms) | 1.290ms (1.007ms–2.761ms) | -7.6% | noise-sensitive |
| 50000 | append_sparse_diff | 5 | 844.250µs (466.833µs–1.317ms) | 572.500µs (317.750µs–742.167µs) | -32.2% | noise-sensitive |
| 50000 | clustered_batch_deletes | 5 | 4.699ms (3.876ms–11.981ms) | 7.835ms (5.655ms–18.356ms) | +66.7% | material regression |
| 50000 | clustered_batch_updates | 5 | 5.907ms (4.865ms–13.572ms) | 6.353ms (5.523ms–19.697ms) | +7.6% | noise-sensitive |
| 50000 | clustered_conflict_resolved_merge | 5 | 1.504ms (1.082ms–2.060ms) | 925.792µs (746.000µs–3.080ms) | -38.5% | noise-sensitive |
| 50000 | clustered_delete_diff | 5 | 19.833ms (17.611ms–32.657ms) | 445.875µs (359.541µs–491.000µs) | -97.8% | noise-sensitive |
| 50000 | clustered_disjoint_sparse_merge | 5 | 1.073ms (675.458µs–1.836ms) | 1.627ms (1.285ms–2.384ms) | +51.7% | material regression |
| 50000 | clustered_reads_cold_manager | 5 | 670.979ms (638.575ms–753.027ms) | 456.401ms (423.922ms–548.407ms) | -32.0% | material gain |
| 50000 | clustered_reads_warm_manager | 5 | 647.956ms (626.555ms–786.318ms) | 431.709ms (410.215ms–595.171ms) | -33.4% | material gain |
| 50000 | clustered_sparse_diff | 5 | 710.625µs (565.750µs–824.875µs) | 887.208µs (517.417µs–4.655ms) | +24.8% | noise-sensitive |
| 50000 | identical_diff | 5 | 1.625µs (1.333µs–4.583µs) | 1.584µs (1.333µs–2.459µs) | -2.5% | noise-sensitive |
| 50000 | random_batch_deletes | 5 | 42.768ms (41.102ms–71.187ms) | 41.098ms (34.807ms–90.657ms) | -3.9% | noise-sensitive |
| 50000 | random_batch_updates | 5 | 43.533ms (41.695ms–69.542ms) | 38.893ms (32.799ms–121.042ms) | -10.7% | material gain |
| 50000 | random_conflict_resolved_merge | 5 | 30.714ms (29.572ms–33.481ms) | 16.139ms (15.255ms–21.303ms) | -47.5% | material gain |
| 50000 | random_delete_diff | 5 | 19.411ms (17.587ms–21.737ms) | 14.918ms (14.069ms–22.213ms) | -23.1% | noise-sensitive |
| 50000 | random_disjoint_sparse_merge | 5 | 777.292µs (682.958µs–1.002ms) | 642.792µs (600.834µs–7.119ms) | -17.3% | noise-sensitive |
| 50000 | random_reads_cold_manager | 5 | 715.106ms (664.138ms–799.070ms) | 530.909ms (493.345ms–599.479ms) | -25.8% | material gain |
| 50000 | random_reads_warm_manager | 5 | 680.446ms (642.317ms–755.143ms) | 505.183ms (485.997ms–602.339ms) | -25.8% | material gain |
| 50000 | random_sparse_diff | 5 | 21.480ms (21.323ms–23.264ms) | 11.766ms (10.855ms–20.387ms) | -45.2% | material gain |
| 50000 | right_edge_reads_cold_manager | 5 | 663.088ms (636.537ms–757.319ms) | 445.210ms (418.244ms–743.995ms) | -32.9% | material gain |
| 50000 | right_edge_reads_warm_manager | 5 | 649.490ms (622.462ms–734.643ms) | 428.267ms (408.193ms–646.001ms) | -34.1% | material gain |
| 50000 | shuffled_batch_build | 5 | 27.886ms (26.447ms–39.081ms) | 26.954ms (25.992ms–45.062ms) | -3.3% | noise-sensitive |
| 50000 | sorted_stream_build | 5 | 29.848ms (27.869ms–43.917ms) | 27.238ms (26.643ms–42.043ms) | -8.7% | noise-sensitive |
| 100000 | append_batch_upserts | 5 | 11.167ms (9.634ms–28.191ms) | 8.890ms (7.957ms–26.391ms) | -20.4% | noise-sensitive |
| 100000 | append_disjoint_sparse_merge | 5 | 3.794ms (3.707ms–6.738ms) | 2.777ms (2.648ms–6.809ms) | -26.8% | noise-sensitive |
| 100000 | append_sparse_diff | 5 | 1.088ms (941.459µs–2.203ms) | 766.417µs (534.000µs–1.359ms) | -29.6% | noise-sensitive |
| 100000 | clustered_batch_deletes | 5 | 8.431ms (7.729ms–20.380ms) | 11.014ms (10.904ms–31.231ms) | +30.6% | material regression |
| 100000 | clustered_batch_updates | 5 | 13.619ms (10.642ms–28.365ms) | 13.308ms (12.115ms–36.690ms) | -2.3% | noise-sensitive |
| 100000 | clustered_conflict_resolved_merge | 5 | 1.549ms (1.321ms–2.479ms) | 1.286ms (1.184ms–1.692ms) | -17.0% | material gain |
| 100000 | clustered_delete_diff | 5 | 33.309ms (28.881ms–60.776ms) | 671.917µs (610.958µs–742.708µs) | -98.0% | noise-sensitive |
| 100000 | clustered_disjoint_sparse_merge | 5 | 2.219ms (1.752ms–4.748ms) | 2.149ms (1.612ms–3.845ms) | -3.1% | noise-sensitive |
| 100000 | clustered_reads_cold_manager | 5 | 777.821ms (749.857ms–945.476ms) | 442.563ms (434.846ms–533.069ms) | -43.1% | material gain |
| 100000 | clustered_reads_warm_manager | 5 | 766.979ms (727.102ms–856.259ms) | 424.657ms (419.757ms–482.198ms) | -44.6% | material gain |
| 100000 | clustered_sparse_diff | 5 | 1.543ms (1.305ms–4.632ms) | 1.248ms (1.106ms–1.740ms) | -19.1% | material gain |
| 100000 | identical_diff | 5 | 1.292µs (1.250µs–1.709µs) | 1.542µs (1.375µs–1.958µs) | +19.3% | noise-sensitive |
| 100000 | random_batch_deletes | 5 | 104.764ms (97.785ms–170.624ms) | 91.157ms (85.577ms–171.697ms) | -13.0% | noise-sensitive |
| 100000 | random_batch_updates | 5 | 103.348ms (98.646ms–186.567ms) | 84.385ms (78.818ms–173.101ms) | -18.3% | noise-sensitive |
| 100000 | random_conflict_resolved_merge | 5 | 60.824ms (58.121ms–67.234ms) | 33.766ms (31.951ms–40.353ms) | -44.5% | material gain |
| 100000 | random_delete_diff | 5 | 39.634ms (37.623ms–72.143ms) | 33.023ms (30.958ms–35.397ms) | -16.7% | material gain |
| 100000 | random_disjoint_sparse_merge | 5 | 533.666µs (477.875µs–638.042µs) | 865.375µs (835.625µs–1.852ms) | +62.2% | noise-sensitive |
| 100000 | random_reads_cold_manager | 5 | 842.817ms (806.179ms–971.898ms) | 635.139ms (599.270ms–1.125s) | -24.6% | material gain |
| 100000 | random_reads_warm_manager | 5 | 792.582ms (757.921ms–1.038s) | 592.127ms (566.080ms–660.348ms) | -25.3% | material gain |
| 100000 | random_sparse_diff | 5 | 45.152ms (41.149ms–51.174ms) | 25.027ms (23.196ms–26.877ms) | -44.6% | material gain |
| 100000 | right_edge_reads_cold_manager | 5 | 793.318ms (753.333ms–893.176ms) | 445.279ms (443.629ms–539.598ms) | -43.9% | material gain |
| 100000 | right_edge_reads_warm_manager | 5 | 766.181ms (732.653ms–849.952ms) | 422.037ms (419.898ms–513.877ms) | -44.9% | material gain |
| 100000 | shuffled_batch_build | 5 | 68.743ms (60.198ms–98.226ms) | 65.776ms (61.305ms–98.817ms) | -4.3% | noise-sensitive |
| 100000 | sorted_stream_build | 5 | 67.162ms (63.062ms–114.436ms) | 62.949ms (58.583ms–107.433ms) | -6.3% | noise-sensitive |
| 1000000 | append_batch_upserts | 5 | 65.905ms (62.187ms–135.764ms) | 69.882ms (66.290ms–147.548ms) | +6.0% | noise-sensitive |
| 1000000 | append_disjoint_sparse_merge | 5 | 11.616ms (10.305ms–21.012ms) | 11.307ms (10.096ms–17.648ms) | -2.7% | noise-sensitive |
| 1000000 | append_sparse_diff | 5 | 10.770ms (10.119ms–20.363ms) | 9.330ms (8.527ms–18.482ms) | -13.4% | noise-sensitive |
| 1000000 | clustered_batch_deletes | 5 | 64.427ms (61.701ms–136.944ms) | 79.956ms (75.340ms–159.692ms) | +24.1% | noise-sensitive |
| 1000000 | clustered_batch_updates | 5 | 106.852ms (102.494ms–227.471ms) | 106.982ms (103.453ms–235.402ms) | +0.1% | noise-sensitive |
| 1000000 | clustered_conflict_resolved_merge | 5 | 12.579ms (11.722ms–26.201ms) | 11.708ms (11.529ms–21.872ms) | -6.9% | noise-sensitive |
| 1000000 | clustered_delete_diff | 5 | 213.637ms (206.283ms–1.058s) | 3.344ms (3.238ms–4.218ms) | -98.4% | material gain |
| 1000000 | clustered_disjoint_sparse_merge | 5 | 8.470ms (6.405ms–13.133ms) | 4.643ms (4.377ms–45.436ms) | -45.2% | material gain |
| 1000000 | clustered_reads_cold_manager | 5 | 953.005ms (846.610ms–1.236s) | 500.867ms (474.610ms–608.282ms) | -47.4% | material gain |
| 1000000 | clustered_reads_warm_manager | 5 | 838.306ms (790.393ms–937.959ms) | 445.923ms (417.343ms–505.216ms) | -46.8% | material gain |
| 1000000 | clustered_sparse_diff | 5 | 12.091ms (11.491ms–18.314ms) | 10.173ms (10.058ms–15.805ms) | -15.9% | noise-sensitive |
| 1000000 | identical_diff | 5 | 1.500µs (1.291µs–2.209µs) | 1.750µs (1.375µs–19.458µs) | +16.7% | noise-sensitive |
| 1000000 | random_batch_deletes | 5 | 1.234s (1.199s–2.188s) | 1.012s (1.001s–1.835s) | -17.9% | noise-sensitive |
| 1000000 | random_batch_updates | 5 | 1.255s (1.204s–2.306s) | 987.331ms (942.166ms–1.706s) | -21.3% | material gain |
| 1000000 | random_conflict_resolved_merge | 5 | 714.171ms (693.563ms–734.116ms) | 424.281ms (409.643ms–439.906ms) | -40.6% | material gain |
| 1000000 | random_delete_diff | 5 | 431.020ms (409.244ms–445.998ms) | 383.043ms (377.315ms–417.598ms) | -11.1% | material gain |
| 1000000 | random_disjoint_sparse_merge | 5 | 1.109ms (1.067ms–1.426ms) | 1.110ms (1.074ms–1.390ms) | +0.1% | noise-sensitive |
| 1000000 | random_reads_cold_manager | 5 | 1.761s (1.651s–2.342s) | 1.631s (1.517s–2.352s) | -7.4% | noise-sensitive |
| 1000000 | random_reads_warm_manager | 5 | 1.210s (1.087s–1.590s) | 1.089s (1.057s–1.222s) | -9.9% | material gain |
| 1000000 | random_sparse_diff | 5 | 501.296ms (495.390ms–534.340ms) | 327.148ms (316.073ms–339.447ms) | -34.7% | material gain |
| 1000000 | right_edge_reads_cold_manager | 5 | 892.972ms (870.062ms–965.820ms) | 499.831ms (468.598ms–1.015s) | -44.0% | material gain |
| 1000000 | right_edge_reads_warm_manager | 5 | 847.110ms (793.648ms–933.380ms) | 441.362ms (424.300ms–466.728ms) | -47.9% | material gain |
| 1000000 | shuffled_batch_build | 5 | 837.914ms (754.691ms–1.199s) | 842.469ms (765.397ms–1.405s) | +0.5% | noise-sensitive |
| 1000000 | sorted_stream_build | 5 | 788.645ms (715.943ms–953.758ms) | 711.353ms (685.109ms–1.235s) | -9.8% | material gain |
| 10000000 | append_batch_upserts | 5 | 117.003ms (110.967ms–253.052ms) | 111.154ms (107.288ms–281.255ms) | -5.0% | noise-sensitive |
| 10000000 | append_disjoint_sparse_merge | 5 | 19.795ms (16.891ms–34.005ms) | 21.168ms (20.407ms–45.556ms) | +6.9% | material regression |
| 10000000 | append_sparse_diff | 5 | 12.566ms (11.926ms–27.905ms) | 12.634ms (11.718ms–28.099ms) | +0.5% | noise-sensitive |
| 10000000 | clustered_batch_deletes | 5 | 102.107ms (94.449ms–226.927ms) | 340.643ms (331.723ms–821.056ms) | +233.6% | material regression |
| 10000000 | clustered_batch_updates | 5 | 177.066ms (167.916ms–1.839s) | 199.932ms (190.333ms–481.052ms) | +12.9% | material regression |
| 10000000 | clustered_conflict_resolved_merge | 5 | 19.505ms (17.944ms–144.596ms) | 24.344ms (11.226ms–82.914ms) | +24.8% | material regression |
| 10000000 | clustered_delete_diff | 5 | 303.812ms (280.128ms–658.781ms) | 4.907ms (4.060ms–5.430ms) | -98.4% | material gain |
| 10000000 | clustered_disjoint_sparse_merge | 5 | 10.079ms (9.496ms–20.703ms) | 7.044ms (6.693ms–73.197ms) | -30.1% | material gain |
| 10000000 | clustered_reads_cold_manager | 5 | 912.561ms (900.530ms–1.046s) | 541.476ms (516.960ms–693.981ms) | -40.7% | material gain |
| 10000000 | clustered_reads_warm_manager | 5 | 819.511ms (812.658ms–1.268s) | 423.423ms (420.764ms–452.484ms) | -48.3% | material gain |
| 10000000 | clustered_sparse_diff | 5 | 16.449ms (14.958ms–25.204ms) | 14.089ms (12.766ms–26.612ms) | -14.3% | noise-sensitive |
| 10000000 | identical_diff | 5 | 1.500µs (1.333µs–2.209µs) | 168.375µs (1.417µs–292.666µs) | +11125.0% | noise-sensitive |
| 10000000 | random_batch_deletes | 5 | 6.903s (6.455s–12.455s) | 7.740s (7.565s–15.185s) | +12.1% | noise-sensitive |
| 10000000 | random_batch_updates | 5 | 6.873s (6.483s–16.141s) | 5.609s (5.368s–12.776s) | -18.4% | noise-sensitive |
| 10000000 | random_conflict_resolved_merge | 5 | 1.655s (1.573s–7.208s) | 962.904ms (943.501ms–1.867s) | -41.8% | material gain |
| 10000000 | random_delete_diff | 5 | 2.462s (2.350s–4.716s) | 1.677s (1.520s–2.168s) | -31.9% | material gain |
| 10000000 | random_disjoint_sparse_merge | 5 | 2.321ms (1.272ms–6.067ms) | 604.000µs (580.542µs–2.649ms) | -74.0% | noise-sensitive |
| 10000000 | random_reads_cold_manager | 5 | 5.209s (5.014s–9.659s) | 5.605s (4.736s–10.060s) | +7.6% | noise-sensitive |
| 10000000 | random_reads_warm_manager | 5 | 1.505s (1.325s–2.559s) | 1.475s (1.370s–1.627s) | -2.0% | noise-sensitive |
| 10000000 | random_sparse_diff | 5 | 1.710s (1.608s–1.965s) | 1.157s (1.085s–1.266s) | -32.3% | material gain |
| 10000000 | right_edge_reads_cold_manager | 5 | 927.052ms (923.877ms–1.724s) | 522.722ms (517.628ms–703.397ms) | -43.6% | material gain |
| 10000000 | right_edge_reads_warm_manager | 5 | 875.366ms (845.572ms–979.357ms) | 426.869ms (425.940ms–461.132ms) | -51.2% | material gain |
| 10000000 | shuffled_batch_build | 5 | 11.165s (9.792s–13.753s) | 10.613s (10.020s–32.653s) | -4.9% | material gain |
| 10000000 | sorted_stream_build | 5 | 8.916s (8.705s–12.279s) | 9.136s (8.797s–12.999s) | +2.5% | noise-sensitive |

## Structural, storage, memory, and I/O matrix

| Profile | Records | Workload | RSS O→C | Fixture O→C | Nodes read O→C | Nodes written O→C | Bytes read O→C | Bytes written O→C | Tree bytes O→C | Height O→C | Flags |
|---|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| full | 1000 | append_batch_upserts | 4.31MiB→4.20MiB | 68.0KiB→84.0KiB | 2→2 | 2→2 | 9151→3573 | 13263→7687 | 45573→46200 | 1→1 | — |
| full | 1000 | append_disjoint_sparse_merge | 4.75MiB→4.42MiB | 100.0KiB→100.0KiB | 6→6 | 2→2 | 31567→14833 | 13263→7687 | 45573→46200 | 1→1 | — |
| full | 1000 | append_sparse_diff | 4.39MiB→4.25MiB | 100.0KiB→116.0KiB | 4→4 | 0→0 | 22414→11260 | 0→0 | 45573→46200 | 1→1 | — |
| full | 1000 | clustered_batch_deletes | 4.17MiB→4.20MiB | 64.0KiB→80.0KiB | 2→4 | 2→2 | 12523→23773 | 8413→3491 | 37351→37974 | 1→1 | I/O regression |
| full | 1000 | clustered_batch_updates | 4.22MiB→4.12MiB | 68.0KiB→84.0KiB | 3→3 | 2→2 | 21484→10814 | 12523→7603 | 41461→42086 | 1→1 | — |
| full | 1000 | clustered_conflict_resolved_merge | 4.39MiB→4.34MiB | 88.0KiB→92.0KiB | 6→6 | 0→0 | 37569→22809 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | clustered_delete_diff | 4.22MiB→4.28MiB | 96.0KiB→112.0KiB | 4→4 | 0→0 | 20936→11094 | 0→0 | 37351→37974 | 1→1 | — |
| full | 1000 | clustered_disjoint_sparse_merge | 4.44MiB→4.31MiB | 100.0KiB→100.0KiB | 6→6 | 2→2 | 37569→22809 | 12523→7603 | 41461→42086 | 1→1 | — |
| full | 1000 | clustered_reads_cold_manager | 3.80MiB→3.81MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | clustered_reads_warm_manager | 3.81MiB→3.81MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | clustered_sparse_diff | 4.33MiB→4.20MiB | 100.0KiB→116.0KiB | 4→4 | 0→0 | 25046→15206 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | identical_diff | 3.83MiB→3.84MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | random_batch_deletes | 4.62MiB→4.69MiB | 104.0KiB→116.0KiB | 5→8 | 5→8 | 41461→42086 | 37361→37986 | 37361→37986 | 1→1 | I/O regression |
| full | 1000 | random_batch_updates | 4.58MiB→4.45MiB | 104.0KiB→128.0KiB | 5→8 | 5→8 | 41461→42086 | 41461→42086 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | random_conflict_resolved_merge | 4.95MiB→5.12MiB | 148.0KiB→184.0KiB | 15→24 | 0→0 | 124383→126258 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | random_delete_diff | 4.94MiB→4.92MiB | 136.0KiB→148.0KiB | 10→16 | 0→0 | 78822→80072 | 0→0 | 37361→37986 | 1→1 | I/O regression |
| full | 1000 | random_disjoint_sparse_merge | 4.72MiB→4.70MiB | 128.0KiB→148.0KiB | 6→6 | 2→2 | 37569→22809 | 12523→7603 | 41461→42086 | 1→1 | — |
| full | 1000 | random_reads_cold_manager | 3.86MiB→3.86MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | random_reads_warm_manager | 3.86MiB→3.86MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | random_sparse_diff | 4.72MiB→4.73MiB | 136.0KiB→160.0KiB | 10→16 | 0→0 | 82922→84172 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | right_edge_reads_cold_manager | 3.81MiB→3.84MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| full | 1000 | right_edge_reads_warm_manager | 3.83MiB→3.84MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | shuffled_batch_build | 4.91MiB→4.97MiB | 56.0KiB→68.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 1000 | sorted_stream_build | 4.62MiB→4.69MiB | 56.0KiB→68.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| full | 10000 | append_batch_upserts | 7.59MiB→6.55MiB | 488.0KiB→488.0KiB | 2→2 | 3→2 | 6995→5718 | 11188→9833 | 418414→420190 | 1→1 | — |
| full | 10000 | append_disjoint_sparse_merge | 7.69MiB→6.88MiB | 512.0KiB→524.0KiB | 6→6 | 2→2 | 25101→21274 | 11108→9833 | 418334→420190 | 1→1 | — |
| full | 10000 | append_sparse_diff | 7.19MiB→6.55MiB | 520.0KiB→520.0KiB | 5→4 | 0→0 | 18183→15551 | 0→0 | 418414→420190 | 1→1 | — |
| full | 10000 | clustered_batch_deletes | 7.41MiB→7.03MiB | 500.0KiB→508.0KiB | 3→7 | 3→4 | 23564→67048 | 19451→30238 | 410108→411838 | 1→1 | I/O regression |
| full | 10000 | clustered_batch_updates | 7.48MiB→6.80MiB | 500.0KiB→508.0KiB | 4→5 | 3→4 | 29031→33569 | 23564→29454 | 414221→416075 | 1→1 | I/O regression |
| full | 10000 | clustered_conflict_resolved_merge | 7.62MiB→6.70MiB | 516.0KiB→492.0KiB | 6→6 | 0→0 | 46026→14079 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | clustered_delete_diff | 9.66MiB→7.22MiB | 532.0KiB→540.0KiB | 43→9 | 0→0 | 433672→64713 | 0→0 | 410108→411838 | 1→1 | — |
| full | 10000 | clustered_disjoint_sparse_merge | 7.69MiB→7.00MiB | 544.0KiB→536.0KiB | 6→6 | 2→2 | 46026→14079 | 15342→4693 | 414221→416075 | 1→1 | — |
| full | 10000 | clustered_reads_cold_manager | 5.61MiB→5.42MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | clustered_reads_warm_manager | 5.59MiB→5.39MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | clustered_sparse_diff | 7.23MiB→6.80MiB | 532.0KiB→540.0KiB | 6→8 | 0→0 | 47128→58908 | 0→0 | 414221→416075 | 1→1 | I/O regression |
| full | 10000 | identical_diff | 6.78MiB→6.22MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | random_batch_deletes | 9.95MiB→9.61MiB | 900.0KiB→916.0KiB | 35→40 | 35→36 | 381623→417677 | 377523→397649 | 410121→411972 | 1→1 | I/O regression |
| full | 10000 | random_batch_updates | 10.08MiB→9.81MiB | 900.0KiB→904.0KiB | 35→35 | 35→35 | 381623→390152 | 381623→390152 | 414221→416075 | 1→1 | — |
| full | 10000 | random_conflict_resolved_merge | 11.98MiB→11.36MiB | 1.17MiB→1.22MiB | 84→93 | 0→0 | 987885→1057785 | 0→0 | 414221→416075 | 1→1 | I/O regression |
| full | 10000 | random_delete_diff | 10.41MiB→9.89MiB | 932.0KiB→948.0KiB | 70→72 | 0→0 | 759146→799401 | 0→0 | 410121→411972 | 1→1 | I/O regression |
| full | 10000 | random_disjoint_sparse_merge | 8.70MiB→8.33MiB | 920.0KiB→956.0KiB | 6→6 | 2→2 | 29250→68220 | 9750→22740 | 414221→416075 | 1→1 | I/O regression |
| full | 10000 | random_reads_cold_manager | 5.78MiB→5.56MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | random_reads_warm_manager | 5.80MiB→5.59MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | random_sparse_diff | 10.56MiB→9.80MiB | 932.0KiB→936.0KiB | 70→70 | 0→0 | 763246→780304 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | right_edge_reads_cold_manager | 5.61MiB→5.38MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | right_edge_reads_warm_manager | 5.58MiB→5.39MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | shuffled_batch_build | 10.16MiB→9.98MiB | 472.0KiB→472.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 10000 | sorted_stream_build | 8.16MiB→7.64MiB | 472.0KiB→472.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| full | 50000 | append_batch_upserts | 16.09MiB→15.17MiB | 2.30MiB→2.31MiB | 2→2 | 6→5 | 8594→17614 | 29466→38557 | 2092074→2100520 | 1→1 | I/O regression |
| full | 50000 | append_disjoint_sparse_merge | 16.33MiB→15.02MiB | 2.32MiB→2.34MiB | 7→7 | 3→3 | 37988→59058 | 20795→23825 | 2091996→2100520 | 1→1 | I/O regression |
| full | 50000 | append_sparse_diff | 15.70MiB→14.64MiB | 2.33MiB→2.34MiB | 8→7 | 0→0 | 38060→56171 | 0→0 | 2092074→2100520 | 1→1 | I/O regression |
| full | 50000 | clustered_batch_deletes | 15.77MiB→14.59MiB | 2.28MiB→2.29MiB | 4→6 | 3→2 | 39304→70384 | 18668→18360 | 2050566→2058769 | 1→1 | I/O regression |
| full | 50000 | clustered_batch_updates | 15.98MiB→14.72MiB | 2.30MiB→2.31MiB | 5→5 | 4→4 | 40289→49246 | 39304→39168 | 2071202→2079577 | 1→1 | I/O regression |
| full | 50000 | clustered_conflict_resolved_merge | 16.28MiB→14.73MiB | 2.34MiB→2.35MiB | 12→9 | 0→0 | 117912→101949 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | clustered_delete_diff | 29.17MiB→14.50MiB | 2.31MiB→2.32MiB | 202→6 | 0→0 | 2089870→57528 | 0→0 | 2050566→2058769 | 1→1 | — |
| full | 50000 | clustered_disjoint_sparse_merge | 16.03MiB→14.92MiB | 2.33MiB→2.35MiB | 6→6 | 2→2 | 34419→52476 | 11473→17492 | 2071202→2079577 | 1→1 | I/O regression |
| full | 50000 | clustered_reads_cold_manager | 14.53MiB→13.39MiB | 2.29MiB→2.30MiB | 54→41 | 0→0 | 428763→426678 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | clustered_reads_warm_manager | 14.41MiB→13.34MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | clustered_sparse_diff | 15.59MiB→14.41MiB | 2.33MiB→2.34MiB | 8→8 | 0→0 | 78608→78336 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | identical_diff | 14.88MiB→13.73MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | random_batch_deletes | 32.05MiB→26.30MiB | 4.29MiB→4.34MiB | 169→188 | 169→160 | 1915782→2086957 | 1895282→1906853 | 2050702→2058570 | 1→1 | I/O regression |
| full | 50000 | random_batch_updates | 32.73MiB→28.50MiB | 4.33MiB→4.32MiB | 169→158 | 169→158 | 1915782→1902498 | 1915782→1902498 | 2071202→2079577 | 1→1 | — |
| full | 50000 | random_conflict_resolved_merge | 35.86MiB→33.08MiB | 5.77MiB→5.93MiB | 414→399 | 0→0 | 4975320→5173467 | 0→0 | 2071202→2079577 | 1→1 | I/O regression |
| full | 50000 | random_delete_diff | 47.70MiB→27.52MiB | 4.32MiB→4.37MiB | 368→324 | 0→0 | 3966484→3834713 | 0→0 | 2050702→2058570 | 1→1 | — |
| full | 50000 | random_disjoint_sparse_merge | 22.12MiB→19.33MiB | 4.34MiB→4.36MiB | 6→6 | 2→2 | 49218→52845 | 16406→17615 | 2071202→2079577 | 1→1 | I/O regression |
| full | 50000 | random_reads_cold_manager | 12.00MiB→11.22MiB | 2.29MiB→2.30MiB | 199→187 | 0→0 | 2071202→2079577 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | random_reads_warm_manager | 11.98MiB→11.19MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | random_sparse_diff | 32.28MiB→28.59MiB | 4.36MiB→4.36MiB | 338→316 | 0→0 | 3831564→3804996 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | right_edge_reads_cold_manager | 14.45MiB→13.38MiB | 2.29MiB→2.30MiB | 40→36 | 0→0 | 428358→428374 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | right_edge_reads_warm_manager | 14.55MiB→13.36MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | shuffled_batch_build | 31.09MiB→30.50MiB | 2.26MiB→2.27MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 50000 | sorted_stream_build | 19.53MiB→18.97MiB | 2.26MiB→2.27MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| full | 100000 | append_batch_upserts | 26.67MiB→24.59MiB | 4.56MiB→4.54MiB | 3→3 | 9→5 | 24446→8001 | 66032→49375 | 4182715→4200803 | 2→2 | — |
| full | 100000 | append_disjoint_sparse_merge | 26.69MiB→24.61MiB | 4.60MiB→4.59MiB | 10→10 | 4→4 | 79092→45106 | 30194→29098 | 4182481→4200803 | 2→2 | — |
| full | 100000 | append_sparse_diff | 26.03MiB→24.27MiB | 4.59MiB→4.57MiB | 12→8 | 0→0 | 90478→57376 | 0→0 | 4182715→4200803 | 2→2 | — |
| full | 100000 | clustered_batch_deletes | 26.05MiB→24.11MiB | 4.53MiB→4.52MiB | 6→10 | 4→3 | 78963→98359 | 37691→23984 | 4099857→4117806 | 2→2 | I/O regression |
| full | 100000 | clustered_batch_updates | 26.52MiB→24.78MiB | 4.57MiB→4.57MiB | 8→9 | 6→7 | 103293→73438 | 78963→65607 | 4141129→4159429 | 2→2 | I/O regression |
| full | 100000 | clustered_conflict_resolved_merge | 26.83MiB→24.67MiB | 4.61MiB→4.61MiB | 15→18 | 0→0 | 173613→157086 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| full | 100000 | clustered_delete_diff | 35.64MiB→23.98MiB | 4.56MiB→4.55MiB | 208→10 | 0→0 | 2183166→89591 | 0→0 | 4099857→4117806 | 2→2 | — |
| full | 100000 | clustered_disjoint_sparse_merge | 26.33MiB→24.73MiB | 4.62MiB→4.61MiB | 9→9 | 3→3 | 86913→63054 | 28971→21018 | 4141129→4159429 | 2→2 | — |
| full | 100000 | clustered_reads_cold_manager | 24.42MiB→22.66MiB | 4.52MiB→4.52MiB | 41→43 | 0→0 | 449147→427654 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| full | 100000 | clustered_reads_warm_manager | 24.59MiB→22.70MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | clustered_sparse_diff | 26.02MiB→24.08MiB | 4.60MiB→4.60MiB | 12→14 | 0→0 | 157926→131214 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| full | 100000 | identical_diff | 25.00MiB→23.06MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | random_batch_deletes | 54.91MiB→47.81MiB | 8.50MiB→8.60MiB | 320→377 | 320→321 | 3802839→4159641 | 3761837→3831138 | 4100127→4118169 | 2→2 | I/O regression |
| full | 100000 | random_batch_updates | 54.86MiB→49.97MiB | 8.52MiB→8.59MiB | 320→315 | 320→315 | 3802839→3828688 | 3802839→3828688 | 4141129→4159429 | 2→2 | — |
| full | 100000 | random_conflict_resolved_merge | 61.88MiB→57.34MiB | 11.31MiB→11.51MiB | 756→762 | 0→0 | 9711192→9968892 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | random_delete_diff | 71.03MiB→47.95MiB | 8.53MiB→8.63MiB | 701→644 | 0→0 | 7902966→7703536 | 0→0 | 4100127→4118169 | 2→2 | — |
| full | 100000 | random_disjoint_sparse_merge | 36.06MiB→30.44MiB | 8.56MiB→8.63MiB | 6→9 | 2→3 | 23640→68847 | 7880→22949 | 4141129→4159429 | 2→2 | I/O regression |
| full | 100000 | random_reads_cold_manager | 19.64MiB→18.11MiB | 4.52MiB→4.52MiB | 381→376 | 0→0 | 4141129→4159429 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | random_reads_warm_manager | 19.69MiB→18.11MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | random_sparse_diff | 55.39MiB→49.83MiB | 8.55MiB→8.62MiB | 640→630 | 0→0 | 7605678→7657376 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | right_edge_reads_cold_manager | 24.55MiB→22.66MiB | 4.52MiB→4.52MiB | 42→41 | 0→0 | 431633→423175 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | right_edge_reads_warm_manager | 24.42MiB→22.67MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | shuffled_batch_build | 52.38MiB→51.48MiB | 4.49MiB→4.49MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 100000 | sorted_stream_build | 30.55MiB→29.12MiB | 4.49MiB→4.49MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| full | 1000000 | append_batch_upserts | 205.09MiB→190.48MiB | 44.89MiB→45.12MiB | 3→3 | 36→42 | 26426→16886 | 440151→433014 | 41812942→41996705 | 2→2 | I/O regression |
| full | 1000000 | append_disjoint_sparse_merge | 205.25MiB→190.58MiB | 44.93MiB→45.17MiB | 27→31 | 21→25 | 274983→264951 | 222125→231173 | 41813253→41996705 | 2→2 | I/O regression |
| full | 1000000 | append_sparse_diff | 208.08MiB→193.61MiB | 44.92MiB→45.16MiB | 39→45 | 0→0 | 466577→449900 | 0→0 | 41812942→41996705 | 2→2 | I/O regression |
| full | 1000000 | clustered_batch_deletes | 204.19MiB→187.20MiB | 44.47MiB→44.67MiB | 39→59 | 4→3 | 459956→590464 | 46083→23911 | 40985344→41164194 | 2→2 | I/O regression |
| full | 1000000 | clustered_batch_updates | 206.86MiB→190.61MiB | 44.88MiB→45.13MiB | 41→46 | 39→44 | 485859→456487 | 459956→440294 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | clustered_conflict_resolved_merge | 205.86MiB→189.23MiB | 44.96MiB→45.14MiB | 63→69 | 0→0 | 746826→679260 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | clustered_delete_diff | 217.25MiB→190.84MiB | 44.50MiB→44.70MiB | 518→47 | 0→0 | 6090498→464205 | 0→0 | 40985344→41164194 | 2→2 | — |
| full | 1000000 | clustered_disjoint_sparse_merge | 203.95MiB→188.28MiB | 44.97MiB→45.19MiB | 9→9 | 3→3 | 123783→105381 | 41261→35127 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | clustered_reads_cold_manager | 202.19MiB→187.27MiB | 44.45MiB→44.68MiB | 39→44 | 0→0 | 459956→440294 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | clustered_reads_warm_manager | 202.14MiB→187.42MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | clustered_sparse_diff | 209.16MiB→193.61MiB | 44.91MiB→45.16MiB | 78→88 | 0→0 | 919912→880588 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | identical_diff | 202.70MiB→187.86MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | random_batch_deletes | 505.81MiB→411.20MiB | 84.58MiB→85.97MiB | 3088→3635 | 3088→3187 | 38260666→41518660 | 37850637→38767651 | 40989188→41165275 | 2→2 | I/O regression |
| full | 1000000 | random_batch_updates | 541.27MiB→411.66MiB | 85.03MiB→85.72MiB | 3088→3115 | 3088→3115 | 38260666→38578275 | 38260666→38578275 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | random_conflict_resolved_merge | 565.53MiB→500.52MiB | 113.27MiB→113.93MiB | 7383→7467 | 0→0 | 98281305→98688111 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | random_delete_diff | 587.31MiB→413.19MiB | 84.61MiB→86.00MiB | 6703→6423 | 0→0 | 79061635→77993243 | 0→0 | 40989188→41165275 | 2→2 | — |
| full | 1000000 | random_disjoint_sparse_merge | 322.39MiB→231.28MiB | 85.02MiB→85.68MiB | 6→9 | 2→3 | 60507→76395 | 20169→25465 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | random_reads_cold_manager | 159.78MiB→144.36MiB | 44.45MiB→44.68MiB | 3088→3115 | 0→0 | 38260666→38578275 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | random_reads_warm_manager | 159.83MiB→144.34MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | random_sparse_diff | 545.19MiB→437.28MiB | 85.06MiB→85.75MiB | 6176→6230 | 0→0 | 76521332→77156550 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | right_edge_reads_cold_manager | 202.39MiB→187.33MiB | 44.45MiB→44.68MiB | 36→38 | 0→0 | 432590→425301 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| full | 1000000 | right_edge_reads_warm_manager | 202.16MiB→187.41MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | shuffled_batch_build | 385.34MiB→377.52MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 1000000 | sorted_stream_build | 205.92MiB→191.78MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| full | 10000000 | append_batch_upserts | 1.90GiB→1.75GiB | 445.45MiB→447.36MiB | 3→3 | 40→39 | 19027→22012 | 433066→437765 | 414415620→416242123 | 2→2 | I/O regression |
| full | 10000000 | append_disjoint_sparse_merge | 1.90GiB→1.75GiB | 445.51MiB→447.44MiB | 27→26 | 21→20 | 271858→284257 | 233795→240223 | 414415771→416242123 | 2→2 | I/O regression |
| full | 10000000 | append_sparse_diff | 1.90GiB→1.75GiB | 445.48MiB→447.39MiB | 43→42 | 0→0 | 452093→459777 | 0→0 | 414415620→416242123 | 2→2 | — |
| full | 10000000 | clustered_batch_deletes | 1.89GiB→1.75GiB | 445.02MiB→446.94MiB | 37→177 | 4→4 | 432979→1925057 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| full | 10000000 | clustered_batch_updates | 1.90GiB→1.75GiB | 445.46MiB→447.38MiB | 39→45 | 37→43 | 446511→478955 | 432979→462887 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | clustered_conflict_resolved_merge | 1.90GiB→1.75GiB | 445.50MiB→447.41MiB | 69→69 | 0→0 | 674877→752367 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | clustered_delete_diff | 1.90GiB→1.76GiB | 445.05MiB→446.97MiB | 214→48 | 0→0 | 2302588→521297 | 0→0 | 413587853→415410109 | 2→2 | — |
| full | 10000000 | clustered_disjoint_sparse_merge | 1.89GiB→1.75GiB | 445.54MiB→447.44MiB | 9→9 | 3→3 | 71685→78951 | 23895→26317 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | clustered_reads_cold_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 37→43 | 0→0 | 432979→462887 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | clustered_reads_warm_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | clustered_sparse_diff | 1.90GiB→1.75GiB | 445.49MiB→447.41MiB | 74→86 | 0→0 | 865958→925774 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | identical_diff | 1.89GiB→1.75GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_batch_deletes | 2.55GiB→2.80GiB | 572.69MiB→600.33MiB | 8559→21391 | 8559→10741 | 123383495→256941114 | 122973473→146532854 | 413591559→415411354 | 2→2 | memory regression, size regression, I/O regression |
| full | 10000000 | random_batch_updates | 2.85GiB→2.52GiB | 573.06MiB→575.89MiB | 8560→8578 | 8559→8577 | 123384274→124100545 | 123383495→124096676 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_conflict_resolved_merge | 2.48GiB→2.38GiB | 586.92MiB→589.25MiB | 14106→14157 | 0→0 | 205341162→205937286 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_delete_diff | 2.65GiB→2.72GiB | 572.72MiB→600.36MiB | 25278→21545 | 0→0 | 330783557→293687663 | 0→0 | 413591559→415411354 | 2→2 | size regression |
| full | 10000000 | random_disjoint_sparse_merge | 2.02GiB→1.88GiB | 573.02MiB→575.79MiB | 6→3 | 2→1 | 40467→17832 | 13489→5944 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_reads_cold_manager | 1.75GiB→1.61GiB | 445.02MiB→446.91MiB | 8559→8577 | 0→0 | 123383495→124096676 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_reads_warm_manager | 1.75GiB→1.61GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | random_sparse_diff | 2.71GiB→2.52GiB | 573.09MiB→575.92MiB | 17118→17154 | 0→0 | 246766990→248193352 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | right_edge_reads_cold_manager | 1.89GiB→1.75GiB | 445.02MiB→446.91MiB | 32→42 | 0→0 | 441597→438020 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| full | 10000000 | right_edge_reads_warm_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| full | 10000000 | shuffled_batch_build | 2.81GiB→3.17GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | memory regression |
| full | 10000000 | sorted_stream_build | 1.90GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 1000 | append_batch_upserts | 4.31MiB→4.20MiB | 68.0KiB→84.0KiB | 2→2 | 2→2 | 9151→3573 | 13263→7687 | 45573→46200 | 1→1 | — |
| normal | 1000 | append_disjoint_sparse_merge | 4.75MiB→4.44MiB | 100.0KiB→100.0KiB | 6→6 | 2→2 | 31567→14833 | 13263→7687 | 45573→46200 | 1→1 | — |
| normal | 1000 | append_sparse_diff | 4.39MiB→4.25MiB | 100.0KiB→116.0KiB | 4→4 | 0→0 | 22414→11260 | 0→0 | 45573→46200 | 1→1 | — |
| normal | 1000 | clustered_batch_deletes | 4.09MiB→4.19MiB | 64.0KiB→80.0KiB | 2→4 | 2→2 | 12523→23773 | 8413→3491 | 37351→37974 | 1→1 | I/O regression |
| normal | 1000 | clustered_batch_updates | 4.22MiB→4.12MiB | 68.0KiB→84.0KiB | 3→3 | 2→2 | 21484→10814 | 12523→7603 | 41461→42086 | 1→1 | — |
| normal | 1000 | clustered_conflict_resolved_merge | 4.39MiB→4.34MiB | 88.0KiB→92.0KiB | 6→6 | 0→0 | 37569→22809 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | clustered_delete_diff | 4.22MiB→4.25MiB | 96.0KiB→112.0KiB | 4→4 | 0→0 | 20936→11094 | 0→0 | 37351→37974 | 1→1 | — |
| normal | 1000 | clustered_disjoint_sparse_merge | 4.44MiB→4.34MiB | 100.0KiB→100.0KiB | 6→6 | 2→2 | 37569→22809 | 12523→7603 | 41461→42086 | 1→1 | — |
| normal | 1000 | clustered_reads_cold_manager | 3.81MiB→3.80MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | clustered_reads_warm_manager | 3.81MiB→3.83MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | clustered_sparse_diff | 4.31MiB→4.20MiB | 100.0KiB→116.0KiB | 4→4 | 0→0 | 25046→15206 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | identical_diff | 3.81MiB→3.84MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | random_batch_deletes | 4.59MiB→4.67MiB | 104.0KiB→116.0KiB | 5→8 | 5→8 | 41461→42086 | 37361→37986 | 37361→37986 | 1→1 | I/O regression |
| normal | 1000 | random_batch_updates | 4.56MiB→4.48MiB | 104.0KiB→128.0KiB | 5→8 | 5→8 | 41461→42086 | 41461→42086 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | random_conflict_resolved_merge | 4.89MiB→5.12MiB | 148.0KiB→184.0KiB | 15→24 | 0→0 | 124383→126258 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | random_delete_diff | 4.94MiB→4.91MiB | 136.0KiB→148.0KiB | 10→16 | 0→0 | 78822→80072 | 0→0 | 37361→37986 | 1→1 | I/O regression |
| normal | 1000 | random_disjoint_sparse_merge | 4.67MiB→4.73MiB | 128.0KiB→148.0KiB | 6→6 | 2→2 | 37569→22809 | 12523→7603 | 41461→42086 | 1→1 | — |
| normal | 1000 | random_reads_cold_manager | 3.86MiB→3.86MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | random_reads_warm_manager | 3.86MiB→3.84MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | random_sparse_diff | 4.72MiB→4.70MiB | 136.0KiB→160.0KiB | 10→16 | 0→0 | 82922→84172 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | right_edge_reads_cold_manager | 3.81MiB→3.84MiB | 88.0KiB→100.0KiB | 5→8 | 0→0 | 41461→42086 | 0→0 | 41461→42086 | 1→1 | I/O regression |
| normal | 1000 | right_edge_reads_warm_manager | 3.81MiB→3.84MiB | 88.0KiB→100.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | shuffled_batch_build | 4.94MiB→4.98MiB | 56.0KiB→68.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 1000 | sorted_stream_build | 4.64MiB→4.69MiB | 56.0KiB→68.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 41461→42086 | 1→1 | — |
| normal | 10000 | append_batch_upserts | 7.48MiB→6.56MiB | 488.0KiB→488.0KiB | 2→2 | 3→2 | 6995→5718 | 11188→9833 | 418414→420190 | 1→1 | — |
| normal | 10000 | append_disjoint_sparse_merge | 7.58MiB→6.83MiB | 512.0KiB→524.0KiB | 6→6 | 2→2 | 25101→21274 | 11108→9833 | 418334→420190 | 1→1 | — |
| normal | 10000 | append_sparse_diff | 7.14MiB→6.55MiB | 520.0KiB→520.0KiB | 5→4 | 0→0 | 18183→15551 | 0→0 | 418414→420190 | 1→1 | — |
| normal | 10000 | clustered_batch_deletes | 7.28MiB→6.97MiB | 500.0KiB→508.0KiB | 3→7 | 3→4 | 23564→67048 | 19451→30238 | 410108→411838 | 1→1 | I/O regression |
| normal | 10000 | clustered_batch_updates | 7.38MiB→6.80MiB | 500.0KiB→508.0KiB | 4→5 | 3→4 | 29031→33569 | 23564→29454 | 414221→416075 | 1→1 | I/O regression |
| normal | 10000 | clustered_conflict_resolved_merge | 7.38MiB→6.72MiB | 516.0KiB→492.0KiB | 6→6 | 0→0 | 46026→14079 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | clustered_delete_diff | 9.67MiB→7.19MiB | 532.0KiB→540.0KiB | 43→9 | 0→0 | 433672→64713 | 0→0 | 410108→411838 | 1→1 | — |
| normal | 10000 | clustered_disjoint_sparse_merge | 7.56MiB→7.03MiB | 544.0KiB→536.0KiB | 6→6 | 2→2 | 46026→14079 | 15342→4693 | 414221→416075 | 1→1 | — |
| normal | 10000 | clustered_reads_cold_manager | 5.59MiB→5.42MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | clustered_reads_warm_manager | 5.61MiB→5.42MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | clustered_sparse_diff | 7.30MiB→6.84MiB | 532.0KiB→540.0KiB | 6→8 | 0→0 | 47128→58908 | 0→0 | 414221→416075 | 1→1 | I/O regression |
| normal | 10000 | identical_diff | 6.73MiB→6.28MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | random_batch_deletes | 10.09MiB→9.62MiB | 900.0KiB→916.0KiB | 35→40 | 35→36 | 381623→417677 | 377523→397649 | 410121→411972 | 1→1 | I/O regression |
| normal | 10000 | random_batch_updates | 10.28MiB→9.81MiB | 900.0KiB→904.0KiB | 35→35 | 35→35 | 381623→390152 | 381623→390152 | 414221→416075 | 1→1 | — |
| normal | 10000 | random_conflict_resolved_merge | 11.78MiB→11.34MiB | 1.17MiB→1.22MiB | 84→93 | 0→0 | 987885→1057785 | 0→0 | 414221→416075 | 1→1 | I/O regression |
| normal | 10000 | random_delete_diff | 10.02MiB→9.88MiB | 932.0KiB→948.0KiB | 70→72 | 0→0 | 759146→799401 | 0→0 | 410121→411972 | 1→1 | I/O regression |
| normal | 10000 | random_disjoint_sparse_merge | 8.69MiB→8.38MiB | 920.0KiB→956.0KiB | 6→6 | 2→2 | 29250→68220 | 9750→22740 | 414221→416075 | 1→1 | I/O regression |
| normal | 10000 | random_reads_cold_manager | 5.83MiB→5.58MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | random_reads_warm_manager | 5.80MiB→5.53MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | random_sparse_diff | 10.28MiB→9.88MiB | 932.0KiB→936.0KiB | 70→70 | 0→0 | 763246→780304 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | right_edge_reads_cold_manager | 5.59MiB→5.44MiB | 504.0KiB→504.0KiB | 40→39 | 0→0 | 414221→416075 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | right_edge_reads_warm_manager | 5.61MiB→5.42MiB | 504.0KiB→504.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | shuffled_batch_build | 10.45MiB→9.95MiB | 472.0KiB→472.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 10000 | sorted_stream_build | 7.97MiB→7.67MiB | 472.0KiB→472.0KiB | 0→0 | 0→0 | 0→0 | 0→0 | 414221→416075 | 1→1 | — |
| normal | 50000 | append_batch_upserts | 16.12MiB→15.05MiB | 2.30MiB→2.31MiB | 2→2 | 6→5 | 8594→17614 | 29466→38557 | 2092074→2100520 | 1→1 | I/O regression |
| normal | 50000 | append_disjoint_sparse_merge | 16.16MiB→15.20MiB | 2.32MiB→2.34MiB | 7→7 | 3→3 | 37988→59058 | 20795→23825 | 2091996→2100520 | 1→1 | I/O regression |
| normal | 50000 | append_sparse_diff | 15.70MiB→14.61MiB | 2.33MiB→2.34MiB | 8→7 | 0→0 | 38060→56171 | 0→0 | 2092074→2100520 | 1→1 | I/O regression |
| normal | 50000 | clustered_batch_deletes | 15.75MiB→14.61MiB | 2.28MiB→2.29MiB | 4→6 | 3→2 | 39304→70384 | 18668→18360 | 2050566→2058769 | 1→1 | I/O regression |
| normal | 50000 | clustered_batch_updates | 16.12MiB→14.61MiB | 2.30MiB→2.31MiB | 5→5 | 4→4 | 40289→49246 | 39304→39168 | 2071202→2079577 | 1→1 | I/O regression |
| normal | 50000 | clustered_conflict_resolved_merge | 16.41MiB→14.78MiB | 2.34MiB→2.35MiB | 12→9 | 0→0 | 117912→101949 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | clustered_delete_diff | 29.16MiB→14.56MiB | 2.31MiB→2.32MiB | 202→6 | 0→0 | 2089870→57528 | 0→0 | 2050566→2058769 | 1→1 | — |
| normal | 50000 | clustered_disjoint_sparse_merge | 15.95MiB→14.66MiB | 2.33MiB→2.35MiB | 6→6 | 2→2 | 34419→52476 | 11473→17492 | 2071202→2079577 | 1→1 | I/O regression |
| normal | 50000 | clustered_reads_cold_manager | 14.64MiB→13.39MiB | 2.29MiB→2.30MiB | 54→41 | 0→0 | 428763→426678 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | clustered_reads_warm_manager | 14.55MiB→13.41MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | clustered_sparse_diff | 15.59MiB→14.28MiB | 2.33MiB→2.34MiB | 8→8 | 0→0 | 78608→78336 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | identical_diff | 14.97MiB→13.78MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | random_batch_deletes | 31.81MiB→27.64MiB | 4.29MiB→4.34MiB | 169→188 | 169→160 | 1915782→2086957 | 1895282→1906853 | 2050702→2058570 | 1→1 | I/O regression |
| normal | 50000 | random_batch_updates | 32.03MiB→28.52MiB | 4.33MiB→4.32MiB | 169→158 | 169→158 | 1915782→1902498 | 1915782→1902498 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | random_conflict_resolved_merge | 35.84MiB→33.08MiB | 5.77MiB→5.93MiB | 414→399 | 0→0 | 4975320→5173467 | 0→0 | 2071202→2079577 | 1→1 | I/O regression |
| normal | 50000 | random_delete_diff | 45.75MiB→27.81MiB | 4.32MiB→4.37MiB | 368→324 | 0→0 | 3966484→3834713 | 0→0 | 2050702→2058570 | 1→1 | — |
| normal | 50000 | random_disjoint_sparse_merge | 22.25MiB→19.30MiB | 4.34MiB→4.36MiB | 6→6 | 2→2 | 49218→52845 | 16406→17615 | 2071202→2079577 | 1→1 | I/O regression |
| normal | 50000 | random_reads_cold_manager | 12.00MiB→11.16MiB | 2.29MiB→2.30MiB | 199→187 | 0→0 | 2071202→2079577 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | random_reads_warm_manager | 11.98MiB→11.14MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | random_sparse_diff | 32.06MiB→28.59MiB | 4.36MiB→4.36MiB | 338→316 | 0→0 | 3831564→3804996 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | right_edge_reads_cold_manager | 14.59MiB→13.31MiB | 2.29MiB→2.30MiB | 40→36 | 0→0 | 428358→428374 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | right_edge_reads_warm_manager | 14.48MiB→13.33MiB | 2.29MiB→2.30MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | shuffled_batch_build | 29.23MiB→30.52MiB | 2.26MiB→2.27MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 50000 | sorted_stream_build | 19.67MiB→18.97MiB | 2.26MiB→2.27MiB | 0→0 | 0→0 | 0→0 | 0→0 | 2071202→2079577 | 1→1 | — |
| normal | 100000 | append_batch_upserts | 26.75MiB→24.81MiB | 4.56MiB→4.54MiB | 3→3 | 9→5 | 24446→8001 | 66032→49375 | 4182715→4200803 | 2→2 | — |
| normal | 100000 | append_disjoint_sparse_merge | 26.72MiB→24.53MiB | 4.60MiB→4.59MiB | 10→10 | 4→4 | 79092→45106 | 30194→29098 | 4182481→4200803 | 2→2 | — |
| normal | 100000 | append_sparse_diff | 25.98MiB→24.09MiB | 4.59MiB→4.57MiB | 12→8 | 0→0 | 90478→57376 | 0→0 | 4182715→4200803 | 2→2 | — |
| normal | 100000 | clustered_batch_deletes | 26.19MiB→24.03MiB | 4.53MiB→4.52MiB | 6→10 | 4→3 | 78963→98359 | 37691→23984 | 4099857→4117806 | 2→2 | I/O regression |
| normal | 100000 | clustered_batch_updates | 26.41MiB→24.53MiB | 4.57MiB→4.57MiB | 8→9 | 6→7 | 103293→73438 | 78963→65607 | 4141129→4159429 | 2→2 | I/O regression |
| normal | 100000 | clustered_conflict_resolved_merge | 26.84MiB→24.44MiB | 4.61MiB→4.61MiB | 15→18 | 0→0 | 173613→157086 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| normal | 100000 | clustered_delete_diff | 35.61MiB→24.00MiB | 4.56MiB→4.55MiB | 208→10 | 0→0 | 2183166→89591 | 0→0 | 4099857→4117806 | 2→2 | — |
| normal | 100000 | clustered_disjoint_sparse_merge | 26.53MiB→24.56MiB | 4.62MiB→4.61MiB | 9→9 | 3→3 | 86913→63054 | 28971→21018 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | clustered_reads_cold_manager | 24.45MiB→22.80MiB | 4.52MiB→4.52MiB | 41→43 | 0→0 | 449147→427654 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| normal | 100000 | clustered_reads_warm_manager | 24.47MiB→22.67MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | clustered_sparse_diff | 25.91MiB→24.12MiB | 4.60MiB→4.60MiB | 12→14 | 0→0 | 157926→131214 | 0→0 | 4141129→4159429 | 2→2 | I/O regression |
| normal | 100000 | identical_diff | 24.83MiB→23.11MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | random_batch_deletes | 54.86MiB→47.86MiB | 8.50MiB→8.60MiB | 320→377 | 320→321 | 3802839→4159641 | 3761837→3831138 | 4100127→4118169 | 2→2 | I/O regression |
| normal | 100000 | random_batch_updates | 55.16MiB→49.88MiB | 8.52MiB→8.59MiB | 320→315 | 320→315 | 3802839→3828688 | 3802839→3828688 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | random_conflict_resolved_merge | 63.14MiB→57.25MiB | 11.31MiB→11.51MiB | 756→762 | 0→0 | 9711192→9968892 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | random_delete_diff | 71.20MiB→45.36MiB | 8.53MiB→8.63MiB | 701→644 | 0→0 | 7902966→7703536 | 0→0 | 4100127→4118169 | 2→2 | — |
| normal | 100000 | random_disjoint_sparse_merge | 35.94MiB→30.39MiB | 8.56MiB→8.63MiB | 6→9 | 2→3 | 23640→68847 | 7880→22949 | 4141129→4159429 | 2→2 | I/O regression |
| normal | 100000 | random_reads_cold_manager | 19.67MiB→18.09MiB | 4.52MiB→4.52MiB | 381→376 | 0→0 | 4141129→4159429 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | random_reads_warm_manager | 19.62MiB→18.14MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | random_sparse_diff | 54.64MiB→49.77MiB | 8.55MiB→8.62MiB | 640→630 | 0→0 | 7605678→7657376 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | right_edge_reads_cold_manager | 24.58MiB→22.64MiB | 4.52MiB→4.52MiB | 42→41 | 0→0 | 431633→423175 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | right_edge_reads_warm_manager | 24.58MiB→22.70MiB | 4.52MiB→4.52MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | shuffled_batch_build | 52.55MiB→51.44MiB | 4.49MiB→4.49MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 100000 | sorted_stream_build | 27.39MiB→29.12MiB | 4.49MiB→4.49MiB | 0→0 | 0→0 | 0→0 | 0→0 | 4141129→4159429 | 2→2 | — |
| normal | 1000000 | append_batch_upserts | 205.69MiB→190.16MiB | 44.89MiB→45.12MiB | 3→3 | 36→42 | 26426→16886 | 440151→433014 | 41812942→41996705 | 2→2 | I/O regression |
| normal | 1000000 | append_disjoint_sparse_merge | 205.78MiB→190.22MiB | 44.93MiB→45.17MiB | 27→31 | 21→25 | 274983→264951 | 222125→231173 | 41813253→41996705 | 2→2 | I/O regression |
| normal | 1000000 | append_sparse_diff | 208.16MiB→193.56MiB | 44.92MiB→45.16MiB | 39→45 | 0→0 | 466577→449900 | 0→0 | 41812942→41996705 | 2→2 | I/O regression |
| normal | 1000000 | clustered_batch_deletes | 203.70MiB→187.38MiB | 44.47MiB→44.67MiB | 39→59 | 4→3 | 459956→590464 | 46083→23911 | 40985344→41164194 | 2→2 | I/O regression |
| normal | 1000000 | clustered_batch_updates | 206.81MiB→189.89MiB | 44.88MiB→45.13MiB | 41→46 | 39→44 | 485859→456487 | 459956→440294 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | clustered_conflict_resolved_merge | 205.30MiB→189.47MiB | 44.96MiB→45.14MiB | 63→69 | 0→0 | 746826→679260 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | clustered_delete_diff | 217.14MiB→190.80MiB | 44.50MiB→44.70MiB | 518→47 | 0→0 | 6090498→464205 | 0→0 | 40985344→41164194 | 2→2 | — |
| normal | 1000000 | clustered_disjoint_sparse_merge | 204.06MiB→188.89MiB | 44.97MiB→45.19MiB | 9→9 | 3→3 | 123783→105381 | 41261→35127 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | clustered_reads_cold_manager | 202.17MiB→187.50MiB | 44.45MiB→44.68MiB | 39→44 | 0→0 | 459956→440294 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | clustered_reads_warm_manager | 202.31MiB→187.28MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | clustered_sparse_diff | 210.30MiB→193.31MiB | 44.91MiB→45.16MiB | 78→88 | 0→0 | 919912→880588 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | identical_diff | 202.75MiB→187.89MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | random_batch_deletes | 556.11MiB→383.30MiB | 84.58MiB→85.97MiB | 3088→3635 | 3088→3187 | 38260666→41518660 | 37850637→38767651 | 40989188→41165275 | 2→2 | I/O regression |
| normal | 1000000 | random_batch_updates | 540.20MiB→433.62MiB | 85.03MiB→85.72MiB | 3088→3115 | 3088→3115 | 38260666→38578275 | 38260666→38578275 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | random_conflict_resolved_merge | 577.00MiB→501.19MiB | 113.27MiB→113.93MiB | 7383→7467 | 0→0 | 98281305→98688111 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | random_delete_diff | 595.45MiB→387.08MiB | 84.61MiB→86.00MiB | 6703→6423 | 0→0 | 79061635→77993243 | 0→0 | 40989188→41165275 | 2→2 | — |
| normal | 1000000 | random_disjoint_sparse_merge | 314.44MiB→231.67MiB | 85.02MiB→85.68MiB | 6→9 | 2→3 | 60507→76395 | 20169→25465 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | random_reads_cold_manager | 159.78MiB→144.33MiB | 44.45MiB→44.68MiB | 3088→3115 | 0→0 | 38260666→38578275 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | random_reads_warm_manager | 159.86MiB→144.38MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | random_sparse_diff | 544.55MiB→388.45MiB | 85.06MiB→85.75MiB | 6176→6230 | 0→0 | 76521332→77156550 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | right_edge_reads_cold_manager | 202.22MiB→187.28MiB | 44.45MiB→44.68MiB | 36→38 | 0→0 | 432590→425301 | 0→0 | 41399217→41580577 | 2→2 | I/O regression |
| normal | 1000000 | right_edge_reads_warm_manager | 202.34MiB→187.45MiB | 44.45MiB→44.68MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 1000000 | shuffled_batch_build | 340.08MiB→377.48MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | memory regression |
| normal | 1000000 | sorted_stream_build | 205.88MiB→191.78MiB | 44.41MiB→44.64MiB | 0→0 | 0→0 | 0→0 | 0→0 | 41399217→41580577 | 2→2 | — |
| normal | 10000000 | append_batch_upserts | 1.89GiB→1.75GiB | 445.45MiB→447.36MiB | 3→3 | 40→39 | 19027→22012 | 433066→437765 | 414415620→416242123 | 2→2 | I/O regression |
| normal | 10000000 | append_disjoint_sparse_merge | 1.90GiB→1.75GiB | 445.51MiB→447.44MiB | 27→26 | 21→20 | 271858→284257 | 233795→240223 | 414415771→416242123 | 2→2 | I/O regression |
| normal | 10000000 | append_sparse_diff | 1.90GiB→1.75GiB | 445.48MiB→447.39MiB | 43→42 | 0→0 | 452093→459777 | 0→0 | 414415620→416242123 | 2→2 | — |
| normal | 10000000 | clustered_batch_deletes | 1.90GiB→1.75GiB | 445.02MiB→446.94MiB | 37→177 | 4→4 | 432979→1925057 | 19251→52518 | 413587853→415410109 | 2→2 | I/O regression |
| normal | 10000000 | clustered_batch_updates | 1.90GiB→1.75GiB | 445.46MiB→447.38MiB | 39→45 | 37→43 | 446511→478955 | 432979→462887 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | clustered_conflict_resolved_merge | 1.90GiB→1.75GiB | 445.50MiB→447.41MiB | 69→69 | 0→0 | 674877→752367 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | clustered_delete_diff | 1.90GiB→1.76GiB | 445.05MiB→446.97MiB | 214→48 | 0→0 | 2302588→521297 | 0→0 | 413587853→415410109 | 2→2 | — |
| normal | 10000000 | clustered_disjoint_sparse_merge | 1.89GiB→1.75GiB | 445.54MiB→447.44MiB | 9→9 | 3→3 | 71685→78951 | 23895→26317 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | clustered_reads_cold_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 37→43 | 0→0 | 432979→462887 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | clustered_reads_warm_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | clustered_sparse_diff | 1.90GiB→1.75GiB | 445.49MiB→447.41MiB | 74→86 | 0→0 | 865958→925774 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | identical_diff | 1.89GiB→1.75GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_batch_deletes | 2.71GiB→2.95GiB | 572.69MiB→600.33MiB | 8559→21391 | 8559→10741 | 123383495→256941114 | 122973473→146532854 | 413591559→415411354 | 2→2 | memory regression, size regression, I/O regression |
| normal | 10000000 | random_batch_updates | 2.71GiB→2.52GiB | 573.05MiB→575.89MiB | 8560→8578 | 8559→8577 | 123384274→124100545 | 123383495→124096676 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_conflict_resolved_merge | 2.57GiB→2.38GiB | 586.94MiB→589.25MiB | 14106→14157 | 0→0 | 205341162→205937286 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_delete_diff | 2.68GiB→2.91GiB | 572.72MiB→600.36MiB | 25278→21545 | 0→0 | 330783557→293687663 | 0→0 | 413591559→415411354 | 2→2 | memory regression, size regression |
| normal | 10000000 | random_disjoint_sparse_merge | 2.02GiB→1.88GiB | 573.02MiB→575.79MiB | 6→3 | 2→1 | 40467→17832 | 13489→5944 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_reads_cold_manager | 1.75GiB→1.61GiB | 445.02MiB→446.91MiB | 8559→8577 | 0→0 | 123383495→124096676 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_reads_warm_manager | 1.75GiB→1.61GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | random_sparse_diff | 2.85GiB→2.52GiB | 573.09MiB→575.92MiB | 17118→17154 | 0→0 | 246766990→248193352 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | right_edge_reads_cold_manager | 1.89GiB→1.75GiB | 445.02MiB→446.91MiB | 32→42 | 0→0 | 441597→438020 | 0→0 | 414001581→415826370 | 2→2 | I/O regression |
| normal | 10000000 | right_edge_reads_warm_manager | 1.89GiB→1.74GiB | 445.02MiB→446.91MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | shuffled_batch_build | 3.05GiB→3.14GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |
| normal | 10000000 | sorted_stream_build | 1.89GiB→1.75GiB | 444.99MiB→446.88MiB | 0→0 | 0→0 | 0→0 | 0→0 | 414001581→415826370 | 2→2 | — |

## Methodology and limitations

The two revisions use byte-identical benchmark sources, deterministic keys and mutation sets, separate WAL+FULL and WAL+NORMAL profiles, alternating process order, and isolated SQLite fixture clones. Cold-manager means a fresh decoded-node cache; the operating-system page cache is not flushed. Diff and merge branch preparation is outside the timed interval, while process peak RSS includes that preparation. Validation and SQLite integrity checks are required before a row enters the aggregates.

Latency is material at ±3% only when both medians are at least 1 ms and measured ranges do not broadly overlap. Memory requires +5% and +4 MiB; fixture size requires +3% and +1 MiB; prolly I/O flags any +3% median increase.

## Machine and build metadata

```text
timestamp_utc=2026-07-14T23:59:53Z
current_revision=bf6fca50730e4dfc15c84e5ff328b9d2cec9a8af
baseline_revision=fa7c219afc7e1ee5769dd85e5223ea5dde9e3074
current_dirty=false
harness_sha256=6d89b3c8856bd157084a31de80654a68a7eded4f17e2a997e045b4dd1ded7268
current_binary_sha256=5971befd381eddd141e704159fd2d41a12da0163d6446addeba501bdb399ce96
baseline_binary_sha256=ae381d6bca341494681ea984f6d1a29a90d0401b283ab08f52da11cce0aa34b9
rustc=rustc 1.97.0 (2d8144b78 2026-07-07);binary: rustc;commit-hash: 2d8144b7880597b6e6d3dfd63a9a9efae3f533d3;commit-date: 2026-07-07;host: aarch64-apple-darwin;release: 1.97.0;LLVM version: 22.1.6;
cargo=cargo 1.97.0 (c980f4866 2026-06-30)
sqlite_cli=3.51.0 2025-06-12 13:14:41 f0ca7bba1c5e232e5d279fad6338121ab55af0c8c68c84cdfb18ba5114dcaapl (64-bit)
uname=Darwin Haipings-Mac-Studio.local 25.5.0 Darwin Kernel Version 25.5.0: Tue Jun  9 22:28:24 PDT 2026; root:xnu-12377.121.10~1/RELEASE_ARM64_T6020 arm64
cpu_count=12
memory_bytes=34359738368
filesystem=/dev/disk3s5 460Gi 410Gi 19Gi 96% 3.8M 198M 2% /System/Volumes/Data
copy_method=clonefile
sizes=1000 10000 50000 100000 1000000 10000000
runs=5
profiles=full normal
workloads=sorted_stream_build shuffled_batch_build random_reads_cold_manager random_reads_warm_manager clustered_reads_cold_manager clustered_reads_warm_manager right_edge_reads_cold_manager right_edge_reads_warm_manager append_batch_upserts random_batch_updates clustered_batch_updates random_batch_deletes clustered_batch_deletes identical_diff append_sparse_diff random_sparse_diff clustered_sparse_diff random_delete_diff clustered_delete_diff append_disjoint_sparse_merge random_disjoint_sparse_merge clustered_disjoint_sparse_merge random_conflict_resolved_merge clustered_conflict_resolved_merge
```
