# Dolt Go vs Rust Prolly Performance

All figures are medians from process-isolated, single-worker, in-memory runs. Lower nanoseconds per operation is better. Winner selection uses unrounded elapsed nanoseconds divided by logical operations.

Rust wins: 88; Dolt Go wins: 2; ties: 0.

| Operation | Rust wins | Dolt Go wins | Ties |
|---|---:|---:|---:|
| write | 28 | 2 | 0 |
| point_read | 30 | 0 | 0 |
| range_scan | 30 | 0 | 0 |

| Records | Phase | Workload | Operation | Runs | Rust ns/op | Dolt Go ns/op | Rust CV | Dolt CV | Winner | Speedup |
|---:|---|---|---|---:|---:|---:|---:|---:|---|---:|
| 10000 | fresh | append | point_read | 3 | 144.483 | 367.946 | 0.007618 | 0.003089 | rust | 2.546632x |
| 10000 | fresh | append | range_scan | 3 | 4.517 | 20.475 | 0.007356 | 0.001343 | rust | 4.533177x |
| 10000 | fresh | append | write | 3 | 281.587 | 618.579 | 0.007224 | 0.041990 | rust | 2.196757x |
| 10000 | fresh | clustered | point_read | 3 | 145.525 | 367.350 | 0.055603 | 0.013062 | rust | 2.524309x |
| 10000 | fresh | clustered | range_scan | 3 | 4.583 | 20.679 | 0.063408 | 0.009295 | rust | 4.511836x |
| 10000 | fresh | clustered | write | 3 | 282.587 | 644.888 | 0.064056 | 0.011817 | rust | 2.282081x |
| 10000 | fresh | random | point_read | 3 | 144.387 | 367.954 | 0.101474 | 0.001568 | rust | 2.548380x |
| 10000 | fresh | random | range_scan | 3 | 4.854 | 20.517 | 0.094225 | 0.009330 | rust | 4.226587x |
| 10000 | fresh | random | write | 3 | 358.925 | 713.571 | 0.084682 | 0.002400 | rust | 1.988078x |
| 10000 | mutation | append | point_read | 3 | 101.077 | 332.667 | 0.006579 | 0.005708 | rust | 3.291222x |
| 10000 | mutation | append | range_scan | 3 | 5.449 | 21.109 | 0.017976 | 0.005328 | rust | 3.874126x |
| 10000 | mutation | append | write | 3 | 272.528 | 792.639 | 0.015091 | 0.016769 | rust | 2.908468x |
| 10000 | mutation | clustered | point_read | 3 | 92.721 | 335.192 | 0.035804 | 0.030844 | rust | 3.615058x |
| 10000 | mutation | clustered | range_scan | 3 | 5.348 | 21.290 | 0.052740 | 0.029294 | rust | 3.981024x |
| 10000 | mutation | clustered | write | 3 | 596.333 | 643.222 | 0.068153 | 0.033489 | rust | 1.078629x |
| 10000 | mutation | random | point_read | 3 | 144.064 | 372.314 | 0.001379 | 0.001353 | rust | 2.584365x |
| 10000 | mutation | random | range_scan | 3 | 5.420 | 20.583 | 0.020527 | 0.005015 | rust | 3.797475x |
| 10000 | mutation | random | write | 3 | 1774.708 | 1430.486 | 0.003960 | 0.004412 | dolt-go | 1.240633x |
| 50000 | fresh | append | point_read | 3 | 165.770 | 428.072 | 0.032483 | 0.025816 | rust | 2.582323x |
| 50000 | fresh | append | range_scan | 3 | 4.404 | 20.609 | 0.040592 | 0.033627 | rust | 4.679476x |
| 50000 | fresh | append | write | 3 | 270.795 | 666.183 | 0.049576 | 0.038121 | rust | 2.460099x |
| 50000 | fresh | clustered | point_read | 3 | 165.083 | 429.299 | 0.002303 | 0.003646 | rust | 2.600513x |
| 50000 | fresh | clustered | range_scan | 3 | 4.344 | 20.621 | 0.007050 | 0.003267 | rust | 4.746796x |
| 50000 | fresh | clustered | write | 3 | 313.082 | 694.950 | 0.008883 | 0.026665 | rust | 2.219708x |
| 50000 | fresh | random | point_read | 3 | 165.643 | 430.657 | 0.003197 | 0.020875 | rust | 2.599903x |
| 50000 | fresh | random | range_scan | 3 | 4.468 | 20.732 | 0.015539 | 0.031758 | rust | 4.639678x |
| 50000 | fresh | random | write | 3 | 342.349 | 804.433 | 0.002918 | 0.075779 | rust | 2.349745x |
| 50000 | mutation | append | point_read | 3 | 102.658 | 374.479 | 0.002647 | 0.042605 | rust | 3.647840x |
| 50000 | mutation | append | range_scan | 3 | 4.654 | 21.261 | 0.043756 | 0.053969 | rust | 4.568456x |
| 50000 | mutation | append | write | 3 | 255.244 | 934.697 | 0.026863 | 0.028342 | rust | 3.661969x |
| 50000 | mutation | clustered | point_read | 3 | 92.881 | 361.540 | 0.032198 | 0.021832 | rust | 3.892514x |
| 50000 | mutation | clustered | range_scan | 3 | 4.487 | 21.333 | 0.011980 | 0.030818 | rust | 4.754523x |
| 50000 | mutation | clustered | write | 3 | 470.103 | 757.572 | 0.034545 | 0.025351 | rust | 1.611503x |
| 50000 | mutation | random | point_read | 3 | 168.756 | 437.040 | 0.004731 | 0.002468 | rust | 2.589770x |
| 50000 | mutation | random | range_scan | 3 | 4.385 | 20.844 | 0.007091 | 0.002258 | rust | 4.752984x |
| 50000 | mutation | random | write | 3 | 1741.114 | 1557.394 | 0.008632 | 0.086641 | dolt-go | 1.117966x |
| 1000000 | fresh | append | point_read | 3 | 408.417 | 890.265 | 0.012806 | 0.002931 | rust | 2.179795x |
| 1000000 | fresh | append | range_scan | 3 | 4.679 | 23.742 | 0.044718 | 0.002746 | rust | 5.074305x |
| 1000000 | fresh | append | write | 3 | 270.289 | 957.194 | 0.006222 | 0.019268 | rust | 3.541375x |
| 1000000 | fresh | clustered | point_read | 3 | 403.792 | 923.818 | 0.053323 | 0.067683 | rust | 2.287856x |
| 1000000 | fresh | clustered | range_scan | 3 | 4.533 | 24.224 | 0.023160 | 0.016918 | rust | 5.344009x |
| 1000000 | fresh | clustered | write | 3 | 371.211 | 836.846 | 0.020528 | 0.023270 | rust | 2.254365x |
| 1000000 | fresh | random | point_read | 3 | 403.128 | 943.063 | 0.030206 | 0.012936 | rust | 2.339364x |
| 1000000 | fresh | random | range_scan | 3 | 4.590 | 24.880 | 0.030581 | 0.002988 | rust | 5.419887x |
| 1000000 | fresh | random | write | 3 | 525.397 | 2330.478 | 0.054921 | 0.018231 | rust | 4.435650x |
| 1000000 | mutation | append | point_read | 3 | 128.653 | 470.667 | 0.003454 | 0.012482 | rust | 3.658425x |
| 1000000 | mutation | append | range_scan | 3 | 5.151 | 24.168 | 0.018458 | 0.049749 | rust | 4.692252x |
| 1000000 | mutation | append | write | 3 | 269.474 | 1023.216 | 0.000326 | 0.043906 | rust | 3.797085x |
| 1000000 | mutation | clustered | point_read | 3 | 110.106 | 471.683 | 0.017656 | 0.024768 | rust | 4.283901x |
| 1000000 | mutation | clustered | range_scan | 3 | 5.059 | 24.061 | 0.055650 | 0.008981 | rust | 4.756491x |
| 1000000 | mutation | clustered | write | 3 | 497.326 | 707.739 | 0.027482 | 0.035344 | rust | 1.423091x |
| 1000000 | mutation | random | point_read | 3 | 396.709 | 1122.100 | 0.000075 | 0.066308 | rust | 2.828519x |
| 1000000 | mutation | random | range_scan | 3 | 4.718 | 25.974 | 0.002094 | 0.022148 | rust | 5.505629x |
| 1000000 | mutation | random | write | 3 | 1980.422 | 4025.504 | 0.040720 | 0.024689 | rust | 2.032649x |
| 5000000 | fresh | append | point_read | 3 | 676.758 | 1159.733 | 0.004714 | 0.006543 | rust | 1.713660x |
| 5000000 | fresh | append | range_scan | 3 | 4.984 | 33.055 | 0.017283 | 0.022792 | rust | 6.631783x |
| 5000000 | fresh | append | write | 3 | 272.293 | 1028.678 | 0.016068 | 0.008783 | rust | 3.777834x |
| 5000000 | fresh | clustered | point_read | 3 | 673.431 | 1171.149 | 0.059932 | 0.011329 | rust | 1.739077x |
| 5000000 | fresh | clustered | range_scan | 3 | 5.117 | 34.198 | 0.014276 | 0.014360 | rust | 6.682810x |
| 5000000 | fresh | clustered | write | 3 | 394.239 | 861.234 | 0.010237 | 0.023002 | rust | 2.184548x |
| 5000000 | fresh | random | point_read | 3 | 675.337 | 1236.780 | 0.005917 | 0.048148 | rust | 1.831352x |
| 5000000 | fresh | random | range_scan | 3 | 5.018 | 36.632 | 0.011484 | 0.018987 | rust | 7.300723x |
| 5000000 | fresh | random | write | 3 | 748.630 | 2542.648 | 0.003181 | 0.028477 | rust | 3.396399x |
| 5000000 | mutation | append | point_read | 3 | 125.594 | 496.205 | 0.000309 | 0.042960 | rust | 3.950873x |
| 5000000 | mutation | append | range_scan | 3 | 5.340 | 34.054 | 0.081618 | 0.093234 | rust | 6.376964x |
| 5000000 | mutation | append | write | 3 | 286.803 | 1070.985 | 0.036527 | 0.005243 | rust | 3.734214x |
| 5000000 | mutation | clustered | point_read | 3 | 111.807 | 493.080 | 0.004101 | 0.045623 | rust | 4.410117x |
| 5000000 | mutation | clustered | range_scan | 3 | 5.473 | 35.898 | 0.031304 | 0.046830 | rust | 6.558752x |
| 5000000 | mutation | clustered | write | 3 | 483.650 | 730.561 | 0.011955 | 0.005638 | rust | 1.510515x |
| 5000000 | mutation | random | point_read | 3 | 728.940 | 1653.751 | 0.015972 | 0.012332 | rust | 2.268708x |
| 5000000 | mutation | random | range_scan | 3 | 5.388 | 38.692 | 0.019049 | 0.025592 | rust | 7.180870x |
| 5000000 | mutation | random | write | 3 | 2110.098 | 10058.776 | 0.002646 | 0.016826 | rust | 4.766971x |
| 10000000 | fresh | append | point_read | 3 | 973.806 | 1745.676 | 0.060187 | 0.071933 | rust | 1.792633x |
| 10000000 | fresh | append | range_scan | 3 | 6.128 | 41.302 | 0.025731 | 0.018587 | rust | 6.739758x |
| 10000000 | fresh | append | write | 3 | 284.259 | 1078.917 | 0.027099 | 0.017381 | rust | 3.795541x |
| 10000000 | fresh | clustered | point_read | 3 | 1052.953 | 1805.868 | 0.042626 | 0.008233 | rust | 1.715052x |
| 10000000 | fresh | clustered | range_scan | 3 | 5.943 | 42.326 | 0.013404 | 0.005368 | rust | 7.121551x |
| 10000000 | fresh | clustered | write | 3 | 414.863 | 928.817 | 0.082225 | 0.022343 | rust | 2.238855x |
| 10000000 | fresh | random | point_read | 3 | 1042.014 | 1801.918 | 0.031659 | 0.015625 | rust | 1.729265x |
| 10000000 | fresh | random | range_scan | 3 | 5.888 | 45.227 | 0.007498 | 0.009877 | rust | 7.681128x |
| 10000000 | fresh | random | write | 3 | 845.166 | 7447.262 | 0.090840 | 0.045192 | rust | 8.811593x |
| 10000000 | mutation | append | point_read | 3 | 135.575 | 582.000 | 0.009045 | 0.143608 | rust | 4.292824x |
| 10000000 | mutation | append | range_scan | 3 | 6.875 | 43.320 | 0.008435 | 0.228740 | rust | 6.301425x |
| 10000000 | mutation | append | write | 3 | 333.334 | 1144.025 | 0.091519 | 0.002291 | rust | 3.432067x |
| 10000000 | mutation | clustered | point_read | 3 | 122.241 | 547.226 | 0.029028 | 0.018596 | rust | 4.476609x |
| 10000000 | mutation | clustered | range_scan | 3 | 5.958 | 41.183 | 0.164827 | 0.009358 | rust | 6.912468x |
| 10000000 | mutation | clustered | write | 3 | 510.930 | 768.617 | 0.024976 | 0.001014 | rust | 1.504350x |
| 10000000 | mutation | random | point_read | 3 | 1043.201 | 4633.052 | 0.741260 | 0.422240 | rust | 4.441188x |
| 10000000 | mutation | random | range_scan | 3 | 6.049 | 67.932 | 0.607021 | 0.314126 | rust | 11.230607x |
| 10000000 | mutation | random | write | 3 | 2547.499 | 12886.517 | 0.034392 | 0.025813 | rust | 5.058498x |

## Limitations

This compares native product paths with each implementation's default encoding, chunking, allocator, runtime, and in-memory store. It does not isolate language runtime speed. Three observations expose gross variance but do not establish statistical significance; inspect min/max and CV before treating a narrow winner as robust.

Peak RSS covers the entire scenario process, including untimed fixture and tuple preparation. Timing rows exclude that preparation.
