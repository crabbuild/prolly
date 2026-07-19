# Optimized Rust version-operation verification

This directory records the three-repetition, single-worker, warm in-memory
verification of `prolly-version-compare-v2` at 1,000,000 base records. It covers
the complete 0%, 1%, and 30% density matrix with append, clustered, and random
locality where applicable.

- `report.md` is the human-readable comparison and Rust lifecycle report.
- `results-common.csv` and `results-lifecycle.csv` contain every validated row.
- `summary-common.csv` and `summary-lifecycle.csv` contain medians and variation.
- `reproducibility.csv` records parity and repetition checks.
- `scale-10m-30-random.csv` records the three-repetition 10M/30%-random scale
  check made with the exact copied binaries.
- `manifest.txt` and `machine.txt` record source, runner, toolchain, and machine
  provenance.

All expected cells and repetitions are present. Cross-language workload,
operation-count, logical-result, cardinality, and conflict-count checks pass.
Rust wins 36 of 47 common-operation medians; Dolt Go wins 11. Two
short-duration groups change winner direction across repetitions.

The v2 Rust patch is a verified, content-addressed target-root envelope intended
for same-store version operations. The referenced target subtree must already
exist in the destination store; transferring missing nodes remains a separate
snapshot synchronization concern. In the 10M scale check, Rust reports one
native patch envelope and Dolt Go reports 30 native structural patches; both
represent the same 3,000,000 logical changes and produce the same result digest.
