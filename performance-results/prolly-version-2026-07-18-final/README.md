# Version-operation benchmark artifacts

This directory contains the publishable aggregate artifacts from the complete
three-repetition 10K, 50K, 1M, 5M, and 10M native benchmark run.

- `report.md`: human-readable 10M comparison and Rust lifecycle results.
- `results-common.csv`: all 1,410 validated common-operation measurements.
- `results-lifecycle.csv`: all 195 validated Rust lifecycle measurements.
- `summary-common.csv` and `summary-lifecycle.csv`: medians and dispersion.
- `reproducibility.csv`: matrix and variability audit.
- `manifest.txt` and `machine.txt`: source, binary, toolchain, and host provenance.

The copied release binaries and per-process raw files are intentionally not
checked in. Their hashes are preserved in `manifest.txt`; the aggregate CSVs
retain every timed operation row. Run `scripts/run_prolly_version_comparison.sh`
with a fresh `BENCH_OUT` path to regenerate the complete raw artifact set.
