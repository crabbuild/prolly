# Turso scale baseline implementation plan

1. Rename the mixed `sqlite-turso-local` harness to standalone `benchmarks/turso-scale` and retain historical comparison outputs.
2. Replace the dual-adapter matrix with the SQLite-compatible 25-cell Turso-only workload contract.
3. Implement async sorted build, reads, scans, mutations, diff, merge, exact validation, stats, persistence, resume, and reporting.
4. Harden `scripts/run_turso_scale_benchmark.sh` with disk guards, local-only feature verification, logs, and source/binary provenance.
5. Run formatting, tests, clippy, shell checks, and a release smoke matrix.
6. Run three independent 1M fixtures into `performance-results/turso/baseline` and validate 75 raw rows, three fixture rows, 25 summary groups, and artifact checksums.
