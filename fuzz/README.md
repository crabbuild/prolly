# Fuzzing

The TurboQuant fuzz targets supplement the deterministic 10,000-case unit
smoke with coverage-guided execution through public production APIs.

- `proximity_turboquant_decode` feeds at most 4 KiB of arbitrary bytes into
  source-bound manifest loading and bounded typed-content traversal.
- `proximity_turboquant_lifecycle` constructs at most eight 64-dimensional
  records, exercises every metric, bit width, and query kernel, then corrupts
  one authenticated ordered node and requires a fresh reopen or verification
  to fail closed.

Run bounded local smoke coverage with:

```sh
cargo +nightly fuzz run proximity_turboquant_decode -- \
  -runs=2048 -max_len=4096 -timeout=5
cargo +nightly fuzz run proximity_turboquant_lifecycle -- \
  -runs=256 -max_len=1024 -timeout=5
```

Long-running fuzz campaigns should retain corpora outside the repository and
must preserve the same target-side allocation and record-count bounds.
