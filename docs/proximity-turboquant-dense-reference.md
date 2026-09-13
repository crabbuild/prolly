# TurboQuant Dense-Reference Comparison

This report records the development-only comparison required by Phase 1 of
the native TurboQuant design. It is not a production benchmark, a portable
latency claim, or evidence sufficient to enable `Auto`.

## Method

[`scripts/turboquant_dense_reference.py`](../scripts/turboquant_dense_reference.py)
implements the paper's Algorithm 1 reference rotation by QR-decomposing a
dense matrix of independent standard-normal values. The signs of the QR
factor are canonicalized by multiplying each column of `Q` by the sign of the
corresponding diagonal entry of `R`.

The same program independently reproduces Prolly's two-round structured
rotation and refuses to run unless that reproduction matches every frozen
SplitMix, plan, and rotation-output fixture. Both paths use the checked-in
2/3/4-bit Lloyd-Max tables. Neither the script nor its output is linked,
loaded, or consulted by production code.

The retained run used:

- source implementation revision `8a90bb970214d27811595a75db1b3bb0ba4721f9`;
- 1,024 deterministic synthetic records and 32 queries per dimension;
- dimensions 128, 200, 768, 1536, and 3072;
- bit widths 2, 3, and 4;
- L2 squared, cosine, and inner-product recall@10;
- an 8x shortlist (80 candidates from 1,024 records);
- seed zero and NumPy PCG64 for the development data and dense matrix;
- Python 3.12.13 and NumPy 2.5.3 on arm64 macOS 26.5.2.

Raw inputs, environment metadata, generator digest, distortion, norm error,
and recall values are retained in
[`dense-reference-2026-09-12-arm64.json`](../performance-results/proximity-turboquant/dense-reference-2026-09-12-arm64.json).

Reproduce the run with:

```sh
uv run scripts/turboquant_dense_reference.py \
  --records 1024 \
  --queries 32 \
  --output performance-results/proximity-turboquant/dense-reference-2026-09-12-arm64.json
```

## Results

The following table shows mean routing MSE for structured/dense rotation and
recall@10 for structured rotation in L2/cosine/inner-product order. The raw
artifact also contains maximum MSE, norm error, and dense-reference recall.

| Dimensions | Bits | Mean MSE structured / dense | Structured recall@10 L2 / cosine / IP |
| ---: | ---: | ---: | ---: |
| 128 | 2 | 0.116490 / 0.115731 | 1.000 / 0.991 / 0.984 |
| 128 | 3 | 0.034171 / 0.034001 | 1.000 / 1.000 / 1.000 |
| 128 | 4 | 0.009414 / 0.009292 | 1.000 / 1.000 / 1.000 |
| 200 | 2 | 0.116524 / 0.116051 | 0.997 / 0.997 / 0.997 |
| 200 | 3 | 0.034161 / 0.034107 | 1.000 / 1.000 / 1.000 |
| 200 | 4 | 0.009394 / 0.009360 | 1.000 / 1.000 / 1.000 |
| 768 | 2 | 0.117217 / 0.116854 | 1.000 / 1.000 / 0.997 |
| 768 | 3 | 0.034494 / 0.034306 | 1.000 / 1.000 / 1.000 |
| 768 | 4 | 0.009460 / 0.009438 | 1.000 / 1.000 / 1.000 |
| 1536 | 2 | 0.117238 / 0.117427 | 1.000 / 0.994 / 0.994 |
| 1536 | 3 | 0.034500 / 0.034409 | 1.000 / 1.000 / 1.000 |
| 1536 | 4 | 0.009492 / 0.009463 | 1.000 / 1.000 / 1.000 |
| 3072 | 2 | 0.117437 / 0.117389 | 0.997 / 0.994 / 0.994 |
| 3072 | 3 | 0.034562 / 0.034482 | 1.000 / 1.000 / 1.000 |
| 3072 | 4 | 0.009509 / 0.009485 | 1.000 / 1.000 / 1.000 |

Across these 15 development rows, structured mean MSE is between 0.9984 and
1.0132 times dense Gaussian-QR MSE. The default four-bit configuration has
recall@10 of 1.0 for every checked dimension and metric in this fixture.

This closes the development-oracle comparison requirement. It does not close
the GA recall gate: the larger retained dataset/store matrix remains required,
and no aggregate here may hide a failing production qualification row.
