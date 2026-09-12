# TurboQuant Provenance Record

## Research basis

The algorithmic reference is
[TurboQuant: Online Vector Quantization with Near-optimal Distortion Rate](https://arxiv.org/abs/2504.19874),
arXiv:2504.19874v1, submitted April 28, 2025, by Amir Zandieh, Majid Daliri,
Majid Hadian, and Vahab Mirrokni. The
[Google Research overview](https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/)
is explanatory background, not the source-level specification.

The paper page identifies its text as CC BY 4.0. This implementation is source
code under the repository's MIT OR Apache-2.0 license and includes attribution
without implying endorsement by the paper's authors or Google. The paper's
license is not treated as a patent grant.

## Independent implementation declaration

The production implementation was written against the approved Prolly design,
the public paper's mathematical description, and the existing Prolly PQ/HNSW
integration boundaries. It does not link, copy, translate, or persist Turbovec
source code, APIs, or wire formats.

An earlier feasibility discussion considered whether Turbovec could be
integrated. That review is disclosed here because it motivated choosing a
Prolly-native implementation. It did not provide source-level material for the
implementation. The frozen `TQTQ` manifest, structured transform, scalar
codebooks, packed-code layout, content kinds, and binding APIs are Prolly
formats.

## Engineering deviation

The GA candidate generator implements the MSE-oriented rotate-then-scalar-
quantize structure. It uses two deterministic signed/permuted Hadamard rounds
instead of a dense Gaussian-QR/Haar rotation. This keeps construction and
search bounded and portable across CPU and browser WASM targets, but it means
the dense-rotation theorem from the paper is not claimed. Quality and
performance claims require Prolly's retained empirical evidence.

The product-oriented residual/QJL estimator is not implemented. Adding it
requires a separately approved design and independent qualification.

## Release disposition

- Provenance owner: the Prolly release maintainer approving the release.
- Legal/patent disposition owner: the organization or maintainer authorizing
  commercial/default enablement.
- Current legal/patent disposition: pending; no approval is inferred by this
  technical record.
- Enforcement: TurboQuant remains excluded from `Auto`, and the release ledger
  treats the missing disposition as a closed GA gate.

The approving owner must replace the pending disposition with a dated,
reviewable decision before commercial release or default enablement. Technical
contributors must not convert successful tests or benchmarks into legal
approval.
