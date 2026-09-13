# TurboQuant Fault-Injection Report

This report records deterministic store-boundary fault coverage for the native
TurboQuant accelerator. The executable evidence is in
[`tests/proximity_turboquant.rs`](../tests/proximity_turboquant.rs), in:

- `turboquant_fails_closed_at_every_publication_boundary`; and
- `turboquant_fails_closed_at_every_manifest_tree_proof_and_rerank_read`.

## Method

The test store wraps an immutable in-memory store and numbers every cold
`Store::get` and `Store::publish_nodes` boundary. It first runs each lifecycle
without a fault to discover the complete boundary count. It then reconstructs
cold map and accelerator handles and repeats the lifecycle once for every
ordinal, failing exactly that store operation.

Every injected read must return `Error::Store`; explicit TurboQuant execution
must not fall back to another backend. Every injected publication must fail the
build, stop subsequent publication calls, and leave the expected manifest CID
absent. Unreachable code-tree descendants from a failed build remain safe for
ordinary content-addressed GC.

The fixture uses 193 records at 128 dimensions so the source directory,
proximity hierarchy, and TurboQuant code tree all cross real persisted tree
boundaries. On revision `8a90bb970214d27811595a75db1b3bb0ba4721f9` plus
the fault-harness change, it exercised:

| Lifecycle | Store boundaries injected |
| --- | ---: |
| TurboQuant code-tree and manifest publication | 2 writes |
| cold load plus full source/code verification | 16 reads |
| cold load plus forced scan and authoritative rerank | 16 reads |
| cold load plus search-proof generation and closure collection | 43 reads |

The ordinal loops are derived from the successful run rather than frozen to
these counts. Future tree-shape or proof-closure changes therefore expand the
fault matrix automatically instead of silently leaving new read boundaries
untested.

Run the retained check with:

```sh
cargo test --test proximity_turboquant \
  turboquant_fails_closed_at_every_ -- --nocapture
```

Malformed bytes, missing objects, CID mismatches, and noncanonical
padding/norms are covered separately by the same test module and
`tests/proximity_wire.rs`. The deterministic bounded fuzz smoke in the
TurboQuant unit and proof tests exercises arbitrary manifest bytes, arbitrary
packed code bits and lengths, supported and maximum-size transform derivation,
code scoring for every metric, and randomized proof-transcript replay
mutations. Every case must finish without panic or unbounded allocation, and
every altered proof transcript must fail closed.
`every_turboquant_manifest_binding_mutation_fails_closed` separately mutates
each semantic manifest binding named by the design: source CID, dimensions,
metric, count, seed, transform ID, codebook ID, bit width, quality bits,
zero-vector count, code root, and configuration fingerprint. Each candidate is
rehash-addressed where applicable and must fail either trusted open or full
source verification.
An old TurboQuant proof version is rejected immediately with the typed
`UnsupportedProximityVersion` error and exact required version, before request
commitment or authenticated closure work.
