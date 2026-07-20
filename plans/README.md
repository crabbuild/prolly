# Implementation Plans

This directory records implementation-ready plans derived from codebase
research. Plans are intentionally separate from source changes so their scope,
interfaces, compatibility constraints, and stop conditions can be reviewed
before implementation begins.

## Active plans

| ID | Priority | Effort | Risk | Status | Plan |
| --- | --- | --- | --- | --- | --- |
| 001 | P1 | L | High | Complete | [Direct deterministic proximity map](001-implement-direct-proximity-map.md) |
| 010 | P0 | L | High | In progress | [Selectable tree format and node layout](010-format-and-node-layout.md) |
| 011 | P0 | L | Medium | Proposed | [Selectable deterministic chunking](011-selectable-chunking.md) |
| 012 | P0 | XL | High | Proposed | [Canonical resynchronizing writer](012-canonical-writer.md) |
| 013 | P1 | L | Medium | Proposed | [Subtree cardinality and ordinal navigation](013-subtree-cardinality.md) |
| 014 | P1 | L | High | Proposed | [Logical and structural patches](014-structural-patches.md) |
| 015 | P1 | L | Medium | Proposed | [Bounded external bulk builder](015-external-builder.md) |
| 016 | P2 | M | Medium | Proposed | [Bounded read-through write session](016-write-session.md) |
| 017 | P2 | L | High | Proposed | [Offset-table layout and custom registries](017-offset-layout-and-registries.md) |

## Research records

- [Ordered prolly tree implementation study](000-dolt-prolly-study-and-rust-design.md) — approved
  design for selectable chunking and layouts, canonical mutation, subtree
  counts, structural patches, bounded ingestion, and write sessions.
- [Dolt proximity map synthesis](dolt-proximity-map-synthesis.md) — analysis of
  Dolt revision 6b2372c7d4ded1a54f55c6204304dbb72a33835c and Rust revision
  fa7c219, including the hierarchy, build/search/mutation algorithms, wire
  format, invariants, strengths, failure modes, and an adoption matrix.

## Dependency notes

- Plan 001 establishes a deterministic wire format, canonical bulk builder,
  exact directory, ANN search, and Dolt-style localized copy-on-write mutation
  whose results equal clean rebuilds.
- Generic graph traversal for sync/GC, root manifest integration, async APIs,
  quantization, and SIMD are follow-on work, not hidden prerequisites.
- The plan uses the crate's current xxHash64 and store traits. Adding a runtime
  or hashing dependency requires revisiting the plan.

## Considered and rejected for the first slice

- Reusing the ordered CRAB v1 node encoding for proximity nodes: rejected
  because separator keys and representative keys have different invariants.
- Treating the proximity hierarchy as the sole map: rejected because an ANN
  route cannot provide reliable absent-key detection or duplicate-vector
  identity semantics.
- Translating Dolt's Go files line-for-line: rejected for architectural,
  provenance, and licensing reasons.
- Shipping localized mutation before a canonical bulk builder: rejected
  because clean rebuild equality is the required mutation oracle.
- Coupling result count and beam width: rejected because recall tuning must be
  independent of the requested number of results.
- Adopting HNSW, quantization, or an external ANN dependency in the first
  implementation: rejected until the deterministic hierarchy has Rust-native
  recall and storage benchmarks.
