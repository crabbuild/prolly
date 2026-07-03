# Prolly Tree Documentation

This folder is the end-user documentation set for the `prolly-map` package.
The package publishes a Rust library crate named `prolly`, so users add
`prolly-map` to `Cargo.toml` and write Rust imports such as:

```rust
use prolly::{Config, MemStore, Prolly};
```

The Rust implementation is the source of truth today. The design is intended
to be portable, so future Python and other language bindings should follow the
same byte ordering, node encoding contracts, merge semantics, and conformance
tests described here.

## Documentation Map

- [Getting Started](getting-started.md): install the crate, build your first
  tree, run examples, choose features, and understand the core mental model.
- [Guides](guides.md): practical guidance for keys, values, range scans,
  named roots, merge resolvers, async storage, large values, GC, sync, and
  operational inspection.
- [Async Store](async-store.md): design track for async-native stores,
  sync-store adapters, object-store backends, browser/WASM storage, and remote
  sync.
- [Object-Store VCS Design](object-store-vcs-design.md): technical design for
  direct object-store node/blob storage, distributed ref CAS, publish protocol,
  and GC for Git-like version-control systems.
- [Cookbook](cookbook.md): application recipes for local-first state, RAG
  indexes, agent memory, event logs, compaction, vector sidecars, provenance,
  secondary indexes, materialized views, blob storage, durable SQLite, object
  stores, and browser storage.
- [Architecture](architecture.md): the major components and how data flows
  through the tree, store, manifest, diff, merge, and sync layers.
- [Design Spec](design-spec.md): normative behavior for ordering, content
  addressing, conflict resolution, stores, manifests, large values, GC, and
  cross-language compatibility.
- [Implementation](implementation.md): how the Rust crate is organized, how
  read/write/batch/diff/merge paths work, and where to extend it safely.
- [Performance](performance.md): optimization principles, tuning guidance,
  benchmark harnesses, current evidence, workload playbooks, and future
  performance work.
- [Roadmap](roadmap.md): the canonical roadmap for public `0.1`,
  compatibility, production hardening, async/remote storage, AI-native
  primitives, language bindings, and collaboration.
- [Language Ports](language-ports.md): legacy native-port notes for Python and
  TypeScript compatibility work.
- [Language Bindings Design](language-bindings-design.md): technical design
  for exposing the Rust implementation through UniFFI and language-specific
  binding adapters.

## What Prolly Gives You

Prolly trees are content-addressed ordered maps. They feel like immutable B+
trees, but their node boundaries are chosen from content rather than fixed
page numbers. That gives the tree stable structure across independently edited
versions, which makes diff, merge, sync, and storage reuse efficient.

Use `prolly-map` when you want:

- Ordered byte-key lookup and range scans.
- Immutable snapshots with cheap branching.
- Stable content IDs for tree nodes.
- Structural sharing across versions.
- Efficient diffs by skipping equal subtrees.
- Three-way merge with application-defined conflict resolution.
- Named roots for durable branch heads, checkpoints, and published views.
- Storage backends that can be local, embedded, object-store based, remote, or
  browser/WASM backed.

## Current Status

The crate is Rust-only for now. Public APIs are being prepared for an open
source `0.1`, so a few breaking cleanups are still acceptable when they make
the long-term contract clearer. The important compatibility boundary is:

- key ordering is raw byte lexicographic ordering;
- node CIDs are derived from deterministic node bytes;
- unchanged content should keep the same CID across updates;
- delete/absence state is represented explicitly in merge conflicts;
- durable roots are small manifests that point to immutable tree snapshots.

For implementation task tracking and user-facing sequencing, use
[`roadmap.md`](roadmap.md). This folder explains the user-facing model and
future direction.
