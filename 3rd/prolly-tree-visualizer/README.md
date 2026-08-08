# Prolly Tree Visualizer

An interactive browser visualizer for the real trees produced by this
repository's Rust `prolly-map` engine. The interaction model is inspired by
[btree.app](https://btree.app/), but mutations execute in the
`@crabbuild/prolly-wasm` binding rather than in a JavaScript tree simulation.

The visualizer demonstrates:

- content-defined leaf boundaries and node splits;
- SHA-256 content IDs for leaf and internal nodes;
- copy-on-write updates and structural sharing between versions;
- point and range lookup paths through lower-bound separators;
- hash-pruned structural diffs;
- history-independent builds from different insertion orders;
- inserts, updates, deletes, random writes, and sequential growth;
- live and historical encoded-node storage.

## Run it

Prerequisites: Node.js 18 or newer, npm, and the checked-in generated WASM
artifacts under `bindings/wasm/pkg/`.

```bash
npm --prefix 3rd/prolly-tree-visualizer install
npm --prefix 3rd/prolly-tree-visualizer run dev
```

The app starts a fresh in-memory Rust engine on each page load. It runs fully
inside the browser and sends no rows to a server.

For a production build:

```bash
npm --prefix 3rd/prolly-tree-visualizer run build
npm --prefix 3rd/prolly-tree-visualizer run preview
```

## Engine data path

```text
browser control
  → @crabbuild/prolly-wasm
  → Rust prolly-map mutation
  → content-addressed in-memory node store
  → typed Rust debug traversal and real node CIDs
  → SVG visualization
```

`src/engine.ts` owns the WASM memory engine and persistent tree handle.
`src/prolly.ts` converts the binding's deterministic breadth-first debug view
into the hierarchy consumed by the existing canvas. Internal routing uses this
engine's lower-bound separator convention. Leaf rows come back through the
binding's ordered range API, while node levels, encoded sizes, ranges, and CIDs
come from Rust diagnostics.

The storage view tracks every distinct encoded node observed in the current
in-memory engine. Garbage collection rebuilds HEAD in a fresh engine and
verifies that its root CID remains unchanged before replacing the old store.

## Attribution

This project is based on
[`timsehn/prolly-tree-visualizer`](https://github.com/timsehn/prolly-tree-visualizer).
Credit goes to Tim Sehn and the original contributors for the visualizer's
design and implementation. This vendored version adapts the original project
to use the `prolly-map` WASM engine.

## Validation

```bash
npm --prefix 3rd/prolly-tree-visualizer test
npm --prefix 3rd/prolly-tree-visualizer run build
npm --prefix 3rd/prolly-tree-visualizer run smoke:wasm
```

## Project layout

```text
src/engine.ts                    prolly WASM mutation and snapshot adapter
src/prolly.ts                    debug-view hierarchy, search, and diff helpers
src/components/TreeCanvas.tsx    SVG tree layout and version highlighting
src/components/NodeInspector.tsx selected node details
src/App.tsx                      controls, views, timeline, guided demos
```
