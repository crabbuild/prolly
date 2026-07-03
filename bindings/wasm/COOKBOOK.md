# Prolly WASM Cookbook

The WASM package targets browser and worker environments. It exposes the Rust
memory engine, wire helpers, ranges, diffs, structural diffs, and built-in merge
policies. Persistent stores and host-store callbacks are not part of the browser
package.

## Build

```sh
cargo check -p prolly-wasm --target wasm32-unknown-unknown
npm --prefix crates/prolly/bindings/wasm test
```

Release builds should run `wasm-pack build` or the package build command used
by CI.

Runnable browser/worker scenarios live as separate files under `examples/`.
They require generated `pkg/` artifacts:

```sh
npm --prefix crates/prolly/bindings/wasm run build:wasm
npm --prefix crates/prolly/bindings/wasm run example:cookbook
npm --prefix crates/prolly/bindings/wasm run example:basic-map
npm --prefix crates/prolly/bindings/wasm run example:secondary-index
```

Browser-safe application files include `batch-build.ts`,
`local-first-state.ts`, `resolver.ts`, `conversation-memory.ts`,
`agent-event-log.ts`, `background-compaction.ts`,
`deterministic-rag-snapshot.ts`, `document-chunk-index.ts`,
`vector-sidecar.ts`, `provenance-values.ts`, `materialized-view.ts`, and
`browser-storage.ts`.

## Create A Browser Snapshot

```ts
import init, { WasmProllyEngine, WasmRangeCursor } from "@crabdb/prolly-wasm";

await init();

const text = new TextEncoder();
const engine = WasmProllyEngine.memory();

let tree = engine.create();
tree = engine.put(tree, text.encode("todo/1"), text.encode("write cookbook"));
tree = engine.put(tree, text.encode("todo/2"), text.encode("ship bindings"));
```

## Query And Page In The UI

```ts
const prefix = text.encode("todo/");
const rows = engine.range(tree, prefix, null);

let cursor = null;
for (;;) {
  const page = engine.rangePage(tree, cursor, null, 50);
  renderRows(page.entries);
  if (!page.nextCursor) break;
  cursor = page.nextCursor;
}

const diffs = engine.diffFromCursor(oldTree, newTree, new WasmRangeCursor(text.encode("todo/42")), null);
```

## Diff Two UI States

```ts
const next = engine.put(tree, text.encode("todo/1"), text.encode("done"));
const diffs = engine.diff(tree, next);

const structural = engine.structuralDiffPage(tree, next, null, 100);
console.log(structural.stats);
```

## Merge Local And Remote Edits

```ts
const base = tree;
const local = engine.put(base, text.encode("todo/1"), text.encode("local"));
const remote = engine.put(base, text.encode("todo/1"), text.encode("remote"));

const merged = engine.merge(base, local, remote, "prefer_right");
const explanation = engine.mergeExplain(base, local, remote, "prefer_right");
console.log(JSON.parse(explanation.traceJson));
```

## Inspect Stats For Debug Panels

```ts
const stats = JSON.parse(engine.collectStatsJson(tree));
const textView = engine.debugTreeText(tree);
const comparison = JSON.parse(engine.debugCompareTreesJson(tree, merged));
```

## Browser Persistence Pattern

WASM snapshots are immutable values. Store the root bytes plus application
metadata in IndexedDB or another browser database, then rebuild an engine and
replay or sync nodes according to your application protocol.

For durable node storage, use the Node-API or UniFFI-backed server bindings and
sync the browser with missing-node plans or application-level snapshots.
