# Prolly Node/TypeScript Cookbook

The Node package exposes a native Node-API engine and async Promise wrappers.
Build the native module before running examples from the source tree.

```sh
npm --prefix crates/prolly/bindings/node ci
npm --prefix crates/prolly/bindings/node run build:native
node --test crates/prolly/bindings/node/test/*.test.ts
```

Runnable scenarios live as separate files under `examples/`, matching the Rust
example style:

```sh
npm --prefix crates/prolly/bindings/node ci
npm --prefix crates/prolly/bindings/node run build:native
npm --prefix crates/prolly/bindings/node run example:cookbook
npm --prefix crates/prolly/bindings/node run example:basic-map
npm --prefix crates/prolly/bindings/node run example:secondary-index
```

Application-style files include `batch-build.ts`, `local-first-state.ts`,
`resolver.ts`, `crdt-merge.ts`, `conversation-memory.ts`,
`agent-event-log.ts`, `background-compaction.ts`,
`deterministic-rag-snapshot.ts`, `document-chunk-index.ts`,
`vector-sidecar.ts`, `provenance-values.ts`, `materialized-view.ts`,
`filesystem-snapshot.ts`, and `durable-sqlite.ts`.

## Create A Durable Index

```ts
import { loadNative } from "./src/native.ts";

const native = await loadNative();
const engine = native.NativeProllyEngine.sqlite("app.prolly.db");

let tree = engine.create();
tree = engine.batch(tree, [
  { kind: "upsert", key: Buffer.from("user/1"), value: Buffer.from("Ada") },
  { kind: "upsert", key: Buffer.from("user/2"), value: Buffer.from("Linus") },
]);

engine.publishNamedRoot(Buffer.from("users/main"), tree);
```

## Use The Async Wrapper

```ts
import { AsyncProllyEngine } from "./src/async.ts";

const engine = await AsyncProllyEngine.sqlite("app.prolly.db");
let tree = await engine.create();
tree = await engine.put(tree, new TextEncoder().encode("k"), new TextEncoder().encode("v"));

const value = await engine.get(tree, new TextEncoder().encode("k"));
```

## Prefix Queries And Pages

```ts
const prefix = Buffer.from("user/");
const end = native.prefixEnd(prefix);

const entries = engine.range(tree, prefix, end);
for (const entry of entries) {
  console.log(Buffer.from(entry.key).toString(), Buffer.from(entry.value).toString());
}

let cursor = null;
for (;;) {
  const page = engine.rangePage(tree, cursor, null, "100");
  for (const entry of page.entries) {
    handle(entry.key, entry.value);
  }
  if (!page.nextCursor) break;
  cursor = page.nextCursor;
}

const diffs = engine.diffFromCursor(oldTree, newTree, { afterKey: Buffer.from("user/42") }, end);
```

## Merge Writers

```ts
const base = tree;
const left = engine.put(base, Buffer.from("user/1"), Buffer.from("Ada Lovelace"));
const right = engine.put(base, Buffer.from("user/1"), Buffer.from("Countess Ada"));

const merged = engine.merge(base, left, right, "prefer_right");

const callbackMerged = engine.mergeWithResolver(base, left, right, (conflict) => {
  if (conflict.left && conflict.right) {
    return {
      kind: "value",
      value: Buffer.concat([Buffer.from(conflict.left), Buffer.from(" | "), Buffer.from(conflict.right)]),
    };
  }
  return { kind: "unresolved" };
});
```

## Large Values And Blob GC

```ts
const blobStore = native.NativeProllyBlobStore.file("app.blobs");
const large = new Uint8Array(1_000_000);

tree = engine.putLargeValue(blobStore, tree, Buffer.from("doc/1"), large, {
  inlineThreshold: "4096",
});

const valueRef = engine.getValueRef(tree, Buffer.from("doc/1"));
const loaded = engine.getLargeValue(blobStore, tree, Buffer.from("doc/1"));

const plan = engine.planBlobStoreGc(blobStore, [tree]);
if (plan.reclaimableBlobCount !== "0") {
  engine.sweepBlobStoreGc(blobStore, [tree]);
}
```

## Custom Stores

`NativeHostStore` lets JavaScript own node persistence while Rust owns the tree
engine. The constructor accepts callbacks for node bytes, batch reads, hints,
node scans, named roots, CAS, root listing, and named-root manifest listing.

```ts
const nodes = new Map<string, Uint8Array>();
const roots = new Map<string, Uint8Array>();
const hex = (bytes: Uint8Array) => Buffer.from(bytes).toString("hex");

const hostStore = new native.NativeHostStore(
  ({ key }) => ({ value: nodes.get(hex(key)) }),
  ({ key, value }) => {
    nodes.set(hex(key), value);
    return {};
  },
  ({ key }) => {
    nodes.delete(hex(key));
    return {};
  },
  ({ ops }) => {
    for (const op of ops) {
      if (op.kind === "upsert" && op.value) nodes.set(hex(op.key), op.value);
      else nodes.delete(hex(op.key));
    }
    return {};
  },
  ({ keys }) => ({ values: keys.map((key) => ({ value: nodes.get(hex(key)) })) }),
  () => ({ value: true }),
  () => ({ value: false }),
  () => ({}),
  () => ({}),
  () => ({ values: [...nodes.keys()].map((key) => Buffer.from(key, "hex")) }),
  ({ name }) => ({ value: roots.get(hex(name)) }),
  ({ name, manifest }) => {
    roots.set(hex(name), manifest);
    return {};
  },
  ({ name }) => {
    roots.delete(hex(name));
    return {};
  },
  ({ name, expected, replacement }) => {
    const current = roots.get(hex(name));
    const same = Buffer.compare(Buffer.from(current ?? []), Buffer.from(expected ?? [])) === 0;
    if (!same) return { applied: false, current };
    if (replacement) roots.set(hex(name), replacement);
    else roots.delete(hex(name));
    return { applied: true };
  },
  () => ({ values: [...roots].map(([name, manifest]) => ({ name: Buffer.from(name, "hex"), manifest })) }),
);

const customEngine = native.NativeProllyEngine.customStore(hostStore);
```
