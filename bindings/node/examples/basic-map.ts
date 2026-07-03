import assert from "node:assert/strict";

import { loadNative } from "../src/native.ts";

const native = await loadNative();
const bytes = (value: string): Buffer => Buffer.from(value);
const text = (value: Uint8Array | null | undefined): string | null =>
  value == null ? null : Buffer.from(value).toString();

const engine = native.NativeProllyEngine.memory();
let tree = engine.create();
tree = engine.put(tree, bytes("user:001"), bytes("Ada"));
tree = engine.put(tree, bytes("user:002"), bytes("Grace"));
tree = engine.put(tree, bytes("user:003"), bytes("Linus"));

assert.equal(text(engine.get(tree, bytes("user:001"))), "Ada");

tree = engine.delete(tree, bytes("user:003"));
assert.equal(engine.get(tree, bytes("user:003")), null);

const users = engine.range(tree, bytes("user:"), bytes("user;"));
assert.deepEqual(users.map((entry) => [text(entry.key), text(entry.value)]), [
  ["user:001", "Ada"],
  ["user:002", "Grace"],
]);

console.log(`basic_map: ${users.length} users in range`);
