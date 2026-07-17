import assert from "node:assert/strict";
import test from "node:test";
import { missingBytes, presentBytes } from "@trail/prolly-wasm/remote-store";
import { OpfsStore, type OpfsDirectoryHandle } from "../src/index.ts";

const bytes = (value: string): Uint8Array => new TextEncoder().encode(value);
const text = (value: Uint8Array): string => new TextDecoder().decode(value);

class MemoryDirectory implements OpfsDirectoryHandle {
  content = "";
  failWrite = false;
  async getFileHandle(): Promise<any> {
    const directory = this;
    return {
      async getFile() { return { text: async () => directory.content }; },
      async createWritable() {
        let next = "";
        return {
          async write(value: string) { if (directory.failWrite) { directory.failWrite = false; throw new Error("secret-opfs-failure"); } next = value; },
          async close() { directory.content = next; },
          async abort() {},
        };
      },
    };
  }
}

test("OPFS implements durable atomic batches, CAS, and transactions", async () => {
  const directory = new MemoryDirectory();
  const store = new OpfsStore(directory);
  assert.equal((await store.descriptor()).provider, "opfs");
  await store.putNode(new Uint8Array(32).fill(7), bytes("node"));
  await store.batchPutNodesWithHint(
    [1, 2].map((value) => ({ cid: new Uint8Array(32).fill(value), node: bytes(`n${value}`) })),
    bytes("ns"), bytes("key"), bytes("hint"),
  );
  assert.equal(text((await store.getHint(bytes("ns"), bytes("key"))).value), "hint");

  const contenders = await Promise.all(Array.from({ length: 32 }, (_, index) =>
    store.compareAndSwapRootManifest(bytes("main"), missingBytes(), presentBytes(Uint8Array.of(index))),
  ));
  assert.equal(contenders.filter(({ applied }) => applied).length, 1);
  const conflict = await store.commitTransaction(
    [{ kind: "upsert", cid: bytes("rollback"), node: bytes("bad") }],
    [{ name: bytes("main"), expected: missingBytes() }], [],
  );
  assert.equal(conflict.applied, false);
  assert.equal((await store.getNode(bytes("rollback"))).present, false);

  directory.failWrite = true;
  await assert.rejects(store.batchNodes([{ kind: "upsert", cid: bytes("failed"), node: bytes("bad") }]), (error: any) => !error.message.includes("secret"));
  assert.equal((await store.getNode(bytes("failed"))).present, false);
  const controller = new AbortController(); controller.abort();
  await assert.rejects(store.getNode(bytes("cancel"), controller.signal), (error: any) => error.code === "cancelled");
  await store.close();

  const reopened = new OpfsStore(directory);
  assert.equal(text((await reopened.getNode(new Uint8Array(32).fill(7))).value), "node");
  await reopened.close();
});
