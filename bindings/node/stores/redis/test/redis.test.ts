import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { promisify } from "node:util";

import { createClient, RESP_TYPES, type RedisClientType } from "redis";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import { missingBytes, presentBytes, StoreError, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { RedisStore } from "../src/index.ts";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));
const redisUrl = process.env.PROLLY_REDIS_URL;
const prefix = Buffer.from(`prolly:test:node:${process.pid}:`);

test("Redis provider", { skip: redisUrl === undefined }, async (suite) => {
  await suite.test("satisfies conformance and uses exact binary-safe key families", async () => {
    await withStore(async (client, store) => {
      await runStoreConformance(() => store);
      const cid = specialBytes(32);
      const root = specialBytes(9);
      const namespace = specialBytes(7);
      const hintKey = specialBytes(5);
      await store.putNode(cid, bytes("node"));
      await store.putRootManifest(root, bytes("manifest"));
      await store.putHint(namespace, hintKey, bytes("hint"));

      assert.equal((await getRaw(client, familyKey("node:", cid)))?.toString(), "node");
      assert.equal((await getRaw(client, familyKey("root:", root)))?.toString(), "manifest");
      assert.equal((await getRaw(client, expectedHintKey(namespace, hintKey)))?.toString(), "hint");
      assert.ok((await store.listNodeCids()).some((value) => Buffer.from(value).equals(cid)));

      await store.putRootManifest(Buffer.from([0xff]), bytes("last"));
      await store.putRootManifest(Buffer.from([0x00]), bytes("first"));
      const names = (await store.listRootManifests()).map(({ name }) => Buffer.from(name));
      assert.deepEqual(names, [...names].sort(Buffer.compare));
    });
  });

  await suite.test("Lua CAS has one winner and strict conflicts roll back", async () => {
    await withStore(async (_client, store) => {
      const results = await Promise.all(Array.from({ length: 32 }, (_, index) =>
        store.compareAndSwapRootManifest(bytes("race"), missingBytes(), presentBytes(bytes(`winner-${index}`))),
      ));
      assert.equal(results.filter(({ applied }) => applied).length, 1);
      const conflict = await store.commitTransaction(
        [upsertNode(bytes("rollback-node"), bytes("must-not-write"))],
        [{ name: bytes("race"), expected: missingBytes() }],
        [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }],
      );
      assert.equal(conflict.applied, false);
      assert.equal((await store.getNode(bytes("rollback-node"))).present, false);
      assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
    });
  });

  await suite.test("AbortSignal cancels commands and close keeps the injected client open", async () => {
    await withStore(async (client, store) => {
      const admin = createClient({ url: redisUrl });
      await admin.connect();
      try {
        await admin.sendCommand(["CLIENT", "PAUSE", "1000", "WRITE"]);
        const controller = new AbortController();
        const started = performance.now();
        const write = store.putNode(bytes("cancelled"), bytes("value"), controller.signal);
        setTimeout(() => controller.abort("test cancellation"), 50);
        await assert.rejects(write, (error: unknown) => {
          assert.ok(error instanceof StoreError);
          assert.equal(error.code, "cancelled");
          return true;
        });
        assert.ok(performance.now() - started < 500, "Redis command did not cancel promptly");
      } finally {
        admin.destroy();
      }
      await store.close();
      assert.equal(await client.ping(), "PONG");
    });
  });

  await suite.test("reads Rust trees and Rust reads Node trees", async () => {
    await withStore(async (_client, store) => {
      await runRustInterop("write", "rust-main", "rust-key", "rust-value");
      const engine = await RemoteAsyncProllyEngine.open(store);
      try {
        const rustTree = assertNonNull(await engine.loadNamedRoot(bytes("rust-main")));
        assert.equal(Buffer.from(assertNonNull(await engine.get(rustTree, bytes("rust-key")))).toString(), "rust-value");
        const tree = await engine.put(engine.create(), bytes("node-key"), bytes("node-value"));
        await engine.publishNamedRoot(bytes("node-main"), tree);
      } finally {
        engine.close();
      }
      await runRustInterop("verify", "node-main", "node-key", "node-value");
    });
  });

  await suite.test("documents durable primary-storage configuration", async () => {
    const readme = await readFile(new URL("../README.md", import.meta.url), "utf8");
    assert.match(readme, /AOF/i);
    assert.match(readme, /appendfsync/i);
    assert.match(readme, /backup/i);
  });
});

async function withStore(run: (client: RedisClientType, store: RedisStore) => Promise<void>): Promise<void> {
  const client = createClient({ url: redisUrl });
  await client.connect();
  const store = new RedisStore(client, { keyPrefix: prefix });
  try {
    await store.clearNamespace();
    await run(client, store);
  } finally {
    await store.clearNamespace().catch(() => undefined);
    await store.close();
    client.destroy();
  }
}

async function getRaw(client: RedisClientType, key: Buffer): Promise<Buffer | null> {
  const binary = client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer });
  return binary.sendCommand<Buffer | null>(["GET", key]);
}

function familyKey(family: string, suffix: Uint8Array): Buffer {
  return Buffer.concat([prefix, Buffer.from(family), Buffer.from(suffix)]);
}

function expectedHintKey(namespace: Uint8Array, key: Uint8Array): Buffer {
  const length = Buffer.alloc(8);
  length.writeBigUInt64BE(BigInt(namespace.byteLength));
  return Buffer.concat([prefix, Buffer.from("hint:"), length, Buffer.from(namespace), Buffer.from(key)]);
}

async function runRustInterop(operation: "write" | "verify", root: string, key: string, value: string): Promise<void> {
  await execFileAsync("cargo", ["run", "--quiet", "--manifest-path", "stores/prolly-store-redis/Cargo.toml", "--example", "language_interop", "--", operation, redisUrl!, prefix.toString("hex"), root, key, value], { cwd: repositoryRoot });
}

function specialBytes(length: number): Buffer {
  const result = Buffer.alloc(length);
  const pattern = [0x00, 0x7f, 0x80, 0xff];
  for (let index = 0; index < result.length; index += 1) result[index] = pattern[index % pattern.length]!;
  return result;
}
function bytes(value: string): Uint8Array { return Uint8Array.from(Buffer.from(value)); }
function assertNonNull<T>(value: T | null): T { assert.notEqual(value, null); return value as T; }
