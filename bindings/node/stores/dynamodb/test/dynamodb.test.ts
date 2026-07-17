import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { promisify } from "node:util";

import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  DynamoDBClient,
  GetItemCommand,
  ListTablesCommand,
} from "@aws-sdk/client-dynamodb";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import { StoreError, missingBytes, presentBytes, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { DynamoDbStore } from "../src/index.ts";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));
const endpoint = process.env.PROLLY_DYNAMODB_ENDPOINT;

test("DynamoDB provider", { skip: endpoint === undefined }, async (suite) => {
  await suite.test("satisfies conformance and uses the exact binary table layout", async () => {
    await withStore(async (client, store, tableName, prefix) => {
      await runStoreConformance(() => store);
      const description = await client.send(new DescribeTableCommand({ TableName: tableName }));
      assert.deepEqual(description.Table?.KeySchema, [{ AttributeName: "pk", KeyType: "HASH" }]);
      assert.ok(description.Table?.AttributeDefinitions?.some((value) => value.AttributeName === "pk" && value.AttributeType === "B"));

      const cid = specialBytes(32); const root = specialBytes(9); const namespace = specialBytes(7); const hintKey = specialBytes(5);
      await store.putNode(cid, bytes("node")); await store.putRootManifest(root, bytes("manifest")); await store.putHint(namespace, hintKey, bytes("hint"));
      assert.equal(Buffer.from(await rawValue(client, tableName, familyKey(prefix, "node:", cid))).toString(), "node");
      assert.equal(Buffer.from(await rawValue(client, tableName, familyKey(prefix, "root:", root))).toString(), "manifest");
      assert.equal(Buffer.from(await rawValue(client, tableName, expectedHintKey(prefix, namespace, hintKey))).toString(), "hint");
      assert.ok((await store.listNodeCids()).some((value) => Buffer.from(value).equals(cid)));
    });
  });

  await suite.test("conditional CAS has one winner and strict conflicts roll back", async () => {
    await withStore(async (_client, store) => {
      const results = await Promise.all(Array.from({ length: 32 }, (_, index) => store.compareAndSwapRootManifest(bytes("race"), missingBytes(), presentBytes(bytes(`winner-${index}`)))));
      assert.equal(results.filter(({ applied }) => applied).length, 1);
      const conflict = await store.commitTransaction(
        [upsertNode(bytes("rollback-node"), bytes("must-not-write"))],
        [{ name: bytes("race"), expected: missingBytes() }],
        [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }],
      );
      assert.equal(conflict.applied, false); assert.equal((await store.getNode(bytes("rollback-node"))).present, false); assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
    });
  });

  await suite.test("reads Rust trees and Rust reads Node trees", async () => {
    await withStore(async (_client, store, tableName, prefix) => {
      await runRustInterop("write", tableName, prefix, "rust-main", "rust-key", "rust-value");
      const engine = await RemoteAsyncProllyEngine.open(store);
      try {
        const rustTree = assertNonNull(await engine.loadNamedRoot(bytes("rust-main")));
        assert.equal(Buffer.from(assertNonNull(await engine.get(rustTree, bytes("rust-key")))).toString(), "rust-value");
        const tree = await engine.put(engine.create(), bytes("node-key"), bytes("node-value")); await engine.publishNamedRoot(bytes("node-main"), tree);
      } finally { engine.close(); }
      await runRustInterop("verify", tableName, prefix, "node-main", "node-key", "node-value");
    });
  });

  await suite.test("close preserves ownership of the injected AWS client", async () => {
    await withStore(async (client, store) => { await store.close(); assert.ok(Array.isArray((await client.send(new ListTablesCommand({}))).TableNames)); });
  });
});

test("DynamoDB SDK contract", async (suite) => {
  await suite.test("chunks limits, retries unprocessed reads, and restores duplicates", async () => {
    const batchGetSizes: number[] = []; const batchWriteSizes: number[] = []; let firstGet = true;
    const client = {
      async send(command: object): Promise<any> {
        if (command instanceof BatchGetItemCommand) {
          const keys = command.input.RequestItems!.table!.Keys!; batchGetSizes.push(keys.length);
          if (firstGet) { firstGet = false; const deferred = keys[keys.length - 1]!; return { Responses: { table: keys.slice(0, -1).map((key) => ({ pk: key.pk!, value: { B: Buffer.from(`v${Buffer.from((key.pk as { B: Uint8Array }).B).at(-1)}`) } })) }, UnprocessedKeys: { table: { Keys: [deferred] } } }; }
          return { Responses: { table: keys.map((key) => ({ pk: key.pk!, value: { B: Buffer.from(`v${Buffer.from((key.pk as { B: Uint8Array }).B).at(-1)}`) } })) } };
        }
        if (command instanceof BatchWriteItemCommand) { batchWriteSizes.push(command.input.RequestItems!.table!.length); return {}; }
        throw new Error("unexpected SDK call");
      },
    };
    const store = new DynamoDbStore(client as unknown as DynamoDBClient, { tableName: "table", keyPrefix: bytes("p:") });
    const cids = Array.from({ length: 101 }, (_, index) => Uint8Array.of(index)); const result = await store.batchGetNodesOrdered([...cids, cids[0]!]);
    assert.deepEqual(batchGetSizes, [100, 1, 1]); assert.equal(result.length, 102); assert.equal(Buffer.from(result[0]!.value).toString(), "v0"); assert.deepEqual(result[0], result[101]);
    await store.batchNodes(Array.from({ length: 26 }, (_, index) => upsertNode(Uint8Array.of(index), bytes("v")))); assert.deepEqual(batchWriteSizes, [25, 1]);
  });

  await suite.test("rejects oversized transactions before any SDK call", async () => {
    let calls = 0; const client = { async send(): Promise<never> { calls += 1; throw new Error("must not be called"); } };
    const store = new DynamoDbStore(client as unknown as DynamoDBClient, { tableName: "table" });
    await assert.rejects(store.commitTransaction(Array.from({ length: 101 }, (_, index) => upsertNode(Uint8Array.of(index), bytes("v"))), [], []), (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "resource_exhausted"); return true; });
    assert.equal(calls, 0);
  });

  await suite.test("propagates AbortSignal without destroying the borrowed client", async () => {
    let destroyed = false;
    const client = {
      destroy(): void { destroyed = true; },
      async send(_command: object, options?: { abortSignal?: AbortSignal }): Promise<never> {
        return new Promise((_resolve, reject) => options?.abortSignal?.addEventListener("abort", () => reject(Object.assign(new Error("aborted"), { name: "AbortError" })), { once: true }));
      },
    };
    const store = new DynamoDbStore(client as unknown as DynamoDBClient, { tableName: "table" }); const controller = new AbortController(); const write = store.putNode(bytes("cid"), bytes("value"), controller.signal); controller.abort("test");
    await assert.rejects(write, (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "cancelled"); return true; }); await store.close(); assert.equal(destroyed, false);
  });
});

async function withStore(run: (client: DynamoDBClient, store: DynamoDbStore, tableName: string, prefix: Buffer) => Promise<void>): Promise<void> {
  const client = new DynamoDBClient({ region: "us-west-2", endpoint, credentials: { accessKeyId: "local", secretAccessKey: "local" } });
  const tableName = `prolly_node_${process.pid}_${Date.now()}_${Math.random().toString(16).slice(2)}`; const prefix = Buffer.from("prolly:test:node:"); const store = new DynamoDbStore(client, { tableName, keyPrefix: prefix });
  await store.initializeTable();
  try { await run(client, store, tableName, prefix); }
  finally { await store.close(); await client.send(new DeleteTableCommand({ TableName: tableName })).catch(() => undefined); client.destroy(); }
}

async function rawValue(client: DynamoDBClient, tableName: string, key: Uint8Array): Promise<Uint8Array> { const output = await client.send(new GetItemCommand({ TableName: tableName, Key: { pk: { B: key } }, ConsistentRead: true })); return (output.Item?.value as { B: Uint8Array }).B; }
function familyKey(prefix: Uint8Array, family: string, suffix: Uint8Array): Buffer { return Buffer.concat([Buffer.from(prefix), Buffer.from(family), Buffer.from(suffix)]); }
function expectedHintKey(prefix: Uint8Array, namespace: Uint8Array, key: Uint8Array): Buffer { const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(namespace.byteLength)); return Buffer.concat([Buffer.from(prefix), Buffer.from("hint:"), length, Buffer.from(namespace), Buffer.from(key)]); }
async function runRustInterop(operation: "write" | "verify", tableName: string, prefix: Uint8Array, root: string, key: string, value: string): Promise<void> { await execFileAsync("cargo", ["run", "--quiet", "--manifest-path", "stores/prolly-store-dynamodb/Cargo.toml", "--example", "language_interop", "--", operation, endpoint!, tableName, Buffer.from(prefix).toString("hex"), root, key, value], { cwd: repositoryRoot }); }
function specialBytes(length: number): Buffer { const pattern = [0, 0x7f, 0x80, 0xff]; return Buffer.from(Array.from({ length }, (_, index) => pattern[index % 4]!)); }
function bytes(value: string): Uint8Array { return Uint8Array.from(Buffer.from(value)); }
function assertNonNull<T>(value: T | null): T { assert.notEqual(value, null); return value as T; }
