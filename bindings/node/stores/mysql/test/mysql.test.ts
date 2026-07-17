import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { promisify } from "node:util";

import { createPool, type Pool, type RowDataPacket } from "mysql2/promise";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import { missingBytes, presentBytes, StoreError, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { MysqlStore } from "../src/index.ts";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));
const databaseUrl = process.env.PROLLY_MYSQL_URL;

test("MySQL provider", { skip: databaseUrl === undefined }, async (suite) => {
  await suite.test("satisfies conformance, binary ordering, and pre-driver CID limits", async () => {
    await withStore(async (_pool, store) => {
      await runStoreConformance(() => store);
      const binaryKeys = [Uint8Array.of(0x00), Uint8Array.of(0x7f), Uint8Array.of(0x80), Uint8Array.of(0xff)];
      for (const key of binaryKeys) await store.putNode(key, key);
      const listed = (await store.listNodeCids()).filter((key) => key.byteLength === 1);
      assert.deepEqual(listed.map(hex), ["00", "7f", "80", "ff"]);
    });

    const endedPool = createPool(databaseUrl!);
    const store = new MysqlStore(endedPool);
    await endedPool.end();
    await assert.rejects(store.getNode(new Uint8Array(33)), (error: unknown) => {
      assert.ok(error instanceof StoreError);
      assert.equal(error.code, "invalid_argument");
      return true;
    });
  });

  await suite.test("uses the exact Rust table layout in both directions", async () => {
    await withStore(async (pool, store) => {
      const [columns] = await pool.query<RowDataPacket[]>(`
        SELECT TABLE_NAME AS tableName, COLUMN_NAME AS columnName,
               DATA_TYPE AS dataType, CHARACTER_MAXIMUM_LENGTH AS maximumLength
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name IN ('prolly_nodes', 'prolly_hints', 'prolly_roots')
        ORDER BY table_name, ordinal_position
      `);
      assert.deepEqual(
        columns.map((row) => [row.tableName, row.columnName, row.dataType, Number(row.maximumLength)]),
        [
          ["prolly_hints", "namespace", "varbinary", 255],
          ["prolly_hints", "key", "varbinary", 255],
          ["prolly_hints", "value", "longblob", 4294967295],
          ["prolly_nodes", "cid", "varbinary", 32],
          ["prolly_nodes", "node", "longblob", 4294967295],
          ["prolly_roots", "name", "varbinary", 255],
          ["prolly_roots", "manifest", "longblob", 4294967295],
        ],
      );
      await pool.execute("INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)", [bytes("rust-cid"), bytes("rust-node")]);
      await pool.execute("INSERT INTO prolly_hints (namespace, `key`, value) VALUES (?, ?, ?)", [bytes("rust-ns"), bytes("rust-key"), bytes("rust-hint")]);
      await pool.execute("INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)", [bytes("rust-root"), bytes("rust-manifest")]);
      assertOptional(await store.getNode(bytes("rust-cid")), "rust-node");
      assertOptional(await store.getHint(bytes("rust-ns"), bytes("rust-key")), "rust-hint");
      assertOptional(await store.getRootManifest(bytes("rust-root")), "rust-manifest");
      await store.putNode(bytes("node-cid"), bytes("node-value"));
      await store.putHint(bytes("node-ns"), bytes("node-key"), bytes("node-hint"));
      await store.putRootManifest(bytes("node-root"), bytes("node-manifest"));
      assert.equal(await scalar(pool, "SELECT node value FROM prolly_nodes WHERE cid = ?", bytes("node-cid")), "node-value");
      assert.equal(await scalar(pool, "SELECT value FROM prolly_hints WHERE namespace = ? AND `key` = ?", bytes("node-ns"), bytes("node-key")), "node-hint");
      assert.equal(await scalar(pool, "SELECT manifest value FROM prolly_roots WHERE name = ?", bytes("node-root")), "node-manifest");
    });
  });

  await suite.test("serializes missing-root CAS and rolls conflicts back", async () => {
    await withStore(async (_pool, store) => {
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

  await suite.test("cancels blocked writes and keeps the injected pool open", async () => {
    await withStore(async (pool, store) => {
      await store.putNode(bytes("blocked"), bytes("original"));
      const blocker = await pool.getConnection();
      try {
        await blocker.beginTransaction();
        await blocker.execute("SELECT node FROM prolly_nodes WHERE cid = ? FOR UPDATE", [bytes("blocked")]);
        const controller = new AbortController();
        const release = setTimeout(() => void blocker.rollback(), 2_000);
        const started = performance.now();
        const write = store.putNode(bytes("blocked"), bytes("must-not-write"), controller.signal);
        setTimeout(() => controller.abort("test cancellation"), 50);
        await assert.rejects(write, (error: unknown) => {
          assert.ok(error instanceof StoreError);
          assert.equal(error.code, "cancelled");
          return true;
        });
        assert.ok(performance.now() - started < 1_000, "write did not react promptly to cancellation");
        clearTimeout(release);
        await blocker.rollback();
      } finally {
        blocker.release();
      }
      assertOptional(await store.getNode(bytes("blocked")), "original");
      await store.close();
      assert.equal(await scalar(pool, "SELECT 1 value"), "1");
    });
  });

  await suite.test("reads Rust trees and Rust reads Node trees", async () => {
    await withStore(async (_pool, store) => {
      await runRustInterop("write", "rust-main", "rust-key", "rust-value");
      const engine = await RemoteAsyncProllyEngine.open(store);
      try {
        const rustTree = assertNonNull(await engine.loadNamedRoot(bytes("rust-main")));
        assert.equal(Buffer.from(assertNonNull(await engine.get(rustTree, bytes("rust-key")))).toString(), "rust-value");
        const nodeTree = await engine.put(engine.create(), bytes("node-key"), bytes("node-value"));
        await engine.publishNamedRoot(bytes("node-main"), nodeTree);
      } finally {
        engine.close();
      }
      await runRustInterop("verify", "node-main", "node-key", "node-value");
    });
  });
});

async function withStore(run: (pool: Pool, store: MysqlStore) => Promise<void>): Promise<void> {
  const pool = createPool({ uri: databaseUrl!, connectionLimit: 40 });
  const store = new MysqlStore(pool);
  try {
    await store.initializeSchema();
    await pool.query("TRUNCATE prolly_nodes");
    await pool.query("TRUNCATE prolly_hints");
    await pool.query("TRUNCATE prolly_roots");
    await run(pool, store);
  } finally {
    await store.close();
    await pool.end();
  }
}

async function scalar(pool: Pool, sql: string, ...values: Uint8Array[]): Promise<string | undefined> {
  const [rows] = await pool.execute<RowDataPacket[]>(sql, values);
  const value = rows[0]?.value as Buffer | number | undefined;
  return value === undefined ? undefined : Buffer.isBuffer(value) ? value.toString() : String(value);
}

async function runRustInterop(operation: "write" | "verify", root: string, key: string, value: string): Promise<void> {
  await execFileAsync("cargo", ["run", "--quiet", "--manifest-path", "stores/prolly-store-mysql/Cargo.toml", "--example", "language_interop", "--", operation, databaseUrl!, root, key, value], { cwd: repositoryRoot });
}

function assertOptional(value: { present: boolean; value: Uint8Array }, expected: string): void {
  assert.equal(value.present, true);
  assert.equal(Buffer.from(value.value).toString(), expected);
}
function bytes(value: string): Uint8Array { return Uint8Array.from(Buffer.from(value)); }
function hex(value: Uint8Array): string { return Buffer.from(value).toString("hex"); }
function assertNonNull<T>(value: T | null): T { assert.notEqual(value, null); return value as T; }
