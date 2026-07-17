import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { promisify } from "node:util";

import { Pool } from "pg";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import {
  missingBytes,
  presentBytes,
  StoreError,
  upsertNode,
} from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { PostgresStore } from "../src/index.ts";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));
const databaseUrl = process.env.PROLLY_POSTGRES_URL;

test("PostgreSQL provider", { skip: databaseUrl === undefined }, async (suite) => {
  await suite.test("satisfies the shared remote-store protocol", async () => {
    await withStore(async (_pool, store) => runStoreConformance(() => store));
  });

  await suite.test("uses the exact Rust table layout in both directions", async () => {
    await withStore(async (pool, store) => {
      const columns = await pool.query<{ table_name: string; column_name: string; data_type: string }>(`
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name IN ('prolly_nodes', 'prolly_hints', 'prolly_roots')
        ORDER BY table_name, ordinal_position
      `);
      assert.deepEqual(
        columns.rows.map(({ table_name, column_name, data_type }) => [table_name, column_name, data_type]),
        [
          ["prolly_hints", "namespace", "bytea"],
          ["prolly_hints", "key", "bytea"],
          ["prolly_hints", "value", "bytea"],
          ["prolly_nodes", "cid", "bytea"],
          ["prolly_nodes", "node", "bytea"],
          ["prolly_roots", "name", "bytea"],
          ["prolly_roots", "manifest", "bytea"],
        ],
      );

      await pool.query("INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2)", [bytes("rust-cid"), bytes("rust-node")]);
      await pool.query("INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3)", [bytes("rust-ns"), bytes("rust-key"), bytes("rust-hint")]);
      await pool.query("INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2)", [bytes("rust-root"), bytes("rust-manifest")]);
      assertOptional(await store.getNode(bytes("rust-cid")), "rust-node");
      assertOptional(await store.getHint(bytes("rust-ns"), bytes("rust-key")), "rust-hint");
      assertOptional(await store.getRootManifest(bytes("rust-root")), "rust-manifest");

      await store.putNode(bytes("node-cid"), bytes("node-value"));
      await store.putHint(bytes("node-ns"), bytes("node-key"), bytes("node-hint"));
      await store.putRootManifest(bytes("node-root"), bytes("node-manifest"));
      assert.equal((await pool.query<{ value: Buffer }>("SELECT node value FROM prolly_nodes WHERE cid = $1", [bytes("node-cid")])).rows[0]?.value.toString(), "node-value");
      assert.equal((await pool.query<{ value: Buffer }>("SELECT value FROM prolly_hints WHERE namespace = $1 AND key = $2", [bytes("node-ns"), bytes("node-key")])).rows[0]?.value.toString(), "node-hint");
      assert.equal((await pool.query<{ value: Buffer }>("SELECT manifest value FROM prolly_roots WHERE name = $1", [bytes("node-root")])).rows[0]?.value.toString(), "node-manifest");
    });
  });

  await suite.test("serializes missing-root CAS and rolls conflicts back", async () => {
    await withStore(async (_pool, store) => {
      const results = await Promise.all(
        Array.from({ length: 32 }, (_, index) =>
          store.compareAndSwapRootManifest(
            bytes("race"),
            missingBytes(),
            presentBytes(bytes(`winner-${index}`)),
          ),
        ),
      );
      assert.equal(results.filter(({ applied }) => applied).length, 1);

      const result = await store.commitTransaction(
        [upsertNode(bytes("rollback-node"), bytes("must-not-write"))],
        [{ name: bytes("race"), expected: missingBytes() }],
        [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }],
      );
      assert.equal(result.applied, false);
      assert.equal((await store.getNode(bytes("rollback-node"))).present, false);
      assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
    });
  });

  await suite.test("cancels blocked queries and keeps the injected pool open", async () => {
    await withStore(async (pool, store) => {
      const blocker = await pool.connect();
      try {
        await blocker.query("BEGIN");
        await blocker.query("LOCK TABLE prolly_nodes IN ACCESS EXCLUSIVE MODE");
        const controller = new AbortController();
        const release = setTimeout(() => void blocker.query("ROLLBACK"), 2_000);
        const started = performance.now();
        const write = store.putNode(bytes("cancelled"), bytes("must-not-write"), controller.signal);
        setTimeout(() => controller.abort("test cancellation"), 50);
        await assert.rejects(write, (error: unknown) => {
          assert.ok(error instanceof StoreError);
          assert.equal(error.code, "cancelled");
          return true;
        });
        assert.ok(performance.now() - started < 1_000, "query did not react promptly to cancellation");
        clearTimeout(release);
        await blocker.query("ROLLBACK");
      } finally {
        blocker.release();
      }
      await store.close();
      assert.equal((await pool.query<{ value: number }>("SELECT 1 value")).rows[0]?.value, 1);
    });
  });

  await suite.test("reads Rust trees and Rust reads Node trees", async () => {
    await withStore(async (_pool, store) => {
      await runRustInterop("write", "rust-main", "rust-key", "rust-value");
      const engine = await RemoteAsyncProllyEngine.open(store);
      try {
        const rustTree = assertNonNull(await engine.loadNamedRoot(bytes("rust-main")));
        assert.deepEqual(Buffer.from(assertNonNull(await engine.get(rustTree, bytes("rust-key")))), Buffer.from("rust-value"));
        const nodeTree = await engine.put(engine.create(), bytes("node-key"), bytes("node-value"));
        await engine.publishNamedRoot(bytes("node-main"), nodeTree);
      } finally {
        engine.close();
      }
      await runRustInterop("verify", "node-main", "node-key", "node-value");
    });
  });
});

async function withStore(run: (pool: Pool, store: PostgresStore) => Promise<void>): Promise<void> {
  const pool = new Pool({ connectionString: databaseUrl, max: 40 });
  const store = new PostgresStore(pool);
  try {
    await store.initializeSchema();
    await pool.query("TRUNCATE prolly_nodes, prolly_hints, prolly_roots");
    await run(pool, store);
  } finally {
    await store.close();
    await pool.end();
  }
}

async function runRustInterop(operation: "write" | "verify", root: string, key: string, value: string): Promise<void> {
  await execFileAsync("cargo", [
    "run", "--quiet", "--manifest-path", "stores/prolly-store-postgres/Cargo.toml",
    "--example", "language_interop", "--", operation, databaseUrl!, root, key, value,
  ], { cwd: repositoryRoot });
}

function assertOptional(value: { present: boolean; value: Uint8Array }, expected: string): void {
  assert.equal(value.present, true);
  assert.equal(Buffer.from(value.value).toString(), expected);
}

function bytes(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value));
}

function assertNonNull<T>(value: T | null): T {
  assert.notEqual(value, null);
  return value as T;
}
