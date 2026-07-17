import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import Database from "better-sqlite3";
import { RemoteAsyncProllyEngine } from "@trail/prolly-node/remote-async";
import { missingBytes, presentBytes } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { SqliteStore } from "../src/index.ts";

const execFileAsync = promisify(execFile);
const repositoryRoot = fileURLToPath(new URL("../../../../../", import.meta.url));

async function withDatabase(
  run: (database: Database.Database, store: SqliteStore) => Promise<void>,
): Promise<void> {
  const directory = await mkdtemp(join(tmpdir(), "prolly-node-sqlite-"));
  const database = new Database(join(directory, "store.db"));
  const store = new SqliteStore(database);
  try {
    await store.initializeSchema();
    await run(database, store);
  } finally {
    await store.close();
    database.close();
    await rm(directory, { recursive: true, force: true });
  }
}

test("SQLite satisfies the shared remote-store protocol", async () => {
  await withDatabase(async (_database, store) => {
    await runStoreConformance(() => store);
  });
});

test("SQLite uses the exact Rust table layout in both directions", async () => {
  await withDatabase(async (database, store) => {
    const tables = database
      .prepare("SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name")
      .all() as Array<{ name: string; sql: string }>;
    assert.deepEqual(
      tables.map(({ name }) => name),
      ["prolly_hints", "prolly_nodes", "prolly_roots"],
    );
    for (const table of tables) {
      assert.match(table.sql, /WITHOUT ROWID$/i);
    }

    database
      .prepare("INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)")
      .run(Buffer.from("rust-cid"), Buffer.from("rust-node"));
    database
      .prepare("INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)")
      .run(Buffer.from("rust-ns"), Buffer.from("rust-key"), Buffer.from("rust-hint"));
    database
      .prepare("INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)")
      .run(Buffer.from("rust-root"), Buffer.from("rust-manifest"));

    assert.deepEqual(await store.getNode(bytes("rust-cid")), presentBytes(bytes("rust-node")));
    assert.deepEqual(
      await store.getHint(bytes("rust-ns"), bytes("rust-key")),
      presentBytes(bytes("rust-hint")),
    );
    assert.deepEqual(
      await store.getRootManifest(bytes("rust-root")),
      presentBytes(bytes("rust-manifest")),
    );

    await store.putNode(bytes("node-cid"), bytes("node-value"));
    await store.putHint(bytes("node-ns"), bytes("node-key"), bytes("node-hint"));
    await store.putRootManifest(bytes("node-root"), bytes("node-manifest"));
    assert.equal(
      scalar(database, "SELECT hex(node) value FROM prolly_nodes WHERE cid = ?", bytes("node-cid")),
      Buffer.from("node-value").toString("hex").toUpperCase(),
    );
    assert.equal(
      scalar(
        database,
        "SELECT hex(value) value FROM prolly_hints WHERE namespace = ? AND key = ?",
        bytes("node-ns"),
        bytes("node-key"),
      ),
      Buffer.from("node-hint").toString("hex").toUpperCase(),
    );
    assert.equal(
      scalar(
        database,
        "SELECT hex(manifest) value FROM prolly_roots WHERE name = ?",
        bytes("node-root"),
      ),
      Buffer.from("node-manifest").toString("hex").toUpperCase(),
    );
  });
});

test("SQLite serializes CAS races and keeps the injected database open", async () => {
  const directory = await mkdtemp(join(tmpdir(), "prolly-node-sqlite-owner-"));
  const database = new Database(join(directory, "store.db"));
  const store = new SqliteStore(database);
  try {
    await store.initializeSchema();
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

    await store.close();
    assert.equal(scalar(database, "SELECT 1 value"), 1);
  } finally {
    database.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test("SQLite reads Rust trees and Rust reads Node trees", async () => {
  const directory = await mkdtemp(join(tmpdir(), "prolly-node-rust-sqlite-"));
  const path = join(directory, "interop.db");
  const database = new Database(path);
  const store = new SqliteStore(database);
  try {
    await store.initializeSchema();
    await runRustInterop("write", path, "rust-main", "rust-key", "rust-value");

    const engine = await RemoteAsyncProllyEngine.open(store);
    try {
      const rustTree = await engine.loadNamedRoot(bytes("rust-main"));
      assert.ok(rustTree);
      assert.deepEqual(
        Buffer.from(assertNonNull(await engine.get(rustTree, bytes("rust-key")))),
        Buffer.from(bytes("rust-value")),
      );

      const nodeTree = await engine.put(engine.create(), bytes("node-key"), bytes("node-value"));
      await engine.publishNamedRoot(bytes("node-main"), nodeTree);
    } finally {
      engine.close();
    }

    await runRustInterop("verify", path, "node-main", "node-key", "node-value");
  } finally {
    await store.close();
    database.close();
    await rm(directory, { recursive: true, force: true });
  }
});

function bytes(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value));
}

function scalar(
  database: Database.Database,
  sql: string,
  ...values: Uint8Array[]
): unknown {
  return (database.prepare(sql).get(...values) as { value: unknown } | undefined)?.value;
}

async function runRustInterop(
  operation: "write" | "verify",
  path: string,
  root: string,
  key: string,
  value: string,
): Promise<void> {
  await execFileAsync(
    "cargo",
    [
      "run",
      "--quiet",
      "--manifest-path",
      "stores/prolly-store-sqlite/Cargo.toml",
      "--example",
      "language_interop",
      "--",
      operation,
      path,
      root,
      key,
      value,
    ],
    { cwd: repositoryRoot },
  );
}

function assertNonNull<T>(value: T | null): T {
  assert.notEqual(value, null);
  return value as T;
}
