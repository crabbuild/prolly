import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { PGlite } from "@electric-sql/pglite";
import { StoreError, missingBytes, presentBytes, upsertNode } from "@trail/prolly-node/remote-store";
import { runStoreConformance } from "@trail/prolly-storetest";

import { PGliteStore } from "../src/index.ts";

test("PGlite provider", async (suite) => {
  await suite.test("satisfies conformance and uses the PostgreSQL bytea layout", async () => {
    await withStore(async (database, store) => {
      await runStoreConformance(() => store);
      const columns = await database.query<{ table_name: string; column_name: string; data_type: string }>("SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = current_schema() AND table_name IN ('prolly_nodes', 'prolly_hints', 'prolly_roots') ORDER BY table_name, ordinal_position");
      assert.deepEqual(columns.rows.map(({ table_name, column_name, data_type }) => [table_name, column_name, data_type]), [
        ["prolly_hints", "namespace", "bytea"], ["prolly_hints", "key", "bytea"], ["prolly_hints", "value", "bytea"],
        ["prolly_nodes", "cid", "bytea"], ["prolly_nodes", "node", "bytea"], ["prolly_roots", "name", "bytea"], ["prolly_roots", "manifest", "bytea"],
      ]);
      await database.query("INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2)", [bytes("raw-cid"), bytes("raw-node")]); assertOptional(await store.getNode(bytes("raw-cid")), "raw-node");
      await store.putRootManifest(bytes("node-root"), bytes("node-manifest")); const raw = await database.query<{ value: Uint8Array }>("SELECT manifest value FROM prolly_roots WHERE name = $1", [bytes("node-root")]); assert.equal(text(raw.rows[0]!.value), "node-manifest");
    });
  });

  await suite.test("serializes missing-root CAS and rolls strict conflicts back", async () => {
    await withStore(async (_database, store) => {
      const results = await Promise.all(Array.from({ length: 32 }, (_, index) => store.compareAndSwapRootManifest(bytes("race"), missingBytes(), presentBytes(bytes(`winner-${index}`))))); assert.equal(results.filter(({ applied }) => applied).length, 1);
      const conflict = await store.commitTransaction([upsertNode(bytes("rollback-node"), bytes("must-not-write"))], [{ name: bytes("race"), expected: missingBytes() }], [{ kind: "put", name: bytes("rollback-root"), manifest: bytes("must-not-publish") }]);
      assert.equal(conflict.applied, false); assert.equal((await store.getNode(bytes("rollback-node"))).present, false); assert.equal((await store.getRootManifest(bytes("rollback-root"))).present, false);
    });
  });

  await suite.test("honors cancellation and preserves database ownership", async () => {
    await withStore(async (database, store) => {
      const controller = new AbortController(); controller.abort("test"); await assert.rejects(store.getNode(bytes("cancelled"), controller.signal), (error: unknown) => { assert.ok(error instanceof StoreError); assert.equal(error.code, "cancelled"); return true; });
      await store.close(); assert.equal((await database.query<{ value: number }>("SELECT 1 value")).rows[0]?.value, 1);
    });
  });

  await suite.test("reopens a durable Node filesystem database", async () => {
    const directory = await mkdtemp(join(tmpdir(), "prolly-pglite-"));
    try {
      const first = new PGlite(directory); await first.waitReady; const firstStore = new PGliteStore(first); await firstStore.initializeSchema(); await firstStore.putNode(bytes("durable"), bytes("value")); await firstStore.close(); await first.close();
      const second = new PGlite(directory); await second.waitReady; const secondStore = new PGliteStore(second); try { assertOptional(await secondStore.getNode(bytes("durable")), "value"); } finally { await secondStore.close(); await second.close(); }
    } finally { await rm(directory, { recursive: true, force: true }); }
  });
});

async function withStore(run: (database: PGlite, store: PGliteStore) => Promise<void>): Promise<void> { const database = new PGlite(); await database.waitReady; const store = new PGliteStore(database); try { await store.initializeSchema(); await run(database, store); } finally { await store.close(); await database.close(); } }
function assertOptional(value: { present: boolean; value: Uint8Array }, expected: string): void { assert.equal(value.present, true); assert.equal(text(value.value), expected); }
function bytes(value: string): Uint8Array { return new TextEncoder().encode(value); }
function text(value: Uint8Array): string { return new TextDecoder().decode(value); }
