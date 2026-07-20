import type Database from "better-sqlite3";

import {
  StoreError,
  deleteNode,
  missingBytes,
  normalizeOptionalBytes,
  ownBytes,
  presentBytes,
  publishNodesWithGeneralPath,
  throwIfAborted,
  upsertNode,
  validateStoreDescriptor,
  type NamedStoreRoot,
  type NodeEntry,
  type NodeMutation,
  type NodePublication,
  type OptionalBytes,
  type RemoteStore,
  type RootCasResult,
  type RootCondition,
  type RootWrite,
  type StoreDescriptor,
  type StoreTransactionResult,
} from "@trail/prolly-node/remote-store";

const CREATE_SCHEMA_SQL = `
CREATE TABLE IF NOT EXISTS prolly_nodes (
    cid  BLOB PRIMARY KEY NOT NULL,
    node BLOB NOT NULL
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS prolly_hints (
    namespace BLOB NOT NULL,
    key       BLOB NOT NULL,
    value     BLOB NOT NULL,
    PRIMARY KEY (namespace, key)
) WITHOUT ROWID;

CREATE TABLE IF NOT EXISTS prolly_roots (
    name     BLOB PRIMARY KEY NOT NULL,
    manifest BLOB NOT NULL
) WITHOUT ROWID;
`;

const SELECT_NODE = "SELECT node AS value FROM prolly_nodes WHERE cid = ?";
const UPSERT_NODE = `INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node`;
const DELETE_NODE = "DELETE FROM prolly_nodes WHERE cid = ?";
const SELECT_HINT = "SELECT value FROM prolly_hints WHERE namespace = ? AND key = ?";
const UPSERT_HINT = `INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`;
const SELECT_ROOT = "SELECT manifest AS value FROM prolly_roots WHERE name = ?";
const UPSERT_ROOT = `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest`;
const DELETE_ROOT = "DELETE FROM prolly_roots WHERE name = ?";

export interface SqliteStoreOptions {
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

export class SqliteStore implements RemoteStore {
  readonly #database: Database.Database;
  readonly #descriptor: StoreDescriptor;
  #tail: Promise<void> = Promise.resolve();
  #accepting = true;
  #closed = false;

  constructor(database: Database.Database, options: SqliteStoreOptions = {}) {
    if (database == null) {
      throw new StoreError("invalid_argument", "SQLite database is required");
    }
    this.#database = database;
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 2,
      adapterName: options.adapterName?.trim() || "sqlite-v1",
      provider: "sqlite",
      schemaVersion: 1,
      capabilities: {
        nativeBatchReads: true,
        atomicBatchWrites: true,
        nodeScan: true,
        hints: true,
        atomicNodesAndHint: true,
        rootScan: true,
        rootCompareAndSwap: true,
        transactions: true,
        readParallelism: options.readParallelism ?? 16,
      },
      limits: {},
    });
  }

  async initializeSchema(signal?: AbortSignal): Promise<void> {
    return this.#schedule(() => {
      this.#database.exec(CREATE_SCHEMA_SQL);
    }, signal);
  }

  async close(): Promise<void> {
    if (!this.#accepting) {
      await this.#tail;
      return;
    }
    this.#accepting = false;
    await this.#tail;
    this.#closed = true;
  }

  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> {
    return this.#schedule(() => this.#descriptor, signal);
  }

  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const key = ownBytes(cid);
    return this.#schedule(() => queryOptional(this.#database.prepare(SELECT_NODE), key), signal);
  }

  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedCid = ownBytes(cid);
    const ownedValue = ownBytes(value);
    return this.#schedule(() => {
      this.#database.prepare(UPSERT_NODE).run(ownedCid, ownedValue);
    }, signal);
  }

  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> {
    const key = ownBytes(cid);
    return this.#schedule(() => {
      this.#database.prepare(DELETE_NODE).run(key);
    }, signal);
  }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation);
    return this.#schedule(() => {
      this.#database.transaction(() => applyNodeMutations(this.#database, owned)).immediate();
    }, signal);
  }

  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> {
    return publishNodesWithGeneralPath(this, publication, signal);
  }

  async batchGetNodesOrdered(
    cids: readonly Uint8Array[],
    signal?: AbortSignal,
  ): Promise<OptionalBytes[]> {
    const keys = cids.map(ownBytes);
    return this.#schedule(() => {
      const statement = this.#database.prepare(SELECT_NODE);
      return keys.map((key) => queryOptional(statement, key));
    }, signal);
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#schedule(
      () =>
        this.#database
          .prepare("SELECT cid FROM prolly_nodes ORDER BY cid")
          .all()
          .map((row) => rowBytes(row, "cid")),
      signal,
    );
  }

  async getHint(
    namespace: Uint8Array,
    key: Uint8Array,
    signal?: AbortSignal,
  ): Promise<OptionalBytes> {
    const ownedNamespace = ownBytes(namespace);
    const ownedKey = ownBytes(key);
    return this.#schedule(
      () => queryOptional(this.#database.prepare(SELECT_HINT), ownedNamespace, ownedKey),
      signal,
    );
  }

  async putHint(
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    const ownedNamespace = ownBytes(namespace);
    const ownedKey = ownBytes(key);
    const ownedValue = ownBytes(value);
    return this.#schedule(() => {
      this.#database.prepare(UPSERT_HINT).run(ownedNamespace, ownedKey, ownedValue);
    }, signal);
  }

  async batchPutNodesWithHint(
    nodes: readonly NodeEntry[],
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    const ownedNodes = nodes.map(({ cid, node }) => ({ cid: ownBytes(cid), node: ownBytes(node) }));
    const ownedNamespace = ownBytes(namespace);
    const ownedKey = ownBytes(key);
    const ownedValue = ownBytes(value);
    return this.#schedule(() => {
      this.#database.transaction(() => {
        const nodeStatement = this.#database.prepare(UPSERT_NODE);
        for (const node of ownedNodes) nodeStatement.run(node.cid, node.node);
        this.#database.prepare(UPSERT_HINT).run(ownedNamespace, ownedKey, ownedValue);
      }).immediate();
    }, signal);
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const ownedName = ownBytes(name);
    return this.#schedule(
      () => queryOptional(this.#database.prepare(SELECT_ROOT), ownedName),
      signal,
    );
  }

  async putRootManifest(
    name: Uint8Array,
    manifest: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    const ownedName = ownBytes(name);
    const ownedManifest = ownBytes(manifest);
    return this.#schedule(() => {
      this.#database.prepare(UPSERT_ROOT).run(ownedName, ownedManifest);
    }, signal);
  }

  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedName = ownBytes(name);
    return this.#schedule(() => {
      this.#database.prepare(DELETE_ROOT).run(ownedName);
    }, signal);
  }

  async compareAndSwapRootManifest(
    name: Uint8Array,
    expected: OptionalBytes,
    replacement: OptionalBytes,
    signal?: AbortSignal,
  ): Promise<RootCasResult> {
    const ownedName = ownBytes(name);
    const ownedExpected = normalizeOptionalBytes(expected);
    const ownedReplacement = normalizeOptionalBytes(replacement);
    return this.#schedule(
      () =>
        this.#database.transaction(() => {
          const current = queryOptional(this.#database.prepare(SELECT_ROOT), ownedName);
          if (!optionalEqual(current, ownedExpected)) {
            return { applied: false, current };
          }
          writeOptionalRoot(this.#database, ownedName, ownedReplacement);
          return { applied: true, current: normalizeOptionalBytes(ownedReplacement) };
        }).immediate(),
      signal,
    );
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#schedule(
      () =>
        this.#database
          .prepare("SELECT name, manifest FROM prolly_roots ORDER BY name")
          .all()
          .map((row) => ({ name: rowBytes(row, "name"), manifest: rowBytes(row, "manifest") })),
      signal,
    );
  }

  async commitTransaction(
    nodes: readonly NodeMutation[],
    conditions: readonly RootCondition[],
    roots: readonly RootWrite[],
    signal?: AbortSignal,
  ): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation);
    const ownedConditions = conditions.map(({ name, expected }) => ({
      name: ownBytes(name),
      expected: normalizeOptionalBytes(expected),
    }));
    const ownedRoots = roots.map(cloneRootWrite);
    return this.#schedule(
      () =>
        this.#database.transaction((): StoreTransactionResult => {
          const selectRoot = this.#database.prepare(SELECT_ROOT);
          for (const condition of ownedConditions) {
            const current = queryOptional(selectRoot, condition.name);
            if (!optionalEqual(current, condition.expected)) {
              return {
                applied: false,
                conflict: {
                  name: ownBytes(condition.name),
                  expected: normalizeOptionalBytes(condition.expected),
                  current,
                },
              };
            }
          }
          applyNodeMutations(this.#database, ownedNodes);
          for (const root of ownedRoots) {
            writeOptionalRoot(
              this.#database,
              root.name,
              root.kind === "put" ? presentBytes(root.manifest) : missingBytes(),
            );
          }
          return { applied: true };
        }).immediate(),
      signal,
    );
  }

  async #schedule<T>(operation: () => T, signal?: AbortSignal): Promise<T> {
    throwIfAborted(signal);
    if (!this.#accepting) throw new StoreError("internal", "SQLite store is closed");
    const result = this.#tail.then(() => {
      throwIfAborted(signal);
      if (this.#closed) throw new StoreError("internal", "SQLite store is closed");
      try {
        return operation();
      } catch (error) {
        throw mapSqliteError(error);
      }
    });
    this.#tail = result.then(
      () => undefined,
      () => undefined,
    );
    return result;
  }
}

function queryOptional(
  statement: { get(...values: unknown[]): unknown },
  ...values: Uint8Array[]
): OptionalBytes {
  const row = statement.get(...values);
  return row === undefined ? missingBytes() : presentBytes(rowBytes(row, "value"));
}

function applyNodeMutations(database: Database.Database, operations: readonly NodeMutation[]): void {
  const upsert = database.prepare(UPSERT_NODE);
  const remove = database.prepare(DELETE_NODE);
  for (const operation of operations) {
    if (operation.kind === "upsert") upsert.run(operation.cid, operation.node);
    else remove.run(operation.cid);
  }
}

function writeOptionalRoot(
  database: Database.Database,
  name: Uint8Array,
  replacement: OptionalBytes,
): void {
  if (replacement.present) database.prepare(UPSERT_ROOT).run(name, replacement.value);
  else database.prepare(DELETE_ROOT).run(name);
}

function cloneNodeMutation(operation: NodeMutation): NodeMutation {
  return operation.kind === "upsert"
    ? upsertNode(operation.cid, operation.node)
    : deleteNode(operation.cid);
}

function cloneRootWrite(write: RootWrite): RootWrite {
  return write.kind === "put"
    ? { kind: "put", name: ownBytes(write.name), manifest: ownBytes(write.manifest) }
    : { kind: "delete", name: ownBytes(write.name) };
}

function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean {
  return (
    left.present === right.present &&
    (!left.present || Buffer.from(left.value).equals(Buffer.from(right.value)))
  );
}

function rowBytes(row: unknown, column: string): Uint8Array {
  if (typeof row !== "object" || row === null) {
    throw new StoreError("invalid_data", "SQLite returned a malformed row");
  }
  const value = (row as Record<string, unknown>)[column];
  if (!(value instanceof Uint8Array)) {
    throw new StoreError("invalid_data", "SQLite returned a non-binary value");
  }
  return ownBytes(value);
}

function mapSqliteError(error: unknown): StoreError {
  if (error instanceof StoreError) return error;
  const providerCode =
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    typeof error.code === "string" &&
    /^SQLITE_[A-Z0-9_]+$/.test(error.code)
      ? error.code
      : undefined;
  const retryable = providerCode === "SQLITE_BUSY" || providerCode === "SQLITE_LOCKED";
  return new StoreError(retryable ? "unavailable" : "internal", "SQLite operation failed", {
    retryable,
    providerCode,
    cause: error,
  });
}
