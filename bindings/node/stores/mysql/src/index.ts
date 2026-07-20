import {
  type Pool,
  type PoolConnection,
  type ResultSetHeader,
  type RowDataPacket,
} from "mysql2/promise";

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

const CREATE_SCHEMA = [
  "CREATE TABLE IF NOT EXISTS prolly_nodes (cid VARBINARY(32) PRIMARY KEY, node LONGBLOB NOT NULL)",
  "CREATE TABLE IF NOT EXISTS prolly_hints (namespace VARBINARY(255) NOT NULL, `key` VARBINARY(255) NOT NULL, value LONGBLOB NOT NULL, PRIMARY KEY(namespace, `key`))",
  "CREATE TABLE IF NOT EXISTS prolly_roots (name VARBINARY(255) PRIMARY KEY, manifest LONGBLOB NOT NULL)",
] as const;

const SELECT_NODE = "SELECT node AS value FROM prolly_nodes WHERE cid = ?";
const UPSERT_NODE = `INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)
ON DUPLICATE KEY UPDATE node = VALUES(node)`;
const DELETE_NODE = "DELETE FROM prolly_nodes WHERE cid = ?";
const SELECT_HINT = "SELECT value FROM prolly_hints WHERE namespace = ? AND `key` = ?";
const UPSERT_HINT = `INSERT INTO prolly_hints (namespace, \`key\`, value) VALUES (?, ?, ?)
ON DUPLICATE KEY UPDATE value = VALUES(value)`;
const SELECT_ROOT = "SELECT manifest AS value FROM prolly_roots WHERE name = ?";
const SELECT_ROOT_FOR_UPDATE = `${SELECT_ROOT} FOR UPDATE`;
const UPSERT_ROOT = `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)
ON DUPLICATE KEY UPDATE manifest = VALUES(manifest)`;
const DELETE_ROOT = "DELETE FROM prolly_roots WHERE name = ?";

export interface MysqlStoreOptions {
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

export class MysqlStore implements RemoteStore {
  readonly #pool: Pool;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(pool: Pool, options: MysqlStoreOptions = {}) {
    if (pool == null) throw new StoreError("invalid_argument", "MySQL pool is required");
    this.#pool = pool;
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 2,
      adapterName: options.adapterName?.trim() || "mysql-v1",
      provider: "mysql",
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
    return this.#run("initialize_schema", signal, () =>
      this.#withConnection(signal, async (connection) => {
        for (const statement of CREATE_SCHEMA) await this.#execute(connection, statement, [], signal);
      }),
    );
  }

  async close(): Promise<void> {
    if (!this.#accepting) {
      await Promise.allSettled([...this.#pending]);
      return;
    }
    this.#accepting = false;
    await Promise.allSettled([...this.#pending]);
  }

  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> {
    return this.#run("descriptor", signal, async () => this.#descriptor);
  }

  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const key = validateBinaryKey(cid, 32, "node CID");
    return this.#run("get_node", signal, () => this.#queryOptional(SELECT_NODE, [buffer(key)], signal));
  }

  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const key = validateBinaryKey(cid, 32, "node CID");
    const node = ownBytes(value);
    return this.#run("put_node", signal, () => this.#executeOnce(UPSERT_NODE, [buffer(key), buffer(node)], signal));
  }

  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> {
    const key = validateBinaryKey(cid, 32, "node CID");
    return this.#run("delete_node", signal, () => this.#executeOnce(DELETE_NODE, [buffer(key)], signal));
  }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation);
    return this.#run("batch_nodes", signal, () =>
      this.#transaction(signal, (connection) => applyNodeMutations(this, connection, owned, signal)),
    );
  }

  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> {
    return publishNodesWithGeneralPath(this, publication, signal);
  }

  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> {
    const keys = cids.map((cid) => validateBinaryKey(cid, 32, "node CID"));
    return this.#run("batch_get_nodes_ordered", signal, async () => {
      if (keys.length === 0) return [];
      const placeholders = keys.map(() => "?").join(",");
      const rows = await this.#withConnection(signal, (connection) =>
        this.#rows<{ cid: Buffer; node: Buffer } & RowDataPacket>(
          connection,
          `SELECT cid, node FROM prolly_nodes WHERE cid IN (${placeholders})`,
          keys.map(buffer),
          signal,
        ),
      );
      const values = new Map(rows.map(({ cid, node }) => [cid.toString("hex"), ownBytes(node)]));
      return keys.map((key) => {
        const value = values.get(buffer(key).toString("hex"));
        return value === undefined ? missingBytes() : presentBytes(value);
      });
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#run("list_node_cids", signal, async () => {
      const rows = await this.#withConnection(signal, (connection) =>
        this.#rows<{ cid: Buffer } & RowDataPacket>(connection, "SELECT cid FROM prolly_nodes ORDER BY cid", [], signal),
      );
      return rows.map(({ cid }) => ownBytes(cid));
    });
  }

  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const ownedNamespace = validateBinaryKey(namespace, 255, "hint namespace");
    const ownedKey = validateBinaryKey(key, 255, "hint key");
    return this.#run("get_hint", signal, () =>
      this.#queryOptional(SELECT_HINT, [buffer(ownedNamespace), buffer(ownedKey)], signal),
    );
  }

  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedNamespace = validateBinaryKey(namespace, 255, "hint namespace");
    const ownedKey = validateBinaryKey(key, 255, "hint key");
    const ownedValue = ownBytes(value);
    return this.#run("put_hint", signal, () =>
      this.#executeOnce(UPSERT_HINT, [buffer(ownedNamespace), buffer(ownedKey), buffer(ownedValue)], signal),
    );
  }

  async batchPutNodesWithHint(
    nodes: readonly NodeEntry[],
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    const ownedNodes = nodes.map(({ cid, node }) => ({
      cid: validateBinaryKey(cid, 32, "node CID"),
      node: ownBytes(node),
    }));
    const ownedNamespace = validateBinaryKey(namespace, 255, "hint namespace");
    const ownedKey = validateBinaryKey(key, 255, "hint key");
    const ownedValue = ownBytes(value);
    return this.#run("batch_put_nodes_with_hint", signal, () =>
      this.#transaction(signal, async (connection) => {
        for (const node of ownedNodes) await this.#execute(connection, UPSERT_NODE, [buffer(node.cid), buffer(node.node)], signal);
        await this.#execute(connection, UPSERT_HINT, [buffer(ownedNamespace), buffer(ownedKey), buffer(ownedValue)], signal);
      }),
    );
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const ownedName = validateBinaryKey(name, 255, "root name");
    return this.#run("get_root_manifest", signal, () => this.#queryOptional(SELECT_ROOT, [buffer(ownedName)], signal));
  }

  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedName = validateBinaryKey(name, 255, "root name");
    const ownedManifest = ownBytes(manifest);
    return this.#run("put_root_manifest", signal, () =>
      this.#executeOnce(UPSERT_ROOT, [buffer(ownedName), buffer(ownedManifest)], signal),
    );
  }

  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedName = validateBinaryKey(name, 255, "root name");
    return this.#run("delete_root_manifest", signal, () => this.#executeOnce(DELETE_ROOT, [buffer(ownedName)], signal));
  }

  async compareAndSwapRootManifest(
    name: Uint8Array,
    expected: OptionalBytes,
    replacement: OptionalBytes,
    signal?: AbortSignal,
  ): Promise<RootCasResult> {
    const ownedName = validateBinaryKey(name, 255, "root name");
    const ownedExpected = normalizeOptionalBytes(expected);
    const ownedReplacement = normalizeOptionalBytes(replacement);
    return this.#run("compare_and_swap_root_manifest", signal, () =>
      this.#transaction(signal, async (connection) => {
        const current = await queryOptionalWithConnection(this, connection, SELECT_ROOT_FOR_UPDATE, [buffer(ownedName)], signal);
        if (!optionalEqual(current, ownedExpected)) return { applied: false, current };
        await writeOptionalRoot(this, connection, ownedName, ownedReplacement, signal);
        return { applied: true, current: normalizeOptionalBytes(ownedReplacement) };
      }),
    );
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#run("list_root_manifests", signal, async () => {
      const rows = await this.#withConnection(signal, (connection) =>
        this.#rows<{ name: Buffer; manifest: Buffer } & RowDataPacket>(
          connection,
          "SELECT name, manifest FROM prolly_roots ORDER BY name",
          [],
          signal,
        ),
      );
      return rows.map(({ name, manifest }) => ({ name: ownBytes(name), manifest: ownBytes(manifest) }));
    });
  }

  async commitTransaction(
    nodes: readonly NodeMutation[],
    conditions: readonly RootCondition[],
    roots: readonly RootWrite[],
    signal?: AbortSignal,
  ): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation);
    const ownedConditions = conditions.map(({ name, expected }) => ({
      name: validateBinaryKey(name, 255, "root name"),
      expected: normalizeOptionalBytes(expected),
    }));
    const ownedRoots = roots.map((root) => cloneRootWrite(root));
    return this.#run("commit_transaction", signal, () =>
      this.#transaction(signal, async (connection) => {
        const currentByName = new Map<string, OptionalBytes>();
        for (const name of uniqueSortedNames(ownedConditions.map(({ name }) => name))) {
          currentByName.set(
            buffer(name).toString("hex"),
            await queryOptionalWithConnection(this, connection, SELECT_ROOT_FOR_UPDATE, [buffer(name)], signal),
          );
        }
        for (const condition of ownedConditions) {
          const current = currentByName.get(buffer(condition.name).toString("hex"))!;
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
        await applyNodeMutations(this, connection, ownedNodes, signal);
        for (const root of ownedRoots) {
          await writeOptionalRoot(
            this,
            connection,
            root.name,
            root.kind === "put" ? presentBytes(root.manifest) : missingBytes(),
            signal,
          );
        }
        return { applied: true };
      }),
    );
  }

  async rawExecute(
    connection: PoolConnection,
    sql: string,
    values: readonly Buffer[],
    signal?: AbortSignal,
  ): Promise<ResultSetHeader> {
    return this.#execute(connection, sql, values, signal);
  }

  async rawRows<R extends RowDataPacket>(
    connection: PoolConnection,
    sql: string,
    values: readonly Buffer[],
    signal?: AbortSignal,
  ): Promise<R[]> {
    return this.#rows<R>(connection, sql, values, signal);
  }

  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal);
    if (!this.#accepting) throw new StoreError("internal", "MySQL store is closed");
    const pending = call().catch((error: unknown) => {
      if (signal?.aborted) throw new StoreError("cancelled", "MySQL operation was cancelled", { cause: signal.reason });
      throw mapMysqlError(operation, error);
    });
    this.#pending.add(pending);
    try {
      return await pending;
    } finally {
      this.#pending.delete(pending);
    }
  }

  async #executeOnce(sql: string, values: readonly Buffer[], signal?: AbortSignal): Promise<void> {
    await this.#withConnection(signal, (connection) => this.#execute(connection, sql, values, signal));
  }

  async #queryOptional(sql: string, values: readonly Buffer[], signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#withConnection(signal, (connection) => queryOptionalWithConnection(this, connection, sql, values, signal));
  }

  async #withConnection<T>(signal: AbortSignal | undefined, call: (connection: PoolConnection) => Promise<T>): Promise<T> {
    const connection = await connectWithAbort(this.#pool, signal);
    try {
      return await call(connection);
    } finally {
      connection.release();
    }
  }

  async #transaction<T>(signal: AbortSignal | undefined, call: (connection: PoolConnection) => Promise<T>): Promise<T> {
    for (let attempt = 0; ; attempt += 1) {
      try {
        return await this.#withConnection(signal, async (connection) => {
          await this.#statement(connection, () => connection.beginTransaction(), signal);
          try {
            const result = await call(connection);
            await this.#statement(connection, () => connection.commit(), signal);
            return result;
          } catch (error) {
            if (!signal?.aborted) await connection.rollback().catch(() => undefined);
            throw error;
          }
        });
      } catch (error) {
        if (attempt >= 5 || !isRetryableTransactionError(error)) throw error;
        throwIfAborted(signal);
        await new Promise<void>((resolve) => setTimeout(resolve, 5 * (attempt + 1)));
        throwIfAborted(signal);
      }
    }
  }

  async #execute(
    connection: PoolConnection,
    sql: string,
    values: readonly Buffer[],
    signal?: AbortSignal,
  ): Promise<ResultSetHeader> {
    const [result] = await this.#statement(connection, () => connection.execute<ResultSetHeader>(sql, [...values]), signal);
    return result;
  }

  async #rows<R extends RowDataPacket>(
    connection: PoolConnection,
    sql: string,
    values: readonly Buffer[],
    signal?: AbortSignal,
  ): Promise<R[]> {
    const [rows] = await this.#statement(connection, () => connection.execute<R[]>(sql, [...values]), signal);
    return rows;
  }

  async #statement<T>(connection: PoolConnection, call: () => Promise<T>, signal?: AbortSignal): Promise<T> {
    throwIfAborted(signal);
    const running = call();
    if (signal === undefined) return running;
    return new Promise<T>((resolve, reject) => {
      let settled = false;
      const abort = (): void => {
        if (settled) return;
        settled = true;
        connection.destroy();
        reject(new StoreError("cancelled", "MySQL operation was cancelled", { cause: signal.reason }));
      };
      signal.addEventListener("abort", abort, { once: true });
      running.then(
        (result) => {
          if (settled) return;
          settled = true;
          signal.removeEventListener("abort", abort);
          if (signal.aborted) reject(new StoreError("cancelled", "MySQL operation was cancelled", { cause: signal.reason }));
          else resolve(result);
        },
        (error: unknown) => {
          if (settled) return;
          settled = true;
          signal.removeEventListener("abort", abort);
          reject(error);
        },
      );
    });
  }
}

async function connectWithAbort(pool: Pool, signal?: AbortSignal): Promise<PoolConnection> {
  throwIfAborted(signal);
  const connecting = pool.getConnection();
  if (signal === undefined) return connecting;
  return new Promise<PoolConnection>((resolve, reject) => {
    let settled = false;
    const abort = (): void => {
      if (settled) return;
      settled = true;
      connecting.then((connection) => connection.release(), () => undefined);
      reject(new StoreError("cancelled", "MySQL operation was cancelled", { cause: signal.reason }));
    };
    signal.addEventListener("abort", abort, { once: true });
    connecting.then(
      (connection) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        resolve(connection);
      },
      (error: unknown) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        reject(error);
      },
    );
  });
}

async function queryOptionalWithConnection(
  store: MysqlStore,
  connection: PoolConnection,
  sql: string,
  values: readonly Buffer[],
  signal?: AbortSignal,
): Promise<OptionalBytes> {
  const rows = await store.rawRows<{ value: Buffer } & RowDataPacket>(connection, sql, values, signal);
  return rows[0] === undefined ? missingBytes() : presentBytes(rowBytes(rows[0], "value"));
}

async function applyNodeMutations(
  store: MysqlStore,
  connection: PoolConnection,
  operations: readonly NodeMutation[],
  signal?: AbortSignal,
): Promise<void> {
  for (const operation of operations) {
    if (operation.kind === "upsert") await store.rawExecute(connection, UPSERT_NODE, [buffer(operation.cid), buffer(operation.node)], signal);
    else await store.rawExecute(connection, DELETE_NODE, [buffer(operation.cid)], signal);
  }
}

async function writeOptionalRoot(
  store: MysqlStore,
  connection: PoolConnection,
  name: Uint8Array,
  replacement: OptionalBytes,
  signal?: AbortSignal,
): Promise<void> {
  if (replacement.present) await store.rawExecute(connection, UPSERT_ROOT, [buffer(name), buffer(replacement.value)], signal);
  else await store.rawExecute(connection, DELETE_ROOT, [buffer(name)], signal);
}

function cloneNodeMutation(operation: NodeMutation): NodeMutation {
  const cid = validateBinaryKey(operation.cid, 32, "node CID");
  return operation.kind === "upsert" ? upsertNode(cid, operation.node) : deleteNode(cid);
}

function cloneRootWrite(write: RootWrite): RootWrite {
  const name = validateBinaryKey(write.name, 255, "root name");
  return write.kind === "put"
    ? { kind: "put", name, manifest: ownBytes(write.manifest) }
    : { kind: "delete", name };
}

function validateBinaryKey(value: Uint8Array, maximum: number, name: string): Uint8Array {
  if (!(value instanceof Uint8Array)) throw new StoreError("invalid_argument", `${name} must be bytes`);
  if (value.byteLength > maximum) throw new StoreError("invalid_argument", `${name} exceeds ${maximum} bytes`);
  return ownBytes(value);
}

function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean {
  return left.present === right.present && (!left.present || buffer(left.value).equals(buffer(right.value)));
}

function uniqueSortedNames(names: readonly Uint8Array[]): Uint8Array[] {
  const sorted = names.map(ownBytes).sort((left, right) => Buffer.compare(buffer(left), buffer(right)));
  return sorted.filter((name, index) => index === 0 || !buffer(name).equals(buffer(sorted[index - 1]!)));
}

function rowBytes(row: RowDataPacket, column: string): Uint8Array {
  const value = row[column] as unknown;
  if (!(value instanceof Uint8Array)) throw new StoreError("invalid_data", "MySQL returned a non-binary value");
  return ownBytes(value);
}

function buffer(value: Uint8Array): Buffer {
  return Buffer.from(value);
}

function mapMysqlError(operation: string, error: unknown): StoreError {
  if (error instanceof StoreError) return error;
  const errno = mysqlErrno(error);
  const retryable = errno !== undefined && [1040, 1205, 1213, 2006, 2013].includes(errno);
  return new StoreError(retryable ? "unavailable" : "internal", "MySQL operation failed", {
    retryable,
    providerCode: errno === undefined ? undefined : `mysql:${errno}:${operation}`,
    cause: error,
  });
}

function mysqlErrno(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null || !("errno" in error) || typeof error.errno !== "number") return undefined;
  return Number.isSafeInteger(error.errno) && error.errno >= 0 ? error.errno : undefined;
}

function isRetryableTransactionError(error: unknown): boolean {
  const errno = mysqlErrno(error);
  return errno === 1205 || errno === 1213;
}
