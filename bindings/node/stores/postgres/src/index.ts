import {
  Client,
  Pool,
  type PoolClient,
  type QueryResult,
  type QueryResultRow,
} from "pg";

import {
  StoreError,
  deleteNode,
  missingBytes,
  normalizeOptionalBytes,
  ownBytes,
  presentBytes,
  throwIfAborted,
  upsertNode,
  validateStoreDescriptor,
  type NamedStoreRoot,
  type NodeEntry,
  type NodeMutation,
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
  cid bytea PRIMARY KEY,
  node bytea NOT NULL
);
CREATE TABLE IF NOT EXISTS prolly_hints (
  namespace bytea NOT NULL,
  key bytea NOT NULL,
  value bytea NOT NULL,
  PRIMARY KEY(namespace, key)
);
CREATE TABLE IF NOT EXISTS prolly_roots (
  name bytea PRIMARY KEY,
  manifest bytea NOT NULL
);
`;

const SELECT_NODE = "SELECT node AS value FROM prolly_nodes WHERE cid = $1";
const UPSERT_NODE = `INSERT INTO prolly_nodes (cid, node) VALUES ($1, $2)
ON CONFLICT(cid) DO UPDATE SET node = excluded.node`;
const DELETE_NODE = "DELETE FROM prolly_nodes WHERE cid = $1";
const SELECT_HINT = "SELECT value FROM prolly_hints WHERE namespace = $1 AND key = $2";
const UPSERT_HINT = `INSERT INTO prolly_hints (namespace, key, value) VALUES ($1, $2, $3)
ON CONFLICT(namespace, key) DO UPDATE SET value = excluded.value`;
const SELECT_ROOT = "SELECT manifest AS value FROM prolly_roots WHERE name = $1";
const SELECT_ROOT_FOR_UPDATE = `${SELECT_ROOT} FOR UPDATE`;
const UPSERT_ROOT = `INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2)
ON CONFLICT(name) DO UPDATE SET manifest = excluded.manifest`;
const DELETE_ROOT = "DELETE FROM prolly_roots WHERE name = $1";
const LOCK_ROOT = "SELECT pg_advisory_xact_lock(hashtextextended(encode($1::bytea, 'hex'), 0))";

export interface PostgresStoreOptions {
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

export class PostgresStore implements RemoteStore {
  readonly #pool: Pool;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(pool: Pool, options: PostgresStoreOptions = {}) {
    if (pool == null) throw new StoreError("invalid_argument", "PostgreSQL pool is required");
    this.#pool = pool;
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 1,
      adapterName: options.adapterName?.trim() || "postgres-v1",
      provider: "postgresql",
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
    return this.#run("initialize_schema", signal, async () => {
      await this.#withClient(signal, (client) => this.#query(client, CREATE_SCHEMA_SQL, [], signal));
    });
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
    const key = ownBytes(cid);
    return this.#run("get_node", signal, () => this.#queryOptional(SELECT_NODE, [key], signal));
  }

  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const key = ownBytes(cid);
    const node = ownBytes(value);
    return this.#run("put_node", signal, async () => {
      await this.#execute(UPSERT_NODE, [key, node], signal);
    });
  }

  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> {
    const key = ownBytes(cid);
    return this.#run("delete_node", signal, async () => {
      await this.#execute(DELETE_NODE, [key], signal);
    });
  }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation);
    return this.#run("batch_nodes", signal, () =>
      this.#transaction(signal, async (client) => applyNodeMutations(this, client, owned, signal)),
    );
  }

  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> {
    const keys = cids.map(ownBytes);
    return this.#run("batch_get_nodes_ordered", signal, async () => {
      if (keys.length === 0) return [];
      const result = await this.#withClient(signal, (client) =>
        this.#query<{ cid: Buffer; node: Buffer }>(
          client,
          "SELECT cid, node FROM prolly_nodes WHERE cid = ANY($1::bytea[])",
          [keys.map((key) => Buffer.from(key))],
          signal,
        ),
      );
      const values = new Map(result.rows.map(({ cid, node }) => [cid.toString("hex"), ownBytes(node)]));
      return keys.map((key) => {
        const value = values.get(Buffer.from(key).toString("hex"));
        return value === undefined ? missingBytes() : presentBytes(value);
      });
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#run("list_node_cids", signal, async () => {
      const result = await this.#withClient(signal, (client) =>
        this.#query<{ cid: Buffer }>(client, "SELECT cid FROM prolly_nodes ORDER BY cid", [], signal),
      );
      return result.rows.map(({ cid }) => ownBytes(cid));
    });
  }

  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const ownedNamespace = ownBytes(namespace);
    const ownedKey = ownBytes(key);
    return this.#run("get_hint", signal, () => this.#queryOptional(SELECT_HINT, [ownedNamespace, ownedKey], signal));
  }

  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedNamespace = ownBytes(namespace);
    const ownedKey = ownBytes(key);
    const ownedValue = ownBytes(value);
    return this.#run("put_hint", signal, async () => {
      await this.#execute(UPSERT_HINT, [ownedNamespace, ownedKey, ownedValue], signal);
    });
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
    return this.#run("batch_put_nodes_with_hint", signal, () =>
      this.#transaction(signal, async (client) => {
        for (const node of ownedNodes) await this.#query(client, UPSERT_NODE, [node.cid, node.node], signal);
        await this.#query(client, UPSERT_HINT, [ownedNamespace, ownedKey, ownedValue], signal);
      }),
    );
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    const ownedName = ownBytes(name);
    return this.#run("get_root_manifest", signal, () => this.#queryOptional(SELECT_ROOT, [ownedName], signal));
  }

  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedName = ownBytes(name);
    const ownedManifest = ownBytes(manifest);
    return this.#run("put_root_manifest", signal, async () => {
      await this.#execute(UPSERT_ROOT, [ownedName, ownedManifest], signal);
    });
  }

  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> {
    const ownedName = ownBytes(name);
    return this.#run("delete_root_manifest", signal, async () => {
      await this.#execute(DELETE_ROOT, [ownedName], signal);
    });
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
    return this.#run("compare_and_swap_root_manifest", signal, () =>
      this.#transaction(signal, async (client) => {
        await this.#query(client, LOCK_ROOT, [ownedName], signal);
        const current = await queryOptionalWithClient(this, client, SELECT_ROOT_FOR_UPDATE, [ownedName], signal);
        if (!optionalEqual(current, ownedExpected)) return { applied: false, current };
        await writeOptionalRoot(this, client, ownedName, ownedReplacement, signal);
        return { applied: true, current: normalizeOptionalBytes(ownedReplacement) };
      }),
    );
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#run("list_root_manifests", signal, async () => {
      const result = await this.#withClient(signal, (client) =>
        this.#query<{ name: Buffer; manifest: Buffer }>(
          client,
          "SELECT name, manifest FROM prolly_roots ORDER BY name",
          [],
          signal,
        ),
      );
      return result.rows.map(({ name, manifest }) => ({ name: ownBytes(name), manifest: ownBytes(manifest) }));
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
      name: ownBytes(name),
      expected: normalizeOptionalBytes(expected),
    }));
    const ownedRoots = roots.map(cloneRootWrite);
    return this.#run("commit_transaction", signal, () =>
      this.#transaction(signal, async (client) => {
        for (const name of uniqueSortedNames(ownedConditions.map(({ name }) => name))) {
          await this.#query(client, LOCK_ROOT, [name], signal);
        }
        for (const condition of ownedConditions) {
          const current = await queryOptionalWithClient(this, client, SELECT_ROOT_FOR_UPDATE, [condition.name], signal);
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
        await applyNodeMutations(this, client, ownedNodes, signal);
        for (const root of ownedRoots) {
          await writeOptionalRoot(
            this,
            client,
            root.name,
            root.kind === "put" ? presentBytes(root.manifest) : missingBytes(),
            signal,
          );
        }
        return { applied: true };
      }),
    );
  }

  async rawQuery<R extends QueryResultRow>(
    client: PoolClient,
    text: string,
    values: readonly unknown[],
    signal?: AbortSignal,
  ): Promise<QueryResult<R>> {
    return this.#query<R>(client, text, values, signal);
  }

  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal);
    if (!this.#accepting) throw new StoreError("internal", "PostgreSQL store is closed");
    const pending = call().catch((error: unknown) => {
      if (signal?.aborted) {
        throw new StoreError("cancelled", "PostgreSQL operation was cancelled", { cause: signal.reason });
      }
      throw mapPostgresError(operation, error);
    });
    this.#pending.add(pending);
    try {
      return await pending;
    } finally {
      this.#pending.delete(pending);
    }
  }

  async #execute(text: string, values: readonly unknown[], signal?: AbortSignal): Promise<void> {
    await this.#withClient(signal, (client) => this.#query(client, text, values, signal));
  }

  async #queryOptional(text: string, values: readonly unknown[], signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#withClient(signal, (client) => queryOptionalWithClient(this, client, text, values, signal));
  }

  async #withClient<T>(signal: AbortSignal | undefined, call: (client: PoolClient) => Promise<T>): Promise<T> {
    const client = await connectWithAbort(this.#pool, signal);
    try {
      return await call(client);
    } finally {
      client.release();
    }
  }

  async #transaction<T>(signal: AbortSignal | undefined, call: (client: PoolClient) => Promise<T>): Promise<T> {
    return this.#withClient(signal, async (client) => {
      await this.#query(client, "BEGIN", [], signal);
      try {
        const result = await call(client);
        await this.#query(client, "COMMIT", [], signal);
        return result;
      } catch (error) {
        await this.#query(client, "ROLLBACK", [], undefined).catch(() => undefined);
        throw error;
      }
    });
  }

  async #query<R extends QueryResultRow>(
    client: PoolClient,
    text: string,
    values: readonly unknown[],
    signal?: AbortSignal,
  ): Promise<QueryResult<R>> {
    throwIfAborted(signal);
    const running = client.query<R>(text, [...values]);
    return new Promise<QueryResult<R>>((resolve, reject) => {
      const abort = (): void => {
        const processId = (client as unknown as { processID?: number }).processID;
        if (processId === undefined) return;
        const cancelClient = new Client(this.#pool.options);
        void cancelClient
          .connect()
          .then(() => cancelClient.query("SELECT pg_cancel_backend($1)", [processId]))
          .finally(() => cancelClient.end())
          .catch(() => undefined);
      };
      signal?.addEventListener("abort", abort, { once: true });
      running.then((result) => {
        signal?.removeEventListener("abort", abort);
        if (signal?.aborted) {
          reject(new StoreError("cancelled", "PostgreSQL operation was cancelled", { cause: signal.reason }));
        } else {
          resolve(result);
        }
      }, (error: unknown) => {
        signal?.removeEventListener("abort", abort);
        reject(error);
      });
    });
  }
}

async function connectWithAbort(pool: Pool, signal?: AbortSignal): Promise<PoolClient> {
  throwIfAborted(signal);
  const connecting = pool.connect();
  if (signal === undefined) return connecting;
  return new Promise<PoolClient>((resolve, reject) => {
    let settled = false;
    const abort = (): void => {
      if (settled) return;
      settled = true;
      connecting.then((client) => client.release(), () => undefined);
      reject(new StoreError("cancelled", "PostgreSQL operation was cancelled", { cause: signal.reason }));
    };
    signal.addEventListener("abort", abort, { once: true });
    connecting.then(
      (client) => {
        if (settled) return;
        settled = true;
        signal.removeEventListener("abort", abort);
        resolve(client);
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

async function queryOptionalWithClient(
  store: PostgresStore,
  client: PoolClient,
  text: string,
  values: readonly unknown[],
  signal?: AbortSignal,
): Promise<OptionalBytes> {
  const result = await store.rawQuery<{ value: Buffer }>(client, text, values, signal);
  const value = result.rows[0]?.value;
  return value === undefined ? missingBytes() : presentBytes(value);
}

async function applyNodeMutations(
  store: PostgresStore,
  client: PoolClient,
  operations: readonly NodeMutation[],
  signal?: AbortSignal,
): Promise<void> {
  for (const operation of operations) {
    if (operation.kind === "upsert") await store.rawQuery(client, UPSERT_NODE, [operation.cid, operation.node], signal);
    else await store.rawQuery(client, DELETE_NODE, [operation.cid], signal);
  }
}

async function writeOptionalRoot(
  store: PostgresStore,
  client: PoolClient,
  name: Uint8Array,
  replacement: OptionalBytes,
  signal?: AbortSignal,
): Promise<void> {
  if (replacement.present) await store.rawQuery(client, UPSERT_ROOT, [name, replacement.value], signal);
  else await store.rawQuery(client, DELETE_ROOT, [name], signal);
}

function cloneNodeMutation(operation: NodeMutation): NodeMutation {
  return operation.kind === "upsert" ? upsertNode(operation.cid, operation.node) : deleteNode(operation.cid);
}

function cloneRootWrite(write: RootWrite): RootWrite {
  return write.kind === "put"
    ? { kind: "put", name: ownBytes(write.name), manifest: ownBytes(write.manifest) }
    : { kind: "delete", name: ownBytes(write.name) };
}

function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean {
  return left.present === right.present && (!left.present || Buffer.from(left.value).equals(Buffer.from(right.value)));
}

function uniqueSortedNames(names: readonly Uint8Array[]): Uint8Array[] {
  const sorted = names.map(ownBytes).sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
  return sorted.filter((name, index) => index === 0 || !Buffer.from(name).equals(Buffer.from(sorted[index - 1]!)));
}

function mapPostgresError(operation: string, error: unknown): StoreError {
  if (error instanceof StoreError) return error;
  const code = providerCode(error);
  const retryable = code?.startsWith("08") === true || code === "40001" || code === "40P01" || code === "55P03";
  return new StoreError(retryable ? "unavailable" : "internal", "PostgreSQL operation failed", {
    retryable,
    providerCode: code === undefined ? undefined : `postgres:${code}:${operation}`,
    cause: error,
  });
}

function providerCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error) || typeof error.code !== "string") return undefined;
  return /^[0-9A-Z]{5}$/.test(error.code) ? error.code : undefined;
}
