import { RESP_TYPES, type RedisClientType } from "redis";

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

const CAS_SCRIPT = `
local current = redis.call('GET', KEYS[1])
local expected_present = ARGV[1] == '1'
if expected_present then
  if current == false or current ~= ARGV[2] then
    return {0, current == false and 0 or 1, current or ''}
  end
elseif current ~= false then
  return {0, 1, current}
end
if ARGV[3] == '1' then
  redis.call('SET', KEYS[1], ARGV[4])
  return {1, 1, ARGV[4]}
end
redis.call('DEL', KEYS[1])
return {1, 0, ''}
`;

const MUTATE_SCRIPT = `
for index = 1, #KEYS do
  local offset = (index - 1) * 2
  if ARGV[offset + 1] == '1' then
    redis.call('SET', KEYS[index], ARGV[offset + 2])
  else
    redis.call('DEL', KEYS[index])
  end
end
return 1
`;

const TRANSACTION_SCRIPT = `
local condition_count = tonumber(ARGV[1])
local node_count = tonumber(ARGV[2])
local root_count = tonumber(ARGV[3])
local argument = 4
for index = 1, condition_count do
  local current = redis.call('GET', KEYS[index])
  local expected_present = ARGV[argument] == '1'
  local matches = (expected_present and current ~= false and current == ARGV[argument + 1])
    or (not expected_present and current == false)
  if not matches then
    return {0, index, current == false and 0 or 1, current or ''}
  end
  argument = argument + 2
end
local key_index = condition_count + 1
for _ = 1, node_count do
  if ARGV[argument] == '1' then
    redis.call('SET', KEYS[key_index], ARGV[argument + 1])
  else
    redis.call('DEL', KEYS[key_index])
  end
  argument = argument + 2
  key_index = key_index + 1
end
for _ = 1, root_count do
  if ARGV[argument] == '1' then
    redis.call('SET', KEYS[key_index], ARGV[argument + 1])
  else
    redis.call('DEL', KEYS[key_index])
  end
  argument = argument + 2
  key_index = key_index + 1
end
return {1}
`;

const ASCII = {
  node: Buffer.from("node:"),
  root: Buffer.from("root:"),
  hint: Buffer.from("hint:"),
} as const;

export interface RedisStoreOptions {
  readonly keyPrefix?: Uint8Array;
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

interface BinaryClient {
  sendCommand<T = unknown>(
    args: Array<string | Buffer>,
    options?: { readonly abortSignal?: AbortSignal },
  ): Promise<T>;
}

export class RedisStore implements RemoteStore {
  readonly #client: BinaryClient;
  readonly #keyPrefix: Buffer;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(client: RedisClientType, options: RedisStoreOptions = {}) {
    if (client == null) throw new StoreError("invalid_argument", "Redis client is required");
    this.#client = client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer }) as BinaryClient;
    this.#keyPrefix = Buffer.from(options.keyPrefix ?? Buffer.from("prolly:"));
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 1,
      adapterName: options.adapterName?.trim() || "redis-v1",
      provider: "redis",
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

  async close(): Promise<void> {
    this.#accepting = false;
    await Promise.allSettled([...this.#pending]);
  }

  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> {
    return this.#run("descriptor", signal, async () => this.#descriptor);
  }

  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#get(this.#familyKey(ASCII.node, cid), "get_node", signal);
  }

  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#set(this.#familyKey(ASCII.node, cid), value, "put_node", signal);
  }

  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#delete(this.#familyKey(ASCII.node, cid), "delete_node", signal);
  }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const owned = operations.map(cloneNodeMutation);
    return this.#run("batch_nodes", signal, async () => {
      if (owned.length === 0) return;
      const keys = owned.map((operation) => this.#familyKey(ASCII.node, operation.cid));
      const args = owned.flatMap((operation) => operation.kind === "upsert"
        ? [Buffer.from("1"), Buffer.from(operation.node)]
        : [Buffer.from("0"), Buffer.alloc(0)]);
      await this.#eval(MUTATE_SCRIPT, keys, args, signal);
    });
  }

  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> {
    const keys = cids.map((cid) => this.#familyKey(ASCII.node, cid));
    return this.#run("batch_get_nodes_ordered", signal, async () => {
      if (keys.length === 0) return [];
      const values = await this.#command<unknown>(["MGET", ...keys], signal);
      if (!Array.isArray(values) || values.length !== keys.length) {
        throw new StoreError("invalid_data", "Redis returned an invalid MGET response");
      }
      return values.map(optionalFromRedis);
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#run("list_node_cids", signal, async () => {
      const family = Buffer.concat([this.#keyPrefix, ASCII.node]);
      const keys = await this.#scanFamily(family, signal);
      return keys
        .map((key) => key.subarray(family.length))
        .filter((cid) => cid.length === 32)
        .map(ownBytes)
        .sort(compareBytes);
    });
  }

  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#get(this.#hintKey(namespace, key), "get_hint", signal);
  }

  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#set(this.#hintKey(namespace, key), value, "put_hint", signal);
  }

  async batchPutNodesWithHint(
    nodes: readonly NodeEntry[],
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    const ownedNodes = nodes.map(({ cid, node }) => ({ cid: ownBytes(cid), node: ownBytes(node) }));
    const hintKey = this.#hintKey(namespace, key);
    const hintValue = ownBytes(value);
    return this.#run("batch_put_nodes_with_hint", signal, async () => {
      const keys = [...ownedNodes.map((node) => this.#familyKey(ASCII.node, node.cid)), hintKey];
      const args = [...ownedNodes.map((node) => [Buffer.from("1"), Buffer.from(node.node)]).flat(), Buffer.from("1"), Buffer.from(hintValue)];
      await this.#eval(MUTATE_SCRIPT, keys, args, signal);
    });
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#get(this.#familyKey(ASCII.root, name), "get_root_manifest", signal);
  }

  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#set(this.#familyKey(ASCII.root, name), manifest, "put_root_manifest", signal);
  }

  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> {
    return this.#delete(this.#familyKey(ASCII.root, name), "delete_root_manifest", signal);
  }

  async compareAndSwapRootManifest(
    name: Uint8Array,
    expected: OptionalBytes,
    replacement: OptionalBytes,
    signal?: AbortSignal,
  ): Promise<RootCasResult> {
    const key = this.#familyKey(ASCII.root, name);
    const ownedExpected = normalizeOptionalBytes(expected);
    const ownedReplacement = normalizeOptionalBytes(replacement);
    return this.#run("compare_and_swap_root_manifest", signal, async () => {
      const response = await this.#eval(CAS_SCRIPT, [key], [
        flag(ownedExpected.present), Buffer.from(ownedExpected.value),
        flag(ownedReplacement.present), Buffer.from(ownedReplacement.value),
      ], signal);
      const values = redisArray(response, "CAS");
      return { applied: redisInteger(values[0]) === 1, current: optionalFromParts(values[1], values[2]) };
    });
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#run("list_root_manifests", signal, async () => {
      const family = Buffer.concat([this.#keyPrefix, ASCII.root]);
      const keys = (await this.#scanFamily(family, signal)).sort(Buffer.compare);
      if (keys.length === 0) return [];
      const values = await this.#command<unknown>(["MGET", ...keys], signal);
      if (!Array.isArray(values) || values.length !== keys.length) {
        throw new StoreError("invalid_data", "Redis returned an invalid root MGET response");
      }
      return keys.flatMap((key, index) => {
        const value = values[index];
        return value == null ? [] : [{ name: ownBytes(key.subarray(family.length)), manifest: ownBytes(redisBytes(value)) }];
      });
    });
  }

  async commitTransaction(
    nodes: readonly NodeMutation[],
    conditions: readonly RootCondition[],
    roots: readonly RootWrite[],
    signal?: AbortSignal,
  ): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneNodeMutation);
    const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) }));
    const ownedRoots = roots.map(cloneRootWrite);
    return this.#run("commit_transaction", signal, async () => {
      const keys = [
        ...ownedConditions.map(({ name }) => this.#familyKey(ASCII.root, name)),
        ...ownedNodes.map(({ cid }) => this.#familyKey(ASCII.node, cid)),
        ...ownedRoots.map(({ name }) => this.#familyKey(ASCII.root, name)),
      ];
      const args: Buffer[] = [Buffer.from(String(ownedConditions.length)), Buffer.from(String(ownedNodes.length)), Buffer.from(String(ownedRoots.length))];
      for (const condition of ownedConditions) args.push(flag(condition.expected.present), Buffer.from(condition.expected.value));
      for (const node of ownedNodes) args.push(flag(node.kind === "upsert"), node.kind === "upsert" ? Buffer.from(node.node) : Buffer.alloc(0));
      for (const root of ownedRoots) args.push(flag(root.kind === "put"), root.kind === "put" ? Buffer.from(root.manifest) : Buffer.alloc(0));
      const values = redisArray(await this.#eval(TRANSACTION_SCRIPT, keys, args, signal), "transaction");
      if (redisInteger(values[0]) === 1) return { applied: true };
      const index = redisInteger(values[1]) - 1;
      const conflict = ownedConditions[index];
      if (conflict === undefined) throw new StoreError("invalid_data", "Redis returned an invalid transaction conflict index");
      return {
        applied: false,
        conflict: {
          name: ownBytes(conflict.name),
          expected: normalizeOptionalBytes(conflict.expected),
          current: optionalFromParts(values[2], values[3]),
        },
      };
    });
  }

  async clearNamespace(signal?: AbortSignal): Promise<void> {
    if (this.#keyPrefix.length === 0) throw new StoreError("invalid_argument", "refusing to clear an empty Redis key prefix");
    return this.#run("clear_namespace", signal, async () => {
      const keys = await this.#scanFamily(this.#keyPrefix, signal);
      for (let index = 0; index < keys.length; index += 256) {
        await this.#command(["DEL", ...keys.slice(index, index + 256)], signal);
      }
    });
  }

  async #get(key: Buffer, operation: string, signal?: AbortSignal): Promise<OptionalBytes> {
    return this.#run(operation, signal, async () => optionalFromRedis(await this.#command(["GET", key], signal)));
  }

  async #set(key: Buffer, value: Uint8Array, operation: string, signal?: AbortSignal): Promise<void> {
    const owned = ownBytes(value);
    return this.#run(operation, signal, async () => { await this.#command(["SET", key, Buffer.from(owned)], signal); });
  }

  async #delete(key: Buffer, operation: string, signal?: AbortSignal): Promise<void> {
    return this.#run(operation, signal, async () => { await this.#command(["DEL", key], signal); });
  }

  #familyKey(family: Buffer, suffix: Uint8Array): Buffer {
    return Buffer.concat([this.#keyPrefix, family, Buffer.from(ownBytes(suffix))]);
  }

  #hintKey(namespace: Uint8Array, key: Uint8Array): Buffer {
    const ownedNamespace = ownBytes(namespace);
    const length = Buffer.alloc(8);
    length.writeBigUInt64BE(BigInt(ownedNamespace.byteLength));
    return Buffer.concat([this.#keyPrefix, ASCII.hint, length, Buffer.from(ownedNamespace), Buffer.from(ownBytes(key))]);
  }

  async #scanFamily(family: Buffer, signal?: AbortSignal): Promise<Buffer[]> {
    const keys: Buffer[] = [];
    let cursor = "0";
    do {
      const response = redisArray(await this.#command(["SCAN", cursor, "COUNT", "1024"], signal), "SCAN");
      cursor = redisBytes(response[0]).toString();
      const page = redisArray(response[1], "SCAN keys").map(redisBytes);
      for (const key of page) if (key.subarray(0, family.length).equals(family)) keys.push(Buffer.from(key));
    } while (cursor !== "0");
    return keys;
  }

  async #eval(script: string, keys: readonly Buffer[], args: readonly Buffer[], signal?: AbortSignal): Promise<unknown> {
    return this.#command(["EVAL", script, String(keys.length), ...keys, ...args], signal);
  }

  async #command<T = unknown>(args: Array<string | Buffer>, signal?: AbortSignal): Promise<T> {
    const command = this.#client.sendCommand<T>(args, signal === undefined ? undefined : { abortSignal: signal });
    if (signal === undefined) return command;
    return new Promise<T>((resolve, reject) => {
      const abort = (): void => reject(new StoreError("cancelled", "Redis operation was cancelled", { cause: signal.reason }));
      signal.addEventListener("abort", abort, { once: true });
      command.then(
        (value) => { signal.removeEventListener("abort", abort); resolve(value); },
        (error: unknown) => { signal.removeEventListener("abort", abort); reject(error); },
      );
    });
  }

  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal);
    if (!this.#accepting) throw new StoreError("internal", "Redis store is closed");
    const pending = call().catch((error: unknown) => {
      if (signal?.aborted) throw new StoreError("cancelled", "Redis operation was cancelled", { cause: signal.reason });
      throw mapRedisError(operation, error);
    });
    this.#pending.add(pending);
    try {
      return await pending;
    } finally {
      this.#pending.delete(pending);
    }
  }
}

function cloneNodeMutation(operation: NodeMutation): NodeMutation {
  return operation.kind === "upsert" ? upsertNode(operation.cid, operation.node) : deleteNode(operation.cid);
}

function cloneRootWrite(write: RootWrite): RootWrite {
  const name = ownBytes(write.name);
  return write.kind === "put" ? { kind: "put", name, manifest: ownBytes(write.manifest) } : { kind: "delete", name };
}

function optionalFromRedis(value: unknown): OptionalBytes {
  return value == null ? missingBytes() : presentBytes(redisBytes(value));
}

function optionalFromParts(present: unknown, value: unknown): OptionalBytes {
  return redisInteger(present) === 0 ? missingBytes() : presentBytes(redisBytes(value));
}

function redisArray(value: unknown, operation: string): unknown[] {
  if (!Array.isArray(value)) throw new StoreError("invalid_data", `Redis returned an invalid ${operation} response`);
  return value;
}

function redisBytes(value: unknown): Buffer {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array) return Buffer.from(value);
  if (typeof value === "string" || typeof value === "number") return Buffer.from(String(value));
  throw new StoreError("invalid_data", "Redis returned a non-binary value");
}

function redisInteger(value: unknown): number {
  const parsed = Number(redisBytes(value).toString());
  if (!Number.isSafeInteger(parsed)) throw new StoreError("invalid_data", "Redis returned an invalid integer");
  return parsed;
}

function flag(value: boolean): Buffer { return Buffer.from(value ? "1" : "0"); }
function compareBytes(left: Uint8Array, right: Uint8Array): number { return Buffer.compare(Buffer.from(left), Buffer.from(right)); }

function mapRedisError(operation: string, error: unknown): StoreError {
  if (error instanceof StoreError) return error;
  const code = redisErrorCode(error);
  const retryable = code !== undefined && ["ECONNRESET", "ECONNREFUSED", "ETIMEDOUT", "EPIPE", "NR_CLOSED"].includes(code);
  return new StoreError(retryable ? "unavailable" : "internal", "Redis operation failed", {
    retryable,
    providerCode: code === undefined ? undefined : `redis:${code}:${operation}`,
    cause: error,
  });
}

function redisErrorCode(error: unknown): string | undefined {
  if (typeof error !== "object" || error === null || !("code" in error) || typeof error.code !== "string") return undefined;
  return error.code;
}
