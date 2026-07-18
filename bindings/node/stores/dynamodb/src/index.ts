import {
  BatchGetItemCommand,
  BatchWriteItemCommand,
  CreateTableCommand,
  DeleteItemCommand,
  DeleteTableCommand,
  DescribeTableCommand,
  GetItemCommand,
  PutItemCommand,
  ScanCommand,
  TransactWriteItemsCommand,
  type AttributeValue,
  type DynamoDBClient,
  type TransactWriteItem,
  type WriteRequest,
} from "@aws-sdk/client-dynamodb";

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

const BATCH_GET_LIMIT = 100;
const BATCH_WRITE_LIMIT = 25;
const TRANSACTION_LIMIT = 100;
const RETRY_LIMIT = 8;
const PK = "pk";
const VALUE = "value";
const NODE = Buffer.from("node:");
const ROOT = Buffer.from("root:");
const HINT = Buffer.from("hint:");

export interface DynamoDbStoreOptions {
  readonly tableName: string;
  readonly keyPrefix?: Uint8Array;
  readonly adapterName?: string;
  readonly readParallelism?: number;
}

interface DynamoClient {
  send(command: object, options?: { readonly abortSignal?: AbortSignal }): Promise<any>;
}

export class DynamoDbStore implements RemoteStore {
  readonly #client: DynamoClient;
  readonly #tableName: string;
  readonly #keyPrefix: Buffer;
  readonly #descriptor: StoreDescriptor;
  readonly #pending = new Set<Promise<unknown>>();
  #accepting = true;

  constructor(client: DynamoDBClient, options: DynamoDbStoreOptions) {
    if (client == null) throw new StoreError("invalid_argument", "DynamoDB client is required");
    if (options == null || options.tableName?.trim().length === 0) throw new StoreError("invalid_argument", "DynamoDB table name is required");
    this.#client = client as DynamoClient;
    this.#tableName = options.tableName;
    this.#keyPrefix = Buffer.from(options.keyPrefix ?? Buffer.from("prolly:"));
    this.#descriptor = validateStoreDescriptor({
      protocolMajor: 1,
      adapterName: options.adapterName?.trim() || "dynamodb-v1",
      provider: "dynamodb",
      schemaVersion: 1,
      capabilities: {
        nativeBatchReads: true,
        atomicBatchWrites: false,
        nodeScan: true,
        hints: true,
        atomicNodesAndHint: false,
        rootScan: true,
        rootCompareAndSwap: true,
        transactions: true,
        readParallelism: options.readParallelism ?? 16,
      },
      limits: { maxBatchReadItems: 100, maxBatchWriteItems: 25, maxTransactionOperations: 100 },
    });
  }

  async initializeTable(signal?: AbortSignal): Promise<void> {
    return this.#run("initialize_table", signal, async () => {
      try {
        const described = await this.#send(new DescribeTableCommand({ TableName: this.#tableName }), signal);
        validateTable(described.Table);
        return;
      } catch (error: unknown) {
        if (errorName(error) !== "ResourceNotFoundException") throw error;
      }
      try {
        await this.#send(new CreateTableCommand({
          TableName: this.#tableName,
          AttributeDefinitions: [{ AttributeName: PK, AttributeType: "B" }],
          KeySchema: [{ AttributeName: PK, KeyType: "HASH" }],
          BillingMode: "PAY_PER_REQUEST",
        }), signal);
      } catch (error: unknown) {
        if (errorName(error) !== "ResourceInUseException") throw error;
      }
      for (let attempt = 0; attempt < 100; attempt += 1) {
        try {
          const described = await this.#send(new DescribeTableCommand({ TableName: this.#tableName }), signal);
          if (described.Table?.TableStatus === "ACTIVE") { validateTable(described.Table); return; }
        } catch (error: unknown) {
          if (errorName(error) !== "ResourceNotFoundException") throw error;
        }
        await abortableDelay(50, signal);
      }
      throw new StoreError("unavailable", "DynamoDB table did not become active", { retryable: true });
    });
  }

  async deleteTable(signal?: AbortSignal): Promise<void> {
    return this.#run("delete_table", signal, async () => {
      try { await this.#send(new DeleteTableCommand({ TableName: this.#tableName }), signal); }
      catch (error: unknown) { if (errorName(error) !== "ResourceNotFoundException") throw error; }
    });
  }

  async close(): Promise<void> { this.#accepting = false; await Promise.allSettled([...this.#pending]); }
  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> { return this.#run("descriptor", signal, async () => this.#descriptor); }
  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#familyKey(NODE, cid), "get_node", signal); }
  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#put(this.#familyKey(NODE, cid), value, "put_node", signal); }
  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(this.#familyKey(NODE, cid), "delete_node", signal); }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    const requests = operations.map((operation): WriteRequest => operation.kind === "upsert"
      ? { PutRequest: { Item: item(this.#familyKey(NODE, operation.cid), ownBytes(operation.node)) } }
      : { DeleteRequest: { Key: keyItem(this.#familyKey(NODE, operation.cid)) } });
    return this.#run("batch_nodes", signal, () => this.#batchWrite(requests, signal));
  }

  async batchGetNodesOrdered(cids: readonly Uint8Array[], signal?: AbortSignal): Promise<OptionalBytes[]> {
    const storageKeys = cids.map((cid) => this.#familyKey(NODE, cid));
    return this.#run("batch_get_nodes_ordered", signal, async () => {
      const unique = uniqueBuffers(storageKeys);
      const values = new Map<string, Uint8Array>();
      for (let start = 0; start < unique.length; start += BATCH_GET_LIMIT) {
        let pending = unique.slice(start, start + BATCH_GET_LIMIT).map(keyItem);
        for (let attempt = 0; pending.length > 0; attempt += 1) {
          const output = await this.#send(new BatchGetItemCommand({ RequestItems: {
            [this.#tableName]: { Keys: pending, ConsistentRead: true, ProjectionExpression: "#pk, #value", ExpressionAttributeNames: { "#pk": PK, "#value": VALUE } },
          } }), signal);
          for (const result of output.Responses?.[this.#tableName] ?? []) values.set(hex(binary(result, PK)), ownBytes(binary(result, VALUE)));
          pending = output.UnprocessedKeys?.[this.#tableName]?.Keys ?? [];
          if (pending.length > 0) {
            if (attempt + 1 >= RETRY_LIMIT) throw new StoreError("resource_exhausted", `DynamoDB batch get left ${pending.length} keys unprocessed`, { retryable: true });
            await abortableDelay(10 * (2 ** Math.min(attempt, 6)), signal);
          }
        }
      }
      return storageKeys.map((key) => values.has(hex(key)) ? presentBytes(values.get(hex(key))!) : missingBytes());
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    return this.#run("list_node_cids", signal, async () => {
      const prefix = Buffer.concat([this.#keyPrefix, NODE]);
      return (await this.#scanKeys(prefix, signal)).map((key) => key.subarray(prefix.length)).filter((key) => key.length === 32).map(ownBytes).sort(compareBytes);
    });
  }

  async getHint(namespace: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#hintKey(namespace, key), "get_hint", signal); }
  async putHint(namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#put(this.#hintKey(namespace, key), value, "put_hint", signal); }
  async batchPutNodesWithHint(nodes: readonly NodeEntry[], namespace: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    await this.batchNodes(nodes.map(({ cid, node }) => upsertNode(cid, node)), signal);
    await this.putHint(namespace, key, value, signal);
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> { return this.#get(this.#familyKey(ROOT, name), "get_root_manifest", signal); }
  async putRootManifest(name: Uint8Array, manifest: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#put(this.#familyKey(ROOT, name), manifest, "put_root_manifest", signal); }
  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> { return this.#delete(this.#familyKey(ROOT, name), "delete_root_manifest", signal); }

  async compareAndSwapRootManifest(name: Uint8Array, expected: OptionalBytes, replacement: OptionalBytes, signal?: AbortSignal): Promise<RootCasResult> {
    const key = this.#familyKey(ROOT, name); const wanted = normalizeOptionalBytes(expected); const next = normalizeOptionalBytes(replacement);
    return this.#run("compare_and_swap_root_manifest", signal, async () => {
      const condition = conditionFor(wanted);
      try {
        if (next.present) await this.#send(new PutItemCommand({ TableName: this.#tableName, Item: item(key, next.value), ...condition, ReturnValuesOnConditionCheckFailure: "ALL_OLD" }), signal);
        else await this.#send(new DeleteItemCommand({ TableName: this.#tableName, Key: keyItem(key), ...condition, ReturnValuesOnConditionCheckFailure: "ALL_OLD" }), signal);
        return { applied: true, current: normalizeOptionalBytes(next) };
      } catch (error: unknown) {
        if (errorName(error) !== "ConditionalCheckFailedException") throw error;
        return { applied: false, current: await this.#getRaw(key, signal) };
      }
    });
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    return this.#run("list_root_manifests", signal, async () => {
      const prefix = Buffer.concat([this.#keyPrefix, ROOT]);
      const names = (await this.#scanKeys(prefix, signal)).map((key) => ownBytes(key.subarray(prefix.length))).sort(compareBytes);
      const result: NamedStoreRoot[] = [];
      for (const name of names) { const value = await this.#getRaw(this.#familyKey(ROOT, name), signal); if (value.present) result.push({ name, manifest: value.value }); }
      return result;
    });
  }

  async commitTransaction(nodes: readonly NodeMutation[], conditions: readonly RootCondition[], roots: readonly RootWrite[], signal?: AbortSignal): Promise<StoreTransactionResult> {
    const ownedNodes = nodes.map(cloneMutation); const ownedConditions = conditions.map(({ name, expected }) => ({ name: ownBytes(name), expected: normalizeOptionalBytes(expected) })); const ownedRoots = roots.map(cloneRootWrite);
    const written = new Set(ownedRoots.map(({ name }) => hex(name)));
    const count = ownedNodes.length + ownedRoots.length + ownedConditions.filter(({ name }) => !written.has(hex(name))).length;
    if (count > TRANSACTION_LIMIT) throw new StoreError("resource_exhausted", `DynamoDB transaction has ${count} operations, exceeding the ${TRANSACTION_LIMIT} operation limit`);
    return this.#run("commit_transaction", signal, async () => {
      const conditionByName = new Map(ownedConditions.map((condition) => [hex(condition.name), condition]));
      const items: TransactWriteItem[] = [];
      for (const condition of ownedConditions) if (!written.has(hex(condition.name))) items.push({ ConditionCheck: { TableName: this.#tableName, Key: keyItem(this.#familyKey(ROOT, condition.name)), ...conditionFor(condition.expected), ReturnValuesOnConditionCheckFailure: "ALL_OLD" } });
      for (const root of ownedRoots) {
        const condition = conditionByName.get(hex(root.name)); const conditional = condition === undefined ? {} : { ...conditionFor(condition.expected), ReturnValuesOnConditionCheckFailure: "ALL_OLD" as const };
        if (root.kind === "put") items.push({ Put: { TableName: this.#tableName, Item: item(this.#familyKey(ROOT, root.name), root.manifest), ...conditional } });
        else items.push({ Delete: { TableName: this.#tableName, Key: keyItem(this.#familyKey(ROOT, root.name)), ...conditional } });
      }
      for (const node of ownedNodes) {
        if (node.kind === "upsert") items.push({ Put: { TableName: this.#tableName, Item: item(this.#familyKey(NODE, node.cid), node.node) } });
        else items.push({ Delete: { TableName: this.#tableName, Key: keyItem(this.#familyKey(NODE, node.cid)) } });
      }
      if (items.length === 0) return { applied: true };
      try { await this.#send(new TransactWriteItemsCommand({ TransactItems: items }), signal); return { applied: true }; }
      catch (error: unknown) {
        if (errorName(error) !== "TransactionCanceledException") throw error;
        for (const condition of ownedConditions) {
          const current = await this.#getRaw(this.#familyKey(ROOT, condition.name), signal);
          if (!optionalEqual(current, condition.expected)) return { applied: false, conflict: { name: ownBytes(condition.name), expected: normalizeOptionalBytes(condition.expected), current } };
        }
        throw error;
      }
    });
  }

  async clearNamespace(signal?: AbortSignal): Promise<void> {
    if (this.#keyPrefix.length === 0) throw new StoreError("invalid_argument", "refusing to clear an empty DynamoDB key prefix");
    return this.#run("clear_namespace", signal, async () => this.#batchWrite((await this.#scanKeys(this.#keyPrefix, signal)).map((key) => ({ DeleteRequest: { Key: keyItem(key) } })), signal));
  }

  async #get(key: Buffer, operation: string, signal?: AbortSignal): Promise<OptionalBytes> { return this.#run(operation, signal, () => this.#getRaw(key, signal)); }
  async #getRaw(key: Buffer, signal?: AbortSignal): Promise<OptionalBytes> {
    const output = await this.#send(new GetItemCommand({ TableName: this.#tableName, Key: keyItem(key), ConsistentRead: true, ProjectionExpression: "#value", ExpressionAttributeNames: { "#value": VALUE } }), signal);
    return output.Item === undefined || Object.keys(output.Item).length === 0 ? missingBytes() : presentBytes(binary(output.Item, VALUE));
  }
  async #put(key: Buffer, value: Uint8Array, operation: string, signal?: AbortSignal): Promise<void> { const owned = ownBytes(value); return this.#run(operation, signal, async () => { await this.#send(new PutItemCommand({ TableName: this.#tableName, Item: item(key, owned) }), signal); }); }
  async #delete(key: Buffer, operation: string, signal?: AbortSignal): Promise<void> { return this.#run(operation, signal, async () => { await this.#send(new DeleteItemCommand({ TableName: this.#tableName, Key: keyItem(key) }), signal); }); }

  async #batchWrite(requests: readonly WriteRequest[], signal?: AbortSignal): Promise<void> {
    for (let start = 0; start < requests.length; start += BATCH_WRITE_LIMIT) {
      let pending = requests.slice(start, start + BATCH_WRITE_LIMIT);
      for (let attempt = 0; pending.length > 0; attempt += 1) {
        const output = await this.#send(new BatchWriteItemCommand({ RequestItems: { [this.#tableName]: pending } }), signal);
        pending = output.UnprocessedItems?.[this.#tableName] ?? [];
        if (pending.length > 0) {
          if (attempt + 1 >= RETRY_LIMIT) throw new StoreError("resource_exhausted", `DynamoDB batch write left ${pending.length} requests unprocessed`, { retryable: true });
          await abortableDelay(10 * (2 ** Math.min(attempt, 6)), signal);
        }
      }
    }
  }

  async #scanKeys(prefix: Uint8Array, signal?: AbortSignal): Promise<Buffer[]> {
    const keys: Buffer[] = []; let start: Record<string, AttributeValue> | undefined;
    do {
      const output = await this.#send(new ScanCommand({ TableName: this.#tableName, ConsistentRead: true, ProjectionExpression: "#pk", FilterExpression: "begins_with(#pk, :prefix)", ExpressionAttributeNames: { "#pk": PK }, ExpressionAttributeValues: { ":prefix": { B: ownBytes(prefix) } }, ExclusiveStartKey: start }), signal);
      for (const result of output.Items ?? []) keys.push(Buffer.from(binary(result, PK)));
      start = output.LastEvaluatedKey;
    } while (start !== undefined && Object.keys(start).length > 0);
    return keys;
  }

  #familyKey(family: Buffer, suffix: Uint8Array): Buffer { return Buffer.concat([this.#keyPrefix, family, Buffer.from(ownBytes(suffix))]); }
  #hintKey(namespace: Uint8Array, key: Uint8Array): Buffer { const length = Buffer.alloc(8); length.writeBigUInt64BE(BigInt(namespace.byteLength)); return Buffer.concat([this.#keyPrefix, HINT, length, Buffer.from(ownBytes(namespace)), Buffer.from(ownBytes(key))]); }
  async #send(command: object, signal?: AbortSignal): Promise<any> { return this.#client.send(command, signal === undefined ? undefined : { abortSignal: signal }); }
  async #run<T>(operation: string, signal: AbortSignal | undefined, call: () => Promise<T>): Promise<T> {
    throwIfAborted(signal); if (!this.#accepting) throw new StoreError("internal", "DynamoDB store is closed");
    const pending = call().catch((error: unknown) => { if (signal?.aborted) throw new StoreError("cancelled", "DynamoDB operation was cancelled", { cause: signal.reason }); throw mapDynamoError(operation, error); });
    this.#pending.add(pending); try { return await pending; } finally { this.#pending.delete(pending); }
  }
}

function keyItem(key: Uint8Array): Record<string, AttributeValue> { return { [PK]: { B: ownBytes(key) } }; }
function item(key: Uint8Array, value: Uint8Array): Record<string, AttributeValue> { return { [PK]: { B: ownBytes(key) }, [VALUE]: { B: ownBytes(value) } }; }
function binary(value: Record<string, AttributeValue>, name: string): Uint8Array { const result = value[name]; if (result === undefined || !("B" in result) || result.B === undefined) throw new StoreError("invalid_data", `DynamoDB item has invalid ${name} attribute`); return ownBytes(result.B); }
function conditionFor(expected: OptionalBytes): { ConditionExpression: string; ExpressionAttributeNames: Record<string, string>; ExpressionAttributeValues?: Record<string, AttributeValue> } { return expected.present ? { ConditionExpression: "#value = :expected", ExpressionAttributeNames: { "#value": VALUE }, ExpressionAttributeValues: { ":expected": { B: ownBytes(expected.value) } } } : { ConditionExpression: "attribute_not_exists(#pk)", ExpressionAttributeNames: { "#pk": PK } }; }
function validateTable(table: any): void { if (table == null || table.KeySchema?.length !== 1 || table.KeySchema[0]?.AttributeName !== PK || table.KeySchema[0]?.KeyType !== "HASH" || !table.AttributeDefinitions?.some((entry: any) => entry.AttributeName === PK && entry.AttributeType === "B")) throw new StoreError("invalid_argument", "DynamoDB table must use one binary HASH key named pk"); }
function cloneMutation(value: NodeMutation): NodeMutation { return value.kind === "upsert" ? upsertNode(value.cid, value.node) : deleteNode(value.cid); }
function cloneRootWrite(value: RootWrite): RootWrite { const name = ownBytes(value.name); return value.kind === "put" ? { kind: "put", name, manifest: ownBytes(value.manifest) } : { kind: "delete", name }; }
function uniqueBuffers(values: readonly Buffer[]): Buffer[] { const seen = new Set<string>(); return values.filter((value) => { const key = hex(value); if (seen.has(key)) return false; seen.add(key); return true; }); }
function optionalEqual(left: OptionalBytes, right: OptionalBytes): boolean { return left.present === right.present && (!left.present || Buffer.from(left.value).equals(Buffer.from(right.value))); }
function compareBytes(left: Uint8Array, right: Uint8Array): number { return Buffer.compare(Buffer.from(left), Buffer.from(right)); }
function hex(value: Uint8Array): string { return Buffer.from(value).toString("hex"); }
function errorName(error: unknown): string | undefined { return typeof error === "object" && error !== null && "name" in error && typeof error.name === "string" ? error.name : undefined; }
function mapDynamoError(operation: string, error: unknown): StoreError { if (error instanceof StoreError) return error; const name = errorName(error); const retryable = name !== undefined && ["ProvisionedThroughputExceededException", "RequestLimitExceeded", "InternalServerError", "ServiceUnavailable", "ThrottlingException", "TimeoutError"].includes(name); return new StoreError(retryable ? "unavailable" : "internal", "DynamoDB operation failed", { retryable, providerCode: name === undefined ? undefined : `dynamodb:${name}:${operation}`, cause: error }); }
async function abortableDelay(milliseconds: number, signal?: AbortSignal): Promise<void> {
  throwIfAborted(signal);
  await new Promise<void>((resolve, reject) => {
    const finish = (): void => { signal?.removeEventListener("abort", abort); resolve(); };
    const timer = setTimeout(finish, milliseconds);
    const abort = (): void => { clearTimeout(timer); signal?.removeEventListener("abort", abort); reject(new StoreError("cancelled", "DynamoDB operation was cancelled", { cause: signal?.reason })); };
    signal?.addEventListener("abort", abort, { once: true });
  });
}
