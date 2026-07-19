import {
  missingBytes,
  normalizeOptionalBytes,
  ownBytes,
  presentBytes,
  publishNodesWithGeneralPath,
  throwIfAborted,
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
} from "../../src/remote-store.ts";

const keyOf = (value: Uint8Array): string => Buffer.from(value).toString("hex");
const bytesOf = (value: string): Uint8Array => Uint8Array.from(Buffer.from(value, "hex"));

export class FakeRemoteStore implements RemoteStore {
  readonly nodes = new Map<string, Uint8Array>();
  readonly hints = new Map<string, Uint8Array>();
  readonly roots = new Map<string, Uint8Array>();

  async descriptor(signal?: AbortSignal): Promise<StoreDescriptor> {
    throwIfAborted(signal);
    return {
      protocolMajor: 2,
      adapterName: "node-test-memory",
      provider: "memory",
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
        readParallelism: 4,
      },
      limits: {},
    };
  }

  async getNode(cid: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    throwIfAborted(signal);
    const value = this.nodes.get(keyOf(cid));
    return value === undefined ? missingBytes() : presentBytes(value);
  }

  async putNode(cid: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    this.nodes.set(keyOf(cid), ownBytes(value));
  }

  async deleteNode(cid: Uint8Array, signal?: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    this.nodes.delete(keyOf(cid));
  }

  async batchNodes(operations: readonly NodeMutation[], signal?: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    for (const operation of operations) {
      if (operation.kind === "upsert") {
        this.nodes.set(keyOf(operation.cid), ownBytes(operation.node));
      } else {
        this.nodes.delete(keyOf(operation.cid));
      }
    }
  }

  async publishNodes(publication: NodePublication, signal?: AbortSignal): Promise<void> {
    return publishNodesWithGeneralPath(this, publication, signal);
  }

  async batchGetNodesOrdered(
    cids: readonly Uint8Array[],
    signal?: AbortSignal,
  ): Promise<OptionalBytes[]> {
    throwIfAborted(signal);
    return cids.map((cid) => {
      const value = this.nodes.get(keyOf(cid));
      return value === undefined ? missingBytes() : presentBytes(value);
    });
  }

  async listNodeCids(signal?: AbortSignal): Promise<Uint8Array[]> {
    throwIfAborted(signal);
    return [...this.nodes.keys()].sort().map(bytesOf);
  }

  async getHint(
    namespace: Uint8Array,
    key: Uint8Array,
    signal?: AbortSignal,
  ): Promise<OptionalBytes> {
    throwIfAborted(signal);
    const value = this.hints.get(`${keyOf(namespace)}:${keyOf(key)}`);
    return value === undefined ? missingBytes() : presentBytes(value);
  }

  async putHint(
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    this.hints.set(`${keyOf(namespace)}:${keyOf(key)}`, ownBytes(value));
  }

  async batchPutNodesWithHint(
    nodes: readonly NodeEntry[],
    namespace: Uint8Array,
    key: Uint8Array,
    value: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    for (const node of nodes) this.nodes.set(keyOf(node.cid), ownBytes(node.node));
    this.hints.set(`${keyOf(namespace)}:${keyOf(key)}`, ownBytes(value));
  }

  async getRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<OptionalBytes> {
    throwIfAborted(signal);
    const value = this.roots.get(keyOf(name));
    return value === undefined ? missingBytes() : presentBytes(value);
  }

  async putRootManifest(
    name: Uint8Array,
    manifest: Uint8Array,
    signal?: AbortSignal,
  ): Promise<void> {
    throwIfAborted(signal);
    this.roots.set(keyOf(name), ownBytes(manifest));
  }

  async deleteRootManifest(name: Uint8Array, signal?: AbortSignal): Promise<void> {
    throwIfAborted(signal);
    this.roots.delete(keyOf(name));
  }

  async compareAndSwapRootManifest(
    name: Uint8Array,
    expected: OptionalBytes,
    replacement: OptionalBytes,
    signal?: AbortSignal,
  ): Promise<RootCasResult> {
    throwIfAborted(signal);
    const mapKey = keyOf(name);
    const current = this.roots.get(mapKey);
    if (!matches(current, expected)) {
      return {
        applied: false,
        current: current === undefined ? missingBytes() : presentBytes(current),
      };
    }
    const next = normalizeOptionalBytes(replacement);
    if (next.present) this.roots.set(mapKey, ownBytes(next.value));
    else this.roots.delete(mapKey);
    return { applied: true, current: missingBytes() };
  }

  async listRootManifests(signal?: AbortSignal): Promise<NamedStoreRoot[]> {
    throwIfAborted(signal);
    return [...this.roots.entries()]
      .sort(([left], [right]) => left.localeCompare(right))
      .map(([name, manifest]) => ({ name: bytesOf(name), manifest: ownBytes(manifest) }));
  }

  async commitTransaction(
    nodes: readonly NodeMutation[],
    conditions: readonly RootCondition[],
    roots: readonly RootWrite[],
    signal?: AbortSignal,
  ): Promise<StoreTransactionResult> {
    throwIfAborted(signal);
    for (const condition of conditions) {
      const current = this.roots.get(keyOf(condition.name));
      if (!matches(current, condition.expected)) {
        return {
          applied: false,
          conflict: {
            name: ownBytes(condition.name),
            expected: normalizeOptionalBytes(condition.expected),
            current: current === undefined ? missingBytes() : presentBytes(current),
          },
        };
      }
    }
    await this.batchNodes(nodes, signal);
    for (const write of roots) {
      if (write.kind === "put") this.roots.set(keyOf(write.name), ownBytes(write.manifest));
      else this.roots.delete(keyOf(write.name));
    }
    return { applied: true };
  }
}

function matches(current: Uint8Array | undefined, expected: OptionalBytes): boolean {
  const normalized = normalizeOptionalBytes(expected);
  if (!normalized.present) return current === undefined;
  return current !== undefined && Buffer.from(current).equals(Buffer.from(normalized.value));
}
