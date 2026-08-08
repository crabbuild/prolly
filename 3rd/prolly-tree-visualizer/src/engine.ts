import {
  loadProllyWasm,
  type ProllyWasmModule,
  type WasmEntryRecord,
  type WasmProllyEngineInstance,
  type WasmTree,
  type WasmTreeDebugViewRecord,
} from '@crabbuild/prolly-wasm';
import { bootstrapKeys, deterministicShuffle } from './bootstrap';
import { buildTreeFromDebug, leafNodes, toHex } from './prolly';
import type { RowValue, TreeSnapshot } from './types';

const textEncoder = new TextEncoder();
const textDecoder = new TextDecoder();

function decodeI64Key(bytes: Uint8Array) {
  let encoded = 0n;
  for (const byte of bytes) encoded = (encoded << 8n) | BigInt(byte);
  const decoded = BigInt.asIntN(64, encoded ^ (1n << 63n));
  const number = Number(decoded);
  if (!Number.isSafeInteger(number)) throw new Error(`integer key ${decoded} is outside JavaScript's safe range`);
  return number;
}

function rowsFromEntries(entries: WasmEntryRecord[]): RowValue[] {
  return entries.map((entry) => ({
    key: decodeI64Key(entry.key),
    value: textDecoder.decode(entry.value),
  }));
}

function sumSizes(sizes: Map<string, number>) {
  let total = 0;
  for (const size of sizes.values()) total += size;
  return total;
}

export interface GrowthResult {
  before: TreeSnapshot;
  after: TreeSnapshot;
  added: number;
}

export interface GarbageCollectionResult {
  head: TreeSnapshot;
  message: string;
  beforeChunks: number;
  afterChunks: number;
  beforeBytes: number;
  afterBytes: number;
}

export interface InsertionOrderBuild {
  order: number[];
  updates: number[];
  rootHash: string;
  nodeHashes: string[];
  snapshot: TreeSnapshot;
}

export interface InsertionOrderSource {
  rootHash: string;
  nodeHashes: string[];
  snapshot: TreeSnapshot;
}

export interface InsertionOrderResult {
  current: InsertionOrderSource;
  rebuilt: InsertionOrderBuild;
  identicalRoot: boolean;
  identicalChunks: boolean;
  rowCount: number;
}

export class ProllyEngine {
  private readonly wasm: ProllyWasmModule;
  private runtime: WasmProllyEngineInstance;
  private tree: WasmTree;
  private snapshotId = 0;
  private storedNodeSizes = new Map<string, number>();
  readonly version = '@crabbuild/prolly-wasm 0.1.0';

  private constructor(wasm: ProllyWasmModule) {
    this.wasm = wasm;
    this.runtime = wasm.WasmProllyEngine.memory();
    this.tree = this.runtime.create();
  }

  static async create() {
    return new ProllyEngine(await loadProllyWasm());
  }

  private key(key: number) {
    if (!Number.isSafeInteger(key)) throw new Error('keys must be safe integers');
    return this.wasm.i64Key(String(key));
  }

  private entry(row: RowValue) {
    return { key: this.key(row.key), value: textEncoder.encode(row.value) };
  }

  private rows(runtime = this.runtime, tree = this.tree): RowValue[] {
    return rowsFromEntries(runtime.range(tree, new Uint8Array()));
  }

  private replaceTree(next: WasmTree) {
    this.tree.free();
    this.tree = next;
  }

  private replaceRuntime(runtime: WasmProllyEngineInstance, tree: WasmTree) {
    this.tree.free();
    this.runtime.free();
    this.runtime = runtime;
    this.tree = tree;
  }

  private snapshotFrom(
    runtime: WasmProllyEngineInstance,
    tree: WasmTree,
    rows: RowValue[],
    label: string,
    id: number,
    storedNodeSizes?: Map<string, number>,
  ): TreeSnapshot {
    const debug = runtime.debugTree(tree) as WasmTreeDebugViewRecord;
    const { root, nodes } = buildTreeFromDebug(debug, rows);
    const rootBytes = tree.root as Uint8Array | number[] | null;
    if (!rootBytes) throw new Error('the prolly tree is empty');
    const rootHash = toHex(rootBytes);
    if (root.hash !== rootHash) throw new Error('debug tree root does not match the engine root');

    const physicalSizes = storedNodeSizes ?? new Map<string, number>();
    for (const [hash, node] of nodes) physicalSizes.set(hash, node.size);
    return {
      id,
      label,
      rootHash,
      root,
      rows,
      nodes,
      chunksInStore: physicalSizes.size,
      databaseBytes: sumSizes(physicalSizes),
      timestamp: Date.now(),
    };
  }

  seed(count = 240) {
    const rows = bootstrapKeys(count).map((key) => ({ key, value: `value-${key}` }));
    this.replaceTree(this.runtime.buildFromSortedEntries(rows.map((row) => this.entry(row))));
    return this.capture(`Built ${rows.length} sparse rows`);
  }

  capture(label: string): TreeSnapshot {
    return this.snapshotFrom(
      this.runtime,
      this.tree,
      this.rows(),
      label,
      this.snapshotId++,
      this.storedNodeSizes,
    );
  }

  put(key: number, value: string) {
    const encodedKey = this.key(key);
    const exists = this.runtime.get(this.tree, encodedKey) !== null;
    this.replaceTree(this.runtime.put(this.tree, encodedKey, textEncoder.encode(value)));
    return this.capture(`${exists ? 'Updated' : 'Inserted'} key ${key}`);
  }

  remove(key: number) {
    this.replaceTree(this.runtime.delete(this.tree, this.key(key)));
    return this.capture(`Deleted key ${key}`);
  }

  addSequential(count: number) {
    const rows = this.rows();
    const first = (rows.at(-1)?.key ?? 0) + 1;
    this.insertSequential(first, count, 'value');
    return this.capture(`Appended ${count} sequential rows`);
  }

  private insertSequential(first: number, count: number, prefix: string) {
    const amount = Math.max(1, Math.trunc(count));
    const mutations = Array.from({ length: amount }, (_, index) => {
      const key = first + index;
      return { kind: 'upsert', key: this.key(key), value: textEncoder.encode(`${prefix}-${key}`) };
    });
    this.replaceTree(this.runtime.appendBatch(this.tree, mutations));
  }

  addRandom() {
    const rows = this.rows();
    if (rows.length > 0 && Math.random() < 0.5) {
      const row = rows[Math.floor(Math.random() * rows.length)];
      return this.put(row.key, `random-update-${row.key}-${this.snapshotId}`);
    }
    const maxKey = rows.at(-1)?.key ?? 0;
    const key = maxKey + Math.floor(Math.random() * Math.max(160, maxKey)) + 1;
    return this.put(key, `random-${key}`);
  }

  growUntilSplit(limit = 512, batchSize = 16): GrowthResult {
    const before = this.capture('Before split search');
    const leafCount = leafNodes(before.root).length;
    let next = (before.rows.at(-1)?.key ?? 0) + 1;
    let candidate = before;
    for (let added = 0; added < limit;) {
      const amount = Math.min(batchSize, limit - added);
      this.insertSequential(next, amount, 'split');
      added += amount;
      next += amount;
      candidate = this.capture(`Split after appending ${added} rows`);
      if (leafNodes(candidate.root).length > leafCount) return { before, after: candidate, added };
    }
    candidate.label = `No split after ${limit} inserts`;
    return { before, after: candidate, added: limit };
  }

  growUntilNextLevel(): GrowthResult {
    const before = this.capture('Before level growth');
    const rootLevel = before.root.level;
    if (rootLevel >= 2) throw new Error('Four-level trees exceed the full-node browser rendering limit');
    const limit = rootLevel === 0 ? 2_048 : 48_000;
    const batchSize = rootLevel === 0 ? 256 : 2_000;
    let next = (before.rows.at(-1)?.key ?? 0) + 1;
    let candidate = before;
    for (let added = 0; added < limit;) {
      const amount = Math.min(batchSize, limit - added);
      this.insertSequential(next, amount, 'level');
      added += amount;
      next += amount;
      candidate = this.capture(`Reached level ${rootLevel + 1} after appending ${added} rows`);
      if (candidate.root.level > rootLevel) return { before, after: candidate, added };
    }
    candidate.label = `No new level after ${limit} inserts`;
    return { before, after: candidate, added: limit };
  }

  garbageCollect(): GarbageCollectionResult {
    const beforeChunks = this.storedNodeSizes.size;
    const beforeBytes = sumSizes(this.storedNodeSizes);
    const rows = this.rows();
    const previousRoot = toHex(this.tree.root as Uint8Array | number[]);
    const replacementRuntime = this.wasm.WasmProllyEngine.memory();
    const empty = replacementRuntime.create();
    const replacementTree = replacementRuntime.buildFromSortedEntries(rows.map((row) => this.entry(row)));
    empty.free();
    const replacementRoot = toHex(replacementTree.root as Uint8Array | number[]);
    if (replacementRoot !== previousRoot) {
      replacementTree.free();
      replacementRuntime.free();
      throw new Error('HEAD root changed during garbage collection');
    }

    this.replaceRuntime(replacementRuntime, replacementTree);
    this.storedNodeSizes = new Map();
    const head = this.capture('Garbage collected');
    return {
      head,
      message: `${Math.max(0, beforeChunks - head.chunksInStore)} chunks removed, ${head.chunksInStore} chunks kept`,
      beforeChunks,
      afterChunks: head.chunksInStore,
      beforeBytes,
      afterBytes: head.databaseBytes,
    };
  }

  reset(count = 240) {
    const runtime = this.wasm.WasmProllyEngine.memory();
    const tree = runtime.create();
    this.replaceRuntime(runtime, tree);
    this.snapshotId = 0;
    this.storedNodeSizes = new Map();
    return this.seed(count);
  }

  compareInsertionOrder(snapshot: TreeSnapshot): InsertionOrderResult {
    const rows = snapshot.rows;
    const build = (ordered: RowValue[], draftKeys = new Set<number>()): InsertionOrderBuild => {
      const runtime = this.wasm.WasmProllyEngine.memory();
      let tree = runtime.create();
      const pending: RowValue[] = [];
      const updates: number[] = [];
      try {
        for (let index = 0; index < ordered.length; index += 1) {
          const row = ordered[index];
          const draft = draftKeys.has(row.key);
          const next = runtime.put(tree, this.key(row.key), textEncoder.encode(draft ? `draft-${row.key}` : row.value));
          tree.free();
          tree = next;
          if (draft) pending.push(row);
          if (index % 5 === 4 && pending.length > 0) {
            const update = pending.shift()!;
            const updated = runtime.put(tree, this.key(update.key), textEncoder.encode(update.value));
            tree.free();
            tree = updated;
            updates.push(update.key);
          }
        }
        for (const update of pending) {
          const updated = runtime.put(tree, this.key(update.key), textEncoder.encode(update.value));
          tree.free();
          tree = updated;
          updates.push(update.key);
        }
        const finalRows = rowsFromEntries(runtime.range(tree, new Uint8Array()));
        const rebuiltSnapshot = this.snapshotFrom(runtime, tree, finalRows, 'History independence build', -1);
        return {
          order: ordered.map((row) => row.key),
          updates,
          rootHash: rebuiltSnapshot.rootHash,
          nodeHashes: [...rebuiltSnapshot.nodes.keys()].sort(),
          snapshot: rebuiltSnapshot,
        };
      } finally {
        tree.free();
        runtime.free();
      }
    };

    const current: InsertionOrderSource = {
      rootHash: snapshot.rootHash,
      nodeHashes: [...snapshot.nodes.keys()].sort(),
      snapshot,
    };
    const shuffledRows = deterministicShuffle(rows);
    const draftKeys = new Set(shuffledRows.filter((_, index) => index % 6 === 0).map((row) => row.key));
    const rebuilt = build(shuffledRows, draftKeys);
    return {
      current,
      rebuilt,
      identicalRoot: current.rootHash === rebuilt.rootHash,
      identicalChunks: current.nodeHashes.length === rebuilt.nodeHashes.length
        && current.nodeHashes.every((hash, index) => hash === rebuilt.nodeHashes[index]),
      rowCount: rows.length,
    };
  }

  close() {
    this.tree.free();
    this.runtime.free();
  }
}
