import type { WasmTreeDebugNodeRecord, WasmTreeDebugViewRecord } from '@trail/prolly-wasm';
import type { ProllyEntry, ProllyNode, RowDiff, RowValue } from './types';

const DEFAULT_MIN_CHUNK_ENTRIES = 4;
const DEFAULT_BOUNDARY_FACTOR = 128;

type Bytes = Uint8Array | number[];

function asBytes(value: Bytes) {
  return value instanceof Uint8Array ? value : Uint8Array.from(value);
}

export function toHex(value: Bytes) {
  return [...asBytes(value)].map((byte) => byte.toString(16).padStart(2, '0')).join('');
}

function decodeI64Key(value: Bytes) {
  let encoded = 0n;
  for (const byte of asBytes(value)) encoded = (encoded << 8n) | BigInt(byte);
  const decoded = BigInt.asIntN(64, encoded ^ (1n << 63n));
  const number = Number(decoded);
  return Number.isSafeInteger(number) ? number : decoded.toString();
}

function encodeI64KeyHex(value: number) {
  let encoded = BigInt.asUintN(64, BigInt(value)) ^ (1n << 63n);
  const bytes = new Uint8Array(8);
  for (let index = bytes.length - 1; index >= 0; index -= 1) {
    bytes[index] = Number(encoded & 0xffn);
    encoded >>= 8n;
  }
  return toHex(bytes);
}

function debugHash(node: WasmTreeDebugNodeRecord) {
  return toHex(node.cid as unknown as Bytes);
}

function debugKey(value: Uint8Array | number[] | null | undefined) {
  return value == null ? null : decodeI64Key(value);
}

function valueHex(value: string) {
  return toHex(new TextEncoder().encode(value));
}

function leafFromDebug(node: WasmTreeDebugNodeRecord, rows: RowValue[]): ProllyNode {
  const minKey = debugKey(node.first_key as Uint8Array | number[] | null | undefined);
  const maxKey = debugKey(node.last_key as Uint8Array | number[] | null | undefined);
  const entries = rows
    .filter((row) => Number(minKey) <= row.key && row.key <= Number(maxKey))
    .map((row): ProllyEntry => ({
      key: row.key,
      keyHex: encodeI64KeyHex(row.key),
      valueHex: valueHex(row.value),
    }));
  if (entries.length !== node.entry_count) {
    throw new Error(`leaf ${debugHash(node)} reports ${node.entry_count} entries but exposes ${entries.length} rows`);
  }
  return {
    hash: debugHash(node),
    level: node.level,
    size: node.encoded_bytes,
    flags: 0,
    entries,
    children: [],
    minKey,
    maxKey,
  };
}

/**
 * Reconstruct the renderable hierarchy from the Rust engine's debug view.
 *
 * Debug levels are emitted in deterministic breadth-first order. Internal
 * entries are lower-bound separators, so each parent's next `entry_count`
 * nodes in the following level are its direct children.
 */
export function buildTreeFromDebug(view: WasmTreeDebugViewRecord, rows: RowValue[]) {
  if (view.levels.length === 0) throw new Error('cannot visualize an empty prolly tree');
  const metadataByLevel = new Map(view.levels.map((level) => [level.level, level.nodes]));
  const nodesByLevel = new Map<number, ProllyNode[]>();
  const leafMetadata = metadataByLevel.get(0);
  if (!leafMetadata) throw new Error('prolly debug view has no leaf level');
  nodesByLevel.set(0, leafMetadata.map((node) => leafFromDebug(node, rows)));

  const rootLevel = Math.max(...view.levels.map((level) => level.level));
  for (let level = 1; level <= rootLevel; level += 1) {
    const metadata = metadataByLevel.get(level);
    const children = nodesByLevel.get(level - 1);
    if (!metadata || !children) throw new Error(`prolly debug view is missing level ${level}`);
    let childCursor = 0;
    const parents = metadata.map((node): ProllyNode => {
      const directChildren = children.slice(childCursor, childCursor + node.entry_count);
      childCursor += node.entry_count;
      if (directChildren.length !== node.entry_count) {
        throw new Error(`internal node ${debugHash(node)} references missing children`);
      }
      const entries = directChildren.map((child): ProllyEntry => ({
        key: child.minKey ?? '',
        keyHex: typeof child.minKey === 'number' ? encodeI64KeyHex(child.minKey) : '',
        valueHex: child.hash,
        childHash: child.hash,
        subtreeCount: leafNodes(child).reduce((total, leaf) => total + leaf.entries.length, 0),
      }));
      const reportedFirst = debugKey(node.first_key as Uint8Array | number[] | null | undefined);
      const reportedLast = debugKey(node.last_key as Uint8Array | number[] | null | undefined);
      if (entries[0]?.key !== reportedFirst || entries.at(-1)?.key !== reportedLast) {
        throw new Error(`internal node ${debugHash(node)} separators do not match its children`);
      }
      return {
        hash: debugHash(node),
        level: node.level,
        size: node.encoded_bytes,
        flags: 0,
        entries,
        children: directChildren,
        minKey: directChildren[0]?.minKey ?? null,
        maxKey: directChildren.at(-1)?.maxKey ?? null,
      };
    });
    if (childCursor !== children.length) throw new Error(`level ${level} leaves ${children.length - childCursor} children unclaimed`);
    nodesByLevel.set(level, parents);
  }

  const roots = nodesByLevel.get(rootLevel)!;
  if (roots.length !== 1) throw new Error(`prolly debug view has ${roots.length} roots`);
  const nodes = new Map<string, ProllyNode>();
  for (const levelNodes of nodesByLevel.values()) {
    for (const node of levelNodes) nodes.set(node.hash, node);
  }
  return { root: roots[0], nodes };
}

export function traceSearch(root: ProllyNode, key: number) {
  const trace: string[] = [];
  let node: ProllyNode | undefined = root;
  while (node) {
    trace.push(node.hash);
    if (node.level === 0) break;
    let index = node.entries.findLastIndex((entry) => Number(entry.key) <= key);
    if (index < 0) index = 0;
    node = node.children[index];
  }
  return trace;
}

export function traceRange(root: ProllyNode, start: number, end: number) {
  const low = Math.min(start, end);
  const high = Math.max(start, end);
  const trace: string[] = [];
  const visit = (node: ProllyNode) => {
    const min = Number(node.minKey);
    const max = Number(node.maxKey);
    if (!Number.isFinite(min) || !Number.isFinite(max) || max < low || min > high) return;
    trace.push(node.hash);
    node.children.forEach(visit);
  };
  visit(root);
  return trace;
}

export function diffRows(before: RowValue[], after: RowValue[]): RowDiff[] {
  const left = new Map(before.map((row) => [row.key, row.value]));
  const right = new Map(after.map((row) => [row.key, row.value]));
  const keys = [...new Set([...left.keys(), ...right.keys()])].sort((a, b) => a - b);
  return keys.flatMap((key): RowDiff[] => {
    const oldValue = left.get(key);
    const newValue = right.get(key);
    if (oldValue === newValue) return [];
    if (oldValue === undefined) return [{ key, after: newValue, kind: 'added' }];
    if (newValue === undefined) return [{ key, before: oldValue, kind: 'deleted' }];
    return [{ key, before: oldValue, after: newValue, kind: 'modified' }];
  });
}

export function groupRowDiffsByLeaf(root: ProllyNode, diffs: RowDiff[]) {
  const grouped = new Map<string, RowDiff[]>();
  for (const diff of diffs) {
    const leafHash = traceSearch(root, diff.key).at(-1);
    if (!leafHash) continue;
    const existing = grouped.get(leafHash);
    if (existing) existing.push(diff);
    else grouped.set(leafHash, [diff]);
  }
  return grouped;
}

export function leafNodes(root: ProllyNode) {
  const leaves: ProllyNode[] = [];
  const visit = (node: ProllyNode) => {
    if (node.level === 0) leaves.push(node);
    else node.children.forEach(visit);
  };
  visit(root);
  return leaves;
}

export function countSharedNodes(left: Map<string, ProllyNode>, right: Map<string, ProllyNode>) {
  let shared = 0;
  for (const hash of left.keys()) if (right.has(hash)) shared += 1;
  return shared;
}

export function estimateMutationSplitProbability(node: ProllyNode) {
  if (node.level !== 0 || node.entries.length + 1 < DEFAULT_MIN_CHUNK_ENTRIES) return 0;
  return 1 / DEFAULT_BOUNDARY_FACTOR;
}
