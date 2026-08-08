import { describe, expect, it } from 'vitest';
import type { WasmTreeDebugViewRecord } from '@crabbuild/prolly-wasm';
import {
  buildTreeFromDebug,
  diffRows,
  estimateMutationSplitProbability,
  groupRowDiffsByLeaf,
  traceRange,
  traceSearch,
} from './prolly';

function intKey(value: number) {
  let encoded = BigInt.asUintN(64, BigInt(value)) ^ (1n << 63n);
  const bytes = new Uint8Array(8);
  for (let index = 7; index >= 0; index -= 1) {
    bytes[index] = Number(encoded & 0xffn);
    encoded >>= 8n;
  }
  return bytes;
}

function cid(byte: number) {
  return Uint8Array.from({ length: 32 }, () => byte);
}

function debugTree(): WasmTreeDebugViewRecord {
  return {
    levels: [
      {
        level: 1,
        nodes: [{
          cid: cid(3),
          leaf: false,
          level: 1,
          entry_count: 2,
          max_entries: 1_048_576,
          fill_factor: 2 / 1_048_576,
          encoded_bytes: 100,
          first_key: intKey(1),
          last_key: intKey(7),
        }],
      },
      {
        level: 0,
        nodes: [
          {
            cid: cid(1),
            leaf: true,
            level: 0,
            entry_count: 2,
            max_entries: 1_048_576,
            fill_factor: 2 / 1_048_576,
            encoded_bytes: 80,
            first_key: intKey(1),
            last_key: intKey(3),
          },
          {
            cid: cid(2),
            leaf: true,
            level: 0,
            entry_count: 2,
            max_entries: 1_048_576,
            fill_factor: 2 / 1_048_576,
            encoded_bytes: 80,
            first_key: intKey(7),
            last_key: intKey(9),
          },
        ],
      },
    ],
  };
}

const rows = [1, 3, 7, 9].map((key) => ({ key, value: `value-${key}` }));

describe('prolly WASM debug adapter', () => {
  it('builds child links and lower-bound lookup paths', () => {
    const tree = buildTreeFromDebug(debugTree(), rows);
    const rootHash = '03'.repeat(32);
    const leftHash = '01'.repeat(32);
    const rightHash = '02'.repeat(32);

    expect(tree.root.hash).toBe(rootHash);
    expect(tree.root.children).toHaveLength(2);
    expect(tree.root.entries.map((entry) => entry.key)).toEqual([1, 7]);
    expect(traceSearch(tree.root, 2)).toEqual([rootHash, leftHash]);
    expect(traceSearch(tree.root, 8)).toEqual([rootHash, rightHash]);
    expect(traceRange(tree.root, 2, 8)).toEqual([rootHash, leftHash, rightHash]);
    expect(traceRange(tree.root, 7, 8)).toEqual([rootHash, rightHash]);
  });

  it('rejects debug metadata that does not match the returned rows', () => {
    expect(() => buildTreeFromDebug(debugTree(), rows.slice(0, 3))).toThrow('reports 2 entries');
  });
});

describe('row diff', () => {
  it('classifies additions, edits, and deletes', () => {
    expect(diffRows(
      [{ key: 1, value: 'old' }, { key: 2, value: 'gone' }],
      [{ key: 1, value: 'new' }, { key: 3, value: 'added' }],
    )).toEqual([
      { key: 1, before: 'old', after: 'new', kind: 'modified' },
      { key: 2, before: 'gone', kind: 'deleted' },
      { key: 3, after: 'added', kind: 'added' },
    ]);
  });

  it('maps changed rows to their current leaf', () => {
    const tree = buildTreeFromDebug(debugTree(), rows);
    const diffs = [
      { key: 2, after: 'added', kind: 'added' as const },
      { key: 7, before: 'old', after: 'new', kind: 'modified' as const },
      { key: 8, before: 'gone', kind: 'deleted' as const },
    ];

    expect(groupRowDiffsByLeaf(tree.root, diffs)).toEqual(new Map([
      ['01'.repeat(32), [diffs[0]]],
      ['02'.repeat(32), [diffs[1], diffs[2]]],
    ]));
  });
});

describe('split probability', () => {
  it('uses prolly-map default hash-threshold chunking', () => {
    const tree = buildTreeFromDebug(debugTree(), rows);
    expect(estimateMutationSplitProbability(tree.root.children[0])).toBe(0);
    tree.root.children[0].entries.push({ key: 4, keyHex: '', valueHex: '' });
    expect(estimateMutationSplitProbability(tree.root.children[0])).toBe(1 / 128);
  });
});
