import { describe, expect, it } from 'vitest';
import { calculateMutationCost } from './mutationCost';
import type { ProllyNode, RowDiff } from './types';

function nodes(entries: [string, number, number][]) {
  return new Map(entries.map(([hash, level, size]) => [hash, { level, size } as ProllyNode]));
}

describe('calculateMutationCost', () => {
  it('separates rewritten leaves and ancestors', () => {
    const previous = { nodes: nodes([['root-old', 1, 140], ['leaf-old', 0, 900], ['leaf-shared', 0, 700]]) };
    const current = { nodes: nodes([['root-new', 1, 150], ['leaf-new', 0, 960], ['leaf-shared', 0, 700]]) };
    const rows: RowDiff[] = [{ key: 42, after: 'new-value', kind: 'modified' }];

    expect(calculateMutationCost(current, previous, rows)).toEqual({
      rowChanges: 1,
      mutationBytes: 17,
      leafHashes: ['leaf-new'],
      leafBytes: 960,
      internalHashes: ['root-new'],
      internalBytes: 150,
      changedHashes: ['root-new', 'leaf-new'],
      changedBytes: 1110,
      sharedNodes: 1,
      retiredNodes: 2,
      amplification: 1110 / 17,
    });
  });

  it('reports no amplification for a no-op', () => {
    const version = { nodes: nodes([['root', 0, 200]]) };
    expect(calculateMutationCost(version, version, [])).toMatchObject({
      rowChanges: 0,
      mutationBytes: 0,
      changedBytes: 0,
      amplification: undefined,
    });
  });
});
