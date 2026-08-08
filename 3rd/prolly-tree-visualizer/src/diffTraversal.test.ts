import { describe, expect, it } from 'vitest';
import { buildDiffTraversal } from './diffTraversal';
import type { ProllyNode, TreeSnapshot } from './types';

function leaf(hash: string, keys: number[]): ProllyNode {
  return {
    hash,
    level: 0,
    size: 100,
    flags: 0,
    entries: keys.map((key) => ({ key, keyHex: '', valueHex: '' })),
    children: [],
    minKey: keys[0],
    maxKey: keys.at(-1) ?? null,
  };
}

function snapshot(id: number, root: ProllyNode): TreeSnapshot {
  const nodes = new Map<string, ProllyNode>();
  const visit = (node: ProllyNode) => {
    nodes.set(node.hash, node);
    node.children.forEach(visit);
  };
  visit(root);
  const rows = [...nodes.values()].filter((node) => node.level === 0).flatMap((node) => node.entries.map((entry) => ({ key: Number(entry.key), value: `v${entry.key}` })));
  return { id, label: '', rootHash: root.hash, root, rows, nodes, chunksInStore: nodes.size, databaseBytes: 0, timestamp: 0 };
}

function root(hash: string, children: ProllyNode[]): ProllyNode {
  return {
    hash,
    level: 1,
    size: 100,
    flags: 0,
    entries: children.map((child) => ({ key: Number(child.maxKey), keyHex: '', valueHex: '', childHash: child.hash })),
    children,
    minKey: children[0].minKey,
    maxKey: children.at(-1)?.maxKey ?? null,
  };
}

describe('buildDiffTraversal', () => {
  it('stops at shared children and counts their rows', () => {
    const shared = leaf('shared', [1, 2, 3]);
    const before = snapshot(1, root('before-root', [shared, leaf('old', [4])]));
    const after = snapshot(2, root('after-root', [shared, leaf('new', [4])]));
    const frames = buildDiffTraversal(after, before, [{ key: 4, before: 'old', after: 'new', kind: 'modified' }]);

    expect(frames.at(-1)).toMatchObject({
      addressesChecked: 3,
      nodesVisited: 2,
      skippedSubtrees: 1,
      skippedRows: 3,
      revealedRows: true,
    });
    expect(frames[1].activeSkippedHashes).toEqual(['shared']);
  });

  it('skips the whole tree when root addresses match', () => {
    const tree = snapshot(1, leaf('same', [1, 2]));
    expect(buildDiffTraversal(tree, tree, [])).toEqual([expect.objectContaining({
      addressesChecked: 1,
      nodesVisited: 0,
      skippedSubtrees: 1,
      skippedRows: 2,
      revealedRows: true,
    })]);
  });
});
