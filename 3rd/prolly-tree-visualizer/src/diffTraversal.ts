import type { ProllyNode, RowDiff, TreeSnapshot } from './types';

export interface DiffTraversalFrame {
  message: string;
  changedHashes: string[];
  skippedHashes: string[];
  activeChangedHashes: string[];
  activeSkippedHashes: string[];
  addressesChecked: number;
  nodesVisited: number;
  skippedSubtrees: number;
  skippedRows: number;
  revealedRows: boolean;
}

function rowCount(node: ProllyNode): number {
  return node.level === 0
    ? node.entries.length
    : node.children.reduce((total, child) => total + rowCount(child), 0);
}

export function buildDiffTraversal(current: TreeSnapshot, baseline: TreeSnapshot, rowDiffs: RowDiff[]) {
  if (current.rootHash === baseline.rootHash) {
    return [{
      message: 'Root addresses match · entire tree skipped',
      changedHashes: [],
      skippedHashes: [current.rootHash],
      activeChangedHashes: [],
      activeSkippedHashes: [current.rootHash],
      addressesChecked: 1,
      nodesVisited: 0,
      skippedSubtrees: 1,
      skippedRows: current.rows.length,
      revealedRows: true,
    } satisfies DiffTraversalFrame];
  }

  const changed = new Set<string>([current.rootHash]);
  const skipped = new Set<string>();
  let addressesChecked = 1;
  let nodesVisited = 1;
  let skippedRows = 0;
  let frontier = [current.root];
  const frames: DiffTraversalFrame[] = [{
    message: 'Root addresses differ · opening the new root',
    changedHashes: [...changed],
    skippedHashes: [],
    activeChangedHashes: [current.rootHash],
    activeSkippedHashes: [],
    addressesChecked,
    nodesVisited,
    skippedSubtrees: 0,
    skippedRows,
    revealedRows: false,
  }];

  while (frontier.some((node) => node.children.length > 0)) {
    const children = frontier.flatMap((node) => node.children);
    const nextChanged: ProllyNode[] = [];
    const nextSkipped: ProllyNode[] = [];
    for (const child of children) {
      addressesChecked += 1;
      if (baseline.nodes.has(child.hash)) {
        skipped.add(child.hash);
        nextSkipped.push(child);
        skippedRows += rowCount(child);
      } else {
        changed.add(child.hash);
        nextChanged.push(child);
        nodesVisited += 1;
      }
    }
    const descent = nextChanged.length === 0
      ? 'no changed children to open'
      : `opening ${nextChanged.length} changed ${nextChanged.length === 1 ? 'child' : 'children'}`;
    const avoided = nextSkipped.length === 0
      ? ''
      : ` · skipped ${nextSkipped.length} shared ${nextSkipped.length === 1 ? 'subtree' : 'subtrees'}`;
    frames.push({
      message: `${descent}${avoided}`,
      changedHashes: [...changed],
      skippedHashes: [...skipped],
      activeChangedHashes: nextChanged.map((node) => node.hash),
      activeSkippedHashes: nextSkipped.map((node) => node.hash),
      addressesChecked,
      nodesVisited,
      skippedSubtrees: skipped.size,
      skippedRows,
      revealedRows: false,
    });
    frontier = nextChanged;
    if (frontier.length === 0) break;
  }

  const changedLeaves = [...changed].filter((hash) => current.nodes.get(hash)?.level === 0);
  frames.push({
    message: rowDiffs.length === 0
      ? 'Leaf comparison complete · rows are equal'
      : `${rowDiffs.length} row ${rowDiffs.length === 1 ? 'difference' : 'differences'} emitted`,
    changedHashes: [...changed],
    skippedHashes: [...skipped],
    activeChangedHashes: changedLeaves,
    activeSkippedHashes: [],
    addressesChecked,
    nodesVisited,
    skippedSubtrees: skipped.size,
    skippedRows,
    revealedRows: true,
  });
  return frames;
}
