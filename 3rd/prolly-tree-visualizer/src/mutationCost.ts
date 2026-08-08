import type { ProllyNode, RowDiff } from './types';

interface CostVersion {
  nodes: Map<string, Pick<ProllyNode, 'level' | 'size'>>;
}

export interface MutationCost {
  rowChanges: number;
  mutationBytes: number;
  leafHashes: string[];
  leafBytes: number;
  internalHashes: string[];
  internalBytes: number;
  changedHashes: string[];
  changedBytes: number;
  sharedNodes: number;
  retiredNodes: number;
  amplification?: number;
}

function valueBytes(value?: string) {
  return value === undefined ? 0 : new TextEncoder().encode(value).length;
}

export function calculateMutationCost(current: CostVersion, previous: CostVersion, rowDiffs: RowDiff[]): MutationCost {
  const changed = [...current.nodes].filter(([hash]) => !previous.nodes.has(hash));
  const leaf = changed.filter(([, node]) => node.level === 0);
  const internal = changed.filter(([, node]) => node.level > 0);
  const mutationBytes = rowDiffs.reduce((total, row) => total + 8 + (row.kind === 'deleted' ? 0 : valueBytes(row.after)), 0);
  const changedBytes = changed.reduce((total, [, node]) => total + node.size, 0);
  const sharedNodes = [...current.nodes.keys()].filter((hash) => previous.nodes.has(hash)).length;
  return {
    rowChanges: rowDiffs.length,
    mutationBytes,
    leafHashes: leaf.map(([hash]) => hash),
    leafBytes: leaf.reduce((total, [, node]) => total + node.size, 0),
    internalHashes: internal.map(([hash]) => hash),
    internalBytes: internal.reduce((total, [, node]) => total + node.size, 0),
    changedHashes: changed.map(([hash]) => hash),
    changedBytes,
    sharedNodes,
    retiredNodes: [...previous.nodes.keys()].filter((hash) => !current.nodes.has(hash)).length,
    amplification: mutationBytes === 0 ? undefined : changedBytes / mutationBytes,
  };
}
