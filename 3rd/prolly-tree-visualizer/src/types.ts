export interface RowValue {
  key: number;
  value: string;
}

export interface ProllyEntry {
  key: number | string;
  keyHex: string;
  valueHex: string;
  childHash?: string;
  subtreeCount?: number;
}

export interface ProllyNode {
  hash: string;
  level: number;
  size: number;
  flags: number;
  entries: ProllyEntry[];
  children: ProllyNode[];
  minKey: number | string | null;
  maxKey: number | string | null;
}

export interface TreeSnapshot {
  id: number;
  label: string;
  rootHash: string;
  root: ProllyNode;
  rows: RowValue[];
  nodes: Map<string, ProllyNode>;
  chunksInStore: number;
  databaseBytes: number;
  timestamp: number;
}

export type NodeChange = 'new' | 'shared' | 'neutral' | 'trace';

export interface RowDiff {
  key: number;
  before?: string;
  after?: string;
  kind: 'added' | 'modified' | 'deleted';
}

export type LookupResult =
  | { kind: 'key'; key: number; found: boolean }
  | { kind: 'range'; start: number; end: number };
