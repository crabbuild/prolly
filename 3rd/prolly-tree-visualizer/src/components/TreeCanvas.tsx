import { useEffect, useMemo, useRef, type CSSProperties } from 'react';
import { groupRowDiffsByLeaf } from '../prolly';
import type { LookupResult, ProllyNode, RowDiff, TreeSnapshot } from '../types';

interface Point {
  x: number;
  y: number;
  node: ProllyNode;
}

const NODE_WIDTH = 282;
const NODE_HEIGHT = 126;
const X_GAP = 34;
const Y_GAP = 184;

interface KeyLabelPart {
  text: string;
  match: boolean;
  missing?: boolean;
}

const KEY_PREVIEW_CHARACTERS = 34;
const NODE_ID_PREFIX_CHARACTERS = 16;
const NODE_ID_SUFFIX_CHARACTERS = 12;

export function compactNodeId(nodeId: string) {
  const visibleCharacters = NODE_ID_PREFIX_CHARACTERS + NODE_ID_SUFFIX_CHARACTERS;
  if (nodeId.length <= visibleCharacters + 1) return nodeId;
  return `${nodeId.slice(0, NODE_ID_PREFIX_CHARACTERS)}…${nodeId.slice(-NODE_ID_SUFFIX_CHARACTERS)}`;
}

function partsForIndexes(keys: string[], indexes: number[], matchIndexes: number[]) {
  return indexes.flatMap((index, position): KeyLabelPart[] => {
    const previousIndex = indexes[position - 1];
    const omitted = position > 0 && index - previousIndex > 1
      ? [{ text: '…', match: false }]
      : [];
    return [...omitted, { text: keys[index], match: matchIndexes.includes(index) }];
  });
}

function previewLength(parts: KeyLabelPart[]) {
  return parts.reduce((length, part) => length + part.text.length, 0) + Math.max(0, parts.length - 1) * 3;
}

function layout(root: ProllyNode, minimumWidth: number) {
  const positions = new Map<string, Point>();
  let leafIndex = 0;

  const place = (node: ProllyNode): number => {
    let center: number;
    if (node.children.length === 0) {
      center = leafIndex * (NODE_WIDTH + X_GAP) + NODE_WIDTH / 2 + 38;
      leafIndex += 1;
    } else {
      const childCenters = node.children.map(place);
      center = (childCenters[0] + childCenters.at(-1)!) / 2;
    }
    positions.set(node.hash, {
      x: center - NODE_WIDTH / 2,
      y: (root.level - node.level) * Y_GAP + 38,
      node,
    });
    return center;
  };

  const rootCenter = place(root);
  const contentWidth = Math.max(leafIndex * (NODE_WIDTH + X_GAP) + 42, NODE_WIDTH + 76);
  if (leafIndex === 1) {
    const offset = Math.max(0, minimumWidth / 2 - rootCenter);
    for (const point of positions.values()) point.x += offset;
  }
  return {
    positions,
    width: Math.max(minimumWidth, contentWidth),
    height: (root.level + 1) * Y_GAP + 30,
  };
}

export function shownKeys(node: ProllyNode, lookupKeys: number[] = []): KeyLabelPart[] {
  const keys = node.entries.map((entry) => String(entry.key));
  const lookupKeySet = new Set(lookupKeys);
  const matchIndexes = node.entries.flatMap((entry, index) => lookupKeySet.has(Number(entry.key)) ? [index] : []);
  const allIndexes = keys.map((_, index) => index);
  const allParts = partsForIndexes(keys, allIndexes, matchIndexes);
  if (matchIndexes.length === 0 && previewLength(allParts) <= KEY_PREVIEW_CHARACTERS) return allParts;

  if (matchIndexes.length > 0) {
    const withEdges = [...new Set([0, ...matchIndexes, keys.length - 1])].sort((left, right) => left - right);
    const edgeParts = partsForIndexes(keys, withEdges, matchIndexes);
    if (previewLength(edgeParts) <= KEY_PREVIEW_CHARACTERS) return edgeParts;
    if (matchIndexes.length === 1) {
      const match = matchIndexes[0];
      const nearby = [match - 1, match, match + 1].filter((index) => index >= 0 && index < keys.length);
      const nearbyParts = partsForIndexes(keys, nearby, matchIndexes);
      if (previewLength(nearbyParts) <= KEY_PREVIEW_CHARACTERS) return nearbyParts;
    }
    return partsForIndexes(keys, matchIndexes, matchIndexes);
  }

  const outsideIndexes = [...new Set([0, 1, keys.length - 2, keys.length - 1])]
    .filter((index) => index >= 0 && index < keys.length)
    .sort((left, right) => left - right);
  const outsideParts = partsForIndexes(keys, outsideIndexes, matchIndexes);
  if (previewLength(outsideParts) <= KEY_PREVIEW_CHARACTERS) return outsideParts;
  return partsForIndexes(keys, [0, keys.length - 1], matchIndexes);
}

export function missingKeyLabel(node: ProllyNode, lookupKey: number): KeyLabelPart[] {
  const keys = node.entries.map((entry) => Number(entry.key));
  const insertionIndex = keys.findIndex((key) => key > lookupKey);
  if (insertionIndex > 0) {
    return [
      { text: String(keys[insertionIndex - 1]), match: false },
      { text: `key ${lookupKey} not found`, match: false, missing: true },
      { text: String(keys[insertionIndex]), match: false },
    ];
  }
  if (insertionIndex === 0) {
    return [
      { text: `key ${lookupKey} not found`, match: false, missing: true },
      ...keys.slice(0, 2).map((key) => ({ text: String(key), match: false })),
    ];
  }
  return [
    ...keys.slice(-2).map((key) => ({ text: String(key), match: false })),
    { text: `key ${lookupKey} not found`, match: false, missing: true },
  ];
}

export function routeKeyForLookup(node: ProllyNode, lookupKey: number) {
  const entry = node.entries.findLast((candidate) => Number(candidate.key) <= lookupKey) ?? node.entries[0];
  return entry === undefined ? undefined : Number(entry.key);
}

export function rangeKeysForNode(node: ProllyNode, start: number, end: number) {
  const low = Math.min(start, end);
  const high = Math.max(start, end);
  if (node.level > 0) {
    return node.children.flatMap((child, index) => {
      const childMin = Number(child.minKey);
      const childMax = Number(child.maxKey);
      return childMax >= low && childMin <= high ? [Number(node.entries[index].key)] : [];
    });
  }
  const matches = node.entries.map((entry) => Number(entry.key)).filter((key) => key >= low && key <= high);
  return matches.length <= 1 ? matches : [matches[0], matches.at(-1)!];
}

function shownChanges(changes: RowDiff[]) {
  if (changes.length === 1) {
    const change = changes[0];
    return `key ${change.key} ${change.kind === 'modified' ? 'updated' : change.kind}`;
  }
  const keys = changes.slice(0, 3).map((change) => change.key).join(', ');
  return `${changes.length} changed rows · ${keys}${changes.length > 3 ? ', …' : ''}`;
}

interface TreeCanvasProps {
  snapshot: TreeSnapshot;
  baseline?: TreeSnapshot;
  trace: Set<string>;
  activeTraceHash?: string;
  diffHighlight: Set<string>;
  activeDiffHashes?: Set<string>;
  diffSkipped?: Set<string>;
  activeDiffSkipped?: Set<string>;
  rowDiffs: RowDiff[];
  lookup?: LookupResult;
  compact?: boolean;
  selectedHash?: string;
  onSelect(hash: string): void;
}

export function TreeCanvas({ snapshot, baseline, trace, activeTraceHash, diffHighlight, activeDiffHashes, diffSkipped, activeDiffSkipped, rowDiffs, lookup, compact = false, selectedHash, onSelect }: TreeCanvasProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const treeLayout = useMemo(() => layout(snapshot.root, compact ? 586 : 900), [compact, snapshot]);
  const rowDiffsByLeaf = useMemo(() => groupRowDiffsByLeaf(snapshot.root, rowDiffs), [snapshot, rowDiffs]);
  const baselineHashes = baseline?.nodes;

  useEffect(() => {
    const scroll = scrollRef.current;
    const activeDiff = activeDiffHashes && activeDiffHashes.size > 0
      ? [...activeDiffHashes]
      : activeDiffSkipped ? [...activeDiffSkipped] : [];
    const focusHashes = activeTraceHash ? [activeTraceHash] : activeDiff;
    const points = focusHashes.flatMap((hash) => {
      const point = treeLayout.positions.get(hash);
      return point ? [point] : [];
    });
    if (!scroll || points.length === 0 || scroll.scrollWidth <= scroll.clientWidth) return;
    const left = Math.min(...points.map((point) => point.x));
    const right = Math.max(...points.map((point) => point.x + NODE_WIDTH));
    scroll.scrollTo({
      left: Math.max(0, (left + right) / 2 - scroll.clientWidth / 2),
      behavior: window.matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth',
    });
  }, [activeDiffHashes, activeDiffSkipped, activeTraceHash, treeLayout]);

  return (
    <div
      ref={scrollRef}
      className="tree-scroll"
      aria-label="Prolly tree visualization"
      style={{ '--focus-delay': snapshot.nodes.size > 100 ? '1.2s' : snapshot.nodes.size > 20 ? '.6s' : '.3s' } as CSSProperties}
    >
      <svg className="tree-svg" width={treeLayout.width} height={treeLayout.height} role="img">
        <title>Prolly tree rooted at {snapshot.rootHash}</title>
        <g className="tree-edges">
          {[...treeLayout.positions.values()].flatMap((point) =>
            point.node.children.map((child) => {
              const childPoint = treeLayout.positions.get(child.hash)!;
              const changed = baselineHashes ? !baselineHashes.has(child.hash) : false;
              const highlighted = diffHighlight.has(child.hash);
              const skipped = diffSkipped?.has(child.hash);
              const traced = trace.has(point.node.hash) && trace.has(child.hash);
              return (
                <path
                  key={`${point.node.hash}-${child.hash}`}
                  className={highlighted ? 'edge edge-diff' : skipped ? 'edge edge-skip' : traced ? 'edge edge-trace' : changed ? 'edge edge-new' : 'edge'}
                  d={`M ${point.x + NODE_WIDTH / 2} ${point.y + NODE_HEIGHT} C ${point.x + NODE_WIDTH / 2} ${point.y + NODE_HEIGHT + 42}, ${childPoint.x + NODE_WIDTH / 2} ${childPoint.y - 42}, ${childPoint.x + NODE_WIDTH / 2} ${childPoint.y}`}
                />
              );
            }),
          )}
        </g>
        {[...treeLayout.positions.values()].map(({ node, x, y }) => {
          const nodeRowDiffs = diffHighlight.has(node.hash) ? rowDiffsByLeaf.get(node.hash) ?? [] : [];
          const isTrace = trace.has(node.hash);
          const isActiveTrace = activeTraceHash === node.hash;
          const isDiff = diffHighlight.has(node.hash);
          const isSkipped = Boolean(diffSkipped?.has(node.hash));
          const isActiveSkipped = Boolean(activeDiffSkipped?.has(node.hash));
          const isNew = Boolean(baselineHashes && !baselineHashes.has(node.hash));
          const lookupKeys = lookup && isTrace
            ? lookup.kind === 'range'
              ? rangeKeysForNode(node, lookup.start, lookup.end)
              : node.level === 0 && lookup.found
                ? [lookup.key]
                : node.level > 0
                  ? [routeKeyForLookup(node, lookup.key)].filter((key): key is number => key !== undefined)
                  : []
            : [];
          const keyParts = lookup?.kind === 'key' && !lookup.found && node.level === 0 && isTrace
            ? missingKeyLabel(node, lookup.key)
            : shownKeys(node, lookupKeys);
          const className = [
            'tree-node',
            node.level === 0 ? 'leaf-node' : 'internal-node',
            isNew ? 'node-new' : '',
            isTrace ? 'node-trace' : '',
            isDiff ? 'node-diff' : '',
            isSkipped ? 'node-skip' : '',
            isActiveSkipped ? 'node-skip-active' : '',
            selectedHash === node.hash ? 'node-selected' : '',
          ].filter(Boolean).join(' ');
          return (
            <g
              key={node.hash}
              className={className}
              transform={`translate(${x} ${y})`}
              tabIndex={0}
              role="button"
              aria-label={`${node.level === 0 ? 'Leaf' : `Level ${node.level}`} node ${node.hash}`}
              onMouseDown={(event) => event.preventDefault()}
              onClick={() => onSelect(node.hash)}
              onKeyDown={(event) => {
                if (event.key === 'Enter' || event.key === ' ') onSelect(node.hash);
              }}
            >
              <rect width={NODE_WIDTH} height={NODE_HEIGHT} rx="14" />
              <text className="node-kind" x="15" y="24">
                {isSkipped ? 'SHARED · SKIP' : node.level === 0 ? 'LEAF CHUNK' : `INTERNAL · LEVEL ${node.level}`}
              </text>
              <text className="node-count" x={NODE_WIDTH - 15} y="24" textAnchor="end">
                {node.entries.length} {node.entries.length === 1 ? 'entry' : 'entries'}
              </text>
              <line x1="14" x2={NODE_WIDTH - 14} y1="36" y2="36" />
              <text className={nodeRowDiffs.length ? 'node-keys node-row-change' : 'node-keys'} x="15" y="61">
                {nodeRowDiffs.length ? shownChanges(nodeRowDiffs) : keyParts.map((part, index) => (
                  <tspan key={`${part.text}-${index}`} className={part.match ? isActiveTrace ? 'node-key-match' : 'node-key-route' : part.missing ? isActiveTrace ? 'node-key-missing' : 'node-key-missing-static' : undefined}>{index > 0 ? '  ·  ' : ''}{part.text}</tspan>
                ))}
              </text>
              <text className="node-range" x="15" y="84">
                {node.size.toLocaleString()} bytes
              </text>
              <text className={activeDiffHashes?.has(node.hash) ? 'node-hash node-hash-active' : isActiveSkipped ? 'node-hash node-hash-skip-active' : 'node-hash'} x="15" y="108">
                <title>Full content address: {node.hash}</title>
                {compactNodeId(node.hash)}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}
