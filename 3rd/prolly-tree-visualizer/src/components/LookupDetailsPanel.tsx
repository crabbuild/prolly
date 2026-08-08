import type { ProllyNode, TreeSnapshot } from '../types';
import { InfoTip } from './InfoTip';

export type LookupDetails =
  | { kind: 'key'; key: number; found: boolean; path: string[] }
  | { kind: 'range'; start: number; end: number; matchedRows: number; path: string[] };

interface LookupDetailsPanelProps {
  details: LookupDetails;
  snapshot: TreeSnapshot;
  visited: Set<string>;
  activeHash?: string;
}

function countLevel(path: string[], nodes: Map<string, ProllyNode>, leaf: boolean) {
  return path.filter((hash) => (nodes.get(hash)?.level === 0) === leaf).length;
}

export function LookupDetailsPanel({ details, snapshot, visited, activeHash }: LookupDetailsPanelProps) {
  const allHashes = [...snapshot.nodes.keys()];
  const totalInternal = countLevel(allHashes, snapshot.nodes, false);
  const totalLeaves = countLevel(allHashes, snapshot.nodes, true);
  const visitedPath = details.path.filter((hash) => visited.has(hash));
  const visitedInternal = countLevel(visitedPath, snapshot.nodes, false);
  const visitedLeaves = countLevel(visitedPath, snapshot.nodes, true);
  const progress = details.path.length === 0 ? 0 : visitedPath.length / details.path.length * 100;
  const complete = visitedPath.length === details.path.length && activeHash === undefined;
  const activeNode = activeHash ? snapshot.nodes.get(activeHash) : undefined;
  const title = details.kind === 'key' ? 'Key lookup' : 'Range lookup';
  const target = details.kind === 'key'
    ? details.key.toLocaleString()
    : `${Math.min(details.start, details.end).toLocaleString()} → ${Math.max(details.start, details.end).toLocaleString()}`;
  const result = details.kind === 'key'
    ? details.found ? 'Found' : 'Not found'
    : details.matchedRows.toLocaleString();
  const message = complete
    ? details.kind === 'key' ? `Key ${details.key.toLocaleString()} ${details.found ? 'found' : 'not found'}` : `${details.matchedRows.toLocaleString()} rows returned`
    : activeNode?.level === 0 ? 'Reading leaf chunk' : `Routing through level ${activeNode?.level ?? snapshot.root.level}`;
  return (
    <section className="mutation-cost lookup-details-panel" aria-label={`${title} details`}>
      <div className="mutation-cost-head">
        <div><span>{title}</span><strong>{message}</strong></div>
        <em>{complete ? 'Complete' : 'Searching'}</em>
      </div>
      <div className="mutation-flow">
        <div className="mutation-stage mutation-input">
          <span>{details.kind === 'key' ? 'Key' : 'Range'}</span>
          <b>{target}</b>
          <small>{details.kind === 'key' ? 'point lookup' : 'inclusive lookup'}</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage">
          <span>Internal chunks</span>
          <b>{visitedInternal} / {totalInternal}</b>
          <small>live routing nodes visited</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage">
          <span>Leaf chunks</span>
          <b>{visitedLeaves} / {totalLeaves}</b>
          <small>live data chunks read</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage mutation-total">
          <span>Result</span>
          <b>{complete ? result : '—'}</b>
          <small>{details.kind === 'key' ? 'matching key' : 'rows returned'}</small>
        </div>
      </div>
      <div className="mutation-cost-foot">
        <div className="mutation-cost-ratio">
          <div className="mutation-cost-bar"><span style={{ width: `${progress}%` }} /></div>
          <InfoTip>{details.kind === 'key' ? 'A balanced prolly tree follows one root-to-leaf path in O(log n), like a B-tree.' : 'A range lookup seeks to the range in O(log n), then reads the matching leaves and returns k rows: O(log n + k).'}</InfoTip>
        </div>
        <span>{snapshot.rows.length.toLocaleString()} rows · {snapshot.root.level + 1} levels</span>
        <span>{details.kind === 'key' ? 'O(log n)' : 'O(log n + k)'}</span>
      </div>
    </section>
  );
}
