import type { ProllyNode, RowValue } from '../types';

interface NodeInspectorProps {
  node?: ProllyNode;
  rows: RowValue[];
  onClose(): void;
}

export function NodeInspector({ node, rows, onClose }: NodeInspectorProps) {
  if (!node) return null;
  const rowMap = new Map(rows.map((row) => [row.key, row.value]));
  const visibleEntries = node.entries.slice(0, 12);
  const hiddenEntries = node.entries.length - visibleEntries.length;

  return (
    <aside className="inspector">
      <div className="inspector-head">
        <div>
          <span className="eyebrow">Chunk inspector</span>
          <h2>{node.level === 0 ? 'Leaf node' : `Internal node · level ${node.level}`}</h2>
        </div>
        <button className="icon-button" onClick={onClose} aria-label="Close inspector">×</button>
      </div>
      <dl className="facts">
        <div><dt>Content address</dt><dd className="full-hash">{node.hash}</dd></div>
        <div><dt>Encoded size</dt><dd>{node.size.toLocaleString()} bytes</dd></div>
        <div><dt>Key range</dt><dd>{node.minKey ?? 'empty'} → {node.maxKey ?? 'empty'}</dd></div>
        <div><dt>Entries</dt><dd>{node.entries.length}</dd></div>
      </dl>
      <div className="entry-list">
        <div className="entry-list-head">
          <span>{node.level === 0 ? 'decoded key → value' : 'lower bound → child address'}</span>
        </div>
        {visibleEntries.map((entry, index) => (
          <div className="entry-row" key={`${entry.keyHex}-${index}`}>
            <span className="entry-key">{String(entry.key)}</span>
            <span className="entry-arrow">→</span>
            <span className={entry.childHash ? 'entry-value full-hash' : 'entry-value'}>
              {entry.childHash ?? rowMap.get(Number(entry.key)) ?? `${entry.valueHex.slice(0, 24)}…`}
            </span>
            {entry.subtreeCount !== undefined && <span className="entry-count">{entry.subtreeCount} rows</span>}
          </div>
        ))}
        {hiddenEntries > 0 && <div className="entry-more">and {hiddenEntries.toLocaleString()} more entries</div>}
      </div>
    </aside>
  );
}
