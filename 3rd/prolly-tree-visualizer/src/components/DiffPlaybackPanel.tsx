import type { DiffTraversalFrame } from '../diffTraversal';
import { InfoTip } from './InfoTip';

interface DiffPlaybackPanelProps {
  playback: DiffTraversalFrame & { step: number; totalSteps: number; running: boolean };
  rowDifferences: number;
}

export function DiffPlaybackPanel({ playback, rowDifferences }: DiffPlaybackPanelProps) {
  const progress = (playback.step + 1) / playback.totalSteps * 100;
  return (
    <section className="mutation-cost diff-playback-panel" aria-label="Fast diff details">
      <div className="mutation-cost-head">
        <div><span>Fast diff</span><strong>{playback.message}</strong></div>
        <em>{playback.running ? 'Diffing' : 'Complete'}</em>
      </div>
      <div className="mutation-flow">
        <div className="mutation-stage mutation-input">
          <span>Addresses checked</span>
          <b>{playback.addressesChecked}</b>
          <small>content-address comparisons</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage">
          <span>Changed nodes</span>
          <b>{playback.nodesVisited}</b>
          <small>chunks opened</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage diff-skip-stage">
          <span>Shared subtrees</span>
          <b>{playback.skippedSubtrees}</b>
          <small>skipped by address</small>
        </div>
        <i aria-hidden="true">→</i>
        <div className="mutation-stage mutation-total">
          <span>Rows avoided</span>
          <b>{playback.skippedRows.toLocaleString()}</b>
          <small>{rowDifferences} row {rowDifferences === 1 ? 'difference' : 'differences'} {playback.revealedRows ? 'emitted' : 'pending'}</small>
        </div>
      </div>
      <div className="mutation-cost-foot">
        <div className="mutation-cost-ratio">
          <div className="mutation-cost-bar"><span style={{ width: `${progress}%` }} /></div>
          <InfoTip>The bar advances as diff compares each tree level, skips matching addresses, and reaches the changed leaves.</InfoTip>
        </div>
        <span>step {playback.step + 1} of {playback.totalSteps}</span>
      </div>
    </section>
  );
}
