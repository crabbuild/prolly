import type { MutationCost } from '../mutationCost';
import { InfoTip } from './InfoTip';

interface MutationCostPanelProps {
  cost: MutationCost;
  label: string;
  split: boolean;
  onHighlight(hashes: string[]): void;
}

function countLabel(count: number, singular: string) {
  return `${count} ${count === 1 ? singular : `${singular}s`}`;
}

function amplificationLabel(amplification?: number) {
  if (amplification === undefined) return '—';
  return `${amplification < 10 ? amplification.toFixed(1) : Math.round(amplification).toLocaleString()}×`;
}

export function MutationCostPanel({ cost, label, split, onHighlight }: MutationCostPanelProps) {
  const currentNodes = cost.changedHashes.length + cost.sharedNodes;
  const changedWidth = currentNodes === 0 ? 0 : cost.changedHashes.length / currentNodes * 100;
  return (
    <section className="mutation-cost" aria-label="Mutation cost">
      <div className="mutation-cost-head">
        <div><span>Mutation cost</span><strong>{label}</strong></div>
        {split && <em>Split</em>}
      </div>
      <div className="mutation-flow">
        <div className="mutation-stage mutation-input">
          <span>Mutation</span>
          <b>{countLabel(cost.rowChanges, 'row')}</b>
          <small>{cost.mutationBytes.toLocaleString()} B input</small>
        </div>
        <i aria-hidden="true">→</i>
        <button className="mutation-stage" disabled={cost.leafHashes.length === 0} onClick={() => onHighlight(cost.leafHashes)}>
          <span>Leaf chunks</span>
          <b>{cost.leafHashes.length}</b>
          <small>{cost.leafBytes.toLocaleString()} B changed</small>
        </button>
        <i aria-hidden="true">→</i>
        <button className="mutation-stage" disabled={cost.internalHashes.length === 0} onClick={() => onHighlight(cost.internalHashes)}>
          <span>Internal chunks</span>
          <b>{cost.internalHashes.length}</b>
          <small>{cost.internalBytes.toLocaleString()} B changed</small>
        </button>
        <i aria-hidden="true">→</i>
        <button className="mutation-stage mutation-total" disabled={cost.changedHashes.length === 0} onClick={() => onHighlight(cost.changedHashes)}>
          <span>Tree bytes changed</span>
          <b>{cost.changedBytes.toLocaleString()} B</b>
          <small>{amplificationLabel(cost.amplification)} amplification <InfoTip>Changed tree bytes divided by the submitted key and value bytes.</InfoTip></small>
        </button>
      </div>
      <div className="mutation-cost-foot">
        <div className="mutation-cost-ratio">
          <div className="mutation-cost-bar"><span style={{ width: `${changedWidth}%` }} /></div>
          <InfoTip>Orange is the share of HEAD nodes rewritten by this mutation. Gray nodes remained structurally shared.</InfoTip>
        </div>
        <span>{cost.changedHashes.length} rewritten</span>
        <span>{cost.sharedNodes} shared</span>
        <span>{cost.retiredNodes} retired</span>
      </div>
    </section>
  );
}
