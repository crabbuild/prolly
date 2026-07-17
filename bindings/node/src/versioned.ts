import { nativePromise, ownedBytes, scopedBytes, type ViewScope } from "./packed.ts";
import type { NativeSnapshotBundleRecord } from "./native.ts";

export type SnapshotBundle = NativeSnapshotBundleRecord;

export interface MapVersion {
  id: Uint8Array;
  tree: unknown;
  createdAtMillis?: bigint;
  isHead: boolean;
}

export interface MapMutation {
  kind: "upsert" | "delete";
  key: Uint8Array;
  value?: Uint8Array;
}

export interface MapUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previous?: Uint8Array;
  current?: MapVersion;
}

export interface VersionPrune {
  retained: Uint8Array[];
  removed: Uint8Array[];
}
export interface VersionedParallelConfig { maxThreads: bigint; parallelismThreshold: bigint; }
export interface VersionedBatchApplyStats {
  inputMutations: bigint;
  effectiveMutations: bigint;
  preprocessInputSorted: boolean;
  affectedLeaves: bigint;
  changedLeaves: bigint;
  sparseLeafApplies: bigint;
  writtenNodes: bigint;
  writtenBytes: bigint;
  usedAppendFastPath: boolean;
  usedBatchedRoute: boolean;
  usedCoalescedRebuild: boolean;
  usedDeferredRebalancing: boolean;
  usedBottomUpRebuild: boolean;
  cacheWrittenNodes: boolean;
}
export interface VersionedMapBatchResult { version: MapVersion; stats: VersionedBatchApplyStats; }
export interface CatalogVerification {
  head: Uint8Array;
  versionCount: bigint;
  reachableNodes: bigint;
  reachableBytes: bigint;
}
export interface GcReachability {
  liveCids: Uint8Array[];
  liveNodes: bigint;
  liveBytes: bigint;
  leafNodes: bigint;
  internalNodes: bigint;
}
export interface GcPlan {
  reachability: GcReachability;
  candidateNodes: bigint;
  reclaimableCids: Uint8Array[];
  reclaimableNodes: bigint;
  reclaimableBytes: bigint;
  missingCandidates: bigint;
}
export interface GcSweep { plan: GcPlan; deletedNodes: bigint; deletedBytes: bigint; }
export interface NamedRootRetention {
  kind: "all" | "exact" | "prefix" | "newest_by_name" | "updated_since";
  names: Uint8Array[];
  prefix?: Uint8Array;
  count?: bigint;
  minUpdatedAtMillis?: bigint;
}

export interface MapEntry { key: Uint8Array; value: Uint8Array; }
export interface RangeCursor { afterKey?: Uint8Array; }
export interface ReverseCursor { beforeKey?: Uint8Array; }
export interface RangePage { entries: MapEntry[]; nextCursor?: RangeCursor; }
export interface ReversePage { entries: MapEntry[]; nextCursor?: ReverseCursor; }
export interface MapDiff {
  kind: "added" | "removed" | "changed";
  key: Uint8Array;
  value?: Uint8Array;
  old?: Uint8Array;
  newValue?: Uint8Array;
}
export interface DiffPage { diffs: MapDiff[]; nextCursor?: RangeCursor; }
export interface MapConflict { key: Uint8Array; base?: Uint8Array; left?: Uint8Array; right?: Uint8Array; }
export interface ConflictPage { conflicts: MapConflict[]; nextCursor?: RangeCursor; }
export interface MapChangeEvent { previous?: Uint8Array; current: MapVersion; diffs: MapDiff[]; }
export interface VersionedTransactionCommit {
  applied: boolean;
  versions: MapVersion[];
  conflictMapId?: Uint8Array;
  conflictCurrent?: MapVersion;
}
/** Read-only page views that expire when the scan callback returns. */
export interface MapEntryView { key: Uint8Array; value: Uint8Array; }
export interface ReadScanOutcome { visited: bigint; stopped: boolean; }
export interface KeyProofVerification {
  valid: boolean;
  exists: boolean;
  absence?: boolean;
  root?: Uint8Array;
  key?: Uint8Array;
  value?: Uint8Array;
}
export interface MultiKeyProofVerification {
  valid: boolean;
  root?: Uint8Array;
  results: KeyProofVerification[];
}
export interface RangeProofVerification {
  valid: boolean;
  root?: Uint8Array;
  start: Uint8Array;
  end?: Uint8Array;
  entries: MapEntry[];
}
export interface RangePageProofVerification {
  valid: boolean;
  root?: Uint8Array;
  after?: Uint8Array;
  end?: Uint8Array;
  entries: MapEntry[];
}

interface NativeMapVersion {
  id: Uint8Array;
  tree: unknown;
  createdAtMillis?: string;
  isHead: boolean;
}

interface NativeVersionedMap {
  id(): Uint8Array;
  isInitialized(): boolean;
  initialize(): NativeMapVersion;
  initializeSorted(entries: MapEntry[]): NativeMapUpdate;
  head(): NativeMapVersion | null;
  headId(): Uint8Array | null;
  version(id: Uint8Array): NativeMapVersion | null;
  versions(): NativeMapVersion[];
  get(key: Uint8Array): Uint8Array | null;
  containsKey(key: Uint8Array): boolean;
  getMany(keys: Uint8Array[]): Array<Uint8Array | null>;
  getAt(id: Uint8Array, key: Uint8Array): Uint8Array | null;
  getManyAt(id: Uint8Array, keys: Uint8Array[]): Array<Uint8Array | null>;
  range(start: Uint8Array, end: Uint8Array | null): MapEntry[];
  prefix(prefix: Uint8Array): MapEntry[];
  rangeAt(id: Uint8Array, start: Uint8Array, end: Uint8Array | null): MapEntry[];
  prefixAt(id: Uint8Array, prefix: Uint8Array): MapEntry[];
  rangePage(cursor: RangeCursor | null, end: Uint8Array | null, limit: string): RangePage;
  prefixPage(prefix: Uint8Array, cursor: RangeCursor | null, limit: string): RangePage;
  rangePageAt(id: Uint8Array, cursor: RangeCursor | null, end: Uint8Array | null, limit: string): RangePage;
  prefixPageAt(id: Uint8Array, prefix: Uint8Array, cursor: RangeCursor | null, limit: string): RangePage;
  diff(base: Uint8Array, target: Uint8Array): MapDiff[];
  changesSince(base: Uint8Array): MapDiff[];
  rollbackTo(id: Uint8Array): NativeMapVersion;
  put(key: Uint8Array, value: Uint8Array): NativeMapVersion;
  apply(mutations: MapMutation[]): NativeMapVersion;
  append(mutations: MapMutation[]): NativeMapVersion;
  parallelApply(mutations: MapMutation[], config: NativeVersionedParallelConfig): NativeVersionedMapBatchResult;
  rebuildSortedIf(expected: Uint8Array | null, entries: MapEntry[]): NativeMapUpdate;
  rebuildFromEntriesIf(expected: Uint8Array | null, entries: MapEntry[]): NativeMapUpdate;
  applyAtMillis(mutations: MapMutation[], timestampMillis: string): NativeMapVersion;
  applyIf(expected: Uint8Array | null, mutations: MapMutation[]): NativeMapUpdate;
  applyIfAtMillis(expected: Uint8Array | null, mutations: MapMutation[], timestampMillis: string): NativeMapUpdate;
  putIf(expected: Uint8Array | null, key: Uint8Array, value: Uint8Array): NativeMapUpdate;
  deleteIf(expected: Uint8Array | null, key: Uint8Array): NativeMapUpdate;
  delete(key: Uint8Array): NativeMapVersion;
  snapshot(): NativeMapSnapshot | null;
  snapshotAt(id: Uint8Array): NativeMapSnapshot | null;
  compare(base: Uint8Array, target: Uint8Array): NativeMapComparison;
  compareToHead(base: Uint8Array): NativeMapComparison;
  subscribe(): NativeMapSubscription;
  subscribeFrom(lastSeen: Uint8Array | null): NativeMapSubscription;
  prepareMerge(base: Uint8Array, candidate: Uint8Array): NativeMapMerge;
  backup(): Uint8Array;
  restoreBackup(bytes: Uint8Array): NativeMapVersion;
  importAsHead(bundle: SnapshotBundle): NativeMapVersion;
  importAsHeadAtMillis(bundle: SnapshotBundle, timestampMillis: string): NativeMapVersion;
  keepLast(count: number): { retained: Uint8Array[]; removed: Uint8Array[] };
  pruneVersions(keepLatest: string): { retained: Uint8Array[]; removed: Uint8Array[] };
  keepForAt(nowMillis: string, maxAgeMillis: string): { retained: Uint8Array[]; removed: Uint8Array[] };
  keepFor(maxAgeMillis: string): { retained: Uint8Array[]; removed: Uint8Array[] };
  keepVersions(ids: Uint8Array[]): { retained: Uint8Array[]; removed: Uint8Array[] };
  retentionPolicy(): NativeNamedRootRetention;
  verifyCatalog(): NativeCatalogVerification;
  planGc(): NativeGcPlan;
  sweepGc(): NativeGcSweep;
}

interface NativeMapComparison {
  base(): NativeMapVersion;
  target(): NativeMapVersion;
  diff(): MapDiff[];
  diffPage(cursor: RangeCursor | null, end: Uint8Array | null, limit: string): DiffPage;
}
interface NativeMapChangeEvent { previous?: Uint8Array; current: NativeMapVersion; diffs: MapDiff[]; }
interface NativeMapSubscription {
  lastSeen(): Uint8Array | null;
  poll(): NativeMapChangeEvent | null;
}
interface NativeMapMerge {
  base(): NativeMapVersion;
  head(): NativeMapVersion;
  candidate(): NativeMapVersion;
  merge(resolver: string | null): unknown;
  conflictPage(cursor: RangeCursor | null, limit: string): ConflictPage;
  publish(resolver: string | null): NativeMapUpdate;
}
interface NativeVersionedTransactionCommit {
  applied: boolean;
  versions: NativeMapVersion[];
  conflictMapId?: Uint8Array;
  conflictCurrent?: NativeMapVersion;
}
interface NativeVersionedTransaction {
  head(mapId: Uint8Array): NativeMapVersion | null;
  get(mapId: Uint8Array, key: Uint8Array): Uint8Array | null;
  apply(mapId: Uint8Array, mutations: MapMutation[]): NativeMapVersion;
  applyIf(mapId: Uint8Array, expected: Uint8Array | null, mutations: MapMutation[]): NativeMapUpdate;
  put(mapId: Uint8Array, key: Uint8Array, value: Uint8Array): NativeMapVersion;
  delete(mapId: Uint8Array, key: Uint8Array): NativeMapVersion;
  commit(): NativeVersionedTransactionCommit;
  rollback(): void;
}

interface NativeMapUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previous?: Uint8Array;
  current?: NativeMapVersion;
}

interface NativeVersionedParallelConfig { maxThreads: string; parallelismThreshold: string; }
interface NativeVersionedBatchApplyStats {
  inputMutations: string; effectiveMutations: string; preprocessInputSorted: boolean;
  affectedLeaves: string; changedLeaves: string; sparseLeafApplies: string;
  writtenNodes: string; writtenBytes: string; usedAppendFastPath: boolean;
  usedBatchedRoute: boolean; usedCoalescedRebuild: boolean; usedDeferredRebalancing: boolean;
  usedBottomUpRebuild: boolean; cacheWrittenNodes: boolean;
}
interface NativeVersionedMapBatchResult { version: NativeMapVersion; stats: NativeVersionedBatchApplyStats; }

interface NativeMaintenanceSummary { itemCount: string; byteCount: string; }
interface NativeCatalogVerification { head: Uint8Array; versionCount: string; reachableNodes: string; reachableBytes: string; }
interface NativeGcReachability { liveCids: Uint8Array[]; liveNodes: string; liveBytes: string; leafNodes: string; internalNodes: string; }
interface NativeGcPlan { reachability: NativeGcReachability; candidateNodes: string; reclaimableCids: Uint8Array[]; reclaimableNodes: string; reclaimableBytes: string; missingCandidates: string; }
interface NativeGcSweep { plan: NativeGcPlan; deletedNodes: string; deletedBytes: string; }
interface NativeNamedRootRetention { kind: NamedRootRetention["kind"]; names: Uint8Array[]; prefix?: Uint8Array; count?: string; minUpdatedAtMillis?: string; }
interface NativeMapSnapshot {
  id(): Uint8Array;
  version(): NativeMapVersion;
  get(key: Uint8Array): Uint8Array | null;
  getMany(keys: Uint8Array[]): Array<Uint8Array | null>;
  containsKey(key: Uint8Array): boolean;
  firstEntry(): MapEntry | null;
  lastEntry(): MapEntry | null;
  lowerBound(key: Uint8Array): MapEntry | null;
  upperBound(key: Uint8Array): MapEntry | null;
  range(start: Uint8Array, end: Uint8Array | null): MapEntry[];
  prefix(prefix: Uint8Array): MapEntry[];
  rangePage(cursor: RangeCursor | null, end: Uint8Array | null, limit: string): RangePage;
  prefixPage(prefix: Uint8Array, cursor: RangeCursor | null, limit: string): RangePage;
  reversePage(cursor: ReverseCursor | null, start: Uint8Array, limit: string): ReversePage;
  prefixReversePage(prefix: Uint8Array, cursor: ReverseCursor | null, limit: string): ReversePage;
  proveKey(key: Uint8Array): NativeKeyProof;
  proveKeys(keys: Uint8Array[]): NativeMultiKeyProof;
  proveRange(start: Uint8Array, end: Uint8Array | null): NativeRangeProof;
  provePrefix(prefix: Uint8Array): NativeRangeProof;
  proveRangePage(cursor: RangeCursor | null, end: Uint8Array | null, limit: string): NativeProvedRangePage;
  stats(): NativeMaintenanceSummary;
  export(): SnapshotBundle;
  read(): NativeReadSession;
}
interface NativeReadSession {
  get(key: Uint8Array): Uint8Array | null;
  scanRangePages(
    start: Uint8Array,
    end: Uint8Array | null,
    visit: (page: NativePackedReadPage) => number,
  ): { visited: string; stopped: boolean };
}
interface NativePackedReadPage { bytes: Uint8Array; recordCount: number; terminal: boolean; }
interface NativeKeyProof {
  verify(): KeyProofVerification;
}

function ownedSnapshotBundle(bundle: SnapshotBundle): SnapshotBundle {
  const config = bundle.tree.config;
  return {
    formatVersion: bundle.formatVersion,
    tree: {
      root: bundle.tree.root == null ? bundle.tree.root : ownedBytes(bundle.tree.root),
      config: config == null ? config : {
        ...config,
        encoding: { ...config.encoding },
        formatBytes: config.formatBytes == null ? config.formatBytes : ownedBytes(config.formatBytes),
      },
    },
    nodes: bundle.nodes.map((node) => ({
      cid: ownedBytes(node.cid),
      bytes: ownedBytes(node.bytes),
    })),
  };
}
interface NativeMultiKeyProof { verify(): MultiKeyProofVerification; }
interface NativeRangeProof { verify(): RangeProofVerification; }
interface NativeProvedRangePage {
  page(): RangePage;
  verify(): RangePageProofVerification;
}

export interface MaintenanceSummary { itemCount: bigint; byteCount: bigint; }

function maintenance(value: NativeMaintenanceSummary): MaintenanceSummary {
  return { itemCount: BigInt(value.itemCount), byteCount: BigInt(value.byteCount) };
}

function catalogVerification(value: NativeCatalogVerification): CatalogVerification {
  return {
    head: value.head,
    versionCount: BigInt(value.versionCount),
    reachableNodes: BigInt(value.reachableNodes),
    reachableBytes: BigInt(value.reachableBytes),
  };
}

export function gcPlan(value: NativeGcPlan): GcPlan {
  return {
    reachability: {
      liveCids: value.reachability.liveCids,
      liveNodes: BigInt(value.reachability.liveNodes),
      liveBytes: BigInt(value.reachability.liveBytes),
      leafNodes: BigInt(value.reachability.leafNodes),
      internalNodes: BigInt(value.reachability.internalNodes),
    },
    candidateNodes: BigInt(value.candidateNodes),
    reclaimableCids: value.reclaimableCids,
    reclaimableNodes: BigInt(value.reclaimableNodes),
    reclaimableBytes: BigInt(value.reclaimableBytes),
    missingCandidates: BigInt(value.missingCandidates),
  };
}

function retention(value: NativeNamedRootRetention): NamedRootRetention {
  return {
    kind: value.kind,
    names: value.names,
    prefix: value.prefix,
    count: value.count == null ? undefined : BigInt(value.count),
    minUpdatedAtMillis: value.minUpdatedAtMillis == null ? undefined : BigInt(value.minUpdatedAtMillis),
  };
}

function mapVersion(value: NativeMapVersion): MapVersion {
  return {
    id: value.id,
    tree: value.tree,
    createdAtMillis: value.createdAtMillis == null ? undefined : BigInt(value.createdAtMillis),
    isHead: value.isHead,
  };
}

function mapUpdate(value: NativeMapUpdate): MapUpdate {
  return {
    kind: value.kind,
    previous: value.previous,
    current: value.current == null ? undefined : mapVersion(value.current),
  };
}

function ownedMutations(mutations: readonly MapMutation[]): MapMutation[] {
  return mutations.map((mutation) => ({
    kind: mutation.kind,
    key: ownedBytes(mutation.key),
    value: mutation.value == null ? undefined : ownedBytes(mutation.value),
  }));
}

function ownedEntries(entries: readonly MapEntry[]): MapEntry[] {
  return entries.map((entry) => ({ key: ownedBytes(entry.key), value: ownedBytes(entry.value) }));
}

function versionedBatchResult(value: NativeVersionedMapBatchResult): VersionedMapBatchResult {
  return {
    version: mapVersion(value.version),
    stats: {
      inputMutations: BigInt(value.stats.inputMutations),
      effectiveMutations: BigInt(value.stats.effectiveMutations),
      preprocessInputSorted: value.stats.preprocessInputSorted,
      affectedLeaves: BigInt(value.stats.affectedLeaves),
      changedLeaves: BigInt(value.stats.changedLeaves),
      sparseLeafApplies: BigInt(value.stats.sparseLeafApplies),
      writtenNodes: BigInt(value.stats.writtenNodes),
      writtenBytes: BigInt(value.stats.writtenBytes),
      usedAppendFastPath: value.stats.usedAppendFastPath,
      usedBatchedRoute: value.stats.usedBatchedRoute,
      usedCoalescedRebuild: value.stats.usedCoalescedRebuild,
      usedDeferredRebalancing: value.stats.usedDeferredRebalancing,
      usedBottomUpRebuild: value.stats.usedBottomUpRebuild,
      cacheWrittenNodes: value.stats.cacheWrittenNodes,
    },
  };
}

function checkedPageLimit(limit: bigint): string {
  if (limit < 0n || limit > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError("page limit must be an unsigned 64-bit integer");
  }
  return limit.toString();
}

function checkedU64(value: bigint, name: string): string {
  if (value < 0n || value > 0xffff_ffff_ffff_ffffn) {
    throw new RangeError(`${name} must be an unsigned 64-bit integer`);
  }
  return value.toString();
}

function ownedRangeCursor(cursor: RangeCursor | undefined): RangeCursor | null {
  return cursor == null ? null : {
    afterKey: cursor.afterKey == null ? undefined : ownedBytes(cursor.afterKey),
  };
}

function ownedReverseCursor(cursor: ReverseCursor | undefined): ReverseCursor | null {
  return cursor == null ? null : {
    beforeKey: cursor.beforeKey == null ? undefined : ownedBytes(cursor.beforeKey),
  };
}

export class VersionedMap implements Disposable {
  #native?: NativeVersionedMap;

  constructor(native: NativeVersionedMap) {
    this.#native = native;
  }

  #open(): NativeVersionedMap {
    if (this.#native == null) throw new Error("versioned map is closed");
    return this.#native;
  }

  id(): Uint8Array { return this.#open().id(); }

  isInitialized(signal?: AbortSignal): Promise<boolean> {
    const native = this.#open();
    return nativePromise(signal, () => native.isInitialized());
  }

  initialize(signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open();
    return nativePromise(signal, () => mapVersion(native.initialize()));
  }

  initializeSorted(entries: readonly MapEntry[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); const owned = ownedEntries(entries);
    return nativePromise(signal, () => mapUpdate(native.initializeSorted(owned)));
  }

  head(signal?: AbortSignal): Promise<MapVersion | undefined> {
    const native = this.#open();
    return nativePromise(signal, () => {
      const value = native.head();
      return value == null ? undefined : mapVersion(value);
    });
  }

  headId(signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open();
    return nativePromise(signal, () => native.headId() ?? undefined);
  }

  version(id: Uint8Array, signal?: AbortSignal): Promise<MapVersion | undefined> {
    const native = this.#open(); id = ownedBytes(id);
    return nativePromise(signal, () => {
      const value = native.version(id);
      return value == null ? undefined : mapVersion(value);
    });
  }

  versions(signal?: AbortSignal): Promise<MapVersion[]> {
    const native = this.#open();
    return nativePromise(signal, () => native.versions().map(mapVersion));
  }

  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open();
    key = ownedBytes(key);
    return nativePromise(signal, () => native.get(key) ?? undefined);
  }

  containsKey(key: Uint8Array, signal?: AbortSignal): Promise<boolean> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.containsKey(key));
  }

  getMany(keys: readonly Uint8Array[], signal?: AbortSignal): Promise<Array<Uint8Array | undefined>> {
    const native = this.#open(); const owned = keys.map(ownedBytes);
    return nativePromise(signal, () => native.getMany(owned).map((value) => value ?? undefined));
  }

  getAt(id: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); id = ownedBytes(id); key = ownedBytes(key);
    return nativePromise(signal, () => native.getAt(id, key) ?? undefined);
  }

  getManyAt(id: Uint8Array, keys: readonly Uint8Array[], signal?: AbortSignal): Promise<Array<Uint8Array | undefined>> {
    const native = this.#open(); id = ownedBytes(id); const owned = keys.map(ownedBytes);
    return nativePromise(signal, () => native.getManyAt(id, owned).map((value) => value ?? undefined));
  }

  range(start: Uint8Array = new Uint8Array(), end?: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); start = ownedBytes(start); const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.range(start, ownedEnd));
  }

  prefix(prefix: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); prefix = ownedBytes(prefix);
    return nativePromise(signal, () => native.prefix(prefix));
  }

  rangeAt(id: Uint8Array, start: Uint8Array = new Uint8Array(), end?: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); id = ownedBytes(id); start = ownedBytes(start);
    const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.rangeAt(id, start, ownedEnd));
  }

  prefixAt(id: Uint8Array, prefix: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); id = ownedBytes(id); prefix = ownedBytes(prefix);
    return nativePromise(signal, () => native.prefixAt(id, prefix));
  }

  rangePage(cursor?: RangeCursor, end?: Uint8Array, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); const ownedCursor = ownedRangeCursor(cursor);
    const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.rangePage(ownedCursor, ownedEnd, checkedPageLimit(limit)));
  }

  prefixPage(prefix: Uint8Array, cursor?: RangeCursor, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); prefix = ownedBytes(prefix); const ownedCursor = ownedRangeCursor(cursor);
    return nativePromise(signal, () => native.prefixPage(prefix, ownedCursor, checkedPageLimit(limit)));
  }

  rangePageAt(id: Uint8Array, cursor?: RangeCursor, end?: Uint8Array, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); id = ownedBytes(id); const ownedCursor = ownedRangeCursor(cursor);
    const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.rangePageAt(id, ownedCursor, ownedEnd, checkedPageLimit(limit)));
  }

  prefixPageAt(id: Uint8Array, prefix: Uint8Array, cursor?: RangeCursor, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); id = ownedBytes(id); prefix = ownedBytes(prefix);
    const ownedCursor = ownedRangeCursor(cursor);
    return nativePromise(signal, () => native.prefixPageAt(id, prefix, ownedCursor, checkedPageLimit(limit)));
  }

  diff(base: Uint8Array, target: Uint8Array, signal?: AbortSignal): Promise<MapDiff[]> {
    const native = this.#open(); base = ownedBytes(base); target = ownedBytes(target);
    return nativePromise(signal, () => native.diff(base, target));
  }

  changesSince(base: Uint8Array, signal?: AbortSignal): Promise<MapDiff[]> {
    const native = this.#open(); base = ownedBytes(base);
    return nativePromise(signal, () => native.changesSince(base));
  }

  rollbackTo(id: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); id = ownedBytes(id);
    return nativePromise(signal, () => mapVersion(native.rollbackTo(id)));
  }

  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open();
    key = ownedBytes(key);
    value = ownedBytes(value);
    return nativePromise(signal, () => mapVersion(native.put(key, value)));
  }

  apply(mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapVersion(native.apply(owned)));
  }

  append(mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapVersion(native.append(owned)));
  }
  parallelApply(mutations: readonly MapMutation[], config: VersionedParallelConfig, signal?: AbortSignal): Promise<VersionedMapBatchResult> {
    const native = this.#open(); const owned = ownedMutations(mutations);
    const ownedConfig = {
      maxThreads: checkedU64(config.maxThreads, "maxThreads"),
      parallelismThreshold: checkedU64(config.parallelismThreshold, "parallelismThreshold"),
    };
    return nativePromise(signal, () => versionedBatchResult(native.parallelApply(owned, ownedConfig)));
  }
  rebuildSortedIf(expected: Uint8Array | undefined, entries: readonly MapEntry[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedEntries(entries);
    return nativePromise(signal, () => mapUpdate(native.rebuildSortedIf(ownedExpected, owned)));
  }
  rebuildFromEntriesIf(expected: Uint8Array | undefined, entries: readonly MapEntry[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedEntries(entries);
    return nativePromise(signal, () => mapUpdate(native.rebuildFromEntriesIf(ownedExpected, owned)));
  }
  rebuildFromIterIf(expected: Uint8Array | undefined, entries: readonly MapEntry[], signal?: AbortSignal): Promise<MapUpdate> {
    return this.rebuildFromEntriesIf(expected, entries, signal);
  }

  applyAtMillis(mutations: readonly MapMutation[], timestampMillis: bigint, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); const owned = ownedMutations(mutations);
    const timestamp = checkedU64(timestampMillis, "timestampMillis");
    return nativePromise(signal, () => mapVersion(native.applyAtMillis(owned, timestamp)));
  }

  applyIf(expected: Uint8Array | undefined, mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapUpdate(native.applyIf(ownedExpected, owned)));
  }

  applyIfAtMillis(expected: Uint8Array | undefined, mutations: readonly MapMutation[], timestampMillis: bigint, signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedMutations(mutations); const timestamp = checkedU64(timestampMillis, "timestampMillis");
    return nativePromise(signal, () => mapUpdate(native.applyIfAtMillis(ownedExpected, owned, timestamp)));
  }

  putIf(expected: Uint8Array | undefined, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? null : ownedBytes(expected);
    key = ownedBytes(key); value = ownedBytes(value);
    return nativePromise(signal, () => mapUpdate(native.putIf(ownedExpected, key, value)));
  }

  deleteIf(expected: Uint8Array | undefined, key: Uint8Array, signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? null : ownedBytes(expected); key = ownedBytes(key);
    return nativePromise(signal, () => mapUpdate(native.deleteIf(ownedExpected, key)));
  }

  delete(key: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open();
    key = ownedBytes(key);
    return nativePromise(signal, () => mapVersion(native.delete(key)));
  }

  snapshot(signal?: AbortSignal): Promise<MapSnapshot | undefined> {
    const native = this.#open();
    return nativePromise(signal, () => {
      const value = native.snapshot();
      return value == null ? undefined : new MapSnapshot(value);
    });
  }

  snapshotAt(id: Uint8Array, signal?: AbortSignal): Promise<MapSnapshot | undefined> {
    const native = this.#open(); id = ownedBytes(id);
    return nativePromise(signal, () => {
      const value = native.snapshotAt(id);
      return value == null ? undefined : new MapSnapshot(value);
    });
  }

  compare(base: Uint8Array, target: Uint8Array): MapComparison {
    const native = this.#open();
    return new MapComparison(native.compare(ownedBytes(base), ownedBytes(target)));
  }

  compareToHead(base: Uint8Array): MapComparison {
    return new MapComparison(this.#open().compareToHead(ownedBytes(base)));
  }

  subscribe(): MapSubscription { return new MapSubscription(this.#open().subscribe()); }

  subscribeFrom(lastSeen?: Uint8Array): MapSubscription {
    return new MapSubscription(this.#open().subscribeFrom(
      lastSeen == null ? null : ownedBytes(lastSeen),
    ));
  }

  prepareMerge(base: Uint8Array, candidate: Uint8Array): MapMerge {
    return new MapMerge(this.#open().prepareMerge(ownedBytes(base), ownedBytes(candidate)));
  }

  backup(signal?: AbortSignal): Promise<Uint8Array> {
    const native = this.#open();
    return nativePromise(signal, () => native.backup());
  }

  restoreBackup(bytes: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); bytes = ownedBytes(bytes);
    return nativePromise(signal, () => mapVersion(native.restoreBackup(bytes)));
  }

  importAsHead(bundle: SnapshotBundle, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); const owned = ownedSnapshotBundle(bundle);
    return nativePromise(signal, () => mapVersion(native.importAsHead(owned)));
  }

  importAsHeadAtMillis(
    bundle: SnapshotBundle,
    timestampMillis: bigint,
    signal?: AbortSignal,
  ): Promise<MapVersion> {
    const native = this.#open(); const owned = ownedSnapshotBundle(bundle);
    const timestamp = checkedU64(timestampMillis, "timestampMillis");
    return nativePromise(signal, () => mapVersion(native.importAsHeadAtMillis(owned, timestamp)));
  }

  keepLast(count: number, signal?: AbortSignal): Promise<VersionPrune> {
    if (!Number.isSafeInteger(count) || count < 0 || count > 0xffff_ffff) {
      return Promise.reject(new RangeError("keepLast count must be a non-negative uint32"));
    }
    const native = this.#open();
    return nativePromise(signal, () => native.keepLast(count));
  }

  pruneVersions(keepLatest: bigint, signal?: AbortSignal): Promise<VersionPrune> {
    const native = this.#open(); const count = checkedU64(keepLatest, "keepLatest");
    return nativePromise(signal, () => native.pruneVersions(count));
  }

  keepForAt(nowMillis: bigint, maxAgeMillis: bigint, signal?: AbortSignal): Promise<VersionPrune> {
    const native = this.#open(); const now = checkedU64(nowMillis, "nowMillis");
    const age = checkedU64(maxAgeMillis, "maxAgeMillis");
    return nativePromise(signal, () => native.keepForAt(now, age));
  }

  keepFor(maxAgeMillis: bigint, signal?: AbortSignal): Promise<VersionPrune> {
    const native = this.#open(); const age = checkedU64(maxAgeMillis, "maxAgeMillis");
    return nativePromise(signal, () => native.keepFor(age));
  }

  keepVersions(ids: readonly Uint8Array[], signal?: AbortSignal): Promise<VersionPrune> {
    const native = this.#open(); const owned = ids.map(ownedBytes);
    return nativePromise(signal, () => native.keepVersions(owned));
  }

  retentionPolicy(): NamedRootRetention { return retention(this.#open().retentionPolicy()); }

  verifyCatalog(signal?: AbortSignal): Promise<CatalogVerification> {
    const native = this.#open();
    return nativePromise(signal, () => catalogVerification(native.verifyCatalog()));
  }

  planGc(signal?: AbortSignal): Promise<GcPlan> {
    const native = this.#open();
    return nativePromise(signal, () => gcPlan(native.planGc()));
  }

  sweepGc(signal?: AbortSignal): Promise<GcSweep> {
    const native = this.#open();
    return nativePromise(signal, () => {
      const value = native.sweepGc();
      return { plan: gcPlan(value.plan), deletedNodes: BigInt(value.deletedNodes), deletedBytes: BigInt(value.deletedBytes) };
    });
  }

  close(): void {
    this.#native = undefined;
  }

  [Symbol.dispose](): void {
    this.close();
  }
}

export class VersionedTransaction implements Disposable {
  #native?: NativeVersionedTransaction;
  constructor(native: NativeVersionedTransaction) { this.#native = native; }
  #open(): NativeVersionedTransaction {
    if (this.#native == null) throw new Error("versioned transaction is completed");
    return this.#native;
  }
  head(mapId: Uint8Array, signal?: AbortSignal): Promise<MapVersion | undefined> {
    const native = this.#open(); mapId = ownedBytes(mapId);
    return nativePromise(signal, () => {
      const value = native.head(mapId); return value == null ? undefined : mapVersion(value);
    });
  }
  get(mapId: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); mapId = ownedBytes(mapId); key = ownedBytes(key);
    return nativePromise(signal, () => native.get(mapId, key) ?? undefined);
  }
  apply(mapId: Uint8Array, mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); mapId = ownedBytes(mapId); const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapVersion(native.apply(mapId, owned)));
  }
  applyIf(mapId: Uint8Array, expected: Uint8Array | undefined, mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); mapId = ownedBytes(mapId);
    const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapUpdate(native.applyIf(mapId, ownedExpected, owned)));
  }
  put(mapId: Uint8Array, key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); mapId = ownedBytes(mapId); key = ownedBytes(key); value = ownedBytes(value);
    return nativePromise(signal, () => mapVersion(native.put(mapId, key, value)));
  }
  delete(mapId: Uint8Array, key: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); mapId = ownedBytes(mapId); key = ownedBytes(key);
    return nativePromise(signal, () => mapVersion(native.delete(mapId, key)));
  }
  commit(signal?: AbortSignal): Promise<VersionedTransactionCommit> {
    const native = this.#open(); this.#native = undefined;
    return nativePromise(signal, () => {
      const value = native.commit();
      return {
        applied: value.applied,
        versions: value.versions.map(mapVersion),
        conflictMapId: value.conflictMapId,
        conflictCurrent: value.conflictCurrent == null ? undefined : mapVersion(value.conflictCurrent),
      };
    });
  }
  rollback(signal?: AbortSignal): Promise<void> {
    const native = this.#open(); this.#native = undefined;
    return nativePromise(signal, () => native.rollback());
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class MapComparison implements Disposable {
  #native?: NativeMapComparison;
  constructor(native: NativeMapComparison) { this.#native = native; }
  #open(): NativeMapComparison {
    if (this.#native == null) throw new Error("map comparison is closed");
    return this.#native;
  }
  base(): MapVersion { return mapVersion(this.#open().base()); }
  target(): MapVersion { return mapVersion(this.#open().target()); }
  diff(signal?: AbortSignal): Promise<MapDiff[]> {
    const native = this.#open();
    return nativePromise(signal, () => native.diff());
  }
  diffPage(cursor?: RangeCursor, end?: Uint8Array, limit: bigint = 256n, signal?: AbortSignal): Promise<DiffPage> {
    const native = this.#open();
    const ownedCursor = ownedRangeCursor(cursor);
    const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.diffPage(ownedCursor, ownedEnd, checkedPageLimit(limit)));
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class MapSubscription implements Disposable {
  #native?: NativeMapSubscription;
  constructor(native: NativeMapSubscription) { this.#native = native; }
  #open(): NativeMapSubscription {
    if (this.#native == null) throw new Error("map subscription is closed");
    return this.#native;
  }
  lastSeen(): Uint8Array | undefined { return this.#open().lastSeen() ?? undefined; }
  poll(signal?: AbortSignal): Promise<MapChangeEvent | undefined> {
    const native = this.#open();
    return nativePromise(signal, () => {
      const event = native.poll();
      return event == null ? undefined : {
        previous: event.previous,
        current: mapVersion(event.current),
        diffs: event.diffs,
      };
    });
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class MapMerge implements Disposable {
  #native?: NativeMapMerge;
  constructor(native: NativeMapMerge) { this.#native = native; }
  #open(): NativeMapMerge { if (this.#native == null) throw new Error("map merge is closed"); return this.#native; }
  base(): MapVersion { return mapVersion(this.#open().base()); }
  head(): MapVersion { return mapVersion(this.#open().head()); }
  candidate(): MapVersion { return mapVersion(this.#open().candidate()); }
  merge(resolver?: string, signal?: AbortSignal): Promise<unknown> {
    const native = this.#open(); return nativePromise(signal, () => native.merge(resolver ?? null));
  }
  conflictPage(cursor?: RangeCursor, limit: bigint = 256n, signal?: AbortSignal): Promise<ConflictPage> {
    const native = this.#open(); const ownedCursor = ownedRangeCursor(cursor);
    return nativePromise(signal, () => native.conflictPage(ownedCursor, checkedPageLimit(limit)));
  }
  publish(resolver?: string, signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open(); return nativePromise(signal, () => mapUpdate(native.publish(resolver ?? null)));
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class MapSnapshot implements Disposable {
  #native?: NativeMapSnapshot;
  constructor(native: NativeMapSnapshot) { this.#native = native; }
  #open(): NativeMapSnapshot {
    if (this.#native == null) throw new Error("map snapshot is closed");
    return this.#native;
  }
  id(): Uint8Array { return this.#open().id(); }
  version(): MapVersion { return mapVersion(this.#open().version()); }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.get(key) ?? undefined);
  }
  getMany(keys: readonly Uint8Array[], signal?: AbortSignal): Promise<Array<Uint8Array | undefined>> {
    const native = this.#open(); const owned = keys.map(ownedBytes);
    return nativePromise(signal, () => native.getMany(owned).map((value) => value ?? undefined));
  }
  containsKey(key: Uint8Array, signal?: AbortSignal): Promise<boolean> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.containsKey(key));
  }
  firstEntry(signal?: AbortSignal): Promise<MapEntry | undefined> {
    const native = this.#open(); return nativePromise(signal, () => native.firstEntry() ?? undefined);
  }
  lastEntry(signal?: AbortSignal): Promise<MapEntry | undefined> {
    const native = this.#open(); return nativePromise(signal, () => native.lastEntry() ?? undefined);
  }
  lowerBound(key: Uint8Array, signal?: AbortSignal): Promise<MapEntry | undefined> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.lowerBound(key) ?? undefined);
  }
  upperBound(key: Uint8Array, signal?: AbortSignal): Promise<MapEntry | undefined> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.upperBound(key) ?? undefined);
  }
  range(start: Uint8Array = new Uint8Array(), end?: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); start = ownedBytes(start); const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.range(start, ownedEnd));
  }
  prefix(prefix: Uint8Array, signal?: AbortSignal): Promise<MapEntry[]> {
    const native = this.#open(); prefix = ownedBytes(prefix);
    return nativePromise(signal, () => native.prefix(prefix));
  }
  rangePage(cursor?: RangeCursor, end?: Uint8Array, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); const ownedCursor = ownedRangeCursor(cursor); const ownedEnd = end == null ? null : ownedBytes(end);
    return nativePromise(signal, () => native.rangePage(ownedCursor, ownedEnd, checkedPageLimit(limit)));
  }
  prefixPage(prefix: Uint8Array, cursor?: RangeCursor, limit: bigint = 256n, signal?: AbortSignal): Promise<RangePage> {
    const native = this.#open(); prefix = ownedBytes(prefix); const ownedCursor = ownedRangeCursor(cursor);
    return nativePromise(signal, () => native.prefixPage(prefix, ownedCursor, checkedPageLimit(limit)));
  }
  reversePage(cursor?: ReverseCursor, start: Uint8Array = new Uint8Array(), limit: bigint = 256n, signal?: AbortSignal): Promise<ReversePage> {
    const native = this.#open(); const ownedCursor = ownedReverseCursor(cursor); start = ownedBytes(start);
    return nativePromise(signal, () => native.reversePage(ownedCursor, start, checkedPageLimit(limit)));
  }
  prefixReversePage(prefix: Uint8Array, cursor?: ReverseCursor, limit: bigint = 256n, signal?: AbortSignal): Promise<ReversePage> {
    const native = this.#open(); prefix = ownedBytes(prefix); const ownedCursor = ownedReverseCursor(cursor);
    return nativePromise(signal, () => native.prefixReversePage(prefix, ownedCursor, checkedPageLimit(limit)));
  }
  proveKey(key: Uint8Array): KeyProof {
    const native = this.#open();
    return new KeyProof(native.proveKey(ownedBytes(key)));
  }
  proveKeys(keys: readonly Uint8Array[]): MultiKeyProof {
    const native = this.#open();
    return new MultiKeyProof(native.proveKeys(keys.map(ownedBytes)));
  }
  proveRange(start: Uint8Array = new Uint8Array(), end?: Uint8Array): RangeProof {
    const native = this.#open(); start = ownedBytes(start); const ownedEnd = end == null ? null : ownedBytes(end);
    return new RangeProof(native.proveRange(start, ownedEnd));
  }
  provePrefix(prefix: Uint8Array): RangeProof {
    const native = this.#open();
    return new RangeProof(native.provePrefix(ownedBytes(prefix)));
  }
  proveRangePage(cursor?: RangeCursor, end?: Uint8Array, limit: bigint = 256n): ProvedRangePage {
    const native = this.#open(); const ownedCursor = ownedRangeCursor(cursor); const ownedEnd = end == null ? null : ownedBytes(end);
    return new ProvedRangePage(native.proveRangePage(ownedCursor, ownedEnd, checkedPageLimit(limit)));
  }
  stats(): MaintenanceSummary { return maintenance(this.#open().stats()); }
  export(signal?: AbortSignal): Promise<SnapshotBundle> {
    const native = this.#open();
    return nativePromise(signal, () => native.export());
  }
  read(): ReadSession { return new ReadSession(this.#open().read()); }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ReadSession implements Disposable {
  #native?: NativeReadSession;
  constructor(native: NativeReadSession) { this.#native = native; }
  get(key: Uint8Array): Uint8Array | undefined {
    if (this.#native == null) throw new Error("read session is closed");
    return this.#native.get(ownedBytes(key)) ?? undefined;
  }
  getAsync(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    if (this.#native == null) throw new Error("read session is closed");
    const native = this.#native; const owned = ownedBytes(key);
    return nativePromise(signal, () => native.get(owned) ?? undefined);
  }
  scanRangeView(
    start: Uint8Array,
    end: Uint8Array | undefined,
    visit: (entry: MapEntryView) => boolean,
  ): ReadScanOutcome {
    if (this.#native == null) throw new Error("read session is closed");
    if (typeof visit !== "function") throw new TypeError("scan visitor must be a function");
    let previousPageKey: Uint8Array | undefined;
    const outcome = this.#native.scanRangePages(
      ownedBytes(start), end == null ? null : ownedBytes(end), (nativePage) => {
        const page = decodePackedReadPage(nativePage);
        const scope: ViewScope = { alive: true };
        let consumed = 0;
        let previousView: Uint8Array | undefined;
        try {
          for (const rawEntry of page.entries) {
            if (previousView != null && compareBytes(previousView, rawEntry.key) >= 0) {
              throw new Error("packed scan page keys are not strictly ordered");
            }
            if (previousView == null && previousPageKey != null && compareBytes(previousPageKey, rawEntry.key) >= 0) {
              throw new Error("packed scan page keys are not strictly ordered");
            }
            const key = scopedBytes(rawEntry.key, scope);
            const value = scopedBytes(rawEntry.value, scope);
            previousView = rawEntry.key;
            consumed += 1;
            if (!visit({ key, value })) return consumed;
          }
          if (!nativePage.terminal && previousView == null) {
            throw new Error("non-terminal packed scan page made no progress");
          }
          previousPageKey = previousView == null ? previousPageKey : ownedBytes(previousView);
          return consumed;
        } finally {
          scope.alive = false;
        }
      },
    );
    return { visited: BigInt(outcome.visited), stopped: outcome.stopped };
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

function decodePackedReadPage(page: NativePackedReadPage): {
  entries: Array<{ key: Uint8Array; value: Uint8Array }>;
} {
  const bytes = page.bytes;
  if (bytes.byteLength < 28) throw new Error("packed scan page header is truncated");
  const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
  if (view.getUint32(0, false) !== 0x50525047 || view.getUint16(4, true) !== 1 || view.getUint16(6, true) !== 1) {
    throw new Error("invalid packed scan page header");
  }
  const flags = view.getUint32(8, true);
  const count = view.getUint32(12, true);
  const tableBytes = view.getUint32(16, true);
  const arenaBytes = view.getBigUint64(20, true);
  if (count !== page.recordCount || tableBytes < count * 16 || tableBytes % 16 !== 0 || Boolean(flags & 1) !== page.terminal) {
    throw new Error("inconsistent packed scan page metadata");
  }
  const arenaStart = 28 + tableBytes;
  if (arenaBytes > BigInt(Number.MAX_SAFE_INTEGER) || BigInt(arenaStart) + arenaBytes !== BigInt(bytes.byteLength)) {
    throw new Error("invalid packed scan page bounds");
  }
  const arenaLength = Number(arenaBytes);
  const entries: Array<{ key: Uint8Array; value: Uint8Array }> = [];
  for (let index = 0; index < count; index += 1) {
    const base = 28 + index * 16;
    const keyOffset = view.getUint32(base, true);
    const keyLength = view.getUint32(base + 4, true);
    const valueOffset = view.getUint32(base + 8, true);
    const valueLength = view.getUint32(base + 12, true);
    if (keyOffset > arenaLength || keyLength > arenaLength - keyOffset || valueOffset > arenaLength || valueLength > arenaLength - valueOffset) {
      throw new Error("packed scan field exceeds arena");
    }
    entries.push({
      key: new Uint8Array(bytes.buffer, bytes.byteOffset + arenaStart + keyOffset, keyLength),
      value: new Uint8Array(bytes.buffer, bytes.byteOffset + arenaStart + valueOffset, valueLength),
    });
  }
  return { entries };
}

function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const shared = Math.min(left.byteLength, right.byteLength);
  for (let index = 0; index < shared; index += 1) {
    if (left[index] !== right[index]) return left[index]! - right[index]!;
  }
  return left.byteLength - right.byteLength;
}

export class KeyProof implements Disposable {
  #native?: NativeKeyProof;
  constructor(native: NativeKeyProof) { this.#native = native; }
  verify(): KeyProofVerification {
    if (this.#native == null) throw new Error("key proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class MultiKeyProof implements Disposable {
  #native?: NativeMultiKeyProof;
  constructor(native: NativeMultiKeyProof) { this.#native = native; }
  verify(): MultiKeyProofVerification {
    if (this.#native == null) throw new Error("multi-key proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class RangeProof implements Disposable {
  #native?: NativeRangeProof;
  constructor(native: NativeRangeProof) { this.#native = native; }
  verify(): RangeProofVerification {
    if (this.#native == null) throw new Error("range proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProvedRangePage implements Disposable {
  #native?: NativeProvedRangePage;
  constructor(native: NativeProvedRangePage) { this.#native = native; }
  page(): RangePage {
    if (this.#native == null) throw new Error("proved range page is closed");
    return this.#native.page();
  }
  verify(): RangePageProofVerification {
    if (this.#native == null) throw new Error("proved range page is closed");
    return this.#native.verify();
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}
