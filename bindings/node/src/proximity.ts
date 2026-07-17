import { nativePromise, ownedBytes } from "./packed.ts";

export interface ProximityRecord {
  key: Uint8Array;
  vector: Float32Array;
  value?: Uint8Array;
}

export interface SearchRequest {
  vector: Float32Array;
  topK: number;
  policy: "exact";
  signal?: AbortSignal;
}

export function exactSearch(vector: Float32Array, topK: number, signal?: AbortSignal): SearchRequest {
  return { vector: new Float32Array(vector), topK, policy: "exact", signal };
}

export interface Neighbor {
  key: Uint8Array;
  value: Uint8Array;
  distance: number;
}

export interface SearchResult {
  neighbors: Neighbor[];
  completion: string;
  backend: string;
}

export interface ProximityConfig {
  dimensions: number;
  metric: "l2_squared" | "cosine" | "inner_product";
  logChunkSize: number;
  levelHashSeed: bigint;
  minPageBytes: number;
  targetPageBytes: number;
  maxPageBytes: number;
  overflowHashSeed: bigint;
  inlineThresholdBytes: number;
  scalarQuantizationGroupSize?: number;
}

export interface ProximityMutation {
  key: Uint8Array;
  vector?: Float32Array;
  value?: Uint8Array;
}

export interface ProximityMutationStats {
  directoryEntriesScanned: bigint;
  directoryNodesRead: bigint;
  directoryNodesRebuilt: bigint;
  directoryNodesWritten: bigint;
  directoryNodesReused: bigint;
  directoryLevelsRebuilt: bigint;
  directoryRightEdgeRebuilt: boolean;
  recordsRebuilt: bigint;
  nodesRead: bigint;
  nodesWritten: bigint;
  nodesReused: bigint;
  distanceEvaluations: bigint;
  fullProximityRebuild: boolean;
}

export interface ProximityVerification {
  recordCount: bigint;
  proximityNodeCount: bigint;
  externalVectorCount: bigint;
  quantizedNodeCount: bigint;
  scalarQuantizerCount: bigint;
  overflowPageCount: bigint;
  overflowDirectoryCount: bigint;
  maximumLevel: number;
  maximumNodeBytes: bigint;
  distanceChecks: bigint;
}

interface NativeProximityConfig {
  dimensions: number; metric: ProximityConfig["metric"]; logChunkSize: number;
  levelHashSeed: string; minPageBytes: number; targetPageBytes: number;
  maxPageBytes: number; overflowHashSeed: string; inlineThresholdBytes: number;
  scalarQuantizationGroupSize?: number;
}
interface NativeProximityMutationStats {
  directoryEntriesScanned: string; directoryNodesRead: string; directoryNodesRebuilt: string;
  directoryNodesWritten: string; directoryNodesReused: string; directoryLevelsRebuilt: string;
  directoryRightEdgeRebuilt: boolean;
  recordsRebuilt: string; nodesRead: string; nodesWritten: string; nodesReused: string;
  distanceEvaluations: string; fullProximityRebuild: boolean;
}
interface NativeProximityVerification {
  recordCount: string; proximityNodeCount: string; externalVectorCount: string;
  quantizedNodeCount: string; scalarQuantizerCount: string; overflowPageCount: string;
  overflowDirectoryCount: string; maximumLevel: number; maximumNodeBytes: string;
  distanceChecks: string;
}
interface NativeProximityMutationResult {
  map(): NativeProximityMap;
  stats(): NativeProximityMutationStats;
}

interface NativeProximityMap {
  read(): NativeProximityReadSession;
  search(vector: Float32Array, topK: string): SearchResult;
  count(): string;
  config(): NativeProximityConfig;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  mutate(mutations: Array<{ key: Uint8Array; vector?: Float32Array; value?: Uint8Array }>): NativeProximityMutationResult;
  rebuild(mutations: Array<{ key: Uint8Array; vector?: Float32Array; value?: Uint8Array }>): NativeProximityMap;
  descriptor(): Uint8Array;
  verify(): NativeProximityVerification;
  proveMembership(key: Uint8Array): NativeProximityProof;
  proveSearch(vector: Float32Array, topK: string): NativeProximitySearchProof;
  proveStructure(): NativeProximityStructuralProof;
}
interface NativeProximityReadSession {
  search(vector: Float32Array, topK: string): SearchResult;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  fastHandle(): string;
}
interface NativeProximityProof { verify(expectedDescriptor?: Uint8Array): Uint8Array | null; }
interface NativeProximitySearchProof {
  verify(expectedDescriptor?: Uint8Array): {
    result: SearchResult;
    claim: string;
    terminalLowerBound?: number;
    replayedEvents: string;
  };
}
interface NativeProximityStructuralProof {
  verify(expectedDescriptor?: Uint8Array): {
    descriptor: Uint8Array;
    objectCount: string;
    summary: NativeProximityVerification;
  };
}

function ownMutations(mutations: ProximityMutation[]) {
  return mutations.map((mutation) => ({
    key: ownedBytes(mutation.key),
    vector: mutation.vector == null ? undefined : new Float32Array(mutation.vector),
    value: mutation.value == null ? undefined : ownedBytes(mutation.value),
  }));
}

function verification(value: NativeProximityVerification): ProximityVerification {
  return {
    recordCount: BigInt(value.recordCount),
    proximityNodeCount: BigInt(value.proximityNodeCount),
    externalVectorCount: BigInt(value.externalVectorCount),
    quantizedNodeCount: BigInt(value.quantizedNodeCount),
    scalarQuantizerCount: BigInt(value.scalarQuantizerCount),
    overflowPageCount: BigInt(value.overflowPageCount),
    overflowDirectoryCount: BigInt(value.overflowDirectoryCount),
    maximumLevel: value.maximumLevel,
    maximumNodeBytes: BigInt(value.maximumNodeBytes),
    distanceChecks: BigInt(value.distanceChecks),
  };
}

export class ProximityMap implements Disposable {
  #native?: NativeProximityMap;
  constructor(native: NativeProximityMap) { this.#native = native; }
  nativeHandle(): NativeProximityMap {
    if (this.#native == null) throw new Error("proximity map is closed");
    return this.#native;
  }
  read(): ProximityReadSession { return new ProximityReadSession(this.nativeHandle().read()); }
  search(request: SearchRequest): Promise<SearchResult> { return this.read().search(request); }
  get(key: Uint8Array): { vector: Float32Array; value: Uint8Array } | undefined {
    const record = this.nativeHandle().get(ownedBytes(key));
    return record == null ? undefined : { vector: new Float32Array(record.vector), value: record.value };
  }
  contains(key: Uint8Array): boolean { return this.nativeHandle().contains(ownedBytes(key)); }
  count(): bigint { return BigInt(this.nativeHandle().count()); }
  config(): ProximityConfig {
    const value = this.nativeHandle().config();
    return { ...value, levelHashSeed: BigInt(value.levelHashSeed), overflowHashSeed: BigInt(value.overflowHashSeed) };
  }
  mutate(mutations: ProximityMutation[]): { map: ProximityMap; stats: ProximityMutationStats } {
    const result = this.nativeHandle().mutate(ownMutations(mutations));
    const stats = result.stats();
    return {
      map: new ProximityMap(result.map()),
      stats: {
        directoryEntriesScanned: BigInt(stats.directoryEntriesScanned),
        directoryNodesRead: BigInt(stats.directoryNodesRead),
        directoryNodesRebuilt: BigInt(stats.directoryNodesRebuilt),
        directoryNodesWritten: BigInt(stats.directoryNodesWritten),
        directoryNodesReused: BigInt(stats.directoryNodesReused),
        directoryLevelsRebuilt: BigInt(stats.directoryLevelsRebuilt),
        directoryRightEdgeRebuilt: stats.directoryRightEdgeRebuilt,
        recordsRebuilt: BigInt(stats.recordsRebuilt), nodesRead: BigInt(stats.nodesRead),
        nodesWritten: BigInt(stats.nodesWritten), nodesReused: BigInt(stats.nodesReused),
        distanceEvaluations: BigInt(stats.distanceEvaluations),
        fullProximityRebuild: stats.fullProximityRebuild,
      },
    };
  }
  rebuild(mutations: ProximityMutation[]): ProximityMap {
    return new ProximityMap(this.nativeHandle().rebuild(ownMutations(mutations)));
  }
  descriptor(): Uint8Array { return this.nativeHandle().descriptor(); }
  verify(): ProximityVerification { return verification(this.nativeHandle().verify()); }
  proveMembership(key: Uint8Array): ProximityMembershipProof {
    return new ProximityMembershipProof(this.nativeHandle().proveMembership(ownedBytes(key)));
  }
  proveSearch(request: SearchRequest): ProximitySearchProof {
    return new ProximitySearchProof(
      this.nativeHandle().proveSearch(new Float32Array(request.vector), request.topK.toString()),
    );
  }
  proveStructure(): ProximityStructuralProof {
    return new ProximityStructuralProof(this.nativeHandle().proveStructure());
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximityStructuralProof implements Disposable {
  #native?: NativeProximityStructuralProof;
  constructor(native: NativeProximityStructuralProof) { this.#native = native; }
  verify(expectedDescriptor?: Uint8Array): {
    descriptor: Uint8Array; objectCount: bigint; summary: ProximityVerification;
  } {
    if (this.#native == null) throw new Error("proximity structural proof is closed");
    const value = this.#native.verify(
      expectedDescriptor == null ? undefined : ownedBytes(expectedDescriptor),
    );
    return { descriptor: value.descriptor, objectCount: BigInt(value.objectCount), summary: verification(value.summary) };
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximityMembershipProof implements Disposable {
  #native?: NativeProximityProof;
  constructor(native: NativeProximityProof) { this.#native = native; }
  verify(expectedDescriptor?: Uint8Array): { value?: Uint8Array } {
    if (this.#native == null) throw new Error("proximity proof is closed");
    const value = this.#native.verify(
      expectedDescriptor == null ? undefined : ownedBytes(expectedDescriptor));
    return { value: value ?? undefined };
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximitySearchProof implements Disposable {
  #native?: NativeProximitySearchProof;
  constructor(native: NativeProximitySearchProof) { this.#native = native; }
  verify(expectedDescriptor?: Uint8Array): {
    result: SearchResult;
    claim: string;
    terminalLowerBound?: number;
    replayedEvents: bigint;
  } {
    if (this.#native == null) throw new Error("proximity search proof is closed");
    const value = this.#native.verify(
      expectedDescriptor == null ? undefined : ownedBytes(expectedDescriptor));
    return {
      result: value.result,
      claim: value.claim,
      terminalLowerBound: value.terminalLowerBound,
      replayedEvents: BigInt(value.replayedEvents),
    };
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximityReadSession implements Disposable {
  #native?: NativeProximityReadSession;
  constructor(native: NativeProximityReadSession) { this.#native = native; }
  get(key: Uint8Array): { vector: Float32Array; value: Uint8Array } | undefined {
    if (this.#native == null) throw new Error("proximity session is closed");
    const record = this.#native.get(ownedBytes(key));
    return record == null ? undefined : {
      vector: new Float32Array(record.vector),
      value: record.value,
    };
  }
  contains(key: Uint8Array): boolean {
    if (this.#native == null) throw new Error("proximity session is closed");
    return this.#native.contains(ownedBytes(key));
  }
  search(request: SearchRequest): Promise<SearchResult> {
    if (this.#native == null) return Promise.reject(new Error("proximity session is closed"));
    const native = this.#native;
    const vector = new Float32Array(request.vector);
    return nativePromise(request.signal, () => native.search(vector, request.topK.toString()));
  }
  fastHandle(): bigint {
    if (this.#native == null) throw new Error("proximity session is closed");
    return BigInt(this.#native.fastHandle());
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export function ownProximityRecords(records: ProximityRecord[]): Array<{
  key: Uint8Array; vector: Float32Array; value: Uint8Array;
}> {
  return records.map((record) => ({
    key: ownedBytes(record.key),
    vector: new Float32Array(record.vector),
    value: ownedBytes(record.value ?? new Uint8Array()),
  }));
}
