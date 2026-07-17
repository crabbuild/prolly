import { nativePromise, ownedBytes } from "./packed.ts";

export interface ProximityRecord {
  key: Uint8Array;
  vector: Float32Array;
  value?: Uint8Array;
}

export interface SearchRequest {
  vector: Float32Array;
  topK: number;
  policy: "exact" | "fixed_budget" | "adaptive";
  adaptiveQuality?: "fast" | "balanced" | "high_recall";
  budget?: SearchBudget;
  filter?: SearchFilter;
  kernel?: "scalar_deterministic" | "simd_deterministic" | "auto_deterministic";
  backend?: "native" | "product_quantized" | "hnsw" | "composite" | "auto";
  hnswEfSearch?: number;
  pqRerankMultiplier?: number;
  signal?: AbortSignal;
}

export interface SearchBudget {
  maxNodes?: bigint;
  maxCommittedBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  maxFrontierEntries?: bigint;
}

export type SearchFilter =
  | { kind: "all" }
  | { kind: "key_range"; start?: Uint8Array; rangeEnd?: Uint8Array }
  | { kind: "prefix"; prefix: Uint8Array }
  | { kind: "eligible_keys"; eligibleKeys: Uint8Array[] };

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
  stats: SearchStats;
  completion: string;
  backend: string;
  planFormatVersion: number;
}

export interface SearchStats {
  levelsVisited: bigint;
  nodesRead: bigint;
  bytesRead: bigint;
  physicalBytesRead: bigint;
  committedBytes: bigint;
  distanceEvaluations: bigint;
  quantizedDistanceEvaluations: bigint;
  rerankedCandidates: bigint;
  frontierPeak: bigint;
  candidateHandlesPeak: bigint;
  candidateRetainedBytesPeak: bigint;
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

export interface HnswConfig {
  maxConnections: number;
  efConstruction: number;
  efSearch: number;
  levelBits: number;
  overfetchMultiplier: number;
  seed: bigint;
  routingVectorEncoding: "full_f32";
}

export interface HnswBuildLimits {
  maxRecords?: bigint;
  maxOwnedBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  workerThreads: bigint;
  maxEncodedGraphBytes?: bigint;
}

export interface HnswBuildStats {
  records: bigint;
  distanceEvaluations: bigint;
  directedEdges: bigint;
  maximumLevel: number;
  ownedBytes: bigint;
  encodedGraphBytes: bigint;
}

export interface HnswBuildOptions {
  config?: HnswConfig;
  limits?: HnswBuildLimits;
  signal?: AbortSignal;
}

export interface HnswBuildResult {
  index: HnswIndex;
  stats: HnswBuildStats;
}

export function defaultHnswConfig(): HnswConfig {
  return {
    maxConnections: 16,
    efConstruction: 128,
    efSearch: 64,
    levelBits: 4,
    overfetchMultiplier: 8,
    seed: 0n,
    routingVectorEncoding: "full_f32",
  };
}

export function defaultHnswBuildLimits(): HnswBuildLimits {
  return { workerThreads: 1n };
}

export interface ProductQuantizationConfig {
  subquantizers: number;
  centroidsPerSubquantizer: number;
  trainingIterations: number;
  rerankMultiplier: number;
  seed: bigint;
  maxTrainingVectors: bigint;
}

export interface ProductQuantizationBuildLimits {
  maxTrainingVectors?: bigint;
  maxTrainingBytes?: bigint;
  maxTemporaryCodeBytes?: bigint;
  maxDistanceEvaluations?: bigint;
  maxEncodedOutputBytes?: bigint;
  maxWorkerThreads?: bigint;
}

export interface ProductQuantizationBuildStats {
  trainingDistanceEvaluations: bigint;
  encodingDistanceEvaluations: bigint;
  encodedVectors: bigint;
  trainingVectors: bigint;
  trainingBytes: bigint;
  encodedOutputBytes: bigint;
}

export interface ProductQuantizationQuality {
  meanSquaredError: number;
  maximumSquaredError: number;
}

export interface ProductQuantizationBuildOptions {
  config?: ProductQuantizationConfig;
  workerThreads?: bigint;
  limits?: ProductQuantizationBuildLimits;
  signal?: AbortSignal;
}

export interface ProductQuantizationBuildResult {
  index: ProductQuantizer;
  stats: ProductQuantizationBuildStats;
}

export function defaultPqConfig(): ProductQuantizationConfig {
  return {
    subquantizers: 8,
    centroidsPerSubquantizer: 256,
    trainingIterations: 12,
    rerankMultiplier: 8,
    seed: 0n,
    maxTrainingVectors: 65_536n,
  };
}

export function defaultPqBuildLimits(): ProductQuantizationBuildLimits {
  return {};
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

interface NativeHnswConfig {
  maxConnections: number;
  efConstruction: number;
  efSearch: number;
  levelBits: number;
  overfetchMultiplier: number;
  seed: string;
  routingVectorEncoding: string;
}
interface NativeHnswBuildLimits {
  maxRecords?: string;
  maxOwnedBytes?: string;
  maxDistanceEvaluations?: string;
  workerThreads: string;
  maxEncodedGraphBytes?: string;
}
interface NativeHnswBuildStats {
  records: string;
  distanceEvaluations: string;
  directedEdges: string;
  maximumLevel: number;
  ownedBytes: string;
  encodedGraphBytes: string;
}
interface NativeHnswBuildResult {
  index(): NativeHnswIndex;
  stats(): NativeHnswBuildStats;
}
interface NativeHnswIndex {
  manifest(): Uint8Array;
  sourceDescriptor(): Uint8Array;
  config(): NativeHnswConfig;
  isCanonical(): boolean;
  search(map: NativeProximityMap, request: NativeSearchRequest): NativeSearchResult;
  proveSearch(map: NativeProximityMap, request: NativeSearchRequest): NativeProximitySearchProof;
}

interface NativePqConfig {
  subquantizers: number;
  centroidsPerSubquantizer: number;
  trainingIterations: number;
  rerankMultiplier: number;
  seed: string;
  maxTrainingVectors: string;
}
interface NativePqBuildLimits {
  maxTrainingVectors?: string;
  maxTrainingBytes?: string;
  maxTemporaryCodeBytes?: string;
  maxDistanceEvaluations?: string;
  maxEncodedOutputBytes?: string;
  maxWorkerThreads?: string;
}
interface NativePqBuildStats {
  trainingDistanceEvaluations: string;
  encodingDistanceEvaluations: string;
  encodedVectors: string;
  trainingVectors: string;
  trainingBytes: string;
  encodedOutputBytes: string;
}
interface NativePqBuildResult {
  index(): NativeProductQuantizer;
  stats(): NativePqBuildStats;
}
interface NativeProductQuantizer {
  manifest(): Uint8Array;
  sourceDescriptor(): Uint8Array;
  config(): NativePqConfig;
  quality(): ProductQuantizationQuality;
  search(map: NativeProximityMap, request: NativeSearchRequest): NativeSearchResult;
  proveSearch(map: NativeProximityMap, request: NativeSearchRequest): NativeProximitySearchProof;
}

interface NativeSearchRequest {
  query: Float32Array;
  k: string;
  policy: SearchRequest["policy"];
  adaptiveQuality?: SearchRequest["adaptiveQuality"];
  budget: {
    maxNodes?: string; maxCommittedBytes?: string;
    maxDistanceEvaluations?: string; maxFrontierEntries?: string;
  };
  filter: {
    kind: SearchFilter["kind"];
    start?: Uint8Array; rangeEnd?: Uint8Array; prefix?: Uint8Array;
    eligibleKeys: Uint8Array[];
  };
  kernel: NonNullable<SearchRequest["kernel"]>;
  backend: NonNullable<SearchRequest["backend"]>;
  hnswEfSearch?: number;
  pqRerankMultiplier?: number;
}

interface NativeSearchResult {
  neighbors: Neighbor[];
  stats: {
    levelsVisited: string; nodesRead: string; bytesRead: string; physicalBytesRead: string;
    committedBytes: string; distanceEvaluations: string; quantizedDistanceEvaluations: string;
    rerankedCandidates: string; frontierPeak: string; candidateHandlesPeak: string;
    candidateRetainedBytesPeak: string;
  };
  completion: string;
  backend: string;
  planFormatVersion: number;
}

interface NativeProximityMap {
  buildHnsw(config?: NativeHnswConfig, limits?: NativeHnswBuildLimits): NativeHnswBuildResult;
  loadHnsw(manifest: Uint8Array): NativeHnswIndex;
  buildPq(config: NativePqConfig | undefined, workerThreads: string, limits?: NativePqBuildLimits): NativePqBuildResult;
  loadPq(manifest: Uint8Array): NativeProductQuantizer;
  read(): NativeProximityReadSession;
  search(request: NativeSearchRequest): NativeSearchResult;
  count(): string;
  config(): NativeProximityConfig;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  mutate(mutations: Array<{ key: Uint8Array; vector?: Float32Array; value?: Uint8Array }>): NativeProximityMutationResult;
  rebuild(mutations: Array<{ key: Uint8Array; vector?: Float32Array; value?: Uint8Array }>): NativeProximityMap;
  descriptor(): Uint8Array;
  verify(): NativeProximityVerification;
  proveMembership(key: Uint8Array): NativeProximityProof;
  proveSearch(request: NativeSearchRequest): NativeProximitySearchProof;
  proveStructure(): NativeProximityStructuralProof;
}
interface NativeProximityReadSession {
  search(request: NativeSearchRequest): NativeSearchResult;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  fastHandle(): string;
}
interface NativeProximityProof { verify(expectedDescriptor?: Uint8Array): Uint8Array | null; }
interface NativeProximitySearchProof {
  verify(expectedDescriptor?: Uint8Array): {
    result: NativeSearchResult;
    claim: string;
    terminalLowerBound?: number;
    replayedEvents: string;
  };
}

function ownSearchRequest(request: SearchRequest): NativeSearchRequest {
  if (!Number.isSafeInteger(request.topK) || request.topK <= 0) {
    throw new RangeError("topK must be a positive safe integer");
  }
  if (request.policy === "adaptive" && request.adaptiveQuality == null) {
    throw new TypeError("adaptive search requires adaptiveQuality");
  }
  const budget = request.budget ?? {};
  const filter = request.filter ?? { kind: "all" as const };
  let nativeFilter: NativeSearchRequest["filter"];
  switch (filter.kind) {
    case "all":
      nativeFilter = { kind: "all", eligibleKeys: [] };
      break;
    case "key_range":
      nativeFilter = {
        kind: "key_range",
        start: filter.start == null ? undefined : ownedBytes(filter.start),
        rangeEnd: filter.rangeEnd == null ? undefined : ownedBytes(filter.rangeEnd),
        eligibleKeys: [],
      };
      break;
    case "prefix":
      nativeFilter = { kind: "prefix", prefix: ownedBytes(filter.prefix), eligibleKeys: [] };
      break;
    case "eligible_keys":
      nativeFilter = {
        kind: "eligible_keys",
        eligibleKeys: filter.eligibleKeys.map(ownedBytes),
      };
      break;
  }
  return {
    query: new Float32Array(request.vector),
    k: request.topK.toString(),
    policy: request.policy,
    adaptiveQuality: request.adaptiveQuality,
    budget: {
      maxNodes: budget.maxNodes?.toString(),
      maxCommittedBytes: budget.maxCommittedBytes?.toString(),
      maxDistanceEvaluations: budget.maxDistanceEvaluations?.toString(),
      maxFrontierEntries: budget.maxFrontierEntries?.toString(),
    },
    filter: nativeFilter,
    kernel: request.kernel ?? "auto_deterministic",
    backend: request.backend ?? "native",
    hnswEfSearch: request.hnswEfSearch,
    pqRerankMultiplier: request.pqRerankMultiplier,
  };
}

function requireUnsignedInteger(value: number, maximum: number, name: string): number {
  if (!Number.isSafeInteger(value) || value < 0 || value > maximum) {
    throw new RangeError(`${name} must be an unsigned integer no greater than ${maximum}`);
  }
  return value;
}

function requireUnsignedBigInt(value: bigint, name: string): string {
  if (value < 0n || value > ((1n << 64n) - 1n)) {
    throw new RangeError(`${name} must fit an unsigned 64-bit integer`);
  }
  return value.toString();
}

function ownHnswConfig(value: HnswConfig | undefined): NativeHnswConfig | undefined {
  if (value == null) return undefined;
  return {
    maxConnections: requireUnsignedInteger(value.maxConnections, 0xffff, "maxConnections"),
    efConstruction: requireUnsignedInteger(value.efConstruction, 0xffff_ffff, "efConstruction"),
    efSearch: requireUnsignedInteger(value.efSearch, 0xffff_ffff, "efSearch"),
    levelBits: requireUnsignedInteger(value.levelBits, 0xff, "levelBits"),
    overfetchMultiplier: requireUnsignedInteger(
      value.overfetchMultiplier,
      0xffff_ffff,
      "overfetchMultiplier",
    ),
    seed: requireUnsignedBigInt(value.seed, "seed"),
    routingVectorEncoding: value.routingVectorEncoding,
  };
}

function ownHnswBuildLimits(value: HnswBuildLimits | undefined): NativeHnswBuildLimits | undefined {
  if (value == null) return undefined;
  return {
    maxRecords: value.maxRecords == null ? undefined : requireUnsignedBigInt(value.maxRecords, "maxRecords"),
    maxOwnedBytes: value.maxOwnedBytes == null ? undefined : requireUnsignedBigInt(value.maxOwnedBytes, "maxOwnedBytes"),
    maxDistanceEvaluations: value.maxDistanceEvaluations == null
      ? undefined
      : requireUnsignedBigInt(value.maxDistanceEvaluations, "maxDistanceEvaluations"),
    workerThreads: requireUnsignedBigInt(value.workerThreads, "workerThreads"),
    maxEncodedGraphBytes: value.maxEncodedGraphBytes == null
      ? undefined
      : requireUnsignedBigInt(value.maxEncodedGraphBytes, "maxEncodedGraphBytes"),
  };
}

function hnswBuildStats(value: NativeHnswBuildStats): HnswBuildStats {
  return {
    records: BigInt(value.records),
    distanceEvaluations: BigInt(value.distanceEvaluations),
    directedEdges: BigInt(value.directedEdges),
    maximumLevel: value.maximumLevel,
    ownedBytes: BigInt(value.ownedBytes),
    encodedGraphBytes: BigInt(value.encodedGraphBytes),
  };
}

function ownPqConfig(value: ProductQuantizationConfig | undefined): NativePqConfig | undefined {
  if (value == null) return undefined;
  return {
    subquantizers: requireUnsignedInteger(value.subquantizers, 0xffff_ffff, "subquantizers"),
    centroidsPerSubquantizer: requireUnsignedInteger(
      value.centroidsPerSubquantizer, 0xffff, "centroidsPerSubquantizer",
    ),
    trainingIterations: requireUnsignedInteger(
      value.trainingIterations, 0xffff, "trainingIterations",
    ),
    rerankMultiplier: requireUnsignedInteger(
      value.rerankMultiplier, 0xffff_ffff, "rerankMultiplier",
    ),
    seed: requireUnsignedBigInt(value.seed, "seed"),
    maxTrainingVectors: requireUnsignedBigInt(
      value.maxTrainingVectors, "maxTrainingVectors",
    ),
  };
}

function ownPqBuildLimits(
  value: ProductQuantizationBuildLimits | undefined,
): NativePqBuildLimits | undefined {
  if (value == null) return undefined;
  const optional = (candidate: bigint | undefined, name: string) =>
    candidate == null ? undefined : requireUnsignedBigInt(candidate, name);
  return {
    maxTrainingVectors: optional(value.maxTrainingVectors, "maxTrainingVectors"),
    maxTrainingBytes: optional(value.maxTrainingBytes, "maxTrainingBytes"),
    maxTemporaryCodeBytes: optional(value.maxTemporaryCodeBytes, "maxTemporaryCodeBytes"),
    maxDistanceEvaluations: optional(value.maxDistanceEvaluations, "maxDistanceEvaluations"),
    maxEncodedOutputBytes: optional(value.maxEncodedOutputBytes, "maxEncodedOutputBytes"),
    maxWorkerThreads: optional(value.maxWorkerThreads, "maxWorkerThreads"),
  };
}

function pqBuildStats(value: NativePqBuildStats): ProductQuantizationBuildStats {
  return {
    trainingDistanceEvaluations: BigInt(value.trainingDistanceEvaluations),
    encodingDistanceEvaluations: BigInt(value.encodingDistanceEvaluations),
    encodedVectors: BigInt(value.encodedVectors),
    trainingVectors: BigInt(value.trainingVectors),
    trainingBytes: BigInt(value.trainingBytes),
    encodedOutputBytes: BigInt(value.encodedOutputBytes),
  };
}

function pqConfig(value: NativePqConfig): ProductQuantizationConfig {
  return {
    subquantizers: value.subquantizers,
    centroidsPerSubquantizer: value.centroidsPerSubquantizer,
    trainingIterations: value.trainingIterations,
    rerankMultiplier: value.rerankMultiplier,
    seed: BigInt(value.seed),
    maxTrainingVectors: BigInt(value.maxTrainingVectors),
  };
}

function searchResult(value: NativeSearchResult): SearchResult {
  return {
    neighbors: value.neighbors.map((neighbor) => ({
      key: ownedBytes(neighbor.key),
      value: ownedBytes(neighbor.value),
      distance: neighbor.distance,
    })),
    stats: {
      levelsVisited: BigInt(value.stats.levelsVisited),
      nodesRead: BigInt(value.stats.nodesRead),
      bytesRead: BigInt(value.stats.bytesRead),
      physicalBytesRead: BigInt(value.stats.physicalBytesRead),
      committedBytes: BigInt(value.stats.committedBytes),
      distanceEvaluations: BigInt(value.stats.distanceEvaluations),
      quantizedDistanceEvaluations: BigInt(value.stats.quantizedDistanceEvaluations),
      rerankedCandidates: BigInt(value.stats.rerankedCandidates),
      frontierPeak: BigInt(value.stats.frontierPeak),
      candidateHandlesPeak: BigInt(value.stats.candidateHandlesPeak),
      candidateRetainedBytesPeak: BigInt(value.stats.candidateRetainedBytesPeak),
    },
    completion: value.completion,
    backend: value.backend,
    planFormatVersion: value.planFormatVersion,
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
  buildHnsw(options: HnswBuildOptions = {}): Promise<HnswBuildResult> {
    const native = this.nativeHandle();
    const config = ownHnswConfig(options.config);
    const limits = ownHnswBuildLimits(options.limits);
    return nativePromise(options.signal, () => {
      const result = native.buildHnsw(config, limits);
      return { index: new HnswIndex(result.index()), stats: hnswBuildStats(result.stats()) };
    });
  }
  loadHnsw(manifest: Uint8Array): HnswIndex {
    return new HnswIndex(this.nativeHandle().loadHnsw(ownedBytes(manifest)));
  }
  buildPq(options: ProductQuantizationBuildOptions = {}): Promise<ProductQuantizationBuildResult> {
    const native = this.nativeHandle();
    const config = ownPqConfig(options.config);
    const limits = ownPqBuildLimits(options.limits);
    const workerThreads = requireUnsignedBigInt(options.workerThreads ?? 1n, "workerThreads");
    return nativePromise(options.signal, () => {
      const result = native.buildPq(config, workerThreads, limits);
      return {
        index: new ProductQuantizer(result.index()),
        stats: pqBuildStats(result.stats()),
      };
    });
  }
  loadPq(manifest: Uint8Array): ProductQuantizer {
    return new ProductQuantizer(this.nativeHandle().loadPq(ownedBytes(manifest)));
  }
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
      this.nativeHandle().proveSearch(ownSearchRequest(request)),
    );
  }
  proveStructure(): ProximityStructuralProof {
    return new ProximityStructuralProof(this.nativeHandle().proveStructure());
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class HnswIndex implements Disposable {
  #native?: NativeHnswIndex;
  constructor(native: NativeHnswIndex) { this.#native = native; }
  #open(): NativeHnswIndex {
    if (this.#native == null) throw new Error("HNSW index is closed");
    return this.#native;
  }
  manifest(): Uint8Array { return ownedBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedBytes(this.#open().sourceDescriptor()); }
  config(): HnswConfig {
    const value = this.#open().config();
    return {
      maxConnections: value.maxConnections,
      efConstruction: value.efConstruction,
      efSearch: value.efSearch,
      levelBits: value.levelBits,
      overfetchMultiplier: value.overfetchMultiplier,
      seed: BigInt(value.seed),
      routingVectorEncoding: value.routingVectorEncoding as "full_f32",
    };
  }
  isCanonical(): boolean { return this.#open().isCanonical(); }
  search(map: ProximityMap, request: SearchRequest): Promise<SearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const owned = ownSearchRequest(request);
    return nativePromise(request.signal, () => searchResult(native.search(nativeMap, owned)));
  }
  proveSearch(map: ProximityMap, request: SearchRequest): ProximitySearchProof {
    return new ProximitySearchProof(
      this.#open().proveSearch(map.nativeHandle(), ownSearchRequest(request)),
    );
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProductQuantizer implements Disposable {
  #native?: NativeProductQuantizer;
  constructor(native: NativeProductQuantizer) { this.#native = native; }
  #open(): NativeProductQuantizer {
    if (this.#native == null) throw new Error("product quantizer is closed");
    return this.#native;
  }
  manifest(): Uint8Array { return ownedBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedBytes(this.#open().sourceDescriptor()); }
  config(): ProductQuantizationConfig { return pqConfig(this.#open().config()); }
  quality(): ProductQuantizationQuality { return this.#open().quality(); }
  search(map: ProximityMap, request: SearchRequest): Promise<SearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const owned = ownSearchRequest(request);
    return nativePromise(request.signal, () => searchResult(native.search(nativeMap, owned)));
  }
  proveSearch(map: ProximityMap, request: SearchRequest): ProximitySearchProof {
    return new ProximitySearchProof(
      this.#open().proveSearch(map.nativeHandle(), ownSearchRequest(request)),
    );
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
      result: searchResult(value.result),
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
    const owned = ownSearchRequest(request);
    return nativePromise(request.signal, () => searchResult(native.search(owned)));
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
