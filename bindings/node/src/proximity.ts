import { abortError, nativePromise, ownedBytes, scopedBytes, type ViewScope } from "./packed.ts";

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

export interface NeighborView {
  key: Uint8Array;
  distance: number;
  rank: number;
  value?: Uint8Array;
  proof?: Uint8Array;
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

export interface ProximitySearchRuntimePolicy {
  maxEntries: bigint;
  maxBytes: bigint;
  authoritativeMaxBytes: bigint;
  hnswMaxBytes: bigint;
  pqMaxBytes: bigint;
}

export interface ProximitySearchRuntimeStats {
  physicalReads: bigint;
  physicalBytesRead: bigint;
}

export function defaultProximitySearchRuntimePolicy(): ProximitySearchRuntimePolicy {
  return {
    maxEntries: 16_384n,
    maxBytes: 256n * 1024n * 1024n,
    authoritativeMaxBytes: 128n * 1024n * 1024n,
    hnswMaxBytes: 96n * 1024n * 1024n,
    pqMaxBytes: 32n * 1024n * 1024n,
  };
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

export interface CompositeAcceleratorConfig {
  maxDeltaRecords: bigint;
  maxShadowRecords: bigint;
  maxDeltaRatioPpm: number;
  maxShadowRatioPpm: number;
  baseOverfetchMultiplier: number;
}

export interface CompositeBuildLimits {
  maxDiffEntries?: bigint;
  maxOwnedBytes?: bigint;
  maxEncodedOutputBytes?: bigint;
  maxDistanceEvaluations?: bigint;
}

export interface CompositeBuildStats {
  diffEntries: bigint;
  insertedRecords: bigint;
  vectorUpdatedRecords: bigint;
  valueOnlyRecords: bigint;
  deletedRecords: bigint;
  deltaRecords: bigint;
  shadowRecords: bigint;
  ownedBytesPeak: bigint;
  encodedOutputBytes: bigint;
  distanceEvaluations: bigint;
}

export interface FullRebuildReason {
  kind: "delta_records" | "shadow_records" | "delta_ratio" | "shadow_ratio";
  actual: bigint;
  maximum: bigint;
}

export interface CompositeRebuildOptions {
  hnswLimits?: HnswBuildLimits;
  pqWorkerThreads?: bigint;
  pqLimits?: ProductQuantizationBuildLimits;
}

export interface CompositeBuildOptions {
  config?: CompositeAcceleratorConfig;
  limits?: CompositeBuildLimits;
  signal?: AbortSignal;
}

export interface CompositeBuildOrRebuildOptions extends CompositeBuildOptions {
  rebuild?: CompositeRebuildOptions;
}

export interface CompositeBuildOutcome {
  accelerator?: CompositeAccelerator;
  reasons: FullRebuildReason[];
  stats: CompositeBuildStats;
}

export interface CompositeBuildOrRebuildOutcome {
  kind: "composite" | "no_accelerator_required" | "hnsw_rebuilt" | "product_quantized_rebuilt";
  composite?: CompositeAccelerator;
  hnsw?: HnswIndex;
  pq?: ProductQuantizer;
  reasons: FullRebuildReason[];
  compositeStats: CompositeBuildStats;
  hnswStats?: HnswBuildStats;
  pqStats?: ProductQuantizationBuildStats;
}

export interface AcceleratorCatalogEntry {
  kind: "hnsw" | "product_quantized" | "composite";
  configurationFingerprint: Uint8Array;
  manifest: Uint8Array;
}

export function defaultCompositeAcceleratorConfig(): CompositeAcceleratorConfig {
  return {
    maxDeltaRecords: 4_096n,
    maxShadowRecords: 8_192n,
    maxDeltaRatioPpm: 100_000,
    maxShadowRatioPpm: 200_000,
    baseOverfetchMultiplier: 2,
  };
}

export function defaultCompositeBuildLimits(): CompositeBuildLimits { return {}; }

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
  searchWithRuntime(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  searchCancellable(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime | undefined, cancellation: NativeProximityCancellationToken): Promise<NativeSearchResult>;
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
  searchWithRuntime(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  searchCancellable(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime | undefined, cancellation: NativeProximityCancellationToken): Promise<NativeSearchResult>;
  proveSearch(map: NativeProximityMap, request: NativeSearchRequest): NativeProximitySearchProof;
}

interface NativeCompositeConfig {
  maxDeltaRecords: string;
  maxShadowRecords: string;
  maxDeltaRatioPpm: number;
  maxShadowRatioPpm: number;
  baseOverfetchMultiplier: number;
}
interface NativeCompositeBuildLimits {
  maxDiffEntries?: string;
  maxOwnedBytes?: string;
  maxEncodedOutputBytes?: string;
  maxDistanceEvaluations?: string;
}
interface NativeCompositeBuildStats {
  diffEntries: string;
  insertedRecords: string;
  vectorUpdatedRecords: string;
  valueOnlyRecords: string;
  deletedRecords: string;
  deltaRecords: string;
  shadowRecords: string;
  ownedBytesPeak: string;
  encodedOutputBytes: string;
  distanceEvaluations: string;
}
interface NativeFullRebuildReason { kind: FullRebuildReason["kind"]; actual: string; maximum: string; }
interface NativeCompositeRebuildOptions {
  hnswLimits: NativeHnswBuildLimits;
  pqWorkerThreads: string;
  pqLimits: NativePqBuildLimits;
}
interface NativeCompositeBuildResult {
  accelerator(): NativeCompositeAccelerator | undefined;
  reasons(): NativeFullRebuildReason[];
  stats(): NativeCompositeBuildStats;
}
interface NativeCompositeBuildOrRebuildResult {
  kind(): CompositeBuildOrRebuildOutcome["kind"];
  composite(): NativeCompositeAccelerator | undefined;
  hnsw(): NativeHnswIndex | undefined;
  pq(): NativeProductQuantizer | undefined;
  reasons(): NativeFullRebuildReason[];
  compositeStats(): NativeCompositeBuildStats;
  hnswStats(): NativeHnswBuildStats | undefined;
  pqStats(): NativePqBuildStats | undefined;
}
interface NativeCompositeAccelerator {
  manifest(): Uint8Array;
  currentSourceDescriptor(): Uint8Array;
  baseSourceDescriptor(): Uint8Array;
  baseKind(): "hnsw" | "product_quantized";
  deltaCount(): string;
  shadowCount(): string;
  config(): NativeCompositeConfig;
  buildStats(): NativeCompositeBuildStats;
  search(map: NativeProximityMap, request: NativeSearchRequest): NativeSearchResult;
  searchWithRuntime(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  searchCancellable(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime | undefined, cancellation: NativeProximityCancellationToken): Promise<NativeSearchResult>;
  proveSearch(map: NativeProximityMap, request: NativeSearchRequest): NativeProximitySearchProof;
}
interface NativeAcceleratorCatalog {
  manifest(): Uint8Array;
  sourceDescriptor(): Uint8Array;
  entries(): Array<{ kind: AcceleratorCatalogEntry["kind"]; configurationFingerprint: Uint8Array; manifest: Uint8Array }>;
  search(map: NativeProximityMap, request: NativeSearchRequest): NativeSearchResult;
  searchWithRuntime(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  searchCancellable(map: NativeProximityMap, request: NativeSearchRequest, runtime: NativeProximitySearchRuntime | undefined, cancellation: NativeProximityCancellationToken): Promise<NativeSearchResult>;
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

export interface NativeProximitySearchRuntimePolicy {
  maxEntries: string;
  maxBytes: string;
  authoritativeMaxBytes: string;
  hnswMaxBytes: string;
  pqMaxBytes: string;
}

export interface NativeProximitySearchRuntime {
  policy(): NativeProximitySearchRuntimePolicy;
  stats(): { physicalReads: string; physicalBytesRead: string };
  clear(): void;
}

export interface NativeProximityCancellationToken {
  cancel(): void;
  isCancelled(): boolean;
}

interface NativeProximityMap {
  buildHnsw(config?: NativeHnswConfig, limits?: NativeHnswBuildLimits): NativeHnswBuildResult;
  loadHnsw(manifest: Uint8Array): NativeHnswIndex;
  buildPq(config: NativePqConfig | undefined, workerThreads: string, limits?: NativePqBuildLimits): NativePqBuildResult;
  loadPq(manifest: Uint8Array): NativeProductQuantizer;
  buildCompositeHnsw(baseMap: NativeProximityMap, base: NativeHnswIndex, config?: NativeCompositeConfig, limits?: NativeCompositeBuildLimits): NativeCompositeBuildResult;
  buildCompositePq(baseMap: NativeProximityMap, base: NativeProductQuantizer, config?: NativeCompositeConfig, limits?: NativeCompositeBuildLimits): NativeCompositeBuildResult;
  buildOrRebuildCompositeHnsw(baseMap: NativeProximityMap, base: NativeHnswIndex, config?: NativeCompositeConfig, limits?: NativeCompositeBuildLimits, rebuild?: NativeCompositeRebuildOptions): NativeCompositeBuildOrRebuildResult;
  buildOrRebuildCompositePq(baseMap: NativeProximityMap, base: NativeProductQuantizer, config?: NativeCompositeConfig, limits?: NativeCompositeBuildLimits, rebuild?: NativeCompositeRebuildOptions): NativeCompositeBuildOrRebuildResult;
  loadComposite(manifest: Uint8Array): NativeCompositeAccelerator;
  buildAcceleratorCatalog(hnsw?: NativeHnswIndex, pq?: NativeProductQuantizer, composite?: NativeCompositeAccelerator): NativeAcceleratorCatalog;
  loadAcceleratorCatalog(manifest: Uint8Array): NativeAcceleratorCatalog;
  read(): NativeProximityReadSession;
  search(request: NativeSearchRequest): NativeSearchResult;
  searchWithRuntime(request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  cancellationToken(): NativeProximityCancellationToken;
  searchCancellable(
    request: NativeSearchRequest,
    runtime: NativeProximitySearchRuntime | undefined,
    cancellation: NativeProximityCancellationToken,
  ): Promise<NativeSearchResult>;
  count(): string;
  config(): NativeProximityConfig;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  scanRecords(visitor: (record: { key: Uint8Array; vector: Float32Array; value: Uint8Array }) => boolean): string;
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
  searchWithRuntime(request: NativeSearchRequest, runtime: NativeProximitySearchRuntime): NativeSearchResult;
  cancellationToken(): NativeProximityCancellationToken;
  searchCancellable(request: NativeSearchRequest, runtime: NativeProximitySearchRuntime | undefined, cancellation: NativeProximityCancellationToken): Promise<NativeSearchResult>;
  get(key: Uint8Array): { vector: number[]; value: Uint8Array } | null;
  contains(key: Uint8Array): boolean;
  scanRecords(visitor: (record: { key: Uint8Array; vector: Float32Array; value: Uint8Array }) => boolean): string;
  withSearchPage(
    query: Float32Array,
    k: number,
    visitor: (page: { bytes: Buffer; recordCount: number; terminal: boolean }) => boolean,
  ): void;
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

export function ownProximitySearchRuntimePolicy(
  value: ProximitySearchRuntimePolicy,
): NativeProximitySearchRuntimePolicy {
  return {
    maxEntries: requireUnsignedBigInt(value.maxEntries, "maxEntries"),
    maxBytes: requireUnsignedBigInt(value.maxBytes, "maxBytes"),
    authoritativeMaxBytes: requireUnsignedBigInt(
      value.authoritativeMaxBytes, "authoritativeMaxBytes",
    ),
    hnswMaxBytes: requireUnsignedBigInt(value.hnswMaxBytes, "hnswMaxBytes"),
    pqMaxBytes: requireUnsignedBigInt(value.pqMaxBytes, "pqMaxBytes"),
  };
}

function proximitySearchRuntimePolicy(
  value: NativeProximitySearchRuntimePolicy,
): ProximitySearchRuntimePolicy {
  return {
    maxEntries: BigInt(value.maxEntries),
    maxBytes: BigInt(value.maxBytes),
    authoritativeMaxBytes: BigInt(value.authoritativeMaxBytes),
    hnswMaxBytes: BigInt(value.hnswMaxBytes),
    pqMaxBytes: BigInt(value.pqMaxBytes),
  };
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

function ownCompositeConfig(value: CompositeAcceleratorConfig | undefined): NativeCompositeConfig | undefined {
  if (value == null) return undefined;
  return {
    maxDeltaRecords: requireUnsignedBigInt(value.maxDeltaRecords, "maxDeltaRecords"),
    maxShadowRecords: requireUnsignedBigInt(value.maxShadowRecords, "maxShadowRecords"),
    maxDeltaRatioPpm: requireUnsignedInteger(value.maxDeltaRatioPpm, 1_000_000, "maxDeltaRatioPpm"),
    maxShadowRatioPpm: requireUnsignedInteger(value.maxShadowRatioPpm, 1_000_000, "maxShadowRatioPpm"),
    baseOverfetchMultiplier: requireUnsignedInteger(value.baseOverfetchMultiplier, 0xffff_ffff, "baseOverfetchMultiplier"),
  };
}

function compositeConfig(value: NativeCompositeConfig): CompositeAcceleratorConfig {
  return {
    maxDeltaRecords: BigInt(value.maxDeltaRecords),
    maxShadowRecords: BigInt(value.maxShadowRecords),
    maxDeltaRatioPpm: value.maxDeltaRatioPpm,
    maxShadowRatioPpm: value.maxShadowRatioPpm,
    baseOverfetchMultiplier: value.baseOverfetchMultiplier,
  };
}

function ownCompositeBuildLimits(value: CompositeBuildLimits | undefined): NativeCompositeBuildLimits | undefined {
  if (value == null) return undefined;
  const optional = (candidate: bigint | undefined, name: string) =>
    candidate == null ? undefined : requireUnsignedBigInt(candidate, name);
  return {
    maxDiffEntries: optional(value.maxDiffEntries, "maxDiffEntries"),
    maxOwnedBytes: optional(value.maxOwnedBytes, "maxOwnedBytes"),
    maxEncodedOutputBytes: optional(value.maxEncodedOutputBytes, "maxEncodedOutputBytes"),
    maxDistanceEvaluations: optional(value.maxDistanceEvaluations, "maxDistanceEvaluations"),
  };
}

function compositeBuildStats(value: NativeCompositeBuildStats): CompositeBuildStats {
  return {
    diffEntries: BigInt(value.diffEntries),
    insertedRecords: BigInt(value.insertedRecords),
    vectorUpdatedRecords: BigInt(value.vectorUpdatedRecords),
    valueOnlyRecords: BigInt(value.valueOnlyRecords),
    deletedRecords: BigInt(value.deletedRecords),
    deltaRecords: BigInt(value.deltaRecords),
    shadowRecords: BigInt(value.shadowRecords),
    ownedBytesPeak: BigInt(value.ownedBytesPeak),
    encodedOutputBytes: BigInt(value.encodedOutputBytes),
    distanceEvaluations: BigInt(value.distanceEvaluations),
  };
}

function rebuildReasons(values: NativeFullRebuildReason[]): FullRebuildReason[] {
  return values.map((value) => ({ kind: value.kind, actual: BigInt(value.actual), maximum: BigInt(value.maximum) }));
}

function ownCompositeRebuildOptions(value: CompositeRebuildOptions | undefined): NativeCompositeRebuildOptions | undefined {
  if (value == null) return undefined;
  return {
    hnswLimits: ownHnswBuildLimits(value.hnswLimits ?? defaultHnswBuildLimits())!,
    pqWorkerThreads: requireUnsignedBigInt(value.pqWorkerThreads ?? 1n, "pqWorkerThreads"),
    pqLimits: ownPqBuildLimits(value.pqLimits ?? defaultPqBuildLimits())!,
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

function compositeRebuildOutcome(value: NativeCompositeBuildOrRebuildResult): CompositeBuildOrRebuildOutcome {
  const composite = value.composite();
  const hnsw = value.hnsw();
  const pq = value.pq();
  const hnswStats = value.hnswStats();
  const pqStats = value.pqStats();
  return {
    kind: value.kind(),
    composite: composite == null ? undefined : new CompositeAccelerator(composite),
    hnsw: hnsw == null ? undefined : new HnswIndex(hnsw),
    pq: pq == null ? undefined : new ProductQuantizer(pq),
    reasons: rebuildReasons(value.reasons()),
    compositeStats: compositeBuildStats(value.compositeStats()),
    hnswStats: hnswStats == null ? undefined : hnswBuildStats(hnswStats),
    pqStats: pqStats == null ? undefined : pqBuildStats(pqStats),
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

export class ProximitySearchRuntime implements Disposable {
  #native?: NativeProximitySearchRuntime;
  constructor(native: NativeProximitySearchRuntime) { this.#native = native; }
  nativeHandle(): NativeProximitySearchRuntime {
    if (this.#native == null) throw new Error("proximity search runtime is closed");
    return this.#native;
  }
  policy(): ProximitySearchRuntimePolicy {
    return proximitySearchRuntimePolicy(this.nativeHandle().policy());
  }
  stats(): ProximitySearchRuntimeStats {
    const value = this.nativeHandle().stats();
    return {
      physicalReads: BigInt(value.physicalReads),
      physicalBytesRead: BigInt(value.physicalBytesRead),
    };
  }
  clear(): void { this.nativeHandle().clear(); }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximityCancellationToken implements Disposable {
  #native?: NativeProximityCancellationToken;
  constructor(native: NativeProximityCancellationToken) { this.#native = native; }
  nativeHandle(): NativeProximityCancellationToken {
    if (this.#native == null) throw new Error("proximity cancellation token is closed");
    return this.#native;
  }
  cancel(): void { this.nativeHandle().cancel(); }
  get isCancelled(): boolean { return this.nativeHandle().isCancelled(); }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

async function cooperativeNativeSearch(
  request: SearchRequest,
  cancellation: ProximityCancellationToken,
  invoke: (owned: NativeSearchRequest, token: NativeProximityCancellationToken) => Promise<NativeSearchResult>,
): Promise<SearchResult> {
  const owned = ownSearchRequest(request);
  const nativeCancellation = cancellation.nativeHandle();
  if (request.signal?.aborted) {
    cancellation.cancel();
    throw abortError();
  }
  const onAbort = () => cancellation.cancel();
  request.signal?.addEventListener("abort", onAbort, { once: true });
  try {
    const result = searchResult(await invoke(owned, nativeCancellation));
    if (request.signal?.aborted) throw abortError();
    return result;
  } finally {
    request.signal?.removeEventListener("abort", onAbort);
  }
}

export class ProximityMap implements Disposable {
  #native?: NativeProximityMap;
  constructor(native: NativeProximityMap) { this.#native = native; }
  nativeHandle(): NativeProximityMap {
    if (this.#native == null) throw new Error("proximity map is closed");
    return this.#native;
  }
  cancellationToken(): ProximityCancellationToken {
    return new ProximityCancellationToken(this.nativeHandle().cancellationToken());
  }
  read(): ProximityReadSession { return new ProximityReadSession(this.nativeHandle().read()); }
  search(request: SearchRequest): Promise<SearchResult> {
    const cancellation = new ProximityCancellationToken(this.nativeHandle().cancellationToken());
    return this.searchCancellable(request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    request: SearchRequest,
    runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = new ProximityCancellationToken(this.nativeHandle().cancellationToken());
    return this.searchCancellable(request, cancellation, runtime).finally(() => cancellation.close());
  }
  async searchCancellable(
    request: SearchRequest,
    cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const native = this.nativeHandle();
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(owned, nativeRuntime, token),
    );
  }
  get(key: Uint8Array): { vector: Float32Array; value: Uint8Array } | undefined {
    const record = this.nativeHandle().get(ownedBytes(key));
    return record == null ? undefined : { vector: new Float32Array(record.vector), value: record.value };
  }
  contains(key: Uint8Array): boolean { return this.nativeHandle().contains(ownedBytes(key)); }
  scanRecords(visitor: (record: ProximityRecord) => boolean): bigint {
    return BigInt(this.nativeHandle().scanRecords((record) => visitor({
      key: ownedBytes(record.key),
      vector: new Float32Array(record.vector),
      value: ownedBytes(record.value),
    })));
  }
  withSearchView<R>(query: Float32Array, k: number, visitor: (neighbors: NeighborView[]) => R): R {
    const session = this.read();
    try { return session.withSearchView(query, k, visitor); }
    finally { session.close(); }
  }
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
  buildCompositeHnsw(
    baseMap: ProximityMap,
    base: HnswIndex,
    options: CompositeBuildOptions = {},
  ): Promise<CompositeBuildOutcome> {
    const native = this.nativeHandle();
    return nativePromise(options.signal, () => {
      const result = native.buildCompositeHnsw(
        baseMap.nativeHandle(), base.nativeHandle(),
        ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits),
      );
      const accelerator = result.accelerator();
      return {
        accelerator: accelerator == null ? undefined : new CompositeAccelerator(accelerator),
        reasons: rebuildReasons(result.reasons()),
        stats: compositeBuildStats(result.stats()),
      };
    });
  }
  buildCompositePq(
    baseMap: ProximityMap,
    base: ProductQuantizer,
    options: CompositeBuildOptions = {},
  ): Promise<CompositeBuildOutcome> {
    const native = this.nativeHandle();
    return nativePromise(options.signal, () => {
      const result = native.buildCompositePq(
        baseMap.nativeHandle(), base.nativeHandle(),
        ownCompositeConfig(options.config), ownCompositeBuildLimits(options.limits),
      );
      const accelerator = result.accelerator();
      return {
        accelerator: accelerator == null ? undefined : new CompositeAccelerator(accelerator),
        reasons: rebuildReasons(result.reasons()),
        stats: compositeBuildStats(result.stats()),
      };
    });
  }
  buildOrRebuildCompositeHnsw(
    baseMap: ProximityMap,
    base: HnswIndex,
    options: CompositeBuildOrRebuildOptions = {},
  ): Promise<CompositeBuildOrRebuildOutcome> {
    const native = this.nativeHandle();
    return nativePromise(options.signal, () => compositeRebuildOutcome(native.buildOrRebuildCompositeHnsw(
      baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config),
      ownCompositeBuildLimits(options.limits), ownCompositeRebuildOptions(options.rebuild),
    )));
  }
  buildOrRebuildCompositePq(
    baseMap: ProximityMap,
    base: ProductQuantizer,
    options: CompositeBuildOrRebuildOptions = {},
  ): Promise<CompositeBuildOrRebuildOutcome> {
    const native = this.nativeHandle();
    return nativePromise(options.signal, () => compositeRebuildOutcome(native.buildOrRebuildCompositePq(
      baseMap.nativeHandle(), base.nativeHandle(), ownCompositeConfig(options.config),
      ownCompositeBuildLimits(options.limits), ownCompositeRebuildOptions(options.rebuild),
    )));
  }
  loadComposite(manifest: Uint8Array): CompositeAccelerator {
    return new CompositeAccelerator(this.nativeHandle().loadComposite(ownedBytes(manifest)));
  }
  buildAcceleratorCatalog(options: {
    hnsw?: HnswIndex;
    pq?: ProductQuantizer;
    composite?: CompositeAccelerator;
  } = {}): AcceleratorCatalog {
    return new AcceleratorCatalog(this.nativeHandle().buildAcceleratorCatalog(
      options.hnsw?.nativeHandle(), options.pq?.nativeHandle(), options.composite?.nativeHandle(),
    ));
  }
  loadAcceleratorCatalog(manifest: Uint8Array): AcceleratorCatalog {
    return new AcceleratorCatalog(this.nativeHandle().loadAcceleratorCatalog(ownedBytes(manifest)));
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
  nativeHandle(): NativeHnswIndex { return this.#open(); }
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
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    map: ProximityMap, request: SearchRequest, runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation, runtime)
      .finally(() => cancellation.close());
  }
  searchCancellable(
    map: ProximityMap, request: SearchRequest, cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(nativeMap, owned, nativeRuntime, token),
    );
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
  nativeHandle(): NativeProductQuantizer { return this.#open(); }
  manifest(): Uint8Array { return ownedBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedBytes(this.#open().sourceDescriptor()); }
  config(): ProductQuantizationConfig { return pqConfig(this.#open().config()); }
  quality(): ProductQuantizationQuality { return this.#open().quality(); }
  search(map: ProximityMap, request: SearchRequest): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    map: ProximityMap, request: SearchRequest, runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation, runtime)
      .finally(() => cancellation.close());
  }
  searchCancellable(
    map: ProximityMap, request: SearchRequest, cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(nativeMap, owned, nativeRuntime, token),
    );
  }
  proveSearch(map: ProximityMap, request: SearchRequest): ProximitySearchProof {
    return new ProximitySearchProof(
      this.#open().proveSearch(map.nativeHandle(), ownSearchRequest(request)),
    );
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class CompositeAccelerator implements Disposable {
  #native?: NativeCompositeAccelerator;
  constructor(native: NativeCompositeAccelerator) { this.#native = native; }
  nativeHandle(): NativeCompositeAccelerator {
    if (this.#native == null) throw new Error("composite accelerator is closed");
    return this.#native;
  }
  manifest(): Uint8Array { return ownedBytes(this.nativeHandle().manifest()); }
  currentSourceDescriptor(): Uint8Array { return ownedBytes(this.nativeHandle().currentSourceDescriptor()); }
  baseSourceDescriptor(): Uint8Array { return ownedBytes(this.nativeHandle().baseSourceDescriptor()); }
  baseKind(): "hnsw" | "product_quantized" { return this.nativeHandle().baseKind(); }
  deltaCount(): bigint { return BigInt(this.nativeHandle().deltaCount()); }
  shadowCount(): bigint { return BigInt(this.nativeHandle().shadowCount()); }
  config(): CompositeAcceleratorConfig { return compositeConfig(this.nativeHandle().config()); }
  buildStats(): CompositeBuildStats { return compositeBuildStats(this.nativeHandle().buildStats()); }
  search(map: ProximityMap, request: SearchRequest): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    map: ProximityMap, request: SearchRequest, runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation, runtime)
      .finally(() => cancellation.close());
  }
  searchCancellable(
    map: ProximityMap, request: SearchRequest, cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const native = this.nativeHandle();
    const nativeMap = map.nativeHandle();
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(nativeMap, owned, nativeRuntime, token),
    );
  }
  proveSearch(map: ProximityMap, request: SearchRequest): ProximitySearchProof {
    return new ProximitySearchProof(
      this.nativeHandle().proveSearch(map.nativeHandle(), ownSearchRequest(request)),
    );
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class AcceleratorCatalog implements Disposable {
  #native?: NativeAcceleratorCatalog;
  constructor(native: NativeAcceleratorCatalog) { this.#native = native; }
  #open(): NativeAcceleratorCatalog {
    if (this.#native == null) throw new Error("accelerator catalog is closed");
    return this.#native;
  }
  manifest(): Uint8Array { return ownedBytes(this.#open().manifest()); }
  sourceDescriptor(): Uint8Array { return ownedBytes(this.#open().sourceDescriptor()); }
  entries(): AcceleratorCatalogEntry[] {
    return this.#open().entries().map((entry) => ({
      kind: entry.kind,
      configurationFingerprint: ownedBytes(entry.configurationFingerprint),
      manifest: ownedBytes(entry.manifest),
    }));
  }
  search(map: ProximityMap, request: SearchRequest): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    map: ProximityMap, request: SearchRequest, runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = map.cancellationToken();
    return this.searchCancellable(map, request, cancellation, runtime)
      .finally(() => cancellation.close());
  }
  searchCancellable(
    map: ProximityMap, request: SearchRequest, cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const native = this.#open();
    const nativeMap = map.nativeHandle();
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(nativeMap, owned, nativeRuntime, token),
    );
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
  cancellationToken(): ProximityCancellationToken {
    if (this.#native == null) throw new Error("proximity session is closed");
    return new ProximityCancellationToken(this.#native.cancellationToken());
  }
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
  scanRecords(visitor: (record: ProximityRecord) => boolean): bigint {
    if (this.#native == null) throw new Error("proximity session is closed");
    return BigInt(this.#native.scanRecords((record) => visitor({
      key: ownedBytes(record.key),
      vector: new Float32Array(record.vector),
      value: ownedBytes(record.value),
    })));
  }
  withSearchView<R>(query: Float32Array, k: number, visitor: (neighbors: NeighborView[]) => R): R {
    if (this.#native == null) throw new Error("proximity session is closed");
    if (!Number.isSafeInteger(k) || k <= 0 || k > 0xffff_ffff) {
      throw new RangeError("k must be a positive unsigned 32-bit integer");
    }
    const scope: ViewScope = { alive: true };
    let output: R | undefined;
    let called = false;
    try {
      this.#native.withSearchPage(new Float32Array(query), k, (page) => {
        output = visitor(decodeNeighborViews(page.bytes, page.recordCount, page.terminal, scope));
        called = true;
        return true;
      });
    } finally {
      scope.alive = false;
    }
    if (!called) throw new Error("native proximity search did not invoke its scoped visitor");
    return output as R;
  }
  search(request: SearchRequest): Promise<SearchResult> {
    if (this.#native == null) return Promise.reject(new Error("proximity session is closed"));
    const cancellation = this.cancellationToken();
    return this.searchCancellable(request, cancellation).finally(() => cancellation.close());
  }
  searchWithRuntime(
    request: SearchRequest,
    runtime: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    const cancellation = this.cancellationToken();
    return this.searchCancellable(request, cancellation, runtime)
      .finally(() => cancellation.close());
  }
  searchCancellable(
    request: SearchRequest,
    cancellation: ProximityCancellationToken,
    runtime?: ProximitySearchRuntime,
  ): Promise<SearchResult> {
    if (this.#native == null) return Promise.reject(new Error("proximity session is closed"));
    const native = this.#native;
    const nativeRuntime = runtime?.nativeHandle();
    return cooperativeNativeSearch(
      request, cancellation,
      (owned, token) => native.searchCancellable(owned, nativeRuntime, token),
    );
  }
  fastHandle(): bigint {
    if (this.#native == null) throw new Error("proximity session is closed");
    return BigInt(this.#native.fastHandle());
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

function decodeNeighborViews(
  page: Buffer,
  recordCount: number,
  terminal: boolean,
  scope: ViewScope,
): NeighborView[] {
  if (page.length < 28 || page.toString("ascii", 0, 4) !== "PRPG") {
    throw new Error("invalid proximity packed page header");
  }
  if (page.readUInt16LE(4) !== 2 || page.readUInt16LE(6) !== 7) {
    throw new Error("packed page is not a v2 proximity-neighbor page");
  }
  const flags = page.readUInt32LE(8);
  const count = page.readUInt32LE(12);
  const tableBytes = page.readUInt32LE(16);
  const arenaBytes = Number(page.readBigUInt64LE(20));
  const arenaStart = 28 + tableBytes;
  if ((flags & ~1) !== 0 || terminal !== ((flags & 1) !== 0)
    || count !== recordCount || tableBytes !== count * 40
    || !Number.isSafeInteger(arenaBytes) || arenaStart + arenaBytes !== page.length) {
    throw new Error("invalid proximity packed page bounds");
  }
  const field = (offset: number, length: number): Uint8Array => {
    if (offset > arenaBytes || length > arenaBytes - offset) {
      throw new Error("proximity packed field exceeds its arena");
    }
    return scopedBytes(page.subarray(arenaStart + offset, arenaStart + offset + length), scope);
  };
  const rows: NeighborView[] = [];
  for (let index = 0; index < count; index += 1) {
    const base = 28 + index * 40;
    const recordFlags = page.readUInt32LE(base);
    if ((recordFlags & ~3) !== 0) throw new Error("invalid proximity neighbor flags");
    const distance = page.readDoubleLE(base + 12);
    if (!Number.isFinite(distance)) throw new Error("invalid proximity neighbor distance");
    const key = field(page.readUInt32LE(base + 4), page.readUInt32LE(base + 8));
    const valueOffset = page.readUInt32LE(base + 24);
    const valueLength = page.readUInt32LE(base + 28);
    const proofOffset = page.readUInt32LE(base + 32);
    const proofLength = page.readUInt32LE(base + 36);
    if ((recordFlags & 1) === 0 && (valueOffset !== 0 || valueLength !== 0)) {
      throw new Error("absent proximity value has a non-empty range");
    }
    if ((recordFlags & 2) === 0 && (proofOffset !== 0 || proofLength !== 0)) {
      throw new Error("absent proximity proof has a non-empty range");
    }
    rows.push({
      key,
      distance,
      rank: page.readUInt32LE(base + 20),
      value: (recordFlags & 1) === 0 ? undefined : field(valueOffset, valueLength),
      proof: (recordFlags & 2) === 0 ? undefined : field(proofOffset, proofLength),
    });
  }
  return rows;
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
