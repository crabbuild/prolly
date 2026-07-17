import { nativePromise, ownedBytes, scopedBytes, type ViewScope } from "./packed.ts";
import { gcPlan, type GcPlan } from "./versioned.ts";

export type IndexProjection = "keys_only" | "include" | "all";

export interface IndexEntry {
  term: Uint8Array;
  projection?: Uint8Array;
}

export interface IndexRegistration {
  name: Uint8Array;
  generation: bigint;
  extractorId: string;
  projection: IndexProjection;
  extract(primaryKey: Uint8Array, sourceValue: Uint8Array): IndexEntry[];
}

export interface IndexedVersion {
  sourceVersion: Uint8Array;
  catalogVersion?: Uint8Array;
  indexCount: bigint;
}

export type IndexedMutation =
  | { kind: "upsert"; key: Uint8Array; value: Uint8Array }
  | { kind: "delete"; key: Uint8Array };

export interface IndexedUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previousSourceVersion?: Uint8Array;
  current?: IndexedVersion;
}

export interface IndexedSnapshotId {
  sourceVersion: Uint8Array;
  catalogVersion: Uint8Array;
}

export interface IndexBuildResult {
  sourceVersion: Uint8Array;
  indexVersion: Uint8Array;
  catalogVersion: Uint8Array;
  generation: bigint;
  entries: bigint;
  attempts: bigint;
  activated: boolean;
}

export interface ActiveIndexHealth {
  name: Uint8Array;
  generation: bigint;
  fingerprint: Uint8Array;
  projection: IndexProjection;
  indexMapId: Uint8Array;
  indexVersion: Uint8Array;
}

export interface IndexedMapHealth {
  sourceMapId: Uint8Array;
  sourceVersion?: Uint8Array;
  catalogVersion?: Uint8Array;
  activeIndexes: ActiveIndexHealth[];
  supportsTransactions: boolean;
}

export interface IndexVerification {
  name: Uint8Array;
  sourceVersion: Uint8Array;
  expectedIndexVersion: Uint8Array;
  actualIndexVersion: Uint8Array;
  expectedEntries: bigint;
  actualEntries: bigint;
  semanticDifferences: bigint;
  valid: boolean;
  canonical: boolean;
}

export interface IndexedMapMetrics {
  normalizedSourceMutations: bigint;
  recordsExtracted: bigint;
  termsEmitted: bigint;
  projectedBytes: bigint;
  physicalUpserts: bigint;
  physicalDeletes: bigint;
  unchangedEmissionsSkipped: bigint;
  sourceNodesWritten: bigint;
  indexNodesWritten: bigint;
  catalogNodesWritten: bigint;
  retries: bigint;
  buildAttempts: bigint;
  verificationOutcomes: bigint;
  retainedRoots: bigint;
}

export interface IndexedRetention {
  retainedSourceVersions: Uint8Array[];
  removedSourceVersions: Uint8Array[];
  retainedIndexVersions: Uint8Array[];
  removedIndexVersions: Uint8Array[];
  removedCatalogVersions: Uint8Array[];
  removedCheckpointRecords: bigint;
  removedNamedRoots: Uint8Array[];
}

export interface IndexMatch {
  term: Uint8Array;
  primaryKey: Uint8Array;
  projection?: Uint8Array;
}

export interface IndexedSource extends IndexMatch {
  sourceValue: Uint8Array;
}

export interface IndexMatchView extends IndexMatch {}

export interface IndexPageOptions {
  pageSize?: number;
  signal?: AbortSignal;
}

interface NativeIndexRegistry {
  register(
    name: Uint8Array,
    generation: string,
    extractorId: string,
    projection: string,
    extract: (request: { primaryKey: Uint8Array; sourceValue: Uint8Array }) => IndexEntry[],
  ): void;
}

interface NativeIndexedVersion {
  sourceVersion: Uint8Array;
  catalogVersion?: Uint8Array;
  indexCount: string;
}

interface NativeIndexedUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previousSourceVersion?: Uint8Array;
  current?: NativeIndexedVersion;
}

interface NativeIndexedSnapshotId {
  sourceVersion: Uint8Array;
  catalogVersion: Uint8Array;
}

interface NativeIndexBuildResult {
  sourceVersion: Uint8Array;
  indexVersion: Uint8Array;
  catalogVersion: Uint8Array;
  generation: string;
  entries: string;
  attempts: string;
  activated: boolean;
}

interface NativeIndexMatch {
  term: Uint8Array;
  primaryKey: Uint8Array;
  projection?: Uint8Array;
}

interface NativeIndexedSource extends NativeIndexMatch {
  sourceValue: Uint8Array;
}

export class IndexRegistry implements Disposable {
  #native?: NativeIndexRegistry;

  constructor(native: NativeIndexRegistry) {
    this.#native = native;
  }

  nativeHandle(): NativeIndexRegistry {
    if (this.#native == null) throw new Error("index registry is closed");
    return this.#native;
  }

  register(registration: IndexRegistration): void {
    const native = this.nativeHandle();
    native.register(
      ownedBytes(registration.name),
      registration.generation.toString(),
      registration.extractorId,
      registration.projection,
      ({ primaryKey, sourceValue }) => registration.extract(primaryKey, sourceValue).map((entry) => ({
        term: ownedBytes(entry.term),
        projection: entry.projection == null ? undefined : ownedBytes(entry.projection),
      })),
    );
  }

  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

interface NativeIndexedMap {
  id(): Uint8Array;
  get(key: Uint8Array): Uint8Array | null;
  put(key: Uint8Array, value: Uint8Array): NativeIndexedVersion;
  apply(mutations: { kind: string; key: Uint8Array; value?: Uint8Array }[]): NativeIndexedVersion;
  applyIf(expectedSource: Uint8Array | undefined, mutations: { kind: string; key: Uint8Array; value?: Uint8Array }[]): NativeIndexedUpdate;
  delete(key: Uint8Array): NativeIndexedVersion;
  ensureIndex(name: Uint8Array): NativeIndexBuildResult;
  snapshot(): NativeIndexedSnapshot;
  snapshotAt(sourceVersion: Uint8Array): NativeIndexedSnapshot;
  snapshotById(id: NativeIndexedSnapshotId): NativeIndexedSnapshot;
  health(): {
    sourceMapId: Uint8Array; sourceVersion?: Uint8Array; catalogVersion?: Uint8Array;
    activeIndexes: { name: Uint8Array; generation: string; fingerprint: Uint8Array; projection: IndexProjection; indexMapId: Uint8Array; indexVersion: Uint8Array }[];
    supportsTransactions: boolean;
  };
  metrics(): Record<keyof IndexedMapMetrics, string>;
  verifyIndex(name: Uint8Array, sourceVersion: Uint8Array): NativeIndexVerification;
  verifyAll(sourceVersion: Uint8Array): NativeIndexVerification[];
  repairIndex(name: Uint8Array, sourceVersion: Uint8Array): NativeIndexVerification;
  deactivateIndex(name: Uint8Array): NativeIndexedVersion;
  exportCurrent(): Uint8Array;
  importCurrent(bundle: Uint8Array, expectedSource?: Uint8Array): NativeIndexedVersion;
  keepLast(count: string): {
    retainedSourceVersions: Uint8Array[]; removedSourceVersions: Uint8Array[];
    retainedIndexVersions: Uint8Array[]; removedIndexVersions: Uint8Array[];
    removedCatalogVersions: Uint8Array[]; removedCheckpointRecords: string;
    removedNamedRoots: Uint8Array[];
  };
  planGc(): any;
}

interface NativeIndexVerification {
  name: Uint8Array; sourceVersion: Uint8Array; expectedIndexVersion: Uint8Array;
  actualIndexVersion: Uint8Array; expectedEntries: string; actualEntries: string;
  semanticDifferences: string; valid: boolean; canonical: boolean;
}

interface NativeIndexPage {
  matches: NativeIndexMatch[];
  nextCursor?: Uint8Array;
}

interface NativeIndexedSnapshot {
  id(): NativeIndexedSnapshotId;
  index(name: Uint8Array): NativeSecondaryIndex;
}
interface NativeSecondaryIndex {
  name(): Uint8Array;
  fastHandle(): string;
  exact(term: Uint8Array): NativeIndexMatch[];
  prefix(prefix: Uint8Array): NativeIndexMatch[];
  range(start: Uint8Array, end?: Uint8Array): NativeIndexMatch[];
  records(term: Uint8Array): NativeIndexedSource[];
  exactPage(term: Uint8Array, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
  exactReversePage(term: Uint8Array, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
  prefixPage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
  prefixReversePage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
  rangePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
  rangeReversePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: string): NativeIndexPage;
}

function indexedVersion(value: NativeIndexedVersion): IndexedVersion {
  return {
    sourceVersion: value.sourceVersion,
    catalogVersion: value.catalogVersion,
    indexCount: BigInt(value.indexCount),
  };
}

function indexedUpdate(value: NativeIndexedUpdate): IndexedUpdate {
  return {
    kind: value.kind,
    previousSourceVersion: value.previousSourceVersion,
    current: value.current == null ? undefined : indexedVersion(value.current),
  };
}

function ownMutations(mutations: readonly IndexedMutation[]) {
  return mutations.map((mutation) => ({
    kind: mutation.kind,
    key: ownedBytes(mutation.key),
    value: mutation.kind === "upsert" ? ownedBytes(mutation.value) : undefined,
  }));
}

function indexVerification(value: NativeIndexVerification): IndexVerification {
  return {
    name: value.name, sourceVersion: value.sourceVersion,
    expectedIndexVersion: value.expectedIndexVersion, actualIndexVersion: value.actualIndexVersion,
    expectedEntries: BigInt(value.expectedEntries), actualEntries: BigInt(value.actualEntries),
    semanticDifferences: BigInt(value.semanticDifferences), valid: value.valid, canonical: value.canonical,
  };
}

export class IndexedMap implements Disposable {
  #native?: NativeIndexedMap;
  constructor(native: NativeIndexedMap) { this.#native = native; }
  #open(): NativeIndexedMap {
    if (this.#native == null) throw new Error("indexed map is closed");
    return this.#native;
  }
  id(): Uint8Array { return this.#open().id(); }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.get(key) ?? undefined);
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open(); key = ownedBytes(key); value = ownedBytes(value);
    return nativePromise(signal, () => indexedVersion(native.put(key, value)));
  }
  apply(mutations: readonly IndexedMutation[], signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open();
    const owned = ownMutations(mutations);
    return nativePromise(signal, () => indexedVersion(native.apply(owned)));
  }
  applyIf(expectedSource: Uint8Array | undefined, mutations: readonly IndexedMutation[], signal?: AbortSignal): Promise<IndexedUpdate> {
    const native = this.#open();
    expectedSource = expectedSource == null ? undefined : ownedBytes(expectedSource);
    const owned = ownMutations(mutations);
    return nativePromise(signal, () => indexedUpdate(native.applyIf(expectedSource, owned)));
  }
  delete(key: Uint8Array, signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => indexedVersion(native.delete(key)));
  }
  ensureIndex(name: Uint8Array, signal?: AbortSignal): Promise<IndexBuildResult> {
    const native = this.#open(); name = ownedBytes(name);
    return nativePromise(signal, () => {
      const value = native.ensureIndex(name);
      return {
        sourceVersion: value.sourceVersion, indexVersion: value.indexVersion,
        catalogVersion: value.catalogVersion, generation: BigInt(value.generation),
        entries: BigInt(value.entries), attempts: BigInt(value.attempts), activated: value.activated,
      };
    });
  }
  snapshot(signal?: AbortSignal): Promise<IndexedSnapshot> {
    const native = this.#open();
    return nativePromise(signal, () => new IndexedSnapshot(native.snapshot()));
  }
  snapshotAt(sourceVersion: Uint8Array, signal?: AbortSignal): Promise<IndexedSnapshot> {
    const native = this.#open(); sourceVersion = ownedBytes(sourceVersion);
    return nativePromise(signal, () => new IndexedSnapshot(native.snapshotAt(sourceVersion)));
  }
  snapshotById(id: IndexedSnapshotId, signal?: AbortSignal): Promise<IndexedSnapshot> {
    const native = this.#open();
    const owned = { sourceVersion: ownedBytes(id.sourceVersion), catalogVersion: ownedBytes(id.catalogVersion) };
    return nativePromise(signal, () => new IndexedSnapshot(native.snapshotById(owned)));
  }
  health(): IndexedMapHealth {
    const value = this.#open().health();
    return {
      sourceMapId: value.sourceMapId, sourceVersion: value.sourceVersion,
      catalogVersion: value.catalogVersion, supportsTransactions: value.supportsTransactions,
      activeIndexes: value.activeIndexes.map((index) => ({
        name: index.name, generation: BigInt(index.generation), fingerprint: index.fingerprint,
        projection: index.projection, indexMapId: index.indexMapId, indexVersion: index.indexVersion,
      })),
    };
  }
  metrics(): IndexedMapMetrics {
    const value = this.#open().metrics();
    return Object.fromEntries(Object.entries(value).map(([key, count]) => [key, BigInt(count)])) as unknown as IndexedMapMetrics;
  }
  verifyIndex(name: Uint8Array, sourceVersion: Uint8Array): IndexVerification {
    return indexVerification(this.#open().verifyIndex(ownedBytes(name), ownedBytes(sourceVersion)));
  }
  verifyAll(sourceVersion: Uint8Array): IndexVerification[] {
    return this.#open().verifyAll(ownedBytes(sourceVersion)).map(indexVerification);
  }
  repairIndex(name: Uint8Array, sourceVersion: Uint8Array): IndexVerification {
    return indexVerification(this.#open().repairIndex(ownedBytes(name), ownedBytes(sourceVersion)));
  }
  deactivateIndex(name: Uint8Array, signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open(); name = ownedBytes(name);
    return nativePromise(signal, () => indexedVersion(native.deactivateIndex(name)));
  }
  exportCurrent(): Uint8Array { return this.#open().exportCurrent(); }
  importCurrent(bundle: Uint8Array, expectedSource?: Uint8Array, signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open(); bundle = ownedBytes(bundle);
    expectedSource = expectedSource == null ? undefined : ownedBytes(expectedSource);
    return nativePromise(signal, () => indexedVersion(native.importCurrent(bundle, expectedSource)));
  }
  keepLast(count: bigint): IndexedRetention {
    const value = this.#open().keepLast(count.toString());
    return {
      retainedSourceVersions: value.retainedSourceVersions,
      removedSourceVersions: value.removedSourceVersions,
      retainedIndexVersions: value.retainedIndexVersions,
      removedIndexVersions: value.removedIndexVersions,
      removedCatalogVersions: value.removedCatalogVersions,
      removedCheckpointRecords: BigInt(value.removedCheckpointRecords),
      removedNamedRoots: value.removedNamedRoots,
    };
  }
  planGc(): GcPlan { return gcPlan(this.#open().planGc()); }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class IndexedSnapshot implements Disposable {
  #native?: NativeIndexedSnapshot;
  constructor(native: NativeIndexedSnapshot) { this.#native = native; }
  id(): IndexedSnapshotId {
    if (this.#native == null) throw new Error("indexed snapshot is closed");
    return this.#native.id();
  }
  index(name: Uint8Array): SecondaryIndex {
    if (this.#native == null) throw new Error("indexed snapshot is closed");
    return new SecondaryIndex(this.#native.index(ownedBytes(name)));
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

function indexMatch(value: NativeIndexMatch): IndexMatch {
  return { term: value.term, primaryKey: value.primaryKey, projection: value.projection };
}

export class SecondaryIndex implements Disposable {
  #native?: NativeSecondaryIndex;
  constructor(native: NativeSecondaryIndex) { this.#native = native; }
  #open(): NativeSecondaryIndex {
    if (this.#native == null) throw new Error("secondary index is closed");
    return this.#native;
  }
  name(): Uint8Array { return this.#open().name(); }
  fastHandle(): bigint { return BigInt(this.#open().fastHandle()); }
  exact(term: Uint8Array, signal?: AbortSignal): Promise<IndexMatch[]> {
    const native = this.#open(); term = ownedBytes(term);
    return nativePromise(signal, () => native.exact(term).map(indexMatch));
  }
  prefix(prefix: Uint8Array, signal?: AbortSignal): Promise<IndexMatch[]> {
    const native = this.#open(); prefix = ownedBytes(prefix);
    return nativePromise(signal, () => native.prefix(prefix).map(indexMatch));
  }
  range(start: Uint8Array, end?: Uint8Array, signal?: AbortSignal): Promise<IndexMatch[]> {
    const native = this.#open(); start = ownedBytes(start); end = end == null ? undefined : ownedBytes(end);
    return nativePromise(signal, () => native.range(start, end).map(indexMatch));
  }
  records(term: Uint8Array, signal?: AbortSignal): Promise<IndexedSource[]> {
    const native = this.#open(); term = ownedBytes(term);
    return nativePromise(signal, () => native.records(term).map((value) => ({
      ...indexMatch(value), sourceValue: value.sourceValue,
    })));
  }

  async *#pages(
    next: (cursor: Uint8Array | undefined, limit: string) => NativeIndexPage,
    options: IndexPageOptions,
  ): AsyncIterable<IndexMatch[]> {
    const pageSize = options.pageSize ?? 256;
    if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
      throw new RangeError("index pageSize must be a positive safe integer");
    }
    let cursor: Uint8Array | undefined;
    do {
      const page = await nativePromise(options.signal, () => next(cursor, pageSize.toString()));
      yield page.matches.map(indexMatch);
      cursor = page.nextCursor;
    } while (cursor != null);
  }

  exactPage(term: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); term = ownedBytes(term); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.exactPage(term, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  exactReversePage(term: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); term = ownedBytes(term); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.exactReversePage(term, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  prefixPage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); prefix = ownedBytes(prefix); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.prefixPage(prefix, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  prefixReversePage(prefix: Uint8Array, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); prefix = ownedBytes(prefix); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.prefixReversePage(prefix, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  rangePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); start = ownedBytes(start); end = end == null ? undefined : ownedBytes(end); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.rangePage(start, end, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  rangeReversePage(start: Uint8Array, end: Uint8Array | undefined, cursor: Uint8Array | undefined, limit: bigint, signal?: AbortSignal): Promise<{ matches: IndexMatch[]; nextCursor?: Uint8Array }> {
    const native = this.#open(); start = ownedBytes(start); end = end == null ? undefined : ownedBytes(end); cursor = cursor == null ? undefined : ownedBytes(cursor);
    return nativePromise(signal, () => {
      const page = native.rangeReversePage(start, end, cursor, limit.toString());
      return { matches: page.matches.map(indexMatch), nextCursor: page.nextCursor };
    });
  }
  exactPages(term: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); term = ownedBytes(term);
    return this.#pages((cursor, limit) => native.exactPage(term, cursor, limit), options);
  }
  exactReversePages(term: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); term = ownedBytes(term);
    return this.#pages((cursor, limit) => native.exactReversePage(term, cursor, limit), options);
  }
  prefixPages(prefix: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); prefix = ownedBytes(prefix);
    return this.#pages((cursor, limit) => native.prefixPage(prefix, cursor, limit), options);
  }
  prefixReversePages(prefix: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); prefix = ownedBytes(prefix);
    return this.#pages((cursor, limit) => native.prefixReversePage(prefix, cursor, limit), options);
  }
  rangePages(start: Uint8Array, end?: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); start = ownedBytes(start); end = end == null ? undefined : ownedBytes(end);
    return this.#pages((cursor, limit) => native.rangePage(start, end, cursor, limit), options);
  }
  rangeReversePages(start: Uint8Array, end?: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open(); start = ownedBytes(start); end = end == null ? undefined : ownedBytes(end);
    return this.#pages((cursor, limit) => native.rangeReversePage(start, end, cursor, limit), options);
  }

  async exactView(
    term: Uint8Array,
    visit: (row: IndexMatchView) => boolean | void,
    signal?: AbortSignal,
  ): Promise<void> {
    for await (const rows of this.exactPages(term, { signal })) {
      for (const row of rows) {
        const scope: ViewScope = { alive: true };
        try {
          const view = {
            term: scopedBytes(row.term, scope),
            primaryKey: scopedBytes(row.primaryKey, scope),
            projection: row.projection == null ? undefined : scopedBytes(row.projection, scope),
          };
          if (visit(view) === false) return;
        } finally {
          scope.alive = false;
        }
      }
    }
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}
