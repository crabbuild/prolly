import { nativePromise, ownedBytes, scopedBytes, type ViewScope } from "./packed.ts";

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

export interface IndexBuildResult {
  sourceVersion: Uint8Array;
  indexVersion: Uint8Array;
  catalogVersion: Uint8Array;
  generation: bigint;
  entries: bigint;
  attempts: bigint;
  activated: boolean;
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
  get(key: Uint8Array): Uint8Array | null;
  put(key: Uint8Array, value: Uint8Array): NativeIndexedVersion;
  delete(key: Uint8Array): NativeIndexedVersion;
  ensureIndex(name: Uint8Array): NativeIndexBuildResult;
  snapshot(): NativeIndexedSnapshot;
}

interface NativeIndexedSnapshot { index(name: Uint8Array): NativeSecondaryIndex; }
interface NativeSecondaryIndex {
  exact(term: Uint8Array): NativeIndexMatch[];
  prefix(prefix: Uint8Array): NativeIndexMatch[];
  range(start: Uint8Array, end?: Uint8Array): NativeIndexMatch[];
  records(term: Uint8Array): NativeIndexedSource[];
  exactPage(term: Uint8Array, cursor: Uint8Array | undefined, limit: string): {
    matches: NativeIndexMatch[];
    nextCursor?: Uint8Array;
  };
}

function indexedVersion(value: NativeIndexedVersion): IndexedVersion {
  return {
    sourceVersion: value.sourceVersion,
    catalogVersion: value.catalogVersion,
    indexCount: BigInt(value.indexCount),
  };
}

export class IndexedMap implements Disposable {
  #native?: NativeIndexedMap;
  constructor(native: NativeIndexedMap) { this.#native = native; }
  #open(): NativeIndexedMap {
    if (this.#native == null) throw new Error("indexed map is closed");
    return this.#native;
  }
  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open(); key = ownedBytes(key);
    return nativePromise(signal, () => native.get(key) ?? undefined);
  }
  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<IndexedVersion> {
    const native = this.#open(); key = ownedBytes(key); value = ownedBytes(value);
    return nativePromise(signal, () => indexedVersion(native.put(key, value)));
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
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class IndexedSnapshot implements Disposable {
  #native?: NativeIndexedSnapshot;
  constructor(native: NativeIndexedSnapshot) { this.#native = native; }
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

  async *exactPages(term: Uint8Array, options: IndexPageOptions = {}): AsyncIterable<IndexMatch[]> {
    const native = this.#open();
    term = ownedBytes(term);
    const pageSize = options.pageSize ?? 256;
    if (!Number.isSafeInteger(pageSize) || pageSize <= 0) {
      throw new RangeError("index pageSize must be a positive safe integer");
    }
    let cursor: Uint8Array | undefined;
    do {
      const page = await nativePromise(options.signal, () =>
        native.exactPage(term, cursor, pageSize.toString()));
      yield page.matches.map(indexMatch);
      cursor = page.nextCursor;
    } while (cursor != null);
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
