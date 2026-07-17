import { nativePromise, ownedBytes } from "./packed.ts";

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
  head(): NativeMapVersion | null;
  headId(): Uint8Array | null;
  version(id: Uint8Array): NativeMapVersion | null;
  versions(): NativeMapVersion[];
  get(key: Uint8Array): Uint8Array | null;
  containsKey(key: Uint8Array): boolean;
  getMany(keys: Uint8Array[]): Array<Uint8Array | null>;
  getAt(id: Uint8Array, key: Uint8Array): Uint8Array | null;
  getManyAt(id: Uint8Array, keys: Uint8Array[]): Array<Uint8Array | null>;
  put(key: Uint8Array, value: Uint8Array): NativeMapVersion;
  apply(mutations: MapMutation[]): NativeMapVersion;
  applyIf(expected: Uint8Array | null, mutations: MapMutation[]): NativeMapUpdate;
  putIf(expected: Uint8Array | null, key: Uint8Array, value: Uint8Array): NativeMapUpdate;
  deleteIf(expected: Uint8Array | null, key: Uint8Array): NativeMapUpdate;
  delete(key: Uint8Array): NativeMapVersion;
  snapshot(): NativeMapSnapshot | null;
  snapshotAt(id: Uint8Array): NativeMapSnapshot | null;
  backup(): Uint8Array;
  restoreBackup(bytes: Uint8Array): NativeMapVersion;
  keepLast(count: number): { retained: Uint8Array[]; removed: Uint8Array[] };
  verifyCatalog(): NativeMaintenanceSummary;
  planGc(): NativeMaintenanceSummary;
}

interface NativeMapUpdate {
  kind: "applied" | "unchanged" | "conflict";
  previous?: Uint8Array;
  current?: NativeMapVersion;
}

interface NativeMaintenanceSummary { itemCount: string; byteCount: string; }
interface NativeMapSnapshot {
  id(): Uint8Array;
  version(): NativeMapVersion;
  get(key: Uint8Array): Uint8Array | null;
  proveKey(key: Uint8Array): NativeKeyProof;
  stats(): NativeMaintenanceSummary;
  export(): NativeMaintenanceSummary;
  read(): NativeReadSession;
}
interface NativeReadSession { get(key: Uint8Array): Uint8Array | null; }
interface NativeKeyProof {
  verify(): { valid: boolean; exists: boolean; value?: Uint8Array };
}

export interface MaintenanceSummary { itemCount: bigint; byteCount: bigint; }

function maintenance(value: NativeMaintenanceSummary): MaintenanceSummary {
  return { itemCount: BigInt(value.itemCount), byteCount: BigInt(value.byteCount) };
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

  applyIf(expected: Uint8Array | undefined, mutations: readonly MapMutation[], signal?: AbortSignal): Promise<MapUpdate> {
    const native = this.#open();
    const ownedExpected = expected == null ? null : ownedBytes(expected);
    const owned = ownedMutations(mutations);
    return nativePromise(signal, () => mapUpdate(native.applyIf(ownedExpected, owned)));
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

  backup(signal?: AbortSignal): Promise<Uint8Array> {
    const native = this.#open();
    return nativePromise(signal, () => native.backup());
  }

  restoreBackup(bytes: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open(); bytes = ownedBytes(bytes);
    return nativePromise(signal, () => mapVersion(native.restoreBackup(bytes)));
  }

  keepLast(count: number, signal?: AbortSignal): Promise<VersionPrune> {
    if (!Number.isSafeInteger(count) || count < 0 || count > 0xffff_ffff) {
      return Promise.reject(new RangeError("keepLast count must be a non-negative uint32"));
    }
    const native = this.#open();
    return nativePromise(signal, () => native.keepLast(count));
  }

  verifyCatalog(signal?: AbortSignal): Promise<MaintenanceSummary> {
    const native = this.#open();
    return nativePromise(signal, () => maintenance(native.verifyCatalog()));
  }

  planGc(signal?: AbortSignal): Promise<MaintenanceSummary> {
    const native = this.#open();
    return nativePromise(signal, () => maintenance(native.planGc()));
  }

  close(): void {
    this.#native = undefined;
  }

  [Symbol.dispose](): void {
    this.close();
  }
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
  proveKey(key: Uint8Array): KeyProof {
    const native = this.#open();
    return new KeyProof(native.proveKey(ownedBytes(key)));
  }
  stats(): MaintenanceSummary { return maintenance(this.#open().stats()); }
  exportSummary(): MaintenanceSummary { return maintenance(this.#open().export()); }
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
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class KeyProof implements Disposable {
  #native?: NativeKeyProof;
  constructor(native: NativeKeyProof) { this.#native = native; }
  verify(): { valid: boolean; exists: boolean; value?: Uint8Array } {
    if (this.#native == null) throw new Error("key proof is closed");
    return this.#native.verify();
  }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}
