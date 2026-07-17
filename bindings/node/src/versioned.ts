import { nativePromise, ownedBytes } from "./packed.ts";

export interface MapVersion {
  id: Uint8Array;
  tree: unknown;
  createdAtMillis?: bigint;
  isHead: boolean;
}

interface NativeMapVersion {
  id: Uint8Array;
  tree: unknown;
  createdAtMillis?: string;
  isHead: boolean;
}

interface NativeVersionedMap {
  initialize(): NativeMapVersion;
  get(key: Uint8Array): Uint8Array | null;
  put(key: Uint8Array, value: Uint8Array): NativeMapVersion;
  delete(key: Uint8Array): NativeMapVersion;
  snapshot(): NativeMapSnapshot | null;
  backup(): Uint8Array;
  verifyCatalog(): NativeMaintenanceSummary;
  planGc(): NativeMaintenanceSummary;
}

interface NativeMaintenanceSummary { itemCount: string; byteCount: string; }
interface NativeMapSnapshot {
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

export class VersionedMap implements Disposable {
  #native?: NativeVersionedMap;

  constructor(native: NativeVersionedMap) {
    this.#native = native;
  }

  #open(): NativeVersionedMap {
    if (this.#native == null) throw new Error("versioned map is closed");
    return this.#native;
  }

  initialize(signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open();
    return nativePromise(signal, () => mapVersion(native.initialize()));
  }

  get(key: Uint8Array, signal?: AbortSignal): Promise<Uint8Array | undefined> {
    const native = this.#open();
    key = ownedBytes(key);
    return nativePromise(signal, () => native.get(key) ?? undefined);
  }

  put(key: Uint8Array, value: Uint8Array, signal?: AbortSignal): Promise<MapVersion> {
    const native = this.#open();
    key = ownedBytes(key);
    value = ownedBytes(value);
    return nativePromise(signal, () => mapVersion(native.put(key, value)));
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

  backup(signal?: AbortSignal): Promise<Uint8Array> {
    const native = this.#open();
    return nativePromise(signal, () => native.backup());
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
