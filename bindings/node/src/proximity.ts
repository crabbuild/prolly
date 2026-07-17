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

interface NativeProximityMap {
  search(vector: Float32Array, topK: string): SearchResult;
}

export class ProximityMap implements Disposable {
  #native?: NativeProximityMap;
  constructor(native: NativeProximityMap) { this.#native = native; }
  nativeHandle(): NativeProximityMap {
    if (this.#native == null) throw new Error("proximity map is closed");
    return this.#native;
  }
  read(): ProximityReadSession { return new ProximityReadSession(this.nativeHandle()); }
  search(request: SearchRequest): Promise<SearchResult> { return this.read().search(request); }
  close(): void { this.#native = undefined; }
  [Symbol.dispose](): void { this.close(); }
}

export class ProximityReadSession implements Disposable {
  #native?: NativeProximityMap;
  constructor(native: NativeProximityMap) { this.#native = native; }
  search(request: SearchRequest): Promise<SearchResult> {
    if (this.#native == null) return Promise.reject(new Error("proximity session is closed"));
    const native = this.#native;
    const vector = new Float32Array(request.vector);
    return nativePromise(request.signal, () => native.search(vector, request.topK.toString()));
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
