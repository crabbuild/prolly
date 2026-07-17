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
  descriptor(): Uint8Array;
  verify(): string;
  proveMembership(key: Uint8Array): NativeProximityProof;
}
interface NativeProximityProof { verify(expectedDescriptor?: Uint8Array): Uint8Array | null; }

export class ProximityMap implements Disposable {
  #native?: NativeProximityMap;
  constructor(native: NativeProximityMap) { this.#native = native; }
  nativeHandle(): NativeProximityMap {
    if (this.#native == null) throw new Error("proximity map is closed");
    return this.#native;
  }
  read(): ProximityReadSession { return new ProximityReadSession(this.nativeHandle()); }
  search(request: SearchRequest): Promise<SearchResult> { return this.read().search(request); }
  descriptor(): Uint8Array { return this.nativeHandle().descriptor(); }
  verify(): { recordCount: bigint } { return { recordCount: BigInt(this.nativeHandle().verify()) }; }
  proveMembership(key: Uint8Array): ProximityMembershipProof {
    return new ProximityMembershipProof(this.nativeHandle().proveMembership(ownedBytes(key)));
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
