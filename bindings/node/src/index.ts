import { createHash } from "node:crypto";

const MASK64 = (1n << 64n) - 1n;
const PRIME64_1 = 11400714785074694791n;
const PRIME64_2 = 14029467366897019727n;
const PRIME64_3 = 1609587929392839161n;
const PRIME64_4 = 9650029242287828579n;
const PRIME64_5 = 2870177450012600261n;

export type EncodingKind = "raw" | "cbor" | "json" | "custom";

export interface EncodingRecord {
  kind: EncodingKind;
  customName?: string;
}

export interface LargeValueConfig {
  inlineThreshold: number;
}

export interface ParallelConfig {
  maxThreads: number;
  parallelismThreshold: number;
}

export interface RangeBounds {
  start: Uint8Array;
  end?: Uint8Array;
}

export interface ChangedSpan {
  start: Uint8Array;
  end?: Uint8Array;
}

export function fromHex(value: string): Uint8Array {
  return Uint8Array.from(Buffer.from(value, "hex"));
}

export function toHex(value: Uint8Array): string {
  return Buffer.from(value).toString("hex");
}

export function concatBytes(parts: Uint8Array[]): Uint8Array {
  const len = parts.reduce((sum, part) => sum + part.length, 0);
  const out = new Uint8Array(len);
  let offset = 0;
  for (const part of parts) {
    out.set(part, offset);
    offset += part.length;
  }
  return out;
}

export class Cid {
  readonly bytes: Uint8Array;

  constructor(bytes: Uint8Array) {
    if (bytes.length !== 32) {
      throw new Error("CID must be 32 bytes");
    }
    this.bytes = Uint8Array.from(bytes);
  }

  static fromBytes(data: Uint8Array): Cid {
    return new Cid(createHash("sha256").update(data).digest());
  }

  static fromHex(value: string): Cid {
    return new Cid(fromHex(value));
  }

  hex(): string {
    return toHex(this.bytes);
  }
}

export class Config {
  minChunkSize: number;
  maxChunkSize: number;
  chunkingFactor: number;
  hashSeed: bigint;
  encoding: EncodingKind;
  customEncodingName?: string;
  nodeCacheMaxNodes?: number;
  nodeCacheMaxBytes?: number;

  constructor(options: Partial<Config> = {}) {
    this.minChunkSize = options.minChunkSize ?? 4;
    this.maxChunkSize = options.maxChunkSize ?? 1024 * 1024;
    this.chunkingFactor = options.chunkingFactor ?? 128;
    this.hashSeed = options.hashSeed ?? 0n;
    this.encoding = options.encoding ?? "raw";
    this.customEncodingName = options.customEncodingName;
    this.nodeCacheMaxNodes = options.nodeCacheMaxNodes;
    this.nodeCacheMaxBytes = options.nodeCacheMaxBytes;
  }

  static fromFixture(fixture: any): Config {
    return new Config({
      minChunkSize: fixture.min_chunk_size,
      maxChunkSize: fixture.max_chunk_size,
      chunkingFactor: fixture.chunking_factor,
      hashSeed: BigInt(fixture.hash_seed),
      encoding: fixture.encoding?.kind ?? "raw",
      customEncodingName: fixture.encoding?.custom_name ?? undefined,
      nodeCacheMaxNodes: fixture.node_cache_max_nodes ?? undefined,
      nodeCacheMaxBytes: fixture.node_cache_max_bytes ?? undefined,
    });
  }
}

export function encodingRaw(): EncodingRecord {
  return { kind: "raw" };
}

export function encodingCbor(): EncodingRecord {
  return { kind: "cbor" };
}

export function encodingJson(): EncodingRecord {
  return { kind: "json" };
}

export function encodingCustom(name: string): EncodingRecord {
  return { kind: "custom", customName: name };
}

export function treeConfig(options: {
  minChunkSize: number;
  maxChunkSize: number;
  chunkingFactor: number;
  hashSeed: bigint;
  encoding: EncodingRecord;
  nodeCacheMaxNodes?: number;
  nodeCacheMaxBytes?: number;
}): Config {
  if (options.encoding.kind === "custom" && options.encoding.customName == null) {
    throw new Error("custom encoding requires customName");
  }
  return new Config({
    minChunkSize: options.minChunkSize,
    maxChunkSize: options.maxChunkSize,
    chunkingFactor: options.chunkingFactor,
    hashSeed: options.hashSeed,
    encoding: options.encoding.kind,
    customEncodingName: options.encoding.customName,
    nodeCacheMaxNodes: options.nodeCacheMaxNodes,
    nodeCacheMaxBytes: options.nodeCacheMaxBytes,
  });
}

export function largeValueConfig(inlineThreshold: number): LargeValueConfig {
  return { inlineThreshold };
}

export function parallelConfig(maxThreads: number, parallelismThreshold: number): ParallelConfig {
  return { maxThreads, parallelismThreshold };
}

export function parallelConfigSequential(): ParallelConfig {
  return { maxThreads: 1, parallelismThreshold: Number.MAX_SAFE_INTEGER };
}

export class ProllyNode {
  keys: Uint8Array[];
  vals: Uint8Array[];
  leaf: boolean;
  level: number;
  minChunkSize: number;
  maxChunkSize: number;
  chunkingFactor: number;
  hashSeed: bigint;
  encoding: EncodingKind;
  customEncodingName?: string;

  constructor(options: {
    keys: Uint8Array[];
    vals: Uint8Array[];
    leaf?: boolean;
    level?: number;
    minChunkSize?: number;
    maxChunkSize?: number;
    chunkingFactor?: number;
    hashSeed?: bigint;
    encoding?: EncodingKind;
    customEncodingName?: string;
  }) {
    this.keys = options.keys.map((key) => Uint8Array.from(key));
    this.vals = options.vals.map((value) => Uint8Array.from(value));
    this.leaf = options.leaf ?? true;
    this.level = options.level ?? 0;
    this.minChunkSize = options.minChunkSize ?? 4;
    this.maxChunkSize = options.maxChunkSize ?? 1024 * 1024;
    this.chunkingFactor = options.chunkingFactor ?? 128;
    this.hashSeed = options.hashSeed ?? 0n;
    this.encoding = options.encoding ?? "raw";
    this.customEncodingName = options.customEncodingName;
  }

  static fromBytes(data: Uint8Array): ProllyNode {
    if (toHex(data.slice(0, 4)) !== "43524142") {
      throw new Error("legacy CBOR node decoding is not implemented in the TypeScript port");
    }
    const cursor = new Cursor(data, 4);
    const version = cursor.readVarintNumber();
    if (version !== 1) throw new Error(`unsupported compact node version ${version}`);
    const leafFlag = cursor.readVarintNumber();
    if (leafFlag !== 0 && leafFlag !== 1) throw new Error(`invalid leaf flag ${leafFlag}`);
    const leaf = leafFlag === 1;
    const level = cursor.readVarintNumber();
    const minChunkSize = cursor.readVarintNumber();
    const maxChunkSize = cursor.readVarintNumber();
    const chunkingFactor = cursor.readVarintNumber();
    const hashSeed = cursor.readVarintBigint();
    const encoding = cursor.readEncoding();
    const entryCount = cursor.readVarintNumber();
    const keys: Uint8Array[] = [];
    const vals: Uint8Array[] = [];
    let previous = new Uint8Array();

    for (let entry = 0; entry < entryCount; entry++) {
      const shared = cursor.readVarintNumber();
      if (shared > previous.length) throw new Error("shared key prefix exceeds previous key");
      const suffix = cursor.readBytes(cursor.readVarintNumber());
      const key = concatBytes([previous.slice(0, shared), suffix]);
      let value: Uint8Array;
      if (leaf) {
        value = cursor.readBytes(cursor.readVarintNumber());
      } else {
        const tag = cursor.readByte();
        if (tag === 0) {
          value = cursor.readBytes(32);
        } else if (tag === 1) {
          value = cursor.readBytes(cursor.readVarintNumber());
        } else {
          throw new Error(`invalid internal value tag ${tag}`);
        }
      }
      keys.push(key);
      vals.push(value);
      previous = key;
    }
    if (!cursor.done()) throw new Error("trailing bytes in compact node");

    return new ProllyNode({
      keys,
      vals,
      leaf,
      level,
      minChunkSize,
      maxChunkSize,
      chunkingFactor,
      hashSeed,
      encoding: encoding.kind,
      customEncodingName: encoding.customName,
    });
  }

  toBytes(): Uint8Array {
    const out: number[] = [...new TextEncoder().encode("CRAB")];
    writeVarint(out, 1);
    writeVarint(out, this.leaf ? 1 : 0);
    writeVarint(out, this.level);
    writeVarint(out, this.minChunkSize);
    writeVarint(out, this.maxChunkSize);
    writeVarint(out, this.chunkingFactor);
    writeVarint(out, this.hashSeed);
    writeEncoding(out, this.encoding, this.customEncodingName);
    writeVarint(out, this.keys.length);
    let previous = new Uint8Array();
    for (let index = 0; index < this.keys.length; index++) {
      const key = this.keys[index];
      const value = this.vals[index];
      const shared = commonPrefixLen(previous, key);
      const suffix = key.slice(shared);
      writeVarint(out, shared);
      writeVarint(out, suffix.length);
      pushBytes(out, suffix);
      if (this.leaf) {
        writeVarint(out, value.length);
        pushBytes(out, value);
      } else if (value.length === 32) {
        out.push(0);
        pushBytes(out, value);
      } else {
        out.push(1);
        writeVarint(out, value.length);
        pushBytes(out, value);
      }
      previous = key;
    }
    return Uint8Array.from(out);
  }

  cid(): Cid {
    return Cid.fromBytes(this.toBytes());
  }

  search(key: Uint8Array): { found: boolean; index: number } {
    let lo = 0;
    let hi = this.keys.length;
    while (lo < hi) {
      const mid = Math.floor((lo + hi) / 2);
      const cmp = compareBytes(this.keys[mid], key);
      if (cmp < 0) lo = mid + 1;
      else if (cmp > 0) hi = mid;
      else return { found: true, index: mid };
    }
    return { found: false, index: lo };
  }
}

export class Tree {
  root: Cid | null;
  config: Config;

  constructor(root: Cid | null, config: Config) {
    this.root = root;
    this.config = config;
  }

  static fromFixture(fixture: any): Tree {
    return new Tree(fixture.root ? Cid.fromHex(fixture.root) : null, Config.fromFixture(fixture.config));
  }
}

export class MemoryStore {
  data = new Map<string, Uint8Array>();

  get(key: Uint8Array): Uint8Array | undefined {
    const value = this.data.get(toHex(key));
    return value ? Uint8Array.from(value) : undefined;
  }

  put(key: Uint8Array, value: Uint8Array): void {
    this.data.set(toHex(key), Uint8Array.from(value));
  }

  delete(key: Uint8Array): void {
    this.data.delete(toHex(key));
  }

  static fromFixture(fixture: any): MemoryStore {
    const store = new MemoryStore();
    for (const entry of fixture.store) {
      store.put(fromHex(entry.cid), fromHex(entry.bytes));
    }
    return store;
  }
}

export class Prolly {
  store: MemoryStore;
  config: Config;

  constructor(store: MemoryStore, config = new Config()) {
    this.store = store;
    this.config = config;
  }

  get(tree: Tree, key: Uint8Array): Uint8Array | undefined {
    if (tree.root === null) return undefined;
    let cid = tree.root;
    while (true) {
      const node = this.load(cid);
      let { found, index } = node.search(key);
      if (!found) {
        if (index === 0) return undefined;
        index -= 1;
      }
      if (node.leaf) {
        return compareBytes(node.keys[index], key) === 0 ? node.vals[index] : undefined;
      }
      cid = new Cid(node.vals[index]);
    }
  }

  range(tree: Tree, start = new Uint8Array(), end?: Uint8Array): [Uint8Array, Uint8Array][] {
    return this.entries(tree).filter(([key]) => compareBytes(key, start) >= 0 && (!end || compareBytes(key, end) < 0));
  }

  entries(tree: Tree): [Uint8Array, Uint8Array][] {
    if (tree.root === null) return [];
    const entries = this.entriesFromNode(this.load(tree.root));
    entries.sort(([left], [right]) => compareBytes(left, right));
    return entries;
  }

  diff(base: Tree, other: Tree): any[] {
    return diffEntries(this.entries(base), this.entries(other));
  }

  load(cid: Cid): ProllyNode {
    const bytes = this.store.get(cid.bytes);
    if (!bytes) throw new Error(`missing node ${cid.hex()}`);
    const node = ProllyNode.fromBytes(bytes);
    if (node.cid().hex() !== cid.hex()) throw new Error(`CID mismatch for ${cid.hex()}`);
    return node;
  }

  entriesFromNode(node: ProllyNode): [Uint8Array, Uint8Array][] {
    if (node.leaf) return node.keys.map((key, index) => [key, node.vals[index]]);
    return node.vals.flatMap((value) => this.entriesFromNode(this.load(new Cid(value))));
  }
}

export class VersionedValue {
  schema: string;
  version: bigint;
  encoding: EncodingKind;
  payload: Uint8Array;
  customEncodingName?: string;

  constructor(schema: string, version: bigint, encoding: EncodingKind, payload: Uint8Array, customEncodingName?: string) {
    this.schema = schema;
    this.version = version;
    this.encoding = encoding;
    this.payload = Uint8Array.from(payload);
    this.customEncodingName = customEncodingName;
  }

  static fromBytes(data: Uint8Array): VersionedValue {
    if (data.length < 30 || new TextDecoder().decode(data.slice(0, 4)) !== "PLVV") throw new Error("invalid versioned value envelope");
    if (data[4] !== 1) throw new Error(`unsupported versioned value wire version ${data[4]}`);
    const tag = data[5];
    const version = readU64be(data, 6);
    const schemaLen = Number(readU32be(data, 14));
    const customLen = Number(readU32be(data, 18));
    const payloadLen = Number(readU64be(data, 22));
    const schemaStart = 30;
    const customStart = schemaStart + schemaLen;
    const payloadStart = customStart + customLen;
    const expectedLen = payloadStart + payloadLen;
    if (expectedLen !== data.length) throw new Error("versioned value length mismatch");
    const schema = new TextDecoder().decode(data.slice(schemaStart, customStart));
    const custom = new TextDecoder().decode(data.slice(customStart, payloadStart));
    const encoding = decodeEncodingTag(tag, custom);
    return new VersionedValue(schema, version, encoding.kind, data.slice(payloadStart), encoding.customName);
  }

  toBytes(): Uint8Array {
    const schema = new TextEncoder().encode(this.schema);
    const custom = new TextEncoder().encode(this.customEncodingName ?? "");
    const out: number[] = [...new TextEncoder().encode("PLVV"), 1, encodingTag(this.encoding)];
    pushBytes(out, u64be(this.version));
    pushBytes(out, u32be(schema.length));
    pushBytes(out, u32be(custom.length));
    pushBytes(out, u64be(BigInt(this.payload.length)));
    pushBytes(out, schema);
    pushBytes(out, custom);
    pushBytes(out, this.payload);
    return Uint8Array.from(out);
  }
}

export class BlobRef {
  cid: Cid;
  length: bigint;

  constructor(cid: Cid, length: bigint) {
    this.cid = cid;
    this.length = length;
  }
}

export class ValueRef {
  kind: "inline" | "blob";
  value?: Uint8Array;
  blobRef?: BlobRef;

  constructor(kind: "inline" | "blob", value?: Uint8Array, blobRef?: BlobRef) {
    this.kind = kind;
    this.value = value;
    this.blobRef = blobRef;
  }

  static fromBytes(data: Uint8Array): ValueRef {
    if (data.length < 6 || new TextDecoder().decode(data.slice(0, 4)) !== "PLVB") throw new Error("invalid value ref envelope");
    if (data[4] !== 1) throw new Error(`unsupported value ref version ${data[4]}`);
    if (data[5] === 0) {
      const length = Number(readU64be(data, 6));
      const value = data.slice(14);
      if (value.length !== length) throw new Error("inline value ref length mismatch");
      return new ValueRef("inline", value);
    }
    if (data[5] === 1) {
      if (data.length !== 46) throw new Error("blob value ref length mismatch");
      return new ValueRef("blob", undefined, new BlobRef(new Cid(data.slice(6, 38)), readU64be(data, 38)));
    }
    throw new Error(`unknown value ref kind ${data[5]}`);
  }

  toBytes(): Uint8Array {
    const out: number[] = [...new TextEncoder().encode("PLVB"), 1];
    if (this.kind === "inline") {
      const value = this.value ?? new Uint8Array();
      out.push(0);
      pushBytes(out, u64be(BigInt(value.length)));
      pushBytes(out, value);
      return Uint8Array.from(out);
    }
    if (!this.blobRef) throw new Error("missing blob reference");
    out.push(1);
    pushBytes(out, this.blobRef.cid.bytes);
    pushBytes(out, u64be(this.blobRef.length));
    return Uint8Array.from(out);
  }
}

export function diffEntries(base: [Uint8Array, Uint8Array][], other: [Uint8Array, Uint8Array][]): any[] {
  const baseMap = new Map(base.map(([key, value]) => [toHex(key), value]));
  const otherMap = new Map(other.map(([key, value]) => [toHex(key), value]));
  const keys = [...new Set([...baseMap.keys(), ...otherMap.keys()])].sort();
  const out: any[] = [];
  for (const key of keys) {
    const baseValue = baseMap.get(key);
    const otherValue = otherMap.get(key);
    if (baseValue === undefined && otherValue !== undefined) out.push({ kind: "added", key, value: toHex(otherValue), old: null, new: null });
    else if (baseValue !== undefined && otherValue === undefined) out.push({ kind: "removed", key, value: toHex(baseValue), old: null, new: null });
    else if (baseValue !== undefined && otherValue !== undefined && toHex(baseValue) !== toHex(otherValue)) {
      out.push({ kind: "changed", key, value: null, old: toHex(baseValue), new: toHex(otherValue) });
    }
  }
  return out;
}

export function isBoundaryConfig(config: Config, count: number, key: Uint8Array, value: Uint8Array): boolean {
  if (count < config.minChunkSize) return false;
  if (count >= config.maxChunkSize) return true;
  const hashValue = Number(xxh64(concatBytes([key, value]), config.hashSeed) & 0xFFFF_FFFFn);
  const threshold = Math.floor(0xFFFF_FFFF / config.chunkingFactor);
  return hashValue <= threshold;
}

export function xxh64(data: Uint8Array, seed = 0n): bigint {
  const rotl = (value: bigint, bits: bigint) => ((value << bits) | (value >> (64n - bits))) & MASK64;
  const round64 = (accIn: bigint, lane: bigint) => {
    let acc = (accIn + lane * PRIME64_2) & MASK64;
    acc = rotl(acc, 31n);
    return (acc * PRIME64_1) & MASK64;
  };
  const mergeRound = (accIn: bigint, value: bigint) => ((accIn ^ round64(0n, value)) * PRIME64_1 + PRIME64_4) & MASK64;
  let index = 0;
  let h64: bigint;
  seed &= MASK64;
  if (data.length >= 32) {
    let v1 = (seed + PRIME64_1 + PRIME64_2) & MASK64;
    let v2 = (seed + PRIME64_2) & MASK64;
    let v3 = seed;
    let v4 = (seed - PRIME64_1) & MASK64;
    const limit = data.length - 32;
    while (index <= limit) {
      v1 = round64(v1, readU64le(data, index));
      v2 = round64(v2, readU64le(data, index + 8));
      v3 = round64(v3, readU64le(data, index + 16));
      v4 = round64(v4, readU64le(data, index + 24));
      index += 32;
    }
    h64 = (rotl(v1, 1n) + rotl(v2, 7n) + rotl(v3, 12n) + rotl(v4, 18n)) & MASK64;
    h64 = mergeRound(h64, v1);
    h64 = mergeRound(h64, v2);
    h64 = mergeRound(h64, v3);
    h64 = mergeRound(h64, v4);
  } else {
    h64 = (seed + PRIME64_5) & MASK64;
  }
  h64 = (h64 + BigInt(data.length)) & MASK64;
  while (index + 8 <= data.length) {
    h64 ^= round64(0n, readU64le(data, index));
    h64 = (rotl(h64, 27n) * PRIME64_1 + PRIME64_4) & MASK64;
    index += 8;
  }
  if (index + 4 <= data.length) {
    h64 ^= (readU32le(data, index) * PRIME64_1) & MASK64;
    h64 &= MASK64;
    h64 = (rotl(h64, 23n) * PRIME64_2 + PRIME64_3) & MASK64;
    index += 4;
  }
  while (index < data.length) {
    h64 ^= (BigInt(data[index]) * PRIME64_5) & MASK64;
    h64 &= MASK64;
    h64 = (rotl(h64, 11n) * PRIME64_1) & MASK64;
    index += 1;
  }
  h64 ^= h64 >> 33n;
  h64 = (h64 * PRIME64_2) & MASK64;
  h64 ^= h64 >> 29n;
  h64 = (h64 * PRIME64_3) & MASK64;
  h64 ^= h64 >> 32n;
  return h64 & MASK64;
}

export function prefixEnd(prefix: Uint8Array): Uint8Array | undefined {
  if (prefix.length === 0) return undefined;
  const end = Array.from(prefix);
  while (end.length > 0) {
    const last = end.length - 1;
    if (end[last] === 0xff) end.pop();
    else {
      end[last] += 1;
      return Uint8Array.from(end);
    }
  }
  return undefined;
}

export function prefixRange(prefix: Uint8Array): RangeBounds {
  return { start: Uint8Array.from(prefix), end: prefixEnd(prefix) };
}

export function u64Key(value: bigint): Uint8Array {
  return u64be(value);
}

export function u128Key(value: bigint): Uint8Array {
  return u128be(value);
}

export function i64Key(value: bigint): Uint8Array {
  return u64be(BigInt.asUintN(64, value) ^ (1n << 63n));
}

export function i128Key(value: bigint): Uint8Array {
  return u128be(BigInt.asUintN(128, value) ^ (1n << 127n));
}

export function encodeSegment(segment: Uint8Array): Uint8Array {
  const out: number[] = [];
  for (const byte of segment) {
    if (byte === 0) out.push(0, 0xff);
    else out.push(byte);
  }
  out.push(0, 0);
  return Uint8Array.from(out);
}

export function keyFromSegments(segments: Uint8Array[]): Uint8Array {
  return concatBytes(segments.map(encodeSegment));
}

export function keyFromPrefixedSegments(prefix: Uint8Array, segments: Uint8Array[]): Uint8Array {
  return concatBytes([prefix, ...segments.map(encodeSegment)]);
}

export function changedSpan(start: Uint8Array, end?: Uint8Array): ChangedSpan {
  return { start: Uint8Array.from(start), end: end == null ? undefined : Uint8Array.from(end) };
}

export function changedSpanFromKey(key: Uint8Array): ChangedSpan {
  return { start: Uint8Array.from(key), end: concatBytes([key, Uint8Array.of(0)]) };
}

export function changedSpanForPrefix(prefix: Uint8Array): ChangedSpan {
  return { start: Uint8Array.from(prefix), end: prefixEnd(prefix) };
}

export function decodeSegments(key: Uint8Array): Uint8Array[] {
  const segments: Uint8Array[] = [];
  const current: number[] = [];
  let offset = 0;
  while (offset < key.length) {
    const byte = key[offset];
    if (byte !== 0) {
      current.push(byte);
      offset += 1;
      continue;
    }
    if (offset + 1 >= key.length) throw new Error(`encoded key ended unexpectedly at byte offset ${offset}`);
    const marker = key[offset + 1];
    if (marker === 0) {
      segments.push(Uint8Array.from(current));
      current.length = 0;
      offset += 2;
    } else if (marker === 0xff) {
      current.push(0);
      offset += 2;
    } else {
      throw new Error(`invalid encoded key escape at byte offset ${offset}: 0x${marker.toString(16).padStart(2, "0")}`);
    }
  }
  if (current.length !== 0) throw new Error(`encoded key ended unexpectedly at byte offset ${key.length}`);
  return segments;
}

export function compareBytes(left: Uint8Array, right: Uint8Array): number {
  const len = Math.min(left.length, right.length);
  for (let i = 0; i < len; i++) {
    if (left[i] !== right[i]) return left[i] - right[i];
  }
  return left.length - right.length;
}

class Cursor {
  data: Uint8Array;
  pos: number;

  constructor(data: Uint8Array, pos = 0) {
    this.data = data;
    this.pos = pos;
  }

  done(): boolean {
    return this.pos === this.data.length;
  }

  readByte(): number {
    if (this.pos >= this.data.length) throw new Error("unexpected end of bytes");
    return this.data[this.pos++];
  }

  readBytes(length: number): Uint8Array {
    const end = this.pos + length;
    if (end > this.data.length) throw new Error("unexpected end of bytes");
    const value = this.data.slice(this.pos, end);
    this.pos = end;
    return value;
  }

  readVarintBigint(): bigint {
    let value = 0n;
    let shift = 0n;
    for (let i = 0; i < 10; i++) {
      const byte = this.readByte();
      const part = BigInt(byte & 0x7f);
      if (shift === 63n && part > 1n) throw new Error("varint overflow");
      value |= part << shift;
      if ((byte & 0x80) === 0) return value;
      shift += 7n;
    }
    throw new Error("varint overflow");
  }

  readVarintNumber(): number {
    const value = this.readVarintBigint();
    if (value > BigInt(Number.MAX_SAFE_INTEGER)) throw new Error("varint exceeds safe integer");
    return Number(value);
  }

  readEncoding(): { kind: EncodingKind; customName?: string } {
    const tag = this.readByte();
    if (tag === 0) return { kind: "raw" };
    if (tag === 1) return { kind: "cbor" };
    if (tag === 2) return { kind: "json" };
    if (tag === 3) return { kind: "custom", customName: new TextDecoder().decode(this.readBytes(this.readVarintNumber())) };
    throw new Error(`invalid encoding tag ${tag}`);
  }
}

function writeVarint(out: number[], valueIn: number | bigint): void {
  let value = BigInt(valueIn);
  while (value >= 0x80n) {
    out.push(Number((value & 0x7fn) | 0x80n));
    value >>= 7n;
  }
  out.push(Number(value));
}

function writeEncoding(out: number[], encoding: EncodingKind, customName?: string): void {
  const tag = encodingTag(encoding);
  out.push(tag);
  if (tag === 3) {
    const name = new TextEncoder().encode(customName ?? "");
    writeVarint(out, name.length);
    pushBytes(out, name);
  }
}

function encodingTag(encoding: EncodingKind): number {
  if (encoding === "raw") return 0;
  if (encoding === "cbor") return 1;
  if (encoding === "json") return 2;
  if (encoding === "custom") return 3;
  throw new Error(`unknown encoding ${encoding}`);
}

function decodeEncodingTag(tag: number, custom: string): { kind: EncodingKind; customName?: string } {
  if (tag === 0 && custom === "") return { kind: "raw" };
  if (tag === 1 && custom === "") return { kind: "cbor" };
  if (tag === 2 && custom === "") return { kind: "json" };
  if (tag === 3) return { kind: "custom", customName: custom };
  throw new Error("invalid encoding/custom combination");
}

function commonPrefixLen(left: Uint8Array, right: Uint8Array): number {
  let count = 0;
  while (count < left.length && count < right.length && left[count] === right[count]) count += 1;
  return count;
}

function pushBytes(out: number[], bytes: Uint8Array): void {
  for (const byte of bytes) out.push(byte);
}

function readU64le(data: Uint8Array, offset: number): bigint {
  let value = 0n;
  for (let i = 7; i >= 0; i--) value = (value << 8n) | BigInt(data[offset + i]);
  return value;
}

function readU32le(data: Uint8Array, offset: number): bigint {
  return BigInt(data[offset]) | (BigInt(data[offset + 1]) << 8n) | (BigInt(data[offset + 2]) << 16n) | (BigInt(data[offset + 3]) << 24n);
}

function readU64be(data: Uint8Array, offset: number): bigint {
  let value = 0n;
  for (let i = 0; i < 8; i++) value = (value << 8n) | BigInt(data[offset + i]);
  return value;
}

function readU32be(data: Uint8Array, offset: number): bigint {
  let value = 0n;
  for (let i = 0; i < 4; i++) value = (value << 8n) | BigInt(data[offset + i]);
  return value;
}

function u64be(value: bigint): Uint8Array {
  const out = new Uint8Array(8);
  let v = BigInt.asUintN(64, value);
  for (let i = 7; i >= 0; i--) {
    out[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return out;
}

function u128be(value: bigint): Uint8Array {
  const out = new Uint8Array(16);
  let v = BigInt.asUintN(128, value);
  for (let i = 15; i >= 0; i--) {
    out[i] = Number(v & 0xffn);
    v >>= 8n;
  }
  return out;
}

function u32be(value: number): Uint8Array {
  const out = new Uint8Array(4);
  out[0] = (value >>> 24) & 0xff;
  out[1] = (value >>> 16) & 0xff;
  out[2] = (value >>> 8) & 0xff;
  out[3] = value & 0xff;
  return out;
}
