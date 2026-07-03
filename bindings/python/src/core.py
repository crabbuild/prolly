from __future__ import annotations

from dataclasses import dataclass
import hashlib
from pathlib import Path
from typing import Iterable

MASK64 = (1 << 64) - 1
PRIME64_1 = 11400714785074694791
PRIME64_2 = 14029467366897019727
PRIME64_3 = 1609587929392839161
PRIME64_4 = 9650029242287828579
PRIME64_5 = 2870177450012600261


def from_hex(value: str) -> bytes:
    return bytes.fromhex(value)


def to_hex(value: bytes) -> str:
    return value.hex()


@dataclass(frozen=True)
class Cid:
    bytes: bytes

    def __post_init__(self) -> None:
        if len(self.bytes) != 32:
            raise ValueError("CID must be 32 bytes")

    @classmethod
    def from_bytes(cls, data: bytes) -> "Cid":
        return cls(hashlib.sha256(data).digest())

    @classmethod
    def from_hex(cls, value: str) -> "Cid":
        return cls(from_hex(value))

    def hex(self) -> str:
        return self.bytes.hex()


@dataclass(frozen=True)
class RangeBounds:
    start: bytes
    end: bytes | None


@dataclass(frozen=True)
class Config:
    min_chunk_size: int = 4
    max_chunk_size: int = 1024 * 1024
    chunking_factor: int = 128
    hash_seed: int = 0
    encoding: str = "raw"
    custom_encoding_name: str | None = None
    node_cache_max_nodes: int | None = None
    node_cache_max_bytes: int | None = None

    @classmethod
    def from_fixture(cls, fixture: dict) -> "Config":
        encoding = fixture.get("encoding", {"kind": "raw", "custom_name": None})
        return cls(
            min_chunk_size=fixture["min_chunk_size"],
            max_chunk_size=fixture["max_chunk_size"],
            chunking_factor=fixture["chunking_factor"],
            hash_seed=fixture["hash_seed"],
            encoding=encoding["kind"],
            custom_encoding_name=encoding.get("custom_name"),
            node_cache_max_nodes=fixture.get("node_cache_max_nodes"),
            node_cache_max_bytes=fixture.get("node_cache_max_bytes"),
        )


@dataclass(frozen=True)
class Node:
    keys: tuple[bytes, ...]
    vals: tuple[bytes, ...]
    leaf: bool = True
    level: int = 0
    min_chunk_size: int = 4
    max_chunk_size: int = 1024 * 1024
    chunking_factor: int = 128
    hash_seed: int = 0
    encoding: str = "raw"
    custom_encoding_name: str | None = None

    @classmethod
    def from_bytes(cls, data: bytes) -> "Node":
        if not data.startswith(b"CRAB"):
            raise ValueError("legacy CBOR node decoding is not implemented in the Python port")
        cursor = _Cursor(data, 4)
        version = cursor.read_varint()
        if version != 1:
            raise ValueError(f"unsupported compact node version {version}")
        leaf_flag = cursor.read_varint()
        if leaf_flag not in (0, 1):
            raise ValueError(f"invalid leaf flag {leaf_flag}")
        leaf = leaf_flag == 1
        level = cursor.read_varint()
        min_chunk_size = cursor.read_varint()
        max_chunk_size = cursor.read_varint()
        chunking_factor = cursor.read_varint()
        hash_seed = cursor.read_varint()
        encoding, custom_name = cursor.read_encoding()
        entry_count = cursor.read_varint()

        keys: list[bytes] = []
        vals: list[bytes] = []
        previous = b""
        for _ in range(entry_count):
            shared = cursor.read_varint()
            if shared > len(previous):
                raise ValueError("shared key prefix exceeds previous key")
            suffix_len = cursor.read_varint()
            key = previous[:shared] + cursor.read_bytes(suffix_len)
            if leaf:
                value = cursor.read_bytes(cursor.read_varint())
            else:
                tag = cursor.read_byte()
                if tag == 0:
                    value = cursor.read_bytes(32)
                elif tag == 1:
                    value = cursor.read_bytes(cursor.read_varint())
                else:
                    raise ValueError(f"invalid internal value tag {tag}")
            keys.append(key)
            vals.append(value)
            previous = key

        if not cursor.done:
            raise ValueError("trailing bytes in compact node")

        return cls(
            keys=tuple(keys),
            vals=tuple(vals),
            leaf=leaf,
            level=level,
            min_chunk_size=min_chunk_size,
            max_chunk_size=max_chunk_size,
            chunking_factor=chunking_factor,
            hash_seed=hash_seed,
            encoding=encoding,
            custom_encoding_name=custom_name,
        )

    @classmethod
    def from_fixture(cls, fixture: dict) -> "Node":
        return cls.from_bytes(from_hex(fixture["bytes"]))

    def to_bytes(self) -> bytes:
        out = bytearray(b"CRAB")
        _write_varint(out, 1)
        _write_varint(out, 1 if self.leaf else 0)
        _write_varint(out, self.level)
        _write_varint(out, self.min_chunk_size)
        _write_varint(out, self.max_chunk_size)
        _write_varint(out, self.chunking_factor)
        _write_varint(out, self.hash_seed)
        _write_encoding(out, self.encoding, self.custom_encoding_name)
        _write_varint(out, len(self.keys))
        previous = b""
        for key, value in zip(self.keys, self.vals, strict=True):
            shared = _common_prefix_len(previous, key)
            suffix = key[shared:]
            _write_varint(out, shared)
            _write_varint(out, len(suffix))
            out.extend(suffix)
            if self.leaf:
                _write_varint(out, len(value))
                out.extend(value)
            elif len(value) == 32:
                out.append(0)
                out.extend(value)
            else:
                out.append(1)
                _write_varint(out, len(value))
                out.extend(value)
            previous = key
        return bytes(out)

    def cid(self) -> Cid:
        return Cid.from_bytes(self.to_bytes())

    def search(self, key: bytes) -> tuple[bool, int]:
        lo = 0
        hi = len(self.keys)
        while lo < hi:
            mid = (lo + hi) // 2
            if self.keys[mid] < key:
                lo = mid + 1
            elif self.keys[mid] > key:
                hi = mid
            else:
                return True, mid
        return False, lo


@dataclass(frozen=True)
class Tree:
    root: Cid | None
    config: Config

    @classmethod
    def from_fixture(cls, fixture: dict) -> "Tree":
        root = fixture.get("root")
        return cls(Cid.from_hex(root) if root else None, Config.from_fixture(fixture["config"]))


@dataclass(frozen=True)
class KeyProof:
    root: Cid | None
    key: bytes
    path: tuple[Node, ...]

    @classmethod
    def from_node_bytes(
        cls,
        root: Cid | bytes | None,
        key: bytes,
        path_node_bytes: Iterable[bytes],
    ) -> "KeyProof":
        return cls(
            _coerce_cid(root),
            bytes(key),
            tuple(Node.from_bytes(bytes(node)) for node in path_node_bytes),
        )

    def verify(self) -> "KeyProofVerification":
        return verify_key_proof(self)

    def path_node_bytes(self) -> list[bytes]:
        return [node.to_bytes() for node in self.path]


@dataclass(frozen=True)
class KeyProofVerification:
    valid: bool
    root: Cid | None
    key: bytes
    value: bytes | None

    @property
    def exists(self) -> bool:
        return self.valid and self.value is not None

    @property
    def absence(self) -> bool:
        return self.valid and self.value is None


class MemoryStore:
    def __init__(self) -> None:
        self._data: dict[bytes, bytes] = {}

    def get(self, key: bytes) -> bytes | None:
        return self._data.get(key)

    def put(self, key: bytes, value: bytes) -> None:
        self._data[bytes(key)] = bytes(value)

    def delete(self, key: bytes) -> None:
        self._data.pop(key, None)

    def batch(self, ops: Iterable[tuple[str, bytes, bytes | None]]) -> None:
        for op, key, value in ops:
            if op == "delete":
                self.delete(key)
            elif op == "upsert" and value is not None:
                self.put(key, value)
            else:
                raise ValueError(f"invalid batch op {op!r}")

    @classmethod
    def from_fixture(cls, fixture: dict) -> "MemoryStore":
        store = cls()
        for entry in fixture["store"]:
            store.put(from_hex(entry["cid"]), from_hex(entry["bytes"]))
        return store


class Prolly:
    def __init__(self, store: MemoryStore, config: Config | None = None) -> None:
        self.store = store
        self.config = config or Config()

    def get(self, tree: Tree, key: bytes) -> bytes | None:
        if tree.root is None:
            return None
        cid = tree.root
        while True:
            node = self._load(cid)
            found, index = node.search(key)
            if not found:
                if index == 0:
                    return None
                index -= 1
            if node.leaf:
                return node.vals[index] if node.keys[index] == key else None
            cid = Cid(node.vals[index])

    def prove_key(self, tree: Tree, key: bytes) -> KeyProof:
        if tree.root is None:
            return KeyProof(None, bytes(key), ())

        path: list[Node] = []
        cid = tree.root
        while True:
            node = self._load(cid)
            path.append(node)
            if node.leaf:
                return KeyProof(tree.root, bytes(key), tuple(path))
            index = _path_child_index(node, key)
            if index >= len(node.vals) or len(node.vals[index]) != 32:
                raise ValueError("invalid internal node child pointer")
            cid = Cid(node.vals[index])

    def range(self, tree: Tree, start: bytes = b"", end: bytes | None = None) -> list[tuple[bytes, bytes]]:
        return [
            (key, value)
            for key, value in self.entries(tree)
            if key >= start and (end is None or key < end)
        ]

    def entries(self, tree: Tree) -> list[tuple[bytes, bytes]]:
        if tree.root is None:
            return []
        entries = self._entries_from_node(self._load(tree.root))
        entries.sort(key=lambda entry: entry[0])
        return entries

    def diff(self, base: Tree, other: Tree) -> list[dict]:
        return diff_entries(self.entries(base), self.entries(other))

    def _entries_from_node(self, node: Node) -> list[tuple[bytes, bytes]]:
        if node.leaf:
            return list(zip(node.keys, node.vals, strict=True))
        entries: list[tuple[bytes, bytes]] = []
        for child in node.vals:
            entries.extend(self._entries_from_node(self._load(Cid(child))))
        return entries

    def _load(self, cid: Cid) -> Node:
        data = self.store.get(cid.bytes)
        if data is None:
            raise KeyError(f"missing node {cid.hex()}")
        node = Node.from_bytes(data)
        if node.cid() != cid:
            raise ValueError(f"CID mismatch for {cid.hex()}")
        return node


@dataclass(frozen=True)
class VersionedValue:
    schema: str
    version: int
    encoding: str
    payload: bytes
    custom_encoding_name: str | None = None

    @classmethod
    def from_bytes(cls, data: bytes) -> "VersionedValue":
        if len(data) < 30 or not data.startswith(b"PLVV"):
            raise ValueError("invalid versioned value envelope")
        if data[4] != 1:
            raise ValueError(f"unsupported versioned value wire version {data[4]}")
        tag = data[5]
        version = int.from_bytes(data[6:14], "big")
        schema_len = int.from_bytes(data[14:18], "big")
        custom_len = int.from_bytes(data[18:22], "big")
        payload_len = int.from_bytes(data[22:30], "big")
        schema_start = 30
        custom_start = schema_start + schema_len
        payload_start = custom_start + custom_len
        expected_len = payload_start + payload_len
        if expected_len != len(data):
            raise ValueError("versioned value length mismatch")
        schema = data[schema_start:custom_start].decode()
        custom = data[custom_start:payload_start].decode()
        encoding, custom_name = _decode_encoding_tag(tag, custom)
        return cls(schema, version, encoding, data[payload_start:], custom_name)

    def to_bytes(self) -> bytes:
        schema = self.schema.encode()
        custom = (self.custom_encoding_name or "").encode()
        out = bytearray(b"PLVV")
        out.append(1)
        out.append(_encoding_tag(self.encoding))
        out.extend(self.version.to_bytes(8, "big"))
        out.extend(len(schema).to_bytes(4, "big"))
        out.extend(len(custom).to_bytes(4, "big"))
        out.extend(len(self.payload).to_bytes(8, "big"))
        out.extend(schema)
        out.extend(custom)
        out.extend(self.payload)
        return bytes(out)


@dataclass(frozen=True)
class BlobRef:
    cid: Cid
    length: int

    @classmethod
    def from_bytes(cls, data: bytes) -> "BlobRef":
        return cls(Cid.from_bytes(data), len(data))


@dataclass(frozen=True)
class ValueRef:
    kind: str
    value: bytes | None = None
    blob_ref: BlobRef | None = None

    @classmethod
    def from_bytes(cls, data: bytes) -> "ValueRef":
        if len(data) < 6 or not data.startswith(b"PLVB"):
            raise ValueError("invalid value ref envelope")
        if data[4] != 1:
            raise ValueError(f"unsupported value ref version {data[4]}")
        kind = data[5]
        if kind == 0:
            length = int.from_bytes(data[6:14], "big")
            value = data[14:]
            if len(value) != length:
                raise ValueError("inline value ref length mismatch")
            return cls("inline", value=value)
        if kind == 1:
            if len(data) != 46:
                raise ValueError("blob value ref length mismatch")
            cid = Cid(data[6:38])
            length = int.from_bytes(data[38:46], "big")
            return cls("blob", blob_ref=BlobRef(cid, length))
        raise ValueError(f"unknown value ref kind {kind}")

    def to_bytes(self) -> bytes:
        out = bytearray(b"PLVB")
        out.append(1)
        if self.kind == "inline":
            value = self.value or b""
            out.append(0)
            out.extend(len(value).to_bytes(8, "big"))
            out.extend(value)
            return bytes(out)
        if self.kind == "blob" and self.blob_ref is not None:
            out.append(1)
            out.extend(self.blob_ref.cid.bytes)
            out.extend(self.blob_ref.length.to_bytes(8, "big"))
            return bytes(out)
        raise ValueError("invalid value ref")


def load_fixture(path: str | Path) -> dict:
    import json

    return json.loads(Path(path).read_text())


def diff_entries(base: list[tuple[bytes, bytes]], other: list[tuple[bytes, bytes]]) -> list[dict]:
    base_map = dict(base)
    other_map = dict(other)
    out: list[dict] = []
    for key in sorted(base_map.keys() | other_map.keys()):
        if key not in base_map:
            out.append({"kind": "added", "key": key, "value": other_map[key], "old": None, "new": None})
        elif key not in other_map:
            out.append({"kind": "removed", "key": key, "value": base_map[key], "old": None, "new": None})
        elif base_map[key] != other_map[key]:
            out.append({"kind": "changed", "key": key, "value": None, "old": base_map[key], "new": other_map[key]})
    return out


def verify_key_proof(proof: KeyProof) -> KeyProofVerification:
    valid = _proof_is_consistent(proof)
    value = _verified_leaf_value(proof.path[-1], proof.key) if valid and proof.path else None
    return KeyProofVerification(
        valid=valid,
        root=proof.root,
        key=proof.key,
        value=value,
    )


def key_proof_path_node_bytes(proof: KeyProof) -> list[bytes]:
    return proof.path_node_bytes()


def key_proof_from_node_bytes(
    root: Cid | bytes | None,
    key: bytes,
    path_node_bytes: Iterable[bytes],
) -> KeyProof:
    return KeyProof.from_node_bytes(root, key, path_node_bytes)


def is_boundary_config(config: Config, count: int, key: bytes, value: bytes) -> bool:
    if count < config.min_chunk_size:
        return False
    if count >= config.max_chunk_size:
        return True
    hash_value = xxh64(key + value, config.hash_seed) & 0xFFFF_FFFF
    threshold = 0xFFFF_FFFF // config.chunking_factor
    return hash_value <= threshold


def _coerce_cid(value: Cid | bytes | None) -> Cid | None:
    if value is None:
        return None
    if isinstance(value, Cid):
        return value
    return Cid(bytes(value))


def _proof_is_consistent(proof: KeyProof) -> bool:
    if proof.root is None:
        return len(proof.path) == 0
    if not proof.path:
        return False
    if proof.path[0].cid() != proof.root:
        return False

    for depth, node in enumerate(proof.path):
        if not _node_shape_is_valid(node):
            return False

        is_last = depth + 1 == len(proof.path)
        if is_last:
            return node.leaf
        if node.leaf:
            return False

        next_node = proof.path[depth + 1]
        if node.level != next_node.level + 1:
            return False
        child_index = _path_child_index(node, proof.key)
        if child_index >= len(node.vals):
            return False
        if len(node.vals[child_index]) != 32:
            return False
        if Cid(node.vals[child_index]) != next_node.cid():
            return False

    return False


def _verified_leaf_value(leaf: Node, key: bytes) -> bytes | None:
    found, index = leaf.search(key)
    if not found:
        return None
    return leaf.vals[index]


def _node_shape_is_valid(node: Node) -> bool:
    if not node.keys or len(node.keys) != len(node.vals):
        return False
    if any(left >= right for left, right in zip(node.keys, node.keys[1:])):
        return False
    return node.leaf or all(len(value) == 32 for value in node.vals)


def _path_child_index(node: Node, key: bytes) -> int:
    lo = 0
    hi = len(node.keys)
    while lo < hi:
        mid = (lo + hi) // 2
        if node.keys[mid] <= key:
            lo = mid + 1
        else:
            hi = mid
    return max(lo - 1, 0)


def xxh64(data: bytes, seed: int = 0) -> int:
    def rotl(value: int, bits: int) -> int:
        return ((value << bits) | (value >> (64 - bits))) & MASK64

    def round64(acc: int, lane: int) -> int:
        acc = (acc + lane * PRIME64_2) & MASK64
        acc = rotl(acc, 31)
        return (acc * PRIME64_1) & MASK64

    def merge_round(acc: int, value: int) -> int:
        acc ^= round64(0, value)
        return (acc * PRIME64_1 + PRIME64_4) & MASK64

    index = 0
    length = len(data)
    seed &= MASK64
    if length >= 32:
        v1 = (seed + PRIME64_1 + PRIME64_2) & MASK64
        v2 = (seed + PRIME64_2) & MASK64
        v3 = seed
        v4 = (seed - PRIME64_1) & MASK64
        limit = length - 32
        while index <= limit:
            v1 = round64(v1, int.from_bytes(data[index:index + 8], "little"))
            v2 = round64(v2, int.from_bytes(data[index + 8:index + 16], "little"))
            v3 = round64(v3, int.from_bytes(data[index + 16:index + 24], "little"))
            v4 = round64(v4, int.from_bytes(data[index + 24:index + 32], "little"))
            index += 32
        h64 = (rotl(v1, 1) + rotl(v2, 7) + rotl(v3, 12) + rotl(v4, 18)) & MASK64
        h64 = merge_round(h64, v1)
        h64 = merge_round(h64, v2)
        h64 = merge_round(h64, v3)
        h64 = merge_round(h64, v4)
    else:
        h64 = (seed + PRIME64_5) & MASK64

    h64 = (h64 + length) & MASK64
    while index + 8 <= length:
        lane = int.from_bytes(data[index:index + 8], "little")
        h64 ^= round64(0, lane)
        h64 = (rotl(h64, 27) * PRIME64_1 + PRIME64_4) & MASK64
        index += 8
    if index + 4 <= length:
        h64 ^= (int.from_bytes(data[index:index + 4], "little") * PRIME64_1) & MASK64
        h64 &= MASK64
        h64 = (rotl(h64, 23) * PRIME64_2 + PRIME64_3) & MASK64
        index += 4
    while index < length:
        h64 ^= (data[index] * PRIME64_5) & MASK64
        h64 &= MASK64
        h64 = (rotl(h64, 11) * PRIME64_1) & MASK64
        index += 1

    h64 ^= h64 >> 33
    h64 = (h64 * PRIME64_2) & MASK64
    h64 ^= h64 >> 29
    h64 = (h64 * PRIME64_3) & MASK64
    h64 ^= h64 >> 32
    return h64 & MASK64


def prefix_end(prefix: bytes) -> bytes | None:
    if not prefix:
        return None
    end = bytearray(prefix)
    while end:
        if end[-1] == 0xFF:
            end.pop()
        else:
            end[-1] += 1
            return bytes(end)
    return None


def prefix_range(prefix: bytes) -> RangeBounds:
    return RangeBounds(start=bytes(prefix), end=prefix_end(prefix))


def u64_key(value: int) -> bytes:
    return value.to_bytes(8, "big", signed=False)


def u128_key(value: int | str) -> bytes:
    return int(value).to_bytes(16, "big", signed=False)


def i64_key(value: int) -> bytes:
    return ((value + (1 << 64)) % (1 << 64) ^ (1 << 63)).to_bytes(8, "big")


def i128_key(value: int | str) -> bytes:
    encoded = (int(value) + (1 << 128)) % (1 << 128) ^ (1 << 127)
    return encoded.to_bytes(16, "big", signed=False)


def encode_segment(segment: bytes) -> bytes:
    out = bytearray()
    for byte in segment:
        if byte == 0:
            out.extend((0x00, 0xFF))
        else:
            out.append(byte)
    out.extend((0x00, 0x00))
    return bytes(out)


def decode_segments(key: bytes) -> list[bytes]:
    segments: list[bytes] = []
    current = bytearray()
    offset = 0
    while offset < len(key):
        byte = key[offset]
        if byte != 0:
            current.append(byte)
            offset += 1
            continue
        if offset + 1 >= len(key):
            raise ValueError(f"encoded key ended unexpectedly at byte offset {offset}")
        marker = key[offset + 1]
        if marker == 0:
            segments.append(bytes(current))
            current.clear()
            offset += 2
        elif marker == 0xFF:
            current.append(0)
            offset += 2
        else:
            raise ValueError(f"invalid encoded key escape at byte offset {offset}: 0x{marker:02x}")
    if current:
        raise ValueError(f"encoded key ended unexpectedly at byte offset {len(key)}")
    return segments


class _Cursor:
    def __init__(self, data: bytes, pos: int = 0) -> None:
        self.data = data
        self.pos = pos

    @property
    def done(self) -> bool:
        return self.pos == len(self.data)

    def read_byte(self) -> int:
        if self.pos >= len(self.data):
            raise ValueError("unexpected end of bytes")
        value = self.data[self.pos]
        self.pos += 1
        return value

    def read_bytes(self, length: int) -> bytes:
        end = self.pos + length
        if end > len(self.data):
            raise ValueError("unexpected end of bytes")
        value = self.data[self.pos:end]
        self.pos = end
        return value

    def read_varint(self) -> int:
        value = 0
        shift = 0
        for _ in range(10):
            byte = self.read_byte()
            part = byte & 0x7F
            if shift == 63 and part > 1:
                raise ValueError("varint overflow")
            value |= part << shift
            if byte & 0x80 == 0:
                return value
            shift += 7
        raise ValueError("varint overflow")

    def read_encoding(self) -> tuple[str, str | None]:
        tag = self.read_byte()
        if tag == 0:
            return "raw", None
        if tag == 1:
            return "cbor", None
        if tag == 2:
            return "json", None
        if tag == 3:
            return "custom", self.read_bytes(self.read_varint()).decode()
        raise ValueError(f"invalid encoding tag {tag}")


def _write_varint(out: bytearray, value: int) -> None:
    while value >= 0x80:
        out.append((value & 0x7F) | 0x80)
        value >>= 7
    out.append(value)


def _write_encoding(out: bytearray, encoding: str, custom_name: str | None) -> None:
    tag = _encoding_tag(encoding)
    out.append(tag)
    if tag == 3:
        name = (custom_name or "").encode()
        _write_varint(out, len(name))
        out.extend(name)


def _encoding_tag(encoding: str) -> int:
    if encoding == "raw":
        return 0
    if encoding == "cbor":
        return 1
    if encoding == "json":
        return 2
    if encoding == "custom":
        return 3
    raise ValueError(f"unknown encoding {encoding!r}")


def _decode_encoding_tag(tag: int, custom: str) -> tuple[str, str | None]:
    if tag == 0 and not custom:
        return "raw", None
    if tag == 1 and not custom:
        return "cbor", None
    if tag == 2 and not custom:
        return "json", None
    if tag == 3:
        return "custom", custom
    raise ValueError("invalid encoding/custom combination")


def _common_prefix_len(left: bytes, right: bytes) -> int:
    count = 0
    for left_byte, right_byte in zip(left, right, strict=False):
        if left_byte != right_byte:
            break
        count += 1
    return count
