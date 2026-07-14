# Prolly Wire Format

This document is the language-neutral compatibility contract for `prolly-map`
ports. The Rust implementation remains the reference; the checked-in
conformance fixtures are generated from Rust.

## Byte Strings and CIDs

- Keys, values, node bytes, and store keys are byte strings.
- Fixture JSON encodes byte strings as lowercase hex.
- A node CID is `SHA-256(node_bytes)`.
- Stores persist nodes under the raw 32-byte CID and store the exact serialized
  node bytes as the value.

## Node Format: `CRAB` Version 1

New nodes are encoded in a deterministic compact binary format:

1. magic bytes: ASCII `CRAB`
2. unsigned LEB128 varint version, currently `1`
3. unsigned LEB128 leaf flag: `1` for leaf, `0` for internal
4. unsigned LEB128 level, where leaves are level `0`
5. unsigned LEB128 `min_chunk_size`
6. unsigned LEB128 `max_chunk_size`
7. unsigned LEB128 `chunking_factor`
8. unsigned LEB128 `hash_seed`
9. encoding tag: `0` raw, `1` CBOR, `2` JSON, `3` custom
10. for custom encoding, unsigned LEB128 UTF-8 name length plus name bytes
11. unsigned LEB128 entry count
12. entries in sorted key order

Each entry stores the key as a prefix-compressed delta from the previous key:

- unsigned LEB128 shared prefix length
- unsigned LEB128 suffix length
- suffix bytes

Leaf values are `varint length + value bytes`.

Internal values are child pointers:

- tag `0` plus 32 CID bytes when the value length is exactly 32 bytes
- tag `1` plus `varint length + bytes` for non-CID legacy/internal payloads

Decoders may read legacy CBOR node bytes for old stores. Writers must write
`CRAB` version 1.

## Tree Semantics

- Raw byte keys are ordered lexicographically.
- Internal node keys are the first key of each child.
- Lookup in an internal node descends to the exact matching separator if found;
  otherwise it descends to the previous separator. If the insertion point is
  zero, the key is absent.
- Empty trees have no root CID.
- Tree snapshots are immutable; writes return a new root.
- Delete and an empty byte value are distinct states.

## Boundary Predicate

The chunk boundary predicate is:

1. if current chunk entry count is below `min_chunk_size`, no boundary
2. if count is at or above `max_chunk_size`, boundary
3. otherwise compute xxHash64 with `hash_seed` over `key || value`
4. take the lower 32 bits and compare `<= floor(u32::MAX / chunking_factor)`

Ports must match the Rust xxHash64 result exactly for writer compatibility.

## Key Helpers

- `u64`, `u128`, and timestamp keys use big-endian unsigned bytes.
- `i64` and `i128` flip the sign bit before big-endian encoding.
- Composite segments escape `0x00` as `00 ff` and terminate each segment with
  `00 00`.
- Prefix ranges are half-open: `[prefix, prefix_end(prefix))`; an empty or all
  `ff` prefix has no exclusive upper bound.

## Value and Blob Envelopes

`VersionedValue` bytes:

- magic `PLVV`
- one-byte wire version `1`
- one-byte encoding tag using the node encoding tags
- big-endian `u64` schema version
- big-endian `u32` schema length
- big-endian `u32` custom encoding name length
- big-endian `u64` payload length
- schema UTF-8 bytes
- custom encoding UTF-8 bytes
- payload bytes

`ValueRef` bytes:

- magic `PLVB`
- one-byte version `1`
- one-byte kind: `0` inline, `1` blob
- inline: big-endian `u64` length plus value bytes
- blob: 32-byte blob CID plus big-endian `u64` blob length

Non-`PLVB` stored values are interpreted as inline bytes by large-value helpers.

## Manifests

`RootManifest` is packed CBOR containing version `1`, optional root CID,
`Config`, and optional Unix millisecond timestamps. Ports that implement named
roots must decode the conformance manifest fixture and preserve CAS semantics.

## Proximity Objects: Version 1

Proximity objects occupy the same content-addressed store namespace but use
independent magic values. All integer varints are canonical unsigned LEB128;
all CIDs are 32 raw SHA-256 bytes. Decoders reject non-canonical varints,
overflow, truncation, unknown flags, and trailing bytes.

### `PRVR` exact-directory record

1. ASCII magic `PRVR`
2. one-byte version `1`
3. vector dimension count as varint
4. exactly that many little-endian IEEE-754 `f32` words
5. opaque value length as varint
6. opaque value bytes

Negative zero is encoded as positive zero. NaN and infinity are forbidden.

### `PRXN` proximity node

1. ASCII magic `PRXN`
2. one-byte version `1`
3. one-byte flags; bit zero means leaf and all other bits are zero
4. one-byte logical level; leaf status is equivalent to level zero
5. logical leaf-descendant count as varint
6. entry count as varint
7. entries in strict byte-lexicographic key order

Each entry stores key length, key bytes, the descriptor's fixed number of
canonical `f32` words, and—only for internal nodes—a 32-byte child CID. Values
are never duplicated in PRXN.

### `PRXI` compound descriptor

1. ASCII magic `PRXI`
2. one-byte version `1`
3. one-byte vector encoding `1` for canonical little-endian `f32`
4. dimensions as varint
5. one-byte metric `1` for squared L2
6. one-byte `log_chunk_size`
7. little-endian `u64` promotion hash seed
8. maximum encoded PRXN bytes as varint
9. logical record count as varint
10. directory-root presence tag and optional 32-byte root CID
11. complete ordered `Config`: min/max chunk sizes and chunking factor as
    varints, little-endian hash seed, encoding tag/custom name, and tagged
    optional node-cache limits
12. 32-byte proximity root CID
13. reserved-field count byte, required to be zero in version 1

Search options are not persisted because they affect latency and recall rather
than content identity.
