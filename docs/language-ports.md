# Language Ports

The crate is Rust-only today. Future Python, TypeScript, or other language
ports should treat Rust as the reference implementation until a shared
conformance suite exists.

## Porting Goals

A port can target different levels:

1. Inspector: read roots, nodes, manifests, and stats from a store.
2. Logical client: expose get, range, and diff against existing snapshots.
3. Writer: create nodes and roots compatible with Rust.
4. Full engine: support batch, merge, GC, sync, async stores, and blobs.

Do not start by promising full byte compatibility unless node encoding and
fixture tests are in place.

## Non-Negotiable Semantics

Every port must match:

- byte lexicographic key ordering;
- delete vs empty value semantics;
- immutable snapshot behavior;
- content-addressed node identity;
- deterministic boundary decisions;
- manifest compare-and-swap behavior;
- standard conflict shape with `None` for absence;
- resolver outcomes of value, delete, or unresolved;
- CRDT custom outcomes of value or delete.

Language APIs can be idiomatic, but storage semantics cannot drift.

## Python Port Strategy

Suggested phases:

Phase 1: read-only tooling

- parse manifests;
- load nodes by CID;
- traverse tree snapshots;
- run point lookups and range scans;
- print stats and debug views.

Phase 2: logical conformance

- compare Python lookup and range results against Rust fixtures;
- decode value envelopes;
- verify diff output;
- verify delete/empty-value behavior.

Phase 3: write support

- implement node building;
- implement content-defined boundaries;
- serialize nodes byte-for-byte with fixtures;
- compute matching CIDs;
- publish named roots through a manifest store.

Phase 4: merge and sync

- implement standard merge;
- implement delete-aware resolvers;
- implement CRDT strategies;
- implement missing-node planning;
- add object-store and local filesystem backends.

Python should probably expose bytes-first APIs initially:

```python
tree = await prolly.load_named_root("main")
value = await tree.get(b"user:001")
rows = [row async for row in tree.range(b"user:", b"user;")]
```

Higher-level string and serde helpers can come later.

## TypeScript and Browser Strategy

The browser story should be async-native:

- `AsyncStore` over IndexedDB, OPFS, Cache Storage, or remote HTTP;
- range pages for UI rendering;
- background sync of missing CIDs;
- local named roots for offline branches;
- optional WASM bridge to the Rust implementation.

TypeScript may start as a client-side inspector or a WASM wrapper before a
native implementation.

## Shared Fixtures

A compatibility fixture should include:

- config;
- sorted input key/value pairs;
- expected root CID;
- serialized node bytes by CID;
- point lookup cases;
- range cases;
- diff cases;
- merge cases;
- manifest cases;
- blob reference cases.

Example fixture shape:

```json
{
  "version": 1,
  "config": {
    "min_chunk_size": 4,
    "max_chunk_size": 1024,
    "chunking_factor": 128,
    "hash_seed": 42,
    "encoding": "raw"
  },
  "entries": [
    ["757365723a303031", "416461"]
  ],
  "root": "hex-encoded-cid"
}
```

The fixture format should use hex for byte strings so every language can load
it without Unicode ambiguity.

## Value Encoding Across Languages

The tree only sees bytes. Cross-language applications should agree on value
encoding separately.

Recommended:

- versioned JSON for debuggable app state;
- versioned CBOR for compact binary state;
- explicit schema version fields;
- checksums for large external payloads;
- no reliance on language-specific serialization formats.

Avoid:

- Rust-only bincode for data meant for Python;
- locale-dependent string normalization in keys;
- timestamps without timezone and precision rules;
- maps with nondeterministic key ordering inside values when those values are
  merged structurally by the application.

## Error Mapping

Ports should map core errors into idiomatic language exceptions or result
types, but preserve categories:

- store error;
- missing node;
- invalid encoding;
- conflict;
- invalid input;
- unsupported feature.

Merge conflict errors should carry the full conflict shape.

## Testing a Port

Minimum test suite:

- sorted ordering over tricky byte keys;
- insert, replace, delete, empty value;
- range boundaries and prefix ranges;
- root stability for fixture datasets;
- diff against fixtures;
- merge with no conflict;
- merge with value/value conflict;
- merge with delete/update conflict;
- custom resolver value, delete, unresolved;
- named root CAS success and failure;
- store missing-node behavior.

Only after these pass should the port add higher-level conveniences.

