# Typed Document and Value Engine Design

Status: approved design

Date: 2026-07-16

Scope: canonical native values, adaptive typed Merkle containers, document
collections, paths, queries, patches, diff and merge, schemas, secondary
indexes, specialized typed maps, content-graph integration, and portable
conformance

## Summary

The current Prolly engine is substantially more capable than a simple mutable
key-value store. It already provides immutable content-addressed trees,
deterministic chunking, structural diff and merge, logical and structural
patches, versioned maps, transactions, strict secondary indexes, large-value
offload, content graphs, proofs, sync, retention, garbage collection, and
portable bindings. It also provides JSON and CBOR codecs, schema-versioned
value envelopes, and a typed `VersionedMap` facade.

Those typed APIs still treat an application value as one opaque byte string.
A JSON field lookup decodes the complete value, a field update rewrites the
complete value, a map diff stops at the document ID, and generic secondary
indexes receive untyped source bytes. The configured `Encoding::Json` marker
describes leaf bytes but does not add document semantics.

This design adds a typed value and document layer above the existing ordered
byte-tree core. The native `DocumentValue` algebra includes practical database
scalars, arrays, UTF-8-keyed objects, tuples, sets, and maps whose keys and
values may be any finite `DocumentValue`. JSON is a standards-oriented import,
export, pointer, patch, and query adapter rather than the storage model.

Small values remain inline. Large values become typed Merkle container DAGs
whose associative and positional containers reuse existing Prolly trees. The
logical `ValueCid` is independent of inline versus indexed representation and
physical chunking. Path lookup, localized mutation, structural diff, patch,
merge, indexing, sync, proofs, and GC operate directly on lazy typed views.

`DocumentMap` provides a versioned collection facade. `TupleMap`, `AddressMap`,
and `ArtifactMap` are thin domain-specific facades over the same value engine,
not separate physical engines.

## Goals

1. Make typed values a first-class engine capability without changing the
   ordered byte-tree node model.
2. Support the complete MVP value algebra: native scalars, array, object,
   tuple, set, and arbitrary typed map.
3. Give every value a stable, cross-language canonical encoding, logical CID,
   equality relation, and total order.
4. Keep small values cheap while allowing large nested containers to be read,
   updated, diffed, merged, indexed, proved, synced, and collected without
   materializing the complete value.
5. Provide standards-oriented JSON Pointer, JSON Patch, JSON import/export,
   and a bounded JSONPath-style query subset for JSON-compatible values.
6. Provide native paths, patches, and queries for tuples, sets, and typed maps.
7. Maintain source documents and declarative secondary indexes atomically.
8. Support optional content-addressed schemas and deterministic migrations.
9. Expose reusable `TupleMap`, `AddressMap`, and `ArtifactMap` developer APIs.
10. Make corruption, resource exhaustion, query planning, representation
    changes, and structural reuse observable and testable.
11. Establish language-neutral fixtures before any persisted format is called
    stable.

## Non-Goals

1. Replacing the existing ordered byte-tree, node layouts, chunking engine,
   store traits, or immutable tree handles.
2. Building a SQL parser, SQL server, or relational optimizer.
3. Supporting unrestricted JSONPath functions or implementation-defined query
   evaluation.
4. Treating full-text or vector search as ordinary ordered indexes.
5. Providing locale-sensitive collation or implicit Unicode normalization.
6. Coercing numeric types for identity, hashing, patch tests, or uniqueness
   without an explicit query or index policy.
7. Providing CRDT or rank-stable sequence semantics for arrays in MVP.
8. Performing implicit lazy schema migrations during reads.
9. Silently reading unknown value, sort-key, query, schema, or content-object
   versions.
10. Making raw `VersionedMap` writes bypass active document indexes or schema
    validation.

## Approved Product Decisions

The following decisions are fixed for this design.

1. The first product milestone is the document and typed-value engine, not a
   full database server.
2. Storage is adaptive: small values are inline and large containers are
   indexed.
3. JSON compatibility is standards-first. JSON Pointer addresses exact JSON
   locations, JSON Patch mutates them, and a limited JSONPath-style language
   compiles into the native query algebra.
4. The native persisted model is `DocumentValue`; JSON is one representation.
5. MVP includes tuples, sets, and typed maps in addition to JSON-shaped
   containers.
6. Typed-map keys and set members may be any finite `DocumentValue` subject to
   deterministic resource limits.
7. Numeric identity is type-strict.
8. Arrays use positional paths and bounded smart diff in MVP.
9. The physical architecture is a typed Merkle container DAG over the existing
   Prolly engine.

## Current Foundation and Required Gaps

The design builds on shipped components rather than replacing them.

| Existing capability | Reuse | Required extension |
| --- | --- | --- |
| `Prolly<S>` ordered byte tree | Physical container indexes | No node-model change |
| `VersionedMap` | Collection roots, versions, snapshots, CAS, subscriptions | Typed collection descriptor and document operations |
| `IndexedMap` | Atomic source/index/catalog publication | Declarative value queries and unique indexes |
| `ValueCodec` and `VersionedValue` | Host typed adapters and legacy values | Native canonical DVF codec and lazy views |
| `StructuralPatch` | Format-bound patch envelope | Verified subtree application instead of rejection |
| `BlobStore` | Large scalar payload policy and existing applications | Typed bytes remain semantically distinct from blob placement |
| Content graph | Reachability, copy, publication, GC | Typed document content kinds and child references |
| Proofs | Collection membership | Nested value-path proof chains |
| Sync and backup | Immutable object transfer | Closed typed value graphs |
| Secondary indexes | Exact snapshot consistency | Native sort keys, compiled queries, uniqueness, dependency summaries |

The existing `TreeFormat.value_encoding` remains a physical descriptor. It
does not become a dynamic type system. Document semantics live entirely in the
new layer.

## Layered Architecture

The system has two public layers and one unchanged substrate.

```text
DocumentMap and specialized maps
├── IDs, versions, schemas, indexes, transactions, subscriptions
└── collection snapshots, query planning, retention, backup

DocumentValue engine
├── canonical values, identity, ordering, paths, queries
├── adaptive container DAG, lazy views, editors
└── patch, diff, merge, proof, sync, GC integration

Existing Prolly substrate
├── ordered byte trees and deterministic chunking
├── stores, manifests, transactions, snapshots
└── structural traversal, indexes, proofs, sync, and GC
```

The initial implementation may live in a `document` module within the crate so
it can integrate with transactions, content graphs, and proofs without a
premature public crate boundary. Its public types must not expose internal
`Node` representations. A later extraction into a sibling crate is possible
because dependency flow is one-way from the document layer to the ordered
engine.

## Native Value Model

MVP defines the following closed algebra.

```rust
pub enum DocumentValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(CanonicalF64),
    Decimal(Decimal128),
    String(String),
    Bytes(Vec<u8>),
    Date(Date),
    Timestamp(Timestamp),
    Uuid([u8; 16]),
    Cid(Cid),
    Array(Vec<DocumentValue>),
    Object(DocumentObject),
    Tuple(Vec<DocumentValue>),
    Set(DocumentSet),
    TypedMap(DocumentMapValue),
}
```

The owned enum is the materialized API. Indexed reads use `ValueView` and
container-specific lazy views so callers do not need to allocate a complete
owned value.

### Scalar canonicalization

- `Null` has no payload.
- `Bool` has exactly two values.
- `I64` and `U64` retain distinct type identity even when they represent the
  same mathematical integer.
- `F64` accepts finite values only. All negative zero inputs normalize to
  positive zero. NaN and infinities are rejected.
- `Decimal128` represents `coefficient * 10^exponent` with a signed 128-bit
  coefficient and signed 32-bit exponent. Zero is `(0, 0)`; every nonzero
  coefficient has trailing base-10 zeroes removed and the exponent adjusted.
  A collection may impose a narrower exponent limit without changing DVF.
- `String` must be valid UTF-8. The engine performs no NFC, NFD, case, or
  locale normalization.
- `Bytes` are arbitrary bytes and are not interchangeable with strings.
- `Date` is a signed 32-bit count of days relative to the Unix epoch.
- `Timestamp` is a signed 64-bit seconds count plus a normalized unsigned
  32-bit nanosecond fraction in UTC. Time-zone presentation data is not part
  of identity.
- `Uuid` is exactly 16 bytes.
- `Cid` uses the engine's fixed authenticated CID representation.

### Container semantics

- `Array` is ordered, variable length, and position addressed.
- `Object` has unique UTF-8 string field names. Canonical iteration sorts field
  names by unsigned UTF-8 bytes.
- `Tuple` is ordered and fixed arity for the life of that tuple value. Changing
  arity replaces the tuple.
- `Set` contains unique values under type-strict canonical identity. Iteration
  uses the canonical total order.
- `TypedMap` maps any finite `DocumentValue` key to any finite
  `DocumentValue` value. Keys are unique under type-strict identity and iterate
  in canonical total order.

Owned values cannot contain cycles. Recursive keys and members remain finite
but are limited by depth, encoded bytes, collection cardinality, and comparison
work.

## Three Canonical Contracts

The feature defines three separate versioned contracts.

```text
DVF v1       canonical value interchange and inline encoding
ValueCid v1  representation-independent logical identity
SortKey v1   reversible order-preserving encoding
```

These contracts must never be substituted for one another merely because they
encode the same logical value.

### DVF v1

DVF is a language-neutral binary format. It has explicit magic, a format
version, stable type tags, canonical length framing, and complete-consumption
validation. It does not use Rust enum layout, `serde` implementation details,
or a host platform's integer sizes.

Nested values use the same canonical type and payload rules without repeating
the top-level file header. Container counts and byte lengths use one canonical
unsigned length encoding. Decoders reject overlong encodings, invalid scalar
forms, duplicate members or keys, unsorted canonical containers, trailing
bytes, unsupported tags, and unsupported versions.

DVF is used for:

- inline `DocumentRef` and `ValueCell` payloads;
- language-neutral import/export and conformance fixtures;
- schema defaults and patch payloads;
- canonical rebuilding and corruption diagnostics.

### ValueCid v1

`ValueCid` is a logical typed Merkle digest. It uses SHA-256 with an explicit
domain prefix and unambiguous length framing. Its input is the canonical value
structure, not encoded Prolly nodes.

Conceptually:

```text
scalar = H(domain, type, canonical scalar payload)
array  = H(domain, array, count, child ValueCids in order)
object = H(domain, object, count, sorted field bytes and child ValueCids)
tuple  = H(domain, tuple, arity, child ValueCids in order)
set    = H(domain, set, count, members in SortKey order)
map    = H(domain, map, count, key/value ValueCids in SortKey order)
```

Exact framing is defined in the wire-format specification and golden fixtures.
Physical node layout, chunk boundaries, store placement, inline versus indexed
representation, cache policy, and promotion thresholds do not participate.

The same logical value therefore has the same `ValueCid` after rechunking,
promotion, demotion, sync, backup restoration, or a compatible physical-format
migration.

### SortKey v1 and total order

`SortKey` is reversible and byte-order preserving. A hash is insufficient
because typed-map decoding, collision-free membership, exact lookup, and range
queries require the original key or member.

The persisted cross-type order is:

```text
Null < Bool < I64 < U64 < F64 < Decimal < String < Bytes
     < Date < Timestamp < UUID < CID
     < Array < Object < Tuple < Set < TypedMap
```

Within one type:

- booleans order false before true;
- signed and unsigned integers use natural numeric order;
- finite canonical floats use total numeric order after zero normalization;
- canonical decimals use exact decimal numeric order;
- strings use unsigned UTF-8 byte order;
- bytes, UUIDs, and CIDs use unsigned byte order;
- dates and timestamps use chronological order;
- arrays and tuples compare lexicographically by element and then length;
- objects compare canonical field/value pairs;
- sets compare their canonically ordered members;
- typed maps compare canonical key/value pairs.

Container encodings use escaped terminators or length components that preserve
recursive lexicographic order and remain reversible. Golden fixtures verify
that comparing `SortKey` bytes produces exactly the same result as `ValueOrd`.

Numeric types never coerce for identity or default ordering. Query and index
definitions may explicitly request a versioned numeric coercion policy. That
policy affects only that query or index and never changes DVF, `ValueCid`, or a
patch `test` operation.

## JSON Profiles

JSON is an adapter over the native model.

### Import

The default strict profile maps JSON values as follows:

- null, booleans, strings, arrays, and objects map directly;
- an integral token maps to `I64` when it fits, then to `U64` when nonnegative
  and outside `I64`;
- a fractional or exponent token maps to `Decimal128` when representable;
- producing `F64` requires an explicit import policy;
- duplicate object names are rejected.

Numbers outside the configured native range fail rather than silently round.
The collection descriptor pins its JSON number-import policy. JSON Patch
payloads use that same policy, and `test` compares the resulting native values
with type-strict identity. A caller cannot change number interpretation for
one patch operation.

### Export

Strict JSON export supports compatible values. It can print native integer,
decimal, and finite float values, but callers are warned that a strict JSON
round trip cannot preserve their distinct native type identity. Native types
without a strict JSON representation return a structured conversion error
unless the caller supplies a conversion policy.

An explicit Extended JSON profile represents bytes, date, timestamp, UUID,
CID, tuple, set, and typed map with versioned tags. Tagged objects are
interpreted only when the Extended profile is selected, so ordinary objects
cannot be mistaken for typed values.

## Adaptive Representation

Each collection entry contains a canonical `DocumentRef`.

```rust
pub struct DocumentRef {
    pub value_cid: ValueCid,
    pub logical_bytes: u64,
    pub representation: Representation,
}

pub enum Representation {
    Inline(Vec<u8>),
    Indexed(TypedValueRoot),
}
```

Nested containers use the analogous `ValueCell`:

```rust
pub enum ValueCell {
    Inline {
        value_cid: ValueCid,
        dvf: Vec<u8>,
    },
    Reference {
        value_cid: ValueCid,
        root: TypedValueRoot,
        logical_bytes: u64,
    },
}
```

Fixed-width and small scalars remain inline. Large strings and byte values use
a typed scalar-blob root, while containers are promoted when their canonical
logical size exceeds the persisted threshold for their kind. The decision is
a pure function of the value and `DocumentFormat`; it does not depend on cache
state, write history, or the host language. Falling below the threshold
demotes the value on the next canonical rewrite.

`DocumentFormat` records:

- DVF, ValueCid, SortKey, query, and cell codec versions;
- inline thresholds by scalar and container kind;
- the underlying ordered `TreeFormat`;
- scalar, container, key, depth, graph, query, patch, and diff limits;
- content-kind versions and schema codec version.

Its canonical digest participates in physical manifests and structural
patches, but not `ValueCid`.

## Typed Merkle Container DAG

Indexed containers reuse existing ordered Prolly trees.

| Container | Ordered-tree key | Leaf value |
| --- | --- | --- |
| Object | sortable UTF-8 field name | `ValueCell` |
| TypedMap | `SortKey` of native key | canonical key verification data plus `ValueCell` |
| Set | `SortKey` of member | canonical member `ValueCell` or verification data |
| Array | big-endian `u64` position | `ValueCell` |
| Tuple | big-endian `u64` slot | `ValueCell` |

Object fields, typed-map keys, and set members are strictly ordered and
duplicate free. Array keys are dense positions. Tuple keys are dense positions
and the root metadata records arity.

MVP accepts that insertion or removal near the front of a large array may
rewrite a positional suffix. Append and element replacement remain localized.
Rank-stable sequence storage is a future compatible container-format version,
not an implicit behavior change.

The typed layer adds built-in authenticated content kinds for document
manifests and typed container roots. Internal ordered-node children preserve
the current node format. At typed leaves, the content walker decodes
`ValueCell` references and follows referenced child roots.

Large string and byte roots reuse authenticated blob storage and participate
in the typed graph. Their placement never changes whether the logical value is
`String` or `Bytes`, and their `ValueCid` is computed from the complete logical
payload rather than the blob reference.

The implementation must not infer document references by scanning arbitrary
raw leaf values. Typed root context and value-encoding identifiers determine
when leaf bytes contain authenticated child references.

## Content Graph Integration

Typed values form closed authenticated graphs. The content graph recognizes:

- document and value manifests;
- object and typed-map container roots;
- set container roots;
- array and tuple sequence roots;
- typed string and byte scalar blobs;
- existing ordered nodes reached beneath those roots;
- existing blob references when an application explicitly uses blob-backed
  scalar policy.

Graph walking is descendant-first and bounded. It verifies object bytes against
their storage CID, validates the declared kind, enforces container format, and
extracts authenticated child references.

This integration applies to:

- named typed-root publication;
- store-to-store copy and missing-object planning;
- backup and restore closure;
- retention and garbage collection;
- nested path proofs;
- verification and inspection.

Failed optimistic writes may leave immutable unreferenced objects. They are not
visible through a published collection version and are reclaimed by existing
retention-aware GC.

## Exact Paths

The API separates native exact paths from standards adapters.

```rust
pub struct ValuePath(Vec<PathSegment>);

pub enum PathSegment {
    Field(String),
    ArrayIndex(u64),
    TupleSlot(u64),
    MapKey(DocumentValue),
    SetMember(DocumentValue),
}
```

`ValuePath` addresses zero or one exact location. Resolution is type-strict and
returns structured errors for a missing value, wrong container kind, invalid
position, or noncanonical map key/member.

`MapKey(key)` selects the value associated with `key`; typed-map keys are
immutable and change only through remove plus upsert. `SetMember(member)`
selects that canonical member. Replacing or mutating a selected set member is
defined as removing the old member and inserting the resulting member, with
normal duplicate and resource validation.

JSON Pointer parses into object-field and array-index path segments. Resolution
is context aware because a pointer token is a field name in an object and an
index in an array. The `-` token is valid only as the final destination of JSON
Patch `add`; it is never a readable location.

JSON Pointer does not acquire nonstandard syntax for tuples, sets, or typed-map
keys. Native callers use `ValuePath` for those containers.

## Query Algebra

`ValueQuery` is a versioned compiled algebra that produces zero or more
locations or projected values. It supports:

- exact fields, array indexes, tuple slots, map keys, and set members;
- object, array, tuple, map, and set wildcards;
- distinct typed-map key and value traversal;
- recursive descent with explicit depth and visit bounds;
- type predicates and type-strict equality;
- explicit versioned numeric coercion;
- path, key, member, value, and subvalue projection;
- explicit `each` expansion for multi-valued index terms;
- bounded boolean predicates for partial indexes and residual filtering.

Query results follow container canonical order unless an explicit ordered
index supplies a different documented order. Query evaluation never depends
on host hash-map iteration.

A standards-oriented JSONPath subset compiles into this algebra for
JSON-compatible values. Persisted query consumers store the canonical compiled
plan and query-language version. Original source text is optional diagnostic
metadata and never determines semantics by itself.

Every evaluation has visit, depth, decoded-byte, match, and emitted-byte
budgets. Hitting a budget is a structured error, not a partial successful query
unless the caller explicitly chose a streaming API whose partial-delivery
contract says otherwise.

## Lazy Views and Read API

`DocumentValue` is the owned form. `ValueView` is a snapshot-bound lazy view
that can represent inline bytes or an indexed typed root.

Container views provide:

- exact lookup;
- forward and reverse iteration;
- bounded pages and stable snapshot-bound cursors;
- borrowed callback-scoped scalar and inline value access;
- explicit materialization to `DocumentValue`;
- child `ValueCid` access without materialization;
- path and query evaluation.

A `DocumentMap` snapshot pins the source collection version, schema,
`DocumentFormat`, and index catalog. Every nested view and cursor is bound to
that snapshot. Reusing it with another collection or version returns a version
mismatch rather than reading a nearby state.

## Mutations and Editors

Mutation has two public frontends.

1. JSON Patch provides `add`, `remove`, `replace`, `move`, `copy`, and `test`
   over JSON-compatible containers.
2. `ValuePatch` provides native operations for array insertion/removal,
   tuple-slot replacement, set insertion/removal, typed-map upsert/removal,
   object field changes, generic replacement, and type-strict test.

```rust
pub struct PatchEnvelope {
    pub expected_value_cid: Option<ValueCid>,
    pub operations: Vec<ValuePatch>,
}
```

Operations execute sequentially against one transient logical value. A later
operation observes earlier changes. The editor validates all operations and
limits, rebuilds changed containers bottom-up, stages immutable objects, and
publishes only after the complete patch succeeds.

An indexed editor groups paths by common ancestor so multiple operations do
not repeatedly load or rebuild the same container. Inline containers decode
locally. A move or copy reads its source from the transient state at the point
the operation executes.

Patch limits include operation count, path bytes, embedded value bytes, nodes
loaded, nodes staged, total comparison work, and output logical size.

Patch errors include:

- zero-based operation index;
- resolved native path and original JSON Pointer when applicable;
- stable error code;
- expected and actual container kinds;
- failed test metadata without unbounded value rendering.

## Diff

`ValueDiffer` streams deterministic path-level changes. It compares `ValueCid`
before loading descendants.

- Equal CIDs produce no changes and no descendant reads.
- Different scalar types or values produce a replacement.
- Different container types produce one replacement at the nearest changed
  path.
- Objects and typed maps merge ordered key cursors and recurse into values.
- Sets emit member additions and removals in canonical order.
- Equal-arity tuples recurse by slot; different arity replaces the tuple.
- Arrays use bounded positional smart diff.

Array diff:

1. removes equal prefix and suffix elements using child `ValueCid`;
2. runs a deterministic bounded sequence-diff algorithm over the changed
   middle;
3. emits insertions, removals, and recursive replacements within budget;
4. falls back to replacement of the nearest array subtree when its element,
   memory, or comparison budget is exceeded.

Logical diff event order is not assumed to be directly executable after array
indices shift. Patch generation emits a safe operation order. Move detection
is optional presentation logic and does not alter canonical diff identity.

Diff APIs include lazy streams, bounded pages, structural checkpoints, range or
path scoping, traversal statistics, and early termination.

## Logical and Structural Patches

The document engine distinguishes:

```text
ValuePatch
└── portable logical operations and logical preconditions

ValueStructuralPatch
└── exact base storage root, DocumentFormat digest, and subtree edits
```

Logical patches work across compatible physical layouts. Structural patches
may reuse transferred subtrees and are rejected when the base root or physical
format differs.

The existing ordered engine already models `StructuralEdit::Subtree` but its
application path rejects such edits. Supporting work completes verified
subtree application. Before reuse, the applier verifies:

- referenced bytes hash to the claimed CID;
- the tree and document format digests match;
- node level and content kind match;
- key range and end key match;
- logical entry count matches;
- edits are strictly ordered and nonoverlapping;
- imported descendants form a valid closed graph.

Subtree application goes through the canonical writer or a verified splice
path and never trusts caller-provided metadata alone.

## Three-Way Merge

Merge begins with `ValueCid` rules:

- if one side equals base, take the other;
- if left equals right, take either;
- otherwise inspect the typed values recursively.

Container semantics are:

- objects merge disjoint fields and recursively merge a field changed on both
  sides;
- typed maps merge disjoint keys and recursively merge a value changed on both
  sides;
- equal-arity tuples merge disjoint slots and recursively merge overlapping
  slots;
- sets merge deterministic membership changes by member;
- different scalar changes conflict;
- container type changes conflict unless both sides made the same change;
- array replacements at stable independent indexes merge, while overlapping
  edits or positional shifts conflict conservatively.

```rust
pub struct ValueConflict<'a> {
    pub path: ValuePath,
    pub kind: ConflictKind,
    pub base: Option<ValueView<'a>>,
    pub left: Option<ValueView<'a>>,
    pub right: Option<ValueView<'a>>,
}
```

Resolvers may select base, left, or right, delete, provide a replacement, or
leave the conflict unresolved. Conflict values stay lazy until requested.

Unresolved conflicts may be returned as a stream or persisted into an
`ArtifactMap` alongside a candidate merge root. Persisting artifacts is an
explicit atomic operation, not an implicit side effect of a failed merge.

## DocumentMap

`DocumentMap` is a versioned collection of document IDs to `DocumentRef`.

```rust
let users = engine.document_map(b"users", config)?;
let snapshot = users.snapshot()?;
let user = snapshot.document(b"user-1")?;
let city = user.get_pointer("/address/city")?;
```

The collection descriptor identifies:

- source `VersionedMap` and current source version;
- `DocumentFormat` digest;
- optional schema CID;
- exact secondary-index catalog version;
- collection limits and compatibility versions.

Collection writes accept two independent optimistic preconditions:

- expected collection version;
- expected document `ValueCid`.

The engine may automatically retry a disjoint document update after a
collection-head race. It must never weaken an explicit document precondition.

Source documents, affected secondary indexes, catalog checkpoints, immutable
version roots, and head movements publish in one `TransactionalStore`
transaction. Once a document index or schema is active, raw head-changing
`VersionedMap` routes are fenced and return a structured error directing the
caller to `DocumentMap`.

Pinned snapshots support historical values, paths, queries, diffs, proofs,
statistics, export, and exact indexed reads without substituting current
metadata.

## Declarative Secondary Indexes

Document indexes compile native value queries instead of persisting opaque
extractor callback identity.

```rust
let by_email = DocumentIndex::builder("by-email", 1)
    .key(ValueQuery::field("email"))
    .unique()
    .sparse()
    .include([
        ValueQuery::field("display_name"),
        ValueQuery::field("active"),
    ])
    .build()?;
```

A persisted index definition includes:

- canonical compiled key query and query-language version;
- `SortKey` codec and optional explicit coercion policy;
- sparse, unique, multi-valued, and partial-index semantics;
- projection queries and projection codec;
- generation, fingerprint, and state;
- path-dependency summary;
- extraction, emission, projection, build, and verification limits.

Index terms are `DocumentValue`s. Compound terms are tuples. A query operator
such as `each` explicitly expands an array or set into multiple terms. Compound
indexes do not create implicit Cartesian products; every expansion is visible
in the compiled query plan and bounded.

Physical non-unique keys are conceptually:

```text
SortKey(term) || escaped document ID
```

Unique indexes use the term as the ownership key and transactionally reject a
different document ID. This extends current `IndexedMap` v1, which deliberately
excludes uniqueness.

Before mutation, the engine compares patch paths with every index dependency
summary. An index that cannot be affected performs no extraction. Affected
indexes lazily evaluate old and new emissions, calculate deltas, enforce
uniqueness and limits, and stage their mutations with the source document.

Invalid index source types fail the write unless an explicit partial predicate
excludes that value. They are never silently skipped.

Queries support exact, prefix, range, reverse, stable cursor pagination, and
covering projections over pinned indexed snapshots. MVP includes a small
deterministic planner and explicit index selection. Explain output reports:

- chosen or rejected indexes and reasons;
- matched leading expressions and bounds;
- residual predicates;
- projection coverage;
- estimated and actual examined entries;
- query and resource-limit counters.

Full-text and vector search remain specialized derived engines. They can join
the same atomic multi-map publication flow but do not masquerade as ordered
document indexes.

## Schemas

Collections are schemaless by default. An optional immutable
`DocumentSchema` can constrain:

- scalar types, unions, nullability, ranges, and lengths;
- required, optional, and additional object fields;
- homogeneous arrays and cardinality;
- fixed tuple slots and arity;
- set member schemas;
- typed-map key and value schemas;
- nesting and logical-size limits;
- default values represented in canonical DVF.

A compatible JSON Schema subset may compile into this native algebra. The
persisted source of truth is the canonical compiled schema and schema CID, not
parser-specific objects or source-text ordering.

Patches validate affected subtrees incrementally when the compiled schema can
prove the validation scope. Whole replacement, import, and uncertain
query-based updates validate the complete value. Historical snapshots retain
their exact schema CID.

## Schema and Format Migration

Schema evolution and physical representation evolution are distinct.

Compatible validation changes may publish a new collection descriptor without
rewriting values after the pinned current collection has been validated
against the new schema. A change is compatible only when that validation
succeeds and no value transformation is required. Transforming changes:

1. pin one source collection version;
2. transform values into a shadow collection root;
3. build affected shadow indexes;
4. validate values, indexes, and graph closure;
5. atomically publish the new schema, source, indexes, and catalog if the
   source remains current.

MVP does not perform write-on-read migration.

A `DocumentFormat` migration may re-encode cells, change promotion thresholds,
or adopt a compatible physical container version while preserving `ValueCid`.
Tooling reports it as a representation migration. Logical document diff
suppresses representation-only changes even though collection storage roots
may change.

## Specialized Map Facades

Purpose-built maps are developer-facing semantic facades over the same engine.

### TupleMap

`TupleMap` uses a tuple as the typed key and a configured native value or tuple
as the value. It provides descriptor-aware construction, composite prefix and
range bounds, typed iteration, diff, merge, and schema validation.

### AddressMap

`AddressMap` maps UTF-8 names to CIDs. It provides exact lookup, prefix
iteration, typed graph walking, root verification, and an editor with add,
update, delete, and atomic flush semantics.

### ArtifactMap

`ArtifactMap` uses an `ArtifactKey` tuple and typed artifact records. It
supports artifact-kind scans, conflict artifacts, constraint or validation
artifacts, count and clear by type, resolution lifecycle, merge, and atomic
publication with a candidate source state.

### Shared behavior

The facades share:

- DVF, `ValueCid`, `SortKey`, and schemas;
- adaptive storage and lazy views;
- snapshots, history, diff, merge, and patch;
- transactions and secondary indexes;
- proofs, sync, backup, retention, and GC;
- conformance fixtures and binding-domain records.

They do not introduce independent node serializers unless a measured workload
later justifies a versioned physical specialization.

## Error Model

The public error model uses stable categories with structured details.

1. Invalid or noncanonical encoding.
2. Unsupported wire, content, schema, query, or format version.
3. Invalid scalar normalization.
4. Missing path, invalid path, or wrong container kind.
5. Patch test failure or invalid patch order.
6. Collection-version conflict or document-`ValueCid` conflict.
7. Schema violation.
8. Unique-index violation.
9. Query, diff, patch, graph, allocation, key, or comparison resource limit.
10. Missing child, CID mismatch, graph cycle, or authenticated corruption.
11. Index definition, runtime catalog, cursor, or snapshot mismatch.
12. Unsupported raw operation while document invariants are active.

Errors do not render unbounded values. Patch and schema errors carry exact
operation and path context. Unique-index errors identify the index and
conflicting document ID. Store errors preserve their source while mapping to a
stable document-engine category.

## Resource Limits and Untrusted Input

All persisted and imported bytes are untrusted. `DocumentLimits` bounds:

- total DVF and logical value bytes;
- string and byte-scalar length;
- nesting depth;
- entries per container and total graph entries;
- typed-map key and set-member encoded bytes;
- comparison work and sort memory;
- graph objects, graph depth, and referenced bytes;
- patch operations, embedded values, loaded nodes, and staged nodes;
- query visits, recursion, matches, and projected bytes;
- diff elements, array-diff work, pending events, and checkpoint bytes;
- index emissions, projections, mutations, build memory, and retries.

Decoding, graph walking, deep comparison, hashing, and query evaluation use
explicit stacks where input-controlled recursion could exhaust the call stack.
Limit failures occur before publication. Streaming APIs document whether an
error can follow already-delivered results; owned and atomic APIs never return
a partial successful value.

## Concurrency and Transactions

Document collection heads use optimistic compare-and-swap. Convenience writes
may retry collection-head conflicts by reopening a fresh snapshot and
reapplying a deterministic operation. They do not retry after an explicit
document `ValueCid` precondition fails.

One transaction can update multiple documents, specialized maps, materialized
views, and indexes. It validates all original heads and stages every immutable
object before committing root movements. Application migration functions and
merge resolvers must be deterministic, side-effect free, and retry safe when
used by automatically retried operations.

Read snapshots remain immutable. Cursors, views, and proofs never silently
advance to a newer head.

## Verification and Repair

Verification has explicit scopes.

```text
verify_value       one typed value graph and ValueCid
verify_document    collection membership plus DocumentRef and nested graph
verify_collection  source map, schema, format, roots, and catalog
verify_indexes     semantic rebuild comparison against exact source version
```

Verification checks canonical values, sort order, duplicate exclusion,
metadata counts, content kinds, child references, physical CIDs, logical
`ValueCid`, schema validity, and index emissions.

Repair never occurs implicitly during reads. It shadow-builds a replacement,
verifies it, and publishes through an explicit conditional operation. Failed
repair publication leaves only unreachable immutable objects.

## Proofs

A value-path proof chains:

1. collection source-map membership for document ID and `DocumentRef`;
2. typed-root and `DocumentFormat` identity;
3. ordered-container membership or absence at every path segment;
4. inline cell bytes or referenced child root;
5. final value or absence claim and logical `ValueCid`.

Proof verification is store independent and bounded. JSON Pointer proofs are a
presentation of native `ValuePath` proofs. Query proofs are limited to query
forms whose completeness can be demonstrated by ordered range or prefix
proofs; unsupported proof shapes return an explicit error.

## Sync, Backup, Retention, and GC

Export and sync operate on closed typed content graphs. Bundles include the
collection manifest, exact schema and index descriptors, source and index
trees, typed value roots, and globally deduplicated CID-sorted content objects.

Import verifies all object bytes, kinds, versions, graph closure, logical
metadata, schemas, and runtime compatibility before conditional publication.

Retention begins with named collection, historical version, migration, repair,
and artifact roots. Typed graph traversal makes every referenced nested
container live. GC must not infer liveness from caches. Operational live-root
leases or external serialization remain necessary where concurrent sweep and
read semantics require them.

## Observability and Developer Experience

Metrics and traces include:

- inline/indexed promotion and demotion;
- typed containers and nodes read, reused, written, or verified;
- bytes decoded, materialized, projected, and staged;
- subtrees skipped by `ValueCid`;
- array-diff budget use and replacement fallback;
- patch paths grouped and ancestors rebuilt;
- selected indexes, examined entries, residual predicates, and coverage;
- index emissions and uniqueness probes;
- graph objects copied, proved, retained, and collected;
- schema and format migration progress;
- optimistic retries and precondition conflicts.

`prolly-inspect` gains typed document summaries, bounded path lookup, root and
schema display, container statistics, index descriptors, graph verification,
and logical-versus-physical diff. It redacts or truncates values by default.

Explain APIs exist for query, patch, diff fallback, merge conflicts, index
maintenance, and representation migration. They return stable structured data
with a human-readable formatter.

## Portable Bindings

Rust establishes the authoritative implementation, but persisted formats are
language neutral from the first commit.

Bindings expose:

- owned `DocumentValue` records;
- lazy document and container handles where the host supports resources;
- canonical DVF import/export;
- native path and query builders;
- JSON Pointer, JSON Patch, and JSON profiles;
- patch, diff, conflict, schema, index, and explain records;
- bounded pages and deterministic disposal.

Hosts whose native map or set cannot represent arbitrary typed keys use entry
sequences and iterators. No binding converts a typed map into a string-keyed
host object and loses key identity.

A binding may not claim document-engine compatibility until it passes the same
DVF, `ValueCid`, `SortKey`, path, patch, diff, merge, schema, index, and error
fixtures as Rust.

## Testing Strategy

### Golden conformance

Checked-in language-neutral fixtures cover:

- every scalar boundary and invalid scalar form;
- empty, singleton, nested, and large containers;
- arbitrary container keys and set members;
- all cross-type and within-type ordering boundaries;
- DVF bytes, `ValueCid`, and `SortKey` bytes;
- JSON and Extended JSON conversion profiles;
- schemas, query plans, patches, conflicts, and errors;
- supported and unsupported version behavior.

### Property and model testing

Property tests prove:

- canonical encode/decode round trips;
- equivalent construction produces identical DVF and `ValueCid`;
- `ValueOrd(a, b)` equals byte comparison of `SortKey(a)` and `SortKey(b)`;
- inline and indexed views have identical behavior;
- promotion and physical migration preserve `ValueCid`;
- map and set duplicate rules are type strict;
- generated logical patches transform base into target;
- structural patches match logical results;
- merge satisfies base/left/right identities and deterministic resolver rules.

A simple in-memory owned-value model acts as the oracle for lazy indexed
paths, queries, patches, diff, and merge.

### Corruption and fuzz testing

Fuzzers cover DVF, cells, sort keys, query plans, schemas, content manifests,
path parsers, patch streams, graph walking, and subtree imports. Tests inject:

- truncation and trailing bytes;
- overlong lengths and integer encodings;
- invalid order and duplicates;
- wrong kinds and CIDs;
- excessive nesting, keys, values, and comparison work;
- malformed JSON Pointer and JSONPath input;
- inconsistent logical counts and subtree metadata.

### Transaction and failure testing

Tests cover concurrent disjoint and overlapping document updates, explicit
preconditions, unique races, index extraction failures, schema failures,
transaction rollback, crash injection, stale cursors, interrupted migration,
failed shadow builds, and orphan collection by GC.

### Graph and historical testing

Every value kind participates in proof, sync, backup, import, retention, GC,
historical snapshot, index rebuild, and format migration tests. A bundle is
valid only when a fresh destination can verify and read the same logical
values without access to the source store.

## Performance Acceptance

Correct asymptotic and bounded behavior is part of MVP acceptance.

1. Equal `ValueCid` diff performs no descendant reads.
2. Indexed exact-path lookup reads work proportional to path depth and
   container search rather than complete logical value size.
3. A localized object or typed-map update rewrites only the affected container
   and ancestor chain, aside from deterministic chunk resynchronization.
4. Array append and element replacement are localized; positional insertion
   remains explicitly measured and bounded by write limits.
5. Array diff never exceeds its configured sequence-work budget.
6. An index whose dependency summary cannot overlap a patch performs no value
   extraction.
7. Small inline values create no nested container trees.
8. Streaming query, diff, and conflict APIs retain bounded working memory.
9. Arbitrary typed-key comparison and encoding never exceed configured work or
   output limits.

Benchmarks include small-record CRUD, deep field lookup and update, large
objects, large arrays with append and front insertion, nested typed maps,
recursive set members, path diff, three-way merge, indexed writes, unique
races, migration, proof, sync, and GC.

## Delivery Sequence

Implementation follows dependency order.

1. DVF, owned `DocumentValue`, scalar normalization, and errors.
2. `ValueCid`, `ValueOrd`, `SortKey`, limits, and golden fixtures.
3. `DocumentFormat`, `DocumentRef`, `ValueCell`, and typed content kinds.
4. Container builders, adaptive promotion, graph walking, sync, GC, and
   verification.
5. Lazy views, native paths, JSON Pointer, query algebra, and JSON profiles.
6. Editors, JSON Patch, native `ValuePatch`, and preconditions.
7. Streaming diff, bounded array diff, logical patch generation, and merge.
8. Verified structural subtree patch application.
9. `DocumentMap`, collection descriptors, transactions, and subscriptions.
10. Schemas, validation, shadow migration, and representation migration.
11. Declarative indexes, uniqueness, dependency pruning, planner, and explain.
12. `TupleMap`, `AddressMap`, and `ArtifactMap` facades.
13. Nested proofs, inspection tooling, portable bindings, and complete
    conformance/performance gates.

This sequence is a dependency outline, not the implementation task plan. The
implementation plan will split it into independently verifiable commits after
this design is reviewed.

## MVP Completion Criteria

MVP is complete only when all of the following are true.

1. Every agreed scalar and container has stable DVF, `ValueCid`, `SortKey`,
   equality, ordering, and invalid-input fixtures.
2. Tuple, set, and typed map participate fully in paths, queries, patches,
   diff, merge, schemas, proofs, sync, backup, retention, and GC. Serialization
   alone does not count as support.
3. Inline and indexed representations are behaviorally identical.
4. JSON Pointer and JSON Patch pass standards-oriented fixtures on the
   JSON-compatible subset.
5. Native paths and patches cover every container.
6. Logical patches reproduce target values and verified structural patches
   reproduce the same logical result.
7. Document source and all active ordered indexes publish atomically.
8. Unique, sparse, multi-valued, compound, partial, and covering index
   semantics are deterministic and verified.
9. Schemas are optional, content addressed, historical, and enforced on every
   invariant-preserving write route.
10. Raw write routes fail closed while document invariants are active.
11. Typed content graphs are closed under proof, sync, backup, import,
    retention, and GC.
12. Resource limits and corruption tests cover every untrusted recursive path.
13. Performance acceptance tests demonstrate localized large-value behavior
    and bounded fallbacks.
14. Rust documentation and examples let a developer build a typed document
    collection, create an index, patch a nested value, inspect a diff, merge
    branches, and migrate a schema without using raw bytes.
