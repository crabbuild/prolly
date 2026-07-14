# Secondary Index Documentation Design

**Date:** 2026-07-13

## Objective

Make the shipped `IndexedMap` secondary-index layer understandable and usable
without requiring readers to study its implementation specification. The
documentation must serve developers who are new to secondary indexes and
experienced Rust/database engineers through one progressive learning path.

The examples will use typed serde models serialized as JSON so that schemas,
derived terms, projections, and application access patterns resemble production
code. Low-level byte behavior will still be made explicit wherever it affects
correctness or ordering.

## Documentation architecture

### Progressive user guide

Add `docs/secondary-indexes.md` as the primary entry point. It will be organized
in this order:

1. What problem a secondary index solves.
2. The `IndexedMap` mental model and strict consistency guarantee.
3. A typed serde/JSON quickstart.
4. Adding an index after source data already exists.
5. Strict indexed writes and snapshot-bound queries.
6. Choosing `KeysOnly`, `Include`, or `All` projection.
7. Sparse, multi-valued, derived, composite, and ordered terms.
8. Pagination, cursors, and historical snapshots.
9. Real-world access-pattern recipes.
10. Lifecycle and production operations.
11. Errors, resource limits, and troubleshooting.
12. V1 boundaries and a design checklist.

The guide will optimize for first-use comprehension while retaining precise
behavioral details needed for production decisions. It will link to the design
reference rather than duplicating persisted-format internals.

### Semantics and implementation reference

Retain `docs/secondary-index-design.md` as the concise reference for consistency,
runtime definitions, projections, lifecycle, retention, transfer, DynamoDB
analogies, and deliberate V1 exclusions. Add a prominent link to the progressive
guide and remove or adjust wording only where necessary to avoid contradictory
or duplicated onboarding guidance.

### Runnable example

Add `examples/secondary_index_json.rs`. It will be a runnable, assertion-backed
example using serde/JSON records and the same schema as the user guide. The
existing lower-level `examples/secondary_index.rs` remains useful as a compact
API/lifecycle tour.

### Discovery

Update `README.md` to describe the secondary-index layer in application terms
and link to:

- the progressive guide;
- the typed JSON example;
- the semantics/design reference.

## Shared application model

The guide and runnable example will use a SaaS user-directory record with these
logical fields:

```rust
struct User {
    tenant_id: String,
    email: String,
    status: UserStatus,
    display_name: String,
    tags: Vec<String>,
    created_at: u64,
}
```

The source-map primary key will be a stable user ID. JSON source values will be
decoded by deterministic extractors. Extractor failures will be returned as
`SecondaryIndexError`, not panics.

The guide will introduce these indexes progressively:

| Index | Pattern taught |
| --- | --- |
| `by-status` | Basic non-unique lookup with one term per source row |
| `by-tag` | Sparse and multi-valued terms |
| `by-tenant-status` | Canonical composite terms and tenant-scoped lookup |
| `by-email-domain` | A term derived from stored data |
| `by-created-day` | Lexicographically ordered terms and range scans |
| `by-status-summary` | `Include` projection with a typed list-view summary |
| `by-status-full` | `All` projection and write/storage amplification |

Composite and ordered term examples must use unambiguous canonical byte
encodings. They must not rely on delimiter concatenation that can collide or on
human-formatted numbers whose lexical and numeric order differ.

## Concepts and data flow

The guide will establish this model before introducing lifecycle details:

```text
primary key + JSON source value
        |
        v extractor
zero or more index terms
        |
        v
(term, primary key) -> projection
```

It will explicitly explain:

- the source map is authoritative;
- physical entries are ordered by term and then primary key;
- terms and primary keys are arbitrary bytes and compare lexicographically;
- an extractor may emit zero terms for a sparse index;
- an extractor may emit multiple terms for tags and other repeated fields;
- exact duplicate emissions are deduplicated;
- V1 indexes are non-unique;
- callbacks must be deterministic, side-effect free, and retry safe;
- callback code is not persisted, so every process must register an exact
  runtime definition;
- source, index, catalog, and head changes publish atomically;
- a pinned `IndexedSnapshot` prevents torn source/index reads.

## Query and projection guidance

The quickstart will demonstrate `exact`, `primary_keys`, and `records`. Later
sections will add `prefix`, half-open `range`, forward and reverse pages,
serialized cursors, and exact historical snapshots.

Projection guidance will use a decision table and concrete examples:

- `KeysOnly` for general-purpose indexes and records that can be batch-loaded
  from the pinned source snapshot;
- `Include` for an index-only list or search-result view with an intentionally
  small typed summary;
- `All` only when copying the complete raw stored value is worth the write and
  storage amplification.

The guide will distinguish `projected`, which never reads the source, from
`records`, which performs one ordered batched read against the pinned source.
It will also explain that projection-only changes rewrite `Include` and `All`
entries even when the term is unchanged.

## Real-world recipes

Each recipe will state the source primary key, emitted term, recommended
projection, supported query, and important trade-off:

1. User or customer directory by account status.
2. Tagged documents or products with zero or many tags.
3. Tenant-scoped task queues using a composite tenant/status term.
4. Order or support-ticket dashboards using a small covering summary.
5. Time-bucketed events using canonical big-endian ordered terms.
6. Email-domain or other derived-field lookup.

The recipes will also identify when a secondary index is the wrong tool, such as
enforcing uniqueness, full-text ranking, fuzzy search, asynchronous analytics,
or independently scaled distributed indexes.

## Lifecycle and operations

The advanced path will explain the operational sequence:

1. Register runtime definitions before opening an active index.
2. Open `IndexedMap` around an existing or empty source.
3. Run `ensure_index` to perform a retryable snapshot build and atomic
   activation.
4. Route all subsequent source mutations through `IndexedMap`.
5. Query only pinned indexed snapshots.
6. Monitor health and metrics.
7. Use `verify_index` or `verify_all` for explicit semantic integrity checks.
8. Use `repair_index` only as an explicit maintenance operation.
9. Introduce changed semantics with a greater generation through
   `replace_index`.
10. Coordinate retention before indexed GC.
11. Transfer current state with verified indexed bundles.

The guide will explain that `is_valid` means logical entry equality and
`is_canonical` additionally means the selected tree root matches a sorted
rebuild. It will state the retry and unpublished-node implications of build
conflicts and the need to serialize GC with live readers or provide an external
root lease.

## Errors and troubleshooting

Failures will be grouped by the action an operator or developer should take:

| Failure class | Expected response |
| --- | --- |
| Invalid definition or extraction | Correct callback identity, schema handling, or malformed source data |
| Projection mismatch | Emit projection bytes only for the configured mode |
| Resource limit exceeded | Reduce term/projection fanout or deliberately adjust explicit limits |
| Transaction conflict | Use coordinator retrying APIs or retry the conditional operation |
| `IndexesRequireIndexedMap` | Replace a raw `VersionedMap` mutation with its indexed counterpart |
| Cursor/version mismatch | Restart paging from the intended pinned snapshot |
| Missing/mismatched runtime definition | Deploy the exact name, generation, extractor ID, projection, and limits |
| Verification failure | Diagnose source/index drift and invoke repair explicitly if appropriate |

Unsupported V1 operations will be described as deliberate fail-closed behavior,
not transient errors.

## Verification requirements

Documentation changes are complete only when:

- quickstart snippets and focused examples are doctested where practical;
- `secondary_index_json` runs successfully and asserts every documented access
  pattern;
- the runnable example covers post-population creation, sparse and multi-valued
  extraction, composite terms, every projection mode, indexed updates, exact
  queries, range queries, and verification;
- every documented method, error, and semantic statement matches the public
  implementation;
- links from the README, user guide, and design reference resolve;
- formatting and strict Clippy pass;
- all targets and all features pass;
- all doctests pass.

## Scope

This work documents the shipped Rust V1 behavior. It does not add new index
semantics, change persisted bytes, add unique or asynchronous indexes, introduce
derive macros, or broaden the current lifecycle API.
