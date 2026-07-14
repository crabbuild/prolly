# Secondary Index Documentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Deliver a progressive beginner-to-advanced secondary-index guide, a runnable typed serde/JSON example, and clear discovery links without changing IndexedMap behavior or persisted bytes.

**Architecture:** `docs/secondary-indexes.md` becomes the application-facing learning path, while `docs/secondary-index-design.md` remains the precise semantics and implementation reference. `examples/secondary_index_json.rs` is the executable source of truth for production-shaped record extraction and access patterns; README and docs-index links make both layers discoverable.

**Tech Stack:** Rust 1.81, `prolly-map` 0.2.1, serde derive, `serde_json`, Markdown, Cargo examples and test tooling.

## Global Constraints

- Serve newcomers and experienced Rust/database engineers through one progressive beginner-to-advanced path.
- Use typed serde/JSON records that resemble production applications.
- Use canonical segment-safe or big-endian ordered term encodings; never delimiter concatenation for composite terms.
- Keep `docs/secondary-index-design.md` as the semantics/reference layer.
- Document only shipped Rust V1 behavior; do not change index semantics or persisted bytes.
- Preserve unrelated untracked workspace content under `.baoyu-skills/`, `articles/`, and `bindings/kotlin/.gradle/`.
- Every documented method, error name, and guarantee must match the public implementation.

---

## File structure

- Create `examples/secondary_index_json.rs`: runnable typed source records, index definitions, post-population activation, writes, queries, projections, range scans, cursor paging, and verification.
- Create `docs/secondary-indexes.md`: progressive user guide and real-world cookbook.
- Modify `docs/secondary-index-design.md`: identify it as the semantics/reference layer and link to the user guide.
- Modify `docs/README.md`: add separate user-guide and design-reference entries.
- Modify `README.md`: feature discovery and runnable-example links.

### Task 1: Typed JSON example

**Files:**
- Create: `examples/secondary_index_json.rs`

**Interfaces:**
- Consumes: `SecondaryIndex`, `SecondaryIndexRegistry`, `SecondaryIndexEntry`, `IndexProjection`, `KeyBuilder`, `IndexedMap`, `IndexedSnapshot`, serde, and `serde_json`.
- Produces: a runnable example named `secondary_index_json` whose model and index names are reused verbatim by the guide.

- [ ] **Step 1: Verify that the planned example does not exist**

Run:

```bash
cargo run --example secondary_index_json
```

Expected: failure containing `no example target named 'secondary_index_json'`.

- [ ] **Step 2: Create the typed record model and canonical term helpers**

Create `examples/secondary_index_json.rs` with these concrete types and helpers:

```rust
use prolly::{
    Config, Error, IndexProjection, KeyBuilder, MemStore, Prolly, SecondaryIndex,
    SecondaryIndexEntry, SecondaryIndexError, SecondaryIndexRegistry,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
enum UserStatus {
    Active,
    Invited,
    Suspended,
}

impl UserStatus {
    fn term(self) -> &'static [u8] {
        match self {
            Self::Active => b"active",
            Self::Invited => b"invited",
            Self::Suspended => b"suspended",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct User {
    tenant_id: String,
    email: String,
    status: UserStatus,
    display_name: String,
    tags: Vec<String>,
    created_at: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
struct UserSummary {
    display_name: String,
    email: String,
}

fn decode_user(value: &[u8]) -> Result<User, SecondaryIndexError> {
    serde_json::from_slice(value)
        .map_err(|error| SecondaryIndexError::new(format!("invalid user JSON: {error}")))
}

fn encode_user(user: &User) -> Result<Vec<u8>, Error> {
    serde_json::to_vec(user).map_err(|error| Error::Serialize(error.to_string()))
}

fn tenant_status_term(tenant_id: &str, status: UserStatus) -> Vec<u8> {
    KeyBuilder::new()
        .push_str(tenant_id)
        .push_segment(status.term())
        .finish()
}

fn created_day_term(day: u64) -> Vec<u8> {
    KeyBuilder::new().push_u64(day).finish()
}
```

- [ ] **Step 3: Add all documented runtime definitions**

Add a `registry()` function that registers these exact definitions:

```rust
fn registry() -> Result<SecondaryIndexRegistry, Error> {
    let by_status = SecondaryIndex::non_unique(
        "by-status",
        1,
        "examples.users.by-status/v1",
        |_, value| Ok(vec![decode_user(value)?.status.term().to_vec()]),
    )?;
    let by_tag = SecondaryIndex::non_unique(
        "by-tag",
        1,
        "examples.users.by-tag/v1",
        |_, value| {
            Ok(decode_user(value)?
                .tags
                .into_iter()
                .map(|tag| tag.to_ascii_lowercase().into_bytes())
                .collect())
        },
    )?;
    let by_tenant_status = SecondaryIndex::non_unique(
        "by-tenant-status",
        1,
        "examples.users.by-tenant-status/v1",
        |_, value| {
            let user = decode_user(value)?;
            Ok(vec![tenant_status_term(&user.tenant_id, user.status)])
        },
    )?;
    let by_email_domain = SecondaryIndex::non_unique(
        "by-email-domain",
        1,
        "examples.users.by-email-domain/v1",
        |_, value| {
            let user = decode_user(value)?;
            Ok(user
                .email
                .rsplit_once('@')
                .map(|(_, domain)| vec![domain.to_ascii_lowercase().into_bytes()])
                .unwrap_or_default())
        },
    )?;
    let by_created_day = SecondaryIndex::non_unique(
        "by-created-day",
        1,
        "examples.users.by-created-day/v1",
        |_, value| Ok(vec![created_day_term(decode_user(value)?.created_at / 86_400)]),
    )?;
    let by_status_summary = SecondaryIndex::builder(
        "by-status-summary",
        1,
        "examples.users.by-status-summary/v1",
    )
    .projection(IndexProjection::Include)
    .extract(|_, value| {
        let user = decode_user(value)?;
        let summary = UserSummary {
            display_name: user.display_name,
            email: user.email,
        };
        let projection = serde_json::to_vec(&summary)
            .map_err(|error| SecondaryIndexError::new(error.to_string()))?;
        Ok(vec![SecondaryIndexEntry::included(
            user.status.term(),
            projection,
        )])
    })?;
    let by_status_full = SecondaryIndex::builder(
        "by-status-full",
        1,
        "examples.users.by-status-full/v1",
    )
    .projection(IndexProjection::All)
    .extract_terms(|_, value| Ok(vec![decode_user(value)?.status.term().to_vec()]))?;

    SecondaryIndexRegistry::new()
        .register(by_status)?
        .register(by_tag)?
        .register(by_tenant_status)?
        .register(by_email_domain)?
        .register(by_created_day)?
        .register(by_status_summary)?
        .register(by_status_full)
}
```

- [ ] **Step 4: Add the end-to-end scenario and assertions**

Add this complete `main` function:

```rust
fn main() -> Result<(), Error> {
    let engine = Prolly::new(Arc::new(MemStore::new()), Config::default());
    let source = engine.versioned_map(b"users");

    let ada = User {
        tenant_id: "acme".into(),
        email: "ada@example.com".into(),
        status: UserStatus::Active,
        display_name: "Ada".into(),
        tags: vec!["rust".into(), "database".into()],
        created_at: 10 * 86_400,
    };
    let grace_invited = User {
        tenant_id: "acme".into(),
        email: "grace@example.com".into(),
        status: UserStatus::Invited,
        display_name: "Grace".into(),
        tags: vec!["rust".into()],
        created_at: 11 * 86_400,
    };
    let lin = User {
        tenant_id: "globex".into(),
        email: "lin@globex.test".into(),
        status: UserStatus::Active,
        display_name: "Lin".into(),
        tags: vec!["local-first".into()],
        created_at: 12 * 86_400,
    };

    source.put(b"user-001", encode_user(&ada)?)?;
    source.put(b"user-002", encode_user(&grace_invited)?)?;
    source.put(b"user-003", encode_user(&lin)?)?;

    let users = engine.indexed_map(b"users", registry()?)?;
    for name in [
        b"by-status".as_slice(),
        b"by-tag".as_slice(),
        b"by-tenant-status".as_slice(),
        b"by-email-domain".as_slice(),
        b"by-created-day".as_slice(),
        b"by-status-summary".as_slice(),
        b"by-status-full".as_slice(),
    ] {
        users.ensure_index(name)?;
    }

    let grace_active = User {
        status: UserStatus::Active,
        ..grace_invited
    };
    let mina = User {
        tenant_id: "globex".into(),
        email: "mina@internal".into(),
        status: UserStatus::Suspended,
        display_name: "Mina".into(),
        tags: Vec::new(),
        created_at: 13 * 86_400,
    };
    let grace_bytes = encode_user(&grace_active)?;
    let mina_bytes = encode_user(&mina)?;
    users.edit(|edit| {
        edit.put(b"user-002", grace_bytes);
        edit.put(b"user-004", mina_bytes);
    })?;

    let snapshot = users.snapshot()?;
    let by_status = snapshot.index(b"by-status")?;
    assert_eq!(
        by_status.primary_keys(b"active")?,
        vec![b"user-001".to_vec(), b"user-002".to_vec(), b"user-003".to_vec()]
    );
    assert_eq!(
        snapshot.index(b"by-tag")?.primary_keys(b"rust")?,
        vec![b"user-001".to_vec(), b"user-002".to_vec()]
    );
    assert_eq!(
        snapshot
            .index(b"by-tenant-status")?
            .primary_keys(&tenant_status_term("acme", UserStatus::Active))?,
        vec![b"user-001".to_vec(), b"user-002".to_vec()]
    );
    assert_eq!(
        snapshot
            .index(b"by-email-domain")?
            .primary_keys(b"example.com")?,
        vec![b"user-001".to_vec(), b"user-002".to_vec()]
    );
    assert_eq!(
        snapshot
            .index(b"by-created-day")?
            .range(&created_day_term(10), Some(&created_day_term(12)))?
            .len(),
        2
    );

    let summary_bytes = snapshot.index(b"by-status-summary")?.projected(b"active")?[0]
        .1
        .clone()
        .expect("Include stores summary bytes");
    let summary: UserSummary = serde_json::from_slice(&summary_bytes)
        .map_err(|error| Error::Deserialize(error.to_string()))?;
    assert_eq!(summary.display_name, "Ada");

    let full_bytes = snapshot.index(b"by-status-full")?.projected(b"active")?[0]
        .1
        .clone()
        .expect("All stores the source value");
    let full: User = serde_json::from_slice(&full_bytes)
        .map_err(|error| Error::Deserialize(error.to_string()))?;
    assert_eq!(full, ada);

    let records = by_status.records(b"active")?;
    let decoded_records: Vec<User> = records
        .iter()
        .map(|(_, value)| {
            serde_json::from_slice(value)
                .map_err(|error| Error::Deserialize(error.to_string()))
        })
        .collect::<Result<_, _>>()?;
    assert_eq!(decoded_records.len(), 3);

    let first = by_status.exact_page(b"active", None, 1)?;
    let cursor_bytes = first
        .next_cursor
        .as_ref()
        .expect("more active users remain")
        .to_bytes()?;
    let cursor = prolly::SecondaryIndexCursor::from_bytes(&cursor_bytes)?;
    let second = by_status.exact_page(b"active", Some(&cursor), 1)?;
    assert_eq!(first.matches.len(), 1);
    assert_eq!(second.matches.len(), 1);
    assert_ne!(first.matches[0].primary_key, second.matches[0].primary_key);

    let verification = users.verify_all(&snapshot.id().source_version)?;
    assert_eq!(verification.len(), 7);
    assert!(verification.iter().all(prolly::IndexVerification::is_valid));

    println!(
        "verified {} active indexes for {} active users",
        users.health()?.active_indexes.len(),
        by_status.primary_keys(b"active")?.len()
    );
    Ok(())
}
```

- [ ] **Step 5: Run the example and lint it**

Run:

```bash
cargo run --example secondary_index_json
cargo clippy --example secondary_index_json --all-features -- -D warnings
```

Expected: both commands exit 0; the example prints `verified 7 active indexes`.

- [ ] **Step 6: Commit the executable example**

```bash
git add examples/secondary_index_json.rs
git commit -m "docs(index): add typed JSON example"
```

### Task 2: Progressive secondary-index guide

**Files:**
- Create: `docs/secondary-indexes.md`
- Reference: `examples/secondary_index_json.rs`

**Interfaces:**
- Consumes: the exact `User`, `UserStatus`, `UserSummary`, helper, and index names from Task 1.
- Produces: the primary user-facing guide linked by Task 3.

- [ ] **Step 1: Establish the guide outline and verify every required concept has a home**

Create `docs/secondary-indexes.md` with these exact headings in this order:

```markdown
# Secondary Indexes with IndexedMap
## When you need a secondary index
## Mental model
## Quickstart: index typed JSON records
### Define the source record
### Define and register an index
### Add an index after data already exists
### Write through IndexedMap
### Query a pinned snapshot
## Terms are an access-pattern design
### Sparse terms
### Multi-valued terms
### Derived terms
### Composite terms
### Ordered numeric and time terms
## Choose a projection
### KeysOnly
### Include
### All
## Query APIs
### Exact, prefix, and range
### Resolve complete source records
### Page forward and backward
### Historical snapshots
## Real-world recipes
### Customer directory by status
### Tagged documents and products
### Tenant-scoped task queue
### Order or ticket dashboard
### Time-bucketed events
### Derived email-domain lookup
## Consistency and write behavior
## Creating and replacing indexes
## Production operations
### Health and metrics
### Verify and repair
### Retention and garbage collection
### Export and import
## Errors and troubleshooting
## When not to use IndexedMap
## V1 feature boundary
## Checklist
## Further reading
```

Run:

```bash
rg -n '^##? ' docs/secondary-indexes.md
```

Expected: every heading above appears exactly once and in order.

- [ ] **Step 2: Write the beginner path with copyable JSON code**

The first six sections must:

- contrast a primary-key lookup with a status access pattern;
- show the physical relationship `(term, primary_key) -> projection`;
- state that the source map is authoritative;
- include the `User` and `UserStatus` serde definitions from Task 1;
- show `decode_user` returning `SecondaryIndexError`;
- register `by-status` with generation `1` and extractor ID
  `examples.users.by-status/v1`;
- populate `versioned_map(b"users")` before calling `ensure_index`;
- route all later writes through `IndexedMap`;
- query `primary_keys` and `records` from one pinned `IndexedSnapshot`.

Use compilable Rust blocks for self-contained fragments and link to
`../examples/secondary_index_json.rs` after the first abbreviated block.

- [ ] **Step 3: Write term-design and projection guidance**

Document these exact rules:

- zero emissions make an index sparse;
- multiple emissions model repeated fields such as tags;
- exact duplicate emissions are deduplicated;
- index entries are non-unique and order by term then primary key;
- arbitrary bytes compare lexicographically;
- `KeyBuilder::push_segment` prevents composite-component collisions;
- `KeyBuilder::push_u64` and big-endian helpers preserve numeric order;
- extractors must be deterministic, side-effect free, and retry safe.

Add a projection decision table with columns `Mode`, `Stored in index`, `Source read`, `Use when`, and `Cost`. State that `projected` never reads the source, `records` performs one ordered `get_many` against the pinned source, and projection-only changes rewrite `Include` and `All` entries.

- [ ] **Step 4: Write query, paging, and historical-read guidance**

Show concrete calls for:

```rust
let by_status = snapshot.index(b"by-status")?;
let exact = by_status.exact(b"active")?;
let prefix = by_status.prefix(b"act")?;
let range = by_status.range(b"active", Some(b"suspended"))?;
let first = by_status.exact_page(b"active", None, 25)?;
let second = by_status.exact_page(b"active", first.next_cursor.as_ref(), 25)?;
```

Explain cursor serialization, snapshot/fingerprint binding,
`IndexCursorVersionMismatch`, `snapshot_at(source_version)`, and
`snapshot_by_id(IndexedSnapshotId)` without promising source-scan fallback.

- [ ] **Step 5: Write the six real-world recipes**

For each recipe, include one table row with: source primary key, emitted term,
projection recommendation, supported query, and trade-off. Use these exact
recommendations:

- customer status: `KeysOnly` by default;
- tags: `KeysOnly`, zero or many normalized tag terms;
- tenant task queue: composite `(tenant, status)` plus optional ordered time segment;
- dashboard: `Include` with a deliberately small `UserSummary`-like projection;
- time buckets: `KeysOnly` with canonical `u64` day/bucket terms;
- email domain: sparse derived lowercase domain term.

- [ ] **Step 6: Write production operations, troubleshooting, and boundaries**

Cover the lifecycle in this order: register definitions, open coordinator,
`ensure_index`, indexed mutations, pinned queries, health/metrics,
`verify_all`, explicit `repair_index`, greater-generation `replace_index`,
coordinated `keep_last`, `plan_indexed_gc`, `export_current`, and
`import_current`.

Add a troubleshooting table for:

- `InvalidIndexDefinition`;
- `IndexExtractionFailed`;
- `IndexProjectionMismatch`;
- `IndexResourceLimitExceeded`;
- `TransactionConflict`;
- `IndexesRequireIndexedMap`;
- `IndexCursorVersionMismatch`;
- `IndexRuntimeDefinitionMissing`;
- `IndexDefinitionMismatch`;
- checkpoint/verification failures;
- `IndexOperationUnsupported`.

State explicitly that unique constraints, full-text relevance, fuzzy search,
asynchronous analytics, and independently scaled distributed GSIs are not V1
use cases.

- [ ] **Step 7: Review the guide against the executable example and API**

Run:

```bash
rg -n "by-status|by-tag|by-tenant-status|by-email-domain|by-created-day|by-status-summary|by-status-full" docs/secondary-indexes.md examples/secondary_index_json.rs
rg -n "InvalidIndexDefinition|IndexExtractionFailed|IndexProjectionMismatch|IndexResourceLimitExceeded|TransactionConflict|IndexesRequireIndexedMap|IndexCursorVersionMismatch|IndexRuntimeDefinitionMissing|IndexDefinitionMismatch|IndexOperationUnsupported" src/prolly/error.rs docs/secondary-indexes.md
git diff --check
```

Expected: all seven index names occur in both files, every documented error occurs in the implementation and guide, and `git diff --check` exits 0.

- [ ] **Step 8: Commit the progressive guide**

```bash
git add docs/secondary-indexes.md
git commit -m "docs(index): add progressive user guide"
```

### Task 3: Documentation discovery and reference boundaries

**Files:**
- Modify: `README.md`
- Modify: `docs/README.md`
- Modify: `docs/secondary-index-design.md`

**Interfaces:**
- Consumes: `docs/secondary-indexes.md` and `examples/secondary_index_json.rs`.
- Produces: stable navigation between beginner guide, executable example, and design reference.

- [ ] **Step 1: Add a user-guide banner to the design reference**

Immediately after the opening paragraph in `docs/secondary-index-design.md`, add:

```markdown
> New to `IndexedMap`? Start with [Secondary Indexes with IndexedMap](secondary-indexes.md)
> for a typed JSON quickstart, access-pattern guidance, and production recipes.
> This document is the precise semantics and implementation reference.
```

Keep persisted semantics, lifecycle, DynamoDB analogy, and V1 exclusion content in the design reference.

- [ ] **Step 2: Split the docs index into guide and reference entries**

In `docs/README.md`, replace the existing single secondary-index entry with:

```markdown
- [Secondary Indexes with IndexedMap](secondary-indexes.md): progressive typed
  JSON guide, query patterns, projections, lifecycle, and real-world recipes.
- [Secondary Index Semantics and Design](secondary-index-design.md): strict
  consistency model, persisted identity, lifecycle, retention, and V1 boundaries.
```

- [ ] **Step 3: Improve README discovery**

In the README feature list, make `IndexedMap` link to
`docs/secondary-indexes.md`. In the examples list, add
`examples/secondary_index_json.rs` immediately after `secondary_index.rs` with
the description `typed serde/JSON records, sparse and composite terms, all projection modes, paging, and verification`.

- [ ] **Step 4: Validate all relative links**

Run:

```bash
test -f docs/secondary-indexes.md
test -f docs/secondary-index-design.md
test -f examples/secondary_index_json.rs
rg -n "secondary-indexes.md|secondary-index-design.md|secondary_index_json.rs" README.md docs/README.md docs/secondary-index-design.md
```

Expected: all three files exist and every discovery surface contains the appropriate link.

- [ ] **Step 5: Commit navigation updates**

```bash
git add README.md docs/README.md docs/secondary-index-design.md
git commit -m "docs(index): link guide and reference"
```

### Task 4: Final documentation verification

**Files:**
- Verify: `examples/secondary_index_json.rs`
- Verify: `docs/secondary-indexes.md`
- Verify: `docs/secondary-index-design.md`
- Verify: `docs/README.md`
- Verify: `README.md`

**Interfaces:**
- Consumes: all prior tasks.
- Produces: release-gate evidence for the completed documentation set.

- [ ] **Step 1: Run formatting and strict lint**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
```

Expected: both commands exit 0 with no warnings.

- [ ] **Step 2: Run the typed example and focused secondary-index tests**

```bash
cargo run --example secondary_index_json
cargo test --test secondary_index
```

Expected: the example reports seven verified indexes and all secondary-index tests pass.

- [ ] **Step 3: Run all-feature and documentation tests**

```bash
cargo test --all-targets --all-features
cargo test --doc --all-features
```

Expected: all targets, benchmarks, examples, and doctests pass with zero failures.

- [ ] **Step 4: Check prose completeness and repository cleanliness**

```bash
rg -n "TBD|TODO|FIXME|XXX" docs/secondary-indexes.md docs/secondary-index-design.md examples/secondary_index_json.rs
git diff --check
git status --short
```

Expected: the placeholder scan has no matches, `git diff --check` exits 0, and only the pre-existing unrelated untracked paths remain.

- [ ] **Step 5: Commit any verification-only corrections**

If verification required corrections, commit only those files:

```bash
git add README.md docs/README.md docs/secondary-indexes.md docs/secondary-index-design.md examples/secondary_index_json.rs
git commit -m "docs(index): polish secondary index documentation"
```

If no corrections were needed, do not create an empty commit.
