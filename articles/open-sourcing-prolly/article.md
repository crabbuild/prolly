---
title: Open Sourcing Prolly: Versioned App State Across Local and Remote Stores
---

# Open Sourcing Prolly: Versioned App State Across Local and Remote Stores

We are open sourcing **Prolly**, a Rust engine for versioned, verifiable application state.

Prolly is not trying to be a full database. It is a storage primitive: an immutable, ordered key-value tree with content-addressed nodes, cheap snapshots, diff, merge, sync, proofs, and pluggable local or remote stores.

In short:

```text
Prolly =
  ordered map
  + immutable snapshots
  + content IDs
  + diff / merge
  + proof bundles
  + local and remote stores
```

```text
              OPEN SOURCING PROLLY

       Versioned app state that can live anywhere

  +------------------+  +------------------+
  | Immutable roots  |  | Diff and merge   |
  | branch/checkpoint|  | skip equal CIDs  |
  +------------------+  +------------------+

  +------------------+  +------------------+
  | Proof bundles    |  | Pluggable stores |
  | verify remotely  |  | local + remote   |
  +------------------+  +------------------+

      ordered map + content-addressed nodes
         + portable storage semantics
```

## Why build another state engine?

Modern applications increasingly need state that can move.

Not just data that sits in one database, but state that can:

- work offline and sync later;
- branch into multiple versions;
- merge concurrent edits;
- produce reproducible AI or RAG snapshots;
- prove what changed without trusting a remote backend;
- live in memory, SQLite, files, object storage, cloud databases, or custom services.

That combination is awkward to build directly on top of a normal mutable key-value store.

Prolly exists for the layer above raw bytes and below the application model.

## The core idea

A Prolly tree is an immutable ordered map over byte keys and byte values.

Every update returns a new tree handle. Unchanged subtrees keep the same content ID, so versions share storage naturally.

```text
Before put(k, v)          After put(k, v)

        Root A                    Root B
       /  |  \                   /  |  \
     C1  C2  C3                C1  X2  C3

Only the changed path is rewritten.
Unchanged nodes keep their CIDs.
```

That gives applications a useful property: every root is a stable snapshot.

You can keep a root for a user workspace, an agent run, a RAG index, a checkpoint, a branch, or a published view.

## Architecture

The engine is intentionally split from the store.

Prolly owns tree semantics:

- ordering;
- content-defined chunking;
- node encoding;
- immutable updates;
- diff and merge;
- named roots;
- garbage collection;
- proof generation and verification.

The store owns bytes:

- `get`;
- `put`;
- `delete`;
- `batch`.

```text
  Application layer             Prolly engine                 Store backends

  +-------------+        +----------------------+        +------------------+
  | App state   | -----> | Tree manager         | -----> | Local            |
  | keys/values |        | put/delete/batch     |        | memory, file    |
  +-------------+        +----------------------+        +------------------+

  +-------------+        +----------------------+        +------------------+
  | Workflows   | -----> | Named roots          | -----> | Embedded         |
  | sync/branch |        | branches/views/CAS   |        | SQLite, RocksDB |
  +-------------+        +----------------------+        +------------------+

  +-------------+        +----------------------+        +------------------+
  | Trust edge  | -----> | Proofs               | -----> | Remote           |
  | verify data |        | key/range/page/diff  |        | Redis, DB, API  |
  +-------------+        +----------------------+        +------------------+

                         +----------------------+
                         | GC and sync          |
                         | copy missing CIDs    |
                         +----------------------+
```

This separation matters because the same tree logic can run over local and remote stores.

## Local and remote by design

Prolly nodes are content-addressed bytes. A node key is its CID. A node value is deterministic serialized node bytes.

That means nodes do not have to live in one local embedded database.

They can live in:

- memory for tests and temporary state;
- files for local durable object-layout storage;
- SQLite or RocksDB for embedded apps;
- SlateDB or object-store-backed systems;
- Redis, Postgres, MySQL, DynamoDB, Cosmos DB, Spanner, or another remote service through store adapters;
- browser or background-agent storage through the async store API.

The application keeps the same root, diff, merge, proof, and sync model while changing the storage backend underneath.

```text
                 +----------------+
Application ---> | Prolly Engine  |
                 +----------------+
                          |
                          v
              content-addressed nodes
                          |
        +-----------------+-----------------+
        |                 |                 |
        v                 v                 v
   Local Store       Embedded DB       Remote Store
   Memory/File         SQLite       Object/DB/Service
```

## A tiny concrete example

Here is a minimal in-memory tree.

```rust
use prolly::{Config, MemStore, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());

let root_0 = prolly.create();
let root_1 = prolly
    .put(&root_0, b"user/1/name".to_vec(), b"Ada".to_vec())
    .unwrap();

assert!(prolly.get(&root_0, b"user/1/name").unwrap().is_none());
assert_eq!(
    prolly.get(&root_1, b"user/1/name").unwrap(),
    Some(b"Ada".to_vec())
);
```

`root_0` is still valid. `root_1` is a new snapshot.

That small property becomes powerful once the application has branches, sync, or background indexing.

## Sync by copying missing nodes

Because nodes are content-addressed, sync can be planned as a missing-node transfer.

```rust
let plan = source
    .plan_missing_nodes(&tree, &destination_store)
    .unwrap();

println!(
    "send {} nodes / {} bytes",
    plan.missing_nodes,
    plan.missing_bytes
);

source
    .copy_missing_nodes(&tree, &destination_store)
    .unwrap();
```

The destination can be local or remote. Prolly verifies node bytes against their CIDs instead of blindly trusting returned data.

## What can you build on it?

```text
                         +-------------+
                         | Prolly Tree |
                         +-------------+
                               |
       +-----------------------+-----------------------+
       |                       |                       |
       v                       v                       v
+--------------+       +---------------+       +----------------+
| Local-first  |       | AI memory/RAG |       | Versioned views |
| app state    |       | snapshots     |       | and indexes     |
+--------------+       +---------------+       +----------------+
       |                       |                       |
       +-----------------------+-----------------------+
                               |
       +-----------------------+-----------------------+
       |                                               |
       v                                               v
+----------------+                              +----------------+
| Verifiable     |                              | File/object    |
| data exchange  |                              | manifests      |
+----------------+                              +----------------+
```

### Mental model: tree roots and named roots

Before the examples, it helps to separate two ideas.

A **tree root** is an immutable snapshot of an ordered key-value map.

A **named root** is an application-controlled pointer to one of those snapshots.

```text
Data model:

  Tree root R1
    doc/17/title -> "Offline notes"
    doc/17/body  -> "Drafted on the train"

Update model:

  put(R1, "doc/17/body", "...edited...") => Tree root R2

Named root model:

  workspace/t1/head           -> R1
  workspace/t1/draft/device-a -> R2
```

The tree root is content-addressed state. The named root is workflow state.

That distinction is useful:

- app data lives under ordered byte keys;
- snapshots are immutable and cheap to keep;
- named roots act like branch heads, checkpoints, indexes, views, or run records;
- `compare_and_swap_named_root` lets writers move a name only when it still points at the expected snapshot.

```rust
let head_name = b"workspace/t1/head";

let tree_v1 = prolly
    .put(&prolly.create(), b"doc/17/title".to_vec(), b"Offline notes".to_vec())
    .unwrap();

let update = prolly
    .compare_and_swap_named_root(head_name, None, Some(&tree_v1))
    .unwrap();
assert!(update.is_applied());

let current = prolly.load_named_root(head_name).unwrap().unwrap();
let tree_v2 = prolly
    .put(&current, b"doc/17/body".to_vec(), b"Drafted offline".to_vec())
    .unwrap();

let update = prolly
    .compare_and_swap_named_root(head_name, Some(&current), Some(&tree_v2))
    .unwrap();
assert!(update.is_applied());
```

### 1. Local-first app state

Use Prolly as the durable state layer for apps that need offline writes and later sync.

Examples:

- notes and documents;
- project management tools;
- personal knowledge bases;
- collaborative editors;
- mobile or edge apps with intermittent network access.

Each device can keep local roots, branch work in progress, then merge or publish named roots when it reconnects.

The key idea is that app names point at immutable tree roots:

```text
workspace/t1/head
  published root that other devices sync against

workspace/t1/base/device-a
  the root device-a started editing from

workspace/t1/draft/device-a
  device-a's offline draft root
```

While offline, the device does not mutate `workspace/t1/head`. It loads the
last known head, creates a new draft tree, and saves that draft under a
device-specific name.

```rust
let head_name = b"workspace/t1/head";
let base_name = b"workspace/t1/base/device-a";
let draft_name = b"workspace/t1/draft/device-a";

let last_known_head = prolly.load_named_root(head_name).unwrap();
let base = last_known_head.clone().unwrap_or_else(|| prolly.create());

// Keep the base root so reconnect can merge against the exact snapshot
// this device edited from.
prolly.publish_named_root(base_name, &base).unwrap();

let draft = prolly
    .put(&base, b"doc/17/title".to_vec(), b"Offline notes".to_vec())
    .unwrap();

let draft = prolly
    .put(
        &draft,
        b"doc/17/body".to_vec(),
        b"Drafted on the train".to_vec(),
    )
    .unwrap();

// This is a local draft pointer. The published head has not moved yet.
prolly.publish_named_root(draft_name, &draft).unwrap();
assert_eq!(prolly.load_named_root(head_name).unwrap(), last_known_head);
```

When the device reconnects, it loads the latest published head. If no one else
moved the head, the draft can become the new head directly. If another device
published first, Prolly can merge `base`, `latest`, and `draft`.

```rust
let base = prolly.load_named_root(base_name).unwrap().unwrap();
let draft = prolly.load_named_root(draft_name).unwrap().unwrap();
let latest_head = prolly.load_named_root(head_name).unwrap();
let latest = latest_head.clone().unwrap_or_else(|| prolly.create());

let next_head = if latest == base {
    draft
} else {
    prolly.merge(&base, &latest, &draft, None).unwrap()
};

let update = prolly
    .compare_and_swap_named_root(head_name, latest_head.as_ref(), Some(&next_head))
    .unwrap();

assert!(update.is_applied());
```

### 2. AI memory and RAG snapshots

AI systems need reproducibility.

An answer is not just text. It usually depends on:

- conversation memory;
- document chunks;
- embedding metadata;
- parser versions;
- model versions;
- retrieval filters;
- provenance.

Prolly can store the ordered metadata and root pointers that explain exactly what an agent or RAG pipeline saw.

The mental model is:

```text
corpus/current
  points to the latest searchable metadata root

run/r456/index
  points to the exact index root used for one answer

run/r456/answer
  points to the answer record for that run
```

The vector bytes can stay in a sidecar vector engine. Prolly keeps the metadata, chunk records, provenance, and reproducible roots.

Example: publish a RAG corpus root.

```rust
let corpus_head = b"tenant/t1/rag/corpus/current";

let index = prolly.create();
let index = prolly
    .put(
        &index,
        b"chunk/doc-17/000012".to_vec(),
        br#"{"source":"s3://docs/design.pdf","parser":"pdf-v3","embedding_id":"vec_9f31"}"#.to_vec(),
    )
    .unwrap();

let index = prolly
    .put(
        &index,
        b"memory/conversation-c42/message/00000007".to_vec(),
        br#"{"role":"user","text":"summarize the design"}"#.to_vec(),
    )
    .unwrap();

let update = prolly
    .compare_and_swap_named_root(corpus_head, None, Some(&index))
    .unwrap();
assert!(update.is_applied());
```

Example: record the exact snapshot used for one answer.

```rust
let run_index = b"tenant/t1/rag/run/r456/index";
let run_answer = b"tenant/t1/rag/run/r456/answer";

let index_snapshot = prolly.load_named_root(corpus_head).unwrap().unwrap();
prolly.publish_named_root(run_index, &index_snapshot).unwrap();

let answer = prolly
    .put(
        &prolly.create(),
        b"answer/text".to_vec(),
        b"Use named roots to pin the exact index used by the answer.".to_vec(),
    )
    .unwrap();

prolly.publish_named_root(run_answer, &answer).unwrap();
```

If the corpus is re-parsed later, `corpus/current` can move to a new root.

The old answer still points at the old index.

```rust
let replay_index = prolly.load_named_root(run_index).unwrap().unwrap();
let replay_answer = prolly.load_named_root(run_answer).unwrap().unwrap();

assert_eq!(replay_index, index_snapshot);
assert!(prolly.get(&replay_answer, b"answer/text").unwrap().is_some());
```

### 3. Versioned indexes and materialized views

Many applications maintain secondary indexes or derived views.

The source data and the derived view can be separate tree roots:

```text
source/current:
  ticket/123/status = open
  ticket/123/assignee = ada

view/by-status/current:
  by_status/open/123 = ticket/123
  by_assignee/ada/123 = ticket/123
```

Named roots make the relationship explicit.

```text
tickets/source/current         -> Source root S2
tickets/view/by-status/current -> View root V2
tickets/view/manifest/current  -> Manifest root M2
```

That is useful for search indexes, dashboards, catalogs, workspace views, and event-derived state.

Example: publish a source tree and a secondary index.

```rust
let source_v1 = prolly
    .put(
        &prolly.create(),
        b"ticket/123/status".to_vec(),
        b"open".to_vec(),
    )
    .unwrap();

let by_status_v1 = prolly
    .put(
        &prolly.create(),
        b"by_status/open/123".to_vec(),
        b"ticket/123".to_vec(),
    )
    .unwrap();

prolly
    .publish_named_root(b"tickets/source/current", &source_v1)
    .unwrap();
prolly
    .publish_named_root(b"tickets/view/by-status/current", &by_status_v1)
    .unwrap();
```

Example: update the view when the source changes.

```rust
let source_v2 = prolly
    .put(&source_v1, b"ticket/123/status".to_vec(), b"closed".to_vec())
    .unwrap();

let changes = prolly.diff(&source_v1, &source_v2).unwrap();
assert_eq!(changes.len(), 1);

let by_status_v2 = prolly
    .delete(&by_status_v1, b"by_status/open/123")
    .unwrap();

let by_status_v2 = prolly
    .put(
        &by_status_v2,
        b"by_status/closed/123".to_vec(),
        b"ticket/123".to_vec(),
    )
    .unwrap();

prolly
    .publish_named_root(b"tickets/source/current", &source_v2)
    .unwrap();
prolly
    .publish_named_root(b"tickets/view/by-status/current", &by_status_v2)
    .unwrap();
```

For larger systems, the manifest root records which source root produced which view root.

```rust
let source_root_id = format!("{:?}", source_v2.root).into_bytes();
let view_root_id = format!("{:?}", by_status_v2.root).into_bytes();

let manifest_v2 = prolly
    .put(
        &prolly.create(),
        b"view/by-status/source-root".to_vec(),
        source_root_id,
    )
    .unwrap();

let manifest_v2 = prolly
    .put(
        &manifest_v2,
        b"view/by-status/view-root".to_vec(),
        view_root_id,
    )
    .unwrap();

prolly
    .publish_named_root(b"tickets/view/manifest/current", &manifest_v2)
    .unwrap();
```

### 4. Verifiable sync and data exchange

Prolly can generate key, range, cursor-page, and diff-page proofs.

A peer can verify a proof against a root CID without opening the original store.

```text
sender:
  root CID + proof bundle + selected values

receiver:
  verify node hashes
  verify child links
  verify inclusion or absence
```

Proof bundles are useful when state crosses a trust boundary.

Examples:

- an edge cache proves it returned the correct record;
- a tenant export proves a key was included or absent;
- a sync service proves the next diff page;
- an audit system keeps proof bytes with the event.

Example: prove and verify one key.

```rust
use prolly::verify_proof_bundle;

let tree = prolly
    .put(
        &prolly.create(),
        b"tenant/t1/doc/17".to_vec(),
        b"release notes".to_vec(),
    )
    .unwrap();

let proof = prolly.prove_key(&tree, b"tenant/t1/doc/17").unwrap();
let bundle = proof.to_bundle_bytes().unwrap();

let verified = verify_proof_bundle(&bundle).unwrap();
assert!(verified.valid);
assert_eq!(verified.kind_name(), "key");
assert_eq!(verified.exists_count, 1);
```

Example: prove a bounded page or a diff page.

```rust
use prolly::{verify_proof_bundle, RangeCursor};

let page = prolly
    .prove_range_page(&tree, &RangeCursor::start(), None, 10)
    .unwrap();
let page_bundle = page.proof.to_bundle_bytes().unwrap();
let page_verified = verify_proof_bundle(&page_bundle).unwrap();
assert!(page_verified.valid);

let updated = prolly
    .put(&tree, b"tenant/t1/doc/18".to_vec(), b"new note".to_vec())
    .unwrap();

let diff_page = prolly
    .prove_diff_page(&tree, &updated, &RangeCursor::start(), None, 10)
    .unwrap();
let diff_bundle = diff_page.proof.to_bundle_bytes().unwrap();
let diff_verified = verify_proof_bundle(&diff_bundle).unwrap();
assert!(diff_verified.valid);
```

Proof bundles can also be wrapped in HMAC-SHA256 envelopes when peers need tamper detection, key IDs, nonces, context, or expiration times.

### 5. File, object, and repository manifests

Prolly can represent Git-like snapshots without forcing every application into Git's object model.

Examples:

- dataset manifests;
- build artifact indexes;
- backup catalogs;
- file metadata snapshots;
- package registries;
- object-store repository layers.

Large values can live in a blob store. The tree stores references and metadata.

```text
refs/heads/main -> Manifest root M2

Manifest root M2:
  path/src/lib.rs     -> blob reference
  path/Cargo.toml     -> blob reference
  meta/src/lib.rs/mode -> 100644
```

Example: store a large file body in a blob store and keep the tree entry as a reference.

```rust
use prolly::{FileBlobStore, LargeValueConfig};

let blobs = FileBlobStore::open("./target/prolly-blobs").unwrap();
let policy = LargeValueConfig::new(1024);

let source_bytes = b"pub fn answer() -> u8 { 42 }\n".to_vec();

let snapshot = prolly.create();
let snapshot = prolly
    .put_large_value(
        &blobs,
        &snapshot,
        b"path/src/lib.rs".to_vec(),
        source_bytes.clone(),
        policy.clone(),
    )
    .unwrap();

let snapshot = prolly
    .put(
        &snapshot,
        b"meta/src/lib.rs/mode".to_vec(),
        b"100644".to_vec(),
    )
    .unwrap();

let update = prolly
    .compare_and_swap_named_root(b"refs/heads/main", None, Some(&snapshot))
    .unwrap();
assert!(update.is_applied());
```

Example: publish a new manifest root after one file changes.

```rust
let current = prolly
    .load_named_root(b"refs/heads/main")
    .unwrap()
    .expect("branch head exists");

let updated_bytes = b"pub fn answer() -> u8 { 43 }\n".to_vec();
let next_snapshot = prolly
    .put_large_value(
        &blobs,
        &current,
        b"path/src/lib.rs".to_vec(),
        updated_bytes.clone(),
        policy,
    )
    .unwrap();

let update = prolly
    .compare_and_swap_named_root(b"refs/heads/main", Some(&current), Some(&next_snapshot))
    .unwrap();
assert!(update.is_applied());

let current = prolly.load_named_root(b"refs/heads/main").unwrap().unwrap();
assert_eq!(
    prolly
        .get_large_value(&blobs, &current, b"path/src/lib.rs")
        .unwrap(),
    Some(updated_bytes)
);
```

## Why proofs matter

Remote stores are useful, but applications should not have to trust every remote byte blindly.

Prolly's proof APIs give the receiver a smaller verification surface:

```text
Root CID
   |
   v
Proof bundle
   |
   v
Verify:
  - every node hash
  - every child link
  - key inclusion or absence
  - range/page/diff boundaries
```

This is especially important when state crosses a process, machine, account, tenant, or trust boundary.

## What is in the open source repo?

The repository includes:

- the Rust `prolly-map` crate;
- memory, file, SQLite, RocksDB, PGlite, and SlateDB paths;
- remote store adapter crates for systems such as Redis, Postgres, MySQL, DynamoDB, Cosmos DB, and Spanner;
- sync and GC primitives;
- named roots and snapshot namespaces;
- key, range, page, diff, and authenticated proof APIs;
- examples for local-first state, AI memory, RAG snapshots, materialized views, vector sidecars, provenance values, filesystem snapshots, and blob storage;
- language binding work for Python, Node/TypeScript, Go, Java/Kotlin, Swift, Ruby, and WASM.

## Try it

```toml
[dependencies]
prolly-map = { git = "https://github.com/crabbuild/prolly" }
```

```rust
use prolly::{Config, MemStore, Prolly};

let prolly = Prolly::new(MemStore::new(), Config::default());
let tree = prolly.create();
```

Repository:

[https://github.com/crabbuild/prolly](https://github.com/crabbuild/prolly)

## Closing

The goal of Prolly is simple:

Give application builders a small, composable engine for state that can be versioned, synced, merged, verified, and stored wherever the application needs it.

Local-first apps need that.

AI-native systems need that.

Remote and edge applications need that.

That is why we are open sourcing Prolly.
