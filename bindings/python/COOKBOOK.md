# Prolly Python Cookbook

These examples use the Rust-backed UniFFI package exposed as `prolly`. Build the
native library first or install the package with `maturin develop`.

```sh
cargo build -p prolly-bindings
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python3 app.py
```

Runnable scenarios live as separate files under `examples/`, matching the Rust
example style:

```sh
cargo build -p prolly-bindings
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python3 crates/prolly/bindings/python/examples/cookbook_scenarios.py
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python3 crates/prolly/bindings/python/examples/basic_map.py
PROLLY_BINDINGS_LIBRARY="$PWD/target/debug/libprolly_bindings.dylib" \
  PYTHONPATH=crates/prolly/bindings/python \
  python3 crates/prolly/bindings/python/examples/secondary_index.py
```

Application-style files include `batch_build.py`, `local_first_state.py`,
`resolver.py`, `crdt_merge.py`, `conversation_memory.py`,
`agent_event_log.py`, `background_compaction.py`,
`deterministic_rag_snapshot.py`, `document_chunk_index.py`,
`vector_sidecar.py`, `provenance_values.py`, `materialized_view.py`,
`filesystem_snapshot.py`, and `durable_sqlite.py`.

## Create A Durable Index

Use SQLite for an application-local durable store. Keep immutable snapshots as
`TreeRecord` values and publish named roots for app-level pointers.

```python
import prolly

engine = prolly.ProllyEngine.sqlite("app.prolly.db", prolly.default_config())
tree = engine.create()

tree = engine.batch(
    tree,
    [
        prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b"user/1", value=b"Ada"),
        prolly.MutationRecord(kind=prolly.MutationKind.UPSERT, key=b"user/2", value=b"Linus"),
    ],
)

engine.publish_named_root(b"users/main", tree)
loaded = engine.load_named_root(b"users/main")
assert loaded == tree
```

## Query By Prefix

Keys are byte-lexicographic. Use `prefix_end` to build prefix ranges.

```python
start = b"user/"
end = prolly.prefix_end(start)
rows = engine.range(tree, start, end)

for entry in rows:
    print(entry.key.decode(), entry.value.decode())
```

## Page Through Results

Use pages for UI lists or background jobs that must resume.

```python
cursor = None
while True:
    page = engine.range_page(tree, cursor, None, 100)
    for entry in page.entries:
        handle(entry.key, entry.value)
    if page.next_cursor is None:
        break
    cursor = page.next_cursor

diffs = engine.diff_from_cursor(
    old_tree,
    new_tree,
    prolly.RangeCursorRecord(after_key=b"user/42"),
    None,
)
```

## Merge Two Writers

Applications can merge immutable snapshots with built-in policies or Python
callbacks.

```python
base = tree
left = engine.put(base, b"user/1", b"Ada Lovelace")
right = engine.put(base, b"user/1", b"Countess Ada")

merged = engine.merge(base, left, right, "prefer_right")

class JoinResolver(prolly.MergeResolverCallback):
    def resolve(self, conflict):
        if conflict.left is not None and conflict.right is not None:
            return prolly.ResolutionRecord(
                kind=prolly.ResolutionKind.VALUE,
                value=conflict.left + b" | " + conflict.right,
            )
        return prolly.ResolutionRecord(kind=prolly.ResolutionKind.UNRESOLVED, value=None)

merged_with_callback = engine.merge_with_resolver(base, left, right, JoinResolver())
```

## Store Large Values In A Blob Store

Use value refs when documents can be larger than normal inline values.

```python
blob_store = prolly.ProllyBlobStore.file("app.blobs")
large = b"x" * 1_000_000

tree = engine.put_large_value(
    blob_store,
    tree,
    b"doc/1",
    large,
    prolly.LargeValueConfigRecord(inline_threshold=4096),
)

ref = engine.get_value_ref(tree, b"doc/1")
assert ref.kind == prolly.ValueRefKind.BLOB
assert engine.get_large_value(blob_store, tree, b"doc/1") == large
```

## Sync Missing Nodes To Another Store

Use missing-node planning when sending a snapshot to another store.

```python
destination = prolly.ProllyEngine.sqlite("replica.prolly.db", prolly.default_config())
plan = engine.plan_missing_nodes(tree, destination)
if plan.missing_nodes:
    copied = engine.copy_missing_nodes(tree, destination)
    assert copied.copied_nodes == plan.missing_nodes
```

## Implement A Custom Store

Custom stores let Python own persistence while Rust owns tree logic.

```python
class MemoryHostStore(prolly.HostStoreCallback):
    def __init__(self):
        self.nodes = {}
        self.roots = {}
        self.hints = {}

    def get(self, key):
        return prolly.HostStoreBytesResultRecord(value=self.nodes.get(bytes(key)), error=None)

    def put(self, key, value):
        self.nodes[bytes(key)] = bytes(value)
        return prolly.HostStoreUnitResultRecord(error=None)

    def delete(self, key):
        self.nodes.pop(bytes(key), None)
        return prolly.HostStoreUnitResultRecord(error=None)

    def batch(self, ops):
        for op in ops:
            if op.kind == prolly.MutationKind.UPSERT:
                self.nodes[bytes(op.key)] = bytes(op.value)
            else:
                self.nodes.pop(bytes(op.key), None)
        return prolly.HostStoreUnitResultRecord(error=None)

    def batch_get_ordered(self, keys):
        return prolly.HostStoreBatchGetResultRecord(values=[self.nodes.get(bytes(k)) for k in keys], error=None)

    def prefers_batch_reads(self):
        return prolly.HostStoreBoolResultRecord(value=True, error=None)

    def supports_hints(self):
        return prolly.HostStoreBoolResultRecord(value=False, error=None)

    def get_hint(self, namespace, key):
        return prolly.HostStoreBytesResultRecord(value=None, error=None)

    def put_hint(self, namespace, key, value):
        return prolly.HostStoreUnitResultRecord(error=None)

    def list_node_cids(self):
        return prolly.HostStoreListBytesResultRecord(values=list(self.nodes.keys()), error=None)

    def get_root(self, name):
        return prolly.HostStoreRootResultRecord(value=self.roots.get(bytes(name)), error=None)

    def put_root(self, name, manifest):
        self.roots[bytes(name)] = manifest
        return prolly.HostStoreUnitResultRecord(error=None)

    def delete_root(self, name):
        self.roots.pop(bytes(name), None)
        return prolly.HostStoreUnitResultRecord(error=None)

    def compare_and_swap_root(self, name, expected, replacement):
        current = self.roots.get(bytes(name))
        if current == expected:
            if replacement is None:
                self.roots.pop(bytes(name), None)
            else:
                self.roots[bytes(name)] = replacement
            return prolly.HostStoreRootCasResultRecord(applied=True, current=None, error=None)
        return prolly.HostStoreRootCasResultRecord(applied=False, current=current, error=None)

    def list_roots(self):
        return prolly.HostStoreListRootsResultRecord(
            values=[
                prolly.HostStoreNamedRootManifestRecord(name=name, manifest=manifest)
                for name, manifest in self.roots.items()
            ],
            error=None,
        )

engine = prolly.ProllyEngine.custom_store(MemoryHostStore(), prolly.default_config())
```
