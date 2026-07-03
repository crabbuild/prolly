# Prolly Go Cookbook

The Go binding is a cgo wrapper over the Rust UniFFI facade. Build the Rust
library before running Go programs locally.

```sh
cargo build -p prolly-bindings
cd crates/prolly/bindings/go
go test ./...
```

Runnable scenarios live as separate `go run` targets under `examples/`,
matching the Rust example style:

```sh
cargo build -p prolly-bindings
(cd crates/prolly/bindings/go && go run ./examples/cookbook_scenarios)
(cd crates/prolly/bindings/go && go run ./examples/basic_map)
(cd crates/prolly/bindings/go && go run ./examples/secondary_index)
```

Application-style directories include `batch_build`, `local_first_state`,
`resolver`, `crdt_merge`, `conversation_memory`, `agent_event_log`,
`background_compaction`, `deterministic_rag_snapshot`,
`document_chunk_index`, `vector_sidecar`, `provenance_values`,
`materialized_view`, `filesystem_snapshot`, and `durable_sqlite`.

## Create A Durable Index

```go
package main

import (
	"log"

	prolly "build.crab/prolly-go"
)

func main() {
	engine, err := prolly.OpenSQLite("app.prolly.db")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	tree, err := engine.Create()
	if err != nil {
		log.Fatal(err)
	}

	tree, err = engine.Batch(tree, []prolly.Mutation{
		{Kind: "upsert", Key: []byte("user/1"), Value: []byte("Ada")},
		{Kind: "upsert", Key: []byte("user/2"), Value: []byte("Linus")},
	})
	if err != nil {
		log.Fatal(err)
	}

	if err := engine.PublishNamedRoot([]byte("users/main"), tree); err != nil {
		log.Fatal(err)
	}
}
```

## Prefix Queries And Pages

```go
start := []byte("user/")
end, ok, err := prolly.PrefixEnd(start)
if err != nil {
	log.Fatal(err)
}
if !ok {
	end = nil
}

entries, err := engine.Range(tree, start, end)
if err != nil {
	log.Fatal(err)
}
for _, entry := range entries {
	log.Printf("%s=%s", entry.Key, entry.Value)
}

var cursor *prolly.RangeCursor
for {
	page, err := engine.RangePage(tree, cursor, nil, 100)
	if err != nil {
		log.Fatal(err)
	}
	for _, entry := range page.Entries {
		handle(entry.Key, entry.Value)
	}
	if page.NextCursor == nil {
		break
	}
	cursor = page.NextCursor
}

diffs, err := engine.DiffFromCursor(oldTree, newTree, &prolly.RangeCursor{AfterKey: []byte("user/42")}, end)
if err != nil {
	log.Fatal(err)
}
_ = diffs
```

## Use Context-Aware Calls

Context methods keep application code cancellable while the Rust core stays
synchronous.

```go
ctx, cancel := context.WithTimeout(context.Background(), time.Second)
defer cancel()

value, ok, err := engine.GetContext(ctx, tree, []byte("user/1"))
if err != nil {
	log.Fatal(err)
}
if ok {
	log.Printf("user/1=%s", value)
}
```

## Merge Writers

```go
base := tree
left, _ := engine.Put(base, []byte("user/1"), []byte("Ada Lovelace"))
right, _ := engine.Put(base, []byte("user/1"), []byte("Countess Ada"))

merged, err := engine.Merge(base, left, right, "prefer_right")
if err != nil {
	log.Fatal(err)
}
_ = merged

resolver := func(conflict prolly.Conflict) prolly.Resolution {
	if conflict.Left != nil && conflict.Right != nil {
		value := append(append([]byte{}, conflict.Left...), []byte(" | ")...)
		value = append(value, conflict.Right...)
		return prolly.ResolveValue(value)
	}
	return prolly.ResolveUnresolved()
}

merged, err = engine.MergeWithResolver(base, left, right, resolver)
```

## Large Values And Blob GC

```go
blobStore, err := prolly.MemoryBlobStore()
if err != nil {
	log.Fatal(err)
}
defer blobStore.Close()

large := bytes.Repeat([]byte("x"), 1_000_000)
tree, err = engine.PutLargeValue(blobStore, tree, []byte("doc/1"), large, prolly.LargeValueConfig{
	InlineThreshold: 4096,
})
if err != nil {
	log.Fatal(err)
}

loaded, ok, err := engine.GetLargeValue(blobStore, tree, []byte("doc/1"))
if err != nil || !ok {
	log.Fatal("missing large value")
}
_ = loaded

plan, err := engine.PlanBlobStoreGC(blobStore, []prolly.Tree{tree})
if err != nil {
	log.Fatal(err)
}
if plan.ReclaimableBlobCount > 0 {
	_, _ = engine.SweepBlobStoreGC(blobStore, []prolly.Tree{tree})
}
```

## Custom Stores

Implement `HostStore` when Go owns persistence and Rust owns the tree engine.
The full interface includes node bytes, ordered reads, hints, named roots, CAS,
root/manifest listing, and node scans. See `prolly_test.go` for a complete in-memory
implementation.

```go
store := NewMemoryHostStore()
config, err := prolly.DefaultConfig()
if err != nil {
	log.Fatal(err)
}
engine, err := prolly.CustomStore(store, config)
if err != nil {
	log.Fatal(err)
}
defer engine.Close()
```
