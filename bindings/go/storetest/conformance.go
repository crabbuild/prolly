package storetest

import (
	"bytes"
	"context"
	"errors"
	"testing"

	prolly "build.crab/prolly-go"
)

type Factory func(context.Context, *testing.T) prolly.RemoteStore

func Run(t *testing.T, factory Factory) {
	t.Helper()
	run := func(name string, test func(*testing.T, context.Context, prolly.RemoteStore)) {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			test(t, ctx, factory(ctx, t))
		})
	}

	run("descriptor", testDescriptor)
	run("missing-versus-empty", testMissingVersusEmpty)
	run("idempotent-put-delete", testIdempotentPutDelete)
	run("ordered-batch-reads", testOrderedBatchReads)
	run("node-scan-order", testNodeScanOrder)
	run("hints-and-atomic-nodes-hint", testHints)
	run("root-crud-list-order", testRootCRUD)
	run("root-cas", testRootCAS)
	run("transaction-apply-conflict-no-partial", testTransaction)
	run("cancellation", testCancellation)
	run("engine-cid-manifest", testEngine)
}

func RunWithStore(t *testing.T, store prolly.RemoteStore) {
	Run(t, func(context.Context, *testing.T) prolly.RemoteStore { return store })
}

func testDescriptor(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	descriptor, err := store.Descriptor(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := descriptor.Validate(); err != nil {
		t.Fatal(err)
	}
}

func testMissingVersusEmpty(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	missing, err := store.GetNode(ctx, []byte("missing"))
	if err != nil || missing.Present {
		t.Fatalf("missing = %#v, %v", missing, err)
	}
	if err := store.PutNode(ctx, []byte("empty"), []byte{}); err != nil {
		t.Fatal(err)
	}
	empty, err := store.GetNode(ctx, []byte("empty"))
	if err != nil || !empty.Present || len(empty.Value) != 0 {
		t.Fatalf("empty = %#v, %v", empty, err)
	}
}

func testIdempotentPutDelete(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	for range 2 {
		if err := store.PutNode(ctx, []byte("key"), []byte("value")); err != nil {
			t.Fatal(err)
		}
	}
	for range 2 {
		if err := store.DeleteNode(ctx, []byte("key")); err != nil {
			t.Fatal(err)
		}
	}
}

func testOrderedBatchReads(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	if err := store.BatchNodes(ctx, []prolly.NodeMutation{
		prolly.UpsertNode([]byte("b"), []byte("2")),
		prolly.UpsertNode([]byte("a"), []byte("1")),
	}); err != nil {
		t.Fatal(err)
	}
	values, err := store.BatchGetNodesOrdered(ctx, [][]byte{[]byte("b"), []byte("missing"), []byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 4 || !values[0].Present || values[1].Present || !bytes.Equal(values[2].Value, []byte("1")) || !bytes.Equal(values[3].Value, []byte("2")) {
		t.Fatalf("ordered values = %#v", values)
	}
}

func testNodeScanOrder(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	descriptor, _ := store.Descriptor(ctx)
	if !descriptor.Capabilities.NodeScan {
		t.Skip("node scan unsupported")
	}
	aKey := bytes.Repeat([]byte{0x11}, 32)
	zKey := bytes.Repeat([]byte{0xee}, 32)
	_ = store.PutNode(ctx, zKey, []byte("z"))
	_ = store.PutNode(ctx, aKey, []byte("a"))
	keys, err := store.ListNodeCIDs(ctx)
	if err != nil || !strictlySorted(keys) || !containsBytes(keys, aKey) || !containsBytes(keys, zKey) {
		t.Fatalf("keys = %q, %v", keys, err)
	}
}

func strictlySorted(values [][]byte) bool {
	for index := 1; index < len(values); index++ {
		if bytes.Compare(values[index-1], values[index]) >= 0 {
			return false
		}
	}
	return true
}

func containsBytes(values [][]byte, expected []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, expected) {
			return true
		}
	}
	return false
}

func testHints(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	descriptor, _ := store.Descriptor(ctx)
	if !descriptor.Capabilities.Hints {
		t.Skip("hints unsupported")
	}
	if err := store.BatchPutNodesWithHint(ctx, []prolly.NodeEntry{{Key: []byte("node"), Value: []byte("bytes")}}, []byte("ns"), []byte("key"), []byte("hint")); err != nil {
		t.Fatal(err)
	}
	node, nodeErr := store.GetNode(ctx, []byte("node"))
	hint, hintErr := store.GetHint(ctx, []byte("ns"), []byte("key"))
	if nodeErr != nil || hintErr != nil || !node.Present || !hint.Present {
		t.Fatalf("node/hint = %#v/%#v, %v/%v", node, hint, nodeErr, hintErr)
	}
}

func testRootCRUD(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	_ = store.PutRootManifest(ctx, []byte("z"), []byte("2"))
	_ = store.PutRootManifest(ctx, []byte("a"), []byte("1"))
	root, err := store.GetRootManifest(ctx, []byte("a"))
	if err != nil || !root.Present || !bytes.Equal(root.Value, []byte("1")) {
		t.Fatalf("root = %#v, %v", root, err)
	}
	descriptor, _ := store.Descriptor(ctx)
	if descriptor.Capabilities.RootScan {
		roots, err := store.ListRootManifests(ctx)
		if err != nil || len(roots) != 2 || bytes.Compare(roots[0].Name, roots[1].Name) >= 0 {
			t.Fatalf("roots = %#v, %v", roots, err)
		}
	}
	if err := store.DeleteRootManifest(ctx, []byte("a")); err != nil {
		t.Fatal(err)
	}
}

func testRootCAS(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	descriptor, _ := store.Descriptor(ctx)
	if !descriptor.Capabilities.RootCompareAndSwap {
		t.Skip("root CAS unsupported")
	}
	created, err := store.CompareAndSwapRootManifest(ctx, []byte("root"), prolly.MissingBytes(), prolly.PresentBytes([]byte("one")))
	if err != nil || !created.Applied {
		t.Fatalf("create = %#v, %v", created, err)
	}
	conflict, err := store.CompareAndSwapRootManifest(ctx, []byte("root"), prolly.MissingBytes(), prolly.PresentBytes([]byte("two")))
	if err != nil || conflict.Applied || !conflict.Current.Present {
		t.Fatalf("conflict = %#v, %v", conflict, err)
	}
	deleted, err := store.CompareAndSwapRootManifest(ctx, []byte("root"), prolly.PresentBytes([]byte("one")), prolly.MissingBytes())
	if err != nil || !deleted.Applied {
		t.Fatalf("delete = %#v, %v", deleted, err)
	}
}

func testTransaction(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	descriptor, _ := store.Descriptor(ctx)
	if !descriptor.Capabilities.Transactions {
		t.Skip("transactions unsupported")
	}
	result, err := store.CommitTransaction(ctx,
		[]prolly.NodeMutation{prolly.UpsertNode([]byte("node"), []byte("one"))},
		[]prolly.RootCondition{{Name: []byte("root"), Expected: prolly.MissingBytes()}},
		[]prolly.RootWrite{{Name: []byte("root"), Replacement: prolly.PresentBytes([]byte("manifest"))}},
	)
	if err != nil || !result.Applied {
		t.Fatalf("apply = %#v, %v", result, err)
	}
	conflict, err := store.CommitTransaction(ctx,
		[]prolly.NodeMutation{prolly.UpsertNode([]byte("partial"), []byte("bad"))},
		[]prolly.RootCondition{{Name: []byte("root"), Expected: prolly.MissingBytes()}}, nil)
	if err != nil || conflict.Applied || conflict.Conflict == nil {
		t.Fatalf("conflict = %#v, %v", conflict, err)
	}
	partial, _ := store.GetNode(ctx, []byte("partial"))
	if partial.Present {
		t.Fatal("conflicting transaction partially wrote nodes")
	}
}

func testCancellation(t *testing.T, _ context.Context, store prolly.RemoteStore) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.GetNode(ctx, []byte("key"))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel error = %v", err)
	}
}

func testEngine(t *testing.T, ctx context.Context, store prolly.RemoteStore) {
	engine, err := prolly.NewAsyncEngine(ctx, store, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = engine.Close() })
	tree, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = engine.Put(ctx, tree, []byte("key"), []byte("value"))
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := engine.Get(ctx, tree, []byte("key"))
	if err != nil || !found || !bytes.Equal(value, []byte("value")) {
		t.Fatalf("engine Get = %q, %v, %v", value, found, err)
	}
}
