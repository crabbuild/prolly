package sqlite

import (
	"context"
	"database/sql"
	"net/url"
	"testing"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
)

func TestSQLiteConformance(t *testing.T) {
	storetest.Run(t, func(ctx context.Context, t *testing.T) prolly.RemoteStore {
		store, err := Open("file:"+url.PathEscape(t.Name())+"?mode=memory&cache=shared", Options{})
		if err != nil {
			t.Fatal(err)
		}
		if err := store.InitializeSchema(ctx); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = store.Close() })
		return store
	})
}

func TestSQLiteUsesRustPhysicalSchema(t *testing.T) {
	ctx := context.Background()
	db, err := sql.Open("sqlite", "file:rust-schema-fixture?mode=memory&cache=shared")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.ExecContext(ctx, createSchemaSQL); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO prolly_nodes (cid, node) VALUES (?, ?)`, []byte("rust-cid"), []byte("rust-node")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO prolly_hints (namespace, key, value) VALUES (?, ?, ?)`, []byte("rust-ns"), []byte("rust-key"), []byte("rust-hint")); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)`, []byte("rust-root"), []byte("rust-manifest")); err != nil {
		t.Fatal(err)
	}

	store := New(db, Options{})
	node, err := store.GetNode(ctx, []byte("rust-cid"))
	if err != nil || !node.Present || string(node.Value) != "rust-node" {
		t.Fatalf("node = %#v, %v", node, err)
	}
	hint, err := store.GetHint(ctx, []byte("rust-ns"), []byte("rust-key"))
	if err != nil || !hint.Present || string(hint.Value) != "rust-hint" {
		t.Fatalf("hint = %#v, %v", hint, err)
	}
	root, err := store.GetRootManifest(ctx, []byte("rust-root"))
	if err != nil || !root.Present || string(root.Value) != "rust-manifest" {
		t.Fatalf("root = %#v, %v", root, err)
	}
}
