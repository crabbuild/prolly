package postgres

import (
	"context"
	"os"
	"testing"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresConformance(t *testing.T) {
	dsn := os.Getenv("PROLLY_POSTGRES_URL")
	if dsn == "" {
		t.Skip("PROLLY_POSTGRES_URL is not set")
	}
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	store := New(pool, Options{})
	if err := store.InitializeSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE prolly_nodes, prolly_hints, prolly_roots`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO prolly_roots (name, manifest) VALUES ($1, $2)`, []byte("rust-root"), []byte("rust-manifest")); err != nil {
		t.Fatal(err)
	}
	root, err := store.GetRootManifest(context.Background(), []byte("rust-root"))
	if err != nil || !root.Present || string(root.Value) != "rust-manifest" {
		t.Fatalf("Rust-layout root = %#v, %v", root, err)
	}
	if err := store.PutNode(context.Background(), []byte("go-cid"), []byte("go-node")); err != nil {
		t.Fatal(err)
	}
	var rawNode []byte
	if err := pool.QueryRow(context.Background(), `SELECT node FROM prolly_nodes WHERE cid = $1`, []byte("go-cid")).Scan(&rawNode); err != nil || string(rawNode) != "go-node" {
		t.Fatalf("Go-layout node = %q, %v", rawNode, err)
	}
	if _, err := pool.Exec(context.Background(), `TRUNCATE prolly_nodes, prolly_hints, prolly_roots`); err != nil {
		t.Fatal(err)
	}
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
