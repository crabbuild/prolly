package mysql

import (
	"context"
	"database/sql"
	"os"
	"testing"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	_ "github.com/go-sql-driver/mysql"
)

func TestMySQLConformance(t *testing.T) {
	dsn := os.Getenv("PROLLY_MYSQL_DSN")
	if dsn == "" {
		t.Skip("PROLLY_MYSQL_DSN is not set")
	}
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	store := New(db, Options{})
	if err := store.InitializeSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"prolly_nodes", "prolly_hints", "prolly_roots"} {
		if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.ExecContext(context.Background(), `INSERT INTO prolly_roots (name, manifest) VALUES (?, ?)`, []byte("rust-root"), []byte("rust-manifest")); err != nil {
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
	if err := db.QueryRowContext(context.Background(), `SELECT node FROM prolly_nodes WHERE cid = ?`, []byte("go-cid")).Scan(&rawNode); err != nil || string(rawNode) != "go-node" {
		t.Fatalf("Go-layout node = %q, %v", rawNode, err)
	}
	for _, table := range []string{"prolly_nodes", "prolly_hints", "prolly_roots"} {
		if _, err := db.ExecContext(context.Background(), "DELETE FROM "+table); err != nil {
			t.Fatal(err)
		}
	}
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
