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
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
