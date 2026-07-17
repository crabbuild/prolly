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
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
