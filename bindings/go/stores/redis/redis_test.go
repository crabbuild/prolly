package redis

import (
	"context"
	"os"
	"testing"

	prolly "build.crab/prolly-go"
	"build.crab/prolly-go/storetest"
	redisclient "github.com/redis/go-redis/v9"
)

func TestRedisConformance(t *testing.T) {
	address := os.Getenv("PROLLY_REDIS_ADDR")
	if address == "" {
		t.Skip("PROLLY_REDIS_ADDR is not set")
	}
	client := redisclient.NewClient(&redisclient.Options{Addr: address})
	t.Cleanup(func() { _ = client.Close() })
	store := New(client, Options{KeyPrefix: []byte("prolly:test:")})
	if err := store.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := client.Set(context.Background(), "prolly:test:root:rust-root", []byte("rust-manifest"), 0).Err(); err != nil {
		t.Fatal(err)
	}
	root, err := store.GetRootManifest(context.Background(), []byte("rust-root"))
	if err != nil || !root.Present || string(root.Value) != "rust-manifest" {
		t.Fatalf("Rust-layout root = %#v, %v", root, err)
	}
	if err := store.PutHint(context.Background(), []byte("go-ns"), []byte("go-key"), []byte("go-hint")); err != nil {
		t.Fatal(err)
	}
	rawHint, err := client.Get(context.Background(), store.hintKey([]byte("go-ns"), []byte("go-key"))).Bytes()
	if err != nil || string(rawHint) != "go-hint" {
		t.Fatalf("Go-layout hint = %q, %v", rawHint, err)
	}
	if err := store.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
