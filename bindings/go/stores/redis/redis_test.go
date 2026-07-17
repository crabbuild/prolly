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
	storetest.RunWithStore(t, prolly.RemoteStore(store))
}
