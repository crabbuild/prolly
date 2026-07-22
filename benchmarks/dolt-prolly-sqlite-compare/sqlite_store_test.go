package main

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"

	"github.com/dolthub/dolt/go/store/chunks"
	"github.com/dolthub/dolt/go/store/hash"
)

func TestSQLiteChunkStorePersistsChunkAndRoot(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "prolly.db")
	store, err := openSQLiteChunkStore(path)
	if err != nil {
		t.Fatal(err)
	}
	chunk := chunks.NewChunk([]byte("payload"))
	if err := putChunkWithoutRefs(ctx, store, chunk); err != nil {
		t.Fatal(err)
	}
	ok, err := store.Commit(ctx, chunk.Hash(), hash.Hash{})
	if err != nil || !ok {
		t.Fatalf("commit ok=%v err=%v", ok, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := openSQLiteChunkStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	got, err := reopened.Get(ctx, chunk.Hash())
	if err != nil || !bytes.Equal(got.Data(), chunk.Data()) {
		t.Fatalf("get=%q err=%v", got.Data(), err)
	}
	root, err := reopened.Root(ctx)
	if err != nil || root != chunk.Hash() {
		t.Fatalf("root=%s err=%v", root, err)
	}
}

func TestSQLiteChunkStoreRejectsStaleRoot(t *testing.T) {
	ctx := context.Background()
	store, err := openSQLiteChunkStore(filepath.Join(t.TempDir(), "prolly.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	first := chunks.NewChunk([]byte("first"))
	if err := putChunkWithoutRefs(ctx, store, first); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Commit(ctx, first.Hash(), hash.Hash{}); err != nil || !ok {
		t.Fatal(ok, err)
	}
	second := chunks.NewChunk([]byte("second"))
	if err := putChunkWithoutRefs(ctx, store, second); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Commit(ctx, second.Hash(), hash.Of([]byte("stale"))); err != nil || ok {
		t.Fatalf("stale commit ok=%v err=%v", ok, err)
	}
	root, _ := store.Root(ctx)
	if root != first.Hash() {
		t.Fatalf("root moved to %s", root)
	}
}

func putChunkWithoutRefs(ctx context.Context, store *sqliteChunkStore, chunk chunks.Chunk) error {
	return store.Put(ctx, chunk, func(chunks.Chunk) chunks.InsertAddrsCb {
		return func(context.Context, hash.HashSet, chunks.PendingRefExists) error { return nil }
	})
}
