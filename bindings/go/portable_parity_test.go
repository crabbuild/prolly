package prolly

import (
	"bytes"
	"context"
	"errors"
	"testing"
)

func TestPortableVersionedIndexedAndProximityMaps(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)

	versioned, err := engine.VersionedMap([]byte("users"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(versioned.Close)
	if _, err := versioned.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err := versioned.Put([]byte("u1"), []byte("Ada")); err != nil {
		t.Fatal(err)
	}
	value, ok, err := versioned.Get([]byte("u1"))
	if err != nil || !ok || !bytes.Equal(value, []byte("Ada")) {
		t.Fatalf("versioned get = %q, %v, %v", value, ok, err)
	}

	registry, err := NewIndexRegistry()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(registry.Close)
	err = registry.Register(
		[]byte("by_team"), 1, "team-v1", IndexProjectionAll, nil,
		IndexExtractorFunc(func(_ []byte, source []byte) ([]IndexEntry, error) {
			return []IndexEntry{{Term: bytes.Clone(source)}}, nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	indexed, err := engine.IndexedMap([]byte("members"), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(indexed.Close)
	if _, err := indexed.Put([]byte("u1"), []byte("red")); err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.EnsureIndex([]byte("by_team")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := indexed.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(snapshot.Close)
	index, err := snapshot.Index([]byte("by_team"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(index.Close)
	rows, err := index.Records([]byte("red"))
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || !bytes.Equal(rows[0].PrimaryKey, []byte("u1")) || !bytes.Equal(rows[0].SourceValue, []byte("red")) {
		t.Fatalf("joined index rows = %#v", rows)
	}

	var escaped IndexMatchView
	if err := index.QueryView(context.Background(), ExactIndex([]byte("red")), func(row IndexMatchView) bool {
		escaped = row
		if !bytes.Equal(row.PrimaryKey.Bytes(), []byte("u1")) {
			t.Fatalf("view primary key = %q", row.PrimaryKey.Bytes())
		}
		return true
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := escaped.PrimaryKey.Copy(); !errors.Is(err, ErrViewExpired) {
		t.Fatalf("escaped view error = %v", err)
	}

	proximity, err := engine.BuildProximity(
		2,
		[]ProximityRecord{{Key: []byte("a"), Vector: []float32{0, 0}, Value: []byte("alpha")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	session, err := proximity.Read()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	result, err := session.Search(context.Background(), ExactSearch([]float32{0.1, 0.1}, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighbors) != 1 || !bytes.Equal(result.Neighbors[0].Key, []byte("a")) {
		t.Fatalf("neighbors = %#v", result.Neighbors)
	}
}

func TestPortableContextCancellation(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	proximity, err := engine.BuildProximity(1, []ProximityRecord{{Key: []byte("a"), Vector: []float32{0}}})
	if err != nil {
		t.Fatal(err)
	}
	defer proximity.Close()
	session, err := proximity.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = session.Search(ctx, ExactSearch([]float32{0}, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("search error = %v", err)
	}
}

func TestPortableAsyncWrappersCopyInputsBeforeHandoff(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("async"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	if _, err := versioned.Initialize(); err != nil {
		t.Fatal(err)
	}

	key := []byte("original-key")
	value := []byte("original-value")
	future := versioned.PutAsync(context.Background(), key, value)
	copy(key, []byte("mutated-key!"))
	copy(value, []byte("mutated-value!"))
	if _, err := future.Await(context.Background()); err != nil {
		t.Fatal(err)
	}
	got, ok, err := versioned.Get([]byte("original-key"))
	if err != nil || !ok || !bytes.Equal(got, []byte("original-value")) {
		t.Fatalf("async put get = %q, %v, %v", got, ok, err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := versioned.GetAsync(cancelled, []byte("original-key")).Await(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled async get error = %v", err)
	}
}

func TestPortableProofSessionAndMaintenance(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("proofs"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	if _, err = versioned.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err = versioned.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := versioned.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	proof, err := snapshot.ProveKey([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyKeyProof(proof)
	if err != nil || !verified.Valid || string(verified.Value) != "v" {
		t.Fatalf("proof = %+v, %v", verified, err)
	}
	session, err := snapshot.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	value, ok, err := session.Get([]byte("k"))
	if err != nil || !ok || string(value) != "v" {
		t.Fatalf("read = %q, %v, %v", value, ok, err)
	}
	backup, err := versioned.Backup()
	if err != nil || len(backup) == 0 {
		t.Fatalf("backup = %d, %v", len(backup), err)
	}
	catalog, err := versioned.VerifyCatalog()
	if err != nil || catalog.VersionCount < 2 {
		t.Fatalf("catalog = %+v, %v", catalog, err)
	}
}
