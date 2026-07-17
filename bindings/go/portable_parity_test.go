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

func TestPortableIndexedProofAndMaintenance(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	registry, err := NewIndexRegistry()
	if err != nil {
		t.Fatal(err)
	}
	defer registry.Close()
	if err := registry.Register(
		[]byte("by_team"), 1, "team-v1", IndexProjectionAll, nil,
		IndexExtractorFunc(func(_ []byte, source []byte) ([]IndexEntry, error) {
			return []IndexEntry{{Term: bytes.Clone(source)}}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	indexed, err := engine.IndexedMap([]byte("indexed-maintenance"), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	version, err := indexed.Put([]byte("u1"), []byte("red"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.EnsureIndex([]byte("by_team")); err != nil {
		t.Fatal(err)
	}
	verification, err := indexed.VerifyIndex([]byte("by_team"), version.SourceVersion)
	if err != nil || !verification.Valid || !verification.Canonical {
		t.Fatalf("verification = %+v, %v", verification, err)
	}
	all, err := indexed.VerifyAll(version.SourceVersion)
	if err != nil || len(all) != 1 || !all[0].Valid {
		t.Fatalf("verify all = %+v, %v", all, err)
	}
	metrics, err := indexed.Metrics()
	if err != nil || metrics.VerificationOutcomes < 2 {
		t.Fatalf("metrics = %+v, %v", metrics, err)
	}
	bundle, err := indexed.ExportCurrent()
	if err != nil || len(bundle) == 0 {
		t.Fatalf("export = %d, %v", len(bundle), err)
	}
	retention, err := indexed.KeepLast(1)
	if err != nil || len(retention.RetainedSourceVersions) != 1 {
		t.Fatalf("retention = %+v, %v", retention, err)
	}
}

func TestPortableProximityProofAndMaintenance(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	proximity, err := engine.BuildProximity(
		2,
		[]ProximityRecord{{Key: []byte("a"), Vector: []float32{0, 0}, Value: []byte("alpha")}},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer proximity.Close()
	if count, err := proximity.Count(); err != nil || count != 1 {
		t.Fatalf("count = %d, %v", count, err)
	}
	if ok, err := proximity.Contains([]byte("a")); err != nil || !ok {
		t.Fatalf("contains = %v, %v", ok, err)
	}
	record, ok, err := proximity.Get([]byte("a"))
	if err != nil || !ok || !bytes.Equal(record.Value, []byte("alpha")) {
		t.Fatalf("record = %+v, %v, %v", record, ok, err)
	}
	descriptor, err := proximity.Descriptor()
	if err != nil || len(descriptor) == 0 {
		t.Fatalf("descriptor = %x, %v", descriptor, err)
	}
	proof, err := proximity.ProveMembership([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	verified, err := VerifyProximityMembershipProof(proof, descriptor)
	if err != nil || !bytes.Equal(verified.Key, []byte("a")) || verified.Record == nil {
		t.Fatalf("verified = %+v, %v", verified, err)
	}
	searchProof, err := proximity.ProveSearch(ExactSearch([]float32{0, 0}, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer searchProof.Close()
	verifiedSearch, err := searchProof.Verify(descriptor)
	if err != nil || len(verifiedSearch.Result.Neighbors) != 1 ||
		!bytes.Equal(verifiedSearch.Result.Neighbors[0].Key, []byte("a")) ||
		verifiedSearch.ReplayedEvents == 0 {
		t.Fatalf("verified search = %+v, %v", verifiedSearch, err)
	}
	health, err := proximity.Verify()
	if err != nil || health.RecordCount != 1 {
		t.Fatalf("health = %+v, %v", health, err)
	}
	if err := proximity.ClearContentCache(); err != nil {
		t.Fatal(err)
	}
}
