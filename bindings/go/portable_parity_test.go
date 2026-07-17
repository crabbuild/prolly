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
	if ok, err := session.Contains([]byte("a")); err != nil || !ok {
		t.Fatalf("session contains = %v, %v", ok, err)
	}
	if record, ok, err := session.Get([]byte("a")); err != nil || !ok || !bytes.Equal(record.Value, []byte("alpha")) {
		t.Fatalf("session record = %+v, %v, %v", record, ok, err)
	}
	proximity.Close()
	result, err := session.Search(context.Background(), ExactSearch([]float32{0.1, 0.1}, 1))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighbors) != 1 || !bytes.Equal(result.Neighbors[0].Key, []byte("a")) {
		t.Fatalf("neighbors = %#v", result.Neighbors)
	}
}

func TestPortableVersionedComparisonPinsVersionsAndPagesDiffs(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("comparison"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	base, err := versioned.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	target, err := versioned.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	comparison, err := versioned.Compare(base.ID, target.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer comparison.Close()
	diffs, err := comparison.Diff()
	if err != nil || len(diffs) != 1 || !bytes.Equal(diffs[0].Key, []byte("k")) {
		t.Fatalf("diffs = %+v, %v", diffs, err)
	}
	page, err := comparison.DiffPage(nil, nil, 1)
	if err != nil || len(page.Diffs) != 1 || !bytes.Equal(page.Diffs[0].Key, []byte("k")) {
		t.Fatalf("page = %+v, %v", page, err)
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

func TestPortableVersionedSnapshotLifecycle(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("versioned-lifecycle"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	id, err := versioned.ID()
	if err != nil || !bytes.Equal(id, []byte("versioned-lifecycle")) {
		t.Fatalf("id = %q, %v", id, err)
	}
	if initialized, err := versioned.IsInitialized(); err != nil || initialized {
		t.Fatalf("initial state = %v, %v", initialized, err)
	}
	initial, err := versioned.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	headID, ok, err := versioned.HeadID()
	if err != nil || !ok || !bytes.Equal(headID, initial.ID) {
		t.Fatalf("initial head = %x, %v, %v", headID, ok, err)
	}
	first, err := versioned.Put([]byte("k"), []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := versioned.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	head, err := versioned.Head()
	if err != nil || head == nil {
		t.Fatalf("head = %#v, %v", head, err)
	}
	headID, ok, err = versioned.HeadID()
	if err != nil || !ok || !bytes.Equal(head.ID, headID) {
		t.Fatalf("head id = %x, %v, %v", headID, ok, err)
	}
	loaded, err := versioned.Version(first.ID)
	if err != nil || loaded == nil || !bytes.Equal(loaded.ID, first.ID) {
		t.Fatalf("version = %#v, %v", loaded, err)
	}
	versions, err := versioned.Versions()
	if err != nil || len(versions) < 3 {
		t.Fatalf("versions = %d, %v", len(versions), err)
	}
	historical, err := versioned.SnapshotAt(first.ID)
	if err != nil || historical == nil {
		t.Fatalf("snapshot at = %#v, %v", historical, err)
	}
	defer historical.Close()
	snapshotID, err := historical.ID()
	if err != nil || !bytes.Equal(snapshotID, first.ID) {
		t.Fatalf("snapshot id = %x, %v", snapshotID, err)
	}
	snapshotVersion, err := historical.Version()
	if err != nil || !bytes.Equal(snapshotVersion.ID, first.ID) {
		t.Fatalf("snapshot version = %#v, %v", snapshotVersion, err)
	}
	value, ok, err := historical.Get([]byte("k"))
	if err != nil || !ok || !bytes.Equal(value, []byte("v1")) {
		t.Fatalf("historical get = %q, %v, %v", value, ok, err)
	}
}

func TestPortableVersionedSnapshotOrderedNavigationAndBoundedPages(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("versioned-ordered"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	if _, err := versioned.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err := versioned.Apply([]Mutation{
		UpsertMutation([]byte("a"), []byte("one")),
		UpsertMutation([]byte("ab"), []byte("two")),
		UpsertMutation([]byte("b"), []byte("three")),
		UpsertMutation([]byte("c"), []byte("four")),
	}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := versioned.Snapshot()
	if err != nil || snapshot == nil {
		t.Fatalf("snapshot = %#v, %v", snapshot, err)
	}
	defer snapshot.Close()
	if contains, err := snapshot.ContainsKey([]byte("ab")); err != nil || !contains {
		t.Fatalf("contains = %v, %v", contains, err)
	}
	values, err := snapshot.GetMany([][]byte{[]byte("a"), []byte("missing")})
	if err != nil || !bytes.Equal(values[0], []byte("one")) || values[1] != nil {
		t.Fatalf("get many = %#v, %v", values, err)
	}
	first, err := snapshot.FirstEntry()
	if err != nil || first == nil || !bytes.Equal(first.Key, []byte("a")) {
		t.Fatalf("first = %#v, %v", first, err)
	}
	last, err := snapshot.LastEntry()
	if err != nil || last == nil || !bytes.Equal(last.Key, []byte("c")) {
		t.Fatalf("last = %#v, %v", last, err)
	}
	lower, err := snapshot.LowerBound([]byte("aa"))
	if err != nil || lower == nil || !bytes.Equal(lower.Key, []byte("ab")) {
		t.Fatalf("lower = %#v, %v", lower, err)
	}
	upper, err := snapshot.UpperBound([]byte("ab"))
	if err != nil || upper == nil || !bytes.Equal(upper.Key, []byte("b")) {
		t.Fatalf("upper = %#v, %v", upper, err)
	}
	prefix, err := snapshot.Prefix([]byte("a"))
	if err != nil || len(prefix) != 2 || !bytes.Equal(prefix[1].Key, []byte("ab")) {
		t.Fatalf("prefix = %#v, %v", prefix, err)
	}
	ranged, err := snapshot.Range([]byte("ab"), []byte("c"))
	if err != nil || len(ranged) != 2 || !bytes.Equal(ranged[1].Key, []byte("b")) {
		t.Fatalf("range = %#v, %v", ranged, err)
	}
	prefixPage, err := snapshot.PrefixPage([]byte("a"), nil, 1)
	if err != nil || len(prefixPage.Entries) != 1 || prefixPage.NextCursor == nil || !bytes.Equal(prefixPage.Entries[0].Key, []byte("a")) {
		t.Fatalf("prefix page = %#v, %v", prefixPage, err)
	}
	page, err := snapshot.RangePage(nil, []byte("c"), 2)
	if err != nil || len(page.Entries) != 2 || page.NextCursor == nil {
		t.Fatalf("first page = %#v, %v", page, err)
	}
	page, err = snapshot.RangePage(page.NextCursor, []byte("c"), 2)
	if err != nil || len(page.Entries) != 1 || !bytes.Equal(page.Entries[0].Key, []byte("b")) {
		t.Fatalf("second page = %#v, %v", page, err)
	}
	reverse, err := snapshot.ReversePage(nil, []byte("a"), 2)
	if err != nil || len(reverse.Entries) != 2 || !bytes.Equal(reverse.Entries[0].Key, []byte("c")) {
		t.Fatalf("reverse = %#v, %v", reverse, err)
	}
	prefixed, err := snapshot.PrefixReversePage([]byte("a"), nil, 2)
	if err != nil || len(prefixed.Entries) != 2 || !bytes.Equal(prefixed.Entries[0].Key, []byte("ab")) {
		t.Fatalf("prefix reverse = %#v, %v", prefixed, err)
	}
}

func TestPortableVersionedBatchCASAndPinnedPointReads(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("versioned-cas"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	if _, err := versioned.Initialize(); err != nil {
		t.Fatal(err)
	}
	first, err := versioned.Apply([]Mutation{
		UpsertMutation([]byte("a"), []byte("one")),
		UpsertMutation([]byte("b"), []byte("two")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if contains, err := versioned.ContainsKey([]byte("a")); err != nil || !contains {
		t.Fatalf("contains = %v, %v", contains, err)
	}
	values, err := versioned.GetMany([][]byte{[]byte("a"), []byte("missing")})
	if err != nil || !bytes.Equal(values[0], []byte("one")) || values[1] != nil {
		t.Fatalf("get many = %#v, %v", values, err)
	}
	applied, err := versioned.PutIf(first.ID, []byte("a"), []byte("updated"))
	if err != nil || applied.Kind != MapUpdateApplied || applied.Current == nil {
		t.Fatalf("put if = %#v, %v", applied, err)
	}
	conflict, err := versioned.DeleteIf(first.ID, []byte("b"))
	if err != nil || conflict.Kind != MapUpdateConflict {
		t.Fatalf("delete conflict = %#v, %v", conflict, err)
	}
	values, err = versioned.GetManyAt(first.ID, [][]byte{[]byte("a"), []byte("b")})
	if err != nil || !bytes.Equal(values[0], []byte("one")) || !bytes.Equal(values[1], []byte("two")) {
		t.Fatalf("historical get many = %#v, %v", values, err)
	}
	value, ok, err := versioned.GetAt(first.ID, []byte("a"))
	if err != nil || !ok || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("historical get = %q, %v, %v", value, ok, err)
	}
	batch, err := versioned.ApplyIf(applied.Current.ID, []Mutation{DeleteMutation([]byte("b"))})
	if err != nil || batch.Kind != MapUpdateApplied {
		t.Fatalf("apply if = %#v, %v", batch, err)
	}
}

func TestPortableVersionedBackupRestoreAndRetention(t *testing.T) {
	sourceEngine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer sourceEngine.Close()
	targetEngine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer targetEngine.Close()
	source, err := sourceEngine.VersionedMap([]byte("versioned-backup"))
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	if _, err := source.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put([]byte("k"), []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if _, err := source.Put([]byte("k"), []byte("v2")); err != nil {
		t.Fatal(err)
	}
	backup, err := source.Backup()
	if err != nil {
		t.Fatal(err)
	}
	target, err := targetEngine.VersionedMap([]byte("versioned-backup"))
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	restored, err := target.RestoreBackup(backup)
	if err != nil {
		t.Fatal(err)
	}
	headID, ok, err := source.HeadID()
	if err != nil || !ok || !bytes.Equal(restored.ID, headID) {
		t.Fatalf("restored head = %x, source = %x, %v", restored.ID, headID, err)
	}
	value, ok, err := target.Get([]byte("k"))
	if err != nil || !ok || !bytes.Equal(value, []byte("v2")) {
		t.Fatalf("restored get = %q, %v, %v", value, ok, err)
	}
	pruned, err := source.KeepLast(1)
	if err != nil || len(pruned.Retained) == 0 || len(pruned.Removed) == 0 {
		t.Fatalf("pruned = %#v, %v", pruned, err)
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
	if _, err = versioned.Put([]byte("ka"), []byte("v2")); err != nil {
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
	multiProof, err := snapshot.ProveKeys([][]byte{[]byte("k"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	multi, err := VerifyMultiKeyProof(multiProof)
	if err != nil || !multi.Valid || len(multi.Results) != 2 || !multi.Results[0].Exists || multi.Results[1].Exists {
		t.Fatalf("multi proof = %+v, %v", multi, err)
	}
	rangeProof, err := snapshot.ProveRange([]byte("k"), []byte("l"))
	if err != nil {
		t.Fatal(err)
	}
	ranged, err := VerifyRangeProof(rangeProof)
	if err != nil || !ranged.Valid || len(ranged.Entries) != 2 {
		t.Fatalf("range proof = %+v, %v", ranged, err)
	}
	prefixProof, err := snapshot.ProvePrefix([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	prefixed, err := VerifyRangeProof(prefixProof)
	if err != nil || !prefixed.Valid || len(prefixed.Entries) != 2 {
		t.Fatalf("prefix proof = %+v, %v", prefixed, err)
	}
	provedPage, err := snapshot.ProveRangePage(nil, []byte("l"), 1)
	if err != nil {
		t.Fatal(err)
	}
	page, err := VerifyRangePageProof(provedPage.Proof)
	if err != nil || !page.Valid || len(provedPage.Page.Entries) != 1 {
		t.Fatalf("page proof = %+v, %v", page, err)
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

func TestPortableIndexedBatchCASAndHistoricalSnapshots(t *testing.T) {
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
		[]byte("by_value"), 1, "value-v1", IndexProjectionAll, nil,
		IndexExtractorFunc(func(_ []byte, value []byte) ([]IndexEntry, error) {
			return []IndexEntry{{Term: bytes.Clone(value)}}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	indexed, err := engine.IndexedMap([]byte("indexed-lifecycle"), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	if id, err := indexed.ID(); err != nil || !bytes.Equal(id, []byte("indexed-lifecycle")) {
		t.Fatalf("id = %q, %v", id, err)
	}
	first, err := indexed.Apply([]Mutation{
		UpsertMutation([]byte("u1"), []byte("red")),
		UpsertMutation([]byte("u2"), []byte("red")),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.EnsureIndex([]byte("by_value")); err != nil {
		t.Fatal(err)
	}
	firstSnapshot, err := indexed.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer firstSnapshot.Close()
	firstID, err := firstSnapshot.ID()
	if err != nil || !bytes.Equal(firstID.SourceVersion, first.SourceVersion) {
		t.Fatalf("first snapshot id = %+v, %v", firstID, err)
	}
	applied, err := indexed.ApplyIf(first.SourceVersion, []Mutation{
		UpsertMutation([]byte("u3"), []byte("blue")),
	})
	if err != nil || applied.Kind != IndexedUpdateApplied || applied.Current == nil {
		t.Fatalf("applied = %+v, %v", applied, err)
	}
	conflict, err := indexed.ApplyIf(first.SourceVersion, []Mutation{
		DeleteMutation([]byte("u1")),
	})
	if err != nil || conflict.Kind != IndexedUpdateConflict {
		t.Fatalf("conflict = %+v, %v", conflict, err)
	}
	historical, err := indexed.SnapshotAt(first.SourceVersion)
	if err != nil {
		t.Fatal(err)
	}
	defer historical.Close()
	historicalID, err := historical.ID()
	if err != nil || !bytes.Equal(historicalID.SourceVersion, firstID.SourceVersion) {
		t.Fatalf("historical id = %+v, %v", historicalID, err)
	}
	reopened, err := indexed.SnapshotByID(firstID)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	reopenedID, err := reopened.ID()
	if err != nil || !bytes.Equal(reopenedID.CatalogVersion, firstID.CatalogVersion) {
		t.Fatalf("reopened id = %+v, %v", reopenedID, err)
	}
}

func TestPortableIndexedCompleteMaintenanceRecords(t *testing.T) {
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
		[]byte("by_value"), 1, "value-v1", IndexProjectionAll, nil,
		IndexExtractorFunc(func(_ []byte, value []byte) ([]IndexEntry, error) {
			return []IndexEntry{{Term: bytes.Clone(value)}}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	indexed, err := engine.IndexedMap([]byte("indexed-records"), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	version, err := indexed.Put([]byte("u1"), []byte("red"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.EnsureIndex([]byte("by_value")); err != nil {
		t.Fatal(err)
	}
	health, err := indexed.Health()
	if err != nil || !bytes.Equal(health.SourceMapID, []byte("indexed-records")) || len(health.ActiveIndexes) != 1 {
		t.Fatalf("health = %+v, %v", health, err)
	}
	repaired, err := indexed.RepairIndex([]byte("by_value"), version.SourceVersion)
	if err != nil || !repaired.Valid || !repaired.Canonical {
		t.Fatalf("repair = %+v, %v", repaired, err)
	}
	bundle, err := indexed.ExportCurrent()
	if err != nil {
		t.Fatal(err)
	}
	next, err := indexed.Put([]byte("u2"), []byte("blue"))
	if err != nil {
		t.Fatal(err)
	}
	imported, err := indexed.ImportCurrent(bundle, next.SourceVersion)
	if err != nil || !bytes.Equal(imported.SourceVersion, version.SourceVersion) {
		t.Fatalf("imported = %+v, %v", imported, err)
	}
	if _, err := indexed.DeactivateIndex([]byte("by_value")); err != nil {
		t.Fatal(err)
	}
	health, err = indexed.Health()
	if err != nil || len(health.ActiveIndexes) != 0 {
		t.Fatalf("deactivated health = %+v, %v", health, err)
	}
}

func TestPortableSecondaryIndexOwnedPagesCoverEveryDirection(t *testing.T) {
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
		[]byte("by_value"), 1, "value-v1", IndexProjectionAll, nil,
		IndexExtractorFunc(func(_ []byte, value []byte) ([]IndexEntry, error) {
			return []IndexEntry{{Term: bytes.Clone(value)}}, nil
		}),
	); err != nil {
		t.Fatal(err)
	}
	indexed, err := engine.IndexedMap([]byte("indexed-pages"), registry)
	if err != nil {
		t.Fatal(err)
	}
	defer indexed.Close()
	if _, err := indexed.Apply([]Mutation{
		UpsertMutation([]byte("u1"), []byte("red")),
		UpsertMutation([]byte("u2"), []byte("red")),
		UpsertMutation([]byte("u3"), []byte("rose")),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := indexed.EnsureIndex([]byte("by_value")); err != nil {
		t.Fatal(err)
	}
	snapshot, err := indexed.Snapshot()
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	index, err := snapshot.Index([]byte("by_value"))
	if err != nil {
		t.Fatal(err)
	}
	defer index.Close()
	if name, err := index.Name(); err != nil || !bytes.Equal(name, []byte("by_value")) {
		t.Fatalf("name = %q, %v", name, err)
	}
	checkPage := func(name, want string, page IndexPage, err error) {
		t.Helper()
		if err != nil || len(page.Matches) != 1 || !bytes.Equal(page.Matches[0].PrimaryKey, []byte(want)) {
			t.Fatalf("%s page = %+v, %v", name, page, err)
		}
	}
	page, pageErr := index.ExactPage([]byte("red"), nil, 1)
	checkPage("exact", "u1", page, pageErr)
	page, pageErr = index.ExactReversePage([]byte("red"), nil, 1)
	checkPage("exact reverse", "u2", page, pageErr)
	page, pageErr = index.PrefixPage([]byte("r"), nil, 1)
	checkPage("prefix", "u1", page, pageErr)
	page, pageErr = index.PrefixReversePage([]byte("r"), nil, 1)
	checkPage("prefix reverse", "u3", page, pageErr)
	page, pageErr = index.RangePage([]byte("red"), []byte("s"), nil, 1)
	checkPage("range", "u1", page, pageErr)
	page, pageErr = index.RangeReversePage([]byte("red"), []byte("s"), nil, 1)
	checkPage("range reverse", "u3", page, pageErr)
	if rows, err := index.Exact([]byte("red")); err != nil || len(rows) != 2 {
		t.Fatalf("exact = %+v, %v", rows, err)
	}
	if rows, err := index.Prefix([]byte("r")); err != nil || len(rows) != 3 {
		t.Fatalf("prefix = %+v, %v", rows, err)
	}
	if rows, err := index.Range([]byte("red"), []byte("s")); err != nil || len(rows) != 3 {
		t.Fatalf("range = %+v, %v", rows, err)
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
	if config, err := proximity.Config(); err != nil || config.Dimensions != 2 {
		t.Fatalf("config = %+v, %v", config, err)
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
	structural, err := proximity.ProveStructure()
	if err != nil {
		t.Fatal(err)
	}
	verifiedStructure, err := VerifyProximityStructuralProof(structural, descriptor)
	if err != nil || verifiedStructure.Summary.RecordCount != 1 {
		t.Fatalf("verified structure = %+v, %v", verifiedStructure, err)
	}
	mutated, mutationStats, err := proximity.Mutate([]ProximityMutation{
		UpsertProximity([]byte("b"), []float32{1, 1}, []byte("beta")),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer mutated.Close()
	if count, err := mutated.Count(); err != nil || count != 2 || mutationStats.RecordsRebuilt == 0 {
		t.Fatalf("mutated count/stats = %d, %+v, %v", count, mutationStats, err)
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
