package prolly

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
)

func joinMergeResolver(conflict Conflict) Resolution {
	if conflict.LeftPresent && conflict.RightPresent {
		value := append([]byte{}, conflict.Left...)
		value = append(value, '|')
		value = append(value, conflict.Right...)
		return ResolutionValue(value)
	}
	if conflict.LeftPresent {
		return ResolutionValue(conflict.Left)
	}
	if conflict.RightPresent {
		return ResolutionValue(conflict.Right)
	}
	return ResolutionDelete()
}

func joinCrdtResolver(conflict Conflict) CrdtResolution {
	if conflict.LeftPresent && conflict.RightPresent {
		value := append([]byte{}, conflict.Left...)
		value = append(value, '|')
		value = append(value, conflict.Right...)
		return CrdtResolutionValue(value)
	}
	if conflict.LeftPresent {
		return CrdtResolutionValue(conflict.Left)
	}
	if conflict.RightPresent {
		return CrdtResolutionValue(conflict.Right)
	}
	return CrdtResolutionDelete()
}

type memoryHostStore struct {
	mu    sync.Mutex
	nodes map[string][]byte
	hints map[string][]byte
	roots map[string]RootManifest
}

func newMemoryHostStore() *memoryHostStore {
	return &memoryHostStore{
		nodes: map[string][]byte{},
		hints: map[string][]byte{},
		roots: map[string]RootManifest{},
	}
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	return append([]byte(nil), value...)
}

func hintKey(namespace []byte, key []byte) string {
	return string(namespace) + "\x00" + string(key)
}

func (s *memoryHostStore) Get(key []byte) HostStoreResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.nodes[string(key)]
	return HostStoreResult{Value: cloneBytes(value), Ok: ok}
}

func (s *memoryHostStore) Put(key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[string(key)] = cloneBytes(value)
	return nil
}

func (s *memoryHostStore) Delete(key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, string(key))
	return nil
}

func (s *memoryHostStore) Batch(ops []Mutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, op := range ops {
		switch op.Kind {
		case "upsert":
			s.nodes[string(op.Key)] = cloneBytes(op.Value)
		case "delete":
			delete(s.nodes, string(op.Key))
		}
	}
	return nil
}

func (s *memoryHostStore) BatchGetOrdered(keys [][]byte) ([]HostStoreResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	results := make([]HostStoreResult, 0, len(keys))
	for _, key := range keys {
		value, ok := s.nodes[string(key)]
		results = append(results, HostStoreResult{Value: cloneBytes(value), Ok: ok})
	}
	return results, nil
}

func (s *memoryHostStore) PrefersBatchReads() bool {
	return true
}

func (s *memoryHostStore) SupportsHints() bool {
	return true
}

func (s *memoryHostStore) GetHint(namespace []byte, key []byte) HostStoreResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.hints[hintKey(namespace, key)]
	return HostStoreResult{Value: cloneBytes(value), Ok: ok}
}

func (s *memoryHostStore) PutHint(namespace []byte, key []byte, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hints[hintKey(namespace, key)] = cloneBytes(value)
	return nil
}

func (s *memoryHostStore) ListNodeCids() ([][]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cids := make([][]byte, 0, len(s.nodes))
	for key := range s.nodes {
		cids = append(cids, []byte(key))
	}
	sort.Slice(cids, func(i, j int) bool {
		return bytes.Compare(cids[i], cids[j]) < 0
	})
	return cids, nil
}

func (s *memoryHostStore) GetRoot(name []byte) (*RootManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	manifest, ok := s.roots[string(name)]
	if !ok {
		return nil, nil
	}
	return &RootManifest{raw: cloneBytes(manifest.raw)}, nil
}

func (s *memoryHostStore) PutRoot(name []byte, manifest RootManifest) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roots[string(name)] = RootManifest{raw: cloneBytes(manifest.raw)}
	return nil
}

func (s *memoryHostStore) DeleteRoot(name []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roots, string(name))
	return nil
}

func (s *memoryHostStore) CompareAndSwapRoot(name []byte, expected *RootManifest, replacement *RootManifest) HostStoreCasResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.roots[string(name)]
	if !rootManifestEqual(expected, current, ok) {
		if !ok {
			return HostStoreCasResult{Current: nil}
		}
		return HostStoreCasResult{Current: &RootManifest{raw: cloneBytes(current.raw)}}
	}
	if replacement == nil {
		delete(s.roots, string(name))
	} else {
		s.roots[string(name)] = RootManifest{raw: cloneBytes(replacement.raw)}
	}
	return HostStoreCasResult{Applied: true}
}

func rootManifestEqual(expected *RootManifest, current RootManifest, currentOK bool) bool {
	if expected == nil {
		return !currentOK
	}
	return currentOK && bytes.Equal(expected.raw, current.raw)
}

func (s *memoryHostStore) ListRoots() ([]NamedRootManifest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.roots))
	for name := range s.roots {
		names = append(names, name)
	}
	sort.Strings(names)
	roots := make([]NamedRootManifest, 0, len(names))
	for _, name := range names {
		roots = append(roots, NamedRootManifest{
			Name:     []byte(name),
			Manifest: RootManifest{raw: cloneBytes(s.roots[name].raw)},
		})
	}
	return roots, nil
}

type fixtureFile struct {
	NodeFixtures     []nodeFixture     `json:"node_fixtures"`
	BoundaryFixtures []boundaryFixture `json:"boundary_fixtures"`
	KeyFixtures      keyFixtures       `json:"key_fixtures"`
	TreeFixtures     []treeFixture     `json:"tree_fixtures"`
	DiffFixtures     []diffFixture     `json:"diff_fixtures"`
	ValueFixtures    []valueFixture    `json:"value_fixtures"`
	BlobFixtures     []blobFixture     `json:"blob_fixtures"`
	ManifestFixtures []manifestFixture `json:"manifest_fixtures"`
}

type configFixture struct {
	MinChunkSize      uint64          `json:"min_chunk_size"`
	MaxChunkSize      uint64          `json:"max_chunk_size"`
	ChunkingFactor    uint32          `json:"chunking_factor"`
	HashSeed          uint64          `json:"hash_seed"`
	Encoding          encodingFixture `json:"encoding"`
	NodeCacheMaxNodes *uint64         `json:"node_cache_max_nodes"`
	NodeCacheMaxBytes *uint64         `json:"node_cache_max_bytes"`
}

type encodingFixture struct {
	Kind       string  `json:"kind"`
	CustomName *string `json:"custom_name"`
}

type nodeFixture struct {
	Name  string `json:"name"`
	Bytes string `json:"bytes"`
	Cid   string `json:"cid"`
}

type boundaryFixture struct {
	Name       string        `json:"name"`
	Config     configFixture `json:"config"`
	Count      uint64        `json:"count"`
	Key        string        `json:"key"`
	Value      string        `json:"value"`
	IsBoundary bool          `json:"is_boundary"`
}

type keyFixtures struct {
	PrefixEnd []struct {
		Prefix string  `json:"prefix"`
		End    *string `json:"end"`
	} `json:"prefix_end"`
	Numeric []struct {
		Kind    string `json:"kind"`
		Value   string `json:"value"`
		Encoded string `json:"encoded"`
	} `json:"numeric"`
	Segments []struct {
		Segments []string `json:"segments"`
		Encoded  string   `json:"encoded"`
		Decoded  []string `json:"decoded"`
	} `json:"segments"`
	Debug []struct {
		Key   string `json:"key"`
		Debug string `json:"debug"`
	} `json:"debug"`
}

type treeFixture struct {
	Name    string         `json:"name"`
	Config  configFixture  `json:"config"`
	Root    string         `json:"root"`
	Entries []entryFixture `json:"entries"`
	Lookups []struct {
		Key   string  `json:"key"`
		Value *string `json:"value"`
	} `json:"lookups"`
	Ranges []struct {
		Start   string         `json:"start"`
		End     *string        `json:"end"`
		Entries []entryFixture `json:"entries"`
	} `json:"ranges"`
}

type diffFixture struct {
	Name      string        `json:"name"`
	Config    configFixture `json:"config"`
	BaseRoot  string        `json:"base_root"`
	OtherRoot string        `json:"other_root"`
	Diffs     []struct {
		Kind  string  `json:"kind"`
		Key   string  `json:"key"`
		Value *string `json:"value"`
		Old   *string `json:"old"`
		New   *string `json:"new"`
	} `json:"diffs"`
}

type entryFixture struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type valueFixture struct {
	SchemaName string `json:"schema_name"`
	Version    uint64 `json:"version"`
	Bytes      string `json:"bytes"`
}

type blobFixture struct {
	Bytes string `json:"bytes"`
	Kind  string `json:"kind"`
}

type manifestFixture struct {
	Bytes string `json:"bytes"`
}

func TestMemoryEngineCrudAndRange(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	tree, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = engine.Put(tree, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatal(err)
	}

	value, ok, err := engine.Get(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(value, []byte("1")) {
		t.Fatalf("get returned %q, %v; want %q, true", value, ok, []byte("1"))
	}

	entries, err := engine.Range(tree, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("range returned %d entries; want 1", len(entries))
	}
	if !bytes.Equal(entries[0].Key, []byte("a")) || !bytes.Equal(entries[0].Value, []byte("1")) {
		t.Fatalf("range returned %#v", entries[0])
	}
}

func TestCustomStoreCallbacksDriveEngine(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := CustomStore(newMemoryHostStore(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	tree, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = engine.Batch(tree, []Mutation{
		{Kind: "upsert", Key: []byte("a"), Value: []byte("1")},
		{Kind: "upsert", Key: []byte("b"), Value: []byte("2")},
	})
	if err != nil {
		t.Fatal(err)
	}

	values, present, err := engine.GetMany(tree, [][]byte{[]byte("a"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if !present[0] || string(values[0]) != "1" || present[1] {
		t.Fatalf("unexpected get-many results values=%q present=%v", values, present)
	}

	publishedHint, err := engine.PublishPrefixPathHint(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	hydratedHint, err := engine.HydratePrefixPathHint(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !publishedHint || !hydratedHint {
		t.Fatalf("expected custom store hint path to publish and hydrate")
	}

	if err := engine.PublishNamedRootAtMillis([]byte("main"), tree, 42); err != nil {
		t.Fatal(err)
	}
	loaded, err := engine.LoadNamedRoot([]byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("expected named root from custom store")
	}
	roots, err := engine.ListNamedRoots()
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 || string(roots[0].Name) != "main" {
		t.Fatalf("unexpected named roots: %#v", roots)
	}
	manifests, err := engine.ListNamedRootManifests()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || string(manifests[0].Name) != "main" {
		t.Fatalf("unexpected named root manifests: %#v", manifests)
	}
	if !bytes.Equal(manifests[0].Manifest.Tree.raw, tree.raw) {
		t.Fatalf("manifest tree does not match published tree")
	}
	if manifests[0].Manifest.CreatedAtMillis == nil || *manifests[0].Manifest.CreatedAtMillis != 42 {
		t.Fatalf("unexpected manifest created timestamp: %#v", manifests[0].Manifest.CreatedAtMillis)
	}
	if manifests[0].Manifest.UpdatedAtMillis == nil || *manifests[0].Manifest.UpdatedAtMillis != 42 {
		t.Fatalf("unexpected manifest updated timestamp: %#v", manifests[0].Manifest.UpdatedAtMillis)
	}

	cids, err := engine.ListNodeCids()
	if err != nil {
		t.Fatal(err)
	}
	if len(cids) == 0 {
		t.Fatal("expected custom store node scan to return CIDs")
	}
	plan, err := engine.PlanStoreGC([]Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if plan.ReclaimableNodes != 0 {
		t.Fatalf("expected no reclaimable live nodes, got %d", plan.ReclaimableNodes)
	}
	retainedPlan, err := engine.PlanStoreGCForRetention(NamedRootRetention{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if retainedPlan.ReclaimableNodes != 0 {
		t.Fatalf("expected no reclaimable retained nodes, got %d", retainedPlan.ReclaimableNodes)
	}

	destination, err := CustomStore(newMemoryHostStore(), config)
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	missing, err := engine.PlanMissingNodes(tree, destination)
	if err != nil {
		t.Fatal(err)
	}
	if missing.MissingNodes == 0 {
		t.Fatal("expected destination custom store to miss source nodes")
	}
	copied, err := engine.CopyMissingNodes(tree, destination)
	if err != nil {
		t.Fatal(err)
	}
	if copied.CopiedNodes != missing.MissingNodes {
		t.Fatalf("copied %d nodes, want %d", copied.CopiedNodes, missing.MissingNodes)
	}
	copiedValue, ok, err := destination.Get(tree, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(copiedValue) != "2" {
		t.Fatalf("destination custom store get returned ok=%v value=%q", ok, copiedValue)
	}

	update, err := engine.CompareAndSwapNamedRoot([]byte("main"), &tree, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !update.Applied {
		t.Fatalf("expected named-root CAS delete to apply: %#v", update)
	}
}

func TestFileEnginePersistsNodesAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "nodes")

	first, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := first.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = first.Put(tree, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	first.Close()

	reopened, err := OpenFile(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, ok, err := reopened.Get(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "v" {
		t.Fatalf("reopened file engine returned ok=%v value=%q", ok, value)
	}
}

func TestSQLiteEnginePersistsNodesAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "prolly.db")

	first, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := first.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = first.Put(tree, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	first.Close()

	reopened, err := OpenSQLite(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	value, ok, err := reopened.Get(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "v" {
		t.Fatalf("reopened SQLite engine returned ok=%v value=%q", ok, value)
	}

	inMemory, err := OpenSQLiteInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer inMemory.Close()
	empty, err := inMemory.Create()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := inMemory.Put(empty, []byte("transient"), []byte("ok")); err != nil {
		t.Fatal(err)
	}
}

func TestParityBatchPagesMergeAndNamedRoots(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	empty, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	if upsert := UpsertMutation([]byte("probe"), []byte("value")); upsert.Kind != "upsert" || string(upsert.Value) != "value" {
		t.Fatalf("unexpected upsert mutation helper: %#v", upsert)
	}
	if deletion := DeleteMutation([]byte("probe")); deletion.Kind != "delete" || len(deletion.Value) != 0 {
		t.Fatalf("unexpected delete mutation helper: %#v", deletion)
	}
	tree, err := engine.Batch(empty, []Mutation{
		UpsertMutation([]byte("a"), []byte("1")),
		UpsertMutation([]byte("b"), []byte("2")),
		UpsertMutation([]byte("a"), []byte("11")),
		DeleteMutation([]byte("missing")),
	})
	if err != nil {
		t.Fatal(err)
	}

	values, present, err := engine.GetMany(tree, [][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(values) != 3 || len(present) != 3 {
		t.Fatalf("unexpected getMany result lengths: %d %d", len(values), len(present))
	}
	if !present[0] || string(values[0]) != "11" {
		t.Fatalf("unexpected value for a: present=%v value=%q", present[0], values[0])
	}
	if present[1] {
		t.Fatalf("expected missing key to be absent")
	}
	if !present[2] || string(values[2]) != "2" {
		t.Fatalf("unexpected value for b: present=%v value=%q", present[2], values[2])
	}

	proof, err := engine.ProveKey(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	verifiedProof, err := VerifyKeyProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedProof.Valid || !verifiedProof.Exists || verifiedProof.Absence || !verifiedProof.HasValue || string(verifiedProof.Value) != "11" {
		t.Fatalf("unexpected verified proof: %#v", verifiedProof)
	}
	pathBytes, err := KeyProofPathNodeBytes(proof)
	if err != nil {
		t.Fatal(err)
	}
	decodedProof, err := KeyProofFromNodeBytes(proof.Root, proof.HasRoot, proof.Key, pathBytes)
	if err != nil {
		t.Fatal(err)
	}
	decodedVerification, err := VerifyKeyProof(decodedProof)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedVerification.Valid || string(decodedVerification.Value) != "11" {
		t.Fatalf("unexpected decoded proof verification: %#v", decodedVerification)
	}
	proofBytes, err := KeyProofToBytes(proof)
	if err != nil {
		t.Fatal(err)
	}
	keySummary, err := InspectProofBundle(proofBytes)
	if err != nil {
		t.Fatal(err)
	}
	treeRoot, hasTreeRoot, err := tree.Root()
	if err != nil {
		t.Fatal(err)
	}
	if keySummary.Kind != "key" || !keySummary.HasRoot || !hasTreeRoot || !bytes.Equal(keySummary.Root, treeRoot) || keySummary.KeyCount != 1 || keySummary.PathNodeCount != uint64(len(proof.Path)) {
		t.Fatalf("unexpected key proof bundle summary: %#v", keySummary)
	}
	keyBundleVerification, err := VerifyProofBundle(proofBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !keyBundleVerification.Valid || keyBundleVerification.Summary.Kind != "key" || keyBundleVerification.ExistsCount != 1 || keyBundleVerification.AbsenceCount != 0 {
		t.Fatalf("unexpected key proof bundle verification: %#v", keyBundleVerification)
	}
	decodedProofFromBytes, err := KeyProofFromBytes(proofBytes)
	if err != nil {
		t.Fatal(err)
	}
	bundledVerification, err := VerifyKeyProof(decodedProofFromBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bundledVerification.Valid || string(bundledVerification.Value) != "11" {
		t.Fatalf("unexpected bundled proof verification: %#v", bundledVerification)
	}
	absentProof, err := engine.ProveKey(tree, []byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	verifiedAbsence, err := VerifyKeyProof(absentProof)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedAbsence.Valid || verifiedAbsence.Exists || !verifiedAbsence.Absence || verifiedAbsence.HasValue {
		t.Fatalf("unexpected verified absence: %#v", verifiedAbsence)
	}
	tamperedProof := proof
	tamperedProof.Root = cloneBytes(proof.Root)
	tamperedProof.Root[0] ^= 0xff
	tamperedVerification, err := VerifyKeyProof(tamperedProof)
	if err != nil {
		t.Fatal(err)
	}
	if tamperedVerification.Valid {
		t.Fatalf("expected tampered proof to be invalid: %#v", tamperedVerification)
	}
	tamperedProofBytes, err := KeyProofToBytes(tamperedProof)
	if err != nil {
		t.Fatal(err)
	}
	tamperedBundleVerification, err := VerifyProofBundle(tamperedProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	if tamperedBundleVerification.Valid {
		t.Fatalf("expected tampered proof bundle to be invalid: %#v", tamperedBundleVerification)
	}
	multiProof, err := engine.ProveKeys(tree, [][]byte{[]byte("a"), []byte("missing"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	multiVerified, err := VerifyMultiKeyProof(multiProof)
	if err != nil {
		t.Fatal(err)
	}
	if !multiVerified.Valid || len(multiVerified.Results) != 3 {
		t.Fatalf("unexpected multi-key proof verification: %#v", multiVerified)
	}
	if !multiVerified.Results[0].Exists || string(multiVerified.Results[0].Value) != "11" {
		t.Fatalf("unexpected multi proof value for a: %#v", multiVerified.Results[0])
	}
	if !multiVerified.Results[1].Absence || multiVerified.Results[1].HasValue {
		t.Fatalf("unexpected multi proof absence: %#v", multiVerified.Results[1])
	}
	if !multiVerified.Results[2].Exists || string(multiVerified.Results[2].Value) != "2" {
		t.Fatalf("unexpected multi proof value for b: %#v", multiVerified.Results[2])
	}
	multiPathBytes, err := MultiKeyProofPathNodeBytes(multiProof)
	if err != nil {
		t.Fatal(err)
	}
	decodedMultiProof, err := MultiKeyProofFromNodeBytes(multiProof.Root, multiProof.HasRoot, multiProof.Keys, multiPathBytes)
	if err != nil {
		t.Fatal(err)
	}
	decodedMultiVerification, err := VerifyMultiKeyProof(decodedMultiProof)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedMultiVerification.Valid || string(decodedMultiVerification.Results[2].Value) != "2" {
		t.Fatalf("unexpected decoded multi proof verification: %#v", decodedMultiVerification)
	}
	multiProofBytes, err := MultiKeyProofToBytes(multiProof)
	if err != nil {
		t.Fatal(err)
	}
	decodedMultiProofFromBytes, err := MultiKeyProofFromBytes(multiProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	bundledMultiVerification, err := VerifyMultiKeyProof(decodedMultiProofFromBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bundledMultiVerification.Valid || string(bundledMultiVerification.Results[2].Value) != "2" {
		t.Fatalf("unexpected bundled multi proof verification: %#v", bundledMultiVerification)
	}
	rangeProof, err := engine.ProveRange(tree, []byte("a"), []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	rangeVerified, err := VerifyRangeProof(rangeProof)
	if err != nil {
		t.Fatal(err)
	}
	if !rangeVerified.Valid || len(rangeVerified.Entries) != 2 || string(rangeVerified.Entries[1].Value) != "2" {
		t.Fatalf("unexpected range proof verification: %#v", rangeVerified)
	}
	rangePathBytes, err := RangeProofPathNodeBytes(rangeProof)
	if err != nil {
		t.Fatal(err)
	}
	decodedRangeProof, err := RangeProofFromNodeBytes(rangeProof.Root, rangeProof.HasRoot, rangeProof.Start, rangeProof.End, rangePathBytes)
	if err != nil {
		t.Fatal(err)
	}
	decodedRangeVerification, err := VerifyRangeProof(decodedRangeProof)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedRangeVerification.Valid || string(decodedRangeVerification.Entries[1].Value) != "2" {
		t.Fatalf("unexpected decoded range proof verification: %#v", decodedRangeVerification)
	}
	rangeProofBytes, err := RangeProofToBytes(rangeProof)
	if err != nil {
		t.Fatal(err)
	}
	decodedRangeProofFromBytes, err := RangeProofFromBytes(rangeProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	bundledRangeVerification, err := VerifyRangeProof(decodedRangeProofFromBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bundledRangeVerification.Valid || string(bundledRangeVerification.Entries[1].Value) != "2" {
		t.Fatalf("unexpected bundled range proof verification: %#v", bundledRangeVerification)
	}
	prefixProof, err := engine.ProvePrefix(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	prefixVerified, err := VerifyRangeProof(prefixProof)
	if err != nil {
		t.Fatal(err)
	}
	if !prefixVerified.Valid || len(prefixVerified.Entries) != 1 || string(prefixVerified.Entries[0].Value) != "11" {
		t.Fatalf("unexpected prefix proof verification: %#v", prefixVerified)
	}

	provedPage, err := engine.ProveRangePage(tree, &RangeCursor{AfterKey: []byte("a")}, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(provedPage.Page.Entries) != 1 || string(provedPage.Page.Entries[0].Key) != "b" {
		t.Fatalf("unexpected proved range page: %#v", provedPage)
	}
	if provedPage.Page.NextCursor != nil {
		t.Fatalf("expected final proved range page without cursor: %#v", provedPage.Page.NextCursor)
	}
	if !provedPage.Proof.HasAfter || string(provedPage.Proof.After) != "a" || provedPage.Proof.HasEnd {
		t.Fatalf("unexpected proved range page proof bounds: %#v", provedPage.Proof)
	}
	pageVerified, err := VerifyRangePageProof(provedPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	if !pageVerified.Valid || len(pageVerified.Entries) != 1 || string(pageVerified.Entries[0].Value) != "2" {
		t.Fatalf("unexpected range page proof verification: %#v", pageVerified)
	}
	pagePathBytes, err := RangePageProofPathNodeBytes(provedPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	decodedPageProof, err := RangePageProofFromNodeBytes(
		provedPage.Proof.Root,
		provedPage.Proof.HasRoot,
		provedPage.Proof.After,
		provedPage.Proof.HasAfter,
		provedPage.Proof.End,
		provedPage.Proof.HasEnd,
		pagePathBytes,
	)
	if err != nil {
		t.Fatal(err)
	}
	decodedPageVerification, err := VerifyRangePageProof(decodedPageProof)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedPageVerification.Valid || string(decodedPageVerification.Entries[0].Key) != "b" {
		t.Fatalf("unexpected decoded range page proof verification: %#v", decodedPageVerification)
	}
	pageProofBytes, err := RangePageProofToBytes(provedPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	decodedPageProofFromBytes, err := RangePageProofFromBytes(pageProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	bundledPageVerification, err := VerifyRangePageProof(decodedPageProofFromBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bundledPageVerification.Valid || string(bundledPageVerification.Entries[0].Key) != "b" {
		t.Fatalf("unexpected bundled range page proof verification: %#v", bundledPageVerification)
	}

	other, err := engine.Delete(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	other, err = engine.Put(other, []byte("b"), []byte("22"))
	if err != nil {
		t.Fatal(err)
	}
	other, err = engine.Put(other, []byte("d"), []byte("4"))
	if err != nil {
		t.Fatal(err)
	}
	provedDiffPage, err := engine.ProveDiffPage(tree, other, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(provedDiffPage.Page.Diffs) != 1 || provedDiffPage.Page.Diffs[0].Kind != "removed" || string(provedDiffPage.Page.Diffs[0].Key) != "a" {
		t.Fatalf("unexpected proved diff page: %#v", provedDiffPage.Page)
	}
	if provedDiffPage.Page.NextCursor == nil || string(provedDiffPage.Page.NextCursor.AfterKey) != "a" {
		t.Fatalf("unexpected proved diff page cursor: %#v", provedDiffPage.Page.NextCursor)
	}
	if !provedDiffPage.Proof.Base.HasEnd || string(provedDiffPage.Proof.Base.End) != "b" {
		t.Fatalf("unexpected proved diff page proof bound: %#v", provedDiffPage.Proof.Base)
	}
	if !provedDiffPage.Proof.HasLookaheadBase || string(provedDiffPage.Proof.LookaheadBase.Key) != "b" {
		t.Fatalf("unexpected proved diff page lookahead: %#v", provedDiffPage.Proof.LookaheadBase)
	}
	diffPageVerified, err := VerifyDiffPageProof(provedDiffPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	if !diffPageVerified.Valid || !diffPageVerified.LookaheadValid || len(diffPageVerified.Diffs) != 1 || string(diffPageVerified.Diffs[0].Key) != "a" {
		t.Fatalf("unexpected diff page proof verification: %#v", diffPageVerified)
	}
	if !diffPageVerified.HasNextCursor || string(diffPageVerified.NextCursor.AfterKey) != "a" {
		t.Fatalf("unexpected diff page proof cursor: %#v", diffPageVerified.NextCursor)
	}
	diffPageProofBytes, err := DiffPageProofToBytes(provedDiffPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	diffPageProofBytesAgain, err := DiffPageProofToBytes(provedDiffPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(diffPageProofBytes, diffPageProofBytesAgain) {
		t.Fatal("diff page proof bytes should be deterministic")
	}
	diffPageSummary, err := InspectProofBundle(diffPageProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	otherRoot, hasOtherRoot, err := other.Root()
	if err != nil {
		t.Fatal(err)
	}
	if diffPageSummary.Kind != "diff_page" || !diffPageSummary.HasRoot || !bytes.Equal(diffPageSummary.Root, treeRoot) || !diffPageSummary.HasOtherRoot || !hasOtherRoot || !bytes.Equal(diffPageSummary.OtherRoot, otherRoot) || !diffPageSummary.HasLimit || diffPageSummary.Limit != 1 || !diffPageSummary.HasLookahead {
		t.Fatalf("unexpected diff page proof bundle summary: %#v", diffPageSummary)
	}
	diffPageBundleVerification, err := VerifyProofBundle(diffPageProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !diffPageBundleVerification.Valid || diffPageBundleVerification.Summary.Kind != "diff_page" || diffPageBundleVerification.DiffCount != 1 || !diffPageBundleVerification.HasNextCursor || string(diffPageBundleVerification.NextCursor.AfterKey) != "a" {
		t.Fatalf("unexpected diff page proof bundle verification: %#v", diffPageBundleVerification)
	}
	decodedDiffPageProof, err := DiffPageProofFromBytes(diffPageProofBytes)
	if err != nil {
		t.Fatal(err)
	}
	decodedDiffPageVerification, err := VerifyDiffPageProof(decodedDiffPageProof)
	if err != nil {
		t.Fatal(err)
	}
	if !decodedDiffPageVerification.Valid || len(decodedDiffPageVerification.Diffs) != 1 || string(decodedDiffPageVerification.Diffs[0].Key) != "a" {
		t.Fatalf("unexpected decoded diff page proof verification: %#v", decodedDiffPageVerification)
	}

	keyProofBundle, err := KeyProofToBytes(proof)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := uint64(1_700_000_000_000)
	expiresAt := uint64(1_700_000_100_000)
	signedEnvelope, err := SignProofBundleHmacSha256(
		keyProofBundle,
		[]byte("go-key"),
		[]byte("shared secret"),
		[]byte("tenant=t1"),
		&issuedAt,
		&expiresAt,
		[]byte("nonce-1"),
	)
	if err != nil {
		t.Fatal(err)
	}
	signedEnvelopeBytes, err := AuthenticatedProofEnvelopeToBytes(signedEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	signedEnvelopeBytesAgain, err := AuthenticatedProofEnvelopeToBytes(signedEnvelope)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(signedEnvelopeBytes, signedEnvelopeBytesAgain) {
		t.Fatal("authenticated proof envelope bytes should be deterministic")
	}
	decodedEnvelope, err := AuthenticatedProofEnvelopeFromBytes(signedEnvelopeBytes)
	if err != nil {
		t.Fatal(err)
	}
	now := uint64(1_700_000_050_000)
	envelopeVerified, err := VerifyAuthenticatedProofEnvelope(decodedEnvelope, []byte("shared secret"), &now)
	if err != nil {
		t.Fatal(err)
	}
	if !envelopeVerified.Valid || !envelopeVerified.SignatureValid || !bytes.Equal(envelopeVerified.KeyID, []byte("go-key")) || !bytes.Equal(envelopeVerified.Context, []byte("tenant=t1")) {
		t.Fatalf("unexpected authenticated proof envelope verification: %#v", envelopeVerified)
	}
	decodedSignedProof, err := KeyProofFromBytes(envelopeVerified.ProofBundle)
	if err != nil {
		t.Fatal(err)
	}
	decodedSignedProofVerified, err := VerifyKeyProof(decodedSignedProof)
	if err != nil {
		t.Fatal(err)
	}
	if string(decodedSignedProofVerified.Value) != "11" {
		t.Fatalf("unexpected authenticated proof payload: %#v", decodedSignedProofVerified)
	}
	authenticatedBundle, err := VerifyAuthenticatedProofBundle(signedEnvelopeBytes, []byte("shared secret"), &now)
	if err != nil {
		t.Fatal(err)
	}
	if !authenticatedBundle.Valid || !authenticatedBundle.Envelope.Valid || !authenticatedBundle.HasProof || authenticatedBundle.Proof.ExistsCount != 1 || authenticatedBundle.HasProofError {
		t.Fatalf("unexpected authenticated proof bundle verification: %#v", authenticatedBundle)
	}
	wrongEnvelope, err := VerifyAuthenticatedProofEnvelope(decodedEnvelope, []byte("wrong secret"), &now)
	if err != nil {
		t.Fatal(err)
	}
	if wrongEnvelope.Valid || wrongEnvelope.SignatureValid {
		t.Fatalf("expected wrong envelope secret to fail: %#v", wrongEnvelope)
	}
	wrongBundle, err := VerifyAuthenticatedProofBundle(signedEnvelopeBytes, []byte("wrong secret"), &now)
	if err != nil {
		t.Fatal(err)
	}
	if wrongBundle.Valid || wrongBundle.Envelope.Valid || wrongBundle.HasProof {
		t.Fatalf("expected wrong authenticated proof bundle secret to fail: %#v", wrongBundle)
	}

	built, err := engine.BuildFromEntries([]Entry{
		{Key: []byte("c"), Value: []byte("3")},
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
	})
	if err != nil {
		t.Fatal(err)
	}
	sortedBuilt, err := engine.BuildFromSortedEntries([]Entry{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("c"), Value: []byte("3")},
	})
	if err != nil {
		t.Fatal(err)
	}
	builtRoot, builtRootPresent, err := built.Root()
	if err != nil {
		t.Fatal(err)
	}
	sortedRoot, sortedRootPresent, err := sortedBuilt.Root()
	if err != nil {
		t.Fatal(err)
	}
	if builtRootPresent != sortedRootPresent || !bytes.Equal(builtRoot, sortedRoot) {
		t.Fatalf("bulk roots differ: built=%x/%v sorted=%x/%v", builtRoot, builtRootPresent, sortedRoot, sortedRootPresent)
	}
	if _, err := engine.BuildFromSortedEntries([]Entry{
		{Key: []byte("b"), Value: []byte("2")},
		{Key: []byte("a"), Value: []byte("1")},
	}); err == nil {
		t.Fatal("expected sorted builder to reject out-of-order keys")
	}
	batchStats, err := engine.BatchWithStats(empty, []Mutation{
		{Kind: "upsert", Key: []byte("b"), Value: []byte("2")},
		{Kind: "upsert", Key: []byte("a"), Value: []byte("1")},
		{Kind: "upsert", Key: []byte("a"), Value: []byte("11")},
	})
	if err != nil {
		t.Fatal(err)
	}
	batchValue, ok, err := engine.Get(batchStats.Tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(batchValue) != "11" {
		t.Fatalf("unexpected batch_with_stats value: ok=%v value=%q", ok, batchValue)
	}
	if batchStats.Stats.InputMutations != 3 || batchStats.Stats.EffectiveMutations != 2 || batchStats.Stats.PreprocessInputSorted {
		t.Fatalf("unexpected batch stats: %#v", batchStats.Stats)
	}
	defaultParallelConfig, err := DefaultParallelConfig()
	if err != nil {
		t.Fatal(err)
	}
	if defaultParallelConfig.ParallelismThreshold != 100 {
		t.Fatalf("unexpected default parallel config: %#v", defaultParallelConfig)
	}
	parallelTree, err := engine.ParallelBatch(empty, []Mutation{
		{Kind: "upsert", Key: []byte("p"), Value: []byte("parallel")},
		{Kind: "upsert", Key: []byte("q"), Value: []byte("go")},
	}, NewParallelConfig(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	parallelValue, ok, err := engine.Get(parallelTree, []byte("q"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(parallelValue) != "go" {
		t.Fatalf("unexpected parallel batch value: ok=%v value=%q", ok, parallelValue)
	}
	parallelStats, err := engine.ParallelBatchWithStats(empty, []Mutation{
		{Kind: "upsert", Key: []byte("r"), Value: []byte("route")},
		{Kind: "upsert", Key: []byte("s"), Value: []byte("stats")},
	}, NewParallelConfig(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if parallelStats.Stats.InputMutations != 2 || parallelStats.Stats.EffectiveMutations != 2 || parallelStats.Stats.WrittenNodes == 0 {
		t.Fatalf("unexpected parallel batch stats: %#v", parallelStats.Stats)
	}
	parallelStatsValue, ok, err := engine.Get(parallelStats.Tree, []byte("s"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(parallelStatsValue) != "stats" {
		t.Fatalf("unexpected parallel batch stats value: ok=%v value=%q", ok, parallelStatsValue)
	}
	appended, err := engine.AppendBatch(built, []Mutation{
		{Kind: "upsert", Key: []byte("d"), Value: []byte("4")},
		{Kind: "upsert", Key: []byte("e"), Value: []byte("5")},
		{Kind: "upsert", Key: []byte("d"), Value: []byte("44")},
	})
	if err != nil {
		t.Fatal(err)
	}
	appendedValue, ok, err := engine.Get(appended, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(appendedValue) != "44" {
		t.Fatalf("unexpected append value: ok=%v value=%q", ok, appendedValue)
	}
	appendedStats, err := engine.AppendBatchWithStats(built, []Mutation{
		{Kind: "upsert", Key: []byte("d"), Value: []byte("4")},
		{Kind: "upsert", Key: []byte("e"), Value: []byte("5")},
		{Kind: "upsert", Key: []byte("d"), Value: []byte("44")},
	})
	if err != nil {
		t.Fatal(err)
	}
	appendedStatsValue, ok, err := engine.Get(appendedStats.Tree, []byte("d"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(appendedStatsValue) != "44" {
		t.Fatalf("unexpected append_with_stats value: ok=%v value=%q", ok, appendedStatsValue)
	}
	if appendedStats.Stats.InputMutations != 3 ||
		appendedStats.Stats.EffectiveMutations != 2 ||
		appendedStats.Stats.PreprocessInputSorted ||
		!appendedStats.Stats.UsedAppendFastPath ||
		appendedStats.Stats.WrittenNodes == 0 {
		t.Fatalf("unexpected append stats: %#v", appendedStats.Stats)
	}

	firstPage, err := engine.RangePage(tree, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstPage.Entries) != 1 || string(firstPage.Entries[0].Key) != "a" {
		t.Fatalf("unexpected first range page: %#v", firstPage)
	}
	if firstPage.NextCursor == nil {
		t.Fatal("expected next cursor after first range page")
	}
	startCursor, err := RangeCursorStart()
	if err != nil {
		t.Fatal(err)
	}
	if startCursor.AfterKey != nil {
		t.Fatalf("unexpected start cursor: %#v", startCursor)
	}

	afterA, err := engine.RangeAfter(tree, []byte("a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(afterA) != 1 || string(afterA[0].Key) != "b" {
		t.Fatalf("unexpected range_after result: %#v", afterA)
	}
	afterACursor, err := RangeCursorAfterKey([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(afterACursor.AfterKey, []byte("a")) {
		t.Fatalf("unexpected after-key cursor: %#v", afterACursor)
	}
	fromCursor, err := engine.RangeFromCursor(tree, &afterACursor, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(fromCursor) != len(afterA) || string(fromCursor[0].Key) != string(afterA[0].Key) {
		t.Fatalf("range_from_cursor mismatch: after=%#v cursor=%#v", afterA, fromCursor)
	}
	prefixEntries, err := engine.Prefix(tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixEntries) != 1 || string(prefixEntries[0].Key) != "a" || string(prefixEntries[0].Value) != "11" {
		t.Fatalf("unexpected prefix entries: %#v", prefixEntries)
	}
	prefixPage, err := engine.PrefixPage(tree, []byte("a"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixPage.Entries) != 1 || string(prefixPage.Entries[0].Value) != "11" || prefixPage.NextCursor == nil {
		t.Fatalf("unexpected prefix page: %#v", prefixPage)
	}

	firstEntry, err := engine.FirstEntry(tree)
	if err != nil {
		t.Fatal(err)
	}
	lastEntry, err := engine.LastEntry(tree)
	if err != nil {
		t.Fatal(err)
	}
	lowerBound, err := engine.LowerBound(tree, []byte("aa"))
	if err != nil {
		t.Fatal(err)
	}
	upperBound, err := engine.UpperBound(tree, []byte("b"))
	if err != nil {
		t.Fatal(err)
	}
	if firstEntry == nil || string(firstEntry.Key) != "a" || string(firstEntry.Value) != "11" ||
		lastEntry == nil || string(lastEntry.Key) != "b" || string(lastEntry.Value) != "2" ||
		lowerBound == nil || string(lowerBound.Key) != "b" ||
		upperBound != nil {
		t.Fatalf("unexpected boundary entries: first=%#v last=%#v lower=%#v upper=%#v", firstEntry, lastEntry, lowerBound, upperBound)
	}

	window, err := engine.CursorWindow(tree, []byte("aa"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !window.HasPositionKey || string(window.PositionKey) != "a" ||
		!window.HasPositionValue || string(window.PositionValue) != "11" ||
		window.Found ||
		len(window.Entries) != 1 || string(window.Entries[0].Key) != "b" ||
		window.NextCursor == nil || string(window.NextCursor.AfterKey) != "b" {
		t.Fatalf("unexpected cursor window: %#v", window)
	}

	exactProbe, err := engine.CursorWindow(tree, []byte("a"), nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !exactProbe.Found || string(exactProbe.PositionKey) != "a" ||
		len(exactProbe.Entries) != 0 || exactProbe.NextCursor != nil {
		t.Fatalf("unexpected exact cursor probe: %#v", exactProbe)
	}

	secondPage, err := engine.RangePage(tree, firstPage.NextCursor, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Entries) != 1 || string(secondPage.Entries[0].Key) != "b" {
		t.Fatalf("unexpected second range page: %#v", secondPage)
	}
	if secondPage.NextCursor != nil {
		thirdPage, err := engine.RangePage(tree, secondPage.NextCursor, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(thirdPage.Entries) != 0 {
			t.Fatalf("unexpected third range page: %#v", thirdPage)
		}
		if thirdPage.NextCursor != nil {
			t.Fatalf("expected no next cursor after third range page: %#v", thirdPage.NextCursor)
		}
	}

	reverseEnd, err := ReverseCursorEnd()
	if err != nil {
		t.Fatal(err)
	}
	if reverseEnd.BeforeKey != nil {
		t.Fatalf("unexpected reverse end cursor: %#v", reverseEnd)
	}
	beforeCCursor, err := ReverseCursorBeforeKey([]byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(beforeCCursor.BeforeKey, []byte("c")) {
		t.Fatalf("unexpected before-key cursor: %#v", beforeCCursor)
	}
	reverseFirst, err := engine.ReversePage(built, nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(reverseFirst.Entries) != 2 ||
		string(reverseFirst.Entries[0].Key) != "c" ||
		string(reverseFirst.Entries[1].Key) != "b" ||
		reverseFirst.NextCursor == nil ||
		string(reverseFirst.NextCursor.BeforeKey) != "b" {
		t.Fatalf("unexpected first reverse page: %#v", reverseFirst)
	}
	reverseSecond, err := engine.ReversePage(built, reverseFirst.NextCursor, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(reverseSecond.Entries) != 1 ||
		string(reverseSecond.Entries[0].Key) != "a" ||
		reverseSecond.NextCursor != nil {
		t.Fatalf("unexpected second reverse page: %#v", reverseSecond)
	}
	reverseBounded, err := engine.ReversePage(built, nil, []byte("b"), 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(reverseBounded.Entries) != 2 ||
		string(reverseBounded.Entries[0].Key) != "c" ||
		string(reverseBounded.Entries[1].Key) != "b" {
		t.Fatalf("unexpected bounded reverse page: %#v", reverseBounded)
	}
	prefixReverse, err := engine.PrefixReversePage(built, []byte("b"), nil, 8)
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixReverse.Entries) != 1 ||
		string(prefixReverse.Entries[0].Key) != "b" ||
		prefixReverse.NextCursor != nil {
		t.Fatalf("unexpected prefix reverse page: %#v", prefixReverse)
	}

	changed, err := engine.Put(tree, []byte("b"), []byte("22"))
	if err != nil {
		t.Fatal(err)
	}
	diffPage, err := engine.DiffPage(tree, changed, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffPage.Diffs) != 1 || diffPage.Diffs[0].Kind != "changed" {
		t.Fatalf("unexpected diff page: %#v", diffPage)
	}
	if diffPage.NextCursor != nil {
		secondDiffPage, err := engine.DiffPage(tree, changed, diffPage.NextCursor, nil, 1)
		if err != nil {
			t.Fatal(err)
		}
		if len(secondDiffPage.Diffs) != 0 {
			t.Fatalf("unexpected second diff page: %#v", secondDiffPage)
		}
		if secondDiffPage.NextCursor != nil {
			t.Fatalf("expected no next diff cursor: %#v", secondDiffPage.NextCursor)
		}
	}

	changedForCursor, err := engine.Batch(built, []Mutation{
		{Kind: "upsert", Key: []byte("b"), Value: []byte("22")},
		{Kind: "upsert", Key: []byte("c"), Value: []byte("33")},
	})
	if err != nil {
		t.Fatal(err)
	}
	resumedDiffs, err := engine.DiffFromCursor(built, changedForCursor, &RangeCursor{AfterKey: []byte("a")}, []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	if len(resumedDiffs) != 1 || resumedDiffs[0].Kind != "changed" || string(resumedDiffs[0].Key) != "b" {
		t.Fatalf("unexpected diff_from_cursor result: %#v", resumedDiffs)
	}

	conflictBase, err := engine.Batch(empty, []Mutation{
		{Kind: "upsert", Key: []byte("a"), Value: []byte("base-a")},
		{Kind: "upsert", Key: []byte("b"), Value: []byte("base-b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictLeft, err := engine.Batch(conflictBase, []Mutation{
		{Kind: "upsert", Key: []byte("a"), Value: []byte("left-a")},
		{Kind: "upsert", Key: []byte("b"), Value: []byte("left-b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictRight, err := engine.Batch(conflictBase, []Mutation{
		{Kind: "upsert", Key: []byte("a"), Value: []byte("right-a")},
		{Kind: "upsert", Key: []byte("b"), Value: []byte("right-b")},
	})
	if err != nil {
		t.Fatal(err)
	}
	conflictPage, err := engine.ConflictPage(conflictBase, conflictLeft, conflictRight, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflictPage.Conflicts) != 1 {
		t.Fatalf("unexpected first conflict page: %#v", conflictPage)
	}
	firstConflict := conflictPage.Conflicts[0]
	if string(firstConflict.Key) != "a" ||
		!firstConflict.BasePresent || string(firstConflict.Base) != "base-a" ||
		!firstConflict.LeftPresent || string(firstConflict.Left) != "left-a" ||
		!firstConflict.RightPresent || string(firstConflict.Right) != "right-a" {
		t.Fatalf("unexpected first conflict: %#v", firstConflict)
	}
	if conflictPage.NextCursor == nil {
		t.Fatal("expected next cursor after first conflict page")
	}
	secondConflictPage, err := engine.ConflictPage(conflictBase, conflictLeft, conflictRight, conflictPage.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondConflictPage.Conflicts) != 1 {
		t.Fatalf("unexpected second conflict page: %#v", secondConflictPage)
	}
	if string(secondConflictPage.Conflicts[0].Key) != "b" {
		t.Fatalf("unexpected second conflict: %#v", secondConflictPage.Conflicts[0])
	}
	if secondConflictPage.NextCursor != nil {
		t.Fatalf("expected no next conflict cursor: %#v", secondConflictPage.NextCursor)
	}

	base, err := engine.Put(empty, []byte("k"), []byte("base"))
	if err != nil {
		t.Fatal(err)
	}
	left, err := engine.Put(base, []byte("k"), []byte("left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := engine.Put(base, []byte("k"), []byte("right"))
	if err != nil {
		t.Fatal(err)
	}
	explanation, err := engine.MergeExplain(base, left, right, "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Result == nil || explanation.HasError || !strings.Contains(explanation.TraceJSON, "events") {
		t.Fatalf("unexpected merge explanation: %#v", explanation)
	}
	foundResolverTrace := false
	for _, event := range explanation.Trace.Events {
		if event.Kind == "resolver_called" && event.Resolution == "value" {
			foundResolverTrace = true
			break
		}
	}
	if !foundResolverTrace {
		t.Fatalf("expected typed merge trace to include resolver-called value event: %#v", explanation.Trace)
	}
	merged, err := engine.Merge(base, left, right, "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := engine.Get(merged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "right" {
		t.Fatalf("unexpected merged value: ok=%v value=%q", ok, value)
	}
	mergedRange, err := engine.MergeRange(base, left, right, []byte("k"), nil, "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(mergedRange, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "right" {
		t.Fatalf("unexpected range merged value: ok=%v value=%q", ok, value)
	}
	mergedPrefix, err := engine.MergePrefix(base, left, right, []byte("k"), "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(mergedPrefix, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "right" {
		t.Fatalf("unexpected prefix merged value: ok=%v value=%q", ok, value)
	}
	preferRightCallback, err := engine.MergeWithResolver(base, left, right, ResolvePreferRight)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(preferRightCallback, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "right" {
		t.Fatalf("unexpected built-in resolver helper value: ok=%v value=%q", ok, value)
	}
	updateConflict := Conflict{
		Key:          []byte("k"),
		Base:         []byte("base"),
		BasePresent:  true,
		Left:         []byte("left"),
		LeftPresent:  true,
		Right:        []byte("right"),
		RightPresent: true,
	}
	if got := ResolvePreferLeft(updateConflict); got.Kind != "value" || string(got.Value) != "left" {
		t.Fatalf("unexpected prefer-left helper: %#v", got)
	}
	if got := ResolveDeleteWins(updateConflict); got.Kind != "unresolved" {
		t.Fatalf("expected delete-wins update/update conflict to remain unresolved: %#v", got)
	}
	deleteConflict := Conflict{
		Key:          []byte("k"),
		Base:         []byte("base"),
		BasePresent:  true,
		Right:        []byte("right"),
		RightPresent: true,
	}
	if got := ResolveDeleteWins(deleteConflict); got.Kind != "delete" {
		t.Fatalf("unexpected delete-wins helper: %#v", got)
	}
	if got := ResolveUpdateWins(deleteConflict); got.Kind != "value" || string(got.Value) != "right" {
		t.Fatalf("unexpected update-wins helper: %#v", got)
	}
	callbackMerged, err := engine.MergeWithResolver(base, left, right, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(callbackMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left|right" {
		t.Fatalf("unexpected callback merged value: ok=%v value=%q", ok, value)
	}
	callbackExplanation, err := engine.MergeExplainWithResolver(base, left, right, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	if callbackExplanation.Result == nil || callbackExplanation.HasError {
		t.Fatalf("unexpected callback merge explanation: %#v", callbackExplanation)
	}
	callbackRange, err := engine.MergeRangeWithResolver(base, left, right, []byte("k"), nil, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(callbackRange, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left|right" {
		t.Fatalf("unexpected callback range merge value: ok=%v value=%q", ok, value)
	}
	callbackPrefix, err := engine.MergePrefixWithResolver(base, left, right, []byte("k"), joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(callbackPrefix, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left|right" {
		t.Fatalf("unexpected callback prefix merge value: ok=%v value=%q", ok, value)
	}

	policyBase, err := engine.Batch(empty, []Mutation{
		{Kind: "upsert", Key: []byte("doc/title"), Value: []byte("base-title")},
		{Kind: "upsert", Key: []byte("k"), Value: []byte("base-k")},
	})
	if err != nil {
		t.Fatal(err)
	}
	policyLeft, err := engine.Batch(policyBase, []Mutation{
		{Kind: "upsert", Key: []byte("doc/title"), Value: []byte("left-title")},
		{Kind: "upsert", Key: []byte("k"), Value: []byte("left-k")},
	})
	if err != nil {
		t.Fatal(err)
	}
	policyRight, err := engine.Batch(policyBase, []Mutation{
		{Kind: "upsert", Key: []byte("doc/title"), Value: []byte("right-title")},
		{Kind: "upsert", Key: []byte("k"), Value: []byte("right-k")},
	})
	if err != nil {
		t.Fatal(err)
	}
	policy, err := NewMergePolicyRegistry()
	if err != nil {
		t.Fatal(err)
	}
	defer policy.Close()
	emptyPolicy, err := policy.IsEmpty()
	if err != nil {
		t.Fatal(err)
	}
	if !emptyPolicy {
		t.Fatal("expected new merge policy registry to be empty")
	}
	if err := policy.SetDefaultResolverName("prefer_left"); err != nil {
		t.Fatal(err)
	}
	if err := policy.PushPrefixResolver([]byte("doc/"), joinMergeResolver); err != nil {
		t.Fatal(err)
	}
	if err := policy.PushExactResolverName([]byte("k"), "prefer_right"); err != nil {
		t.Fatal(err)
	}
	policyLen, err := policy.Len()
	if err != nil {
		t.Fatal(err)
	}
	if policyLen != 2 {
		t.Fatalf("unexpected merge policy rule count %d", policyLen)
	}
	hasDefault, err := policy.HasDefault()
	if err != nil {
		t.Fatal(err)
	}
	if !hasDefault {
		t.Fatal("expected merge policy registry to have a default")
	}
	policyMerged, err := engine.MergeWithPolicy(policyBase, policyLeft, policyRight, policy)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(policyMerged, []byte("doc/title"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left-title|right-title" {
		t.Fatalf("unexpected policy merged doc value: ok=%v value=%q", ok, value)
	}
	value, ok, err = engine.Get(policyMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "right-k" {
		t.Fatalf("unexpected policy exact value: ok=%v value=%q", ok, value)
	}
	policyExplanation, err := engine.MergeExplainWithPolicy(policyBase, policyLeft, policyRight, policy)
	if err != nil {
		t.Fatal(err)
	}
	if policyExplanation.Result == nil || policyExplanation.HasError {
		t.Fatalf("unexpected policy merge explanation: %#v", policyExplanation)
	}
	policyRange, err := engine.MergeRangeWithPolicy(policyBase, policyLeft, policyRight, []byte("doc/"), []byte("doc0"), policy)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(policyRange, []byte("doc/title"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left-title|right-title" {
		t.Fatalf("unexpected policy range value: ok=%v value=%q", ok, value)
	}
	policyPrefix, err := engine.MergePrefixWithPolicy(policyBase, policyLeft, policyRight, []byte("doc/"), policy)
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err = engine.Get(policyPrefix, []byte("doc/title"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "left-title|right-title" {
		t.Fatalf("unexpected policy prefix value: ok=%v value=%q", ok, value)
	}

	if err := engine.PublishNamedRootAtMillis([]byte("main"), merged, 42); err != nil {
		t.Fatal(err)
	}
	loaded, err := engine.LoadNamedRoot([]byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded == nil {
		t.Fatal("expected named root to load")
	}
	roots, err := engine.ListNamedRoots()
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 1 {
		t.Fatalf("expected one named root, got %d", len(roots))
	}
	manifests, err := engine.ListNamedRootManifests()
	if err != nil {
		t.Fatal(err)
	}
	if len(manifests) != 1 || string(manifests[0].Name) != "main" {
		t.Fatalf("unexpected named root manifests: %#v", manifests)
	}
	if !bytes.Equal(manifests[0].Manifest.Tree.raw, merged.raw) {
		t.Fatalf("manifest tree does not match published tree")
	}
	if manifests[0].Manifest.CreatedAtMillis == nil || *manifests[0].Manifest.CreatedAtMillis != 42 {
		t.Fatalf("unexpected manifest created timestamp: %#v", manifests[0].Manifest.CreatedAtMillis)
	}
	if manifests[0].Manifest.UpdatedAtMillis == nil || *manifests[0].Manifest.UpdatedAtMillis != 42 {
		t.Fatalf("unexpected manifest updated timestamp: %#v", manifests[0].Manifest.UpdatedAtMillis)
	}
	selection, err := engine.LoadNamedRoots([][]byte{[]byte("main"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if len(selection.Roots) != 1 || len(selection.MissingNames) != 1 {
		t.Fatalf("unexpected named root selection: %#v", selection)
	}
	retainAll, err := RetainAllNamedRoots()
	if err != nil {
		t.Fatal(err)
	}
	if retainAll.Kind != "all" {
		t.Fatalf("unexpected retain-all policy: %#v", retainAll)
	}
	retainExact, err := RetainExactNamedRoots([][]byte{[]byte("main"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if retainExact.Kind != "exact" || len(retainExact.Names) != 2 {
		t.Fatalf("unexpected exact retention policy: %#v", retainExact)
	}
	retainPrefix, err := RetainNamedRootPrefix([]byte("ma"))
	if err != nil {
		t.Fatal(err)
	}
	if retainPrefix.Kind != "prefix" || !bytes.Equal(retainPrefix.Prefix, []byte("ma")) {
		t.Fatalf("unexpected prefix retention policy: %#v", retainPrefix)
	}
	retainNewest, err := RetainNewestNamedRoots([]byte("checkpoint/"), 2)
	if err != nil {
		t.Fatal(err)
	}
	if retainNewest.Kind != "newest_by_name" || retainNewest.Count == nil || *retainNewest.Count != 2 {
		t.Fatalf("unexpected newest retention policy: %#v", retainNewest)
	}
	retainUpdated, err := RetainNamedRootsUpdatedSince([]byte("checkpoint/"), 42)
	if err != nil {
		t.Fatal(err)
	}
	if retainUpdated.Kind != "updated_since" ||
		retainUpdated.MinUpdatedAtMillis == nil ||
		*retainUpdated.MinUpdatedAtMillis != 42 {
		t.Fatalf("unexpected updated-since retention policy: %#v", retainUpdated)
	}
	retained, err := engine.LoadRetainedNamedRoots(retainAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(retained.Roots) != 1 {
		t.Fatalf("unexpected retained roots: %#v", retained)
	}
	retainedPlan, err := engine.PlanStoreGCForRetention(retainAll)
	if err != nil {
		t.Fatal(err)
	}
	if retainedPlan.Reachability.LiveNodes == 0 {
		t.Fatalf("expected retained GC plan to trace live nodes: %#v", retainedPlan)
	}
	update, err := engine.CompareAndSwapNamedRoot([]byte("main"), &merged, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !update.Applied || update.Conflict {
		t.Fatalf("unexpected named root update: %#v", update)
	}
	loaded, err = engine.LoadNamedRoot([]byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loaded != nil {
		t.Fatal("expected named root to be deleted by CAS")
	}

	branch, err := SnapshotNamespaceBranch()
	if err != nil {
		t.Fatal(err)
	}
	tag, err := SnapshotNamespaceTag()
	if err != nil {
		t.Fatal(err)
	}
	custom, err := SnapshotNamespaceCustom([]byte("refs/custom/"))
	if err != nil {
		t.Fatal(err)
	}
	snapshotName, err := SnapshotRootName(branch, []byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if string(snapshotName) != "refs/heads/main" {
		t.Fatalf("unexpected branch snapshot root name %q", snapshotName)
	}
	snapshotID, ok, err := SnapshotIDFromName(branch, snapshotName)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(snapshotID) != "main" {
		t.Fatalf("unexpected snapshot id: ok=%v id=%q", ok, snapshotID)
	}
	customName, err := SnapshotRootName(custom, []byte("draft"))
	if err != nil {
		t.Fatal(err)
	}
	if string(customName) != "refs/custom/draft" {
		t.Fatalf("unexpected custom snapshot root name %q", customName)
	}

	if err := engine.PublishSnapshotAtMillis(branch, []byte("main"), merged, 77); err != nil {
		t.Fatal(err)
	}
	loadedSnapshot, err := engine.LoadSnapshot(branch, []byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loadedSnapshot == nil {
		t.Fatal("expected branch snapshot to load")
	}
	if err := engine.PublishSnapshot(tag, []byte("v1"), merged); err != nil {
		t.Fatal(err)
	}
	branchSnapshots, err := engine.ListSnapshots(branch)
	if err != nil {
		t.Fatal(err)
	}
	if len(branchSnapshots) != 1 || string(branchSnapshots[0].ID) != "main" || string(branchSnapshots[0].Name) != "refs/heads/main" {
		t.Fatalf("unexpected branch snapshots: %#v", branchSnapshots)
	}
	if branchSnapshots[0].UpdatedAtMillis == nil || *branchSnapshots[0].UpdatedAtMillis != 77 {
		t.Fatalf("unexpected branch snapshot timestamp: %#v", branchSnapshots[0].UpdatedAtMillis)
	}
	tagSnapshots, err := engine.ListSnapshots(tag)
	if err != nil {
		t.Fatal(err)
	}
	if len(tagSnapshots) != 1 || string(tagSnapshots[0].ID) != "v1" {
		t.Fatalf("unexpected tag snapshots: %#v", tagSnapshots)
	}
	snapshotSelection, err := engine.LoadSnapshots(branch, [][]byte{[]byte("main"), []byte("missing")})
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshotSelection.Snapshots) != 1 || len(snapshotSelection.MissingIDs) != 1 {
		t.Fatalf("unexpected snapshot selection: %#v", snapshotSelection)
	}
	conflict, err := engine.CompareAndSwapSnapshot(branch, []byte("main"), nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if conflict.Applied || !conflict.Conflict || conflict.Current == nil {
		t.Fatalf("unexpected snapshot CAS conflict: %#v", conflict)
	}
	snapshotUpdate, err := engine.CompareAndSwapSnapshotAtMillis(branch, []byte("main"), &merged, nil, 88)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshotUpdate.Applied || snapshotUpdate.Conflict {
		t.Fatalf("unexpected snapshot CAS update: %#v", snapshotUpdate)
	}
	loadedSnapshot, err = engine.LoadSnapshot(branch, []byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loadedSnapshot != nil {
		t.Fatal("expected snapshot to be deleted by CAS")
	}
}

func TestContextWrappers(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	ctx := context.Background()
	empty, err := engine.CreateContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tree, err := engine.PutContext(ctx, empty, []byte("a"), []byte("1"))
	if err != nil {
		t.Fatal(err)
	}
	value, found, err := engine.GetContext(ctx, tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(value) != "1" {
		t.Fatalf("unexpected context get result: found=%v value=%q", found, value)
	}
	proof, err := engine.ProveKeyContext(ctx, tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	verifiedProof, err := VerifyKeyProof(proof)
	if err != nil {
		t.Fatal(err)
	}
	if !verifiedProof.Valid || !verifiedProof.Exists || string(verifiedProof.Value) != "1" {
		t.Fatalf("unexpected context proof: %#v", verifiedProof)
	}
	page, err := engine.RangePageContext(ctx, tree, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 || string(page.Entries[0].Key) != "a" {
		t.Fatalf("unexpected context range page: %#v", page)
	}
	reversePage, err := engine.ReversePageContext(ctx, tree, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(reversePage.Entries) != 1 || string(reversePage.Entries[0].Key) != "a" {
		t.Fatalf("unexpected context reverse page: %#v", reversePage)
	}
	prefixEntries, err := engine.PrefixContext(ctx, tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixEntries) != 1 || string(prefixEntries[0].Key) != "a" {
		t.Fatalf("unexpected context prefix entries: %#v", prefixEntries)
	}
	prefixPage, err := engine.PrefixPageContext(ctx, tree, []byte("a"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixPage.Entries) != 1 || string(prefixPage.Entries[0].Key) != "a" {
		t.Fatalf("unexpected context prefix page: %#v", prefixPage)
	}
	prefixReversePage, err := engine.PrefixReversePageContext(ctx, tree, []byte("a"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(prefixReversePage.Entries) != 1 || string(prefixReversePage.Entries[0].Key) != "a" {
		t.Fatalf("unexpected context prefix reverse page: %#v", prefixReversePage)
	}
	firstEntry, err := engine.FirstEntryContext(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	lowerBound, err := engine.LowerBoundContext(ctx, tree, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	missingLowerBound, err := engine.LowerBoundContext(ctx, tree, []byte("aa"))
	if err != nil {
		t.Fatal(err)
	}
	if firstEntry == nil || string(firstEntry.Key) != "a" ||
		lowerBound == nil || string(lowerBound.Key) != "a" ||
		missingLowerBound != nil {
		t.Fatalf("unexpected context boundary entries: first=%#v lower=%#v missingLower=%#v", firstEntry, lowerBound, missingLowerBound)
	}
	window, err := engine.CursorWindowContext(ctx, tree, []byte("aa"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if !window.HasPositionKey || string(window.PositionKey) != "a" ||
		window.Found ||
		len(window.Entries) != 0 ||
		window.NextCursor != nil {
		t.Fatalf("unexpected context cursor window: %#v", window)
	}
	provedPage, err := engine.ProveRangePageContext(ctx, tree, nil, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	pageProofVerification, err := VerifyRangePageProof(provedPage.Proof)
	if err != nil {
		t.Fatal(err)
	}
	if !pageProofVerification.Valid || len(pageProofVerification.Entries) != 1 || string(pageProofVerification.Entries[0].Key) != "a" {
		t.Fatalf("unexpected context range page proof: %#v", pageProofVerification)
	}
	changed, err := engine.PutContext(ctx, tree, []byte("a"), []byte("11"))
	if err != nil {
		t.Fatal(err)
	}
	diffs, err := engine.RangeDiffContext(ctx, tree, changed, []byte("a"), nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != 1 || diffs[0].Kind != "changed" {
		t.Fatalf("unexpected context range diff: %#v", diffs)
	}
	parallelConfig, err := DefaultParallelConfig()
	if err != nil {
		t.Fatal(err)
	}
	parallelTree, err := engine.ParallelBatchContext(ctx, tree, []Mutation{
		{Kind: "upsert", Key: []byte("p"), Value: []byte("parallel")},
	}, parallelConfig)
	if err != nil {
		t.Fatal(err)
	}
	parallelValue, parallelFound, err := engine.GetContext(ctx, parallelTree, []byte("p"))
	if err != nil {
		t.Fatal(err)
	}
	if !parallelFound || string(parallelValue) != "parallel" {
		t.Fatalf("unexpected context parallel batch result: found=%v value=%q", parallelFound, parallelValue)
	}
	parallelStats, err := engine.ParallelBatchWithStatsContext(ctx, tree, []Mutation{
		{Kind: "upsert", Key: []byte("ps"), Value: []byte("parallel-stats")},
	}, parallelConfig)
	if err != nil {
		t.Fatal(err)
	}
	if parallelStats.Stats.InputMutations != 1 || parallelStats.Stats.WrittenNodes == 0 {
		t.Fatalf("unexpected context parallel batch stats: %#v", parallelStats.Stats)
	}
	conflictPage, err := engine.ConflictPageContext(ctx, tree, changed, tree, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(conflictPage.Conflicts) != 0 {
		t.Fatalf("expected no conflicts when one side matches base: %#v", conflictPage)
	}

	base, err := engine.PutContext(ctx, empty, []byte("k"), []byte("base"))
	if err != nil {
		t.Fatal(err)
	}
	left, err := engine.PutContext(ctx, base, []byte("k"), []byte("left"))
	if err != nil {
		t.Fatal(err)
	}
	right, err := engine.PutContext(ctx, base, []byte("k"), []byte("right"))
	if err != nil {
		t.Fatal(err)
	}
	merged, err := engine.MergeContext(ctx, base, left, right, "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	mergedValue, found, err := engine.GetContext(ctx, merged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(mergedValue) != "right" {
		t.Fatalf("unexpected context merge result: found=%v value=%q", found, mergedValue)
	}
	explanation, err := engine.MergeExplainContext(ctx, base, left, right, "prefer_right")
	if err != nil {
		t.Fatal(err)
	}
	if explanation.Result == nil || explanation.HasError {
		t.Fatalf("unexpected context merge explanation: %#v", explanation)
	}
	callbackMerged, err := engine.MergeWithResolverContext(ctx, base, left, right, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	callbackValue, found, err := engine.GetContext(ctx, callbackMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(callbackValue) != "left|right" {
		t.Fatalf("unexpected context callback merge result: found=%v value=%q", found, callbackValue)
	}
	callbackRange, err := engine.MergeRangeWithResolverContext(ctx, base, left, right, []byte("k"), nil, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	callbackValue, found, err = engine.GetContext(ctx, callbackRange, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(callbackValue) != "left|right" {
		t.Fatalf("unexpected context callback range merge result: found=%v value=%q", found, callbackValue)
	}
	callbackPrefix, err := engine.MergePrefixWithResolverContext(ctx, base, left, right, []byte("k"), joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	callbackValue, found, err = engine.GetContext(ctx, callbackPrefix, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(callbackValue) != "left|right" {
		t.Fatalf("unexpected context callback prefix merge result: found=%v value=%q", found, callbackValue)
	}
	callbackExplanation, err := engine.MergeExplainWithResolverContext(ctx, base, left, right, joinMergeResolver)
	if err != nil {
		t.Fatal(err)
	}
	if callbackExplanation.Result == nil || callbackExplanation.HasError {
		t.Fatalf("unexpected context callback merge explanation: %#v", callbackExplanation)
	}
	policy, err := NewMergePolicyRegistry()
	if err != nil {
		t.Fatal(err)
	}
	defer policy.Close()
	if err := policy.SetDefaultResolver(joinMergeResolver); err != nil {
		t.Fatal(err)
	}
	policyMerged, err := engine.MergeWithPolicyContext(ctx, base, left, right, policy)
	if err != nil {
		t.Fatal(err)
	}
	callbackValue, found, err = engine.GetContext(ctx, policyMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(callbackValue) != "left|right" {
		t.Fatalf("unexpected context policy merge result: found=%v value=%q", found, callbackValue)
	}
	crdtCallbackMerged, err := engine.CrdtMergeWithResolverContext(ctx, base, left, right, "update_wins", joinCrdtResolver)
	if err != nil {
		t.Fatal(err)
	}
	callbackValue, found, err = engine.GetContext(ctx, crdtCallbackMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(callbackValue) != "left|right" {
		t.Fatalf("unexpected context CRDT callback merge result: found=%v value=%q", found, callbackValue)
	}

	blobStore, err := MemoryBlobStore()
	if err != nil {
		t.Fatal(err)
	}
	defer blobStore.Close()
	blobRef, err := blobStore.PutBlobContext(ctx, []byte("direct"))
	if err != nil {
		t.Fatal(err)
	}
	blobBytes, blobFound, err := blobStore.GetBlobContext(ctx, blobRef)
	if err != nil {
		t.Fatal(err)
	}
	if !blobFound || string(blobBytes) != "direct" {
		t.Fatalf("unexpected context blob get: found=%v value=%q", blobFound, blobBytes)
	}
	if err := blobStore.DeleteBlobContext(ctx, blobRef); err != nil {
		t.Fatal(err)
	}
	largeValue := bytes.Repeat([]byte{42}, 32)
	largeTree, err := engine.PutLargeValueContext(ctx, blobStore, empty, []byte("big"), largeValue, LargeValueConfig{InlineThreshold: 8})
	if err != nil {
		t.Fatal(err)
	}
	valueRef, err := engine.GetValueRefContext(ctx, largeTree, []byte("big"))
	if err != nil {
		t.Fatal(err)
	}
	if valueRef == nil || valueRef.Kind != "blob" {
		t.Fatalf("unexpected context value ref: %#v", valueRef)
	}
	largeLoaded, largeFound, err := engine.GetLargeValueContext(ctx, blobStore, largeTree, []byte("big"))
	if err != nil {
		t.Fatal(err)
	}
	if !largeFound || !bytes.Equal(largeLoaded, largeValue) {
		t.Fatalf("unexpected context large value: found=%v len=%d", largeFound, len(largeLoaded))
	}
	blobPlan, err := engine.PlanBlobStoreGCContext(ctx, blobStore, []Tree{largeTree})
	if err != nil {
		t.Fatal(err)
	}
	if blobPlan.Reachability.LiveBlobCount != 1 {
		t.Fatalf("unexpected context blob GC plan: %#v", blobPlan)
	}

	if err := engine.PublishNamedRootContext(ctx, []byte("main"), merged); err != nil {
		t.Fatal(err)
	}
	loadedRoot, err := engine.LoadNamedRootContext(ctx, []byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loadedRoot == nil {
		t.Fatal("expected context named root to load")
	}
	rootManifests, err := engine.ListNamedRootManifestsContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rootManifests) != 1 || string(rootManifests[0].Name) != "main" {
		t.Fatalf("unexpected context named root manifests: %#v", rootManifests)
	}
	retainedPlan, err := engine.PlanStoreGCForRetentionContext(ctx, NamedRootRetention{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if retainedPlan.Reachability.LiveNodes == 0 {
		t.Fatalf("unexpected context retained GC plan: %#v", retainedPlan)
	}
	rootUpdate, err := engine.CompareAndSwapNamedRootAtMillisContext(ctx, []byte("main"), &merged, nil, 123)
	if err != nil {
		t.Fatal(err)
	}
	if !rootUpdate.Applied || rootUpdate.Conflict {
		t.Fatalf("unexpected context named-root CAS result: %#v", rootUpdate)
	}

	branch, err := SnapshotNamespaceBranch()
	if err != nil {
		t.Fatal(err)
	}
	if err := engine.PublishSnapshotContext(ctx, branch, []byte("main"), merged); err != nil {
		t.Fatal(err)
	}
	loadedSnapshot, err := engine.LoadSnapshotContext(ctx, branch, []byte("main"))
	if err != nil {
		t.Fatal(err)
	}
	if loadedSnapshot == nil {
		t.Fatal("expected context snapshot to load")
	}
	snapshotUpdate, err := engine.CompareAndSwapSnapshotAtMillisContext(ctx, branch, []byte("main"), &merged, nil, 456)
	if err != nil {
		t.Fatal(err)
	}
	if !snapshotUpdate.Applied || snapshotUpdate.Conflict {
		t.Fatalf("unexpected context snapshot CAS result: %#v", snapshotUpdate)
	}

	statsJSON, err := engine.CollectStatsJSONContext(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(statsJSON, `"num_nodes"`) {
		t.Fatalf("context stats JSON missing num_nodes: %s", statsJSON)
	}
	typedStats, err := engine.CollectStatsContext(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if typedStats.TotalKeyValuePairs == 0 || typedStats.NodesPerLevel["0"] == 0 {
		t.Fatalf("unexpected context typed stats: %#v", typedStats)
	}
	debugView, err := engine.DebugTreeContext(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if len(debugView.Levels) == 0 || len(debugView.Levels[0].Nodes) == 0 {
		t.Fatalf("unexpected context debug view: %#v", debugView)
	}
	pinned, err := engine.PinTreeRootContext(ctx, tree)
	if err != nil {
		t.Fatal(err)
	}
	if pinned == 0 {
		t.Fatal("expected context pin_tree_root to pin nodes")
	}
	if _, err := engine.CacheStatsContext(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := engine.UnpinAllCacheNodesContext(ctx); err != nil {
		t.Fatal(err)
	}
	reachable, err := engine.MarkReachableContext(ctx, []Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if reachable.LiveNodes == 0 {
		t.Fatalf("unexpected context reachability: %#v", reachable)
	}
	nodeCids, err := engine.ListNodeCidsContext(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodeCids) == 0 {
		t.Fatal("expected context list_node_cids to return nodes")
	}
	gcPlan, err := engine.PlanGCContext(ctx, []Tree{tree}, nodeCids)
	if err != nil {
		t.Fatal(err)
	}
	if gcPlan.CandidateNodes != uint64(len(nodeCids)) {
		t.Fatalf("unexpected context GC plan: %#v", gcPlan)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := engine.CreateContext(cancelled); err != context.Canceled {
		t.Fatalf("expected canceled context before FFI call, got %v", err)
	}
	if err := engine.PublishNamedRootContext(cancelled, []byte("cancelled"), tree); err != context.Canceled {
		t.Fatalf("expected canceled context before void FFI call, got %v", err)
	}
}

func TestOperationalApis(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	empty, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err := engine.Put(empty, []byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}

	stats, err := engine.CollectStatsJSON(tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stats, `"num_nodes"`) {
		t.Fatalf("stats JSON missing num_nodes: %s", stats)
	}
	typedStats, err := engine.CollectStats(tree)
	if err != nil {
		t.Fatal(err)
	}
	if typedStats.TotalKeyValuePairs != 1 || typedStats.NodesPerLevel["0"] == 0 {
		t.Fatalf("unexpected typed stats: %#v", typedStats)
	}
	diffStats, err := engine.StatsDiffJSON(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(diffStats, `"absolute"`) {
		t.Fatalf("stats diff JSON missing absolute: %s", diffStats)
	}
	typedDiffStats, err := engine.StatsDiff(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if typedDiffStats.Before.TotalKeyValuePairs != 0 ||
		typedDiffStats.After.TotalKeyValuePairs != 1 ||
		typedDiffStats.Absolute.TotalKeyValuePairsDiff != 1 {
		t.Fatalf("unexpected typed stats diff: %#v", typedDiffStats)
	}
	debugJSON, err := engine.DebugTreeJSON(tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(debugJSON, `"levels"`) {
		t.Fatalf("debug JSON missing levels: %s", debugJSON)
	}
	debugView, err := engine.DebugTree(tree)
	if err != nil {
		t.Fatal(err)
	}
	if len(debugView.Levels) == 0 || len(debugView.Levels[0].Nodes) == 0 {
		t.Fatalf("unexpected typed debug tree: %#v", debugView)
	}
	debugText, err := engine.DebugTreeText(tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(debugText, "level") {
		t.Fatalf("debug text missing level: %s", debugText)
	}
	compareJSON, err := engine.DebugCompareTreesJSON(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compareJSON, `"right_only_nodes"`) {
		t.Fatalf("debug comparison JSON missing right_only_nodes: %s", compareJSON)
	}
	typedCompare, err := engine.DebugCompareTrees(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if typedCompare.RightOnlyNodes == 0 {
		t.Fatalf("unexpected typed debug comparison: %#v", typedCompare)
	}
	compareText, err := engine.DebugCompareTreesText(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(compareText, "right_only") {
		t.Fatalf("debug comparison text missing right_only: %s", compareText)
	}

	pinnedPath, err := engine.PinTreePath(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if pinnedPath == 0 {
		t.Fatal("expected pin_tree_path to pin at least one node")
	}
	if _, err := engine.UnpinAllCacheNodes(); err != nil {
		t.Fatal(err)
	}
	pinnedRoot, err := engine.PinTreeRoot(tree)
	if err != nil {
		t.Fatal(err)
	}
	if pinnedRoot == 0 {
		t.Fatal("expected pin_tree_root to pin at least one node")
	}
	cacheStats, err := engine.CacheStats()
	if err != nil {
		t.Fatal(err)
	}
	if cacheStats.CachedNodes == 0 || cacheStats.PinnedNodes == 0 {
		t.Fatalf("expected cache stats to include cached and pinned nodes: %#v", cacheStats)
	}
	if _, err := engine.UnpinAllCacheNodes(); err != nil {
		t.Fatal(err)
	}
	if err := engine.ClearCache(); err != nil {
		t.Fatal(err)
	}

	metrics, err := engine.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.NodesWritten == 0 {
		t.Fatalf("expected written-node metrics after put: %#v", metrics)
	}
	if err := engine.ResetMetrics(); err != nil {
		t.Fatal(err)
	}
	metrics, err = engine.Metrics()
	if err != nil {
		t.Fatal(err)
	}
	if metrics.NodesWritten != 0 {
		t.Fatalf("expected reset metrics to clear nodes written: %#v", metrics)
	}

	published, err := engine.PublishPrefixPathHint(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if published {
		t.Fatal("memory engine should not persist prefix path hints")
	}
	hydrated, err := engine.HydratePrefixPathHint(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if hydrated {
		t.Fatal("memory engine should not hydrate prefix path hints")
	}
	rangeSpan, err := ChangedSpanRange([]byte("k"), []byte("l"))
	if err != nil {
		t.Fatal(err)
	}
	exactSpan, err := ChangedSpanFromKey([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(exactSpan.End, []byte("k\x00")) {
		t.Fatalf("exact changed span end=%x", exactSpan.End)
	}
	prefixSpan, err := ChangedSpanForPrefix([]byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(prefixSpan.Start, []byte("k")) || !bytes.Equal(prefixSpan.End, []byte("l")) {
		t.Fatalf("prefix changed span=%#v", prefixSpan)
	}
	publishedSpans, err := engine.PublishChangedSpansHint(empty, tree, []ChangedSpan{rangeSpan})
	if err != nil {
		t.Fatal(err)
	}
	if publishedSpans {
		t.Fatal("memory engine should not persist changed-span hints")
	}
	hint, err := engine.LoadChangedSpansHint(empty, tree)
	if err != nil {
		t.Fatal(err)
	}
	if hint != nil {
		t.Fatalf("memory engine should not load changed-span hints: %#v", hint)
	}

	structuralPage, err := engine.StructuralDiffPage(empty, tree, nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(structuralPage.Diffs) == 0 || structuralPage.Stats.EmittedDiffs == 0 {
		t.Fatalf("expected structural diff page to emit diffs: %#v", structuralPage)
	}
	cursorPage, err := engine.StructuralDiffPage(empty, tree, nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	if cursorPage.NextCursor == nil || !cursorPage.HasNextCursor {
		t.Fatalf("expected structural diff page to expose typed and JSON cursors: %#v", cursorPage)
	}
	resumedStructuralPage, err := engine.StructuralDiffPageWithCursor(empty, tree, cursorPage.NextCursor, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(resumedStructuralPage.Diffs) == 0 {
		t.Fatalf("expected typed structural diff cursor to resume: %#v", resumedStructuralPage)
	}

	reachability, err := engine.MarkReachable([]Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if reachability.LiveNodes == 0 || len(reachability.LiveCids) == 0 {
		t.Fatalf("expected reachable live nodes: %#v", reachability)
	}
	nodeCids, err := engine.ListNodeCids()
	if err != nil {
		t.Fatal(err)
	}
	if len(nodeCids) == 0 {
		t.Fatal("expected memory store to list node CIDs")
	}
	gcPlan, err := engine.PlanGC([]Tree{tree}, nodeCids)
	if err != nil {
		t.Fatal(err)
	}
	if gcPlan.CandidateNodes != uint64(len(nodeCids)) || gcPlan.ReclaimableNodes != 0 {
		t.Fatalf("unexpected GC plan for live candidates: %#v", gcPlan)
	}
	gcSweep, err := engine.SweepGC([]Tree{tree}, nodeCids)
	if err != nil {
		t.Fatal(err)
	}
	if gcSweep.DeletedNodes != 0 {
		t.Fatalf("expected no live nodes to be swept: %#v", gcSweep)
	}
	storePlan, err := engine.PlanStoreGC([]Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if storePlan.ReclaimableNodes != 0 {
		t.Fatalf("expected no reclaimable store nodes: %#v", storePlan)
	}
	storeSweep, err := engine.SweepStoreGC([]Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if storeSweep.DeletedNodes != 0 {
		t.Fatalf("expected no store GC deletions: %#v", storeSweep)
	}
	if err := engine.PublishNamedRootAtMillis([]byte("live"), tree, 100); err != nil {
		t.Fatal(err)
	}
	retainedPlan, err := engine.PlanStoreGCForRetention(NamedRootRetention{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if retainedPlan.ReclaimableNodes != 0 {
		t.Fatalf("expected no reclaimable retained store nodes: %#v", retainedPlan)
	}
	retainedSweep, err := engine.SweepStoreGCForRetention(NamedRootRetention{Kind: "all"})
	if err != nil {
		t.Fatal(err)
	}
	if retainedSweep.DeletedNodes != 0 {
		t.Fatalf("expected no retained store GC deletions: %#v", retainedSweep)
	}

	destination, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer destination.Close()
	missing, err := engine.PlanMissingNodes(tree, destination)
	if err != nil {
		t.Fatal(err)
	}
	if missing.MissingNodes == 0 {
		t.Fatalf("expected missing nodes before sync: %#v", missing)
	}
	copyResult, err := engine.CopyMissingNodes(tree, destination)
	if err != nil {
		t.Fatal(err)
	}
	if copyResult.CopiedNodes != missing.MissingNodes {
		t.Fatalf("copied nodes %d want %d", copyResult.CopiedNodes, missing.MissingNodes)
	}
	afterCopy, err := engine.PlanMissingNodes(tree, destination)
	if err != nil {
		t.Fatal(err)
	}
	if afterCopy.MissingNodes != 0 {
		t.Fatalf("expected no missing nodes after sync: %#v", afterCopy)
	}
	copiedValue, copiedOK, err := destination.Get(tree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !copiedOK || string(copiedValue) != "v" {
		t.Fatalf("destination get after sync returned ok=%v value=%q", copiedOK, copiedValue)
	}

	bundle, err := engine.ExportSnapshot(tree)
	if err != nil {
		t.Fatal(err)
	}
	if bundle.FormatVersion != 1 {
		t.Fatalf("snapshot format version = %d, want 1", bundle.FormatVersion)
	}
	if len(bundle.Nodes) == 0 {
		t.Fatal("expected exported snapshot bundle nodes")
	}
	bundleBytes, err := SnapshotBundleToBytes(bundle)
	if err != nil {
		t.Fatal(err)
	}
	bundleDigest, err := SnapshotBundleDigest(bundle)
	if err != nil {
		t.Fatal(err)
	}
	byteBundleDigest, err := SnapshotBundleDigestBytes(bundleBytes)
	if err != nil {
		t.Fatal(err)
	}
	expectedBundleDigest, err := CidFromBytes(bundleBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(bundleDigest, expectedBundleDigest) || !bytes.Equal(byteBundleDigest, bundleDigest) {
		t.Fatalf("snapshot bundle digest mismatch")
	}
	summary, err := SummarizeSnapshotBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if summary.FormatVersion != 1 || summary.NodeCount != uint64(len(bundle.Nodes)) || summary.ByteCount == 0 || !summary.HasRoot {
		t.Fatalf("snapshot bundle summary mismatch: %#v", summary)
	}
	byteSummary, err := SummarizeSnapshotBundleBytes(bundleBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(byteSummary, summary) {
		t.Fatalf("snapshot bundle byte summary mismatch: %#v != %#v", byteSummary, summary)
	}
	verification, err := VerifySnapshotBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if !verification.Valid || len(verification.MissingCids) != 0 || len(verification.ExtraCids) != 0 {
		t.Fatalf("snapshot bundle verification mismatch: %#v", verification)
	}
	byteVerification, err := VerifySnapshotBundleBytes(bundleBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !byteVerification.Valid || !reflect.DeepEqual(byteVerification.Summary, summary) {
		t.Fatalf("snapshot bundle byte verification mismatch: %#v", byteVerification)
	}
	incompleteBundle := bundle
	incompleteBundle.Nodes = append([]SnapshotBundleNode(nil), bundle.Nodes[:len(bundle.Nodes)-1]...)
	incompleteVerification, err := VerifySnapshotBundle(incompleteBundle)
	if err != nil {
		t.Fatal(err)
	}
	if incompleteVerification.Valid || len(incompleteVerification.MissingCids) == 0 {
		t.Fatalf("expected incomplete snapshot bundle to report missing CIDs: %#v", incompleteVerification)
	}
	decodedBundle, err := SnapshotBundleFromBytes(bundleBytes)
	if err != nil {
		t.Fatal(err)
	}
	if decodedBundle.FormatVersion != bundle.FormatVersion || len(decodedBundle.Nodes) != len(bundle.Nodes) {
		t.Fatalf("decoded snapshot bundle mismatch: %#v", decodedBundle)
	}
	importDestination, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer importDestination.Close()
	importedTree, err := importDestination.ImportSnapshot(decodedBundle)
	if err != nil {
		t.Fatal(err)
	}
	importedValue, importedOK, err := importDestination.Get(importedTree, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !importedOK || string(importedValue) != "v" {
		t.Fatalf("destination get after snapshot import returned ok=%v value=%q", importedOK, importedValue)
	}
}

func TestCrdtTombstoneAndMultiValueHelpers(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	empty, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	baseValue, err := TimestampedValueToBytes(TimestampedValue{Value: []byte("base"), Timestamp: 1})
	if err != nil {
		t.Fatal(err)
	}
	leftValue, err := TimestampedValueToBytes(TimestampedValue{Value: []byte("left"), Timestamp: 2})
	if err != nil {
		t.Fatal(err)
	}
	rightValue, err := TimestampedValueToBytes(TimestampedValue{Value: []byte("right"), Timestamp: 3})
	if err != nil {
		t.Fatal(err)
	}
	base, err := engine.Put(empty, []byte("k"), baseValue)
	if err != nil {
		t.Fatal(err)
	}
	left, err := engine.Put(base, []byte("k"), leftValue)
	if err != nil {
		t.Fatal(err)
	}
	right, err := engine.Put(base, []byte("k"), rightValue)
	if err != nil {
		t.Fatal(err)
	}

	lww, err := CrdtConfigLWW("update_wins")
	if err != nil {
		t.Fatal(err)
	}
	if lww.Strategy != "last_writer_wins" || lww.DeletePolicy != "update_wins" {
		t.Fatalf("unexpected LWW config: %#v", lww)
	}
	merged, err := engine.CrdtMerge(base, left, right, lww)
	if err != nil {
		t.Fatal(err)
	}
	mergedBytes, ok, err := engine.Get(merged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("merged CRDT value missing")
	}
	mergedValue, err := TimestampedValueFromBytes(mergedBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(mergedValue.Value, []byte("right")) || mergedValue.Timestamp != 3 {
		t.Fatalf("unexpected merged timestamped value: %#v", mergedValue)
	}

	callbackMerged, err := engine.CrdtMergeWithResolver(base, left, right, "update_wins", joinCrdtResolver)
	if err != nil {
		t.Fatal(err)
	}
	callbackBytes, ok, err := engine.Get(callbackMerged, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(callbackBytes) != string(leftValue)+"|"+string(rightValue) {
		t.Fatalf("unexpected CRDT callback merge result: ok=%v value=%q", ok, callbackBytes)
	}
	deleteLeft, err := engine.Delete(base, []byte("k"))
	if err != nil {
		t.Fatal(err)
	}
	updateRight, err := engine.Put(base, []byte("k"), []byte("right"))
	if err != nil {
		t.Fatal(err)
	}
	deleted, err := engine.CrdtMergeWithResolver(
		base,
		deleteLeft,
		updateRight,
		"update_wins",
		func(Conflict) CrdtResolution { return CrdtResolveDelete() },
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok, err := engine.Get(deleted, []byte("k")); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("expected CRDT callback delete resolution")
	}

	now, err := TimestampedValueNow([]byte("now"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(now.Value, []byte("now")) || now.Timestamp == 0 {
		t.Fatalf("unexpected timestamped now value: %#v", now)
	}

	multiConfig, err := CrdtConfigMultiValue("delete_wins")
	if err != nil {
		t.Fatal(err)
	}
	if multiConfig.Strategy != "multi_value" || multiConfig.DeletePolicy != "delete_wins" {
		t.Fatalf("unexpected multi-value config: %#v", multiConfig)
	}
	encodedSet, err := MultiValueSetToBytes([][]byte{[]byte("b"), []byte("a"), []byte("a")})
	if err != nil {
		t.Fatal(err)
	}
	decodedSet, err := MultiValueSetFromBytes(encodedSet)
	if err != nil {
		t.Fatal(err)
	}
	if len(decodedSet) != 2 || !bytes.Equal(decodedSet[0], []byte("a")) || !bytes.Equal(decodedSet[1], []byte("b")) {
		t.Fatalf("unexpected decoded multi-value set: %#v", decodedSet)
	}
	mergedSet, err := MultiValueSetMerge([][]byte{[]byte("b")}, [][]byte{[]byte("a"), []byte("b")})
	if err != nil {
		t.Fatal(err)
	}
	if len(mergedSet) != 2 || !bytes.Equal(mergedSet[0], []byte("a")) || !bytes.Equal(mergedSet[1], []byte("b")) {
		t.Fatalf("unexpected merged multi-value set: %#v", mergedSet)
	}

	tombstone := Tombstone{
		Actor:           []byte("actor"),
		TimestampMillis: 7,
		CausalMetadata:  []TombstoneMetadata{{Key: "clock", Value: []byte("7")}},
	}
	tombstoneBytes, err := TombstoneToBytes(tombstone)
	if err != nil {
		t.Fatal(err)
	}
	isTombstone, err := IsTombstoneValue(tombstoneBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !isTombstone {
		t.Fatal("expected tombstone bytes to be identified as tombstone")
	}
	decodedTombstone, err := TombstoneFromBytes(tombstoneBytes)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(decodedTombstone.Actor, tombstone.Actor) || decodedTombstone.TimestampMillis != tombstone.TimestampMillis || len(decodedTombstone.CausalMetadata) != 1 {
		t.Fatalf("unexpected decoded tombstone: %#v", decodedTombstone)
	}
	storedTombstone, err := TombstoneFromStoredBytes(tombstoneBytes)
	if err != nil {
		t.Fatal(err)
	}
	if storedTombstone == nil || storedTombstone.TimestampMillis != tombstone.TimestampMillis {
		t.Fatalf("unexpected stored tombstone: %#v", storedTombstone)
	}
	upsert, err := TombstoneUpsertMutation([]byte("deleted"), tombstone)
	if err != nil {
		t.Fatal(err)
	}
	if upsert.Kind != "upsert" || !bytes.Equal(upsert.Key, []byte("deleted")) || upsert.Value == nil {
		t.Fatalf("unexpected tombstone upsert mutation: %#v", upsert)
	}
	compaction, err := TombstoneCompactionMutation([]byte("deleted"), tombstoneBytes)
	if err != nil {
		t.Fatal(err)
	}
	if compaction == nil || compaction.Kind != "delete" || !bytes.Equal(compaction.Key, []byte("deleted")) || compaction.Value != nil {
		t.Fatalf("unexpected tombstone compaction mutation: %#v", compaction)
	}
}

func TestBlobStoreLargeValuesAndBlobGC(t *testing.T) {
	engine, err := OpenMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	blobStore, err := MemoryBlobStore()
	if err != nil {
		t.Fatal(err)
	}
	defer blobStore.Close()

	count, err := blobStore.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("new blob store count=%d want 0", count)
	}

	directRef, err := blobStore.PutBlob([]byte("direct"))
	if err != nil {
		t.Fatal(err)
	}
	if err := BlobRefValidateBytes(directRef, []byte("direct")); err != nil {
		t.Fatal(err)
	}
	if err := BlobRefValidateBytes(directRef, []byte("wrong")); err == nil {
		t.Fatal("expected blob ref validation to reject wrong bytes")
	}
	direct, ok, err := blobStore.GetBlob(directRef)
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(direct) != "direct" {
		t.Fatalf("direct blob get returned ok=%v value=%q", ok, direct)
	}
	if err := blobStore.DeleteBlob(directRef); err != nil {
		t.Fatal(err)
	}
	count, err = blobStore.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("blob store count after delete=%d want 0", count)
	}

	empty, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	largeValue := bytes.Repeat([]byte{42}, 64)
	tree, err := engine.PutLargeValue(blobStore, empty, []byte("big"), largeValue, NewLargeValueConfig(8))
	if err != nil {
		t.Fatal(err)
	}
	valueRef, err := engine.GetValueRef(tree, []byte("big"))
	if err != nil {
		t.Fatal(err)
	}
	if valueRef == nil || valueRef.Kind != "blob" || valueRef.Blob == nil {
		t.Fatalf("expected blob value ref, got %#v", valueRef)
	}
	loaded, ok, err := engine.GetLargeValue(blobStore, tree, []byte("big"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || !bytes.Equal(loaded, largeValue) {
		t.Fatalf("large value get returned ok=%v len=%d", ok, len(loaded))
	}

	reachable, err := engine.MarkReachableBlobs([]Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if reachable.LiveBlobCount != 1 || len(reachable.LiveBlobs) != 1 {
		t.Fatalf("unexpected reachable blobs: %#v", reachable)
	}
	livePlan, err := engine.PlanBlobGC(blobStore, []Tree{tree}, reachable.LiveBlobs)
	if err != nil {
		t.Fatal(err)
	}
	if livePlan.ReclaimableBlobCount != 0 {
		t.Fatalf("live blob should not be reclaimable: %#v", livePlan)
	}

	orphanRef, err := blobStore.PutBlob([]byte("orphan"))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := blobStore.ListBlobRefs()
	if err != nil {
		t.Fatal(err)
	}
	if len(refs) != 2 {
		t.Fatalf("blob refs after orphan=%d want 2", len(refs))
	}
	_ = orphanRef
	storePlan, err := engine.PlanBlobStoreGC(blobStore, []Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if storePlan.ReclaimableBlobCount != 1 {
		t.Fatalf("expected one orphan reclaimable blob: %#v", storePlan)
	}
	storeSweep, err := engine.SweepBlobStoreGC(blobStore, []Tree{tree})
	if err != nil {
		t.Fatal(err)
	}
	if storeSweep.DeletedBlobs != 1 {
		t.Fatalf("expected one orphan deletion: %#v", storeSweep)
	}
	count, err = blobStore.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("blob store count after orphan sweep=%d want 1", count)
	}

	withoutBig, err := engine.Delete(tree, []byte("big"))
	if err != nil {
		t.Fatal(err)
	}
	unreachablePlan, err := engine.PlanBlobStoreGC(blobStore, []Tree{withoutBig})
	if err != nil {
		t.Fatal(err)
	}
	if unreachablePlan.ReclaimableBlobCount != 1 {
		t.Fatalf("expected large blob to become reclaimable: %#v", unreachablePlan)
	}
	unreachableSweep, err := engine.SweepBlobStoreGC(blobStore, []Tree{withoutBig})
	if err != nil {
		t.Fatal(err)
	}
	if unreachableSweep.DeletedBlobs != 1 {
		t.Fatalf("expected large blob deletion: %#v", unreachableSweep)
	}
	count, err = blobStore.BlobCount()
	if err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("blob store count after full sweep=%d want 0", count)
	}
}

func TestConformanceNodeFixtures(t *testing.T) {
	fixtures := loadFixtures(t)
	for _, fixture := range fixtures.NodeFixtures {
		nodeBytes := mustHex(t, fixture.Bytes)
		roundTrip, err := NodeBytesRoundTrip(nodeBytes)
		if err != nil {
			t.Fatalf("%s: node round trip: %v", fixture.Name, err)
		}
		if !bytes.Equal(roundTrip, nodeBytes) {
			t.Fatalf("%s: node bytes changed", fixture.Name)
		}
		cid, err := NodeCidFromBytes(nodeBytes)
		if err != nil {
			t.Fatalf("%s: node cid: %v", fixture.Name, err)
		}
		assertHexBytes(t, fixture.Cid, cid)
		contentCid, err := CidFromBytes(nodeBytes)
		if err != nil {
			t.Fatalf("%s: cid from bytes: %v", fixture.Name, err)
		}
		assertHexBytes(t, fixture.Cid, contentCid)
	}
}

func TestConformanceBoundaryAndKeyFixtures(t *testing.T) {
	fixtures := loadFixtures(t)

	for _, fixture := range fixtures.BoundaryFixtures {
		config := configFromFixture(t, fixture.Config)
		actual, err := IsBoundary(config, fixture.Count, mustHex(t, fixture.Key), mustHex(t, fixture.Value))
		if err != nil {
			t.Fatalf("%s: boundary: %v", fixture.Name, err)
		}
		if actual != fixture.IsBoundary {
			t.Fatalf("%s: boundary=%v want %v", fixture.Name, actual, fixture.IsBoundary)
		}
	}

	for _, fixture := range fixtures.KeyFixtures.PrefixEnd {
		prefix := mustHex(t, fixture.Prefix)
		actual, ok, err := PrefixEnd(mustHex(t, fixture.Prefix))
		if err != nil {
			t.Fatalf("prefix_end(%s): %v", fixture.Prefix, err)
		}
		if fixture.End == nil {
			if ok {
				t.Fatalf("prefix_end(%s)=%x want none", fixture.Prefix, actual)
			}
		} else {
			if !ok {
				t.Fatalf("prefix_end(%s)=none want %s", fixture.Prefix, *fixture.End)
			}
			assertHexBytes(t, *fixture.End, actual)
		}
		bounds, err := PrefixRange(prefix)
		if err != nil {
			t.Fatalf("prefix_range(%s): %v", fixture.Prefix, err)
		}
		if !bytes.Equal(bounds.Start, prefix) {
			t.Fatalf("prefix_range(%s).start=%x want %x", fixture.Prefix, bounds.Start, prefix)
		}
		if fixture.End == nil {
			if bounds.End != nil {
				t.Fatalf("prefix_range(%s).end=%x want nil", fixture.Prefix, bounds.End)
			}
		} else {
			assertHexBytes(t, *fixture.End, bounds.End)
		}
	}

	for _, fixture := range fixtures.KeyFixtures.Numeric {
		switch fixture.Kind {
		case "u64":
			value, err := strconv.ParseUint(fixture.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := U64Key(value)
			if err != nil {
				t.Fatal(err)
			}
			assertHexBytes(t, fixture.Encoded, actual)
		case "u128":
			actual, err := U128Key(fixture.Value)
			if err != nil {
				t.Fatal(err)
			}
			assertHexBytes(t, fixture.Encoded, actual)
		case "i64":
			value, err := strconv.ParseInt(fixture.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := I64Key(value)
			if err != nil {
				t.Fatal(err)
			}
			assertHexBytes(t, fixture.Encoded, actual)
		case "i128":
			actual, err := I128Key(fixture.Value)
			if err != nil {
				t.Fatal(err)
			}
			assertHexBytes(t, fixture.Encoded, actual)
		case "timestamp_millis":
			value, err := strconv.ParseUint(fixture.Value, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			actual, err := TimestampMillisKey(value)
			if err != nil {
				t.Fatal(err)
			}
			assertHexBytes(t, fixture.Encoded, actual)
		}
	}

	for _, fixture := range fixtures.KeyFixtures.Segments {
		var encoded []byte
		segmentBytes := make([][]byte, 0, len(fixture.Segments))
		for _, segment := range fixture.Segments {
			bytes := mustHex(t, segment)
			segmentBytes = append(segmentBytes, bytes)
			part, err := EncodeSegment(bytes)
			if err != nil {
				t.Fatal(err)
			}
			encoded = append(encoded, part...)
		}
		assertHexBytes(t, fixture.Encoded, encoded)
		key, err := KeyFromSegments(segmentBytes)
		if err != nil {
			t.Fatal(err)
		}
		assertHexBytes(t, fixture.Encoded, key)
		prefix, err := KeyFromSegments(segmentBytes[:1])
		if err != nil {
			t.Fatal(err)
		}
		prefixed, err := KeyFromPrefixedSegments(prefix, segmentBytes[1:])
		if err != nil {
			t.Fatal(err)
		}
		assertHexBytes(t, fixture.Encoded, prefixed)
		segments, err := DecodeSegments(mustHex(t, fixture.Encoded))
		if err != nil {
			t.Fatal(err)
		}
		if len(segments) != len(fixture.Decoded) {
			t.Fatalf("decoded %d segments want %d", len(segments), len(fixture.Decoded))
		}
		for i := range segments {
			assertHexBytes(t, fixture.Decoded[i], segments[i])
		}
	}

	for _, fixture := range fixtures.KeyFixtures.Debug {
		actual, err := DebugKey(mustHex(t, fixture.Key))
		if err != nil {
			t.Fatal(err)
		}
		if actual != fixture.Debug {
			t.Fatalf("debug_key(%s)=%q want %q", fixture.Key, actual, fixture.Debug)
		}
	}
}

func TestConformanceTreeAndDiffFixtures(t *testing.T) {
	fixtures := loadFixtures(t)

	for _, fixture := range fixtures.TreeFixtures {
		config := configFromFixture(t, fixture.Config)
		engine, err := Memory(config)
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()

		tree := buildTree(t, engine, fixture.Entries)
		root, ok, err := tree.Root()
		if err != nil {
			t.Fatal(err)
		}
		if !ok {
			t.Fatalf("%s: built tree has no root", fixture.Name)
		}
		assertHexBytes(t, fixture.Root, root)

		for _, lookup := range fixture.Lookups {
			actual, ok, err := engine.Get(tree, mustHex(t, lookup.Key))
			if err != nil {
				t.Fatal(err)
			}
			if lookup.Value == nil {
				if ok {
					t.Fatalf("get(%s)=%x want none", lookup.Key, actual)
				}
			} else {
				if !ok {
					t.Fatalf("get(%s)=none want %s", lookup.Key, *lookup.Value)
				}
				assertHexBytes(t, *lookup.Value, actual)
			}
		}

		for _, rangeFixture := range fixture.Ranges {
			var end []byte
			if rangeFixture.End != nil {
				end = mustHex(t, *rangeFixture.End)
			}
			entries, err := engine.Range(tree, mustHex(t, rangeFixture.Start), end)
			if err != nil {
				t.Fatal(err)
			}
			assertEntries(t, rangeFixture.Entries, entries)
		}
	}

	diffFixture := fixtures.DiffFixtures[0]
	config := configFromFixture(t, diffFixture.Config)
	engine, err := Memory(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	base := buildTree(t, engine, []entryFixture{
		{Key: "61", Value: "31"},
		{Key: "62", Value: "32"},
		{Key: "63", Value: "33"},
	})
	other := buildTree(t, engine, []entryFixture{
		{Key: "61", Value: "31"},
		{Key: "62", Value: "3232"},
		{Key: "64", Value: "34"},
	})
	baseRoot, _, err := base.Root()
	if err != nil {
		t.Fatal(err)
	}
	assertHexBytes(t, diffFixture.BaseRoot, baseRoot)
	otherRoot, _, err := other.Root()
	if err != nil {
		t.Fatal(err)
	}
	assertHexBytes(t, diffFixture.OtherRoot, otherRoot)

	diffs, err := engine.Diff(base, other)
	if err != nil {
		t.Fatal(err)
	}
	if len(diffs) != len(diffFixture.Diffs) {
		t.Fatalf("diff count %d want %d", len(diffs), len(diffFixture.Diffs))
	}
	for i, expected := range diffFixture.Diffs {
		actual := diffs[i]
		if actual.Kind != expected.Kind {
			t.Fatalf("diff[%d].kind=%s want %s", i, actual.Kind, expected.Kind)
		}
		assertHexBytes(t, expected.Key, actual.Key)
		assertOptionalHexBytes(t, expected.Value, actual.Value)
		assertOptionalHexBytes(t, expected.Old, actual.OldValue)
		assertOptionalHexBytes(t, expected.New, actual.NewValue)
	}
}

func TestConformanceCodecFixtures(t *testing.T) {
	fixtures := loadFixtures(t)
	for _, fixture := range fixtures.ValueFixtures {
		valueBytes := mustHex(t, fixture.Bytes)
		actual, err := VersionedValueBytesRoundTrip(valueBytes)
		if err != nil {
			t.Fatal(err)
		}
		assertHexBytes(t, fixture.Bytes, actual)
		matches, err := VersionedValueBytesMatchesSchema(valueBytes, fixture.SchemaName, fixture.Version)
		if err != nil {
			t.Fatal(err)
		}
		if !matches {
			t.Fatalf("expected %s to match %s@%d", fixture.Bytes, fixture.SchemaName, fixture.Version)
		}
		matches, err = VersionedValueBytesMatchesSchema(valueBytes, fixture.SchemaName, fixture.Version+1)
		if err != nil {
			t.Fatal(err)
		}
		if matches {
			t.Fatalf("expected %s not to match %s@%d", fixture.Bytes, fixture.SchemaName, fixture.Version+1)
		}
		if err := VersionedValueBytesRequireSchema(valueBytes, fixture.SchemaName, fixture.Version); err != nil {
			t.Fatal(err)
		}
		if err := VersionedValueBytesRequireSchema(valueBytes, fixture.SchemaName, fixture.Version+1); err == nil {
			t.Fatalf("expected schema guard mismatch for %s", fixture.Bytes)
		}
	}
	for _, fixture := range fixtures.BlobFixtures {
		bytes := mustHex(t, fixture.Bytes)
		actual, err := ValueRefBytesRoundTrip(bytes)
		if err != nil {
			t.Fatal(err)
		}
		assertHexBytes(t, fixture.Bytes, actual)
		stored, err := ValueRefFromStoredBytes(bytes)
		if err != nil {
			t.Fatal(err)
		}
		if stored.Kind != fixture.Kind {
			t.Fatalf("unexpected stored value ref kind: got %q want %q", stored.Kind, fixture.Kind)
		}
		requiresEscape, err := ValueRefInlineRequiresEscape(bytes)
		if err != nil {
			t.Fatal(err)
		}
		if !requiresEscape {
			t.Fatalf("expected value ref envelope to require inline escaping: %s", fixture.Bytes)
		}
	}
	rawInline, err := ValueRefFromStoredBytes([]byte("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if rawInline.Kind != "inline" || string(rawInline.Value) != "plain" {
		t.Fatalf("unexpected raw inline value ref: %#v", rawInline)
	}
	requiresEscape, err := ValueRefInlineRequiresEscape([]byte("plain"))
	if err != nil {
		t.Fatal(err)
	}
	if requiresEscape {
		t.Fatal("plain inline bytes should not require escaping")
	}
	for _, fixture := range fixtures.ManifestFixtures {
		actual, err := RootManifestBytesRoundTrip(mustHex(t, fixture.Bytes))
		if err != nil {
			t.Fatal(err)
		}
		assertHexBytes(t, fixture.Bytes, actual)
	}
}

func buildTree(t *testing.T, engine *Engine, entries []entryFixture) Tree {
	t.Helper()
	tree, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		tree, err = engine.Put(tree, mustHex(t, entry.Key), mustHex(t, entry.Value))
		if err != nil {
			t.Fatal(err)
		}
	}
	return tree
}

func assertEntries(t *testing.T, expected []entryFixture, actual []Entry) {
	t.Helper()
	if len(actual) != len(expected) {
		t.Fatalf("entry count %d want %d", len(actual), len(expected))
	}
	for i := range expected {
		assertHexBytes(t, expected[i].Key, actual[i].Key)
		assertHexBytes(t, expected[i].Value, actual[i].Value)
	}
}

func TestConfigConstructors(t *testing.T) {
	encoding := EncodingCustom("postcard")
	if encoding.Kind != "custom" || encoding.CustomName == nil || *encoding.CustomName != "postcard" {
		t.Fatalf("unexpected custom encoding: %#v", encoding)
	}
	if EncodingRaw().Kind != "raw" || EncodingCBOR().Kind != "cbor" || EncodingJSON().Kind != "json" {
		t.Fatal("unexpected built-in encoding helper")
	}

	maxNodes := uint64(16)
	maxBytes := uint64(4096)
	config, err := TreeConfig(2, 64, 32, 7, encoding, &maxNodes, &maxBytes)
	if err != nil {
		t.Fatal(err)
	}
	engine, err := Memory(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	tree, err := engine.Create()
	if err != nil {
		t.Fatal(err)
	}
	tree, err = engine.Put(tree, []byte("cfg"), []byte("ok"))
	if err != nil {
		t.Fatal(err)
	}
	value, ok, err := engine.Get(tree, []byte("cfg"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok || string(value) != "ok" {
		t.Fatalf("unexpected config helper value: ok=%v value=%q", ok, value)
	}

	sequential := SequentialParallelConfig()
	if sequential.MaxThreads != 1 || sequential.ParallelismThreshold != ^uint64(0) {
		t.Fatalf("unexpected sequential parallel config: %#v", sequential)
	}
	if _, err := TreeConfig(2, 64, 32, 7, Encoding{Kind: "custom"}, nil, nil); err == nil {
		t.Fatal("expected custom encoding without a name to fail")
	}
}

func configFromFixture(t *testing.T, fixture configFixture) Config {
	t.Helper()
	customEncoding := ""
	if fixture.Encoding.CustomName != nil {
		customEncoding = *fixture.Encoding.CustomName
	}
	config, err := NewConfig(ConfigOptions{
		MinChunkSize:      fixture.MinChunkSize,
		MaxChunkSize:      fixture.MaxChunkSize,
		ChunkingFactor:    fixture.ChunkingFactor,
		HashSeed:          fixture.HashSeed,
		EncodingKind:      fixture.Encoding.Kind,
		CustomEncoding:    customEncoding,
		NodeCacheMaxNodes: fixture.NodeCacheMaxNodes,
		NodeCacheMaxBytes: fixture.NodeCacheMaxBytes,
	})
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func loadFixtures(t *testing.T) fixtureFile {
	t.Helper()
	path := filepath.Clean("../../conformance/prolly-fixtures.v1.json")
	bytes, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var fixtures fixtureFile
	if err := json.Unmarshal(bytes, &fixtures); err != nil {
		t.Fatal(err)
	}
	return fixtures
}

func mustHex(t *testing.T, value string) []byte {
	t.Helper()
	bytes, err := hex.DecodeString(value)
	if err != nil {
		t.Fatalf("decode hex %q: %v", value, err)
	}
	return bytes
}

func assertHexBytes(t *testing.T, expected string, actual []byte) {
	t.Helper()
	if !bytes.Equal(mustHex(t, expected), actual) {
		t.Fatalf("bytes=%x want %s", actual, expected)
	}
}

func assertOptionalHexBytes(t *testing.T, expected *string, actual []byte) {
	t.Helper()
	if expected == nil {
		if actual != nil {
			t.Fatalf("bytes=%x want nil", actual)
		}
		return
	}
	assertHexBytes(t, *expected, actual)
}
