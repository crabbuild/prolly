package prolly

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
)

func TestRetainedSearchRuntimeReusesValidatedContent(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	records := make([]ProximityRecord, 16)
	for index := range records {
		records[index] = ProximityRecord{
			Key: []byte(fmt.Sprintf("vector-%02d", index)), Vector: []float32{float32(index), 0},
			Value: []byte(fmt.Sprintf("value-%02d", index)),
		}
	}
	proximity, err := engine.BuildProximity(2, records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	runtime, err := engine.NewProximitySearchRuntime()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	request := ExactSearch([]float32{0, 0}, 3)

	cold, err := proximity.SearchWithRuntime(context.Background(), request, runtime)
	if err != nil {
		t.Fatal(err)
	}
	warm, err := proximity.SearchWithRuntime(context.Background(), request, runtime)
	if err != nil {
		t.Fatal(err)
	}
	if cold.Stats.PhysicalBytesRead == 0 || warm.Stats.PhysicalBytesRead != 0 {
		t.Fatalf("physical bytes cold=%d warm=%d", cold.Stats.PhysicalBytesRead, warm.Stats.PhysicalBytesRead)
	}
	stats, err := runtime.Stats()
	if err != nil || stats.PhysicalReads == 0 {
		t.Fatalf("runtime stats = %#v, %v", stats, err)
	}
	if err := runtime.Clear(); err != nil {
		t.Fatal(err)
	}
	recold, err := proximity.SearchWithRuntime(context.Background(), request, runtime)
	if err != nil || recold.Stats.PhysicalBytesRead == 0 {
		t.Fatalf("search after clear = %#v, %v", recold, err)
	}
}

func TestProximityFutureUsesNativeCooperativeCancellation(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	records := make([]ProximityRecord, 256)
	for index := range records {
		records[index] = ProximityRecord{
			Key:    []byte(fmt.Sprintf("vector-%04d", index)),
			Vector: []float32{float32(index), float32(index % 7)},
			Value:  []byte(fmt.Sprint(index)),
		}
	}
	proximity, err := engine.BuildProximity(2, records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	runtime, err := engine.NewProximitySearchRuntime()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(runtime.Close)
	cancellation, err := NewProximityCancellationToken()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancellation.Close)
	if err := cancellation.Cancel(); err != nil {
		t.Fatal(err)
	}
	result, err := proximity.SearchCancellable(
		context.Background(), ExactSearch([]float32{0, 0}, 10), runtime, cancellation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Completion != "cancelled" || len(result.Neighbors) != 0 {
		t.Fatalf("cancelled result = %#v", result)
	}
	session, err := proximity.Read()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	sessionResult, err := session.SearchCancellable(
		context.Background(), ExactSearch([]float32{0, 0}, 10), runtime, cancellation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if sessionResult.Completion != "cancelled" || len(sessionResult.Neighbors) != 0 {
		t.Fatalf("cancelled session result = %#v", sessionResult)
	}
}

func TestRichProximitySearchPreservesPolicyFilterStatsSessionAndProof(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	proximity, err := engine.BuildProximity(2, []ProximityRecord{
		{Key: []byte("a"), Vector: []float32{0, 0}, Value: []byte("alpha")},
		{Key: []byte("ab"), Vector: []float32{1, 0}, Value: []byte("alphabet")},
		{Key: []byte("b"), Vector: []float32{0.1, 0}, Value: []byte("beta")},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	maxNodes, maxBytes, maxDistances, maxFrontier := uint64(1_000), uint64(1_000_000), uint64(1_000), uint64(1_000)
	request := SearchRequest{
		Query:  []float32{0, 0},
		K:      3,
		Policy: SearchPolicyFixedBudget,
		Budget: SearchBudget{
			MaxNodes: &maxNodes, MaxCommittedBytes: &maxBytes,
			MaxDistanceEvaluations: &maxDistances, MaxFrontierEntries: &maxFrontier,
		},
		Filter:  PrefixFilter([]byte("a")),
		Kernel:  QueryKernelScalarDeterministic,
		Backend: SearchBackendAuto,
	}
	mapResult, err := proximity.Search(context.Background(), request)
	if err != nil || len(mapResult.Neighbors) != 2 {
		t.Fatalf("map search = %#v, %v", mapResult, err)
	}
	session, err := proximity.Read()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(session.Close)
	result, err := session.Search(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Neighbors) != 2 {
		t.Fatalf("filtered neighbors = %#v", result.Neighbors)
	}
	if got := [][]byte{result.Neighbors[0].Key, result.Neighbors[1].Key}; !bytes.Equal(got[0], []byte("a")) || !bytes.Equal(got[1], []byte("ab")) {
		t.Fatalf("filtered neighbors = %q", got)
	}
	if result.Stats.DistanceEvaluations == 0 || result.PlanFormatVersion == 0 {
		t.Fatalf("incomplete result metadata = %#v", result)
	}
	var scanned []string
	visited, err := proximity.ScanRecords(func(record ProximityRecord) bool {
		scanned = append(scanned, string(record.Key))
		return len(scanned) < 2
	})
	if err != nil || visited != 2 || fmt.Sprint(scanned) != "[a ab]" {
		t.Fatalf("map record scan = %v, %d, %v", scanned, visited, err)
	}
	var retained []string
	visited, err = session.ScanRecords(func(record ProximityRecord) bool {
		retained = append(retained, string(record.Key))
		return true
	})
	if err != nil || visited != 3 || fmt.Sprint(retained) != "[a ab b]" {
		t.Fatalf("session record scan = %v, %d, %v", retained, visited, err)
	}
	previousProcs := runtime.GOMAXPROCS(1)
	asyncRequest := cloneSearchRequest(request)
	future := session.SearchAsync(context.Background(), asyncRequest)
	asyncRequest.Filter.Prefix[0] = 'b'
	asyncResult, err := future.Await(context.Background())
	runtime.GOMAXPROCS(previousProcs)
	if err != nil || len(asyncResult.Neighbors) != 2 || !bytes.Equal(asyncResult.Neighbors[1].Key, []byte("ab")) {
		t.Fatalf("async search did not own filter inputs = %#v, %v", asyncResult, err)
	}
	proof, err := proximity.ProveSearch(request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proof.Close)
	descriptor, err := proximity.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	verified, err := proof.Verify(descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if len(verified.Result.Neighbors) != 2 || !bytes.Equal(verified.Result.Neighbors[1].Key, []byte("ab")) {
		t.Fatalf("verified neighbors = %#v", verified.Result.Neighbors)
	}
}

func TestHNSWAcceleratorLifecycleIsPortable(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	records := make([]ProximityRecord, 16)
	for index := range records {
		records[index] = ProximityRecord{
			Key:    []byte(fmt.Sprintf("vector-%02d", index)),
			Vector: []float32{float32(index), 0},
			Value:  []byte(fmt.Sprintf("value-%02d", index)),
		}
	}
	proximity, err := engine.BuildProximity(2, records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	hnswConfig, err := DefaultHNSWConfig()
	if err != nil {
		t.Fatal(err)
	}
	limits, err := DefaultHNSWBuildLimits()
	if err != nil {
		t.Fatal(err)
	}
	built, err := proximity.BuildHNSW(hnswConfig, limits)
	if err != nil {
		t.Fatal(err)
	}
	if built.Stats.Records != 16 {
		t.Fatalf("HNSW records = %d", built.Stats.Records)
	}
	index := built.Index
	manifest, err := index.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := proximity.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	source, err := index.SourceDescriptor()
	if err != nil || !bytes.Equal(source, descriptor) {
		t.Fatalf("HNSW source = %x, %v", source, err)
	}
	canonical, err := index.IsCanonical()
	if err != nil || !canonical {
		t.Fatalf("HNSW canonical = %v, %v", canonical, err)
	}
	request := ExactSearch([]float32{0, 0}, 3)
	request.Policy = SearchPolicyFixedBudget
	request.Backend = SearchBackendHNSW
	result, err := index.Search(context.Background(), proximity, request)
	if err != nil || result.Backend != "hnsw" || !bytes.Equal(result.Neighbors[0].Key, []byte("vector-00")) {
		t.Fatalf("HNSW search = %#v, %v", result, err)
	}
	cancellation, err := NewProximityCancellationToken()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cancellation.Close)
	if err := cancellation.Cancel(); err != nil {
		t.Fatal(err)
	}
	cancelled, err := index.SearchCancellable(
		context.Background(), proximity, request, nil, cancellation,
	)
	if err != nil || cancelled.Completion != "cancelled" || len(cancelled.Neighbors) != 0 {
		t.Fatalf("cancelled HNSW search = %#v, %v", cancelled, err)
	}
	proof, err := index.ProveSearch(proximity, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proof.Close)
	verified, err := proof.Verify(descriptor)
	if err != nil || verified.Result.Backend != "hnsw" {
		t.Fatalf("HNSW proof = %#v, %v", verified, err)
	}
	index.Close()
	loaded, err := proximity.LoadHNSW(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(loaded.Close)
	loadedManifest, err := loaded.Manifest()
	if err != nil || !bytes.Equal(loadedManifest, manifest) {
		t.Fatalf("loaded HNSW manifest = %x, %v", loadedManifest, err)
	}
}

func TestProductQuantizerLifecycleIsPortableAndBounded(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	records := make([]ProximityRecord, 16)
	for index := range records {
		records[index] = ProximityRecord{
			Key: []byte(fmt.Sprintf("pq-vector-%02d", index)),
			Vector: []float32{
				float32(index), float32(index % 3), float32(index % 5), float32(index % 7),
			},
			Value: []byte(fmt.Sprintf("pq-value-%02d", index)),
		}
	}
	proximity, err := engine.BuildProximity(4, records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proximity.Close)
	pqConfig := ProductQuantizationConfig{
		Subquantizers: 2, CentroidsPerSubquantizer: 4, TrainingIterations: 2,
		RerankMultiplier: 4, Seed: ^uint64(0), MaxTrainingVectors: 16,
	}
	limits, err := DefaultPQBuildLimits()
	if err != nil {
		t.Fatal(err)
	}
	built, err := proximity.BuildPQ(pqConfig, 2, limits)
	if err != nil {
		t.Fatal(err)
	}
	if built.Stats.EncodedVectors != 16 || built.Stats.TrainingVectors != 16 {
		t.Fatalf("PQ build stats = %#v", built.Stats)
	}
	index := built.Index
	manifest, err := index.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := proximity.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	source, err := index.SourceDescriptor()
	if err != nil || !bytes.Equal(source, descriptor) {
		t.Fatalf("PQ source = %x, %v", source, err)
	}
	actualConfig, err := index.Config()
	if err != nil || actualConfig != pqConfig {
		t.Fatalf("PQ config = %#v, %v", actualConfig, err)
	}
	quality, err := index.Quality()
	if err != nil || quality.MeanSquaredError < 0 || quality.MaximumSquaredError < 0 {
		t.Fatalf("PQ quality = %#v, %v", quality, err)
	}
	request := ExactSearch([]float32{0, 0, 0, 0}, 3)
	request.Policy = SearchPolicyFixedBudget
	request.Backend = SearchBackendProductQuantized
	result, err := index.Search(context.Background(), proximity, request)
	if err != nil || result.Backend != "product-quantized" || !bytes.Equal(result.Neighbors[0].Key, []byte("pq-vector-00")) {
		t.Fatalf("PQ search = %#v, %v", result, err)
	}
	proof, err := index.ProveSearch(proximity, request)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(proof.Close)
	verified, err := proof.Verify(descriptor)
	if err != nil || verified.Result.Backend != "product-quantized" {
		t.Fatalf("PQ proof = %#v, %v", verified, err)
	}
	index.Close()
	loaded, err := proximity.LoadPQ(manifest)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(loaded.Close)
	loadedManifest, err := loaded.Manifest()
	if err != nil || !bytes.Equal(loadedManifest, manifest) {
		t.Fatalf("loaded PQ manifest = %x, %v", loadedManifest, err)
	}
}

func TestCompositeAndCatalogLifecycleIsPortableAndBounded(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	records := make([]ProximityRecord, 16)
	for index := range records {
		records[index] = ProximityRecord{
			Key:    []byte(fmt.Sprintf("composite-vector-%02d", index)),
			Vector: []float32{float32(index), 0},
			Value:  []byte(fmt.Sprintf("composite-value-%02d", index)),
		}
	}
	baseMap, err := engine.BuildProximity(2, records)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(baseMap.Close)
	hnswConfig, err := DefaultHNSWConfig()
	if err != nil {
		t.Fatal(err)
	}
	hnswLimits, err := DefaultHNSWBuildLimits()
	if err != nil {
		t.Fatal(err)
	}
	baseBuild, err := baseMap.BuildHNSW(hnswConfig, hnswLimits)
	if err != nil {
		t.Fatal(err)
	}
	base := baseBuild.Index
	t.Cleanup(base.Close)
	current, _, err := baseMap.Mutate([]ProximityMutation{UpsertProximity(
		[]byte("composite-vector-00"), []float32{0.25, 0}, []byte("updated"),
	)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(current.Close)
	compositeConfig, err := DefaultCompositeAcceleratorConfig()
	if err != nil {
		t.Fatal(err)
	}
	limits, err := DefaultCompositeBuildLimits()
	if err != nil {
		t.Fatal(err)
	}
	built, err := current.BuildCompositeHNSW(baseMap, base, compositeConfig, limits)
	if err != nil {
		t.Fatal(err)
	}
	if built.Accelerator == nil || len(built.Reasons) != 0 || built.Stats.VectorUpdatedRecords != 1 {
		t.Fatalf("composite build = %#v", built)
	}
	composite := built.Accelerator
	currentDescriptor, err := current.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	baseDescriptor, err := baseMap.Descriptor()
	if err != nil {
		t.Fatal(err)
	}
	currentSource, err := composite.CurrentSourceDescriptor()
	if err != nil || !bytes.Equal(currentSource, currentDescriptor) {
		t.Fatalf("current source = %x, %v", currentSource, err)
	}
	baseSource, err := composite.BaseSourceDescriptor()
	if err != nil || !bytes.Equal(baseSource, baseDescriptor) {
		t.Fatalf("base source = %x, %v", baseSource, err)
	}
	baseKind, err := composite.BaseKind()
	if err != nil || baseKind != CompositeBaseHNSW {
		t.Fatalf("base kind = %v, %v", baseKind, err)
	}
	delta, err := composite.DeltaCount()
	if err != nil || delta != 1 {
		t.Fatalf("delta = %d, %v", delta, err)
	}
	shadow, err := composite.ShadowCount()
	if err != nil || shadow != 1 {
		t.Fatalf("shadow = %d, %v", shadow, err)
	}
	request := ExactSearch([]float32{0, 0}, 3)
	request.Policy = SearchPolicyFixedBudget
	request.Backend = SearchBackendComposite
	result, err := composite.Search(context.Background(), current, request)
	if err != nil || result.Backend != "composite" {
		t.Fatalf("composite search = %#v, %v", result, err)
	}
	proof, err := composite.ProveSearch(current, request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := proof.Verify(currentDescriptor)
	proof.Close()
	if err != nil || verified.Result.Backend != "composite" {
		t.Fatalf("composite proof = %#v, %v", verified, err)
	}
	manifest, err := composite.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := current.BuildAcceleratorCatalog(nil, nil, composite)
	if err != nil {
		t.Fatal(err)
	}
	entries, err := catalog.Entries()
	if err != nil || len(entries) != 1 || entries[0].Kind != CatalogComposite {
		t.Fatalf("catalog entries = %#v, %v", entries, err)
	}
	catalogResult, err := catalog.Search(context.Background(), current, request)
	if err != nil || catalogResult.Backend != "composite" {
		t.Fatalf("catalog search = %#v, %v", catalogResult, err)
	}
	catalogManifest, err := catalog.Manifest()
	if err != nil {
		t.Fatal(err)
	}
	catalog.Close()
	loadedCatalog, err := current.LoadAcceleratorCatalog(catalogManifest)
	if err != nil {
		t.Fatal(err)
	}
	loadedCatalogManifest, err := loadedCatalog.Manifest()
	loadedCatalog.Close()
	if err != nil || !bytes.Equal(loadedCatalogManifest, catalogManifest) {
		t.Fatalf("loaded catalog = %x, %v", loadedCatalogManifest, err)
	}
	composite.Close()
	loaded, err := current.LoadComposite(manifest)
	if err != nil {
		t.Fatal(err)
	}
	loadedManifest, err := loaded.Manifest()
	loaded.Close()
	if err != nil || !bytes.Equal(loadedManifest, manifest) {
		t.Fatalf("loaded composite = %x, %v", loadedManifest, err)
	}

	compositeConfig.MaxDeltaRecords = 0
	rebuild, err := DefaultCompositeRebuildOptions()
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := current.BuildOrRebuildCompositeHNSW(baseMap, base, compositeConfig, limits, rebuild)
	if err != nil {
		t.Fatal(err)
	}
	if rebuilt.Kind != CompositeHNSWRebuilt || rebuilt.HNSW == nil || len(rebuilt.Reasons) == 0 {
		t.Fatalf("composite rebuild = %#v", rebuilt)
	}
	rebuiltSource, err := rebuilt.HNSW.SourceDescriptor()
	rebuilt.HNSW.Close()
	if err != nil || !bytes.Equal(rebuiltSource, currentDescriptor) {
		t.Fatalf("rebuilt source = %x, %v", rebuiltSource, err)
	}
}

func TestVersionedBulkPublicationUsesNativePerformancePaths(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(engine.Close)
	versioned, err := engine.VersionedMap([]byte("bulk-publication"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(versioned.Close)
	initialized, err := versioned.InitializeSorted([]Entry{{Key: []byte("a"), Value: []byte("one")}, {Key: []byte("b"), Value: []byte("two")}})
	if err != nil || initialized.Current == nil {
		t.Fatalf("initialize sorted = %#v, %v", initialized, err)
	}
	if _, err := versioned.Append([]Mutation{UpsertMutation([]byte("c"), []byte("three"))}); err != nil {
		t.Fatal(err)
	}
	parallel, err := versioned.ParallelApply([]Mutation{
		UpsertMutation([]byte("b"), []byte("updated")), UpsertMutation([]byte("d"), []byte("four")),
	}, NewParallelConfig(1, 1))
	if err != nil {
		t.Fatal(err)
	}
	if parallel.Stats.InputMutations != 2 {
		t.Fatalf("parallel stats = %#v", parallel.Stats)
	}
	rebuilt, err := versioned.RebuildSortedIf(parallel.Version.ID, []Entry{{Key: []byte("x"), Value: []byte("nine")}, {Key: []byte("y"), Value: []byte("ten")}})
	if err != nil || rebuilt.Kind != MapUpdateApplied || rebuilt.Current == nil {
		t.Fatalf("sorted rebuild = %#v, %v", rebuilt, err)
	}
	iterRebuilt, err := versioned.RebuildFromEntriesIf(rebuilt.Current.ID, []Entry{{Key: []byte("q"), Value: []byte("queue")}, {Key: []byte("p"), Value: []byte("priority")}})
	if err != nil || iterRebuilt.Current == nil {
		t.Fatalf("iter rebuild = %#v, %v", iterRebuilt, err)
	}
	value, ok, err := versioned.Get([]byte("p"))
	if err != nil || !ok || !bytes.Equal(value, []byte("priority")) {
		t.Fatalf("rebuilt value = %q, %v, %v", value, ok, err)
	}
}

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

func TestPortableVersionedHistoryNavigationDiffAndRollback(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("history-navigation"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	if _, err = versioned.Initialize(); err != nil {
		t.Fatal(err)
	}
	if _, err = versioned.Put([]byte("a"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err = versioned.Put([]byte("ab"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	base, err := versioned.Put([]byte("b"), []byte("three"))
	if err != nil {
		t.Fatal(err)
	}
	target, err := versioned.Put([]byte("a"), []byte("updated"))
	if err != nil {
		t.Fatal(err)
	}

	assertEntryKeys := func(label string, rows []Entry, expected ...string) {
		t.Helper()
		if len(rows) != len(expected) {
			t.Fatalf("%s keys = %#v", label, rows)
		}
		for i, key := range expected {
			if !bytes.Equal(rows[i].Key, []byte(key)) {
				t.Fatalf("%s key[%d] = %q", label, i, rows[i].Key)
			}
		}
	}
	rows, err := versioned.Range([]byte("a"), []byte("c"))
	if err != nil {
		t.Fatal(err)
	}
	assertEntryKeys("range", rows, "a", "ab", "b")
	rows, err = versioned.Prefix([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	assertEntryKeys("prefix", rows, "a", "ab")
	rows, err = versioned.RangeAt(base.ID, []byte("a"), []byte("b"))
	if err != nil || len(rows) != 2 || !bytes.Equal(rows[0].Value, []byte("one")) {
		t.Fatalf("historical range = %#v, %v", rows, err)
	}
	rows, err = versioned.PrefixAt(base.ID, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	assertEntryKeys("historical prefix", rows, "a", "ab")
	page, err := versioned.RangePage(nil, nil, 2)
	if err != nil {
		t.Fatal(err)
	}
	assertEntryKeys("range page", page.Entries, "a", "ab")
	page, err = versioned.PrefixPage([]byte("a"), nil, 1)
	if err != nil {
		t.Fatal(err)
	}
	assertEntryKeys("prefix page", page.Entries, "a")
	page, err = versioned.PrefixPageAt(base.ID, []byte("a"), nil, 1)
	if err != nil || page.NextCursor == nil {
		t.Fatalf("historical page = %#v, %v", page, err)
	}
	assertEntryKeys("historical prefix page", page.Entries, "a")
	diffs, err := versioned.Diff(base.ID, target.ID)
	if err != nil || len(diffs) != 1 || !bytes.Equal(diffs[0].Key, []byte("a")) {
		t.Fatalf("diff = %#v, %v", diffs, err)
	}
	diffs, err = versioned.ChangesSince(base.ID)
	if err != nil || len(diffs) != 1 || !bytes.Equal(diffs[0].Key, []byte("a")) {
		t.Fatalf("changes = %#v, %v", diffs, err)
	}
	rolledBack, err := versioned.RollbackTo(base.ID)
	if err != nil {
		t.Fatal(err)
	}
	head, ok, err := versioned.HeadID()
	if err != nil || !ok || !bytes.Equal(head, rolledBack.ID) {
		t.Fatalf("rollback head = %x, %v, %v", head, ok, err)
	}
	value, ok, err := versioned.Get([]byte("a"))
	if err != nil || !ok || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("rollback value = %q, %v, %v", value, ok, err)
	}
	diffs, err = versioned.ChangesSince(base.ID)
	if err != nil || len(diffs) != 0 {
		t.Fatalf("rollback changes = %#v, %v", diffs, err)
	}
}

func TestPortableVersionedTimestampedWritesAndCompleteMaintenance(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("maintenance-complete"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	first, err := versioned.ApplyAtMillis([]Mutation{UpsertMutation([]byte("k"), []byte("one"))}, 1_000)
	if err != nil {
		t.Fatal(err)
	}
	secondUpdate, err := versioned.ApplyIfAtMillis(first.ID, []Mutation{UpsertMutation([]byte("k"), []byte("two"))}, 2_000)
	if err != nil || secondUpdate.Current == nil {
		t.Fatalf("second = %+v, %v", secondUpdate, err)
	}
	second := *secondUpdate.Current
	third, err := versioned.ApplyAtMillis([]Mutation{UpsertMutation([]byte("k"), []byte("three"))}, 3_000)
	if err != nil {
		t.Fatal(err)
	}
	if first.CreatedAtMillis == nil || *first.CreatedAtMillis != 1_000 {
		t.Fatalf("first timestamp = %v", first.CreatedAtMillis)
	}
	if second.CreatedAtMillis == nil || *second.CreatedAtMillis != 2_000 {
		t.Fatalf("second timestamp = %v", second.CreatedAtMillis)
	}
	policy, err := versioned.RetentionPolicy()
	if err != nil || policy.Kind != "prefix" {
		t.Fatalf("policy = %+v, %v", policy, err)
	}
	verification, err := versioned.VerifyCatalog()
	if err != nil || !bytes.Equal(verification.Head, third.ID) || verification.VersionCount != 3 {
		t.Fatalf("verification = %+v, %v", verification, err)
	}
	plan, err := versioned.PlanGC()
	if err != nil || plan.Reachability.LiveNodes == 0 || plan.CandidateNodes < plan.ReclaimableNodes {
		t.Fatalf("plan = %+v, %v", plan, err)
	}
	aged, err := versioned.KeepForAt(3_000, 1_500)
	if err != nil || !containsBytes(aged.Retained, second.ID) || !containsBytes(aged.Removed, first.ID) {
		t.Fatalf("aged = %+v, %v", aged, err)
	}
	explicit, err := versioned.KeepVersions([][]byte{second.ID})
	if err != nil || !containsBytes(explicit.Retained, third.ID) {
		t.Fatalf("explicit = %+v, %v", explicit, err)
	}
	pruned, err := versioned.PruneVersions(0)
	if err != nil || len(pruned.Retained) != 1 || !bytes.Equal(pruned.Retained[0], third.ID) || !containsBytes(pruned.Removed, second.ID) {
		t.Fatalf("pruned = %+v, %v", pruned, err)
	}
	kept, err := versioned.KeepFor(10_000)
	if err != nil || len(kept.Retained) == 0 {
		t.Fatalf("kept = %+v, %v", kept, err)
	}
	sweep, err := versioned.SweepGC()
	if err != nil {
		t.Fatal(err)
	}
	if sweep.DeletedNodes > sweep.Plan.CandidateNodes {
		t.Fatalf("sweep = %+v", sweep)
	}
}

func containsBytes(values [][]byte, expected []byte) bool {
	for _, value := range values {
		if bytes.Equal(value, expected) {
			return true
		}
	}
	return false
}

func TestPortableVersionedSubscriptionResumesAndPollsOwnedDiffs(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("subscription"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	initial, err := versioned.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	subscription, err := versioned.Subscribe()
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()
	lastSeen, ok, err := subscription.LastSeen()
	if err != nil || !ok || !bytes.Equal(lastSeen, initial.ID) {
		t.Fatalf("last seen = %x, %v, %v", lastSeen, ok, err)
	}
	if event, err := subscription.Poll(); err != nil || event != nil {
		t.Fatalf("initial poll = %+v, %v", event, err)
	}
	current, err := versioned.Put([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatal(err)
	}
	event, err := subscription.Poll()
	if err != nil || event == nil {
		t.Fatalf("poll = %+v, %v", event, err)
	}
	if !bytes.Equal(event.Previous, initial.ID) || !bytes.Equal(event.Current.ID, current.ID) || len(event.Diffs) != 1 || !bytes.Equal(event.Diffs[0].Key, []byte("k")) {
		t.Fatalf("event = %+v", event)
	}
}

func TestPortableMultiMapTransactionsAreAtomicAndReadStagedValues(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	tx, err := engine.BeginVersionedTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Put([]byte("a"), []byte("k"), []byte("one")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Put([]byte("b"), []byte("k"), []byte("two")); err != nil {
		t.Fatal(err)
	}
	value, ok, err := tx.Get([]byte("a"), []byte("k"))
	if err != nil || !ok || !bytes.Equal(value, []byte("one")) {
		t.Fatalf("staged get = %q, %v, %v", value, ok, err)
	}
	committed, err := tx.Commit()
	if err != nil || !committed.Applied || len(committed.Versions) != 2 {
		t.Fatalf("commit = %+v, %v", committed, err)
	}
	for mapID, expected := range map[string]string{"a": "one", "b": "two"} {
		managed, err := engine.VersionedMap([]byte(mapID))
		if err != nil {
			t.Fatal(err)
		}
		value, ok, err := managed.Get([]byte("k"))
		managed.Close()
		if err != nil || !ok || !bytes.Equal(value, []byte(expected)) {
			t.Fatalf("%s = %q, %v, %v", mapID, value, ok, err)
		}
	}
	rolledBack, err := engine.BeginVersionedTransaction()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = rolledBack.Put([]byte("a"), []byte("discard"), []byte("x")); err != nil {
		t.Fatal(err)
	}
	if err = rolledBack.Rollback(); err != nil {
		t.Fatal(err)
	}
	a, err := engine.VersionedMap([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	if _, ok, err = a.Get([]byte("discard")); err != nil || ok {
		t.Fatalf("rolled-back value exists: %v, %v", ok, err)
	}
}

func TestPortablePinnedMergesPageConflictsAndCASPublish(t *testing.T) {
	config, err := DefaultConfig()
	if err != nil {
		t.Fatal(err)
	}
	engine, err := NewMemoryEngine(config)
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()
	versioned, err := engine.VersionedMap([]byte("merge"))
	if err != nil {
		t.Fatal(err)
	}
	defer versioned.Close()
	base, err := versioned.Initialize()
	if err != nil {
		t.Fatal(err)
	}
	candidate, err := versioned.Put([]byte("k"), []byte("candidate"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = versioned.Put([]byte("k"), []byte("head")); err != nil {
		t.Fatal(err)
	}
	merge, err := versioned.PrepareMerge(base.ID, candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer merge.Close()
	page, err := merge.ConflictPage(nil, 1)
	if err != nil || len(page.Conflicts) != 1 || !bytes.Equal(page.Conflicts[0].Key, []byte("k")) {
		t.Fatalf("conflicts = %+v, %v", page, err)
	}
	published, err := merge.Publish("prefer_right")
	if err != nil || published.Current == nil || !bytes.Equal(published.Current.ID, candidate.ID) {
		t.Fatalf("publish = %+v, %v", published, err)
	}
	value, ok, err := versioned.Get([]byte("k"))
	if err != nil || !ok || !bytes.Equal(value, []byte("candidate")) {
		t.Fatalf("merged value = %q, %v, %v", value, ok, err)
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
	subscription, err := versioned.SubscribeAsync(context.Background()).Await(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer subscription.Close()

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
	head, err := versioned.HeadAsync(context.Background()).Await(context.Background())
	if err != nil || head == nil {
		t.Fatalf("async head = %#v, %v", head, err)
	}
	snapshot, err := versioned.SnapshotAtAsync(context.Background(), head.ID).Await(context.Background())
	if err != nil || snapshot == nil {
		t.Fatalf("async snapshot = %#v, %v", snapshot, err)
	}
	defer snapshot.Close()
	read, err := snapshot.GetAsync(context.Background(), []byte("original-key")).Await(context.Background())
	if err != nil || !read.Found || !bytes.Equal(read.Value, []byte("original-value")) {
		t.Fatalf("async snapshot get = %#v, %v", read, err)
	}
	session, err := snapshot.Read()
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	sessionRead, err := session.GetAsync(context.Background(), []byte("original-key")).Await(context.Background())
	if err != nil || !sessionRead.Found || !bytes.Equal(sessionRead.Value, []byte("original-value")) {
		t.Fatalf("async session get = %#v, %v", sessionRead, err)
	}
	if event, err := subscription.PollAsync(context.Background()).Await(context.Background()); err != nil || event == nil {
		t.Fatalf("async subscription poll = %#v, %v", event, err)
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
