package prolly

import "context"

// Future is the idiomatic Go equivalent of the Rust binding's async wrapper.
// Await may be called more than once. Cancelling an Await does not discard the
// eventual result, so another caller may still collect it.
type Future[T any] struct {
	done   chan struct{}
	result T
	err    error
}

func startFuture[T any](ctx context.Context, call func() (T, error)) *Future[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	future := &Future[T]{done: make(chan struct{})}
	go func() {
		defer close(future.done)
		if err := ctx.Err(); err != nil {
			future.err = err
			return
		}
		future.result, future.err = call()
	}()
	return future
}

func (f *Future[T]) Await(ctx context.Context) (T, error) {
	var zero T
	if f == nil {
		return zero, ErrNilFuture
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case <-f.done:
		return f.result, f.err
	}
}

// ErrNilFuture is returned when Await is called on a nil future.
var ErrNilFuture = errNilFuture{}

type errNilFuture struct{}

func (errNilFuture) Error() string { return "nil prolly future" }

type VersionedGetResult struct {
	Value []byte
	Found bool
}

func (m *VersionedMap) InitializeAsync(ctx context.Context) *Future[MapVersion] {
	return startFuture(ctx, m.Initialize)
}

func (m *VersionedMap) HeadAsync(ctx context.Context) *Future[*MapVersion] {
	return startFuture(ctx, m.Head)
}

func (m *VersionedMap) VersionAsync(ctx context.Context, id []byte) *Future[*MapVersion] {
	id = append([]byte(nil), id...)
	return startFuture(ctx, func() (*MapVersion, error) { return m.Version(id) })
}

func (m *VersionedMap) GetAsync(ctx context.Context, key []byte) *Future[VersionedGetResult] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (VersionedGetResult, error) {
		value, found, err := m.Get(key)
		return VersionedGetResult{Value: value, Found: found}, err
	})
}

func (m *VersionedMap) PutAsync(ctx context.Context, key, value []byte) *Future[MapVersion] {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	return startFuture(ctx, func() (MapVersion, error) { return m.Put(key, value) })
}

func (m *VersionedMap) ApplyAsync(ctx context.Context, mutations []Mutation) *Future[MapVersion] {
	mutations = cloneMutations(mutations)
	return startFuture(ctx, func() (MapVersion, error) { return m.Apply(mutations) })
}

func (m *VersionedMap) DeleteAsync(ctx context.Context, key []byte) *Future[MapVersion] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (MapVersion, error) { return m.Delete(key) })
}

func (m *VersionedMap) SnapshotAsync(ctx context.Context) *Future[*MapSnapshot] {
	return startFuture(ctx, m.Snapshot)
}

func (m *VersionedMap) SnapshotAtAsync(ctx context.Context, id []byte) *Future[*MapSnapshot] {
	id = append([]byte(nil), id...)
	return startFuture(ctx, func() (*MapSnapshot, error) { return m.SnapshotAt(id) })
}

func (m *VersionedMap) SubscribeAsync(ctx context.Context) *Future[*MapSubscription] {
	return startFuture(ctx, m.Subscribe)
}

func (m *VersionedMap) SubscribeFromAsync(ctx context.Context, lastSeen []byte) *Future[*MapSubscription] {
	lastSeen = append([]byte(nil), lastSeen...)
	return startFuture(ctx, func() (*MapSubscription, error) { return m.SubscribeFrom(lastSeen) })
}

func (s *MapSubscription) PollAsync(ctx context.Context) *Future[*MapChangeEvent] {
	return startFuture(ctx, s.Poll)
}

func (s *MapSnapshot) GetAsync(ctx context.Context, key []byte) *Future[VersionedGetResult] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (VersionedGetResult, error) {
		value, found, err := s.Get(key)
		return VersionedGetResult{Value: value, Found: found}, err
	})
}

func (s *MapSnapshot) GetManyAsync(ctx context.Context, keys [][]byte) *Future[[][]byte] {
	keys = cloneByteSlices(keys)
	return startFuture(ctx, func() ([][]byte, error) { return s.GetMany(keys) })
}

func (s *MapSnapshot) RangeAsync(ctx context.Context, start, end []byte) *Future[[]Entry] {
	start, end = append([]byte(nil), start...), append([]byte(nil), end...)
	return startFuture(ctx, func() ([]Entry, error) { return s.Range(start, end) })
}

func (s *MapSnapshot) PrefixAsync(ctx context.Context, prefix []byte) *Future[[]Entry] {
	prefix = append([]byte(nil), prefix...)
	return startFuture(ctx, func() ([]Entry, error) { return s.Prefix(prefix) })
}

func (s *MapSnapshot) RangePageAsync(ctx context.Context, cursor *RangeCursor, end []byte, limit uint64) *Future[RangePage] {
	cursor, end = cloneRangeCursor(cursor), append([]byte(nil), end...)
	return startFuture(ctx, func() (RangePage, error) { return s.RangePage(cursor, end, limit) })
}

func (s *MapSnapshot) PrefixPageAsync(ctx context.Context, prefix []byte, cursor *RangeCursor, limit uint64) *Future[RangePage] {
	prefix, cursor = append([]byte(nil), prefix...), cloneRangeCursor(cursor)
	return startFuture(ctx, func() (RangePage, error) { return s.PrefixPage(prefix, cursor, limit) })
}

func (s *MapSnapshot) ProveKeyAsync(ctx context.Context, key []byte) *Future[KeyProof] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (KeyProof, error) { return s.ProveKey(key) })
}

func (s *MapSnapshot) ProveKeysAsync(ctx context.Context, keys [][]byte) *Future[MultiKeyProof] {
	keys = cloneByteSlices(keys)
	return startFuture(ctx, func() (MultiKeyProof, error) { return s.ProveKeys(keys) })
}

func (s *MapSnapshot) ProveRangeAsync(ctx context.Context, start, end []byte) *Future[RangeProof] {
	start, end = append([]byte(nil), start...), append([]byte(nil), end...)
	return startFuture(ctx, func() (RangeProof, error) { return s.ProveRange(start, end) })
}

func (s *MapSnapshot) ProvePrefixAsync(ctx context.Context, prefix []byte) *Future[RangeProof] {
	prefix = append([]byte(nil), prefix...)
	return startFuture(ctx, func() (RangeProof, error) { return s.ProvePrefix(prefix) })
}

func (s *ReadSession) GetAsync(ctx context.Context, key []byte) *Future[VersionedGetResult] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (VersionedGetResult, error) {
		value, found, err := s.Get(key)
		return VersionedGetResult{Value: value, Found: found}, err
	})
}

type ReadSessionGetManyResult struct {
	Values  [][]byte
	Present []bool
}

func (s *ReadSession) GetManyAsync(ctx context.Context, keys [][]byte) *Future[ReadSessionGetManyResult] {
	keys = cloneByteSlices(keys)
	return startFuture(ctx, func() (ReadSessionGetManyResult, error) {
		values, present, err := s.GetMany(keys)
		return ReadSessionGetManyResult{Values: values, Present: present}, err
	})
}

type IndexedGetResult struct {
	Value []byte
	Found bool
}

func (m *IndexedMap) GetAsync(ctx context.Context, key []byte) *Future[IndexedGetResult] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (IndexedGetResult, error) {
		value, found, err := m.Get(key)
		return IndexedGetResult{Value: value, Found: found}, err
	})
}

func (m *IndexedMap) PutAsync(ctx context.Context, key, value []byte) *Future[IndexedVersion] {
	key = append([]byte(nil), key...)
	value = append([]byte(nil), value...)
	return startFuture(ctx, func() (IndexedVersion, error) { return m.Put(key, value) })
}

func (m *IndexedMap) DeleteAsync(ctx context.Context, key []byte) *Future[IndexedVersion] {
	key = append([]byte(nil), key...)
	return startFuture(ctx, func() (IndexedVersion, error) { return m.Delete(key) })
}

func (m *IndexedMap) EnsureIndexAsync(ctx context.Context, name []byte) *Future[IndexBuildResult] {
	name = append([]byte(nil), name...)
	return startFuture(ctx, func() (IndexBuildResult, error) { return m.EnsureIndex(name) })
}

func (m *IndexedMap) SnapshotAsync(ctx context.Context) *Future[*IndexedSnapshot] {
	return startFuture(ctx, m.Snapshot)
}

func (m *IndexedMap) SnapshotAtAsync(ctx context.Context, sourceVersion []byte) *Future[*IndexedSnapshot] {
	sourceVersion = append([]byte(nil), sourceVersion...)
	return startFuture(ctx, func() (*IndexedSnapshot, error) { return m.SnapshotAt(sourceVersion) })
}

func startProximitySearchFuture(
	ctx context.Context,
	request SearchRequest,
	call func(SearchRequest, *ProximityCancellationToken) (SearchResult, error),
) *Future[SearchResult] {
	request = cloneSearchRequest(request)
	return startFuture(ctx, func() (SearchResult, error) {
		token, err := NewProximityCancellationToken()
		if err != nil {
			return SearchResult{}, err
		}
		defer token.Close()
		return call(request, token)
	})
}

func (s *ProximitySession) SearchAsync(ctx context.Context, request SearchRequest) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return s.SearchCancellable(ctx, owned, nil, token)
	})
}

func (s *ProximitySession) SearchWithRuntimeAsync(
	ctx context.Context, request SearchRequest, searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return s.SearchCancellable(ctx, owned, searchRuntime, token)
	})
}

func (m *ProximityMap) SearchAsync(ctx context.Context, request SearchRequest) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return m.SearchCancellable(ctx, owned, nil, token)
	})
}

func (m *ProximityMap) SearchWithRuntimeAsync(
	ctx context.Context, request SearchRequest, searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return m.SearchCancellable(ctx, owned, searchRuntime, token)
	})
}

func (i *HNSWIndex) SearchAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return i.SearchCancellable(ctx, proximity, owned, nil, token)
	})
}

func (i *HNSWIndex) SearchWithRuntimeAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
	searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return i.SearchCancellable(ctx, proximity, owned, searchRuntime, token)
	})
}

func (i *ProductQuantizer) SearchAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return i.SearchCancellable(ctx, proximity, owned, nil, token)
	})
}

func (i *ProductQuantizer) SearchWithRuntimeAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
	searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return i.SearchCancellable(ctx, proximity, owned, searchRuntime, token)
	})
}

func (a *CompositeAccelerator) SearchAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return a.SearchCancellable(ctx, proximity, owned, nil, token)
	})
}

func (a *CompositeAccelerator) SearchWithRuntimeAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
	searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return a.SearchCancellable(ctx, proximity, owned, searchRuntime, token)
	})
}

func (a *AcceleratorCatalog) SearchAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return a.SearchCancellable(ctx, proximity, owned, nil, token)
	})
}

func (a *AcceleratorCatalog) SearchWithRuntimeAsync(
	ctx context.Context, proximity *ProximityMap, request SearchRequest,
	searchRuntime *ProximitySearchRuntime,
) *Future[SearchResult] {
	return startProximitySearchFuture(ctx, request, func(owned SearchRequest, token *ProximityCancellationToken) (SearchResult, error) {
		return a.SearchCancellable(ctx, proximity, owned, searchRuntime, token)
	})
}

func (e *Engine) BuildProximityAsync(ctx context.Context, dimensions uint32, records []ProximityRecord) *Future[*ProximityMap] {
	records = cloneProximityRecords(records)
	return startFuture(ctx, func() (*ProximityMap, error) { return e.BuildProximity(dimensions, records) })
}

func cloneProximityRecords(records []ProximityRecord) []ProximityRecord {
	result := make([]ProximityRecord, len(records))
	for index, record := range records {
		result[index] = ProximityRecord{
			Key:    append([]byte(nil), record.Key...),
			Vector: append([]float32(nil), record.Vector...),
			Value:  append([]byte(nil), record.Value...),
		}
	}
	return result
}
