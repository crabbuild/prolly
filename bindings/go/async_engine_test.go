package prolly

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type remoteStoreStub struct {
	descriptor func(context.Context) (StoreDescriptor, error)
}

func (s *remoteStoreStub) Descriptor(ctx context.Context) (StoreDescriptor, error) {
	if s.descriptor != nil {
		return s.descriptor(ctx)
	}
	return validRemoteStoreDescriptor(), nil
}

func (*remoteStoreStub) GetNode(context.Context, []byte) (OptionalBytes, error) {
	return MissingBytes(), nil
}
func (*remoteStoreStub) PutNode(context.Context, []byte, []byte) error { return nil }
func (*remoteStoreStub) DeleteNode(context.Context, []byte) error      { return nil }
func (*remoteStoreStub) BatchNodes(context.Context, []NodeMutation) error {
	return nil
}
func (s *remoteStoreStub) PublishNodes(ctx context.Context, publication NodePublication) error {
	return PublishNodesWithGeneralPath(ctx, s, publication)
}
func (*remoteStoreStub) BatchGetNodesOrdered(_ context.Context, keys [][]byte) ([]OptionalBytes, error) {
	return make([]OptionalBytes, len(keys)), nil
}
func (*remoteStoreStub) ListNodeCIDs(context.Context) ([][]byte, error) { return nil, nil }
func (*remoteStoreStub) GetHint(context.Context, []byte, []byte) (OptionalBytes, error) {
	return MissingBytes(), nil
}
func (*remoteStoreStub) PutHint(context.Context, []byte, []byte, []byte) error { return nil }
func (*remoteStoreStub) BatchPutNodesWithHint(context.Context, []NodeEntry, []byte, []byte, []byte) error {
	return nil
}
func (*remoteStoreStub) GetRootManifest(context.Context, []byte) (OptionalBytes, error) {
	return MissingBytes(), nil
}
func (*remoteStoreStub) PutRootManifest(context.Context, []byte, []byte) error { return nil }
func (*remoteStoreStub) DeleteRootManifest(context.Context, []byte) error      { return nil }
func (*remoteStoreStub) CompareAndSwapRootManifest(context.Context, []byte, OptionalBytes, OptionalBytes) (RootCASResult, error) {
	return RootCASResult{}, nil
}
func (*remoteStoreStub) ListRootManifests(context.Context) ([]NamedStoreRoot, error) {
	return nil, nil
}
func (*remoteStoreStub) CommitTransaction(context.Context, []NodeMutation, []RootCondition, []RootWrite) (StoreTransactionResult, error) {
	return StoreTransactionResult{Applied: true}, nil
}

func TestAsyncEngineCancellationReachesStore(t *testing.T) {
	cancelled := make(chan struct{})
	store := &remoteStoreStub{descriptor: func(ctx context.Context) (StoreDescriptor, error) {
		<-ctx.Done()
		close(cancelled)
		return StoreDescriptor{}, ctx.Err()
	}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := NewAsyncEngine(ctx, store, nil)
		done <- err
	}()
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("NewAsyncEngine error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("NewAsyncEngine did not observe context cancellation")
	}
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("store callback context was not cancelled")
	}
}

type memoryRemoteStore struct {
	remoteStoreStub
	mu           sync.Mutex
	nodes        map[string][]byte
	hints        map[string][]byte
	roots        map[string][]byte
	publications []NodePublication
}

func newMemoryRemoteStore() *memoryRemoteStore {
	return &memoryRemoteStore{
		nodes: map[string][]byte{},
		hints: map[string][]byte{},
		roots: map[string][]byte{},
	}
}

func (s *memoryRemoteStore) GetNode(_ context.Context, key []byte) (OptionalBytes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.nodes[string(key)]
	if !ok {
		return MissingBytes(), nil
	}
	return PresentBytes(value), nil
}

func (s *memoryRemoteStore) PutNode(_ context.Context, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[string(key)] = cloneRemoteBytes(value)
	return nil
}

func (s *memoryRemoteStore) DeleteNode(_ context.Context, key []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, string(key))
	return nil
}

func (s *memoryRemoteStore) BatchNodes(_ context.Context, mutations []NodeMutation) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	applyNodeMutations(s.nodes, mutations)
	return nil
}

func (s *memoryRemoteStore) PublishNodes(ctx context.Context, publication NodePublication) error {
	s.mu.Lock()
	s.publications = append(s.publications, cloneNodePublication(publication))
	s.mu.Unlock()
	return PublishNodesWithGeneralPath(ctx, s, publication)
}

func cloneNodePublication(publication NodePublication) NodePublication {
	cloned := NodePublication{Origin: publication.Origin, Nodes: make([]NodeEntry, len(publication.Nodes))}
	for index, node := range publication.Nodes {
		cloned.Nodes[index] = NodeEntry{Key: cloneRemoteBytes(node.Key), Value: cloneRemoteBytes(node.Value)}
	}
	if publication.Hint != nil {
		cloned.Hint = &NodePublicationHint{
			Namespace: cloneRemoteBytes(publication.Hint.Namespace),
			Key:       cloneRemoteBytes(publication.Hint.Key),
			Value:     cloneRemoteBytes(publication.Hint.Value),
		}
	}
	return cloned
}

func (s *memoryRemoteStore) BatchGetNodesOrdered(_ context.Context, keys [][]byte) ([]OptionalBytes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]OptionalBytes, len(keys))
	for index, key := range keys {
		if value, ok := s.nodes[string(key)]; ok {
			result[index] = PresentBytes(value)
		}
	}
	return result, nil
}

func (s *memoryRemoteStore) GetHint(_ context.Context, namespace, key []byte) (OptionalBytes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.hints[string(namespace)+"\x00"+string(key)]
	if !ok {
		return MissingBytes(), nil
	}
	return PresentBytes(value), nil
}

func (s *memoryRemoteStore) PutHint(_ context.Context, namespace, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hints[string(namespace)+"\x00"+string(key)] = cloneRemoteBytes(value)
	return nil
}

func (s *memoryRemoteStore) BatchPutNodesWithHint(_ context.Context, nodes []NodeEntry, namespace, key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, node := range nodes {
		s.nodes[string(node.Key)] = cloneRemoteBytes(node.Value)
	}
	s.hints[string(namespace)+"\x00"+string(key)] = cloneRemoteBytes(value)
	return nil
}

func (s *memoryRemoteStore) GetRootManifest(_ context.Context, name []byte) (OptionalBytes, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	value, ok := s.roots[string(name)]
	if !ok {
		return MissingBytes(), nil
	}
	return PresentBytes(value), nil
}

func (s *memoryRemoteStore) PutRootManifest(_ context.Context, name, manifest []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roots[string(name)] = cloneRemoteBytes(manifest)
	return nil
}

func (s *memoryRemoteStore) DeleteRootManifest(_ context.Context, name []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roots, string(name))
	return nil
}

func (s *memoryRemoteStore) CompareAndSwapRootManifest(_ context.Context, name []byte, expected, replacement OptionalBytes) (RootCASResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current, ok := s.roots[string(name)]
	if ok != expected.Present || ok && !bytes.Equal(current, expected.Value) {
		if ok {
			return RootCASResult{Current: PresentBytes(current)}, nil
		}
		return RootCASResult{Current: MissingBytes()}, nil
	}
	if replacement.Present {
		s.roots[string(name)] = cloneRemoteBytes(replacement.Value)
	} else {
		delete(s.roots, string(name))
	}
	return RootCASResult{Applied: true, Current: replacement.Clone()}, nil
}

func (s *memoryRemoteStore) ListRootManifests(context.Context) ([]NamedStoreRoot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]NamedStoreRoot, 0, len(s.roots))
	for name, manifest := range s.roots {
		result = append(result, NamedStoreRoot{Name: []byte(name), Manifest: cloneRemoteBytes(manifest)})
	}
	return result, nil
}

func (s *memoryRemoteStore) CommitTransaction(_ context.Context, nodes []NodeMutation, conditions []RootCondition, roots []RootWrite) (StoreTransactionResult, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, condition := range conditions {
		current, ok := s.roots[string(condition.Name)]
		if ok != condition.Expected.Present || ok && !bytes.Equal(current, condition.Expected.Value) {
			conflict := &StoreTransactionConflict{Name: cloneRemoteBytes(condition.Name), Expected: condition.Expected.Clone()}
			if ok {
				conflict.Current = PresentBytes(current)
			}
			return StoreTransactionResult{Conflict: conflict}, nil
		}
	}
	applyNodeMutations(s.nodes, nodes)
	for _, root := range roots {
		if root.Replacement.Present {
			s.roots[string(root.Name)] = cloneRemoteBytes(root.Replacement.Value)
		} else {
			delete(s.roots, string(root.Name))
		}
	}
	return StoreTransactionResult{Applied: true}, nil
}

func applyNodeMutations(nodes map[string][]byte, mutations []NodeMutation) {
	for _, mutation := range mutations {
		if mutation.Value.Present {
			nodes[string(mutation.Key)] = cloneRemoteBytes(mutation.Value.Value)
		} else {
			delete(nodes, string(mutation.Key))
		}
	}
}

func TestAsyncEngineRoundTrip(t *testing.T) {
	ctx := context.Background()
	store := newMemoryRemoteStore()
	engine, err := NewAsyncEngine(ctx, store, nil)
	if err != nil {
		t.Fatalf("NewAsyncEngine: %v", err)
	}
	defer engine.Close()

	tree, err := engine.Create()
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	tree, err = engine.Put(ctx, tree, []byte("answer"), []byte("42"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	store.mu.Lock()
	publicationCount := len(store.publications)
	if publicationCount != 1 {
		store.mu.Unlock()
		t.Fatalf("publication count = %d, want 1", publicationCount)
	}
	publication := cloneNodePublication(store.publications[0])
	store.mu.Unlock()
	if publication.Origin != PublicationOriginPointUpsert {
		t.Fatalf("publication origin = %d, want %d", publication.Origin, PublicationOriginPointUpsert)
	}
	if len(publication.Nodes) == 0 || publication.Hint == nil {
		t.Fatalf("publication = %#v, want nodes and rightmost hint", publication)
	}
	value, found, err := engine.Get(ctx, tree, []byte("answer"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if !found || !bytes.Equal(value, []byte("42")) {
		t.Fatalf("Get = %q, %v", value, found)
	}
	if err := engine.PublishNamedRoot(ctx, []byte("main"), tree); err != nil {
		t.Fatalf("PublishNamedRoot: %v", err)
	}
	loaded, err := engine.LoadNamedRoot(ctx, []byte("main"))
	if err != nil {
		t.Fatalf("LoadNamedRoot: %v", err)
	}
	loadedValue, found, err := engine.Get(ctx, *loaded, []byte("answer"))
	if err != nil || !found || !bytes.Equal(loadedValue, []byte("42")) {
		t.Fatalf("loaded Get = %q, %v, %v", loadedValue, found, err)
	}
	entries, err := engine.Range(ctx, *loaded, nil, nil)
	if err != nil || len(entries) != 1 || !bytes.Equal(entries[0].Key, []byte("answer")) {
		t.Fatalf("Range = %#v, %v", entries, err)
	}
	page, err := engine.RangePage(ctx, *loaded, nil, nil, 1)
	if err != nil || len(page.Entries) != 1 {
		t.Fatalf("RangePage = %#v, %v", page, err)
	}
	stats, err := engine.CollectStats(ctx, *loaded)
	if err != nil || stats.TotalKeyValuePairs != 1 {
		t.Fatalf("CollectStats = %#v, %v", stats, err)
	}
	roots, err := engine.ListNamedRoots(ctx)
	if err != nil || len(roots) != 1 || !bytes.Equal(roots[0].Name, []byte("main")) {
		t.Fatalf("ListNamedRoots = %#v, %v", roots, err)
	}
}

func TestAsyncTransactionCommitsOwnedOverlay(t *testing.T) {
	ctx := context.Background()
	engine, err := NewAsyncEngine(ctx, newMemoryRemoteStore(), nil)
	if err != nil {
		t.Fatalf("NewAsyncEngine: %v", err)
	}
	defer engine.Close()

	transaction, err := engine.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	defer transaction.Close()
	tree, err := transaction.Create(ctx)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	tree, err = transaction.Put(ctx, tree, []byte("tx"), []byte("committed"))
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
	if err := transaction.PublishNamedRoot(ctx, []byte("main"), tree); err != nil {
		t.Fatalf("PublishNamedRoot: %v", err)
	}
	update, err := transaction.Commit(ctx)
	if err != nil {
		t.Fatalf("Commit: %v", err)
	}
	if !update.Applied || update.Conflict {
		t.Fatalf("Commit update = %#v", update)
	}

	loaded, err := engine.LoadNamedRoot(ctx, []byte("main"))
	if err != nil {
		t.Fatalf("LoadNamedRoot: %v", err)
	}
	value, found, err := engine.Get(ctx, *loaded, []byte("tx"))
	if err != nil || !found || !bytes.Equal(value, []byte("committed")) {
		t.Fatalf("Get committed = %q, %v, %v", value, found, err)
	}
}
