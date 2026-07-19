package storetest

import (
	"bytes"
	"context"
	"sort"
	"sync"

	prolly "build.crab/prolly-go"
)

type FakeStore struct {
	mu           sync.RWMutex
	capabilities prolly.StoreCapabilities
	nodes        map[string][]byte
	hints        map[string][]byte
	roots        map[string][]byte
}

func AllCapabilities() prolly.StoreCapabilities {
	return prolly.StoreCapabilities{
		NativeBatchReads:   true,
		AtomicBatchWrites:  true,
		NodeScan:           true,
		Hints:              true,
		AtomicNodesAndHint: true,
		RootScan:           true,
		RootCompareAndSwap: true,
		Transactions:       true,
		ReadParallelism:    4,
	}
}

func NewFakeStore(capabilities prolly.StoreCapabilities) *FakeStore {
	return &FakeStore{
		capabilities: capabilities,
		nodes:        map[string][]byte{},
		hints:        map[string][]byte{},
		roots:        map[string][]byte{},
	}
}

func (s *FakeStore) Descriptor(ctx context.Context) (prolly.StoreDescriptor, error) {
	if err := contextError(ctx); err != nil {
		return prolly.StoreDescriptor{}, err
	}
	return prolly.StoreDescriptor{
		ProtocolMajor: prolly.StoreProtocolMajor,
		AdapterName:   "fake-v1",
		Provider:      "fake",
		SchemaVersion: 1,
		Capabilities:  s.capabilities,
	}, nil
}

func (s *FakeStore) GetNode(ctx context.Context, key []byte) (prolly.OptionalBytes, error) {
	if err := contextError(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return optional(s.nodes, key), nil
}

func (s *FakeStore) PutNode(ctx context.Context, key, value []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nodes[string(key)] = clone(value)
	return nil
}

func (s *FakeStore) DeleteNode(ctx context.Context, key []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.nodes, string(key))
	return nil
}

func (s *FakeStore) BatchNodes(ctx context.Context, mutations []prolly.NodeMutation) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	applyNodes(s.nodes, mutations)
	return nil
}

func (s *FakeStore) PublishNodes(ctx context.Context, publication prolly.NodePublication) error {
	return prolly.PublishNodesWithGeneralPath(ctx, s, publication)
}

func (s *FakeStore) BatchGetNodesOrdered(ctx context.Context, keys [][]byte) ([]prolly.OptionalBytes, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]prolly.OptionalBytes, len(keys))
	for index, key := range keys {
		result[index] = optional(s.nodes, key)
	}
	return result, nil
}

func (s *FakeStore) ListNodeCIDs(ctx context.Context) ([][]byte, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return sortedKeys(s.nodes), nil
}

func (s *FakeStore) GetHint(ctx context.Context, namespace, key []byte) (prolly.OptionalBytes, error) {
	if err := contextError(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return optional(s.hints, hintKey(namespace, key)), nil
}

func (s *FakeStore) PutHint(ctx context.Context, namespace, key, value []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hints[string(hintKey(namespace, key))] = clone(value)
	return nil
}

func (s *FakeStore) BatchPutNodesWithHint(ctx context.Context, nodes []prolly.NodeEntry, namespace, key, value []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, node := range nodes {
		s.nodes[string(node.Key)] = clone(node.Value)
	}
	s.hints[string(hintKey(namespace, key))] = clone(value)
	return nil
}

func (s *FakeStore) GetRootManifest(ctx context.Context, name []byte) (prolly.OptionalBytes, error) {
	if err := contextError(ctx); err != nil {
		return prolly.OptionalBytes{}, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return optional(s.roots, name), nil
}

func (s *FakeStore) PutRootManifest(ctx context.Context, name, manifest []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.roots[string(name)] = clone(manifest)
	return nil
}

func (s *FakeStore) DeleteRootManifest(ctx context.Context, name []byte) error {
	if err := contextError(ctx); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.roots, string(name))
	return nil
}

func (s *FakeStore) CompareAndSwapRootManifest(ctx context.Context, name []byte, expected, replacement prolly.OptionalBytes) (prolly.RootCASResult, error) {
	if err := contextError(ctx); err != nil {
		return prolly.RootCASResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	current := optional(s.roots, name)
	if !optionalEqual(current, expected) {
		return prolly.RootCASResult{Current: current}, nil
	}
	writeOptional(s.roots, name, replacement)
	return prolly.RootCASResult{Applied: true, Current: replacement.Clone()}, nil
}

func (s *FakeStore) ListRootManifests(ctx context.Context) ([]prolly.NamedStoreRoot, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	keys := sortedKeys(s.roots)
	result := make([]prolly.NamedStoreRoot, len(keys))
	for index, key := range keys {
		result[index] = prolly.NamedStoreRoot{Name: key, Manifest: clone(s.roots[string(key)])}
	}
	return result, nil
}

func (s *FakeStore) CommitTransaction(ctx context.Context, nodes []prolly.NodeMutation, conditions []prolly.RootCondition, roots []prolly.RootWrite) (prolly.StoreTransactionResult, error) {
	if err := contextError(ctx); err != nil {
		return prolly.StoreTransactionResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, condition := range conditions {
		current := optional(s.roots, condition.Name)
		if !optionalEqual(current, condition.Expected) {
			return prolly.StoreTransactionResult{Conflict: &prolly.StoreTransactionConflict{
				Name: clone(condition.Name), Expected: condition.Expected.Clone(), Current: current,
			}}, nil
		}
	}
	applyNodes(s.nodes, nodes)
	for _, root := range roots {
		writeOptional(s.roots, root.Name, root.Replacement)
	}
	return prolly.StoreTransactionResult{Applied: true}, nil
}

func contextError(ctx context.Context) error {
	if ctx == nil {
		return nil
	}
	return ctx.Err()
}

func clone(value []byte) []byte { return append([]byte(nil), value...) }

func optional(values map[string][]byte, key []byte) prolly.OptionalBytes {
	value, ok := values[string(key)]
	if !ok {
		return prolly.MissingBytes()
	}
	return prolly.PresentBytes(value)
}

func optionalEqual(left, right prolly.OptionalBytes) bool {
	return left.Present == right.Present && (!left.Present || bytes.Equal(left.Value, right.Value))
}

func writeOptional(values map[string][]byte, key []byte, value prolly.OptionalBytes) {
	if value.Present {
		values[string(key)] = clone(value.Value)
	} else {
		delete(values, string(key))
	}
}

func applyNodes(nodes map[string][]byte, mutations []prolly.NodeMutation) {
	for _, mutation := range mutations {
		writeOptional(nodes, mutation.Key, mutation.Value)
	}
}

func hintKey(namespace, key []byte) []byte {
	value := make([]byte, 0, len(namespace)+1+len(key))
	value = append(value, namespace...)
	value = append(value, 0)
	return append(value, key...)
}

func sortedKeys(values map[string][]byte) [][]byte {
	result := make([][]byte, 0, len(values))
	for key := range values {
		result = append(result, []byte(key))
	}
	sort.Slice(result, func(i, j int) bool { return bytes.Compare(result[i], result[j]) < 0 })
	return result
}
