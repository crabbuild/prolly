package prolly

import (
	"bytes"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
)

type IndexProjection int32

const (
	IndexProjectionKeysOnly IndexProjection = 1
	IndexProjectionInclude  IndexProjection = 2
	IndexProjectionAll      IndexProjection = 3
)

type IndexEntry struct {
	Term       []byte
	Projection []byte
}

type IndexExtractor interface {
	Extract(primaryKey, sourceValue []byte) ([]IndexEntry, error)
}

type IndexExtractorFunc func(primaryKey, sourceValue []byte) ([]IndexEntry, error)

func (f IndexExtractorFunc) Extract(primaryKey, sourceValue []byte) ([]IndexEntry, error) {
	return f(primaryKey, sourceValue)
}

type SecondaryIndexLimits struct {
	MaxTermBytes                      uint64
	MaxProjectionBytes                uint64
	MaxAllValueBytes                  uint64
	MaxTermsPerRecord                 uint64
	MaxProjectedBytesPerRecord        uint64
	MaxDerivedMutationsPerTransaction uint64
	MaxProjectedBytesPerTransaction   uint64
	MaxIndexes                        uint64
	BuildPageSize                     uint64
	MaxTemporarySortBytes             uint64
	MaxBundleNodes                    uint64
	MaxBundleBytes                    uint64
	MaxVerificationEntries            uint64
	MaxWriteRetries                   uint64
	MaxBuildRetries                   uint64
}

type IndexRegistry struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

var indexExtractorVtableOnce sync.Once

func NewIndexRegistry() (*IndexRegistry, error) {
	indexExtractorVtableOnce.Do(ffiRegisterIndexExtractorVtable)
	handle, err := ffiNewIndexRegistry()
	if err != nil {
		return nil, err
	}
	registry := &IndexRegistry{handle: handle}
	runtime.SetFinalizer(registry, (*IndexRegistry).Close)
	return registry, nil
}

func (r *IndexRegistry) Close() {
	if r == nil || r.closed.Swap(true) {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	runtime.SetFinalizer(r, nil)
	if r.handle != 0 {
		ffiFreeIndexRegistry(r.handle)
		r.handle = 0
	}
}

func (r *IndexRegistry) withHandle() (uint64, func(), error) {
	if r == nil || r.closed.Load() {
		return 0, nil, errors.New("index registry is closed")
	}
	r.mu.RLock()
	if r.closed.Load() || r.handle == 0 {
		r.mu.RUnlock()
		return 0, nil, errors.New("index registry is closed")
	}
	return r.handle, r.mu.RUnlock, nil
}

func (r *IndexRegistry) Register(name []byte, generation uint64, extractorID string, projection IndexProjection, limits *SecondaryIndexLimits, extractor IndexExtractor) error {
	if extractor == nil {
		return errors.New("nil secondary index extractor")
	}
	if projection < IndexProjectionKeysOnly || projection > IndexProjectionAll {
		return errors.New("invalid index projection")
	}
	handle, unlock, err := r.withHandle()
	if err != nil {
		return err
	}
	defer unlock()
	callback := registerGoIndexExtractor(extractor)
	err = ffiIndexRegistryRegister(handle, append([]byte(nil), name...), generation, extractorID, int32(projection), encodeOptionalSecondaryIndexLimits(limits), callback)
	if err != nil {
		removeGoIndexExtractor(callback)
	}
	return err
}

func encodeOptionalSecondaryIndexLimits(limits *SecondaryIndexLimits) []byte {
	if limits == nil {
		return []byte{0}
	}
	var out bytes.Buffer
	out.WriteByte(1)
	values := [...]uint64{
		limits.MaxTermBytes, limits.MaxProjectionBytes, limits.MaxAllValueBytes,
		limits.MaxTermsPerRecord, limits.MaxProjectedBytesPerRecord,
		limits.MaxDerivedMutationsPerTransaction, limits.MaxProjectedBytesPerTransaction,
		limits.MaxIndexes, limits.BuildPageSize, limits.MaxTemporarySortBytes,
		limits.MaxBundleNodes, limits.MaxBundleBytes, limits.MaxVerificationEntries,
		limits.MaxWriteRetries, limits.MaxBuildRetries,
	}
	for _, value := range values {
		writeU64(&out, value)
	}
	return out.Bytes()
}

type IndexedVersion struct {
	SourceVersion  []byte
	CatalogVersion []byte
	IndexCount     uint64
}

type IndexBuildResult struct {
	SourceVersion  []byte
	IndexVersion   []byte
	CatalogVersion []byte
	Generation     uint64
	Entries        uint64
	Attempts       uint64
	Activated      bool
}

type IndexedMap struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (e *Engine) IndexedMap(id []byte, registry *IndexRegistry) (*IndexedMap, error) {
	registryHandle, unlock, err := registry.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	handle, err := ffiEngineIndexedMap(e, append([]byte(nil), id...), registryHandle)
	if err != nil {
		return nil, err
	}
	result := &IndexedMap{handle: handle}
	runtime.SetFinalizer(result, (*IndexedMap).Close)
	return result, nil
}

func (m *IndexedMap) Close() {
	if m == nil || m.closed.Swap(true) {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	runtime.SetFinalizer(m, nil)
	if m.handle != 0 {
		ffiFreeIndexedMap(m.handle)
		m.handle = 0
	}
}
func (m *IndexedMap) withHandle() (uint64, func(), error) {
	if m == nil || m.closed.Load() {
		return 0, nil, errors.New("indexed map is closed")
	}
	m.mu.RLock()
	if m.closed.Load() || m.handle == 0 {
		m.mu.RUnlock()
		return 0, nil, errors.New("indexed map is closed")
	}
	return m.handle, m.mu.RUnlock, nil
}

func (m *IndexedMap) Put(key, value []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapPut(handle, append([]byte(nil), key...), append([]byte(nil), value...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}
func (m *IndexedMap) Delete(key []byte) (IndexedVersion, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexedVersion{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapDelete(handle, append([]byte(nil), key...))
	if err != nil {
		return IndexedVersion{}, err
	}
	return decodeIndexedVersion(raw)
}
func (m *IndexedMap) Get(key []byte) ([]byte, bool, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, false, err
	}
	defer unlock()
	raw, err := ffiIndexedMapGet(handle, append([]byte(nil), key...))
	if err != nil {
		return nil, false, err
	}
	decoder := byteDecoder{data: raw}
	value, ok, err := decoder.readOptionalByteArray()
	if err != nil {
		return nil, false, err
	}
	return value, ok, decoder.done()
}
func (m *IndexedMap) EnsureIndex(name []byte) (IndexBuildResult, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return IndexBuildResult{}, err
	}
	defer unlock()
	raw, err := ffiIndexedMapEnsureIndex(handle, append([]byte(nil), name...))
	if err != nil {
		return IndexBuildResult{}, err
	}
	return decodeIndexBuildResult(raw)
}
func (m *IndexedMap) Snapshot() (*IndexedSnapshot, error) {
	handle, unlock, err := m.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	snapshot, err := ffiIndexedMapSnapshot(handle)
	if err != nil {
		return nil, err
	}
	result := &IndexedSnapshot{handle: snapshot}
	runtime.SetFinalizer(result, (*IndexedSnapshot).Close)
	return result, nil
}

type IndexedSnapshot struct {
	handle uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (s *IndexedSnapshot) Close() {
	if s == nil || s.closed.Swap(true) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime.SetFinalizer(s, nil)
	if s.handle != 0 {
		ffiFreeIndexedSnapshot(s.handle)
		s.handle = 0
	}
}
func (s *IndexedSnapshot) withHandle() (uint64, func(), error) {
	if s == nil || s.closed.Load() {
		return 0, nil, errors.New("indexed snapshot is closed")
	}
	s.mu.RLock()
	if s.closed.Load() || s.handle == 0 {
		s.mu.RUnlock()
		return 0, nil, errors.New("indexed snapshot is closed")
	}
	return s.handle, s.mu.RUnlock, nil
}
func (s *IndexedSnapshot) Index(name []byte) (*SecondaryIndex, error) {
	handle, unlock, err := s.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	index, err := ffiIndexedSnapshotIndex(handle, append([]byte(nil), name...))
	if err != nil {
		return nil, err
	}
	fast, err := ffiSecondaryIndexFastHandle(index)
	if err != nil {
		ffiFreeSecondaryIndex(index)
		return nil, err
	}
	result := &SecondaryIndex{handle: index, fast: fast}
	runtime.SetFinalizer(result, (*SecondaryIndex).Close)
	return result, nil
}

type SecondaryIndex struct {
	handle uint64
	fast   uint64
	closed atomic.Bool
	mu     sync.RWMutex
}

func (i *SecondaryIndex) Close() {
	if i == nil || i.closed.Swap(true) {
		return
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	runtime.SetFinalizer(i, nil)
	if i.handle != 0 {
		ffiFreeSecondaryIndex(i.handle)
		i.handle = 0
		i.fast = 0
	}
}
func (i *SecondaryIndex) withHandle() (uint64, uint64, func(), error) {
	if i == nil || i.closed.Load() {
		return 0, 0, nil, errors.New("secondary index is closed")
	}
	i.mu.RLock()
	if i.closed.Load() || i.handle == 0 || i.fast == 0 {
		i.mu.RUnlock()
		return 0, 0, nil, errors.New("secondary index is closed")
	}
	return i.handle, i.fast, i.mu.RUnlock, nil
}

type IndexedSourceRecord struct {
	Term        []byte
	PrimaryKey  []byte
	Projection  []byte
	SourceValue []byte
}

func (i *SecondaryIndex) Records(term []byte) ([]IndexedSourceRecord, error) {
	handle, _, unlock, err := i.withHandle()
	if err != nil {
		return nil, err
	}
	defer unlock()
	raw, err := ffiSecondaryIndexRecords(handle, append([]byte(nil), term...))
	if err != nil {
		return nil, err
	}
	return decodeIndexedSourceRecords(raw)
}

func decodeIndexedVersion(raw []byte) (IndexedVersion, error) {
	d := byteDecoder{data: raw}
	source, err := d.readByteArray()
	if err != nil {
		return IndexedVersion{}, err
	}
	catalog, _, err := d.readOptionalByteArray()
	if err != nil {
		return IndexedVersion{}, err
	}
	count, err := d.readUint64()
	if err != nil {
		return IndexedVersion{}, err
	}
	return IndexedVersion{source, catalog, count}, d.done()
}
func decodeIndexBuildResult(raw []byte) (IndexBuildResult, error) {
	d := byteDecoder{data: raw}
	source, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	index, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	catalog, err := d.readByteArray()
	if err != nil {
		return IndexBuildResult{}, err
	}
	generation, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	entries, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	attempts, err := d.readUint64()
	if err != nil {
		return IndexBuildResult{}, err
	}
	activated, err := d.readBool()
	if err != nil {
		return IndexBuildResult{}, err
	}
	return IndexBuildResult{source, index, catalog, generation, entries, attempts, activated}, d.done()
}
func decodeIndexedSourceRecords(raw []byte) ([]IndexedSourceRecord, error) {
	d := byteDecoder{data: raw}
	count, err := d.readInt32()
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, errors.New("negative indexed source record count")
	}
	rows := make([]IndexedSourceRecord, 0, count)
	for range count {
		term, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		key, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		projection, _, err := d.readOptionalByteArray()
		if err != nil {
			return nil, err
		}
		value, err := d.readByteArray()
		if err != nil {
			return nil, err
		}
		rows = append(rows, IndexedSourceRecord{term, key, projection, value})
	}
	return rows, d.done()
}
